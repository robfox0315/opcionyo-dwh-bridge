"""
╔══════════════════════════════════════════════════════════════╗
║  DWH BRIDGE · Opción Yo · v1.3.3                                ║
║  API de solo lectura hacia ClickHouse + monitores de negocio  ║
║  (SLA ATC, Pedidos de especialista, Escalamiento de pushes,   ║
║  Fallos de pushes de onboarding). Un solo proceso, apto Render ║
║  free.                                                          ║
╚══════════════════════════════════════════════════════════════╝

FIX v1.3.1: _cliente() usaba settings={"max_execution_time": ...},
que ClickHouse rechaza para el usuario readonly. Se reemplazó por
send_receive_timeout (timeout de socket del cliente).

FIX v1.3.2: filtro de Pedidos de especialista corregido (exige que
el asunto empiece literalmente con la frase) + alerta SLA histórica
restaurada.

NUEVO v1.3.3: Monitor de fallos en pushes de onboarding. Hallazgo
01/09/2026: el push de WhatsApp de confirmación de sesiones a veces
falla con FAILURE_BY_HUMAN_HANDOVER o FAILURE_BY_UNABLE_TO_CONTACT
incluso cuando el agente SÍ lo disparó bien — es una falla de
entrega real de Treble, no un problema de que se olviden de
mandarlo. No se puede arreglar desde HubSpot (vive en la config del
bot en Treble, sin acceso). Este monitor no soluciona la causa raíz,
pero asegura que ningún cliente se quede sin el mensaje en silencio:
avisa en minutos para que alguien lo reintente a mano.
"""

import os
import re
import json
import time
import uuid
import sqlite3
import logging
import threading
import urllib.request
import urllib.error
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Header
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","modulo":"%(name)s","msg":%(message)r}',
)
log = logging.getLogger("bridge")

METRICAS = {
    "revisiones_sla": 0, "alertas_sla_enviadas": 0, "alertas_sla_fallidas": 0,
    "alertas_sla_historico_enviadas": 0,
    "revisiones_pedidos": 0, "alertas_pedidos_enviadas": 0,
    "revisiones_escalamiento": 0, "contactos_encontrados": 0,
    "contactos_ambiguos": 0, "contactos_no_encontrados": 0,
    "pacientes_recuperados": 0, "pacientes_escalados": 0,
    "revisiones_onboarding": 0, "alertas_onboarding_enviadas": 0,
    "errores_slack": 0, "errores_hubspot": 0, "errores_dwh": 0,
}


def _mask_phone(numero):
    if not numero or len(numero) < 8:
        return "***"
    return numero[:5] + "****" + numero[-4:]


ACCOUNT_ID = int(os.environ.get("HUBSPOT_ACCOUNT_ID", "40159402"))

DWH_HOST = os.environ.get("DWH_HOST", "")
DWH_PORT = int(os.environ.get("DWH_PORT", "8443"))
DWH_USER = os.environ.get("DWH_USER", "")
DWH_PASSWORD = os.environ.get("DWH_PASSWORD", "")
DWH_DATABASE = os.environ.get("DWH_DATABASE", "client_analytics")
API_KEY = os.environ.get("BRIDGE_API_KEY", "")
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
PEDIDOS_SLACK_WEBHOOK_URL = os.environ.get("PEDIDOS_SLACK_WEBHOOK_URL", "")
ESCALAMIENTO_SLACK_WEBHOOK_URL = os.environ.get("ESCALAMIENTO_SLACK_WEBHOOK_URL", "")
ONBOARDING_SLACK_WEBHOOK_URL = os.environ.get("ONBOARDING_SLACK_WEBHOOK_URL", "")

SLA_THRESHOLD_SECONDS = int(os.environ.get("SLA_THRESHOLD_SECONDS", "120"))
SLA_POLL_INTERVAL_SECONDS = int(os.environ.get("SLA_POLL_INTERVAL_SECONDS", "60"))
PEDIDOS_POLL_INTERVAL_SECONDS = int(os.environ.get("PEDIDOS_POLL_INTERVAL_SECONDS", "60"))
ESCALAMIENTO_UMBRAL = int(os.environ.get("ESCALAMIENTO_UMBRAL", "3"))
ESCALAMIENTO_HORA_UTC = int(os.environ.get("ESCALAMIENTO_HORA_UTC", "13"))
ESCALAMIENTO_VENTANA_SIN_RESULTADO_HORAS = int(os.environ.get("ESCALAMIENTO_VENTANA_SIN_RESULTADO_HORAS", "720"))
ONBOARDING_POLL_INTERVAL_SECONDS = int(os.environ.get("ONBOARDING_POLL_INTERVAL_SECONDS", "300"))  # cada 5 min

ATC_AGENTS_RAW = os.environ.get(
    "ATC_AGENTS",
    "Camila Rodriguez,Estefany Suárez,Mary Cárdenas,Sofia Castro,"
    "Yesith Solano,Eduardo Liendo,Samira Pirique,Lizbeth Calcina",
)
ATC_AGENTS = [a.strip() for a in ATC_AGENTS_RAW.split(",") if a.strip()]

# Los 6 pushes de onboarding/confirmación de sesión que armamos —
# los que están sujetos a este monitor de fallos.
ONBOARDING_POLL_IDS_RAW = os.environ.get(
    "ONBOARDING_POLL_IDS",
    "1466598,1466668,1468229,1468224,1466613,1466629"
)
ONBOARDING_POLL_IDS = [p.strip() for p in ONBOARDING_POLL_IDS_RAW.split(",") if p.strip()]

ONBOARDING_NOMBRES_POLL = {
    "1466598": "Confirmación de sesiones premium",
    "1466668": "Confirmacion de sesiones basico",
    "1468229": "Recordatorio autoenrollment premium",
    "1468224": "Recordatorio autoenrollment básico",
    "1466613": "Especialista confirmación 6 horas antes",
    "1466629": "Especialista confirmación 3 dias antes",
}

DB_PATH = os.environ.get("BRIDGE_DB_PATH", "/tmp/bridge_state.db")
LOCK_PATH = os.environ.get("BRIDGE_LOCK_PATH", "/tmp/bridge_monitors.lock")

REQUISITOS = {
    "api_dwh": {"DWH_HOST": DWH_HOST, "DWH_USER": DWH_USER, "DWH_PASSWORD": DWH_PASSWORD, "BRIDGE_API_KEY": API_KEY},
    "hubspot": {"HUBSPOT_TOKEN": HUBSPOT_TOKEN},
    "monitor_sla": {"SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL},
    "monitor_pedidos": {"PEDIDOS_SLACK_WEBHOOK_URL": PEDIDOS_SLACK_WEBHOOK_URL},
    "monitor_escalamiento": {"ESCALAMIENTO_SLACK_WEBHOOK_URL": ESCALAMIENTO_SLACK_WEBHOOK_URL},
    "monitor_onboarding": {"ONBOARDING_SLACK_WEBHOOK_URL": ONBOARDING_SLACK_WEBHOOK_URL},
}


def _validar_entorno():
    estado = {}
    for componente, variables in REQUISITOS.items():
        faltantes = [k for k, v in variables.items() if not v]
        estado[componente] = {"ok": len(faltantes) == 0, "faltantes": faltantes}
        if faltantes:
            log.warning(f"[config] {componente} incompleto — faltan: {faltantes}")
        else:
            log.info(f"[config] {componente} OK")
    return estado


ESTADO_CONFIG = _validar_entorno()

_db_lock = threading.Lock()


def _init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS eventos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                external_id TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                notified_at TEXT,
                status TEXT NOT NULL,
                UNIQUE(event_type, external_id)
            )
        """)
        con.commit()


def _evento_ya_notificado(event_type, external_id):
    with _db_lock, sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT status FROM eventos WHERE event_type=? AND external_id=?",
            (event_type, external_id),
        ).fetchone()
        return row is not None and row[0] == "notified"


def _evento_marcar(event_type, external_id, status, notified=False):
    ahora = datetime.now(timezone.utc).isoformat()
    with _db_lock, sqlite3.connect(DB_PATH) as con:
        con.execute(
            """INSERT INTO eventos (event_type, external_id, detected_at, notified_at, status)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(event_type, external_id) DO UPDATE SET
                 notified_at=excluded.notified_at, status=excluded.status""",
            (event_type, external_id, ahora, ahora if notified else None, status),
        )
        con.commit()


def _evento_seed_baseline(event_type, external_ids):
    ahora = datetime.now(timezone.utc).isoformat()
    with _db_lock, sqlite3.connect(DB_PATH) as con:
        con.executemany(
            """INSERT OR IGNORE INTO eventos (event_type, external_id, detected_at, notified_at, status)
               VALUES (?, ?, ?, ?, 'baseline')""",
            [(event_type, eid, ahora, ahora) for eid in external_ids],
        )
        con.commit()


def _adquirir_lock_monitores():
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        try:
            with open(LOCK_PATH) as f:
                pid_viejo = int(f.read().strip())
            os.kill(pid_viejo, 0)
            return False
        except (ProcessLookupError, ValueError, OSError):
            os.remove(LOCK_PATH)
            return _adquirir_lock_monitores()


def _con_reintentos(fn, *, intentos=3, base_espera=1.5, nombre="operacion"):
    ultimo_error = None
    for intento in range(1, intentos + 1):
        try:
            return fn()
        except urllib.error.HTTPError as e:
            ultimo_error = e
            if e.code == 429 or e.code >= 500:
                espera = base_espera * (2 ** (intento - 1))
                log.warning(f"[reintentos] {nombre} HTTP {e.code}, intento {intento}/{intentos}, espero {espera:.1f}s")
                time.sleep(espera)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            ultimo_error = e
            espera = base_espera * (2 ** (intento - 1))
            log.warning(f"[reintentos] {nombre} error de red, intento {intento}/{intentos}, espero {espera:.1f}s: {e}")
            time.sleep(espera)
    raise ultimo_error


app = FastAPI(title="Opción Yo · DWH Bridge", version="1.3.3")

PALABRAS_PROHIBIDAS = [
    "insert", "update", "delete", "drop", "alter", "create", "truncate",
    "grant", "revoke", "rename", "attach", "detach", "kill", "system",
    "optimize", "exec", "call",
]
VERBOS_PERMITIDOS = ("select", "show", "describe", "desc", "explain", "with")
MAX_FILAS = int(os.environ.get("BRIDGE_MAX_FILAS", "5000"))
QUERY_TIMEOUT_SEGUNDOS = int(os.environ.get("BRIDGE_QUERY_TIMEOUT", "20"))


def _validar_sql(sql):
    sql_limpio = sql.strip().rstrip(";").strip()
    if not sql_limpio:
        raise HTTPException(400, "SQL vacío.")
    primera_palabra = sql_limpio.lower().split(None, 1)[0]
    if primera_palabra not in VERBOS_PERMITIDOS:
        raise HTTPException(403, f"Solo se permiten consultas de lectura ({', '.join(VERBOS_PERMITIDOS)}).")
    if ";" in sql_limpio:
        raise HTTPException(403, "No se permiten múltiples sentencias.")
    bajo = sql_limpio.lower()
    for palabra in PALABRAS_PROHIBIDAS:
        if re.search(rf"\b{palabra}\b", bajo):
            raise HTTPException(403, f"Palabra no permitida: '{palabra}'.")
    if primera_palabra == "select" and "limit" not in bajo:
        sql_limpio += f" LIMIT {MAX_FILAS}"
    return sql_limpio


def _cliente():
    if not (DWH_HOST and DWH_USER and DWH_PASSWORD):
        raise HTTPException(500, "Credenciales del DWH no configuradas.")
    try:
        return clickhouse_connect.get_client(
            host=DWH_HOST, port=DWH_PORT, username=DWH_USER, password=DWH_PASSWORD,
            database=DWH_DATABASE, secure=True, connect_timeout=10,
            send_receive_timeout=QUERY_TIMEOUT_SEGUNDOS,
        )
    except Exception as e:
        METRICAS["errores_dwh"] += 1
        raise HTTPException(502, f"No se pudo conectar al DWH: {e}")


def _chequear_clave(x_api_key):
    if not API_KEY:
        raise HTTPException(500, "BRIDGE_API_KEY no configurada.")
    if x_api_key != API_KEY:
        raise HTTPException(401, "Clave inválida o faltante (header X-API-Key).")


@app.get("/")
def home():
    return {"servicio": "Opción Yo DWH Bridge", "version": "1.3.3", "estado": "activo"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/deep")
def health_deep(x_api_key: str | None = Header(default=None)):
    _chequear_clave(x_api_key)
    try:
        _cliente().query("SELECT 1")
        dwh_ok = True
    except HTTPException:
        dwh_ok = False
    return {
        "dwh_conectado": dwh_ok,
        "config": {k: v["ok"] for k, v in ESTADO_CONFIG.items()},
        "metricas": METRICAS,
    }


@app.post("/query")
def query(body: dict, x_api_key: str | None = Header(default=None)):
    _chequear_clave(x_api_key)
    sql_seguro = _validar_sql(body.get("sql", ""))
    client = _cliente()
    try:
        result = client.query(sql_seguro)
        columnas = result.column_names
        filas = [dict(zip(columnas, row)) for row in result.result_rows]
        return {"columnas": list(columnas), "filas": filas, "total_filas": len(filas)}
    except HTTPException:
        raise
    except Exception as e:
        METRICAS["errores_dwh"] += 1
        raise HTTPException(400, f"Error al ejecutar la consulta: {e}")


def _query_interna(sql):
    sql_seguro = _validar_sql(sql)
    client = _cliente()
    result = client.query(sql_seguro)
    columnas = result.column_names
    return [dict(zip(columnas, row)) for row in result.result_rows]


def _hubspot_request(method, path, body=None):
    def _do():
        url = f"https://api.hubspot.com{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode()) if resp.length != 0 else {}
    try:
        return _con_reintentos(_do, nombre=f"hubspot {method} {path}")
    except Exception as e:
        METRICAS["errores_hubspot"] += 1
        raise


def _normalizar_e164(country_code, cellphone):
    cc = re.sub(r"\D", "", country_code or "")
    num = re.sub(r"\D", "", cellphone or "")
    if not cc or not num:
        return None
    return f"+{cc}{num}"


def _buscar_contacto_por_telefono(country_code, cellphone):
    e164 = _normalizar_e164(country_code, cellphone)
    if not e164:
        return {"resultado": "no_encontrado", "contact": None, "candidatos": []}

    body = {
        "filterGroups": [
            {"filters": [{"propertyName": "hs_whatsapp_phone_number", "operator": "EQ", "value": e164}]},
            {"filters": [{"propertyName": "phone", "operator": "EQ", "value": e164}]},
            {"filters": [{"propertyName": "mobilephone", "operator": "EQ", "value": e164}]},
        ],
        "properties": ["firstname", "lastname", "hs_object_id", "fecha_sesion", "proxima_sesion", "fecha_ultimo_pago"],
        "limit": 10,
    }
    data = _hubspot_request("POST", "/crm/v3/objects/contacts/search", body)
    resultados = data.get("results", [])
    unicos = list({r["id"]: r for r in resultados}.values())

    if len(unicos) == 0:
        METRICAS["contactos_no_encontrados"] += 1
        return {"resultado": "no_encontrado", "contact": None, "candidatos": []}
    if len(unicos) == 1:
        METRICAS["contactos_encontrados"] += 1
        return {"resultado": "valido", "contact": unicos[0], "candidatos": []}
    METRICAS["contactos_ambiguos"] += 1
    return {"resultado": "ambiguo", "contact": None, "candidatos": unicos}


def _slack_enviar(webhook_url, texto, nombre="slack"):
    def _do():
        req = urllib.request.Request(
            webhook_url, data=json.dumps({"text": texto}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    try:
        return _con_reintentos(_do, nombre=nombre)
    except Exception as e:
        METRICAS["errores_slack"] += 1
        log.error(f"[slack] fallo definitivo enviando a {nombre}: {e}")
        raise


# ══════════════════════════════════════════════════════════════
#  MONITOR DE SLA ATC — dos tipos de alerta, ambas restauradas:
#  - "activo": sigue esperando respuesta ahora mismo.
#  - "histórico": ya respondió, pero tardó más del umbral. Mensaje
#    distinto, nunca dice "sin respuesta" (porque ya la hubo).
# ══════════════════════════════════════════════════════════════

_atc_agents_sql = ", ".join(f"'{a}'" for a in ATC_AGENTS)

SQL_SLA_ACTIVO = f"""
SELECT conversation_id, agent_name, contact_wa_id, assigned_at,
       dateDiff('second', assigned_at, now()) as seg_esperando
FROM client_analytics.fact_conversations
WHERE assigned_at IS NOT NULL
  AND assigned_at > now() - INTERVAL 1 DAY
  AND agent_name IN ({_atc_agents_sql})
  AND first_agent_message_at IS NULL
  AND dateDiff('second', assigned_at, now()) >= {SLA_THRESHOLD_SECONDS}
ORDER BY assigned_at ASC
"""

SQL_SLA_HISTORICO = f"""
SELECT conversation_id, agent_name, contact_wa_id, assigned_at, first_agent_message_at,
       dateDiff('second', assigned_at, first_agent_message_at) as seg_tardanza
FROM client_analytics.fact_conversations
WHERE assigned_at IS NOT NULL
  AND assigned_at > now() - INTERVAL 1 DAY
  AND agent_name IN ({_atc_agents_sql})
  AND first_agent_message_at IS NOT NULL
  AND dateDiff('second', assigned_at, first_agent_message_at) >= {SLA_THRESHOLD_SECONDS}
ORDER BY assigned_at DESC
"""


def _sla_revisar_activo():
    filas = _query_interna(SQL_SLA_ACTIVO)
    log.info(f"[sla-activo] {len(filas)} conversaciones activas sobre el umbral")
    for fila in filas:
        cid = str(fila["conversation_id"])
        if _evento_ya_notificado("sla_activo", cid):
            continue
        minutos = round(fila["seg_esperando"] / 60, 1)
        texto = (
            f":stopwatch: *SLA de respuesta activo (sigue esperando)*\n"
            f"Conversación #{fila['conversation_id']} asignada a *{fila.get('agent_name') or 'Sin agente'}* "
            f"hace *{minutos} min* sin primera respuesta.\n"
            f"Cliente (WhatsApp): `{fila.get('contact_wa_id') or 'N/D'}`"
        )
        try:
            if SLACK_WEBHOOK_URL:
                _slack_enviar(SLACK_WEBHOOK_URL, texto, nombre="sla-activo")
            _evento_marcar("sla_activo", cid, "notified", notified=True)
            METRICAS["alertas_sla_enviadas"] += 1
        except Exception:
            METRICAS["alertas_sla_fallidas"] += 1
            _evento_marcar("sla_activo", cid, "error_envio")


def _sla_revisar_historico():
    filas = _query_interna(SQL_SLA_HISTORICO)
    log.info(f"[sla-historico] {len(filas)} conversaciones respondidas fuera de tiempo")
    for fila in filas:
        cid = str(fila["conversation_id"])
        if _evento_ya_notificado("sla_historico", cid):
            continue
        minutos = round(fila["seg_tardanza"] / 60, 1)
        texto = (
            f":warning: *SLA incumplido — respondió tarde*\n"
            f"Conversación #{fila['conversation_id']} de *{fila.get('agent_name') or 'Sin agente'}* "
            f"tardó *{minutos} min* en la primera respuesta (umbral: {SLA_THRESHOLD_SECONDS // 60} min).\n"
            f"Cliente (WhatsApp): `{fila.get('contact_wa_id') or 'N/D'}`"
        )
        try:
            if SLACK_WEBHOOK_URL:
                _slack_enviar(SLACK_WEBHOOK_URL, texto, nombre="sla-historico")
            _evento_marcar("sla_historico", cid, "notified", notified=True)
            METRICAS["alertas_sla_historico_enviadas"] += 1
        except Exception:
            _evento_marcar("sla_historico", cid, "error_envio")


def _sla_monitor_loop():
    log.info("[sla] hilo iniciado")
    try:
        baseline_activo = _query_interna(SQL_SLA_ACTIVO)
        _evento_seed_baseline("sla_activo", [str(f["conversation_id"]) for f in baseline_activo])
        baseline_hist = _query_interna(SQL_SLA_HISTORICO)
        _evento_seed_baseline("sla_historico", [str(f["conversation_id"]) for f in baseline_hist])
        log.info(f"[sla] foto inicial: {len(baseline_activo)} activos + {len(baseline_hist)} históricos marcados como vistos")
    except Exception as e:
        log.error(f"[sla] error en foto inicial: {e}")

    while True:
        try:
            METRICAS["revisiones_sla"] += 1
            _sla_revisar_activo()
            _sla_revisar_historico()
        except Exception as e:
            log.error(f"[sla] error en revisión: {e}")
        time.sleep(SLA_POLL_INTERVAL_SECONDS)


# ══════════════════════════════════════════════════════════════
#  TRIAGE "PEDIDO DE ESPECIALISTA" — filtro de asunto corregido
# ══════════════════════════════════════════════════════════════

PIPELINE_ADMINISTRACION = os.environ.get("PIPELINE_ADMINISTRACION", "74755616")
STAGE_BANDEJA_ENTRADA = os.environ.get("STAGE_BANDEJA_ENTRADA", "143884924")

PEDIDOS_DRAFTS = {
    "Reagendar sesión": "Hola {nombre}, recibido — reviso la disponibilidad para reagendar la sesión de la clienta {id_cliente} y te confirmo un horario en breve.",
    "Pausar plan": "Listo {nombre}, pauso el plan de la clienta {id_cliente} según lo que indicaste. Te aviso cuando esté hecho.",
    "Postergar pago": "Confirmado {nombre}, gestiono la postergación del cobro de la clienta {id_cliente}. Te confirmo cuando quede aplicado.",
    "Seguimiento / contactar cliente": "Gracias por avisar {nombre}, nos comunicamos con la clienta {id_cliente} para dar seguimiento y te contamos qué nos responde.",
    "Corrección de estado de sesión": "Listo {nombre}, corrijo el estado de la sesión de la clienta {id_cliente} tal como indicaste.",
}
PEDIDOS_EMOJI = {
    "🔴 SENSIBLE — requiere revisión humana, no automatizar": "🔴",
    "Reagendar sesión": "📅", "Pausar plan": "⏸️", "Postergar pago": "💳",
    "Seguimiento / contactar cliente": "📞", "Corrección de estado de sesión": "✏️",
    "Soporte técnico / sistema": "🛠️", "Otro — revisar manualmente": "❓",
}


def _pedidos_obtener_tickets():
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_ADMINISTRACION},
            {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": STAGE_BANDEJA_ENTRADA},
            {"propertyName": "subject", "operator": "CONTAINS_TOKEN", "value": "*Pedido de especialista*"},
        ]}],
        "properties": ["subject", "content", "createdate"],
        "limit": 100,
    }
    data = _hubspot_request("POST", "/crm/v3/objects/tickets/search", body)
    resultados = data.get("results", [])
    return [r for r in resultados if (r["properties"].get("subject") or "").strip().lower().startswith("pedido de especialista")]


def _pedidos_clasificar(subject, content):
    t = (subject + " " + content).lower()
    if any(k in t for k in ["hospitaliz", "salud", "grave", "riesgo", "hematocrito", "no volverá", "no insistir", "delicad", "suicid", "crisis"]):
        return "🔴 SENSIBLE — requiere revisión humana, no automatizar"
    if any(k in t for k in ["reagendar", "cambiar horario", "reprogramar", "próxima cita", "no logre reagendar"]):
        return "Reagendar sesión"
    if any(k in t for k in ["pausar", "pausa su plan", "pausa el plan"]):
        return "Pausar plan"
    if any(k in t for k in ["postergar pago", "cobrarse", "cobrársele", "postergar", "más adelante el cobro"]):
        return "Postergar pago"
    if any(k in t for k in ["no la veo", "no ha asistido", "se pudieran comunicar", "contactar", "no lee sus mensajes"]):
        return "Seguimiento / contactar cliente"
    if any(k in t for k in ["cambiar el estatus", "marcar completada", "corregir estado", "se fue la luz", "desconect", "no logre marcar"]):
        return "Corrección de estado de sesión"
    if any(k in t for k in ["app descargada", "no la tiene descargada", "no le permite", "agendar en un ar de"]):
        return "Soporte técnico / sistema"
    return "Otro — revisar manualmente"


def _pedidos_extraer_nombre(subject):
    m = re.search(r'especialista:\s*(?:\(E\)\s*)?(.+?)\s*Por ID', subject)
    if m:
        return m.group(1).strip()
    m2 = re.match(r'^(.+?)\s*\(ID:', subject)
    return m2.group(1).strip() if m2 else subject


def _pedidos_extraer_id(subject, content):
    m = re.search(r'ID:\s*(\d+)', subject + " " + content)
    return m.group(1) if m else "N/D"


def _pedidos_enviar_slack_ticket(t):
    e = PEDIDOS_EMOJI.get(t["categoria"], "•")
    link = f"https://app.hubspot.com/contacts/{ACCOUNT_ID}/record/0-5/{t['ticket_id']}"
    partes = [
        f"{e} *Nuevo Pedido de especialista*",
        f"*Categoría:* {t['categoria']}", f"*Especialista:* {t['especialista']}",
        f"*Cliente ID:* {t['id_cliente']}", "", "*Mensaje completo:*", f"> {t['content']}",
    ]
    if t.get("draft_respuesta"):
        partes += ["", "*💬 Borrador de respuesta sugerido:*", f"> {t['draft_respuesta']}"]
    partes += ["", f"<{link}|Abrir ticket en HubSpot>"]
    _slack_enviar(PEDIDOS_SLACK_WEBHOOK_URL, "\n".join(partes), nombre="pedidos")


def _pedidos_clasificar_ticket(r):
    p = r["properties"]
    subject, content = p.get("subject") or "", p.get("content") or ""
    cat = _pedidos_clasificar(subject, content)
    nombre = _pedidos_extraer_nombre(subject)
    id_cliente = _pedidos_extraer_id(subject, content)
    draft = PEDIDOS_DRAFTS.get(cat, "").format(nombre=nombre, id_cliente=id_cliente) if cat in PEDIDOS_DRAFTS else None
    return {"ticket_id": r["id"], "content": content, "categoria": cat,
            "especialista": nombre, "id_cliente": id_cliente, "draft_respuesta": draft}


def _pedidos_monitor_loop():
    log.info("[pedidos] hilo iniciado")
    try:
        tickets_iniciales = _pedidos_obtener_tickets()
        _evento_seed_baseline("pedido_especialista", [r["id"] for r in tickets_iniciales])
        log.info(f"[pedidos] foto inicial: {len(tickets_iniciales)} tickets marcados como vistos")
    except Exception as e:
        log.error(f"[pedidos] error en foto inicial: {e}")

    while True:
        try:
            METRICAS["revisiones_pedidos"] += 1
            tickets = _pedidos_obtener_tickets()
            nuevos = [r for r in tickets if not _evento_ya_notificado("pedido_especialista", r["id"])]
            log.info(f"[pedidos] {len(tickets)} en bandeja, {len(nuevos)} nuevos")
            for r in nuevos:
                clasificado = _pedidos_clasificar_ticket(r)
                try:
                    if PEDIDOS_SLACK_WEBHOOK_URL:
                        _pedidos_enviar_slack_ticket(clasificado)
                    _evento_marcar("pedido_especialista", r["id"], "notified", notified=True)
                    METRICAS["alertas_pedidos_enviadas"] += 1
                except Exception as e:
                    log.error(f"[pedidos] error enviando ticket_id={r['id']}: {e}")
                    _evento_marcar("pedido_especialista", r["id"], "error_envio")
        except Exception as e:
            log.error(f"[pedidos] error en revisión: {e}")
        time.sleep(PEDIDOS_POLL_INTERVAL_SECONDS)


# ══════════════════════════════════════════════════════════════
#  ESCALAMIENTO DE PUSHES
# ══════════════════════════════════════════════════════════════

POLLS_PAGO = [p.strip() for p in os.environ.get(
    "POLLS_PAGO", "Informe pago fallido 48hs"
).split(",") if p.strip()]

POLLS_INASISTENCIA = [p.strip() for p in os.environ.get(
    "POLLS_INASISTENCIA",
    "Inasistencia 2, 3 o 4ta sesión con AR,Inasistencia 2, 3, o 4ta sesión,"
    "Inasistencia Primera sesión,Inasistencias Lau O,Saludo Carol INASISTENCIAS,"
    "Saludo Giselle INASISTENCIAS,Carlos inasistencias"
).split(",") if p.strip()]


def _escalamiento_query_sin_resultado(polls):
    polls_sql = ",".join(f"'{p}'" for p in polls)
    sql = f"""
    SELECT deployment_id, country_code, cellphone, poll_name, status, timestamps_eta
    FROM client_analytics.fact_deployment_status
    WHERE poll_name IN ({polls_sql})
      AND status NOT IN ('DELIVERED', 'SUCCESS')
      AND timestamps_eta > now() - INTERVAL {ESCALAMIENTO_VENTANA_SIN_RESULTADO_HORAS} HOUR
    ORDER BY country_code, cellphone, timestamps_eta ASC
    """
    return _query_interna(sql)


def _agrupar_consecutivos(filas):
    agrupado = {}
    for f in filas:
        key = (f["country_code"], f["cellphone"])
        if key not in agrupado:
            agrupado[key] = {"veces": 0, "deployment_ids": [], "ultimo_push": None, "poll_names": set()}
        agrupado[key]["veces"] += 1
        agrupado[key]["deployment_ids"].append(f["deployment_id"])
        agrupado[key]["poll_names"].add(f["poll_name"])
        ts = f["timestamps_eta"]
        if agrupado[key]["ultimo_push"] is None or ts > agrupado[key]["ultimo_push"]:
            agrupado[key]["ultimo_push"] = ts
    return agrupado


def _escalamiento_recuperacion_pago(contacto, ultimo_push):
    fecha_pago = contacto["properties"].get("fecha_ultimo_pago")
    if not fecha_pago or not ultimo_push:
        return False
    try:
        fp = datetime.fromisoformat(fecha_pago.replace("Z", "+00:00"))
        up = ultimo_push if ultimo_push.tzinfo else ultimo_push.replace(tzinfo=timezone.utc)
        return fp > up
    except Exception:
        return False


def _escalamiento_recuperacion_inasistencia(contacto, ultimo_push):
    if not ultimo_push:
        return False
    up = ultimo_push if ultimo_push.tzinfo else ultimo_push.replace(tzinfo=timezone.utc)
    for campo in ("proxima_sesion", "fecha_sesion"):
        val = contacto["properties"].get(campo)
        if not val:
            continue
        try:
            fecha = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if fecha > up:
                return True
        except Exception:
            continue
    return False


def _escalamiento_procesar_categoria(polls, categoria, campo_contador):
    filas = _escalamiento_query_sin_resultado(polls)
    agrupado = _agrupar_consecutivos(filas)
    log.info(f"[escalamiento:{categoria}] {len(agrupado)} números con pushes sin resultado")

    casos_a_notificar = []
    for (country_code, cellphone), info in agrupado.items():
        if info["veces"] < ESCALAMIENTO_UMBRAL:
            continue
        event_id = f"{categoria}:{country_code}{cellphone}:{max(info['deployment_ids'])}"
        if _evento_ya_notificado("escalamiento", event_id):
            continue

        resultado_match = _buscar_contacto_por_telefono(country_code, cellphone)
        if resultado_match["resultado"] == "ambiguo":
            log.warning(f"[escalamiento] teléfono ambiguo ({len(resultado_match['candidatos'])} candidatos): {_mask_phone(cellphone)}")
            casos_a_notificar.append({
                "categoria": categoria, "veces": info["veces"], "country_code": country_code,
                "cellphone": cellphone, "contact_id": None, "nombre": None, "estado": "ambiguo",
                "n_candidatos": len(resultado_match["candidatos"]),
            })
            _evento_marcar("escalamiento", event_id, "ambiguo")
            continue

        if resultado_match["resultado"] == "no_encontrado":
            casos_a_notificar.append({
                "categoria": categoria, "veces": info["veces"], "country_code": country_code,
                "cellphone": cellphone, "contact_id": None, "nombre": None, "estado": "no_encontrado",
            })
            _evento_marcar("escalamiento", event_id, "no_encontrado")
            continue

        contacto = resultado_match["contact"]
        if categoria == "pago":
            recuperado = _escalamiento_recuperacion_pago(contacto, info["ultimo_push"])
        else:
            recuperado = _escalamiento_recuperacion_inasistencia(contacto, info["ultimo_push"])

        if recuperado:
            METRICAS["pacientes_recuperados"] += 1
            _evento_marcar("escalamiento", event_id, "recuperado")
            continue

        nombre = f"{contacto['properties'].get('firstname') or ''} {contacto['properties'].get('lastname') or ''}".strip()
        try:
            _hubspot_request("PATCH", f"/crm/v3/objects/contacts/{contacto['id']}", {
                "properties": {campo_contador: info["veces"], "requiere_gestion_humana": "true"}
            })
        except Exception as e:
            log.error(f"[escalamiento] error actualizando HubSpot contact_id={contacto['id']}: {e}")

        casos_a_notificar.append({
            "categoria": categoria, "veces": info["veces"], "country_code": country_code,
            "cellphone": cellphone, "contact_id": contacto["id"], "nombre": nombre, "estado": "escalado",
        })
        _evento_marcar("escalamiento", event_id, "notified", notified=True)
        METRICAS["pacientes_escalados"] += 1

    return casos_a_notificar


def _escalamiento_enviar_slack(casos):
    if not casos:
        texto = "*🚨 Escalamiento de pushes — ningún caso nuevo hoy.* ✅"
    else:
        lines = [f"*🚨 Escalamiento de pushes sin resultado — {len(casos)} casos*\n"
                 "_Ya se excluyeron los que se recuperaron (pago real / sesión posterior confirmada)._\n"]
        for c in casos[:30]:
            tel_enmascarado = _mask_phone(f"{c['country_code']}{c['cellphone']}")
            if c["estado"] == "ambiguo":
                lines.append(f"⚠️ *{c['categoria']}* — {c['veces']}x sin resultado — `{tel_enmascarado}` "
                              f"→ {c['n_candidatos']} contactos coinciden, revisar manual")
            elif c["estado"] == "no_encontrado":
                lines.append(f"❓ *{c['categoria']}* — {c['veces']}x sin resultado — `{tel_enmascarado}` → sin match en HubSpot")
            else:
                link = f"https://app.hubspot.com/contacts/{ACCOUNT_ID}/record/0-1/{c['contact_id']}"
                lines.append(f"• *{c['nombre'] or 'Sin nombre'}* — {c['categoria']}, {c['veces']}x sin resultado")
                lines.append(f"  <{link}|Ver contacto>")
        texto = "\n".join(lines)
    if ESCALAMIENTO_SLACK_WEBHOOK_URL:
        _slack_enviar(ESCALAMIENTO_SLACK_WEBHOOK_URL, texto, nombre="escalamiento")


def _escalamiento_revisar_una_vez():
    METRICAS["revisiones_escalamiento"] += 1
    casos_pago = _escalamiento_procesar_categoria(POLLS_PAGO, "pago", "payment_push_count")
    casos_inasist = _escalamiento_procesar_categoria(POLLS_INASISTENCIA, "inasistencia", "attendance_push_count")
    todos = casos_pago + casos_inasist
    _escalamiento_enviar_slack(todos)
    log.info(f"[escalamiento] {len(todos)} casos procesados")


def _escalamiento_monitor_loop():
    log.info("[escalamiento] hilo iniciado")
    while True:
        try:
            ahora = time.gmtime()
            en_ventana = ahora.tm_hour == ESCALAMIENTO_HORA_UTC and ahora.tm_min < 10
            hoy_str = f"{ahora.tm_year}-{ahora.tm_yday}"
            if en_ventana and not _evento_ya_notificado("escalamiento_diario", hoy_str):
                _escalamiento_revisar_una_vez()
                _evento_marcar("escalamiento_diario", hoy_str, "notified", notified=True)
        except Exception as e:
            log.error(f"[escalamiento] error: {e}")
        time.sleep(180)


# ══════════════════════════════════════════════════════════════
#  MONITOR DE FALLOS EN PUSHES DE ONBOARDING (nuevo v1.3.3)
#
#  No arregla la causa raíz (vive en Treble, sin acceso). Solo
#  asegura que ningún fallo real quede en silencio: revisa cada
#  5 minutos los 6 poll_id de onboarding, y en cuanto detecta un
#  registro con status distinto de DELIVERED/SUCCESS, avisa con
#  el contacto y el link, para que alguien lo reintente a mano
#  desde HubSpot (Acciones → Inscribir en workflow) en minutos,
#  no días después por un reclamo del cliente.
# ══════════════════════════════════════════════════════════════

def _onboarding_query_fallos():
    polls_sql = ",".join(f"'{p}'" for p in ONBOARDING_POLL_IDS)
    sql = f"""
    SELECT deployment_id, poll_id, country_code, cellphone, status, timestamps_eta
    FROM client_analytics.fact_deployment_status
    WHERE poll_id IN ({polls_sql})
      AND status NOT IN ('DELIVERED', 'SUCCESS')
      AND timestamps_eta > now() - INTERVAL 24 HOUR
    ORDER BY timestamps_eta DESC
    """
    return _query_interna(sql)


def _onboarding_enviar_alerta(fila, contacto):
    poll_id = str(fila["poll_id"])
    nombre_push = ONBOARDING_NOMBRES_POLL.get(poll_id, f"poll_id {poll_id}")
    nombre_cliente = "Sin nombre"
    link = None
    if contacto:
        p = contacto["properties"]
        nombre_cliente = f"{p.get('firstname') or ''} {p.get('lastname') or ''}".strip() or "Sin nombre"
        link = f"https://app.hubspot.com/contacts/{ACCOUNT_ID}/record/0-1/{contacto['id']}"

    texto = (
        f":rotating_light: *Push de onboarding sin entregar*\n"
        f"*Push:* {nombre_push}\n"
        f"*Cliente:* {nombre_cliente}\n"
        f"*Motivo:* `{fila['status']}`\n"
        f"*Teléfono:* `{_mask_phone(fila['country_code'] + fila['cellphone'])}`\n"
    )
    if link:
        texto += f"<{link}|Ver contacto> — reintentar manual: Acciones → Inscribir en workflow → \"{nombre_push}\""
    else:
        texto += "⚠️ No se encontró el contacto en HubSpot por este número — revisar manual."

    if ONBOARDING_SLACK_WEBHOOK_URL:
        _slack_enviar(ONBOARDING_SLACK_WEBHOOK_URL, texto, nombre="onboarding")


def _onboarding_revisar_una_vez():
    METRICAS["revisiones_onboarding"] += 1
    filas = _onboarding_query_fallos()
    log.info(f"[onboarding] {len(filas)} fallos detectados en ventana de 24h")

    for fila in filas:
        event_id = str(fila["deployment_id"])
        if _evento_ya_notificado("onboarding_fallo", event_id):
            continue

        resultado = _buscar_contacto_por_telefono(fila["country_code"], fila["cellphone"])
        contacto = resultado["contact"] if resultado["resultado"] == "valido" else None

        try:
            _onboarding_enviar_alerta(fila, contacto)
            _evento_marcar("onboarding_fallo", event_id, "notified", notified=True)
            METRICAS["alertas_onboarding_enviadas"] += 1
        except Exception as e:
            log.error(f"[onboarding] error enviando alerta deployment_id={event_id}: {e}")
            _evento_marcar("onboarding_fallo", event_id, "error_envio")


def _onboarding_monitor_loop():
    log.info("[onboarding] hilo iniciado")
    try:
        baseline = _onboarding_query_fallos()
        _evento_seed_baseline("onboarding_fallo", [str(f["deployment_id"]) for f in baseline])
        log.info(f"[onboarding] foto inicial: {len(baseline)} fallos existentes marcados como vistos")
    except Exception as e:
        log.error(f"[onboarding] error en foto inicial: {e}")

    while True:
        try:
            _onboarding_revisar_una_vez()
        except Exception as e:
            log.error(f"[onboarding] error en revisión: {e}")
        time.sleep(ONBOARDING_POLL_INTERVAL_SECONDS)



# ══════════════════════════════════════════════════════════════
#  WORKFLOWS "PUSH - ... (auto)" — crear / listar / auditar / borrar
#  Agregado 05/09/2026 (v1.3.4). BLOQUE PURAMENTE ADITIVO:
#  no modifica ni una línea del código anterior, para no arriesgar
#  nada de lo que ya está corriendo (monitores de SLA, pedidos,
#  escalamiento, onboarding y el endpoint /query).
#
#  Motivo: cierra el gap detectado en la auditoría del 04/09 — cuando
#  aparece un push nuevo en la propiedad `enviar_push` sin su workflow,
#  se crea con una llamada en vez de ~10 min de clics en la UI.
#
#  Esquema descubierto por prueba y error controlado contra la API real
#  (POST /automation/v4/flows es BETA y su schema NO está documentado
#  para acciones de apps custom). Lo que costó encontrar:
#   - "actions" es un ARRAY (no un objeto con claves "1","2": esa forma
#     es solo la representación interna de lectura de la UI).
#   - La acción de Treble usa actionTypeId "1-49295660" con fields
#     planos {"conversation_id","channel_id"}; channel_id es constante
#     (42571) en los 102 pushes, verificado contra workflows reales.
#   - La acción "Editar registro" usa actionTypeId "0-5" y su value
#     EXIGE {"staticValue": "", "type": "STATIC_VALUE"} — sin el "type"
#     HubSpot responde 500 genérico, no un error de validación.
#   - El trigger va en enrollmentCriteria.type = "LIST_BASED" con
#     "listFilterBranch" (no "EVENT_BASED"/"filterBranch").
# ══════════════════════════════════════════════════════════════

CHANNEL_ID_WHATSAPP = os.environ.get("TREBLE_CHANNEL_ID", "42571")
TREBLE_ACTION_TYPE_ID = os.environ.get("TREBLE_ACTION_TYPE_ID", "1-49295660")
TREBLE_ACTION_VERSION = int(os.environ.get("TREBLE_ACTION_VERSION", "4"))

PREFIJO_WORKFLOW_PUSH = "PUSH - "
SUFIJO_WORKFLOW_PUSH = " (auto)"
PROP_ENVIAR_PUSH = "enviar_push"
# Tope de detalles individuales a pedir cuando un workflow no cruza por nombre.
MAX_DETALLES_FLOW = int(os.environ.get("MAX_DETALLES_FLOW", "25"))
LIMITE_SEGUNDOS_DETALLE = int(os.environ.get("LIMITE_SEGUNDOS_DETALLE", "20"))
# Salvaguarda: el portal tiene 1.100+ workflows, muchos críticos de
# Marketing y Ventas. Este bridge solo puede borrar los que encajan con
# estos prefijos, salvo override explícito en la query string.
PREFIJOS_BORRABLES = (PREFIJO_WORKFLOW_PUSH, "TEST - ")

CACHE_FLOWS_SEGUNDOS = int(os.environ.get("CACHE_FLOWS_SEGUNDOS", "60"))
# Presupuesto de tiempo para paginar los ~1.100 flows. Sin tope, el peor caso
# eran 60 páginas × 20s = 20 min y Render cortaba el request a medias.
LIMITE_SEGUNDOS_LISTADO = int(os.environ.get("LIMITE_SEGUNDOS_LISTADO", "45"))
_CACHE_FLOWS = {"ts": 0.0, "datos": None, "completo": False}


def _a_bool(valor, por_defecto=True):
    """
    bool("false") es True en Python: si el body traía {"activar": "false"} el
    workflow se creaba ACTIVO igual. Esto interpreta el string de verdad.
    """
    if valor is None:
        return por_defecto
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, (int, float)):
        return bool(valor)
    return str(valor).strip().lower() in ("1", "true", "t", "yes", "y", "si", "sí")

# Contadores propios: se agregan al dict existente sin tocar su declaración.
for _m in ("workflows_creados", "workflows_eliminados", "auditorias_workflows"):
    METRICAS.setdefault(_m, 0)


def _hubspot_api(method, path, body=None):
    """
    Igual que _hubspot_request pero tolera respuestas sin cuerpo (un DELETE
    devuelve 204 vacío y json.loads("") reventaría). Se define aparte en vez
    de modificar _hubspot_request para no tocar el camino que ya usan los
    monitores en producción.
    """
    def _do():
        url = f"https://api.hubspot.com{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            crudo = resp.read().decode()
            return json.loads(crudo) if crudo.strip() else {}

    try:
        return _con_reintentos(_do, nombre=f"hubspot {method} {path}")
    except Exception:
        METRICAS["errores_hubspot"] += 1
        raise


def _validar_id_numerico(valor, campo):
    """
    Los IDs se concatenan a la URL de HubSpot: sin validar, un valor como
    "../../crm/v3/objects/contacts" saldría de la ruta prevista.
    """
    valor = str(valor or "").strip()
    if not re.fullmatch(r"\d{1,20}", valor):
        raise HTTPException(400, f"{campo} inválido: debe ser numérico.")
    return valor


def _wf_listar_flows(usar_cache=True):
    """
    Todos los flows del portal (~1.100 → ~12 llamadas), con caché corto.
    Devuelve (flows, completo). `completo` es False si la paginación se cortó
    por tiempo o por el tope de páginas: un listado a medias haría que la
    auditoría reporte como "faltantes" pushes que SÍ tienen workflow, y crear
    sobre esa base generaría DUPLICADOS (el cliente recibiría el push dos
    veces). Por eso la completitud se propaga en vez de ocultarse.
    """
    ahora = time.time()
    if usar_cache and _CACHE_FLOWS["datos"] is not None and (ahora - _CACHE_FLOWS["ts"]) < CACHE_FLOWS_SEGUNDOS:
        return _CACHE_FLOWS["datos"], _CACHE_FLOWS["completo"]

    flows, after, vistos, completo = [], None, set(), False
    inicio = time.time()
    for _ in range(60):
        if time.time() - inicio > LIMITE_SEGUNDOS_LISTADO:
            log.warning(f"[workflows] listado cortado por tiempo tras {len(flows)} flows")
            break
        path = "/automation/v4/flows?limit=100"
        if after:
            path += f"&after={after}"
        data = _hubspot_api("GET", path)
        flows.extend(data.get("results", []))
        after = ((data.get("paging") or {}).get("next") or {}).get("after")
        if not after or after in vistos:   # sin cursor = terminó; repetido = bucle
            completo = not after
            break
        vistos.add(after)

    _CACHE_FLOWS.update({"datos": flows, "ts": ahora, "completo": completo})
    log.info(f"[workflows] listados {len(flows)} flows del portal (completo={completo})")
    return flows, completo


def _wf_conversation_ids(flow):
    """
    conversation_id que dispara un flow, leído del detalle del flow.
    OJO: el LISTADO (GET /automation/v4/flows) NO devuelve `actions` ni
    `enrollmentCriteria` — solo id, name, isEnabled y fechas. Verificado
    contra la API real. Por eso esta función solo sirve sobre el DETALLE
    (GET /automation/v4/flows/{id}); para el listado se usa el cruce por
    nombre de _wf_push().
    """
    ids = set()
    for accion in flow.get("actions") or []:
        if accion.get("actionTypeId") == TREBLE_ACTION_TYPE_ID:
            cid = (accion.get("fields") or {}).get("conversation_id")
            if cid:
                ids.add(str(cid))
    if ids:
        return ids
    ids.update(re.findall(r"PUSH_(\d+)", json.dumps(flow.get("enrollmentCriteria") or {})))
    return ids


def _label_desde_nombre(nombre):
    """'PUSH - Saludo Gestoras Consultoria (auto)' → 'Saludo Gestoras Consultoria'."""
    interno = nombre[len(PREFIJO_WORKFLOW_PUSH):]
    if interno.endswith(SUFIJO_WORKFLOW_PUSH):
        interno = interno[: -len(SUFIJO_WORKFLOW_PUSH)]
    return interno.strip()


def _wf_push(usar_cache=True, resolver_dudosos=True):
    """
    Devuelve (workflows_push, confiable).

    Cómo se resuelve el conversation_id de cada workflow, dado que el listado
    NO trae las acciones:
      1) Cruce por nombre: los workflows se llaman "PUSH - <label> (auto)" y
         las opciones de `enviar_push` traen ese mismo <label>. Es exacto,
         determinista y no cuesta ni una llamada extra.
      2) Los que no cruzan por nombre (renombrados a mano, por ejemplo) se
         resuelven pidiendo el detalle del flow uno por uno, con tope para no
         agotar el tiempo del request.
    `confiable` es False si el listado quedó incompleto o si quedaron
    workflows sin resolver: en ese caso no se puede afirmar cobertura ni
    crear nada sin arriesgar duplicados.
    """
    flows, completo = _wf_listar_flows(usar_cache)
    try:
        pushes = _wf_opciones_push()
    except Exception:
        pushes = {}
    por_label = {label.strip(): cid for cid, label in pushes.items()}

    resultado, dudosos = [], []
    for f in flows:
        nombre = f.get("name") or ""
        # SOLO los que siguen EXACTAMENTE nuestro patrón "PUSH - X (auto)".
        # El portal tiene ~103 workflows preexistentes llamados "PUSH - X"
        # (sin el sufijo) que son de la operación normal y NO se disparan por
        # la propiedad `enviar_push`. Antes se colaban en el cruce: al quitar
        # el sufijo quedaban con el mismo label que los nuestros y salían
        # marcados como 102 "duplicados" falsos.
        if not (nombre.startswith(PREFIJO_WORKFLOW_PUSH) and nombre.endswith(SUFIJO_WORKFLOW_PUSH)):
            continue
        entrada = {
            "flow_id": f.get("id"),
            "nombre": nombre,
            "activo": f.get("isEnabled"),
            "conversation_ids": [],
            "origen": None,
        }
        cid = por_label.get(_label_desde_nombre(nombre))
        if cid:
            entrada["conversation_ids"] = [cid]
            entrada["origen"] = "nombre"
        else:
            dudosos.append(entrada)
        resultado.append(entrada)

    sin_resolver = 0
    if resolver_dudosos and dudosos:
        inicio = time.time()
        for entrada in dudosos:
            if len(dudosos) > MAX_DETALLES_FLOW or (time.time() - inicio) > LIMITE_SEGUNDOS_DETALLE:
                sin_resolver += 1
                continue
            try:
                detalle = _hubspot_api("GET", f"/automation/v4/flows/{entrada['flow_id']}")
                entrada["conversation_ids"] = sorted(_wf_conversation_ids(detalle))
                entrada["origen"] = "detalle" if entrada["conversation_ids"] else "no_resuelto"
                if not entrada["conversation_ids"]:
                    sin_resolver += 1
            except Exception as e:
                log.warning(f"[workflows] no se pudo leer el detalle de {entrada['flow_id']}: {e}")
                entrada["origen"] = "error"
                sin_resolver += 1
    elif dudosos:
        sin_resolver = len(dudosos)

    confiable = completo and sin_resolver == 0
    if not confiable:
        log.warning(f"[workflows] cruce NO confiable (listado_completo={completo}, sin_resolver={sin_resolver})")
    return resultado, confiable


def _wf_opciones_push():
    """{conversation_id: label} desde las opciones de la propiedad enviar_push."""
    prop = _hubspot_api("GET", f"/crm/v3/properties/contacts/{PROP_ENVIAR_PUSH}")
    pushes = {}
    for opcion in prop.get("options") or []:
        m = re.fullmatch(r"PUSH_(\d+)", (opcion.get("value") or "").strip())
        if m:
            pushes[m.group(1)] = opcion.get("label") or ""
    return pushes


def _wf_payload(label, conversation_id, activar=True, descripcion=None):
    push_value = f"PUSH_{conversation_id}"
    nombre = f"{PREFIJO_WORKFLOW_PUSH}{label} (auto)"
    payload = {
        "isEnabled": bool(activar),
        "flowType": "WORKFLOW",
        "type": "CONTACT_FLOW",
        "name": nombre,
        "objectTypeId": "0-1",
        "startActionId": "1",
        "nextAvailableActionId": "3",
        "actions": [
            {
                "type": "SINGLE_CONNECTION",
                "actionId": "1",
                "actionTypeVersion": TREBLE_ACTION_VERSION,
                "actionTypeId": TREBLE_ACTION_TYPE_ID,
                "connection": {"edgeType": "STANDARD", "nextActionId": "2"},
                "fields": {"conversation_id": str(conversation_id), "channel_id": CHANNEL_ID_WHATSAPP},
            },
            {
                "type": "SINGLE_CONNECTION",
                "actionId": "2",
                "actionTypeVersion": 0,
                "actionTypeId": "0-5",
                "fields": {
                    "property_name": PROP_ENVIAR_PUSH,
                    "value": {"staticValue": "", "type": "STATIC_VALUE"},
                },
            },
        ],
        "enrollmentCriteria": {
            "shouldReEnroll": True,
            "type": "LIST_BASED",
            "listFilterBranch": {
                "filterBranches": [{
                    "filterBranches": [],
                    "filters": [{
                        "property": PROP_ENVIAR_PUSH,
                        "operation": {
                            "operator": "IS_ANY_OF",
                            "includeObjectsWithNoValueSet": False,
                            "values": [push_value],
                            "operationType": "ENUMERATION",
                        },
                        "filterType": "PROPERTY",
                    }],
                    "filterBranchType": "AND",
                    "filterBranchOperator": "AND",
                }],
                "filters": [],
                "filterBranchType": "OR",
                "filterBranchOperator": "OR",
            },
            "unEnrollObjectsNotMeetingCriteria": False,
            "reEnrollmentTriggersFilterBranches": [],
        },
        "timeWindows": [],
        "blockedDates": [],
        "customProperties": {},
        "crmObjectCreationStatus": "COMPLETE",
        "suppressionListIds": [],
        "canEnrollFromSalesforce": False,
    }
    if descripcion:
        payload["description"] = descripcion
    return nombre, push_value, payload


@app.post("/workflows/push")
def crear_workflow_push(body: dict, x_api_key: str | None = Header(default=None)):
    """
    Body: {"label": "...", "conversation_id": "1391721", "activar": true, "forzar": false}

    Salvaguardas:
      - conversation_id debe ser numérico.
      - El push debe existir como opción de `enviar_push`; si no, el workflow
        nunca se dispararía (se devuelve 409).
      - No puede existir ya un workflow para ese conversation_id: un duplicado
        haría que el cliente reciba el mismo WhatsApp dos veces (409).
      - "forzar": true salta esas dos verificaciones.
    """
    _chequear_clave(x_api_key)
    label = (body.get("label") or "").strip()
    conversation_id = _validar_id_numerico(body.get("conversation_id"), "conversation_id")
    activar = _a_bool(body.get("activar"), True)
    forzar = _a_bool(body.get("forzar"), False)

    if not label:
        raise HTTPException(400, "Se requiere 'label'.")
    if len(label) > 180:
        raise HTTPException(400, "'label' demasiado largo (máx. 180 caracteres).")

    if not forzar:
        try:
            pushes = _wf_opciones_push()
        except Exception as e:
            raise HTTPException(502, f"No se pudieron leer las opciones de {PROP_ENVIAR_PUSH}: {e}")
        if conversation_id not in pushes:
            raise HTTPException(409, f"No existe la opción PUSH_{conversation_id} en {PROP_ENVIAR_PUSH}. "
                                     f"El workflow nunca se dispararía. Usá \"forzar\": true para crearlo igual.")
        try:
            existentes, confiable = _wf_push(usar_cache=False)
        except Exception as e:
            raise HTTPException(502, f"No se pudo verificar si ya existe el workflow: {e}")
        # Si el cruce no es confiable (listado cortado o workflows sin resolver)
        # NO se crea: podría existir ya uno y terminaríamos duplicando el envío.
        if not confiable:
            raise HTTPException(503, "No se pudo verificar de forma confiable si el workflow ya existe "
                                     "(listado incompleto o workflows sin resolver). No se crea nada para "
                                     "no arriesgar un duplicado. Reintentá en un minuto.")
        nombre_previsto = f"{PREFIJO_WORKFLOW_PUSH}{label}{SUFIJO_WORKFLOW_PUSH}"
        ya = [w for w in existentes
              if conversation_id in w["conversation_ids"] or w["nombre"] == nombre_previsto]
        # Además: cualquier workflow "PUSH - <label>" (aunque le falte el
        # sufijo "(auto)" por haber sido renombrado a mano, o sea preexistente
        # de la operación) cuenta como coincidencia. Para CREAR se es
        # deliberadamente conservador: ante la duda, no se crea, porque un
        # duplicado significa que el cliente recibe el WhatsApp dos veces.
        if not ya:
            try:
                todos, _ = _wf_listar_flows()
                parecidos = [{"flow_id": f.get("id"), "nombre": f.get("name")}
                             for f in todos
                             if (f.get("name") or "").startswith(PREFIJO_WORKFLOW_PUSH)
                             and _label_desde_nombre(f.get("name") or "").casefold() == label.casefold()]
            except Exception:
                parecidos = []
            if parecidos:
                raise HTTPException(409, f"Ya existe un workflow con ese mismo nombre de push: "
                                         f"{[p['nombre'] for p in parecidos]} (flow_id {[p['flow_id'] for p in parecidos]}). "
                                         f"Puede ser uno renombrado o preexistente. Revisalo antes de crear otro; "
                                         f"si igual querés crearlo, usá \"forzar\": true.")
        if ya:
            raise HTTPException(409, f"Ya existe workflow para conversation_id {conversation_id}: "
                                     f"{[w['nombre'] for w in ya]} (flow_id {[w['flow_id'] for w in ya]}). "
                                     f"Duplicarlo haría que el cliente reciba el push dos veces.")

    descripcion = (f"Se dispara solo cuando el campo 'Enviar push (WhatsApp)' = {label}. "
                   f"Envia el HSM (conversation_id {conversation_id}) y limpia el campo "
                   f"para reutilizarlo. Creado automaticamente via bridge.")
    nombre, push_value, payload = _wf_payload(label, conversation_id, activar, descripcion)

    try:
        resultado = _hubspot_api("POST", "/automation/v4/flows", payload)
    except urllib.error.HTTPError as e:
        detalle = e.read().decode() if hasattr(e, "read") else str(e)
        # "description" no está en el schema público de la v4 (beta): si lo
        # rechaza, se reintenta sin él en vez de fallar por algo cosmético.
        if "description" in detalle.lower():
            log.warning("[workflows] HubSpot rechazó 'description', reintento sin ese campo")
            payload.pop("description", None)
            try:
                resultado = _hubspot_api("POST", "/automation/v4/flows", payload)
            except urllib.error.HTTPError as e2:
                d2 = e2.read().decode() if hasattr(e2, "read") else str(e2)
                raise HTTPException(e2.code, f"HubSpot rechazó el workflow: {d2}")
        else:
            raise HTTPException(e.code, f"HubSpot rechazó el workflow: {detalle}")
    except Exception as e:
        raise HTTPException(502, f"Error creando el workflow en HubSpot: {e}")

    _CACHE_FLOWS["datos"] = None
    flow_id = resultado.get("id")
    METRICAS["workflows_creados"] += 1
    log.info(f"[workflows] creado flow_id={flow_id} nombre={nombre!r} activo={bool(activar)}")
    return {
        "creado": True, "flow_id": flow_id, "nombre": nombre, "push_value": push_value,
        "activo": bool(activar),
        "link": f"https://app.hubspot.com/workflows/{ACCOUNT_ID}/platform/flow/{flow_id}/edit",
    }


@app.get("/workflows/push")
def listar_workflows_push(x_api_key: str | None = Header(default=None)):
    """Lista los workflows 'PUSH - ... (auto)' con su conversation_id."""
    _chequear_clave(x_api_key)
    try:
        workflows, confiable = _wf_push()
    except Exception as e:
        raise HTTPException(502, f"Error listando workflows en HubSpot: {e}")
    return {"total": len(workflows), "cruce_confiable": confiable, "workflows": workflows}


@app.get("/workflows/push/auditoria")
def auditar_workflows_push(x_api_key: str | None = Header(default=None)):
    """
    Cruza las opciones de `enviar_push` contra los workflows existentes.
    Automatiza la auditoría que el 04/09/2026 se hizo a mano: devuelve los
    pushes sin workflow (el gap), los duplicados (mismo push con 2+ workflows
    → el cliente recibiría el mensaje repetido), los huérfanos y los apagados.
    """
    _chequear_clave(x_api_key)
    try:
        pushes = _wf_opciones_push()
        workflows, confiable = _wf_push()
    except Exception as e:
        raise HTTPException(502, f"Error auditando workflows en HubSpot: {e}")

    cubiertos = {}
    for w in workflows:
        for cid in w["conversation_ids"]:
            cubiertos.setdefault(cid, []).append(w["flow_id"])

    faltantes = [{"conversation_id": c, "label": l} for c, l in sorted(pushes.items()) if c not in cubiertos]
    duplicados = {c: ids for c, ids in cubiertos.items() if len(ids) > 1}
    huerfanos = [w for w in workflows
                 if w["conversation_ids"] and not any(c in pushes for c in w["conversation_ids"])]

    try:
        todos_flows, _ = _wf_listar_flows()
        otros = [{"flow_id": f.get("id"), "nombre": f.get("name"), "activo": f.get("isEnabled")}
                 for f in todos_flows
                 if (f.get("name") or "").startswith(PREFIJO_WORKFLOW_PUSH)
                 and not (f.get("name") or "").endswith(SUFIJO_WORKFLOW_PUSH)]
    except Exception:
        otros = []

    METRICAS["auditorias_workflows"] += 1
    resultado = {
        "cruce_confiable": confiable,
        "total_pushes": len(pushes),
        "total_workflows_push": len(workflows),
        # Sin un cruce confiable no se puede afirmar cobertura: los
        # "faltantes" podrían tener workflow y no haberse podido resolver.
        "cobertura_ok": confiable and not faltantes and not duplicados,
        "faltantes": faltantes,
        "duplicados": duplicados,
        "huerfanos": huerfanos,
        "sin_referencia_detectable": [w for w in workflows if not w["conversation_ids"]],
        "desactivados": [w for w in workflows if w["activo"] is False],
        # Informativo: workflows "PUSH - ..." preexistentes de la operación que
        # NO siguen nuestro patrón "(auto)". No entran en el cruce ni cuentan
        # como duplicados; se listan para que nadie los confunda con los nuestros.
        "otros_workflows_push_no_gestionados": otros,
    }
    if not confiable:
        resultado["advertencia"] = ("El cruce no es confiable (listado cortado por tiempo o workflows que no "
                                    "se pudieron resolver): los 'faltantes' pueden ser falsos. NO crear "
                                    "workflows a partir de esta auditoría; reintentá en un minuto.")
        resultado["no_resueltos"] = [w for w in workflows if w.get("origen") in (None, "no_resuelto", "error")]
    return resultado


@app.get("/workflows/{flow_id}/detalle")
def detalle_workflow(flow_id: str, x_api_key: str | None = Header(default=None)):
    """
    Detalle completo de un workflow (solo lectura): trigger, acciones y estado.
    Sirve para verificar qué dispara realmente un workflow sin depender de la
    UI de HubSpot, y para diagnosticar sin adivinar.
    """
    _chequear_clave(x_api_key)
    flow_id = _validar_id_numerico(flow_id, "flow_id")
    try:
        flow = _hubspot_api("GET", f"/automation/v4/flows/{flow_id}")
    except urllib.error.HTTPError as e:
        detalle = e.read().decode() if hasattr(e, "read") else str(e)
        raise HTTPException(e.code, f"No se pudo leer el workflow {flow_id}: {detalle}")
    except Exception as e:
        raise HTTPException(502, f"No se pudo leer el workflow {flow_id}: {e}")

    criterios = json.dumps(flow.get("enrollmentCriteria") or {}, ensure_ascii=False)
    return {
        "flow_id": flow.get("id"),
        "nombre": flow.get("name"),
        "activo": flow.get("isEnabled"),
        "tipo_inscripcion": (flow.get("enrollmentCriteria") or {}).get("type"),
        # Lo que de verdad importa: ¿este workflow escucha la propiedad de pushes?
        "escucha_enviar_push": PROP_ENVIAR_PUSH in criterios,
        "pushes_en_el_trigger": sorted(set(re.findall(r"PUSH_(\d+)", criterios))),
        "conversation_ids_en_acciones": sorted(_wf_conversation_ids(flow)),
        "acciones": [{"actionId": a.get("actionId"), "actionTypeId": a.get("actionTypeId"),
                      "fields": a.get("fields")} for a in (flow.get("actions") or [])],
        "creado": flow.get("createdAt"),
        "actualizado": flow.get("updatedAt"),
    }


@app.delete("/workflows/{flow_id}")
def eliminar_workflow(flow_id: str, confirmar: bool = False, permitir_cualquiera: bool = False,
                      x_api_key: str | None = Header(default=None)):
    """
    Elimina un workflow. En HubSpot queda en la vista 'Eliminado' del portal
    (recuperable), no es destrucción permanente.

    Salvaguardas:
      - ?confirmar=true obligatorio, para que un curl mal escrito no borre nada.
      - Solo nombres que empiecen por "PUSH - " o "TEST - "; para cualquier otro
        hace falta además ?permitir_cualquiera=true. Sin esto, la API key sola
        permitiría borrar cualquiera de los 1.100+ workflows del portal.
      - Deja en el log qué se borró y si estaba activo.
    """
    _chequear_clave(x_api_key)
    flow_id = _validar_id_numerico(flow_id, "flow_id")

    if not confirmar:
        raise HTTPException(400, "Falta ?confirmar=true. El borrado no se ejecuta sin confirmación explícita.")

    try:
        antes = _hubspot_api("GET", f"/automation/v4/flows/{flow_id}")
    except urllib.error.HTTPError as e:
        detalle = e.read().decode() if hasattr(e, "read") else str(e)
        raise HTTPException(e.code, f"No se pudo leer el workflow {flow_id}: {detalle}")
    except Exception as e:
        raise HTTPException(502, f"No se pudo leer el workflow {flow_id}: {e}")

    nombre = antes.get("name") or ""
    estaba_activo = antes.get("isEnabled")
    if not nombre.startswith(PREFIJOS_BORRABLES) and not permitir_cualquiera:
        raise HTTPException(403, f"'{nombre}' no es un workflow de push (no empieza por {PREFIJOS_BORRABLES}). "
                                 f"Este bridge no borra workflows de otras áreas por seguridad. "
                                 f"Si de verdad hay que borrarlo, agregá &permitir_cualquiera=true.")

    try:
        _hubspot_api("DELETE", f"/automation/v4/flows/{flow_id}")
    except urllib.error.HTTPError as e:
        detalle = e.read().decode() if hasattr(e, "read") else str(e)
        raise HTTPException(e.code, f"HubSpot rechazó el borrado: {detalle}")
    except Exception as e:
        raise HTTPException(502, f"Error borrando el workflow en HubSpot: {e}")

    _CACHE_FLOWS["datos"] = None
    METRICAS["workflows_eliminados"] += 1
    log.warning(f"[workflows] ELIMINADO flow_id={flow_id} nombre={nombre!r} estaba_activo={estaba_activo}")
    return {"eliminado": True, "flow_id": flow_id, "nombre": nombre, "estaba_activo": estaba_activo}


# ══════════════════════════════════════════════════════════════════
#  COHORTE DE RENOVACIONES + REINTENTO DE PUSHES BLOQUEADOS
#  Agregado 07/09/2026 (v1.3.5). BLOQUE PURAMENTE ADITIVO: no toca
#  ni una línea de lo anterior. Todo lo nuevo vive acá abajo.
#
#  ── Por qué existe ────────────────────────────────────────────────
#  1) COHORTE. Yesica pidió dejar de armar a mano la lista de clientes
#     en primera renovación. No hace falta: un cliente en su primera
#     renovación es simplemente el que tiene UN solo pago registrado
#     (fecha_compra == fecha_ultimo_pago) y una fecha_renovacion
#     próxima. El estado se guarda EN LA FICHA DE HUBSPOT, no en una
#     base acá: así Diana lo ve donde ya trabaja, el equipo arma listas
#     y reportes sin depender de este bridge, y no hay estado frágil
#     que se pierda en un redeploy de Render.
#
#  2) REINTENTO. Un push disparado desde HubSpot no se entrega si el
#     contacto tiene una conversación abierta en Treble (status
#     FAILURE_BY_HUMAN_HANDOVER). Medido sobre 8.046 casos: el 98,8%
#     de los bloqueados tenía conversación abierta contra el 0,6% de
#     los entregados. El 83,5% nunca recibe el mensaje. Como no hay
#     forma de forzar el envío desde Treble, esto reintenta solo,
#     una vez que la conversación se cerró.
#
#  ── Salvaguardas ──────────────────────────────────────────────────
#  · /cohorte/procesar y /pushes/reintentar son DRY-RUN por defecto.
#    Sin ?aplicar=true simulan y devuelven qué harían, sin escribir.
#  · El reintento solo toca contactos cuya conversación YA está
#    cerrada (si sigue abierta, volvería a fallar y gastaría el envío).
#  · Cada reintento se registra en la tabla de eventos: un mismo push
#    bloqueado nunca se reintenta dos veces.
#  · Tope por corrida configurable, para que un error no dispare miles
#    de WhatsApps.
# ══════════════════════════════════════════════════════════════════

from datetime import timedelta  # no estaba importado; se agrega acá para no tocar la cabecera

PROP_COHORTE_ESTADO = "cohorte_renovacion"
PROP_COHORTE_ENTRADA = "cohorte_fecha_entrada"
PROP_COHORTE_PAGO_BASE = "cohorte_pago_al_entrar"
GRUPO_COHORTE = "contactinformation"

# Estados del cohorte. Son el ciclo de vida completo que pidió Yesica:
# entra preventivo → paga, o falla → se recupera, o termina en churn.
COHORTE_ESTADOS = [
    ("preventivo", "Preventivo · renueva pronto"),
    ("pago", "Pagó la renovación"),
    ("fallo", "Falló la primera renovación"),
    ("recuperado", "Recuperado tras fallar"),
    ("churn", "Churn"),
]

# Ventana por defecto: los que renuevan en los próximos 7 días.
COHORTE_DIAS_VENTANA = int(os.environ.get("COHORTE_DIAS_VENTANA", "7"))
# Días después de la fecha de renovación sin pago para declarar churn.
COHORTE_DIAS_CHURN = int(os.environ.get("COHORTE_DIAS_CHURN", "30"))
COHORTE_MAX_CONTACTOS = int(os.environ.get("COHORTE_MAX_CONTACTOS", "2000"))

REINTENTO_MAX_POR_CORRIDA = int(os.environ.get("REINTENTO_MAX_POR_CORRIDA", "150"))
REINTENTO_HORAS_ATRAS = int(os.environ.get("REINTENTO_HORAS_ATRAS", "72"))

for _m in ("cohorte_marcados", "cohorte_actualizados", "pushes_reintentados"):
    METRICAS.setdefault(_m, 0)


def _dia_ms(fecha):
    """HubSpot guarda las propiedades de tipo date como epoch ms a medianoche UTC."""
    return int(datetime(fecha.year, fecha.month, fecha.day, tzinfo=timezone.utc).timestamp() * 1000)


def _a_fecha(valor):
    """Acepta '2026-09-07', epoch ms como string o int. Devuelve date o None."""
    if valor in (None, ""):
        return None
    if isinstance(valor, (int, float)):
        return datetime.fromtimestamp(float(valor) / 1000, tz=timezone.utc).date()
    texto = str(valor).strip()
    if texto.isdigit() and len(texto) >= 12:
        return datetime.fromtimestamp(int(texto) / 1000, tz=timezone.utc).date()
    try:
        return datetime.strptime(texto[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _hs_buscar_todo(objeto, body, tope=COHORTE_MAX_CONTACTOS):
    """
    Search paginado. HubSpot devuelve como máximo 200 por página y corta en
    10.000 resultados; el tope acá es una segunda red por si un filtro mal
    puesto intentara traerse el portal entero.
    """
    salida, after, vueltas = [], None, 0
    while len(salida) < tope and vueltas < 60:
        vueltas += 1
        cuerpo = dict(body, limit=min(200, tope - len(salida)))
        if after:
            cuerpo["after"] = after
        data = _hubspot_api("POST", f"/crm/v3/objects/{objeto}/search", cuerpo)
        salida.extend(data.get("results", []))
        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break
    return salida


def _hs_batch_update(objeto, entradas):
    """
    Actualiza en lotes de 100 (el máximo de la API). Devuelve cuántos se
    escribieron y los errores por lote, sin abortar todo si uno falla.
    """
    escritos, errores = 0, []
    for i in range(0, len(entradas), 100):
        lote = entradas[i:i + 100]
        try:
            _hubspot_api("POST", f"/crm/v3/objects/{objeto}/batch/update", {"inputs": lote})
            escritos += len(lote)
        except Exception as e:
            errores.append({"desde": i, "cantidad": len(lote), "error": str(e)[:200]})
            log.error(f"[cohorte] fallo lote {i}-{i+len(lote)}: {e}")
    return escritos, errores


# ── Propiedades en HubSpot ────────────────────────────────────────

def _cohorte_props_existentes():
    faltan, existen = [], {}
    for nombre in (PROP_COHORTE_ESTADO, PROP_COHORTE_ENTRADA, PROP_COHORTE_PAGO_BASE):
        try:
            existen[nombre] = _hubspot_api("GET", f"/crm/v3/properties/contacts/{nombre}")
        except Exception:
            faltan.append(nombre)
    return existen, faltan


@app.post("/cohorte/setup")
def cohorte_setup(x_api_key: str | None = Header(default=None)):
    """
    Crea las tres propiedades del cohorte si no existen. Idempotente:
    llamarlo dos veces no rompe nada ni duplica.
    """
    _chequear_clave(x_api_key)
    existen, faltan = _cohorte_props_existentes()
    if not faltan:
        return {"creadas": [], "ya_existian": list(existen), "mensaje": "Nada que hacer, ya estaban las tres."}

    definiciones = {
        PROP_COHORTE_ESTADO: {
            "name": PROP_COHORTE_ESTADO, "label": "Cohorte renovación", "type": "enumeration",
            "fieldType": "select", "groupName": GRUPO_COHORTE,
            "description": "Estado del cliente dentro del cohorte de primera renovación. Lo mantiene el bridge automáticamente.",
            "options": [{"label": et, "value": v, "displayOrder": i, "hidden": False}
                        for i, (v, et) in enumerate(COHORTE_ESTADOS)],
        },
        PROP_COHORTE_ENTRADA: {
            "name": PROP_COHORTE_ENTRADA, "label": "Cohorte · fecha de entrada", "type": "date",
            "fieldType": "date", "groupName": GRUPO_COHORTE,
            "description": "Cuándo entró este cliente al cohorte de renovación.",
        },
        PROP_COHORTE_PAGO_BASE: {
            "name": PROP_COHORTE_PAGO_BASE, "label": "Cohorte · último pago al entrar", "type": "date",
            "fieldType": "date", "groupName": GRUPO_COHORTE,
            "description": "Fecha del último pago en el momento de entrar al cohorte. Sirve para detectar si después pagó.",
        },
    }
    creadas, errores = [], []
    for nombre in faltan:
        try:
            _hubspot_api("POST", "/crm/v3/properties/contacts", definiciones[nombre])
            creadas.append(nombre)
        except Exception as e:
            errores.append({"propiedad": nombre, "error": str(e)[:300]})
    log.warning(f"[cohorte] setup creadas={creadas} errores={errores}")
    return {"creadas": creadas, "ya_existian": list(existen), "errores": errores}


# ── Detección del cohorte ─────────────────────────────────────────

PROPS_COHORTE_LEER = [
    "hs_object_id", "hs_full_name_or_email", "firstname", "lastname", "email",
    "yopsi_id", "fecha_compra", "fecha_ultimo_pago", "fecha_renovacion",
    "hs_whatsapp_phone_number", "phone", "lifecyclestage",
    PROP_COHORTE_ESTADO, PROP_COHORTE_ENTRADA, PROP_COHORTE_PAGO_BASE,
]


def _cohorte_detectar(dias=COHORTE_DIAS_VENTANA):
    """
    Los que renuevan dentro de la ventana. Primera renovación =
    fecha_compra == fecha_ultimo_pago (un solo cobro registrado).
    """
    hoy = datetime.now(timezone.utc).date()
    hasta = hoy + timedelta(days=dias)
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "lifecyclestage", "operator": "EQ", "value": "customer"},
            {"propertyName": "fecha_renovacion", "operator": "BETWEEN",
             "value": str(_dia_ms(hoy)), "highValue": str(_dia_ms(hasta))},
        ]}],
        "properties": PROPS_COHORTE_LEER,
        "sorts": [{"propertyName": "fecha_renovacion", "direction": "ASCENDING"}],
    }
    salida = []
    for r in _hs_buscar_todo("contacts", body):
        p = r.get("properties", {})
        compra, ultimo, renov = _a_fecha(p.get("fecha_compra")), _a_fecha(p.get("fecha_ultimo_pago")), _a_fecha(p.get("fecha_renovacion"))
        salida.append({
            "id": r["id"],
            "nombre": p.get("hs_full_name_or_email") or f"{p.get('firstname','')} {p.get('lastname','')}".strip(),
            "yopsi_id": p.get("yopsi_id"), "email": p.get("email"),
            "whatsapp": p.get("hs_whatsapp_phone_number") or "",
            "telefono": p.get("phone") or "",
            "fecha_compra": compra.isoformat() if compra else None,
            "fecha_ultimo_pago": ultimo.isoformat() if ultimo else None,
            "fecha_renovacion": renov.isoformat() if renov else None,
            "dias_para_renovar": (renov - hoy).days if renov else None,
            "primera_renovacion": bool(compra and ultimo and compra == ultimo),
            "estado_actual": p.get(PROP_COHORTE_ESTADO) or "",
            "tiene_whatsapp": bool(p.get("hs_whatsapp_phone_number")),
        })
    return salida


def _cohorte_marcados():
    """Todos los que ya tienen una marca de cohorte, para actualizar su estado."""
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": PROP_COHORTE_ESTADO, "operator": "IN",
             "values": [v for v, _ in COHORTE_ESTADOS if v not in ("churn",)]},
        ]}],
        "properties": PROPS_COHORTE_LEER,
    }
    return _hs_buscar_todo("contacts", body)


def _cohorte_nuevo_estado(p, hoy):
    """
    Reglas de transición. Se apoyan solo en lo que HubSpot ya tiene, así que
    son auditables por cualquiera desde la ficha del cliente.
    """
    estado = p.get(PROP_COHORTE_ESTADO) or ""
    base = _a_fecha(p.get(PROP_COHORTE_PAGO_BASE))
    ultimo = _a_fecha(p.get("fecha_ultimo_pago"))
    renov = _a_fecha(p.get("fecha_renovacion"))
    ciclo = p.get("lifecyclestage") or ""

    pago_nuevo = bool(ultimo and base and ultimo > base)

    if pago_nuevo:
        return "recuperado" if estado == "fallo" else "pago"
    if estado in ("pago", "recuperado"):
        return estado
    if renov and renov < hoy:
        dias = (hoy - renov).days
        if dias >= COHORTE_DIAS_CHURN or (ciclo and ciclo != "customer"):
            return "churn"
        return "fallo"
    return estado or "preventivo"


@app.post("/cohorte/procesar")
def cohorte_procesar(
    x_api_key: str | None = Header(default=None),
    aplicar: str | None = None,
    dias: int | None = None,
):
    """
    La corrida semanal. Hace dos cosas:
      1. Marca como "preventivo" a los que entran al cohorte y todavía no
         tienen marca, guardando su fecha de último pago para poder detectar
         después si pagaron.
      2. Recorre los ya marcados y actualiza su estado: pagó, falló,
         recuperado o churn.

    DRY-RUN por defecto. Sin ?aplicar=true no escribe nada en HubSpot.
    """
    _chequear_clave(x_api_key)
    _, faltan = _cohorte_props_existentes()
    if faltan:
        raise HTTPException(
            status_code=409,
            detail=f"Faltan propiedades en HubSpot: {faltan}. Llamá primero a POST /cohorte/setup.",
        )

    escribir = _a_bool(aplicar, por_defecto=False)
    ventana = int(dias) if dias else COHORTE_DIAS_VENTANA
    hoy = datetime.now(timezone.utc).date()

    # 1) Altas
    detectados = _cohorte_detectar(ventana)
    nuevos = [c for c in detectados if c["primera_renovacion"] and not c["estado_actual"]]
    altas = [{"id": c["id"], "properties": {
        PROP_COHORTE_ESTADO: "preventivo",
        PROP_COHORTE_ENTRADA: _dia_ms(hoy),
        PROP_COHORTE_PAGO_BASE: _dia_ms(_a_fecha(c["fecha_ultimo_pago"])) if c["fecha_ultimo_pago"] else "",
    }} for c in nuevos]

    # 2) Actualizaciones de estado
    cambios, detalle_cambios = [], []
    for r in _cohorte_marcados():
        p = r.get("properties", {})
        actual = p.get(PROP_COHORTE_ESTADO) or ""
        nuevo = _cohorte_nuevo_estado(p, hoy)
        if nuevo and nuevo != actual:
            cambios.append({"id": r["id"], "properties": {PROP_COHORTE_ESTADO: nuevo}})
            detalle_cambios.append({
                "id": r["id"],
                "nombre": p.get("hs_full_name_or_email"),
                "de": actual, "a": nuevo,
            })

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "ventana_dias": ventana,
        "detectados_en_ventana": len(detectados),
        "primera_renovacion": sum(1 for c in detectados if c["primera_renovacion"]),
        "altas_nuevas": len(altas),
        "cambios_de_estado": len(cambios),
        "detalle_cambios": detalle_cambios[:50],
        "sin_whatsapp": [c["nombre"] for c in detectados if not c["tiene_whatsapp"]][:20],
    }

    if not escribir:
        resultado["aviso"] = "Simulación. Para que escriba en HubSpot: POST /cohorte/procesar?aplicar=true"
        return resultado

    esc_altas, err_altas = _hs_batch_update("contacts", altas) if altas else (0, [])
    esc_cambios, err_cambios = _hs_batch_update("contacts", cambios) if cambios else (0, [])
    METRICAS["cohorte_marcados"] += esc_altas
    METRICAS["cohorte_actualizados"] += esc_cambios
    resultado.update({
        "altas_escritas": esc_altas, "cambios_escritos": esc_cambios,
        "errores": err_altas + err_cambios,
    })
    log.warning(f"[cohorte] procesado altas={esc_altas} cambios={esc_cambios} errores={len(err_altas + err_cambios)}")
    return resultado


@app.get("/cohorte/renovaciones")
def cohorte_renovaciones(x_api_key: str | None = Header(default=None), dias: int | None = None):
    """La lista de la semana, en vivo. Es lo que reemplaza al Excel manual."""
    _chequear_clave(x_api_key)
    ventana = int(dias) if dias else COHORTE_DIAS_VENTANA
    lista = _cohorte_detectar(ventana)
    primera = [c for c in lista if c["primera_renovacion"]]
    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "ventana_dias": ventana,
        "total": len(lista),
        "primera_renovacion": len(primera),
        "recurrentes": len(lista) - len(primera),
        "sin_whatsapp": sum(1 for c in lista if not c["tiene_whatsapp"]),
        "urgentes_2_dias": sum(1 for c in primera if (c["dias_para_renovar"] or 99) <= 2),
        "clientes": sorted(lista, key=lambda c: (not c["primera_renovacion"], c["dias_para_renovar"] or 99, c["nombre"])),
    }


@app.get("/cohorte/kpis")
def cohorte_kpis(x_api_key: str | None = Header(default=None)):
    """
    Los cuatro KPIs que definió Yesica, sobre el mismo cohorte y sin
    trabajo manual: cuántos entran, cuántos se contactaron, cuántos se
    recuperaron, y cómo se reparte el churn.
    """
    _chequear_clave(x_api_key)
    conteo = {}
    for valor, etiqueta in COHORTE_ESTADOS:
        body = {"filterGroups": [{"filters": [
            {"propertyName": PROP_COHORTE_ESTADO, "operator": "EQ", "value": valor}]}],
            "properties": ["hs_object_id"]}
        try:
            data = _hubspot_api("POST", "/crm/v3/objects/contacts/search", dict(body, limit=1))
            conteo[valor] = {"etiqueta": etiqueta, "total": data.get("total", 0)}
        except Exception as e:
            conteo[valor] = {"etiqueta": etiqueta, "total": None, "error": str(e)[:120]}

    fallaron = (conteo.get("fallo", {}).get("total") or 0) + (conteo.get("recuperado", {}).get("total") or 0) \
        + (conteo.get("churn", {}).get("total") or 0)
    recuperados = conteo.get("recuperado", {}).get("total") or 0
    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "por_estado": conteo,
        "entraron_al_cohorte": sum((v.get("total") or 0) for v in conteo.values()),
        "fallaron_alguna_vez": fallaron,
        "recuperados": recuperados,
        "tasa_recuperacion": round(recuperados / fallaron, 4) if fallaron else None,
        "en_seguimiento_ahora": conteo.get("preventivo", {}).get("total"),
    }


# ── Reintento de pushes bloqueados ────────────────────────────────

def _push_opciones_por_id():
    """conversation_id -> label, leído de la propiedad enviar_push."""
    prop = _hubspot_api("GET", f"/crm/v3/properties/contacts/{PROP_ENVIAR_PUSH}")
    salida = {}
    for o in prop.get("options", []):
        valor = str(o.get("value", ""))
        if valor.startswith("PUSH_") and valor[5:].isdigit():
            salida[valor[5:]] = o.get("label", "")
    return salida


def _bloqueados_pendientes(horas=REINTENTO_HORAS_ATRAS, tope=500):
    """
    Pushes bloqueados por conversación abierta que:
      · ya tienen la conversación CERRADA (si sigue abierta volvería a fallar),
      · y no recibieron ningún envío exitoso posterior.
    """
    sql = f"""
    WITH f AS (
      SELECT deployment_id did, treble_id tid, cellphone cel, country_code cc,
             poll_id pid, timestamps_eta ts
      FROM fact_deployment_status
      WHERE origin = 'HELPDESK_INTEGRATION' AND status = 'FAILURE_BY_HUMAN_HANDOVER'
        AND timestamps_eta >= now() - INTERVAL {int(horas)} HOUR
    ),
    ok AS (
      SELECT treble_id tid, timestamps_eta ts FROM fact_deployment_status
      WHERE origin = 'HELPDESK_INTEGRATION' AND status IN ('DELIVERED','SUCCESS')
        AND timestamps_eta >= now() - INTERVAL {int(horas) + 24} HOUR
    ),
    abierta AS (
      SELECT contact_wa_id wa FROM fact_conversations WHERE status = 'assigned'
    ),
    conv AS (
      SELECT contact_wa_id wa, helpdesk_contact_id hs FROM fact_conversations
      WHERE helpdesk_contact_id != ''
    )
    SELECT f.did did, f.tid tid, f.cel cel, f.cc cc, f.pid pid, f.ts ts,
           any(conv.hs) hubspot_id
    FROM f
    LEFT JOIN ok ON f.tid = ok.tid
    LEFT JOIN abierta ON f.tid = abierta.wa
    LEFT JOIN conv ON f.tid = conv.wa
    GROUP BY did, tid, cel, cc, pid, ts
    HAVING max(if(ok.ts > f.ts, 1, 0)) = 0 AND max(if(abierta.wa != '', 1, 0)) = 0
    ORDER BY ts DESC
    LIMIT {int(tope)}
    """
    return _query_interna(sql)


@app.get("/pushes/bloqueados")
def pushes_bloqueados(x_api_key: str | None = Header(default=None), horas: int | None = None):
    """Diagnóstico: qué hay pendiente de reintentar y qué fracción es recuperable."""
    _chequear_clave(x_api_key)
    ventana = int(horas) if horas else REINTENTO_HORAS_ATRAS
    filas = _bloqueados_pendientes(ventana)
    opciones = _push_opciones_por_id()
    recuperables = [f for f in filas if str(f["pid"]) in opciones]
    sin_opcion = {}
    for f in filas:
        if str(f["pid"]) not in opciones:
            sin_opcion[str(f["pid"])] = sin_opcion.get(str(f["pid"]), 0) + 1
    return {
        "ventana_horas": ventana,
        "pendientes": len(filas),
        "reintentables_ahora": len(recuperables),
        "sin_workflow_asociado": sorted(
            [{"conversation_id": k, "casos": v} for k, v in sin_opcion.items()],
            key=lambda x: -x["casos"])[:20],
        "nota": ("Los 'sin workflow asociado' son campañas que no están en la propiedad "
                 "enviar_push. Para reintentarlas hay que darles de alta su workflow "
                 "con POST /workflows/push."),
    }


@app.post("/pushes/reintentar")
def pushes_reintentar(
    x_api_key: str | None = Header(default=None),
    aplicar: str | None = None,
    horas: int | None = None,
    tope: int | None = None,
):
    """
    Reintenta escribiendo la propiedad enviar_push del contacto, que es lo
    que dispara el workflow correspondiente. No toca Treble ni requiere
    credenciales suyas: usa la misma maquinaria que ya opera el equipo.

    DRY-RUN por defecto. Sin ?aplicar=true no escribe nada.
    """
    _chequear_clave(x_api_key)
    escribir = _a_bool(aplicar, por_defecto=False)
    ventana = int(horas) if horas else REINTENTO_HORAS_ATRAS
    limite = min(int(tope), REINTENTO_MAX_POR_CORRIDA) if tope else REINTENTO_MAX_POR_CORRIDA

    filas = _bloqueados_pendientes(ventana)
    opciones = _push_opciones_por_id()

    plan, omitidos = [], {"sin_workflow": 0, "sin_contacto": 0, "ya_reintentado": 0}
    for f in filas:
        pid = str(f["pid"])
        if pid not in opciones:
            omitidos["sin_workflow"] += 1
            continue
        hs_id = str(f.get("hubspot_id") or "").strip()
        if not hs_id or not hs_id.isdigit():
            omitidos["sin_contacto"] += 1
            continue
        if _evento_ya_notificado("reintento_push", f["did"]):
            omitidos["ya_reintentado"] += 1
            continue
        plan.append({
            "deployment_id": f["did"], "hubspot_id": hs_id,
            "conversation_id": pid, "push": opciones[pid],
            "telefono": _mask_phone(f"{f['cc']}{f['cel']}"),
        })
        if len(plan) >= limite:
            break

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "ventana_horas": ventana, "tope": limite,
        "pendientes_totales": len(filas),
        "a_reintentar": len(plan),
        "omitidos": omitidos,
        "muestra": plan[:15],
    }
    if not escribir:
        resultado["aviso"] = "Simulación. Para reintentar de verdad: POST /pushes/reintentar?aplicar=true"
        return resultado

    enviados, errores = 0, []
    for item in plan:
        try:
            _hubspot_api("PATCH", f"/crm/v3/objects/contacts/{item['hubspot_id']}",
                         {"properties": {PROP_ENVIAR_PUSH: f"PUSH_{item['conversation_id']}"}})
            # OJO: _evento_ya_notificado() solo devuelve True si status == "notified".
            # Con cualquier otro valor el candado no cierra y el push se reenvía
            # en cada corrida. Lo detectó el test de duplicados.
            _evento_marcar("reintento_push", item["deployment_id"], "notified", notified=True)
            enviados += 1
        except Exception as e:
            errores.append({"deployment_id": item["deployment_id"], "error": str(e)[:200]})
    METRICAS["pushes_reintentados"] += enviados
    resultado.update({"reintentados": enviados, "errores": errores})
    log.warning(f"[reintento] reintentados={enviados} errores={len(errores)}")
    return resultado
@app.get("/version")
def version_bloques(x_api_key: str | None = Header(default=None)):
    """
    Sirve para confirmar de un vistazo qué está realmente desplegado, sin
    tener que cambiar la versión del endpoint raíz (que no se toca).
    """
    _chequear_clave(x_api_key)
    return {
        "base": "1.3.3",
        "bloques": ["workflows_push (1.3.4)", "cohorte_renovaciones (1.3.5)", "reintento_pushes (1.3.5)", "contador_sesiones (1.3.6)", "workflows_crudo (1.3.7)", "riesgo_cancelacion (1.3.8)", "salud_mensajeria (1.3.9)", "correccion_veteranos (1.4.0)", "salud_detalle (1.4.1)", "arreglos_cruce_y_auditoria (1.4.2)", "reintento_automatico (1.4.2)", "cobertura_bifurcacion (1.4.2)", "sesiones_agendadas (1.4.3)", "monitor_riesgo (1.4.4)", "riesgo_lista_v2 (1.4.4)", "segmento_dormant (1.4.5)", "parte_operativo (1.4.6)", "reintento_por_nombre (1.4.7)", "caducidad_reintento (1.4.8)"],
        "endpoints_nuevos": [
            "POST /cohorte/setup", "POST /cohorte/procesar",
            "GET /cohorte/renovaciones", "GET /cohorte/kpis",
            "GET /pushes/bloqueados", "POST /pushes/reintentar",
            "POST /sesiones/setup", "POST /sesiones/sincronizar", "GET /sesiones/embudo",
            "GET /workflows/{id}/crudo", "GET /workflows/buscar", "POST /workflows/crear-crudo",
            "POST /riesgo/setup", "POST /riesgo/calcular", "GET /riesgo/lista",
            "GET /salud/mensajeria", "POST /salud/enviar",
            "GET /sesiones/estado", "POST /sesiones/corregir-veteranos",
            "GET /salud/detalle", "GET /salud/detalle-v2",
            "GET /sesiones/polls", "GET /auditoria/contactos",
            "GET /pushes/reintento-estado",
            "POST /sesiones/completar-nuevos", "GET /sesiones/cobertura",
            "GET /sesiones/senal", "POST /sesiones/recalcular-agendadas", "GET /riesgo/lista-v2", "GET /riesgo/monitor-estado", "GET /riesgo/dormant", "GET /riesgo/embudo-retencion", "GET /operativo/parte", "POST /operativo/enviar", "GET /pushes/bloqueados-v2", "POST /pushes/reintentar-v2", "GET /pushes/caducidad",
        ],
    }


# ══════════════════════════════════════════════════════════════════
#  CONTADOR DE SESIONES REALIZADAS
#  Agregado 07/09/2026 (v1.3.6). BLOQUE PURAMENTE ADITIVO.
#
#  ── El problema ───────────────────────────────────────────────────
#  Angela quiere que los clientes cumplan sus 2 o 4 sesiones del mes
#  para frenar bajas y reembolsos, y pidió bifurcar el push de 72 h
#  según si el cliente va en sus primeras 4 sesiones o en la 5ta en
#  adelante. Las dos cosas chocaban con lo mismo: en HubSpot NO existe
#  ningún campo con las sesiones realizadas. `sesiones_plan` dice
#  cuántas contrató (2 o 4), no cuántas hizo; `numero_de_sesiones_hsm`
#  está vacío; `hsm_sesion_1..4` guarda las fechas AGENDADAS, no la
#  asistencia; y MEETING_EVENT en HubSpot son reuniones internas.
#
#  ── De dónde sale el dato entonces ────────────────────────────────
#  La plataforma ya dispara un push distinto después de cada sesión
#  según si el cliente asistió o no, y eso queda registrado en el DWH.
#  Contar esos envíos por cliente ES el contador que falta:
#
#    1282086  Pos primera sesión Sí Asistió
#    1255377  Pos Segunda sesión Sí Asistió
#    1255383  Pos Tercera sesión Sí Asistió
#    1255390  Pos Cuarta sesión Sí Asistió
#    1255396  Pos Primera sesión No Asistió
#    1282059  Pos 2,3,4 sesión No Asistió sin AR
#    1282062  Pos 2,3,4 sesión No Asistió con AR
#
#  Verificado antes de construir: 2.362 clientes con sesiones
#  registradas, 96,2% mapeables a su contacto de HubSpot, historial
#  desde el 16/06/2026. El ciclo se repite (104 clientes recibieron
#  el push de primera sesión dos veces), así que se cuentan ENVÍOS
#  acumulados, no un máximo.
#
#  ── Límite honesto ────────────────────────────────────────────────
#  Esto mide "sesiones con push de cierre DISPARADO". El push se dispara
#  porque la sesión ocurrió; si Meta lo entrega o no es un hecho
#  posterior e independiente, así que NO se filtra por estado de entrega.
#  Esa distinción no es cosmética: filtrando por entrega, el embudo daba
#  "40% llega a la cuarta sesión" cuando la cifra real es ~84%, porque el
#  push de la cuarta es rechazado por Meta más de la mitad de las veces.
#  Un mismo push reenviado el mismo día se cuenta una sola vez.
#  El campo `sesiones_origen` deja explícito que el dato es derivado: el
#  día que la plataforma escriba el número real, se cambia la fuente y
#  nada más se toca.
# ══════════════════════════════════════════════════════════════════

PROP_SES_ASISTIDAS = "sesiones_asistidas"
PROP_SES_INASISTENCIAS = "sesiones_inasistencias"
PROP_SES_ULTIMA = "sesiones_ultima_fecha"
PROP_SES_ETAPA = "sesiones_etapa"
PROP_SES_ORIGEN = "sesiones_origen"

# La bifurcación que pidió Angela: hasta completar 4 sesiones el cliente
# está en acompañamiento (gestoras de consultoría); de la 5ta en adelante
# pasa a soporte (ATC).
SESIONES_CORTE_ETAPA = int(os.environ.get("SESIONES_CORTE_ETAPA", "4"))
SESIONES_ETAPAS = [
    ("acompanamiento", "Sesiones 1 a 4 · Gestoras de consultoría"),
    ("soporte", "Sesión 5 en adelante · ATC"),
]

PUSHES_ASISTIO = {"1282086": 1, "1255377": 2, "1255383": 3, "1255390": 4}
PUSHES_NO_ASISTIO = {"1255396": 1, "1282059": 0, "1282062": 0}
SESIONES_MAX_CONTACTOS = int(os.environ.get("SESIONES_MAX_CONTACTOS", "6000"))

for _m in ("sesiones_sincronizadas",):
    METRICAS.setdefault(_m, 0)


def _sesiones_props_existentes():
    faltan, existen = [], {}
    for nombre in (PROP_SES_ASISTIDAS, PROP_SES_INASISTENCIAS, PROP_SES_ULTIMA,
                   PROP_SES_ETAPA, PROP_SES_ORIGEN):
        try:
            existen[nombre] = _hubspot_api("GET", f"/crm/v3/properties/contacts/{nombre}")
        except Exception:
            faltan.append(nombre)
    return existen, faltan


@app.post("/sesiones/setup")
def sesiones_setup(x_api_key: str | None = Header(default=None)):
    """Crea las propiedades del contador si no existen. Idempotente."""
    _chequear_clave(x_api_key)
    existen, faltan = _sesiones_props_existentes()
    if not faltan:
        return {"creadas": [], "ya_existian": list(existen), "mensaje": "Ya estaban todas."}

    defs = {
        PROP_SES_ASISTIDAS: {
            "name": PROP_SES_ASISTIDAS, "label": "Sesiones asistidas", "type": "number",
            "fieldType": "number", "groupName": GRUPO_COHORTE,
            "description": "Cuántas sesiones asistió el cliente. Derivado de los pushes de cierre de sesión. Lo mantiene el bridge.",
        },
        PROP_SES_INASISTENCIAS: {
            "name": PROP_SES_INASISTENCIAS, "label": "Sesiones no asistidas", "type": "number",
            "fieldType": "number", "groupName": GRUPO_COHORTE,
            "description": "Cuántas veces no asistió a una sesión agendada. Derivado de los pushes de inasistencia.",
        },
        PROP_SES_ULTIMA: {
            "name": PROP_SES_ULTIMA, "label": "Última sesión registrada", "type": "date",
            "fieldType": "date", "groupName": GRUPO_COHORTE,
            "description": "Fecha del último cierre de sesión registrado para este cliente.",
        },
        PROP_SES_ETAPA: {
            "name": PROP_SES_ETAPA, "label": "Etapa de acompañamiento", "type": "enumeration",
            "fieldType": "select", "groupName": GRUPO_COHORTE,
            "description": "Define a qué equipo va la respuesta del cliente: gestoras de consultoría en sus primeras 4 sesiones, ATC de la 5ta en adelante.",
            "options": [{"label": et, "value": v, "displayOrder": i, "hidden": False}
                        for i, (v, et) in enumerate(SESIONES_ETAPAS)],
        },
        PROP_SES_ORIGEN: {
            "name": PROP_SES_ORIGEN, "label": "Origen del conteo de sesiones", "type": "string",
            "fieldType": "text", "groupName": GRUPO_COHORTE,
            "description": "De dónde salió el número. Hoy: derivado de pushes de cierre. Cambia el día que la plataforma escriba el dato real.",
        },
    }
    creadas, errores = [], []
    for nombre in faltan:
        try:
            _hubspot_api("POST", "/crm/v3/properties/contacts", defs[nombre])
            creadas.append(nombre)
        except Exception as e:
            errores.append({"propiedad": nombre, "error": str(e)[:300]})
    log.warning(f"[sesiones] setup creadas={creadas} errores={errores}")
    return {"creadas": creadas, "ya_existian": list(existen), "errores": errores}


def _sesiones_desde_dwh(dias=None):
    """
    Un renglón por cliente con su conteo de sesiones, ya mapeado al
    contacto de HubSpot. Cuenta pushes DISPARADOS, no entregados: el push
    sale porque la sesión ocurrió. Un mismo push repetido el mismo día es
    un reintento, no otra sesión, y se cuenta una vez.
    """
    asistio = ",".join(f"'{k}'" for k in PUSHES_ASISTIO)
    no_asistio = ",".join(f"'{k}'" for k in PUSHES_NO_ASISTIO)
    corte = f"AND timestamps_eta >= now() - INTERVAL {int(dias)} DAY" if dias else ""
    sql = f"""
    WITH d AS (
      SELECT treble_id tid, toString(poll_id) pid, timestamps_eta ts
      FROM fact_deployment_status
      WHERE toString(poll_id) IN ({asistio},{no_asistio}) {corte}
    ),
    c AS (
      SELECT contact_wa_id wa, any(helpdesk_contact_id) hs
      FROM fact_conversations WHERE helpdesk_contact_id != '' GROUP BY wa
    )
    SELECT d.tid tid, any(c.hs) hubspot_id,
           uniqExactIf(concat(d.pid, '|', toString(toDate(d.ts))), d.pid IN ({asistio})) asistidas,
           uniqExactIf(concat(d.pid, '|', toString(toDate(d.ts))), d.pid IN ({no_asistio})) inasistencias,
           toDate(max(d.ts)) ultima
    FROM d LEFT JOIN c ON d.tid = c.wa
    GROUP BY tid
    HAVING hubspot_id != ''
    ORDER BY ultima DESC
    LIMIT {SESIONES_MAX_CONTACTOS}
    """
    return _query_interna(sql)


@app.get("/sesiones/embudo")
def sesiones_embudo(x_api_key: str | None = Header(default=None), dias: int | None = None):
    """
    El embudo de asistencia: cuántos clientes llegan a cada sesión.
    Es el KPI que hoy no existe y que Angela necesita para saber si la
    meta de "2 a 4 sesiones al mes" se está cumpliendo.
    """
    _chequear_clave(x_api_key)
    corte = f"AND timestamps_eta >= now() - INTERVAL {int(dias)} DAY" if dias else ""
    sql = f"""
    SELECT
      uniqExactIf(treble_id, toString(poll_id)='1282086') sesion_1,
      uniqExactIf(treble_id, toString(poll_id)='1255377') sesion_2,
      uniqExactIf(treble_id, toString(poll_id)='1255383') sesion_3,
      uniqExactIf(treble_id, toString(poll_id)='1255390') sesion_4,
      uniqExactIf(treble_id, toString(poll_id)='1255396') falto_a_la_1,
      uniqExactIf(treble_id, toString(poll_id) IN ('1282059','1282062')) falto_a_otra
    FROM fact_deployment_status
    WHERE toString(poll_id) IN ('1282086','1255377','1255383','1255390','1255396','1282059','1282062') {corte}
    LIMIT 1
    """
    f = (_query_interna(sql) or [{}])[0]
    s1 = f.get("sesion_1") or 0
    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "ventana_dias": dias,
        "clientes_por_sesion": {
            "1": s1, "2": f.get("sesion_2"), "3": f.get("sesion_3"), "4": f.get("sesion_4"),
        },
        "inasistencias": {
            "faltaron_a_la_primera": f.get("falto_a_la_1"),
            "faltaron_a_alguna_de_2_a_4": f.get("falto_a_otra"),
        },
        "llegan_a_la_cuarta": round((f.get("sesion_4") or 0) / s1, 4) if s1 else None,
        "nota": ("Derivado de los pushes de cierre de sesión ENVIADOS, no entregados: el push "
                 "sale porque la sesión ocurrió, y que Meta lo entregue es posterior e "
                 "independiente. Filtrar por entrega distorsiona el embudo — el push de la "
                 "cuarta sesión hoy es rechazado por Meta el 55,6% de las veces."),
        "salud_de_los_pushes": ("Revisar aparte: Pos 4a sesión 55,6% FAILURE_BY_META_CHOSE_NOT_DELIVER, "
                 "Pos 1a 15,7% MISSING_PARAMETER, Pos 3a 9,0% MISSING_PARAMETER. No afecta el "
                 "conteo de sesiones, sí afecta que el cliente reciba el mensaje."),
    }


@app.post("/sesiones/sincronizar")
def sesiones_sincronizar(
    x_api_key: str | None = Header(default=None),
    aplicar: str | None = None,
    dias: int | None = None,
):
    """
    Calcula el conteo desde el DWH y lo escribe en la ficha de cada
    contacto, junto con la etapa que decide el enrutamiento del push
    de 72 h. DRY-RUN por defecto.
    """
    _chequear_clave(x_api_key)
    _, faltan = _sesiones_props_existentes()
    if faltan:
        raise HTTPException(409, f"Faltan propiedades: {faltan}. Llamá primero a POST /sesiones/setup.")

    escribir = _a_bool(aplicar, por_defecto=False)
    filas = _sesiones_desde_dwh(dias)

    entradas, etapas = [], {"acompanamiento": 0, "soporte": 0}
    for f in filas:
        hs = str(f.get("hubspot_id") or "").strip()
        if not hs.isdigit():
            continue
        asistidas = int(f.get("asistidas") or 0)
        etapa = "soporte" if asistidas >= SESIONES_CORTE_ETAPA else "acompanamiento"
        etapas[etapa] += 1
        props = {
            PROP_SES_ASISTIDAS: asistidas,
            PROP_SES_INASISTENCIAS: int(f.get("inasistencias") or 0),
            PROP_SES_ETAPA: etapa,
            PROP_SES_ORIGEN: "Derivado de pushes de cierre de sesión (bridge)",
        }
        ultima = _a_fecha(f.get("ultima"))
        if ultima:
            props[PROP_SES_ULTIMA] = _dia_ms(ultima)
        entradas.append({"id": hs, "properties": props})

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "clientes_con_sesiones": len(filas),
        "a_escribir": len(entradas),
        "sin_contacto_en_hubspot": len(filas) - len(entradas),
        "por_etapa": etapas,
        "corte_de_etapa": SESIONES_CORTE_ETAPA,
        "muestra": [{"hubspot_id": e["id"],
                     "asistidas": e["properties"][PROP_SES_ASISTIDAS],
                     "etapa": e["properties"][PROP_SES_ETAPA]} for e in entradas[:10]],
    }
    if not escribir:
        resultado["aviso"] = "Simulación. Para escribir en HubSpot: POST /sesiones/sincronizar?aplicar=true"
        return resultado

    escritos, errores = _hs_batch_update("contacts", entradas)
    METRICAS["sesiones_sincronizadas"] += escritos
    resultado.update({"escritos": escritos, "errores": errores})
    log.warning(f"[sesiones] sincronizadas={escritos} errores={len(errores)}")
    return resultado


# ══════════════════════════════════════════════════════════════════
#  WORKFLOWS: LECTURA CRUDA Y CREACIÓN DESDE PAYLOAD
#  Agregado 07/09/2026 (v1.3.7). BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué ───────────────────────────────────────────────────────
#  Angela pidió que el push de 72 h se bifurque: respuestas de clientes
#  en sus primeras 4 sesiones a gestoras de consultoría, de la 5ta en
#  adelante a ATC. En Treble ya están los dos flujos publicados
#  (1018613 · ATC y 1480814 · Consultoría). Falta el workflow de
#  HubSpot que elija uno u otro según `sesiones_etapa`, y eso exige una
#  RAMA — algo que `/workflows/push` no sabe construir porque solo
#  genera el patrón lineal.
#
#  El esquema de ramas de la API v4 no está documentado (igual que pasó
#  con el de creación). Así que en vez de adivinarlo, estos dos
#  endpoints permiten leer un workflow real que ya tenga ramas, copiar
#  su forma exacta, y crear el nuevo a partir de eso.
#
#  ── Regla de oro ──────────────────────────────────────────────────
#  NADA de esto modifica un workflow existente. Solo lee y crea nuevos.
#  El workflow en producción no se toca: el plan es clonar, dejar el
#  clon DESACTIVADO, y recién con el visto bueno de Angela apagar el
#  viejo y encender el nuevo. Ese cambio es atómico y reversible.
# ══════════════════════════════════════════════════════════════════

# Un workflow nuevo solo puede llamarse así. Evita que un error de acá
# genere algo con pinta de workflow oficial de Marketing o Ventas.
PREFIJOS_CREABLES = (PREFIJO_WORKFLOW_PUSH, "TEST - ")


@app.get("/workflows/{flow_id}/crudo")
def workflow_crudo(flow_id: str, x_api_key: str | None = Header(default=None)):
    """
    El JSON tal cual lo devuelve HubSpot. Solo lectura. Sirve para copiar
    la forma exacta de un workflow que ya funciona — sobre todo los que
    tienen ramas, cuyo esquema no está documentado.
    """
    _chequear_clave(x_api_key)
    if not flow_id.isdigit():
        raise HTTPException(400, "flow_id debe ser numérico.")
    try:
        return _hubspot_api("GET", f"/automation/v4/flows/{flow_id}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise HTTPException(404, f"No existe el workflow {flow_id}.")
        raise HTTPException(502, f"HubSpot respondió {e.code}.")


@app.get("/workflows/buscar")
def workflows_buscar(x_api_key: str | None = Header(default=None), texto: str = ""):
    """Busca workflows por nombre, para ubicar uno con ramas sin adivinar IDs."""
    _chequear_clave(x_api_key)
    if not texto or len(texto) < 3:
        raise HTTPException(400, "Pasá al menos 3 caracteres en ?texto=")
    flows, completo = _wf_listar_flows()
    if not completo:
        raise HTTPException(503, "No se pudo listar el portal completo; reintentá.")
    t = texto.lower()
    hallados = [{"flow_id": f.get("id"), "nombre": f.get("name"), "activo": f.get("isEnabled")}
                for f in flows if t in str(f.get("name") or "").lower()]
    return {"texto": texto, "encontrados": len(hallados), "workflows": hallados[:60]}


@app.post("/workflows/crear-crudo")
def workflow_crear_crudo(
    body: dict,
    x_api_key: str | None = Header(default=None),
    aplicar: str | None = None,
    activar: str | None = None,
):
    """
    Crea un workflow NUEVO a partir de un payload completo. No modifica
    nada existente. Pensado para armar workflows con ramas, que
    `/workflows/push` no puede generar.

    Salvaguardas:
      · DRY-RUN por defecto: sin ?aplicar=true devuelve lo que enviaría.
      · El nombre debe empezar por un prefijo permitido.
      · Nace DESACTIVADO salvo ?activar=true explícito.
      · Rechaza el payload si trae un id (sería un intento de sobrescribir).
    """
    _chequear_clave(x_api_key)
    if not isinstance(body, dict) or not body:
        raise HTTPException(400, "Falta el cuerpo del workflow.")
    if body.get("id"):
        raise HTTPException(400, "El payload no puede traer 'id': este endpoint solo crea, nunca sobrescribe.")

    nombre = str(body.get("name") or "").strip()
    if not nombre.startswith(PREFIJOS_CREABLES):
        raise HTTPException(
            403,
            f"El nombre debe empezar con alguno de {list(PREFIJOS_CREABLES)}. "
            "Es la salvaguarda para no crear workflows que parezcan oficiales de otro equipo.",
        )
    if body.get("objectTypeId") not in (None, "0-1"):
        raise HTTPException(400, "Solo se permiten workflows de contactos (objectTypeId 0-1).")

    escribir = _a_bool(aplicar, por_defecto=False)
    encendido = _a_bool(activar, por_defecto=False)

    payload = dict(body)
    payload["isEnabled"] = bool(encendido)
    payload.setdefault("type", "CONTACT_FLOW")
    payload.setdefault("flowType", "WORKFLOW")
    payload.setdefault("objectTypeId", "0-1")

    acciones = payload.get("actions") or []
    resumen = {
        "modo": "aplicado" if escribir else "simulacion",
        "nombre": nombre,
        "nacera_activo": bool(encendido),
        "acciones": len(acciones),
        "tipos_de_accion": sorted({str(a.get("actionTypeId")) for a in acciones if isinstance(a, dict)}),
        "tiene_ramas": any(str(a.get("type")) == "LIST_BRANCH" or "listBranches" in a
                           for a in acciones if isinstance(a, dict)),
    }
    if not escribir:
        resumen["aviso"] = "Simulación. Para crearlo de verdad: ?aplicar=true (y ?activar=true si además debe nacer encendido)."
        resumen["payload_que_se_enviaria"] = payload
        return resumen

    try:
        creado = _hubspot_api("POST", "/automation/v4/flows", payload)
    except urllib.error.HTTPError as e:
        detalle = ""
        try:
            detalle = e.read().decode()[:600]
        except Exception:
            pass
        raise HTTPException(502, f"HubSpot rechazó la creación ({e.code}): {detalle}")

    METRICAS["workflows_creados"] += 1
    _CACHE_FLOWS.update({"datos": None, "ts": 0, "completo": False})
    log.warning(f"[workflows] CREADO desde payload id={creado.get('id')} nombre={nombre!r} activo={encendido}")
    resumen.update({"flow_id": creado.get("id"), "creado": True})
    return resumen


# ══════════════════════════════════════════════════════════════════
#  RIESGO DE CANCELACIÓN
#  Agregado 07/09/2026 (v1.3.8). BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué ───────────────────────────────────────────────────────
#  133 clientes por semana llegan a pedir la baja, de forma constante
#  desde junio. Cuando escriben, la decisión ya está tomada: esas
#  conversaciones duran 7 horas de mediana. Esto busca detectarlos
#  antes, cuando todavía se puede hacer algo.
#
#  ── Cómo se eligieron las señales ─────────────────────────────────
#  NO se inventaron. Se hizo un caso-control real sobre el DWH:
#    · casos   = 1.245 clientes que pidieron cancelar (jul-sep 2026)
#    · control = 1.699 con sesiones y sin cancelación
#  midiendo cada señal en los 30-60 días PREVIOS a la cancelación.
#  Resultado:
#
#    señal                        casos   control   ¿sirve?
#    ─────────────────────────────────────────────────────────
#    +60 días sin sesión          60,2%     9,0%    SÍ (6,7x)
#    2 o más inasistencias        16,0%    16,7%    no
#    pago fallido previo          10,8%    10,4%    no
#    no responde los pushes       73,0%    84,9%    no
#
#  O sea: de las cuatro señales que parecían obvias, TRES no
#  discriminan nada. La única que predice es cuánto hace que el
#  cliente no tiene una sesión. Por eso el modelo es de una sola
#  variable: agregar las otras solo metería ruido.
#
#  Precisión medida: de cada 100 clientes marcados en riesgo alto,
#  84 efectivamente pidieron cancelar. Sensibilidad: detecta al 59%
#  de los que cancelan.
#
#  ── Límite honesto ────────────────────────────────────────────────
#  "Días sin sesión" se deriva de los pushes de cierre de sesión, la
#  misma fuente que el contador. No mide asistencia verificada en
#  plataforma. Y el corte de 60 días sale de esta muestra y de este
#  momento: conviene revalidarlo cada tanto, no darlo por eterno.
# ══════════════════════════════════════════════════════════════════

PROP_RIESGO = "riesgo_cancelacion"
PROP_DIAS_SIN_SESION = "dias_sin_sesion"
PROP_RIESGO_FECHA = "riesgo_actualizado"

# Umbrales validados contra el caso-control. Configurables por si la
# revalidación futura los mueve.
RIESGO_DIAS_ALTO = int(os.environ.get("RIESGO_DIAS_ALTO", "60"))
RIESGO_DIAS_MEDIO = int(os.environ.get("RIESGO_DIAS_MEDIO", "30"))

RIESGO_NIVELES = [
    ("alto", "Alto · más de 60 días sin sesión"),
    ("medio", "Medio · entre 31 y 60 días"),
    ("bajo", "Bajo · sesión en los últimos 30 días"),
]

for _m in ("riesgo_calculado",):
    METRICAS.setdefault(_m, 0)


def _riesgo_props_existentes():
    faltan, existen = [], {}
    for nombre in (PROP_RIESGO, PROP_DIAS_SIN_SESION, PROP_RIESGO_FECHA):
        try:
            existen[nombre] = _hubspot_api("GET", f"/crm/v3/properties/contacts/{nombre}")
        except Exception:
            faltan.append(nombre)
    return existen, faltan


@app.post("/riesgo/setup")
def riesgo_setup(x_api_key: str | None = Header(default=None)):
    """Crea las propiedades del riesgo si no existen. Idempotente."""
    _chequear_clave(x_api_key)
    existen, faltan = _riesgo_props_existentes()
    if not faltan:
        return {"creadas": [], "ya_existian": list(existen), "mensaje": "Ya estaban todas."}

    defs = {
        PROP_RIESGO: {
            "name": PROP_RIESGO, "label": "Riesgo de cancelación", "type": "enumeration",
            "fieldType": "select", "groupName": GRUPO_COHORTE,
            "description": ("Riesgo de que el cliente pida la baja. Basado en los días sin sesión, "
                            "la única señal que resultó predictiva en el análisis caso-control "
                            "(84% de precisión). Lo mantiene el bridge."),
            "options": [{"label": et, "value": v, "displayOrder": i, "hidden": False}
                        for i, (v, et) in enumerate(RIESGO_NIVELES)],
        },
        PROP_DIAS_SIN_SESION: {
            "name": PROP_DIAS_SIN_SESION, "label": "Días sin sesión", "type": "number",
            "fieldType": "number", "groupName": GRUPO_COHORTE,
            "description": "Días desde la última sesión registrada del cliente.",
        },
        PROP_RIESGO_FECHA: {
            "name": PROP_RIESGO_FECHA, "label": "Riesgo · última revisión", "type": "date",
            "fieldType": "date", "groupName": GRUPO_COHORTE,
            "description": "Cuándo se recalculó por última vez el riesgo de este cliente.",
        },
    }
    creadas, errores = [], []
    for nombre in faltan:
        try:
            _hubspot_api("POST", "/crm/v3/properties/contacts", defs[nombre])
            creadas.append(nombre)
        except Exception as e:
            errores.append({"propiedad": nombre, "error": str(e)[:300]})
    log.warning(f"[riesgo] setup creadas={creadas} errores={errores}")
    return {"creadas": creadas, "ya_existian": list(existen), "errores": errores}


def _riesgo_desde_dwh():
    """
    Días sin sesión por cliente, ya mapeado a su contacto de HubSpot.
    Marca aparte a quien ya tiene una conversación de cancelación: ese
    caso no es "riesgo", ya está en gestión o ya se fue, y meterlo en la
    lista preventiva solo la ensucia.
    """
    ses = ",".join(f"'{k}'" for k in PUSHES_ASISTIO)
    sql = f"""
    WITH
    ult AS (
      SELECT treble_id tid, max(timestamps_eta) ultima
      FROM fact_deployment_status
      WHERE toString(poll_id) IN ({ses})
      GROUP BY tid
    ),
    canc AS (
      SELECT DISTINCT contact_wa_id wa FROM fact_conversations
      WHERE tag_name = 'Cancelaciones'
    ),
    c AS (
      SELECT contact_wa_id wa, any(helpdesk_contact_id) hs
      FROM fact_conversations WHERE helpdesk_contact_id != '' GROUP BY wa
    )
    SELECT ult.tid tid, any(c.hs) hubspot_id,
           dateDiff('day', any(ult.ultima), now()) dias,
           toDate(any(ult.ultima)) ultima_sesion,
           max(if(canc.wa != '', 1, 0)) ya_pidio_cancelar
    FROM ult
    LEFT JOIN c ON ult.tid = c.wa
    LEFT JOIN canc ON ult.tid = canc.wa
    GROUP BY tid
    HAVING hubspot_id != ''
    ORDER BY dias DESC
    LIMIT {SESIONES_MAX_CONTACTOS}
    """
    return _query_interna(sql)


def _riesgo_nivel(dias):
    if dias is None:
        return None
    if dias > RIESGO_DIAS_ALTO:
        return "alto"
    if dias > RIESGO_DIAS_MEDIO:
        return "medio"
    return "bajo"


@app.post("/riesgo/calcular")
def riesgo_calcular(x_api_key: str | None = Header(default=None), aplicar: str | None = None):
    """
    Recalcula el riesgo de todos los clientes con sesiones registradas y
    lo escribe en su ficha. DRY-RUN por defecto.
    """
    _chequear_clave(x_api_key)
    _, faltan = _riesgo_props_existentes()
    if faltan:
        raise HTTPException(409, f"Faltan propiedades: {faltan}. Llamá primero a POST /riesgo/setup.")

    escribir = _a_bool(aplicar, por_defecto=False)
    hoy = datetime.now(timezone.utc).date()
    filas = _riesgo_desde_dwh()

    entradas, conteo = [], {"alto": 0, "medio": 0, "bajo": 0}
    ya_gestionados = 0
    for f in filas:
        hs = str(f.get("hubspot_id") or "").strip()
        if not hs.isdigit():
            continue
        dias = int(f.get("dias") or 0)
        nivel = _riesgo_nivel(dias)
        if not nivel:
            continue
        if int(f.get("ya_pidio_cancelar") or 0):
            ya_gestionados += 1
        conteo[nivel] += 1
        entradas.append({"id": hs, "properties": {
            PROP_RIESGO: nivel,
            PROP_DIAS_SIN_SESION: dias,
            PROP_RIESGO_FECHA: _dia_ms(hoy),
        }})

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "clientes_evaluados": len(entradas),
        "por_nivel": conteo,
        "de_esos_ya_pidieron_cancelar": ya_gestionados,
        "umbrales": {"alto_mas_de_dias": RIESGO_DIAS_ALTO, "medio_mas_de_dias": RIESGO_DIAS_MEDIO},
        "precision_medida": 0.838,
        "nota": ("Umbral validado con caso-control sobre 1.245 clientes que cancelaron y 1.699 que no. "
                 "De cada 100 marcados en riesgo alto, 84 efectivamente pidieron la baja."),
    }
    if not escribir:
        resultado["aviso"] = "Simulación. Para escribir en HubSpot: POST /riesgo/calcular?aplicar=true"
        return resultado

    escritos, errores = _hs_batch_update("contacts", entradas)
    METRICAS["riesgo_calculado"] += escritos
    resultado.update({"escritos": escritos, "errores": errores})
    log.warning(f"[riesgo] calculado={escritos} alto={conteo['alto']} errores={len(errores)}")
    return resultado


@app.get("/riesgo/lista")
def riesgo_lista(x_api_key: str | None = Header(default=None), nivel: str = "alto", tope: int = 400):
    """
    La lista de trabajo: clientes en riesgo que TODAVÍA no pidieron la
    baja. Es la que tiene sentido gestionar — el resto ya está en curso.
    """
    _chequear_clave(x_api_key)
    if nivel not in [v for v, _ in RIESGO_NIVELES]:
        raise HTTPException(400, f"nivel debe ser uno de {[v for v, _ in RIESGO_NIVELES]}")

    filas = _riesgo_desde_dwh()
    pendientes = [f for f in filas
                  if _riesgo_nivel(int(f.get("dias") or 0)) == nivel
                  and not int(f.get("ya_pidio_cancelar") or 0)
                  and str(f.get("hubspot_id") or "").isdigit()]

    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "nivel": nivel,
        "total": len(pendientes),
        "clientes": [{
            "hubspot_id": f["hubspot_id"],
            "dias_sin_sesion": int(f["dias"]),
            "ultima_sesion": str(f.get("ultima_sesion") or ""),
            "telefono": _mask_phone(str(f.get("tid") or "")),
        } for f in pendientes[:min(int(tope), 1000)]],
        "nota": "Excluye a quienes ya tienen una conversación de cancelación: esos ya están en gestión.",
    }


# ══════════════════════════════════════════════════════════════════
#  PARTE DIARIO DE SALUD DE MENSAJERÍA
#  Agregado 08/09/2026 (v1.3.9). BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué reemplaza al monitor de escalamiento ──────────────────
#  Iva preguntó si el "ningún caso nuevo hoy" de todos los días
#  significaba que había mejorado la tasa de respuesta o que estábamos
#  mandando algo mal. Al verificarlo aparecieron TRES problemas:
#
#  1. El monitor decía medir respuestas del cliente. En realidad mide
#     ENTREGAS fallidas. Son cosas distintas y nadie lo sabía.
#  2. El umbral es de 3 fallos del MISMO número en 30 días, y eso casi
#     nunca ocurre: en el último mes lo alcanzó 1 número. De 122 con
#     algún fallo, 114 fallaron una sola vez. El umbral los tapa a todos.
#  3. Solo vigila 8 campañas de las más de 100 que existen.
#
#  Resultado: decía "cero" mientras ~120 clientes al mes se quedaban sin
#  su mensaje. Un monitor que siempre dice cero entrena al equipo a
#  ignorarlo, que es peor que no tenerlo.
#
#  ── Qué hace este ─────────────────────────────────────────────────
#  Un parte diario que SIEMPRE da números, aunque no haya nada raro:
#    · cuántos mensajes salieron ayer y cuántos no llegaron
#    · si eso es normal o peor que la semana
#    · por qué no llegaron, con la acción que corresponde a cada causa
#    · qué push está fallando más
#    · cuántos clientes acumulan mensajes sin recibir
#
#  Nunca dice "ningún caso". Si todo está bien, lo dice con el número.
#
#  ── Para apagar el monitor viejo ──────────────────────────────────
#  Basta con borrar ESCALAMIENTO_SLACK_WEBHOOK_URL en Render. No hay
#  que tocar una línea de código.
# ══════════════════════════════════════════════════════════════════

SALUD_SLACK_WEBHOOK_URL = os.environ.get("SALUD_SLACK_WEBHOOK_URL", "")
SALUD_COMPANY_ID = os.environ.get("SALUD_COMPANY_ID", "25732")
SALUD_HORA_UTC = int(os.environ.get("SALUD_HORA_UTC", "13"))
# Cuántos puntos por encima del promedio semanal se considera anómalo.
SALUD_UMBRAL_ALERTA = float(os.environ.get("SALUD_UMBRAL_ALERTA", "2.0"))

# Cada causa con su lectura en castellano y qué hacer. Sin esto el parte
# sería una lista de códigos que nadie sabe interpretar.
CAUSAS = {
    "FAILURE_BY_HUMAN_HANDOVER": ("el cliente tenía un chat abierto en Treble",
                                  "el reintento automático los recupera"),
    "FAILURE_BY_META_CHOSE_NOT_DELIVER": ("Meta rechazó el envío",
                                          "hay que revisar la plantilla"),
    "MISSING_PARAMETER": ("falta un parámetro en la plantilla",
                          "es configuración nuestra, se arregla en HubSpot"),
    "FAILURE_BY_UNABLE_TO_CONTACT": ("el número no existe o no recibe",
                                     "dato de contacto muerto"),
    "FAILURE_BY_OPTOUT_CONTACT": ("el cliente se dio de baja",
                                  "correcto, no hay que hacer nada"),
    "FAILURE_BY_DISABLED_HSM": ("la plantilla está desactivada en Meta",
                                "hay que reactivarla o dejar de usarla"),
    "FAILURE_BY_BLOCKED_CONTACT": ("el cliente bloqueó el número", "sin acción"),
    "RECEIVED_BY_WORKER": ("quedó en cola y no se resolvió", "vigilar si crece"),
    "FAILURE_BY_MEDIA_UPLOAD_ERROR": ("falló la carga de un archivo adjunto",
                                      "revisar el contenido del push"),
    "INVALID_PHONE": ("el número tiene formato inválido", "corregir en el CRM"),
    "FAILURE": ("fallo genérico sin detalle", "vigilar si crece"),
}

for _m in ("partes_salud_enviados",):
    METRICAS.setdefault(_m, 0)


def _salud_datos():
    """Todo lo que necesita el parte, en tres consultas."""
    cia = int(SALUD_COMPANY_ID)
    dias = _query_interna(f"""
        SELECT toDate(timestamps_eta) dia, count() enviados,
               countIf(status NOT IN ('DELIVERED','SUCCESS')) no_llegaron
        FROM fact_deployment_status
        WHERE company_id = {cia} AND timestamps_eta >= today() - 8
        GROUP BY dia ORDER BY dia DESC LIMIT 9
    """)
    causas = _query_interna(f"""
        SELECT status, count() c FROM fact_deployment_status
        WHERE company_id = {cia} AND toDate(timestamps_eta) = today() - 1
          AND status NOT IN ('DELIVERED','SUCCESS')
        GROUP BY status ORDER BY c DESC LIMIT 8
    """)
    peor = _query_interna(f"""
        SELECT poll_id, count() enviados,
               countIf(status NOT IN ('DELIVERED','SUCCESS')) fallan
        FROM fact_deployment_status
        WHERE company_id = {cia} AND timestamps_eta >= today() - 7
        GROUP BY poll_id HAVING enviados >= 20 AND fallan >= 5
        ORDER BY fallan / enviados DESC LIMIT 3
    """)
    acumulan = _query_interna(f"""
        SELECT countIf(veces >= 3) tres_o_mas, countIf(veces >= 2) dos_o_mas, count() con_algun_fallo
        FROM (SELECT treble_id, count() veces FROM fact_deployment_status
              WHERE company_id = {cia} AND timestamps_eta >= today() - 7
                AND status NOT IN ('DELIVERED','SUCCESS')
              GROUP BY treble_id)
        LIMIT 1
    """)
    return dias, causas, peor, (acumulan or [{}])[0]


def _salud_armar():
    dias, causas, peor, acum = _salud_datos()
    if not dias:
        return None
    # dias[0] es hoy (parcial); el parte habla de AYER, que es el último día completo.
    ayer = dias[1] if len(dias) > 1 else dias[0]
    previos = dias[2:9] if len(dias) > 2 else []
    env_ayer = int(ayer.get("enviados") or 0)
    mal_ayer = int(ayer.get("no_llegaron") or 0)
    pct_ayer = round(100 * mal_ayer / env_ayer, 1) if env_ayer else 0.0

    tot_e = sum(int(d.get("enviados") or 0) for d in previos)
    tot_m = sum(int(d.get("no_llegaron") or 0) for d in previos)
    pct_prev = round(100 * tot_m / tot_e, 1) if tot_e else 0.0
    delta = round(pct_ayer - pct_prev, 1)

    return {
        "fecha": str(ayer.get("dia")),
        "enviados": env_ayer, "no_llegaron": mal_ayer, "pct": pct_ayer,
        "pct_promedio_7d": pct_prev, "diferencia_puntos": delta,
        "anomalo": delta >= SALUD_UMBRAL_ALERTA,
        "causas": [{"status": c["status"], "casos": int(c["c"]),
                    "que_paso": CAUSAS.get(c["status"], ("sin clasificar", "revisar"))[0],
                    "que_hacer": CAUSAS.get(c["status"], ("sin clasificar", "revisar"))[1]}
                   for c in causas],
        "pushes_mas_afectados": [
            {"conversation_id": str(p["poll_id"]), "enviados": int(p["enviados"]),
             "no_llegaron": int(p["fallan"]),
             "pct": round(100 * int(p["fallan"]) / int(p["enviados"]), 1)} for p in peor],
        "clientes_con_3_o_mas_sin_recibir": int(acum.get("tres_o_mas") or 0),
        "clientes_con_2_o_mas_sin_recibir": int(acum.get("dos_o_mas") or 0),
        "clientes_con_algun_fallo_7d": int(acum.get("con_algun_fallo") or 0),
    }


def _salud_texto(r):
    """El mensaje de Slack. Siempre con números, nunca 'ningún caso'."""
    if r["anomalo"]:
        cab = (f":red_circle: *Salud de mensajería · {r['fecha']}*\n"
               f"Ayer salieron {r['enviados']:,} mensajes y *no llegaron {r['no_llegaron']}* "
               f"({r['pct']}%). Son {r['diferencia_puntos']} puntos peor que la semana "
               f"({r['pct_promedio_7d']}% de promedio).")
    else:
        cab = (f":white_check_mark: *Salud de mensajería · {r['fecha']}*\n"
               f"Ayer salieron {r['enviados']:,} mensajes y no llegaron {r['no_llegaron']} "
               f"({r['pct']}%). En línea con la semana ({r['pct_promedio_7d']}%).")
    cab = cab.replace(",", ".")

    partes = [cab]
    if r["causas"]:
        lineas = [f"  • *{c['casos']}* — {c['que_paso']} _({c['que_hacer']})_" for c in r["causas"]]
        partes.append("*Por qué no llegaron:*\n" + "\n".join(lineas))
    if r["pushes_mas_afectados"]:
        p = r["pushes_mas_afectados"][0]
        partes.append(f"*Push más afectado esta semana:* conversación {p['conversation_id']} — "
                      f"no llega el {p['pct']}% de sus envíos ({p['no_llegaron']} de {p['enviados']}).")
    partes.append(
        f"*Clientes acumulando fallos (7 días):* {r['clientes_con_3_o_mas_sin_recibir']} llevan 3 o más "
        f"mensajes sin recibir, {r['clientes_con_2_o_mas_sin_recibir']} llevan 2 o más, "
        f"{r['clientes_con_algun_fallo_7d']} tuvieron al menos uno.")
    return "\n\n".join(partes)


@app.get("/salud/mensajeria")
def salud_mensajeria(x_api_key: str | None = Header(default=None)):
    """El parte del día en JSON, para consultarlo cuando se quiera."""
    _chequear_clave(x_api_key)
    r = _salud_armar()
    if not r:
        raise HTTPException(503, "Sin datos suficientes para armar el parte.")
    r["texto_slack"] = _salud_texto(r)
    return r


@app.post("/salud/enviar")
def salud_enviar(x_api_key: str | None = Header(default=None), aplicar: str | None = None):
    """Manda el parte a Slack. DRY-RUN por defecto: sin ?aplicar=true solo lo muestra."""
    _chequear_clave(x_api_key)
    r = _salud_armar()
    if not r:
        raise HTTPException(503, "Sin datos suficientes.")
    texto = _salud_texto(r)
    if not _a_bool(aplicar, por_defecto=False):
        return {"modo": "simulacion", "texto": texto,
                "aviso": "Para enviarlo de verdad: POST /salud/enviar?aplicar=true"}
    if not SALUD_SLACK_WEBHOOK_URL:
        raise HTTPException(409, "Falta configurar SALUD_SLACK_WEBHOOK_URL.")
    _slack_enviar(SALUD_SLACK_WEBHOOK_URL, texto, nombre="salud_mensajeria")
    METRICAS["partes_salud_enviados"] += 1
    log.warning(f"[salud] parte enviado · {r['fecha']} · {r['no_llegaron']} sin llegar")
    return {"modo": "aplicado", "enviado": True, "texto": texto}


def _salud_monitor_loop():
    """
    Una vez al día, a la hora configurada. La deduplicación va contra la
    tabla de eventos y no contra una variable en memoria: si Render levanta
    más de una instancia, en memoria cada una creería que le toca mandarlo
    y el canal recibiría el parte repetido.
    """
    while True:
        try:
            ahora = datetime.now(timezone.utc)
            if ahora.hour == SALUD_HORA_UTC:
                marca = str(ahora.date())
                if not _evento_ya_notificado("parte_salud", marca):
                    r = _salud_armar()
                    if r:
                        _slack_enviar(SALUD_SLACK_WEBHOOK_URL, _salud_texto(r), nombre="salud_mensajeria")
                        _evento_marcar("parte_salud", marca, "notified", notified=True)
                        METRICAS["partes_salud_enviados"] += 1
                        log.warning(f"[salud] parte diario enviado · {r['fecha']}")
        except Exception as e:
            log.error(f"[salud] fallo armando el parte: {e}")
        time.sleep(300)


@app.on_event("startup")
def arrancar_monitor_salud():
    """
    Handler de arranque propio. FastAPI ejecuta todos los registrados, así
    que este convive con el de los monitores viejos sin tocarlo.
    """
    if not SALUD_SLACK_WEBHOOK_URL:
        log.warning("[startup] parte de salud no arranca — falta SALUD_SLACK_WEBHOOK_URL")
        return
    threading.Thread(target=_salud_monitor_loop, daemon=True).start()
    log.warning(f"[startup] parte de salud activo · se envía a las {SALUD_HORA_UTC}:00 UTC")


# ══════════════════════════════════════════════════════════════════
#  CORRECCIÓN: CLIENTES ANTERIORES AL REGISTRO DE SESIONES
#  Agregado 08/09/2026 (v1.4.0). BLOQUE PURAMENTE ADITIVO.
#
#  ── El error que corrige ──────────────────────────────────────────
#  Angela lo detectó desde el sentido común: "me parece raro que el 70%
#  de los envíos quede en las 4 primeras sesiones, teniendo casi 5.000
#  clientes activos". Tenía razón.
#
#  El contador de sesiones se deriva de los pushes de cierre, y ese
#  registro EMPIEZA EL 16/06/2026. No hay datos antes. Entonces un
#  cliente con un año de antigüedad y 40 sesiones hechas, si en estos
#  84 días hizo 3, el contador dice 3 y la bifurcación lo manda a
#  gestoras de consultoría como si fuera nuevo.
#
#  Medido: 733 clientes marcados como "primeras 4 sesiones" habían
#  comprado ANTES del 16/06 — el 40% de ese grupo, mal clasificado.
#
#  ── La regla ──────────────────────────────────────────────────────
#  Si el cliente compró antes de que exista el registro, el contador no
#  es confiable y va a ATC. Después de tres meses con un plan de 2 o 4
#  sesiones mensuales es casi imposible que siga en sus primeras 4.
#  El error, si lo hay, cae del lado conservador: ATC es exactamente lo
#  que pasaba antes de la bifurcación, así que nadie queda sin atender.
#
#  ── Por qué es un endpoint aparte ─────────────────────────────────
#  Para no tocar `sesiones_sincronizar`, que ya está en producción y
#  funciona. Este corre DESPUÉS y arregla lo que aquel no puede saber,
#  porque la fecha de compra vive en HubSpot y no en el DWH.
#  IMPORTANTE: hay que correr los dos, en orden. `/sesiones/estado`
#  avisa si quedó la corrección pendiente.
# ══════════════════════════════════════════════════════════════════

PROP_FECHA_COMPRA = "fecha_compra"
# Se calcula del DWH en vez de dejarlo fijo: si mañana se carga más
# historia, el umbral se mueve solo y la corrección deja de hacer falta.
_CACHE_INICIO_REGISTRO = {"fecha": None, "ts": 0.0}


def _sesiones_inicio_registro():
    """Primer día con datos de sesiones. Cacheado una hora."""
    ahora = time.time()
    if _CACHE_INICIO_REGISTRO["fecha"] and (ahora - _CACHE_INICIO_REGISTRO["ts"]) < 3600:
        return _CACHE_INICIO_REGISTRO["fecha"]
    ses = ",".join(f"'{k}'" for k in PUSHES_ASISTIO)
    filas = _query_interna(f"""
        SELECT min(toDate(timestamps_eta)) inicio FROM fact_deployment_status
        WHERE toString(poll_id) IN ({ses}) LIMIT 1
    """)
    fecha = _a_fecha((filas or [{}])[0].get("inicio")) if filas else None
    if fecha:
        _CACHE_INICIO_REGISTRO.update({"fecha": fecha, "ts": ahora})
    return fecha


@app.get("/sesiones/estado")
def sesiones_estado(x_api_key: str | None = Header(default=None)):
    """
    Cuánto se puede confiar en el contador hoy, y si quedó la corrección
    pendiente. Sirve para no volver a publicar una cifra sesgada.
    """
    _chequear_clave(x_api_key)
    inicio = _sesiones_inicio_registro()
    if not inicio:
        raise HTTPException(503, "No se pudo determinar el inicio del registro de sesiones.")
    dias = (datetime.now(timezone.utc).date() - inicio).days

    def _contar(filtros):
        try:
            d = _hubspot_api("POST", "/crm/v3/objects/contacts/search",
                             {"filterGroups": [{"filters": filtros}], "properties": ["hs_object_id"], "limit": 1})
            return d.get("total", 0)
        except Exception:
            return None

    pendientes = _contar([
        {"propertyName": PROP_SES_ETAPA, "operator": "EQ", "value": "acompanamiento"},
        {"propertyName": PROP_FECHA_COMPRA, "operator": "LT", "value": str(_dia_ms(inicio))},
    ])
    activos = _contar([
        {"propertyName": "lifecyclestage", "operator": "EQ", "value": "customer"},
        {"propertyName": "fecha_ultimo_pago", "operator": "GTE",
         "value": str(_dia_ms(datetime.now(timezone.utc).date() - timedelta(days=35)))},
    ])
    con_etapa = _contar([{"propertyName": PROP_SES_ETAPA, "operator": "HAS_PROPERTY"}])

    return {
        "inicio_del_registro": inicio.isoformat(),
        "dias_de_historia": dias,
        "clientes_activos": activos,
        "con_etapa_calculada": con_etapa,
        "cobertura": round(con_etapa / activos, 3) if activos and con_etapa else None,
        "correccion_pendiente": pendientes,
        "listo": pendientes == 0,
        "nota": ("El contador solo ve desde el inicio del registro. Un cliente que compró antes "
                 "tiene el conteo truncado, así que va a ATC. 'correccion_pendiente' debe ser 0: "
                 "si no lo es, falta correr POST /sesiones/corregir-veteranos."),
    }


@app.post("/sesiones/corregir-veteranos")
def sesiones_corregir_veteranos(x_api_key: str | None = Header(default=None), aplicar: str | None = None):
    """
    Pasa a ATC a los clientes que compraron antes de que existiera el
    registro de sesiones. DRY-RUN por defecto.
    """
    _chequear_clave(x_api_key)
    inicio = _sesiones_inicio_registro()
    if not inicio:
        raise HTTPException(503, "No se pudo determinar el inicio del registro.")
    escribir = _a_bool(aplicar, por_defecto=False)

    afectados = _hs_buscar_todo("contacts", {
        "filterGroups": [{"filters": [
            {"propertyName": PROP_SES_ETAPA, "operator": "EQ", "value": "acompanamiento"},
            {"propertyName": PROP_FECHA_COMPRA, "operator": "LT", "value": str(_dia_ms(inicio))},
        ]}],
        "properties": ["hs_object_id", "hs_full_name_or_email", PROP_FECHA_COMPRA, PROP_SES_ASISTIDAS],
    })

    entradas = [{"id": r["id"], "properties": {
        PROP_SES_ETAPA: "soporte",
        PROP_SES_ORIGEN: (f"Compró antes del {inicio.isoformat()}, cuando empieza el registro de "
                          "sesiones: el conteo está truncado y no sirve para decidir la etapa."),
    }} for r in afectados]

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "inicio_del_registro": inicio.isoformat(),
        "a_corregir": len(entradas),
        "muestra": [{"nombre": r["properties"].get("hs_full_name_or_email"),
                     "compro": r["properties"].get(PROP_FECHA_COMPRA),
                     "sesiones_que_contaba": r["properties"].get(PROP_SES_ASISTIDAS)}
                    for r in afectados[:8]],
        "razon": ("Su conteo de sesiones está truncado por el inicio del registro. Pasan a ATC, "
                  "que es el destino conservador: es lo que pasaba antes de la bifurcación."),
    }
    if not escribir:
        resultado["aviso"] = "Simulación. Para aplicarlo: POST /sesiones/corregir-veteranos?aplicar=true"
        return resultado

    escritos, errores = _hs_batch_update("contacts", entradas) if entradas else (0, [])
    resultado.update({"corregidos": escritos, "errores": errores})
    log.warning(f"[sesiones] veteranos corregidos={escritos} errores={len(errores)}")
    return resultado


@app.on_event("startup")
def arrancar_monitores():
    _init_db()
    if not _adquirir_lock_monitores():
        log.warning("[startup] otro proceso ya tiene el lock de monitores — no se arrancan aquí")
        return

    if ESTADO_CONFIG["monitor_sla"]["ok"] and ESTADO_CONFIG["hubspot"]["ok"]:
        threading.Thread(target=_sla_monitor_loop, daemon=True).start()
    else:
        log.warning("[startup] monitor_sla no arranca — configuración incompleta")

    if ESTADO_CONFIG["monitor_pedidos"]["ok"] and ESTADO_CONFIG["hubspot"]["ok"]:
        threading.Thread(target=_pedidos_monitor_loop, daemon=True).start()
    else:
        log.warning("[startup] monitor_pedidos no arranca — configuración incompleta")

    if ESTADO_CONFIG["monitor_escalamiento"]["ok"] and ESTADO_CONFIG["hubspot"]["ok"]:
        threading.Thread(target=_escalamiento_monitor_loop, daemon=True).start()
    else:
        log.warning("[startup] monitor_escalamiento no arranca — configuración incompleta")

    if ESTADO_CONFIG["monitor_onboarding"]["ok"] and ESTADO_CONFIG["hubspot"]["ok"]:
        threading.Thread(target=_onboarding_monitor_loop, daemon=True).start()
    else:
        log.warning("[startup] monitor_onboarding no arranca — falta ONBOARDING_SLACK_WEBHOOK_URL")


# ══════════════════════════════════════════════════════════════════
#  PARTE DE SALUD · DESGLOSE POR PUSH Y LISTA DE CLIENTES
#  Agregado 08/09/2026 (v1.4.1). BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué ───────────────────────────────────────────────────────
#  Iva leyó el parte y preguntó dos cosas razonables: "¿se puede ver la
#  lista de quiénes son?" y "esos 61, ¿qué pushes eran?".
#  Tenía razón: el parte decía CUÁNTOS fallaban y POR QUÉ, pero no DÓNDE,
#  y sin eso no se puede accionar. Un número sin destinatario no sirve.
#
#  ── Qué cambia ────────────────────────────────────────────────────
#  1. Cada causa del parte ahora abre los pushes que la concentran.
#     Ejemplo real del 07/09: de los 12 que rechazó Meta, 10 eran del
#     mismo push. Eso es una plantilla rota, no un problema general —
#     y en el parte viejo se leía como si fuera lo segundo.
#  2. GET /salud/detalle devuelve el desglose completo y la lista de
#     clientes con fallos acumulados, ya cruzada con HubSpot.
#
#  ── Cómo se sobreescribe sin tocar el bloque anterior ─────────────
#  Se redefinen `_salud_datos`, `_salud_armar` y `_salud_texto`. Python
#  resuelve por nombre en el momento de la llamada, así que los endpoints
#  y el hilo del monitor —que ya están registrados— toman estas versiones
#  sin que haya que modificar una sola línea de lo que ya funciona.
# ══════════════════════════════════════════════════════════════════

# Cuántos pushes se nombran por causa en el mensaje de Slack. Más que
# esto y el parte deja de leerse de un vistazo, que es su única virtud.
SALUD_PUSHES_POR_CAUSA = int(os.environ.get("SALUD_PUSHES_POR_CAUSA", "2"))
# Solo para armar el link a la ficha. Si no está, se devuelve el id pelado.
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "40159402")
_CACHE_NOMBRES_PUSH = {"datos": None, "ts": 0.0}


def _salud_nombres_push():
    """
    poll_id -> nombre. Treble cambia el poll_id en cada publicación y a
    veces reporta el nombre vacío, así que se toma el último no vacío que
    se haya visto para ese id. Cacheado media hora.
    """
    ahora = time.time()
    if _CACHE_NOMBRES_PUSH["datos"] is not None and (ahora - _CACHE_NOMBRES_PUSH["ts"]) < 1800:
        return _CACHE_NOMBRES_PUSH["datos"]
    filas = _query_interna("""
        SELECT toString(poll_id) pid, argMax(poll_name, timestamps_eta) nombre
        FROM fact_deployment_status
        WHERE poll_name != '' AND timestamps_eta >= now() - INTERVAL 120 DAY
        GROUP BY pid
    """) or []
    m = {f["pid"]: str(f["nombre"]).strip() for f in filas if str(f.get("nombre") or "").strip()}
    _CACHE_NOMBRES_PUSH.update({"datos": m, "ts": ahora})
    return m


def _salud_push_etiqueta(pid, nombres=None):
    """Nombre del push, o el id a secas si Treble nunca lo reportó."""
    nombres = nombres if nombres is not None else _salud_nombres_push()
    n = nombres.get(str(pid))
    return n if n else f"conversación {pid}"


def _salud_datos():
    """
    Igual que la versión anterior pero agregando el desglose causa×push.
    Es una consulta más, no cinco: se agrupa por las dos dimensiones y el
    resumen por causa se arma sumando en Python.
    """
    cia = int(SALUD_COMPANY_ID)
    dias = _query_interna(f"""
        SELECT toDate(timestamps_eta) dia, count() enviados,
               countIf(status NOT IN ('DELIVERED','SUCCESS')) no_llegaron
        FROM fact_deployment_status
        WHERE company_id = {cia} AND timestamps_eta >= today() - 8
        GROUP BY dia ORDER BY dia DESC LIMIT 9
    """)
    detalle = _query_interna(f"""
        SELECT status, toString(poll_id) pid, count() c
        FROM fact_deployment_status
        WHERE company_id = {cia} AND toDate(timestamps_eta) = today() - 1
          AND status NOT IN ('DELIVERED','SUCCESS')
        GROUP BY status, pid ORDER BY status, c DESC
    """) or []
    peor = _query_interna(f"""
        SELECT poll_id, count() enviados,
               countIf(status NOT IN ('DELIVERED','SUCCESS')) fallan
        FROM fact_deployment_status
        WHERE company_id = {cia} AND timestamps_eta >= today() - 7
        GROUP BY poll_id HAVING enviados >= 20 AND fallan >= 5
        ORDER BY fallan / enviados DESC LIMIT 3
    """)
    acumulan = _query_interna(f"""
        SELECT countIf(veces >= 3) tres_o_mas, countIf(veces >= 2) dos_o_mas, count() con_algun_fallo
        FROM (SELECT treble_id, count() veces FROM fact_deployment_status
              WHERE company_id = {cia} AND timestamps_eta >= today() - 7
                AND status NOT IN ('DELIVERED','SUCCESS')
              GROUP BY treble_id)
        LIMIT 1
    """)

    porcausa = {}
    for f in detalle:
        st = f["status"]
        d = porcausa.setdefault(st, {"status": st, "c": 0, "pushes": []})
        d["c"] += int(f["c"])
        d["pushes"].append({"poll_id": f["pid"], "casos": int(f["c"])})
    causas = sorted(porcausa.values(), key=lambda d: -d["c"])[:8]
    return dias, causas, peor, (acumulan or [{}])[0]


def _salud_armar():
    dias, causas, peor, acum = _salud_datos()
    if not dias:
        return None
    ayer = dias[1] if len(dias) > 1 else dias[0]
    previos = dias[2:9] if len(dias) > 2 else []
    env_ayer = int(ayer.get("enviados") or 0)
    mal_ayer = int(ayer.get("no_llegaron") or 0)
    pct_ayer = round(100 * mal_ayer / env_ayer, 1) if env_ayer else 0.0

    tot_e = sum(int(d.get("enviados") or 0) for d in previos)
    tot_m = sum(int(d.get("no_llegaron") or 0) for d in previos)
    pct_prev = round(100 * tot_m / tot_e, 1) if tot_e else 0.0
    delta = round(pct_ayer - pct_prev, 1)
    nombres = _salud_nombres_push()

    return {
        "fecha": str(ayer.get("dia")),
        "enviados": env_ayer, "no_llegaron": mal_ayer, "pct": pct_ayer,
        "pct_promedio_7d": pct_prev, "diferencia_puntos": delta,
        "anomalo": delta >= SALUD_UMBRAL_ALERTA,
        "causas": [{
            "status": c["status"], "casos": c["c"],
            "que_paso": CAUSAS.get(c["status"], ("sin clasificar", "revisar"))[0],
            "que_hacer": CAUSAS.get(c["status"], ("sin clasificar", "revisar"))[1],
            "pushes": [{"poll_id": p["poll_id"], "push": _salud_push_etiqueta(p["poll_id"], nombres),
                        "casos": p["casos"]} for p in c["pushes"]],
        } for c in causas],
        "pushes_mas_afectados": [
            {"conversation_id": str(p["poll_id"]), "push": _salud_push_etiqueta(p["poll_id"], nombres),
             "enviados": int(p["enviados"]), "no_llegaron": int(p["fallan"]),
             "pct": round(100 * int(p["fallan"]) / int(p["enviados"]), 1)} for p in peor],
        "clientes_con_3_o_mas_sin_recibir": int(acum.get("tres_o_mas") or 0),
        "clientes_con_2_o_mas_sin_recibir": int(acum.get("dos_o_mas") or 0),
        "clientes_con_algun_fallo_7d": int(acum.get("con_algun_fallo") or 0),
    }


def _salud_texto(r):
    """
    El mensaje de Slack. Ahora cada causa abre los pushes que la
    concentran, que es lo que permite hacer algo con el número.
    """
    if r["anomalo"]:
        cab = (f":red_circle: *Salud de mensajería · {r['fecha']}*\n"
               f"Ayer salieron {r['enviados']:,} mensajes y *no llegaron {r['no_llegaron']}* "
               f"({r['pct']}%). Son {r['diferencia_puntos']} puntos peor que la semana "
               f"({r['pct_promedio_7d']}% de promedio).")
    else:
        cab = (f":white_check_mark: *Salud de mensajería · {r['fecha']}*\n"
               f"Ayer salieron {r['enviados']:,} mensajes y no llegaron {r['no_llegaron']} "
               f"({r['pct']}%). En línea con la semana ({r['pct_promedio_7d']}%).")
    cab = cab.replace(",", ".")

    partes = [cab]
    if r["causas"]:
        lineas = []
        for c in r["causas"]:
            lineas.append(f"  • *{c['casos']}* — {c['que_paso']} _({c['que_hacer']})_")
            top = c.get("pushes") or []
            if top:
                muestra = " · ".join(f"{p['push']} ({p['casos']})"
                                     for p in top[:SALUD_PUSHES_POR_CAUSA])
                resto = len(top) - SALUD_PUSHES_POR_CAUSA
                if resto > 0:
                    muestra += f" · y {resto} push{'es' if resto > 1 else ''} más"
                lineas.append(f"       ↳ {muestra}")
        partes.append("*Por qué no llegaron:*\n" + "\n".join(lineas))
    if r["pushes_mas_afectados"]:
        p = r["pushes_mas_afectados"][0]
        partes.append(f"*Push más afectado esta semana:* {p['push']} — "
                      f"no llega el {p['pct']}% de sus envíos ({p['no_llegaron']} de {p['enviados']}).")
    partes.append(
        f"*Clientes acumulando fallos (7 días):* {r['clientes_con_3_o_mas_sin_recibir']} llevan 3 o más "
        f"mensajes sin recibir, {r['clientes_con_2_o_mas_sin_recibir']} llevan 2 o más, "
        f"{r['clientes_con_algun_fallo_7d']} tuvieron al menos uno.")
    return "\n\n".join(partes)


@app.get("/salud/detalle")
def salud_detalle(x_api_key: str | None = Header(default=None),
                  dias: int = 7, minimo: int = 2, tope: int = 500):
    """
    Lo que el parte no entra a decir: quiénes son.
    Devuelve el desglose completo causa×push del último día cerrado y la
    lista de clientes con `minimo` o más mensajes sin recibir, ya cruzada
    con su ficha de HubSpot para poder ir a buscarlos.
    """
    _chequear_clave(x_api_key)
    cia = int(SALUD_COMPANY_ID)
    d = max(1, min(int(dias), 30))
    m = max(1, int(minimo))
    nombres = _salud_nombres_push()

    detalle = _query_interna(f"""
        SELECT status, toString(poll_id) pid, count() c
        FROM fact_deployment_status
        WHERE company_id = {cia} AND toDate(timestamps_eta) = today() - 1
          AND status NOT IN ('DELIVERED','SUCCESS')
        GROUP BY status, pid ORDER BY status, c DESC
    """) or []

    clientes = _query_interna(f"""
    WITH c AS (
      SELECT contact_wa_id wa, any(helpdesk_contact_id) hs
      FROM fact_conversations WHERE helpdesk_contact_id != '' GROUP BY wa
    )
    SELECT f.treble_id tid, any(c.hs) hubspot_id, count() fallos,
           topK(1)(f.status) causa_principal,
           groupUniqArray(toString(f.poll_id)) polls,
           max(f.timestamps_eta) ultimo
    FROM (SELECT treble_id, status, poll_id, timestamps_eta
          FROM fact_deployment_status
          WHERE company_id = {cia} AND timestamps_eta >= now() - INTERVAL {d} DAY
            AND status NOT IN ('DELIVERED','SUCCESS')) f
    LEFT JOIN c ON f.treble_id = c.wa
    GROUP BY tid HAVING fallos >= {m}
    ORDER BY fallos DESC LIMIT {max(1, min(int(tope), 2000))}
    """) or []

    def _causa(st):
        return CAUSAS.get(st, ("sin clasificar", "revisar"))

    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "ventana_clientes_dias": d,
        "por_causa_y_push": [{
            "status": f["status"], "que_paso": _causa(f["status"])[0],
            "que_hacer": _causa(f["status"])[1],
            "poll_id": f["pid"], "push": _salud_push_etiqueta(f["pid"], nombres),
            "casos": int(f["c"]),
        } for f in detalle],
        "clientes": [{
            "hubspot_id": str(f.get("hubspot_id") or "") or None,
            "ficha": (f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-1/{f['hubspot_id']}"
                      if str(f.get("hubspot_id") or "").isdigit() and HUBSPOT_PORTAL_ID else None),
            "telefono": str(f.get("tid") or ""),
            "mensajes_sin_recibir": int(f["fallos"]),
            "causa_principal": (list(f["causa_principal"]) or [""])[0],
            "que_paso": _causa((list(f["causa_principal"]) or [""])[0])[0],
            "pushes": [_salud_push_etiqueta(p, nombres) for p in (f.get("polls") or [])],
            "ultimo_fallo": str(f.get("ultimo") or "")[:19],
        } for f in clientes],
        "nota": ("El desglose por causa y push corresponde al último día cerrado, igual que el parte. "
                 "La lista de clientes cubre la ventana pedida en ?dias= (7 por defecto). "
                 "Un cliente sin hubspot_id es un número que no está asociado a ningún contacto del CRM."),
    }


# ══════════════════════════════════════════════════════════════════
#  v1.4.2 · TRES ARREGLOS QUE SALIERON DE ERRORES REALES
#  Agregado 10/09/2026. BLOQUE PURAMENTE ADITIVO.
#
#  ── 1. El parte decía una mentira ─────────────────────────────────
#  El texto afirmaba "el reintento automático los recupera". No existe
#  tal reintento automático: el endpoint está, pero nunca quedó nada
#  ejecutándolo. Iva lo preguntó y ahí se descubrió. Durante días el
#  parte tranquilizó al equipo sobre ~50 mensajes diarios que nadie
#  recuperaba. Ahora el texto dice la verdad y muestra cuántos hay
#  pendientes de verdad.
#
#  ── 2. El contador se rompía al republicar un flujo ───────────────
#  Los cuatro pushes de sesión estaban clavados por poll_id. Treble
#  cambia el poll_id en cada publicación del flujo Y reescribe el
#  histórico con el id nuevo, así que republicar "Cuarta sesión sí
#  asistió" habría borrado de golpe todo el conteo de cuartas sesiones.
#  Ahora los ids se resuelven por NOMBRE del push contra el DWH, con
#  los ids conocidos como respaldo. Se toma la unión de ambos: si el
#  nombre cambia sirven los ids, si los ids cambian sirve el nombre.
#
#  ── 3. El cruce con HubSpot marcaba clientes como inexistentes ────
#  Se cruzaba por `helpdesk_contact_id` de fact_conversations, que solo
#  existe si hubo una conversación con el enlace guardado. Iva marcó que
#  era raro que hubiera clientes sin ficha: de 32 revisados a mano, 29
#  sí tenían. Ahora se cruza por teléfono contra HubSpot, normalizando
#  ambos lados, con el cruce viejo como respaldo.
# ══════════════════════════════════════════════════════════════════

# Nombre del push -> número de sesión. Es la fuente de verdad nueva.
# Los poll_id de abajo quedan como red de seguridad, no como definición.
PUSHES_ASISTIO_NOMBRE = {
    "Primera sesión sí asistió": 1,
    "Segunda Sesión - Sí asistió": 2,
    "Tercera sesión sí asistió": 3,
    "Cuarta sesión sí asistió": 4,
}
PUSHES_NO_ASISTIO_NOMBRE = {
    "Inasistencia Primera sesión": 1,
    "Inasistencia 2, 3, o 4ta sesión": 0,
    "Inasistencia 2, 3 o 4ta sesión con AR": 0,
}
_CACHE_POLLS_SESION = {"asistio": None, "no_asistio": None, "ts": 0.0}


def _norm_push(s):
    """Compara nombres de push ignorando tildes, mayúsculas y espacios de más."""
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return " ".join(s.lower().split())


def _polls_de_sesion():
    """
    Resuelve los poll_id vigentes de los pushes de sesión buscándolos por
    nombre en el DWH. Devuelve (set_asistio, set_no_asistio) como strings.
    Cacheado media hora. Si el DWH falla, caen los ids clavados de v1.3.6,
    que es exactamente el comportamiento anterior: nunca queda peor.
    """
    ahora = time.time()
    if _CACHE_POLLS_SESION["asistio"] is not None and (ahora - _CACHE_POLLS_SESION["ts"]) < 1800:
        return _CACHE_POLLS_SESION["asistio"], _CACHE_POLLS_SESION["no_asistio"]

    asistio = set(PUSHES_ASISTIO)          # red de seguridad: los ids de siempre
    no_asistio = set(PUSHES_NO_ASISTIO)
    try:
        filas = _query_interna("""
            SELECT toString(poll_id) pid, argMax(poll_name, timestamps_eta) nombre
            FROM fact_deployment_status
            WHERE poll_name != '' AND timestamps_eta >= now() - INTERVAL 365 DAY
            GROUP BY pid
        """) or []
        buscados_si = {_norm_push(k) for k in PUSHES_ASISTIO_NOMBRE}
        buscados_no = {_norm_push(k) for k in PUSHES_NO_ASISTIO_NOMBRE}
        for f in filas:
            n = _norm_push(f.get("nombre"))
            if n in buscados_si:
                asistio.add(str(f["pid"]))
            elif n in buscados_no:
                no_asistio.add(str(f["pid"]))
    except Exception as e:
        log.error(f"[sesiones] no se pudieron resolver los polls por nombre, uso los fijos: {e}")

    _CACHE_POLLS_SESION.update({"asistio": asistio, "no_asistio": no_asistio, "ts": ahora})
    log.warning(f"[sesiones] polls resueltos · asistio={len(asistio)} no_asistio={len(no_asistio)}")
    return asistio, no_asistio


# ── Mapa teléfono -> contacto de HubSpot ──────────────────────────
# Se arma UNA vez y sirve para el contador, el parte de salud y la
# auditoría. Es la pieza que reemplaza el cruce por conversaciones.
_CACHE_MAPA_TEL = {"datos": None, "ts": 0.0}
AUDITORIA_DIAS_ACTIVO = int(os.environ.get("AUDITORIA_DIAS_ACTIVO", "35"))
# COHORTE_MAX_CONTACTOS son 2.000 y los clientes activos son más de 4.000:
# con el tope por defecto la auditoría se quedaría con la mitad del padrón.
AUDITORIA_MAX_CONTACTOS = int(os.environ.get("AUDITORIA_MAX_CONTACTOS", "12000"))


def _solo_digitos(t):
    return re.sub(r"\D", "", str(t or ""))


def _contactos_activos_hubspot(props=None):
    """
    Todos los clientes activos: lifecyclestage=customer con pago en los
    últimos AUDITORIA_DIAS_ACTIVO días. Una sola pasada paginada.
    """
    corte = _dia_ms(datetime.now(timezone.utc).date() - timedelta(days=AUDITORIA_DIAS_ACTIVO))
    pedidas = ["hs_object_id", "hs_whatsapp_phone_number", "phone", "mobilephone",
               "email", "firstname", "lastname", PROP_SES_ETAPA, "fecha_compra",
               "sesiones_plan", "fecha_ultimo_pago"]
    if props:
        pedidas = sorted(set(pedidas) | set(props))
    return _hs_buscar_todo("contacts", {
        "filterGroups": [{"filters": [
            {"propertyName": "lifecyclestage", "operator": "EQ", "value": "customer"},
            {"propertyName": "fecha_ultimo_pago", "operator": "GTE", "value": str(corte)},
        ]}],
        "properties": pedidas,
    }, tope=AUDITORIA_MAX_CONTACTOS)


def _mapa_telefono_hubspot(forzar=False):
    """
    { solo_dígitos_del_teléfono : hubspot_id } para todos los activos.
    Indexa los tres campos de teléfono, porque el número de WhatsApp real
    aparece tanto en `hs_whatsapp_phone_number` como en `mobilephone`
    según cómo se haya cargado el contacto. Cacheado una hora.
    """
    ahora = time.time()
    if not forzar and _CACHE_MAPA_TEL["datos"] is not None and (ahora - _CACHE_MAPA_TEL["ts"]) < 3600:
        return _CACHE_MAPA_TEL["datos"]
    mapa = {}
    for c in _contactos_activos_hubspot():
        hs = str(c.get("id") or "")
        p = c.get("properties") or {}
        for campo in ("hs_whatsapp_phone_number", "mobilephone", "phone"):
            d = _solo_digitos(p.get(campo))
            if len(d) >= 8:
                mapa.setdefault(d, hs)
    _CACHE_MAPA_TEL.update({"datos": mapa, "ts": ahora})
    log.warning(f"[mapa] teléfonos indexados: {len(mapa)}")
    return mapa


def _resolver_hubspot(tid, hs_conversaciones, mapa):
    """
    El id de HubSpot de un número de Treble. Primero lo que ya venía del
    cruce por conversaciones; si no hay, se busca por teléfono.
    """
    hs = str(hs_conversaciones or "").strip()
    if hs.isdigit():
        return hs, "conversacion"
    d = _solo_digitos(tid)
    for cand in (d, d[1:] if len(d) > 10 else None):
        if cand and cand in mapa:
            return mapa[cand], "telefono"
    return None, "no_encontrado"


# ══════════════════════════════════════════════════════════════════
#  ARREGLO 2 · El contador deja de depender de los poll_id fijos
# ══════════════════════════════════════════════════════════════════
def _sesiones_desde_dwh(dias=None):
    """
    Igual que la versión de v1.3.6 en su lógica de conteo — un push
    repetido el mismo día es un reintento, no otra sesión — pero con dos
    diferencias que importan:
      · los poll_id se resuelven por nombre (ver _polls_de_sesion)
      · si el cruce por conversaciones no encuentra la ficha, se busca
        por teléfono en vez de descartar al cliente
    """
    a_set, n_set = _polls_de_sesion()
    asistio = ",".join(f"'{k}'" for k in sorted(a_set))
    no_asistio = ",".join(f"'{k}'" for k in sorted(n_set))
    corte = f"AND timestamps_eta >= now() - INTERVAL {int(dias)} DAY" if dias else ""
    sql = f"""
    WITH d AS (
      SELECT treble_id tid, toString(poll_id) pid, timestamps_eta ts
      FROM fact_deployment_status
      WHERE toString(poll_id) IN ({asistio},{no_asistio}) {corte}
    ),
    c AS (
      SELECT contact_wa_id wa, any(helpdesk_contact_id) hs
      FROM fact_conversations WHERE helpdesk_contact_id != '' GROUP BY wa
    )
    SELECT d.tid tid, any(c.hs) hubspot_id,
           uniqExactIf(concat(d.pid, '|', toString(toDate(d.ts))), d.pid IN ({asistio})) asistidas,
           uniqExactIf(concat(d.pid, '|', toString(toDate(d.ts))), d.pid IN ({no_asistio})) inasistencias,
           toDate(max(d.ts)) ultima
    FROM d LEFT JOIN c ON d.tid = c.wa
    GROUP BY tid
    ORDER BY ultima DESC
    LIMIT {SESIONES_MAX_CONTACTOS}
    """
    filas = _query_interna(sql) or []

    # El HAVING hubspot_id != '' de la versión anterior descartaba en el SQL
    # a todo el que no tuviera conversación enlazada. Ahora se rescatan acá.
    try:
        mapa = _mapa_telefono_hubspot()
    except Exception as e:
        log.error(f"[sesiones] sin mapa de teléfonos, uso solo el cruce viejo: {e}")
        mapa = {}
    rescatados = 0
    salida = []
    for f in filas:
        hs, via = _resolver_hubspot(f.get("tid"), f.get("hubspot_id"), mapa)
        if not hs:
            continue
        if via == "telefono":
            rescatados += 1
        f["hubspot_id"] = hs
        salida.append(f)
    if rescatados:
        log.warning(f"[sesiones] {rescatados} clientes rescatados por teléfono que antes se perdían")
    return salida


@app.get("/sesiones/polls")
def sesiones_polls(x_api_key: str | None = Header(default=None), refrescar: str | None = None):
    """
    Qué poll_id está usando hoy el contador y de dónde salió cada uno.
    Sirve para verificar, después de republicar un flujo en Treble, que el
    contador siguió al push nuevo en vez de quedarse con el id viejo.
    """
    _chequear_clave(x_api_key)
    if _a_bool(refrescar, por_defecto=False):
        _CACHE_POLLS_SESION.update({"asistio": None, "no_asistio": None, "ts": 0.0})
    a_set, n_set = _polls_de_sesion()
    nombres = _salud_nombres_push()
    def _detalle(ids, fijos):
        return sorted(({"poll_id": p, "push": nombres.get(p, f"conversación {p}"),
                        "origen": "id fijo de respaldo" if p in fijos else "resuelto por nombre"}
                       for p in ids), key=lambda d: d["push"])
    return {
        "asistio": _detalle(a_set, set(PUSHES_ASISTIO)),
        "no_asistio": _detalle(n_set, set(PUSHES_NO_ASISTIO)),
        "nota": ("Los poll_id cambian cada vez que se publica el flujo en Treble, y el histórico se "
                 "reescribe con el id nuevo. Por eso se resuelven por nombre y los ids fijos quedan "
                 "solo como respaldo. Si acá falta un push que sí existe, revisá que su nombre en "
                 "Treble coincida con el esperado."),
        "nombres_esperados": {"asistio": list(PUSHES_ASISTIO_NOMBRE),
                              "no_asistio": list(PUSHES_NO_ASISTIO_NOMBRE)},
    }


# ══════════════════════════════════════════════════════════════════
#  ARREGLO 1 · El parte deja de prometer un reintento que no corre
# ══════════════════════════════════════════════════════════════════
# La frase que había que corregir vivía acá: cada línea de causa cerraba con
# "(el reintento automático los recupera)". Se cambia el diccionario global,
# así queda arreglado tanto en el parte de Slack como en /salud/detalle.
CAUSAS["FAILURE_BY_HUMAN_HANDOVER"] = (
    "el cliente tenía un chat abierto en Treble",
    "recuperable, pero el reenvío hay que ejecutarlo a mano",
)


def _salud_texto(r):
    """
    Igual que v1.4.1 — cada causa abre sus pushes — con una corrección
    importante: ya no afirma que el reintento es automático, porque no lo
    es. En su lugar informa cuántos hay esperando de verdad.
    """
    if r["anomalo"]:
        cab = (f":red_circle: *Salud de mensajería · {r['fecha']}*\n"
               f"Ayer salieron {r['enviados']:,} mensajes y *no llegaron {r['no_llegaron']}* "
               f"({r['pct']}%). Son {r['diferencia_puntos']} puntos peor que la semana "
               f"({r['pct_promedio_7d']}% de promedio).")
    else:
        cab = (f":white_check_mark: *Salud de mensajería · {r['fecha']}*\n"
               f"Ayer salieron {r['enviados']:,} mensajes y no llegaron {r['no_llegaron']} "
               f"({r['pct']}%). En línea con la semana ({r['pct_promedio_7d']}%).")
    cab = cab.replace(",", ".")

    partes = [cab]
    if r["causas"]:
        lineas = []
        for c in r["causas"]:
            lineas.append(f"  • *{c['casos']}* — {c['que_paso']} _({c['que_hacer']})_")
            top = c.get("pushes") or []
            if top:
                muestra = " · ".join(f"{p['push']} ({p['casos']})"
                                     for p in top[:SALUD_PUSHES_POR_CAUSA])
                resto = len(top) - SALUD_PUSHES_POR_CAUSA
                if resto > 0:
                    muestra += f" · y {resto} push{'es' if resto > 1 else ''} más"
                lineas.append(f"       ↳ {muestra}")
        partes.append("*Por qué no llegaron:*\n" + "\n".join(lineas))

    pend, recup = r.get("reintento_pendientes"), r.get("reintento_recuperables")
    if pend is not None:
        if recup is not None and recup > 0:
            partes.append(f":warning: *Reintento:* hay *{pend}* pushes bloqueados esperando, "
                          f"de los cuales *{recup}* se pueden reenviar. El reenvío NO es automático: "
                          f"alguien tiene que ejecutarlo.")
        elif pend > 0:
            partes.append(f":warning: *Reintento:* hay *{pend}* pushes bloqueados esperando y ninguno "
                          f"es reenviable todavía — sus campañas no están dadas de alta en el sistema.")

    if r["pushes_mas_afectados"]:
        p = r["pushes_mas_afectados"][0]
        partes.append(f"*Push más afectado esta semana:* {p['push']} — "
                      f"no llega el {p['pct']}% de sus envíos ({p['no_llegaron']} de {p['enviados']}).")
    partes.append(
        f"*Clientes acumulando fallos (7 días):* {r['clientes_con_3_o_mas_sin_recibir']} llevan 3 o más "
        f"mensajes sin recibir, {r['clientes_con_2_o_mas_sin_recibir']} llevan 2 o más, "
        f"{r['clientes_con_algun_fallo_7d']} tuvieron al menos uno.")
    return "\n\n".join(partes)


_salud_armar_v141 = _salud_armar


def _salud_armar():
    """Lo de v1.4.1 más el estado real de la cola de reintento."""
    r = _salud_armar_v141()
    if not r:
        return r
    try:
        d = _query_interna(f"""
            SELECT count() pendientes FROM fact_deployment_status
            WHERE company_id = {int(SALUD_COMPANY_ID)}
              AND status = 'FAILURE_BY_HUMAN_HANDOVER'
              AND timestamps_eta >= now() - INTERVAL 72 HOUR
        """)
        r["reintento_pendientes"] = int((d or [{}])[0].get("pendientes") or 0)
        r["reintento_recuperables"] = None
        r["reintento_automatico"] = False
    except Exception as e:
        log.error(f"[salud] no se pudo contar la cola de reintento: {e}")
    return r


# ══════════════════════════════════════════════════════════════════
#  ARREGLO 3 · /salud/detalle deja de inventar clientes "sin ficha"
# ══════════════════════════════════════════════════════════════════
_salud_detalle_v141 = salud_detalle


@app.get("/salud/detalle-v2")
def salud_detalle_v2(x_api_key: str | None = Header(default=None),
                     dias: int = 7, minimo: int = 2, tope: int = 500):
    """
    Lo mismo que /salud/detalle pero resolviendo la ficha por teléfono
    cuando el cruce por conversaciones no la encuentra. En la revisión a
    mano del 09/09, de 32 clientes marcados como "sin ficha" 29 sí la
    tenían: el que fallaba era el cruce, no el dato.
    """
    _chequear_clave(x_api_key)
    base = _salud_detalle_v141(x_api_key=x_api_key, dias=dias, minimo=minimo, tope=tope)
    try:
        mapa = _mapa_telefono_hubspot()
    except Exception as e:
        base["aviso"] = f"No se pudo construir el mapa de teléfonos: {e}"
        return base

    rescatados = 0
    for c in base.get("clientes", []):
        if c.get("hubspot_id"):
            continue
        hs, via = _resolver_hubspot(c.get("telefono"), None, mapa)
        if hs:
            c["hubspot_id"] = hs
            c["ficha"] = f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-1/{hs}"
            c["cruce"] = "por teléfono"
            rescatados += 1
    total = len(base.get("clientes", []))
    sin_ficha = sum(1 for c in base.get("clientes", []) if not c.get("hubspot_id"))
    base["cruce"] = {
        "rescatados_por_telefono": rescatados,
        "siguen_sin_ficha": sin_ficha,
        "total": total,
        "nota": ("Un cliente que sigue sin ficha después de buscar por teléfono es un número que "
                 "de verdad no está asociado a ningún contacto activo del CRM."),
    }
    return base


# ══════════════════════════════════════════════════════════════════
#  NUEVO · AUDITORÍA DE CONTACTOS ACTIVOS
#  Lo que el 09/09 hubo que hacer a mano: una pasada completa sobre los
#  clientes activos revisando formato de teléfono, campos faltantes y
#  duplicados. La API de búsqueda de HubSpot no permite filtrar por
#  formato, así que hay que traer el universo y revisarlo acá.
# ══════════════════════════════════════════════════════════════════
def _revisar_telefono(valor):
    """
    Devuelve (problema, sugerencia) o (None, None) si está bien.
    E.164: un '+' seguido de 8 a 15 dígitos, sin espacios ni separadores.
    """
    t = str(valor or "").strip()
    if not t:
        return "vacío", None
    d = _solo_digitos(t)
    if not t.startswith("+"):
        return "sin prefijo internacional", None
    if t != "+" + d:
        return "tiene espacios o separadores", "+" + d
    if len(d) < 8:
        return "demasiado corto", None
    if len(d) > 15:
        return "demasiado largo", None
    if re.fullmatch(r"1\d{11}", d):
        return "posible prefijo +1 duplicado", "+" + d[1:]
    for cc in ("58", "57", "52", "51", "34", "56", "54"):
        if d.startswith(cc + cc):
            return f"posible prefijo +{cc} duplicado", "+" + d[len(cc):]
    return None, None


@app.get("/auditoria/contactos")
def auditoria_contactos(x_api_key: str | None = Header(default=None), tope_detalle: int = 300):
    """
    Auditoría completa de los clientes activos. Solo lectura: no corrige
    nada, devuelve lo que hay que corregir con el link a cada ficha.
    """
    _chequear_clave(x_api_key)
    contactos = _contactos_activos_hubspot()
    if not contactos:
        raise HTTPException(503, "No se pudieron traer los contactos activos de HubSpot.")

    def link(hs):
        return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-1/{hs}"

    hallazgos = {k: [] for k in (
        "sin_whatsapp_pero_con_movil", "sin_ningun_telefono", "telefono_mal_formado",
        "whatsapp_distinto_de_movil", "sin_email", "sin_etapa", "sin_fecha_compra",
        "sin_sesiones_plan")}
    por_telefono, por_email = {}, {}

    for c in contactos:
        hs = str(c.get("id") or "")
        p = c.get("properties") or {}
        nombre = (f"{p.get('firstname') or ''} {p.get('lastname') or ''}").strip() or p.get("email") or hs
        wa, mov, tel = p.get("hs_whatsapp_phone_number"), p.get("mobilephone"), p.get("phone")
        base = {"hubspot_id": hs, "cliente": nombre, "ficha": link(hs)}

        if not str(wa or "").strip():
            otro = next((x for x in (mov, tel) if str(x or "").strip()), None)
            (hallazgos["sin_whatsapp_pero_con_movil"] if otro else hallazgos["sin_ningun_telefono"]).append(
                dict(base, telefono_alternativo=otro))
        else:
            problema, sugerencia = _revisar_telefono(wa)
            if problema:
                hallazgos["telefono_mal_formado"].append(
                    dict(base, campo="Número de WhatsApp", valor=wa,
                         problema=problema, sugerencia=sugerencia))
            dw, dm = _solo_digitos(wa), _solo_digitos(mov)
            if dm and dw and dm != dw and not (dw.endswith(dm) or dm.endswith(dw)):
                hallazgos["whatsapp_distinto_de_movil"].append(
                    dict(base, whatsapp=wa, movil=mov))

        for campo, clave in ((wa, "telefono"), (mov, "telefono")):
            d = _solo_digitos(campo)
            if len(d) >= 8:
                por_telefono.setdefault(d, set()).add(hs)
        em = str(p.get("email") or "").strip().lower()
        if em:
            por_email.setdefault(em, set()).add(hs)
        else:
            hallazgos["sin_email"].append(base)

        if not str(p.get(PROP_SES_ETAPA) or "").strip():
            hallazgos["sin_etapa"].append(base)
        if not str(p.get("fecha_compra") or "").strip():
            hallazgos["sin_fecha_compra"].append(base)
        if not str(p.get("sesiones_plan") or "").strip():
            hallazgos["sin_sesiones_plan"].append(base)

    dup_tel = [{"telefono": _mask_phone(t), "fichas": sorted(ids), "enlaces": [link(i) for i in sorted(ids)]}
               for t, ids in por_telefono.items() if len(ids) > 1]
    dup_mail = [{"email": e, "fichas": sorted(ids), "enlaces": [link(i) for i in sorted(ids)]}
                for e, ids in por_email.items() if len(ids) > 1]

    SEV = {"sin_ningun_telefono": "critico", "sin_whatsapp_pero_con_movil": "critico",
           "telefono_mal_formado": "alto", "whatsapp_distinto_de_movil": "alto",
           "sin_etapa": "alto", "sin_sesiones_plan": "medio",
           "sin_fecha_compra": "medio", "sin_email": "bajo"}
    tope = max(1, min(int(tope_detalle), 2000))

    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "universo": {
            "clientes_activos": len(contactos),
            "criterio": f"lifecyclestage=customer y pago en los últimos {AUDITORIA_DIAS_ACTIVO} días",
        },
        "resumen": [{"hallazgo": k, "contactos": len(v), "severidad": SEV.get(k, "medio"),
                     "pct": round(100 * len(v) / len(contactos), 1)}
                    for k, v in sorted(hallazgos.items(), key=lambda kv: -len(kv[1]))]
                   + [{"hallazgo": "telefono_duplicado_entre_fichas", "contactos": len(dup_tel),
                       "severidad": "critico", "pct": round(100 * len(dup_tel) / len(contactos), 1)},
                      {"hallazgo": "email_duplicado_entre_fichas", "contactos": len(dup_mail),
                       "severidad": "alto", "pct": round(100 * len(dup_mail) / len(contactos), 1)}],
        "detalle": {k: v[:tope] for k, v in hallazgos.items()},
        "duplicados": {"por_telefono": dup_tel[:tope], "por_email": dup_mail[:tope]},
        "nota": ("Solo lectura: no se corrigió nada. 'sin_whatsapp_pero_con_movil' es el más rentable "
                 "de arreglar — el dato ya está en la ficha, solo está en el campo equivocado, y hasta "
                 "que se copie el cliente no recibe ningún push."),
    }


# ══════════════════════════════════════════════════════════════════
#  EL ARREGLO DE VERDAD · QUE EL REINTENTO CORRA SOLO
#
#  Hasta hoy el reintento existía como endpoint y nadie lo ejecutaba:
#  ~50 mensajes por día quedaban bloqueados y ahí morían. Corregir el
#  texto del parte habría sido describir mejor el problema; esto lo
#  resuelve.
#
#  ── Salvaguardas, porque son mensajes a clientes reales ───────────
#  · Solo reenvía lo que YA se puede: contactos cuya conversación en
#    Treble está cerrada. Si sigue abierta, se espera a la vuelta
#    siguiente en vez de forzar.
#  · Nunca reenvía dos veces el mismo push: el candado va contra la
#    tabla de eventos, no contra memoria, así que sobrevive a un
#    reinicio de Render y a que haya más de una instancia.
#  · Tope por corrida (REINTENTO_MAX_POR_CORRIDA).
#  · Solo dentro de una franja horaria razonable: a nadie le sirve
#    recibir un recordatorio a las 4 de la mañana.
#  · Se apaga con REINTENTO_AUTOMATICO=false sin tocar código.
#  · Cada corrida queda en el log con cuántos salieron.
# ══════════════════════════════════════════════════════════════════
REINTENTO_AUTOMATICO = os.environ.get("REINTENTO_AUTOMATICO", "true").strip().lower() not in ("false", "0", "no")
REINTENTO_CADA_MINUTOS = int(os.environ.get("REINTENTO_CADA_MINUTOS", "60"))
REINTENTO_HORA_DESDE = int(os.environ.get("REINTENTO_HORA_DESDE", "12"))  # UTC
REINTENTO_HORA_HASTA = int(os.environ.get("REINTENTO_HORA_HASTA", "23"))  # UTC

for _m in ("reintentos_automaticos", "corridas_reintento"):
    METRICAS.setdefault(_m, 0)


def _reintento_en_horario(ahora=None):
    """
    Los clientes están mayormente en América. La franja por defecto va de
    las 12:00 a las 23:00 UTC, que son las 8 de la mañana a las 7 de la
    tarde en Colombia y las 9 a 20 en Venezuela.
    """
    h = (ahora or datetime.now(timezone.utc)).hour
    return REINTENTO_HORA_DESDE <= h <= REINTENTO_HORA_HASTA


def _reintento_corrida():
    """Una vuelta del reintento. Devuelve el resultado del endpoint."""
    return pushes_reintentar(x_api_key=API_KEY, aplicar="true")


def _reintento_monitor_loop():
    while True:
        try:
            ahora = datetime.now(timezone.utc)
            if _reintento_en_horario(ahora):
                marca = ahora.strftime("%Y-%m-%dT%H")
                # La deduplicación por hora evita que dos instancias de Render
                # disparen la misma corrida en paralelo.
                if not _evento_ya_notificado("corrida_reintento", marca):
                    _evento_marcar("corrida_reintento", marca, "notified", notified=True)
                    r = _reintento_corrida()
                    n = int(r.get("reintentados") or 0)
                    METRICAS["corridas_reintento"] += 1
                    METRICAS["reintentos_automaticos"] += n
                    om = r.get("omitidos") or {}
                    log.warning(
                        f"[reintento-auto] reenviados={n} pendientes={r.get('pendientes_totales')} "
                        f"sin_workflow={om.get('sin_workflow')} ya_hechos={om.get('ya_reintentado')}")
                    if om.get("sin_workflow"):
                        log.warning(
                            f"[reintento-auto] {om['sin_workflow']} quedaron fuera porque su campaña no "
                            f"está dada de alta. Ver GET /pushes/bloqueados → sin_workflow_asociado.")
        except Exception as e:
            log.error(f"[reintento-auto] fallo la corrida: {e}")
        time.sleep(max(60, REINTENTO_CADA_MINUTOS * 60))


@app.get("/pushes/reintento-estado")
def pushes_reintento_estado(x_api_key: str | None = Header(default=None)):
    """Si el reintento automático está corriendo y cuánto lleva hecho."""
    _chequear_clave(x_api_key)
    ahora = datetime.now(timezone.utc)
    return {
        "automatico_activo": REINTENTO_AUTOMATICO,
        "cada_minutos": REINTENTO_CADA_MINUTOS,
        "franja_utc": f"{REINTENTO_HORA_DESDE}:00 a {REINTENTO_HORA_HASTA}:59",
        "dentro_de_franja_ahora": _reintento_en_horario(ahora),
        "corridas": METRICAS.get("corridas_reintento", 0),
        "mensajes_reenviados": METRICAS.get("reintentos_automaticos", 0),
        "tope_por_corrida": REINTENTO_MAX_POR_CORRIDA,
        "nota": ("Los contadores se reinician cuando Render reinicia el proceso. "
                 "Para apagarlo: REINTENTO_AUTOMATICO=false."),
    }


@app.on_event("startup")
def arrancar_reintento_automatico():
    """
    Handler de arranque propio, como el del parte de salud: FastAPI corre
    todos los registrados, así que este convive con los anteriores sin
    tocarlos.
    """
    if not REINTENTO_AUTOMATICO:
        log.warning("[startup] reintento automático APAGADO por configuración")
        return
    threading.Thread(target=_reintento_monitor_loop, daemon=True).start()
    log.warning(f"[startup] reintento automático activo · cada {REINTENTO_CADA_MINUTOS} min · "
                f"franja {REINTENTO_HORA_DESDE}-{REINTENTO_HORA_HASTA} UTC · "
                f"tope {REINTENTO_MAX_POR_CORRIDA} por corrida")


# ══════════════════════════════════════════════════════════════════
#  COBERTURA · CLIENTES NUEVOS QUE TODAVÍA NO TIENEN SESIONES
#
#  ── El problema ───────────────────────────────────────────────────
#  De 4.265 clientes activos, 2.703 no tienen `sesiones_etapa` y por eso
#  caen en la rama por defecto (ATC). Angela lo notó por el lado de la
#  consecuencia: las gestoras casi no reciben nada. De 35 envíos del push
#  de 72 h, solo 11 fueron por consultoría.
#
#  ── Qué se puede afirmar con certeza y qué no ─────────────────────
#  Un cliente que compró hace menos de 35 días y no tiene NINGÚN cierre
#  de sesión registrado está, necesariamente, en sus primeras 4 sesiones:
#  o todavía no tuvo la primera, o la tuvo y el push de cierre falló. En
#  los dos casos su etapa correcta es `acompanamiento`. Son 288 clientes.
#
#  De los otros ~2.400 NO se puede afirmar lo mismo: compraron hace meses
#  y no registran sesiones, así que o están inactivos o su historial es
#  anterior al 16/06. Esos se quedan en ATC, que es el destino
#  conservador. Inventarles una etapa sería peor que no tenerla.
#
#  ── Por qué esto no pisa al contador ──────────────────────────────
#  `sesiones_sincronizar` solo escribe sobre clientes que aparecen en el
#  DWH. Estos no aparecen — justamente por eso no tienen etapa. En cuanto
#  tengan su primera sesión real, el contador toma el mando y este
#  respaldo deja de aplicarse solo.
# ══════════════════════════════════════════════════════════════════
COBERTURA_DIAS_NUEVO = int(os.environ.get("COBERTURA_DIAS_NUEVO", "35"))


def _clientes_nuevos_sin_etapa():
    """Activos, sin etapa, que compraron hace menos de COBERTURA_DIAS_NUEVO días."""
    corte = _dia_ms(datetime.now(timezone.utc).date() - timedelta(days=COBERTURA_DIAS_NUEVO))
    return _hs_buscar_todo("contacts", {
        "filterGroups": [{"filters": [
            {"propertyName": "lifecyclestage", "operator": "EQ", "value": "customer"},
            {"propertyName": "fecha_ultimo_pago", "operator": "GTE", "value": str(corte)},
            {"propertyName": PROP_SES_ETAPA, "operator": "NOT_HAS_PROPERTY"},
            {"propertyName": "fecha_compra", "operator": "GTE", "value": str(corte)},
        ]}],
        "properties": ["hs_object_id", "hs_full_name_or_email", "fecha_compra", "sesiones_plan"],
    }, tope=AUDITORIA_MAX_CONTACTOS)


@app.post("/sesiones/completar-nuevos")
def sesiones_completar_nuevos(x_api_key: str | None = Header(default=None), aplicar: str | None = None):
    """
    Marca como `acompanamiento` a los clientes recién comprados que aún no
    tienen ninguna sesión registrada. DRY-RUN por defecto.

    Es idempotente y seguro de repetir: solo toca fichas sin etapa, así que
    nunca sobreescribe un valor que haya calculado el contador.
    """
    _chequear_clave(x_api_key)
    escribir = _a_bool(aplicar, por_defecto=False)
    nuevos = _clientes_nuevos_sin_etapa()

    entradas = [{"id": c["id"], "properties": {
        PROP_SES_ETAPA: "acompanamiento",
        PROP_SES_ORIGEN: (f"Compró hace menos de {COBERTURA_DIAS_NUEVO} días y todavía no tiene "
                          "ninguna sesión registrada, así que está en sus primeras 4. Se recalcula "
                          "solo en cuanto tenga su primera sesión."),
    }} for c in nuevos]

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "ventana_dias": COBERTURA_DIAS_NUEVO,
        "a_marcar_acompanamiento": len(entradas),
        "muestra": [{"cliente": (c.get("properties") or {}).get("hs_full_name_or_email"),
                     "compro": (c.get("properties") or {}).get("fecha_compra"),
                     "plan": (c.get("properties") or {}).get("sesiones_plan")}
                    for c in nuevos[:8]],
        "razon": ("Sin cierre de sesión registrado y compra reciente: o no tuvo la primera sesión "
                  "todavía, o la tuvo y el push falló. En los dos casos está en sus primeras 4."),
    }
    if not escribir:
        resultado["aviso"] = "Simulación. Para aplicarlo: POST /sesiones/completar-nuevos?aplicar=true"
        return resultado

    escritos, errores = _hs_batch_update("contacts", entradas) if entradas else (0, [])
    resultado.update({"marcados": escritos, "errores": errores})
    log.warning(f"[cobertura] nuevos marcados como acompanamiento={escritos} errores={len(errores)}")
    return resultado


@app.get("/sesiones/cobertura")
def sesiones_cobertura(x_api_key: str | None = Header(default=None)):
    """
    Cuánto del padrón activo puede enrutar la bifurcación y cuánto cae en
    ATC solo por falta de dato. Es el número que hay que mirar cuando
    alguien pregunta por qué las gestoras reciben poco.
    """
    _chequear_clave(x_api_key)

    def _contar(filtros):
        try:
            return _hubspot_api("POST", "/crm/v3/objects/contacts/search",
                                {"filterGroups": [{"filters": filtros}],
                                 "properties": ["hs_object_id"], "limit": 1}).get("total", 0)
        except Exception:
            return None

    corte_pago = str(_dia_ms(datetime.now(timezone.utc).date() - timedelta(days=AUDITORIA_DIAS_ACTIVO)))
    base = [{"propertyName": "lifecyclestage", "operator": "EQ", "value": "customer"},
            {"propertyName": "fecha_ultimo_pago", "operator": "GTE", "value": corte_pago}]
    activos = _contar(base)
    acomp = _contar(base + [{"propertyName": PROP_SES_ETAPA, "operator": "EQ", "value": "acompanamiento"}])
    sop = _contar(base + [{"propertyName": PROP_SES_ETAPA, "operator": "EQ", "value": "soporte"}])
    sin = _contar(base + [{"propertyName": PROP_SES_ETAPA, "operator": "NOT_HAS_PROPERTY"}])

    def _pct(n):
        return round(100 * n / activos, 1) if activos and n is not None else None

    return {
        "clientes_activos": activos,
        "a_consultoria": {"clientes": acomp, "pct": _pct(acomp)},
        "a_atc_por_dato": {"clientes": sop, "pct": _pct(sop)},
        "a_atc_por_falta_de_dato": {"clientes": sin, "pct": _pct(sin)},
        "cobertura_del_dato": _pct((acomp or 0) + (sop or 0)),
        "nota": ("'a_atc_por_falta_de_dato' son clientes que van a ATC porque no sabemos en qué "
                 "sesión están, no porque les corresponda. Es el número a bajar. "
                 "POST /sesiones/completar-nuevos recupera la parte que se puede afirmar con certeza."),
    }



# ══════════════════════════════════════════════════════════════════
#  v1.4.3 · EL CONTADOR PASA A MEDIR SESIONES AGENDADAS
#  Agregado 10/09/2026. BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué ───────────────────────────────────────────────────────
#  Angela lo detectó sin ver una sola línea de código: "me parece muy
#  raro que desde el jueves solo haya 11 personas que estén por tener su
#  primera, segunda, tercera o cuarta sesión". Tenía razón. El sistema
#  veía 669 clientes en sus primeras 4 sesiones; el número real es 2.586.
#
#  ── Qué estaba mal ────────────────────────────────────────────────
#  El contador contaba los pushes de "sí asistió", que tienen tres
#  defectos que se suman:
#    1. Solo existen para las sesiones 1 a 4. De la quinta en adelante
#       no hay señal, así que un cliente con 20 sesiones se ve igual
#       que uno con 5.
#    2. Meta rechaza el de 4ta sesión el 53% de las veces: sesiones
#       reales que nunca quedan registradas.
#    3. Cubren 2.758 clientes de un padrón activo de 4.265.
#
#  ── La señal nueva ────────────────────────────────────────────────
#  El recordatorio que sale antes de CADA sesión agendada. Cubre 6.212
#  clientes y no tiene ninguno de los tres problemas.
#  Son dos pushes que se relevan sin solaparse (verificado: cero
#  coincidencias el mismo día para el mismo cliente):
#    · "Recordatorio Sesión en 28hs"          10/06/2026 → 03/09/2026
#    · "Especialista confirmación 6 horas antes"  03/09/2026 → hoy
#  Se usa el de 6 horas y no el de 3 días porque sale el MISMO día de la
#  sesión: así una fecha distinta es una sesión distinta.
#
#  ── El límite honesto ─────────────────────────────────────────────
#  Un recordatorio significa sesión AGENDADA, no necesariamente asistida.
#  Para decidir a qué equipo va la respuesta alcanza — mide dónde está el
#  cliente en su plan — pero no es lo mismo que "asistió". Las
#  inasistencias registradas se descuentan; las que no dejaron rastro, no.
#
#  ── Sesgo de veteranos, otra vez ──────────────────────────────────
#  El registro empieza el 10/06. Un cliente que compró antes tiene el
#  conteo truncado igual que con la señal vieja, así que se lo deja en
#  ATC. Es el mismo criterio conservador de v1.4.0.
# ══════════════════════════════════════════════════════════════════

PUSHES_AGENDADA_NOMBRE = (
    # "Recordatorio Sesión en 28hs" murió el 03/09/2026: Yopsi-Admin dejó de
    # inscribir en él y lo reemplazó por los dos de abajo. Se deja en la lista
    # porque el histórico del contador todavía se apoya en sus envíos.
    "Recordatorio Sesión en 28hs",
    "Especialista confirmación 6 horas antes",
    # Agregado 10/09/2026 (v1.4.4). Es el de MAYOR volumen desde la migración
    # — 2.926 envíos en 7 días — y faltaba. Sin él, la cobertura del dato que
    # subimos de 37% a 91,7% se degradaba en silencio a medida que envejecía
    # el histórico del push de 28hs.
    "Especialista confirmación 3 dias antes",
)
_CACHE_POLLS_AGENDADA = {"datos": None, "ts": 0.0}


def _polls_de_sesion_agendada():
    """poll_id vigentes de los recordatorios, resueltos por nombre."""
    ahora = time.time()
    if _CACHE_POLLS_AGENDADA["datos"] is not None and (ahora - _CACHE_POLLS_AGENDADA["ts"]) < 1800:
        return _CACHE_POLLS_AGENDADA["datos"]
    buscados = {_norm_push(n) for n in PUSHES_AGENDADA_NOMBRE}
    ids = set()
    try:
        for f in _query_interna("""
            SELECT toString(poll_id) pid, argMax(poll_name, timestamps_eta) nombre
            FROM fact_deployment_status
            WHERE poll_name != '' AND timestamps_eta >= now() - INTERVAL 365 DAY
            GROUP BY pid
        """) or []:
            if _norm_push(f.get("nombre")) in buscados:
                ids.add(str(f["pid"]))
    except Exception as e:
        log.error(f"[agendadas] no se pudieron resolver los polls: {e}")
    _CACHE_POLLS_AGENDADA.update({"datos": ids, "ts": ahora})
    log.warning(f"[agendadas] polls de recordatorio resueltos: {sorted(ids)}")
    return ids


def _sesiones_agendadas_dwh():
    """
    Sesiones por cliente contadas desde los recordatorios, ya cruzado con
    HubSpot (por conversación y, si falta, por teléfono).
    Una fecha distinta es una sesión distinta.
    """
    ids = _polls_de_sesion_agendada()
    if not ids:
        raise HTTPException(503, "No se encontraron los pushes de recordatorio en el DWH. "
                                 "Revisá GET /sesiones/senal para ver qué está viendo el bridge.")
    agend = ",".join(f"'{p}'" for p in sorted(ids))
    inasis = ",".join(f"'{p}'" for p in sorted(_polls_de_sesion()[1]))
    sql = f"""
    WITH d AS (
      SELECT treble_id tid, toString(poll_id) pid, toDate(timestamps_eta) dia
      FROM fact_deployment_status
      WHERE toString(poll_id) IN ({agend},{inasis})
    ),
    c AS (
      SELECT contact_wa_id wa, any(helpdesk_contact_id) hs
      FROM fact_conversations WHERE helpdesk_contact_id != '' GROUP BY wa
    )
    SELECT d.tid tid, any(c.hs) hubspot_id,
           uniqExactIf(d.dia, d.pid IN ({agend})) agendadas,
           uniqExactIf(d.dia, d.pid IN ({inasis})) inasistencias,
           max(d.dia) ultima
    FROM d LEFT JOIN c ON d.tid = c.wa
    GROUP BY tid
    ORDER BY ultima DESC
    LIMIT {SESIONES_MAX_CONTACTOS}
    """
    filas = _query_interna(sql) or []
    try:
        mapa = _mapa_telefono_hubspot()
    except Exception as e:
        log.error(f"[agendadas] sin mapa de teléfonos: {e}")
        mapa = {}
    salida = []
    for f in filas:
        hs, _ = _resolver_hubspot(f.get("tid"), f.get("hubspot_id"), mapa)
        if hs:
            f["hubspot_id"] = hs
            salida.append(f)
    return salida


@app.get("/sesiones/senal")
def sesiones_senal(x_api_key: str | None = Header(default=None)):
    """
    Compara las dos señales para poder decidir con el dato a la vista, sin
    tocar nada. Es lo que hay que mirar antes de cambiar el contador.
    """
    _chequear_clave(x_api_key)
    nombres = _salud_nombres_push()
    ids_ag = _polls_de_sesion_agendada()
    a_set, _ = _polls_de_sesion()

    def _cobertura(ids):
        if not ids:
            return 0
        lst = ",".join(f"'{p}'" for p in sorted(ids))
        d = _query_interna(f"""SELECT uniqExact(treble_id) n FROM fact_deployment_status
                               WHERE toString(poll_id) IN ({lst})""")
        return int((d or [{}])[0].get("n") or 0)

    filas = _sesiones_agendadas_dwh()
    tramos = {"1_a_4": 0, "5_a_8": 0, "9_o_mas": 0}
    for f in filas:
        n = max(0, int(f.get("agendadas") or 0) - int(f.get("inasistencias") or 0))
        if n <= 0:
            continue
        tramos["1_a_4" if n <= 4 else ("5_a_8" if n <= 8 else "9_o_mas")] += 1

    return {
        "senal_actual": {
            "pushes": sorted(nombres.get(p, f"conversación {p}") for p in a_set),
            "clientes_cubiertos": _cobertura(a_set),
        },
        "senal_nueva": {
            "pushes": sorted(nombres.get(p, f"conversación {p}") for p in ids_ag),
            "clientes_cubiertos": _cobertura(ids_ag),
        },
        "reparto_con_la_senal_nueva": tramos,
        "a_consultoria": tramos["1_a_4"],
        "nota": ("Un recordatorio significa sesión AGENDADA. Se descuentan las inasistencias "
                 "registradas. Para decidir el enrutamiento alcanza; no es lo mismo que asistencia "
                 "verificada en plataforma."),
    }


@app.post("/sesiones/recalcular-agendadas")
def sesiones_recalcular_agendadas(
    x_api_key: str | None = Header(default=None),
    aplicar: str | None = None,
    hasta_sesiones: int | None = None,
):
    """
    Recalcula la etapa usando los recordatorios. DRY-RUN por defecto.

    `hasta_sesiones` permite activarlo por tramos: con ?hasta_sesiones=2
    solo se marcan como acompañamiento los clientes con 1 o 2 sesiones, y
    el resto queda como está. Sirve para que las gestoras absorban el
    volumen de a poco en vez de recibirlo todo de golpe.
    """
    _chequear_clave(x_api_key)
    _, faltan = _sesiones_props_existentes()
    if faltan:
        raise HTTPException(409, f"Faltan propiedades: {faltan}. Corré antes POST /sesiones/setup.")

    escribir = _a_bool(aplicar, por_defecto=False)
    # `corte` es el MÁXIMO de sesiones que todavía se considera "primeras 4".
    # Con SESIONES_CORTE_ETAPA=4 el default es 3: de la cuarta en adelante ya es ATC.
    tope = SESIONES_CORTE_ETAPA - 1
    corte = int(hasta_sesiones) if hasta_sesiones else tope
    corte = max(1, min(corte, tope))
    inicio = _sesiones_inicio_registro()

    filas = _sesiones_agendadas_dwh()
    entradas, etapas, omitidos = [], {"acompanamiento": 0, "soporte": 0}, 0
    for f in filas:
        hs = str(f.get("hubspot_id") or "")
        if not hs.isdigit():
            continue
        n = max(0, int(f.get("agendadas") or 0) - int(f.get("inasistencias") or 0))
        if n <= 0:
            continue
        if n <= corte:
            etapa = "acompanamiento"
        elif n >= SESIONES_CORTE_ETAPA:
            etapa = "soporte"
        else:
            # Está entre el corte gradual y el corte real: se deja como está
            # para no adelantarle volumen a las gestoras antes de tiempo.
            omitidos += 1
            continue
        etapas[etapa] += 1
        entradas.append({"id": hs, "properties": {
            PROP_SES_ASISTIDAS: n,
            PROP_SES_ETAPA: etapa,
            PROP_SES_ORIGEN: (f"Derivado de los recordatorios de sesión agendada "
                              f"({n} sesiones, menos las inasistencias registradas)."),
        }})

    resultado = {
        "modo": "aplicado" if escribir else "simulacion",
        "corte_aplicado": corte,
        "corte_real": SESIONES_CORTE_ETAPA - 1,
        "activacion_gradual": corte < SESIONES_CORTE_ETAPA - 1,
        "clientes_evaluados": len(filas),
        "a_escribir": len(entradas),
        "por_etapa": etapas,
        "omitidos_por_activacion_gradual": omitidos,
        "inicio_del_registro": inicio.isoformat() if inicio else None,
        "muestra": [{"hubspot_id": e["id"], "sesiones": e["properties"][PROP_SES_ASISTIDAS],
                     "etapa": e["properties"][PROP_SES_ETAPA]} for e in entradas[:10]],
    }
    if not escribir:
        resultado["aviso"] = ("Simulación. Para aplicarlo: POST /sesiones/recalcular-agendadas?aplicar=true "
                              "(agregá &hasta_sesiones=2 para arrancar solo con los de 1 y 2 sesiones).")
        return resultado

    escritos, errores = _hs_batch_update("contacts", entradas) if entradas else (0, [])
    resultado.update({"escritos": escritos, "errores": errores})
    log.warning(f"[agendadas] recalculadas={escritos} corte={corte} "
                f"acomp={etapas['acompanamiento']} sop={etapas['soporte']} errores={len(errores)}")
    return resultado


# ══════════════════════════════════════════════════════════════════
#  MONITOR DE RIESGO DE CANCELACIÓN + LISTA CORREGIDA
#  Agregado 10/09/2026 (v1.4.4). BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué existe ────────────────────────────────────────────────
#  El análisis de cancelaciones del 10/09 encontró que el 94,7% de los
#  clientes que piden la baja nunca recibieron un intento de retención,
#  y que la causa raíz es de infraestructura, no de criterio:
#
#  1. El riesgo NO se recalculaba solo. De los 6 monitores del bridge
#     (SLA, pedidos, escalamiento, onboarding, salud, reintento),
#     ninguno era de riesgo. `/riesgo/calcular` solo corría si alguien
#     lo llamaba a mano, así que la lista quedaba congelada días.
#
#  2. BUG ENCONTRADO LEYENDO EL CÓDIGO: `_riesgo_desde_dwh()` excluye
#     a quien "ya pidió cancelar" mirando SOLO `tag_name='Cancelaciones'`.
#     Pero al medirlo: en 30 días hubo 2.584 conversaciones que mencionan
#     cancelar y solo 518 llevan ese tag. **El 66% no se etiqueta.**
#
#     Consecuencia: la lista de riesgo alto contiene clientes que YA
#     pidieron la baja, porque su conversación quedó en DEFAULT. Mandarles
#     un push de retención es el peor error posible — le estamos pidiendo
#     que se quede a alguien que ya se fue.
#
#     Acá se corrige detectando por CONTENIDO del mensaje además del tag.
#
#  Este bloque NO modifica ninguna línea existente. `/riesgo/calcular` y
#  `/riesgo/lista` siguen intactos y funcionando igual. Lo nuevo vive en
#  `/riesgo/lista-v2` y en el monitor.
# ══════════════════════════════════════════════════════════════════

RIESGO_MONITOR_ACTIVO = os.environ.get("RIESGO_MONITOR_ACTIVO", "true")
RIESGO_MONITOR_HORA_UTC = int(os.environ.get("RIESGO_MONITOR_HORA_UTC", "9"))
RIESGO_SLACK_WEBHOOK_URL = (
    os.environ.get("RIESGO_SLACK_WEBHOOK_URL")
    or os.environ.get("SALUD_SLACK_WEBHOOK_URL")
    or os.environ.get("SLACK_WEBHOOK_URL")
    or ""
)

# Palabras que aparecen cuando alguien pide la baja. Validadas contra el
# DWH: encuentran 2.584 conversaciones en 30 días, contra 518 del tag.
# Se comparan en minúsculas y sin exigir palabra completa, porque el
# cliente escribe "cancelacion", "cancelarla", "cancelar mi plan".
RIESGO_PALABRAS_BAJA = [
    "cancelar", "cancelacion", "cancelación", "cancelo", "cancele",
    "dar de baja", "darme de baja", "de baja",
    "no quiero renovar", "no deseo renovar", "no renovar",
    "suspender", "suspension", "suspensión",
    "reembolso", "devolucion del dinero", "devolución del dinero",
]

for _m in ("riesgo_recalculos_automaticos",):
    METRICAS.setdefault(_m, 0)


def _evento_leer_status(event_type, external_id):
    """
    Igual que `_evento_ya_notificado` pero devuelve el status crudo en vez
    de compararlo con "notified". Hace falta para guardar el conteo del
    día anterior y poder informar el delta.
    """
    with _db_lock, sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT status FROM eventos WHERE event_type=? AND external_id=?",
            (event_type, external_id),
        ).fetchone()
        return row[0] if row else None


def _riesgo_en_gestion_por_texto(dias_atras=120):
    """
    Teléfonos que pidieron la baja, detectados por el CONTENIDO del
    mensaje y no por el tag.

    El tag `Cancelaciones` se pone a mano y se pone mal dos de cada tres
    veces. Buscar en el texto recupera el 66% invisible. Se mira solo lo
    que escribió el cliente (`sender='USER'`): si filtráramos también los
    mensajes del agente, una respuesta del tipo "te ayudo a cancelar"
    marcaría al cliente aunque él nunca lo haya pedido.
    """
    like = " OR ".join(
        "lower(content) LIKE '%" + p.replace("'", "") + "%'"
        for p in RIESGO_PALABRAS_BAJA
    )
    sql = f"""
    SELECT DISTINCT c.contact_wa_id AS wa
    FROM fact_agent_messages m
    INNER JOIN fact_conversations c ON m.conversation_id = c.conversation_id
    WHERE m.company_id = {int(SALUD_COMPANY_ID)}
      AND c.company_id = {int(SALUD_COMPANY_ID)}
      AND m.sender = 'USER'
      AND m.created_at >= now() - INTERVAL {int(dias_atras)} DAY
      AND ({like})
      AND c.contact_wa_id != ''
    """
    try:
        return {str(f.get("wa") or "") for f in _query_interna(sql) if f.get("wa")}
    except Exception as e:
        # Si esta consulta falla, NO se sigue de largo: sin ella la lista
        # incluiría a gente que ya canceló. Se devuelve None para que el
        # llamador sepa que el filtro no se pudo aplicar y avise.
        log.error(f"[riesgo] no se pudo detectar cancelaciones por texto: {e}")
        return None


def _riesgo_recalcular_interno(escribir=True):
    """
    El cálculo del riesgo sin la capa HTTP, para que el monitor pueda
    usarlo. Réplica deliberada de la lógica de `/riesgo/calcular`: se
    duplica en vez de refactorizar el endpoint porque la regla del
    bridge es que los bloques nuevos no tocan código que ya funciona.
    """
    _, faltan = _riesgo_props_existentes()
    if faltan:
        raise RuntimeError(f"Faltan propiedades en HubSpot: {faltan}")

    hoy = datetime.now(timezone.utc).date()
    filas = _riesgo_desde_dwh()

    entradas, conteo = [], {"alto": 0, "medio": 0, "bajo": 0}
    for f in filas:
        hs = str(f.get("hubspot_id") or "").strip()
        if not hs.isdigit():
            continue
        dias = int(f.get("dias") or 0)
        nivel = _riesgo_nivel(dias)
        if not nivel:
            continue
        conteo[nivel] += 1
        entradas.append({"id": hs, "properties": {
            PROP_RIESGO: nivel,
            PROP_DIAS_SIN_SESION: dias,
            PROP_RIESGO_FECHA: _dia_ms(hoy),
        }})

    res = {"evaluados": len(entradas), "por_nivel": conteo, "escritos": 0, "errores": []}
    if escribir and entradas:
        escritos, errores = _hs_batch_update("contacts", entradas)
        METRICAS["riesgo_calculado"] += escritos
        res["escritos"], res["errores"] = escritos, errores
    return res


def _riesgo_lista_pendientes(nivel="alto", excluir_por_texto=True):
    """
    La lista de trabajo real: riesgo `nivel` y que NO haya pedido la baja,
    mirando tag Y contenido.

    Devuelve `(clientes, filtro_texto_aplicado)`. El segundo valor importa:
    si la detección por texto falló, quien consuma esto tiene que saber que
    la lista puede contener gente que ya canceló.
    """
    filas = _riesgo_desde_dwh()
    en_gestion = _riesgo_en_gestion_por_texto() if excluir_por_texto else set()
    filtro_ok = en_gestion is not None
    if not filtro_ok:
        en_gestion = set()

    pendientes = []
    for f in filas:
        if _riesgo_nivel(int(f.get("dias") or 0)) != nivel:
            continue
        if int(f.get("ya_pidio_cancelar") or 0):
            continue                                    # excluido por tag
        tid = str(f.get("tid") or "")
        if tid and tid in en_gestion:
            continue                                    # excluido por texto
        if not str(f.get("hubspot_id") or "").isdigit():
            continue
        pendientes.append(f)

    pendientes.sort(key=lambda f: int(f.get("dias") or 0), reverse=True)
    return pendientes, filtro_ok


@app.get("/riesgo/lista-v2")
def riesgo_lista_v2(
    x_api_key: str | None = Header(default=None),
    nivel: str = "alto",
    tope: int = 400,
    solo_customer: str | None = None,
):
    """
    Igual que `/riesgo/lista` pero excluyendo también a quien pidió la baja
    por texto (el 66% que no lleva tag), y opcionalmente filtrando a
    `lifecyclestage = customer` — la mejora pendiente #1 del doc de riesgo.
    """
    _chequear_clave(x_api_key)
    if nivel not in [v for v, _ in RIESGO_NIVELES]:
        raise HTTPException(400, f"nivel debe ser uno de {[v for v, _ in RIESGO_NIVELES]}")

    pendientes, filtro_ok = _riesgo_lista_pendientes(nivel)

    # Comparación honesta contra la lista vieja, para ver cuánta gente
    # estaba entrando de más.
    sin_filtro_texto, _ = _riesgo_lista_pendientes(nivel, excluir_por_texto=False)
    rescatados = len(sin_filtro_texto) - len(pendientes)

    filtrado_customer = None
    if _a_bool(solo_customer, por_defecto=False):
        ids = [str(f["hubspot_id"]) for f in pendientes[:min(int(tope), 1000)]]
        vivos = _riesgo_solo_customers(ids)
        if vivos is not None:
            antes = len(pendientes)
            pendientes = [f for f in pendientes if str(f["hubspot_id"]) in vivos]
            filtrado_customer = {"descartados": antes - len(pendientes)}

    salida = {
        "generado": datetime.now(timezone.utc).isoformat(),
        "nivel": nivel,
        "total": len(pendientes),
        "excluidos_por_pedir_la_baja_en_el_texto": rescatados,
        "filtro_texto_aplicado": filtro_ok,
        "clientes": [{
            "hubspot_id": f["hubspot_id"],
            "dias_sin_sesion": int(f["dias"]),
            "ultima_sesion": str(f.get("ultima_sesion") or ""),
            "telefono": _mask_phone(str(f.get("tid") or "")),
        } for f in pendientes[:min(int(tope), 1000)]],
    }
    if filtrado_customer:
        salida["filtro_customer"] = filtrado_customer
    if not filtro_ok:
        salida["aviso"] = ("No se pudo consultar el DWH para detectar cancelaciones por texto. "
                           "La lista puede incluir clientes que ya pidieron la baja — no la uses "
                           "para disparar retención hasta que esto se resuelva.")
    return salida


def _riesgo_solo_customers(ids):
    """
    De una lista de contact ids, cuáles siguen siendo `customer`.
    Devuelve None si HubSpot falla, para no confundir "no pude verificar"
    con "ninguno es cliente".
    """
    vivos = set()
    try:
        for i in range(0, len(ids), 100):
            lote = ids[i:i + 100]
            r = _hubspot_api("POST", "/crm/v3/objects/contacts/batch/read", {
                "properties": ["lifecyclestage"],
                "inputs": [{"id": x} for x in lote],
            })
            for res in (r.get("results") or []):
                if (res.get("properties") or {}).get("lifecyclestage") == "customer":
                    vivos.add(str(res.get("id")))
        return vivos
    except Exception as e:
        log.error(f"[riesgo] no se pudo filtrar por lifecyclestage: {e}")
        return None


def _riesgo_texto_slack(res, pendientes, rescatados, filtro_ok, previo):
    c = res["por_nivel"]
    hoy_alto = c["alto"]
    lineas = [
        "*Riesgo de cancelación · recálculo diario*",
        f"Clientes evaluados: *{res['evaluados']}*",
        f"Riesgo alto: *{hoy_alto}*  ·  medio: {c['medio']}  ·  bajo: {c['bajo']}",
    ]
    if previo is not None:
        d = hoy_alto - previo
        signo = f"+{d}" if d > 0 else str(d)
        lineas.append(f"Cambio contra ayer en riesgo alto: *{signo}*")
    lineas.append(f"Lista de trabajo (alto, sin pedido de baja): *{pendientes}*")
    if rescatados:
        lineas.append(f"Excluidos por haber pedido la baja en el texto del chat: {rescatados}")
    if not filtro_ok:
        lineas.append(":warning: No se pudo aplicar el filtro por texto — la lista puede "
                      "incluir a quien ya canceló. No dispares retención con esta corrida.")
    if res.get("errores"):
        lineas.append(f":warning: Errores al escribir en HubSpot: {len(res['errores'])}")
    return "\n".join(lineas)


def _riesgo_monitor_loop():
    """
    Una vez al día. Recalcula el riesgo, lo escribe en HubSpot y publica el
    resultado con el delta contra el día anterior.

    La deduplicación va contra la tabla de eventos, no contra memoria: si
    Render levanta más de una instancia, cada una creería que le toca y el
    canal recibiría el parte repetido — y peor, HubSpot recibiría la misma
    escritura dos veces.
    """
    while True:
        try:
            ahora = datetime.now(timezone.utc)
            if ahora.hour == RIESGO_MONITOR_HORA_UTC:
                marca = str(ahora.date())
                if not _evento_ya_notificado("riesgo_recalculo", marca):
                    ayer = str((ahora - timedelta(days=1)).date())
                    prev_raw = _evento_leer_status("riesgo_conteo_alto", ayer)
                    try:
                        previo = int(prev_raw) if prev_raw is not None else None
                    except (TypeError, ValueError):
                        previo = None

                    res = _riesgo_recalcular_interno(escribir=True)
                    pendientes, filtro_ok = _riesgo_lista_pendientes("alto")
                    sin_filtro, _ = _riesgo_lista_pendientes("alto", excluir_por_texto=False)
                    rescatados = len(sin_filtro) - len(pendientes)

                    if RIESGO_SLACK_WEBHOOK_URL:
                        _slack_enviar(
                            RIESGO_SLACK_WEBHOOK_URL,
                            _riesgo_texto_slack(res, len(pendientes), rescatados, filtro_ok, previo),
                            nombre="riesgo_cancelacion",
                        )
                    _evento_marcar("riesgo_recalculo", marca, "notified", notified=True)
                    _evento_marcar("riesgo_conteo_alto", marca, str(res["por_nivel"]["alto"]))
                    METRICAS["riesgo_recalculos_automaticos"] += 1
                    log.warning(f"[riesgo] recálculo automático · alto={res['por_nivel']['alto']} "
                                f"lista={len(pendientes)} escritos={res['escritos']}")
        except Exception as e:
            log.error(f"[riesgo] fallo en el recálculo automático: {e}")
        time.sleep(300)


@app.get("/riesgo/monitor-estado")
def riesgo_monitor_estado(x_api_key: str | None = Header(default=None)):
    """Para saber si el monitor corrió hoy sin tener que mirar los logs."""
    _chequear_clave(x_api_key)
    hoy = str(datetime.now(timezone.utc).date())
    historial = []
    for d in range(7):
        dia = str((datetime.now(timezone.utc) - timedelta(days=d)).date())
        v = _evento_leer_status("riesgo_conteo_alto", dia)
        if v is not None:
            historial.append({"dia": dia, "riesgo_alto": v})
    return {
        "activo": _a_bool(RIESGO_MONITOR_ACTIVO, por_defecto=True),
        "hora_utc": RIESGO_MONITOR_HORA_UTC,
        "corrio_hoy": _evento_ya_notificado("riesgo_recalculo", hoy),
        "recalculos_automaticos": METRICAS.get("riesgo_recalculos_automaticos", 0),
        "historial_7_dias": historial,
        "slack_configurado": bool(RIESGO_SLACK_WEBHOOK_URL),
    }


@app.on_event("startup")
def arrancar_monitor_riesgo():
    if not _a_bool(RIESGO_MONITOR_ACTIVO, por_defecto=True):
        log.warning("[startup] monitor de riesgo desactivado por RIESGO_MONITOR_ACTIVO")
        return
    threading.Thread(target=_riesgo_monitor_loop, daemon=True).start()
    log.warning(f"[startup] monitor de riesgo activo · recalcula a las {RIESGO_MONITOR_HORA_UTC}:00 UTC")


# ══════════════════════════════════════════════════════════════════
#  SEGMENTO DORMANT — LA VENTANA DONDE TODAVÍA HAY RETORNO
#  Agregado 10/09/2026 (v1.4.5). BLOQUE PURAMENTE ADITIVO.
#
#  ── Por qué existe ────────────────────────────────────────────────
#  El Framework de Retención de Opción Yo (Notion · Proyecto Health
#  Score, 13/08/2026), validado contra el warehouse de producto, dice
#  en su hallazgo 7, textual:
#
#    "Dormant (un solo mes en cero) es la alarma: la retención se
#     desploma a ~28–35% y el 2º mes en cero ya no agrega daño →
#     intervenir al PRIMER mes en cero; del At Risk (2+ meses) ~90%
#     ya no vuelve."
#
#  Nuestro modelo de riesgo marca "alto" recién a los 60 días. Eso ya
#  es At Risk. Medido el 10/09: 648 clientes en Dormant (31-60 días),
#  a los que el sistema clasifica "riesgo medio" y con los que no se
#  hace absolutamente nada, contra 739 en At Risk que se llevan toda
#  la atención de retención.
#
#  ── Lo que este bloque NO hace, a propósito ───────────────────────
#  NO cambia `RIESGO_DIAS_ALTO`. El umbral de 60 días se validó con un
#  caso-control de 1.245 casos y 1.699 controles y dio 83,8% de
#  precisión; moverlo sin revalidar sería romper algo medido por algo
#  supuesto.
#
#  Se intentó revalidar con corte a 30 días usando el DWH de Treble y
#  NO SE PUDO CONCLUIR: el único proxy de sesión disponible acá es el
#  push de "sí asistió", y los controles quedan contaminados con
#  clientes que ya se fueron en silencio (churn sin aviso), lo que
#  invierte artificialmente la señal. El dato bueno —sesiones reales y
#  renovaciones— vive en el warehouse de producto (marts), no acá.
#
#  Así que este bloque se limita a EXPONER el segmento, que hoy es
#  invisible. Decidir el umbral es de negocio y necesita el otro dato.
# ══════════════════════════════════════════════════════════════════

RIESGO_DORMANT_DESDE = int(os.environ.get("RIESGO_DORMANT_DESDE", "31"))
RIESGO_DORMANT_HASTA = int(os.environ.get("RIESGO_DORMANT_HASTA", "60"))


@app.get("/riesgo/dormant")
def riesgo_dormant(
    x_api_key: str | None = Header(default=None),
    tope: int = 400,
    desde: int | None = None,
    hasta: int | None = None,
):
    """
    La lista de trabajo que el framework pide y que hoy no existe:
    clientes con entre 31 y 60 días sin sesión, que TODAVÍA no pidieron
    la baja.

    Es el mismo filtro de `/riesgo/lista-v2` (tag + texto), porque el
    error de mandarle retención a alguien que ya canceló es igual de
    caro acá.

    Ordena por días DESCENDENTE: el que está más cerca de cruzar a
    At Risk es el más urgente, no el que recién entró.
    """
    _chequear_clave(x_api_key)
    d0 = int(desde) if desde is not None else RIESGO_DORMANT_DESDE
    d1 = int(hasta) if hasta is not None else RIESGO_DORMANT_HASTA
    if d0 < 1 or d1 <= d0:
        raise HTTPException(400, "Rango inválido: se espera 1 <= desde < hasta.")

    filas = _riesgo_desde_dwh()
    en_gestion = _riesgo_en_gestion_por_texto()
    filtro_ok = en_gestion is not None
    if not filtro_ok:
        en_gestion = set()

    dentro, ya_pidieron = [], 0
    for f in filas:
        dias = int(f.get("dias") or 0)
        if not (d0 <= dias <= d1):
            continue
        if not str(f.get("hubspot_id") or "").isdigit():
            continue
        tid = str(f.get("tid") or "")
        if int(f.get("ya_pidio_cancelar") or 0) or (tid and tid in en_gestion):
            ya_pidieron += 1
            continue
        dentro.append(f)

    dentro.sort(key=lambda f: int(f.get("dias") or 0), reverse=True)

    # Cuántos cruzan a At Risk esta semana. Es la urgencia real: pasado
    # ese punto, el framework dice que ~90% ya no vuelve.
    cruzan_en_7 = sum(1 for f in dentro if int(f["dias"]) >= d1 - 7)

    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "ventana_dias": {"desde": d0, "hasta": d1},
        "total": len(dentro),
        "cruzan_a_at_risk_en_7_dias": cruzan_en_7,
        "excluidos_por_ya_pedir_la_baja": ya_pidieron,
        "filtro_texto_aplicado": filtro_ok,
        "clientes": [{
            "hubspot_id": f["hubspot_id"],
            "dias_sin_sesion": int(f["dias"]),
            "dias_para_at_risk": max(0, d1 - int(f["dias"])),
            "ultima_sesion": str(f.get("ultima_sesion") or ""),
            "telefono": _mask_phone(str(f.get("tid") or "")),
        } for f in dentro[:min(int(tope), 1000)]],
        "fundamento": ("Framework de Retención · Proyecto Health Score (Notion, 13/08/2026), "
                       "hallazgo 7: intervenir al primer mes en cero; del At Risk (2+ meses) "
                       "~90% ya no vuelve."),
        "aviso": None if filtro_ok else (
            "No se pudo detectar cancelaciones por texto: la lista puede incluir a quien ya "
            "pidió la baja. No la uses para disparar retención con esta corrida."),
    }


@app.get("/riesgo/embudo-retencion")
def riesgo_embudo_retencion(x_api_key: str | None = Header(default=None)):
    """
    Foto de los cuatro estados del framework en una sola llamada, para
    ver de un vistazo dónde está parada la base y cuánto pesa el
    segmento que hoy no se atiende.
    """
    _chequear_clave(x_api_key)
    filas = _riesgo_desde_dwh()
    en_gestion = _riesgo_en_gestion_por_texto() or set()

    est = {"activo": 0, "dormant": 0, "at_risk": 0, "perdido": 0}
    sin_pedir = {"activo": 0, "dormant": 0, "at_risk": 0, "perdido": 0}
    for f in filas:
        if not str(f.get("hubspot_id") or "").isdigit():
            continue
        d = int(f.get("dias") or 0)
        k = ("activo" if d <= 30 else "dormant" if d <= 60 else "at_risk" if d <= 90 else "perdido")
        est[k] += 1
        tid = str(f.get("tid") or "")
        if not int(f.get("ya_pidio_cancelar") or 0) and not (tid and tid in en_gestion):
            sin_pedir[k] += 1

    return {
        "generado": datetime.now(timezone.utc).isoformat(),
        "estados": [
            {"estado": "Activo", "rango_dias": "0-30", "clientes": est["activo"],
             "sin_pedir_la_baja": sin_pedir["activo"], "accion": "ninguna"},
            {"estado": "Dormant", "rango_dias": "31-60", "clientes": est["dormant"],
             "sin_pedir_la_baja": sin_pedir["dormant"],
             "accion": "INTERVENIR — es donde el framework dice que todavía hay retorno"},
            {"estado": "At Risk", "rango_dias": "61-90", "clientes": est["at_risk"],
             "sin_pedir_la_baja": sin_pedir["at_risk"],
             "accion": "~90% ya no vuelve — intentar, pero no es acá donde se gana"},
            {"estado": "Perdido", "rango_dias": ">90", "clientes": est["perdido"],
             "sin_pedir_la_baja": sin_pedir["perdido"], "accion": "no invertir"},
        ],
        "nota": ("`sin_pedir_la_baja` ya excluye a quien mencionó cancelar, por tag o por el "
                 "texto del chat. Es sobre ese número que se arma cualquier campaña."),
    }


# ══════════════════════════════════════════════════════════════════
#  PARTE OPERATIVO A SLACK — INVERTIR EL SENTIDO DEL DATO
#  Agregado 11/09/2026 (v1.4.6). BLOQUE PURAMENTE ADITIVO.
#
#  ── El problema que resuelve ──────────────────────────────────────
#  La tarea programada que revisa Slack tres veces al día corre en un
#  sandbox cuya política de red BLOQUEA este bridge y ClickHouse (403
#  en el CONNECT del proxy). El 10/09 eso dejó sin responder tres de
#  las cuatro preguntas de Iva, porque todas necesitaban el DWH.
#
#  El bloqueo no se puede levantar desde acá y no hay que buscarle la
#  vuelta. Pero se puede invertir el sentido del dato:
#
#      ANTES:  sandbox  ──(bloqueado)──>  bridge  ──>  DWH
#      AHORA:  bridge   ──(salida libre)──>  Slack  <──  sandbox
#
#  El bridge corre en Render y sí tiene salida. Si publica el estado
#  operativo en Slack, la tarea lo lee con el MCP de Slack —que sí
#  funciona— y puede responder con números reales aunque no alcance
#  el DWH.
#
#  Sale a las 11:00 UTC por defecto, antes de la primera corrida de la
#  tarea (13:00 UTC / 8:00 Colombia), para que siempre tenga el parte
#  del día fresco.
# ══════════════════════════════════════════════════════════════════

OPERATIVO_ACTIVO = os.environ.get("OPERATIVO_ACTIVO", "true")
OPERATIVO_HORA_UTC = int(os.environ.get("OPERATIVO_HORA_UTC", "11"))
OPERATIVO_SLACK_WEBHOOK_URL = (
    os.environ.get("OPERATIVO_SLACK_WEBHOOK_URL")
    or os.environ.get("SALUD_SLACK_WEBHOOK_URL")
    or os.environ.get("SLACK_WEBHOOK_URL")
    or ""
)

# Botones cuya desaparición significa que un flujo perdió sus ramas.
# El 03/09 se migró el recordatorio de 28hs y la plantilla nueva salió
# sin botones: pasamos de ~1.300 confirmaciones semanales a 5, y nadie
# lo notó durante una semana. Esto es la alarma para que no se repita.
OPERATIVO_BOTONES_VIGILADOS = [
    "Confirmar sesión", "Reagendar", "Confirmar",
    "Todo estuvo bien", "Quiero que me escribas",
]

for _m in ("partes_operativos_enviados",):
    METRICAS.setdefault(_m, 0)


def _operativo_datos(dias=1):
    """
    Todo lo que la tarea programada no puede consultar por su cuenta,
    en una sola pasada al DWH.
    """
    cia = int(SALUD_COMPANY_ID)
    desde = f"now() - INTERVAL {int(dias)} DAY"

    lag = _query_interna(
        f"SELECT dateDiff('minute', max(timestamps_eta), now()) lag "
        f"FROM fact_deployment_status WHERE company_id={cia}") or [{}]

    total = _query_interna(f"""
        SELECT count() env,
               countIf(status IN ('DELIVERED','SUCCESS')) ent,
               countIf(status='FAILURE_BY_META_CHOSE_NOT_DELIVER') meta,
               countIf(status='FAILURE_BY_HUMAN_HANDOVER') chat,
               countIf(status='FAILURE_BY_UNABLE_TO_CONTACT') muerto,
               countIf(status='MISSING_PARAMETER') config
        FROM fact_deployment_status
        WHERE company_id={cia} AND timestamps_eta >= {desde}""") or [{}]

    peores = _query_interna(f"""
        SELECT argMax(poll_name, timestamps_eta) push, count() env,
               round(100*countIf(status IN ('DELIVERED','SUCCESS'))/count(),1) pct,
               countIf(status='FAILURE_BY_META_CHOSE_NOT_DELIVER') meta
        FROM fact_deployment_status
        WHERE company_id={cia} AND timestamps_eta >= {desde} AND poll_name != ''
        GROUP BY poll_id HAVING env >= 10 ORDER BY pct ASC LIMIT 5""") or []

    lista = ",".join("'" + b.replace("'", "") + "'" for b in OPERATIVO_BOTONES_VIGILADOS)
    botones = _query_interna(f"""
        SELECT answer_text b, count() n FROM fact_hsm_responses
        WHERE company_id={cia} AND response_date >= {desde} AND answer_text IN ({lista})
        GROUP BY b ORDER BY n DESC""") or []

    # Una plantilla MARKETING con envíos es una bomba: rechaza el 56%.
    marketing = _query_interna(f"""
        SELECT d.name plantilla, count() respuestas
        FROM fact_hsm_responses r
        INNER JOIN dim_hsm d ON r.hsm_id = d.id AND d.company_id={cia}
        WHERE r.company_id={cia} AND r.response_date >= {desde} AND d.category='MARKETING'
        GROUP BY plantilla ORDER BY respuestas DESC LIMIT 5""") or []

    return {
        "lag_min": (lag[0] or {}).get("lag"),
        "total": total[0] or {},
        "peores": peores,
        "botones": botones,
        "plantillas_marketing_activas": marketing,
    }


def _operativo_texto(d):
    t = d["total"] or {}
    env = int(t.get("env") or 0)
    ent = int(t.get("ent") or 0)
    pct = round(100 * ent / env, 1) if env else 0.0

    L = ["*Parte operativo de mensajería · últimas 24 h*",
         f"Lag del DWH al generarlo: {d.get('lag_min')} min",
         "",
         f"*Envíos:* {env}  ·  *Entregados:* {ent} ({pct}%)"]

    causas = [("Meta rechazó", t.get("meta")), ("chat abierto", t.get("chat")),
              ("número muerto", t.get("muerto")), ("config nuestra", t.get("config"))]
    hay = [f"{n}: {int(v)}" for n, v in causas if int(v or 0) > 0]
    L.append("*No entregados:* " + (" · ".join(hay) if hay else "ninguno"))

    if d["botones"]:
        L.append("")
        # .get() y no corchetes en todo lo que sigue: este texto lo arma un
        # monitor que corre solo una vez al día. Si una fila del DWH viene sin
        # la clave esperada, el parte entero se cae y nadie se entera — el
        # except del loop lo traga y el canal simplemente no recibe nada.
        L.append("*Botones:* " + " · ".join(
            f"{b.get('b', '?')} {b.get('n', 0)}" for b in d["botones"]))
    else:
        L.append("")
        L.append(":rotating_light: *Cero respuestas con botón en 24 h.* Si algún flujo se "
                 "republicó, revisá que la plantilla nueva conserve sus botones — sin ellos "
                 "las ramas quedan sueltas y no da error.")

    if d["plantillas_marketing_activas"]:
        L.append("")
        L.append(":warning: *Plantillas MARKETING con actividad* (rechazan ~56% contra 0% de "
                 "UTILITY): " + " · ".join(
                     f"{m.get('plantilla', '?')} ({m.get('respuestas', 0)})"
                     for m in d["plantillas_marketing_activas"]))

    if d["peores"]:
        L.append("")
        L.append("*Peor entrega (con 10+ envíos):*")
        for p in d["peores"]:
            extra = f" · {p.get('meta')} rechazos de Meta" if int(p.get("meta") or 0) else ""
            L.append(f"  · {p.get('push') or 'sin nombre'} — "
                     f"{p.get('pct', '?')}% de {p.get('env', '?')}{extra}")

    L.append("")
    L.append("_Publicado por el bridge. La tarea de Slack lee esto cuando no alcanza el DWH._")
    return "\n".join(L)


@app.get("/operativo/parte")
def operativo_parte(x_api_key: str | None = Header(default=None), dias: int = 1):
    """El parte sin publicarlo, para revisarlo o pedirlo a demanda."""
    _chequear_clave(x_api_key)
    d = _operativo_datos(dias)
    return {"generado": datetime.now(timezone.utc).isoformat(),
            "ventana_dias": dias, "datos": d, "texto_slack": _operativo_texto(d)}


@app.post("/operativo/enviar")
def operativo_enviar(x_api_key: str | None = Header(default=None), dias: int = 1):
    """Publica el parte ahora, sin esperar al horario."""
    _chequear_clave(x_api_key)
    if not OPERATIVO_SLACK_WEBHOOK_URL:
        raise HTTPException(409, "Falta OPERATIVO_SLACK_WEBHOOK_URL (o SALUD_/SLACK_WEBHOOK_URL).")
    texto = _operativo_texto(_operativo_datos(dias))
    _slack_enviar(OPERATIVO_SLACK_WEBHOOK_URL, texto, nombre="parte_operativo")
    METRICAS["partes_operativos_enviados"] += 1
    return {"enviado": True, "caracteres": len(texto)}


def _operativo_monitor_loop():
    """
    Una vez al día, antes de que corra la tarea programada de Slack.
    Dedup contra la tabla de eventos, no contra memoria: si Render
    levanta dos instancias, el canal recibiría el parte repetido.
    """
    while True:
        try:
            ahora = datetime.now(timezone.utc)
            if ahora.hour == OPERATIVO_HORA_UTC:
                marca = str(ahora.date())
                if not _evento_ya_notificado("parte_operativo", marca):
                    texto = _operativo_texto(_operativo_datos(1))
                    _slack_enviar(OPERATIVO_SLACK_WEBHOOK_URL, texto, nombre="parte_operativo")
                    _evento_marcar("parte_operativo", marca, "notified", notified=True)
                    METRICAS["partes_operativos_enviados"] += 1
                    log.warning(f"[operativo] parte publicado · {marca}")
        except Exception as e:
            log.error(f"[operativo] fallo armando el parte: {e}")
        time.sleep(300)


@app.on_event("startup")
def arrancar_monitor_operativo():
    if not _a_bool(OPERATIVO_ACTIVO, por_defecto=True):
        log.warning("[startup] parte operativo desactivado por OPERATIVO_ACTIVO")
        return
    if not OPERATIVO_SLACK_WEBHOOK_URL:
        log.warning("[startup] parte operativo no arranca — falta webhook de Slack")
        return
    threading.Thread(target=_operativo_monitor_loop, daemon=True).start()
    log.warning(f"[startup] parte operativo activo · sale a las {OPERATIVO_HORA_UTC}:00 UTC")


# ══════════════════════════════════════════════════════════════════
#  REINTENTO QUE SOBREVIVE A LA REASIGNACIÓN DE IDS DE TREBLE
#  Agregado 11/09/2026 (v1.4.7). BLOQUE PURAMENTE ADITIVO.
#
#  ── El bug ────────────────────────────────────────────────────────
#  `/pushes/bloqueados` y `/pushes/reintentar` cruzan así:
#
#      recuperables = [f for f in filas if str(f["pid"]) in opciones]
#
#  `f["pid"]` es el **poll_id del DWH**, que Treble REASIGNA cada vez
#  que alguien publica el flujo. `opciones` viene de `enviar_push`, que
#  guarda el **conversation_id**, estable. Cuando un flujo se republica
#  los dos dejan de coincidir y el push entero cae a
#  "sin_workflow_asociado" aunque su workflow exista y funcione.
#
#  Medido el 11/09: 236 de 304 bloqueados (78%) figuraban sin workflow.
#  Ninguno lo estaba. "Especialista confirmación 3 dias antes" tenía
#  3.263 envíos bajo `1485179` hasta el 10/09 19:09 y 121 bajo
#  `1466629` desde las 20:09 — la publicación de esa tarde devolvió el
#  id viejo. `PUSH_1466629` siempre estuvo en `enviar_push`.
#
#  ── Por qué importa además del reintento ──────────────────────────
#  El mensaje de ese diagnóstico invitaba a crear el workflow faltante.
#  Hacerlo habría DUPLICADO el push de mayor volumen de la operación:
#  cada cliente recibiendo el recordatorio de su sesión dos veces. La
#  salvaguarda de `POST /workflows/push` lo frenó con un 409.
#
#  ── El arreglo ────────────────────────────────────────────────────
#  Resolver por NOMBRE, que es lo único estable — el mismo patrón que
#  `_polls_de_sesion()` ya usa para el contador de sesiones. Dos ids
#  con el mismo `poll_name` son el mismo push.
#
#  Ambigüedad: si un nombre corresponde a DOS conversation_id distintos
#  ya registrados (pasa de verdad: "Inasistencias Seg 1 Lau O" tiene la
#  "versión anterior" y "el más usado", ambas en `enviar_push`), NO se
#  resuelve. Elegir una al azar mandaría al cliente el push equivocado.
#
#  El hilo automático NO se toca: sigue usando el cruce viejo hasta que
#  el v2 se valide en vivo. Hoy reenvía y funciona; romperlo para
#  arreglarlo sería el peor negocio.
# ══════════════════════════════════════════════════════════════════

_CACHE_MAPA_POLL = {"datos": None, "ts": 0.0}


def _mapa_poll_a_push_registrado(forzar=False):
    """
    poll_id del DWH -> conversation_id dado de alta en `enviar_push`.

    Devuelve `(mapa, opciones, ambiguos)`:
      · `mapa`    incluye el id propio cuando ya está registrado, y el id
                  registrado equivalente cuando se resolvió por nombre.
      · `ambiguos` son los nombres con más de un id registrado: quedan
                  fuera a propósito y se informan para que un humano
                  desempate.
    """
    ahora = time.time()
    if not forzar and _CACHE_MAPA_POLL["datos"] and (ahora - _CACHE_MAPA_POLL["ts"]) < 900:
        return _CACHE_MAPA_POLL["datos"]

    opciones = _push_opciones_por_id()          # conversation_id -> label

    nombre_de = {}
    try:
        for f in _query_interna(f"""
            SELECT toString(poll_id) pid, argMax(poll_name, timestamps_eta) nombre
            FROM fact_deployment_status
            WHERE company_id = {int(SALUD_COMPANY_ID)} AND poll_name != ''
              AND timestamps_eta >= now() - INTERVAL 365 DAY
            GROUP BY pid""") or []:
            n = _norm_push(f.get("nombre") or "")
            if n:
                nombre_de[str(f["pid"])] = n
    except Exception as e:
        # Sin el DWH no se puede resolver por nombre. Se devuelve el cruce
        # viejo, que es conservador: sub-reporta recuperables, nunca manda
        # un push equivocado.
        log.error(f"[reintento] no se pudo armar el mapa por nombre: {e}")
        return {k: k for k in opciones}, opciones, {}

    # nombre -> ids registrados con ese nombre
    por_nombre = {}
    for pid_reg in opciones:
        n = nombre_de.get(pid_reg)
        if n:
            por_nombre.setdefault(n, []).append(pid_reg)

    ambiguos = {n: ids for n, ids in por_nombre.items() if len(ids) > 1}

    mapa = {}
    for pid, n in nombre_de.items():
        if pid in opciones:
            mapa[pid] = pid                              # ya estaba bien
        elif n in por_nombre and len(por_nombre[n]) == 1:
            mapa[pid] = por_nombre[n][0]                 # id reasignado
    for pid_reg in opciones:
        mapa.setdefault(pid_reg, pid_reg)

    _CACHE_MAPA_POLL["datos"] = (mapa, opciones, ambiguos)
    _CACHE_MAPA_POLL["ts"] = ahora
    return mapa, opciones, ambiguos


@app.get("/pushes/bloqueados-v2")
def pushes_bloqueados_v2(x_api_key: str | None = Header(default=None), horas: int | None = None):
    """
    Igual que `/pushes/bloqueados` pero resolviendo los poll_id que Treble
    reasignó. Informa aparte cuánto recupera respecto del cruce viejo.
    """
    _chequear_clave(x_api_key)
    ventana = int(horas) if horas else REINTENTO_HORAS_ATRAS
    filas = _bloqueados_pendientes(ventana)
    mapa, opciones, ambiguos = _mapa_poll_a_push_registrado()

    recuperables_v1 = sum(1 for f in filas if str(f["pid"]) in opciones)
    recuperables_v2, sin_resolver = 0, {}
    for f in filas:
        pid = str(f["pid"])
        if pid in mapa:
            recuperables_v2 += 1
        else:
            sin_resolver[pid] = sin_resolver.get(pid, 0) + 1

    return {
        "ventana_horas": ventana,
        "pendientes": len(filas),
        "reintentables_cruce_viejo": recuperables_v1,
        "reintentables_ahora": recuperables_v2,
        "rescatados_por_nombre": recuperables_v2 - recuperables_v1,
        "sin_resolver": sorted(
            [{"poll_id": k, "casos": v} for k, v in sin_resolver.items()],
            key=lambda x: -x["casos"])[:20],
        "nombres_ambiguos": [
            {"nombre": n, "ids_registrados": ids} for n, ids in ambiguos.items()],
        "nota": ("Un poll_id 'sin resolver' es un push que nunca estuvo en `enviar_push`, "
                 "o cuyo nombre no aparece en el DWH del último año. Antes de darle de alta "
                 "un workflow, verificá que no sea un id viejo de un push que YA existe: "
                 "crear el workflow duplicaría los envíos."),
    }


@app.post("/pushes/reintentar-v2")
def pushes_reintentar_v2(
    x_api_key: str | None = Header(default=None),
    aplicar: str | None = None,
    horas: int | None = None,
    tope: int | None = None,
):
    """
    Reintento con el mapa por nombre. DRY-RUN por defecto.

    Comparte la tabla de eventos con `/pushes/reintentar` (`reintento_push`
    + deployment_id), así que los dos no se pisan: lo que uno reenvió, el
    otro lo saltea.
    """
    _chequear_clave(x_api_key)
    escribir = _a_bool(aplicar, por_defecto=False)
    ventana = int(horas) if horas else REINTENTO_HORAS_ATRAS
    limite = min(int(tope), REINTENTO_MAX_POR_CORRIDA) if tope else REINTENTO_MAX_POR_CORRIDA

    filas = _bloqueados_pendientes(ventana)
    mapa, opciones, ambiguos = _mapa_poll_a_push_registrado()

    plan, omitidos = [], {"sin_workflow": 0, "sin_contacto": 0, "ya_reintentado": 0}
    for f in filas:
        pid = str(f["pid"])
        destino = mapa.get(pid)
        if not destino:
            omitidos["sin_workflow"] += 1
            continue
        hs_id = str(f.get("hubspot_id") or "").strip()
        if not hs_id.isdigit():
            omitidos["sin_contacto"] += 1
            continue
        if _evento_ya_notificado("reintento_push", f["did"]):
            omitidos["ya_reintentado"] += 1
            continue
        plan.append({
            "deployment_id": f["did"], "hubspot_id": hs_id,
            "poll_id_original": pid, "conversation_id": destino,
            "reasignado": destino != pid,
            "push": opciones.get(destino, ""),
            "telefono": _mask_phone(f"{f['cc']}{f['cel']}"),
        })
        if len(plan) >= limite:
            break

    res = {
        "modo": "aplicado" if escribir else "simulacion",
        "ventana_horas": ventana, "tope": limite,
        "pendientes_totales": len(filas),
        "a_reintentar": len(plan),
        "de_esos_por_id_reasignado": sum(1 for p in plan if p["reasignado"]),
        "omitidos": omitidos,
        "nombres_ambiguos_excluidos": len(ambiguos),
        "muestra": plan[:15],
    }
    if not escribir:
        res["aviso"] = "Simulación. Para reintentar de verdad: POST /pushes/reintentar-v2?aplicar=true"
        return res

    enviados, errores = 0, []
    for item in plan:
        try:
            _hubspot_api("PATCH", f"/crm/v3/objects/contacts/{item['hubspot_id']}",
                         {"properties": {PROP_ENVIAR_PUSH: f"PUSH_{item['conversation_id']}"}})
            _evento_marcar("reintento_push", item["deployment_id"], "notified", notified=True)
            enviados += 1
        except Exception as e:
            errores.append({"hubspot_id": item["hubspot_id"], "error": str(e)[:200]})
    res.update({"reenviados": enviados, "errores": errores})
    log.warning(f"[reintento-v2] reenviados={enviados} reasignados="
                f"{res['de_esos_por_id_reasignado']} errores={len(errores)}")
    return res


# ══════════════════════════════════════════════════════════════════
#  CADUCIDAD DEL REINTENTO + DOS TEXTOS QUE MENTÍAN
#  Agregado 11/09/2026 (v1.4.8). BLOQUE PURAMENTE ADITIVO.
#
#  ── El problema, encontrado al revisar el parte del 11/09 ─────────
#  El reintento reenvía cualquier push bloqueado dentro de 72 h sin
#  preguntarse si el mensaje SIGUE TENIENDO SENTIDO. Medido hoy sobre
#  los bloqueados de "Especialista confirmación 6 horas antes" —el que
#  avisa que la sesión es HOY:
#
#      0-6 h atrás ....  7   ← reenviar sirve
#      12 h ...........  3   ← la sesión ya pasó
#      18 h ........... 22   ← ya pasó
#      24 h ...........  7   ← ya pasó
#      30 h y más .....  8   ← ya pasó
#
#  De 47, solo 7 estaban en una ventana donde el mensaje todavía es
#  cierto. Los otros 40 le avisan al cliente de una sesión que ya
#  ocurrió. Eso no es recuperar un envío: es mandar ruido, y encima
#  gastando plantilla y cuota de Meta.
#
#  El reintento automático viene haciendo esto desde que se encendió.
#
#  ── La regla ──────────────────────────────────────────────────────
#  Un push que nombra un momento ("hoy", "en 6 horas", "mañana")
#  caduca. Uno que no lo nombra (saludos, seguimientos, NPS) no.
#  La ventana va por push y se puede ajustar sin tocar código.
#
#  El hilo automático viejo NO se reemplaza solo: el nuevo se enciende
#  con REINTENTO_V2_AUTOMATICO=true y el viejo se apaga con
#  REINTENTO_AUTOMATICO=false. Nunca los dos a la vez — dos hilos
#  reintentando en paralelo duplicarían los envíos.
# ══════════════════════════════════════════════════════════════════

# ── Texto 1: el parte decía que el reenvío es a mano. Ya no lo es ──
# La corrección de v1.4.2 se escribió cuando el hilo NO existía. Se creó
# ese mismo día y el texto quedó viejo. Iva lo marcó dos veces.
CAUSAS["FAILURE_BY_HUMAN_HANDOVER"] = (
    "el cliente tenía un chat abierto en Treble",
    "el reintento automático reenvía los que están dados de alta y no caducaron",
)

# ── Caducidad por push ────────────────────────────────────────────
# Se compara contra el nombre normalizado del push. Primer patrón que
# coincide, gana. Las horas son desde el envío ORIGINAL que falló.
REINTENTO_CADUCIDAD = [
    ("6 horas antes", 6),
    ("1 hora antes", 2),
    ("3 hs antes", 4),
    ("3 horas antes", 4),
    ("30 minutos", 1),
    ("28hs", 24),
    ("28 hs", 24),
    ("26 hs", 24),
    ("3 dias antes", 48),
    ("72h", 48),
    ("sesion en 72", 48),
    ("confirmacion de sesiones", 24),
    ("recordatorio", 12),
]
# Los que no nombran un momento (saludos, seguimientos, NPS, inasistencias)
# no caducan dentro de la ventana de reintento.
REINTENTO_CADUCIDAD_DEFECTO = int(os.environ.get("REINTENTO_CADUCIDAD_DEFECTO", "72"))

REINTENTO_V2_AUTOMATICO = os.environ.get("REINTENTO_V2_AUTOMATICO", "false")

_CACHE_NOMBRE_POLL = {"datos": None, "ts": 0.0}


def _nombre_de_poll(forzar=False):
    """poll_id -> nombre normalizado del push, con cache de 15 min."""
    ahora = time.time()
    if not forzar and _CACHE_NOMBRE_POLL["datos"] and (ahora - _CACHE_NOMBRE_POLL["ts"]) < 900:
        return _CACHE_NOMBRE_POLL["datos"]
    salida = {}
    try:
        for f in _query_interna(f"""
            SELECT toString(poll_id) pid, argMax(poll_name, timestamps_eta) nombre
            FROM fact_deployment_status
            WHERE company_id = {int(SALUD_COMPANY_ID)} AND poll_name != ''
              AND timestamps_eta >= now() - INTERVAL 365 DAY
            GROUP BY pid""") or []:
            salida[str(f["pid"])] = _norm_push(f.get("nombre") or "")
    except Exception as e:
        log.error(f"[reintento] no se pudieron leer los nombres de push: {e}")
        return {}
    _CACHE_NOMBRE_POLL["datos"] = salida
    _CACHE_NOMBRE_POLL["ts"] = ahora
    return salida


def _horas_utiles(nombre_normalizado):
    """Cuántas horas después del envío original sigue teniendo sentido reenviar."""
    n = nombre_normalizado or ""
    for patron, horas in REINTENTO_CADUCIDAD:
        if patron in n:
            return horas
    return REINTENTO_CADUCIDAD_DEFECTO


def _caducado(fila, nombres):
    """
    True si el mensaje ya no es cierto. Ante la duda —sin nombre o sin
    timestamp— devuelve False: preferimos reenviar de más que descartar
    un envío legítimo por un dato que falta.
    """
    ts = fila.get("ts")
    if not ts:
        return False
    nombre = nombres.get(str(fila.get("pid")), "")
    if not nombre:
        return False
    try:
        t = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        horas = (datetime.now(timezone.utc) - t).total_seconds() / 3600
    except Exception:
        return False
    return horas > _horas_utiles(nombre)


@app.get("/pushes/caducidad")
def pushes_caducidad(x_api_key: str | None = Header(default=None), horas: int | None = None):
    """
    Qué se reintentaría y qué está caducado, sin mandar nada. Sirve para
    ver de una si la tabla de ventanas está bien calibrada.
    """
    _chequear_clave(x_api_key)
    ventana = int(horas) if horas else REINTENTO_HORAS_ATRAS
    filas = _bloqueados_pendientes(ventana)
    mapa, opciones, _ = _mapa_poll_a_push_registrado()
    nombres = _nombre_de_poll()

    detalle = {}
    for f in filas:
        pid = str(f.get("pid"))
        nom = nombres.get(pid, "") or f"conversación {pid}"
        d = detalle.setdefault(nom, {"push": nom, "vigentes": 0, "caducados": 0,
                                     "sin_alta": 0, "ventana_horas": _horas_utiles(nombres.get(pid, ""))})
        if pid not in mapa:
            d["sin_alta"] += 1
        elif _caducado(f, nombres):
            d["caducados"] += 1
        else:
            d["vigentes"] += 1

    lista = sorted(detalle.values(), key=lambda x: -(x["caducados"] + x["vigentes"]))
    return {
        "ventana_horas": ventana,
        "pendientes": len(filas),
        "vigentes": sum(d["vigentes"] for d in lista),
        "caducados": sum(d["caducados"] for d in lista),
        "sin_alta": sum(d["sin_alta"] for d in lista),
        "por_push": lista[:25],
        "nota": ("Un push caducado nombra un momento que ya pasó — reenviar "
                 "'tu sesión es en 6 horas' un día después confunde al cliente y "
                 "gasta cuota de Meta. Las ventanas se ajustan en REINTENTO_CADUCIDAD."),
    }


def _reintento_corrida_v2(tope=None, escribir=True):
    """
    Una corrida con las dos correcciones: resuelve los ids reasignados y
    descarta lo caducado. Devuelve el mismo shape que `_reintento_corrida`
    para que el parte y las métricas no tengan que cambiar.
    """
    limite = min(int(tope), REINTENTO_MAX_POR_CORRIDA) if tope else REINTENTO_MAX_POR_CORRIDA
    filas = _bloqueados_pendientes(REINTENTO_HORAS_ATRAS)
    mapa, opciones, ambiguos = _mapa_poll_a_push_registrado()
    nombres = _nombre_de_poll()

    plan, om = [], {"sin_workflow": 0, "sin_contacto": 0, "ya_reintentado": 0, "caducado": 0}
    for f in filas:
        pid = str(f["pid"])
        destino = mapa.get(pid)
        if not destino:
            om["sin_workflow"] += 1
            continue
        if _caducado(f, nombres):
            om["caducado"] += 1
            continue
        hs_id = str(f.get("hubspot_id") or "").strip()
        if not hs_id.isdigit():
            om["sin_contacto"] += 1
            continue
        if _evento_ya_notificado("reintento_push", f["did"]):
            om["ya_reintentado"] += 1
            continue
        plan.append((f["did"], hs_id, destino))
        if len(plan) >= limite:
            break

    enviados, errores = 0, []
    if escribir:
        for did, hs_id, destino in plan:
            try:
                _hubspot_api("PATCH", f"/crm/v3/objects/contacts/{hs_id}",
                             {"properties": {PROP_ENVIAR_PUSH: f"PUSH_{destino}"}})
                _evento_marcar("reintento_push", did, "notified", notified=True)
                enviados += 1
            except Exception as e:
                errores.append(str(e)[:150])
    return {"reintentados": enviados if escribir else 0, "a_reintentar": len(plan),
            "pendientes_totales": len(filas), "omitidos": om, "errores": errores}


def _reintento_v2_monitor_loop():
    """
    Reemplazo del hilo viejo. NO arranca salvo REINTENTO_V2_AUTOMATICO=true,
    y hay que apagar el viejo con REINTENTO_AUTOMATICO=false: dos hilos
    reintentando a la vez duplicarían los envíos. Comparten la tabla de
    eventos, así que el candado por deployment_id igual los protege, pero
    no hay que depender de eso.
    """
    while True:
        try:
            ahora = datetime.now(timezone.utc)
            if REINTENTO_HORA_DESDE <= ahora.hour <= REINTENTO_HORA_HASTA:
                marca = f"{ahora.date()}-{ahora.hour}-v2"
                if not _evento_ya_notificado("corrida_reintento", marca):
                    _evento_marcar("corrida_reintento", marca, "notified", notified=True)
                    r = _reintento_corrida_v2()
                    om = r.get("omitidos") or {}
                    METRICAS["corridas_reintento"] += 1
                    METRICAS["reintentos_automaticos"] += int(r.get("reintentados") or 0)
                    log.warning(
                        f"[reintento-v2-auto] reenviados={r.get('reintentados')} "
                        f"caducados={om.get('caducado')} sin_workflow={om.get('sin_workflow')} "
                        f"ya_hechos={om.get('ya_reintentado')}")
        except Exception as e:
            log.error(f"[reintento-v2-auto] falló la corrida: {e}")
        time.sleep(max(60, REINTENTO_CADA_MINUTOS * 60))


@app.on_event("startup")
def arrancar_reintento_v2():
    if not _a_bool(REINTENTO_V2_AUTOMATICO, por_defecto=False):
        return
    if _a_bool(REINTENTO_AUTOMATICO, por_defecto=True):
        log.error("[startup] REINTENTO_V2_AUTOMATICO está encendido pero el viejo TAMBIÉN "
                  "(REINTENTO_AUTOMATICO). El v2 no arranca para no duplicar envíos. "
                  "Apagá el viejo con REINTENTO_AUTOMATICO=false.")
        return
    threading.Thread(target=_reintento_v2_monitor_loop, daemon=True).start()
    log.warning(f"[startup] reintento v2 activo · cada {REINTENTO_CADA_MINUTOS} min "
                f"entre las {REINTENTO_HORA_DESDE} y las {REINTENTO_HORA_HASTA} UTC")

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
        "bloques": ["workflows_push (1.3.4)", "cohorte_renovaciones (1.3.5)", "reintento_pushes (1.3.5)"],
        "endpoints_nuevos": [
            "POST /cohorte/setup", "POST /cohorte/procesar",
            "GET /cohorte/renovaciones", "GET /cohorte/kpis",
            "GET /pushes/bloqueados", "POST /pushes/reintentar",
        ],
    }


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

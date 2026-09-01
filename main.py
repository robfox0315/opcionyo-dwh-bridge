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

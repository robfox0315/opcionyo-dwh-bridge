"""
╔══════════════════════════════════════════════════════════════╗
║  DWH BRIDGE · Opción Yo                                       ║
║  API mínima de solo lectura para que NOVA (Claude) pueda      ║
║  consultar el Data Warehouse de Treble sin exponer las        ║
║  credenciales directamente ni permitir escritura alguna.      ║
║  Deploy sugerido: Render.com (free tier) o similar.           ║
║                                                                 ║
║  Incluye además: monitor de SLA de respuesta ATC (2 min),     ║
║  corriendo en segundo plano dentro de este mismo proceso,     ║
║  sin costo ni servicio adicional.                              ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import re
import json
import time
import threading
import urllib.request
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import clickhouse_connect

app = FastAPI(title="Opción Yo · DWH Bridge", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Configuración (todo por variables de entorno, nunca hardcodeado) ──
DWH_HOST = os.environ.get("DWH_HOST", "")
DWH_PORT = int(os.environ.get("DWH_PORT", "8443"))
DWH_USER = os.environ.get("DWH_USER", "")
DWH_PASSWORD = os.environ.get("DWH_PASSWORD", "")
DWH_DATABASE = os.environ.get("DWH_DATABASE", "client_analytics")
API_KEY = os.environ.get("BRIDGE_API_KEY", "")  # clave que solo tú y Claude conocen

# ── Reglas de seguridad para el SQL que llega ──
PALABRAS_PROHIBIDAS = [
    "insert", "update", "delete", "drop", "alter", "create", "truncate",
    "grant", "revoke", "rename", "attach", "detach", "kill", "system",
    "optimize", "exec", "call",
]
VERBOS_PERMITIDOS = ("select", "show", "describe", "desc", "explain", "with")


def _validar_sql(sql: str) -> str:
    sql_limpio = sql.strip().rstrip(";").strip()
    if not sql_limpio:
        raise HTTPException(400, "SQL vacío.")
    primera_palabra = sql_limpio.lower().split(None, 1)[0]
    if primera_palabra not in VERBOS_PERMITIDOS:
        raise HTTPException(403, f"Solo se permiten consultas de lectura ({', '.join(VERBOS_PERMITIDOS)}).")
    if ";" in sql_limpio:
        raise HTTPException(403, "No se permiten múltiples sentencias (punto y coma detectado).")
    bajo = sql_limpio.lower()
    for palabra in PALABRAS_PROHIBIDAS:
        if re.search(rf"\b{palabra}\b", bajo):
            raise HTTPException(403, f"Palabra no permitida en la consulta: '{palabra}'.")
    # Si no trae LIMIT y es un SELECT, le agregamos uno por seguridad (tope de resultado)
    if primera_palabra == "select" and "limit" not in bajo:
        sql_limpio += " LIMIT 5000"
    return sql_limpio


def _cliente():
    if not (DWH_HOST and DWH_USER and DWH_PASSWORD):
        raise HTTPException(500, "El servidor no tiene configuradas las credenciales del DWH (variables de entorno).")
    try:
        return clickhouse_connect.get_client(
            host=DWH_HOST, port=DWH_PORT, username=DWH_USER, password=DWH_PASSWORD,
            database=DWH_DATABASE, secure=True, connect_timeout=10,
        )
    except Exception as e:
        raise HTTPException(502, f"No se pudo conectar al Data Warehouse: {e}")


def _chequear_clave(x_api_key: str | None):
    if not API_KEY:
        raise HTTPException(500, "El servidor no tiene configurada BRIDGE_API_KEY — no se puede usar así.")
    if x_api_key != API_KEY:
        raise HTTPException(401, "Clave inválida o faltante (header X-API-Key).")


@app.get("/")
def home():
    return {"servicio": "Opción Yo DWH Bridge", "estado": "activo",
            "uso": "POST /query con header X-API-Key y body {\"sql\": \"SELECT ...\"}"}


@app.get("/health")
def health():
    """Prueba de conexión real al DWH (no requiere clave, no expone datos)."""
    try:
        client = _cliente()
        client.query("SELECT 1")
        return {"dwh_conectado": True}
    except HTTPException as e:
        return {"dwh_conectado": False, "detalle": e.detail}


@app.post("/query")
def query(body: dict, x_api_key: str | None = Header(default=None)):
    """
    Body esperado: {"sql": "SELECT ..."}
    Header requerido: X-API-Key: <tu clave>
    Solo acepta SELECT/SHOW/DESCRIBE/EXPLAIN — cualquier otra cosa se rechaza.
    """
    _chequear_clave(x_api_key)
    sql = body.get("sql", "")
    sql_seguro = _validar_sql(sql)
    client = _cliente()
    try:
        result = client.query(sql_seguro)
        columnas = result.column_names
        filas = [dict(zip(columnas, row)) for row in result.result_rows]
        return {"columnas": list(columnas), "filas": filas, "total_filas": len(filas)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Error al ejecutar la consulta: {e}")


@app.get("/q")
def query_get(sql: str, key: str):
    """
    Versión GET del mismo endpoint — para pegar directo en el navegador:
    https://tu-app.onrender.com/q?key=TU_CLAVE&sql=SELECT+1

    Roberto abre esta URL en el navegador, copia el JSON que aparece, y se lo
    pega a Claude en el chat — mismas reglas de seguridad que /query.
    """
    _chequear_clave(key)
    sql_seguro = _validar_sql(sql)
    client = _cliente()
    try:
        result = client.query(sql_seguro)
        columnas = result.column_names
        filas = [dict(zip(columnas, row)) for row in result.result_rows]
        return {"columnas": list(columnas), "filas": filas, "total_filas": len(filas)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Error al ejecutar la consulta: {e}")


# ══════════════════════════════════════════════════════════════════════
#  MONITOR DE SLA DE RESPUESTA ATC (Treble/WhatsApp) — 2 minutos
#  Corre en segundo plano dentro de este mismo proceso, cada 60 segundos.
#  No crea ningún servicio ni endpoint nuevo, no consume recursos extra
#  significativos, y no interactúa con el enrutamiento de Treble — solo
#  lee datos de fact_conversations.
# ══════════════════════════════════════════════════════════════════════

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")  # agregar en Render → Environment
SLA_THRESHOLD_SECONDS = 60  # 1 minuto: como el ciclo es de 60s, la alerta sale máx. a los 2 min
SLA_POLL_INTERVAL_SECONDS = 60

_sla_already_alerted: set[int] = set()

# Detecta DOS casos (el bug original solo cubría el primero):
# 1. Sigue sin responder ahora mismo, pasado el umbral (en vivo).
# 2. Ya respondió, pero tardó más del umbral, Y la respuesta llegó hace poco
#    (ventana de 90s) — esto evita que se nos escapen casos donde el agente
#    respondió ENTRE una revisión y la siguiente, que es lo que pasó el
#    fin de semana: 267 conversaciones respondidas tarde y ninguna alertada.
ATC_AGENTS = [
    "Camila Rodriguez", "Estefany Suárez", "Mary Cárdenas", "Sofia Castro",
    "Yesith Solano", "Eduardo Liendo", "Samira Pirique", "Lizbeth Calcina",
    "Ivanna Ortiz",  # agregada temporalmente para prueba — quitar después
]
_atc_agents_sql = ", ".join(f"'{a}'" for a in ATC_AGENTS)

SLA_SQL = f"""
SELECT
    conversation_id, agent_name, contact_wa_id, assigned_at, first_agent_message_at,
    dateDiff('second', assigned_at, coalesce(first_agent_message_at, now())) as seg_esperando
FROM client_analytics.fact_conversations
WHERE assigned_at IS NOT NULL
  AND assigned_at > now() - INTERVAL 1 DAY
  AND agent_name IN ({_atc_agents_sql})
  AND (
        (first_agent_message_at IS NULL AND dateDiff('second', assigned_at, now()) >= {SLA_THRESHOLD_SECONDS})
        OR
        (first_agent_message_at IS NOT NULL
         AND dateDiff('second', assigned_at, first_agent_message_at) >= {SLA_THRESHOLD_SECONDS})
      )
ORDER BY assigned_at ASC
"""


def _sla_enviar_slack(fila: dict):
    minutos = round(fila["seg_esperando"] / 60, 1)
    mensaje = {
        "text": (
            f":stopwatch: *SLA de respuesta vencido (Treble/WhatsApp)*\n"
            f"Conversación #{fila['conversation_id']} asignada a *{fila.get('agent_name') or 'Sin agente'}* "
            f"hace *{minutos} min* sin primera respuesta.\n"
            f"Contacto (WhatsApp): `{fila.get('contact_wa_id') or 'N/D'}`"
        )
    }
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(mensaje).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=10)


def _sla_revisar_una_vez():
    global _sla_already_alerted
    sql_seguro = _validar_sql(SLA_SQL)
    client = _cliente()
    result = client.query(sql_seguro)
    columnas = result.column_names
    filas = [dict(zip(columnas, row)) for row in result.result_rows]

    print(f"[SLA monitor] revisión OK — {len(filas)} en incumplimiento detectados en esta corrida")

    enviadas = 0
    for fila in filas:
        cid = fila["conversation_id"]
        if cid not in _sla_already_alerted and SLACK_WEBHOOK_URL:
            try:
                _sla_enviar_slack(fila)
                _sla_already_alerted.add(cid)
                enviadas += 1
                print(f"[SLA monitor] alerta enviada a Slack: conversation_id={cid}")
            except Exception as e:
                print(f"[SLA monitor] ERROR enviando a Slack conversation_id={cid}: {e}")

    if filas and enviadas == 0:
        print(f"[SLA monitor] {len(filas)} detectados pero 0 enviadas (ya estaban alertadas antes, o falta SLACK_WEBHOOK_URL)")

    if len(_sla_already_alerted) > 5000:
        _sla_already_alerted = set(list(_sla_already_alerted)[-2500:])


def _sla_monitor_loop():
    global _sla_already_alerted
    print("[SLA monitor] hilo de monitoreo iniciado")

    # Foto inicial: marca como "ya vistos" los casos que existen AL ARRANCAR,
    # para no mandar de golpe todo el backlog histórico como alertas nuevas.
    # Solo se alertan casos que aparezcan DESPUÉS de este arranque.
    try:
        sql_seguro = _validar_sql(SLA_SQL)
        client = _cliente()
        result = client.query(sql_seguro)
        columnas = result.column_names
        filas_iniciales = [dict(zip(columnas, row)) for row in result.result_rows]
        _sla_already_alerted = {f["conversation_id"] for f in filas_iniciales}
        print(f"[SLA monitor] foto inicial: {len(_sla_already_alerted)} casos existentes marcados como vistos (no se alertan)")
    except Exception as e:
        print(f"[SLA monitor] ERROR en foto inicial: {e}")

    while True:
        try:
            _sla_revisar_una_vez()
        except Exception as e:
            print(f"[SLA monitor] ERROR en la revisión: {e}")
        time.sleep(SLA_POLL_INTERVAL_SECONDS)


# Arranca el monitor en segundo plano cuando el bridge levanta (una sola vez).
threading.Thread(target=_sla_monitor_loop, daemon=True).start()

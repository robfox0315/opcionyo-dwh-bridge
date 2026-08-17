"""
╔══════════════════════════════════════════════════════════════╗
║  DWH BRIDGE · Opción Yo                                       ║
║  API mínima de solo lectura para que NOVA (Claude) pueda      ║
║  consultar el Data Warehouse de Treble sin exponer las        ║
║  credenciales directamente ni permitir escritura alguna.      ║
║  Deploy sugerido: Render.com (free tier) o similar.           ║
║                                                                 ║
║  Incluye además: monitor de SLA de respuesta ATC (2 min),     ║
║  triage diario de Pedido de especialista, y escalamiento de   ║
║  pushes ignorados — todos corriendo en segundo plano dentro   ║
║  de este mismo proceso, sin costo ni servicio adicional.       ║
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

app = FastAPI(title="Opción Yo · DWH Bridge", version="1.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

ACCOUNT_ID = 40159402

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
    try:
        client = _cliente()
        client.query("SELECT 1")
        return {"dwh_conectado": True}
    except HTTPException as e:
        return {"dwh_conectado": False, "detalle": e.detail}


@app.post("/query")
def query(body: dict, x_api_key: str | None = Header(default=None)):
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
# ══════════════════════════════════════════════════════════════════════

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SLA_THRESHOLD_SECONDS = 60
SLA_POLL_INTERVAL_SECONDS = 60

_sla_already_alerted: set[int] = set()

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
        SLACK_WEBHOOK_URL, data=json.dumps(mensaje).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
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


threading.Thread(target=_sla_monitor_loop, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════
#  TRIAGE EN VIVO — Bandeja "Pedido de especialista" (pipeline Administración)
# ══════════════════════════════════════════════════════════════════════

PEDIDOS_SLACK_WEBHOOK_URL = os.environ.get("PEDIDOS_SLACK_WEBHOOK_URL", "")
PIPELINE_ADMINISTRACION = "74755616"
STAGE_BANDEJA_ENTRADA = "143884924"

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
        ]}],
        "properties": ["subject", "content", "createdate"],
        "limit": 100,
    }
    hubspot_token = os.environ.get("HUBSPOT_TOKEN", "")
    req = urllib.request.Request(
        "https://api.hubspot.com/crm/v3/objects/tickets/search",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {hubspot_token}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp).get("results", [])


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
        f"*Categoría:* {t['categoria']}",
        f"*Especialista:* {t['especialista']}",
        f"*Cliente ID:* {t['id_cliente']}",
        "", f"*Mensaje completo:*", f"> {t['content']}",
    ]
    if t.get("draft_respuesta"):
        partes += ["", f"*💬 Borrador de respuesta sugerido:*", f"> {t['draft_respuesta']}"]
    partes += ["", f"<{link}|Abrir ticket en HubSpot>"]
    req = urllib.request.Request(
        PEDIDOS_SLACK_WEBHOOK_URL, data=json.dumps({"text": "\n".join(partes)}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    urllib.request.urlopen(req, timeout=10)


def _pedidos_clasificar_ticket(r):
    p = r["properties"]
    subject = p.get("subject") or ""
    content = p.get("content") or ""
    cat = _pedidos_clasificar(subject, content)
    nombre = _pedidos_extraer_nombre(subject)
    id_cliente = _pedidos_extraer_id(subject, content)
    draft = PEDIDOS_DRAFTS.get(cat, "").format(nombre=nombre, id_cliente=id_cliente) if cat in PEDIDOS_DRAFTS else None
    return {
        "ticket_id": r["id"], "content": content, "categoria": cat,
        "especialista": nombre, "id_cliente": id_cliente, "draft_respuesta": draft,
    }


_pedidos_ya_alertados: set = set()


def _pedidos_monitor_loop():
    global _pedidos_ya_alertados
    print("[Pedidos especialista] hilo en vivo iniciado")
    try:
        tickets_iniciales = _pedidos_obtener_tickets()
        _pedidos_ya_alertados = {r["id"] for r in tickets_iniciales}
        print(f"[Pedidos especialista] foto inicial: {len(_pedidos_ya_alertados)} tickets existentes marcados como vistos")
    except Exception as e:
        print(f"[Pedidos especialista] ERROR en foto inicial: {e}")

    while True:
        try:
            tickets = _pedidos_obtener_tickets()
            nuevos = [r for r in tickets if r["id"] not in _pedidos_ya_alertados]
            print(f"[Pedidos especialista] revisión OK — {len(tickets)} en bandeja, {len(nuevos)} nuevos")
            for r in nuevos:
                clasificado = _pedidos_clasificar_ticket(r)
                if PEDIDOS_SLACK_WEBHOOK_URL:
                    try:
                        _pedidos_enviar_slack_ticket(clasificado)
                        print(f"[Pedidos especialista] alerta enviada: ticket_id={r['id']}")
                    except Exception as e:
                        print(f"[Pedidos especialista] ERROR enviando ticket_id={r['id']}: {e}")
                _pedidos_ya_alertados.add(r["id"])
            if len(_pedidos_ya_alertados) > 5000:
                _pedidos_ya_alertados = set(list(_pedidos_ya_alertados)[-2500:])
        except Exception as e:
            print(f"[Pedidos especialista] ERROR: {e}")
        time.sleep(60)


threading.Thread(target=_pedidos_monitor_loop, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════
#  ESCALAMIENTO DE PUSHES IGNORADOS (pago + inasistencia)
#  Corre 1 vez al día. Detecta pacientes con 3+ pushes fallidos/sin
#  entrega consecutivos en las categorías de pago/inasistencia, marca
#  requiere_gestion_humana=Sí en HubSpot, y avisa a Slack con prioridad
#  para que un humano tome el caso en vez de que el automático le siga
#  insistiendo solo con el mismo mensaje que ya demostró no funcionar.
# ══════════════════════════════════════════════════════════════════════

ESCALAMIENTO_SLACK_WEBHOOK_URL = os.environ.get("ESCALAMIENTO_SLACK_WEBHOOK_URL", "")
ESCALAMIENTO_UMBRAL = int(os.environ.get("ESCALAMIENTO_UMBRAL", "3"))
ESCALAMIENTO_HORA_UTC = int(os.environ.get("ESCALAMIENTO_HORA_UTC", "13"))  # 13 UTC = 9 AM GMT-4

POLLS_PAGO_INASISTENCIA = [
    "Informe pago fallido 48hs",
    "Inasistencia 2, 3 o 4ta sesión con AR",
    "Inasistencia 2, 3, o 4ta sesión",
    "Inasistencia Primera sesión",
    "Inasistencias Lau O",
    "Saludo Carol INASISTENCIAS",
    "Saludo Giselle INASISTENCIAS",
    "Carlos inasistencias",
]


def _escalamiento_query_dwh():
    polls_sql = ",".join(f"'{p}'" for p in POLLS_PAGO_INASISTENCIA)
    # Contador UNIFICADO: cuenta todos los pushes de pago+inasistencia juntos
    # por número, no por categoría separada — un paciente que recibió 2
    # pushes de pago Y 2 de inasistencia también debe escalar (4 en total).
    sql = f"""
    SELECT
        country_code, cellphone,
        count(*) as veces,
        groupArray(DISTINCT poll_name) as polls,
        max(timestamps_eta) as ultimo_push
    FROM client_analytics.fact_deployment_status
    WHERE poll_name IN ({polls_sql})
      AND timestamps_eta > now() - INTERVAL 30 DAY
    GROUP BY country_code, cellphone
    HAVING veces >= {ESCALAMIENTO_UMBRAL}
    ORDER BY veces DESC
    LIMIT 200
    """
    sql_seguro = _validar_sql(sql)
    client = _cliente()
    result = client.query(sql_seguro)
    columnas = result.column_names
    return [dict(zip(columnas, row)) for row in result.result_rows]


def _escalamiento_buscar_contacto_hubspot(country_code, cellphone):
    hubspot_token = os.environ.get("HUBSPOT_TOKEN", "")
    body = {
        "filterGroups": [{"filters": [{"propertyName": "hs_whatsapp_phone_number", "operator": "CONTAINS_TOKEN", "value": f"*{cellphone}*"}]}],
        "properties": ["firstname", "lastname", "hs_object_id", "fecha_sesion", "proxima_sesion"],
        "limit": 1,
    }
    req = urllib.request.Request(
        "https://api.hubspot.com/crm/v3/objects/contacts/search",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {hubspot_token}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.load(resp)
    resultados = data.get("results", [])
    return resultados[0] if resultados else None


def _escalamiento_ya_se_recupero(contacto, ultimo_push_str):
    """True si el contacto ya tiene una sesión programada DESPUÉS del último
    push — en ese caso no hace falta escalar, ya se recuperó solo."""
    from datetime import datetime
    try:
        ultimo_push = datetime.fromisoformat(ultimo_push_str.replace("Z", "+00:00")) if ultimo_push_str else None
    except Exception:
        ultimo_push = None
    if not ultimo_push:
        return False
    for campo in ("proxima_sesion", "fecha_sesion"):
        val = contacto["properties"].get(campo)
        if not val:
            continue
        try:
            fecha = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if fecha > ultimo_push:
                return True
        except Exception:
            continue
    return False


def _escalamiento_marcar_hubspot(contact_id, veces):
    hubspot_token = os.environ.get("HUBSPOT_TOKEN", "")
    body = {"properties": {"pushes_ignorados_consecutivos": veces, "requiere_gestion_humana": "true"}}
    req = urllib.request.Request(
        f"https://api.hubspot.com/crm/v3/objects/contacts/{contact_id}",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {hubspot_token}", "Content-Type": "application/json"},
        method="PATCH",
    )
    urllib.request.urlopen(req, timeout=15)


def _escalamiento_enviar_slack(casos):
    if not casos:
        texto = "*🚨 Escalamiento de pushes ignorados* — ningún caso nuevo hoy. ✅"
    else:
        lines = [f"*🚨 Escalamiento de pushes ignorados — {len(casos)} casos*\n_Ya se filtraron los que se recuperaron solos (tienen sesión después del último push). Estos siguen sin responder._\n"]
        for c in casos[:30]:
            nombre = c.get("nombre") or "Sin nombre en HubSpot"
            link = f"https://app.hubspot.com/contacts/{ACCOUNT_ID}/record/0-1/{c['contact_id']}" if c.get("contact_id") else None
            polls_txt = ", ".join(c.get("polls", []))
            lines.append(f"• *{nombre}* — {c['veces']}x sin responder ({polls_txt}) — `{c['country_code']}{c['cellphone']}`")
            if link:
                lines.append(f"  <{link}|Ver contacto>")
        texto = "\n".join(lines)

    req = urllib.request.Request(
        ESCALAMIENTO_SLACK_WEBHOOK_URL, data=json.dumps({"text": texto}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    urllib.request.urlopen(req, timeout=10)


def _escalamiento_revisar_una_vez():
    filas = _escalamiento_query_dwh()
    print(f"[Escalamiento] {len(filas)} números sobre el umbral")
    casos = []
    recuperados_solos = 0
    for fila in filas:
        try:
            contacto = _escalamiento_buscar_contacto_hubspot(fila["country_code"], fila["cellphone"])
            if not contacto:
                casos.append({**fila, "contact_id": None, "nombre": None})
                continue
            if _escalamiento_ya_se_recupero(contacto, fila.get("ultimo_push")):
                recuperados_solos += 1
                continue  # ya tiene sesión después del último push, no escalar
            nombre = f"{contacto['properties'].get('firstname') or ''} {contacto['properties'].get('lastname') or ''}".strip()
            _escalamiento_marcar_hubspot(contacto["id"], fila["veces"])
            casos.append({**fila, "contact_id": contacto["id"], "nombre": nombre})
        except Exception as e:
            print(f"[Escalamiento] error procesando {fila.get('cellphone')}: {e}")
    print(f"[Escalamiento] {recuperados_solos} ya se habían recuperado solos (no escalados)")
    if ESCALAMIENTO_SLACK_WEBHOOK_URL:
        _escalamiento_enviar_slack(casos)
    print(f"[Escalamiento] {len(casos)} casos procesados y enviados a Slack")


def _escalamiento_monitor_loop():
    print("[Escalamiento] hilo iniciado")
    marca_archivo = "/tmp/escalamiento_ultimo_envio.txt"

    def _leer():
        try:
            with open(marca_archivo) as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def _guardar(v):
        with open(marca_archivo, "w") as f:
            f.write(v)

    while True:
        try:
            ahora = time.gmtime()
            hoy_str = f"{ahora.tm_year}-{ahora.tm_yday}"
            en_ventana = ahora.tm_hour == ESCALAMIENTO_HORA_UTC and ahora.tm_min < 10
            if en_ventana and _leer() != hoy_str:
                _escalamiento_revisar_una_vez()
                _guardar(hoy_str)
        except Exception as e:
            print(f"[Escalamiento] ERROR: {e}")
        time.sleep(180)


threading.Thread(target=_escalamiento_monitor_loop, daemon=True).start()

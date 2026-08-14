"""
Triage diario de la bandeja "Pedido de especialista" (pipeline Administración,
etapa Bandeja de entrada) en HubSpot.

Clasifica cada ticket abierto en categorías, redacta un borrador de respuesta
para las categorías simples, y manda un resumen a Slack (#pedidos-especialista).

NO ejecuta ninguna acción real (no pausa planes, no reagenda, no cambia estados)
porque esos sistemas no están confirmados como accesibles desde HubSpot/API —
solo prepara todo para que una persona lo resuelva en segundos.

Diseñado para Render Cron Job, corriendo 1 vez al día (ej: 8:00 AM).
Schedule sugerido: 0 12 * * *  (12:00 UTC = 8:00 AM GMT-4)

Variables de entorno requeridas:
- HUBSPOT_TOKEN: token del private app con scope crm.objects.tickets.read
- SLACK_WEBHOOK_URL: Incoming Webhook de #pedidos-especialista
"""
import os
import re
import json
import urllib.request

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

PIPELINE_ADMINISTRACION = "74755616"
STAGE_BANDEJA_ENTRADA = "143884924"
ACCOUNT_ID = 40159402

DRAFTS = {
    "Reagendar sesión": "Hola {nombre}, recibido — reviso la disponibilidad para reagendar la sesión de la clienta {id_cliente} y te confirmo un horario en breve.",
    "Pausar plan": "Listo {nombre}, pauso el plan de la clienta {id_cliente} según lo que indicaste. Te aviso cuando esté hecho.",
    "Postergar pago": "Confirmado {nombre}, gestiono la postergación del cobro de la clienta {id_cliente}. Te confirmo cuando quede aplicado.",
    "Seguimiento / contactar cliente": "Gracias por avisar {nombre}, nos comunicamos con la clienta {id_cliente} para dar seguimiento y te contamos qué nos responde.",
    "Corrección de estado de sesión": "Listo {nombre}, corrijo el estado de la sesión de la clienta {id_cliente} tal como indicaste.",
}

EMOJI = {
    "🔴 SENSIBLE — requiere revisión humana, no automatizar": "🔴",
    "Reagendar sesión": "📅",
    "Pausar plan": "⏸️",
    "Postergar pago": "💳",
    "Seguimiento / contactar cliente": "📞",
    "Corrección de estado de sesión": "✏️",
    "Soporte técnico / sistema": "🛠️",
    "Otro — revisar manualmente": "❓",
}


def obtener_tickets():
    url = "https://api.hubspot.com/crm/v3/objects/tickets/search"
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_ADMINISTRACION},
            {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": STAGE_BANDEJA_ENTRADA},
        ]}],
        "properties": ["subject", "content", "createdate"],
        "limit": 100,
    }
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp).get("results", [])


def extraer_id_cliente(subject, content):
    m = re.search(r'ID:\s*(\d+)', subject + " " + content)
    return m.group(1) if m else "N/D"


def extraer_nombre(subject):
    m = re.search(r'especialista:\s*(?:\(E\)\s*)?(.+?)\s*Por ID', subject)
    if m:
        return m.group(1).strip()
    m2 = re.match(r'^(.+?)\s*\(ID:', subject)
    return m2.group(1).strip() if m2 else subject


def clasificar(subject, content):
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


def enviar_slack(tickets_clasificados):
    if not tickets_clasificados:
        texto = "*📋 Bandeja Pedido de especialista* — vacía hoy, nada pendiente. ✅"
    else:
        lines = [f"*📋 Bandeja Pedido de especialista — {len(tickets_clasificados)} casos*\n"]
        for t in sorted(tickets_clasificados, key=lambda x: x["categoria"]):
            e = EMOJI.get(t["categoria"], "•")
            link = f"https://app.hubspot.com/contacts/{ACCOUNT_ID}/record/0-5/{t['ticket_id']}"
            lines.append(f"{e} *{t['categoria']}* — {t['especialista']} (cliente {t['id_cliente']})")
            resumen = t["content"][:120] + ("..." if len(t["content"]) > 120 else "")
            lines.append(f"   _{resumen}_")
            if t.get("draft_respuesta"):
                lines.append(f"   💬 Borrador: {t['draft_respuesta']}")
            lines.append(f"   <{link}|Ver ticket>")
            lines.append("")
        texto = "\n".join(lines)

    body = {"text": texto}
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status


def main():
    if not HUBSPOT_TOKEN or not SLACK_WEBHOOK_URL:
        print("Faltan variables de entorno HUBSPOT_TOKEN o SLACK_WEBHOOK_URL")
        return

    tickets = obtener_tickets()
    print(f"Tickets en bandeja: {len(tickets)}")

    clasificados = []
    for r in tickets:
        p = r["properties"]
        subject = p.get("subject") or ""
        content = p.get("content") or ""
        cat = clasificar(subject, content)
        nombre = extraer_nombre(subject)
        id_cliente = extraer_id_cliente(subject, content)
        draft = DRAFTS.get(cat, "").format(nombre=nombre, id_cliente=id_cliente) if cat in DRAFTS else None
        clasificados.append({
            "ticket_id": r["id"], "subject": subject, "content": content,
            "categoria": cat, "especialista": nombre, "id_cliente": id_cliente,
            "draft_respuesta": draft,
        })

    status = enviar_slack(clasificados)
    print(f"Enviado a Slack: HTTP {status}")


if __name__ == "__main__":
    main()

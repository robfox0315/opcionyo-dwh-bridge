"""
╔══════════════════════════════════════════════════════════════╗
║  DWH BRIDGE · Opción Yo                                       ║
║  API mínima de solo lectura para que NOVA (Claude) pueda      ║
║  consultar el Data Warehouse de Treble sin exponer las        ║
║  credenciales directamente ni permitir escritura alguna.      ║
║  Deploy sugerido: Render.com (free tier) o similar.           ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import re
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import clickhouse_connect

app = FastAPI(title="Opción Yo · DWH Bridge", version="1.0")

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

# DWH Bridge · Opción Yo

API mínima para que NOVA (Claude) pueda consultar el Data Warehouse de Treble
directamente — de solo lectura, con clave, sin exponer las credenciales del DWH.

## Qué hace y qué NO hace

- ✅ Permite `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`.
- ❌ Bloquea `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE` y
  cualquier otra palabra de escritura — aunque alguien tuviera la clave, no puede
  romper ni modificar nada.
- ❌ Bloquea múltiples sentencias en una sola consulta (nada de `SELECT 1; DROP...`).
- Le agrega automáticamente `LIMIT 5000` a cualquier `SELECT` que no traiga límite,
  para que nunca se pueda tirar abajo el servidor con una consulta gigante.
- Requiere una clave (`X-API-Key`) que solo tú vas a generar y compartir conmigo —
  sin ella, nadie puede usar el endpoint aunque encuentre la URL.

## Paso a paso — Deploy en Render.com (gratis)

### 1. Crear el repositorio en GitHub
1. Repo nuevo, **privado** (sugerido: `opcionyo-dwh-bridge`).
2. Sube estos 2 archivos: `main.py` y `requirements.txt`.

### 2. Crear la cuenta en Render (si no tienes una)
1. Ve a [render.com](https://render.com) → **Sign up** (puedes entrar con tu cuenta de GitHub).

### 3. Crear el servicio
1. Dashboard de Render → **New +** → **Web Service**.
2. Conecta el repositorio `opcionyo-dwh-bridge`.
3. Configuración:
   - **Name:** `opcionyo-dwh-bridge` (o el que prefieras)
   - **Region:** la más cercana
   - **Branch:** `main`
   - **Runtime:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Instance Type:** Free

### 4. Variables de entorno
En la misma pantalla de creación (o después en **Environment**), agrega:

| Key | Value |
|---|---|
| `DWH_HOST` | `eaoxkoa7g7.us-east-1.aws.clickhouse.cloud` |
| `DWH_PORT` | `8443` |
| `DWH_USER` | `opcionyo_readonly` |
| `DWH_PASSWORD` | (la contraseña del DWH que ya tienes) |
| `DWH_DATABASE` | `client_analytics` |
| `BRIDGE_API_KEY` | **generá una clave larga y random vos mismo** — ej. abre una terminal y corré `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`, o usá un generador de contraseñas online de al menos 32 caracteres |

### 5. Deploy
Click **Create Web Service**. Render va a buildear y desplegar — tarda 1-3 minutos.
Al terminar, te da una URL tipo `https://opcionyo-dwh-bridge.onrender.com`.

### 6. Probarlo
Abre `https://TU-URL.onrender.com/health` en el navegador — debería decir
`{"dwh_conectado": true}`. Si dice `false`, revisa que las variables de entorno
estén bien escritas (sin espacios de más, contraseña exacta).

### 7. Pasarme los datos
Mándame en el chat:
1. La URL completa (`https://opcionyo-dwh-bridge.onrender.com`)
2. La clave (`BRIDGE_API_KEY`) que generaste

Con eso ya puedo consultar el DWH directamente cuando lo necesite, sin pasar por
capturas de pantalla ni exports manuales.

## Nota sobre el plan gratis de Render

El free tier de Render "duerme" el servicio después de 15 minutos sin uso, y tarda
~30-50 segundos en despertar la primera vez que lo llamo después de estar dormido.
No es un problema — simplemente la primera consulta después de un rato puede
tardar un poco más, las siguientes son rápidas.

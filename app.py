import os, json, requests
from flask import Flask, request, Response, send_from_directory

app = Flask(__name__, static_folder="dashboard")

SHEETS_WEBAPP_URL = os.environ.get("SHEETS_WEBAPP_URL", "")

@app.route("/")
def index():
    return send_from_directory("dashboard", "index.html")

@app.route("/ping")
def ping():
    return "ok", 200

@app.route("/sheets-proxy")
def sheets_proxy():
    def json_err(msg, status=503):
        r = Response(json.dumps({"ok": False, "error": msg}),
                     status=status, content_type="application/json")
        r.headers["Access-Control-Allow-Origin"] = "*"
        return r

    if not SHEETS_WEBAPP_URL:
        return json_err("SHEETS_WEBAPP_URL nao configurada no servidor")

    params = dict(request.args)
    try:
        r = requests.get(SHEETS_WEBAPP_URL, params=params, timeout=120)
        # Repassa a resposta do Apps Script tal como veio
        resp = Response(r.content, status=r.status_code,
                        content_type="application/json")
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except requests.exceptions.Timeout:
        return json_err("Timeout ao conectar ao Google Sheets (>120s). Tente atualizar.")
    except requests.exceptions.ConnectionError as e:
        return json_err(f"Erro de conexão com Google Sheets: {str(e)[:120]}")
    except Exception as e:
        return json_err(f"Erro inesperado no proxy: {str(e)[:200]}", status=500)

@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory("dashboard", filename)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

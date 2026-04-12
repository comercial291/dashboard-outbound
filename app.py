import os
import json
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, request, Response, send_from_directory

app = Flask(__name__, static_folder="dashboard")

# ── Configuração via variáveis de ambiente ─────────────────────────────────────
PIPEDRIVE_API_KEY    = os.environ.get("PIPEDRIVE_API_KEY", "")
PIPEDRIVE_BASE       = "https://api.pipedrive.com/v1"
PRESALES_PIPELINE_ID = int(os.environ.get("PRESALES_PIPELINE_ID", "1"))
SALES_PIPELINE_ID    = int(os.environ.get("SALES_PIPELINE_ID",    "2"))
CACHE_TTL            = int(os.environ.get("CACHE_TTL_SECONDS",    "900"))  # 15 min

# ── Cache em memória ───────────────────────────────────────────────────────────
_cache = {"data": None, "ts": 0.0}


# ── Helpers Pipedrive API ──────────────────────────────────────────────────────
def pd_get(endpoint: str, params: dict = None) -> dict:
    url = f"{PIPEDRIVE_BASE}/{endpoint}"
    p   = {"api_token": PIPEDRIVE_API_KEY, "limit": 500, **(params or {})}
    r   = requests.get(url, params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def pd_get_all(endpoint: str, params: dict = None) -> list:
    """Percorre todas as páginas e retorna lista completa."""
    items, start = [], 0
    while True:
        data  = pd_get(endpoint, {**(params or {}), "start": start})
        batch = data.get("data") or []
        items.extend(batch)
        pg = (data.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"):
            break
        start += 500
    return items


# ── Normalizers ────────────────────────────────────────────────────────────────
def _v(obj, key, fallback=""):
    val = obj.get(key)
    if isinstance(val, dict):
        return val.get("value") or val.get("id") or fallback
    return val if val is not None else fallback


def _name(obj, key, fallback=""):
    val = obj.get(key)
    if isinstance(val, dict):
        return val.get("name", fallback)
    return fallback


def norm_deal(d: dict, pipeline_id: int, stages_map: dict) -> dict:
    sid    = d.get("stage_id")
    stage  = stages_map.get(sid, {})
    status = d.get("status", "")
    close  = d.get("close_time") or ""
    # Garante que won_time / lost_time sempre têm valor para deals fechados
    won    = d.get("won_time")  or (close if status == "won"  else "") or ""
    lost   = d.get("lost_time") or (close if status == "lost" else "") or ""
    return {
        "id":             d.get("id"),
        "title":          d.get("title", ""),
        "pipeline_id":    pipeline_id,
        "stage_id":       sid,
        "stage_name":     stage.get("name", ""),
        "stage_order":    stage.get("order_nr", 0),
        "status":         status,
        "value":          float(d.get("value") or 0),
        "currency":       d.get("currency", "BRL"),
        "owner_id":       _v(d, "owner_id") or _v(d, "user_id"),
        "owner_name":     _name(d, "owner_id") or _name(d, "user_id"),
        "add_time":       d.get("add_time", ""),
        "update_time":    d.get("update_time", ""),
        "close_time":     close,
        "won_time":       won,
        "lost_time":      lost,
        "lost_reason":    d.get("lost_reason") or "",
        "weighted_value": float(d.get("weighted_value") or 0),
    }


def _extract_id(val):
    """Extrai ID numérico de campo que pode ser int ou dict {id: ...}."""
    if val is None:
        return None
    if isinstance(val, dict):
        return val.get("id")
    return val


def norm_activity(a: dict) -> dict:
    return {
        "id":                  a.get("id"),
        "type":                a.get("type", ""),
        "subject":             a.get("subject", ""),
        "done":                bool(a.get("done")),
        "due_date":            a.get("due_date", ""),
        "due_time":            a.get("due_time", ""),
        "deal_id":             _extract_id(a.get("deal_id")),
        "user_id":             _extract_id(a.get("user_id")),
        "assigned_to_user_id": _extract_id(a.get("assigned_to_user_id")),
        "created_by_user_id":  _extract_id(a.get("created_by_user_id")),
        "add_time":            a.get("add_time", ""),
        "marked_as_done_time": a.get("marked_as_done_time") or "",
    }


def norm_stage(s: dict) -> dict:
    return {
        "id":          s.get("id"),
        "name":        s.get("name", ""),
        "pipeline_id": s.get("pipeline_id"),
        "order_nr":    s.get("order_nr", 0),
        "active_flag": s.get("active_flag", True),
    }


def norm_user(u: dict) -> dict:
    return {
        "id":          u.get("id"),
        "name":        u.get("name", ""),
        "active_flag": u.get("active_flag", True),
    }


# ── Busca completa ─────────────────────────────────────────────────────────────
def fetch_all_data() -> dict:
    # 1. Etapas dos pipelines
    stages_raw = []
    for pid in [PRESALES_PIPELINE_ID, SALES_PIPELINE_ID]:
        data = pd_get("stages", {"pipeline_id": pid})
        stages_raw.extend(data.get("data") or [])
    stages     = [norm_stage(s) for s in stages_raw]
    stages_map = {s["id"]: s for s in stages}

    # 2. Deals — ambos os pipelines, todos os status
    deals = []
    for pid in [PRESALES_PIPELINE_ID, SALES_PIPELINE_ID]:
        for status in ("open", "won", "lost"):
            batch = pd_get_all("deals", {"pipeline_id": pid, "status": status})
            deals.extend(norm_deal(d, pid, stages_map) for d in batch)

    # 3. Usuários
    users = [norm_user(u) for u in (pd_get("users").get("data") or [])]

    # 4. Atividades (últimos 365 dias, pendentes e concluídas)
    # Mapa deal_id → owner_id para enriquecer cada atividade com o dono do deal
    deal_owner_map = {str(d["id"]): str(d["owner_id"]) for d in deals if d.get("id") and d.get("owner_id")}

    since = (datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%d")
    activities = []
    for done in (0, 1):
        batch = pd_get_all("activities", {"done": done, "start_date": since})
        for a in batch:
            act = norm_activity(a)
            # Enriquece com o owner do deal vinculado (se existir)
            deal_id = str(act["deal_id"]) if act.get("deal_id") else None
            act["deal_owner_id"] = deal_owner_map.get(deal_id) if deal_id else None
            activities.append(act)

    return {
        "ok":          True,
        "deals":       deals,
        "activities":  activities,
        "users":       users,
        "stages":      stages,
        "lastUpdated": datetime.utcnow().isoformat() + "Z",
    }


# ── Rotas Flask ────────────────────────────────────────────────────────────────
def _json_resp(data, status: int = 200) -> Response:
    r = Response(json.dumps(data, default=str),
                 status=status, content_type="application/json")
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r


@app.route("/")
def index():
    return send_from_directory("dashboard", "index.html")


@app.route("/ping")
def ping():
    return "ok", 200


@app.route("/debug-activities")
def debug_activities():
    """Retorna amostra de atividades brutas do Pipedrive para diagnóstico."""
    from flask import jsonify as _jsonify
    since = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    raw = pd_get_all("activities", {"done": 0, "start_date": since, "limit": 5})
    raw += pd_get_all("activities", {"done": 1, "start_date": since, "limit": 5})
    sample = []
    for a in raw[:10]:
        sample.append({
            "id":       a.get("id"),
            "subject":  a.get("subject"),
            "user_id":  a.get("user_id"),
            "deal_id":  a.get("deal_id"),
            "due_date": a.get("due_date"),
            "done":     a.get("done"),
            "assigned_to_user_id": a.get("assigned_to_user_id"),
            "created_by_user_id":  a.get("created_by_user_id"),
        })
    users = pd_get("users")
    user_list = [{"id": u.get("id"), "name": u.get("name")} for u in (users.get("data") or [])]
    return _jsonify({"activities_sample": sample, "users": user_list})


@app.route("/sheets-proxy")
def sheets_proxy():
    global _cache

    if not PIPEDRIVE_API_KEY:
        return _json_resp(
            {"ok": False, "error": "PIPEDRIVE_API_KEY não configurada no servidor."},
            503
        )

    # ?force=1 → ignora cache (botão Atualizar)
    force = request.args.get("force") == "1"

    if not force and _cache["data"] and (time.time() - _cache["ts"]) < CACHE_TTL:
        return _json_resp(_cache["data"])

    try:
        data   = fetch_all_data()
        _cache = {"data": data, "ts": time.time()}
        return _json_resp(data)

    except requests.exceptions.Timeout:
        if _cache["data"]:
            # Retorna cache antigo com aviso
            stale = dict(_cache["data"])
            stale["_stale"] = True
            return _json_resp(stale)
        return _json_resp(
            {"ok": False, "error": "Pipedrive demorou para responder. Tente novamente em alguns segundos."},
            503
        )
    except requests.exceptions.RequestException as e:
        if _cache["data"]:
            return _json_resp(_cache["data"])
        return _json_resp(
            {"ok": False, "error": f"Erro de conexão com Pipedrive: {str(e)[:150]}"},
            503
        )
    except Exception as e:
        return _json_resp(
            {"ok": False, "error": f"Erro interno: {str(e)[:200]}"},
            500
        )


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory("dashboard", filename)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

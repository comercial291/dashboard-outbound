#!/usr/bin/env python3
"""
Dashboard Outbound Pró Vendas — Sincronização Pipedrive → Google Sheets
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Busca TODOS os dados dos funis de Pré-Vendas e Vendas e envia
para a planilha do Google Sheets via Apps Script Web App.

Agendamento recomendado: Task Scheduler Windows — diariamente às 08h e 17h
"""

import os
import json
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Configuração ───────────────────────────────────────────────────────────────
PIPEDRIVE_API_KEY    = os.getenv("PIPEDRIVE_API_KEY")
WEBAPP_URL           = os.getenv("WEBAPP_URL")
WEBAPP_SECRET        = os.getenv("WEBAPP_SECRET", "routesales2026")
PRESALES_PIPELINE_ID = int(os.getenv("PRESALES_PIPELINE_ID", "1"))
SALES_PIPELINE_ID    = int(os.getenv("SALES_PIPELINE_ID",    "2"))

PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler("sync_outbound_log.txt", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ── Pipedrive API ──────────────────────────────────────────────────────────────
def pd_get(endpoint: str, params: dict = None) -> dict:
    url = f"{PIPEDRIVE_BASE}/{endpoint}"
    p = {"api_token": PIPEDRIVE_API_KEY, "limit": 500, **(params or {})}
    resp = requests.get(url, params=p, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Pipedrive erro: {data}")
    return data


def pd_get_all(endpoint: str, params: dict = None) -> list:
    """Percorre todas as páginas e retorna lista completa."""
    items, start = [], 0
    while True:
        data = pd_get(endpoint, {**(params or {}), "start": start})
        batch = data.get("data") or []
        items.extend(batch)
        pg = (data.get("additional_data") or {}).get("pagination") or {}
        if not pg.get("more_items_in_collection"):
            break
        start += 500
    return items


# ── Fetch functions ────────────────────────────────────────────────────────────
def fetch_stages(pipeline_ids: list) -> tuple[list, dict]:
    """Retorna (lista_normalizada, mapa_id→stage)"""
    log.info("  Buscando etapas dos pipelines %s…", pipeline_ids)
    raw = []
    for pid in pipeline_ids:
        data = pd_get("stages", {"pipeline_id": pid})
        raw.extend(data.get("data") or [])
    log.info("    %d etapas encontradas", len(raw))
    stages = [_norm_stage(s) for s in raw]
    return stages, {s["id"]: s for s in stages}


def fetch_users() -> list:
    log.info("  Buscando usuários…")
    data = pd_get("users")
    raw  = data.get("data") or []
    log.info("    %d usuários", len(raw))
    return [_norm_user(u) for u in raw]


def fetch_deals(pipeline_id: int, stages_map: dict) -> list:
    log.info("  Buscando deals do pipeline %d…", pipeline_id)
    all_deals = []
    for status in ("open", "won", "lost"):
        batch = pd_get_all("deals", {"pipeline_id": pipeline_id, "status": status})
        log.info("    %-6s %d deals", status, len(batch))
        all_deals.extend(_norm_deal(d, pipeline_id, stages_map) for d in batch)
    return all_deals


def fetch_activities() -> list:
    log.info("  Buscando atividades…")
    items = []
    for done in (0, 1):
        batch = pd_get_all("activities", {"done": done})
        items.extend(_norm_activity(a) for a in batch)
    log.info("    %d atividades no total", len(items))
    return items


# ── Normalizers ────────────────────────────────────────────────────────────────
def _v(obj, key, fallback=""):
    """Extrai valor de campo simples ou de objeto aninhado."""
    val = obj.get(key)
    if isinstance(val, dict):
        return val.get("value") or val.get("id") or fallback
    return val if val is not None else fallback


def _name(obj, key, fallback=""):
    """Extrai nome de campo objeto aninhado (ex: owner_id.name)."""
    val = obj.get(key)
    if isinstance(val, dict):
        return val.get("name", fallback)
    return fallback


def _norm_deal(d: dict, pipeline_id: int, stages_map: dict) -> dict:
    sid   = d.get("stage_id")
    stage = stages_map.get(sid, {})
    return {
        "id":                    d.get("id"),
        "title":                 d.get("title", ""),
        "pipeline_id":           pipeline_id,
        "pipeline_name":         d.get("pipeline_name", ""),
        "stage_id":              sid,
        "stage_name":            stage.get("name", d.get("stage_name", "")),
        "stage_order":           stage.get("order_nr", 0),
        "status":                d.get("status", ""),
        "value":                 float(d.get("value") or 0),
        "currency":              d.get("currency", "BRL"),
        "owner_id":              _v(d, "owner_id"),
        "owner_name":            _name(d, "owner_id"),
        "org_id":                _v(d, "org_id"),
        "org_name":              _name(d, "org_id"),
        "person_id":             _v(d, "person_id"),
        "person_name":           _name(d, "person_id"),
        "add_time":              d.get("add_time", ""),
        "update_time":           d.get("update_time", ""),
        "close_time":            d.get("close_time", ""),
        "won_time":              d.get("won_time", ""),
        "lost_time":             d.get("lost_time", ""),
        "lost_reason":           d.get("lost_reason", ""),
        "expected_close_date":   d.get("expected_close_date", ""),
        "weighted_value":        float(d.get("weighted_value") or 0),
        "activities_count":      int(d.get("activities_count") or 0),
        "done_activities_count": int(d.get("done_activities_count") or 0),
        "undone_activities_count": int(d.get("undone_activities_count") or 0),
    }


def _norm_activity(a: dict) -> dict:
    return {
        "id":                 a.get("id"),
        "type":               a.get("type", ""),
        "subject":            a.get("subject", ""),
        "done":               bool(a.get("done")),
        "due_date":           a.get("due_date", ""),
        "due_time":           a.get("due_time", ""),
        "duration":           a.get("duration", ""),
        "deal_id":            a.get("deal_id"),
        "person_id":          a.get("person_id"),
        "org_id":             a.get("org_id"),
        "user_id":            a.get("user_id"),
        "note":               (a.get("note") or "")[:500],   # limita tamanho
        "add_time":           a.get("add_time", ""),
        "marked_as_done_time": a.get("marked_as_done_time", ""),
    }


def _norm_user(u: dict) -> dict:
    return {
        "id":          u.get("id"),
        "name":        u.get("name", ""),
        "email":       u.get("email", ""),
        "role_id":     u.get("role_id"),
        "active_flag": bool(u.get("active_flag", True)),
    }


def _norm_stage(s: dict) -> dict:
    return {
        "id":            s.get("id"),
        "name":          s.get("name", ""),
        "pipeline_id":   s.get("pipeline_id"),
        "pipeline_name": s.get("pipeline_name", ""),
        "order_nr":      s.get("order_nr", 0),
        "active_flag":   bool(s.get("active_flag", True)),
    }


# ── Google Sheets sender ───────────────────────────────────────────────────────
def send_to_sheets(data_type: str, data: list) -> dict:
    payload = {
        "secret": WEBAPP_SECRET,
        "type":   data_type,
        "data":   data,
        "mode":   "replace",
    }
    resp = requests.post(WEBAPP_URL, json=payload, timeout=180)
    resp.raise_for_status()
    return resp.json()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 65)
    log.info("Dashboard Outbound — Sincronização  %s", datetime.now().isoformat())
    log.info("=" * 65)

    # 1. Etapas (necessário antes de normalizar deals)
    log.info("► Etapas")
    stages, stages_map = fetch_stages([PRESALES_PIPELINE_ID, SALES_PIPELINE_ID])

    # 2. Usuários
    log.info("► Usuários")
    users = fetch_users()

    # 3. Deals Pré-Vendas
    log.info("► Deals Pré-Vendas (pipeline %d)", PRESALES_PIPELINE_ID)
    presales_deals = fetch_deals(PRESALES_PIPELINE_ID, stages_map)

    # 4. Deals Vendas
    log.info("► Deals Vendas (pipeline %d)", SALES_PIPELINE_ID)
    sales_deals = fetch_deals(SALES_PIPELINE_ID, stages_map)

    all_deals = presales_deals + sales_deals
    log.info("  Total combinado: %d deals", len(all_deals))

    # 5. Atividades
    log.info("► Atividades")
    activities = fetch_activities()

    # 6. Envia para Google Sheets
    log.info("► Enviando para Google Sheets…")
    datasets = [
        ("stages",     stages),
        ("users",      users),
        ("deals",      all_deals),
        ("activities", activities),
    ]

    all_ok = True
    for dtype, data in datasets:
        log.info("  Enviando %-12s (%d registros)…", dtype, len(data))
        try:
            result = send_to_sheets(dtype, data)
            if result.get("ok"):
                log.info("  ✓ %-12s %d registros gravados", dtype, result.get("updated", 0))
            else:
                log.error("  ✗ %-12s Erro: %s", dtype, result.get("error"))
                all_ok = False
        except Exception as exc:
            log.error("  ✗ %-12s Exceção: %s", dtype, exc)
            all_ok = False

    status = "✅ Concluída com sucesso" if all_ok else "⚠️  Concluída com erros"
    log.info("%s — %s\n", status, datetime.now().isoformat())


if __name__ == "__main__":
    if not PIPEDRIVE_API_KEY:
        raise EnvironmentError("PIPEDRIVE_API_KEY não definida no .env")
    if not WEBAPP_URL:
        raise EnvironmentError("WEBAPP_URL não definida no .env")
    main()

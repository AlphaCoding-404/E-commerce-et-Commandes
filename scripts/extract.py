"""Étape 1 - EXTRACTION : 2 sources différentes.

- `customers` et `products` : API REST publique DummyJSON (https://dummyjson.com)
- `orders`                  : fichier CSV (export des commandes de la boutique)

Chaque source est écrite en zone de staging (data/staging/<source>_raw.csv).
"""
import json
import logging

import pandas as pd
import requests

from scripts.config import (API_BASE_URL, API_ENDPOINTS, API_TIMEOUT, ORDERS_CSV,
                            RAW_DIR, STAGING_DIR)

log = logging.getLogger(__name__)


def _fetch_api(source: str):
    """Appelle l'API. Si elle est injoignable, utilise le dernier snapshot local (data/raw/api_<source>.json).
    Sans snapshot, l'exception remonte : Airflow relance alors la tâche (retries)."""
    cfg = API_ENDPOINTS[source]
    url = f"{API_BASE_URL}{cfg['path']}"
    cache = RAW_DIR / f"api_{source}.json"
    try:
        resp = requests.get(url, params={"limit": 0, "select": cfg["select"]}, timeout=API_TIMEOUT)
        resp.raise_for_status()
        items = resp.json()[cfg["key"]]
        if not items:
            raise ValueError("réponse API vide")
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
        log.info("[extract] GET %s -> %d enregistrements (snapshot : %s)", url, len(items), cache.name)
        return items, "api"
    except (requests.RequestException, KeyError, ValueError) as exc:
        if cache.exists():
            log.warning("[extract] API indisponible (%s). Utilisation du snapshot %s", exc, cache)
            return json.loads(cache.read_text(encoding="utf-8")), "api-cache"
        log.error("[extract] API indisponible (%s) et aucun snapshot local.", exc)
        raise


def _products_frame(items):
    return pd.DataFrame([{"product_id": i["id"], "product_name": i["title"],
                          "category": i["category"], "price": i["price"]} for i in items])


def _customers_frame(items):
    rows = []
    for u in items:
        addr = u.get("address") or {}
        rows.append({"customer_id": u["id"],
                     "customer_name": f"{u.get('firstName', '')} {u.get('lastName', '')}".strip(),
                     "email": u.get("email"), "country": addr.get("country"),
                     "state": addr.get("state"), "city": addr.get("city")})
    return pd.DataFrame(rows)


def extract_source(source: str) -> dict:
    """Extrait une source et renvoie des métadonnées (transmises à la tâche suivante par XCom)."""
    if source == "orders":
        if not ORDERS_CSV.exists():
            raise FileNotFoundError(f"{ORDERS_CSV} introuvable. Lancez : python -m scripts.generate_data")
        df, origin = pd.read_csv(ORDERS_CSV, dtype=str), "csv"
    elif source == "products":
        items, origin = _fetch_api(source)
        df = _products_frame(items)
    elif source == "customers":
        items, origin = _fetch_api(source)
        df = _customers_frame(items)
    else:
        raise ValueError(f"Source inconnue : {source}")

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / f"{source}_raw.csv"
    df.to_csv(out, index=False)
    log.info("[extract] %s (%s) : %d lignes, colonnes=%s -> %s", source, origin, len(df), list(df.columns), out)
    return {"source": source, "path": str(out), "rows": int(len(df)), "origin": origin}

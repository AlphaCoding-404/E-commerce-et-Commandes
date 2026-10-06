"""Étape 4 - CHARGEMENT dans Elasticsearch (index + mapping + bulk)."""
import json
import logging
from pathlib import Path

from elasticsearch import Elasticsearch, helpers

from scripts.config import DEFAULT_INDEX, ES_CONFIG_DIR, ES_CONN_ID, ES_FALLBACK_HOST

log = logging.getLogger(__name__)


def get_es_client() -> Elasticsearch:
    """Client Elasticsearch construit à partir de la Connection Airflow `elasticsearch_default`.
    Hors Airflow (tests en local), on retombe sur la variable d'environnement ES_HOST."""
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(ES_CONN_ID)
        scheme = "https" if conn.schema == "https" else "http"
        url = f"{scheme}://{conn.host}:{conn.port or 9200}"
        log.info("Connexion Airflow '%s' -> %s", ES_CONN_ID, url)
    except Exception as exc:  # Airflow absent ou connexion non définie
        url = ES_FALLBACK_HOST
        log.warning("Connexion Airflow indisponible (%s). Repli sur %s", exc, url)
    return Elasticsearch(url, request_timeout=30)


def create_index_if_needed(es: Elasticsearch, index: str) -> bool:
    if es.indices.exists(index=index):
        log.info("[load] L'index '%s' existe déjà.", index)
        return False
    definition = json.loads((ES_CONFIG_DIR / "mapping.json").read_text(encoding="utf-8"))
    es.indices.create(index=index, settings=definition["settings"], mappings=definition["mappings"])
    log.info("[load] Index '%s' créé avec le mapping défini dans mapping.json.", index)
    return True


def load_orders(path: str, index: str = DEFAULT_INDEX) -> dict:
    es = get_es_client()
    if not es.ping():
        raise ConnectionError("Elasticsearch ne répond pas.")
    create_index_if_needed(es, index)

    def actions():
        with open(path, encoding="utf-8") as f:
            for line in f:
                doc = json.loads(line)
                # _id = order_id  ->  chargement idempotent (relancer le DAG n'ajoute pas de doublons)
                yield {"_index": index, "_id": doc["order_id"], "_source": doc}

    ok, failed = helpers.bulk(es, actions(), chunk_size=1000, stats_only=True, raise_on_error=False)
    es.indices.refresh(index=index)
    total = es.count(index=index)["count"]
    log.info("[load] %d documents indexés, %d erreurs. Total dans '%s' : %d", ok, failed, index, total)
    if failed:
        raise RuntimeError(f"{failed} documents n'ont pas pu être indexés.")
    return {"index": index, "indexed": int(ok), "doc_count": int(total)}

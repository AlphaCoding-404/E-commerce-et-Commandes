"""Étape 5 - ANALYSE : exécute les requêtes d'agrégation de elasticsearch/queries.json."""
import json
import logging

from scripts.config import DATA_DIR, DEFAULT_INDEX, ES_CONFIG_DIR
from scripts.load import get_es_client

log = logging.getLogger(__name__)


def simplify(node):
    """Transforme la réponse brute d'agrégations ES en structure lisible."""
    if isinstance(node, dict):
        if "buckets" in node:
            out = []
            for b in node["buckets"]:
                item = {"key": b.get("key_as_string", b.get("key")), "doc_count": b["doc_count"]}
                for k, v in b.items():
                    if k not in ("key", "key_as_string", "doc_count"):
                        item[k] = simplify(v)
                out.append(item)
            return out
        if "value" in node:
            return node["value"]
    return node


def run_analysis(index: str = DEFAULT_INDEX) -> dict:
    es = get_es_client()
    queries = json.loads((ES_CONFIG_DIR / "queries.json").read_text(encoding="utf-8"))["queries"]
    results = {}
    for q in queries:
        resp = es.search(index=index, **q["body"])
        results[q["name"]] = {
            "question": q["question"],
            "total_hits": resp["hits"]["total"]["value"],
            "result": {name: simplify(agg) for name, agg in resp.get("aggregations", {}).items()},
        }
        log.info("[analyze] %s\n   Q : %s\n   R : %s", q["name"], q["question"],
                 json.dumps(results[q["name"]]["result"], ensure_ascii=False)[:600])

    out = DATA_DIR / "analysis_results.json"
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("[analyze] Résultats sauvegardés dans %s", out)
    return {"queries_run": len(results), "output": str(out)}

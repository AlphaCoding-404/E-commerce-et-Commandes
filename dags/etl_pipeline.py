"""DAG Airflow : pipeline ETL e-commerce
Extract -> Transform -> Validate -> Load (Elasticsearch) -> Analyze
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from scripts.analyze import run_analysis
from scripts.config import SOURCES
from scripts.extract import extract_source
from scripts.generate_data import ensure_source_data
from scripts.load import load_orders
from scripts.transform import transform
from scripts.validate import validate_orders

log = logging.getLogger(__name__)


def notify_failure(context):
    """Notification simple : écrit l'échec dans les logs (remplaçable par un e-mail / Slack)."""
    ti = context["task_instance"]
    log.error("ECHEC - DAG=%s tâche=%s run=%s tentative=%s",
              ti.dag_id, ti.task_id, context["run_id"], ti.try_number)


default_args = {
    "owner": "data-engineering",
    "retries": 2,                          # retries en cas d'échec
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
}


# --- Fonctions appelées par les PythonOperator (elles utilisent XCom) -----------------
def _transform(ti, ds, **_):
    # XCom : on récupère les chemins renvoyés par les 3 tâches d'extraction
    results = ti.xcom_pull(task_ids=[f"extract.extract_{s}" for s in SOURCES])
    paths = {r["source"]: r["path"] for r in results}
    return transform(paths, run_date=ds)   # ds = date logique d'exécution (template Airflow)


def _validate(ti, **_):
    t = ti.xcom_pull(task_ids="transform")
    return validate_orders(t["path"], raw_rows=t["rows_in"])


def _load(ti, index, **_):
    t = ti.xcom_pull(task_ids="transform")
    return load_orders(t["path"], index=index)


def _analyze(index, **_):
    return run_analysis(index=index)


with DAG(
    dag_id="ecommerce_etl_pipeline",
    description="ETL e-commerce : CSV -> nettoyage -> contrôle qualité -> Elasticsearch -> Kibana",
    schedule="@daily",                     # planification quotidienne
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "etl", "elasticsearch"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate = PythonOperator(task_id="ensure_source_data", python_callable=ensure_source_data)

    with TaskGroup(group_id="extract") as extract_group:
        for source in SOURCES:
            PythonOperator(
                task_id=f"extract_{source}",
                python_callable=extract_source,
                op_kwargs={"source": source},
            )

    transform_task = PythonOperator(task_id="transform", python_callable=_transform)
    validate_task = PythonOperator(task_id="validate", python_callable=_validate)

    # Variable Airflow `es_index` lue via un template Jinja (valeur par défaut : ecommerce_orders)
    index_template = "{{ var.value.get('es_index', 'ecommerce_orders') }}"

    load_task = PythonOperator(task_id="load", python_callable=_load, op_kwargs={"index": index_template})
    analyze_task = PythonOperator(task_id="analyze", python_callable=_analyze, op_kwargs={"index": index_template})

    end = EmptyOperator(task_id="end")

    start >> generate >> extract_group >> transform_task >> validate_task >> load_task >> analyze_task >> end

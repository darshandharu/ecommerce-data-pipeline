"""End-to-end E-Commerce pipeline DAG.

Flow:
    ingest_bronze
        -> transform_silver
            -> data_quality_gate
                -> build_gold (+ BigQuery load)
                    -> dbt_run
                        -> dbt_test
                            -> refresh_dashboard_audit
                                -> notify_success

Includes retry logic, SLA monitoring, and email alerts on failure.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from common_callbacks import notify_failure, notify_success, sla_miss_callback

PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow/project")
CONFIG = f"{PROJECT_DIR}/configs/pipeline_config.yaml"
DBT_DIR = f"{PROJECT_DIR}/dbt"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.getenv("AIRFLOW_ALERT_EMAIL", "data-alerts@example.com")],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}


def spark_submit(module: str, extra: str = "") -> str:
    """Build a python -m command for a Spark job module."""
    return (
        f"cd {PROJECT_DIR} && "
        f"python -m {module} --config {CONFIG} {extra}".strip()
    )


with DAG(
    dag_id="ecommerce_end_to_end_pipeline",
    description="Bronze -> Silver -> DQ -> Gold -> dbt for the Olist dataset",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",          # daily at 03:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=sla_miss_callback,
    tags=["ecommerce", "etl", "pyspark", "dbt", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=spark_submit("pyspark_jobs.bronze.ingest_raw"),
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=spark_submit("pyspark_jobs.silver.run_silver"),
    )

    data_quality_gate = BashOperator(
        task_id="data_quality_gate",
        bash_command=spark_submit(
            "pyspark_jobs.data_quality.run_dq", extra="--layer silver"
        ),
        # a FAIL-severity breach raises -> task fails -> pipeline stops
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=spark_submit("pyspark_jobs.gold.run_gold"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --target prod",
    )

    refresh_audit = BashOperator(
        task_id="refresh_dashboard_audit",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -m monitoring.publish_audit --config {CONFIG}"
        ),
    )

    finish = EmptyOperator(
        task_id="notify_success",
        on_success_callback=notify_success,
    )

    (
        start
        >> ingest_bronze
        >> transform_silver
        >> data_quality_gate
        >> build_gold
        >> dbt_run
        >> dbt_test
        >> refresh_audit
        >> finish
    )

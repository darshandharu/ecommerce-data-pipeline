"""Daily CDC DAG.

Each run picks up the CDC batch for its logical date, MERGEs it into the
Silver layer, re-validates data quality on the changed tables, then rebuilds
Gold so downstream marts reflect the increment.

Flow:
    apply_cdc -> dq_recheck -> rebuild_gold -> dbt_run_incremental -> notify
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from common_callbacks import notify_failure, notify_success

PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow/project")
CONFIG = f"{PROJECT_DIR}/configs/pipeline_config.yaml"
DBT_DIR = f"{PROJECT_DIR}/dbt"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": [os.getenv("AIRFLOW_ALERT_EMAIL", "data-alerts@example.com")],
    "sla": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="ecommerce_cdc_daily",
    description="Apply daily CDC increments (upsert + soft delete) to Silver",
    start_date=datetime(2018, 1, 1),
    schedule="0 5 * * *",          # daily at 05:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "cdc", "incremental"],
) as dag:

    start = EmptyOperator(task_id="start")

    # {{ ds }} = the run's logical date, matching the CDC file folder name
    apply_cdc = BashOperator(
        task_id="apply_cdc",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -m pyspark_jobs.cdc.cdc_processor "
            f"--config {CONFIG} --date {{{{ ds }}}}"
        ),
    )

    dq_recheck = BashOperator(
        task_id="dq_recheck",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -m pyspark_jobs.data_quality.run_dq "
            f"--config {CONFIG} --layer silver"
        ),
    )

    rebuild_gold = BashOperator(
        task_id="rebuild_gold",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python -m pyspark_jobs.gold.run_gold --config {CONFIG}"
        ),
    )

    dbt_incremental = BashOperator(
        task_id="dbt_run_incremental",
        bash_command=f"cd {DBT_DIR} && dbt run --target prod",
    )

    finish = EmptyOperator(task_id="notify_success",
                           on_success_callback=notify_success)

    start >> apply_cdc >> dq_recheck >> rebuild_gold >> dbt_incremental >> finish

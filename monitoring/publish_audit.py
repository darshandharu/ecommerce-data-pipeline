"""Publish the locally-written audit Parquet (run log + DQ results) to the
BigQuery ``audit`` dataset so the warehouse-side dashboard views work.

Safe to run locally without GCP — it logs and exits when BQ is not configured.

Run::

    python -m monitoring.publish_audit --config configs/pipeline_config.yaml
"""
from __future__ import annotations

import argparse

from pyspark_jobs.common.bq_loader import load_to_bigquery
from pyspark_jobs.common.config_loader import load_config
from pyspark_jobs.common.io_utils import read_parquet
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.common.spark_session import get_spark

log = get_logger(__name__)

AUDIT_TABLES = ["pipeline_run_log", "dq_results"]


def main(config_path: str) -> None:
    config = load_config(config_path)
    spark = get_spark(config)
    audit_path = config["paths"].get("gold", "./data/gold").replace("gold", "audit")
    try:
        for table in AUDIT_TABLES:
            path = f"{audit_path.rstrip('/')}/{table}"
            try:
                df = read_parquet(spark, path)
            except Exception:
                log.warning("no local audit data at %s — skipping", path)
                continue
            load_to_bigquery(df, table, config, dataset_key="audit", mode="append")
            log.info("published %s (%d rows) to audit dataset", table, df.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish audit data to BigQuery")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    args = parser.parse_args()
    main(args.config)

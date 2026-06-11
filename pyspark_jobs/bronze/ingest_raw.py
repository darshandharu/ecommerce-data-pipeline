"""Bronze ingestion: read every source CSV and persist it to the Bronze
Parquet layer with declared schemas and lineage columns.

Run::

    python -m pyspark_jobs.bronze.ingest_raw --config configs/pipeline_config.yaml
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from pyspark_jobs.common.audit import new_run_id, track_stage, write_run_log
from pyspark_jobs.common.config_loader import load_config
from pyspark_jobs.common.io_utils import build_struct, read_csv, write_parquet
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.common.spark_session import get_spark

log = get_logger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "configs" / "tables_schema.yaml"


def ingest_source(spark, config, schemas, source, run_id, run_records):
    """Ingest a single source table into Bronze."""
    name = source["name"]
    raw_dir = config["paths"]["raw"]
    bronze_dir = config["paths"]["bronze"]
    csv_path = f"{raw_dir.rstrip('/')}/{source['file']}"
    out_path = f"{bronze_dir.rstrip('/')}/{name}"

    with track_stage(run_id, "bronze", name, run_records) as rec:
        struct = build_struct(schemas[name])
        df = read_csv(spark, csv_path, schema=struct)
        df = (
            df.withColumn("_run_id", F.lit(run_id))
            .withColumn("_source_file", F.lit(source["file"]))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.current_date())
        )
        rec["rows_out"] = df.count()
        # partition by ingest date to support incremental/CDC reads later
        write_parquet(df, out_path, mode="overwrite", partition_by=["_ingest_date"])
        log.info("BRONZE %-22s rows=%s -> %s", name, rec["rows_out"], out_path)
    return rec["rows_out"]


def main(config_path: str) -> None:
    config = load_config(config_path)
    schemas = load_config(SCHEMA_FILE)
    spark = get_spark(config)
    run_id = new_run_id()
    run_records: list[dict] = []
    log.info("=== BRONZE INGESTION START | run_id=%s ===", run_id)

    try:
        total = 0
        for source in config["sources"]:
            total += ingest_source(spark, config, schemas, source, run_id, run_records)
        log.info("=== BRONZE COMPLETE | %d tables, %d total rows ===",
                 len(config["sources"]), total)
    finally:
        audit_path = config["paths"].get("gold", "./data/gold").replace("gold", "audit")
        write_run_log(spark, audit_path, run_records)
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze raw ingestion")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    args = parser.parse_args()
    main(args.config)

"""Change Data Capture processor.

Simulates a real CDC feed: each day a file arrives containing INSERTs,
UPDATEs and DELETEs (operation flagged in column ``op``). This job MERGEs that
batch into the existing Silver table, keeping only the latest version of each
primary key and soft-deleting removed rows.

Merge semantics (no Delta Lake required — implemented with window dedup):
    1. Union existing snapshot + incoming changes.
    2. Rank by PK ordered by ``cdc_timestamp`` desc → keep newest.
    3. Drop rows whose newest op is ``D`` (delete).

Run::

    python -m pyspark_jobs.cdc.cdc_processor \
        --config configs/pipeline_config.yaml --table orders --date 2018-01-02
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pyspark_jobs.common.audit import new_run_id, track_stage, write_run_log
from pyspark_jobs.common.config_loader import get_source, load_config
from pyspark_jobs.common.exceptions import CDCError
from pyspark_jobs.common.io_utils import read_parquet, write_parquet
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.common.spark_session import get_spark

log = get_logger(__name__)

CDC_META = ["op", "cdc_timestamp"]


def read_cdc_batch(spark, cdc_dir, table, date) -> DataFrame:
    """Read one day's CDC file for a table: <cdc>/<date>/<table>.csv."""
    path = f"{cdc_dir.rstrip('/')}/{date}/{table}.csv"
    return (
        spark.read.option("header", True).option("inferSchema", True).csv(path)
    )


def apply_merge(current: DataFrame, changes: DataFrame, keys: list[str]) -> DataFrame:
    """Upsert + soft-delete merge of ``changes`` into ``current``."""
    if not keys:
        raise CDCError("CDC requires a primary key to merge on")

    # tag existing snapshot with neutral op so unchanged rows survive ranking
    if "op" not in current.columns:
        current = current.withColumn("op", F.lit("I"))
    if "cdc_timestamp" not in current.columns:
        current = current.withColumn("cdc_timestamp", F.lit("1970-01-01 00:00:00"))

    # align columns
    cols = [c for c in current.columns if c in changes.columns]
    unioned = current.select(*cols).unionByName(changes.select(*cols))

    w = Window.partitionBy(*keys).orderBy(F.col("cdc_timestamp").desc())
    latest = (
        unioned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    # remove rows whose latest operation is a delete
    return latest.filter(F.col("op") != "D")


def process(spark, config, table, date, run_records, run_id) -> dict:
    source = get_source(config, table)
    keys = source.get("primary_key", [])
    silver_path = f"{config['paths']['silver'].rstrip('/')}/{table}"

    with track_stage(run_id, "cdc", table, run_records) as rec:
        changes = read_cdc_batch(spark, config["paths"]["cdc"], table, date)
        rec["rows_in"] = changes.count()
        inserts = changes.filter(F.col("op") == "I").count()
        updates = changes.filter(F.col("op") == "U").count()
        deletes = changes.filter(F.col("op") == "D").count()

        try:
            current = read_parquet(spark, silver_path)
        except Exception:
            log.warning("CDC: no existing snapshot for %s — treating as first load", table)
            current = changes.limit(0)

        merged = apply_merge(current, changes, keys)
        rec["rows_out"] = merged.count()
        # write to a dated path then swap is cleaner; here overwrite snapshot
        write_parquet(merged, silver_path, mode="overwrite")
        rec["message"] = f"I={inserts} U={updates} D={deletes}"
        log.info("CDC %-16s date=%s I=%d U=%d D=%d -> rows=%s",
                 table, date, inserts, updates, deletes, rec["rows_out"])

    return {"table": table, "date": date, "inserts": inserts,
            "updates": updates, "deletes": deletes, "final_rows": rec["rows_out"]}


def main(config_path: str, date: str, tables: list[str] | None = None) -> None:
    config = load_config(config_path)
    spark = get_spark(config)
    run_id = new_run_id()
    run_records: list[dict] = []

    cdc_tables = tables or [s["name"] for s in config["sources"] if s.get("cdc_enabled")]
    log.info("=== CDC START | run_id=%s date=%s tables=%s ===", run_id, date, cdc_tables)
    try:
        for table in cdc_tables:
            process(spark, config, table, date, run_records, run_id)
        log.info("=== CDC COMPLETE for %s ===", date)
    finally:
        audit_path = config["paths"].get("gold", "./data/gold").replace("gold", "audit")
        write_run_log(spark, audit_path, run_records)
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply a daily CDC batch")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    parser.add_argument("--date", required=True, help="batch date YYYY-MM-DD")
    parser.add_argument("--table", nargs="*", help="subset of cdc-enabled tables")
    args = parser.parse_args()
    main(args.config, args.date, args.table)

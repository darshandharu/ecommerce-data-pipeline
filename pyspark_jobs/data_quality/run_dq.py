"""DQ entrypoint: load a layer's tables, run the declarative rules, persist
results to the audit table, and (optionally) fail the pipeline on FAILs.

Run::

    python -m pyspark_jobs.data_quality.run_dq \
        --config configs/pipeline_config.yaml --layer silver
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Row

from pyspark_jobs.common.audit import new_run_id
from pyspark_jobs.common.config_loader import load_config
from pyspark_jobs.common.exceptions import DataQualityError
from pyspark_jobs.common.io_utils import read_parquet
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.common.spark_session import get_spark
from pyspark_jobs.data_quality import framework as fw

log = get_logger(__name__)

DQ_RULES_FILE = Path(__file__).resolve().parents[2] / "configs" / "dq_rules.yaml"


def load_frames(spark, config, layer, rules):
    """Load every table referenced by the rules (and their ref_tables)."""
    base = config["paths"][layer].rstrip("/")
    needed = set(rules.keys())
    for table_rules in rules.values():
        for r in table_rules:
            if r["check"] == "referential":
                needed.add(r["ref_table"])

    frames = {}
    for name in needed:
        path = f"{base}/{name}"
        try:
            frames[name] = read_parquet(spark, path)
        except Exception as exc:
            log.warning("DQ: could not load %s (%s)", path, exc)
    return frames


def write_results(spark, audit_path, results):
    if not results:
        return
    rows = [Row(**r.to_row()) for r in results]
    df = spark.createDataFrame(rows)
    (df.write.mode("append").parquet(f"{audit_path.rstrip('/')}/dq_results"))
    log.info("persisted %d DQ result rows -> %s/dq_results", len(results), audit_path)


def main(config_path: str, layer: str, fail_on_error: bool = True) -> dict:
    config = load_config(config_path)
    rules = load_config(DQ_RULES_FILE)
    spark = get_spark(config)
    run_id = new_run_id()
    audit_path = config["paths"].get("gold", "./data/gold").replace("gold", "audit")
    log.info("=== DATA QUALITY START | run_id=%s layer=%s ===", run_id, layer)

    try:
        frames = load_frames(spark, config, layer, rules)
        results = fw.run_rules(run_id, rules, frames)
        write_results(spark, audit_path, results)

        summary = fw.summarize(results)
        summary.update({"run_id": run_id, "layer": layer,
                        "executed_at": datetime.now(timezone.utc).isoformat()})
        log.info("DQ SUMMARY | %s", summary)

        if fail_on_error:
            fw.assert_no_failures(results)
        return summary
    except DataQualityError:
        log.error("DATA QUALITY GATE FAILED — see audit.dq_results")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data quality framework")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    parser.add_argument("--layer", default="silver", choices=["bronze", "silver", "gold"])
    parser.add_argument("--no-fail", action="store_true",
                        help="log failures but do not raise (report-only mode)")
    args = parser.parse_args()
    main(args.config, args.layer, fail_on_error=not args.no_fail)

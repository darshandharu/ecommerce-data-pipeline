"""Silver transformations: read Bronze, apply per-table cleaning &
standardization, run business rules, and persist conformed Silver tables.

Run::

    python -m pyspark_jobs.silver.run_silver --config configs/pipeline_config.yaml
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame

from pyspark_jobs.common.audit import new_run_id, track_stage, write_run_log
from pyspark_jobs.common.config_loader import get_source, load_config
from pyspark_jobs.common.io_utils import read_parquet, write_parquet
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.common.spark_session import get_spark
from pyspark_jobs.transformations import business_rules as br
from pyspark_jobs.transformations import cleaning as cl

log = get_logger(__name__)


# --------------------------------------------------------------------------
# Per-table cleaning recipes. Each returns a cleaned DataFrame.
# --------------------------------------------------------------------------
def clean_orders(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.lower_case(d, ["order_status"]))
        .transform(lambda d: cl.dedupe_on_keys(d, ["order_id"], "_ingested_at"))
        .transform(br.flag_invalid_orders)
        .transform(br.add_delivery_metrics)
        .transform(lambda d: cl.format_dates(d, ["order_purchase_timestamp"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "orders"))
    )


def clean_order_items(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.dedupe_on_keys(d, ["order_id", "order_item_id"], "_ingested_at"))
        .transform(lambda d: br.validate_monetary(d, ["price", "freight_value"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "order_items"))
    )


def clean_order_payments(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.lower_case(d, ["payment_type"]))
        .transform(lambda d: cl.dedupe_on_keys(d, ["order_id", "payment_sequential"], "_ingested_at"))
        .transform(lambda d: br.validate_monetary(d, ["payment_value"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "order_payments"))
    )


def clean_order_reviews(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.dedupe_on_keys(d, ["review_id"], "review_answer_timestamp"))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "order_reviews"))
    )


def clean_customers(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.lower_case(d, ["customer_city"]))
        .transform(lambda d: cl.standardize_state(d, ["customer_state"]))
        .transform(lambda d: cl.dedupe_on_keys(d, ["customer_id"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "customers"))
    )


def clean_sellers(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.lower_case(d, ["seller_city"]))
        .transform(lambda d: cl.standardize_state(d, ["seller_state"]))
        .transform(lambda d: cl.dedupe_on_keys(d, ["seller_id"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "sellers"))
    )


def clean_products(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.fill_nulls(d, {"product_category_name": "unknown"}))
        .transform(lambda d: cl.dedupe_on_keys(d, ["product_id"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "products"))
    )


def clean_geolocation(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.lower_case(d, ["geolocation_city"]))
        .transform(lambda d: cl.standardize_state(d, ["geolocation_state"]))
        .transform(cl.drop_exact_duplicates)
        .transform(lambda d: cl.add_audit_columns(d, run_id, "geolocation"))
    )


def clean_category_translation(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.transform(cl.trim_strings)
        .transform(lambda d: cl.dedupe_on_keys(d, ["product_category_name"]))
        .transform(lambda d: cl.add_audit_columns(d, run_id, "category_translation"))
    )


CLEANERS = {
    "orders": clean_orders,
    "order_items": clean_order_items,
    "order_payments": clean_order_payments,
    "order_reviews": clean_order_reviews,
    "customers": clean_customers,
    "sellers": clean_sellers,
    "products": clean_products,
    "geolocation": clean_geolocation,
    "category_translation": clean_category_translation,
}


def run_table(spark, config, name, run_id, run_records):
    bronze_dir = config["paths"]["bronze"]
    silver_dir = config["paths"]["silver"]
    with track_stage(run_id, "silver", name, run_records) as rec:
        src = read_parquet(spark, f"{bronze_dir.rstrip('/')}/{name}")
        rec["rows_in"] = src.count()
        cleaned = CLEANERS[name](src, run_id)
        rec["rows_out"] = cleaned.count()
        write_parquet(cleaned, f"{silver_dir.rstrip('/')}/{name}", mode="overwrite")
        log.info("SILVER %-22s in=%s out=%s", name, rec["rows_in"], rec["rows_out"])


def main(config_path: str, only: list[str] | None = None) -> None:
    config = load_config(config_path)
    spark = get_spark(config)
    run_id = new_run_id()
    run_records: list[dict] = []
    tables = only or [s["name"] for s in config["sources"]]
    log.info("=== SILVER START | run_id=%s tables=%s ===", run_id, tables)
    try:
        for name in tables:
            get_source(config, name)  # validate it exists
            run_table(spark, config, name, run_id, run_records)
        log.info("=== SILVER COMPLETE ===")
    finally:
        audit_path = config["paths"].get("gold", "./data/gold").replace("gold", "audit")
        write_run_log(spark, audit_path, run_records)
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver cleaning layer")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    parser.add_argument("--tables", nargs="*", help="subset of tables to process")
    args = parser.parse_args()
    main(args.config, args.tables)

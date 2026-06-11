"""Gold transformations: build the conformed star schema from Silver and load
it to BigQuery (partitioned + clustered).

Star schema produced:
    dim_customers, dim_sellers, dim_products, dim_date
    fct_orders     (grain: one row per order)
    fct_order_items(grain: one row per order item)

Run::

    python -m pyspark_jobs.gold.run_gold --config configs/pipeline_config.yaml
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark_jobs.common.audit import new_run_id, track_stage, write_run_log
from pyspark_jobs.common.bq_loader import load_to_bigquery
from pyspark_jobs.common.config_loader import load_config
from pyspark_jobs.common.io_utils import read_parquet, write_parquet
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.common.spark_session import get_spark
from pyspark_jobs.transformations import business_rules as br

log = get_logger(__name__)


def _silver(spark, config, name) -> DataFrame:
    return read_parquet(spark, f"{config['paths']['silver'].rstrip('/')}/{name}")


# -------------------------- dimensions ------------------------------------
def build_dim_customers(spark, config) -> DataFrame:
    c = _silver(spark, config, "customers")
    return c.select(
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state",
    ).dropDuplicates(["customer_id"])


def build_dim_sellers(spark, config) -> DataFrame:
    s = _silver(spark, config, "sellers")
    return s.select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ).dropDuplicates(["seller_id"])


def build_dim_products(spark, config) -> DataFrame:
    p = _silver(spark, config, "products")
    t = _silver(spark, config, "category_translation")
    return (
        p.join(t, "product_category_name", "left")
        .withColumn(
            "category",
            F.coalesce("product_category_name_english", "product_category_name"),
        )
        .select(
            "product_id", "product_category_name", "category",
            "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
        )
        .dropDuplicates(["product_id"])
    )


def build_dim_date(spark, config) -> DataFrame:
    """Date dimension spanning the order purchase range."""
    o = _silver(spark, config, "orders")
    bounds = o.select(
        F.min("order_purchase_timestamp").alias("lo"),
        F.max("order_purchase_timestamp").alias("hi"),
    ).collect()[0]
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{bounds.lo}'), "
            f"to_date('{bounds.hi}'), interval 1 day)) AS date_day"
        )
        .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date_day"))
        .withColumn("quarter", F.quarter("date_day"))
        .withColumn("month", F.month("date_day"))
        .withColumn("month_name", F.date_format("date_day", "MMMM"))
        .withColumn("day", F.dayofmonth("date_day"))
        .withColumn("day_of_week", F.date_format("date_day", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_day").isin([1, 7]))
    )


# -------------------------- facts -----------------------------------------
def build_fct_order_items(spark, config) -> DataFrame:
    items = _silver(spark, config, "order_items")
    return items.select(
        "order_id", "order_item_id", "product_id", "seller_id",
        "price", "freight_value", "shipping_limit_date",
    ).withColumn("line_revenue", F.round(F.col("price") + F.col("freight_value"), 2))


def build_fct_orders(spark, config) -> DataFrame:
    orders = _silver(spark, config, "orders")
    items = _silver(spark, config, "order_items")
    pays = _silver(spark, config, "order_payments")
    reviews = _silver(spark, config, "order_reviews")

    order_value = br.add_order_value(items)
    pay_agg = pays.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("payment_total"),
        F.max("payment_installments").alias("max_installments"),
        F.first("payment_type").alias("primary_payment_type"),
    )
    review_agg = reviews.groupBy("order_id").agg(
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.count("review_id").alias("review_count"),
    )

    return (
        orders.transform(br.add_delivery_metrics)
        .join(order_value, "order_id", "left")
        .join(pay_agg, "order_id", "left")
        .join(review_agg, "order_id", "left")
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        .select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_purchase_date",
            "order_approved_at", "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "items_total", "freight_total", "order_value", "item_count",
            "seller_count", "payment_total", "max_installments",
            "primary_payment_type", "avg_review_score", "review_count",
            "delivery_days", "estimated_delivery_days",
            "delivery_delay_days", "is_late_delivery",
        )
    )


# table -> (builder, partition_field, cluster_fields)
GOLD_TABLES = {
    "dim_customers": (build_dim_customers, None, ["customer_state"]),
    "dim_sellers":   (build_dim_sellers, None, ["seller_state"]),
    "dim_products":  (build_dim_products, None, ["category"]),
    "dim_date":      (build_dim_date, None, None),
    "fct_order_items": (build_fct_order_items, None, ["seller_id"]),
    "fct_orders":    (build_fct_orders, "order_purchase_date",
                      ["customer_id", "order_status"]),
}


def main(config_path: str, load_bq: bool = True) -> None:
    config = load_config(config_path)
    spark = get_spark(config)
    run_id = new_run_id()
    run_records: list[dict] = []
    gold_dir = config["paths"]["gold"].rstrip("/")
    log.info("=== GOLD START | run_id=%s ===", run_id)

    try:
        for table, (builder, part, cluster) in GOLD_TABLES.items():
            with track_stage(run_id, "gold", table, run_records) as rec:
                df = builder(spark, config).cache()
                rec["rows_out"] = df.count()
                write_parquet(df, f"{gold_dir}/{table}", mode="overwrite")
                if load_bq:
                    load_to_bigquery(
                        df, table, config,
                        dataset_key="gold",
                        partition_field=part,
                        cluster_fields=cluster,
                    )
                log.info("GOLD %-18s rows=%s", table, rec["rows_out"])
                df.unpersist()
        log.info("=== GOLD COMPLETE ===")
    finally:
        audit_path = gold_dir.replace("gold", "audit")
        write_run_log(spark, audit_path, run_records)
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold star-schema build + BQ load")
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    parser.add_argument("--no-bq", action="store_true", help="skip BigQuery load")
    args = parser.parse_args()
    main(args.config, load_bq=not args.no_bq)

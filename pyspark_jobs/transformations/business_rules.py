"""Business-rule validations and derived business attributes.

These encode domain knowledge about the Olist data (order lifecycle,
delivery timing, monetary sanity) and are applied in the Silver/Gold layers.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_ORDER_STATUS = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]


def flag_invalid_orders(df: DataFrame) -> DataFrame:
    """Add ``is_valid_order`` based on status + timestamp sanity.

    Rule: purchase timestamp must exist; if delivered, delivery date must be
    present and not earlier than purchase.
    """
    delivered = F.col("order_status") == "delivered"
    bad_delivery = delivered & (
        F.col("order_delivered_customer_date").isNull()
        | (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"))
    )
    return df.withColumn(
        "is_valid_order",
        F.when(F.col("order_purchase_timestamp").isNull(), F.lit(False))
        .when(bad_delivery, F.lit(False))
        .otherwise(F.lit(True)),
    )


def add_delivery_metrics(df: DataFrame) -> DataFrame:
    """Derive delivery-performance columns used by Gold/dbt marts."""
    return (
        df.withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
        )
        .withColumn(
            "estimated_delivery_days",
            F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"),
        )
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                "order_delivered_customer_date", "order_estimated_delivery_date"
            ),
        )
        .withColumn(
            "is_late_delivery",
            F.when(F.col("delivery_delay_days") > 0, F.lit(True)).otherwise(F.lit(False)),
        )
    )


def validate_monetary(df: DataFrame, columns: list[str]) -> DataFrame:
    """Null out negative monetary values and flag them (business rule)."""
    out = df
    flags = []
    for c in columns:
        if c in df.columns:
            flag = f"_neg_{c}"
            out = out.withColumn(flag, F.col(c) < 0)
            out = out.withColumn(
                c, F.when(F.col(c) < 0, None).otherwise(F.col(c))
            )
            flags.append(flag)
    if flags:
        flag_ints = [F.col(f).cast("int") for f in flags]
        # F.greatest requires >= 2 columns; handle the single-column case.
        combined = flag_ints[0] if len(flag_ints) == 1 else F.greatest(*flag_ints)
        out = out.withColumn("has_monetary_issue", combined == 1).drop(*flags)
    return out


def add_order_value(items_df: DataFrame) -> DataFrame:
    """Aggregate order_items into per-order revenue (price + freight)."""
    return items_df.groupBy("order_id").agg(
        F.round(F.sum("price"), 2).alias("items_total"),
        F.round(F.sum("freight_value"), 2).alias("freight_total"),
        F.round(F.sum(F.col("price") + F.col("freight_value")), 2).alias("order_value"),
        F.count("*").alias("item_count"),
        F.countDistinct("seller_id").alias("seller_count"),
    )

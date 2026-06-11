"""Unit tests for business-rule transformations."""
from __future__ import annotations

import datetime as dt

from pyspark_jobs.transformations import business_rules as br


def test_flag_invalid_orders(spark):
    rows = [
        # valid delivered
        ("o1", "delivered", dt.datetime(2018, 1, 1), dt.datetime(2018, 1, 5)),
        # delivered but no delivery date -> invalid
        ("o2", "delivered", dt.datetime(2018, 1, 1), None),
        # delivered before purchase -> invalid
        ("o3", "delivered", dt.datetime(2018, 1, 10), dt.datetime(2018, 1, 5)),
    ]
    df = spark.createDataFrame(
        rows,
        ["order_id", "order_status",
         "order_purchase_timestamp", "order_delivered_customer_date"],
    )
    flagged = {r["order_id"]: r["is_valid_order"]
               for r in br.flag_invalid_orders(df).collect()}
    assert flagged == {"o1": True, "o2": False, "o3": False}


def test_validate_monetary_nulls_negatives(spark):
    df = spark.createDataFrame([(10.0,), (-5.0,)], ["price"])
    out = br.validate_monetary(df, ["price"]).collect()
    prices = sorted([r["price"] for r in out], key=lambda x: (x is None, x))
    assert prices[0] == 10.0
    assert prices[1] is None


def test_add_order_value(spark):
    df = spark.createDataFrame(
        [("o1", 1, "s1", 100.0, 10.0), ("o1", 2, "s2", 50.0, 5.0)],
        ["order_id", "order_item_id", "seller_id", "price", "freight_value"],
    )
    res = br.add_order_value(df).collect()[0]
    assert res["order_value"] == 165.0
    assert res["item_count"] == 2
    assert res["seller_count"] == 2

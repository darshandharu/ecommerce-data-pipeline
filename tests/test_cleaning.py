"""Unit tests for reusable cleaning transformations."""
from __future__ import annotations

from pyspark_jobs.transformations import cleaning as cl


def test_trim_strings_blanks_to_null(spark):
    df = spark.createDataFrame([("  a  ",), ("",), ("  ",)], ["v"])
    out = {r["v"] for r in cl.trim_strings(df).collect()}
    assert out == {"a", None}


def test_dedupe_on_keys_keeps_latest(spark):
    df = spark.createDataFrame(
        [("k1", "2020-01-01", "old"), ("k1", "2020-02-01", "new")],
        ["id", "ts", "val"],
    )
    out = cl.dedupe_on_keys(df, ["id"], "ts").collect()
    assert len(out) == 1
    assert out[0]["val"] == "new"


def test_drop_exact_duplicates(spark):
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["id", "v"])
    assert cl.drop_exact_duplicates(df).count() == 2


def test_standardize_state(spark):
    df = spark.createDataFrame([(" sp ",), ("rj",)], ["customer_state"])
    out = [r["customer_state"] for r in
           cl.standardize_state(df, ["customer_state"]).collect()]
    assert set(out) == {"SP", "RJ"}


def test_cast_columns(spark):
    df = spark.createDataFrame([("10",), ("20",)], ["price"])
    out = cl.cast_columns(df, {"price": "int"})
    assert dict(out.dtypes)["price"] == "int"


def test_add_audit_columns(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    out = cl.add_audit_columns(df, "run-123", "orders")
    row = out.collect()[0]
    assert row["_run_id"] == "run-123"
    assert row["_source"] == "orders"

"""Unit tests for the CDC merge logic (upsert + soft delete)."""
from __future__ import annotations

from pyspark_jobs.cdc.cdc_processor import apply_merge


def _snapshot(spark):
    return spark.createDataFrame(
        [("k1", "alpha", "I", "2018-01-01 00:00:00"),
         ("k2", "beta", "I", "2018-01-01 00:00:00")],
        ["id", "val", "op", "cdc_timestamp"],
    )


def test_insert_adds_new_key(spark):
    cur = _snapshot(spark)
    changes = spark.createDataFrame(
        [("k3", "gamma", "I", "2018-01-02 00:00:00")],
        ["id", "val", "op", "cdc_timestamp"],
    )
    out = {r["id"] for r in apply_merge(cur, changes, ["id"]).collect()}
    assert out == {"k1", "k2", "k3"}


def test_update_keeps_latest(spark):
    cur = _snapshot(spark)
    changes = spark.createDataFrame(
        [("k1", "alpha_v2", "U", "2018-01-02 00:00:00")],
        ["id", "val", "op", "cdc_timestamp"],
    )
    merged = {r["id"]: r["val"] for r in apply_merge(cur, changes, ["id"]).collect()}
    assert merged["k1"] == "alpha_v2"


def test_delete_removes_key(spark):
    cur = _snapshot(spark)
    changes = spark.createDataFrame(
        [("k2", "beta", "D", "2018-01-02 00:00:00")],
        ["id", "val", "op", "cdc_timestamp"],
    )
    out = {r["id"] for r in apply_merge(cur, changes, ["id"]).collect()}
    assert out == {"k1"}

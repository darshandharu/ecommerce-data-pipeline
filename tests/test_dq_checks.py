"""Unit tests for the data quality checks + framework."""
from __future__ import annotations

from pyspark_jobs.data_quality import checks as C
from pyspark_jobs.data_quality import framework as fw
from pyspark_jobs.common.exceptions import DataQualityError

RUN = "test-run"


def test_not_null_detects_failures(spark):
    df = spark.createDataFrame([("a",), (None,), ("c",)], ["order_id"])
    res = C.check_not_null(df, RUN, "orders", ["order_id"], 0.0, "FAIL")[0]
    assert res.records_failed == 1
    assert res.status == "FAIL"


def test_unique_detects_duplicates(spark):
    df = spark.createDataFrame([("k",), ("k",), ("j",)], ["order_id"])
    res = C.check_unique(df, RUN, "orders", ["order_id"], 0.0, "FAIL")[0]
    assert res.records_failed == 1
    assert res.status == "FAIL"


def test_referential_finds_orphans(spark):
    child = spark.createDataFrame([("o1",), ("o2",), ("oX",)], ["order_id"])
    parent = spark.createDataFrame([("o1",), ("o2",)], ["order_id"])
    res = C.check_referential(child, parent, RUN, "items",
                              "order_id", "order_id", 0.0, "FAIL")[0]
    assert res.records_failed == 1
    assert res.status == "FAIL"


def test_threshold_downgrades_to_pass(spark):
    # 1 failing of 100 with 5% tolerance -> PASS
    rows = [("a",)] * 99 + [(None,)]
    df = spark.createDataFrame(rows, ["order_id"])
    res = C.check_not_null(df, RUN, "orders", ["order_id"], 0.05, "FAIL")[0]
    assert res.status == "PASS"


def test_non_negative(spark):
    df = spark.createDataFrame([(5.0,), (-1.0,)], ["price"])
    res = C.check_non_negative(df, RUN, "items", ["price"], 0.0, "FAIL")[0]
    assert res.records_failed == 1


def test_framework_assert_raises(spark):
    df = spark.createDataFrame([("a",), ("a",)], ["order_id"])
    rules = {"orders": [{"check": "unique", "cols": ["order_id"],
                         "severity": "FAIL", "threshold": 0.0}]}
    results = fw.run_rules(RUN, rules, {"orders": df})
    try:
        fw.assert_no_failures(results)
        assert False, "expected DataQualityError"
    except DataQualityError as e:
        assert e.failures


def test_summarize(spark):
    df = spark.createDataFrame([("a",), (None,)], ["order_id"])
    rules = {"orders": [{"check": "not_null", "cols": ["order_id"],
                         "severity": "WARN", "threshold": 0.0}]}
    results = fw.run_rules(RUN, rules, {"orders": df})
    summary = fw.summarize(results)
    assert summary["checks_total"] == 1
    assert summary["records_failed"] == 1

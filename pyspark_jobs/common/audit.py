"""Audit & run-log helpers.

Two audit artifacts power the monitoring layer / DQ dashboard:

* ``pipeline_run_log`` — one row per (run, stage, table): row counts,
  status, duration. Drives row-count tracking and execution-time metrics.
* ``dq_results``       — one row per DQ check executed (see data_quality/).

Both are written as Parquet under ``<audit>/`` locally and can be loaded to
BigQuery ``audit`` dataset by the gold/load step. Keeping a local copy means
the dashboard works even without GCP credentials.
"""
from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T

from pyspark_jobs.common.logger import get_logger

log = get_logger(__name__)

RUN_LOG_SCHEMA = [
    "run_id", "stage", "table_name", "status",
    "rows_in", "rows_out", "duration_sec", "started_at", "ended_at", "message",
]

# Explicit Spark schema so writing succeeds even when a whole column is NULL
# (e.g. bronze records have no rows_in / message). Inference can't type an
# all-None column -> CANNOT_DETERMINE_TYPE.
RUN_LOG_STRUCT = T.StructType([
    T.StructField("run_id", T.StringType(), True),
    T.StructField("stage", T.StringType(), True),
    T.StructField("table_name", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("rows_in", T.LongType(), True),
    T.StructField("rows_out", T.LongType(), True),
    T.StructField("duration_sec", T.DoubleType(), True),
    T.StructField("started_at", T.StringType(), True),
    T.StructField("ended_at", T.StringType(), True),
    T.StructField("message", T.StringType(), True),
])


def new_run_id() -> str:
    """Generate a sortable run id: <utc-timestamp>-<short-uuid>."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


def _coerce(rec: dict) -> Row:
    """Build a Row matching RUN_LOG_STRUCT, coercing numeric types/None."""
    def as_long(v):
        return int(v) if v is not None else None

    def as_double(v):
        return float(v) if v is not None else None

    return Row(
        run_id=rec.get("run_id"),
        stage=rec.get("stage"),
        table_name=rec.get("table_name"),
        status=rec.get("status"),
        rows_in=as_long(rec.get("rows_in")),
        rows_out=as_long(rec.get("rows_out")),
        duration_sec=as_double(rec.get("duration_sec")),
        started_at=rec.get("started_at"),
        ended_at=rec.get("ended_at"),
        message=rec.get("message"),
    )


def write_run_log(spark: SparkSession, audit_path: str, records: list[dict]) -> None:
    """Append run-log records to the audit Parquet location."""
    if not records:
        return
    rows = [_coerce(rec) for rec in records]
    df = spark.createDataFrame(rows, schema=RUN_LOG_STRUCT)
    (df.write.mode("append").parquet(f"{audit_path.rstrip('/')}/pipeline_run_log"))
    log.info("appended %d run-log record(s)", len(records))


@contextmanager
def track_stage(run_id: str, stage: str, table_name: str, sink: list[dict]):
    """Context manager that times a stage and appends a run-log dict to ``sink``.

    Usage::

        with track_stage(run_id, "silver", "orders", run_records) as rec:
            df = transform(...)
            rec["rows_out"] = df.count()
    """
    rec: dict = {
        "run_id": run_id,
        "stage": stage,
        "table_name": table_name,
        "status": "RUNNING",
        "rows_in": None,
        "rows_out": None,
        "duration_sec": None,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ended_at": None,
        "message": None,
    }
    start = time.time()
    try:
        yield rec
        rec["status"] = "SUCCESS"
    except Exception as exc:
        rec["status"] = "FAILED"
        rec["message"] = str(exc)[:1000]
        log.exception("stage failed | stage=%s table=%s", stage, table_name)
        raise
    finally:
        rec["duration_sec"] = round(time.time() - start, 3)
        rec["ended_at"] = datetime.now(timezone.utc).isoformat()
        sink.append(rec)

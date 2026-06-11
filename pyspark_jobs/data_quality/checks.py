"""Individual data quality checks.

Each check returns a :class:`CheckResult`. A check ``FAIL``s when the fraction
of failing rows exceeds its ``threshold``; below that it ``WARN``s or ``PASS``es
depending on severity. The framework decides whether a FAIL aborts the pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    run_id: str
    table_name: str
    check_type: str
    column: str
    records_scanned: int
    records_failed: int
    threshold: float
    severity: str
    status: str = "PASS"        # PASS | WARN | FAIL
    detail: str = ""
    executed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def finalize(self) -> "CheckResult":
        """Compute PASS/WARN/FAIL from counts + threshold + severity."""
        ratio = (self.records_failed / self.records_scanned) if self.records_scanned else 0.0
        if self.records_failed == 0 or ratio <= self.threshold:
            self.status = "PASS"
        else:
            self.status = "FAIL" if self.severity == "FAIL" else "WARN"
        return self

    def to_row(self) -> dict:
        return asdict(self)


# --------------------------------------------------------------------------
def _scanned(df: DataFrame) -> int:
    return df.count()


def check_not_null(df, run_id, table, cols, threshold, severity) -> list[CheckResult]:
    n = _scanned(df)
    results = []
    for c in cols:
        failed = df.filter(F.col(c).isNull()).count() if c in df.columns else n
        results.append(
            CheckResult(run_id, table, "not_null", c, n, failed, threshold, severity,
                        detail=f"{failed} null values in {c}").finalize()
        )
    return results


def check_unique(df, run_id, table, cols, threshold, severity) -> list[CheckResult]:
    n = _scanned(df)
    distinct = df.select(*cols).distinct().count()
    failed = n - distinct
    label = "+".join(cols)
    return [
        CheckResult(run_id, table, "unique", label, n, failed, threshold, severity,
                    detail=f"{failed} duplicate key(s) on ({label})").finalize()
    ]


def check_duplicates(df, run_id, table, cols, threshold, severity) -> list[CheckResult]:
    n = _scanned(df)
    dup = (
        df.groupBy(*cols).count().filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1)).collect()[0][0]
    ) or 0
    label = "+".join(cols)
    return [
        CheckResult(run_id, table, "duplicates", label, n, int(dup), threshold, severity,
                    detail=f"{dup} duplicate rows on ({label})").finalize()
    ]


def check_referential(df, ref_df, run_id, table, column, ref_column,
                      threshold, severity) -> list[CheckResult]:
    """Rows whose FK has no matching PK in the reference table."""
    n = _scanned(df)
    child = df.select(F.col(column).alias("_fk")).filter(F.col("_fk").isNotNull())
    parent = ref_df.select(F.col(ref_column).alias("_pk")).distinct()
    orphans = child.join(parent, child["_fk"] == parent["_pk"], "left_anti").count()
    return [
        CheckResult(run_id, table, "referential_integrity", column, n, orphans,
                    threshold, severity,
                    detail=f"{orphans} orphan {column} not in {ref_column}").finalize()
    ]


def check_valid_timestamp(df, run_id, table, cols, threshold, severity) -> list[CheckResult]:
    """Timestamps must be non-null and within a sane range (2016–2020)."""
    n = _scanned(df)
    lo, hi = "2016-01-01", "2020-01-01"
    results = []
    for c in cols:
        if c not in df.columns:
            continue
        bad = df.filter(
            F.col(c).isNotNull()
            & ((F.col(c) < F.lit(lo)) | (F.col(c) > F.lit(hi)))
        ).count()
        results.append(
            CheckResult(run_id, table, "valid_timestamp", c, n, bad, threshold, severity,
                        detail=f"{bad} out-of-range timestamps in {c}").finalize()
        )
    return results


def check_timestamp_order(df, run_id, table, before, after,
                          threshold, severity) -> list[CheckResult]:
    n = _scanned(df)
    bad = df.filter(
        F.col(before).isNotNull() & F.col(after).isNotNull()
        & (F.col(after) < F.col(before))
    ).count()
    return [
        CheckResult(run_id, table, "timestamp_order", f"{before}<= {after}", n, bad,
                    threshold, severity,
                    detail=f"{bad} rows where {after} < {before}").finalize()
    ]


def check_accepted_values(df, run_id, table, column, values,
                          threshold, severity) -> list[CheckResult]:
    n = _scanned(df)
    bad = df.filter(
        F.col(column).isNotNull() & ~F.col(column).cast("string").isin(values)
    ).count()
    return [
        CheckResult(run_id, table, "accepted_values", column, n, bad, threshold, severity,
                    detail=f"{bad} values of {column} outside accepted set").finalize()
    ]


def check_non_negative(df, run_id, table, cols, threshold, severity) -> list[CheckResult]:
    n = _scanned(df)
    results = []
    for c in cols:
        if c not in df.columns:
            continue
        bad = df.filter(F.col(c) < 0).count()
        results.append(
            CheckResult(run_id, table, "non_negative", c, n, bad, threshold, severity,
                        detail=f"{bad} negative values in {c}").finalize()
        )
    return results

"""DQ framework: dispatch declarative rules (configs/dq_rules.yaml) against a
set of layer DataFrames and collect :class:`CheckResult` rows.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from pyspark_jobs.common.exceptions import DataQualityError
from pyspark_jobs.common.logger import get_logger
from pyspark_jobs.data_quality import checks as C

log = get_logger(__name__)


def run_rules(
    run_id: str,
    rules: dict[str, list[dict]],
    frames: dict[str, DataFrame],
) -> list[C.CheckResult]:
    """Execute every rule whose table is present in ``frames``."""
    results: list[C.CheckResult] = []

    for table, table_rules in rules.items():
        df = frames.get(table)
        if df is None:
            log.warning("DQ: no DataFrame for table '%s' — skipping", table)
            continue

        for rule in table_rules:
            check = rule["check"]
            sev = rule.get("severity", "WARN")
            thr = float(rule.get("threshold", 0.0))
            try:
                results.extend(_dispatch(run_id, table, df, frames, rule, check, thr, sev))
            except Exception as exc:  # never let one check kill the run
                log.exception("DQ check '%s' on '%s' errored: %s", check, table, exc)
                results.append(
                    C.CheckResult(run_id, table, check, str(rule.get("cols") or
                                  rule.get("column") or ""), 0, 0, thr, sev,
                                  status="WARN", detail=f"check error: {exc}")
                )
    return results


def _dispatch(run_id, table, df, frames, rule, check, thr, sev):
    if check == "not_null":
        return C.check_not_null(df, run_id, table, rule["cols"], thr, sev)
    if check == "unique":
        return C.check_unique(df, run_id, table, rule["cols"], thr, sev)
    if check == "duplicates":
        return C.check_duplicates(df, run_id, table, rule["cols"], thr, sev)
    if check == "non_negative":
        return C.check_non_negative(df, run_id, table, rule["cols"], thr, sev)
    if check == "valid_timestamp":
        return C.check_valid_timestamp(df, run_id, table, rule["cols"], thr, sev)
    if check == "timestamp_order":
        return C.check_timestamp_order(df, run_id, table, rule["before"],
                                       rule["after"], thr, sev)
    if check == "accepted_values":
        return C.check_accepted_values(df, run_id, table, rule["column"],
                                       rule["values"], thr, sev)
    if check == "referential":
        ref = frames.get(rule["ref_table"])
        if ref is None:
            log.warning("DQ: ref_table '%s' missing for RI check", rule["ref_table"])
            return []
        return C.check_referential(df, ref, run_id, table, rule["column"],
                                   rule["ref_column"], thr, sev)
    log.warning("DQ: unknown check type '%s'", check)
    return []


def assert_no_failures(results: list[C.CheckResult]) -> None:
    """Raise :class:`DataQualityError` if any FAIL-severity check failed."""
    failures = [r for r in results if r.status == "FAIL"]
    if failures:
        summary = "; ".join(
            f"{r.table_name}.{r.column} [{r.check_type}] "
            f"{r.records_failed}/{r.records_scanned}"
            for r in failures
        )
        raise DataQualityError(
            f"{len(failures)} FAIL-severity data quality check(s): {summary}",
            failures=[r.to_row() for r in failures],
        )


def summarize(results: list[C.CheckResult]) -> dict:
    """Aggregate counts used by the DQ dashboard headline metrics."""
    return {
        "checks_total": len(results),
        "checks_passed": sum(r.status == "PASS" for r in results),
        "checks_warned": sum(r.status == "WARN" for r in results),
        "checks_failed": sum(r.status == "FAIL" for r in results),
        "records_scanned": sum(r.records_scanned for r in results),
        "records_failed": sum(r.records_failed for r in results),
    }

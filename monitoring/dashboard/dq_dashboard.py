"""Data Quality Dashboard (Streamlit).

Reads the audit artifacts (local Parquet by default, or BigQuery if configured)
and shows the recruiter-friendly headline metrics:

    • Total records processed
    • Failed records
    • Duplicate records
    • Null violations
    • Pipeline status (per stage)
    • Execution time (per job)

Run::

    streamlit run monitoring/dashboard/dq_dashboard.py
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

AUDIT_PATH = Path(os.getenv("AUDIT_PATH", "./data/audit"))

st.set_page_config(page_title="E-Commerce Pipeline — DQ Dashboard",
                   page_icon="📊", layout="wide")


@st.cache_data(ttl=60)
def load_parquet(name: str) -> pd.DataFrame:
    path = AUDIT_PATH / name
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def metric_card(col, label, value, help_text=""):
    col.metric(label, value, help=help_text)


# --------------------------------------------------------------------------
st.title("📊 Data Quality & Pipeline Health Dashboard")
st.caption("Brazilian Olist E-Commerce — Bronze → Silver → Gold → dbt")

dq = load_parquet("dq_results")
runs = load_parquet("pipeline_run_log")

if dq.empty and runs.empty:
    st.warning(
        "No audit data found under `./data/audit`. "
        "Run the pipeline (make run-all) to populate it."
    )
    st.stop()

# ---- run selector ----
run_ids = sorted(
    set(dq.get("run_id", pd.Series(dtype=str)).tolist()
        + runs.get("run_id", pd.Series(dtype=str)).tolist()),
    reverse=True,
)
selected_run = st.sidebar.selectbox("Pipeline run", run_ids) if run_ids else None
if selected_run:
    dq_run = dq[dq["run_id"] == selected_run] if not dq.empty else dq
    runs_run = runs[runs["run_id"] == selected_run] if not runs.empty else runs
else:
    dq_run, runs_run = dq, runs

# ---- headline metrics ----
records_processed = int(dq_run["records_scanned"].sum()) if not dq_run.empty else 0
failed_records = int(dq_run["records_failed"].sum()) if not dq_run.empty else 0
duplicate_records = int(
    dq_run.loc[dq_run["check_type"].isin(["duplicates", "unique"]), "records_failed"].sum()
) if not dq_run.empty else 0
null_violations = int(
    dq_run.loc[dq_run["check_type"] == "not_null", "records_failed"].sum()
) if not dq_run.empty else 0
has_fail = (dq_run["status"] == "FAIL").any() if not dq_run.empty else False
exec_time = round(runs_run["duration_sec"].sum(), 1) if not runs_run.empty else 0.0

c1, c2, c3, c4, c5, c6 = st.columns(6)
metric_card(c1, "Records processed", f"{records_processed:,}")
metric_card(c2, "Failed records", f"{failed_records:,}")
metric_card(c3, "Duplicate records", f"{duplicate_records:,}")
metric_card(c4, "Null violations", f"{null_violations:,}")
metric_card(c5, "Pipeline status", "❌ FAILED" if has_fail else "✅ PASSED")
metric_card(c6, "Execution time (s)", f"{exec_time:,.1f}")

st.divider()

# ---- DQ status breakdown ----
left, right = st.columns(2)
if not dq_run.empty:
    status_counts = dq_run["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    fig = px.pie(status_counts, names="status", values="count",
                 title="DQ check outcomes",
                 color="status",
                 color_discrete_map={"PASS": "#2ecc71", "WARN": "#f1c40f", "FAIL": "#e74c3c"})
    left.plotly_chart(fig, use_container_width=True)

    by_table = (dq_run.groupby(["table_name", "status"])
                .size().reset_index(name="count"))
    fig2 = px.bar(by_table, x="table_name", y="count", color="status",
                  title="Checks by table",
                  color_discrete_map={"PASS": "#2ecc71", "WARN": "#f1c40f", "FAIL": "#e74c3c"})
    right.plotly_chart(fig2, use_container_width=True)

# ---- execution time per stage ----
if not runs_run.empty:
    st.subheader("⏱ Execution time per stage/table")
    fig3 = px.bar(runs_run.sort_values("duration_sec", ascending=False),
                  x="table_name", y="duration_sec", color="stage",
                  hover_data=["rows_in", "rows_out", "status"],
                  title="Stage durations")
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("📦 Row-count tracking")
    st.dataframe(
        runs_run[["stage", "table_name", "rows_in", "rows_out",
                  "duration_sec", "status"]].sort_values(["stage", "table_name"]),
        use_container_width=True,
    )

# ---- failing checks detail ----
if not dq_run.empty:
    fails = dq_run[dq_run["status"].isin(["FAIL", "WARN"])]
    if not fails.empty:
        st.subheader("⚠️ Failing / warning checks")
        st.dataframe(
            fails[["table_name", "check_type", "column", "records_scanned",
                   "records_failed", "severity", "status", "detail"]],
            use_container_width=True,
        )
    else:
        st.success("All data quality checks passed for this run. 🎉")

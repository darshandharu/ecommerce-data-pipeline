"""Shared Airflow callbacks: success/failure notifications + SLA misses.

Email alerting uses Airflow's EmailOperator/`send_email`; for portability the
functions degrade to logging when SMTP is not configured.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def _ctx_summary(context: dict) -> str:
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "?"
    task_id = ti.task_id if ti else "?"
    exec_date = context.get("logical_date") or context.get("execution_date")
    return f"DAG={dag_id} TASK={task_id} RUN={exec_date} TRY={ti.try_number if ti else '?'}"


def notify_failure(context: dict) -> None:
    """on_failure_callback — send an email + structured log line."""
    summary = _ctx_summary(context)
    exc = context.get("exception")
    log.error("PIPELINE FAILURE | %s | error=%s", summary, exc)
    try:
        from airflow.utils.email import send_email
        from airflow.models import Variable

        to = Variable.get("alert_email", default_var="data-alerts@example.com")
        send_email(
            to=to,
            subject=f"[Airflow] FAILED — {summary}",
            html_content=f"<h3>Task failed</h3><p>{summary}</p><pre>{exc}</pre>",
        )
    except Exception as e:  # pragma: no cover - depends on SMTP config
        log.warning("could not send failure email: %s", e)


def notify_success(context: dict) -> None:
    """on_success_callback for the final task — confirms a clean run."""
    log.info("PIPELINE SUCCESS | %s", _ctx_summary(context))


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA-miss callback registered on the DAG."""
    log.warning("SLA MISS | dag=%s tasks=%s", dag.dag_id,
                [s.task_id for s in slas])

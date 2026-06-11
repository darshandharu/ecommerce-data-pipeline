"""BigQuery loading helper.

Two backends are supported so the project runs with *or* without a Spark
BigQuery connector jar:

* ``spark``  — uses the spark-bigquery connector (preferred in cluster/Docker).
* ``pandas`` — falls back to ``pandas_gbq`` for small Gold tables / local dev.

Loading is a no-op (logged) when no GCP project is configured, so the rest of
the pipeline still runs end-to-end locally.
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from pyspark_jobs.common.logger import get_logger

log = get_logger(__name__)


def load_to_bigquery(
    df: DataFrame,
    table: str,
    config: dict,
    dataset_key: str = "gold",
    partition_field: str | None = None,
    cluster_fields: list[str] | None = None,
    mode: str = "overwrite",
    backend: str = "spark",
) -> None:
    """Load ``df`` into ``<project>.<dataset>.<table>``.

    ``partition_field`` / ``cluster_fields`` are applied when creating the
    table (matching the DDLs in sql/).
    """
    bq = config.get("bigquery", {})
    project = bq.get("project_id")
    dataset = bq.get("datasets", {}).get(dataset_key)

    # Treat unset / unresolved / placeholder values as "not configured" so the
    # rest of the pipeline runs end-to-end locally without GCP credentials.
    placeholders = {"", "my-gcp-project", "your-gcp-project", None}
    unconfigured = (
        not project or project in placeholders or str(project).startswith("${")
        or not dataset or str(dataset).startswith("${")
    )
    if unconfigured:
        log.warning("BigQuery not configured — skipping load of '%s' "
                    "(would target %s.%s)", table, dataset, table)
        return

    fqtn = f"{project}.{dataset}.{table}"
    if backend == "spark":
        _load_spark(df, fqtn, bq, partition_field, cluster_fields, mode)
    else:
        _load_pandas(df, dataset, table, project, partition_field,
                     cluster_fields, mode)
    log.info("loaded %s (partition=%s cluster=%s)", fqtn, partition_field, cluster_fields)


def _load_spark(df, fqtn, bq, partition_field, cluster_fields, mode):
    writer = (
        df.write.format("bigquery")
        .option("table", fqtn)
        .option("temporaryGcsBucket", bq.get("staging_bucket"))
        .option("writeMethod", "indirect")
        .mode(mode)
    )
    if partition_field:
        writer = writer.option("partitionField", partition_field) \
                       .option("partitionType", "DAY")
    if cluster_fields:
        writer = writer.option("clusteredFields", ",".join(cluster_fields))
    writer.save()


def _load_pandas(df, dataset, table, project, partition_field, cluster_fields, mode):
    import pandas_gbq  # local import keeps Spark-only runs lightweight

    pdf = df.toPandas()
    pandas_gbq.to_gbq(
        pdf,
        destination_table=f"{dataset}.{table}",
        project_id=project,
        if_exists="replace" if mode == "overwrite" else "append",
    )

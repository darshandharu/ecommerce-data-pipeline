"""Factory for a configured :class:`SparkSession`.

Centralizing session creation ensures every job uses the same timezone,
adaptive-query settings and (when running against GCP) the BigQuery /
GCS connectors.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from pyspark_jobs.common.logger import get_logger

log = get_logger(__name__)


def get_spark(config: dict) -> SparkSession:
    """Build (or fetch) a SparkSession from the ``spark`` block of config."""
    sconf = config.get("spark", {})
    builder = (
        SparkSession.builder.appName(sconf.get("app_name", "ecommerce-pipeline"))
        .master(sconf.get("master", "local[*]"))
        .config("spark.driver.memory", sconf.get("driver_memory", "4g"))
        .config("spark.executor.memory", sconf.get("executor_memory", "4g"))
        .config("spark.sql.shuffle.partitions", str(sconf.get("shuffle_partitions", 64)))
    )

    for key, value in (sconf.get("configs") or {}).items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info(
        "SparkSession ready | app=%s master=%s",
        spark.sparkContext.appName,
        spark.sparkContext.master,
    )
    return spark

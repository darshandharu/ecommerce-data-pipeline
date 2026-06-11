"""Factory for a configured :class:`SparkSession`.

Centralizing session creation ensures every job uses the same timezone,
adaptive-query settings and (when running against GCP) the BigQuery /
GCS connectors.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from pyspark_jobs.common.logger import get_logger

log = get_logger(__name__)


def _resolved(value, default):
    """Fall back to ``default`` if ``value`` is empty or an unresolved ${VAR}.

    Guards against an env var that was never set leaking a literal
    ``${SPARK_DRIVER_MEMORY}`` into the JVM ``-Xmx`` flag.
    """
    if value is None:
        return default
    text = str(value).strip()
    if not text or text.startswith("${"):
        return default
    return text


def get_spark(config: dict) -> SparkSession:
    """Build (or fetch) a SparkSession from the ``spark`` block of config."""
    sconf = config.get("spark", {})
    master = _resolved(sconf.get("master"), "local[*]")
    driver_mem = _resolved(sconf.get("driver_memory"), "2g")
    executor_mem = _resolved(sconf.get("executor_memory"), "2g")
    builder = (
        SparkSession.builder.appName(sconf.get("app_name", "ecommerce-pipeline"))
        .master(master)
        .config("spark.driver.memory", driver_mem)
        .config("spark.executor.memory", executor_mem)
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

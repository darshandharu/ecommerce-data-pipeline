"""Read/write helpers that build a consistent type schema and centralize
Parquet I/O across the medallion layers.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from pyspark_jobs.common.exceptions import IngestionError, SchemaError
from pyspark_jobs.common.logger import get_logger

log = get_logger(__name__)

# map of yaml type names -> Spark DataTypes
_TYPE_MAP: dict[str, T.DataType] = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "float": T.FloatType(),
    "boolean": T.BooleanType(),
    "timestamp": T.TimestampType(),
    "date": T.DateType(),
}


def build_struct(schema_dict: dict[str, str]) -> T.StructType:
    """Translate a {col: type_name} dict into a Spark StructType."""
    fields = []
    for col, type_name in schema_dict.items():
        spark_type = _TYPE_MAP.get(type_name)
        if spark_type is None:
            raise SchemaError(f"Unknown type '{type_name}' for column '{col}'")
        fields.append(T.StructField(col, spark_type, nullable=True))
    return T.StructType(fields)


def read_csv(
    spark: SparkSession,
    path: str,
    schema: T.StructType | None = None,
    infer: bool = False,
) -> DataFrame:
    """Read a CSV. Prefer an explicit ``schema``; fall back to inference."""
    reader = (
        spark.read.option("header", True)
        .option("escape", '"')
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
    )
    try:
        if schema is not None:
            # timestampFormat covers the Olist 'yyyy-MM-dd HH:mm:ss' layout
            return reader.schema(schema).option(
                "timestampFormat", "yyyy-MM-dd HH:mm:ss"
            ).csv(path)
        return reader.option("inferSchema", infer).csv(path)
    except Exception as exc:  # pragma: no cover - passthrough
        raise IngestionError(f"Failed reading CSV {path}: {exc}") from exc


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite",
                  partition_by: list[str] | None = None) -> None:
    """Write a DataFrame to Parquet, optionally partitioned."""
    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
        log.info("wrote parquet | path=%s mode=%s rows≈(lazy)", path, mode)
    except Exception as exc:  # pragma: no cover
        raise IngestionError(f"Failed writing parquet {path}: {exc}") from exc


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read a Parquet dataset, raising a clear error if it is absent."""
    if not Path(path).exists() and not path.startswith(("gs://", "s3://", "hdfs://")):
        raise IngestionError(f"Parquet path does not exist: {path}")
    try:
        return spark.read.parquet(path)
    except Exception as exc:  # pragma: no cover
        raise IngestionError(f"Failed reading parquet {path}: {exc}") from exc

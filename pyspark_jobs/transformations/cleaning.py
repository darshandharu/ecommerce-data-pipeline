"""Generic, reusable cleaning transformations.

Every function takes and returns a DataFrame so they compose cleanly::

    df = (raw
          .transform(trim_strings)
          .transform(lambda d: drop_exact_duplicates(d))
          .transform(lambda d: dedupe_on_keys(d, ["order_id"], "ingested_at")))
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace and convert empty strings to NULL for all string cols."""
    out = df
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            col = F.trim(F.col(field.name))
            out = out.withColumn(
                field.name,
                F.when(col == "", None).otherwise(col),
            )
    return out


def lower_case(df: DataFrame, columns: list[str]) -> DataFrame:
    """Lower-case the given string columns (standardization)."""
    out = df
    for c in columns:
        if c in df.columns:
            out = out.withColumn(c, F.lower(F.col(c)))
    return out


def standardize_state(df: DataFrame, columns: list[str]) -> DataFrame:
    """Upper-case + trim 2-letter Brazilian state codes."""
    out = df
    for c in columns:
        if c in df.columns:
            out = out.withColumn(c, F.upper(F.trim(F.col(c))))
    return out


def fill_nulls(df: DataFrame, mapping: dict[str, object]) -> DataFrame:
    """Fill nulls per column using an explicit {col: default} mapping."""
    valid = {c: v for c, v in mapping.items() if c in df.columns}
    return df.fillna(valid) if valid else df


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Remove fully identical rows."""
    return df.dropDuplicates()


def dedupe_on_keys(df: DataFrame, keys: list[str], order_col: str | None = None,
                   ascending: bool = False) -> DataFrame:
    """Keep one row per key.

    If ``order_col`` is given keep the latest (or earliest) per key; otherwise
    keep an arbitrary row. Used for natural-key deduplication in Silver.
    """
    if not keys:
        return df.dropDuplicates()
    if order_col and order_col in df.columns:
        order = F.col(order_col).asc() if ascending else F.col(order_col).desc()
        w = Window.partitionBy(*keys).orderBy(order)
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    return df.dropDuplicates(keys)


def cast_columns(df: DataFrame, casts: dict[str, str]) -> DataFrame:
    """Apply explicit casts {col: spark_sql_type}. Type-correction step."""
    out = df
    for c, t in casts.items():
        if c in df.columns:
            out = out.withColumn(c, F.col(c).cast(t))
    return out


def format_dates(df: DataFrame, columns: list[str], fmt: str = "yyyy-MM-dd") -> DataFrame:
    """Add ``<col>_date`` string columns formatted consistently for reporting."""
    out = df
    for c in columns:
        if c in df.columns:
            out = out.withColumn(f"{c}_date", F.date_format(F.col(c), fmt))
    return out


def add_audit_columns(df: DataFrame, run_id: str, source: str) -> DataFrame:
    """Stamp lineage columns onto every cleaned row."""
    return (
        df.withColumn("_run_id", F.lit(run_id))
        .withColumn("_source", F.lit(source))
        .withColumn("_ingested_at", F.current_timestamp())
    )

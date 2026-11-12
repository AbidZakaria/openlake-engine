"""Deduplication by business key.

Within one increment the same business key can appear several times: the same
order re-sent in two batches, an exact duplicate row, or an older and a newer
image of a record. We keep exactly one row per key — the most recently
ingested one — and report how many were dropped.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def latest_by_key(df: DataFrame, keys: list[str] | tuple[str, ...],
                  order_by: tuple[str, ...] = ("_ingested_at", "_record_hash")) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    return df.withColumn("__rn", F.row_number().over(window)).filter("__rn = 1").drop("__rn")


def exact_duplicates(df: DataFrame, columns: list[str]) -> int:
    """Rows that are identical on ``columns`` to another row."""
    total = df.count()
    return total - df.dropDuplicates(columns).count()

"""Shared write helpers: evolve-then-write, and MERGE-based upserts."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from openlake.iceberg.catalog_manager import CatalogManager


def evolve_table(cm: CatalogManager, table: str, df: DataFrame) -> list[str]:
    """Add columns present in ``df`` but missing in ``table`` (metadata-only in Iceberg)."""
    existing = {f.name.lower() for f in cm.spark.table(table).schema.fields}
    added = []
    for f in df.schema.fields:
        if f.name.lower() not in existing:
            cm.spark.sql(f"ALTER TABLE {table} ADD COLUMN `{f.name}` {f.dataType.simpleString()}")
            added.append(f.name)
    return added


def align_to_table(cm: CatalogManager, table: str, df: DataFrame) -> DataFrame:
    """Select ``df`` in the table's column order, NULL-filling columns it lacks."""
    target = cm.spark.table(table).schema
    present = set(df.columns)
    return df.select(*[
        (F.col(f.name) if f.name in present else F.lit(None)).cast(f.dataType).alias(f.name) for f in target.fields
    ])


def append(cm: CatalogManager, table: str, df: DataFrame, partition_by=(), comment: str = "") -> int | None:
    cm.create_from_df(table, df, partition_by, comment)
    evolve_table(cm, table, df)
    align_to_table(cm, table, df).writeTo(table).append()
    return cm.current_snapshot_id(table)


def merge_upsert(cm: CatalogManager, table: str, df: DataFrame, keys: list[str] | tuple[str, ...],
                 partition_by=(), comment: str = "", only_if_changed: str | None = None,
                 newer_than: str | None = None) -> int | None:
    """Upsert ``df`` into ``table`` on ``keys`` with an Iceberg MERGE INTO.

    ``only_if_changed`` names a hash column: matched rows are rewritten only when
    the hash differs, so re-delivered identical records do not churn files.
    ``newer_than`` names an ordering column: an older image never overwrites a
    newer one (protects against out-of-order batches).
    """
    cm.create_from_df(table, df, partition_by, comment)
    evolve_table(cm, table, df)
    src = align_to_table(cm, table, df)
    view = f"__src_{table.replace('.', '_')}"
    src.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    conditions = []
    if only_if_changed:
        conditions.append(f"t.`{only_if_changed}` IS DISTINCT FROM s.`{only_if_changed}`")
    if newer_than:
        conditions.append(f"s.`{newer_than}` >= t.`{newer_than}`")
    when = f" AND {' AND '.join(conditions)}" if conditions else ""
    cm.spark.sql(f"""
        MERGE INTO {table} t USING {view} s ON {on}
        WHEN MATCHED{when} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return cm.current_snapshot_id(table)

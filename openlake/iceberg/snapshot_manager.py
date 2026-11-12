"""Snapshot inspection: list, compare, and summarise tables.

Iceberg exposes its metadata as queryable tables (``<table>.snapshots``,
``.history``, ``.files``, ``.manifests``, ``.partitions``, ``.refs``), so
everything here is plain Spark SQL.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from openlake.config.pipeline import PipelineConfig


def list_snapshots(spark: SparkSession, table: str) -> list[dict]:
    rows = spark.sql(f"""
        SELECT s.snapshot_id, s.parent_id, CAST(s.committed_at AS STRING) AS committed_at, s.operation,
               s.summary['added-records']    AS added_records,
               s.summary['deleted-records']  AS deleted_records,
               s.summary['total-records']    AS total_records,
               s.summary['added-data-files'] AS added_files,
               s.summary['total-data-files'] AS total_files,
               h.is_current_ancestor
        FROM {table}.snapshots s LEFT JOIN {table}.history h ON s.snapshot_id = h.snapshot_id
        ORDER BY s.committed_at
    """).collect()
    return [r.asDict() for r in rows]


def current_snapshot(spark: SparkSession, table: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.history WHERE is_current_ancestor "
                     f"ORDER BY made_current_at DESC LIMIT 1").collect()
    return int(rows[0]["snapshot_id"]) if rows else None


def count_at(spark: SparkSession, table: str, snapshot_id: int) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table} VERSION AS OF {snapshot_id}").collect()[0]["n"]


def diff_by_key(spark: SparkSession, table: str, older: int, newer: int, keys: list[str],
                hash_col: str = "_record_hash") -> dict:
    """Rows inserted, deleted and updated between two snapshots of a keyed table."""
    on = " AND ".join(f"a.`{k}` <=> b.`{k}`" for k in keys)
    row = spark.sql(f"""
        SELECT
          sum(CASE WHEN a.`{keys[0]}` IS NULL THEN 1 ELSE 0 END) AS inserted,
          sum(CASE WHEN b.`{keys[0]}` IS NULL THEN 1 ELSE 0 END) AS deleted,
          sum(CASE WHEN a.`{keys[0]}` IS NOT NULL AND b.`{keys[0]}` IS NOT NULL
                    AND a.`{hash_col}` <> b.`{hash_col}` THEN 1 ELSE 0 END) AS updated
        FROM (SELECT * FROM {table} VERSION AS OF {older}) a
        FULL OUTER JOIN (SELECT * FROM {table} VERSION AS OF {newer}) b ON {on}
    """).collect()[0]
    return {k: int(row[k] or 0) for k in ("inserted", "deleted", "updated")}


def table_stats(spark: SparkSession, table: str) -> dict:
    snaps = spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").collect()[0]["n"]
    files = spark.sql(f"SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS bytes, "
                      f"coalesce(sum(record_count), 0) AS records FROM {table}.files").collect()[0]
    return {"table": table, "snapshots": int(snaps), "data_files": int(files["n"]),
            "bytes": int(files["bytes"]), "records": int(files["records"]),
            "current_snapshot": current_snapshot(spark, table)}


def lakehouse_status(spark: SparkSession, config: PipelineConfig) -> list[dict]:
    from openlake.iceberg.catalog_manager import CatalogManager

    cm = CatalogManager(spark, config)
    out = []
    for layer in ("bronze", "silver", "gold", "quarantine", "ops"):
        for table in cm.list_tables(layer):
            out.append({"layer": layer, **table_stats(spark, table)})
    return out

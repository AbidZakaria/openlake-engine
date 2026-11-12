"""Incremental reads.

Bronze tables are append-only, so "what is new since the last run?" is answered
by Iceberg itself: an *incremental append scan* between the snapshot Silver
last consumed and the table's current snapshot. Only data files added by the
snapshots in that range are read — no full scans, no timestamp heuristics.

Gold reads Silver, which is upserted with MERGE (overwrite snapshots), so Gold
uses a row-level change marker instead: ``_silver_updated_at``.

Watermarks are stored in ``ops.watermarks`` as JSON so they can be inspected
(and reset) with SQL.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from openlake.iceberg.catalog_manager import CatalogManager
from openlake.pipeline.ops import OpsLog


@dataclass
class Increment:
    df: DataFrame | None
    start_snapshot: int | None
    end_snapshot: int | None
    end_committed_at: str | None
    mode: str  # "full" | "incremental" | "fallback" | "none"


def snapshot_committed_at(spark: SparkSession, table: str, snapshot_id: int) -> str | None:
    rows = spark.sql(f"SELECT CAST(committed_at AS STRING) AS ts FROM {table}.snapshots "
                     f"WHERE snapshot_id = {snapshot_id}").collect()
    return rows[0]["ts"] if rows else None


def read_bronze_increment(spark: SparkSession, cm: CatalogManager, ops: OpsLog, pipeline: str,
                          dataset: str, table: str) -> Increment:
    current = cm.current_snapshot_id(table)
    if current is None:
        return Increment(None, None, None, None, "none")
    raw_wm = ops.get_watermark(pipeline, dataset)
    wm = json.loads(raw_wm) if raw_wm else None
    committed = snapshot_committed_at(spark, table, current)
    if wm is None:
        return Increment(spark.table(table), None, current, committed, "full")
    start = int(wm["snapshot_id"])
    if start == current:
        return Increment(None, start, current, committed, "none")
    try:
        df = (spark.read.format("iceberg").option("start-snapshot-id", str(start))
              .option("end-snapshot-id", str(current)).load(table))
        df.schema  # noqa: B018 - force planning so a missing snapshot fails here
        return Increment(df, start, current, committed, "incremental")
    except Exception:
        # The watermark snapshot was expired by maintenance: fall back to the
        # ingestion timestamp recorded with it.
        df = spark.table(table).filter(F.col("_ingested_at") > F.lit(wm["ingested_through"]).cast("timestamp"))
        return Increment(df, start, current, committed, "fallback")


def save_bronze_watermark(spark: SparkSession, ops: OpsLog, pipeline: str, dataset: str, table: str,
                          inc: Increment) -> None:
    max_ingested = spark.sql(f"SELECT CAST(max(_ingested_at) AS STRING) AS m FROM {table}").collect()[0]["m"]
    ops.set_watermark(pipeline, dataset, json.dumps({
        "snapshot_id": inc.end_snapshot, "committed_at": inc.end_committed_at, "ingested_through": max_ingested,
    }))

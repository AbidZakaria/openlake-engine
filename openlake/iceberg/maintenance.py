"""Table maintenance.

Frequent incremental commits leave behind small files, many manifests, old
snapshots and (after crashes) unreferenced files. This module runs the four
standard Iceberg maintenance procedures on every managed table:

1. ``rewrite_data_files``  — bin-pack small data files (compaction)
2. ``rewrite_manifests``   — cluster manifests so planning reads fewer of them
3. ``expire_snapshots``    — drop snapshots outside the retention policy and the
   files only they referenced (time travel ends where retention ends)
4. ``remove_orphan_files`` — delete files no snapshot references (e.g. written by
   a crashed job). The age threshold protects files of in-flight writes.

Retention defaults come from ``maintenance`` in the pipeline config.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from openlake.config.pipeline import PipelineConfig
from openlake.iceberg.catalog_manager import CatalogManager
from openlake.pipeline.ops import OpsLog, now

LAYERS = ("bronze", "silver", "gold", "quarantine", "ops")


def _short(table: str, catalog: str) -> str:
    return table.split(".", 1)[1] if table.startswith(catalog + ".") else table


def data_file_count(spark: SparkSession, table: str) -> int:
    return int(spark.sql(f"SELECT count(*) AS n FROM {table}.files").collect()[0]["n"])


def compact(spark: SparkSession, table: str, catalog: str, min_input_files: int, target_mb: int) -> dict:
    row = spark.sql(f"""
        CALL {catalog}.system.rewrite_data_files(
            table => '{_short(table, catalog)}',
            options => map('min-input-files', '{min_input_files}',
                           'target-file-size-bytes', '{target_mb * 1024 * 1024}',
                           'partial-progress.enabled', 'true'))
    """).collect()[0].asDict()
    return {k: int(v) for k, v in row.items() if v is not None}


def rewrite_manifests(spark: SparkSession, table: str, catalog: str) -> dict:
    row = spark.sql(f"CALL {catalog}.system.rewrite_manifests(table => '{_short(table, catalog)}')").collect()[0]
    return {k: int(v) for k, v in row.asDict().items() if v is not None}


def expire_snapshots(spark: SparkSession, table: str, catalog: str, older_than: str, retain_last: int) -> dict:
    row = spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{_short(table, catalog)}', older_than => TIMESTAMP '{older_than}', retain_last => {retain_last})
    """).collect()[0]
    return {k: int(v) for k, v in row.asDict().items() if v is not None}


def unreferenced_data_files(spark: SparkSession, table: str, store) -> list[str]:
    """Files under the table's data/ prefix that no snapshot references (what orphan cleanup targets)."""
    referenced = {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {table}.all_files").collect()}
    location = spark.sql(f"SELECT file FROM {table}.metadata_log_entries LIMIT 1").collect()[0]["file"]
    data_prefix = location.split(f"s3://{store.bucket}/", 1)[1].rsplit("/metadata/", 1)[0] + "/data/"
    return [store.uri(o.key) for o in store.list(data_prefix) if store.uri(o.key) not in referenced]


def remove_orphans_action(spark: SparkSession, table: str, older_than: datetime) -> list[str]:
    """Iceberg's DeleteOrphanFiles action. Unlike the SQL procedure it accepts intervals
    shorter than 24h — safe only when no other writer is active (demo / maintenance window)."""
    jvm = spark._jvm
    jtable = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    result = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
              .deleteOrphanFiles(jtable).olderThan(int(older_than.replace(tzinfo=timezone.utc).timestamp() * 1000))
              .execute())
    it = result.orphanFileLocations().iterator()
    out = []
    while it.hasNext():
        out.append(str(it.next()))
    return out


def remove_orphans(spark: SparkSession, table: str, catalog: str, older_than: str, dry_run: bool = False) -> list[str]:
    cutoff = datetime.strptime(older_than, "%Y-%m-%d %H:%M:%S")
    if not dry_run and datetime.now(timezone.utc).replace(tzinfo=None) - cutoff < timedelta(hours=24):
        return remove_orphans_action(spark, f"{catalog}.{_short(table, catalog)}", cutoff)
    rows = spark.sql(f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{_short(table, catalog)}', older_than => TIMESTAMP '{older_than}', dry_run => {str(dry_run).lower()})
    """).collect()
    return [r["orphan_file_location"] for r in rows]


def run_maintenance(spark: SparkSession, config: PipelineConfig, ops: OpsLog, aggressive: bool = False) -> dict:
    cm = CatalogManager(spark, config)
    policy = config.maintenance
    catalog = config.catalog
    ts_now = now()
    expire_before = (ts_now if aggressive else ts_now - timedelta(days=policy.max_age_days)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (ts_now if aggressive else ts_now - timedelta(hours=policy.orphan_older_than_hours)
                     ).strftime("%Y-%m-%d %H:%M:%S")
    summary: dict[str, dict] = {}
    for layer in LAYERS:
        for table in cm.list_tables(layer):
            metrics: dict = {"files_before": data_file_count(spark, table)}
            started = now()
            t0 = time.perf_counter()
            with ops.step(table.split(".", 1)[1], table) as step:
                if metrics["files_before"] >= policy.min_input_files:
                    metrics["compaction"] = compact(spark, table, catalog, policy.min_input_files,
                                                    policy.target_file_size_mb)
                metrics["manifests"] = rewrite_manifests(spark, table, catalog)
                metrics["expire"] = expire_snapshots(spark, table, catalog, expire_before, policy.retain_last)
                metrics["orphans_removed"] = len(remove_orphans(spark, table, catalog, orphan_before))
                metrics["files_after"] = data_file_count(spark, table)
                step.input_records = metrics["files_before"]
                step.output_records = metrics["files_after"]
                step.snapshot_id = cm.current_snapshot_id(table)
                step.details = metrics
            ops.maintenance(table, "full", metrics, time.perf_counter() - t0, started)
            summary[table] = {"files_before": metrics["files_before"], "files_after": metrics["files_after"],
                              "snapshots_expired": metrics["expire"].get("deleted_manifest_lists_count", 0),
                              "orphans_removed": metrics["orphans_removed"]}
    return summary

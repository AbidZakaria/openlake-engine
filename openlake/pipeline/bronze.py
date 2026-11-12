"""Bronze: raw files -> append-only Iceberg tables.

The raw files themselves stay untouched in ``raw/`` (the immutable landing
zone). Bronze additionally registers their content as Iceberg tables so raw
data is queryable, snapshotted and incrementally consumable:

* every source column is kept as STRING, with its original name — no business
  rule can drop a record here;
* lineage columns are added: ``_batch_id``, ``_source_file``, ``_ingested_at``,
  ``_source_format``, ``_source_columns`` (the file's header) and a
  ``_record_hash`` of the raw values;
* new upstream columns simply widen the Bronze table (Bronze is permissive;
  the Silver contract decides whether a change is acceptable);
* tables are append-only, which is what lets Silver read them incrementally
  by snapshot range.
"""

from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from openlake.config.pipeline import PipelineConfig
from openlake.iceberg.catalog_manager import CatalogManager
from openlake.ingestion.batch_manager import REGISTERED, BatchManager
from openlake.pipeline import writer
from openlake.pipeline.ops import OpsLog
from openlake.storage import ObjectStore

BRONZE_PARTITION = ("days(_ingested_at)",)


def read_raw(spark: SparkSession, uri: str, fmt: str) -> DataFrame:
    if fmt == "csv":
        df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
              .option("escape", '"').option("multiLine", True).csv(uri))
    elif fmt == "json":
        df = spark.read.option("primitivesAsString", True).option("mode", "PERMISSIVE").json(uri)
    elif fmt == "parquet":
        df = spark.read.parquet(uri)
    else:
        raise ValueError(f"unsupported raw format {fmt}")
    return df.select(*[F.col(f"`{c}`").cast("string").alias(c) for c in df.columns])


def with_lineage(df: DataFrame, batch_id: str, source_file: str, ingested_at: datetime, fmt: str,
                 source_columns: list[str]) -> DataFrame:
    data_cols = [c for c in df.columns if not c.startswith("_corrupt")]
    return (
        df.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_format", F.lit(fmt))
        .withColumn("_source_columns", F.lit(json.dumps(source_columns)))
        .withColumn("_record_hash", F.sha2(F.concat_ws("␟", *[F.coalesce(F.col(f"`{c}`"), F.lit("␀"))
                                                                 for c in data_cols]), 256))
    )


def run_bronze(spark: SparkSession, config: PipelineConfig, store: ObjectStore, ops: OpsLog) -> dict:
    cm = CatalogManager(spark, config)
    manager = BatchManager(store, config)
    pending = manager.pending()
    summary = {"batches": [m.batch_id for m in pending], "tables": {}}

    for manifest in pending:
        ingested_at = datetime.fromisoformat(manifest.created_at).replace(tzinfo=None)
        for f in [f for f in manifest.files if f.status == REGISTERED]:
            table = config.table("bronze", f.entity)
            with ops.step(f.entity, table) as step:
                step.batch_ids.append(manifest.batch_id)
                df = read_raw(spark, store.spark_uri(f.key), f.format)
                df = with_lineage(df, manifest.batch_id, f.key, ingested_at, f.format, f.columns or df.columns)
                df = df.cache()
                rows = df.count()
                step.input_records = f.record_count or rows
                snapshot = writer.append(cm, table, df, BRONZE_PARTITION,
                                         comment=f"Bronze: raw {f.entity} records, append-only")
                df.unpersist()
                step.output_records = rows
                step.snapshot_id = snapshot
                step.details = {"file": f.key, "format": f.format, "bytes": f.size_bytes}
                if f.record_count is not None and f.record_count != rows:
                    step.details["record_count_mismatch"] = {"profiled": f.record_count, "loaded": rows}
                manager.mark_loaded(manifest, f.key, snapshot, rows)
                summary["tables"].setdefault(table, 0)
                summary["tables"][table] += rows

    # Mirror every manifest (including duplicate/rejected-only batches) into ops.ingestion_batches.
    ops.upsert_batches(manager.manifests())
    return summary

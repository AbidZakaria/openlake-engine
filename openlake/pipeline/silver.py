"""Silver: validated, typed, deduplicated, upserted.

For each entity, in dependency order:

1. read only the Bronze rows appended since the last run (Iceberg incremental scan)
2. check each source file's columns against the data contract (schema evolution policy)
3. rename/snake_case columns, trim, standardize categorical values
4. cast to contract types, keeping raw text so cast failures are reportable
5. evaluate quality rules; error-level failures go to ``quarantine.<entity>``
6. deduplicate by business key (latest ingested image wins)
7. ``MERGE INTO silver.<entity>`` — insert new keys, update changed ones
8. referential-integrity checks against already-processed Silver tables
9. record a quality report, schema changes, run metrics and the new watermark
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from openlake.config.pipeline import EntitySpec, PipelineConfig
from openlake.iceberg.catalog_manager import CatalogManager
from openlake.iceberg.schema_evolution import PLATFORM_COLUMNS, classify
from openlake.pipeline import writer
from openlake.pipeline.incremental import read_bronze_increment, save_bronze_watermark
from openlake.pipeline.ops import OpsLog, now
from openlake.quality.quality_report import QualityReport
from openlake.quality.validator import FAILED_COL, foreign_key_result, uniqueness_result, validate
from openlake.transformations.cleaning import RAW_PREFIX, cast_to_contract, trim_and_nullify
from openlake.transformations.deduplication import latest_by_key
from openlake.transformations.standardization import standardize_column_names, standardize_values

LINEAGE = ["_batch_id", "_source_file", "_ingested_at", "_record_hash"]


def _evolved_columns(cm: CatalogManager, silver_table: str, entity: EntitySpec) -> dict[str, str]:
    """Columns the Silver table gained through earlier accepted schema changes."""
    if not cm.exists(silver_table):
        return {}
    contract = set(entity.column_names)
    out = {}
    for f in cm.spark.table(silver_table).schema.fields:
        if f.name not in contract and not f.name.startswith("_"):
            out[f.name] = entity.evolution_hints.get(f.name, "string")
    return out


def process_entity(spark: SparkSession, config: PipelineConfig, entity: EntitySpec, ops: OpsLog) -> dict:
    cm = CatalogManager(spark, config)
    bronze_t = config.table("bronze", entity.name)
    silver_t = config.table("silver", entity.name)
    quarantine_t = config.table("quarantine", entity.name)

    with ops.step(entity.name, silver_t) as step:
        inc = read_bronze_increment(spark, cm, ops, "silver", entity.name, bronze_t)
        if inc.df is None:
            step.details = {"skipped": True, "reason": "no new bronze snapshots"}
            return {"entity": entity.name, "skipped": True}
        increment = inc.df.cache()
        total = increment.count()
        batches = [r["_batch_id"] for r in increment.select("_batch_id").distinct().collect()]
        step.batch_ids.extend(batches)
        step.input_records = total

        # ---- 2. schema contract, per source file layout ------------------------------
        evolved = _evolved_columns(cm, silver_t, entity)
        known = entity.column_names + list(evolved)
        rejected_files: list[str] = []
        layouts = increment.select("_batch_id", "_source_file", "_source_columns").distinct().collect()
        for layout in layouts:
            decision = classify(entity, json.loads(layout["_source_columns"]), config.evolution, known)
            for change in decision.changes:
                ops.schema_change(entity.name, layout["_batch_id"], change)
            if not decision.accepted:
                # A file with any rejected change is quarantined as a whole and
                # none of its changes (not even additive ones) reach the table.
                rejected_files.append(layout["_source_file"])
                continue
            for change in decision.columns_to_add:
                if change.column not in evolved:
                    evolved[change.column] = change.new_type or "string"
                    known.append(change.column)

        # ---- 3. names + cleaning + standardization -----------------------------------
        df = standardize_column_names(increment, entity)
        data_cols = [c for c in df.columns if c not in PLATFORM_COLUMNS and not c.startswith("_")]
        df = trim_and_nullify(df, data_cols)
        df = standardize_values(df, entity)

        # ---- 4. typing ----------------------------------------------------------------
        df, type_checks = cast_to_contract(df, entity, evolved)
        contract_cols = entity.column_names + [c for c in evolved if c not in entity.column_names]

        # ---- 5. quality ---------------------------------------------------------------
        schema_fail = (F.when(F.col("_source_file").isin(rejected_files), F.array(F.lit("schema_contract")))
                       if rejected_files else None)
        result = validate(df, entity, type_checks, schema_fail)
        invalid_count = result.invalid_count
        if invalid_count:
            quarantine = result.invalid.select(
                *[F.coalesce(F.col(f"{RAW_PREFIX}{c}") if f"{RAW_PREFIX}{c}" in df.columns else F.col(c).cast("string"),
                             F.lit(None)).cast("string").alias(c) for c in contract_cols],
                *LINEAGE, F.col(FAILED_COL), F.lit(now()).alias("_quarantined_at"), F.lit(ops.run_id).alias("_run_id"),
            )
            writer.append(cm, quarantine_t, quarantine, comment=f"Quarantined {entity.name} records with failed rules")

        valid = result.valid.select(*contract_cols, *LINEAGE)

        # ---- 6. dedup ------------------------------------------------------------------
        unique_rules = [r for r in entity.rules if r.type == "unique"]
        dataset_results = [uniqueness_result(valid, r) for r in unique_rules]
        deduped = latest_by_key(valid, entity.keys).cache()
        valid_count = total - invalid_count
        out_count = deduped.count()
        duplicates = valid_count - out_count

        # ---- 7. upsert -----------------------------------------------------------------
        silver_df = (deduped.withColumnRenamed("_batch_id", "_source_batch_id")
                     .withColumn("_silver_updated_at", F.lit(now()).cast("timestamp"))
                     .withColumn("_silver_run_id", F.lit(ops.run_id)))
        inserted, updated, unchanged = _merge_counts(spark, cm, silver_t, silver_df, entity.keys)
        snapshot = writer.merge_upsert(
            cm, silver_t, silver_df, entity.keys, entity.partition_by,
            comment=f"Silver: cleansed and conformed {entity.name}",
            only_if_changed="_record_hash", newer_than="_ingested_at",
        )

        # ---- 8. referential integrity (warn-level) ------------------------------------
        for rule in entity.rules:
            if rule.type == "foreign_key":
                dataset_results.append(foreign_key_result(spark, deduped, rule, config.table("silver", rule.ref_table)))

        # ---- 9. bookkeeping ------------------------------------------------------------
        report = QualityReport(ops.run_id, entity.name, "silver", batches, total, valid_count, invalid_count,
                               duplicates, result.null_counts, result.rule_results + dataset_results)
        ops.quality(report)
        save_bronze_watermark(spark, ops, "silver", entity.name, bronze_t, inc)
        step.output_records = out_count
        step.quarantined_records = invalid_count
        step.snapshot_id = snapshot
        step.details = {
            "read_mode": inc.mode, "bronze_snapshot_range": [inc.start_snapshot, inc.end_snapshot],
            "inserted": inserted, "updated": updated, "unchanged": unchanged, "duplicates_removed": duplicates,
            "rejected_files": rejected_files, "evolved_columns": list(evolved), "quality_score": report.score,
        }
        increment.unpersist()
        deduped.unpersist()
        return {"entity": entity.name, **step.details, "input": total, "output": out_count,
                "quarantined": invalid_count}


def _merge_counts(spark: SparkSession, cm: CatalogManager, table: str, src: DataFrame,
                  keys: tuple[str, ...]) -> tuple[int, int, int]:
    """Classify incoming rows as insert / update / unchanged before the MERGE commits."""
    if not cm.exists(table):
        return src.count(), 0, 0
    target = spark.table(table).select(*keys, F.col("_record_hash").alias("__t_hash"))
    joined = src.select(*keys, "_record_hash").join(target, list(keys), "left")
    row = joined.agg(
        F.sum(F.when(F.col("__t_hash").isNull(), 1).otherwise(0)).alias("ins"),
        F.sum(F.when(F.col("__t_hash").isNotNull() & (F.col("__t_hash") != F.col("_record_hash")), 1)
              .otherwise(0)).alias("upd"),
        F.sum(F.when(F.col("__t_hash") == F.col("_record_hash"), 1).otherwise(0)).alias("same"),
    ).collect()[0]
    return int(row["ins"] or 0), int(row["upd"] or 0), int(row["same"] or 0)


def run_silver(spark: SparkSession, config: PipelineConfig, ops: OpsLog, entities: list[str] | None = None) -> list[dict]:
    results = []
    for entity in config.ordered_entities():
        if entities and entity.name not in entities:
            continue
        results.append(process_entity(spark, config, entity, ops))
    return results

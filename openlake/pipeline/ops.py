"""Pipeline observability tables (namespace ``ops``).

Every job writes what it did into Iceberg tables next to the data it produced,
so lineage questions are answered with SQL::

    -- which source batches produced the current state of gold.fact_orders?
    SELECT * FROM lake.ops.pipeline_runs WHERE target_table = 'lake.gold.fact_orders';

Rows are buffered per job and flushed in one append per table to keep the
number of commits (and small files) low.
"""

from __future__ import annotations

import json
import time
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from openlake.config.pipeline import PipelineConfig
from openlake.quality.quality_report import QualityReport

S, L, D, TS, INT = T.StringType(), T.LongType(), T.DoubleType(), T.TimestampType(), T.IntegerType()


def _schema(*fields: tuple[str, T.DataType]) -> T.StructType:
    return T.StructType([T.StructField(n, t, True) for n, t in fields])


OPS_SCHEMAS: dict[str, T.StructType] = {
    "pipeline_runs": _schema(
        ("run_id", S), ("job_id", S), ("stage", S), ("dataset", S), ("target_table", S),
        ("source_batch_ids", S), ("input_records", L), ("output_records", L), ("quarantined_records", L),
        ("status", S), ("error", S), ("started_at", TS), ("finished_at", TS), ("duration_s", D),
        ("snapshot_id", L), ("details", S),
    ),
    "ingestion_batches": _schema(
        ("batch_id", S), ("source", S), ("label", S), ("created_at", TS), ("status", S),
        ("file_count", INT), ("registered_files", INT), ("loaded_files", INT), ("duplicate_files", INT),
        ("rejected_files", INT), ("total_bytes", L), ("total_records", L), ("files_json", S), ("updated_at", TS),
    ),
    "quality_reports": _schema(
        ("run_id", S), ("dataset", S), ("layer", S), ("batch_ids", S), ("total_records", L),
        ("valid_records", L), ("invalid_records", L), ("duplicate_records", L), ("quality_score", D),
        ("status", S), ("failed_rules", S), ("null_counts", S), ("executed_at", TS),
    ),
    "quality_rule_results": _schema(
        ("run_id", S), ("dataset", S), ("rule", S), ("rule_type", S), ("dimension", S), ("severity", S),
        ("column_name", S), ("checked", L), ("failed", L), ("pass_rate", D), ("executed_at", TS),
    ),
    "schema_changes": _schema(
        ("run_id", S), ("dataset", S), ("batch_id", S), ("change_type", S), ("column_name", S),
        ("action", S), ("detail", S), ("new_type", S), ("detected_at", TS),
    ),
    "watermarks": _schema(("pipeline", S), ("dataset", S), ("value", S), ("updated_at", TS)),
    "maintenance_runs": _schema(
        ("run_id", S), ("table_name", S), ("operation", S), ("metrics", S), ("started_at", TS), ("duration_s", D),
    ),
}

PARTITIONS = {"pipeline_runs": ["days(started_at)"]}


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_id(stage: str) -> str:
    return f"{stage}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


@dataclass
class StepResult:
    input_records: int = 0
    output_records: int = 0
    quarantined_records: int = 0
    snapshot_id: int | None = None
    target_table: str | None = None
    batch_ids: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


class OpsLog:
    def __init__(self, spark: SparkSession, config: PipelineConfig, stage: str):
        self.spark = spark
        self.config = config
        self.stage = stage
        self.run_id = new_run_id(stage)
        self._buffer: dict[str, list[dict]] = {name: [] for name in OPS_SCHEMAS}

    # ------------------------------------------------------------------ setup
    def table(self, name: str) -> str:
        return self.config.table("ops", name)

    def ensure_tables(self) -> None:
        from openlake.iceberg.catalog_manager import CatalogManager

        cm = CatalogManager(self.spark, self.config)
        for name, schema in OPS_SCHEMAS.items():
            df = self.spark.createDataFrame([], schema)
            cm.create_from_df(self.table(name), df, PARTITIONS.get(name, ()))

    # ---------------------------------------------------------------- logging
    @contextmanager
    def step(self, dataset: str, target_table: str | None = None):
        """Time a unit of work and record it in ``ops.pipeline_runs``."""
        result = StepResult(target_table=target_table)
        started = now()
        t0 = time.perf_counter()
        status, error = "SUCCESS", None
        try:
            yield result
        except Exception as exc:
            status = "FAILED"
            error = f"{type(exc).__name__}: {exc}"[:2000]
            result.details.setdefault("traceback", traceback.format_exc()[-4000:])
            raise
        finally:
            self._buffer["pipeline_runs"].append({
                "run_id": self.run_id,
                "job_id": f"{self.run_id}:{dataset}",
                "stage": self.stage,
                "dataset": dataset,
                "target_table": result.target_table,
                "source_batch_ids": ",".join(sorted(set(result.batch_ids))),
                "input_records": int(result.input_records),
                "output_records": int(result.output_records),
                "quarantined_records": int(result.quarantined_records),
                "status": status if not result.details.get("skipped") else "SKIPPED",
                "error": error,
                "started_at": started,
                "finished_at": now(),
                "duration_s": round(time.perf_counter() - t0, 3),
                "snapshot_id": result.snapshot_id,
                "details": json.dumps(result.details, default=str),
            })

    def quality(self, report: QualityReport) -> None:
        self._buffer["quality_reports"].append(report.to_row())
        self._buffer["quality_rule_results"].extend(report.rule_rows())

    def schema_change(self, dataset: str, batch_id: str, change) -> None:
        self._buffer["schema_changes"].append({
            "run_id": self.run_id, "dataset": dataset, "batch_id": batch_id,
            "change_type": change.change_type, "column_name": change.column, "action": change.action,
            "detail": change.detail, "new_type": change.new_type, "detected_at": now(),
        })

    def maintenance(self, table: str, operation: str, metrics: dict, duration_s: float, started: datetime) -> None:
        self._buffer["maintenance_runs"].append({
            "run_id": self.run_id, "table_name": table, "operation": operation,
            "metrics": json.dumps(metrics, default=str), "started_at": started, "duration_s": round(duration_s, 3),
        })

    def flush(self) -> None:
        for name, rows in self._buffer.items():
            if not rows:
                continue
            df = self.spark.createDataFrame(rows, OPS_SCHEMAS[name])
            df.writeTo(self.table(name)).append()
            self._buffer[name] = []

    # -------------------------------------------------------------- watermarks
    def get_watermark(self, pipeline: str, dataset: str) -> str | None:
        rows = self.spark.sql(
            f"SELECT value FROM {self.table('watermarks')} WHERE pipeline = '{pipeline}' AND dataset = '{dataset}' "
            f"ORDER BY updated_at DESC LIMIT 1"
        ).collect()
        return rows[0]["value"] if rows else None

    def set_watermark(self, pipeline: str, dataset: str, value: str) -> None:
        src = self.spark.createDataFrame([(pipeline, dataset, str(value), now())], OPS_SCHEMAS["watermarks"])
        src.createOrReplaceTempView("__wm")
        self.spark.sql(f"""
            MERGE INTO {self.table('watermarks')} t USING __wm s
            ON t.pipeline = s.pipeline AND t.dataset = s.dataset
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def upsert_batches(self, manifests) -> None:
        if not manifests:
            return
        rows = []
        for m in manifests:
            files = m.files
            rows.append({
                "batch_id": m.batch_id, "source": m.source, "label": m.label,
                "created_at": datetime.fromisoformat(m.created_at).replace(tzinfo=None),
                "status": m.status, "file_count": len(files),
                "registered_files": sum(f.status == "REGISTERED" for f in files),
                "loaded_files": sum(f.status == "BRONZE_LOADED" for f in files),
                "duplicate_files": sum(f.status == "DUPLICATE" for f in files),
                "rejected_files": sum(f.status == "REJECTED" for f in files),
                "total_bytes": sum(f.size_bytes or 0 for f in files),
                "total_records": sum(f.record_count or 0 for f in files),
                "files_json": json.dumps([f.__dict__ for f in files], default=str),
                "updated_at": now(),
            })
        self.spark.createDataFrame(rows, OPS_SCHEMAS["ingestion_batches"]).createOrReplaceTempView("__batches")
        self.spark.sql(f"""
            MERGE INTO {self.table('ingestion_batches')} t USING __batches s ON t.batch_id = s.batch_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

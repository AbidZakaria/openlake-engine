"""A deliberately small orchestrator.

The focus of this project is the lakehouse, not scheduling, so stages are
plain Python functions run in order. Each stage owns one ``OpsLog`` (one
``run_id``) and flushes its observability rows when it finishes — also when it
fails, so failed runs are visible in ``ops.pipeline_runs``.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from openlake.config.pipeline import PipelineConfig, load_config
from openlake.config.settings import get_settings
from openlake.ingestion.batch_manager import BatchManager
from openlake.pipeline.ops import OpsLog
from openlake.storage import ObjectStore, default_store

log = logging.getLogger("openlake")


class Orchestrator:
    def __init__(self, spark=None, config: PipelineConfig | None = None, store: ObjectStore | None = None):
        self.settings = get_settings()
        self.config = config or load_config()
        self._spark = spark
        self._store = store

    # lazily created so storage-only commands never start a JVM
    @property
    def spark(self):
        if self._spark is None:
            from openlake.config.spark import build_spark

            self._spark = build_spark("openlake-pipeline")
        return self._spark

    @property
    def store(self) -> ObjectStore:
        if self._store is None:
            self._store = default_store()
        return self._store

    def _ops(self, stage: str) -> OpsLog:
        return OpsLog(self.spark, self.config, stage)

    def _run_stage(self, stage: str, fn):
        ops = self._ops(stage)
        t0 = time.perf_counter()
        log.info("▶ %s (run %s)", stage, ops.run_id)
        try:
            result = fn(ops)
        finally:
            ops.flush()
        log.info("✔ %s finished in %.1fs", stage, time.perf_counter() - t0)
        return result

    # ------------------------------------------------------------------ stages
    def init(self) -> None:
        from openlake.iceberg.catalog_manager import CatalogManager

        CatalogManager(self.spark, self.config).ensure_namespaces()
        self._ops("init").ensure_tables()

    def ingested_labels(self) -> set[str]:
        return {m.label for m in BatchManager(self.store, self.config).manifests()}

    def ingest(self, path: Path, label: str | None = None, source: str = "ecommerce") -> dict:
        manifest = BatchManager(self.store, self.config).ingest(Path(path), source=source, label=label or Path(path).name)
        return manifest.to_dict()

    def ingest_new_batches(self, root: Path | None = None) -> list[dict]:
        root = Path(root or self.settings.data_dir / "generated")
        done = self.ingested_labels()
        results = []
        for batch_dir in sorted(p for p in root.glob("batch_*") if p.is_dir()):
            if batch_dir.name not in done:
                results.append(self.ingest(batch_dir))
        return results

    def bronze(self) -> dict:
        from openlake.pipeline.bronze import run_bronze

        return self._run_stage("bronze", lambda ops: run_bronze(self.spark, self.config, self.store, ops))

    def silver(self, entities: list[str] | None = None) -> list[dict]:
        from openlake.pipeline.silver import run_silver

        return self._run_stage("silver", lambda ops: run_silver(self.spark, self.config, ops, entities))

    def gold(self) -> dict:
        from openlake.pipeline.gold import run_gold

        return self._run_stage("gold", lambda ops: run_gold(self.spark, self.config, ops))

    def maintain(self, aggressive: bool = False) -> dict:
        from openlake.iceberg.maintenance import run_maintenance

        return self._run_stage("maintenance", lambda ops: run_maintenance(self.spark, self.config, ops, aggressive))

    def run(self) -> dict:
        """Process everything that has landed but not been processed yet."""
        return {"bronze": self.bronze(), "silver": self.silver(), "gold": self.gold()}

    def save_report(self, name: str, payload: dict) -> Path:
        out = self.settings.reports_dir / f"{name}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, default=str))
        return out

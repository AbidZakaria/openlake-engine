"""Ingestion batches: land raw files in MinIO and register them.

Workflow for every call to :meth:`BatchManager.ingest`:

1. read the candidate files and pick a format adapter by extension
2. validate structure (readable, header/columns, not empty)
3. compute ingestion metadata (size, sha256, record count, columns)
4. assign a batch id
5. copy each file to ``raw/<source>/<entity>/ingest_date=…/batch_id=…/<file>``
6. write a batch manifest to ``control/batches/<batch_id>.json``
7. the manifest's files are now ``REGISTERED`` — visible to the Bronze job

Duplicate *files* (same sha256 as a file already landed) are detected via a
checksum index and recorded as ``DUPLICATE`` without being copied again, so
re-running an ingestion is idempotent.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from openlake.config.pipeline import PipelineConfig
from openlake.ingestion import IngestionError, ingestor_for
from openlake.storage import ObjectStore, get_json, put_json

CONTROL_PREFIX = "control/batches"
CHECKSUM_PREFIX = "control/checksums"

REGISTERED = "REGISTERED"
DUPLICATE = "DUPLICATE"
REJECTED = "REJECTED"
BRONZE_LOADED = "BRONZE_LOADED"


def new_batch_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"b{now:%Y%m%d%H%M%S}_{uuid.uuid4().hex[:6]}"


@dataclass
class BatchFile:
    filename: str
    entity: str | None
    format: str | None
    status: str
    key: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    record_count: int | None = None
    columns: list[str] = field(default_factory=list)
    error: str | None = None
    duplicate_of: str | None = None
    bronze_snapshot_id: int | None = None
    bronze_rows: int | None = None


@dataclass
class BatchManifest:
    batch_id: str
    source: str
    label: str
    created_at: str
    files: list[BatchFile] = field(default_factory=list)

    @property
    def status(self) -> str:
        statuses = {f.status for f in self.files}
        if not statuses:
            return "EMPTY"
        if REGISTERED in statuses:
            return REGISTERED
        if statuses <= {DUPLICATE, REJECTED}:
            return REJECTED if REJECTED in statuses else DUPLICATE
        return BRONZE_LOADED

    def to_dict(self) -> dict:
        data = asdict(self)
        data["status"] = self.status
        return data

    @classmethod
    def from_dict(cls, data: dict) -> BatchManifest:
        files = [BatchFile(**f) for f in data.get("files", [])]
        return cls(
            batch_id=data["batch_id"],
            source=data["source"],
            label=data.get("label", ""),
            created_at=data["created_at"],
            files=files,
        )


class BatchManager:
    def __init__(self, store: ObjectStore, config: PipelineConfig):
        self.store = store
        self.config = config

    # ------------------------------------------------------------------ write
    def ingest(self, paths: list[Path] | Path, source: str = "ecommerce", label: str = "",
               batch_id: str | None = None) -> BatchManifest:
        if isinstance(paths, Path | str):
            root = Path(paths)
            paths = sorted(p for p in root.iterdir() if p.is_file()) if root.is_dir() else [root]
        if source not in self.config.raw_prefixes:
            raise IngestionError(f"unknown source {source!r}")
        now = datetime.now(timezone.utc)
        manifest = BatchManifest(batch_id or new_batch_id(now), source, label, now.isoformat())
        raw_prefix = self.config.raw_prefixes[source]

        for path in paths:
            path = Path(path)
            if path.name.startswith((".", "_")):
                continue
            entity = self.config.entity_for_file(path.name)
            ingestor = ingestor_for(path)
            if ingestor is None or entity is None:
                reason = "unsupported file format" if ingestor is None else "no entity matches this file name"
                manifest.files.append(BatchFile(path.name, entity.name if entity else None,
                                                ingestor.format if ingestor else None, REJECTED, error=reason))
                continue
            try:
                profile = ingestor.profile(path)
            except IngestionError as exc:
                manifest.files.append(BatchFile(path.name, entity.name, ingestor.format, REJECTED, error=str(exc)))
                continue

            checksum_key = f"{CHECKSUM_PREFIX}/{profile.sha256}.json"
            if self.store.exists(checksum_key):
                previous = get_json(self.store, checksum_key)
                manifest.files.append(BatchFile(
                    path.name, entity.name, profile.format, DUPLICATE,
                    size_bytes=profile.size_bytes, sha256=profile.sha256, record_count=profile.record_count,
                    columns=profile.columns, duplicate_of=previous.get("batch_id"),
                    error=f"identical file already ingested in batch {previous.get('batch_id')}",
                ))
                continue

            key = (f"{raw_prefix}/{entity.name}/ingest_date={now:%Y-%m-%d}/"
                   f"batch_id={manifest.batch_id}/{path.name}")
            self.store.put_file(path, key)
            put_json(self.store, checksum_key, {"batch_id": manifest.batch_id, "key": key})
            manifest.files.append(BatchFile(
                path.name, entity.name, profile.format, REGISTERED, key=key,
                size_bytes=profile.size_bytes, sha256=profile.sha256,
                record_count=profile.record_count, columns=profile.columns,
            ))

        self.save(manifest)
        return manifest

    def save(self, manifest: BatchManifest) -> None:
        put_json(self.store, f"{CONTROL_PREFIX}/{manifest.batch_id}.json", manifest.to_dict())

    def mark_loaded(self, manifest: BatchManifest, key: str, snapshot_id: int | None, rows: int) -> None:
        for f in manifest.files:
            if f.key == key:
                f.status = BRONZE_LOADED
                f.bronze_snapshot_id = snapshot_id
                f.bronze_rows = rows
        self.save(manifest)

    # ------------------------------------------------------------------- read
    def manifests(self) -> list[BatchManifest]:
        items = [o for o in self.store.list(CONTROL_PREFIX + "/") if o.key.endswith(".json")]
        manifests = [BatchManifest.from_dict(get_json(self.store, o.key)) for o in items]
        return sorted(manifests, key=lambda m: (m.created_at, m.batch_id))

    def pending(self) -> list[BatchManifest]:
        return [m for m in self.manifests() if any(f.status == REGISTERED for f in m.files)]

"""Ingestion throughput: profile (validate + count + sha256) and upload, per file format.

Uploads go to a scratch prefix (``bench/ingest/``) so the benchmark never
registers a batch or trips duplicate detection.
"""

from __future__ import annotations

import time
from pathlib import Path

from openlake.ingestion import ingestor_for
from openlake.storage import ObjectStore


def ingestion_throughput(store: ObjectStore, batch_dir: Path) -> list[dict]:
    results = []
    if not batch_dir.exists():
        return results
    for path in sorted(p for p in batch_dir.iterdir() if p.is_file() and not p.name.startswith("_")):
        ingestor = ingestor_for(path)
        if ingestor is None:
            continue
        t0 = time.perf_counter()
        profile = ingestor.profile(path)
        t1 = time.perf_counter()
        key = f"bench/ingest/{path.name}"
        store.put_file(path, key)
        t2 = time.perf_counter()
        store.delete(key)
        mb = profile.size_bytes / 1_048_576
        results.append({
            "file": path.name, "format": profile.format, "mb": round(mb, 2), "records": profile.record_count,
            "profile_s": round(t1 - t0, 3), "upload_s": round(t2 - t1, 3),
            "mb_per_s": round(mb / (t2 - t0), 1) if t2 > t0 else None,
            "records_per_s": round((profile.record_count or 0) / (t2 - t0)) if t2 > t0 else None,
        })
    return results

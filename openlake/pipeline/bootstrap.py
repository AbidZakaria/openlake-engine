"""End-to-end demo run used by ``docker compose up`` (idempotent).

1. generate the historical batch and two CDC batches (if not present)
2. for each batch, in order: ingest -> bronze -> silver -> gold
   (so the lakehouse history really contains three incremental runs)
3. run the Iceberg feature showcase
4. run table maintenance
5. run the benchmarks
6. write a completion marker so the next ``up`` is a no-op
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

from openlake.config.settings import get_settings
from openlake.storage import get_json, put_json

log = logging.getLogger("openlake")
MARKER = "control/bootstrap.json"


def bootstrap(force: bool = False) -> int:
    from openlake.datagen.cdc_simulator import simulate_batch
    from openlake.datagen.generator import generate_initial
    from openlake.pipeline.orchestrator import Orchestrator
    from openlake.storage import default_store

    settings = get_settings()
    store = default_store()
    for attempt in range(30):
        try:
            store.ensure_bucket()
            break
        except Exception as exc:
            log.warning("waiting for object storage (%s)", exc)
            time.sleep(2 + attempt)
    if store.exists(MARKER) and not force:
        done = get_json(store, MARKER)
        log.info("lakehouse already bootstrapped at %s — nothing to do (use `bootstrap --force` to rerun)",
                 done.get("finished_at"))
        return 0

    t0 = time.perf_counter()
    gen = settings.data_dir / "generated"
    if not (gen / "batch_0001_initial").exists():
        log.info("generating historical dataset (%s orders)", settings.scale_orders)
        generate_initial(gen, n_orders=settings.scale_orders)
    for n in (2, 3):
        if not any(gen.glob(f"batch_{n:04d}_*")):
            log.info("simulating CDC batch %s", n)
            simulate_batch(gen, n)

    orch = Orchestrator(store=store)
    orch.init()
    timings = {}
    done_labels = orch.ingested_labels()
    for batch_dir in sorted(p for p in gen.glob("batch_*") if p.is_dir()):
        if batch_dir.name in done_labels:
            continue
        s = time.perf_counter()
        log.info("━━ batch %s ━━", batch_dir.name)
        orch.ingest(batch_dir)
        orch.run()
        timings[batch_dir.name] = round(time.perf_counter() - s, 1)

    from openlake.benchmarks.query_benchmark import run_benchmarks
    from openlake.iceberg.showcase import run_showcase

    s = time.perf_counter()
    orch.save_report("iceberg_showcase", run_showcase(orch.spark, orch.config, store))
    timings["showcase"] = round(time.perf_counter() - s, 1)
    s = time.perf_counter()
    orch.maintain()
    timings["maintenance"] = round(time.perf_counter() - s, 1)
    s = time.perf_counter()
    run_benchmarks(orch.spark, orch.config, store)
    timings["benchmarks"] = round(time.perf_counter() - s, 1)

    summary = {"finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "seconds": round(time.perf_counter() - t0, 1), "timings": timings}
    put_json(store, MARKER, summary)
    orch.save_report("bootstrap", summary)
    log.info("bootstrap complete in %.0fs — dashboard: http://localhost:8501", summary["seconds"])
    log.info("%s", json.dumps(timings))
    return 0

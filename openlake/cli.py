"""Command-line entry point: ``python -m openlake <command>``.

Storage-only commands (init-storage, generate, simulate, ingest) never start a
JVM; Spark commands share one SparkSession per process.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from openlake.config.settings import get_settings

log = logging.getLogger("openlake")


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def cmd_init_storage(args) -> int:
    from openlake.storage import default_store

    store = default_store()
    for attempt in range(30):
        try:
            created = store.ensure_bucket()
            log.info("bucket %s %s", store.bucket, "created" if created else "already exists")
            return 0
        except Exception as exc:  # MinIO may still be starting
            log.warning("waiting for object storage (%s)", exc)
            time.sleep(2 + attempt * 0.5)
    return 1


def cmd_generate(args) -> int:
    from openlake.datagen.generator import generate_initial

    out = get_settings().data_dir / "generated"
    if (out / "batch_0001_initial").exists() and not args.force:
        log.info("initial batch already generated (use --force to regenerate)")
        return 0
    if args.force and out.exists():
        import shutil

        shutil.rmtree(out)
    _print(generate_initial(out, n_orders=args.orders or get_settings().scale_orders, seed=args.seed))
    return 0


def cmd_simulate(args) -> int:
    from openlake.datagen.cdc_simulator import simulate_batch

    _print(simulate_batch(get_settings().data_dir / "generated", args.batch, new_orders=args.orders))
    return 0


def cmd_ingest(args) -> int:
    from openlake.pipeline.orchestrator import Orchestrator

    orch = Orchestrator()
    results = [orch.ingest(Path(args.path), args.label)] if args.path else orch.ingest_new_batches()
    for m in results:
        statuses = {}
        for f in m["files"]:
            statuses[f["status"]] = statuses.get(f["status"], 0) + 1
        log.info("batch %s (%s): %s", m["batch_id"], m["label"], statuses)
    if not results:
        log.info("nothing new to ingest")
    return 0


def _orch():
    from openlake.pipeline.orchestrator import Orchestrator

    orch = Orchestrator()
    orch.init()
    return orch


def cmd_init(args) -> int:
    _orch()
    return 0


def cmd_bronze(args) -> int:
    _print(_orch().bronze())
    return 0


def cmd_silver(args) -> int:
    _print(_orch().silver(args.entity or None))
    return 0


def cmd_gold(args) -> int:
    _print(_orch().gold())
    return 0


def cmd_run(args) -> int:
    orch = _orch()
    orch.ingest_new_batches()
    _print(orch.run())
    return 0


def cmd_optimize(args) -> int:
    _print(_orch().maintain(aggressive=args.aggressive))
    return 0


def cmd_quality(args) -> int:
    orch = _orch()
    spark = orch.spark
    t = orch.config.table("ops", "quality_reports")
    spark.sql(f"""
        SELECT dataset, batch_ids, total_records, valid_records, invalid_records, duplicate_records,
               round(quality_score * 100, 2) AS score_pct, status, failed_rules
        FROM {t} ORDER BY executed_at DESC LIMIT {args.limit}
    """).show(args.limit, truncate=60)
    return 0


def cmd_showcase(args) -> int:
    from openlake.iceberg.showcase import run_showcase

    orch = _orch()
    result = run_showcase(orch.spark, orch.config, orch.store)
    path = orch.save_report("iceberg_showcase", result)
    log.info("showcase report written to %s", path)
    return 0


def cmd_benchmark(args) -> int:
    from openlake.benchmarks.query_benchmark import run_benchmarks

    orch = _orch()
    result = run_benchmarks(orch.spark, orch.config, orch.store, repeats=args.repeats)
    log.info("benchmarks written: %s", result.get("output"))
    return 0


def cmd_status(args) -> int:
    from openlake.iceberg.snapshot_manager import lakehouse_status

    orch = _orch()
    _print(lakehouse_status(orch.spark, orch.config))
    return 0


def cmd_query(args) -> int:
    from openlake.analytics.duckdb_client import LakeQuery

    with LakeQuery() as lq:
        sql = Path(args.sql).read_text() if args.sql.endswith(".sql") else args.sql
        print(lq.sql(sql).fetchdf().to_string(max_rows=args.limit))
    return 0


def cmd_bootstrap(args) -> int:
    from openlake.pipeline.bootstrap import bootstrap

    return bootstrap(force=args.force)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="openlake", description="OpenLake Engine — Iceberg lakehouse CLI")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init-storage", help="create the MinIO bucket").set_defaults(fn=cmd_init_storage)
    sub.add_parser("init", help="create namespaces and ops tables").set_defaults(fn=cmd_init)

    g = sub.add_parser("generate", help="generate the historical Olist-shaped dataset")
    g.add_argument("--orders", type=int, default=None)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--force", action="store_true")
    g.set_defaults(fn=cmd_generate)

    s = sub.add_parser("simulate", help="generate an incremental CDC batch")
    s.add_argument("--batch", type=int, required=True)
    s.add_argument("--orders", type=int, default=2000)
    s.set_defaults(fn=cmd_simulate)

    i = sub.add_parser("ingest", help="land files in MinIO and register a batch")
    i.add_argument("--path", default=None, help="file or directory (default: every new data/generated/batch_*)")
    i.add_argument("--label", default=None)
    i.set_defaults(fn=cmd_ingest)

    sub.add_parser("bronze", help="load registered raw files into Bronze").set_defaults(fn=cmd_bronze)
    sv = sub.add_parser("silver", help="Bronze -> Silver (incremental)")
    sv.add_argument("--entity", action="append")
    sv.set_defaults(fn=cmd_silver)
    sub.add_parser("gold", help="Silver -> Gold (incremental)").set_defaults(fn=cmd_gold)
    sub.add_parser("run", help="ingest new batches, then bronze + silver + gold").set_defaults(fn=cmd_run)

    q = sub.add_parser("quality", help="show the latest quality reports")
    q.add_argument("--limit", type=int, default=20)
    q.set_defaults(fn=cmd_quality)

    o = sub.add_parser("optimize", help="compaction, manifest rewrite, snapshot expiry, orphan cleanup")
    o.add_argument("--aggressive", action="store_true", help="ignore retention windows (demo only)")
    o.set_defaults(fn=cmd_optimize)

    sub.add_parser("showcase", help="run the Iceberg feature demonstrations").set_defaults(fn=cmd_showcase)
    b = sub.add_parser("benchmark", help="CSV vs Parquet vs Iceberg query benchmarks")
    b.add_argument("--repeats", type=int, default=5)
    b.set_defaults(fn=cmd_benchmark)
    sub.add_parser("status", help="tables, snapshots, files and rows").set_defaults(fn=cmd_status)

    qq = sub.add_parser("query", help="run SQL with DuckDB against the lakehouse")
    qq.add_argument("sql", help="SQL text or path to a .sql file")
    qq.add_argument("--limit", type=int, default=50)
    qq.set_defaults(fn=cmd_query)

    bs = sub.add_parser("bootstrap", help="end-to-end demo run (idempotent)")
    bs.add_argument("--force", action="store_true")
    bs.set_defaults(fn=cmd_bootstrap)
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        stream=sys.stdout)
    for noisy in ("py4j", "botocore", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    args = build_parser().parse_args(argv)
    return int(args.fn(args) or 0)

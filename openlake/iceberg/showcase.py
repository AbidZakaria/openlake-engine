"""Iceberg feature lab.

Each demo runs against copies in the ``lab`` namespace (never against the
production Silver/Gold tables, except read-only time travel), records every
step with the SQL it ran and the numbers it observed, and returns a structured
report that the dashboard renders as the "Iceberg Lab" page.

Demos: atomic commits & failure isolation, time travel (Spark *and* DuckDB),
schema evolution, partition evolution, rollback with tags, write-audit-publish
with branches, compaction, snapshot expiration and orphan-file cleanup.
"""

from __future__ import annotations

import time
import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from openlake.config.pipeline import PipelineConfig
from openlake.iceberg import maintenance as mnt
from openlake.iceberg.catalog_manager import CatalogManager
from openlake.iceberg.rollback import create_tag, rollback_to_snapshot
from openlake.iceberg.snapshot_manager import current_snapshot, list_snapshots
from openlake.storage import ObjectStore


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class Demo:
    def __init__(self, spark: SparkSession, demo_id: str, title: str, summary: str, why: str):
        self.spark = spark
        self.data = {"id": demo_id, "title": title, "summary": summary, "why": why, "steps": [],
                     "metrics": {}, "status": "ok"}

    def sql(self, label: str, query: str, show: bool = False, limit: int = 20):
        t0 = time.perf_counter()
        df = self.spark.sql(query)
        rows = [r.asDict() for r in df.limit(limit).collect()] if show else None
        if not show:
            df.collect()
        self.data["steps"].append({"label": label, "sql": " ".join(query.split()),
                                   "result": rows, "ms": round((time.perf_counter() - t0) * 1000)})
        return rows

    def note(self, label: str, result=None, code: str | None = None):
        self.data["steps"].append({"label": label, "sql": code, "result": result, "ms": None})

    def metric(self, **kw):
        self.data["metrics"].update(kw)


def _scalar(spark: SparkSession, query: str):
    return spark.sql(query).collect()[0][0]


def _files(spark: SparkSession, table: str) -> int:
    return int(_scalar(spark, f"SELECT count(*) FROM {table}.files"))


_STORE: ObjectStore | None = None


def _fresh(spark: SparkSession, table: str) -> None:
    """Drop a lab table and wipe its storage prefix so every run starts from a clean slate.

    DROP ... PURGE deletes only files the table *references*; leftovers of earlier
    failed demo runs would otherwise show up as orphans in the next run.
    """
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    if _STORE is not None:
        _, ns, name = table.split(".")
        for obj in list(_STORE.list(f"warehouse/{ns}/{name}/")):
            _STORE.delete(obj.key)


# --------------------------------------------------------------------------- demos
def demo_acid(spark, config, cm: CatalogManager, store: ObjectStore) -> dict:
    t = config.table("lab", "acid_orders")
    d = Demo(spark, "acid", "Atomic commits & failure isolation",
             "A Spark job dies half-way through an append. Readers never see a partial write.",
             "In Iceberg a write becomes visible only when the catalog atomically swaps the table's metadata "
             "pointer. Data files that tasks wrote before the job failed are not referenced by any snapshot, so "
             "readers never see them — they are 'orphans' in storage, exactly like files left by a killed driver, "
             "and orphan-file cleanup removes them.")
    _fresh(spark, t)
    spark.sql(f"CREATE TABLE {t} USING iceberg TBLPROPERTIES ('format-version'='2') AS "
              f"SELECT order_id, customer_id, order_status, order_purchase_timestamp FROM {config.table('silver', 'orders')}")
    s0, c0 = current_snapshot(spark, t), int(_scalar(spark, f"SELECT count(*) FROM {t}"))
    location = _scalar(spark, f"SELECT file_path FROM {t}.files LIMIT 1").rsplit("/data/", 1)[0]
    data_prefix = location.split(f"s3://{store.bucket}/", 1)[1] + "/data/"
    objects_before = sum(1 for _ in store.list(data_prefix))
    d.note("Committed state before the failing job", {"snapshot_id": s0, "rows": c0, "data_objects_in_storage": objects_before})

    failing = (spark.table(t).repartition(8)
               .withColumn("order_status", F.when(F.spark_partition_id() == 5,
                                                  F.raise_error(F.lit("simulated executor crash")))
                           .otherwise(F.col("order_status"))))
    error = None
    try:
        failing.writeTo(t).append()
    except Exception as exc:  # expected
        error = str(exc).split("\n")[0][:300]
    s1, c1 = current_snapshot(spark, t), int(_scalar(spark, f"SELECT count(*) FROM {t}"))
    objects_after_abort = sum(1 for _ in store.list(data_prefix))
    d.note("Append of 8 partitions; partition 5 throws", {"error": error}, code="df.repartition(8)...writeTo(t).append()")
    d.note("State after the failed job",
           {"snapshot_id": s1, "rows": c1, "snapshot_unchanged": s0 == s1, "rows_unchanged": c0 == c1,
            "data_objects_in_storage": objects_after_abort})

    # Hard crash: a file reaches storage but the commit never happens.
    orphan_key = f"{data_prefix}crashed-writer-{uuid.uuid4().hex[:8]}.parquet"
    store.put_bytes(b"PAR1-not-a-real-file-left-by-a-killed-driver", orphan_key)
    c2 = int(_scalar(spark, f"SELECT count(*) FROM {t}"))
    d.note("Simulated hard crash leaves an uncommitted file in storage",
           {"orphan_file": f"s3://{store.bucket}/{orphan_key}", "rows_visible_to_readers": c2})
    time.sleep(1.5)
    orphans = mnt.unreferenced_data_files(spark, t, store)
    d.note("Storage listing vs. files referenced by any snapshot", {"unreferenced_files": orphans},
           code="files under data/  MINUS  SELECT file_path FROM lab.acid_orders.all_files")
    removed = mnt.remove_orphans(spark, t, config.catalog, _ts(), dry_run=False)
    d.note("DeleteOrphanFiles action removes it", {"removed": removed, "still_in_storage": store.exists(orphan_key),
                                                   "rows_after_cleanup": int(_scalar(spark, f"SELECT count(*) FROM {t}"))},
           code="SparkActions.get(spark).deleteOrphanFiles(table).olderThan(now).execute()")
    d.metric(snapshot_unchanged=s0 == s1, rows_unchanged=c0 == c1, partial_files_left_after_abort=objects_after_abort - objects_before,
             orphans_found=len(orphans), orphans_removed=len(removed))
    return d.data


def demo_time_travel(spark, config, cm, store) -> dict:
    t = config.table("silver", "orders")
    d = Demo(spark, "time_travel", "Time travel",
             "Query silver.orders exactly as it was after each incremental batch — from Spark and from DuckDB.",
             "Every commit is an immutable snapshot. VERSION AS OF / TIMESTAMP AS OF read an old snapshot's "
             "manifests, so historical queries are exact and need no copies or audit tables.")
    snaps = list_snapshots(spark, t)
    d.note("Snapshots of silver.orders", [
        {k: s[k] for k in ("snapshot_id", "committed_at", "operation", "added_records", "deleted_records", "total_records")}
        for s in snaps])
    first, last = snaps[0]["snapshot_id"], snaps[-1]["snapshot_id"]
    by_status = d.sql("Status mix: first snapshot vs current", f"""
        SELECT coalesce(a.order_status, b.order_status) AS order_status, a.n AS first_snapshot, b.n AS current
        FROM (SELECT order_status, count(*) n FROM {t} VERSION AS OF {first} GROUP BY 1) a
        FULL OUTER JOIN (SELECT order_status, count(*) n FROM {t} GROUP BY 1) b ON a.order_status = b.order_status
        ORDER BY current DESC NULLS LAST""", show=True)
    sample = spark.sql(f"""
        SELECT a.order_id FROM {t} VERSION AS OF {first} a JOIN {t} b ON a.order_id = b.order_id
        WHERE a.order_status = 'shipped' AND b.order_status = 'delivered' LIMIT 1""").collect()
    if sample:
        oid = sample[0]["order_id"]
        history = []
        for s in snaps:
            r = spark.sql(f"SELECT order_status, CAST(order_delivered_customer_date AS STRING) AS delivered_at "
                          f"FROM {t} VERSION AS OF {s['snapshot_id']} WHERE order_id = '{oid}'").collect()
            history.append({"snapshot_id": s["snapshot_id"], "committed_at": s["committed_at"],
                            "order_status": r[0]["order_status"] if r else None,
                            "delivered_at": r[0]["delivered_at"] if r else None})
        d.note(f"One order's life across snapshots ({oid[:12]}…)", history,
               code=f"SELECT order_status FROM silver.orders VERSION AS OF <id> WHERE order_id = '{oid}'")
    ts_mid = snaps[1]["committed_at"] if len(snaps) > 1 else snaps[0]["committed_at"]
    d.sql("TIMESTAMP AS OF works the same way", f"SELECT count(*) AS orders FROM {t} TIMESTAMP AS OF '{ts_mid}'", show=True)

    duck = {}
    try:
        from openlake.analytics.duckdb_client import LakeQuery

        with LakeQuery() as lq:
            for s in (first, last):
                duck[str(s)] = lq.scalar(f"SELECT count(*) FROM lake.silver.orders AT (VERSION => {s})")
        spark_counts = {str(s): int(_scalar(spark, f"SELECT count(*) FROM {t} VERSION AS OF {s}")) for s in (first, last)}
        d.note("Same snapshots read by DuckDB (no Spark)", {"duckdb": duck, "spark": spark_counts,
                                                             "engines_agree": duck == spark_counts},
               code="SELECT count(*) FROM lake.silver.orders AT (VERSION => <snapshot_id>)  -- DuckDB")
        d.metric(engines_agree=duck == spark_counts)
    except Exception as exc:
        d.note("DuckDB time travel", {"error": str(exc)[:300]})
    d.metric(snapshots=len(snaps), status_rows=len(by_status or []))
    return d.data


def demo_schema_evolution(spark, config, cm, store) -> dict:
    t = config.table("lab", "customers_evolution")
    d = Demo(spark, "schema_evolution", "Schema evolution",
             "Add, rename, widen and drop columns without rewriting a single data file.",
             "Iceberg identifies columns by ID, not by name or position. Schema changes are metadata commits; "
             "old files are read through the new schema (new columns read as NULL), and old snapshots keep "
             "their own schema for time travel.")
    _fresh(spark, t)
    spark.sql(f"""CREATE TABLE {t} USING iceberg TBLPROPERTIES ('format-version'='2') AS
        SELECT c.customer_id, c.customer_city, c.customer_state, CAST(count(o.order_id) AS INT) AS orders_count,
               CAST(round(sum(coalesce(p.payment_value, 0)), 2) AS FLOAT) AS lifetime_value
        FROM {config.table('silver', 'customers')} c
        LEFT JOIN {config.table('silver', 'orders')} o ON o.customer_id = c.customer_id
        LEFT JOIN {config.table('silver', 'order_payments')} p ON p.order_id = o.order_id
        GROUP BY 1, 2, 3""")
    s0 = current_snapshot(spark, t)
    files0 = sorted(r["file_path"] for r in spark.sql(f"SELECT file_path FROM {t}.files").collect())

    def schema():
        return [f"{f.name}: {f.dataType.simpleString()}" for f in spark.table(t).schema.fields]

    d.note("Initial schema", schema())
    d.sql("Add a column", f"ALTER TABLE {t} ADD COLUMN loyalty_tier STRING COMMENT 'gold/silver/bronze'")
    d.sql("Rename a column", f"ALTER TABLE {t} RENAME COLUMN customer_city TO city")
    d.sql("Widen INT -> BIGINT", f"ALTER TABLE {t} ALTER COLUMN orders_count TYPE BIGINT")
    d.sql("Widen FLOAT -> DOUBLE", f"ALTER TABLE {t} ALTER COLUMN lifetime_value TYPE DOUBLE")
    d.sql("Move a column", f"ALTER TABLE {t} ALTER COLUMN loyalty_tier AFTER customer_id")
    d.sql("Drop a column", f"ALTER TABLE {t} DROP COLUMN customer_state")
    files1 = sorted(r["file_path"] for r in spark.sql(f"SELECT file_path FROM {t}.files").collect())
    d.note("Evolved schema", schema())
    d.note("Data files rewritten by the six schema changes", {"files_before": len(files0), "files_after": len(files1),
                                                             "identical_files": files0 == files1})
    d.sql("Backfill the new column (a normal row-level write)",
          f"UPDATE {t} SET loyalty_tier = CASE WHEN lifetime_value >= 500 THEN 'gold' "
          f"WHEN lifetime_value >= 150 THEN 'silver' ELSE 'bronze' END")
    d.sql("Current snapshot through the new schema",
          f"SELECT customer_id, loyalty_tier, city, orders_count, lifetime_value FROM {t} ORDER BY lifetime_value DESC LIMIT 3",
          show=True)
    old = spark.sql(f"SELECT * FROM {t} VERSION AS OF {s0} LIMIT 3")
    d.note("Time travel to the first snapshot returns the original schema",
           {"columns": [f"{f.name}: {f.dataType.simpleString()}" for f in old.schema.fields],
            "rows": [r.asDict() for r in old.collect()]},
           code=f"SELECT * FROM lab.customers_evolution VERSION AS OF {s0}")
    d.metric(files_rewritten_by_ddl=0 if files0 == files1 else len(set(files1) - set(files0)), schema_changes=6)
    return d.data


def demo_partition_evolution(spark, config, cm, store) -> dict:
    t = config.table("lab", "orders_partitioning")
    d = Demo(spark, "partition_evolution", "Partition evolution & hidden partitioning",
             "Start with monthly partitions, switch new data to daily partitions — no rewrite, one table.",
             "Partitions are derived from column values by transforms (months(ts), days(ts), bucket(n, id)). "
             "Queries filter on the column itself; Iceberg maps predicates to partitions for every spec the "
             "table has ever had, so old and new layouts coexist.")
    _fresh(spark, t)
    src = config.table("silver", "orders")
    spark.sql(f"""CREATE TABLE {t} (order_id STRING, customer_id STRING, order_status STRING,
                  order_purchase_timestamp TIMESTAMP) USING iceberg
                  PARTITIONED BY (months(order_purchase_timestamp)) TBLPROPERTIES ('format-version'='2')""")
    d.sql("Load history with spec 0: months(order_purchase_timestamp)",
          f"INSERT INTO {t} SELECT order_id, customer_id, order_status, order_purchase_timestamp FROM {src} "
          f"WHERE order_purchase_timestamp < '2018-09-01'")
    d.sql("Evolve the spec: monthly -> daily",
          f"ALTER TABLE {t} REPLACE PARTITION FIELD order_purchase_timestamp_month WITH days(order_purchase_timestamp)")
    d.sql("New data lands with spec 1: days(order_purchase_timestamp)",
          f"INSERT INTO {t} SELECT order_id, customer_id, order_status, order_purchase_timestamp FROM {src} "
          f"WHERE order_purchase_timestamp >= '2018-09-01'")
    by_spec = d.sql("Files per partition spec", f"""
        SELECT spec_id, count(*) AS data_files, count(DISTINCT partition) AS partitions, sum(record_count) AS records
        FROM {t}.files GROUP BY spec_id ORDER BY spec_id""", show=True)
    d.sql("Partitions table (sample)", f"SELECT partition, spec_id, record_count, file_count FROM {t}.partitions "
          f"ORDER BY spec_id DESC, record_count DESC", show=True, limit=6)
    d.sql("One query spans both layouts — filter on the column, not on a partition column", f"""
        SELECT date_format(order_purchase_timestamp, 'yyyy-MM') AS month, count(*) AS orders
        FROM {t} WHERE order_purchase_timestamp >= '2018-08-15' GROUP BY 1 ORDER BY 1""", show=True)
    d.metric(specs=len(by_spec or []))
    return d.data


def demo_rollback(spark, config, cm, store) -> dict:
    t = config.table("lab", "fact_orders_rollback")
    d = Demo(spark, "rollback", "Rollback after a bad deploy",
             "A buggy job multiplies order values by 100. Tag, inspect, roll back — in seconds.",
             "rollback_to_snapshot only moves the current-snapshot pointer, so recovery is instant and needs no "
             "backups. The bad snapshot stays in history for investigation until it is expired.")
    _fresh(spark, t)
    spark.sql(f"CREATE TABLE {t} USING iceberg PARTITIONED BY (months(order_date)) TBLPROPERTIES ('format-version'='2') "
              f"AS SELECT * FROM {config.table('gold', 'fact_orders')}")
    good = current_snapshot(spark, t)
    create_tag(spark, t, "before-bad-deploy", good, retain_days=30)
    revenue_sql = f"SELECT round(sum(order_value), 2) AS revenue, round(avg(order_value), 2) AS avg_order_value FROM {t}"
    before = d.sql("Known-good state (tagged 'before-bad-deploy')", revenue_sql, show=True)
    d.sql("Bad deploy: a unit bug multiplies 2018 order values by 100",
          f"UPDATE {t} SET order_value = order_value * 100 WHERE order_date >= DATE '2018-01-01'")
    bad = d.sql("Broken state", revenue_sql, show=True)
    d.sql("Snapshots", f"SELECT snapshot_id, operation, CAST(committed_at AS STRING) AS committed_at FROM {t}.snapshots "
          f"ORDER BY committed_at", show=True)
    t0 = time.perf_counter()
    rb = rollback_to_snapshot(spark, t, good, config.catalog)
    rb_ms = round((time.perf_counter() - t0) * 1000)
    d.note("Roll back to the tagged snapshot", {**rb, "ms": rb_ms},
           code=f"CALL lake.system.rollback_to_snapshot(table => 'lab.fact_orders_rollback', snapshot_id => {good})")
    after = d.sql("Restored state", revenue_sql, show=True)
    d.sql("Tag still points at the good snapshot", f"SELECT name, type, snapshot_id FROM {t}.refs", show=True)
    d.metric(restored=before == after, revenue_good=before[0]["revenue"], revenue_bad=bad[0]["revenue"],
             rollback_ms=rb_ms)
    return d.data


def demo_wap(spark, config, cm, store) -> dict:
    t = config.table("lab", "daily_revenue_wap")
    d = Demo(spark, "wap", "Write-Audit-Publish with branches",
             "Stage a new day of data on an 'audit' branch, validate it, then publish atomically.",
             "Branches are named snapshot lineages. Consumers read 'main' while a job writes to a branch; once "
             "quality checks pass, fast_forward publishes the branch to main in one metadata commit.")
    _fresh(spark, t)
    spark.sql(f"CREATE TABLE {t} USING iceberg TBLPROPERTIES ('format-version'='2') AS "
              f"SELECT order_date, orders, revenue FROM {config.table('gold', 'daily_revenue')}")
    main_before = int(_scalar(spark, f"SELECT count(*) FROM {t}"))
    d.sql("Create the audit branch", f"ALTER TABLE {t} CREATE BRANCH audit")
    d.sql("Write tomorrow's data to the branch (with one bad row)", f"""
        INSERT INTO {t}.branch_audit VALUES
        (DATE '2018-10-01', 151, 21734.55), (DATE '2018-10-02', 149, -999.0)""")
    main_mid = int(_scalar(spark, f"SELECT count(*) FROM {t}"))
    audit_rows = int(_scalar(spark, f"SELECT count(*) FROM {t} VERSION AS OF 'audit'"))
    d.note("Readers of main are unaffected", {"main_rows": main_mid, "audit_rows": audit_rows,
                                              "main_unchanged": main_mid == main_before})
    failures = d.sql("Audit: revenue must be non-negative",
                     f"SELECT order_date, revenue FROM {t} VERSION AS OF 'audit' WHERE revenue < 0", show=True)
    d.sql("Fix on the branch", f"DELETE FROM {t}.branch_audit WHERE revenue < 0")
    failures_after = d.sql("Audit again", f"SELECT count(*) AS failing_rows FROM {t} VERSION AS OF 'audit' "
                                          f"WHERE revenue < 0", show=True)
    d.sql("Publish: fast-forward main to audit",
          f"CALL {config.catalog}.system.fast_forward(table => 'lab.daily_revenue_wap', branch => 'main', to => 'audit')")
    main_after = int(_scalar(spark, f"SELECT count(*) FROM {t}"))
    d.note("Main after publish", {"main_rows": main_after, "new_rows_published": main_after - main_before})
    d.metric(bad_rows_caught=len(failures or []), published_rows=main_after - main_before,
             audit_clean=failures_after[0]["failing_rows"] == 0)
    return d.data


def demo_compaction_and_expiry(spark, config, cm, store) -> dict:
    t = config.table("lab", "payments_small_files")
    d = Demo(spark, "compaction", "Compaction, manifest rewrite & snapshot expiration",
             "40 micro-batches create 40+ small files and 40 snapshots. Compact, then expire history.",
             "Each commit adds files and a snapshot. Many small files slow planning and scanning; "
             "rewrite_data_files bin-packs them, and expire_snapshots bounds metadata growth (and how far "
             "back time travel can go).")
    _fresh(spark, t)
    src = spark.table(config.table("gold", "fact_payments"))
    spark.sql(f"CREATE TABLE {t} (payment_id STRING, order_id STRING, payment_type STRING, payment_installments INT, "
              f"payment_value DOUBLE, order_date DATE) USING iceberg TBLPROPERTIES ('format-version'='2')")
    sliced = src.select("payment_id", "order_id", "payment_type", "payment_installments", "payment_value", "order_date") \
        .withColumn("__slice", F.abs(F.xxhash64("payment_id")) % 40).cache()
    for i in range(40):
        sliced.filter(F.col("__slice") == i).drop("__slice").coalesce(2).writeTo(t).append()
    sliced.unpersist()
    query = (f"SELECT payment_type, count(*) AS n, round(sum(payment_value), 2) AS value FROM {t} "
             f"GROUP BY payment_type ORDER BY value DESC")

    def timed(n=5):
        runs = []
        for _ in range(n):
            t0 = time.perf_counter()
            spark.sql(query).collect()
            runs.append((time.perf_counter() - t0) * 1000)
        return round(sorted(runs)[len(runs) // 2], 1)

    files_before, snaps_before = _files(spark, t), int(_scalar(spark, f"SELECT count(*) FROM {t}.snapshots"))
    manifests_before = int(_scalar(spark, f"SELECT count(*) FROM {t}.manifests"))
    ms_before = timed()
    d.note("After 40 micro-batch appends", {"data_files": files_before, "snapshots": snaps_before,
                                            "manifests": manifests_before, "query_median_ms": ms_before})
    res = mnt.compact(spark, t, config.catalog, 2, 128)
    d.note("rewrite_data_files (bin-pack)", res,
           code="CALL lake.system.rewrite_data_files(table => 'lab.payments_small_files', options => map('min-input-files','2'))")
    man = mnt.rewrite_manifests(spark, t, config.catalog)
    d.note("rewrite_manifests", man, code="CALL lake.system.rewrite_manifests(table => 'lab.payments_small_files')")
    files_after = _files(spark, t)
    manifests_after = int(_scalar(spark, f"SELECT count(*) FROM {t}.manifests"))
    ms_after = timed()
    d.note("After compaction", {"data_files": files_after, "manifests": manifests_after, "query_median_ms": ms_after})
    snaps_pre_expire = int(_scalar(spark, f"SELECT count(*) FROM {t}.snapshots"))
    time.sleep(1.2)
    exp = mnt.expire_snapshots(spark, t, config.catalog, _ts(), retain_last=3)
    snaps_after = int(_scalar(spark, f"SELECT count(*) FROM {t}.snapshots"))
    d.note("expire_snapshots(retain_last => 3)", {**exp, "snapshots_before": snaps_pre_expire, "snapshots_after": snaps_after},
           code="CALL lake.system.expire_snapshots(table => 'lab.payments_small_files', older_than => now(), retain_last => 3)")
    d.metric(files_before=files_before, files_after=files_after, manifests_before=manifests_before,
             manifests_after=manifests_after, query_ms_before=ms_before, query_ms_after=ms_after,
             snapshots_before=snaps_pre_expire, snapshots_after=snaps_after)
    return d.data


DEMOS = [demo_acid, demo_time_travel, demo_schema_evolution, demo_partition_evolution, demo_rollback, demo_wap,
         demo_compaction_and_expiry]


def run_showcase(spark: SparkSession, config: PipelineConfig, store: ObjectStore) -> dict:
    global _STORE
    _STORE = store
    cm = CatalogManager(spark, config)
    cm.ensure_namespaces()
    report = {"generated_at": _ts(), "demos": []}
    for demo in DEMOS:
        t0 = time.perf_counter()
        try:
            data = demo(spark, config, cm, store)
        except Exception as exc:
            data = {"id": demo.__name__.replace("demo_", ""), "title": demo.__name__, "status": "error",
                    "error": f"{type(exc).__name__}: {exc}"[:1500], "trace": traceback.format_exc()[-3000:],
                    "steps": [], "metrics": {}}
        data["seconds"] = round(time.perf_counter() - t0, 1)
        report["demos"].append(data)
        print(f"[showcase] {data['title']}: {data['status']} ({data['seconds']}s)", flush=True)
    return report

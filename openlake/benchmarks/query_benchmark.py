"""Query benchmarks: raw CSV vs raw Parquet vs Silver Iceberg vs Gold Iceberg.

Methodology
-----------
* Same hardware, same object store, same query *logic* per layer; the SQL is
  adapted only to each layer's schema (raw files have Olist names and
  untyped columns, Gold is pre-modelled).
* Raw CSV and raw Parquet contain the historical batch only; Silver/Gold also
  contain the two incremental batches (~4% more orders). Row counts are
  reported next to every timing so the difference is visible.
* Each query runs once to warm up, then ``repeats`` times; the median is kept.
* DuckDB is the primary engine (single process, no cluster); Spark timings on
  the Iceberg layers are reported alongside for reference.
* "Eligible files/bytes" for Iceberg come from table metadata *after* partition
  pruning for the query's predicate — this is the data the engine has to open.

Nothing here is tuned to flatter a layer; the point is to show the trade-offs.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from openlake.config.pipeline import PipelineConfig
from openlake.config.settings import get_settings
from openlake.storage import ObjectStore

BENCH_PARQUET_PREFIX = "bench/parquet"
RAW_TABLES = ("orders", "order_items", "products", "product_categories")
SAMPLE_DAY = "2018-03-15"


def _median_ms(fn, repeats: int) -> tuple[float, list[float], object]:
    result = fn()  # warm-up (also captures the result)
    runs = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        runs.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(runs), 1), [round(r, 1) for r in runs], result


def _initial_raw_keys(store: ObjectStore, entity: str) -> list:
    objs = [o for o in store.list(f"raw/ecommerce/{entity}/") if "batch_id=" in o.key]
    if not objs:
        return []
    first_batch = sorted({o.key.split("batch_id=")[1].split("/")[0] for o in objs})[0]
    return [o for o in objs if f"batch_id={first_batch}/" in o.key]


def prepare_raw_parquet(spark: SparkSession, store: ObjectStore) -> dict:
    """Convert the historical raw CSVs to typed Parquet once (the 'raw Parquet' layer)."""
    settings = get_settings()
    out = {}
    casts = {
        "orders": {"order_purchase_timestamp": "timestamp", "order_delivered_customer_date": "timestamp",
                   "order_estimated_delivery_date": "timestamp"},
        "order_items": {"order_item_id": "int", "price": "double", "freight_value": "double"},
        "products": {"product_weight_g": "int"},
        "product_categories": {},
    }
    for entity in RAW_TABLES:
        keys = _initial_raw_keys(store, entity)
        if not keys:
            continue
        target = f"{BENCH_PARQUET_PREFIX}/{entity}/"
        if not any(True for _ in store.list(target)):
            df = spark.read.option("header", True).csv([settings.minio.s3a_uri(k.key) for k in keys])
            for col, typ in casts[entity].items():
                df = df.withColumn(col, F.expr(f"try_cast(`{col}` AS {typ})"))
            df.coalesce(1).write.mode("overwrite").option("compression", "zstd").parquet(settings.minio.s3a_uri(target))
        out[entity] = [o for o in store.list(target) if o.key.endswith(".parquet")]
    return out


def _queries(store: ObjectStore, raw_keys: dict, parquet: dict) -> dict:
    b = store.bucket

    def csv(entity):
        files = ", ".join(f"'s3://{b}/{o.key}'" for o in raw_keys[entity])
        return f"read_csv([{files}], header = true, auto_detect = true)"

    def pq(entity):
        files = ", ".join(f"'s3://{b}/{o.key}'" for o in parquet[entity])
        return f"read_parquet([{files}])"

    monthly = {
        "raw_csv": f"""SELECT strftime(TRY_CAST(o.order_purchase_timestamp AS TIMESTAMP), '%Y-%m') m, round(sum(i.price), 2) revenue
                       FROM {csv('orders')} o JOIN {csv('order_items')} i USING (order_id)
                       WHERE lower(trim(o.order_status)) NOT IN ('canceled','unavailable') GROUP BY 1 ORDER BY 1""",
        "raw_parquet": f"""SELECT strftime(o.order_purchase_timestamp, '%Y-%m') m, round(sum(i.price), 2) revenue
                       FROM {pq('orders')} o JOIN {pq('order_items')} i USING (order_id)
                       WHERE lower(trim(o.order_status)) NOT IN ('canceled','unavailable') GROUP BY 1 ORDER BY 1""",
        "silver_iceberg": """SELECT strftime(o.order_purchase_timestamp, '%Y-%m') m, round(sum(i.price), 2) revenue
                       FROM lake.silver.orders o JOIN lake.silver.order_items i USING (order_id)
                       WHERE o.order_status NOT IN ('canceled','unavailable') GROUP BY 1 ORDER BY 1""",
        "gold_iceberg": """SELECT strftime(order_date, '%Y-%m') m, round(sum(revenue), 2) revenue
                       FROM lake.gold.daily_revenue GROUP BY 1 ORDER BY 1""",
    }
    categories = {
        "raw_csv": f"""SELECT coalesce(c.product_category_name_english, p.product_category_name) cat, round(sum(i.price), 2) revenue
                       FROM {csv('order_items')} i JOIN {csv('products')} p USING (product_id)
                       LEFT JOIN {csv('product_categories')} c USING (product_category_name)
                       GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
        "raw_parquet": f"""SELECT coalesce(c.product_category_name_english, p.product_category_name) cat, round(sum(i.price), 2) revenue
                       FROM {pq('order_items')} i JOIN {pq('products')} p USING (product_id)
                       LEFT JOIN {pq('product_categories')} c USING (product_category_name)
                       GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
        "silver_iceberg": """SELECT coalesce(c.product_category_name_english, p.product_category_name) cat, round(sum(i.price), 2) revenue
                       FROM lake.silver.order_items i JOIN lake.silver.products p USING (product_id)
                       LEFT JOIN lake.silver.product_categories c USING (product_category_name)
                       GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
        "gold_iceberg": """SELECT p.category cat, round(sum(i.price), 2) revenue
                       FROM lake.gold.fact_order_items i JOIN lake.gold.dim_products p USING (product_key)
                       GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
    }
    one_day = {
        "raw_csv": f"""SELECT count(*) orders FROM {csv('orders')}
                       WHERE CAST(TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS DATE) = DATE '{SAMPLE_DAY}'""",
        "raw_parquet": f"""SELECT count(*) orders FROM {pq('orders')}
                       WHERE CAST(order_purchase_timestamp AS DATE) = DATE '{SAMPLE_DAY}'""",
        "silver_iceberg": f"""SELECT count(*) orders FROM lake.silver.orders
                       WHERE order_purchase_timestamp >= TIMESTAMP '{SAMPLE_DAY}'
                         AND order_purchase_timestamp <  TIMESTAMP '{SAMPLE_DAY}' + INTERVAL 1 DAY""",
        "gold_iceberg": f"""SELECT orders FROM lake.gold.daily_revenue WHERE order_date = DATE '{SAMPLE_DAY}'""",
    }
    return {
        "monthly_revenue": {"title": "Monthly revenue (join orders x items, aggregate)", "sql": monthly},
        "top_categories": {"title": "Top 10 categories by revenue (3-way join)", "sql": categories},
        "single_day": {"title": f"Orders on one day ({SAMPLE_DAY}) — selective filter", "sql": one_day},
    }


def _iceberg_footprint(spark: SparkSession, tables: list[str], predicate: dict | None = None) -> dict:
    files = bytes_ = rows = 0
    for t in tables:
        q = spark.table(f"{t}.files")
        if predicate and t in predicate:
            q = q.filter(predicate[t])
        r = q.agg(F.count(F.lit(1)).alias("n"), F.sum("file_size_in_bytes").alias("b"),
                  F.sum("record_count").alias("r")).collect()[0]
        files, bytes_, rows = files + int(r["n"] or 0), bytes_ + int(r["b"] or 0), rows + int(r["r"] or 0)
    return {"files": files, "bytes": bytes_, "rows": rows}


def run_benchmarks(spark: SparkSession, config: PipelineConfig, store: ObjectStore, repeats: int = 5) -> dict:
    from openlake.analytics.duckdb_client import LakeQuery

    settings = get_settings()
    raw_keys = {e: _initial_raw_keys(store, e) for e in RAW_TABLES}
    parquet = prepare_raw_parquet(spark, store)
    queries = _queries(store, raw_keys, parquet)

    def raw_fp(objs_by_entity, entities):
        objs = [o for e in entities for o in objs_by_entity[e]]
        return {"files": len(objs), "bytes": sum(o.size for o in objs)}

    month_idx = (int(SAMPLE_DAY[:4]) - 1970) * 12 + int(SAMPLE_DAY[5:7]) - 1
    footprints = {
        "monthly_revenue": {
            "raw_csv": raw_fp(raw_keys, ["orders", "order_items"]),
            "raw_parquet": raw_fp(parquet, ["orders", "order_items"]),
            "silver_iceberg": _iceberg_footprint(spark, ["lake.silver.orders", "lake.silver.order_items"]),
            "gold_iceberg": _iceberg_footprint(spark, ["lake.gold.daily_revenue"]),
        },
        "top_categories": {
            "raw_csv": raw_fp(raw_keys, ["order_items", "products", "product_categories"]),
            "raw_parquet": raw_fp(parquet, ["order_items", "products", "product_categories"]),
            "silver_iceberg": _iceberg_footprint(spark, ["lake.silver.order_items", "lake.silver.products",
                                                         "lake.silver.product_categories"]),
            "gold_iceberg": _iceberg_footprint(spark, ["lake.gold.fact_order_items", "lake.gold.dim_products"]),
        },
        "single_day": {
            "raw_csv": raw_fp(raw_keys, ["orders"]),
            "raw_parquet": raw_fp(parquet, ["orders"]),
            "silver_iceberg": _iceberg_footprint(spark, ["lake.silver.orders"], {
                "lake.silver.orders": F.col("partition.order_purchase_timestamp_month") == month_idx}),
            "gold_iceberg": _iceberg_footprint(spark, ["lake.gold.daily_revenue"], {
                "lake.gold.daily_revenue": F.col("partition.order_date_month") == month_idx}),
        },
    }

    results = []
    with LakeQuery() as lq:
        for qid, q in queries.items():
            for layer, sql in q["sql"].items():
                med, runs, rows = _median_ms(lambda s=sql: lq.sql(s).fetchall(), repeats)
                results.append({"query": qid, "title": q["title"], "layer": layer, "engine": "duckdb",
                                "median_ms": med, "runs_ms": runs, "result_rows": len(rows),
                                **{f"eligible_{k}": v for k, v in footprints[qid][layer].items()}})
                print(f"[bench] duckdb {qid:16s} {layer:15s} {med:9.1f} ms", flush=True)
            for layer in ("silver_iceberg", "gold_iceberg"):
                spark_sql = q["sql"][layer].replace("strftime(o.order_purchase_timestamp, '%Y-%m')",
                                                    "date_format(o.order_purchase_timestamp, 'yyyy-MM')") \
                    .replace("strftime(order_date, '%Y-%m')", "date_format(order_date, 'yyyy-MM')")
                med, runs, rows = _median_ms(lambda s=spark_sql: spark.sql(s).collect(), repeats)
                results.append({"query": qid, "title": q["title"], "layer": layer, "engine": "spark",
                                "median_ms": med, "runs_ms": runs, "result_rows": len(rows),
                                **{f"eligible_{k}": v for k, v in footprints[qid][layer].items()}})
                print(f"[bench] spark  {qid:16s} {layer:15s} {med:9.1f} ms", flush=True)

    storage = storage_comparison(spark, store, raw_keys, parquet)
    pipeline = pipeline_timings(spark, config)
    from openlake.benchmarks.ingestion_benchmark import ingestion_throughput

    ingestion = ingestion_throughput(store, settings.data_dir / "generated" / "batch_0001_initial")
    compaction = _compaction_from_showcase(settings.reports_dir)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": environment_info(spark),
        "methodology": __doc__.split("Methodology", 1)[1].split("Nothing here")[0].strip().lstrip("-").strip(),
        "repeats": repeats,
        "queries": results,
        "storage": storage,
        "pipeline": pipeline,
        "ingestion": ingestion,
        "compaction": compaction,
    }
    out_dir = Path(os.environ.get("OPENLAKE_BENCH_DIR", str(Path(settings.data_dir).parent / "benchmarks" / "results")))
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "latest.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    from openlake.benchmarks.generate_reports import write_markdown

    md = write_markdown(payload, Path(settings.data_dir).parent / "docs" / "benchmarks.md")
    return {"output": str(out), "markdown": str(md), "queries": len(results)}


def storage_comparison(spark: SparkSession, store: ObjectStore, raw_keys: dict, parquet: dict) -> list[dict]:
    rows = []
    for entity in ("orders", "order_items"):
        csv_bytes = sum(o.size for o in raw_keys[entity])
        pq_bytes = sum(o.size for o in parquet.get(entity, []))
        ice = _iceberg_footprint(spark, [f"lake.silver.{entity}"])
        rows.append({"dataset": entity, "raw_csv_bytes": csv_bytes, "raw_parquet_bytes": pq_bytes,
                     "silver_iceberg_bytes": ice["bytes"], "silver_iceberg_files": ice["files"],
                     "silver_rows": ice["rows"]})
    return rows


def pipeline_timings(spark: SparkSession, config: PipelineConfig) -> list[dict]:
    t = config.table("ops", "pipeline_runs")
    rows = spark.sql(f"""
        SELECT stage, run_id, min(started_at) AS started_at, round(sum(duration_s), 1) AS seconds,
               sum(input_records) AS input_records, sum(output_records) AS output_records,
               sum(quarantined_records) AS quarantined_records, concat_ws(',', collect_set(source_batch_ids)) AS batches
        FROM {t} WHERE stage IN ('bronze', 'silver', 'gold') AND status <> 'FAILED'
        GROUP BY stage, run_id ORDER BY started_at
    """).collect()
    return [{**r.asDict(), "started_at": str(r["started_at"]),
             "records_per_second": round((r["input_records"] or 0) / r["seconds"]) if r["seconds"] else None}
            for r in rows]


def _compaction_from_showcase(reports_dir: Path) -> dict | None:
    path = Path(reports_dir) / "iceberg_showcase.json"
    if not path.exists():
        return None
    demos = json.loads(path.read_text()).get("demos", [])
    comp = next((d for d in demos if d.get("id") == "compaction"), None)
    return comp.get("metrics") if comp else None


def environment_info(spark: SparkSession) -> dict:
    import duckdb

    return {
        "host_cpus": os.cpu_count(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "spark": spark.version,
        "spark_master": spark.sparkContext.master,
        "executor_memory": spark.conf.get("spark.executor.memory", None),
        "duckdb": duckdb.__version__,
        "iceberg": "1.10.0",
    }

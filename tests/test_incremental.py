"""Hermetic end-to-end pipeline test: local object store + Iceberg filesystem catalog.

Runs the real Bronze/Silver/Gold code over three generated batches and checks
the properties the platform promises: reconciliation, incremental reads,
upserts, schema evolution, contract rejection and duplicate-file detection.
"""

import pytest

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def lake(spark, small_dataset, config, tmp_path_factory):
    from openlake.pipeline.orchestrator import Orchestrator
    from openlake.storage import LocalObjectStore

    store = LocalObjectStore(tmp_path_factory.mktemp("objects"))
    orch = Orchestrator(spark=spark, config=config, store=store)
    orch.init()
    results = {}
    for batch in ("batch_0001_initial", "batch_0002_cdc", "batch_0003_cdc"):
        results[batch] = {"ingest": orch.ingest(small_dataset / batch), **orch.run()}
    return orch, results


def scalar(spark, sql):
    return spark.sql(sql).collect()[0][0]


def test_bronze_keeps_every_raw_record(lake, small_dataset):
    orch, results = lake
    spark = orch.spark
    import pandas as pd

    raw_orders = sum(len(pd.read_csv(small_dataset / b / "olist_orders_dataset.csv"))
                     for b in ("batch_0001_initial", "batch_0002_cdc", "batch_0003_cdc"))
    assert scalar(spark, "SELECT count(*) FROM lake.bronze.orders") == raw_orders


def test_silver_reconciles_with_bronze(lake):
    spark = lake[0].spark
    for entity in ("orders", "order_items", "order_payments", "order_reviews", "customers"):
        bronze = scalar(spark, f"SELECT count(*) FROM lake.bronze.{entity}")
        reports = spark.sql(f"SELECT sum(total_records) t, sum(invalid_records) i, sum(duplicate_records) d "
                            f"FROM lake.ops.quality_reports WHERE dataset = '{entity}'").collect()[0]
        assert reports["t"] == bronze, entity  # every bronze row was validated exactly once
        silver = scalar(spark, f"SELECT count(*) FROM lake.silver.{entity}")
        assert silver <= bronze - reports["i"] - reports["d"], entity  # updates collapse onto existing keys


def test_silver_keys_are_unique(lake):
    spark = lake[0].spark
    assert scalar(spark, "SELECT count(*) - count(DISTINCT order_id) FROM lake.silver.orders") == 0
    assert scalar(spark, "SELECT count(*) - count(DISTINCT order_id, order_item_id) FROM lake.silver.order_items") == 0


def test_second_run_reads_incrementally_and_upserts(lake):
    _, results = lake
    orders = next(r for r in results["batch_0002_cdc"]["silver"] if r["entity"] == "orders")
    assert orders["read_mode"] == "incremental"
    assert orders["updated"] > 0 and orders["inserted"] > 0
    assert results["batch_0002_cdc"]["gold"]["mode"] == "incremental"


def test_status_updates_are_visible_in_silver_and_history_in_time_travel(lake):
    spark = lake[0].spark
    first = scalar(spark, "SELECT snapshot_id FROM lake.silver.orders.snapshots ORDER BY committed_at LIMIT 1")
    shipped_then = scalar(spark, f"SELECT count(*) FROM lake.silver.orders VERSION AS OF {first} "
                                 f"WHERE order_status = 'shipped'")
    shipped_now = scalar(spark, "SELECT count(*) FROM lake.silver.orders WHERE order_status = 'shipped'")
    assert shipped_now < shipped_then


def test_additive_schema_change_evolves_silver_and_gold(lake):
    spark = lake[0].spark
    assert "delivery_priority" in spark.table("lake.silver.orders").columns
    assert scalar(spark, "SELECT count(*) FROM lake.silver.orders WHERE delivery_priority IS NULL") > 0
    assert scalar(spark, "SELECT count(*) FROM lake.gold.fact_orders WHERE delivery_priority IS NOT NULL") > 0
    accepted = spark.sql("SELECT * FROM lake.ops.schema_changes WHERE column_name = 'delivery_priority'").collect()
    assert accepted and accepted[0]["action"] == "ACCEPTED"


def test_breaking_change_is_quarantined_not_loaded(lake):
    spark = lake[0].spark
    assert "state" not in spark.table("lake.silver.sellers").columns
    rejected = scalar(spark, "SELECT count(*) FROM lake.quarantine.sellers WHERE array_contains(_failed_rules, 'schema_contract')")
    assert rejected == 25
    assert scalar(spark, "SELECT count(*) FROM lake.ops.schema_changes WHERE change_type = 'MISSING_REQUIRED' "
                         "AND action = 'REJECTED'") == 1


def test_duplicate_file_is_skipped(lake):
    _, results = lake
    files = results["batch_0003_cdc"]["ingest"]["files"]
    dup = next(f for f in files if f["filename"] == "product_category_name_translation.csv")
    assert dup["status"] == "DUPLICATE"


def test_gold_matches_silver(lake):
    spark = lake[0].spark
    assert scalar(spark, "SELECT count(*) FROM lake.gold.fact_orders") == scalar(spark, "SELECT count(*) FROM lake.silver.orders")
    assert scalar(spark, "SELECT count(*) FROM lake.gold.fact_order_items") == scalar(
        spark, "SELECT count(*) FROM lake.silver.order_items i JOIN lake.silver.orders o USING (order_id)")
    total_fact = scalar(spark, "SELECT round(sum(order_value), 2) FROM lake.gold.fact_orders WHERE NOT is_canceled")
    total_mart = scalar(spark, "SELECT round(sum(revenue), 2) FROM lake.gold.daily_revenue")
    assert abs(total_fact - total_mart) < 0.05


def test_pipeline_runs_are_logged(lake):
    spark = lake[0].spark
    stages = {r[0] for r in spark.sql("SELECT DISTINCT stage FROM lake.ops.pipeline_runs").collect()}
    assert {"bronze", "silver", "gold"} <= stages
    assert scalar(spark, "SELECT count(*) FROM lake.ops.pipeline_runs WHERE status = 'FAILED'") == 0

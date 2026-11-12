"""Checks against the running platform (``docker compose up``).

    docker compose run --rm openlake python3 -m pytest -m integration
"""

import pytest
import requests

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def settings():
    from openlake.config.settings import get_settings

    s = get_settings()
    try:
        requests.get(f"{s.catalog.uri}/v1/config", timeout=3).raise_for_status()
    except Exception:
        pytest.skip("the docker compose stack is not reachable")
    return s


@pytest.fixture(scope="module")
def duck(settings):
    from openlake.analytics.duckdb_client import LakeQuery

    with LakeQuery(settings) as lq:
        yield lq


def test_object_storage_has_bucket_and_raw_files(settings):
    from openlake.storage import S3ObjectStore

    store = S3ObjectStore(settings.minio)
    assert store.client.head_bucket(Bucket=store.bucket)
    assert any(True for _ in store.list("raw/ecommerce/orders/"))


def test_catalog_has_medallion_namespaces(settings):
    namespaces = {n[0] for n in requests.get(f"{settings.catalog.uri}/v1/namespaces", timeout=5).json()["namespaces"]}
    assert {"bronze", "silver", "gold", "quarantine", "ops"} <= namespaces


def test_duckdb_reads_gold_through_the_catalog(duck):
    assert duck.scalar("SELECT count(*) FROM lake.gold.fact_orders") > 0
    assert duck.scalar("SELECT count(*) FROM lake.gold.dim_date") > 365


def test_duckdb_time_travel_matches_snapshot_summary(duck):
    meta = duck.table_metadata("silver", "orders")["metadata"]
    first = sorted(meta["snapshots"], key=lambda s: s["timestamp-ms"])[0]
    expected = int(first["summary"]["total-records"])
    assert duck.scalar(f"SELECT count(*) FROM lake.silver.orders AT (VERSION => {first['snapshot-id']})") == expected


def test_layers_reconcile(duck):
    bronze = duck.scalar("SELECT count(*) FROM lake.bronze.orders")
    q = duck.records("SELECT sum(total_records) t, sum(invalid_records) i, sum(duplicate_records) d "
                     "FROM lake.ops.quality_reports WHERE dataset = 'orders'")[0]
    assert q["t"] == bronze
    assert duck.scalar("SELECT count(*) FROM lake.quarantine.orders") == q["i"]
    assert duck.scalar("SELECT count(*) FROM lake.gold.fact_orders") == duck.scalar("SELECT count(*) FROM lake.silver.orders")

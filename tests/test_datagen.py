import json

import pandas as pd

from openlake.datagen.generator import FILES
from openlake.ingestion import ingestor_for


def test_initial_batch_matches_the_olist_contract(small_dataset, config):
    batch = small_dataset / "batch_0001_initial"
    for entity, filename in FILES.items():
        path = batch / filename
        assert path.exists(), filename
        profile = ingestor_for(path).profile(path)
        spec = config.entities[entity]
        expected = {spec.source_name(c) for c in spec.column_names}
        assert expected <= set(profile.columns), (entity, expected - set(profile.columns))


def test_injected_issues_are_documented(small_dataset):
    summary = json.loads((small_dataset / "batch_0001_initial" / "_generation_summary.json").read_text())
    assert summary["injected_issues"]["orders"]["exact_duplicates"] > 0
    assert summary["injected_issues"]["order_payments"]["negative_value"] > 0


def test_generation_is_deterministic(tmp_path):
    from openlake.datagen.generator import generate_initial

    a = generate_initial(tmp_path / "a", n_orders=200, seed=5)
    b = generate_initial(tmp_path / "b", n_orders=200, seed=5)
    assert a["rows"] == b["rows"]
    fa = (tmp_path / "a" / "batch_0001_initial" / FILES["orders"]).read_bytes()
    fb = (tmp_path / "b" / "batch_0001_initial" / FILES["orders"]).read_bytes()
    assert fa == fb


def test_cdc_batches_update_existing_orders_and_evolve_schema(small_dataset):
    initial = pd.read_csv(small_dataset / "batch_0001_initial" / FILES["orders"])
    cdc = pd.read_csv(small_dataset / "batch_0002_cdc" / FILES["orders"])
    assert "delivery_priority" in cdc.columns and "delivery_priority" not in initial.columns
    updated = set(cdc["order_id"]) & set(initial["order_id"])
    assert updated, "batch 2 must contain updates to historical orders"
    assert set(cdc[cdc["order_id"].isin(updated)]["order_status"]) <= {"delivered", "canceled"}


def test_batch_three_contains_breaking_change_and_duplicate(small_dataset):
    batch = small_dataset / "batch_0003_cdc"
    sellers = pd.read_csv(batch / FILES["sellers"])
    assert "state" in sellers.columns and "seller_state" not in sellers.columns
    dup = batch / FILES["product_categories"]
    assert dup.read_bytes() == (small_dataset / "batch_0001_initial" / FILES["product_categories"]).read_bytes()

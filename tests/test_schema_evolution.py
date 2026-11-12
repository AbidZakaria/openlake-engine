from openlake.config.pipeline import EvolutionPolicy
from openlake.iceberg.schema_evolution import (
    ACCEPTED,
    ADD_COLUMN,
    MISSING_OPTIONAL,
    MISSING_REQUIRED,
    REJECTED,
    add_columns_sql,
    classify,
)

POLICY = EvolutionPolicy()


def test_identical_layout_has_no_changes(config):
    orders = config.entities["orders"]
    d = classify(orders, orders.column_names, POLICY)
    assert d.changes == [] and d.accepted


def test_new_upstream_column_is_accepted_with_hinted_type(config):
    orders = config.entities["orders"]
    d = classify(orders, orders.column_names + ["delivery_priority"], POLICY)
    assert d.accepted
    [change] = d.columns_to_add
    assert (change.change_type, change.column, change.new_type) == (ADD_COLUMN, "delivery_priority", "string")
    assert add_columns_sql("lake.silver.orders", d.columns_to_add)[0].startswith(
        "ALTER TABLE lake.silver.orders ADD COLUMN `delivery_priority` string")


def test_renamed_required_column_rejects_the_whole_file(config):
    sellers = config.entities["sellers"]
    cols = ["seller_id", "seller_zip_code_prefix", "seller_city", "state"]
    d = classify(sellers, cols, POLICY)
    assert not d.accepted
    kinds = {(c.change_type, c.column): c.action for c in d.changes}
    assert kinds[(MISSING_REQUIRED, "seller_state")] == REJECTED
    # the additive half of the rename must not leak into the table
    assert kinds[(ADD_COLUMN, "state")] == REJECTED
    assert d.columns_to_add == []


def test_missing_optional_column_is_accepted(config):
    customers = config.entities["customers"]
    cols = [c for c in customers.column_names if c != "customer_city"]
    d = classify(customers, cols, POLICY)
    assert d.accepted
    assert [(c.change_type, c.action) for c in d.changes] == [(MISSING_OPTIONAL, ACCEPTED)]


def test_source_typos_are_mapped_through_renames(config):
    products = config.entities["products"]
    raw = [products.source_name(c) for c in products.column_names]
    assert "product_name_lenght" in raw
    assert classify(products, raw, POLICY).changes == []


def test_previously_evolved_columns_are_not_reported_again(config):
    orders = config.entities["orders"]
    known = orders.column_names + ["delivery_priority"]
    assert classify(orders, known, POLICY, known_columns=known).changes == []


def test_strict_policy_rejects_additive_changes(config):
    orders = config.entities["orders"]
    d = classify(orders, orders.column_names + ["x"], EvolutionPolicy(additive="reject"))
    assert not d.accepted

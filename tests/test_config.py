import pytest

from openlake.config.pipeline import ConfigError, parse_config


def test_all_olist_files_map_to_entities(config):
    files = {
        "olist_customers_dataset.csv": "customers",
        "olist_orders_dataset.csv": "orders",
        "olist_order_items_dataset.csv": "order_items",
        "olist_order_payments_dataset.parquet": "order_payments",
        "olist_order_reviews_dataset.json": "order_reviews",
        "olist_products_dataset.csv": "products",
        "olist_sellers_dataset.csv": "sellers",
        "product_category_name_translation.csv": "product_categories",
    }
    for filename, entity in files.items():
        assert config.entity_for_file(filename).name == entity
    assert config.entity_for_file("weather.csv") is None


def test_entity_order_puts_references_first(config):
    order = [e.name for e in config.ordered_entities()]
    for entity in config.entities.values():
        for rule in entity.rules:
            if rule.type == "foreign_key":
                assert order.index(rule.ref_table) < order.index(entity.name), rule.name


def test_value_set_references_are_expanded(config):
    rule = next(r for r in config.entities["customers"].rules if r.name == "customer_state_valid")
    assert "SP" in rule.values and len(rule.values) == 27


def test_rename_inverse(config):
    products = config.entities["products"]
    assert products.source_name("product_name_length") == "product_name_lenght"
    assert products.source_name("product_id") == "product_id"


def _minimal(**entity_overrides):
    entity = {"keys": ["id"], "columns": {"id": {"type": "string", "required": True}}, **entity_overrides}
    return {"sources": {"s": {"entities": {"thing": entity}}}}


def test_unknown_rule_type_fails_fast():
    with pytest.raises(ConfigError, match="unknown rule type"):
        parse_config(_minimal(rules=[{"name": "x", "type": "telepathy", "column": "id"}]))


def test_key_must_be_in_contract():
    with pytest.raises(ConfigError, match="key columns"):
        parse_config(_minimal(keys=["nope"]))


def test_foreign_key_must_reference_known_entity():
    with pytest.raises(ConfigError, match="unknown entity"):
        parse_config(_minimal(rules=[{"name": "fk", "type": "foreign_key", "column": "id",
                                      "ref_table": "ghost", "ref_column": "id"}]))


def test_unsupported_type_rejected():
    with pytest.raises(ConfigError, match="unsupported type"):
        parse_config({"sources": {"s": {"entities": {"t": {"keys": ["id"], "columns": {"id": {"type": "uuid"}}}}}}})

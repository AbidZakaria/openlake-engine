import datetime as dt

import pytest

pytestmark = pytest.mark.spark


def test_trim_and_nullify(spark):
    from openlake.transformations.cleaning import trim_and_nullify

    df = spark.createDataFrame([(" a ", "null"), ("", "x"), (None, " NaN ")], "c1 string, c2 string")
    out = trim_and_nullify(df, ["c1", "c2"]).collect()
    assert [(r.c1, r.c2) for r in out] == [("a", None), (None, "x"), (None, None)]


def test_standardization(spark, config):
    from openlake.transformations.standardization import standardize_column_names, standardize_values

    customers = config.entities["customers"]
    df = spark.createDataFrame([(" sp ", "SAO PAULO", "1000")],
                               "customer_state string, customer_city string, customer_zip_code_prefix string")
    from pyspark.sql import functions as F

    df = df.withColumn("customer_state", F.trim("customer_state"))
    r = standardize_values(df, customers).collect()[0]
    assert (r.customer_state, r.customer_city, r.customer_zip_code_prefix) == ("SP", "Sao Paulo", "01000")

    products = config.entities["products"]
    renamed = standardize_column_names(spark.createDataFrame([(1,)], "product_name_lenght int"), products)
    assert renamed.columns == ["product_name_length"]


def test_cast_to_contract_flags_values_that_do_not_cast(spark, config):
    from openlake.transformations.cleaning import cast_to_contract

    items = config.entities["order_items"]
    df = spark.createDataFrame([("o1", "1", "p", "s", "2018-01-02 10:00:00", "19.9", "3.0"),
                                ("o2", "x", "p", "s", "2018-13-45 25:61:00", "abc", None)],
                               ", ".join(f"{c} string" for c in ["order_id", "order_item_id", "product_id", "seller_id",
                                                                 "shipping_limit_date", "price", "freight_value"]))
    out, checks = cast_to_contract(df, items)
    rows = out.orderBy("order_id").collect()
    assert rows[0].order_item_id == 1 and rows[0].price == 19.9
    assert rows[0].shipping_limit_date == dt.datetime(2018, 1, 2, 10, 0)
    assert rows[1].order_item_id is None and rows[1].price is None and rows[1].shipping_limit_date is None
    assert set(checks) == {"order_item_id", "shipping_limit_date", "price", "freight_value"}


def test_validator_quarantines_errors_and_counts_warnings(spark, config):
    from openlake.quality.validator import FAILED_COL, validate
    from openlake.transformations.cleaning import cast_to_contract

    pay = config.entities["order_payments"]
    df = spark.createDataFrame([("o1", "1", "credit_card", "3", "10.0"),
                                ("o2", "1", "pix", "1", "5.0"),          # unknown type  -> error
                                ("o3", "1", "boleto", "30", "-1.0"),     # negative value -> error, installments -> warn
                                (None, "1", "voucher", "1", "2.0")],     # null order id -> error
                               "order_id string, payment_sequential string, payment_type string, "
                               "payment_installments string, payment_value string")
    typed, checks = cast_to_contract(df, pay)
    res = validate(typed, pay, checks)
    assert res.total == 4 and res.invalid_count == 3
    assert [r.order_id for r in res.valid.collect()] == ["o1"]
    failures = {r.order_id: set(r[FAILED_COL]) for r in res.invalid.collect()}
    assert failures["o2"] == {"payment_type_valid"}
    assert failures["o3"] == {"payment_value_non_negative"}
    by_rule = {r.rule: r.failed for r in res.rule_results}
    assert by_rule["installments_range"] == 1        # warning is counted ...
    assert "installments_range" not in failures["o3"]  # ... but never quarantines


def test_latest_by_key_keeps_newest_image(spark):
    from openlake.transformations.deduplication import latest_by_key

    df = spark.createDataFrame([
        ("a", "shipped", dt.datetime(2024, 1, 1), "h1"),
        ("a", "delivered", dt.datetime(2024, 1, 2), "h2"),
        ("b", "shipped", dt.datetime(2024, 1, 1), "h3"),
        ("b", "shipped", dt.datetime(2024, 1, 1), "h3"),
    ], "id string, status string, _ingested_at timestamp, _record_hash string")
    out = {r.id: r.status for r in latest_by_key(df, ["id"]).collect()}
    assert out == {"a": "delivered", "b": "shipped"}

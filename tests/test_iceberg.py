"""Iceberg behaviours the platform relies on, verified on a filesystem catalog."""

import pytest

pytestmark = pytest.mark.spark


@pytest.fixture
def table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.test")
    name = "lake.test.events"
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"CREATE TABLE {name} (id BIGINT, kind STRING, amount INT) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a', 10), (2, 'b', 20)")
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def snapshot(spark, t):
    from openlake.iceberg.snapshot_manager import current_snapshot

    return current_snapshot(spark, t)


def test_time_travel_returns_old_data(spark, table):
    s1 = snapshot(spark, table)
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 30)")
    assert spark.sql(f"SELECT count(*) FROM {table} VERSION AS OF {s1}").collect()[0][0] == 2
    assert spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 3


def test_rollback_restores_previous_state(spark, table):
    from openlake.iceberg.rollback import create_tag, refs, rollback_to_snapshot

    good = snapshot(spark, table)
    create_tag(spark, table, "good")
    spark.sql(f"UPDATE {table} SET amount = amount * 100")
    assert spark.sql(f"SELECT sum(amount) FROM {table}").collect()[0][0] == 3000
    rollback_to_snapshot(spark, table, good)
    assert spark.sql(f"SELECT sum(amount) FROM {table}").collect()[0][0] == 30
    assert any(r["name"] == "good" and r["type"] == "TAG" for r in refs(spark, table))


def test_schema_changes_are_metadata_only(spark, table):
    before = {r[0] for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}
    spark.sql(f"ALTER TABLE {table} ADD COLUMN note STRING")
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN kind TO category")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN amount TYPE BIGINT")
    after = {r[0] for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}
    assert before == after
    row = spark.sql(f"SELECT category, amount, note FROM {table} WHERE id = 1").collect()[0]
    assert (row.category, row.amount, row.note) == ("a", 10, None)


def test_failed_write_does_not_change_the_table(spark, table):
    from pyspark.sql import functions as F

    s0 = snapshot(spark, table)
    bad = spark.range(100).select(F.col("id"), F.lit("x").alias("kind"),
                                  F.when(F.col("id") == 57, F.raise_error(F.lit("boom"))).otherwise(F.lit(1)).alias("amount"))
    with pytest.raises(Exception, match="boom"):
        bad.writeTo(table).append()
    assert snapshot(spark, table) == s0
    assert spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 2


def test_compaction_reduces_files(spark, table):
    from openlake.iceberg.maintenance import compact, data_file_count

    for i in range(6):
        spark.sql(f"INSERT INTO {table} VALUES ({10 + i}, 'z', {i})")
    before = data_file_count(spark, table)
    compact(spark, table, "lake", min_input_files=2, target_mb=16)
    assert data_file_count(spark, table) < before
    assert spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 8


def test_branch_write_audit_publish(spark, table):
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
    spark.sql(f"INSERT INTO {table}.branch_audit VALUES (9, 'staged', 90)")
    assert spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 2
    spark.sql(f"CALL lake.system.fast_forward(table => '{table[5:]}', branch => 'main', to => 'audit')")
    assert spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 3

import datetime as dt
import decimal

import pytest

from openlake.analytics.duckdb_client import QueryRejected, assert_read_only


@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "select * from lake.gold.fact_orders where order_status = 'delete'",
    "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
    "SELECT count(*) FROM lake.silver.orders AT (VERSION => 123)",
])
def test_read_only_queries_are_allowed(sql):
    assert assert_read_only(sql) == sql


@pytest.mark.parametrize("sql", [
    "DROP TABLE lake.gold.fact_orders",
    "SELECT 1; SELECT 2",
    "INSERT INTO lake.gold.dim_date VALUES (1)",
    "ATTACH 'x.db' AS other",
    "COPY (SELECT 1) TO '/tmp/out.csv'",
    "SELECT * FROM read_csv('/etc/passwd') WHERE 1 = 1 AND (SELECT 1) = 1; CREATE TABLE t AS SELECT 1",
])
def test_write_or_multi_statements_are_rejected(sql):
    with pytest.raises(QueryRejected):
        assert_read_only(sql)


def test_dashboard_json_keeps_snapshot_ids_exact():
    pytest.importorskip("requests")
    from dashboard.lake_data import jsonable

    out = jsonable({"snapshot_id": 9022293328887906314, "small": 7, "d": dt.date(2018, 1, 2),
                    "v": decimal.Decimal("1.50"), "nan": float("nan"), "nested": [{"id": 2**60}]})
    assert out == {"snapshot_id": "9022293328887906314", "small": 7, "d": "2018-01-02", "v": 1.5,
                   "nan": None, "nested": [{"id": str(2**60)}]}

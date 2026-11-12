import duckdb
import pytest

from openlake.config.pipeline import RuleSpec
from openlake.quality.quality_report import QualityReport, RuleResult
from openlake.quality.rules import pass_expression, quote_ident, sql_literal


def rule(**kw):
    base = {"name": "r", "type": "not_null", "severity": "error", "column": "c"}
    base.update(kw)
    return RuleSpec(**base)


def test_compiled_rules():
    assert pass_expression(rule()) == "`c` IS NOT NULL"
    assert pass_expression(rule(type="range", min=0, max=5)) == "(`c` IS NULL OR (`c` >= 0 AND `c` <= 5))"
    assert pass_expression(rule(type="accepted_values", values=("a", "b"))) == "(`c` IS NULL OR `c` IN ('a', 'b'))"
    assert "RLIKE '^[0-9]{5}$'" in pass_expression(rule(type="regex", pattern="^[0-9]{5}$"))
    assert pass_expression(rule(type="ordered", column=None, columns=("a", "b"))) == \
        "(`a` IS NULL OR `b` IS NULL OR `a` <= `b`)"


def test_dataset_level_rules_are_not_row_predicates():
    with pytest.raises(ValueError):
        pass_expression(rule(type="unique"))


def test_literals_and_identifiers_are_escaped():
    assert sql_literal("it's") == "'it\\'s'"
    assert quote_ident("we`ird") == "`we``ird`"


def test_compiled_predicates_evaluate_correctly_in_sql():
    """The same predicates are portable: evaluate them in DuckDB (backticks -> quotes)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'SP'), (-3, 'XX'), (NULL, NULL)) v(c, s)")
    checks = {
        rule(type="range", min=0): [True, False, True],
        rule(type="accepted_values", column="s", values=("SP", "RJ")): [True, False, True],
        rule(): [True, True, False],
    }
    for r, expected in checks.items():
        sql = pass_expression(r).replace("`", '"')
        got = [row[0] for row in con.execute(f"SELECT {sql} FROM t ORDER BY rowid").fetchall()]
        assert got == expected, r.type


def test_report_status_and_score():
    rep = QualityReport("run", "orders", "silver", ["b1"], total_records=100, valid_records=97, invalid_records=3)
    rep.rules = [RuleResult("ok", "not_null", "completeness", "error", "id", 0, 100),
                 RuleResult("fk", "foreign_key", "consistency", "warn", "customer_id", 2, 100)]
    assert rep.score == 0.97
    assert rep.status == "PASSED_WITH_WARNINGS"
    rep.rules.append(RuleResult("bad", "range", "validity", "error", "price", 3, 100))
    assert rep.status == "FAILED_RECORDS_QUARANTINED"
    assert rep.failed_rules == ["fk", "bad"]
    row = rep.to_row()
    assert row["batch_ids"] == "b1" and row["quality_score"] == 0.97

"""Compile declarative quality rules into Spark SQL predicates.

Each row-level rule becomes a boolean SQL expression that is TRUE when a row
*passes*. Keeping the compiler free of PySpark makes it unit-testable and lets
the same predicates be reused by other SQL engines (DuckDB uses them in the
integration tests).

Null semantics: only ``not_null`` fails on NULL. Every other rule treats NULL as
"not applicable", so a missing optional value is never double-counted.
"""

from __future__ import annotations

from openlake.config.pipeline import RuleSpec

DIMENSION = {
    "not_null": "completeness",
    "unique": "uniqueness",
    "range": "validity",
    "accepted_values": "validity",
    "regex": "validity",
    "not_future": "timeliness",
    "ordered": "consistency",
    "foreign_key": "consistency",
    "type": "schema",
    "schema_contract": "schema",
}


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def pass_expression(rule: RuleSpec) -> str:
    """SQL predicate that is TRUE when a row satisfies ``rule``."""
    if not rule.is_row_level:
        raise ValueError(f"rule {rule.name} ({rule.type}) is dataset-level, not row-level")
    col = quote_ident(rule.column) if rule.column else None
    if rule.type == "not_null":
        return f"{col} IS NOT NULL"
    if rule.type == "range":
        bounds = []
        if rule.min is not None:
            bounds.append(f"{col} >= {sql_literal(rule.min)}")
        if rule.max is not None:
            bounds.append(f"{col} <= {sql_literal(rule.max)}")
        if not bounds:
            raise ValueError(f"range rule {rule.name} needs min and/or max")
        return f"({col} IS NULL OR ({' AND '.join(bounds)}))"
    if rule.type == "accepted_values":
        values = ", ".join(sql_literal(v) for v in rule.values)
        return f"({col} IS NULL OR {col} IN ({values}))"
    if rule.type == "regex":
        return f"({col} IS NULL OR {col} RLIKE {sql_literal(rule.pattern)})"
    if rule.type == "not_future":
        return f"({col} IS NULL OR {col} <= current_timestamp() + INTERVAL 1 DAY)"
    if rule.type == "ordered":
        first, second = (quote_ident(c) for c in rule.columns)
        return f"({first} IS NULL OR {second} IS NULL OR {first} <= {second})"
    raise ValueError(f"unsupported rule type {rule.type}")


def type_check_expression(raw_column: str, typed_column: str) -> str:
    """A value that was present in the raw data must survive the cast to its contract type."""
    return f"({quote_ident(raw_column)} IS NULL OR {quote_ident(typed_column)} IS NOT NULL)"


def dimension_of(rule_type: str) -> str:
    return DIMENSION.get(rule_type, "validity")

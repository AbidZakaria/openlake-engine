"""Apply quality rules to a Spark DataFrame in a single pass.

Row-level rules are evaluated as boolean columns; error-severity failures send
the record to quarantine (with the list of failed rules attached), warn-severity
failures are only counted. Dataset-level rules (uniqueness, referential
integrity) are computed with one aggregation / anti-join each.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from openlake.config.pipeline import EntitySpec, RuleSpec
from openlake.quality.quality_report import RuleResult
from openlake.quality.rules import dimension_of, pass_expression, type_check_expression

FAILED_COL = "_failed_rules"


@dataclass
class ValidationResult:
    valid: DataFrame
    invalid: DataFrame
    rule_results: list[RuleResult]
    total: int
    invalid_count: int
    null_counts: dict[str, int]


def _flag(name: str) -> str:
    return f"__ok__{name}"


def validate(df: DataFrame, entity: EntitySpec, type_checks: dict[str, str] | None = None,
             extra_failures: Column | None = None) -> ValidationResult:
    """Evaluate row-level rules.

    ``type_checks`` maps contract column -> raw column so values that could not
    be cast to the contract type are flagged as ``type:<column>`` failures.
    ``extra_failures`` is an optional array<string> column with failures computed
    upstream (e.g. schema-contract violations for a whole batch).
    """
    checks: list[tuple[str, str, RuleSpec | None, str]] = []  # (name, severity, rule, expression)
    for column, raw in (type_checks or {}).items():
        checks.append((f"type:{column}", "error", None, type_check_expression(raw, column)))
    for rule in entity.rules:
        if rule.is_row_level:
            checks.append((rule.name, rule.severity, rule, pass_expression(rule)))

    flagged = df
    for name, _sev, _rule, expr in checks:
        flagged = flagged.withColumn(_flag(name), F.coalesce(F.expr(expr), F.lit(False)))

    error_names = [n for n, sev, _, _ in checks if sev == "error"]
    failures = F.filter(
        F.array(*[F.when(~F.col(_flag(n)), F.lit(n)) for n in error_names]) if error_names else F.array(),
        lambda x: x.isNotNull(),
    )
    if extra_failures is not None:
        failures = F.array_distinct(F.concat(failures, F.coalesce(extra_failures, F.array().cast("array<string>"))))
    flagged = flagged.withColumn(FAILED_COL, failures.cast("array<string>"))
    flagged = flagged.cache()

    contract_cols = [c for c in entity.column_names if c in df.columns]
    aggs = [F.count(F.lit(1)).alias("__total"),
            F.sum(F.when(F.size(FAILED_COL) > 0, 1).otherwise(0)).alias("__invalid")]
    aggs += [F.sum(F.when(~F.col(_flag(n)), 1).otherwise(0)).alias(f"__fail__{i}") for i, (n, *_r) in enumerate(checks)]
    aggs += [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"__null__{c}") for c in contract_cols]
    if extra_failures is not None:
        aggs.append(F.sum(F.when(F.array_contains(F.col(FAILED_COL), "schema_contract"), 1).otherwise(0))
                    .alias("__schema_contract"))
    stats = flagged.agg(*aggs).collect()[0].asDict()
    total = int(stats["__total"] or 0)

    results = []
    for i, (name, severity, rule, _expr) in enumerate(checks):
        rtype = rule.type if rule else "type"
        column = rule.column if rule and rule.column else (",".join(rule.columns) if rule else name.split(":", 1)[1])
        results.append(RuleResult(name, rtype, dimension_of(rtype), severity, column,
                                  int(stats[f"__fail__{i}"] or 0), total))
    if extra_failures is not None:
        results.append(RuleResult("schema_contract", "schema_contract", "schema", "error", None,
                                  int(stats.get("__schema_contract") or 0), total))

    drop = [_flag(n) for n, *_ in checks]
    valid = flagged.filter(F.size(FAILED_COL) == 0).drop(FAILED_COL, *drop)
    invalid = flagged.filter(F.size(FAILED_COL) > 0).drop(*drop)
    null_counts = {c: int(stats[f"__null__{c}"] or 0) for c in contract_cols}
    return ValidationResult(valid, invalid, results, total, int(stats["__invalid"] or 0), null_counts)


def uniqueness_result(df: DataFrame, rule: RuleSpec) -> RuleResult:
    """Number of surplus rows sharing a key value (before deduplication)."""
    col = rule.column
    stats = (df.filter(F.col(col).isNotNull()).groupBy(col).count()
             .agg(F.sum(F.col("count") - 1).alias("dupes"), F.sum("count").alias("checked")).collect()[0])
    return RuleResult(rule.name, rule.type, dimension_of(rule.type), rule.severity, col,
                      int(stats["dupes"] or 0), int(stats["checked"] or 0))


def foreign_key_result(spark: SparkSession, df: DataFrame, rule: RuleSpec, ref_table: str) -> RuleResult:
    """Rows whose foreign key does not exist in the referenced Silver table."""
    col = rule.column
    candidates = df.select(F.col(col).alias("__k")).filter(F.col("__k").isNotNull())
    checked = candidates.count()
    if not spark.catalog.tableExists(ref_table):
        orphans = checked
    else:
        ref = spark.table(ref_table).select(F.col(rule.ref_column).alias("__k")).distinct()
        orphans = candidates.join(ref, "__k", "left_anti").count()
    return RuleResult(rule.name, rule.type, dimension_of(rule.type), rule.severity, col, int(orphans), int(checked))

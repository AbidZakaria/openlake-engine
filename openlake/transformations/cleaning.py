"""Cleaning: whitespace, empty strings and casting to the data contract."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from openlake.config.pipeline import EntitySpec
from openlake.iceberg.schema_evolution import spark_type

RAW_PREFIX = "__raw__"
TIMESTAMP_FORMATS = ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS",
                     "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd")


def trim_and_nullify(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim strings and turn '', 'null', 'NaN' and friends into real NULLs."""
    for c in columns:
        trimmed = F.trim(F.col(c).cast("string"))
        df = df.withColumn(
            c, F.when(trimmed.isNull() | F.lower(trimmed).isin("", "null", "none", "nan", "n/a"), F.lit(None))
            .otherwise(trimmed),
        )
    return df


def parse_timestamp(col: F.Column) -> F.Column:
    return F.coalesce(*[F.try_to_timestamp(col, F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def cast_expression(col_name: str, contract_type: str) -> F.Column:
    c = F.col(col_name)
    if contract_type == "string":
        return c
    if contract_type == "timestamp":
        return parse_timestamp(c)
    if contract_type == "date":
        return F.to_date(parse_timestamp(c))
    if contract_type in ("int", "bigint"):
        # tolerate "3.0" coming from float-typed sources
        return F.expr(f"try_cast(try_cast(`{col_name}` AS double) AS {contract_type})")
    if contract_type == "boolean":
        return F.lower(c).isin("true", "1", "yes", "y", "t")
    return F.expr(f"try_cast(`{col_name}` AS {spark_type(contract_type)})")


def cast_to_contract(df: DataFrame, entity: EntitySpec, extra_columns: dict[str, str] | None = None
                     ) -> tuple[DataFrame, dict[str, str]]:
    """Cast every contract column; keep the raw text for values that fail to cast.

    Returns the DataFrame and a ``{typed_column: raw_column}`` map used by the
    validator to flag cast failures as ``type:<column>`` quality violations.
    """
    types = {c.name: c.type for c in entity.columns}
    types.update(extra_columns or {})
    type_checks: dict[str, str] = {}
    for name, ctype in types.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
        if ctype == "string":
            continue
        raw = f"{RAW_PREFIX}{name}"
        df = df.withColumn(raw, F.col(name)).withColumn(name, cast_expression(raw, ctype))
        type_checks[name] = raw
    return df, type_checks

"""Standardization: consistent column names and categorical values."""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from openlake.config.pipeline import EntitySpec

STANDARDIZERS = {"lower", "upper", "title", "snake", "zip5"}


def snake_case(name: str) -> str:
    name = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name.strip())
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def standardize_column_names(df: DataFrame, entity: EntitySpec) -> DataFrame:
    """Apply the contract's explicit renames, then snake_case anything else."""
    for col in df.columns:
        if col.startswith("_"):
            continue
        target = entity.rename.get(col) or snake_case(col)
        if target != col:
            df = df.withColumnRenamed(col, target)
    return df


def standardize_value(col: F.Column, how: str) -> F.Column:
    if how == "lower":
        return F.lower(col)
    if how == "upper":
        return F.upper(col)
    if how == "title":
        return F.initcap(F.lower(col))
    if how == "snake":
        return F.regexp_replace(F.regexp_replace(F.lower(col), r"[\s\-/]+", "_"), r"_+", "_")
    if how == "zip5":
        digits = F.regexp_replace(col, r"[^0-9]", "")
        return F.when(F.length(digits).between(1, 5), F.lpad(digits, 5, "0")).otherwise(col)
    raise ValueError(f"unknown standardizer {how!r}")


def standardize_values(df: DataFrame, entity: EntitySpec) -> DataFrame:
    for col, how in entity.standardize.items():
        if col in df.columns:
            df = df.withColumn(col, standardize_value(F.col(col), how))
    return df

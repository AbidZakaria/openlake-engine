"""Rollback and references (tags/branches).

Rolling back is a metadata operation: the table's *current snapshot pointer* is
moved to an earlier snapshot in a new metadata commit. No data files are
rewritten, and the "bad" snapshot remains in history (until expired) for
forensics.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def _short(table: str, catalog: str) -> str:
    prefix = f"{catalog}."
    return table[len(prefix):] if table.startswith(prefix) else table


def rollback_to_snapshot(spark: SparkSession, table: str, snapshot_id: int, catalog: str = "lake") -> dict:
    row = spark.sql(f"CALL {catalog}.system.rollback_to_snapshot(table => '{_short(table, catalog)}', "
                    f"snapshot_id => {int(snapshot_id)})").collect()[0]
    return {"previous_snapshot_id": row["previous_snapshot_id"], "current_snapshot_id": row["current_snapshot_id"]}


def rollback_to_timestamp(spark: SparkSession, table: str, ts: str, catalog: str = "lake") -> dict:
    row = spark.sql(f"CALL {catalog}.system.rollback_to_timestamp(table => '{_short(table, catalog)}', "
                    f"timestamp => TIMESTAMP '{ts}')").collect()[0]
    return {"previous_snapshot_id": row["previous_snapshot_id"], "current_snapshot_id": row["current_snapshot_id"]}


def create_tag(spark: SparkSession, table: str, tag: str, snapshot_id: int | None = None,
               retain_days: int | None = None) -> None:
    as_of = f" AS OF VERSION {int(snapshot_id)}" if snapshot_id else ""
    retain = f" RETAIN {int(retain_days)} DAYS" if retain_days else ""
    spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE TAG `{tag}`{as_of}{retain}")


def refs(spark: SparkSession, table: str) -> list[dict]:
    return [r.asDict() for r in spark.sql(f"SELECT name, type, snapshot_id FROM {table}.refs").collect()]

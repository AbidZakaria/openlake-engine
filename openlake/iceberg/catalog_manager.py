"""Thin helpers around the Iceberg catalog as seen from Spark."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from openlake.config.pipeline import PipelineConfig


class CatalogManager:
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    # -------------------------------------------------------------- namespaces
    def ensure_namespaces(self) -> None:
        for ns in self.config.namespaces.values():
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.config.catalog}.{ns}")

    def list_tables(self, layer: str) -> list[str]:
        ns = f"{self.config.catalog}.{self.config.namespaces[layer]}"
        try:
            rows = self.spark.sql(f"SHOW TABLES IN {ns}").collect()
        except Exception:
            return []
        return [f"{ns}.{r['tableName']}" for r in rows]

    # ------------------------------------------------------------------ tables
    def exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(table)

    def properties_sql(self, extra: dict[str, str] | None = None) -> str:
        props = {**self.config.table_properties, **(extra or {})}
        return ", ".join(f"'{k}'='{v}'" for k, v in props.items())

    def create_from_df(self, table: str, df: DataFrame, partition_by: tuple[str, ...] | list[str] = (),
                       comment: str = "", extra_props: dict[str, str] | None = None) -> None:
        """Create an empty Iceberg table whose schema matches ``df``."""
        if self.exists(table):
            return
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
        part = f" PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
        cmt = f" COMMENT '{comment}'" if comment else ""
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING iceberg{part}{cmt} "
            f"TBLPROPERTIES ({self.properties_sql(extra_props)})"
        )

    def columns(self, table: str) -> list[str]:
        return [f.name for f in self.spark.table(table).schema.fields]

    def current_snapshot_id(self, table: str) -> int | None:
        if not self.exists(table):
            return None
        rows = self.spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        return int(rows[0]["snapshot_id"]) if rows else None

    def snapshot_ids(self, table: str) -> list[int]:
        if not self.exists(table):
            return []
        return [int(r["snapshot_id"]) for r in
                self.spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()]

    def metadata_location(self, table: str) -> str | None:
        rows = self.spark.sql(
            f"SELECT file FROM {table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
        ).collect()
        return rows[0]["file"] if rows else None

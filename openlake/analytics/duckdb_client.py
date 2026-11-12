"""DuckDB as an independent query engine over the lakehouse.

Spark writes and manages the tables; DuckDB reads them without any Spark
involvement, by attaching the *same* Iceberg REST catalog:

    ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'http://iceberg-rest:8181', AUTHORIZATION_TYPE 'none');
    SELECT * FROM lake.gold.daily_revenue;
    SELECT count(*) FROM lake.silver.orders AT (VERSION => 1234567890);   -- time travel

Both engines see identical committed snapshots because the catalog is the
single source of truth for "which metadata file is current".
"""

from __future__ import annotations

import os
import re
from typing import Any

import duckdb
import requests

from openlake.config.settings import Settings, get_settings

READ_ONLY_TYPES = {"SELECT", "EXPLAIN"}
FORBIDDEN = re.compile(r"\b(attach|detach|copy|install|load|create|insert|update|delete|drop|alter|pragma|"
                       r"set|call|export|import|checkpoint|vacuum|secret)\b", re.IGNORECASE)


class QueryRejected(ValueError):
    pass


def assert_read_only(sql: str) -> str:
    """Allow exactly one SELECT/EXPLAIN statement (used by the dashboard's SQL console)."""
    statements = duckdb.extract_statements(sql)
    if len(statements) != 1:
        raise QueryRejected("exactly one statement is allowed")
    kind = str(statements[0].type).split(".")[-1].upper()
    if kind not in READ_ONLY_TYPES:
        raise QueryRejected(f"only SELECT queries are allowed (got {kind})")
    stripped = re.sub(r"'(?:[^']|'')*'", "''", sql)  # ignore string literals
    if FORBIDDEN.search(stripped):
        raise QueryRejected("statement contains a forbidden keyword")
    return sql


class LakeQuery:
    def __init__(self, settings: Settings | None = None, catalog_alias: str = "lake", threads: int | None = None):
        self.settings = settings or get_settings()
        self.alias = catalog_alias
        self.con = duckdb.connect()
        home = os.environ.get("HOME")
        if not home or not os.path.isdir(home) or not os.access(home, os.W_OK):
            self.con.execute("SET home_directory = '/tmp'")
        if threads:
            self.con.sql(f"SET threads = {int(threads)}")
        self._setup()

    def _setup(self) -> None:
        s3 = self.settings.minio
        self.con.sql("LOAD iceberg; LOAD httpfs;")
        self.con.sql(f"""
            CREATE OR REPLACE SECRET openlake_s3 (
                TYPE s3, KEY_ID '{s3.access_key}', SECRET '{s3.secret_key}',
                ENDPOINT '{s3.host_port}', URL_STYLE 'path', USE_SSL {str(s3.use_ssl).lower()}, REGION '{s3.region}'
            )""")
        self.con.sql(f"""
            ATTACH 'warehouse' AS {self.alias} (
                TYPE iceberg, ENDPOINT '{self.settings.catalog.uri}', AUTHORIZATION_TYPE 'none'
            )""")

    # ------------------------------------------------------------------ query
    def sql(self, query: str, params: list | None = None):
        return self.con.execute(query, params or [])

    def records(self, query: str, params: list | None = None) -> list[dict[str, Any]]:
        cur = self.con.execute(query, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def scalar(self, query: str):
        return self.con.execute(query).fetchone()[0]

    def tables(self) -> list[dict[str, str]]:
        rows = self.con.execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ? "
                                "ORDER BY 1, 2", [self.alias]).fetchall()
        return [{"namespace": r[0], "table": r[1]} for r in rows]

    # -------------------------------------------------------- catalog metadata
    def table_metadata(self, namespace: str, table: str) -> dict:
        """Full Iceberg table metadata straight from the REST catalog."""
        resp = requests.get(f"{self.settings.catalog.uri}/v1/namespaces/{namespace}/tables/{table}", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> LakeQuery:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

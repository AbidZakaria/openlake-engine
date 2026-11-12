"""Read model behind the dashboard.

Everything is read with DuckDB (attached to the Iceberg REST catalog) or from
the catalog's REST API directly — the dashboard never starts Spark. The same
functions feed the live API and the static snapshot used for GitHub Pages.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import math
import os
import threading
import time
from pathlib import Path
from typing import Any

import requests

from openlake.analytics.duckdb_client import LakeQuery, assert_read_only
from openlake.config.settings import get_settings

LAYERS = ("gold", "silver", "bronze", "quarantine", "ops", "lab")
JS_SAFE = 2**53 - 1
QUERIES_DIR = Path(__file__).resolve().parents[1] / "openlake" / "analytics" / "queries"


def jsonable(value: Any) -> Any:
    """Make DuckDB/REST values JSON-safe; 64-bit ids become strings so JS keeps them exact."""
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value) if abs(value) > JS_SAFE else value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dt.datetime | dt.date | dt.time):
        return value.isoformat(sep=" ") if isinstance(value, dt.datetime) else value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


class LakeData:
    def __init__(self, ttl: float = 20.0):
        self.settings = get_settings()
        self.ttl = ttl
        self._lock = threading.RLock()
        self._lq: LakeQuery | None = None
        self._cache: dict[str, tuple[float, Any]] = {}
        self.reports_dir = Path(os.environ.get("OPENLAKE_REPORTS_DIR", str(self.settings.reports_dir)))
        self.bench_file = Path(os.environ.get("OPENLAKE_BENCH_FILE", str(
            Path(__file__).resolve().parents[1] / "benchmarks" / "results" / "latest.json")))

    # ------------------------------------------------------------ plumbing
    @property
    def lq(self) -> LakeQuery:
        if self._lq is None:
            self._lq = LakeQuery(threads=2)
        return self._lq

    def reset(self) -> None:
        with self._lock:
            if self._lq is not None:
                try:
                    self._lq.close()
                except Exception:
                    pass
            self._lq = None
            self._cache.clear()

    def cached(self, key: str, fn, ttl: float | None = None):
        now = time.monotonic()
        hit = self._cache.get(key)
        if hit and now - hit[0] < (ttl or self.ttl):
            return hit[1]
        with self._lock:
            value = fn()
        self._cache[key] = (now, value)
        return value

    def rows(self, sql: str, params: list | None = None) -> list[dict]:
        with self._lock:
            try:
                return self.lq.records(sql, params)
            except Exception as exc:
                # A table may have been replaced since ATTACH; reconnect once.
                if "does not exist" in str(exc) or "Catalog Error" in str(exc):
                    self.reset()
                    return self.lq.records(sql, params)
                raise

    def table_exists(self, ns: str, name: str) -> bool:
        return any(t["namespace"] == ns and t["table"] == name for t in self.catalog_tables())

    # ------------------------------------------------------------- catalog
    def _rest(self, path: str) -> dict:
        r = requests.get(f"{self.settings.catalog.uri}/v1/{path}", timeout=15)
        r.raise_for_status()
        return r.json()

    def catalog_tables(self) -> list[dict]:
        def load():
            out = []
            for ns in self._rest("namespaces").get("namespaces", []):
                nsname = ".".join(ns)
                for ident in self._rest(f"namespaces/{nsname}/tables").get("identifiers", []):
                    out.append({"namespace": nsname, "table": ident["name"]})
            return out
        return self.cached("catalog_tables", load)

    def table_metadata(self, ns: str, name: str) -> dict:
        return self.cached(f"meta:{ns}.{name}", lambda: self._rest(f"namespaces/{ns}/tables/{name}"))

    @staticmethod
    def summarize(ns: str, name: str, payload: dict) -> dict:
        meta = payload["metadata"]
        snaps = meta.get("snapshots", []) or []
        current_id = meta.get("current-snapshot-id")
        current = next((s for s in snaps if s["snapshot-id"] == current_id), None)
        summary = (current or {}).get("summary", {})
        spec = next((s for s in meta.get("partition-specs", []) if s["spec-id"] == meta.get("default-spec-id")), {})
        schema = next((s for s in meta.get("schemas", []) if s["schema-id"] == meta.get("current-schema-id")), {})
        fields_by_id = {f["id"]: f["name"] for f in schema.get("fields", [])}
        return {
            "namespace": ns, "table": name, "layer": ns,
            "rows": int(summary.get("total-records", 0) or 0),
            "data_files": int(summary.get("total-data-files", 0) or 0),
            "bytes": int(summary.get("total-files-size", 0) or 0),
            "snapshots": len(snaps),
            "current_snapshot_id": current_id,
            "last_updated_ms": meta.get("last-updated-ms"),
            "format_version": meta.get("format-version"),
            "columns": len(schema.get("fields", [])),
            "schema_versions": len(meta.get("schemas", [])),
            "partition_specs": len(meta.get("partition-specs", [])),
            "partitioning": [f"{p['transform']}({fields_by_id.get(p['source-id'], p['source-id'])})"
                             for p in spec.get("fields", [])],
            "comment": meta.get("properties", {}).get("comment"),
            "location": meta.get("location"),
        }

    def tables(self) -> list[dict]:
        def load():
            out = []
            for t in self.catalog_tables():
                try:
                    out.append(self.summarize(t["namespace"], t["table"], self.table_metadata(t["namespace"], t["table"])))
                except requests.HTTPError:
                    continue
            order = {layer: i for i, layer in enumerate(LAYERS)}
            return sorted(out, key=lambda r: (order.get(r["layer"], 99), r["table"]))
        return self.cached("tables", load)

    # ------------------------------------------------------------ overview
    def overview(self) -> dict:
        def load():
            tables = self.tables()
            layers = {}
            for t in tables:
                agg = layers.setdefault(t["layer"], {"layer": t["layer"], "tables": 0, "rows": 0, "bytes": 0,
                                                     "data_files": 0, "snapshots": 0})
                for k in ("rows", "bytes", "data_files", "snapshots"):
                    agg[k] += t[k]
                agg["tables"] += 1
            commits = []
            for t in tables:
                if t["layer"] not in ("bronze", "silver", "gold", "quarantine"):
                    continue
                meta = self.table_metadata(t["namespace"], t["table"])["metadata"]
                for s in meta.get("snapshots", []) or []:
                    summ = s.get("summary", {})
                    commits.append({
                        "layer": t["layer"], "table": t["table"], "snapshot_id": s["snapshot-id"],
                        "ts": s["timestamp-ms"], "operation": summ.get("operation"),
                        "added_records": int(summ.get("added-records", 0) or 0),
                        "deleted_records": int(summ.get("deleted-records", 0) or 0),
                        "total_records": int(summ.get("total-records", 0) or 0),
                    })
            batches = self.safe_rows("SELECT batch_id, label, created_at, status, file_count, total_records, total_bytes, "
                                     "duplicate_files, rejected_files FROM lake.ops.ingestion_batches ORDER BY created_at")
            kpis = self.safe_rows("""
                SELECT count(*) AS orders,
                       round(sum(CASE WHEN NOT is_canceled THEN order_value ELSE 0 END), 2) AS revenue,
                       round(avg(review_score), 2) AS avg_review,
                       round(100 * avg(is_late::INT), 1) AS late_pct,
                       min(order_date) AS first_order, max(order_date) AS last_order
                FROM lake.gold.fact_orders""")
            quality = self.safe_rows("""
                SELECT sum(total_records) AS checked, sum(invalid_records) AS quarantined,
                       sum(duplicate_records) AS duplicates
                FROM lake.ops.quality_reports WHERE layer = 'silver'""")
            runs = self.safe_rows("""
                SELECT stage, run_id, min(started_at) AS started_at, round(sum(duration_s), 1) AS seconds,
                       sum(input_records) AS input_records, sum(output_records) AS output_records,
                       sum(quarantined_records) AS quarantined, max(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                       string_agg(DISTINCT source_batch_ids, ',') AS batches
                FROM lake.ops.pipeline_runs GROUP BY stage, run_id ORDER BY started_at""")
            return {"layers": list(layers.values()), "commits": commits, "batches": batches,
                    "kpis": kpis[0] if kpis else {}, "quality": quality[0] if quality else {}, "runs": runs,
                    "bootstrap": self.read_report("bootstrap"),
                    "tables_total": len(tables),
                    "snapshots_total": sum(t["snapshots"] for t in tables)}
        return self.cached("overview", load)

    def safe_rows(self, sql: str) -> list[dict]:
        try:
            return self.rows(sql)
        except Exception:
            return []

    # ------------------------------------------------------------ pipeline
    def pipeline(self) -> dict:
        def load():
            runs = self.safe_rows("""
                SELECT run_id, stage, dataset, target_table, source_batch_ids, input_records, output_records,
                       quarantined_records, status, error, started_at, duration_s, snapshot_id, details
                FROM lake.ops.pipeline_runs ORDER BY started_at DESC LIMIT 400""")
            for r in runs:
                try:
                    r["details"] = json.loads(r["details"]) if r.get("details") else {}
                    r["details"].pop("traceback", None)
                except (TypeError, ValueError):
                    pass
            batches = self.safe_rows("SELECT * FROM lake.ops.ingestion_batches ORDER BY created_at")
            for b in batches:
                try:
                    b["files"] = json.loads(b.pop("files_json") or "[]")
                except ValueError:
                    b["files"] = []
            watermarks = self.safe_rows("SELECT * FROM lake.ops.watermarks ORDER BY pipeline, dataset")
            maintenance = self.safe_rows("SELECT table_name, operation, metrics, started_at, duration_s "
                                         "FROM lake.ops.maintenance_runs ORDER BY started_at DESC LIMIT 100")
            for m in maintenance:
                try:
                    m["metrics"] = json.loads(m["metrics"])
                except (TypeError, ValueError):
                    pass
            return {"runs": runs, "batches": batches, "watermarks": watermarks, "maintenance": maintenance}
        return self.cached("pipeline", load)

    # ------------------------------------------------------------- quality
    def quality(self) -> dict:
        def load():
            reports = self.safe_rows("""
                SELECT run_id, dataset, layer, batch_ids, total_records, valid_records, invalid_records,
                       duplicate_records, quality_score, status, failed_rules, executed_at
                FROM lake.ops.quality_reports ORDER BY executed_at""")
            rules = self.safe_rows("""
                SELECT dataset, rule, rule_type, dimension, severity, column_name,
                       sum(checked) AS checked, sum(failed) AS failed
                FROM lake.ops.quality_rule_results GROUP BY ALL ORDER BY dataset, failed DESC, rule""")
            dims = self.safe_rows("""
                SELECT dimension, sum(checked) AS checked, sum(failed) AS failed,
                       count(DISTINCT rule) AS rules
                FROM lake.ops.quality_rule_results GROUP BY 1 ORDER BY 1""")
            changes = self.safe_rows("SELECT * FROM lake.ops.schema_changes ORDER BY detected_at")
            quarantine = []
            for t in self.tables():
                if t["layer"] != "quarantine":
                    continue
                top = self.safe_rows(f"""
                    SELECT rule, count(*) AS n FROM (
                        SELECT unnest(_failed_rules) AS rule FROM lake.quarantine.{t['table']})
                    GROUP BY 1 ORDER BY 2 DESC""")
                sample = self.safe_rows(f"SELECT * EXCLUDE (_record_hash, _source_file) FROM lake.quarantine.{t['table']} "
                                        f"ORDER BY _quarantined_at DESC LIMIT 6")
                quarantine.append({"table": t["table"], "rows": t["rows"], "rules": top, "sample": sample})
            return {"reports": reports, "rules": rules, "dimensions": dims, "schema_changes": changes,
                    "quarantine": quarantine}
        return self.cached("quality", load)

    # ------------------------------------------------------------ explorer
    def table_detail(self, ns: str, name: str) -> dict:
        def load():
            payload = self.table_metadata(ns, name)
            meta = payload["metadata"]
            schemas = meta.get("schemas", [])
            evolution = []
            prev = None
            for s in schemas:
                fields = {f["id"]: f for f in s.get("fields", [])}
                if prev is not None:
                    changes = []
                    for fid, f in fields.items():
                        if fid not in prev:
                            changes.append({"change": "added", "column": f["name"], "type": str(f["type"]), "field_id": fid})
                        else:
                            if prev[fid]["name"] != f["name"]:
                                changes.append({"change": "renamed", "column": f["name"], "from": prev[fid]["name"],
                                                "field_id": fid})
                            if str(prev[fid]["type"]) != str(f["type"]):
                                changes.append({"change": "type", "column": f["name"], "from": str(prev[fid]["type"]),
                                                "type": str(f["type"]), "field_id": fid})
                    for fid, f in prev.items():
                        if fid not in fields:
                            changes.append({"change": "dropped", "column": f["name"], "field_id": fid})
                    order_prev = [i for i in prev if i in fields]
                    order_now = [i for i in fields if i in prev]
                    if order_prev != order_now and not changes:
                        changes.append({"change": "reordered", "column": "", "field_id": None})
                    evolution.append({"schema_id": s["schema-id"], "changes": changes})
                prev = fields
            current_schema = next((s for s in schemas if s["schema-id"] == meta.get("current-schema-id")), schemas[-1])
            snaps = []
            for s in meta.get("snapshots", []) or []:
                summ = s.get("summary", {})
                snaps.append({"snapshot_id": s["snapshot-id"], "parent_id": s.get("parent-snapshot-id"),
                              "ts": s["timestamp-ms"], "operation": summ.get("operation"),
                              "schema_id": s.get("schema-id"), "summary": summ})
            spec_fields = {f["id"]: f["name"] for f in current_schema.get("fields", [])}
            for sch in schemas:
                for f in sch.get("fields", []):
                    spec_fields.setdefault(f["id"], f["name"])
            specs = [{"spec_id": sp["spec-id"], "fields": [f"{p['transform']}({spec_fields.get(p['source-id'], p['source-id'])})"
                                                              for p in sp.get("fields", [])]}
                     for sp in meta.get("partition-specs", [])]
            sample = self.safe_rows(f'SELECT * FROM lake."{ns}"."{name}" LIMIT 12')
            return {
                **self.summarize(ns, name, payload),
                "metadata_location": payload.get("metadata-location"),
                "schema": [{"id": f["id"], "name": f["name"], "type": str(f["type"]) if not isinstance(f["type"], dict)
                            else f["type"].get("type"), "required": f.get("required", False), "doc": f.get("doc")}
                           for f in current_schema.get("fields", [])],
                "schema_evolution": evolution,
                "snapshots": snaps,
                "refs": meta.get("refs", {}),
                "specs": specs,
                "default_spec_id": meta.get("default-spec-id"),
                "properties": meta.get("properties", {}),
                "sample": sample,
            }
        return self.cached(f"detail:{ns}.{name}", load)

    def snapshot_preview(self, ns: str, name: str, snapshot_id: int) -> dict:
        def load():
            base = f'lake."{ns}"."{name}" AT (VERSION => {int(snapshot_id)})'
            count = self.rows(f"SELECT count(*) AS n FROM {base}")[0]["n"]
            sample = self.rows(f"SELECT * FROM {base} LIMIT 8")
            return {"snapshot_id": snapshot_id, "rows": count, "sample": sample,
                    "sql": f"SELECT count(*) FROM lake.{ns}.{name} AT (VERSION => {snapshot_id});"}
        return self.cached(f"snap:{ns}.{name}.{snapshot_id}", load, ttl=3600)

    # ----------------------------------------------------------- analytics
    def analytics(self) -> dict:
        def load():
            q = self.safe_rows
            return {
                "monthly": q("""SELECT strftime(order_date, '%Y-%m') AS month, sum(orders) AS orders,
                                round(sum(revenue), 2) AS revenue, round(sum(revenue) / nullif(sum(orders - canceled_orders), 0), 2) AS aov,
                                round(avg(late_delivery_rate), 4) AS late_rate
                              FROM lake.gold.daily_revenue GROUP BY 1 ORDER BY 1"""),
                "daily": q("SELECT order_date, orders, revenue FROM lake.gold.daily_revenue ORDER BY order_date"),
                "categories": q("""SELECT category, sum(orders) AS orders, round(sum(revenue), 2) AS revenue,
                                   round(avg(avg_review_score), 2) AS avg_review
                                   FROM lake.gold.category_performance GROUP BY 1 ORDER BY revenue DESC LIMIT 15"""),
                "payments": q("""SELECT payment_type, sum(transactions) AS transactions, round(sum(total_value), 2) AS value,
                                 round(avg(avg_installments), 2) AS avg_installments
                                 FROM lake.gold.payment_mix GROUP BY 1 ORDER BY value DESC"""),
                "states": q("""SELECT state, region, orders, revenue, avg_delivery_days, late_delivery_rate, avg_review_score
                               FROM lake.gold.state_performance WHERE state IS NOT NULL ORDER BY orders DESC"""),
                "sellers": q("""SELECT revenue_rank, seller_id, city, state, orders, revenue, avg_review_score,
                                late_delivery_rate, avg_delivery_days
                                FROM lake.gold.seller_performance ORDER BY revenue_rank LIMIT 12"""),
                "review_vs_delay": q("""SELECT CASE WHEN delay_days IS NULL THEN 'not delivered'
                                             WHEN delay_days <= -10 THEN '10+ days early'
                                             WHEN delay_days <= -3 THEN '3-10 days early'
                                             WHEN delay_days <= 0 THEN 'on time'
                                             WHEN delay_days <= 5 THEN '1-5 days late'
                                             ELSE '5+ days late' END AS bucket,
                                        count(*) AS orders, round(avg(review_score), 2) AS avg_review
                                        FROM lake.gold.fact_orders WHERE review_score IS NOT NULL
                                        GROUP BY 1"""),
                "priority": q("""SELECT coalesce(delivery_priority, 'before the column existed') AS priority,
                                 count(*) AS orders, round(avg(delivery_days), 2) AS avg_delivery_days
                                 FROM lake.gold.fact_orders GROUP BY 1 ORDER BY orders DESC"""),
            }
        return self.cached("analytics", load, ttl=60)

    # --------------------------------------------------------------- files
    def read_report(self, name: str) -> dict | None:
        path = self.reports_dir / f"{name}.json"
        return json.loads(path.read_text()) if path.exists() else None

    def lab(self) -> dict | None:
        return self.read_report("iceberg_showcase")

    def benchmarks(self) -> dict | None:
        return json.loads(self.bench_file.read_text()) if self.bench_file.exists() else None

    # ----------------------------------------------------------------- sql
    def examples(self) -> list[dict]:
        out = []
        for path in sorted(QUERIES_DIR.glob("*.sql")):
            text = path.read_text(encoding="utf-8")
            title = text.splitlines()[0].lstrip("- ").strip() if text.startswith("--") else path.stem
            out.append({"id": path.stem, "title": title, "sql": text.strip()})
        return out

    def run_sql(self, sql: str, limit: int = 500) -> dict:
        assert_read_only(sql)
        t0 = time.perf_counter()
        with self._lock:
            cur = self.lq.con.execute(sql.strip().rstrip(";"))
            cols = [d[0] for d in cur.description]
            data = cur.fetchmany(limit + 1)
        elapsed = round((time.perf_counter() - t0) * 1000, 1)
        return {"columns": cols, "rows": [list(r) for r in data[:limit]], "truncated": len(data) > limit,
                "ms": elapsed}

"""Render benchmark results (``benchmarks/results/latest.json``) as Markdown."""

from __future__ import annotations

import json
import sys
from pathlib import Path

LAYER_NAMES = {"raw_csv": "Raw CSV", "raw_parquet": "Raw Parquet", "silver_iceberg": "Silver (Iceberg)",
               "gold_iceberg": "Gold (Iceberg)"}


def _mb(b) -> str:
    return f"{(b or 0) / 1_048_576:,.1f} MB"


def write_markdown(payload: dict, path: Path) -> Path:
    env = payload["environment"]
    lines = [
        "# Benchmarks",
        "",
        f"_Generated {payload['generated_at']} by `openlake benchmark` — numbers are from a real run, "
        "not hand-edited._",
        "",
        "## Environment",
        "",
        f"- Host CPUs visible to the container: **{env['host_cpus']}**",
        f"- Spark {env['spark']} (`{env['spark_master']}`, executor memory {env['executor_memory']}), "
        f"Iceberg {env['iceberg']}, DuckDB {env['duckdb']}, Python {env['python']}",
        f"- Platform: `{env['platform']}`",
        "",
        "## Methodology",
        "",
        payload["methodology"],
        "",
        "## Query latency (median of "
        f"{payload['repeats']} runs after a warm-up)",
        "",
    ]
    by_query: dict[str, list[dict]] = {}
    for r in payload["queries"]:
        by_query.setdefault(r["query"], []).append(r)
    for _qid, rows in by_query.items():
        lines += [f"### {rows[0]['title']}", "",
                  "| Layer | Engine | Median | Eligible files | Eligible bytes | Rows in inputs |",
                  "|---|---|---:|---:|---:|---:|"]
        for r in rows:
            rows_in = f"{r['eligible_rows']:,}" if r.get("eligible_rows") is not None else "—"
            lines.append(f"| {LAYER_NAMES[r['layer']]} | {r['engine']} | {r['median_ms']:,.0f} ms | "
                         f"{r['eligible_files']:,} | {_mb(r['eligible_bytes'])} | {rows_in} |")
        duck = {r["layer"]: r["median_ms"] for r in rows if r["engine"] == "duckdb"}
        if duck.get("raw_csv") and duck.get("gold_iceberg"):
            lines += ["", f"Gold vs raw CSV (DuckDB): **{duck['raw_csv'] / max(duck['gold_iceberg'], 0.1):,.1f}× faster**; "
                          f"Silver vs raw CSV: **{duck['raw_csv'] / max(duck['silver_iceberg'], 0.1):,.1f}×**."]
        lines.append("")

    lines += ["## Storage footprint", "", "| Dataset | Raw CSV | Raw Parquet (zstd) | Silver Iceberg (zstd) | Silver files | Silver rows |",
              "|---|---:|---:|---:|---:|---:|"]
    for s in payload["storage"]:
        lines.append(f"| {s['dataset']} | {_mb(s['raw_csv_bytes'])} | {_mb(s['raw_parquet_bytes'])} | "
                     f"{_mb(s['silver_iceberg_bytes'])} | {s['silver_iceberg_files']} | {s['silver_rows']:,} |")
    comp = payload.get("compaction")
    if comp:
        lines += ["", "## Small files and compaction", "",
                  "40 micro-batch appends into `lab.payments_small_files`, then `rewrite_data_files` + "
                  "`rewrite_manifests` + `expire_snapshots(retain_last => 3)` (from the Iceberg showcase):", "",
                  "| | Before | After |", "|---|---:|---:|",
                  f"| Data files | {comp['files_before']} | {comp['files_after']} |",
                  f"| Manifests | {comp['manifests_before']} | {comp['manifests_after']} |",
                  f"| Snapshots | {comp['snapshots_before']} | {comp['snapshots_after']} |",
                  f"| Aggregation query (Spark, median) | {comp['query_ms_before']:,.0f} ms | {comp['query_ms_after']:,.0f} ms |"]
    lines += ["", "## Pipeline stage durations", "", "| Stage | Run | Seconds | Input records | Output records | Quarantined | Records/s |",
              "|---|---|---:|---:|---:|---:|---:|"]
    for p in payload["pipeline"]:
        lines.append(f"| {p['stage']} | `{p['run_id']}` | {p['seconds']} | {p['input_records'] or 0:,} | "
                     f"{p['output_records'] or 0:,} | {p['quarantined_records'] or 0:,} | {p['records_per_second'] or 0:,} |")
    if payload.get("ingestion"):
        lines += ["", "## Ingestion throughput (validate + checksum + upload to MinIO)", "",
                  "| File | Format | Size | Records | MB/s | Records/s |", "|---|---|---:|---:|---:|---:|"]
        for i in payload["ingestion"]:
            lines.append(f"| {i['file']} | {i['format']} | {i['mb']} MB | {i['records'] or 0:,} | {i['mb_per_s']} | "
                         f"{i['records_per_s'] or 0:,} |")
    lines += ["", "## Reading the numbers", "",
              "- **Raw CSV** pays for parsing text and type inference on every query, and has no statistics, so "
              "even a one-day filter reads every byte.",
              "- **Raw Parquet** removes parsing and reads only the needed columns, but it is just files: no "
              "snapshots, no schema evolution, no safe concurrent writes.",
              "- **Silver (Iceberg)** adds table semantics on top of Parquet. Hidden partitioning by month lets "
              "the one-day query open only that month's files (see *Eligible files*).",
              "- **Gold (Iceberg)** answers business questions from pre-modelled, pre-aggregated tables — the "
              "biggest win is moving work from query time to pipeline time.",
              "- **Storage:** Silver is larger than the raw Parquet copy because every row carries lineage columns "
              "(`_source_batch_id`, `_source_file`, `_ingested_at`, `_silver_updated_at`, and a 64-character sha256 "
              "`_record_hash` that does not compress) and `orders` is split into monthly partition files. That is the "
              "price of traceability and change detection; dropping the hash would bring it close to raw Parquet.",
              "- Spark has a fixed per-query scheduling overhead; on data of this size a single-process engine "
              "(DuckDB) is faster. Spark's advantage is distributed writes and transformations at larger scale.",
              ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


if __name__ == "__main__":
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results/latest.json")
    dst = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/benchmarks.md")
    print(write_markdown(json.loads(src.read_text()), dst))

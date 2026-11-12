# Benchmarks

_Generated 2026-11-12T00:56:59+00:00 by `openlake benchmark` — numbers are from a real run, not hand-edited._

## Environment

- Host CPUs visible to the container: **4**
- Spark 4.0.1 (`spark://spark-master:7077`, executor memory 2g), Iceberg 1.10.0, DuckDB 1.4.1, Python 3.10.12
- Platform: `Linux-6.18.58.1-microsoft-standard-WSL2-x86_64-with-glibc2.35`

## Methodology

* Same hardware, same object store, same query *logic* per layer; the SQL is
  adapted only to each layer's schema (raw files have Olist names and
  untyped columns, Gold is pre-modelled).
* Raw CSV and raw Parquet contain the historical batch only; Silver/Gold also
  contain the two incremental batches (~4% more orders). Row counts are
  reported next to every timing so the difference is visible.
* Each query runs once to warm up, then ``repeats`` times; the median is kept.
* DuckDB is the primary engine (single process, no cluster); Spark timings on
  the Iceberg layers are reported alongside for reference.
* "Eligible files/bytes" for Iceberg come from table metadata *after* partition
  pruning for the query's predicate — this is the data the engine has to open.

## Query latency (median of 5 runs after a warm-up)

### Monthly revenue (join orders x items, aggregate)

| Layer | Engine | Median | Eligible files | Eligible bytes | Rows in inputs |
|---|---|---:|---:|---:|---:|
| Raw CSV | duckdb | 270 ms | 2 | 31.5 MB | — |
| Raw Parquet | duckdb | 50 ms | 2 | 9.4 MB | — |
| Silver (Iceberg) | duckdb | 100 ms | 22 | 17.8 MB | 226,052 |
| Gold (Iceberg) | duckdb | 20 ms | 21 | 0.1 MB | 636 |
| Silver (Iceberg) | spark | 678 ms | 22 | 17.8 MB | 226,052 |
| Gold (Iceberg) | spark | 266 ms | 21 | 0.1 MB | 636 |

Gold vs raw CSV (DuckDB): **13.8× faster**; Silver vs raw CSV: **2.7×**.

### Top 10 categories by revenue (3-way join)

| Layer | Engine | Median | Eligible files | Eligible bytes | Rows in inputs |
|---|---|---:|---:|---:|---:|
| Raw CSV | duckdb | 200 ms | 3 | 17.2 MB | — |
| Raw Parquet | duckdb | 35 ms | 3 | 4.4 MB | — |
| Silver (Iceberg) | duckdb | 64 ms | 3 | 10.1 MB | 155,114 |
| Gold (Iceberg) | duckdb | 39 ms | 23 | 6.9 MB | 155,045 |
| Silver (Iceberg) | spark | 391 ms | 3 | 10.1 MB | 155,114 |
| Gold (Iceberg) | spark | 541 ms | 23 | 6.9 MB | 155,045 |

Gold vs raw CSV (DuckDB): **5.1× faster**; Silver vs raw CSV: **3.1×**.

### Orders on one day (2018-03-15) — selective filter

| Layer | Engine | Median | Eligible files | Eligible bytes | Rows in inputs |
|---|---|---:|---:|---:|---:|
| Raw CSV | duckdb | 175 ms | 1 | 16.6 MB | — |
| Raw Parquet | duckdb | 12 ms | 1 | 5.8 MB | — |
| Silver (Iceberg) | duckdb | 17 ms | 1 | 0.6 MB | 6,369 |
| Gold (Iceberg) | duckdb | 15 ms | 1 | 0.0 MB | 31 |
| Silver (Iceberg) | spark | 104 ms | 1 | 0.6 MB | 6,369 |
| Gold (Iceberg) | spark | 83 ms | 1 | 0.0 MB | 31 |

Gold vs raw CSV (DuckDB): **11.5× faster**; Silver vs raw CSV: **10.1×**.

## Storage footprint

| Dataset | Raw CSV | Raw Parquet (zstd) | Silver Iceberg (zstd) | Silver files | Silver rows |
|---|---:|---:|---:|---:|---:|
| orders | 16.6 MB | 5.8 MB | 9.6 MB | 21 | 103,970 |
| order_items | 14.9 MB | 3.6 MB | 8.2 MB | 1 | 122,082 |

## Small files and compaction

40 micro-batch appends into `lab.payments_small_files`, then `rewrite_data_files` + `rewrite_manifests` + `expire_snapshots(retain_last => 3)` (from the Iceberg showcase):

| | Before | After |
|---|---:|---:|
| Data files | 80 | 1 |
| Manifests | 40 | 1 |
| Snapshots | 42 | 3 |
| Aggregation query (Spark, median) | 648 ms | 152 ms |

## Pipeline stage durations

| Stage | Run | Seconds | Input records | Output records | Quarantined | Records/s |
|---|---|---:|---:|---:|---:|---:|
| bronze | `bronze_20261112T004934_daab5e` | 34.6 | 549,933 | 549,933 | 0 | 15,894 |
| silver | `silver_20261112T005011_ad4bba` | 79.6 | 549,933 | 549,651 | 132 | 6,909 |
| gold | `gold_20261112T005132_627e92` | 39.0 | 461,841 | 461,841 | 0 | 11,842 |
| bronze | `bronze_20261112T005217_b97b38` | 4.6 | 20,738 | 20,738 | 0 | 4,508 |
| silver | `silver_20261112T005223_7f9727` | 31.5 | 20,738 | 20,738 | 0 | 658 |
| gold | `gold_20261112T005256_127f13` | 33.5 | 31,403 | 31,403 | 0 | 937 |
| bronze | `bronze_20261112T005335_52f04f` | 4.8 | 12,916 | 12,916 | 0 | 2,691 |
| silver | `silver_20261112T005342_2191f3` | 29.2 | 12,916 | 12,891 | 25 | 442 |
| gold | `gold_20261112T005412_c7262c` | 22.5 | 18,716 | 18,716 | 0 | 832 |

## Ingestion throughput (validate + checksum + upload to MinIO)

| File | Format | Size | Records | MB/s | Records/s |
|---|---|---:|---:|---:|---:|
| olist_customers_dataset.csv | csv | 8.19 MB | 100,000 | 10.0 | 122,168 |
| olist_order_items_dataset.csv | csv | 14.92 MB | 117,399 | 13.9 | 109,459 |
| olist_order_payments_dataset.parquet | parquet | 3.61 MB | 104,300 | 10.1 | 290,938 |
| olist_order_reviews_dataset.json | json | 25.48 MB | 91,952 | 12.1 | 43,558 |
| olist_orders_dataset.csv | csv | 16.6 MB | 100,150 | 13.9 | 83,882 |
| olist_products_dataset.csv | csv | 2.24 MB | 33,000 | 8.5 | 125,071 |
| olist_sellers_dataset.csv | csv | 0.16 MB | 3,100 | 3.9 | 77,394 |
| product_category_name_translation.csv | csv | 0.0 MB | 32 | 0.0 | 1,333 |

## Reading the numbers

- **Raw CSV** pays for parsing text and type inference on every query, and has no statistics, so even a one-day filter reads every byte.
- **Raw Parquet** removes parsing and reads only the needed columns, but it is just files: no snapshots, no schema evolution, no safe concurrent writes.
- **Silver (Iceberg)** adds table semantics on top of Parquet. Hidden partitioning by month lets the one-day query open only that month's files (see *Eligible files*).
- **Gold (Iceberg)** answers business questions from pre-modelled, pre-aggregated tables — the biggest win is moving work from query time to pipeline time.
- **Storage:** Silver is larger than the raw Parquet copy because every row carries lineage columns (`_source_batch_id`, `_source_file`, `_ingested_at`, `_silver_updated_at`, and a 64-character sha256 `_record_hash` that does not compress) and `orders` is split into monthly partition files. That is the price of traceability and change detection; dropping the hash would bring it close to raw Parquet.
- Spark has a fixed per-query scheduling overhead; on data of this size a single-process engine (DuckDB) is faster. Spark's advantage is distributed writes and transformations at larger scale.

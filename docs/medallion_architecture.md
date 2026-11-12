# Medallion layers

## Landing zone (`raw/`)

The ingestion service (`openlake/ingestion`) never transforms data. For every
candidate file it:

1. picks a format adapter by extension (CSV, NDJSON, Parquet)
2. validates structure — readable, non-empty, has columns, no duplicate column
   names, not mostly ragged rows
3. computes metadata — size, sha256, record count, column list
4. maps the file to an entity using `file_pattern` from the config
5. checks the sha256 against the checksum index; identical files are recorded
   as `DUPLICATE` and not copied again
6. copies the file to `raw/<source>/<entity>/ingest_date=…/batch_id=…/`
7. writes a batch manifest with a status per file (`REGISTERED`, `DUPLICATE`,
   `REJECTED`), which is what Bronze picks up

## Bronze — raw, queryable, append-only

`openlake/pipeline/bronze.py`

* Every source column is stored as `STRING` under its original name.
* Lineage columns: `_batch_id`, `_source_file`, `_ingested_at`,
  `_source_format`, `_source_columns` (the file's header as JSON) and
  `_record_hash` (sha256 of the raw values).
* New upstream columns widen the table automatically. Bronze is permissive by
  design: no business rule may drop a record here.
* Partitioned by `days(_ingested_at)`; append-only, which is what allows Silver
  to read it with Iceberg incremental scans.

## Silver — cleansed, conformed, current

`openlake/pipeline/silver.py`, per entity in dependency order:

| Step | What happens | Where |
|---|---|---|
| Incremental read | Iceberg incremental append scan between the last consumed Bronze snapshot and the current one | `pipeline/incremental.py` |
| Schema contract | each distinct file layout is classified (added / missing optional / missing required column) and the policy applied | `iceberg/schema_evolution.py` |
| Standardize names | explicit renames from the contract (Olist's `product_name_lenght` → `product_name_length`), snake_case otherwise | `transformations/standardization.py` |
| Clean | trim, empty strings and `null`/`NaN` tokens → NULL | `transformations/cleaning.py` |
| Standardize values | lower/upper/title case, snake-case categories, 5-digit zip prefixes | `transformations/standardization.py` |
| Type | cast to contract types, keeping the raw text for every typed column | `transformations/cleaning.py` |
| Validate | rules compiled to SQL, one pass; error failures → quarantine with reasons | `quality/` |
| Deduplicate | one row per business key, latest `_ingested_at` wins | `transformations/deduplication.py` |
| Upsert | `MERGE INTO` on business keys; matched rows rewritten only if the record hash changed and the incoming image is not older | `pipeline/writer.py` |
| Integrity | referential checks against already-processed Silver tables (warn level) | `quality/validator.py` |
| Bookkeeping | quality report, schema changes, run metrics, new watermark | `pipeline/ops.py` |

Partitioning follows query patterns: `orders` by `months(order_purchase_timestamp)`;
small dimensions and line-item tables are unpartitioned (≈100k rows fit in one
file — partitioning them would only create small files).

## Gold — business models

`openlake/pipeline/gold.py`, `transformations/dimensional_modeling.py`, `transformations/aggregations.py`

Star schema:

| Table | Grain | Notes |
|---|---|---|
| `fact_orders` | order | value, freight, payments, delivery days, delay vs estimate, `is_late`, review, `delivery_priority` |
| `fact_order_items` | order item | price and freight per product × seller |
| `fact_payments` | payment | method, installments, value |
| `dim_customers` | customer | city, state, region (SCD1) |
| `dim_sellers` | seller | city, state, region (SCD1) |
| `dim_products` | product | English category, dimensions, weight class (SCD1) |
| `dim_date` | day | calendar attributes |

Surrogate keys are `xxhash64(business_key)`: deterministic, so a fact row can be
recomputed incrementally without looking up the dimension.

Marts: `daily_revenue` (partitioned by month), `category_performance`,
`seller_performance`, `state_performance`, `payment_mix`.

### Incremental Gold

1. Silver rows changed since the Gold watermark (`_silver_updated_at`) are found.
2. **Affected orders** = every order whose header, items, payments or reviews changed.
3. Facts are recomputed for affected orders only and MERGEd by grain key.
4. `daily_revenue` is recomputed only for the months containing affected orders
   and swapped in with a **dynamic partition overwrite** — one atomic commit,
   other months untouched.
5. Small marts are rebuilt from Gold and replaced with one atomic overwrite.

Dimension changes (SCD1) are merged by key. Because the fact table also carries a
denormalised `product_category`, a category rename would require refreshing
affected facts; that trade-off is accepted for query convenience.

## Quarantine

`quarantine.<entity>` keeps every rejected record with its original text values,
lineage columns, `_failed_rules` (array of rule names), `_quarantined_at` and
`_run_id`. A corrected record arriving later is upserted into Silver normally —
in the demo, a few orders quarantined in batch 1 (null customer, invalid status)
are healed by their clean versions in batch 2.

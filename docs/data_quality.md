# Data quality and observability

## Rules are configuration

Rules live next to the data contract in `config/pipeline_config.yaml`:

```yaml
orders:
  rules:
    - {name: order_status_valid, type: accepted_values, column: order_status,
       values: [created, approved, invoiced, processing, shipped, delivered, canceled, unavailable], severity: error}
    - {name: purchase_ts_not_in_future, type: not_future, column: order_purchase_timestamp, severity: error}
    - {name: delivered_after_purchase, type: ordered, columns: [order_purchase_timestamp, order_delivered_customer_date], severity: warn}
    - {name: order_customer_exists, type: foreign_key, column: customer_id, ref_table: customers, ref_column: customer_id, severity: warn}
```

The config is validated when it is loaded (unknown rule types, keys missing
from the contract, foreign keys to unknown entities fail immediately).

| Rule type | Dimension | Level | Compiles to |
|---|---|---|---|
| `not_null` | completeness | row | `col IS NOT NULL` |
| `range` | validity | row | `col IS NULL OR (col >= min AND col <= max)` |
| `accepted_values` | validity | row | `col IS NULL OR col IN (…)` |
| `regex` | validity | row | `col IS NULL OR col RLIKE '…'` |
| `not_future` | timeliness | row | `col IS NULL OR col <= current_timestamp() + INTERVAL 1 DAY` |
| `ordered` | consistency | row | `a IS NULL OR b IS NULL OR a <= b` |
| `type:<col>` (implicit) | schema | row | raw value present but cast to the contract type failed |
| `schema_contract` (implicit) | schema | row | the record's file was rejected by the evolution policy |
| `unique` | uniqueness | dataset | surplus rows per key before deduplication |
| `foreign_key` | consistency | dataset | anti-join against the referenced Silver table |

Only `not_null` fails on NULL, so a missing optional value is never counted twice.
The compiler is plain Python (`openlake/quality/rules.py`); its output is
evaluated by Spark in the pipeline and by DuckDB in the unit tests.

## Severity

* **error** — the record goes to `quarantine.<entity>` with `_failed_rules`.
* **warn** — counted in the report; the record continues to Silver.

All row-level rules are evaluated in **one pass** over the increment (boolean
flag columns + one aggregation), not one job per rule.

## Reports

Every Silver run writes, per dataset:

* `ops.quality_reports` — total, valid, invalid, duplicate records, score,
  status (`PASSED`, `PASSED_WITH_WARNINGS`, `FAILED_RECORDS_QUARANTINED`),
  failed rule names, null counts per column, batch ids, timestamp
* `ops.quality_rule_results` — checked / failed / pass rate per rule
* `ops.schema_changes` — every schema difference and the decision taken

## Pipeline observability

`ops.pipeline_runs` has one row per dataset per stage run: job id, source batch
ids, input/output/quarantined records, duration, status, error, target table and
the **Iceberg snapshot id the run produced** — so any table state can be traced
back to the batches that made it:

```sql
SELECT stage, dataset, source_batch_ids, output_records, snapshot_id
FROM lake.ops.pipeline_runs
WHERE target_table = 'lake.silver.orders'
ORDER BY started_at;
```

`ops.ingestion_batches` mirrors the batch manifests, `ops.watermarks` holds the
incremental read positions, `ops.maintenance_runs` the before/after metrics of
table maintenance.

## What the demo data exercises

The generator injects a known set of problems into the historical batch, listed in
`data/generated/batch_0001_initial/_generation_summary.json`:

| Entity | Injected | Expected outcome |
|---|---|---|
| customers | 40 messy state codes (`" sp "`) | fixed by standardization |
| customers | 20 invalid states (`XX`) | quarantined (`customer_state_valid`) |
| orders | 150 exact duplicate rows | removed by deduplication |
| orders | 15 unknown statuses, 10 null customers, 12 malformed timestamps | quarantined |
| orders | 20 upper-case statuses | fixed by standardization |
| order_items | 25 negative prices, 10 null product ids | quarantined |
| order_payments | 20 negative values, 5 unknown types (`pix`) | quarantined |
| order_reviews | 15 scores outside 1–5 | quarantined |
| sellers (batch 3) | `seller_state` renamed to `state` | file rejected by the contract, 25 rows quarantined |
| categories (batch 3) | file re-sent unchanged | skipped as `DUPLICATE` at ingestion |

The integration test `tests/test_incremental.py` asserts that the counts reconcile:
every Bronze record is validated exactly once, and Silver ≤ Bronze − quarantined − duplicates
(updates collapse onto existing keys).

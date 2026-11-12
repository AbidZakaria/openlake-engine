# Iceberg features

Writing Parquet into MinIO does not make a lakehouse. These are the table-format
capabilities the platform relies on, where each is used in the pipeline, and
the lab demo (`python3 -m openlake showcase`, code in
[`openlake/iceberg/showcase.py`](../openlake/iceberg/showcase.py)) that proves it.
The lab writes a structured report rendered on the dashboard's **Iceberg lab** page.

## Atomic commits and failure isolation

A write only becomes visible when the catalog swaps the table's metadata pointer.
Files written before that point are not referenced by any snapshot.

*Lab:* an 8-partition append in which partition 5 raises an error. The table's
current snapshot and row count are unchanged afterwards. Data files written by the
tasks that succeeded remain in storage but are invisible; a simulated hard crash
leaves one more. Comparing a storage listing with `<table>.all_files` finds them,
and Iceberg's `DeleteOrphanFiles` action removes them.

*Pipeline:* every stage writes through Iceberg, so a failed job leaves Silver and
Gold at their last good snapshot; the next run picks up from the watermark.

> The SQL procedure `remove_orphan_files` refuses intervals shorter than 24 hours,
> because files younger than that may belong to in-flight writes. Routine
> maintenance uses a 72-hour threshold; only the lab (no concurrent writers)
> calls the action API with a zero interval.

## Time travel

```sql
-- Spark
SELECT order_status, count(*) FROM lake.silver.orders VERSION AS OF 427026919432447512 GROUP BY 1;
SELECT count(*) FROM lake.silver.orders TIMESTAMP AS OF '2026-11-12 00:12:00';
-- DuckDB
SELECT count(*) FROM lake.silver.orders AT (VERSION => 427026919432447512);
```

*Lab:* status mix of `silver.orders` after the historical load vs now; the life
of a single order across snapshots (`shipped` → `delivered`); the same snapshot
counted by Spark and by DuckDB (they must agree).

*Dashboard:* the table explorer runs a live DuckDB time-travel query for any
snapshot you click.

## Schema evolution

Columns are identified by ID, so changes are metadata-only.

*Pipeline:* batch 2 adds `delivery_priority` to the orders feed. The Silver
contract classifies it as an additive change, the policy accepts it, and the
column is added to `silver.orders` (and flows to `gold.fact_orders`). Older rows
read it as NULL. Batch 3 renames `seller_state` to `state` in the sellers feed;
a required column is missing, so the policy rejects the file, quarantines its
rows with `schema_contract`, and — because a file is accepted or rejected as a
whole — does not add `state` either. Every decision is in `ops.schema_changes`.

*Lab:* add, rename, widen `INT → BIGINT` and `FLOAT → DOUBLE`, reorder and drop
columns on a copy of the customers table; the set of data files is identical
before and after the six changes; time travel to the first snapshot returns the
original schema.

## Snapshot management

Every table's snapshots, history and refs are queryable (`<table>.snapshots`,
`.history`, `.refs`) and listed in the dashboard. `openlake/iceberg/snapshot_manager.py`
lists snapshots, diffs two snapshots by key and summarises tables.
`expire_snapshots` runs in maintenance with `retain_last = 10` and
`max_age_days = 7` (`config/pipeline_config.yaml`), so nothing the demo or an
audit needs is expired.

## Rollback

*Lab:* tag the known-good snapshot (`before-bad-deploy`), run a buggy
`UPDATE … SET order_value = order_value * 100`, inspect, then
`CALL lake.system.rollback_to_snapshot(...)`. Revenue returns to the exact
previous value; the bad snapshot stays in history for investigation.

## Partition evolution and hidden partitioning

*Lab:* load history with `months(order_purchase_timestamp)`, replace the field
with `days(order_purchase_timestamp)`, load new data. `.files` shows two
partition specs side by side; a query filtering on `order_purchase_timestamp`
reads both without referring to any partition column.

## Write-audit-publish with branches

*Lab:* create an `audit` branch, write a new day of data (including one bad row)
to `<table>.branch_audit`, check it while readers of `main` see nothing, fix it on
the branch, then `fast_forward` main to the branch in one commit.

## Compaction and maintenance

*Lab:* 40 micro-batch appends produce dozens of small files, 40 manifests and
40 snapshots. `rewrite_data_files` + `rewrite_manifests` collapse them and the
aggregation query gets faster; `expire_snapshots(retain_last => 3)` bounds the
history. Numbers are in [benchmarks](benchmarks.md).

*Pipeline:* `python3 -m openlake optimize` runs compaction, manifest rewrite,
snapshot expiry and orphan cleanup on every managed table and logs the before /
after file counts in `ops.maintenance_runs`.

## Incremental reads

Bronze tables are append-only, so Silver reads only new data with an
incremental append scan:

```python
spark.read.format("iceberg") \
    .option("start-snapshot-id", last_consumed) \
    .option("end-snapshot-id", current) \
    .load("lake.bronze.orders")
```

Compaction commits (`replace`) are skipped by the scan. If the watermark snapshot
has been expired, the reader falls back to the `_ingested_at` high-water mark
stored with it.

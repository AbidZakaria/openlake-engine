-- Which source batches produced each Silver/Gold table state? (pipeline observability)
SELECT
    stage,
    dataset,
    target_table,
    source_batch_ids,
    input_records,
    output_records,
    quarantined_records,
    status,
    round(duration_s, 1) AS seconds,
    snapshot_id,
    started_at
FROM lake.ops.pipeline_runs
WHERE stage IN ('bronze', 'silver', 'gold')
ORDER BY started_at DESC
LIMIT 40;

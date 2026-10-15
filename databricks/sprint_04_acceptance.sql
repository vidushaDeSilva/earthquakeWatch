-- Purpose: inspect Sprint 4 counts, unique keys, lineage and remaining source work.
-- Run each statement in a SQL notebook/editor using earthquake_dev access.
-- Snapshot counts before/after a same-input Bronze recheck; all three must stay equal.
SELECT 'event_versions' AS asset, count(*) AS row_count FROM earthquake_dev.bronze.event_versions
UNION ALL SELECT 'deliveries', count(*) FROM earthquake_dev.bronze.deliveries
UNION ALL SELECT 'quarantine', count(*) FROM earthquake_dev.bronze.quarantine;

-- Every non-empty processed batch must have accepted + rejected = its sealed row count.
SELECT batch_id, partition, offset_first, offset_last, row_count,
       accepted_delivery_count, quarantined_delivery_count, distinct_source_version_count,
       bronze_insert_count, data_stage
FROM earthquake_dev.ops.batches ORDER BY landed_at, partition, offset_first;

-- EXPECT zero rows for every query below, unless explicitly labelled as pending work.
SELECT source_version_id, count(*) FROM earthquake_dev.bronze.event_versions
GROUP BY source_version_id HAVING count(*) > 1;
SELECT delivery_id, count(*) FROM earthquake_dev.bronze.deliveries
GROUP BY delivery_id HAVING count(*) > 1;
SELECT quarantine_id, count(*) FROM earthquake_dev.bronze.quarantine
GROUP BY quarantine_id HAVING count(*) > 1;
SELECT batch_id, count(*) FROM earthquake_dev.ops.batches
GROUP BY batch_id HAVING count(*) > 1;
SELECT capture_id, count(*) FROM earthquake_dev.ops.captures
GROUP BY capture_id HAVING count(*) > 1;
SELECT batch_id FROM earthquake_dev.ops.batches
WHERE data_stage IN ('BRONZE_COMMITTED','SILVER_COMMITTED')
  AND (accepted_delivery_count IS NULL OR quarantined_delivery_count IS NULL
       OR accepted_delivery_count + quarantined_delivery_count <> row_count);

SELECT d.delivery_id FROM earthquake_dev.bronze.deliveries d
LEFT JOIN earthquake_dev.bronze.event_versions v ON d.source_version_id=v.source_version_id
LEFT JOIN earthquake_dev.bronze.quarantine q ON d.delivery_id=q.delivery_id AND q.stage_name='BRONZE'
WHERE (d.disposition='ACCEPTED' AND (v.source_version_id IS NULL OR q.delivery_id IS NOT NULL))
   OR (d.disposition='QUARANTINED' AND (q.delivery_id IS NULL OR d.source_version_id IS NOT NULL))
   OR d.disposition NOT IN ('ACCEPTED','QUARANTINED');

-- Count each capture item once even when multiple Kafka offsets refer to it.
SELECT c.capture_id FROM earthquake_dev.ops.captures c
LEFT JOIN (
  SELECT d.capture_id, count(DISTINCT d.source_item_id) AS proven_items
  FROM earthquake_dev.bronze.deliveries d
  JOIN earthquake_dev.ops.batches b ON d.batch_id=b.batch_id
  WHERE b.data_stage IN ('BRONZE_COMMITTED','SILVER_COMMITTED')
  GROUP BY d.capture_id
) d ON c.capture_id=d.capture_id
WHERE c.accounted_item_count <> coalesce(d.proven_items,0)
   OR (c.state='ACCOUNTED' AND c.accounted_item_count <> c.expected_item_count);

-- Pending work is not necessarily a failure: older CAPTURED-only sources were never dispatched.
SELECT source_id, capture_id, expected_item_count, accounted_item_count, state
FROM earthquake_dev.ops.captures ORDER BY fetched_at DESC;

-- Inspect explicit quality warnings and failures, including deliberately failed drills.
SELECT evaluated_at, run_id, scope_type, scope_id, rule_id, status, observed_value, expected_value
FROM earthquake_dev.ops.check_results WHERE status <> 'PASS' ORDER BY evaluated_at DESC;

-- Most recent Bronze stage attempt should be SUCCESS after a completed recovery.
SELECT run_id, status, started_at, finished_at, error_code, metrics
FROM earthquake_dev.ops.stage_attempts WHERE stage_name='BRONZE' ORDER BY started_at DESC;

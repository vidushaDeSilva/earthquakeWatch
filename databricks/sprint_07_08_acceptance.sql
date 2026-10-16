-- Purpose: inspect Sprint 7 coverage and Sprint 8 recovery evidence without modifying data.
-- Run statements separately to see each result grid. No UPDATE/DELETE is required.
SELECT state, count(*) AS captures FROM earthquake_dev.ops.captures GROUP BY state;
SELECT capture_id,source_id,fetched_at,state,expected_item_count,accounted_item_count
FROM earthquake_dev.ops.captures WHERE state <> 'ACCOUNTED' ORDER BY fetched_at;
SELECT data_stage,count(*) AS batches FROM earthquake_dev.ops.batches GROUP BY data_stage;
SELECT reconciliation_id,checked_at,status,issues,checked_events,pending_captures,historical_dates
FROM earthquake_dev.ops.reconciliations ORDER BY checked_at DESC LIMIT 5;
SELECT kind,outcome,count(*) AS checks FROM earthquake_dev.ops.source_checks GROUP BY kind,outcome;
SELECT requested_event_id,outcome,http_status,fetched_at,capture_id
FROM earthquake_dev.ops.source_checks WHERE kind='EVENT_CHECK' ORDER BY fetched_at DESC LIMIT 30;
SELECT product_key,publication_id,previous_publication_id,pointer_generation,excluded_publication_ids
FROM earthquake_dev.ops.serving_pointer;
SELECT publication_id,state,coverage_status,blocking_issues,expected_watchlist_rows,as_of_utc,is_daily_snapshot
FROM earthquake_dev.ops.publications ORDER BY created_at DESC LIMIT 5;
SELECT * FROM earthquake_dev.ops.v_pipeline_health;
SELECT * FROM earthquake_dev.ops.recovery_snapshots ORDER BY created_at DESC LIMIT 5;
SELECT * FROM earthquake_dev.ops.maintenance_plans ORDER BY created_at DESC LIMIT 5;
SELECT kind,status,count(*) AS actions FROM earthquake_dev.ops.maintenance_actions GROUP BY kind,status;
SELECT * FROM earthquake_dev.ops.rollback_actions ORDER BY created_at DESC LIMIT 5;
SELECT * FROM earthquake_dev.ops.data_holds WHERE active=true;
-- All logical key checks below must return zero rows.
SELECT capture_id,count(*) FROM earthquake_dev.ops.captures GROUP BY capture_id HAVING count(*)<>1;
SELECT event_key,count(*) FROM earthquake_dev.silver.events_current GROUP BY event_key HAVING count(*)<>1;
SELECT request_id,count(*) FROM earthquake_dev.ops.source_checks GROUP BY request_id HAVING count(*)<>1;
SELECT publication_id,event_key,count(*) FROM earthquake_dev.gold.watchlist_snapshots
GROUP BY publication_id,event_key HAVING count(*)<>1;
-- Inspect the latest exact cleanup plan in Catalog Explorer using manifest_uri.
-- Replay verification: replace replay_demo with the replay schema returned by the notebook.
-- SELECT event_key,source_updated_at,source_version_id FROM earthquake_dev.replay_demo.events_current
-- EXCEPT SELECT event_key,source_updated_at,source_version_id FROM earthquake_dev.silver.events_current;
-- Compare observation timestamps only where the whole observation history remains retained.

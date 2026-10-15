-- Purpose: inspect Silver correctness, Gold previews and publication safety.
-- Run after setup and the existing serialized Job. Queries labeled ZERO ROWS
-- are gates. Default Gold preview deliberately leaves the serving pointer NULL.

SELECT data_stage, count(*) batches FROM earthquake_dev.ops.batches GROUP BY data_stage;
SELECT count(*) current_events, count_if(is_deleted) withdrawn,
       count_if(has_unresolved_conflict) conflicted FROM earthquake_dev.silver.events_current;
SELECT count(*) history_versions FROM earthquake_dev.silver.event_versions;

-- ZERO ROWS: unique logical identities.
SELECT source_system,alias_event_id,count(*) copies FROM earthquake_dev.silver.event_aliases
GROUP BY source_system,alias_event_id HAVING count(*)>1;
SELECT silver_version_id,count(*) copies FROM earthquake_dev.silver.event_versions
GROUP BY silver_version_id HAVING count(*)>1;
SELECT event_key,count(*) copies FROM earthquake_dev.silver.events_current
GROUP BY event_key HAVING count(*)>1;

-- ZERO ROWS: each current state references the matching retained history fact.
SELECT c.event_key FROM earthquake_dev.silver.events_current c
LEFT JOIN earthquake_dev.silver.event_versions v ON c.silver_version_id=v.silver_version_id
WHERE v.silver_version_id IS NULL OR c.event_key<>v.event_key
   OR c.source_version_id<>v.source_version_id OR c.source_updated_at<>v.source_updated_at;

-- ZERO ROWS for clean cloud acceptance; conflicts intentionally fail Silver.
SELECT event_key FROM earthquake_dev.silver.events_current WHERE has_unresolved_conflict;
SELECT batch_id,data_stage FROM earthquake_dev.ops.batches WHERE data_stage<>'SILVER_COMMITTED';

SELECT publication_id,state,as_of_utc,silver_table_version,policy_version,coverage_status,
       blocking_issues,expected_watchlist_rows,expected_window_rows,source_checks_json
FROM earthquake_dev.ops.publications ORDER BY created_at DESC LIMIT 10;

-- Preview uses candidate ID explicitly. Public views remain empty before first activation.
WITH latest AS (SELECT publication_id FROM earthquake_dev.ops.publications
    ORDER BY created_at DESC,publication_id DESC LIMIT 1)
SELECT g.source_event_id,g.event_time,g.magnitude,g.pager_alert,g.mmi,g.significance,
       g.inclusion_reasons,g.impact_label,g.quality_flags,g.source_url
FROM earthquake_dev.gold.watchlist_snapshots g JOIN latest l ON g.publication_id=l.publication_id
ORDER BY g.rank_priority,g.significance DESC NULLS LAST,g.magnitude DESC NULLS LAST,g.event_key;

-- ZERO ROWS: release identities and output counts remain unique and complete.
SELECT publication_id,event_key,count(*) copies FROM earthquake_dev.gold.watchlist_snapshots
GROUP BY publication_id,event_key HAVING count(*)>1;
SELECT publication_id,window_grain,window_start,count(*) copies FROM earthquake_dev.gold.window_snapshots
GROUP BY publication_id,window_grain,window_start HAVING count(*)>1;
SELECT publication_id,event_date,count(*) copies FROM earthquake_dev.gold.daily_history
GROUP BY publication_id,event_date HAVING count(*)>1;
SELECT p.publication_id FROM earthquake_dev.ops.publications p
LEFT JOIN (SELECT publication_id,count(*) n FROM earthquake_dev.gold.watchlist_snapshots GROUP BY publication_id) w
    ON p.publication_id=w.publication_id
LEFT JOIN (SELECT publication_id,count(*) n FROM earthquake_dev.gold.window_snapshots GROUP BY publication_id) h
    ON p.publication_id=h.publication_id
WHERE p.expected_watchlist_rows<>coalesce(w.n,0) OR p.expected_window_rows<>coalesce(h.n,0);

-- ZERO ROWS: no withdrawn, non-earthquake or out-of-window candidate is displayed.
SELECT publication_id,event_key FROM earthquake_dev.gold.watchlist_snapshots
WHERE is_deleted OR event_type<>'earthquake' OR event_time IS NULL
   OR event_time<as_of_utc-INTERVAL 30 DAYS OR event_time>=as_of_utc OR size(inclusion_reasons)=0;

SELECT * FROM earthquake_dev.ops.serving_pointer;
SELECT * FROM earthquake_dev.ops.v_pipeline_health;

-- ZERO ROWS: an active pointer must resolve to a fully checked READY release.
SELECT p.publication_id FROM earthquake_dev.ops.serving_pointer p
LEFT JOIN earthquake_dev.ops.publications u ON p.publication_id=u.publication_id
WHERE p.publication_id IS NOT NULL AND (u.publication_id IS NULL OR u.state<>'READY' OR size(u.blocking_issues)>0);

-- Keep known historical pending captures visible; do not manually change their state.
SELECT capture_id,source_id,fetched_at,state,expected_item_count,accounted_item_count
FROM earthquake_dev.ops.captures WHERE state<>'ACCOUNTED' ORDER BY fetched_at;

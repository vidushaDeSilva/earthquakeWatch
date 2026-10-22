-- Earthquake Watch dashboard v1: read-only, bounded, public-safe projections.
-- Run once after bootstrap.sql and consumer_views.sql in earthquake_dev.
-- These views contain no Volume URIs, raw payloads, credentials or error messages.

CREATE OR REPLACE VIEW earthquake_dev.ops.dashboard_run_summary AS
WITH ranked_publications AS (
  SELECT publication_id, pipeline_run_id, validation_status,
         publication_status, activated_at,
         row_number() OVER (
           PARTITION BY pipeline_run_id
           ORDER BY created_at DESC, publication_id DESC
         ) AS publication_rank
  FROM earthquake_dev.ops.publications
  WHERE product_key = 'usgs-watchlist'
), pointer AS (
  SELECT active_publication_id, pointer_generation, updated_at,
         row_number() OVER (
           ORDER BY pointer_generation DESC, updated_at DESC
         ) AS pointer_rank
  FROM earthquake_dev.ops.serving_pointer
  WHERE product_key = 'usgs-watchlist'
)
SELECT r.pipeline_run_id, r.dispatch_id, r.dispatch_attempt_no,
       r.databricks_job_id, r.databricks_run_id, r.batch_ids,
       r.processing_revision, r.run_status, r.retryable,
       r.submitted_at, r.started_at, r.finished_at,
       r.error_class, r.error_code,
       p.publication_id, p.validation_status, p.publication_status,
       p.activated_at,
       CASE WHEN p.publication_id = s.active_publication_id
            THEN true ELSE false END AS is_active_publication,
       CASE WHEN p.publication_id = s.active_publication_id
            THEN s.pointer_generation ELSE NULL END AS pointer_generation
FROM earthquake_dev.ops.pipeline_runs r
LEFT JOIN ranked_publications p
  ON r.pipeline_run_id = p.pipeline_run_id AND p.publication_rank = 1
LEFT JOIN pointer s ON s.pointer_rank = 1;

CREATE OR REPLACE VIEW earthquake_dev.ops.dashboard_stage_runs AS
SELECT pipeline_run_id, stage_name, attempt_number, stage_status,
       records_in, records_out, records_quarantined,
       output_table, output_delta_version, error_code,
       started_at, finished_at
FROM earthquake_dev.ops.stage_runs;

CREATE OR REPLACE VIEW earthquake_dev.ops.dashboard_quality_checks AS
SELECT pipeline_run_id, stage_name, rule_id, severity, check_status,
       observed_value, expected_value, checked_at
FROM earthquake_dev.ops.quality_checks;

CREATE OR REPLACE VIEW earthquake_dev.ops.dashboard_captures AS
SELECT capture_id, source_id, schedule_slot_start, fetched_at,
       source_generated_at, expected_event_count, capture_status
FROM earthquake_dev.ops.captures;

CREATE OR REPLACE VIEW earthquake_dev.ops.dashboard_landing_batches AS
SELECT landing_batch_id, capture_ids, topic, partition_ranges,
       expected_record_count, landed_record_count, landing_status,
       created_at, verified_at
FROM earthquake_dev.ops.landing_batches;

CREATE OR REPLACE VIEW earthquake_dev.ops.dashboard_current_publication AS
SELECT s.product_key, s.active_publication_id AS publication_id,
       s.pointer_generation, s.updated_at AS pointer_updated_at,
       p.pipeline_run_id, p.validation_status, p.publication_status,
       p.activated_at
FROM earthquake_dev.ops.serving_pointer s
LEFT JOIN earthquake_dev.ops.publications p
  ON p.publication_id = s.active_publication_id
 AND p.product_key = s.product_key
WHERE s.product_key = 'usgs-watchlist';

-- Permission example for a separate dashboard principal, after reviewing its identity:
-- GRANT USE CATALOG ON CATALOG earthquake_dev TO `<dashboard-principal>`;
-- GRANT USE SCHEMA ON SCHEMA earthquake_dev.ops TO `<dashboard-principal>`;
-- GRANT USE SCHEMA ON SCHEMA earthquake_dev.gold TO `<dashboard-principal>`;
-- GRANT SELECT ON VIEW earthquake_dev.ops.dashboard_run_summary TO `<dashboard-principal>`;
-- Repeat SELECT only for the other five dashboard views and two active Gold views.

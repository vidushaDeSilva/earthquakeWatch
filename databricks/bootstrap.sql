-- Phase 0 only: reviewed, one-time Unity Catalog bootstrap. No DROP, REPLACE or data deletion.
-- Run under a principal with CREATE CATALOG/SCHEMA/VOLUME/TABLE permissions.
-- Logical keys below are not Delta-enforced PK/FK constraints; pipeline MERGEs/checks enforce them.
CREATE CATALOG IF NOT EXISTS earthquake_dev;
CREATE SCHEMA IF NOT EXISTS earthquake_dev.ops;
CREATE SCHEMA IF NOT EXISTS earthquake_dev.bronze;
CREATE SCHEMA IF NOT EXISTS earthquake_dev.silver;
CREATE SCHEMA IF NOT EXISTS earthquake_dev.gold;
CREATE VOLUME IF NOT EXISTS earthquake_dev.ops.pipeline_files;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.captures (
 capture_id STRING NOT NULL, source_id STRING NOT NULL, schedule_slot_start TIMESTAMP NOT NULL,
 fetched_at TIMESTAMP NOT NULL, source_generated_at TIMESTAMP NOT NULL,
 response_sha256 STRING NOT NULL, response_bytes BIGINT NOT NULL, expected_event_count BIGINT NOT NULL,
 data_uri STRING NOT NULL, manifest_uri STRING NOT NULL, capture_status STRING NOT NULL,
 first_materialized_at TIMESTAMP NOT NULL, last_seen_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.landing_batches (
 landing_batch_id STRING NOT NULL, consumer_group STRING NOT NULL, topic STRING NOT NULL,
 capture_ids ARRAY<STRING> NOT NULL,
 partition_ranges ARRAY<STRUCT<partition_id: INT, first_offset: BIGINT, next_offset: BIGINT, record_count: BIGINT>> NOT NULL,
 data_uri STRING NOT NULL, manifest_uri STRING NOT NULL, manifest_sha256 STRING NOT NULL,
 expected_record_count BIGINT NOT NULL, landed_record_count BIGINT NOT NULL,
 landing_status STRING NOT NULL, created_at TIMESTAMP NOT NULL, verified_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.pipeline_runs (
 pipeline_run_id STRING NOT NULL, dispatch_id STRING NOT NULL, dispatch_attempt_no INT NOT NULL,
 idempotency_token STRING NOT NULL, databricks_job_id BIGINT NOT NULL, databricks_run_id BIGINT,
 batch_ids ARRAY<STRING> NOT NULL, processing_revision STRING NOT NULL, run_status STRING NOT NULL,
 retryable BOOLEAN, submitted_at TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP,
 error_class STRING, error_code STRING, error_message STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.stage_runs (
 stage_run_id STRING NOT NULL, pipeline_run_id STRING NOT NULL, stage_name STRING NOT NULL,
 attempt_number INT NOT NULL, stage_status STRING NOT NULL, input_fingerprint STRING NOT NULL,
 records_in BIGINT, records_out BIGINT, records_quarantined BIGINT, output_table STRING,
 output_delta_version BIGINT, retryable BOOLEAN, error_code STRING, started_at TIMESTAMP, finished_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.quality_checks (
 check_id STRING NOT NULL, pipeline_run_id STRING NOT NULL, stage_name STRING NOT NULL,
 rule_id STRING NOT NULL, severity STRING NOT NULL, check_status STRING NOT NULL,
 observed_value STRING, expected_value STRING, details_json STRING, checked_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.quarantine_records (
 quarantine_id STRING NOT NULL, landing_batch_id STRING, capture_id STRING,
 topic STRING, partition_id INT, offset BIGINT, usgs_event_id STRING,
 payload_sha256 STRING, payload_uri STRING, reason_code STRING NOT NULL, reason_details STRING,
 resolution_status STRING NOT NULL, first_seen_at TIMESTAMP NOT NULL, resolved_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.publications (
 publication_id STRING NOT NULL, pipeline_run_id STRING NOT NULL, product_key STRING NOT NULL,
 policy_version STRING NOT NULL, processing_revision STRING NOT NULL, silver_delta_version BIGINT NOT NULL,
 input_batch_ids ARRAY<STRING> NOT NULL, validation_status STRING NOT NULL, publication_status STRING NOT NULL,
 validation_summary STRING, created_at TIMESTAMP NOT NULL, validated_at TIMESTAMP, activated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.ops.serving_pointer (
 product_key STRING NOT NULL, active_publication_id STRING, previous_publication_id STRING,
 pointer_generation BIGINT NOT NULL, updated_at TIMESTAMP, pipeline_run_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.bronze.event_deliveries (
 topic STRING NOT NULL, partition_id INT NOT NULL, offset BIGINT NOT NULL,
 landing_batch_id STRING NOT NULL, capture_id STRING NOT NULL, schema_version STRING NOT NULL,
 usgs_event_id STRING, usgs_updated_ms BIGINT, feature_sha256 STRING NOT NULL,
 raw_payload STRING NOT NULL, source_generated_at TIMESTAMP, published_at TIMESTAMP,
 landed_at TIMESTAMP NOT NULL, ingested_at TIMESTAMP NOT NULL, processing_revision STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.silver.event_versions (
 event_version_id STRING NOT NULL, usgs_event_id STRING NOT NULL, usgs_updated_ms BIGINT NOT NULL,
 payload_sha256 STRING NOT NULL, event_time TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
 magnitude DOUBLE, place STRING, longitude DOUBLE NOT NULL, latitude DOUBLE NOT NULL,
 depth_km DOUBLE NOT NULL, significance INT, pager_alert STRING, max_mmi DOUBLE, max_cdi DOUBLE,
 felt_count INT, tsunami_flag BOOLEAN NOT NULL, event_status STRING, event_type STRING, source_url STRING,
 first_seen_at TIMESTAMP NOT NULL, last_seen_at TIMESTAMP NOT NULL, first_capture_id STRING NOT NULL,
 version_conflict BOOLEAN NOT NULL, validation_status STRING NOT NULL, processing_revision STRING NOT NULL
) USING DELTA;

CREATE VIEW IF NOT EXISTS earthquake_dev.silver.events_current AS
 SELECT * EXCEPT (current_rank) FROM (
  SELECT *, row_number() OVER (PARTITION BY usgs_event_id
   ORDER BY usgs_updated_ms DESC, last_seen_at DESC, event_version_id DESC) AS current_rank
  FROM earthquake_dev.silver.event_versions WHERE validation_status = 'VALID'
 ) WHERE current_rank = 1;

CREATE TABLE IF NOT EXISTS earthquake_dev.gold.watchlist_snapshots (
 publication_id STRING NOT NULL, usgs_event_id STRING NOT NULL, event_version_id STRING NOT NULL,
 policy_version STRING NOT NULL, event_time TIMESTAMP NOT NULL, magnitude DOUBLE, place STRING,
 longitude DOUBLE NOT NULL, latitude DOUBLE NOT NULL, depth_km DOUBLE NOT NULL,
 significance INT, pager_alert STRING, max_mmi DOUBLE, tsunami_flag BOOLEAN NOT NULL,
 selection_reasons ARRAY<STRING> NOT NULL, impact_label STRING NOT NULL, rank_priority INT NOT NULL,
 as_of_utc TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS earthquake_dev.gold.daily_metrics (
 publication_id STRING NOT NULL, metric_date DATE NOT NULL, event_count BIGINT NOT NULL,
 watchlist_count BIGINT NOT NULL, max_magnitude DOUBLE, pager_yellow_count BIGINT NOT NULL,
 pager_orange_count BIGINT NOT NULL, pager_red_count BIGINT NOT NULL, tsunami_flag_count BIGINT NOT NULL,
 coverage_status STRING NOT NULL, as_of_utc TIMESTAMP NOT NULL
) USING DELTA;

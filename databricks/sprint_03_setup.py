# Databricks notebook source
"""Create the Sprint 3 landing-batch Delta ledger in the development catalog.

Run after Sprint 1–2 setup. Logical keys are checked by the sync notebook.
No Bronze acknowledgement, retention job or ingestion schedule is enabled here.
"""

spark.sql("""CREATE TABLE IF NOT EXISTS earthquake_dev.ops.batches (
    batch_id STRING NOT NULL, run_id STRING NOT NULL,
    stream_generation STRING NOT NULL, topic STRING NOT NULL, partition INT NOT NULL,
    offset_first BIGINT NOT NULL, offset_last BIGINT NOT NULL,
    manifest_uri STRING NOT NULL, manifest_sha256 STRING NOT NULL, row_count BIGINT NOT NULL,
    accepted_delivery_count BIGINT, quarantined_delivery_count BIGINT,
    distinct_source_version_count BIGINT, bronze_insert_count BIGINT,
    landed_at TIMESTAMP NOT NULL, bronze_committed_at TIMESTAMP,
    silver_committed_at TIMESTAMP, data_stage STRING NOT NULL, last_attempt_status STRING NOT NULL
  ) USING DELTA TBLPROPERTIES (
    'delta.deletedFileRetentionDuration'='interval 7 days',
    'delta.logRetentionDuration'='interval 30 days')""")
print("PASS Sprint 3 development batch ledger setup")

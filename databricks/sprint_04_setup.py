# Databricks notebook source
"""Create the Sprint 4 Bronze and quality tables in the development catalog.

Tables are managed Delta and unpartitioned. Existing files/ledgers are preserved;
key uniqueness is enforced by serialized processing and reconciliation checks.
"""

S4_TABLES = {
    "bronze.event_versions": """source_version_id STRING NOT NULL, source_system STRING NOT NULL,
        source_event_id STRING NOT NULL, source_updated_ms BIGINT NOT NULL, event_time_ms BIGINT,
        payload_sha256 STRING NOT NULL, raw_feature_sha256 STRING NOT NULL, raw_event_json STRING NOT NULL,
        contract_version STRING NOT NULL, first_capture_id STRING NOT NULL, first_delivery_id STRING NOT NULL, first_batch_id STRING NOT NULL,
        first_ingested_at TIMESTAMP NOT NULL""",
    "bronze.deliveries": """delivery_id STRING NOT NULL, batch_id STRING NOT NULL,
        stream_generation STRING NOT NULL, topic STRING NOT NULL, partition INT NOT NULL, offset BIGINT NOT NULL,
        kafka_timestamp TIMESTAMP, capture_id STRING, source_item_id STRING, source_item_index BIGINT,
        source_version_id STRING, message_sha256 STRING, record_sha256 STRING NOT NULL, raw_feature_sha256 STRING,
        received_at TIMESTAMP NOT NULL, disposition STRING NOT NULL""",
    "bronze.quarantine": """quarantine_id STRING NOT NULL, delivery_id STRING NOT NULL, batch_id STRING NOT NULL,
        stage_name STRING NOT NULL, contract_or_normalizer_revision STRING NOT NULL, error_codes ARRAY<STRING>,
        raw_value_b64 STRING, raw_reference_uri STRING, rejected_at TIMESTAMP NOT NULL,
        resolution_status STRING NOT NULL, resolved_at TIMESTAMP, replay_run_id STRING""",
    "ops.check_results": """check_id STRING NOT NULL, run_id STRING NOT NULL, scope_type STRING NOT NULL,
        scope_id STRING NOT NULL, rule_id STRING NOT NULL, rule_version STRING NOT NULL, severity STRING NOT NULL,
        status STRING NOT NULL, observed_value DOUBLE, expected_value STRING, evaluated_at TIMESTAMP NOT NULL,
        resolved_at TIMESTAMP, details_json STRING""",
}
for table_name, columns in S4_TABLES.items():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS earthquake_dev.{table_name} ({columns}) USING DELTA
        TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 7 days',
                       'delta.logRetentionDuration'='interval 30 days')""")
    expected_names = {column.strip().split()[0] for column in columns.split(",")}
    if set(spark.table("earthquake_dev." + table_name).columns) != expected_names:
        raise ValueError("Existing Sprint 4 schema differs: " + table_name)
print("PASS Sprint 4 Bronze and quality table setup")

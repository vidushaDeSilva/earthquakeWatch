# Databricks notebook source
"""Create the development namespace and Sprint 1–2 reference/operations tables.

Run this Python notebook on Databricks Serverless once, and safely rerun it if
needed. It creates no Kafka resources and performs no data deletion or scheduling.
Logical keys are maintained by application checks, not enforced PK/FK declarations.
"""

import json
from pyspark.sql import functions as F

# COMMAND ----------
CATALOG = "earthquake_dev"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("ref", "ops", "bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ops.landing")

# COMMAND ----------
TABLE_DDL = {
    "ref.sources": """
      source_id STRING NOT NULL, source_revision INT NOT NULL,
      provider STRING NOT NULL, feed_key STRING NOT NULL, base_url STRING NOT NULL,
      request_parameters MAP<STRING,STRING>, collection_mode STRING NOT NULL,
      lookback_days INT NOT NULL, contract_version STRING NOT NULL,
      enabled BOOLEAN NOT NULL, created_at TIMESTAMP NOT NULL
    """,
    "ref.impact_policies": """
      policy_version STRING NOT NULL, rules_json STRING NOT NULL,
      description STRING NOT NULL, code_commit STRING, created_at TIMESTAMP NOT NULL
    """,
    "ops.runs": """
      run_id STRING NOT NULL, run_kind STRING NOT NULL, github_run_id STRING,
      github_attempt INT, databricks_run_id STRING, window_start TIMESTAMP,
      window_end TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP,
      status STRING NOT NULL, code_commit STRING, contract_version STRING,
      error_class STRING
    """,
    "ops.captures": """
      capture_id STRING NOT NULL, collection_id STRING NOT NULL,
      source_id STRING NOT NULL, source_revision INT NOT NULL, run_id STRING NOT NULL,
      response_uri STRING NOT NULL, manifest_uri STRING NOT NULL,
      response_sha256 STRING NOT NULL, fetched_at TIMESTAMP NOT NULL,
      source_generated_at TIMESTAMP, expected_item_count BIGINT NOT NULL,
      accounted_item_count BIGINT NOT NULL, dispatch_chunk_count BIGINT NOT NULL,
      state STRING NOT NULL, accounted_at TIMESTAMP
    """,
    "ops.stage_attempts": """
      attempt_id STRING NOT NULL, run_id STRING NOT NULL, scope_type STRING NOT NULL,
      scope_id STRING NOT NULL, stage_name STRING NOT NULL, attempt_number INT NOT NULL,
      status STRING NOT NULL, started_at TIMESTAMP, finished_at TIMESTAMP,
      retryable BOOLEAN, http_status INT, error_code STRING, sanitized_message STRING,
      metrics MAP<STRING,STRING>
    """,
}
for table, columns in TABLE_DDL.items():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{table} ({columns}) USING DELTA
      TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 7 days',
                     'delta.logRetentionDuration'='interval 30 days')""")

# COMMAND ----------
source_rows = [
    ("usgs-m2p5-day", "2.5_day", "hourly", 1),
    ("usgs-m2p5-month", "2.5_month", "reconcile", 30),
    ("usgs-significant-month", "significant_month", "reconcile", 30),
]
for source_id, feed_key, mode, days in source_rows:
    # Immutable source revisions are inserted once; edits require a new revision.
    spark.sql(f"""MERGE INTO {CATALOG}.ref.sources t USING (
      SELECT '{source_id}' source_id, 1 source_revision, 'usgs' provider,
      '{feed_key}' feed_key,
      'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed_key}.geojson' base_url,
      CAST(map() AS MAP<STRING,STRING>) request_parameters, '{mode}' collection_mode,
      {days} lookback_days, 'usgs-summary-v1' contract_version, true enabled,
      current_timestamp() created_at) s
      ON t.source_id=s.source_id AND t.source_revision=s.source_revision
      WHEN NOT MATCHED THEN INSERT *""")

rules = {"any": [
    {"field": "significance", "operator": ">", "value": 600},
    {"field": "pager_alert", "operator": "in", "value": ["yellow", "orange", "red"]},
    {"field": "magnitude", "operator": ">=", "value": 6},
    {"field": "mmi", "operator": ">=", "value": 6},
    {"field": "tsunami_flag", "operator": "=", "value": 1},
]}
policy = spark.createDataFrame(
    [("impact-v1", json.dumps(rules), "Estimated/potential impact watchlist; evaluation starts in Sprint 6")],
    "policy_version STRING, rules_json STRING, description STRING")
policy.createOrReplaceTempView("s12_policy_seed")
spark.sql(f"""MERGE INTO {CATALOG}.ref.impact_policies t
  USING (SELECT *, 'sprints-01-02' code_commit, current_timestamp() created_at FROM s12_policy_seed) s
  ON t.policy_version=s.policy_version WHEN NOT MATCHED THEN INSERT *""")
seed_ids = [row[0] for row in source_rows]
seeded = spark.table(f"{CATALOG}.ref.sources").where("source_revision = 1").where(
    F.col("source_id").isin(seed_ids))
assert seeded.count() == 3, "Each required source revision must exist exactly once"
assert seeded.select("source_id").distinct().count() == 3, "Required sources must not be duplicated"
stored_policy = spark.table(f"{CATALOG}.ref.impact_policies").where("policy_version = 'impact-v1'").collect()
assert len(stored_policy) == 1 and json.loads(stored_policy[0].rules_json) == rules, "Immutable policy seed conflicts"
print("PASS Sprint 1 development namespace and reference tables")
print("Volume: /Volumes/earthquake_dev/ops/landing")

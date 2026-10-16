# Databricks notebook source
"""Verify selected cloud captures and idempotently import their operations metadata.

Run after the manual source-capture workflow. The source bytes remain in a managed
Volume; this notebook writes only capture/run ledgers and recognizes verified raw retirement receipts.
Uses explicit schemas and Spark Connect APIs supported by Serverless.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, IntegerType, LongType,
                               StringType, StructField, StructType)

dbutils.widgets.text("volume_root", "/Volumes/earthquake_dev/ops/landing")
root = dbutils.widgets.get("volume_root").rstrip("/")
if root != "/Volumes/earthquake_dev/ops/landing":
    raise ValueError("Sprint 2 ledger sync must use the development landing Volume")
catalog = "earthquake_dev"
spark.conf.set("spark.sql.session.timeZone", "UTC")


def fields(definitions):
    """Construct an explicit nullable JSON schema from named scalar types."""
    return StructType([StructField(name, kind, True) for name, kind in definitions])


def merge_insert(frame, table, key):
    """Insert immutable rows only; never regress a later pipeline stage on replay."""
    if frame.where(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"Missing key for {table}")
    if frame.groupBy(key).count().where("count > 1").limit(1).count():
        raise ValueError(f"Duplicate source keys for {table}")
    frame.createOrReplaceTempView("s12_merge_input")
    spark.sql(f"""MERGE INTO {catalog}.{table} t USING s12_merge_input s
      ON t.{key}=s.{key} WHEN NOT MATCHED THEN INSERT *""")
    if spark.table(f"{catalog}.{table}").groupBy(key).count().where("count > 1").limit(1).count():
        raise ValueError(f"Target key uniqueness failed for {table}")


# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
# MAGIC %run ./sprint_06_gold_library

# COMMAND ----------
# MAGIC %run ./sprint_07_reconciliation_library

# COMMAND ----------
source_store = SparkRecoveryStore(spark, dbutils, root)
new_capture_count = source_store.sync_captures()
print(f"CAPTURE_LEDGER imported_new={new_capture_count}; existing states preserved")

# COMMAND ----------
attempt_schema = fields([(name, StringType()) for name in
                         ("service", "scope_id", "status", "stage_name", "started_at", "finished_at")]
                        + [("request_sequence", IntegerType()), ("attempt_number", IntegerType()),
                           ("http_status", IntegerType()), ("duration_ms", LongType()),
                           ("retryable", BooleanType())])
report_schema = fields([(name, StringType()) for name in
                        ("run_id", "run_kind", "github_run_id", "window_start", "window_end", "started_at",
                         "finished_at", "status", "code_commit", "error_class")]
                       + [("github_attempt", IntegerType()), ("attempts", ArrayType(attempt_schema))])
reports = spark.read.schema(report_schema).option("mode", "FAILFAST").json(root + "/runs/*/report.json")
run_rows = reports.select(
    "run_id", "run_kind", "github_run_id", "github_attempt",
    F.lit(None).cast("string").alias("databricks_run_id"),
    *[F.to_timestamp(name).alias(name) for name in ("window_start", "window_end", "started_at", "finished_at")],
    "status", "code_commit", F.lit("usgs-summary-v1").alias("contract_version"), "error_class")
# Report/failure duplicates for one attempt are a visible error, not silently discarded.
merge_insert(run_rows, "ops.runs", "run_id")
attempts = reports.select("run_id", F.explode("attempts").alias("a")).select("run_id", "a.*")
stage_rows = attempts.select(
    F.sha2(F.to_json(F.struct("run_id", "service", "request_sequence", "attempt_number")), 256).alias("attempt_id"),
    "run_id", F.when(F.col("scope_id") == "RUN", "RUN").otherwise("COLLECTION").alias("scope_type"),
    F.when(F.col("scope_id") == "RUN", F.col("run_id")).otherwise(F.col("scope_id")).alias("scope_id"),
    "stage_name", "attempt_number", "status", F.to_timestamp("started_at").alias("started_at"),
    F.to_timestamp("finished_at").alias("finished_at"), "retryable", "http_status",
    F.lit(None).cast("string").alias("error_code"), F.lit(None).cast("string").alias("sanitized_message"),
    F.create_map(F.lit("service"), F.col("service"), F.lit("duration_ms"), F.col("duration_ms").cast("string")).alias("metrics"))
merge_insert(stage_rows, "ops.stage_attempts", "attempt_id")
print("PASS Sprint 2 capture ledger sync: selected captures independently verified")
display(spark.table(f"{catalog}.ops.captures").select("capture_id", "source_id", "expected_item_count", "state"))

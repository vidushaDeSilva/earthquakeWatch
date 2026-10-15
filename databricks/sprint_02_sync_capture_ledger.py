# Databricks notebook source
"""Verify selected cloud captures and idempotently import their operations metadata.

Run after the manual source-capture workflow. The source bytes remain in a managed
Volume; this notebook writes only Sprint 1–2 Delta ledgers, not Bronze/Silver/Gold.
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
selection_schema = fields([("manifest_version", IntegerType()), ("collection_id", StringType()),
                           ("capture_id", StringType())])
manifest_schema = fields([(name, StringType()) for name in (
    "collection_id", "capture_id", "source_id", "first_run_id", "response_sha256",
    "response_relative_path", "fetched_at", "source_generated_at", "contract_version")]
    + [("source_revision", IntegerType()), ("response_bytes", LongType()),
       ("expected_item_count", LongType())])

# Fail visibly if there are no selected captures; run the GitHub capture first.
selected = spark.read.schema(selection_schema).option("mode", "FAILFAST").json(root + "/collections/*/selected.json")
manifests = spark.read.schema(manifest_schema).option("mode", "FAILFAST").json(root + "/captures/*/*/ready.json")
chosen = selected.select("collection_id", "capture_id").join(manifests, ["collection_id", "capture_id"], "inner")
if chosen.count() != selected.count():
    raise ValueError("A selected source capture is missing its ready manifest")
if chosen.where("contract_version != 'usgs-summary-v1' OR expected_item_count < 0").limit(1).count():
    raise ValueError("Unexpected manifest contract")

payloads = (spark.read.format("binaryFile").load(root + "/captures/*/*/response.json")
            .select(F.regexp_replace(F.regexp_replace("path", "^(dbfs:|file:)", ""), "^/+", "/").alias("path"),
                    F.sha2("content", 256).alias("actual_sha256"),
                    F.length("content").cast("long").alias("actual_bytes")))
# The persisted relative path is bound to known IDs before any payload is accepted.
expected_path = F.concat(F.lit("captures/"), F.col("collection_id"), F.lit("/"),
                         F.col("capture_id"), F.lit("/response.json"))
if chosen.where(F.col("response_relative_path") != expected_path).limit(1).count():
    raise ValueError("Capture path does not match its identities")
verified = chosen.join(payloads, F.concat(F.lit(root + "/"), chosen.response_relative_path) == payloads.path, "left")
if verified.where("actual_sha256 IS NULL OR actual_sha256 != response_sha256 OR actual_bytes != response_bytes").limit(1).count():
    raise ValueError("Source bytes failed independent Volume checksum verification")

capture_rows = verified.select(
    "capture_id", "collection_id", "source_id", "source_revision",
    F.col("first_run_id").alias("run_id"),
    F.concat(F.lit(root + "/"), F.col("response_relative_path")).alias("response_uri"),
    F.concat(F.lit(root + "/captures/"), F.col("collection_id"), F.lit("/"), F.col("capture_id"), F.lit("/ready.json")).alias("manifest_uri"),
    "response_sha256", F.to_timestamp("fetched_at").alias("fetched_at"),
    F.to_timestamp("source_generated_at").alias("source_generated_at"), "expected_item_count",
    F.lit(0).cast("long").alias("accounted_item_count"),
    F.lit(0).cast("long").alias("dispatch_chunk_count"), F.lit("CAPTURED").alias("state"),
    F.lit(None).cast("timestamp").alias("accounted_at"))
merge_insert(capture_rows, "ops.captures", "capture_id")

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

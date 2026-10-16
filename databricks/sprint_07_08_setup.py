# Databricks notebook source
"""Create recovery/lifecycle metadata and migrate the serving pointer safely."""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
# MAGIC %run ./sprint_06_gold_library

# COMMAND ----------
# MAGIC %run ./sprint_07_reconciliation_library

# COMMAND ----------
for table, columns in S78_SCHEMAS.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS earthquake_dev.{table} ({columns}) USING DELTA")
    expected = spark.createDataFrame([], columns).schema
    actual = spark.table("earthquake_dev." + table).schema
    if {f.name:f.dataType for f in expected} != {f.name:f.dataType for f in actual}:
        raise RecoveryError("Existing lifecycle schema differs; migrate without dropping: " + table)
for path in ("receipts/retired/batches", "receipts/retired/captures", "reconciliation/requests",
             "reconciliation/plans", "reconciliation/gaps", "lifecycle/plans", "lifecycle/snapshots", "lifecycle/replays"):
    dbutils.fs.mkdirs(BRONZE_ROOT + "/" + path)
print("PASS Sprint 7/8 setup; now rerun updated sprint_05_06_setup to migrate pointer and views")

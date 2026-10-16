# Databricks notebook source
"""Run the complete lakehouse pass in one Serverless session and seal progress.

The scheduled cloud controller owns retries and continuation. This notebook
contacts neither USGS nor Kafka and never declares a partial release healthy.
"""

# COMMAND ----------
# MAGIC %run ./sprint_02_sync_capture_ledger

# COMMAND ----------
# MAGIC %run ./sprint_03_sync_handoff_ledger

# COMMAND ----------
# MAGIC %run ./sprint_04_process_bronze

# COMMAND ----------
# MAGIC %run ./sprint_05_process_silver

# COMMAND ----------
# MAGIC %run ./sprint_07_reconcile

# COMMAND ----------
# MAGIC %run ./sprint_06_publish_gold

# COMMAND ----------
# MAGIC %run ./sprint_08_maintain

# COMMAND ----------
dbutils.widgets.text("automation_operation_id", "")
operation_id = s78_id(dbutils.widgets.get("automation_operation_id"))
pending_captures = sum(r["state"] != "ACCOUNTED" for r in reconciliation_store.rows("ops.captures", 10000))
pending_batches = sum(r["data_stage"] != "SILVER_COMMITTED" for r in reconciliation_store.rows("ops.batches", 10000))
latest = reconciliation_store.table("ops.reconciliations").orderBy("checked_at", ascending=False).limit(1).collect()
issues = latest[0]["issues"] if latest else ["INITIAL_SOURCE_PLAN"]
report = {"report_version": 2, "operation_id": operation_id,
    "checked_at": s56_fact(reconciliation_store.now()), "pending_captures": pending_captures,
    "pending_batches": pending_batches, "issues": list(issues), "gold": gold_metrics,
    "maintenance": maintenance_result,
    "plan_id": reconciliation_store.event_check_plan()["plan_id"]}
reconciliation_store.put_receipt("automation/reports/" + operation_id + ".json", bronze_json(report))
print("AUTOMATION_PASS " + bronze_json(report).decode())

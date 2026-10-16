# Databricks notebook source
"""Export tracked-event plans or certify coverage before the existing Gold task."""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
# MAGIC %run ./sprint_06_gold_library

# COMMAND ----------
# MAGIC %run ./sprint_07_reconciliation_library

# COMMAND ----------
dbutils.widgets.text("volume_root", BRONZE_ROOT)
dbutils.widgets.text("reconcile_mode", "verify")
reconciliation_store = SparkRecoveryStore(spark, dbutils, dbutils.widgets.get("volume_root"))


def reconcile_operation(run_id):
    """Persist independent reconciliation results; blocked coverage remains inspectable."""
    return reconciliation_store.reconcile(run_id, dbutils.widgets.get("reconcile_mode"))


reconciliation_result = s56_attempt(reconciliation_store, "RECONCILIATION", reconcile_operation)
print("RECONCILIATION_RESULT " + bronze_json(reconciliation_result).decode())

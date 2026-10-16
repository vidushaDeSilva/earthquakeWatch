# Databricks notebook source
"""Run reviewed cleanup, release snapshots, isolated replay or explicit serving rollback."""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
# MAGIC %run ./sprint_06_gold_library

# COMMAND ----------
# MAGIC %run ./sprint_07_reconciliation_library

# COMMAND ----------
# MAGIC %run ./sprint_08_lifecycle_library

# COMMAND ----------
for name, default in (("volume_root", BRONZE_ROOT), ("maintenance_action", "none"),
        ("retention_plan_id", ""), ("rollback_publication_id", ""), ("rollback_expected_generation", "0"),
        ("rollback_reason", ""), ("replay_id", ""), ("maintenance_fail_at", "none")):
    dbutils.widgets.text(name, default)
lifecycle_store = SparkLifecycleStore(spark, dbutils, dbutils.widgets.get("volume_root"))


def maintain_operation(run_id):
    """Use explicit Job parameters so ordinary ingestion cannot accidentally delete data."""
    action = dbutils.widgets.get("maintenance_action")
    engine = LifecycleEngine(lifecycle_store, run_id, dbutils.widgets.get("maintenance_fail_at"))
    if action == "none":
        return {"action": "none"}
    if action == "snapshot":
        return lifecycle_store.snapshot_daily()
    if action == "plan":
        return engine.plan()
    if action == "apply":
        return engine.apply(dbutils.widgets.get("retention_plan_id"))
    if action == "rollback":
        return engine.rollback(dbutils.widgets.get("rollback_publication_id"),
            int(dbutils.widgets.get("rollback_expected_generation")), dbutils.widgets.get("rollback_reason"))
    if action == "replay":
        return lifecycle_store.replay_bronze(dbutils.widgets.get("replay_id"))
    if action in ("vacuum_preview", "vacuum"):
        return lifecycle_store.vacuum(action == "vacuum")
    raise RecoveryError("Unknown maintenance action")


maintenance_result = s56_attempt(lifecycle_store, "MAINTENANCE", maintain_operation)
print("MAINTENANCE_RESULT " + bronze_json(maintenance_result).decode())

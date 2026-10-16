# Databricks notebook source
"""Run Gold preview or checked publication as the fifth serial handoff Job task.

Preview is the safe default while the historical capture backlog is unresolved.
Retry by publication ID to reuse exact pins; a fresh run creates a new candidate.
"""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
# MAGIC %run ./sprint_06_gold_library

# COMMAND ----------
if "GoldEngine" not in globals():
    raise RuntimeError("Import the source notebook with separate %run cells, then Run all")
for name, default in (("volume_root", BRONZE_ROOT), ("gold_action", "preview"), ("gold_as_of_utc", ""),
        ("gold_retry_publication_id", ""), ("gold_policy_version", "impact-v1"),
        ("gold_processing_generation", "v1"), ("gold_fail_at", "none"), ("automation_operation_id", "")):
    dbutils.widgets.text(name, default)
gold_action = dbutils.widgets.get("gold_action")
gold_retry_id = dbutils.widgets.get("gold_retry_publication_id").strip() or None
gold_as_of = dbutils.widgets.get("gold_as_of_utc").strip()
if gold_retry_id and gold_as_of:
    raise GoldError("Retry publication ID supplies its own as-of; leave gold_as_of_utc blank")
gold_store = SparkGoldStore(spark, dbutils, dbutils.widgets.get("volume_root"))


def process_gold_operation(run_id):
    """Execute one bounded release under persisted attempt metadata."""
    if dbutils.widgets.get("automation_operation_id"):
        planning = globals().get("reconciliation_result", {}).get("mode") == "plan"
        pending = (any(r["state"] != "ACCOUNTED" for r in gold_store.rows("ops.captures", 10000)) or
                   any(r["data_stage"] != "SILVER_COMMITTED" for r in gold_store.rows("ops.batches", 10000)))
        if planning or pending:
            return {"action": "deferred", "reason": "BOUNDED_WORK_OR_SOURCE_PLAN"}
    return GoldEngine(gold_store, run_id, dbutils.widgets.get("gold_fail_at")).process(
        action=gold_action, as_of=bronze_time(gold_as_of) if gold_as_of else None, retry_id=gold_retry_id,
        policy_version=dbutils.widgets.get("gold_policy_version"),
        generation=dbutils.widgets.get("gold_processing_generation"))


gold_metrics = s56_attempt(gold_store, "GOLD", process_gold_operation)
print("GOLD_RESULT " + bronze_json(gold_metrics).decode())


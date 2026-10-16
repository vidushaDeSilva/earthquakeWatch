# Databricks notebook source
"""Run the fourth serial handoff Job task: verified Bronze to Silver.

Import as source, retaining separate %run cells. Recovery uses this SAME Job;
normal runs repair missing receipts without re-fetching USGS or contacting Kafka.
"""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
# MAGIC %run ./sprint_05_silver_library

# COMMAND ----------
if "SparkSilverStore" not in globals():
    raise RuntimeError("Import the source notebook with separate %run cells, then Run all")
for name, default in (("volume_root", BRONZE_ROOT), ("silver_recheck", "false"),
                      ("silver_max_batches", "32"), ("silver_fail_at", "none")):
    dbutils.widgets.text(name, default)
silver_recheck = dbutils.widgets.get("silver_recheck").lower()
silver_maximum = int(dbutils.widgets.get("silver_max_batches"))
silver_failure = dbutils.widgets.get("silver_fail_at")
if silver_recheck not in ("true", "false") or not 1 <= silver_maximum <= 32 or silver_failure not in SILVER_BOUNDARIES:
    raise SilverError("Invalid Silver development parameters")
silver_store = SparkSilverStore(spark, dbutils, dbutils.widgets.get("volume_root"))


def process_silver_operation(run_id):
    """Certify a bounded work group and report any durable remaining backlog."""
    selected, remaining = silver_store.silver_batches(silver_recheck == "true", silver_maximum)
    metrics = SilverEngine(silver_store, run_id, silver_failure).process(selected)
    metrics["remaining_batches"] = bool(remaining)
    return metrics


silver_metrics = s56_attempt(silver_store, "SILVER", process_silver_operation)
print("SILVER_COUNTS " + bronze_json(silver_metrics).decode())


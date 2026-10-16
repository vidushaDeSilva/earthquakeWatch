# Databricks notebook source
"""Process existing sealed Kafka batches into Bronze and reconcile source captures.

Run as the third serial task in earthquake-dev-handoff-ledger. This notebook has
no external API/Kafka dependency. Use the same Job for direct recovery launches.
"""

# COMMAND ----------
# MAGIC %run ./sprint_04_bronze_library

# COMMAND ----------
import uuid

for widget, default in (("volume_root", BRONZE_ROOT), ("bronze_fail_at", "none"),
                        ("bronze_recheck", "false"), ("bronze_max_batches", "32")):
    dbutils.widgets.text(widget, default)
fail_at = dbutils.widgets.get("bronze_fail_at")
if fail_at not in ("none", "after-versions", "after-quarantine", "after-deliveries",
                   "after-batch-ledger", "after-batch-receipt", "after-capture-ledger", "after-capture-receipt"):
    raise BronzeError("Invalid development fault boundary")
recheck = dbutils.widgets.get("bronze_recheck").lower()
max_batches = int(dbutils.widgets.get("bronze_max_batches"))
if recheck not in ("true", "false") or not 1 <= max_batches <= 32:
    raise BronzeError("Invalid Bronze processing bounds")
store = SparkBronzeStore(spark, dbutils, dbutils.widgets.get("volume_root"))
started = datetime.now(timezone.utc)
run_id = bronze_id("bronze-run-v1", uuid.uuid4().hex)
run_row = {"run_id": run_id, "run_kind": "RECOVERY", "github_run_id": None, "github_attempt": None,
    "databricks_run_id": None, "window_start": started, "window_end": started,
    "started_at": started, "finished_at": None, "status": "RUNNING", "code_commit": BRONZE_REVISION,
    "contract_version": BRONZE_CONTRACT, "error_class": None}
attempt = {"attempt_id": bronze_id("bronze-attempt-v1", run_id), "run_id": run_id,
    "scope_type": "STREAM", "scope_id": "earthquake_events_dev", "stage_name": "BRONZE",
    "attempt_number": 1, "status": "RUNNING", "started_at": started, "finished_at": None,
    "retryable": False, "http_status": None, "error_code": None, "sanitized_message": None, "metrics": {}}
store.upsert_ops("ops.runs", [run_row], ("status", "finished_at", "error_class"))
store.upsert_ops("ops.stage_attempts", [attempt], ("status", "finished_at", "error_code", "metrics"))
try:
    bundles, remaining = store.bundles(recheck == "true", max_batches)
    engine = BronzeEngine(store, run_id, fail_at)
    processed = engine.process(bundles)
    pending_captures = engine.account_captures()
    # A successful bounded pass may leave work for the scheduled controller.
    # Corrupt evidence still raises above; unfinished work is not corruption.
    run_row.update(status="SUCCESS", finished_at=datetime.now(timezone.utc))
    attempt.update(status="SUCCESS", finished_at=run_row["finished_at"], metrics={
        "processed_batches": str(processed), "remaining_batches": str(bool(remaining)),
        "pending_captures": str(len(pending_captures))})
    print(f"PASS Sprint 4 Bronze ingestion: processed_batches={processed} run_id={run_id}")
except Exception as exc:
    run_row.update(status="FAILED", finished_at=datetime.now(timezone.utc), error_class=type(exc).__name__)
    attempt.update(status="FAILED", finished_at=run_row["finished_at"], error_code=type(exc).__name__)
    try:
        store.check(run_id, "STREAM", "earthquake_events_dev", "bronze_run_completed", "FAIL", 0, "1")
    except Exception:
        print("DIAGNOSTIC_WRITE_FAILED; inspect this Databricks run")
    # Log only a safe class; malformed records and transport errors may contain payloads.
    print(f"BRONZE_FAILED error_class={type(exc).__name__} run_id={run_id}")
    raise
finally:
    store.upsert_ops("ops.runs", [run_row], ("status", "finished_at", "error_class"))
    store.upsert_ops("ops.stage_attempts", [attempt], ("status", "finished_at", "error_code", "metrics"))

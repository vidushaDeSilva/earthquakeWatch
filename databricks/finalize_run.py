"""Reconcile stage evidence and record one terminal completion report.

Configure this task to run after Gold with the Databricks ALL_DONE condition.
An incomplete or unknown outcome is classified conservatively as terminal.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from earthquake_watch.contracts import ContractError, json_bytes, stamp
from earthquake_watch.dispatch import verify_dispatch
from earthquake_watch.volume_client import MountedVolume


TABLE = "earthquake_dev.ops.pipeline_runs"
PRODUCT = "usgs-watchlist"


def classify(stages: list[dict[str, Any]], checks: list[dict[str, Any]],
             publications: list[dict[str, Any]], pointer_id: str | None) -> tuple[str, bool, str]:
    """Use committed evidence to distinguish success, quality, and unknown failure."""
    committed = {row["stage_name"] for row in stages if row["stage_status"] == "COMMITTED"}
    active = [row for row in publications if row["publication_status"] == "ACTIVE"
              and row["validation_status"] == "PASS" and row["publication_id"] == pointer_id]
    if {"BRONZE", "SILVER", "GOLD"} <= committed and len(active) == 1 and not any(
            row["check_status"] == "FAIL" and row["severity"] == "BLOCKER" for row in checks):
        return "SUCCEEDED", False, "COMPLETE"
    if any(row["stage_name"] == "GOLD" and row["check_status"] == "FAIL"
           and row["severity"] == "BLOCKER" for row in checks):
        return "FAILED", False, "QUALITY"
    if any(row["stage_status"] == "RETRYABLE_FAILED" and row.get("error_code") in
           {"TEMPORARY_STORAGE", "COMPUTE_STARTUP", "TRANSIENT_CONNECTION"} for row in stages):
        return "FAILED", True, "TRANSIENT"
    return "FAILED", False, "UNCLASSIFIED"


def _immutable_report(root: Path, run_id: str, report: dict[str, Any]) -> dict[str, Any]:
    """Create the report once, then reuse identical existing evidence on retry."""
    path = root / "completion" / run_id / "report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(json_bytes(report))
        return report
    except FileExistsError:
        try:
            saved = json.loads(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise ContractError("Existing completion report is invalid") from exc
        if (saved.get("pipeline_run_id") != run_id or saved.get("dispatch_id") != report["dispatch_id"]
                or saved.get("status") != report["status"]
                or saved.get("input_manifest_sha256") != report["input_manifest_sha256"]):
            raise ContractError("Existing completion report conflicts with reconciliation")
        return saved


def _finish_parent(spark: Any, pipeline_run_id: str, saved: dict[str, Any]) -> None:
    """Reconcile a durable report to the one matching Ops parent row."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    DeltaTable.forName(spark, TABLE).update(
        (F.col("pipeline_run_id") == pipeline_run_id) &
        (F.col("run_status").isin("RUNNING", saved["status"])), {
            "run_status": F.lit(saved["status"]), "retryable": F.lit(saved["retryable"]),
            "databricks_run_id": F.lit(saved["databricks_run_id"]).cast("long"),
            "finished_at": F.to_timestamp(F.lit(saved["finished_at"])),
            "error_class": F.lit(saved["error_class"]), "error_code": F.lit(saved["error_code"]),
            "error_message": F.lit(None if saved["status"] == "SUCCEEDED" else "See completion report and stage checks")})
    updated = spark.table(TABLE).where(F.col("pipeline_run_id") == pipeline_run_id).collect()
    if len(updated) != 1 or updated[0].run_status != saved["status"]:
        raise ContractError("Completion could not be reconciled to Ops")


def finalize(spark: Any, volume: MountedVolume, batch_id: str, pipeline_run_id: str,
             revision: str, dispatch_id: str, attempt_no: int, databricks_run_id: int) -> dict[str, Any]:
    """Write immutable completion evidence and update only the matching Ops row."""
    from pyspark.sql import functions as F

    intent = verify_dispatch(volume, dispatch_id, attempt_no, batch_id, pipeline_run_id, revision)
    parents = spark.table(TABLE).where(F.col("pipeline_run_id") == pipeline_run_id).collect()
    if len(parents) != 1 or parents[0].dispatch_id != dispatch_id:
        raise ContractError("Cannot finalize an unregistered pipeline run")
    parent = parents[0]
    if parent.databricks_run_id != databricks_run_id or databricks_run_id <= 0:
        raise ContractError("Databricks run ID differs from registered parent")
    report_raw = volume.read_optional(f"completion/{pipeline_run_id}/report.json")
    if report_raw is not None:
        try:
            saved = json.loads(report_raw)
        except (UnicodeError, ValueError) as exc:
            raise ContractError("Existing completion report is invalid") from exc
        if (saved.get("pipeline_run_id") != pipeline_run_id or saved.get("dispatch_id") != dispatch_id
                or saved.get("databricks_run_id") != databricks_run_id
                or saved.get("input_manifest_sha256") != intent["landing_manifest_sha256"]):
            raise ContractError("Existing completion report conflicts with run")
        _finish_parent(spark, pipeline_run_id, saved)
        return saved
    from delta.tables import DeltaTable
    stages = [r.asDict(recursive=True) for r in spark.table("earthquake_dev.ops.stage_runs")
              .where(F.col("pipeline_run_id") == pipeline_run_id).collect()]
    checks = [r.asDict(recursive=True) for r in spark.table("earthquake_dev.ops.quality_checks")
              .where(F.col("pipeline_run_id") == pipeline_run_id).collect()]
    publications = [r.asDict(recursive=True) for r in spark.table("earthquake_dev.ops.publications")
                    .where(F.col("pipeline_run_id") == pipeline_run_id).collect()]
    pointers = spark.table("earthquake_dev.ops.serving_pointer").where(
        F.col("product_key") == PRODUCT).collect()
    if len(pointers) > 1:
        raise ContractError("Serving pointer has duplicate product keys")
    pointer_id = pointers[0].active_publication_id if pointers else None
    status, retryable, code = classify(stages, checks, publications, pointer_id)
    if status == "FAILED":
        # A code failure can occur after validation but before activation. Make
        # every nonactive candidate terminal so it cannot be mistaken for a
        # publishable release or retried under the same immutable revision.
        DeltaTable.forName(spark, "earthquake_dev.ops.publications").update(
            (F.col("pipeline_run_id") == pipeline_run_id) &
            (F.col("publication_status").isin("BUILDING", "READY")),
            {"publication_status": F.lit("REJECTED")})
    if status == "SUCCEEDED" and pointers and pointers[0].previous_publication_id:
        # The pointer already changed; the previous candidate can now be marked
        # historical without affecting what consumers see.
        DeltaTable.forName(spark, "earthquake_dev.ops.publications").update(
            (F.col("publication_id") == pointers[0].previous_publication_id) &
            (F.col("publication_status") == "ACTIVE"),
            {"publication_status": F.lit("SUPERSEDED")})
    receipt_raw = volume.read_optional(f"dispatch/{dispatch_id}/attempt-{attempt_no}-receipt.json")
    receipt = json.loads(receipt_raw) if receipt_raw is not None else {}
    if receipt and (receipt.get("pipeline_run_id") != pipeline_run_id or
                    receipt.get("job_id") != intent["job_id"] or
                    receipt.get("databricks_run_id") != databricks_run_id):
        raise ContractError("Dispatch receipt conflicts with registered run")
    report = {"report_version": 1, "pipeline_run_id": pipeline_run_id,
              "dispatch_id": dispatch_id, "attempt_no": attempt_no,
              "databricks_run_id": databricks_run_id,
              "batch_ids": [batch_id], "processing_revision": revision,
              "input_manifest_sha256": intent["landing_manifest_sha256"],
              "policy_version": "impact-v1", "status": status,
              "retryable": retryable, "error_class": None if status == "SUCCEEDED" else code,
              "error_code": None if status == "SUCCEEDED" else code,
              "stage_summaries": [{k: row.get(k) for k in (
                  "stage_name", "stage_status", "records_in", "records_out",
                  "records_quarantined", "output_delta_version")} for row in stages],
              "quality_summary": {"passed": sum(r["check_status"] == "PASS" for r in checks),
                                  "failed": sum(r["check_status"] == "FAIL" for r in checks)},
              "publication_id": next((p["publication_id"] for p in publications
                                      if p["publication_id"] == pointer_id and status == "SUCCEEDED"), None),
              "started_at": stamp(parent.started_at.replace(tzinfo=timezone.utc)),
              "finished_at": stamp(datetime.now(timezone.utc))}
    saved = _immutable_report(volume.root, pipeline_run_id, report)
    _finish_parent(spark, pipeline_run_id, saved)
    return saved


def main() -> None:
    """Receive the shared Job parameters after all upstream tasks finish."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--dispatch-id", required=True)
    parser.add_argument("--attempt-no", required=True, type=int)
    parser.add_argument("--databricks-run-id", required=True, type=int)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    result = finalize(SparkSession.builder.getOrCreate(), MountedVolume(), args.batch_id,
                      args.pipeline_run_id, args.revision, args.dispatch_id, args.attempt_no,
                      args.databricks_run_id)
    print(json.dumps({k: result[k] for k in ("pipeline_run_id", "status", "retryable", "error_code")}))


if __name__ == "__main__":
    main()

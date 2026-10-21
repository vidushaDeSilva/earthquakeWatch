"""Produce a retention dry run and optionally apply its reviewed path digest.

This is an operator maintenance task, separate from the scheduled transport
workflow. It never runs VACUUM or deletes Delta rows automatically.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from earthquake_watch.contracts import ContractError, json_bytes, stamp
from earthquake_watch.contracts import SourceConfig
from earthquake_watch.kafka_transport import verified_capture
from earthquake_watch.landing import verify_landing
from earthquake_watch.retention import apply_plan, file_plan, plan_sha256, safe_candidates
from earthquake_watch.volume_client import MountedVolume


ROOT = Path("/Volumes/earthquake_dev/ops/pipeline_files")


def _collect(spark: Any, name: str) -> list[dict[str, Any]]:
    """Collect bounded Ops metadata for the small development catalog."""
    rows = spark.table("earthquake_dev.ops." + name).collect()
    if len(rows) > 100000:
        raise ContractError("Retention metadata exceeds the reviewed bound")
    return [row.asDict(recursive=True) for row in rows]


def candidates(spark: Any, root: Path, now: datetime,
               allow_partial_apply: bool = False) -> list[dict[str, Any]]:
    """Recompute eligibility from current Ops and files before every action."""
    captures, batches, runs, stages, publications, quarantine = (
        _collect(spark, name) for name in (
            "captures", "landing_batches", "pipeline_runs", "stage_runs",
            "publications", "quarantine_records"))
    pointer = _collect(spark, "serving_pointer")
    active = [row for row in pointer if row["product_key"] == "usgs-watchlist"]
    if len(active) > 1:
        raise ContractError("Duplicate serving pointers")
    open_batches = {row["landing_batch_id"] for row in quarantine
                    if row["resolution_status"] == "OPEN" and row["landing_batch_id"]}
    safe_captures, safe_batches = safe_candidates(
        now, captures, batches, runs, stages, publications,
        active[0]["active_publication_id"] if active else None, open_batches)
    mounted = MountedVolume(str(root))
    source = SourceConfig("usgs-all-day", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
                          16777216, 30, 300)
    for capture_id in sorted(safe_captures):
        complete = (root / "captures" / capture_id / "response.json").is_file() and (
            root / "captures" / capture_id / "manifest.json").is_file()
        if complete or not allow_partial_apply:
            verified_capture(mounted, capture_id, source)
    for batch_id in sorted(safe_batches):
        complete = (root / "landing" / batch_id / "records.jsonl").is_file() and (
            root / "landing" / batch_id / "manifest.json").is_file()
        if complete or not allow_partial_apply:
            verify_landing(mounted, batch_id)
    return file_plan(root, now, safe_captures, safe_batches)


def maintain(spark: Any, root: Path, expected_digest: str | None = None) -> dict[str, Any]:
    """Persist a dry-run plan or apply only an exact previously reviewed plan."""
    now = datetime.now(timezone.utc)
    rows = candidates(spark, root, now, allow_partial_apply=expected_digest is not None)
    if expected_digest is None:
        digest = plan_sha256(rows)
        report = {"report_version": 1, "created_at": stamp(now), "plan_sha256": digest,
                  "candidate_count": len(rows), "candidates": rows}
        path = root / "retention" / f"plan-{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            prior = json.loads(path.read_bytes())
            if prior.get("plan_sha256") != digest or prior.get("candidates") != rows:
                raise ContractError("Existing retention plan conflicts")
        else:
            with path.open("xb") as stream:
                stream.write(json_bytes(report))
        return {"mode": "DRY_RUN", "plan_sha256": digest, "candidate_count": len(rows),
                "plan_path": str(path)}
    if len(expected_digest) != 64 or any(c not in "0123456789abcdef" for c in expected_digest):
        raise ContractError("Invalid reviewed retention digest")
    path = root / "retention" / f"plan-{expected_digest}.json"
    if not path.is_file():
        raise ContractError("Reviewed retention plan does not exist")
    report = json.loads(path.read_bytes())
    reviewed = report["candidates"]
    if report.get("plan_sha256") != expected_digest or plan_sha256(reviewed) != expected_digest:
        raise ContractError("Reviewed retention plan checksum differs")
    counts = apply_plan(root, reviewed, rows)
    return {"mode": "APPLIED", "plan_sha256": expected_digest, **counts}


def main() -> None:
    """Keep physical deletion behind an explicit reviewed plan digest."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply-plan-sha256")
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    result = maintain(SparkSession.builder.getOrCreate(), ROOT, args.apply_plan_sha256)
    print(json.dumps(result))


if __name__ == "__main__":
    main()

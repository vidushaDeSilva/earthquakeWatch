"""Compute guarded Volume cleanup candidates from Ops lineage and file age.

The pure planner does not delete data. It protects active publications, open
quarantine, unfinished processing and captures with any unfinished batch.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from .contracts import ContractError


INCOMPLETE_DAYS = 7
COMPLETED_FILES_DAYS = 30
RECEIPTS_DAYS = 90
BRONZE_DAYS = 90
SILVER_DAYS = 365
GOLD_DAYS = 365
OPS_MIN_DAYS = 365


def _utc(value: datetime) -> datetime:
    """Normalize Spark's timezone-naive UTC timestamp to an aware value."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def safe_candidates(now: datetime, captures: list[dict[str, Any]],
                    batches: list[dict[str, Any]], runs: list[dict[str, Any]],
                    stages: list[dict[str, Any]], publications: list[dict[str, Any]],
                    active_publication_id: str | None,
                    open_quarantine_batches: set[str]) -> tuple[set[str], set[str]]:
    """Return completed capture and batch identities safe after 30 days."""
    now = _utc(now)
    for rows, key in ((captures, "capture_id"), (batches, "landing_batch_id"),
                      (runs, "pipeline_run_id"), (publications, "publication_id")):
        identifiers = [row[key] for row in rows]
        if len(identifiers) != len(set(identifiers)):
            raise ContractError(f"Duplicate Ops identity in retention input: {key}")
    publication = [p for p in publications if p["publication_id"] == active_publication_id]
    if len(publication) > 1 or (active_publication_id and len(publication) != 1):
        raise ContractError("Active publication lineage is missing or ambiguous")
    if publication and (publication[0]["publication_status"] != "ACTIVE" or
                        publication[0].get("validation_status") != "PASS"):
        raise ContractError("Serving pointer does not reference a validated active publication")
    if active_publication_id is None and any(p["publication_status"] == "ACTIVE" for p in publications):
        raise ContractError("Active publication has no serving pointer")
    protected = set(publication[0]["input_batch_ids"]) if publication else set()
    for run in runs:
        if run["run_status"] not in ("SUCCEEDED", "FAILED"):
            protected.update(run["batch_ids"])
    terminal_by_batch: dict[str, list[dict[str, Any]]] = {}
    for run in runs:
        for batch_id in run["batch_ids"]:
            terminal_by_batch.setdefault(batch_id, []).append(run)
    stage_by_run: dict[str, set[str]] = {}
    for stage in stages:
        if stage["stage_status"] == "COMMITTED":
            stage_by_run.setdefault(stage["pipeline_run_id"], set()).add(stage["stage_name"])
    pub_by_run = {p["pipeline_run_id"] for p in publications
                  if p["publication_status"] in ("ACTIVE", "REJECTED", "SUPERSEDED")}
    safe_batches = set()
    for batch in batches:
        batch_id = batch["landing_batch_id"]
        related = terminal_by_batch.get(batch_id, [])
        if (batch_id in protected or batch_id in open_quarantine_batches or not related
                or batch["landing_status"] != "VERIFIED" or batch["expected_record_count"] != batch["landed_record_count"]
                or _utc(batch["verified_at"]) > now - timedelta(days=COMPLETED_FILES_DAYS)):
            continue
        if all(run["run_status"] in ("SUCCEEDED", "FAILED") and
               {"BRONZE", "SILVER"} <= stage_by_run.get(run["pipeline_run_id"], set()) and
               run["pipeline_run_id"] in pub_by_run for run in related):
            safe_batches.add(batch_id)
    by_capture: dict[str, set[str]] = {}
    for batch in batches:
        for capture_id in batch["capture_ids"]:
            by_capture.setdefault(capture_id, set()).add(batch["landing_batch_id"])
    safe_captures = {c["capture_id"] for c in captures if
                     c["capture_status"] == "PUBLISHED" and
                     _utc(c["fetched_at"]) <= now - timedelta(days=COMPLETED_FILES_DAYS) and
                     by_capture.get(c["capture_id"]) and
                     by_capture[c["capture_id"]] <= safe_batches}
    return safe_captures, safe_batches


def file_plan(root: Path, now: datetime, capture_ids: set[str], batch_ids: set[str]) -> list[dict[str, Any]]:
    """Enumerate completed files, aged receipts, and orphan data with hashes."""
    now = _utc(now)
    rows = []
    for category, identities, names in (
            ("captures", capture_ids, ("response.json", "manifest.json")),
            ("landing", batch_ids, ("records.jsonl", "manifest.json"))):
        for identifier in sorted(identities):
            if len(identifier) != 64 or any(c not in "0123456789abcdef" for c in identifier):
                raise ContractError("Retention identifier is unsafe")
            folder = root / category / identifier
            for name in names:
                path = folder / name
                if path.is_file():
                    rows.append({"path": f"{category}/{identifier}/{name}",
                                 "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                 "reason": "COMPLETED_30D"})
    for category, data_name in (("captures", "response.json"), ("landing", "records.jsonl")):
        directory = root / category
        if not directory.exists():
            continue
        for folder in directory.iterdir():
            if not folder.is_dir() or len(folder.name) != 64 or any(
                    c not in "0123456789abcdef" for c in folder.name):
                continue
            data_path = folder / data_name
            if ((folder / "manifest.json").exists() or not data_path.is_file()
                    or datetime.fromtimestamp(data_path.stat().st_mtime, timezone.utc) > now - timedelta(days=INCOMPLETE_DAYS)):
                continue
            rows.append({"path": f"{category}/{folder.name}/{data_name}",
                         "sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
                         "reason": "INCOMPLETE_7D"})
    for kind, identifiers in (("publish", capture_ids), ("offset", batch_ids)):
        for identifier in sorted(identifiers):
            folder = root / "receipts" / kind / identifier
            if not folder.exists():
                continue
            for receipt in folder.iterdir():
                if (not receipt.is_file() or receipt.is_symlink() or not receipt.name.endswith(".json")
                        or datetime.fromtimestamp(receipt.stat().st_mtime, timezone.utc) > now - timedelta(days=RECEIPTS_DAYS)):
                    continue
                rows.append({"path": f"receipts/{kind}/{identifier}/{receipt.name}",
                             "sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
                             "reason": "TERMINAL_RECEIPT_90D"})
    return sorted(rows, key=lambda r: r["path"])


def plan_sha256(rows: list[dict[str, Any]]) -> str:
    """Fingerprint the reviewed, sorted path and checksum plan."""
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def apply_plan(root: Path, reviewed: list[dict[str, Any]], current: list[dict[str, Any]]) -> dict[str, int]:
    """Recheck every target before unlink; tolerate a prior partial apply."""
    current_by_path = {row["path"]: row for row in current}
    removed = already_removed = 0
    # Data is removed before its manifest; a crash cannot leave an incomplete
    # data file that appears ready to consumers.
    ordered = sorted(reviewed, key=lambda r: (r["path"].rsplit("/", 1)[0],
                                               r["path"].endswith("manifest.json")))
    for row in ordered:
        relative = row["path"]
        parts = Path(relative).parts
        known = ((len(parts) == 3 and parts[0] in ("captures", "landing") and
                  parts[2] in ("response.json", "records.jsonl", "manifest.json")) or
                 (len(parts) == 4 and parts[:2] in (("receipts", "publish"), ("receipts", "offset"))
                  and parts[3].endswith(".json")))
        if (not known or len(parts[2 if len(parts) == 4 else 1]) != 64 or
                any(c not in "0123456789abcdef" for c in parts[2 if len(parts) == 4 else 1]) or
                any(part in (".", "..") for part in parts)):
            raise ContractError("Reviewed retention path is unsafe")
        target = root.joinpath(*parts)
        if target.is_symlink() or target.parent.is_symlink():
            raise ContractError("Cleanup target is a symlink")
        if not target.exists():
            already_removed += 1
            continue
        if current_by_path.get(relative) != row:
            raise ContractError("Cleanup target is no longer eligible or changed")
        if hashlib.sha256(target.read_bytes()).hexdigest() != row["sha256"]:
            raise ContractError("Cleanup target checksum changed")
        target.unlink()
        removed += 1
    return {"removed": removed, "already_removed": already_removed}

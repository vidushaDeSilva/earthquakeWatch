"""Failure injection for dispatch tokens, bounded observation, and cleanup guards."""

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from earthquake_watch.contracts import ContractError, json_bytes
from earthquake_watch.dispatch import dispatch_once
from earthquake_watch.landing import Delivery, freeze_landing
from earthquake_watch.retention import (INCOMPLETE_DAYS, COMPLETED_FILES_DAYS,
                                        RECEIPTS_DAYS, BRONZE_DAYS, SILVER_DAYS,
                                        GOLD_DAYS, OPS_MIN_DAYS, apply_plan,
                                        file_plan, plan_sha256, safe_candidates)
from earthquake_watch.volume_client import MemoryVolume, VolumeError


NOW = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


def test_retention_contract_matches_reviewed_thresholds():
    """Keep the checked policy declaration aligned with compiled runtime limits."""
    config = json.loads((ROOT / "config" / "retention_v1.json").read_text())
    assert config == {"schema_version": 1, "status": "dry-run-default",
                      "incomplete_days": INCOMPLETE_DAYS,
                      "completed_files_days": COMPLETED_FILES_DAYS,
                      "receipts_days": RECEIPTS_DAYS, "bronze_days": BRONZE_DAYS,
                      "silver_days": SILVER_DAYS, "gold_days": GOLD_DAYS,
                      "ops_min_days": OPS_MIN_DAYS}


def _classify():
    """Load the pure completion classifier without importing Spark."""
    spec = importlib.util.spec_from_file_location("finalize_run", ROOT / "databricks" / "finalize_run.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.classify


class Jobs:
    """Remember token use while injecting an ambiguous first response."""

    def __init__(self, lost=False):
        """Start with one optional lost run-now acknowledgement."""
        self.lost = lost
        self.calls = []
        self.status_calls = 0
        self.state = {"life_cycle_state": "RUNNING"}

    def run_now(self, job_id, token, params):
        """Simulate server-side acceptance even when the response is lost."""
        self.calls.append((job_id, token, params))
        if self.lost:
            self.lost = False
            raise VolumeError("lost response")
        return 42 if len({call[1] for call in self.calls}) == 1 else 43

    def get_run(self, run_id):
        """Count observations to rule out waiting or polling."""
        self.status_calls += 1
        assert run_id == 42
        return self.state


def _landing():
    """Create a real verified batch in the in-memory Volume."""
    volume = MemoryVolume()
    batch = freeze_landing([Delivery("earthquake_events_dev", 0, 0, b"us1", b"{}", NOW)],
                           volume, lambda: NOW)
    return volume, batch["landing_batch_id"]


def test_lost_run_now_response_reuses_one_token_and_run():
    """The retry must keep the durable attempt and recover the accepted run."""
    volume, batch_id = _landing()
    jobs = Jobs(lost=True)
    with pytest.raises(VolumeError, match="lost response"):
        dispatch_once(volume, jobs, "rev-1", 123)
    dispatch_id = next(iter(name for name, is_dir in volume.list_entries("dispatch") if is_dir))
    assert volume.read_optional(f"dispatch/{dispatch_id}/intent.json")
    assert volume.read_optional(f"dispatch/{dispatch_id}/attempt-1-intent.json")
    assert volume.read_optional(f"dispatch/{dispatch_id}/attempt-1-receipt.json") is None
    outcome = dispatch_once(volume, jobs, "rev-1", 123)
    assert outcome["action"] == "SUBMITTED" and outcome["databricks_run_id"] == 42
    assert jobs.calls[0][1] == jobs.calls[1][1]
    assert jobs.calls[0][2] == jobs.calls[1][2]
    assert jobs.calls[0][2]["batch_id"] == batch_id


def test_active_run_observed_once_without_new_submission():
    """A later schedule makes one status request and exits."""
    volume, _ = _landing()
    jobs = Jobs()
    dispatch_once(volume, jobs, "rev-1", 123)
    outcome = dispatch_once(volume, jobs, "rev-1", 123)
    assert outcome["action"] == "ACTIVE"
    assert jobs.status_calls == 1 and len(jobs.calls) == 1
    jobs.state = {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}
    assert dispatch_once(volume, jobs, "rev-1", 123)["action"] == "NEEDS_RECONCILIATION"
    assert len(jobs.calls) == 1


def test_classified_retry_gets_new_token_with_same_dispatch():
    """Only a terminal retryable report permits a new attempt."""
    volume, _ = _landing()
    jobs = Jobs()
    first = dispatch_once(volume, jobs, "rev-1", 123)
    report = {"pipeline_run_id": first["pipeline_run_id"],
              "dispatch_id": first["dispatch_id"], "status": "FAILED", "retryable": True}
    volume.put_immutable(f"completion/{first['pipeline_run_id']}/report.json", json_bytes(report))
    second = dispatch_once(volume, jobs, "rev-1", 123)
    assert second["action"] == "SUBMITTED" and second["attempt_no"] == 2
    assert second["dispatch_id"] == first["dispatch_id"]
    assert second["pipeline_run_id"] != first["pipeline_run_id"]
    assert jobs.calls[0][1] != jobs.calls[1][1]


def test_corrected_revision_can_reprocess_terminal_code_failure():
    """A new code revision gets a new dispatch only after a terminal report."""
    volume, batch_id = _landing()
    jobs = Jobs()
    first = dispatch_once(volume, jobs, "rev-1", 123)
    report = {"pipeline_run_id": first["pipeline_run_id"],
              "dispatch_id": first["dispatch_id"], "status": "FAILED",
              "retryable": False, "error_class": "CODE"}
    volume.put_immutable(f"completion/{first['pipeline_run_id']}/report.json", json_bytes(report))
    second = dispatch_once(volume, jobs, "rev-2", 123)
    assert second["action"] == "SUBMITTED" and second["dispatch_id"] != first["dispatch_id"]
    assert jobs.calls[1][2]["batch_id"] == batch_id


def _metadata(age_days=40):
    """Represent one complete batch and its capture with stage and publication."""
    old = NOW - timedelta(days=age_days)
    cap = "c" * 64
    batch = "b" * 64
    run = "r" * 64
    captures = [dict(capture_id=cap, capture_status="PUBLISHED", fetched_at=old)]
    batches = [dict(landing_batch_id=batch, capture_ids=[cap], landing_status="VERIFIED",
                    expected_record_count=2, landed_record_count=2, verified_at=old)]
    runs = [dict(pipeline_run_id=run, batch_ids=[batch], run_status="SUCCEEDED")]
    stages = [dict(pipeline_run_id=run, stage_name=name, stage_status="COMMITTED")
              for name in ("BRONZE", "SILVER", "GOLD")]
    publications = [dict(publication_id="p" * 64, input_batch_ids=[batch],
                         pipeline_run_id=run, publication_status="ACTIVE", validation_status="PASS")]
    return captures, batches, runs, stages, publications


def test_active_publication_and_open_quarantine_block_retention():
    """Lineage and unresolved records protect otherwise old paths."""
    caps, batches, runs, stages, pubs = _metadata()
    assert safe_candidates(NOW, caps, batches, runs, stages, pubs, pubs[0]["publication_id"], set()) == (set(), set())
    pubs[0]["publication_status"] = "SUPERSEDED"
    assert safe_candidates(NOW, caps, batches, runs, stages, pubs, None, {"b" * 64}) == (set(), set())
    assert safe_candidates(NOW, caps, batches, runs, stages, pubs, None, set()) == ({"c" * 64}, {"b" * 64})
    runs[0]["run_status"] = "RUNNING"
    assert safe_candidates(NOW, caps, batches, runs, stages, pubs, None, set()) == (set(), set())


def test_reviewed_cleanup_rechecks_bytes_and_survives_partial_apply(tmp_path):
    """A modified file blocks deletion; repeated apply skips already removed files."""
    folder = tmp_path / "captures" / ("a" * 64)
    folder.mkdir(parents=True)
    (folder / "response.json").write_bytes(b"payload")
    (folder / "manifest.json").write_bytes(b"manifest")
    reviewed = file_plan(tmp_path, NOW, {"a" * 64}, set())
    assert len(plan_sha256(reviewed)) == 64
    (folder / "response.json").write_bytes(b"different")
    with pytest.raises(ContractError, match="changed"):
        apply_plan(tmp_path, reviewed, file_plan(tmp_path, NOW, {"a" * 64}, set()))
    (folder / "response.json").write_bytes(b"payload")
    assert apply_plan(tmp_path, reviewed, file_plan(tmp_path, NOW, {"a" * 64}, set()))["removed"] == 2
    assert apply_plan(tmp_path, reviewed, file_plan(tmp_path, NOW, {"a" * 64}, set()))["already_removed"] == 2


def test_completion_classification_rejects_quality_without_pointer_change():
    """A blocked Gold candidate remains terminal while a valid one succeeds."""
    classify = _classify()
    stages = [{"stage_name": name, "stage_status": "COMMITTED"} for name in ("BRONZE", "SILVER", "GOLD")]
    pub = [{"publication_id": "active", "publication_status": "ACTIVE", "validation_status": "PASS"}]
    assert classify(stages, [], pub, "active") == ("SUCCEEDED", False, "COMPLETE")
    checks = [{"stage_name": "GOLD", "check_status": "FAIL", "severity": "BLOCKER"}]
    assert classify(stages[:-1], checks, [], "old") == ("FAILED", False, "QUALITY")

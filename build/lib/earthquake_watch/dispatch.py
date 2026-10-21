"""Dispatch verified landing batches with immutable intent and token recovery.

The short-lived runner writes Volume evidence but never writes Delta. A lost
run-now response is retried with the exact same token and attempt identity.
"""

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .contracts import ContractError, json_bytes, stamp
from .landing import verify_landing
from .volume_client import VolumeError


HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MAX_ATTEMPTS = 3
ACTIVE_STATES = {"PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"}
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


class EvidenceVolume(Protocol):
    """Minimal immutable Volume operations used by the dispatcher."""

    def read_optional(self, relative: str) -> bytes | None: ...
    def put_immutable(self, relative: str, data: bytes) -> None: ...
    def list_entries(self, relative: str) -> list[tuple[str, bool]]: ...


class DatabricksJobs:
    """Issue bounded Jobs API calls without recording credentials or responses."""

    def __init__(self, host: str, token: str):
        """Bind requests to one HTTPS workspace and the configured credential."""
        if not re.fullmatch(r"https://[^/]+", host) or not token:
            raise ContractError("Databricks Jobs host or credential is missing")
        self.host, self.token = host, token

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Surface ambiguous transport failures for same-token recovery."""
        data = json_bytes(payload) if payload is not None else None
        request = Request(self.host + path, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=25) as response:
                result = json.loads(response.read())
        except HTTPError as exc:
            # A 5xx response can follow a successful submission. Never change token.
            if exc.code in (400, 401, 403, 404, 409):
                raise ContractError(f"Jobs API rejected request with HTTP {exc.code}") from exc
            raise VolumeError(f"Jobs API HTTP {exc.code}; keep the same dispatch attempt") from exc
        except (URLError, TimeoutError, ValueError) as exc:
            raise VolumeError("Jobs API response uncertain; keep the same dispatch attempt") from exc
        if not isinstance(result, dict):
            raise ContractError("Invalid Jobs API response")
        return result

    def run_now(self, job_id: int, token: str, params: dict[str, str]) -> int:
        """Submit one run, or recover its ID by repeating the same token."""
        result = self._request("POST", "/api/2.2/jobs/run-now", {
            "job_id": job_id, "idempotency_token": token, "job_parameters": params})
        run_id = result.get("run_id")
        if type(run_id) is not int or run_id <= 0:
            raise ContractError("Jobs API did not return a valid run ID")
        return run_id

    def get_run(self, run_id: int) -> dict[str, Any]:
        """Observe one run once; the caller never waits or polls."""
        result = self._request("GET", "/api/2.2/jobs/runs/get?" + urlencode({"run_id": run_id}))
        if result.get("run_id") != run_id or not isinstance(result.get("state"), dict):
            raise ContractError("Jobs API run identity or state mismatch")
        return result["state"]


def _read_json(volume: EvidenceVolume, path: str) -> dict[str, Any] | None:
    """Parse one immutable evidence file without treating corruption as absence."""
    raw = volume.read_optional(path)
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (UnicodeError, ValueError) as exc:
        raise ContractError(f"Invalid dispatch evidence at {path}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"Invalid dispatch evidence at {path}")
    return value


def _identity(batch_id: str, revision: str) -> str:
    """Pin a dispatch to one batch and deployed processing revision."""
    if not HEX64.fullmatch(batch_id) or not revision or len(revision) > 128:
        raise ContractError("Invalid dispatch batch or revision")
    return hashlib.sha256(f"{batch_id}|{revision}".encode()).hexdigest()


def _attempt(dispatch_id: str, number: int) -> dict[str, Any]:
    """Give each actual retry a new token while replaying uncertainty unchanged."""
    if number not in range(1, MAX_ATTEMPTS + 1):
        raise ContractError("Dispatch retry budget exceeded")
    return {"attempt_no": number,
            "pipeline_run_id": hashlib.sha256(f"{dispatch_id}|run|{number}".encode()).hexdigest(),
            "idempotency_token": hashlib.sha256(f"{dispatch_id}|token|{number}".encode()).hexdigest()}


def _receipt(volume: EvidenceVolume, dispatch_id: str, number: int) -> dict[str, Any] | None:
    """Find an acknowledged submission for exactly one attempt."""
    return _read_json(volume, f"dispatch/{dispatch_id}/attempt-{number}-receipt.json")


def _submit(volume: EvidenceVolume, jobs: DatabricksJobs, intent: dict[str, Any], number: int) -> dict[str, Any]:
    """Write intent first, then submit and record the returned run ID."""
    dispatch_id = intent["dispatch_id"]
    attempt = _attempt(dispatch_id, number)
    path = f"dispatch/{dispatch_id}/attempt-{number}-intent.json"
    existing = _read_json(volume, path)
    if existing is None:
        volume.put_immutable(path, json_bytes(attempt))
    elif existing != attempt:
        raise ContractError("Dispatch attempt identity changed")
    params = {"batch_id": intent["batch_id"], "pipeline_run_id": attempt["pipeline_run_id"],
              "revision": intent["revision"], "dispatch_id": dispatch_id,
              "attempt_no": str(number)}
    run_id = jobs.run_now(intent["job_id"], attempt["idempotency_token"], params)
    receipt = {"dispatch_id": dispatch_id, "attempt_no": number,
               "pipeline_run_id": attempt["pipeline_run_id"], "databricks_run_id": run_id,
               "job_id": intent["job_id"], "submitted_at": stamp(datetime.now(timezone.utc))}
    volume.put_immutable(f"dispatch/{dispatch_id}/attempt-{number}-receipt.json", json_bytes(receipt))
    return {"action": "SUBMITTED", **receipt}


def _intents(volume: EvidenceVolume) -> list[dict[str, Any]]:
    """Load and validate existing dispatches before choosing new work."""
    result = []
    for name, is_dir in volume.list_entries("dispatch"):
        if not is_dir or not HEX64.fullmatch(name):
            raise ContractError("Unexpected dispatch entry")
        intent = _read_json(volume, f"dispatch/{name}/intent.json")
        if not intent or intent.get("dispatch_id") != name or _identity(intent.get("batch_id", ""), intent.get("revision", "")) != name:
            raise ContractError("Dispatch intent identity mismatch")
        result.append(intent)
    return sorted(result, key=lambda x: (x["created_at"], x["dispatch_id"]))


def dispatch_once(volume: EvidenceVolume, jobs: DatabricksJobs, revision: str, job_id: int) -> dict[str, Any]:
    """Recover an unresolved attempt or submit at most one verified batch."""
    if type(job_id) is not int or job_id <= 0:
        raise ContractError("A valid Databricks Job ID is required")
    existing = _intents(volume)
    for intent in existing:
        dispatch_id = intent["dispatch_id"]
        attempts = [n for n in range(1, MAX_ATTEMPTS + 1)
                    if _read_json(volume, f"dispatch/{dispatch_id}/attempt-{n}-intent.json") is not None]
        if not attempts:
            raise ContractError("Dispatch has no attempt intent")
        number = max(attempts)
        receipt = _receipt(volume, dispatch_id, number)
        if receipt is None:
            return _submit(volume, jobs, intent, number)
        expected = _attempt(dispatch_id, number)
        if receipt.get("pipeline_run_id") != expected["pipeline_run_id"] or receipt.get("job_id") != job_id:
            raise ContractError("Dispatch receipt conflicts with attempt")
        completion = _read_json(volume, f"completion/{expected['pipeline_run_id']}/report.json")
        if completion is not None:
            if completion.get("pipeline_run_id") != expected["pipeline_run_id"] or completion.get("dispatch_id") != dispatch_id:
                raise ContractError("Completion lineage mismatch")
            if completion.get("status") == "SUCCEEDED":
                continue
            if completion.get("status") == "FAILED" and completion.get("retryable") is True and number < MAX_ATTEMPTS:
                if intent["job_id"] != job_id:
                    raise ContractError("Retry Job ID differs from immutable dispatch")
                return _submit(volume, jobs, intent, number + 1)
            if completion.get("status") != "FAILED":
                raise ContractError("Invalid completion status")
            continue
        if intent["job_id"] != job_id:
            raise ContractError("Active dispatch belongs to a different Job ID")
        state = jobs.get_run(receipt["databricks_run_id"])
        lifecycle = state.get("life_cycle_state")
        if lifecycle in ACTIVE_STATES:
            return {"action": "ACTIVE", "dispatch_id": dispatch_id,
                    "databricks_run_id": receipt["databricks_run_id"], "state": lifecycle}
        if lifecycle in TERMINAL_STATES:
            # A terminal Jobs result is not a substitute for Databricks's report.
            return {"action": "NEEDS_RECONCILIATION", "dispatch_id": dispatch_id,
                    "databricks_run_id": receipt["databricks_run_id"], "state": lifecycle}
        raise ContractError("Unrecognized Jobs run lifecycle")

    dispatched = set()
    for intent in existing:
        dispatch_id = intent["dispatch_id"]
        numbers = [n for n in range(1, MAX_ATTEMPTS + 1)
                   if _read_json(volume, f"dispatch/{dispatch_id}/attempt-{n}-intent.json") is not None]
        last = _attempt(dispatch_id, max(numbers))
        completion = _read_json(volume, f"completion/{last['pipeline_run_id']}/report.json")
        # A corrected code revision may explicitly reprocess a terminal code
        # failure. Successful and quality-rejected batches stay accounted for.
        if (intent["revision"] == revision or not completion or completion["status"] == "SUCCEEDED"
                or completion.get("error_class") not in ("CODE", "UNCLASSIFIED")):
            dispatched.add(intent["batch_id"])
    batches = []
    for name, is_dir in volume.list_entries("landing"):
        if not is_dir or not HEX64.fullmatch(name):
            continue
        manifest, _ = verify_landing(volume, name)
        if name not in dispatched:
            batches.append((manifest["created_at"], name, manifest))
    if not batches:
        return {"action": "NO_PENDING_BATCH"}
    _, batch_id, manifest = min(batches)
    dispatch_id = _identity(batch_id, revision)
    raw_manifest = volume.read_optional(f"landing/{batch_id}/manifest.json")
    assert raw_manifest is not None
    intent = {"dispatch_id": dispatch_id, "batch_id": batch_id,
              "revision": revision, "job_id": job_id,
              "landing_manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
              "created_at": stamp(datetime.now(timezone.utc)), "record_count": manifest["record_count"]}
    volume.put_immutable(f"dispatch/{dispatch_id}/intent.json", json_bytes(intent))
    return _submit(volume, jobs, intent, 1)


def verify_dispatch(volume: EvidenceVolume, dispatch_id: str, attempt_no: int,
                    batch_id: str, pipeline_run_id: str, revision: str) -> dict[str, Any]:
    """Recheck source files inside Databricks before creating any Ops parent."""
    if dispatch_id != _identity(batch_id, revision):
        raise ContractError("Dispatch identity differs from Job parameters")
    intent = _read_json(volume, f"dispatch/{dispatch_id}/intent.json")
    attempt = _read_json(volume, f"dispatch/{dispatch_id}/attempt-{attempt_no}-intent.json")
    if not intent or not attempt or attempt != _attempt(dispatch_id, attempt_no):
        raise ContractError("Missing or conflicting dispatch attempt")
    if intent["batch_id"] != batch_id or intent["revision"] != revision or attempt["pipeline_run_id"] != pipeline_run_id:
        raise ContractError("Dispatch parameters do not match immutable intent")
    verify_landing(volume, batch_id)
    raw = volume.read_optional(f"landing/{batch_id}/manifest.json")
    if raw is None or hashlib.sha256(raw).hexdigest() != intent["landing_manifest_sha256"]:
        raise ContractError("Landing manifest changed after dispatch")
    return intent

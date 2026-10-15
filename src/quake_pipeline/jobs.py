"""Trigger a configured Databricks pipeline Job and await a bounded result.

The launch token is stable within a GitHub attempt. Captures are already durable
when this runs, so a job failure never requires refetching the original response.
"""

import os
import time
import uuid
from typing import Callable
from dataclasses import replace

from .cli import emit
from .config import Settings
from .contracts import decode_json, json_bytes, stable_id
from .errors import BudgetExceeded, ConfigurationError, ContractError, PipelineError
from .http import HTTPClient


def sync_ledger(settings: Settings, job_id: int, token: str, client: HTTPClient,
                sleeper: Callable[[float], None] = time.sleep,
                monotonic: Callable[[], float] = time.monotonic,
                wait_seconds: int = 600) -> str:
    """Launch once per token, then poll the existing run rather than creating duplicates."""
    settings.require_cloud()
    if not 60 <= wait_seconds <= 2400:
        raise ConfigurationError("Job wait must be between 60 and 2400 seconds")
    if job_id <= 0:
        raise ConfigurationError("EQ_CAPTURE_LEDGER_JOB_ID must be a positive numeric Job ID")
    headers = {"Authorization": f"Bearer {settings.token}", "Content-Type": "application/json"}
    response = client.request("databricks", "POST", settings.host + "/api/2.2/jobs/run-now",
                              headers=headers, body=json_bytes({"job_id": job_id,
                              "job_parameters": {"volume_root": settings.volume_root},
                              "idempotency_token": token}))
    launched = decode_json(response.body)
    if not isinstance(launched, dict) or type(launched.get("run_id")) is not int:
        raise ContractError("Databricks launch response has no numeric run_id")
    run_id = str(launched["run_id"])
    emit({"event": "LEDGER_JOB_STARTED", "databricks_run_id": run_id})
    deadline = monotonic() + wait_seconds
    while monotonic() < deadline:
        result = client.request("databricks", "GET", settings.host +
                                "/api/2.2/jobs/runs/get?run_id=" + run_id, headers=headers)
        run = decode_json(result.body)
        if not isinstance(run, dict) or not isinstance(run.get("state"), dict):
            raise ContractError("Databricks run response is missing its state")
        state = run["state"]
        if state.get("result_state") == "SUCCESS":
            return run_id
        if state.get("life_cycle_state") in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            raise PipelineError(f"Databricks pipeline job failed; inspect Databricks run {run_id}")
        sleeper(10)
    raise BudgetExceeded(f"Databricks pipeline job still running; inspect Databricks run {run_id} before recovery")


def main() -> int:
    """Read job configuration and expose safe success/failure evidence in Actions."""
    try:
        raw_id = os.getenv("EQ_CAPTURE_LEDGER_JOB_ID", "").strip()
        if not raw_id.isdigit():
            raise ConfigurationError("Set repository variable EQ_CAPTURE_LEDGER_JOB_ID")
        raw_wait = os.getenv("EQ_LEDGER_WAIT_SECONDS", "1800").strip() or "1800"
        if not raw_wait.isdigit() or not 60 <= int(raw_wait) <= 2400:
            raise ConfigurationError("EQ_LEDGER_WAIT_SECONDS must be 60..2400")
        wait_seconds = int(raw_wait)
        settings = replace(Settings.from_env(), run_seconds=wait_seconds + 120, max_requests=1500)
        token = stable_id("ledger-launch-v1", os.getenv("GITHUB_RUN_ID", uuid.uuid4().hex),
                          os.getenv("GITHUB_RUN_ATTEMPT", "1"))
        events = []
        run_id = sync_ledger(settings, int(raw_id), token, HTTPClient(settings, events.append),
                             wait_seconds=wait_seconds)
        print(f"PASS Databricks pipeline Job: run_id={run_id}")
        return 0
    except PipelineError as error:
        emit({"event": "LEDGER_JOB_FAILED", "error_code": error.code, "message": str(error)})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

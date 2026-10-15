"""Trigger the Sprint 2 Databricks capture-ledger job and await a bounded result.

The launch token is stable within a GitHub attempt. Captures are already durable
when this runs, so a job failure never requires refetching the original response.
"""

import os
import time
import uuid
from typing import Callable

from .cli import emit
from .config import Settings
from .contracts import decode_json, json_bytes, stable_id
from .errors import BudgetExceeded, ConfigurationError, ContractError, PipelineError
from .http import HTTPClient


def sync_ledger(settings: Settings, job_id: int, token: str, client: HTTPClient,
                sleeper: Callable[[float], None] = time.sleep,
                monotonic: Callable[[], float] = time.monotonic) -> str:
    """Launch once per token, then poll the existing run rather than creating duplicates."""
    settings.require_cloud()
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
    deadline = monotonic() + 600
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
            raise PipelineError(f"Capture ledger job failed; inspect Databricks run {run_id}")
        sleeper(10)
    raise BudgetExceeded(f"Ledger job still running; inspect Databricks run {run_id} before recovery")


def main() -> int:
    """Read job configuration and expose safe success/failure evidence in Actions."""
    try:
        raw_id = os.getenv("EQ_CAPTURE_LEDGER_JOB_ID", "").strip()
        if not raw_id.isdigit():
            raise ConfigurationError("Set repository variable EQ_CAPTURE_LEDGER_JOB_ID")
        settings = Settings.from_env()
        token = stable_id("ledger-launch-v1", os.getenv("GITHUB_RUN_ID", uuid.uuid4().hex),
                          os.getenv("GITHUB_RUN_ATTEMPT", "1"))
        events = []
        run_id = sync_ledger(settings, int(raw_id), token, HTTPClient(settings, events.append))
        print(f"PASS Sprint 2 Databricks capture ledger: run_id={run_id}")
        return 0
    except PipelineError as error:
        emit({"event": "LEDGER_JOB_FAILED", "error_code": error.code, "message": str(error)})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

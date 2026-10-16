"""Adapt Databricks REST and a cloud-only automation journal for short ticks.

Only the serialized GitHub controller writes the journal. Earthquake payloads
remain in the managed landing Volume; the workspace journal holds metadata only.
"""

import ast
import base64
import re
from pathlib import Path
from urllib.parse import urlencode

from .contracts import decode_json, json_bytes
from .errors import ConfigurationError, ContractError, HTTPFailure
from .http import transport

WORKSPACE = "/Shared/earthquake-watch"
STATE_PATH = WORKSPACE + "/_automation_state"
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def inline_notebook(path, directory, stack=(), libraries=None):
    """Build one source notebook so nested %run widget defaults cannot mask Job parameters."""
    path, directory = Path(path).resolve(), Path(directory).resolve()
    if path.parent != directory or path in stack:
        raise ContractError("Notebook include escapes its directory or contains a cycle")
    libraries = set() if libraries is None else libraries
    output = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"# MAGIC %run \./([A-Za-z0-9_]+)", line)
        if match:
            child = directory / (match[1] + ".py")
            if child.name.endswith("_library.py"):
                if child.name in libraries:
                    continue
                libraries.add(child.name)
            output.append("# COMMAND ----------\n# Included source: " + child.name)
            output.append(inline_notebook(child, directory, (*stack, path), libraries))
            output.append("# COMMAND ----------")
        else:
            output.append(line)
    source = "\n".join(output) + "\n"
    if len(source.encode()) > 2000000:
        raise ContractError("Consolidated notebook exceeds the project source limit")
    compile(source, str(path), "exec")
    return source


class CloudAPI:
    """Use bounded authenticated requests without printing credentials or provider bodies."""

    def __init__(self, settings, http, sender=transport):
        """Share the established retry policy for GET and tokenized run-now requests."""
        self.settings, self.http, self.sender = settings, http, sender

    def request(self, method, path, body=None, expected=(200,)):
        """Never blindly retry an unkeyed configuration POST after an ambiguous reply."""
        if not path.startswith("/api/"):
            raise ConfigurationError("Expected a Databricks API-relative path")
        headers = {"Authorization": f"Bearer {self.settings.token}", "Content-Type": "application/json"}
        payload = json_bytes(body) if body is not None else None
        if method == "GET" or path == "/api/2.2/jobs/run-now":
            response = self.http.request("databricks", method, self.settings.host + path,
                headers=headers, body=payload, expected=expected)
        else:
            response = self.sender(method, self.settings.host + path, headers, payload,
                self.settings.timeout_seconds, self.settings.max_response_bytes)
            if response.status not in expected:
                raise HTTPFailure("databricks", response.status)
        return None if response.status == 404 else (decode_json(response.body) if response.body else {})

    def named_jobs(self, name):
        """Resolve exact project Job names without asking an operator to copy IDs."""
        jobs, token, seen = [], None, set()
        while True:
            query = {"name": name, "limit": 100}
            if token:
                query["page_token"] = token
            page = self.request("GET", "/api/2.2/jobs/list?" + urlencode(query))
            jobs.extend(j for j in page.get("jobs", [])
                        if j.get("settings", {}).get("name", "").lower() == name.lower())
            token = page.get("next_page_token")
            if not token:
                break
            if token in seen:
                raise ContractError("Job discovery pagination token repeated")
            seen.add(token)
        if len(jobs) > 1:
            raise ConfigurationError("Duplicate project Job names must be resolved before mutation")
        return jobs

    def project_job_ids(self, requested="", capture_requested="", directory="databricks"):
        """Adopt the existing project Job, or create its one Serverless task once."""
        name = "earthquake-dev-handoff-ledger"
        job_id = None
        requested = requested.strip()
        if requested.isdigit():
            old = self.request("GET", "/api/2.2/jobs/get?job_id=" + requested, expected=(200, 404))
            if old is not None and old.get("settings", {}).get("name") == name:
                job_id = int(requested)
        if job_id is None:
            found = self.named_jobs(name)
            if found:
                job_id = int(found[0]["job_id"])
            else:
                self.deploy(directory)
                # Do not retry an ambiguous create. The next scheduled tick lists
                # the exact name and adopts any Job created before a lost reply.
                created = self.request("POST", "/api/2.2/jobs/create", {"name": name,
                    "max_concurrent_runs": 1, "queue": {"enabled": False}, "timeout_seconds": 7500,
                    "tasks": [{"task_key": "automated_setup", "timeout_seconds": 7200,
                        "max_retries": 0, "notebook_task": {
                            "notebook_path": WORKSPACE + "/automated_setup", "source": "WORKSPACE"}}]})
                job_id = int(created["job_id"])
        ids = [job_id]
        capture_found = False
        capture_requested = capture_requested.strip()
        if capture_requested.isdigit():
            old = self.request("GET", "/api/2.2/jobs/get?job_id=" + capture_requested, expected=(200, 404))
            if old is not None and old.get("settings", {}).get("name") == "earthquake-dev-capture-ledger":
                ids.append(int(capture_requested))
                capture_found = True
        if not capture_found:
            ids.extend(int(j["job_id"]) for j in self.named_jobs("earthquake-dev-capture-ledger"))
        return job_id, sorted(set(ids))

    def active(self, job_ids):
        """Discover all active project runs with full pagination before any new writer starts."""
        runs = []
        for job_id in sorted(set(job_ids)):
            settings = self.request("GET", "/api/2.2/jobs/get?job_id=" + str(int(job_id)))["settings"]
            if not settings.get("name", "").startswith("earthquake-dev-"):
                raise ConfigurationError("Configured Job ID does not belong to the earthquake project")
            token, seen = None, set()
            while True:
                query = {"job_id": job_id, "active_only": "true", "limit": 25}
                if token:
                    query["page_token"] = token
                page = self.request("GET", "/api/2.2/jobs/runs/list?" + urlencode(query))
                runs.extend(page.get("runs", []))
                token = page.get("next_page_token")
                if not token:
                    break
                if token in seen:
                    raise ContractError("Jobs pagination token repeated")
                seen.add(token)
        return runs

    def pause_schedules(self, job_ids):
        """Stop old project Job schedules before the explicit cross-service reset."""
        for job_id in sorted(set(job_ids)):
            old = self.request("GET", "/api/2.2/jobs/get?job_id=" + str(int(job_id)))["settings"]
            if not old.get("name", "").startswith("earthquake-dev-"):
                raise ConfigurationError("Refuse to pause a non-project Job")
            if old.get("schedule") and old["schedule"].get("pause_status") != "PAUSED":
                self.request("POST", "/api/2.2/jobs/update", {"job_id": int(job_id),
                    "new_settings": {"schedule": {**old["schedule"], "pause_status": "PAUSED"}}})

    def run(self, run_id):
        """Read one existing run without resubmitting a task."""
        return self.request("GET", "/api/2.2/jobs/runs/get?run_id=" + str(int(run_id)))

    def submit(self, job_id, token, parameters):
        """A journaled token survives crashes before the run ID is saved."""
        return self.request("POST", "/api/2.2/jobs/run-now", {"job_id": int(job_id),
            "idempotency_token": token, "job_parameters": parameters, "queue": {"enabled": False}})["run_id"]

    def cancel(self, run_id):
        """Request cancellation of an exactly resolved project run during explicit reset."""
        result = self.request("POST", "/api/2.2/jobs/runs/cancel", {"run_id": int(run_id)}, expected=(200, 400))
        if result.get("error_code") and self.run(run_id).get("state", {}).get("life_cycle_state") not in TERMINAL:
            raise ContractError("Project run cancellation was rejected")

    def configure(self, job_id, setup=False):
        """Replace the known project Job with one Serverless notebook task."""
        existing = self.request("GET", "/api/2.2/jobs/get?job_id=" + str(int(job_id)))["settings"]
        if existing.get("name") != "earthquake-dev-handoff-ledger":
            raise ConfigurationError("EQ_HANDOFF_LEDGER_JOB_ID must name earthquake-dev-handoff-ledger")
        tasks = existing.get("tasks", [])
        # Earlier sprints may have used a classic/multi-task layout. Resetting
        # this exact allowlisted Job removes that compute reference and migrates
        # it to the single managed Serverless task below.
        task = {"task_key": "automated_setup" if setup else "automated_pipeline",
            "notebook_task": {"notebook_path": WORKSPACE + ("/automated_setup" if setup else "/automated_pipeline"),
                              "source": "WORKSPACE"},
            "timeout_seconds": 7200, "max_retries": 0}
        if tasks and tasks[0].get("environment_key"):
            task["environment_key"] = tasks[0]["environment_key"]
        settings = {"name": "earthquake-dev-handoff-ledger", "max_concurrent_runs": 1,
            "timeout_seconds": 7500, "queue": {"enabled": False}, "tasks": [task],
            "parameters": [{"name": k, "default": v} for k, v in {
                "volume_root": "/Volumes/earthquake_dev/ops/landing", "gold_action": "auto",
                "reconcile_mode": "verify", "maintenance_action": "auto",
                "automation_operation_id": "", "reset_id": "", "reset_confirmation": "",
                "reset_smoke": "false"}.items()]}
        if existing.get("environments"):
            settings["environments"] = existing["environments"]
        self.request("POST", "/api/2.2/jobs/reset", {"job_id": int(job_id), "new_settings": settings})

    def deploy(self, directory):
        """Import repository code only; never copy cloud data to the runner."""
        self.request("POST", "/api/2.0/workspace/mkdirs", {"path": WORKSPACE})
        for name in ("automated_setup", "automated_pipeline"):
            path = Path(directory) / (name + ".py")
            source = inline_notebook(path, directory).encode()
            self.request("POST", "/api/2.0/workspace/import", {"path": WORKSPACE + "/" + path.stem,
                "format": "SOURCE", "language": "PYTHON", "overwrite": True,
                "content": base64.b64encode(source).decode()})


class WorkspaceJournal:
    """Persist small recoverable controller state outside the resettable data schemas."""

    def __init__(self, api):
        """Bind one fixed private workspace object, never a caller-supplied path."""
        self.api = api

    def load(self):
        """Parse an assignment as data; the journal is never executed as Python."""
        result = self.api.request("GET", "/api/2.0/workspace/export?" + urlencode({
            "path": STATE_PATH, "format": "SOURCE"}), expected=(200, 404))
        if result is None:
            return None
        try:
            source = base64.b64decode(result["content"], validate=True).decode()
            nodes = ast.parse(source).body
            if (len(nodes) != 1 or not isinstance(nodes[0], ast.Assign)
                    or len(nodes[0].targets) != 1 or not isinstance(nodes[0].targets[0], ast.Name)
                    or nodes[0].targets[0].id != "STATE_JSON"):
                raise ValueError()
            value = decode_json(ast.literal_eval(nodes[0].value).encode())
            if value.get("controller_version") != 2:
                raise ValueError()
            return value
        except (KeyError, ValueError, TypeError, SyntaxError):
            raise ContractError("Automation journal is invalid; preserve it for investigation") from None

    def save(self, state):
        """Overwrite under the repository's single-writer lock and verify exact readback."""
        encoded = json_bytes(state).decode()
        source = "# Databricks notebook source\n# Automation metadata; do not run or edit.\nSTATE_JSON = " + repr(encoded) + "\n"
        self.api.request("POST", "/api/2.0/workspace/mkdirs", {"path": WORKSPACE})
        self.api.request("POST", "/api/2.0/workspace/import", {"path": STATE_PATH,
            "format": "SOURCE", "language": "PYTHON", "overwrite": True,
            "content": base64.b64encode(source.encode()).decode()})
        if self.load() != state:
            raise ContractError("Automation journal write could not be verified")

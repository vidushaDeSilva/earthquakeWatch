"""Advance a fully cloud-hosted pipeline through short, restartable scheduled ticks.

Every external Job launch is preceded by a durable token. The controller never
waits for a long Job, never collects ahead of unfinished work, and never equates
an unfinished pass with a failed data transaction. One repository concurrency
group is the exclusive writer; direct notebook launches are not routine operation.
"""

import argparse
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .automation_cloud import CloudAPI, TERMINAL, WorkspaceJournal
from .automation_reset import KafkaReset
from .capture import Collector
from .config import Settings
from .contracts import SOURCES, collection_window, decode_json, digest, json_bytes, parse_utc, stable_id, utc_text
from .errors import BudgetExceeded, ConfigurationError, ContractError, DeferredError, HTTPFailure, PipelineError, TransportFailure
from .http import HTTPClient
from .kafka_client import KafkaClient
from .kafka_config import KafkaSettings
from .landing import land
from .outbox import bind_stream
from .reconciliation import CatalogCollector, recover_captures
from .volume import Volume


class Runtime:
    """Perform bounded source/Kafka work only when no Databricks writer is active."""

    def __init__(self, settings, http, stream, events):
        """Retain only in-memory payloads and managed Volume storage."""
        self.settings, self.http, self.stream, self.events = settings, http, stream, events
        self.volume, self.kafka = Volume(settings, http), None

    def feeds(self, state, day_only=False):
        """Retry a fixed observation window; a refresh uses its separately saved epoch."""
        key = "refresh_at" if day_only else "cycle_started_at"
        at = parse_utc(state[key])
        epoch = stable_id("auto-feed-v2", state["cycle_id"], state[key])[:32]
        collector = Collector(replace(self.settings, collection_generation=epoch), self.http, self.volume)
        for mode, sources in SOURCES.items():
            if day_only and mode != "hourly" and not state.get("refresh_all"):
                continue
            start, end = collection_window(mode, None, at)
            for source in sources:
                if (source.source_id == "usgs-m2p5-month" and not state.get("month_due", True)
                        and not state.get("refresh_all")):
                    continue
                collector.collect(source, mode, start, end, state["cycle_id"], True)

    def checks(self, state):
        """Collect the exact saved ID plan, reusing already frozen API responses."""
        raw = self.volume.read_optional("reconciliation/plans/" + state["plan_id"] + "/plan.json")
        if raw is None:
            raise ContractError("The exported tracked-event plan is missing")
        plan = decode_json(raw)
        if plan.get("plan_id") != state["plan_id"]:
            raise ContractError("Tracked-event plan identity differs")
        epoch = stable_id("auto-check-v2", state["cycle_id"], state["refresh_at"])[:32]
        collector = CatalogCollector(self.settings, self.http, self.volume, epoch)
        at = parse_utc(state["refresh_at"])
        for event_id in plan["event_ids"]:
            collector.collect({"eventid": event_id}, "EVENT_CHECK", state["cycle_id"], at, at)

    def drain(self, state):
        """Continue frozen captures and offset-safe landing without fetching more feeds."""
        self.kafka = self.kafka or KafkaClient(self.stream)
        bind_stream(self.volume, self.kafka, self.stream)
        emit = lambda value: print(json_bytes(value).decode(), flush=True)
        recover_captures(self.settings, self.http, self.volume, self.kafka, self.stream, emit, maximum=32)
        land(self.volume, self.kafka, self.stream, state["cycle_id"], emit)

    def seal_run(self, operation_id, state):
        """Provide a frozen Sprint 2 run report before the lakehouse reader starts."""
        at = utc_text(datetime.now(timezone.utc))
        report = {"report_version": 1, "run_id": operation_id,
            "run_kind": "RECOVERY", "github_run_id": os.getenv("GITHUB_RUN_ID", "automated"),
            "github_attempt": int(os.getenv("GITHUB_RUN_ATTEMPT", "1")),
            "code_commit": os.getenv("GITHUB_SHA", "unrecorded"), "started_at": at,
            "finished_at": at, "window_start": state["cycle_started_at"], "window_end": at,
            "status": "SUCCESS", "error_class": None, "attempts": list(self.events)}
        # Captures name the stable cycle as first_run_id; it needs a real ledger
        # parent, independently of each processing pass's unique HTTP audit.
        for run_id, attempts in ((state["cycle_id"], []), (operation_id, list(self.events))):
            path = "runs/" + run_id + "/report.json"
            if self.volume.read_optional(path) is None:
                self.volume.put_immutable(path, json_bytes({**report, "run_id": run_id, "attempts": attempts}))

    def report(self, operation_id):
        """Require the exact Job pass report, never infer completeness from Job success."""
        raw = self.volume.read_optional("automation/reports/" + operation_id + ".json")
        if raw is None:
            raise ContractError("Successful Job did not seal its automation report")
        report = decode_json(raw)
        if report.get("operation_id") != operation_id or report.get("report_version") != 2:
            raise ContractError("Automation pass report identity differs")
        return report

    def close(self):
        """Close networking without changing any offsets automatically."""
        if self.kafka is not None:
            self.kafka.close()


class Controller:
    """Persist intention, advance one phase, and leave continuation to the next tick."""

    def __init__(self, journal, api, runtime_factory, reset_factory, job_id, job_ids,
                 generation, group, revision, now=None, notebook_directory="databricks"):
        """Inject service adapters and time so interruptions can be tested offline."""
        self.journal, self.api, self.runtime_factory, self.reset_factory = journal, api, runtime_factory, reset_factory
        self.job_id, self.job_ids = job_id, job_ids
        self.generation, self.group, self.revision = generation, group, revision
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.notebook_directory = notebook_directory

    def save(self, state):
        """Persist the latest metadata before relying on any external side effect."""
        self.journal.save(state)

    def begin_reset(self, state, reset_id, confirmation, smoke):
        """Authorize one scoped reset; rerunning its ID resumes instead of erasing twice."""
        import re
        if confirmation != "RESET_EARTHQUAKE_DEV" or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", reset_id):
            raise ConfigurationError("Reset requires RESET_EARTHQUAKE_DEV and a bounded reset ID")
        if state.get("last_reset_id") == reset_id:
            return
        if state.get("reset_id") == reset_id:
            if state["status"].startswith("HALTED"):
                state.update(status="RESETTING", errors=0, retries=0, next_due=None)
                self.save(state)
            return
        if state.get("reset_id"):
            raise ContractError("Another reset is incomplete; resume its existing ID")
        for field in ("last_report", "last_month_feed_at", "after_setup_phase", "check_rounds", "halt_revision"):
            state.pop(field, None)
        state.update(phase="RESET_STOP", reset_id=reset_id, reset_smoke=smoke,
            generation=stable_id("reset-generation-v2", reset_id)[:24],
            group="earthquake-dev-landing-" + stable_id("reset-group-v2", reset_id)[:16],
            active=None, errors=0, retries=0, pass_number=0, no_progress=0,
            last_progress=None, status="RESETTING", kafka_reset={}, next_due=None,
            cycle_id=stable_id("reset-cycle-v2", reset_id), cycle_started_at=utc_text(self.now()),
            operation_number=0, month_due=True, deployed_revision=None)
        self.save(state)

    def launch(self, state, kind):
        """Save the launch token before POST, so a lost ACK resolves to the same run."""
        self.api.configure(self.job_id, setup=kind == "SETUP")
        number = state.get("operation_number", 0) + 1
        token = stable_id("auto-job-v2", state["cycle_id"], kind, number)
        params = {"volume_root": "/Volumes/earthquake_dev/ops/landing", "automation_operation_id": token,
            "gold_action": "auto" if kind == "VERIFY" else "preview",
            "maintenance_action": "auto" if kind == "VERIFY" else "none",
            "reconcile_mode": "verify" if kind == "VERIFY" else "plan",
            "bronze_recheck": "false", "silver_recheck": "false",
            "bronze_fail_at": "none", "silver_fail_at": "none", "gold_fail_at": "none",
            "maintenance_fail_at": "none", "gold_retry_publication_id": "", "gold_as_of_utc": "",
            "reset_id": state.get("reset_id", "") if kind == "SETUP" else "",
            "reset_confirmation": "RESET_EARTHQUAKE_DEV" if kind == "SETUP" and state.get("reset_id") else "",
            "reset_smoke": str(state.get("reset_smoke", False)).lower()}
        state["operation_number"] = number
        state["active"] = {"token": token, "run_id": None, "kind": kind, "parameters": params,
                           "started_at": utc_text(self.now()), "revision": self.revision}
        self.save(state)
        state["active"]["run_id"] = self.api.submit(self.job_id, token, params)
        self.save(state)

    def poll(self, state, runtime):
        """Keep the same run across ticks; retries begin only after terminal failure."""
        active = state["active"]
        if active["run_id"] is None:
            active["run_id"] = self.api.submit(self.job_id, active["token"], active["parameters"])
            self.save(state)
        run = self.api.run(active["run_id"])
        if int(run.get("job_id", self.job_id)) != self.job_id:
            raise ContractError("Stored run belongs to another Job")
        result = run.get("state", {})
        if result.get("life_cycle_state") not in TERMINAL:
            state["status"] = "WORK_IN_PROGRESS"
            if self.now() - parse_utc(active["started_at"]) > timedelta(hours=3):
                state["status"] = "OVERDUE_JOB"
            self.save(state)
            return
        if result.get("result_state") != "SUCCESS":
            messages = [result.get("state_message", "")] + [t.get("state", {}).get("state_message", "")
                for t in run.get("tasks", [])]
            quota = any(any(word in message.lower() for word in
                ("quota", "fair usage", "resource_exhausted", "daily limit")) for message in messages)
            state.update(active=None, retries=state.get("retries", 0) + 1, status="RETRY_SCHEDULED")
            state["last_failed_run_id"] = active["run_id"]
            state["last_failed_result"] = result.get("result_state", "UNKNOWN")
            state["next_due"] = utc_text(self.now() + timedelta(minutes=5 * 2 ** state["retries"]))
            if state["retries"] >= 3:
                state["status"] = "HALTED_JOB_FAILURE"
                state["halt_revision"] = self.revision
            if quota:
                state.update(status="QUOTA_PAUSED", retries=0, refresh_all=True,
                    next_due=utc_text(self.now() + timedelta(hours=24)))
            self.save(state)
            return
        if active["kind"] == "SETUP":
            after = state.pop("after_setup_phase", "FEEDS")
            state.update(active=None, phase=after, deployed_revision=active["revision"],
                         retries=0, errors=0, status="WORK_IN_PROGRESS")
            if after == "FEEDS":
                state["cycle_started_at"] = utc_text(self.now())
            if state.get("reset_id"):
                state["last_reset_id"] = state.pop("reset_id")
            self.save(state)
            return
        report = runtime.report(active["token"])
        pending = report["pending_captures"] + report["pending_batches"]
        state.update(active=None, retries=0, errors=0, plan_id=report["plan_id"], last_report=report)
        if pending:
            previous = state.get("last_progress")
            state["no_progress"] = state.get("no_progress", 0) + 1 if previous is not None and pending >= previous else 0
            state.update(last_progress=pending, phase="DRAIN_" + active["kind"], status="CONTINUATION_SCHEDULED")
            if state["no_progress"] >= 3:
                state["status"] = "HALTED_NO_PROGRESS"
        elif active["kind"] == "SEED":
            state.update(phase="CHECKS", status="CONTINUATION_SCHEDULED", no_progress=0, last_progress=None)
            state["refresh_at"] = utc_text(self.now())
            state["refresh_all"] = (state.get("refresh_all", False) or
                self.now() - parse_utc(state["cycle_started_at"]) >= timedelta(hours=24))
        elif not report["issues"] and report["gold"].get("action") == "publish":
            state.update(phase="IDLE", status="COMPLETE", no_progress=0, last_progress=None,
                         completed_at=utc_text(self.now()), next_due=utc_text(self.now() + timedelta(hours=1)))
            if state.get("month_due", True):
                state["last_month_feed_at"] = state["cycle_started_at"]
            if state.get("refresh_all"):
                state["last_month_feed_at"] = state["refresh_at"]
        else:
            repairable = all(issue.startswith(("TRACKED_EVENT_UNVERIFIED:", "STALE_OR_UNVERIFIED_SOURCE:",
                "MISSING_RECONCILE_SOURCE:", "INITIAL_SOURCE_PLAN")) for issue in report["issues"])
            state["check_rounds"] = state.get("check_rounds", 0) + 1
            state.update(phase="CHECKS", status="CONTINUATION_SCHEDULED", refresh_at=utc_text(self.now()))
            if not repairable or state["check_rounds"] >= 3:
                state["status"] = "HALTED_DATA_QUALITY"
        if state["status"].startswith("HALTED"):
            state["halt_revision"] = self.revision
        self.save(state)

    def tick(self, reset_id="", confirmation="", smoke=True):
        """Perform at most one bounded transition; scheduled ticks finish the rest."""
        state = self.journal.load() or {"controller_version": 2, "phase": "SETUP", "status": "WORK_IN_PROGRESS",
            "generation": self.generation, "group": self.group, "errors": 0,
            "cycle_id": stable_id("initial-auto-v2", self.now().isoformat()),
            "cycle_started_at": utc_text(self.now())}
        if reset_id:
            self.begin_reset(state, reset_id, confirmation, smoke)
        if state["status"].startswith("HALTED"):
            if state.get("halt_revision") != self.revision:
                state.update(status="WORK_IN_PROGRESS", errors=0, retries=0)
            else:
                return state
        if (state.get("next_due") and parse_utc(state["next_due"]) > self.now()
                and (not state.get("active") or state["status"] in ("SERVICE_COOLDOWN", "RETRY_SCHEDULED"))):
            return state
        if state["status"] == "QUOTA_PAUSED" and state["phase"] == "CHECKS":
            state["refresh_at"] = utc_text(self.now())
            self.save(state)
        runtime = self.runtime_factory(state)
        try:
            if state.get("active"):
                self.poll(state, runtime)
                if (state.get("active") or state["status"].startswith("HALTED")
                        or state["status"] in ("RETRY_SCHEDULED", "QUOTA_PAUSED") or state["phase"] == "IDLE"):
                    return state
            if state["phase"] == "RESET_STOP":
                self.api.pause_schedules(self.job_ids)
            others = self.api.active(self.job_ids)
            if others:
                if state["phase"] == "RESET_STOP":
                    for run in others:
                        self.api.cancel(run["run_id"])
                state["status"] = "WAITING_FOR_EXISTING_JOB"
                self.save(state)
                return state
            if state["phase"] == "RESET_STOP":
                resetter = self.reset_factory(state)
                try:
                    if not resetter.advance(state, self.save):
                        return state
                finally:
                    resetter.close()
                state.update(phase="SETUP", deployed_revision=None)
                self.save(state)
            if state["phase"] == "SETUP" or state.get("deployed_revision") != self.revision:
                self.api.deploy(self.notebook_directory)
                if state["phase"] != "SETUP":
                    state["after_setup_phase"] = state["phase"]
                state["phase"] = "SETUP"
                self.launch(state, "SETUP")
                return state
            if state["phase"] == "IDLE":
                state.update(phase="FEEDS", status="WORK_IN_PROGRESS", pass_number=0,
                    cycle_id=stable_id("auto-cycle-v2", state["generation"], utc_text(self.now())),
                    cycle_started_at=utc_text(self.now()), check_rounds=0, retries=0, refresh_all=False)
                state["month_due"] = (not state.get("last_month_feed_at") or
                    self.now() - parse_utc(state["last_month_feed_at"]) >= timedelta(hours=24))
                self.save(state)
            kind = "SEED" if state["phase"] in ("FEEDS", "DRAIN_SEED") else "VERIFY"
            if state["phase"] == "FEEDS":
                runtime.feeds(state)
            if state["phase"] == "CHECKS":
                runtime.feeds(state, day_only=True)
                runtime.checks(state)
            if state["phase"] == "DRAIN_VERIFY":
                # Refresh the day feed only when the prior observation could go stale.
                if self.now() - parse_utc(state["refresh_at"]) > timedelta(hours=1):
                    state["refresh_at"] = utc_text(self.now())
                    self.save(state)
                    runtime.feeds(state, day_only=True)
            state["pass_number"] = state.get("pass_number", 0) + 1
            if state["pass_number"] > 32:
                state["status"] = "HALTED_PASS_LIMIT"
                state["halt_revision"] = self.revision
                self.save(state)
                return state
            runtime.drain(state)
            operation = stable_id("auto-job-v2", state["cycle_id"], kind, state.get("operation_number", 0) + 1)
            runtime.seal_run(operation, state)
            self.launch(state, kind)
            return state
        except Exception as error:
            if isinstance(error, BudgetExceeded) and not str(error).startswith((
                    "Run request or wall-time budget", "Landing drain budget reached",
                    "Kafka returned no records for the known backlog")):
                state.update(status="HALTED_CAPACITY_LIMIT", halt_revision=self.revision,
                    last_error_class=type(error).__name__)
                self.save(state)
                return state
            if isinstance(error, (DeferredError, BudgetExceeded)):
                state.update(status="CONTINUATION_SCHEDULED", next_due=utc_text(self.now() + timedelta(
                    seconds=max(60, getattr(error, "retry_after_seconds", 0)))))
                self.save(state)
                return state
            state["errors"] = state.get("errors", 0) + 1
            state["last_error_class"] = type(error).__name__
            state["status"] = "RETRY_SCHEDULED"
            from kafka.errors import KafkaError
            known_service_error = isinstance(error, (PipelineError, TransportFailure, KafkaError))
            if (not known_service_error or isinstance(error, (ConfigurationError, ContractError)) or (
                    isinstance(error, HTTPFailure) and error.status in (400, 401, 403))
                    or getattr(error, "errno", 0) in (29, 30, 31, 58)):
                state["status"] = "HALTED_SERVICE_ERROR"
            elif state["errors"] >= 3:
                # Service outages open a circuit, then retry automatically later.
                state.update(status="SERVICE_COOLDOWN", errors=0,
                    next_due=utc_text(self.now() + timedelta(hours=1)))
            if state["status"].startswith("HALTED"):
                state["halt_revision"] = self.revision
            self.save(state)
            raise
        finally:
            runtime.close()


def reset_intent(mode, requested_id, confirmation, bootstrap, previous):
    """Resolve a reviewed one-time reset without rearming it after later resets."""
    if mode == "reset":
        if not requested_id:
            raise ConfigurationError("Explicit reset mode requires a reset ID")
        return requested_id, confirmation
    if previous and previous.get("reset_id"):
        return previous["reset_id"], "RESET_EARTHQUAKE_DEV"
    if previous and previous.get("bootstrap_reset_completed") == bootstrap.get("initial_reset_id"):
        return "", ""
    return bootstrap.get("initial_reset_id", ""), bootstrap.get("initial_reset_confirmation", "")


def record_bootstrap_completion(journal, state, bootstrap):
    """Remember the initial deletion independently of any later explicit reset."""
    initial = bootstrap.get("initial_reset_id")
    if (initial and not state.get("reset_id") and state.get("last_reset_id") == initial
            and state.get("bootstrap_reset_completed") != initial):
        state["bootstrap_reset_completed"] = initial
        journal.save(state)


def main(argv=None):
    """Run the scheduled controller using only configured cloud credentials."""
    parser = argparse.ArgumentParser(description="Automated earthquake pipeline controller")
    parser.add_argument("--mode", choices=("tick", "reset"), default="tick")
    parser.add_argument("--reset-id", default="")
    parser.add_argument("--confirmation", default="")
    args = parser.parse_args(argv)
    try:
        settings = replace(Settings.from_env(), run_seconds=480, max_requests=3000)
        settings.require_cloud()
        if settings.environment != "dev" or settings.volume_root != "/Volumes/earthquake_dev/ops/landing":
            raise ConfigurationError("Automation is confined to the earthquake development Volume")
        stream_env = dict(os.environ)
        for key, value in {"EQ_KAFKA_TOPIC": "earthquake_events_dev",
                "EQ_KAFKA_CONSUMER_GROUP": "earthquake-dev-landing-bootstrap",
                "EQ_STREAM_GENERATION": "bootstrap-v2"}.items():
            if not stream_env.get(key):
                stream_env[key] = value
        stream = KafkaSettings.from_env(stream_env)
        events = []
        http = HTTPClient(settings, events.append)
        # Independent request counters must not collide in the source run ledger.
        control_http = HTTPClient(replace(settings, run_seconds=600, max_requests=500), lambda _: None)
        api = CloudAPI(settings, control_http)
        journal = WorkspaceJournal(api)
        job_id, job_ids = api.project_job_ids(os.environ.get("EQ_HANDOFF_LEDGER_JOB_ID", ""),
                                            os.environ.get("EQ_CAPTURE_LEDGER_JOB_ID", ""))
        bootstrap = decode_json(Path("config/automation.json").read_bytes())
        previous = journal.load()
        reset_id, confirmation = reset_intent(args.mode, args.reset_id, args.confirmation, bootstrap, previous)
        stream_for = lambda state: replace(stream, generation=state["generation"], group=state["group"])
        files = sorted(Path("src/quake_pipeline").glob("*.py")) + sorted(Path("databricks").glob("*.py"))
        revision = stable_id("automation-code-v2", [(str(p), digest(p.read_bytes())) for p in files])
        controller = Controller(journal, api,
            lambda state: Runtime(settings, http, stream_for(state), events),
            lambda state: KafkaReset(stream_for(state)), job_id,
            job_ids, stream.generation, stream.group,
            revision)
        state = controller.tick(reset_id, confirmation, smoke=True)
        record_bootstrap_completion(journal, state, bootstrap)
        summary = {"controller_status": state["status"], "phase": state["phase"],
            "active_run_id": (state.get("active") or {}).get("run_id"),
            "pending_captures": state.get("last_report", {}).get("pending_captures"),
            "pending_batches": state.get("last_report", {}).get("pending_batches"),
            "issues": state.get("last_report", {}).get("issues", []),
            "last_failed_run_id": state.get("last_failed_run_id"),
            "last_error_class": state.get("last_error_class")}
        print(json_bytes(summary).decode(), flush=True)
        if os.getenv("GITHUB_STEP_SUMMARY"):
            # This file contains metadata only, never earthquake records or secrets.
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
                output.write("Earthquake controller: " + json_bytes(summary).decode() + "\n")
        return 1 if state["status"].startswith("HALTED") or state["status"] == "OVERDUE_JOB" else 0
    except Exception as error:
        failure = {"event": "AUTOMATION_ERROR", "error_class": type(error).__name__}
        # Configuration errors are authored by this repository and contain no
        # provider body, payload or credential, so their remedy is safe to show.
        if isinstance(error, ConfigurationError):
            failure["message"] = str(error)
        print(json_bytes(failure).decode(), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

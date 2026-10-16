"""Exercise the real controller across scheduled ticks and uncertain cloud replies.

These tests use deterministic service adapters; they do not certify a real
Databricks reset, Aiven ACL, or free-tier capacity. Existing engine tests remain.
"""

import base64
import copy
import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from quake_pipeline.automation import Controller, reset_intent, record_bootstrap_completion
from quake_pipeline.automation_cloud import CloudAPI, WorkspaceJournal, inline_notebook
from quake_pipeline.automation_reset import KafkaReset
from quake_pipeline.contracts import json_bytes, utc_text
from quake_pipeline.errors import ConfigurationError, ContractError, DeferredError, TransportFailure
from test_sprint_04 import NOW
from test_sprint_05_06 import LakeStore, ROOT, event, s6, s4, s5


class Journal:
    """Simulate durable cloud bytes, never shared mutable Python references."""

    def __init__(self, state=None):
        """Retain the serialized result of the last successful checkpoint."""
        self.state = copy.deepcopy(state)

    def load(self):
        """A new tick always starts from a detached state snapshot."""
        return copy.deepcopy(self.state)

    def save(self, state):
        """Model a durable commit independently of subsequent caller mutations."""
        self.state = copy.deepcopy(state)


class API:
    """Model Databricks idempotency tokens and independent Job execution."""

    def __init__(self):
        """Start with no active external writers and immediate test successes."""
        self.tokens, self.runs, self.submissions = {}, {}, []
        self.deployments, self.configurations, self.cancelled = 0, [], []
        self.others, self.lose_ack, self.result = [], False, "SUCCESS"

    def active(self, ids):
        """Return externally started project runs for collision/reset tests."""
        return self.others

    def pause_schedules(self, ids):
        """The fake has no service-owned schedules to pause."""

    def deploy(self, directory):
        """Count deployments without touching the real workspace."""
        self.deployments += 1

    def configure(self, job_id, setup=False):
        """Record the requested single notebook task kind."""
        self.configurations.append(setup)

    def submit(self, job_id, token, parameters):
        """Accept once, optionally lose the reply, then return the same existing run."""
        self.submissions.append(token)
        if token not in self.tokens:
            rid = len(self.tokens) + 100
            self.tokens[token] = rid
            self.runs[rid] = {"job_id": job_id, "state": {
                "life_cycle_state": "TERMINATED", "result_state": self.result}}
        if self.lose_ack:
            self.lose_ack = False
            raise TransportFailure()
        return self.tokens[token]

    def run(self, run_id):
        """Polling never creates another Job."""
        return self.runs[run_id]

    def cancel(self, run_id):
        """Record an exact cancellation while leaving termination asynchronous."""
        self.cancelled.append(run_id)


class Runtime:
    """Represent frozen source evidence and report outcomes across controller ticks."""

    def __init__(self):
        """Keep source fetches, progress and report failures observable."""
        self.feeds_called, self.checks_called, self.drains, self.seals = [], 0, 0, []
        self.pending, self.issues, self.missing_report, self.error = 0, [], False, None

    def feeds(self, state, day_only=False):
        """Count full versus freshness-only feed observations."""
        self.feeds_called.append(day_only)
        if self.error:
            raise self.error

    def checks(self, state):
        """Model exact-plan source checks."""
        self.checks_called += 1

    def drain(self, state):
        """Count resumed dispatch/landing passes."""
        self.drains += 1

    def seal_run(self, operation, state):
        """Record immutable run metadata before the Job launch."""
        self.seals.append(operation)

    def report(self, operation):
        """Represent successful bounded work without inventing complete publication."""
        if self.missing_report:
            raise ContractError("missing report")
        return {"operation_id": operation, "report_version": 2, "pending_captures": self.pending,
            "pending_batches": self.pending, "issues": self.issues,
            "gold": {"action": "publish" if not self.issues and not self.pending else "preview"},
            "plan_id": "a" * 64}

    def close(self):
        """A fake tick has no live network connection."""


class ControllerTests(unittest.TestCase):
    """Prove that another scheduled tick resumes instead of requiring manual scripts."""

    def setUp(self):
        """Use a fresh journal, one fake Job, and a controllable wall clock."""
        self.time, self.journal, self.api, self.runtime = NOW, Journal(), API(), Runtime()
        self.resetter = SimpleNamespace(advance=lambda state, save: True, close=lambda: None)
        self.controller = Controller(self.journal, self.api, lambda state: self.runtime,
            lambda state: self.resetter, 1, [1], "v1", "earthquake-dev-landing-v1", "code1", now=lambda: self.time)

    def tick(self, **kwargs):
        """Advance the real state machine as a subsequent scheduler invocation."""
        return self.controller.tick(**kwargs)

    def seed(self):
        """Complete setup and submit the first genuine source-processing pass."""
        self.tick()
        return self.tick()

    def test_healthy_cycle_completes_without_manual_actions(self):
        """Six scheduled ticks perform setup, seed, checks and publication."""
        for _ in range(6):
            state = self.tick()
        self.assertEqual(state["status"], "COMPLETE")
        self.assertEqual(len(self.api.tokens), 3)
        self.assertEqual(self.runtime.feeds_called, [False, True])
        self.tick()
        self.assertEqual(len(self.api.tokens), 3)

    def test_active_job_survives_original_forty_minute_wait(self):
        """Long Spark execution never consumes a runner or creates another Job."""
        state = self.seed()
        rid = state["active"]["run_id"]
        self.api.runs[rid]["state"] = {"life_cycle_state": "RUNNING"}
        self.time += timedelta(minutes=70)
        for _ in range(4):
            state = self.tick()
        self.assertEqual(state["status"], "WORK_IN_PROGRESS")
        self.assertEqual(len(self.api.tokens), 2)
        self.assertEqual(self.runtime.drains, 1)

    def test_lost_launch_ack_reuses_journaled_token(self):
        """A crash after run-now acceptance cannot submit a second run."""
        self.api.lose_ack = True
        with self.assertRaises(TransportFailure):
            self.tick()
        self.assertIsNone(self.journal.state["active"]["run_id"])
        self.tick()
        self.assertEqual(len([t for t in self.api.tokens if t == self.api.submissions[0]]), 1)
        self.assertEqual(self.api.submissions[0], self.api.submissions[1])

    def test_pending_work_is_drained_automatically_before_new_feeds(self):
        """Batch bounds produce a continuation, not an exception or another observation."""
        self.seed()
        self.runtime.pending = 20
        state = self.tick()
        self.assertEqual(state["phase"], "DRAIN_SEED")
        self.tick()
        self.assertEqual(self.runtime.feeds_called, [False])
        self.runtime.pending = 0
        self.assertEqual(self.tick()["phase"], "CHECKS")

    def test_job_failure_retries_only_after_terminal_state(self):
        """A failed idempotent task receives a new token after a saved retry delay."""
        state = self.seed()
        self.api.runs[state["active"]["run_id"]]["state"] = {
            "life_cycle_state": "TERMINATED", "result_state": "FAILED"}
        state = self.tick()
        self.assertEqual(state["status"], "RETRY_SCHEDULED")
        self.tick()
        self.assertEqual(len(self.api.tokens), 2)
        self.time += timedelta(minutes=11)
        self.tick()
        self.assertEqual(len(self.api.tokens), 3)

    def test_success_without_pass_receipt_is_not_complete(self):
        """A successful notebook must also prove its expected data postconditions."""
        self.seed()
        self.runtime.missing_report = True
        with self.assertRaises(ContractError):
            self.tick()
        self.assertEqual(self.journal.state["status"], "HALTED_SERVICE_ERROR")

    def test_reset_requires_exact_confirmation(self):
        """No cancellation or data deletion happens with a mistyped reset request."""
        with self.assertRaises(ConfigurationError):
            self.tick(reset_id="start-again", confirmation="yes")
        self.assertEqual(self.api.cancelled, [])
        self.assertEqual(self.api.deployments, 0)

    def test_reset_cancels_exact_active_jobs_before_recreation(self):
        """Reset cannot race with an old notebook still committing Delta data."""
        self.api.others = [{"run_id": 500}]
        state = self.tick(reset_id="clean-01", confirmation="RESET_EARTHQUAKE_DEV")
        self.assertEqual(self.api.cancelled, [500])
        self.assertEqual(self.api.deployments, 0)
        self.assertEqual(state["phase"], "RESET_STOP")
        self.api.others = []
        state = self.tick()
        self.assertEqual(state["active"]["kind"], "SETUP")
        self.assertNotEqual(state["generation"], "v1")

    def test_same_completed_reset_id_cannot_erase_new_data(self):
        """A rerun of the same reset workflow resumes its completed intent."""
        self.tick(reset_id="clean-01", confirmation="RESET_EARTHQUAKE_DEV")
        self.tick()
        self.tick(reset_id="clean-01", confirmation="RESET_EARTHQUAKE_DEV")
        self.assertEqual(self.api.configurations.count(True), 1)
        self.assertEqual(self.journal.state["last_reset_id"], "clean-01")

    def test_no_progress_stops_instead_of_claiming_recovery(self):
        """Repeated identical pending counts surface an incident rather than loop forever."""
        self.seed()
        self.runtime.pending = 10
        for _ in range(7):
            state = self.tick()
        self.assertEqual(state["status"], "HALTED_NO_PROGRESS")
        prior = len(self.api.tokens)
        self.tick()
        self.assertEqual(len(self.api.tokens), prior)

    def test_retry_after_survives_between_scheduled_ticks(self):
        """An upstream two-hour delay prevents a premature source request next tick."""
        self.tick()
        self.runtime.error = DeferredError("wait", 7200)
        state = self.tick()
        self.assertEqual(state["status"], "CONTINUATION_SCHEDULED")
        count = len(self.runtime.feeds_called)
        self.time += timedelta(minutes=30)
        self.tick()
        self.assertEqual(len(self.runtime.feeds_called), count)

    def test_code_update_is_not_deployed_during_an_active_job(self):
        """Notebook imports wait until the old Spark writer terminates."""
        state = self.seed()
        self.api.runs[state["active"]["run_id"]]["state"] = {"life_cycle_state": "RUNNING"}
        self.controller.revision = "code2"
        self.tick()
        self.assertEqual(self.api.deployments, 1)

    def test_quota_failure_automatically_waits_until_later_day(self):
        """Known quota exhaustion preserves the phase rather than requiring manual recovery."""
        state = self.seed()
        self.api.runs[state["active"]["run_id"]]["state"] = {"life_cycle_state":"TERMINATED",
            "result_state":"FAILED", "state_message":"Serverless daily quota exceeded"}
        state = self.tick()
        self.assertEqual(state["status"], "QUOTA_PAUSED")
        count = len(self.api.tokens)
        self.time += timedelta(hours=2)
        self.tick()
        self.assertEqual(len(self.api.tokens), count)

    def test_connection_outage_opens_and_automatically_retries_circuit(self):
        """Repeated service failures defer work without requiring a manual restart."""
        self.tick()
        self.runtime.error = TransportFailure()
        for _ in range(3):
            with self.assertRaises(TransportFailure):
                self.tick()
        self.assertEqual(self.journal.state["status"], "SERVICE_COOLDOWN")
        calls = len(self.runtime.feeds_called)
        self.tick()
        self.assertEqual(len(self.runtime.feeds_called), calls)
        self.runtime.error = None
        self.time += timedelta(hours=2)
        self.assertIsNotNone(self.tick()["active"])


class JournalTests(unittest.TestCase):
    """Reject executable journal content and preserve its exact metadata."""

    def test_journal_does_not_execute_code(self):
        """A changed workspace notebook is treated as invalid input, never eval input."""
        source = "import os\nSTATE_JSON = '{}'"
        api = SimpleNamespace(request=lambda *args, **kwargs: {
            "content": base64.b64encode(source.encode()).decode()})
        with self.assertRaises(ContractError):
            WorkspaceJournal(api).load()

    def test_saved_journal_round_trip(self):
        """The writer's readback proves exact serialized state, not a successful HTTP alone."""
        content = {}
        def request(method, path, body=None, **kwargs):
            """Store the imported source exactly and return it from export."""
            if "import" in path:
                content["content"] = body["content"]
            if "export" in path:
                return content
            return {}
        journal = WorkspaceJournal(SimpleNamespace(request=request))
        journal.save({"controller_version": 2, "phase": "CHECKS"})
        self.assertEqual(journal.load()["phase"], "CHECKS")

    def test_deployed_notebooks_are_self_contained(self):
        """The assembled notebook uses the parent Job widget scope and compiles as one program."""
        for name in ("automated_setup", "automated_pipeline"):
            source = inline_notebook(ROOT / "databricks" / (name + ".py"), ROOT / "databricks")
            self.assertNotIn("# MAGIC %run", source)
            compile(source, name, "exec")
            self.assertLess(len(source.encode()), 2000000)


class BootstrapTests(unittest.TestCase):
    """Prove first deployment is destructive once, never on ordinary future pushes."""

    def setUp(self):
        """Use the same reviewed reset convention as the deployed configuration."""
        self.config = {"initial_reset_id": "initial-1", "initial_reset_confirmation": "RESET_EARTHQUAKE_DEV"}

    def test_first_deployment_requests_configured_reset(self):
        """No journal means the user's explicitly requested clean baseline is armed."""
        self.assertEqual(reset_intent("tick", "", "", self.config, None),
                         ("initial-1", "RESET_EARTHQUAKE_DEV"))

    def test_incomplete_reset_takes_priority(self):
        """A changed deployment cannot replace an in-flight destructive intention."""
        self.assertEqual(reset_intent("tick", "", "", self.config, {"reset_id": "earlier"})[0], "earlier")

    def test_completed_bootstrap_is_not_rearmed_after_later_reset(self):
        """The completion marker survives a differently named later explicit reset."""
        state = {"last_reset_id": "initial-1"}
        journal = Journal()
        record_bootstrap_completion(journal, state, self.config)
        state["last_reset_id"] = "later-2"
        self.assertEqual(reset_intent("tick", "", "", self.config, state), ("", ""))
        self.assertEqual(journal.state["bootstrap_reset_completed"], "initial-1")

    def test_new_reviewed_config_id_requests_new_reset(self):
        """Changing the destructive configuration is intentional, not a docs-only push."""
        state = {"bootstrap_reset_completed": "older"}
        self.assertEqual(reset_intent("tick", "", "", self.config, state)[0], "initial-1")

    def test_explicit_reset_requires_an_id(self):
        """An unidentifiable destructive operation cannot start."""
        with self.assertRaises(ConfigurationError):
            reset_intent("reset", "", "RESET_EARTHQUAKE_DEV", self.config, None)


class DiscoveryTests(unittest.TestCase):
    """Exercise real discovery logic against deterministic API responses."""

    def api(self, handler):
        """Inject transport without constructing live service credentials."""
        api = CloudAPI(None, None)
        api.request = handler
        api.deploy = lambda directory: None
        return api

    def test_adopts_existing_job_and_capture_by_exact_name(self):
        """Existing projects need neither a copied ID nor another Job."""
        def request(method, path, body=None, **kwargs):
            """Return one exact match for each known project Job."""
            name = "earthquake-dev-handoff-ledger" if "handoff" in path else "earthquake-dev-capture-ledger"
            return {"jobs": [{"job_id": 5 if "handoff" in path else 7, "settings": {"name": name}}]}
        self.assertEqual(self.api(request).project_job_ids(), (5, [5, 7]))

    def test_missing_job_creates_one_serverless_task(self):
        """The new Job contains no classic cluster or paid compute specification."""
        bodies = []
        def request(method, path, body=None, **kwargs):
            """Model a fresh workspace and capture the sole create request."""
            if method == "POST":
                bodies.append(body)
                return {"job_id": 8}
            return {"jobs": []}
        self.assertEqual(self.api(request).project_job_ids(), (8, [8]))
        self.assertEqual(len(bodies), 1)
        self.assertEqual(len(bodies[0]["tasks"]), 1)
        self.assertNotIn("new_cluster", bodies[0]["tasks"][0])

    def test_duplicate_exact_names_fail_before_creation(self):
        """Ambiguity never authorizes guessing a destructive target."""
        api = self.api(lambda *args, **kwargs: {"jobs": [
            {"job_id": i, "settings": {"name": "earthquake-dev-handoff-ledger"}} for i in (1, 2)]})
        with self.assertRaises(ConfigurationError):
            api.project_job_ids()

    def test_wrong_explicit_id_is_not_adopted(self):
        """An unrelated named Job cannot be reset even when an ID hint is supplied."""
        api = self.api(lambda *args, **kwargs: {"settings": {"name": "another-project"}})
        with self.assertRaises(ConfigurationError):
            api.project_job_ids("22")

    def test_missing_optional_hint_falls_back_to_discovery(self):
        """A deleted older Job ID does not require a repository-variable repair."""
        def request(method, path, body=None, **kwargs):
            """Return not-found only for the stale numeric hint."""
            if "jobs/get" in path:
                return None
            return {"jobs": [] if "capture" in path else [
                {"job_id": 9, "settings": {"name": "earthquake-dev-handoff-ledger"}}]}
        self.assertEqual(self.api(request).project_job_ids("22"), (9, [9]))

    def test_lost_create_reply_is_discovered_next_tick(self):
        """An uncertain unkeyed create is never immediately retried."""
        created = []
        def request(method, path, body=None, **kwargs):
            """Commit a new Job before simulating the missing acknowledgement."""
            if method == "POST":
                created.append({"job_id": 12, "settings": {"name": body["name"]}})
                raise TransportFailure()
            return {"jobs": [] if "capture" in path else created}
        api = self.api(request)
        with self.assertRaises(TransportFailure):
            api.project_job_ids()
        self.assertEqual(api.project_job_ids(), (12, [12]))
        self.assertEqual(len(created), 1)


class KafkaResetTests(unittest.TestCase):
    """Verify reset boundaries and the delete/recreate interruption boundary."""

    def test_recreated_topic_is_never_deleted_again(self):
        """A saved CREATE phase makes a lost create ACK safe on a later tick."""
        topics, deleted, created = {"earthquake_events_dev", "other-project"}, [], []
        def delete(names, **kwargs):
            """Apply the broker mutation before the next simulated tick."""
            topics.difference_update(names)
            deleted.extend(names)
        def create(values, **kwargs):
            """Make the topic visible while recording its explicit retention config."""
            topics.add(values[0].name)
            created.extend(values)
        admin = SimpleNamespace(list_topics=lambda: list(topics),
            describe_topics=lambda _: [{"partitions": [{"replicas": [1]}]}],
            delete_topics=delete, create_topics=create, list_consumer_groups=lambda: [], close=lambda: None)
        reset = KafkaReset(SimpleNamespace(topic="earthquake_events_dev"), admin)
        state = {"reset_smoke": False}
        for _ in range(4):
            reset.advance(state, lambda s: None)
        self.assertEqual(deleted, ["earthquake_events_dev"])
        self.assertEqual(len(created), 1)
        self.assertIn("other-project", topics)
        self.assertEqual(created[0].topic_configs["retention.ms"], "259200000")

    def test_reset_refuses_an_unrelated_topic(self):
        """User-supplied environment values cannot turn reset into a cluster-wide wipe."""
        with self.assertRaises(ConfigurationError):
            KafkaReset(SimpleNamespace(topic="orders"), object())


class GoldAutoTests(unittest.TestCase):
    """Preserve publication gates while removing routine manual promotion."""

    def test_auto_publishes_only_when_coverage_is_clean(self):
        """Actual Gold engine keeps a partial preview and publishes a clean release."""
        for blocked in (False, True):
            store = LakeStore()
            batch, _ = store.add_bronze([event(alert="yellow", mmi=6)])
            s5.SilverEngine(store, "auto-silver").process([batch])
            store.ready_sources()
            if blocked:
                store.coverage_inputs = lambda: ([], [], {})
            result = s6.GoldEngine(store, "auto-test").process("auto")
            self.assertEqual(result["action"], "preview" if blocked else "publish")
            self.assertEqual(bool(store.pointer()["publication_id"]), not blocked)


class DatabricksResetTests(unittest.TestCase):
    """Execute reset inventory logic before permitting any destructive SQL."""

    def run_reset(self, unknown=False):
        """Provide a small managed project catalog and capture exact DDL targets."""
        calls = []
        class Spark:
            """Return fixed catalog inventory without a cloud connection."""

            def sql(self, statement):
                """Record every statement so preflight-before-delete can be asserted."""
                calls.append(statement)
                if statement == "SHOW CATALOGS":
                    rows = [SimpleNamespace(catalog="earthquake_dev")]
                elif statement.startswith("SHOW SCHEMAS"):
                    rows = [SimpleNamespace(databaseName="ops"), SimpleNamespace(databaseName="bronze")]
                    if unknown:
                        rows.append(SimpleNamespace(databaseName="another_project"))
                elif statement.startswith("SHOW TABLES"):
                    rows = [SimpleNamespace(tableName="captures", isTemporary=False)] if ".ops" in statement else []
                elif statement.startswith("SHOW VOLUMES"):
                    rows = [SimpleNamespace(asDict=lambda: {"volume_name":"landing"})] if ".ops" in statement else []
                elif statement.startswith("DESCRIBE VOLUME"):
                    rows = [SimpleNamespace(asDict=lambda: {"volumeType":"MANAGED"})]
                else:
                    rows = []
                return SimpleNamespace(collect=lambda: rows)
        spark = Spark()
        spark.catalog = SimpleNamespace(getTable=lambda _: SimpleNamespace(tableType="MANAGED"))
        widgets = {"reset_id":"clean-1", "reset_confirmation":"RESET_EARTHQUAKE_DEV", "reset_smoke":"true"}
        namespace = {"spark": spark, "dbutils": SimpleNamespace(widgets=SimpleNamespace(
            text=lambda *args: None, get=widgets.get))}
        code = compile((ROOT / "databricks/automation_reset.py").read_text(), "automation_reset.py", "exec")
        if unknown:
            with self.assertRaises(ValueError):
                exec(code, namespace)
        else:
            exec(code, namespace)
        return calls

    def test_unknown_schema_prevents_every_delete(self):
        """A project namespace containing unexpected data requires investigation."""
        calls = self.run_reset(unknown=True)
        self.assertFalse(any(c.startswith("DROP") for c in calls))

    def test_known_reset_is_confined_to_exact_managed_objects(self):
        """The allowlisted reset never drops a whole account or a wildcard catalog."""
        calls = self.run_reset()
        self.assertEqual([c for c in calls if c.startswith("DROP")], [
            "DROP TABLE IF EXISTS earthquake_dev.ops.captures",
            "DROP VOLUME IF EXISTS earthquake_dev.ops.landing"])


if __name__ == "__main__":
    unittest.main()

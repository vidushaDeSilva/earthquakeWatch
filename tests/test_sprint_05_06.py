"""Exercise actual Silver/Gold engines with producer-compatible frozen evidence.

Fixtures model separately atomic Delta commits, retained snapshots and pointer
CAS. They test convergence and product semantics, not live Databricks execution.
"""

import copy
import json
import runpy
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

from test_sprint_04 import MemoryStore, NOW, bundle, capture, s4
from test_sprint_03 import feature, settings
from quake_pipeline.outbox import envelope

ROOT = Path(__file__).resolve().parents[1]
S5_GLOBALS = runpy.run_path(str(ROOT / "databricks/sprint_05_silver_library.py"), init_globals=vars(s4))
s5 = SimpleNamespace(**S5_GLOBALS)
s6 = SimpleNamespace(**runpy.run_path(str(ROOT / "databricks/sprint_06_gold_library.py"), init_globals=S5_GLOBALS))


def event(index=0, updated=1000, magnitude=6.2, seconds_ago=3600, **properties):
    """Create a recent earthquake while independently controlling source revision order."""
    result = feature(index)
    result["properties"].update(time=int((NOW - timedelta(seconds=seconds_ago)).timestamp() * 1000),
        updated=updated, mag=magnitude, type="earthquake", sig=601)
    result["properties"].update(properties)
    return result


class LakeStore(MemoryStore):
    """Model layer commits while reusing the real Bronze/source verification path."""

    def __init__(self):
        """Initialize isolated table dictionaries, clocks and retained current snapshots."""
        super().__init__()
        for name in [*s5.SILVER_SCHEMAS, *s6.GOLD_SCHEMAS, "ref.impact_policies"]:
            self.tables[name] = {}
        self.clock, self.versions, self.snapshots = NOW, {}, {0: []}
        self.point = {"product_key": "earthquake_watchlist", "publication_id": None,
            "previous_publication_id": None, "pointer_generation": 0, "updated_at": None, "run_id": None}
        policy = json.loads((ROOT / "config/impact_policy_v1.json").read_text())
        self.tables["ref.impact_policies"]["impact-v1"] = {"policy_version": "impact-v1", "rules_json": json.dumps(policy["rules"])}
        self.fail_ack, self.audits = False, []

    def now(self):
        """Return a controllable processing clock."""
        return self.clock

    def rows(self, name, limit=10000):
        """Read a bounded independent table snapshot."""
        rows = copy.deepcopy(list(self.tables[name].values()))
        if len(rows) > limit:
            raise s5.SilverError("Fixture bound")
        return rows

    def optional(self, path):
        """Distinguish absence from verification exceptions in modeled cloud storage."""
        return self.files.get(path)

    def merge_rows(self, name, rows, keys, immutable, mutable):
        """Apply one separately atomic upsert, preserving first immutable provenance."""
        planned, updates = {}, copy.deepcopy(self.tables[name])
        for row in rows:
            key = tuple(row[k] for k in keys)
            if key in planned and any(planned[key][f] != row[f] for f in immutable + mutable):
                raise s5.SilverError("Duplicate input")
            planned[key] = copy.deepcopy(row)
        for key, row in planned.items():
            old = updates.get(key)
            if old and any(old[f] != row[f] for f in immutable):
                raise s5.SilverError("Immutable conflict")
            if old:
                old.update({k: row[k] for k in mutable})
            else:
                updates[key] = row
        self.tables[name] = updates
        if rows:
            self.versions[name] = self.versions.get(name, 0) + 1
            if name == "silver.events_current":
                self.snapshots[self.versions[name]] = self.rows(name)

    def prove_rows(self, name, rows, keys, compare):
        """Verify exact expected facts after each modeled table commit."""
        for row in rows:
            found = [r for r in self.tables[name].values() if all(r[k] == row[k] for k in keys)]
            if len(found) != 1 or any(found[0][f] != row[f] for f in compare):
                raise s5.SilverError("Post-write proof")

    def event_history(self, keys):
        """Return only the affected event histories."""
        return [r for r in self.rows("silver.event_versions") if r["event_key"] in keys]

    def current_rows(self, keys):
        """Return one current row per requested event key."""
        return [r for r in self.rows("silver.events_current") if r["event_key"] in keys]

    def silver_input(self, batch):
        """Exercise the production source/landing verifier rather than inventing observations."""
        return s5.SparkSilverStore.silver_input(self, batch)

    def silver_batches(self, recheck=False, maximum=32):
        """Exercise production receipt selection, including missing-receipt repair."""
        return s5.SparkSilverStore.silver_batches(self, recheck, maximum)

    def ack_silver(self, bid):
        """Advance only a verified Bronze batch in the modeled ledger."""
        row = self.tables["ops.batches"][bid]
        if row["data_stage"] == "BRONZE_COMMITTED":
            row.update(data_stage="SILVER_COMMITTED", silver_committed_at=self.now())
        elif row["data_stage"] != "SILVER_COMMITTED":
            raise s5.SilverError("Invalid stage")

    def stage_check(self, *args):
        """Retain inspectable stage checks without raw payload logging."""
        self.checks.append(args)

    def upsert_ops(self, name, rows, fields):
        """Persist operational metadata for terminal-run control-flow tests."""
        key = s4.BRONZE_KEYS[name]
        for row in rows:
            if row[key] in self.tables[name]:
                self.tables[name][row[key]].update({k: row[k] for k in fields})
            else:
                self.tables[name][row[key]] = copy.deepcopy(row)

    def table_version(self, name):
        """Expose independent per-table commit versions."""
        return self.versions.get(name, 0)

    def snapshot(self, version):
        """Raise when a pinned snapshot is unavailable; never return latest instead."""
        if version not in self.snapshots:
            raise s6.GoldError("Expired snapshot")
        return copy.deepcopy(self.snapshots[version])

    def coverage_inputs(self):
        """Supply operational source evidence and unresolved Silver dispositions."""
        stages = {r["stage_name"] for r in self.tables["bronze.quarantine"].values() if r["resolution_status"] == "OPEN"}
        rejects = {"silver": "SILVER" in stages, "bronze": "BRONZE" in stages}
        return self.rows("ops.captures"), self.rows("ops.batches"), rejects

    def publication(self, pid):
        """Read persisted publication pins independently of in-process dictionaries."""
        if pid is None:
            return None
        rows = [r for r in self.rows("ops.publications") if r["publication_id"] == pid]
        return rows[0] if rows else None

    def save_publication(self, row):
        """Exercise production pin immutability and state-transition verification."""
        return s6.SparkGoldStore.save_publication(self, row)

    def verify_output(self, name, pid, expected):
        """Require the exact release row set, including no unexpected rows."""
        actual = [r for r in self.rows(name) if r["publication_id"] == pid]
        if len(actual) != len(expected) or s6.s56_digest(actual, s6.GOLD_KEYS[name]) != s6.s56_digest(expected, s6.GOLD_KEYS[name]):
            raise s6.GoldError("Fixture output mismatch")

    def pointer(self):
        """Read an independent pointer snapshot."""
        return copy.deepcopy(self.point)

    def activation_gate(self, publication):
        """Exercise real age, source-coverage, pin and generation checks."""
        return s6.SparkGoldStore.activation_gate(self, publication)

    def audit_pointer(self, pointer):
        """Exercise the actual durable activation-audit writer."""
        return s6.SparkGoldStore.audit_pointer(self, pointer)

    def activate(self, publication, run_id):
        """Model one atomic conditional pointer commit and lost-response readback."""
        self.activation_gate(publication)
        self.audit_pointer(self.point)
        if self.point["publication_id"] != publication["publication_id"]:
            old = self.point.copy()
            self.point.update(publication_id=publication["publication_id"], previous_publication_id=old["publication_id"],
                pointer_generation=old["pointer_generation"] + 1, updated_at=self.now(), run_id=run_id)
            if self.fail_ack:
                self.fail_ack = False
                if self.point["publication_id"] != publication["publication_id"]:
                    raise s6.GoldError("Lost acknowledgement")
        self.audit_pointer(self.point)

    def add_bronze(self, features, salt="one", offset=0):
        """Create producer-compatible evidence and commit it through the actual Bronze engine."""
        source = capture(self, features, salt)
        capture_row = self.tables["ops.captures"][source["capture_id"]]
        capture_row["source_generated_at"] = NOW
        messages = [envelope(settings(), source, i, f) for i, f in enumerate(features)]
        evidence = bundle(self, messages, offset)
        manifest, data, definition, raw = evidence
        self.files[manifest["records_relative_path"]] = data
        self.files[manifest["records_relative_path"].rsplit("/", 1)[0] + "/manifest.json"] = raw
        self.files["handoff/" + manifest["stream_id"] + "/definition.json"] = s4.bronze_json(definition)
        row = self.tables["ops.batches"][manifest["batch_id"]]
        row["landed_at"] = NOW
        s4.BronzeEngine(self, "fixture-bronze").process([evidence])
        s4.BronzeEngine(self, "fixture-account").account_captures()
        return row, capture_row

    def ready_sources(self):
        """Add explicit accounted monthly-feed checks for Gold fixtures only."""
        for name in ("usgs-m2p5-month", "usgs-significant-month"):
            manifest = capture(self, [], "coverage-" + name, state="ACCOUNTED")
            manifest["source_id"] = name
            row = self.tables["ops.captures"][manifest["capture_id"]]
            row.update(source_id=name, source_generated_at=NOW)
            self.files[f"captures/{manifest['collection_id']}/{manifest['capture_id']}/ready.json"] = s4.bronze_json(manifest)


class SilverTests(unittest.TestCase):
    """Validate identity, revision ordering and interrupted Silver completion."""

    def setUp(self):
        """Create one committed Bronze observation with real frozen-source proof."""
        self.store = LakeStore()
        self.batch, self.capture = self.store.add_bronze([event()])

    def run_silver(self, fail="none", recheck=False):
        """Run production batch selection and the actual Silver coordinator."""
        batches, _ = self.store.silver_batches(recheck)
        return s5.SilverEngine(self.store, "fixture-silver", fail).process(batches)

    def current(self):
        """Return the only current event used by single-event scenarios."""
        return self.store.rows("silver.events_current")[0]

    def test_valid_typed_state(self):
        """Normalize UTC fields and certify the exact batch."""
        self.run_silver()
        self.assertEqual(self.current()["magnitude"], 6.2)
        self.assertEqual(self.current()["event_date"], (NOW - timedelta(hours=1)).date())
        self.assertEqual(self.batch["data_stage"], "SILVER_COMMITTED")

    def test_replay_preserves_all_business_facts(self):
        """Rechecking the same original observation cannot advance source freshness."""
        self.run_silver()
        before = copy.deepcopy(self.store.tables)
        receipts = self.store.files.copy()
        self.store.clock += timedelta(hours=1)
        self.run_silver(recheck=True)
        self.assertEqual(before, self.store.tables)
        self.assertEqual(receipts, self.store.files)

    def test_each_fault_boundary_converges(self):
        """Repair every table, ledger and receipt interruption without duplicate facts."""
        for boundary in s5.SILVER_BOUNDARIES[1:]:
            with self.subTest(boundary=boundary):
                self.setUp()
                with self.assertRaises(s4.BronzeInjectedFailure):
                    self.run_silver(boundary)
                self.run_silver()
                self.run_silver(recheck=True)
                self.assertEqual(len(self.store.rows("silver.events_current")), 1)
                self.assertEqual(len(self.store.rows("silver.event_versions")), 1)
                self.assertIn("receipts/silver/" + self.batch["batch_id"] + ".json", self.store.files)

    def test_newer_then_older(self):
        """Insert out-of-order history without regressing the current source revision."""
        self.run_silver()
        self.store.add_bronze([event(updated=3000, magnitude=4)], "new", 1)
        self.run_silver()
        self.store.add_bronze([event(updated=2000, magnitude=7)], "old", 2)
        self.run_silver()
        self.assertEqual(self.current()["magnitude"], 4)
        self.assertEqual(len(self.store.rows("silver.event_versions")), 3)

    def test_alias_metadata_variant_is_observed(self):
        """A later alias-only change must survive semantic Bronze version deduplication."""
        self.run_silver()
        changed = event(ids=",eq0,other-id,")
        self.store.add_bronze([changed], "alias", 1)
        self.run_silver()
        self.assertEqual(len(self.store.rows("bronze.event_versions")), 1)
        self.assertEqual(len(self.store.rows("silver.event_versions")), 1)
        self.assertEqual({r["alias_event_id"] for r in self.store.rows("silver.event_aliases")}, {"eq0", "other-id"})

    def test_preferred_id_change_with_identical_payload_is_not_conflict(self):
        """Explicit aliases can connect preferred IDs without inventing a source-payload conflict."""
        self.run_silver()
        changed = event(ids=",eq0,preferred,")
        changed["id"] = "preferred"
        self.store.add_bronze([changed], "preferred", 1)
        self.run_silver()
        self.assertEqual(len(self.store.rows("silver.events_current")), 1)
        self.assertFalse(self.current()["has_unresolved_conflict"])

    def test_alias_collision_is_not_auto_merge(self):
        """Keep established identities separate and leave the conflicting batch uncertified."""
        self.store.add_bronze([event(1)], "second", 1)
        self.run_silver()
        batch, _ = self.store.add_bronze([event(updated=2000, ids=",eq0,eq1,")], "collision", 2)
        with self.assertRaises(s5.SilverError):
            self.run_silver()
        self.assertEqual(len(self.store.rows("silver.events_current")), 2)
        self.assertEqual(batch["data_stage"], "BRONZE_COMMITTED")
        self.assertTrue(all(r["has_unresolved_conflict"] for r in self.store.rows("silver.events_current")))

    def test_same_timestamp_conflict_blocks_completion(self):
        """Preserve conflicting evidence and flag the current record instead of choosing a hash."""
        self.run_silver()
        self.store.add_bronze([event(magnitude=8)], "conflict", 1)
        with self.assertRaises(s5.SilverError):
            self.run_silver()
        self.assertEqual(self.current()["magnitude"], 6.2)
        self.assertTrue(self.current()["has_unresolved_conflict"])
        self.assertEqual({r["selection_status"] for r in self.store.rows("silver.event_versions")}, {"CONFLICT"})

    def test_conflict_in_first_batch_has_no_arbitrary_current(self):
        """Different same-time states in initial input cannot create a false safe current row."""
        self.store = LakeStore()
        self.store.add_bronze([event(), event(magnitude=9)], "initial-conflict")
        with self.assertRaises(s5.SilverError):
            self.run_silver()
        self.assertEqual(self.store.rows("silver.events_current"), [])

    def test_explicit_tombstone(self):
        """An authoritative newer deletion preserves history and marks current withdrawn."""
        self.run_silver()
        deleted = event(updated=2000, status="deleted", time=None)
        deleted["geometry"] = None
        self.store.add_bronze([deleted], "deleted", 1)
        self.run_silver()
        self.assertTrue(self.current()["is_deleted"])
        self.assertIsNone(self.current()["event_time"])

    def test_nullable_fields_and_negative_depth(self):
        """Keep missing impact data distinct from zero and preserve valid negative depth."""
        self.store = LakeStore()
        value = event(magnitude=None, sig=None, mmi=None)
        value["geometry"]["coordinates"][2] = -2
        self.store.add_bronze([value])
        self.run_silver()
        self.assertIsNone(self.current()["magnitude"])
        self.assertEqual(self.current()["depth_km"], -2)

    def test_corrupt_frozen_source_blocks_silver(self):
        """No Silver receipt can be issued when original alias evidence is corrupt."""
        self.store.files[self.capture["response_relative_path"]] = b"corrupt"
        with self.assertRaises(s4.BronzeError):
            self.run_silver()
        self.assertEqual(self.batch["data_stage"], "BRONZE_COMMITTED")

    def test_changed_bronze_delivery_blocks_silver(self):
        """Detect a persisted delivery that disagrees with its sealed landing record."""
        next(iter(self.store.tables["bronze.deliveries"].values()))["raw_feature_sha256"] = "wrong"
        with self.assertRaises(s5.SilverError):
            self.run_silver()

    def test_recheck_reopens_source_evidence(self):
        """A previous successful attempt cannot cache away later corruption on recheck."""
        self.run_silver()
        self.store.files[self.capture["response_relative_path"]] = b"corrupt"
        with self.assertRaises(s4.BronzeError):
            self.run_silver(recheck=True)

    def test_bronze_rejection_can_complete_silver(self):
        """A durably rejected malformed source item creates no current earthquake."""
        self.store = LakeStore()
        self.store.add_bronze([{"invalid": True}])
        self.run_silver()
        self.assertEqual(self.store.rows("silver.events_current"), [])

    def test_stage_run_failure_is_persisted(self):
        """Record failure metadata while leaving incomplete batch state intact."""
        with self.assertRaises(s5.SilverError):
            s5.s56_attempt(self.store, "SILVER", lambda run: s5.SilverEngine(self.store, run, "after-history").process([self.batch]))
        self.assertEqual(next(iter(self.store.tables["ops.runs"].values()))["status"], "FAILED")


class GoldTests(unittest.TestCase):
    """Validate policy semantics, pinned output, coverage gates and publication recovery."""

    def setUp(self):
        """Create one Silver earthquake and explicit accounted source checks."""
        self.store = LakeStore()
        batch, _ = self.store.add_bronze([event(alert="yellow", mmi=6)])
        s5.SilverEngine(self.store, "silver").process([batch])
        self.store.ready_sources()

    def build(self, action="preview", fail="none", retry=None, as_of=None):
        """Execute the actual Gold coordinator with fixed or persisted pins."""
        return s6.GoldEngine(self.store, "gold", fail).process(action, as_of, retry)

    def test_seeded_policy_format_is_supported(self):
        """Consume the exact rules_json shape seeded by Sprint 1."""
        policy, thresholds = s6.gold_policy(self.store.tables["ref.impact_policies"]["impact-v1"])
        self.assertEqual(thresholds["significance"], 600)
        self.assertEqual(policy["policy_version"], "impact-v1")

    def test_preview_does_not_activate(self):
        """Persist inspectable outputs without a READY or serving claim."""
        result = self.build()
        self.assertIsNone(self.store.pointer()["publication_id"])
        self.assertEqual(self.store.publication(result["publication_id"])["state"], "BUILDING")

    def test_publish_and_same_id_retry(self):
        """A repeated successful activation does not advance pointer generation."""
        result = self.build("publish")
        point = self.store.pointer()
        self.build("publish", retry=result["publication_id"])
        self.assertEqual(point, self.store.pointer())
        self.assertEqual(point["pointer_generation"], 1)

    def test_each_gold_fault_boundary_recovers(self):
        """Never expose partial output and converge after all six durable boundaries."""
        for boundary in s6.GOLD_BOUNDARIES[1:]:
            with self.subTest(boundary=boundary):
                self.setUp()
                with self.assertRaises(s4.BronzeInjectedFailure):
                    self.build("publish", boundary)
                pid = self.store.rows("ops.publications")[0]["publication_id"]
                if boundary != "after-pointer":
                    self.assertIsNone(self.store.pointer()["publication_id"])
                self.build("publish", retry=pid)
                self.assertEqual(self.store.pointer()["publication_id"], pid)
                self.assertEqual(len(self.store.rows("gold.watchlist_snapshots")), 1)

    def test_pending_captured_responses_allow_preview_block_publish(self):
        """Keep the known backlog visible without claiming public coverage."""
        pending = copy.deepcopy(self.store.rows("ops.captures")[0])
        pending.update(capture_id="old-captured", state="CAPTURED", accounted_item_count=0)
        self.store.tables["ops.captures"]["old-captured"] = pending
        result = self.build()
        self.assertIn("PENDING_CAPTURES", self.store.publication(result["publication_id"])["blocking_issues"])
        with self.assertRaises(s6.GoldError):
            self.build("publish", retry=result["publication_id"])
        self.assertIsNone(self.store.pointer()["publication_id"])

    def test_stale_generation_is_not_hidden_by_new_fetch(self):
        """A recently fetched stale USGS body cannot pass source freshness checks."""
        for row in self.store.tables["ops.captures"].values():
            if row["source_id"] == "usgs-m2p5-day":
                row["source_generated_at"] -= timedelta(days=1)
        with self.assertRaises(s6.GoldError):
            self.build("publish")

    def test_durable_bronze_rejection_is_not_complete_product_coverage(self):
        """Accounting a rejected item must not imply every source event reached the product."""
        self.store.tables["bronze.quarantine"]["fixture-reject"] = {"stage_name": "BRONZE", "resolution_status": "OPEN"}
        result = self.build()
        self.assertIn("UNRESOLVED_BRONZE_REJECTIONS", self.store.publication(result["publication_id"])["blocking_issues"])
        with self.assertRaises(s6.GoldError):
            self.build("publish", retry=result["publication_id"])

    def test_source_outage_preserves_existing_pointer(self):
        """Keep the last good release active when current source checks become stale."""
        self.build("publish")
        pointer = self.store.pointer()
        self.store.clock += timedelta(days=2)
        with self.assertRaises(s6.GoldError):
            self.build("publish")
        self.assertEqual(pointer, self.store.pointer())

    def test_missing_required_source_blocks_publish(self):
        """A single hourly source cannot establish the configured 30-day feed scope."""
        self.store.tables["ops.captures"] = {k: r for k, r in self.store.tables["ops.captures"].items()
            if r["source_id"] != "usgs-m2p5p-month" and r["source_id"] != "usgs-m2p5-month"}
        with self.assertRaises(s6.GoldError):
            self.build("publish")

    def test_expired_pin_fails_without_latest_fallback(self):
        """Retry must use its retained Silver snapshot even if another one exists."""
        result = self.build()
        self.store.snapshots.clear()
        with self.assertRaises(s6.GoldError):
            self.build(retry=result["publication_id"])

    def test_preview_is_pinned_despite_latest_changes(self):
        """Independent table changes cannot alter a retry's previously pinned output."""
        result = self.build()
        before = copy.deepcopy(self.store.tables["gold.watchlist_snapshots"])
        self.store.snapshots[100] = []
        self.store.versions["silver.events_current"] = 100
        self.build(retry=result["publication_id"])
        self.assertEqual(before, self.store.tables["gold.watchlist_snapshots"])
        with self.assertRaises(s6.GoldError):
            self.build("publish", retry=result["publication_id"])

    def test_zero_watchlist_is_valid(self):
        """An honestly covered release may have no qualifying events and zero-filled windows."""
        for rows in self.store.snapshots.values():
            for row in rows:
                row.update(magnitude=2.5, significance=None, pager_alert=None, mmi=None, tsunami_flag=0)
        result = self.build("publish")
        self.assertEqual(result["watchlist_rows"], 0)
        self.assertEqual(len(self.store.rows("gold.window_snapshots")), 751)
        self.assertEqual(sum(r["event_count"] for r in self.store.rows("gold.window_snapshots") if r["window_grain"] == "HOUR"), 1)

    def test_half_open_window_boundaries(self):
        """Include the lower bound, exclude as-of, and count each event in one bucket."""
        rows = self.store.snapshots[self.store.table_version("silver.events_current")]
        first = rows[0]
        first["event_time"] = NOW - timedelta(days=30)
        second = copy.deepcopy(first)
        second.update(event_key="second", event_time=NOW)
        rows.append(second)
        self.build()
        hours = [r for r in self.store.rows("gold.window_snapshots") if r["window_grain"] == "HOUR"]
        self.assertEqual(sum(r["event_count"] for r in hours), 1)

    def test_mmi_is_not_replaced_with_cdi(self):
        """Community intensity alone cannot trigger the ShakeMap rule."""
        _, thresholds = s6.gold_policy(self.store.tables["ref.impact_policies"]["impact-v1"])
        row = {"magnitude": None, "significance": 600, "pager_alert": None, "mmi": None, "cdi": 9, "tsunami_flag": 0}
        self.assertEqual(s6.gold_reasons(row, thresholds)[0], [])
        row["mmi"] = 6
        self.assertEqual(s6.gold_reasons(row, thresholds)[0], ["SHAKEMAP_MMI_GTE_6"])

    def test_exact_thresholds_and_pager_label(self):
        """Significance is strict while magnitude/MMI are inclusive and PAGER drives labels."""
        self.build()
        row = self.store.rows("gold.watchlist_snapshots")[0]
        self.assertEqual(row["impact_label"], "ESTIMATED_IMPACT")
        self.assertIn("SIG_GT_600", row["inclusion_reasons"])
        self.assertEqual(row["rank_priority"], 2)

    def test_non_earthquake_and_deleted_excluded(self):
        """Do not display quarry blasts or withdrawn records even with qualifying magnitudes."""
        rows = self.store.snapshots[self.store.table_version("silver.events_current")]
        rows[0]["event_type"] = "quarry blast"
        self.assertEqual(self.build("publish")["watchlist_rows"], 0)

    def test_policy_revision_has_new_identity(self):
        """Explicit supported policy revisions yield distinct publications and reason thresholds."""
        first = self.build()["publication_id"]
        policy = json.loads(self.store.tables["ref.impact_policies"]["impact-v1"]["rules_json"])
        policy["any"][0]["value"] = 700
        self.store.tables["ref.impact_policies"]["impact-v2"] = {"policy_version": "impact-v2", "rules_json": json.dumps(policy)}
        result = s6.GoldEngine(self.store, "v2").process(policy_version="impact-v2")
        self.assertNotEqual(first, result["publication_id"])

    def test_pinned_policy_mutation_rejected(self):
        """A retry cannot silently adopt changed content under the same policy version."""
        result = self.build()
        row = self.store.tables["ref.impact_policies"]["impact-v1"]
        policy = json.loads(row["rules_json"])
        policy["any"][0]["value"] = 900
        row["rules_json"] = json.dumps(policy)
        with self.assertRaises(s6.GoldError):
            self.build(retry=result["publication_id"])

    def test_unexpected_output_row_rejected(self):
        """Count/digest verification detects output beyond the expected release set."""
        result = self.build()
        extra = copy.deepcopy(self.store.rows("gold.watchlist_snapshots")[0])
        extra["event_key"] = "unexpected"
        self.store.tables["gold.watchlist_snapshots"][(result["publication_id"], "unexpected")] = extra
        with self.assertRaises(s6.GoldError):
            self.build(retry=result["publication_id"])

    def test_lost_pointer_reply_is_idempotent(self):
        """Readback accepts an already committed activation and retains one audit identity."""
        self.store.fail_ack = True
        result = self.build("publish")
        self.build("publish", retry=result["publication_id"])
        audits = [r for r in self.store.rows("ops.stage_attempts") if r["stage_name"] == "PUBLISH_POINTER"]
        self.assertEqual(len(audits), 1)

    def test_changed_pointer_generation_blocks_old_candidate(self):
        """A build cannot overwrite a generation that changed after its input pins were recorded."""
        result = self.build()
        self.store.point["pointer_generation"] = 1
        with self.assertRaises(s6.GoldError):
            self.build("publish", retry=result["publication_id"])

    def test_event_time_correction_moves_both_buckets(self):
        """A corrected origin time removes the old contribution and adds exactly one new one."""
        self.build("publish")
        batch, _ = self.store.add_bronze([event(updated=3000, seconds_ago=7200)], "correct-time", 1)
        s5.SilverEngine(self.store, "correct").process([batch])
        self.store.clock += timedelta(seconds=1)
        result = self.build("publish")
        hours = [r for r in self.store.rows("gold.window_snapshots") if r["publication_id"] == result["publication_id"]
                 and r["window_grain"] == "HOUR"]
        self.assertEqual(sum(r["event_count"] for r in hours), 1)
        self.assertEqual(next(r for r in hours if r["window_start"] == NOW - timedelta(hours=1))["event_count"], 0)


class NotebookTests(unittest.TestCase):
    """Execute actual terminal notebook cells with modeled stores and widget values."""

    def test_notebook_silver_then_gold_preview(self):
        """Run the shipped task entry points and persist truthful SUCCESS attempt metadata."""
        store = LakeStore()
        store.add_bronze([event()])
        values = {}
        widgets = SimpleNamespace(text=lambda name, default: values.setdefault(name, default), get=lambda name: values[name])
        utilities = SimpleNamespace(widgets=widgets)
        ns = {**s6.__dict__, "spark": None, "dbutils": utilities,
              "SparkSilverStore": lambda *args: store, "SparkGoldStore": lambda *args: store}
        for name in ("sprint_05_process_silver.py", "sprint_06_publish_gold.py"):
            exec(compile((ROOT / "databricks" / name).read_text(), name, "exec"), ns)
        self.assertEqual({r["status"] for r in store.rows("ops.runs")}, {"SUCCESS"})
        self.assertEqual(store.rows("ops.publications")[0]["state"], "BUILDING")
        self.assertIsNone(store.pointer()["publication_id"])

    def test_invalid_notebook_parameters_fail_before_write(self):
        """Reject unbounded work and ambiguous retry pins without creating business rows."""
        store = LakeStore()
        for notebook, values in (("sprint_05_process_silver.py", {"silver_max_batches": "100000"}),
                ("sprint_06_publish_gold.py", {"gold_as_of_utc": "2026-10-15T12:00:00Z", "gold_retry_publication_id": "bad"})):
            widgets = SimpleNamespace(text=lambda name, default: values.setdefault(name, default), get=lambda name: values[name])
            ns = {**s6.__dict__, "spark": None, "dbutils": SimpleNamespace(widgets=widgets),
                  "SparkSilverStore": lambda *args: store, "SparkGoldStore": lambda *args: store}
            with self.assertRaises(s5.SilverError):
                exec(compile((ROOT / "databricks" / notebook).read_text(), notebook, "exec"), ns)
        self.assertEqual(store.rows("ops.publications"), [])


if __name__ == "__main__":
    unittest.main()

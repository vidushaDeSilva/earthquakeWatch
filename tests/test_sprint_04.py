"""Exercise Bronze contracts, durable accounting and replay through partial writes.

Use the actual Databricks helper and Sprint 1–3 producer/landing contracts. This
suite models per-table commits in memory; it does not claim live Spark coverage.
"""

import copy
import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from quake_pipeline.contracts import digest, json_bytes, source_version_id, stable_id
from quake_pipeline.landing import delivery_row
from quake_pipeline.outbox import envelope, stream_id
from test_sprint_03 import feature, settings

SPEC = importlib.util.spec_from_file_location("sprint4", Path(__file__).resolve().parents[1] /
    "databricks/sprint_04_bronze_library.py")
s4 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(s4)
NOW = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)


class MemoryStore:
    """Model atomic individual table writes and immutable receipts without local data."""

    def __init__(self):
        """Initialize empty in-memory facts, ledgers, checks and cloud files."""
        self.tables = {name: {} for name in [*s4.BRONZE_KEYS, "ops.captures", "ops.batches"]}
        self.files, self.checks = {}, []

    def read(self, path, limit=4194304):
        """Expose genuine absence and honor cloud read bounds."""
        payload = self.files[path]
        if len(payload) > limit:
            raise s4.BronzeError("Bounded fixture read")
        return payload

    def lookup(self, table, key, values):
        """Return independent snapshots of the requested target keys."""
        return [copy.deepcopy(row) for row in self.tables[table].values() if row[key] in values]

    def merge(self, table, rows, compare):
        """Commit one entire table or reject it before changing any row."""
        key = s4.BRONZE_KEYS[table]
        updates = copy.deepcopy(self.tables[table])
        for row in s4.bronze_unique(rows, key, compare):
            old = updates.get(row[key])
            if old and any(old.get(field) != row.get(field) for field in compare):
                raise s4.BronzeError("Fixture immutable conflict")
            updates.setdefault(row[key], copy.deepcopy(row))
        self.tables[table] = updates

    def prove(self, table, rows, compare):
        """Use the same post-write proof as the Spark adapter."""
        s4.SparkBronzeStore.prove(self, table, rows, compare)

    def check(self, *args):
        """Keep inspectable rule results for fixture assertions."""
        self.checks.append(args)

    def ack_batch(self, bid, accepted, rejected, distinct, inserted):
        """Advance one batch only after its exact input count reconciles."""
        row = self.tables["ops.batches"][bid]
        if accepted + rejected != row["row_count"]:
            raise s4.BronzeError("Fixture batch accounting")
        if row["data_stage"] == "LANDED":
            row.update(data_stage="BRONZE_COMMITTED", accepted_delivery_count=accepted,
                       quarantined_delivery_count=rejected, distinct_source_version_count=distinct,
                       bronze_insert_count=inserted)

    def put_receipt(self, path, payload):
        """Reject overwrite with different proof bytes."""
        if path in self.files and self.files[path] != payload:
            raise s4.BronzeError("Fixture receipt conflict")
        self.files[path] = payload

    def pending_captures(self):
        """Recheck captures so receipt repair is exercised after ledger completion."""
        return list(self.tables["ops.captures"].values())

    def accounted_items(self, cid):
        """Count only committed batches with matching version or quarantine evidence."""
        result = {}
        rejects = {row["delivery_id"] for row in self.tables["bronze.quarantine"].values()}
        for row in self.tables["bronze.deliveries"].values():
            if row["capture_id"] != cid or self.tables["ops.batches"][row["batch_id"]]["data_stage"] == "LANDED":
                continue
            if ((row["disposition"] == "ACCEPTED" and row["source_version_id"] not in self.tables["bronze.event_versions"])
                    or (row["disposition"] == "QUARANTINED" and row["delivery_id"] not in rejects)):
                raise s4.BronzeError("Missing persisted disposition")
            result[row["source_item_id"]] = row["source_item_index"]
        return result

    def ack_capture(self, cid, count, complete):
        """Protect monotonic source accounting in the modeled operational ledger."""
        row = self.tables["ops.captures"][cid]
        if count < row["accounted_item_count"]:
            raise s4.BronzeError("Accounting regression")
        row["accounted_item_count"] = count
        if complete:
            row["state"] = "ACCOUNTED"


def capture(store, features, salt="one", state="PUBLISH_ACKED"):
    """Build frozen capture bytes matching the real Sprint 2 identity and paths."""
    payload = json_bytes({"type": "FeatureCollection", "metadata": {"count": len(features), "status": 200,
        "generated": 1000}, "features": features})
    collection = stable_id("fixture-collection", salt)
    cid = stable_id("capture-v1", collection, digest(payload))
    prefix = f"captures/{collection}/{cid}"
    manifest = {"capture_id": cid, "collection_id": collection, "response_sha256": digest(payload),
        "response_relative_path": prefix + "/response.json", "response_bytes": len(payload),
        "source_id": "usgs-m2p5-day", "source_revision": 1, "expected_item_count": len(features),
        "contract_version": "usgs-summary-v1", "fetched_at": "2026-10-15T12:00:00.000000Z"}
    store.files[prefix + "/ready.json"] = json_bytes(manifest)
    store.files[prefix + "/response.json"] = payload
    store.tables["ops.captures"][cid] = {**manifest, "manifest_uri": s4.BRONZE_ROOT + "/" + prefix + "/ready.json",
        "response_uri": s4.BRONZE_ROOT + "/" + prefix + "/response.json", "fetched_at": NOW,
        "accounted_item_count": 0, "state": state}
    return manifest


def bundle(store, messages, offset=0, partition=0):
    """Build a sealed batch using the actual Sprint 3 delivery serialization."""
    config = settings()
    rows = [delivery_row(SimpleNamespace(topic=config.topic, key=key, value=value, partition=partition, offset=offset+i,
        timestamp=1000, headers=[]), config) for i, (key, value) in enumerate(messages)]
    data = b"".join(json_bytes(row) + b"\n" for row in rows)
    sid = stream_id(config)
    bid = stable_id("landing-v1", sid, partition, offset, offset + len(rows) - 1, digest(data))
    manifest = {"manifest_version": 1, "stream_id": sid, "stream_generation": "v1",
        "topic": config.topic, "partition": partition, "offset_first": offset,
        "offset_last": offset + len(rows) - 1, "next_offset": offset + len(rows),
        "row_count": len(rows), "data_bytes": len(data), "data_sha256": digest(data), "batch_id": bid,
        "records_relative_path": f"handoff/{sid}/batches/{bid}/records.jsonl",
        "landed_at": "2026-10-15T12:00:00.000000Z", "state": "LANDED"}
    raw_manifest = json_bytes(manifest)
    store.tables["ops.batches"].setdefault(bid, {"batch_id": bid,
        "manifest_uri": s4.BRONZE_ROOT + f"/handoff/{sid}/batches/{bid}/manifest.json",
        "manifest_sha256": digest(raw_manifest), "row_count": len(rows), "data_stage": "LANDED",
        **{key: manifest[key] for key in ("stream_generation", "topic", "partition", "offset_first", "offset_last")}})
    definition = {"manifest_version": 1, "stream_id": sid, "environment": "dev", "topic": config.topic,
                  "stream_generation": "v1", "partitions": [0, 1]}
    return manifest, data, definition, raw_manifest


class BronzeTests(unittest.TestCase):
    """Prove identity, rejection, repeat ingestion and interrupted write behavior."""

    def setUp(self):
        """Prepare one valid event and its real producer envelope."""
        self.store = MemoryStore()
        self.capture = capture(self.store, [feature()])
        self.message = envelope(settings(), self.capture, 0, feature())
        self.bundle = bundle(self.store, [self.message])

    def run_engine(self, bundles=None, fail="none"):
        """Run the real coordinator against independent modeled table commits."""
        engine = s4.BronzeEngine(self.store, stable_id("test-run", len(self.store.checks)), fail)
        engine.process([self.bundle] if bundles is None else bundles)
        return engine.account_captures()

    def test_valid_cloud_path(self):
        """Accept one source version, one delivery and complete its source capture."""
        self.run_engine()
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 1)
        self.assertEqual(self.store.tables["ops.captures"][self.capture["capture_id"]]["state"], "ACCOUNTED")
        self.assertEqual(self.store.tables["ops.batches"][self.bundle[0]["batch_id"]]["bronze_insert_count"], 1)

    def test_repeat_is_idempotent(self):
        """Repeat every write and keep first provenance and immutable proof bytes."""
        self.run_engine()
        facts = copy.deepcopy(self.store.tables)
        receipts = dict(self.store.files)
        self.run_engine()
        self.assertEqual(facts, self.store.tables)
        self.assertEqual(receipts, self.store.files)

    def test_all_fault_boundaries_recover(self):
        """Recover every partial-table, ledger and receipt boundary without duplicate facts."""
        for boundary in ("after-versions", "after-quarantine", "after-deliveries", "after-batch-ledger",
                         "after-batch-receipt", "after-capture-ledger", "after-capture-receipt"):
            with self.subTest(boundary=boundary):
                self.setUp()
                with self.assertRaises(s4.BronzeInjectedFailure):
                    self.run_engine(fail=boundary)
                if boundary in ("after-versions", "after-quarantine", "after-deliveries"):
                    self.assertNotIn("receipts/bronze/" + self.bundle[0]["batch_id"] + ".json", self.store.files)
                self.run_engine()
                self.run_engine()
                self.assertEqual(len(self.store.tables["bronze.deliveries"]), 1)
                self.assertEqual(self.store.tables["ops.batches"][self.bundle[0]["batch_id"]]["bronze_insert_count"], 1)
                self.assertEqual(self.store.tables["ops.captures"][self.capture["capture_id"]]["state"], "ACCOUNTED")

    def test_republish_separates_delivery_and_source(self):
        """New Kafka offsets create deliveries while sharing one source version/item."""
        self.run_engine([self.bundle, bundle(self.store, [self.message], 1)])
        self.assertEqual(len(self.store.tables["bronze.deliveries"]), 2)
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 1)
        self.assertEqual(self.store.tables["ops.captures"][self.capture["capture_id"]]["accounted_item_count"], 1)

    def test_overlapping_batches(self):
        """Overlapping recovery ranges share deliveries and do not double-count inserts."""
        a = bundle(self.store, [self.message, self.message])
        b = bundle(self.store, [self.message], 1)
        self.run_engine([a, b])
        self.assertEqual(len(self.store.tables["bronze.deliveries"]), 2)
        self.assertEqual(sum(row["bronze_insert_count"] for row in self.store.tables["ops.batches"].values()
                             if row["data_stage"] != "LANDED"), 1)

    def test_overlap_across_captures(self):
        """A later feed observation keeps separate item accounting but shares source state."""
        later = capture(self.store, [feature()], "two")
        msg = envelope(settings(), later, 0, feature())
        self.run_engine([self.bundle, bundle(self.store, [msg], 1)])
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 1)
        self.assertTrue(all(row["state"] == "ACCOUNTED" for row in self.store.tables["ops.captures"].values()))

    def test_distinct_revisions(self):
        """An actual semantic revision remains a second raw source version."""
        changed = feature()
        changed["properties"]["updated"] += 1
        later = capture(self.store, [changed], "later")
        self.run_engine([self.bundle, bundle(self.store, [envelope(settings(), later, 0, changed)], 1)])
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 2)

    def test_poison_messages_preserve_bytes(self):
        """Null, binary, empty, scalar and unsupported envelopes become durable rejections."""
        messages = [(b"key", value) for value in (None, b"\xff", b"", b"12", b'{"envelope_version":99}')]
        self.run_engine([bundle(self.store, messages)])
        rejects = list(self.store.tables["bronze.quarantine"].values())
        self.assertEqual(len(rejects), 5)
        self.assertEqual(len(self.store.tables["bronze.deliveries"]), 5)
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 0)
        self.assertTrue(all(row["capture_id"] is None for row in self.store.tables["bronze.deliveries"].values()))
        self.assertEqual(self.store.tables["ops.captures"][self.capture["capture_id"]]["accounted_item_count"], 0)

    def test_invalid_source_item_is_accounted_rejection(self):
        """An authentic but invalid source feature is counted exactly once through quarantine."""
        invalid = feature()
        invalid["geometry"]["coordinates"][0] = 500
        cap = capture(self.store, [invalid], "invalid")
        self.run_engine([bundle(self.store, [envelope(settings(), cap, 0, invalid)], 1)])
        self.assertEqual(self.store.tables["ops.captures"][cap["capture_id"]]["state"], "ACCOUNTED")
        self.assertEqual(len(self.store.tables["bronze.quarantine"]), 1)

    def test_non_object_source_item(self):
        """A source scalar has stable provenance and does not crash the partition."""
        cap = capture(self.store, [None], "scalar")
        self.run_engine([bundle(self.store, [envelope(settings(), cap, 0, None)], 1)])
        self.assertEqual(self.store.tables["ops.captures"][cap["capture_id"]]["state"], "ACCOUNTED")

    def test_tampered_envelope_not_attributed(self):
        """Wrong source item/key/version/feature cannot complete a valid capture."""
        for mutation in ("source_item_id", "source_version_id", "feature", "captured_at", "source_item_index"):
            with self.subTest(mutation=mutation):
                self.setUp()
                value = s4.bronze_decode(self.message[1])
                value[mutation] = {} if mutation == "feature" else -1
                self.run_engine([bundle(self.store, [(self.message[0], json_bytes(value))])])
                d = next(iter(self.store.tables["bronze.deliveries"].values()))
                self.assertIsNone(d["capture_id"])
                self.assertEqual(d["disposition"], "QUARANTINED")

    def test_wrong_key_quarantined(self):
        """The Kafka key must agree with the proven source item identity."""
        self.run_engine([bundle(self.store, [(b"wrong-key", self.message[1])])])
        self.assertEqual(len(self.store.tables["bronze.quarantine"]), 1)

    def test_unknown_capture_not_attributed(self):
        """Unknown capture claims remain quarantined without trusting forged lineage."""
        value = s4.bronze_decode(self.message[1])
        value["capture_id"] = "a" * 64
        self.run_engine([bundle(self.store, [(self.message[0], json_bytes(value))])])
        self.assertIsNone(next(iter(self.store.tables["bronze.deliveries"].values()))["capture_id"])

    def test_partial_capture_stays_pending(self):
        """A capture cannot complete from broker ACKs or partial downstream delivery."""
        cap = capture(self.store, [feature(), feature(1)], "partial")
        pending = self.run_engine([bundle(self.store, [envelope(settings(), cap, 0, feature())], 2)])
        self.assertIn(cap["capture_id"], pending)
        row = self.store.tables["ops.captures"][cap["capture_id"]]
        self.assertEqual((row["accounted_item_count"], row["state"]), (1, "PUBLISH_ACKED"))

    def test_empty_capture(self):
        """A successfully dispatched empty source check has valid zero-item accounting."""
        cap = capture(self.store, [], "empty")
        self.run_engine([])
        self.assertEqual(self.store.tables["ops.captures"][cap["capture_id"]]["state"], "ACCOUNTED")

    def test_empty_undispatched_capture(self):
        """Do not mark even an empty capture ACCOUNTED before dispatch completion."""
        cap = capture(self.store, [], "empty", "CAPTURED")
        self.run_engine([])
        self.assertEqual(self.store.tables["ops.captures"][cap["capture_id"]]["state"], "CAPTURED")

    def test_corrupt_landing_stops_without_receipt(self):
        """Changed file bytes fail before any business-table write."""
        m, data, definition, raw = self.bundle
        with self.assertRaises(s4.BronzeError):
            self.run_engine([(m, data + b" ", definition, raw)])
        self.assertFalse(self.store.tables["bronze.deliveries"])

    def test_corrupt_source_stops(self):
        """Source corruption is an infrastructure integrity failure, not a poison record."""
        self.store.files[self.capture["response_relative_path"]] += b" "
        with self.assertRaises(s4.BronzeError):
            self.run_engine()
        self.assertFalse(self.store.tables["bronze.deliveries"])

    def test_missing_source_connection_error_propagates(self):
        """Unavailable source storage cannot be misclassified as a source rejection."""
        del self.store.files[self.capture["response_relative_path"]]
        with self.assertRaises(KeyError):
            self.run_engine()
        self.assertFalse(self.store.tables["bronze.quarantine"])

    def test_immutable_delivery_conflict(self):
        """Different bytes at the same Kafka coordinates stop replay."""
        self.run_engine()
        other = bundle(self.store, [(b"key", b"bad")])
        with self.assertRaises(s4.BronzeError):
            self.run_engine([other])

    def test_existing_receipt_conflict(self):
        """A conflicting proof file fails rather than being overwritten."""
        path = "receipts/bronze/" + self.bundle[0]["batch_id"] + ".json"
        self.store.files[path] = b"wrong"
        with self.assertRaises(s4.BronzeError):
            self.run_engine()
        self.assertEqual(self.store.files[path], b"wrong")

    def test_missing_receipt_repaired(self):
        """A completed ledger plus missing receipt is repaired by full fact reconciliation."""
        self.run_engine()
        path = "receipts/bronze/" + self.bundle[0]["batch_id"] + ".json"
        expected = self.store.files.pop(path)
        self.run_engine()
        self.assertEqual(self.store.files[path], expected)

    def test_silver_state_preserved(self):
        """A later pipeline stage survives Bronze replay."""
        self.run_engine()
        self.store.tables["ops.batches"][self.bundle[0]["batch_id"]]["data_stage"] = "SILVER_COMMITTED"
        self.run_engine()
        self.assertEqual(self.store.tables["ops.batches"][self.bundle[0]["batch_id"]]["data_stage"], "SILVER_COMMITTED")

    def test_numeric_contract_matches_producer(self):
        """Ensure duplicate canonicalization code cannot silently change v1 identities."""
        for value in [feature(), {**feature(), "geometry": {"type": "Point", "coordinates": [1.0, 2.0, -3.5]}}]:
            self.assertEqual(s4.bronze_version(value), source_version_id(value))
        self.assertEqual(s4.bronze_json({"x": [6.0, -0.0, "é"]}), json_bytes({"x": [6.0, -0.0, "é"]}))

    def test_unknown_fields_do_not_invent_revision(self):
        """An unknown property is retained in the capture and generates a schema warning."""
        changed = feature()
        changed["properties"]["new_field"] = "future"
        cap = capture(self.store, [changed], "extra")
        self.run_engine([self.bundle, bundle(self.store, [envelope(settings(), cap, 0, changed)], 1)])
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 1)
        self.assertTrue(any(args[3] == "schema_observation" and args[4] == "WARN" for args in self.store.checks))

    def test_nullable_values_and_negative_depth(self):
        """Optional impact values remain null; valid negative depth is accepted."""
        value = feature()
        value["properties"].update(mag=None, mmi=None, alert=None)
        value["geometry"]["coordinates"][2] = -1.2
        self.assertEqual(s4.bronze_feature_errors(value), [])

    def test_deleted_event(self):
        """Preserve explicit withdrawal with absent geometry/time for Silver handling."""
        value = feature()
        value["properties"].update(status="deleted", time=None)
        value["geometry"] = None
        self.assertEqual(s4.bronze_feature_errors(value), [])

    def test_bool_numeric_rejected(self):
        """Python bool must not slip through numeric or epoch validation."""
        value = feature()
        value["properties"].update(updated=True, time=False, mag=True)
        self.assertTrue(set(s4.bronze_feature_errors(value)) >= {"UPDATED_TIME", "EVENT_TIME", "NUMERIC_MAG"})

    def test_offset_gap_rejected(self):
        """A rewritten row cannot hide a hole even if the file checksum is recomputed."""
        m, data, definition, raw = self.bundle
        row = s4.bronze_decode(data)
        row["offset"] = 10
        damaged = json_bytes(row) + b"\n"
        m = {**m, "data_sha256": digest(damaged), "data_bytes": len(damaged)}
        with self.assertRaises(s4.BronzeError):
            s4.bronze_verify_batch(m, damaged, definition)

    def test_key_and_headers_conflict_detected(self):
        """Record digests protect key/headers as well as nullable message bytes."""
        self.run_engine()
        did = next(iter(self.store.tables["bronze.deliveries"]))
        self.store.tables["bronze.deliveries"][did]["record_sha256"] = "corrupt"
        with self.assertRaises(s4.BronzeError):
            self.run_engine()

    def test_post_write_missing_fact_blocks_ack(self):
        """A dropped write is detected by reconciliation before a completion receipt."""
        original = self.store.merge
        def dropped(table, rows, compare):
            """Simulate a storage defect that falsely reports delivery-write success."""
            if table != "bronze.deliveries":
                original(table, rows, compare)
        self.store.merge = dropped
        with self.assertRaises(s4.BronzeError):
            self.run_engine()
        self.assertNotIn("receipts/bronze/" + self.bundle[0]["batch_id"] + ".json", self.store.files)

    def test_pre_1970_and_overflow_timestamps(self):
        """Allow valid negative event epochs and reject timestamp overflow."""
        self.assertEqual(s4.bronze_millis(-1000).year, 1969)
        with self.assertRaises(s4.BronzeError):
            s4.bronze_millis(999999999999999999999)


    def test_unhashable_capture_claim_is_quarantined(self):
        """An array capture ID is a poison record rather than a crashing cache lookup."""
        value = s4.bronze_decode(self.message[1])
        value["capture_id"] = []
        self.run_engine([bundle(self.store, [(self.message[0], json_bytes(value))])])
        self.assertEqual(len(self.store.tables["bronze.quarantine"]), 1)

    def test_very_large_numeric_source_value(self):
        """An integer too large for DOUBLE is quarantined without OverflowError."""
        value = feature()
        value["properties"]["mag"] = 10 ** 400
        self.assertIn("NUMERIC_MAG", s4.bronze_feature_errors(value))

    def test_manifest_object_cannot_differ_from_bytes(self):
        """Changing parsed control fields independently of stored evidence must fail."""
        m, data, definition, raw = self.bundle
        with self.assertRaises(s4.BronzeError):
            self.run_engine([({**m, "landed_at": "2026-10-16T12:00:00Z"}, data, definition, raw)])

    def test_duplicate_source_ordinals_are_not_inflated(self):
        """Two identical source features at different ordinals are two accounted items."""
        cap = capture(self.store, [feature(), feature()], "two-ordinals")
        messages = [envelope(settings(), cap, index, feature()) for index in (0, 1)]
        self.run_engine([bundle(self.store, messages)])
        self.assertEqual(self.store.tables["ops.captures"][cap["capture_id"]]["accounted_item_count"], 2)
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 1)

    def test_same_timestamp_different_payload_preserved(self):
        """Keep both revisions at one source timestamp for Silver conflict resolution."""
        other = feature()
        other["properties"]["mag"] = 7
        cap = capture(self.store, [other], "same-time")
        self.run_engine([self.bundle, bundle(self.store, [envelope(settings(), cap, 0, other)], 1)])
        self.assertEqual(len(self.store.tables["bronze.event_versions"]), 2)

    def test_source_connection_failure_is_not_absence(self):
        """The Volume optional-read path propagates an authorization failure."""
        adapter = s4.SparkBronzeStore.__new__(s4.SparkBronzeStore)
        adapter.root = s4.BRONZE_ROOT
        def unavailable(relative=""):
            """Model storage access denied instead of a missing path."""
            raise PermissionError("Access denied")
        adapter.names = unavailable
        with self.assertRaises(PermissionError):
            adapter.optional("receipts/bronze/example.json")

    def test_lost_receipt_put_reply(self):
        """A cloud receipt PUT with a lost reply succeeds only after exact readback."""
        adapter = s4.SparkBronzeStore.__new__(s4.SparkBronzeStore)
        adapter.root = s4.BRONZE_ROOT
        files = {}
        def optional(relative):
            """Return the in-memory cloud object's exact bytes."""
            return files.get(relative)
        def put(path, value, overwrite):
            """Persist the receipt but simulate the missing HTTP response."""
            files[path[len(s4.BRONZE_ROOT) + 1:]] = value.encode()
            raise ConnectionError("Lost response")
        adapter.optional = optional
        adapter.read = optional
        adapter.utils = SimpleNamespace(fs=SimpleNamespace(mkdirs=lambda path: True, put=put))
        adapter.put_receipt("receipts/bronze/example.json", b'{"proof":1}')
        self.assertEqual(files["receipts/bronze/example.json"], b'{"proof":1}')

    def test_volume_paths_are_confined(self):
        """Prevent arbitrary paths from a malformed landing manifest."""
        adapter = s4.SparkBronzeStore.__new__(s4.SparkBronzeStore)
        adapter.root = s4.BRONZE_ROOT
        for path in ("../secret", "/tmp/source", "a/../source", "a//b"):
            with self.assertRaises(s4.BronzeError):
                adapter.path(path)

    def test_job_wait_bounds_validated_before_launch(self):
        """The extended Job poll limit remains bounded before any external request."""
        from quake_pipeline.jobs import sync_ledger
        from quake_pipeline.config import Settings
        from quake_pipeline.errors import ConfigurationError
        config = Settings(host="https://dbc-example.cloud.databricks.com", token="test")
        with self.assertRaises(ConfigurationError):
            sync_ledger(config, 1, "token", None, wait_seconds=2401)


    def test_notebook_orchestrator_success_and_pending_failure(self):
        """Run actual notebook control flow and verify truthful terminal status."""
        import contextlib
        import io
        notebook = Path(__file__).resolve().parents[1] / "databricks/sprint_04_process_bronze.py"
        code = compile(notebook.read_text(), str(notebook), "exec")
        for incomplete in (False, True):
            with self.subTest(incomplete=incomplete):
                self.setUp()
                selected = self.bundle
                if incomplete:
                    cap = capture(self.store, [feature(), feature(1)], "notebook-partial")
                    selected = bundle(self.store, [envelope(settings(), cap, 0, feature())], 1)
                store = self.store
                def upsert(name, rows, fields):
                    """Keep operational terminal records for actual notebook assertions."""
                    key = s4.BRONZE_KEYS[name]
                    for row in rows:
                        store.tables[name][row[key]] = copy.deepcopy(row)
                def work(recheck, maximum):
                    """Supply one genuine pending sealed batch to the notebook."""
                    return [selected], 0
                store.upsert_ops, store.bundles = upsert, work
                widgets = {"volume_root": s4.BRONZE_ROOT, "bronze_fail_at": "none",
                           "bronze_recheck": "false", "bronze_max_batches": "32"}
                namespace = dict(s4.__dict__)
                namespace.update(spark=None,
                    dbutils=SimpleNamespace(widgets=SimpleNamespace(text=lambda *args: None, get=widgets.get)),
                    SparkBronzeStore=lambda *args: store)
                with contextlib.redirect_stdout(io.StringIO()):
                    if incomplete:
                        with self.assertRaises(s4.BronzeError):
                            exec(code, namespace)
                    else:
                        exec(code, namespace)
                run = next(iter(store.tables["ops.runs"].values()))
                self.assertEqual(run["status"], "FAILED" if incomplete else "SUCCESS")


if __name__ == "__main__":
    unittest.main()

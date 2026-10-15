"""Exercise cloud handoff failures using in-memory Kafka and Volume fixtures.

The pinned-client API tests use mocked network clients when kafka-python is
installed. No test contacts a service or writes source/delivery payloads to disk.
"""

import unittest
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from quake_pipeline.contracts import decode_json, digest, json_bytes, stable_id
from quake_pipeline.errors import BudgetExceeded, ConfigurationError, ContentConflict, ContractError
from quake_pipeline.kafka_config import KafkaSettings
from quake_pipeline.landing import (commit_verified, delivery_row, land, recover,
                                     seal_batch, verify_batch)
from quake_pipeline.outbox import (InjectedFailure, bind_stream, dispatch, envelope,
                                  stream_id)

try:
    import kafka
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


def settings():
    """Return non-secret test settings for the isolated development stream."""
    return KafkaSettings("broker.example:1234", "earthquake_events_dev", "earthquake-dev-landing-v1",
                         "v1", "v1", "fake-user", "fake-password", "fake-ca")


def feature(index=0):
    """Build one small versioned USGS feature for dispatch/replay assertions."""
    return {"type": "Feature", "id": "eq" + str(index),
            "properties": {"updated": 1000 + index, "time": 500, "mag": 3},
            "geometry": {"type": "Point", "coordinates": [1, 2, 3]}}


class MemoryVolume:
    """Model immutable cloud bytes and genuine missing-path reads."""

    def __init__(self):
        """Keep source/delivery bytes only in this test object's memory."""
        self.files = {}

    def read_optional(self, path):
        """Return immutable fixture bytes or genuine absence."""
        return self.files.get(path)

    def put_immutable(self, path, payload):
        """Allow exact retries but reject an attempt to replace evidence."""
        if path in self.files and self.files[path] != payload:
            raise ContentConflict("Fixture immutable conflict")
        self.files[path] = payload

    def child_directories(self, path):
        """List immediate generated child IDs without depending on offset state."""
        prefix = path + "/"
        return sorted({key[len(prefix):].split("/")[0] for key in self.files
                       if key.startswith(prefix) and "/" in key[len(prefix):]})


class MemoryKafka:
    """Model broker records, manual position and ambiguous commit responses."""

    def __init__(self, config=None, partitions=None):
        """Initialize one or two retained partitions with no committed positions."""
        self.settings = config or settings()
        self.records = {partition: [] for partition in (partitions or [0])}
        self.starts = {partition: 0 for partition in self.records}
        self.commits = {partition: None for partition in self.records}
        self.position = (0, 0)
        self.commit_calls = []
        self.lose_ack = False
        self.reject_commit = False
        self.poll_append = None
        self.config = {"retention.ms": "259200000", "retention.bytes": "-1", "cleanup.policy": "delete"}

    def topic_config(self):
        """Return effective inspectable retention for the stream contract."""
        return self.config.copy()

    def partitions(self):
        """Return the existing fixture topic's partition IDs."""
        return sorted(self.records)

    def add(self, value, key=b"eq", partition=0, headers=None):
        """Append raw bytes including malformed JSON or null Kafka tombstones."""
        offset = self.starts[partition] + len(self.records[partition])
        record = SimpleNamespace(topic=self.settings.topic, partition=partition, offset=offset,
                                 timestamp=1000, key=key, value=value, headers=headers or [])
        self.records[partition].append(record)
        return {"partition": partition, "offset": offset}

    def produce(self, messages):
        """Acknowledge every physical send; application re-sends get new offsets."""
        return [self.add(value, key) for key, value in messages]

    def bounds(self, partition):
        """Return retained start/end and committed next position."""
        return (self.starts[partition], self.starts[partition] + len(self.records[partition]),
                self.commits[partition])

    def seek(self, partition, offset):
        """Assign and seek the same way as the manual real consumer adapter."""
        self.position = partition, offset

    def poll(self):
        """Return at most one hundred records, optionally appending after snapshot."""
        partition, offset = self.position
        if self.poll_append is not None:
            value, self.poll_append = self.poll_append, None
            self.add(value, partition=partition)
        output = [record for record in self.records[partition] if record.offset >= offset][:100]
        if output:
            self.position = partition, output[-1].offset + 1
        return output

    def commit(self, partition, next_offset, batch_id):
        """Model both a rejected commit and an accepted commit whose ACK is lost."""
        self.commit_calls.append((partition, next_offset, batch_id))
        if self.reject_commit:
            raise OSError("fixture broker rejected commit")
        self.commits[partition] = next_offset
        if self.lose_ack:
            raise OSError("fixture commit response lost")


class HandoffTests(unittest.TestCase):
    """Verify ordering, idempotency, exact bytes, recovery and bounded failures."""

    def setUp(self):
        """Create independent in-memory cloud clients and a metadata event sink."""
        self.settings, self.volume = settings(), MemoryVolume()
        self.kafka = MemoryKafka(self.settings)
        self.events = []

    def capture(self, features):
        """Freeze a fixture FeatureCollection with the same source manifest fields."""
        data = json_bytes({"type": "FeatureCollection", "metadata": {"count": len(features), "generated": 1000, "status": 200}, "features": features})
        cid = stable_id("test-capture", digest(data))
        path = f"captures/{'a' * 64}/{cid}/response.json"
        self.volume.put_immutable(path, data)
        return {"capture_id": cid, "collection_id": "a" * 64, "response_relative_path": path,
                "response_sha256": digest(data), "expected_item_count": len(features),
                "source_id": "usgs-m2p5-day", "source_revision": 1,
                "fetched_at": "2026-10-15T10:01:00.000000Z"}

    def test_stream_binding_and_retention(self):
        """Reject accidental group changes, unbounded retention and compaction."""
        bind_stream(self.volume, self.kafka, self.settings)
        with self.assertRaises(ContentConflict):
            bind_stream(self.volume, self.kafka, replace(self.settings, group="earthquake-dev-landing-other"))
        for config in ({"retention.ms": "-1", "cleanup.policy": "delete"},
                       {"retention.ms": "259200001", "cleanup.policy": "delete"},
                       {"retention.ms": "1000", "cleanup.policy": "compact"}):
            self.kafka.config = config
            with self.assertRaises(ContractError):
                bind_stream(self.volume, self.kafka, self.settings)

    def test_dispatch_repeat_reuses_receipts(self):
        """A normal application retry publishes no additional broker deliveries."""
        capture = self.capture([feature(0), feature(1)])
        first = dispatch(self.volume, self.kafka, self.settings, capture, self.events.append)
        self.assertEqual(first, dispatch(self.volume, self.kafka, self.settings, capture, self.events.append))
        self.assertEqual(len(self.kafka.records[0]), 2)

    def test_after_produce_replay_has_same_source_ids(self):
        """A lost publish receipt can duplicate delivery without changing source identity."""
        capture = self.capture([feature()])
        with self.assertRaises(InjectedFailure):
            dispatch(self.volume, self.kafka, self.settings, capture, self.events.append, "after-produce")
        dispatch(self.volume, self.kafka, self.settings, capture, self.events.append)
        self.assertEqual(len(self.kafka.records[0]), 2)
        self.assertEqual(self.kafka.records[0][0].value, self.kafka.records[0][1].value)

    def test_explicit_dispatch_generation_republishes_capture(self):
        """Deliberate replay after expiry uses the same source item with new delivery."""
        capture = self.capture([feature()])
        dispatch(self.volume, self.kafka, self.settings, capture, self.events.append)
        dispatch(self.volume, self.kafka, replace(self.settings, dispatch_generation="replay1"), capture, self.events.append)
        self.assertEqual(self.kafka.records[0][0].value, self.kafka.records[0][1].value)

    def test_empty_and_invalid_source_items_are_accountable(self):
        """Empty captures seal zero chunks; malformed features still reach Kafka."""
        result = dispatch(self.volume, self.kafka, self.settings, self.capture([]), self.events.append)
        self.assertEqual(result["chunk_count"], 0)
        dispatch(self.volume, self.kafka, self.settings, self.capture([None, {"invalid": True}]), self.events.append)
        values = [decode_json(record.value) for record in self.kafka.records[0]]
        self.assertEqual([item["feature"] for item in values], [None, {"invalid": True}])
        self.assertNotEqual(values[0]["source_item_id"], values[1]["source_item_id"])

    def test_capture_corruption_blocks_dispatch(self):
        """A changed source file never publishes new bytes under an old capture ID."""
        capture = self.capture([feature()])
        self.volume.files[capture["response_relative_path"]] += b" "
        with self.assertRaises(ContractError):
            dispatch(self.volume, self.kafka, self.settings, capture, self.events.append)
        self.assertEqual(self.kafka.records[0], [])

    def test_oversize_source_is_pending_without_produce(self):
        """Admission bounds stop oversized envelopes while preserving frozen evidence."""
        with self.assertRaises(BudgetExceeded):
            dispatch(self.volume, self.kafka, replace(self.settings, max_message_bytes=10), self.capture([feature()]), self.events.append)
        self.assertEqual(self.kafka.records[0], [])

    def test_receipt_corruption_blocks_skip(self):
        """A source receipt cannot silently suppress publication for different bytes."""
        capture = self.capture([feature()])
        dispatch(self.volume, self.kafka, self.settings, capture, self.events.append)
        key = next(key for key in self.volume.files if "/chunk-" in key)
        damaged = decode_json(self.volume.files[key])
        damaged["message_sha256"] = ["incorrect"]
        self.volume.files[key] = json_bytes(damaged)
        with self.assertRaises(ContentConflict):
            dispatch(self.volume, self.kafka, self.settings, capture, self.events.append)

    def test_after_data_no_commit_and_retry_converges(self):
        """An unsealed data file cannot advance offsets and is safe to upload again."""
        self.kafka.add(b"\xff\x00")
        with self.assertRaises(InjectedFailure):
            land(self.volume, self.kafka, self.settings, "first", self.events.append, "after-data")
        self.assertIsNone(self.kafka.commits[0])
        self.assertFalse(any(key.endswith("manifest.json") for key in self.volume.files))
        land(self.volume, self.kafka, self.settings, "retry", self.events.append)
        self.assertEqual(self.kafka.commits[0], 1)

    def test_after_manifest_recovery_commits_without_reconsume(self):
        """Recovery re-verifies sealed bytes and repairs only the pending commit."""
        self.kafka.add(b"not-json")
        with self.assertRaises(InjectedFailure):
            land(self.volume, self.kafka, self.settings, "first", self.events.append, "after-manifest")
        self.assertIsNone(self.kafka.commits[0])
        manifests = recover(self.volume, self.kafka, self.settings, self.events.append)
        self.assertEqual(self.kafka.commits[0], 1)
        self.assertEqual(len(manifests), 1)

    def test_after_commit_manifest_remains_recoverable(self):
        """Already committed offsets never hide pending landing files from recovery."""
        self.kafka.add(b"{}")
        with self.assertRaises(InjectedFailure):
            land(self.volume, self.kafka, self.settings, "first", self.events.append, "after-commit")
        self.assertEqual(self.kafka.commits[0], 1)
        recovered = recover(self.volume, self.kafka, self.settings, self.events.append)
        self.assertEqual(len(recovered), 1)
        self.assertEqual(land(self.volume, self.kafka, self.settings, "retry", self.events.append), 0)

    def test_lost_commit_ack_is_verified(self):
        """An accepted commit with a lost reply succeeds after offset readback."""
        self.kafka.add(b"{}")
        self.kafka.lose_ack = True
        self.assertEqual(land(self.volume, self.kafka, self.settings, "run", self.events.append), 1)
        self.assertEqual(self.kafka.commits[0], 1)

    def test_rejected_commit_preserves_durable_manifest(self):
        """Broker rejection leaves recoverable bytes and does not fake a commit."""
        self.kafka.add(b"{}")
        self.kafka.reject_commit = True
        with self.assertRaises(OSError):
            land(self.volume, self.kafka, self.settings, "run", self.events.append)
        self.assertIsNone(self.kafka.commits[0])
        self.kafka.reject_commit = False
        self.assertEqual(len(recover(self.volume, self.kafka, self.settings, self.events.append)), 1)
        self.assertEqual(self.kafka.commits[0], 1)

    def test_corrupt_landing_blocks_recovery_commit(self):
        """A correct manifest alone cannot authorize a commit for changed data."""
        self.kafka.add(b"{}")
        with self.assertRaises(InjectedFailure):
            land(self.volume, self.kafka, self.settings, "run", self.events.append, "after-manifest")
        key = next(key for key in self.volume.files if key.endswith("records.jsonl"))
        self.volume.files[key] += b" "
        with self.assertRaises(ContractError):
            recover(self.volume, self.kafka, self.settings, self.events.append)
        self.assertIsNone(self.kafka.commits[0])

    def test_null_empty_binary_and_headers_survive(self):
        """Lossless landing differentiates null, empty and invalid UTF-8 values."""
        for value in (None, b"", b"\xff\x00"):
            self.kafka.add(value, key=None, headers=[("opaque", b"\x80"), ("missing", None)])
        land(self.volume, self.kafka, self.settings, "run", self.events.append)
        rows = [decode_json(line) for key, data in self.volume.files.items() if key.endswith("records.jsonl") for line in data.splitlines()]
        self.assertEqual([row["value_b64"] for row in rows], [None, "", "/wA="])
        self.assertEqual(rows[0]["headers_b64"], [["opaque", "gA=="], ["missing", None]])

    def test_two_partitions_are_independently_committed(self):
        """Aiven's two-partition topology preserves distinct delivery identities."""
        self.kafka = MemoryKafka(partitions=[0, 1])
        for partition in (0, 1):
            self.kafka.add(b"{}", partition=partition)
        self.assertEqual(land(self.volume, self.kafka, self.settings, "run", self.events.append), 2)
        self.assertEqual(self.kafka.commits, {0: 1, 1: 1})

    def test_offset_gap_and_missing_group_fail_closed(self):
        """No implicit earliest/latest reset conceals expired broker messages."""
        for committed in (None, 0, 20):
            self.kafka.starts[0] = 10
            self.kafka.commits[0] = committed
            with self.assertRaisesRegex(ContractError, "OFFSET_GAP"):
                land(self.volume, self.kafka, self.settings, "run", self.events.append)
        self.assertEqual(self.kafka.commit_calls, [])

    def test_noncontiguous_polled_records_are_not_committed(self):
        """Unexpected offset holes cannot pass the durable range accounting rule."""
        self.kafka.add(b"{}")
        self.kafka.add(b"{}")
        self.kafka.records[0][1].offset = 2
        with self.assertRaisesRegex(ContractError, "OFFSET_GAP"):
            land(self.volume, self.kafka, self.settings, "run", self.events.append)
        self.assertEqual(self.kafka.commits[0], 1)
        self.assertTrue(all(target <= 1 for partition, target, bid in self.kafka.commit_calls))

    def test_end_snapshot_bounds_new_arrivals(self):
        """A new record arriving after snapshot waits for the next bounded drain."""
        self.kafka.add(b"first")
        self.kafka.poll_append = b"arrived-later"
        self.assertEqual(land(self.volume, self.kafka, self.settings, "run", self.events.append), 1)
        self.assertEqual(self.kafka.commits[0], 1)

    def test_byte_splits_reseek_unlanded_polled_records(self):
        """A byte bound does not discard the remainder of an already-polled group."""
        for index in range(3):
            self.kafka.add(b"x" * 100)
        config = replace(self.settings, max_batch_bytes=850)
        self.assertEqual(land(self.volume, self.kafka, config, "run", self.events.append), 3)
        self.assertEqual(self.kafka.commits[0], 3)
        self.assertGreater(len(self.kafka.commit_calls), 1)

    def test_record_budget_stops_and_next_run_resumes(self):
        """A run record cap preserves committed work and leaves a visible backlog."""
        for index in range(3):
            self.kafka.add(b"{}")
        with self.assertRaises(BudgetExceeded):
            land(self.volume, self.kafka, replace(self.settings, max_land_records=1), "run", self.events.append)
        self.assertEqual(self.kafka.commits[0], 1)
        self.assertEqual(land(self.volume, self.kafka, self.settings, "retry", self.events.append), 2)

    def test_commit_cannot_skip_unlanded_prefix(self):
        """A complete later batch never authorizes committing an earlier hole."""
        for index in range(2):
            self.kafka.add(b"{}")
        manifest = seal_batch(self.volume, self.settings, [delivery_row(self.kafka.records[0][1], self.settings)], "run")
        with self.assertRaises(ContractError):
            commit_verified(self.volume, self.kafka, self.settings, manifest)
        self.assertEqual(self.kafka.commit_calls, [])

    def test_stream_recreation_changes_delivery_ids(self):
        """An explicitly new stream generation cannot collide on reused offset zero."""
        self.kafka.add(b"{}")
        first = delivery_row(self.kafka.records[0][0], self.settings)
        second = delivery_row(self.kafka.records[0][0], replace(self.settings, generation="v2"))
        self.assertNotEqual(first["delivery_id"], second["delivery_id"])

    def test_inventory_bound_is_visible(self):
        """An oversized recovery scan fails instead of hiding unprocessed batches."""
        self.kafka.add(b"{}")
        land(self.volume, self.kafka, self.settings, "run", self.events.append)
        with self.assertRaises(BudgetExceeded):
            recover(self.volume, self.kafka, replace(self.settings, max_batches=0), self.events.append)

    def test_known_backlog_with_empty_polls_fails_bounded(self):
        """A disconnected/stalled consumer cannot claim an empty backlog as success."""
        self.kafka.add(b"{}")
        with patch.object(self.kafka, "poll", return_value=[]):
            with self.assertRaises(BudgetExceeded):
                land(self.volume, self.kafka, self.settings, "run", self.events.append)
        self.assertEqual(self.kafka.commit_calls, [])

    def test_inspection_has_no_volume_writes(self):
        """A read-only topic/capacity inspection cannot create durable source state."""
        bind_stream(self.volume, self.kafka, self.settings, persist=False)
        self.assertEqual(self.volume.files, {})

    def test_configuration_rejects_smoke_topic_and_hides_secrets(self):
        """Misconfiguration cannot mix the proven smoke topic with development data."""
        env = {"AIVEN_BOOTSTRAP_SERVERS": "broker.example:1234", "AIVEN_KAFKA_USERNAME": "fake-user",
               "AIVEN_KAFKA_PASSWORD": "fake-password", "AIVEN_KAFKA_CA_PEM": "fake-ca",
               "EQ_KAFKA_TOPIC": "earthquake_events_dev", "EQ_KAFKA_CONSUMER_GROUP": "earthquake-dev-landing-v1",
               "EQ_STREAM_GENERATION": "v1"}
        result = KafkaSettings.from_env(env)
        self.assertNotIn("fake-password", repr(result))
        self.assertNotIn("fake-ca", repr(result))
        with self.assertRaises(ConfigurationError):
            KafkaSettings.from_env({**env, "EQ_KAFKA_TOPIC": "earthquake_compat_smoke"})


@unittest.skipUnless(HAS_KAFKA, "Install requirements-kafka.txt for pinned-client API checks")
class PinnedClientTests(unittest.TestCase):
    """Exercise actual kafka-python method signatures with all networking replaced."""

    def test_consumer_offsets_commit_and_close_signatures(self):
        """Use autospec to detect missing leader_epoch and unsupported timeout args."""
        from kafka import KafkaConsumer, TopicPartition
        from kafka.structs import OffsetAndMetadata
        from quake_pipeline.kafka_client import KafkaClient
        with patch("kafka.KafkaConsumer", autospec=KafkaConsumer) as factory, patch("quake_pipeline.kafka_client.security", return_value={}):
            client = KafkaClient(settings())
            tp = TopicPartition(settings().topic, 0)
            factory.return_value.beginning_offsets.return_value = {tp: 0}
            factory.return_value.end_offsets.return_value = {tp: 1}
            factory.return_value.committed.return_value = 0
            self.assertEqual(client.bounds(0), (0, 1, 0))
            client.seek(0, 0)
            client.commit(0, 1, "a" * 64)
            saved = factory.return_value.commit.call_args.args[0][tp]
            self.assertEqual(saved, OffsetAndMetadata(1, "eq3:" + "a" * 48, -1))
            client.close()
            factory.return_value.close.assert_called_with(autocommit=False)

    def test_admin_and_producer_accept_exact_config_keys(self):
        """Reject client-specific config drift before it reaches a cloud broker."""
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient
        from quake_pipeline.kafka_client import KafkaClient
        options = {"allow_auto_create_topics": False, "request_timeout_ms": 30000}
        with patch("kafka.KafkaConsumer"), patch("quake_pipeline.kafka_client.security", return_value=options), patch("kafka.admin.KafkaAdminClient", autospec=KafkaAdminClient) as admin, patch("kafka.KafkaProducer", autospec=KafkaProducer) as producer:
            client = KafkaClient(settings())
            entry = (0, "", 2, settings().topic, [("retention.ms", "259200000", False)])
            admin.return_value.describe_configs.return_value = [SimpleNamespace(resources=[entry])]
            self.assertEqual(client.topic_config(), {"retention.ms": "259200000"})
            self.assertTrue(set(admin.call_args.kwargs) <= set(KafkaAdminClient.DEFAULT_CONFIG))
            producer.return_value.send.return_value.get.return_value = SimpleNamespace(partition=0, offset=0)
            self.assertEqual(client.produce([(b"eq", b"{}")]), [{"partition": 0, "offset": 0}])
            self.assertTrue(set(producer.call_args.kwargs) <= set(KafkaProducer.DEFAULT_CONFIG))
            self.assertTrue(producer.call_args.kwargs["enable_idempotence"])
            self.assertEqual(producer.call_args.kwargs["max_in_flight_requests_per_connection"], 1)


if __name__ == "__main__":
    unittest.main()

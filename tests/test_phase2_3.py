"""Phase 2/3 boundary tests: durable manifests, offset repair, and Silver identity."""

from datetime import datetime, timezone
import hashlib
import json

import pytest

from earthquake_watch.capture import freeze_capture
from earthquake_watch.contracts import ContractError, SourceConfig
from earthquake_watch.kafka_transport import make_envelope, publish_capture
from earthquake_watch.landing import Delivery, freeze_landing, land_once, verify_landing
from earthquake_watch.silver_rules import normalize
from earthquake_watch.volume_client import MemoryVolume, MountedVolume, VolumeError


NOW = datetime(2026, 10, 19, 19, 48, tzinfo=timezone.utc)
CFG = SourceConfig("usgs-all-day", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
                   16777216, 30, 300)


def feature(event_id="us1", updated=1792439000000, magnitude=3.2):
    """Return one small, valid USGS-style point feature."""
    return {"type": "Feature", "id": event_id, "properties": {
        "updated": updated, "time": 1792438000000, "mag": magnitude, "tsunami": 0,
        "place": "Test location", "sig": 200},
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0, 5.0]}}


def frozen(volume, features):
    """Write a test capture using the real Phase 1 manifest boundary."""
    raw = json.dumps({"type": "FeatureCollection", "metadata": {
        "generated": 1792437420000, "count": len(features), "status": 200},
        "features": features}).encode()
    result = freeze_capture(raw, NOW, NOW, CFG, volume)
    return json.loads(volume.read_optional(result.manifest_path))


class Broker:
    """Invoke producer callbacks with consecutive broker offsets."""

    def __init__(self):
        """Start with an empty delivered-message log."""
        self.messages = []

    def produce(self, topic, key, value, on_delivery):
        """Ack each enqueue with a known physical offset."""
        offset = len(self.messages)
        self.messages.append((topic, key, value))
        on_delivery(None, Ack(offset))

    def poll(self, seconds):
        """Allow the production code to service delivery events."""

    def flush(self, seconds):
        """Report no unacknowledged messages."""
        return 0


class Position:
    """Minimal Kafka TopicPartition/message position interface."""

    def __init__(self, partition, offset, topic="earthquake_events_dev"):
        """Represent a committed position or a requested seek."""
        self.partition, self.offset, self.topic, self.error = partition, offset, topic, None


class Ack:
    """Minimal delivered-message callback interface."""

    def __init__(self, offset):
        """Store the broker-assigned offset."""
        self.number = offset

    def partition(self):
        """Return the test topic's first partition."""
        return 0

    def offset(self):
        """Return the broker-assigned offset."""
        return self.number


class Consumer:
    """Record explicit commits and serve an empty poll after recovery."""

    def __init__(self, offsets=(0, 0)):
        """Initialize committed offsets independently for two partitions."""
        self.offsets = dict(enumerate(offsets))
        self.commits = []
        self.seeks = []

    def assign(self, partitions):
        """Accept fixed two-partition assignment."""
        assert len(partitions) == 2

    def committed(self, partitions, timeout):
        """Return the broker's current committed next offsets."""
        return [Position(part.partition, self.offsets[part.partition]) for part in partitions]

    def seek(self, partition):
        """Observe the first position after recovery."""
        self.seeks.append((partition.partition, partition.offset))

    def get_watermark_offsets(self, partition, timeout):
        """Allow recovery tests to seek inside a retained topic range."""
        return 0, 100

    def poll(self, seconds):
        """End the one-shot poll with no additional messages."""
        return None

    def commit(self, offsets, asynchronous):
        """Ack an explicit synchronous next-offset commit."""
        assert asynchronous is False
        part = offsets[0]
        self.commits.append((part.partition, part.offset))
        return offsets


class Message:
    """One consumed Kafka record for the live landing branch."""

    def topic(self):
        """Return the configured development topic."""
        return "earthquake_events_dev"

    def partition(self):
        """Return the first partition."""
        return 0

    def offset(self):
        """Return the first physical offset."""
        return 0

    def key(self):
        """Return the stable USGS key."""
        return b"us1"

    def value(self):
        """Return an invalid but durably retainable envelope."""
        return b"{broken"

    def error(self):
        """Report a successful Kafka fetch."""
        return None


def test_publish_acks_and_retry_skips_frozen_capture():
    """A complete receipt prevents an accidental repeat publication."""
    volume, broker = MemoryVolume(), Broker()
    manifest = frozen(volume, [feature()])
    first = publish_capture(volume, manifest["capture_id"], CFG, broker, lambda: NOW)
    second = publish_capture(volume, manifest["capture_id"], CFG, broker, lambda: NOW)
    assert first == second and len(broker.messages) == 1
    key, value = broker.messages[0][1:]
    assert key == b"us1" and json.loads(value)["feature"]["id"] == "us1"
    assert first["acknowledged_count"] == 1


def test_manifest_last_and_bad_bytes_survive_for_quarantine():
    """Invalid wire bytes land, but tampering with their row fails verification."""
    volume = MemoryVolume()
    data = Delivery("earthquake_events_dev", 1, 5, b"bad", b"\xff\x00", NOW)
    manifest = freeze_landing([data], volume, lambda: NOW)
    root = f"landing/{manifest['landing_batch_id']}"
    assert volume.writes == [root + "/records.jsonl", root + "/manifest.json"]
    verified, rows = verify_landing(volume, manifest["landing_batch_id"])
    assert verified == manifest and rows[0]["raw_payload"] is None
    corrupted = dict(rows[0], raw_payload="not the wire bytes")
    replacement = (json.dumps(corrupted) + "\n").encode()
    volume.files[root + "/records.jsonl"] = replacement
    # Even a forged checksum cannot make the altered row pass per-delivery verification.
    altered = dict(manifest, data_bytes=len(replacement), data_sha256=hashlib.sha256(replacement).hexdigest())
    volume.files[root + "/manifest.json"] = json.dumps(altered).encode()
    with pytest.raises(ContractError):
        verify_landing(volume, manifest["landing_batch_id"])


def test_recover_verified_batches_in_each_partition_offset_order():
    """A prior manifest is committed per partition before fresh polling."""
    volume = MemoryVolume()
    def batch(partition, offset):
        """Freeze one independently verified partition prefix."""
        return freeze_landing([Delivery("earthquake_events_dev", partition, offset,
                                        b"us1", b"{}", NOW)], volume, lambda: NOW)
    batch(1, 0)
    batch(0, 0)
    batch(0, 1)
    consumer = Consumer()
    assert land_once(volume, consumer, lambda topic, partition, offset=-1001: Position(partition, offset)) is None
    assert consumer.seeks == [(0, 2), (1, 1)]
    assert set(consumer.commits) == {(0, 1), (0, 2), (1, 1)}


def test_live_landing_commits_only_after_manifest_write():
    """The file and manifest precede the broker offset commit."""
    volume, consumer = MemoryVolume(), Consumer()
    pending = iter([Message(), None])
    consumer.poll = lambda seconds: next(pending)
    result = land_once(volume, consumer, lambda topic, partition, offset=-1001: Position(partition, offset))
    assert result["record_count"] == 1 and consumer.commits == [(0, 1)]
    root = f"landing/{result['landing_batch_id']}"
    assert volume.writes[:2] == [root + "/records.jsonl", root + "/manifest.json"]
    assert volume.writes[2].startswith("receipts/offset/")


def test_missing_manifest_does_not_advance_offsets():
    """A crash at the final manifest leaves Kafka's offset unchanged."""
    class NoManifest(MemoryVolume):
        """Simulate a storage failure at the visibility boundary."""

        def put_immutable(self, relative, data):
            """Refuse the final manifest while preserving the data file."""
            if relative.endswith("manifest.json"):
                raise RuntimeError("test storage failure")
            super().put_immutable(relative, data)

    volume, consumer = NoManifest(), Consumer()
    pending = iter([Message(), None])
    consumer.poll = lambda seconds: next(pending)
    with pytest.raises(RuntimeError, match="storage failure"):
        land_once(volume, consumer, lambda topic, partition, offset=-1001: Position(partition, offset))
    assert consumer.commits == []


def test_silver_repeated_version_and_same_timestamp_conflict_identity():
    """Equal logical versions collapse, but different payloads stay distinct."""
    manifest = {"capture_id": "a" * 64, "source_id": CFG.source_id,
                "source_generated_at": "2026-10-19T19:17:00.000Z"}
    def row(value):
        """Wrap an unchanged or revised feature in the actual transport envelope."""
        key, payload = make_envelope(value, manifest, NOW)
        return {"raw_payload": payload.decode(), "key": key.decode()}
    first = normalize(row(feature()), NOW, "rev1")
    repeat = normalize(row(feature()), NOW, "rev1")
    corrected = normalize(row(feature(magnitude=3.3)), NOW, "rev1")
    assert first["event_version_id"] == repeat["event_version_id"]
    assert corrected["event_version_id"] != first["event_version_id"]
    assert corrected["usgs_updated_ms"] == first["usgs_updated_ms"]
    with pytest.raises(ContractError, match="Kafka key"):
        normalize(dict(row(feature()), key="another-event"), NOW, "rev1")


def test_missing_event_identity_quarantinable_without_blocking_bronze():
    """An event without a source ID is still publishable for quarantine."""
    manifest = {"capture_id": "a" * 64, "source_id": CFG.source_id,
                "source_generated_at": "2026-10-19T19:17:00.000Z"}
    key, payload = make_envelope(feature(event_id=""), manifest, NOW)
    assert key.startswith(b"invalid-")
    with pytest.raises(ContractError, match="USGS event ID"):
        normalize({"raw_payload": payload.decode(), "key": key.decode()}, NOW, "rev1")


def test_mounted_volume_reads_without_a_databricks_token(tmp_path):
    """The Spark-side reader requires file access, not REST credentials."""
    directory = tmp_path / "captures" / "batch"
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_bytes(b"evidence")
    volume = MountedVolume(str(tmp_path))
    assert volume.read_optional("captures/batch/manifest.json") == b"evidence"
    assert volume.list_entries("captures/batch") == [("manifest.json", False)]
    assert volume.read_optional("captures/batch/absent.json") is None
    with pytest.raises(VolumeError):
        volume.read_optional("../unrelated")

"""Publish one envelope per frozen USGS feature and record broker acknowledgements."""

from datetime import datetime, timezone
import hashlib
import json
import os
import uuid
from typing import Any, Protocol

from .contracts import ContractError, SourceConfig, json_bytes, stamp, validate_feed
from .volume_client import VolumeError


TOPIC = "earthquake_events_dev"
GROUP = "earthquake-dev-landing-v1"
ENVELOPE_VERSION = "earthquake-event-envelope-v1"


class ReadonlyVolume(Protocol):
    """Read the immutable capture and landing evidence interface."""

    def read_optional(self, relative: str) -> bytes | None:
        """Read an existing immutable file, or return None for 404."""
        ...

    def list_entries(self, relative: str) -> list[tuple[str, bool]]:
        """Return immediate child names and directory flags."""
        ...


class Volume(ReadonlyVolume, Protocol):
    """Add immutable creation for the external capture and transport processes."""

    def put_immutable(self, relative: str, data: bytes) -> None:
        """Create and verify a file without replacing different bytes."""
        ...


def canonical_feature(feature: dict[str, Any]) -> bytes:
    """Hash a stable JSON representation of the parsed GeoJSON feature."""
    return json.dumps(feature, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def kafka_config(group: str | None = None) -> dict[str, Any]:
    """Load Aiven SASL/TLS settings without writing credentials to a file."""
    required = ("AIVEN_BOOTSTRAP_SERVERS", "AIVEN_KAFKA_USERNAME",
                "AIVEN_KAFKA_PASSWORD", "AIVEN_KAFKA_CA_PEM")
    values = {key: os.environ.get(key, "") for key in required}
    if any(not value for value in values.values()):
        raise ContractError("Aiven Kafka connection settings are incomplete")
    result: dict[str, Any] = {
        "bootstrap.servers": values["AIVEN_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": values["AIVEN_KAFKA_USERNAME"],
        "sasl.password": values["AIVEN_KAFKA_PASSWORD"],
        "ssl.ca.pem": values["AIVEN_KAFKA_CA_PEM"],
    }
    if group is not None:
        result.update({"group.id": group, "enable.auto.commit": False,
                       "enable.auto.offset.store": False,
                       "auto.offset.reset": "earliest"})
    else:
        result["acks"] = "all"
    return result


def verified_capture(volume: ReadonlyVolume, capture_id: str, cfg: SourceConfig) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the complete capture only after its manifest and checksum agree."""
    if len(capture_id) != 64 or any(c not in "0123456789abcdef" for c in capture_id):
        raise ContractError("Invalid capture ID")
    root = f"captures/{capture_id}"
    manifest_bytes = volume.read_optional(root + "/manifest.json")
    if manifest_bytes is None:
        raise ContractError("Capture is not frozen")
    try:
        manifest = json.loads(manifest_bytes)
        path = manifest["response_relative_path"]
    except (ValueError, KeyError, TypeError) as exc:
        raise ContractError("Invalid capture manifest") from exc
    if (manifest.get("manifest_version") != 1 or manifest.get("state") != "FROZEN"
            or manifest.get("capture_id") != capture_id or manifest.get("source_id") != cfg.source_id
            or manifest.get("source_url") != cfg.source_url or path != root + "/response.json"):
        raise ContractError("Capture manifest identity mismatch")
    raw = volume.read_optional(path)
    if raw is None or len(raw) != manifest.get("response_bytes") or hashlib.sha256(raw).hexdigest() != manifest.get("response_sha256"):
        raise ContractError("Frozen capture bytes do not match manifest")
    valid = validate_feed(raw, cfg, datetime.now(timezone.utc))
    if valid.count != manifest.get("expected_event_count"):
        raise ContractError("Frozen capture count mismatch")
    return manifest, json.loads(raw)


def make_envelope(feature: dict[str, Any], manifest: dict[str, Any], published_at: datetime) -> tuple[bytes, bytes]:
    """Keep feature content intact; use a stable fallback key for missing IDs."""
    digest = hashlib.sha256(canonical_feature(feature)).hexdigest()
    event_id = feature.get("id")
    if not isinstance(event_id, str) or not event_id:
        event_id = None
    props = feature.get("properties")
    updated = props.get("updated") if isinstance(props, dict) else None
    envelope = {
        "schema_version": ENVELOPE_VERSION, "capture_id": manifest["capture_id"],
        "source_id": manifest["source_id"],
        "source_generated_ms": int(datetime.fromisoformat(manifest["source_generated_at"].replace("Z", "+00:00")).timestamp() * 1000),
        "published_at": stamp(published_at), "usgs_event_id": event_id,
        "usgs_updated_ms": updated if type(updated) is int else None,
        "feature": feature, "feature_sha256": digest,
    }
    return (event_id or "invalid-" + digest).encode("utf-8"), json_bytes(envelope)


def publish_capture(volume: Volume, capture_id: str, cfg: SourceConfig, producer: Any,
                    clock=lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    """Record success only when every feature receives a broker delivery callback."""
    manifest, feed = verified_capture(volume, capture_id, cfg)
    receipt_dir = f"receipts/publish/{capture_id}"
    for filename, is_dir in volume.list_entries(receipt_dir):
        if is_dir or not filename.endswith(".json"):
            continue
        raw_receipt = volume.read_optional(receipt_dir + "/" + filename)
        try:
            prior = json.loads(raw_receipt) if raw_receipt is not None else {}
        except ValueError as exc:
            raise VolumeError("Invalid publish receipt") from exc
        if prior.get("capture_id") == capture_id and prior.get("acknowledged_count") == manifest["expected_event_count"]:
            return prior

    acknowledgements: list[dict[str, int]] = []
    failures: list[str] = []

    def on_delivery(error: Any, message: Any) -> None:
        """Callbacks run during poll/flush, not at produce enqueue time."""
        if error is not None:
            failures.append("Kafka delivery failed")
        else:
            acknowledgements.append({"partition": message.partition(), "offset": message.offset()})

    for feature in feed["features"]:
        if not isinstance(feature, dict):
            raise ContractError("GeoJSON feature must be an object")
        key, value = make_envelope(feature, manifest, clock())
        producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
        producer.poll(0)
    outstanding = producer.flush(30)
    if outstanding or failures or len(acknowledgements) != manifest["expected_event_count"]:
        raise ContractError("Kafka publish has unacknowledged records; retry may duplicate deliveries")
    receipt = {"receipt_version": 1, "capture_id": capture_id, "topic": TOPIC,
               "attempt_id": uuid.uuid4().hex, "expected_count": manifest["expected_event_count"],
               "acknowledged_count": len(acknowledgements), "acknowledged": acknowledgements,
               "completed_at": stamp(clock())}
    volume.put_immutable(f"{receipt_dir}/{receipt['attempt_id']}.json", json_bytes(receipt))
    return receipt

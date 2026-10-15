"""Publish only verified frozen captures, using immutable per-chunk ACK receipts.

Application retries can resend an unreceipted chunk. Stable source item/version
IDs preserve its meaning while Kafka offsets identify each physical delivery.
"""

from typing import Any, Callable

from .capture import Collector
from .contracts import (CONTRACT_VERSION, decode_json, digest, json_bytes,
                        source_version_id, stable_id, validate_feed)
from .errors import BudgetExceeded, ContentConflict, ContractError
from .kafka_config import KafkaSettings
from .volume import Volume


class InjectedFailure(ContractError):
    """Stop at an explicit development fault point for a recoverable cloud drill."""

    code = "INJECTED_FAILURE"


def fault(selected: str, point: str) -> None:
    """Inject a failure only when the caller requested this exact boundary."""
    if selected == point:
        raise InjectedFailure("Intentional development failure: " + point)


def stream_id(settings: KafkaSettings) -> str:
    """Namespace offset identities by environment, topic and operator generation."""
    return stable_id("stream-v1", "dev", settings.topic, settings.generation)


def bind_stream(volume: Volume, kafka: Any, settings: KafkaSettings,
                persist: bool = True) -> tuple[str, dict[str, Any]]:
    """Seal the topic/group topology and reject unsupported retention settings."""
    partitions = kafka.partitions()
    config = kafka.topic_config()
    try:
        retention = int(config["retention.ms"])
    except (KeyError, TypeError, ValueError):
        raise ContractError("Cannot verify effective Kafka retention.ms") from None
    if not 0 < retention <= 259200000 or config.get("cleanup.policy") != "delete":
        raise ContractError("Kafka requires delete cleanup and positive retention no longer than 72 hours")
    identity = stream_id(settings)
    definition = {"manifest_version": 1, "stream_id": identity, "environment": "dev",
                  "topic": settings.topic, "stream_generation": settings.generation,
                  "consumer_group": settings.group, "partitions": partitions,
                  "bootstrap_sha256": digest(settings.bootstrap.encode())}
    if persist:
        volume.put_immutable(f"handoff/{identity}/definition.json", json_bytes(definition))
    return identity, {**definition, "topic_config": config}


def load_capture(collector: Collector, source: Any, mode: str, start: Any, end: Any) -> dict[str, Any]:
    """Read an already-selected capture; dispatch never silently fetches a live feed."""
    collection_id = collector.identity(source, mode, start, end)
    # Reuse the same full source-byte verification as Sprint 2 retry recovery.
    manifest = collector._selected(source, collection_id)
    if manifest is None:
        raise ContractError("No frozen selected capture for this interval; run source capture first")
    return manifest


def envelope(settings: KafkaSettings, manifest: dict[str, Any], index: int,
             feature: Any) -> tuple[bytes, bytes]:
    """Keep every source item, including invalid features for later Bronze quarantine."""
    item_id = stable_id("source-item-v1", manifest["capture_id"], index)
    event_id = feature.get("id") if isinstance(feature, dict) else None
    event_id = event_id if isinstance(event_id, str) and event_id else None
    try:
        version_id = source_version_id(feature) if isinstance(feature, dict) else None
    except ContractError:
        version_id = None
    value = json_bytes({"envelope_version": 1, "contract_version": CONTRACT_VERSION,
        "stream_generation": settings.generation, "environment": "dev", "topic": settings.topic,
        "capture_id": manifest["capture_id"], "collection_id": manifest["collection_id"],
        "source_id": manifest["source_id"], "source_revision": manifest["source_revision"],
        "source_item_id": item_id, "source_item_index": index, "source_event_id": event_id,
        "source_version_id": version_id, "captured_at": manifest["fetched_at"],
        "raw_feature_sha256": digest(json_bytes(feature)), "feature": feature})
    key = (event_id or item_id).encode("utf-8")
    if len(value) + len(key) > settings.max_message_bytes:
        raise BudgetExceeded("Source item exceeds the 256 KiB Kafka envelope limit; frozen capture remains pending")
    return key, value


def dispatch(volume: Volume, kafka: Any, settings: KafkaSettings, manifest: dict[str, Any],
             emit: Callable[[dict[str, Any]], None], fail_at: str = "none") -> dict[str, Any]:
    """Publish source chunks and seal completeness only after every chunk is receipted."""
    identity = stream_id(settings)
    payload = volume.read_optional(manifest["response_relative_path"])
    if payload is None or digest(payload) != manifest["response_sha256"]:
        raise ContractError("Frozen source response is missing or corrupt")
    features = validate_feed(payload)["features"]
    if len(features) != manifest["expected_item_count"] or len(features) > settings.max_source_items:
        raise BudgetExceeded("Source item count differs from manifest or exceeds 10000 per capture")
    prefix = f"handoff/{identity}/dispatch/{manifest['capture_id']}/{settings.dispatch_generation}"
    chunks = []
    for first in range(0, len(features), settings.chunk_size):
        messages = [envelope(settings, manifest, index, features[index])
                    for index in range(first, min(first + settings.chunk_size, len(features)))]
        chunk_index = first // settings.chunk_size
        receipt_path = f"{prefix}/chunk-{chunk_index:06d}.json"
        expected = {"manifest_version": 1, "stream_id": identity, "topic": settings.topic,
                    "stream_generation": settings.generation, "capture_id": manifest["capture_id"],
                    "dispatch_generation": settings.dispatch_generation, "chunk_index": chunk_index,
                    "first_item_index": first, "item_count": len(messages),
                    "message_sha256": [digest(value) for key, value in messages]}
        existing = volume.read_optional(receipt_path)
        if existing is None:
            deliveries = kafka.produce(messages)
            fault(fail_at, "after-produce")
            if len(deliveries) != len(messages):
                raise ContractError("Kafka acknowledgement count differs from source chunk")
            receipt = {**expected, "deliveries": deliveries, "state": "PUBLISH_ACKED"}
            volume.put_immutable(receipt_path, json_bytes(receipt))
            existing = volume.read_optional(receipt_path)
        receipt = decode_json(existing)
        if not isinstance(receipt, dict) or any(receipt.get(key) != value for key, value in expected.items()):
            raise ContentConflict("Dispatch receipt differs from frozen source chunk")
        deliveries = receipt.get("deliveries")
        if receipt.get("state") != "PUBLISH_ACKED" or not isinstance(deliveries, list) or len(deliveries) != len(messages):
            raise ContractError("Dispatch receipt lacks acknowledged offsets")
        if any(not isinstance(item, dict) or type(item.get("offset")) is not int or item["offset"] < 0
               or type(item.get("partition")) is not int or item["partition"] not in (0, 1) for item in deliveries):
            raise ContractError("Dispatch receipt contains invalid broker coordinates")
        chunks.append({"path": receipt_path, "sha256": digest(existing), "item_count": len(messages)})
    complete = {"manifest_version": 1, "stream_id": identity, "topic": settings.topic,
                "stream_generation": settings.generation, "capture_id": manifest["capture_id"],
                "dispatch_generation": settings.dispatch_generation, "expected_item_count": len(features),
                "chunk_count": len(chunks), "chunks": chunks, "state": "PUBLISH_ACKED"}
    volume.put_immutable(prefix + "/manifest.json", json_bytes(complete))
    emit({"event": "DISPATCH_VERIFIED", "capture_id": manifest["capture_id"],
          "dispatch_generation": settings.dispatch_generation, "item_count": len(features), "chunks": len(chunks)})
    return complete

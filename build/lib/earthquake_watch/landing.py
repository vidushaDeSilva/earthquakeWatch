"""Land bounded Kafka polls with manifest-last durability and safe offset recovery."""

from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import binascii
import hashlib
import json
import time
import uuid
from typing import Any

from .contracts import ContractError, json_bytes, stamp
from .kafka_transport import GROUP, TOPIC, ReadonlyVolume, Volume


MAX_RECORDS = 2000
MAX_BYTES = 32 * 1024 * 1024
MAX_SECONDS = 120


@dataclass(frozen=True)
class Delivery:
    """One physical Kafka record with its authoritative broker position."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    landed_at: datetime


def partition_ranges(records: list[Delivery]) -> list[dict[str, int]]:
    """Require a unique, contiguous consumed prefix for each partition."""
    grouped: dict[int, list[int]] = {}
    for record in records:
        if record.topic != TOPIC or record.partition not in (0, 1) or record.offset < 0:
            raise ContractError("Unexpected Kafka topic, partition or offset")
        grouped.setdefault(record.partition, []).append(record.offset)
    result = []
    for partition, offsets in sorted(grouped.items()):
        ordered = sorted(offsets)
        if ordered != list(range(ordered[0], ordered[-1] + 1)):
            raise ContractError("Landing offsets contain a gap or duplicate")
        result.append({"partition_id": partition, "first_offset": ordered[0],
                       "next_offset": ordered[-1] + 1, "record_count": len(ordered)})
    return result


def _line(record: Delivery) -> tuple[bytes, str]:
    """Preserve bad messages for Bronze quarantine without poisoning Kafka progress."""
    try:
        payload = record.value.decode("utf-8")
    except UnicodeDecodeError:
        payload = None
    try:
        envelope = json.loads(payload) if payload is not None else {}
        capture_id = envelope.get("capture_id", "") if isinstance(envelope, dict) else ""
    except ValueError:
        capture_id = ""
    key = record.key.decode("utf-8", errors="replace") if record.key else None
    line = json_bytes({"topic": record.topic, "partition_id": record.partition,
                       "offset": record.offset, "key": key, "raw_payload": payload,
                       "value_b64": base64.b64encode(record.value).decode("ascii"),
                       "landed_at": stamp(record.landed_at)})
    return line, capture_id if isinstance(capture_id, str) and len(capture_id) == 64 else ""


def verify_landing(volume: ReadonlyVolume, batch_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Check manifest identity, exact file bytes, rows and partition ranges."""
    root = f"landing/{batch_id}"
    raw_manifest = volume.read_optional(root + "/manifest.json")
    if raw_manifest is None:
        raise ContractError("Landing batch has no verified manifest")
    try:
        manifest = json.loads(raw_manifest)
        data = volume.read_optional(root + "/records.jsonl")
        if data is None or manifest.get("landing_batch_id") != batch_id or manifest.get("manifest_version") != 1:
            raise ContractError("Landing batch identity mismatch")
        if manifest.get("state") not in ("VERIFIED", "DISPATCH_READY") or manifest.get("topic") != TOPIC:
            raise ContractError("Landing batch is not ready")
        if manifest.get("consumer_group") != GROUP or manifest.get("data_relative_path") != root + "/records.jsonl":
            raise ContractError("Landing path or group mismatch")
        if len(data) != manifest["data_bytes"] or hashlib.sha256(data).hexdigest() != manifest["data_sha256"]:
            raise ContractError("Landing checksum or size mismatch")
        if not 0 < len(data) <= MAX_BYTES or not 0 < manifest["record_count"] <= MAX_RECORDS:
            raise ContractError("Landing bounds mismatch")
        rows = [json.loads(line) for line in data.splitlines()]
        if len(rows) != manifest["record_count"] or not rows:
            raise ContractError("Landing row count mismatch")
        deliveries = [Delivery(row["topic"], row["partition_id"], row["offset"],
                               row["key"].encode() if row.get("key") is not None else None,
                               base64.b64decode(row["value_b64"], validate=True),
                               datetime.fromisoformat(row["landed_at"].replace("Z", "+00:00"))) for row in rows]
        if any(row.get("raw_payload") != (record.value.decode("utf-8", errors="strict")
                if _is_utf8(record.value) else None) for row, record in zip(rows, deliveries)):
            raise ContractError("Landing raw payload differs from physical delivery")
        if partition_ranges(deliveries) != manifest["partition_ranges"]:
            raise ContractError("Landing partition range mismatch")
        identity = json_bytes({"topic": TOPIC, "consumer_group": GROUP,
                               "partition_ranges": manifest["partition_ranges"],
                               "data_sha256": manifest["data_sha256"]})
        if hashlib.sha256(identity).hexdigest() != batch_id:
            raise ContractError("Landing deterministic identity mismatch")
        captures = sorted({capture for record in deliveries if (capture := _line(record)[1])})
        if captures != manifest["capture_ids"]:
            raise ContractError("Landing capture lineage mismatch")
        return manifest, rows
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, binascii.Error) as exc:
        raise ContractError("Invalid landing manifest or JSONL file") from exc


def _is_utf8(value: bytes) -> bool:
    """Distinguish malformed wire bytes from an actual UTF-8 JSON string."""
    try:
        value.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


def freeze_landing(records: list[Delivery], volume: Volume,
                   clock=lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    """Write the JSONL data first; a verified manifest is the commit boundary."""
    if not records or len(records) > MAX_RECORDS:
        raise ContractError("Landing record count outside bounded range")
    ranges = partition_ranges(records)
    lines_and_ids = [_line(record) for record in records]
    data = b"".join(item[0] for item in lines_and_ids)
    if len(data) > MAX_BYTES:
        raise ContractError("Landing data exceeds the byte limit")
    digest = hashlib.sha256(data).hexdigest()
    identity = json_bytes({"topic": TOPIC, "consumer_group": GROUP,
                           "partition_ranges": ranges, "data_sha256": digest})
    batch_id = hashlib.sha256(identity).hexdigest()
    root = f"landing/{batch_id}"
    if volume.read_optional(root + "/manifest.json") is not None:
        verified, _ = verify_landing(volume, batch_id)
        if verified["data_sha256"] != digest:
            raise ContractError("Existing batch identity conflicts")
        return verified
    volume.put_immutable(root + "/records.jsonl", data)
    now = stamp(clock())
    manifest = {"manifest_version": 1, "landing_batch_id": batch_id,
                "consumer_group": GROUP, "topic": TOPIC, "partition_ranges": ranges,
                "capture_ids": sorted({capture_id for _, capture_id in lines_and_ids if capture_id}),
                "record_count": len(records), "data_sha256": digest, "data_bytes": len(data),
                "data_relative_path": root + "/records.jsonl", "created_at": now,
                "verified_at": now, "state": "VERIFIED"}
    volume.put_immutable(root + "/manifest.json", json_bytes(manifest))
    return manifest


def _commit(consumer: Any, partition: int, next_offset: int, volume: Volume,
            batch_id: str, topic_partition: Any) -> None:
    """Commit only a verified prefix and keep a receipt after broker acknowledgement."""
    result = consumer.commit(offsets=[topic_partition(TOPIC, partition, next_offset)], asynchronous=False)
    if len(result) != 1 or result[0].offset != next_offset or result[0].error is not None:
        raise ContractError("Kafka offset commit was not acknowledged")
    attempt = uuid.uuid4().hex
    volume.put_immutable(f"receipts/offset/{batch_id}/{attempt}.json",
                         json_bytes({"batch_id": batch_id, "topic": TOPIC, "partition_id": partition,
                                     "next_offset": next_offset, "committed_at": stamp(datetime.now(timezone.utc))}))


def land_once(volume: Volume, consumer: Any, topic_partition: Any,
              clock=time.monotonic) -> dict[str, Any] | None:
    """Recover old verified ranges, then land one bounded Kafka poll and commit."""
    partitions = [topic_partition(TOPIC, number) for number in (0, 1)]
    consumer.assign(partitions)
    committed = consumer.committed(partitions, timeout=10)
    safe: dict[int, int] = {}
    for part in committed:
        if part.error is not None:
            raise ContractError("Could not read committed Kafka offsets")
        if part.offset < 0:
            low, _ = consumer.get_watermark_offsets(topic_partition(TOPIC, part.partition), timeout=10)
            if low != 0:
                raise ContractError("New group has a retention gap; manual recovery required")
            safe[part.partition] = 0
        else:
            safe[part.partition] = part.offset

    # A crash after manifest but before commit is repaired before consuming more.
    recovered: dict[int, list[tuple[int, int, str]]] = {0: [], 1: []}
    for batch_id, is_dir in volume.list_entries("landing"):
        if not is_dir:
            continue
        manifest, _ = verify_landing(volume, batch_id)
        for part in manifest["partition_ranges"]:
            number = part["partition_id"]
            recovered[number].append((part["first_offset"], part["next_offset"], batch_id))
    for number in (0, 1):
        for first, next_offset, batch_id in sorted(recovered[number]):
            if first <= safe[number] < next_offset:
                _commit(consumer, number, next_offset, volume, batch_id, topic_partition)
                safe[number] = next_offset
    for number in (0, 1):
        low, high = consumer.get_watermark_offsets(topic_partition(TOPIC, number), timeout=10)
        if not low <= safe[number] <= high:
            raise ContractError("Committed offset is outside retained Kafka range")
        consumer.seek(topic_partition(TOPIC, number, safe[number]))

    start = clock()
    records: list[Delivery] = []
    size = 0
    while len(records) < MAX_RECORDS and clock() - start < MAX_SECONDS:
        message = consumer.poll(1.0)
        if message is None:
            break
        if message.error():
            raise ContractError("Kafka poll failed")
        value = message.value() or b""
        delivery = Delivery(message.topic(), message.partition(), message.offset(),
                            message.key(), value, datetime.now(timezone.utc))
        # The bounded file includes JSON escaping, base64, and row metadata.
        line_size = len(_line(delivery)[0])
        if records and size + line_size > MAX_BYTES:
            consumer.seek(topic_partition(TOPIC, message.partition(), message.offset()))
            break
        if size + line_size > MAX_BYTES:
            raise ContractError("Single Kafka record exceeds landing byte limit")
        records.append(delivery)
        size += line_size
    if not records:
        return None
    manifest = freeze_landing(records, volume)
    for part in manifest["partition_ranges"]:
        number = part["partition_id"]
        if part["first_offset"] != safe[number]:
            raise ContractError("Unsafe Kafka offset gap before landing commit")
        _commit(consumer, number, part["next_offset"], volume, manifest["landing_batch_id"], topic_partition)
    return manifest

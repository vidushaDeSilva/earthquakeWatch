"""Land raw Kafka deliveries before offset commits and recover sealed batches.

Data rows contain lossless base64 key/value/headers, including malformed values.
An exclusive end-offset snapshot bounds each invocation. Offset gaps fail closed.
"""

import base64
import time
from datetime import datetime, timezone
from typing import Any, Callable

from .contracts import decode_json, digest, json_bytes, parse_utc, stable_id, utc_text
from .errors import BudgetExceeded, ContractError
from .kafka_config import KafkaSettings
from .outbox import fault, stream_id
from .volume import Volume


def encode_bytes(value: bytes | None) -> str | None:
    """Represent null separately from empty bytes without lossy UTF-8 decoding."""
    return None if value is None else base64.b64encode(value).decode("ascii")


def decode_bytes(value: Any) -> bytes | None:
    """Reject malformed persisted base64 rather than accepting altered raw bytes."""
    if value is None:
        return None
    try:
        if not isinstance(value, str):
            raise ValueError()
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise ContractError("Landing contains invalid base64 bytes") from None


def delivery_row(record: Any, settings: KafkaSettings) -> dict[str, Any]:
    """Capture raw delivery identity independently of the source payload contract."""
    if record.topic != settings.topic or type(record.partition) is not int or record.partition not in (0, 1):
        raise ContractError("Kafka returned a record from an unexpected topic/partition")
    if type(record.offset) is not int or record.offset < 0:
        raise ContractError("Kafka returned an invalid offset")
    headers = [[name, encode_bytes(value)] for name, value in (record.headers or [])]
    return {"delivery_id": stable_id("delivery-v1", settings.generation, settings.topic,
                                    record.partition, record.offset),
            "stream_generation": settings.generation, "topic": settings.topic,
            "partition": record.partition, "offset": record.offset,
            "kafka_timestamp_ms": record.timestamp, "key_b64": encode_bytes(record.key),
            "value_b64": encode_bytes(record.value), "headers_b64": headers,
            "message_sha256": digest(record.value) if record.value is not None else None}


def verify_batch(volume: Volume, settings: KafkaSettings, batch_id: str) -> tuple[dict[str, Any], bytes]:
    """Re-read actual bytes and prove batch identity, counts, offsets and checksums."""
    identity = stream_id(settings)
    prefix = f"handoff/{identity}/batches/{batch_id}"
    stored = volume.read_optional(prefix + "/manifest.json")
    if stored is None:
        raise ContractError("Landing batch has no complete manifest")
    manifest = decode_json(stored)
    if not isinstance(manifest, dict):
        raise ContractError("Landing manifest must be an object")
    required = {"manifest_version": 1, "stream_id": identity, "stream_generation": settings.generation,
                "topic": settings.topic, "batch_id": batch_id, "state": "LANDED",
                "records_relative_path": prefix + "/records.jsonl"}
    if any(manifest.get(key) != value for key, value in required.items()):
        raise ContractError("Landing manifest has an unexpected stream or data path")
    if type(manifest.get("partition")) is not int or manifest["partition"] not in (0, 1):
        raise ContractError("Landing partition is invalid")
    data = volume.read_optional(manifest["records_relative_path"])
    if data is None or len(data) > settings.max_batch_bytes or digest(data) != manifest.get("data_sha256"):
        raise ContractError("Landing data is missing, oversized or corrupt")
    rows = [decode_json(line) for line in data.splitlines()]
    if not rows or len(rows) != manifest.get("row_count") or len(data) != manifest.get("data_bytes"):
        raise ContractError("Landing row/byte counts differ")
    first = manifest.get("offset_first")
    if type(first) is not int or first < 0:
        raise ContractError("Landing offset range is invalid")
    for index, row in enumerate(rows):
        offset = first + index
        if not isinstance(row, dict) or any(row.get(key) != value for key, value in (
            ("topic", settings.topic), ("stream_generation", settings.generation),
            ("partition", manifest["partition"]), ("offset", offset),
            ("delivery_id", stable_id("delivery-v1", settings.generation, settings.topic, manifest["partition"], offset)))):
            raise ContractError("Landing delivery identity or offset sequence differs")
        value = decode_bytes(row.get("value_b64"))
        decode_bytes(row.get("key_b64"))
        if row.get("message_sha256") != (digest(value) if value is not None else None):
            raise ContractError("Landing raw message checksum differs")
        headers = row.get("headers_b64")
        if not isinstance(headers, list):
            raise ContractError("Landing headers are invalid")
        for pair in headers:
            if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[0], str):
                raise ContractError("Landing header entry is invalid")
            decode_bytes(pair[1])
    last = first + len(rows) - 1
    expected_id = stable_id("landing-v1", identity, manifest["partition"], first, last, digest(data))
    if (manifest.get("offset_last") != last or manifest.get("next_offset") != last + 1
            or batch_id != expected_id):
        raise ContractError("Landing deterministic batch ID or exclusive offset differs")
    parse_utc(manifest.get("landed_at", ""))
    return manifest, stored


def seal_batch(volume: Volume, settings: KafkaSettings, rows: list[dict[str, Any]], run_id: str,
               fail_at: str = "none") -> dict[str, Any]:
    """Seal data first, then a verified manifest; preserve the first sealed timestamp."""
    identity = stream_id(settings)
    data = b"".join(json_bytes(row) + b"\n" for row in rows)
    if not rows or len(data) > settings.max_batch_bytes:
        raise BudgetExceeded("Landing batch is empty or exceeds the four MiB bound")
    partition, first, last = rows[0]["partition"], rows[0]["offset"], rows[-1]["offset"]
    if any(row["partition"] != partition or row["offset"] != first + index for index, row in enumerate(rows)):
        raise ContractError("Offset gap: never commit across missing or compacted deliveries")
    batch_id = stable_id("landing-v1", identity, partition, first, last, digest(data))
    prefix = f"handoff/{identity}/batches/{batch_id}"
    existing = volume.read_optional(prefix + "/manifest.json")
    if existing is not None:
        return verify_batch(volume, settings, batch_id)[0]
    volume.put_immutable(prefix + "/records.jsonl", data)
    fault(fail_at, "after-data")
    manifest = {"manifest_version": 1, "batch_id": batch_id, "stream_id": identity,
        "stream_generation": settings.generation, "topic": settings.topic, "partition": partition,
        "offset_first": first, "offset_last": last, "next_offset": last + 1,
        "row_count": len(rows), "data_bytes": len(data), "data_sha256": digest(data),
        "records_relative_path": prefix + "/records.jsonl", "first_run_id": run_id,
        "landed_at": utc_text(datetime.now(timezone.utc)), "state": "LANDED"}
    volume.put_immutable(prefix + "/manifest.json", json_bytes(manifest))
    verified, _ = verify_batch(volume, settings, batch_id)
    fault(fail_at, "after-manifest")
    return verified


def commit_verified(volume: Volume, kafka: Any, settings: KafkaSettings,
                    manifest: dict[str, Any], fail_at: str = "none") -> None:
    """Commit only a contiguous verified range; read back after any lost commit ACK."""
    manifest, _ = verify_batch(volume, settings, manifest["batch_id"])
    partition, target = manifest["partition"], manifest["next_offset"]
    beginning, end, committed = kafka.bounds(partition)
    position = beginning if committed is None else committed
    if committed is None and beginning != 0:
        raise ContractError("OFFSET_GAP: new consumer cannot prove expired offsets; controlled recovery required")
    if position < beginning or position > end:
        raise ContractError("OFFSET_GAP: committed offset is outside retained topic bounds")
    if position >= target:
        return
    if not manifest["offset_first"] <= position < target or target > end:
        raise ContractError("Cannot commit a manifest beyond an unlanded offset range")
    try:
        kafka.commit(partition, target, manifest["batch_id"])
    except Exception:
        # A failed response does not prove the broker rejected the commit.
        if kafka.bounds(partition)[2] != target:
            raise
    if kafka.bounds(partition)[2] != target:
        raise ContractError("Kafka committed offset did not match the verified landing")
    fault(fail_at, "after-commit")


def recover(volume: Volume, kafka: Any, settings: KafkaSettings,
            emit: Callable[[dict[str, Any]], None], fail_at: str = "none") -> list[dict[str, Any]]:
    """Inventory all sealed batches independently of current Kafka offsets."""
    identity = stream_id(settings)
    batch_ids = volume.child_directories(f"handoff/{identity}/batches")
    if len(batch_ids) > settings.max_batches:
        raise BudgetExceeded("Recovery inventory exceeds 128 batches; bounded paging/retention gate required")
    complete, incomplete = [], 0
    for batch_id in batch_ids:
        if volume.read_optional(f"handoff/{identity}/batches/{batch_id}/manifest.json") is None:
            incomplete += 1
            continue
        manifest, stored = verify_batch(volume, settings, batch_id)
        complete.append(manifest)
        emit({"event": "LANDING_RECOVERY_CANDIDATE", "batch_id": batch_id,
              "partition": manifest["partition"], "rows": manifest["row_count"],
              "manifest_sha256": digest(stored), "data_stage": "LANDED"})
    for manifest in sorted(complete, key=lambda item: (item["partition"], item["offset_first"], item["next_offset"])):
        beginning, end, committed = kafka.bounds(manifest["partition"])
        position = beginning if committed is None else committed
        if manifest["offset_first"] <= position < manifest["next_offset"]:
            commit_verified(volume, kafka, settings, manifest, fail_at)
    emit({"event": "RECOVERY_SCAN_COMPLETE", "sealed_batches": len(complete),
          "incomplete_batches": incomplete, "bronze_acknowledged": False})
    for partition in kafka.partitions():
        beginning, end, committed = kafka.bounds(partition)
        if ((committed is None and beginning != 0) or
                (committed is not None and not beginning <= committed <= end)):
            raise ContractError("OFFSET_GAP: recovery inventory is safe, but Kafka requires controlled gap resolution")
    return complete


def land(volume: Volume, kafka: Any, settings: KafkaSettings, run_id: str,
         emit: Callable[[dict[str, Any]], None], fail_at: str = "none",
         clock: Callable[[], float] = time.monotonic) -> int:
    """Recover manifests, then drain only the initial retained end-offset snapshot."""
    recover(volume, kafka, settings, emit, fail_at)
    total, deadline = 0, clock() + 600
    snapshot = {partition: kafka.bounds(partition) for partition in kafka.partitions()}
    for partition, (beginning, target, committed) in snapshot.items():
        position = beginning if committed is None else committed
        if position < beginning or position > target or (committed is None and beginning != 0):
            raise ContractError("OFFSET_GAP: no automatic reset; retain captures and investigate before replay")
        kafka.seek(partition, position)
        idle_polls = 0
        while position < target:
            if clock() >= deadline or total >= settings.max_land_records:
                raise BudgetExceeded("Landing drain budget reached; completed batches are safe to resume")
            polled = kafka.poll()
            if polled and polled[0].offset > position:
                raise ContractError("OFFSET_GAP: consumer skipped the next unlanded offset")
            records = [record for record in polled if record.offset < target]
            if not records:
                idle_polls += 1
                if kafka.bounds(partition)[0] > position:
                    raise ContractError("OFFSET_GAP: retention passed the next unlanded offset")
                if idle_polls >= 10:
                    raise BudgetExceeded("Kafka returned no records for the known backlog; retry this bounded drain")
                continue
            idle_polls = 0
            rows, size = [], 0
            for record in records:
                row = delivery_row(record, settings)
                if row["offset"] != position + len(rows):
                    raise ContractError("OFFSET_GAP: consumer did not return a contiguous range")
                row_size = len(json_bytes(row)) + 1
                if row_size > settings.max_batch_bytes:
                    raise BudgetExceeded("A raw Kafka delivery exceeds the landing batch byte limit")
                if rows and (size + row_size > settings.max_batch_bytes or total + len(rows) >= settings.max_land_records):
                    break
                rows.append(row)
                size += row_size
            manifest = seal_batch(volume, settings, rows, run_id, fail_at)
            commit_verified(volume, kafka, settings, manifest, fail_at)
            total += len(rows)
            position = manifest["next_offset"]
            emit({"event": "LANDING_COMMITTED", "batch_id": manifest["batch_id"],
                  "partition": partition, "rows": len(rows), "next_offset": position})
            # Re-seek if a polled group was split on a byte/run budget boundary.
            kafka.seek(partition, position)
    emit({"event": "LANDING_DRAIN_COMPLETE", "rows_landed_this_run": total})
    return total

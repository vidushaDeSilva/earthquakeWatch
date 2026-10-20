"""Materialize verified landing evidence and merge physical and logical event versions.

Run inside Databricks with the earthquake_watch package installed. This module
stops after Silver; later phases own dispatch recovery and Gold publication.
"""

from datetime import datetime, timezone
import argparse
import base64
import hashlib
import json
from typing import Any

from earthquake_watch.contracts import ContractError, SourceConfig, stamp
from earthquake_watch.kafka_transport import ENVELOPE_VERSION, TOPIC, verified_capture
from earthquake_watch.landing import verify_landing
from earthquake_watch.silver_rules import normalize
from earthquake_watch.volume_client import MountedVolume


CATALOG = "earthquake_dev"
ROOT = "/Volumes/earthquake_dev/ops/pipeline_files"
SOURCE = SourceConfig("usgs-all-day", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
                      16777216, 30, 300)


def _utc(value: str) -> datetime:
    """Decode manifest UTC timestamps into aware Python datetimes."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _merge_rows(spark: Any, table: str, rows: list[dict[str, Any]], keys: list[str],
                update: dict[str, str] | None = None) -> None:
    """Deduplicate the small bounded batch before Delta MERGE."""
    if not rows:
        return
    from delta.tables import DeltaTable

    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row[column] for column in keys)
        if key in unique and unique[key] != row:
            raise ContractError(f"Conflicting source rows for {table}")
        unique[key] = row
    target = DeltaTable.forName(spark, table)
    schema = target.toDF().schema
    source = spark.createDataFrame(list(unique.values()), schema=schema)
    condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    merger = target.alias("t").merge(source.alias("s"), condition)
    if update:
        merger = merger.whenMatchedUpdate(set=update)
    merger.whenNotMatchedInsertAll().execute()


def _version(spark: Any, table: str) -> int:
    """Capture the Delta commit version used for stage reconciliation."""
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def _stage(spark: Any, pipeline_run_id: str, name: str, fingerprint: str,
           status: str, records_in: int, records_out: int | None = None,
           rejected: int | None = None, output_table: str | None = None,
           output_version: int | None = None) -> None:
    """Keep one deterministic stage-attempt row across a retry after commit."""
    table = f"{CATALOG}.ops.stage_runs"
    # The fingerprint includes the batch manifest, so multiple batches in one run do not collide.
    stage_id = hashlib.sha256(f"{pipeline_run_id}|{fingerprint}|{name}|1".encode()).hexdigest()
    now = datetime.now(timezone.utc)
    row = dict(stage_run_id=stage_id, pipeline_run_id=pipeline_run_id,
               stage_name=name, attempt_number=1, stage_status=status,
               input_fingerprint=fingerprint, records_in=records_in,
               records_out=records_out, records_quarantined=rejected,
               output_table=output_table, output_delta_version=output_version,
               retryable=None, error_code=None, started_at=now, finished_at=now if status == "COMMITTED" else None)
    updates = {key: f"s.{key}" for key in ("stage_status", "records_out", "records_quarantined",
                                                "output_table", "output_delta_version", "finished_at")}
    _merge_rows(spark, table, [row], ["stage_run_id"], update=updates)


def _quality(spark: Any, pipeline_run_id: str, fingerprint: str,
             stage: str, rule: str, observed: int, expected: int) -> None:
    """Persist a passing, count-based blocking check before stage commit."""
    if observed != expected:
        raise ContractError(f"{stage} {rule} reconciliation failed")
    identity = f"{pipeline_run_id}|{fingerprint}|{stage}|{rule}"
    row = dict(check_id=hashlib.sha256(identity.encode()).hexdigest(),
               pipeline_run_id=pipeline_run_id, stage_name=stage, rule_id=rule,
               severity="BLOCKER", check_status="PASS", observed_value=str(observed),
               expected_value=str(expected), details_json=None,
               checked_at=datetime.now(timezone.utc))
    _merge_rows(spark, f"{CATALOG}.ops.quality_checks", [row], ["check_id"])


def _materialize_evidence(spark: Any, volume: MountedVolume,
                          manifest: dict[str, Any], batch_id: str) -> None:
    """Databricks alone writes Ops metadata after verifying immutable files."""
    now = datetime.now(timezone.utc)
    for capture_id in manifest["capture_ids"]:
        captured, _ = verified_capture(volume, capture_id, SOURCE)
        row = dict(capture_id=capture_id, source_id=captured["source_id"],
                   schedule_slot_start=_utc(captured["schedule_slot_start"]),
                   fetched_at=_utc(captured["fetched_at"]), source_generated_at=_utc(captured["source_generated_at"]),
                   response_sha256=captured["response_sha256"], response_bytes=captured["response_bytes"],
                   expected_event_count=captured["expected_event_count"],
                   data_uri=ROOT + "/" + captured["response_relative_path"],
                   manifest_uri=ROOT + f"/captures/{capture_id}/manifest.json",
                   capture_status="PUBLISHED" if volume.list_entries(f"receipts/publish/{capture_id}") else "FROZEN",
                   first_materialized_at=now, last_seen_at=now)
        _merge_rows(spark, f"{CATALOG}.ops.captures", [row], ["capture_id"],
                    update={"last_seen_at": "s.last_seen_at"})
    raw_manifest = volume.read_optional(f"landing/{batch_id}/manifest.json")
    assert raw_manifest is not None
    landing = dict(landing_batch_id=batch_id, consumer_group=manifest["consumer_group"],
                   topic=manifest["topic"], capture_ids=manifest["capture_ids"],
                   partition_ranges=manifest["partition_ranges"],
                   data_uri=ROOT + "/" + manifest["data_relative_path"],
                   manifest_uri=ROOT + f"/landing/{batch_id}/manifest.json",
                   manifest_sha256=hashlib.sha256(raw_manifest).hexdigest(),
                   expected_record_count=manifest["record_count"], landed_record_count=manifest["record_count"],
                   landing_status="VERIFIED", created_at=_utc(manifest["created_at"]),
                   verified_at=_utc(manifest["verified_at"]))
    _merge_rows(spark, f"{CATALOG}.ops.landing_batches", [landing], ["landing_batch_id"])


def _bronze_rows(rows: list[dict[str, Any]], batch_id: str,
                 revision: str) -> list[dict[str, Any]]:
    """Represent every physical Kafka delivery, including malformed envelopes."""
    result = []
    now = datetime.now(timezone.utc)
    for row in rows:
        raw = row.get("raw_payload")
        try:
            envelope = json.loads(raw) if raw is not None else {}
        except ValueError:
            envelope = {}
        if not isinstance(envelope, dict):
            envelope = {}
        payload = raw if raw is not None else "base64:" + row["value_b64"]
        feature_hash = envelope.get("feature_sha256")
        if not isinstance(feature_hash, str) or len(feature_hash) != 64:
            feature_hash = hashlib.sha256(base64.b64decode(row["value_b64"])).hexdigest()
        capture_id = envelope.get("capture_id")
        event_id = envelope.get("usgs_event_id")
        updated = envelope.get("usgs_updated_ms")
        schema_version = envelope.get("schema_version")
        source_generated = envelope.get("source_generated_ms")
        published = envelope.get("published_at")
        try:
            source_time = datetime.fromtimestamp(source_generated / 1000, timezone.utc) if type(source_generated) is int else None
        except (OverflowError, OSError, ValueError):
            source_time = None
        try:
            published_time = _utc(published) if isinstance(published, str) else None
        except ValueError:
            published_time = None
        result.append(dict(topic=row["topic"], partition_id=row["partition_id"], offset=row["offset"],
                           landing_batch_id=batch_id, capture_id=capture_id if isinstance(capture_id, str) else "",
                           schema_version=schema_version if isinstance(schema_version, str) else "INVALID",
                           usgs_event_id=event_id if isinstance(event_id, str) else None,
                           usgs_updated_ms=updated if type(updated) is int else None,
                           feature_sha256=feature_hash, raw_payload=payload,
                           source_generated_at=source_time, published_at=published_time,
                           landed_at=_utc(row["landed_at"]), ingested_at=now, processing_revision=revision))
    return result


def _bronze(spark: Any, rows: list[dict[str, Any]], batch_id: str,
            pipeline_run_id: str, fingerprint: str, revision: str) -> int:
    """Reject conflicting physical keys before an insert-only Delta MERGE."""
    from pyspark.sql import functions as F

    table = f"{CATALOG}.bronze.event_deliveries"
    incoming = _bronze_rows(rows, batch_id, revision)
    _stage(spark, pipeline_run_id, "BRONZE", fingerprint, "RUNNING", len(incoming))
    source = spark.createDataFrame(incoming, schema=spark.table(table).schema).alias("s")
    existing = spark.table(table).alias("t")
    key = ((F.col("s.topic") == F.col("t.topic")) &
           (F.col("s.partition_id") == F.col("t.partition_id")) &
           (F.col("s.offset") == F.col("t.offset")))
    conflict = source.join(existing, key, "inner").where(
        (F.col("s.raw_payload") != F.col("t.raw_payload")) |
        (F.col("s.feature_sha256") != F.col("t.feature_sha256"))).limit(1).count()
    if conflict:
        raise ContractError("Bronze physical offset conflicts with existing payload")
    _merge_rows(spark, table, incoming, ["topic", "partition_id", "offset"])
    # The target may already contain rows when the prior MERGE succeeded but Ops did not.
    matched = source.join(spark.table(table).alias("t"), key, "inner").count()
    _quality(spark, pipeline_run_id, fingerprint, "BRONZE", "physical_row_count",
             matched, len(incoming))
    version = _version(spark, table)
    _stage(spark, pipeline_run_id, "BRONZE", fingerprint, "COMMITTED", len(incoming),
           len(incoming), 0, table, version)
    return version


def _silver(spark: Any, rows: list[dict[str, Any]], batch_id: str,
            pipeline_run_id: str, fingerprint: str, revision: str,
            capture_ids: list[str]) -> int:
    """Quarantine invalid records and merge one row per logical USGS version."""
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    table = f"{CATALOG}.silver.event_versions"
    now = datetime.now(timezone.utc)
    valid: dict[str, dict[str, Any]] = {}
    rejected: list[dict[str, Any]] = []
    _stage(spark, pipeline_run_id, "SILVER", fingerprint, "RUNNING", len(rows))
    for row in rows:
        try:
            event = normalize(row, now, revision)
            if event["first_capture_id"] not in capture_ids:
                raise ContractError("Event references a capture outside this landing batch")
            valid.setdefault(event["event_version_id"], event)
        except (ContractError, ValueError, TypeError, OverflowError) as exc:
            raw = base64.b64decode(row["value_b64"])
            reason = str(exc)[:160]
            key = f"{batch_id}|{row['partition_id']}|{row['offset']}|{reason}"
            rejected.append(dict(quarantine_id=hashlib.sha256(key.encode()).hexdigest(),
                                 landing_batch_id=batch_id, capture_id=None, topic=row["topic"],
                                 partition_id=row["partition_id"], offset=row["offset"], usgs_event_id=None,
                                 payload_sha256=hashlib.sha256(raw).hexdigest(),
                                 payload_uri=ROOT + f"/landing/{batch_id}/records.jsonl",
                                 reason_code="INVALID_EVENT", reason_details=reason,
                                 resolution_status="OPEN", first_seen_at=now, resolved_at=None))
    _merge_rows(spark, f"{CATALOG}.ops.quarantine_records", rejected, ["quarantine_id"])
    events = list(valid.values())
    if events:
        pairs = {(event["usgs_event_id"], event["usgs_updated_ms"]) for event in events}
        pair_rows = spark.createDataFrame(sorted(pairs), "usgs_event_id string, usgs_updated_ms long")
        existing = spark.table(table).join(F.broadcast(pair_rows), ["usgs_event_id", "usgs_updated_ms"], "inner") \
            .select("usgs_event_id", "usgs_updated_ms", "payload_sha256").collect()
        known: dict[tuple[str, int], set[str]] = {}
        for row in existing:
            pair = (row.usgs_event_id, row.usgs_updated_ms)
            known.setdefault(pair, set()).add(row.payload_sha256)
        for event in events:
            known.setdefault((event["usgs_event_id"], event["usgs_updated_ms"]), set()).add(event["payload_sha256"])
        conflicts = {pair for pair, hashes in known.items() if len(hashes) > 1}
        for event in events:
            event["version_conflict"] = (event["usgs_event_id"], event["usgs_updated_ms"]) in conflicts
        _merge_rows(spark, table, events, ["event_version_id"],
                    update={"last_seen_at": "greatest(t.last_seen_at, s.last_seen_at)",
                            "version_conflict": "t.version_conflict OR s.version_conflict"})
        target = DeltaTable.forName(spark, table)
        for event_id, updated in sorted(conflicts):
            target.update(condition=(F.col("usgs_event_id") == event_id) & (F.col("usgs_updated_ms") == updated),
                          set={"version_conflict": F.lit(True)})
        ids = list(valid)
        actual = spark.table(table).where(F.col("event_version_id").isin(ids)).count()
    else:
        actual = 0
    _quality(spark, pipeline_run_id, fingerprint, "SILVER", "logical_version_count",
             actual, len(valid))
    if rejected:
        quarantine = spark.table(f"{CATALOG}.ops.quarantine_records")
        quarantined = quarantine.where(F.col("quarantine_id").isin(
            [record["quarantine_id"] for record in rejected])).count()
    else:
        quarantined = 0
    _quality(spark, pipeline_run_id, fingerprint, "SILVER", "quarantine_count",
             quarantined, len(rejected))
    version = _version(spark, table)
    _stage(spark, pipeline_run_id, "SILVER", fingerprint, "COMMITTED", len(rows),
           len(valid), len(rejected), table, version)
    return version


def process_batch(spark: Any, volume: MountedVolume, batch_id: str,
                  pipeline_run_id: str, revision: str) -> tuple[int, int]:
    """Process a verified batch belonging to an already registered pipeline run."""
    from pyspark.sql import functions as F

    if any(len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
           for value in (batch_id, pipeline_run_id)):
        raise ContractError("Batch and pipeline run IDs must be 64-character hex values")
    if not revision or len(revision) > 128:
        raise ContractError("An immutable processing revision is required")
    parent = spark.table(f"{CATALOG}.ops.pipeline_runs").where(
        F.col("pipeline_run_id") == pipeline_run_id).select(
            "batch_ids", "processing_revision", "run_status").collect()
    if (len(parent) != 1 or batch_id not in parent[0].batch_ids
            or parent[0].processing_revision != revision
            or parent[0].run_status != "RUNNING"):
        raise ContractError("Batch lacks one RUNNING pipeline-run parent with matching revision")
    manifest, rows = verify_landing(volume, batch_id)
    raw_manifest = volume.read_optional(f"landing/{batch_id}/manifest.json")
    assert raw_manifest is not None
    fingerprint = hashlib.sha256(raw_manifest + revision.encode()).hexdigest()
    _materialize_evidence(spark, volume, manifest, batch_id)
    bronze_version = _bronze(spark, rows, batch_id, pipeline_run_id, fingerprint, revision)
    silver_version = _silver(spark, rows, batch_id, pipeline_run_id, fingerprint, revision,
                             manifest["capture_ids"])
    return bronze_version, silver_version


def main() -> None:
    """Run Phase 3 inside a prepared Databricks job or an explicit dev smoke run."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    volume = MountedVolume()
    bronze, silver = process_batch(spark, volume, args.batch_id, args.pipeline_run_id, args.revision)
    print(json.dumps({"batch_id": args.batch_id, "bronze_delta_version": bronze,
                      "silver_delta_version": silver, "finished_at": stamp(datetime.now(timezone.utc))}))


if __name__ == "__main__":
    main()

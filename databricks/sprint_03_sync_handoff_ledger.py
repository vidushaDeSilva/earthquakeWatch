# Databricks notebook source
"""Independently verify dispatch/landing bytes and import Sprint 3 Delta metadata.

Run after the existing Sprint 2 ledger task in the same serial Databricks Job.
This notebook reads cloud files, checks them, and records PUBLISH_ACKED/LANDED.
Only Sprint 4 may account source items or acknowledge Bronze completion.
"""

import base64
import hashlib
import json
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F

dbutils.widgets.text("volume_root", "/Volumes/earthquake_dev/ops/landing")
root = dbutils.widgets.get("volume_root").rstrip("/")
if root != "/Volumes/earthquake_dev/ops/landing":
    raise ValueError("Sprint 3 ledger sync requires the development Volume")
spark.conf.set("spark.sql.session.timeZone", "UTC")
MAX_BATCHES = 128


def sha(payload):
    """Hash exact persisted bytes before interpreting any JSON."""
    return hashlib.sha256(payload).hexdigest()


def canonical(value):
    """Encode deterministic identity tuples used by the runner's Sprint 3 contracts."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def identity(kind, *parts):
    """Mirror the versioned typed-tuple identity formula for scalar metadata."""
    return sha(canonical([kind, *parts]))


def read_bytes(path, limit=4194304):
    """Read one exact bounded cloud file through Serverless-compatible binaryFile."""
    if not path.startswith(root + "/") or "/../" in path:
        raise ValueError("Untrusted cloud path")
    rows = spark.read.format("binaryFile").load(path).select("length", "content").collect()
    if len(rows) != 1 or rows[0]["length"] > limit:
        raise ValueError("Missing, ambiguous or oversized cloud file")
    return bytes(rows[0]["content"])


def children(path):
    """List existing cloud entries; callers never suppress authorization/read failures."""
    return dbutils.fs.ls(path)


def child_name(entry):
    """Use only the final logical name, independent of dbfs URI prefixes."""
    return entry.name.rstrip("/")


def hex_directory(entry):
    """Reject unexpected stream/batch folder identities before interpolating paths."""
    name = child_name(entry)
    if not entry.isDir() or not re.fullmatch("[0-9a-f]{64}", name):
        raise ValueError("Unexpected stream or batch directory identity")
    return name


def merge_insert(rows, table, key):
    """Insert only immutable rows using the existing table schema and checked keys."""
    if not rows:
        return
    target = spark.table("earthquake_dev." + table)
    frame = spark.createDataFrame(rows, target.schema)
    if frame.groupBy(key).count().where("count > 1").limit(1).count():
        raise ValueError("Duplicate source ledger key for " + table)
    if table == "ops.batches":
        if frame.alias("s").join(target.alias("t"), key).where(
                ~F.col("s.manifest_sha256").eqNullSafe(F.col("t.manifest_sha256"))).limit(1).count():
            raise ValueError("Existing batch key references a different immutable manifest")
    frame.createOrReplaceTempView("s3_merge_input")
    spark.sql(f"""MERGE INTO earthquake_dev.{table} t USING s3_merge_input s ON t.{key}=s.{key}
                  WHEN NOT MATCHED THEN INSERT *""")
    if target.groupBy(key).count().where("count > 1").limit(1).count():
        raise ValueError("Duplicate target ledger key for " + table)


def decoded(value):
    """Preserve Kafka null bytes and reject invalid base64 raw-message evidence."""
    return None if value is None else base64.b64decode(value, validate=True)


def timestamp(value):
    """Require UTC-aware persisted times and normalize to UTC for Spark rows."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Naive ledger timestamp")
    return parsed.astimezone(timezone.utc)


batch_rows, dispatch_updates, run_rows, attempt_rows = [], {}, [], []
stream_entries = [entry for entry in children(root) if child_name(entry) == "handoff"]
if not stream_entries:
    raise ValueError("No handoff directory; run the Sprint 3 workflow first")
for stream_entry in children(root + "/handoff"):
    sid = hex_directory(stream_entry)
    prefix = root + "/handoff/" + sid
    definition = json.loads(read_bytes(prefix + "/definition.json"))
    topic, generation = definition["topic"], definition["stream_generation"]
    if (definition["stream_id"] != sid or definition["environment"] != "dev"
            or topic != "earthquake_events_dev" or sid != identity("stream-v1", "dev", topic, generation)
            or definition["partitions"] not in ([0], [0, 1])):
        raise ValueError("Invalid stream definition")
    folders = {child_name(entry) for entry in children(prefix) if entry.isDir()}
    if "batches" in folders:
        for batch_entry in children(prefix + "/batches"):
            bid = hex_directory(batch_entry)
            if len(batch_rows) >= MAX_BATCHES:
                raise ValueError("Bounded ledger scan exceeds 128 batches; review backlog before continuing")
            batch_prefix = prefix + "/batches/" + bid
            files = {child_name(entry) for entry in children(batch_prefix)}
            if "manifest.json" not in files:
                continue  # An unsealed upload is not an eligible batch.
            raw_manifest = read_bytes(batch_prefix + "/manifest.json")
            m = json.loads(raw_manifest)
            relative = f"handoff/{sid}/batches/{bid}/records.jsonl"
            if any(m.get(key) != value for key, value in {
                "manifest_version": 1, "stream_id": sid, "stream_generation": generation,
                "topic": topic, "batch_id": bid, "state": "LANDED", "records_relative_path": relative}.items()):
                raise ValueError("Manifest identity/path differs")
            data = read_bytes(root + "/" + relative)
            items = [json.loads(line) for line in data.splitlines()]
            if (not items or len(items) != m["row_count"] or len(data) != m["data_bytes"]
                    or sha(data) != m["data_sha256"] or m["partition"] not in definition["partitions"]):
                raise ValueError("Independent landing count/checksum verification failed")
            for index, item in enumerate(items):
                offset = m["offset_first"] + index
                if any(item.get(key) != value for key, value in {
                    "stream_generation": generation, "topic": topic, "partition": m["partition"],
                    "offset": offset, "delivery_id": identity("delivery-v1", generation, topic, m["partition"], offset)}.items()):
                    raise ValueError("Independent delivery/offset verification failed")
                value = decoded(item["value_b64"])
                decoded(item["key_b64"])
                for name, header in item["headers_b64"]:
                    if not isinstance(name, str):
                        raise ValueError("Invalid Kafka header")
                    decoded(header)
                if item["message_sha256"] != (sha(value) if value is not None else None):
                    raise ValueError("Independent raw message hash verification failed")
            if (m["offset_last"] != m["offset_first"] + len(items) - 1 or m["next_offset"] != m["offset_last"] + 1
                    or bid != identity("landing-v1", sid, m["partition"], m["offset_first"], m["offset_last"], sha(data))):
                raise ValueError("Independent deterministic batch identity verification failed")
            batch_rows.append({"batch_id": bid, "run_id": m["first_run_id"], "stream_generation": generation,
                "topic": topic, "partition": m["partition"], "offset_first": m["offset_first"], "offset_last": m["offset_last"],
                "manifest_uri": batch_prefix + "/manifest.json", "manifest_sha256": sha(raw_manifest), "row_count": len(items),
                "accepted_delivery_count": None, "quarantined_delivery_count": None, "distinct_source_version_count": None,
                "bronze_insert_count": None, "landed_at": timestamp(m["landed_at"]), "bronze_committed_at": None,
                "silver_committed_at": None, "data_stage": "LANDED", "last_attempt_status": "SUCCESS"})
    if "dispatch" in folders:
        for capture_entry in children(prefix + "/dispatch"):
            cid = hex_directory(capture_entry)
            captures = spark.table("earthquake_dev.ops.captures").where(F.col("capture_id") == cid).collect()
            if len(captures) != 1:
                raise ValueError("Dispatch references a capture missing from Sprint 2 ledger")
            capture = captures[0]
            for dispatch_entry in children(prefix + "/dispatch/" + cid):
                dg = child_name(dispatch_entry)
                if not dispatch_entry.isDir() or not re.fullmatch("[A-Za-z0-9_-]{1,40}", dg):
                    raise ValueError("Invalid dispatch generation")
                dp = prefix + "/dispatch/" + cid + "/" + dg
                if "manifest.json" not in {child_name(entry) for entry in children(dp)}:
                    continue
                published = json.loads(read_bytes(dp + "/manifest.json"))
                if (published.get("manifest_version") != 1 or published.get("state") != "PUBLISH_ACKED"
                        or published.get("capture_id") != cid or published.get("stream_id") != sid
                        or published.get("topic") != topic or published.get("stream_generation") != generation
                        or published.get("dispatch_generation") != dg
                        or published.get("expected_item_count") != capture.expected_item_count
                        or len(published["chunks"]) != published["chunk_count"]):
                    raise ValueError("Dispatch manifest differs from source capture")
                expected_first = 0
                for index, chunk in enumerate(published["chunks"]):
                    expected_path = f"handoff/{sid}/dispatch/{cid}/{dg}/chunk-{index:06d}.json"
                    if chunk["path"] != expected_path:
                        raise ValueError("Untrusted dispatch chunk path")
                    raw = read_bytes(root + "/" + expected_path)
                    receipt = json.loads(raw)
                    if (sha(raw) != chunk["sha256"] or receipt["first_item_index"] != expected_first
                            or receipt["item_count"] != chunk["item_count"] or receipt["chunk_index"] != index
                            or receipt.get("state") != "PUBLISH_ACKED" or receipt.get("capture_id") != cid
                            or receipt.get("stream_id") != sid or receipt.get("dispatch_generation") != dg
                            or len(receipt["deliveries"]) != receipt["item_count"]
                            or len(receipt["message_sha256"]) != receipt["item_count"]):
                        raise ValueError("Incomplete or corrupt Kafka chunk acknowledgements")
                    if any(item["partition"] not in definition["partitions"] or item["offset"] < 0 for item in receipt["deliveries"]):
                        raise ValueError("Invalid acknowledged broker coordinates")
                    expected_first += receipt["item_count"]
                if expected_first != capture.expected_item_count:
                    raise ValueError("Dispatch item count does not reconcile to capture")
                dispatch_updates[cid] = published["chunk_count"]
    if "runs" in folders:
        for run_entry in children(prefix + "/runs"):
            rid = hex_directory(run_entry)
            report = json.loads(read_bytes(prefix + "/runs/" + rid + "/report.json"))
            if report.get("run_id") != rid or report.get("stream_id") != sid or report.get("status") not in ("SUCCESS", "FAILED"):
                raise ValueError("Invalid terminal bridge run report")
            run_rows.append({"run_id": rid, "run_kind": report["run_kind"], "github_run_id": report["github_run_id"],
                "github_attempt": report["github_attempt"], "databricks_run_id": None,
                "window_start": timestamp(report["window_start"]), "window_end": timestamp(report["window_end"]),
                "started_at": timestamp(report["started_at"]), "finished_at": timestamp(report["finished_at"]),
                "status": report["status"], "code_commit": report["code_commit"],
                "contract_version": "usgs-summary-v1", "error_class": report.get("error_class")})
            attempt_rows.append({"attempt_id": identity("bridge-stage-v1", rid, report["action"]), "run_id": rid,
                "scope_type": "STREAM", "scope_id": sid,
                "stage_name": {"dispatch": "PUBLISH", "land": "LAND", "recover": "RECOVERY", "inspect": "INSPECT"}[report["action"]],
                "attempt_number": 1, "status": report["status"], "started_at": timestamp(report["started_at"]),
                "finished_at": timestamp(report["finished_at"]), "retryable": False, "http_status": None,
                "error_code": report.get("error_class"), "sanitized_message": None,
                "metrics": {"safe_event_count": str(len(report.get("events", [])))}})
            for attempt in report.get("attempts", []):
                attempt_rows.append({"attempt_id": identity("bridge-http-v1", rid, attempt["request_sequence"], attempt["attempt_number"]),
                    "run_id": rid, "scope_type": "STREAM", "scope_id": sid, "stage_name": "HTTP",
                    "attempt_number": attempt["attempt_number"], "status": attempt["status"],
                    "started_at": timestamp(attempt["started_at"]), "finished_at": timestamp(attempt["finished_at"]),
                    "retryable": attempt["retryable"], "http_status": attempt.get("http_status"),
                    "error_code": None, "sanitized_message": None,
                    "metrics": {"service": attempt["service"], "duration_ms": str(attempt["duration_ms"])}})

merge_insert(batch_rows, "ops.batches", "batch_id")
merge_insert(run_rows, "ops.runs", "run_id")
merge_insert(attempt_rows, "ops.stage_attempts", "attempt_id")
if dispatch_updates:
    updates = spark.createDataFrame(list(dispatch_updates.items()), "capture_id STRING, chunk_count BIGINT")
    updates.createOrReplaceTempView("s3_dispatch_updates")
    # An older capture sync cannot regress ACCOUNTED or inflate accounted counts.
    spark.sql("""MERGE INTO earthquake_dev.ops.captures t USING s3_dispatch_updates s ON t.capture_id=s.capture_id
      WHEN MATCHED AND t.state = 'CAPTURED' THEN UPDATE SET t.state='PUBLISH_ACKED', t.dispatch_chunk_count=s.chunk_count""")
print(f"PASS Sprint 3 handoff ledger: {len(batch_rows)} verified batches; {len(dispatch_updates)} publish-acknowledged captures")
display(spark.table("earthquake_dev.ops.batches").select("batch_id", "partition", "offset_first", "offset_last", "row_count", "data_stage"))

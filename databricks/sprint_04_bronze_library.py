# Databricks notebook source
"""Validate landed Kafka evidence and implement recoverable Bronze transactions.

Load this helper with %run from the Sprint 4 notebook. Pure contracts and the
engine are also exercised offline. Only SparkBronzeStore accesses Delta/Volumes;
all processing data stays in memory or managed Databricks storage.
"""

import base64
import hashlib
import json
import math
import re
from datetime import datetime, timezone

BRONZE_CONTRACT = "usgs-summary-v1"
BRONZE_REVISION = "bronze-v1"
BRONZE_ROOT = "/Volumes/earthquake_dev/ops/landing"
BRONZE_SEMANTIC_FIELDS = ("time", "updated", "mag", "magType", "place", "felt", "cdi", "mmi",
                         "alert", "status", "tsunami", "sig", "type")
BRONZE_PROP_FIELDS = set(BRONZE_SEMANTIC_FIELDS) | {
    "tz", "url", "detail", "net", "code", "ids", "sources", "types", "nst", "dmin", "rms", "gap", "title"}
BRONZE_KEYS = {"bronze.event_versions": "source_version_id", "bronze.deliveries": "delivery_id",
               "bronze.quarantine": "quarantine_id", "ops.check_results": "check_id",
               "ops.stage_attempts": "attempt_id", "ops.runs": "run_id"}


class BronzeError(ValueError):
    """Stop processing without acknowledging unproven or conflicting data."""


class BronzeInjectedFailure(BronzeError):
    """Represent an explicitly requested development interruption."""


def bronze_fault(selected, boundary):
    """Fail at one requested boundary so a retry can demonstrate convergence."""
    if selected == boundary:
        raise BronzeInjectedFailure("Intentional Bronze interruption: " + boundary)


def bronze_numbers(value):
    """Mirror v1 numeric normalization so 6 and 6.0 share semantic identity."""
    if isinstance(value, float):
        if not math.isfinite(value):
            raise BronzeError("Non-finite JSON number")
        return int(value) if value.is_integer() else value
    if isinstance(value, dict):
        return {key: bronze_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [bronze_numbers(item) for item in value]
    return value


def bronze_json(value):
    """Encode the exact existing canonical JSON contract without local files."""
    return json.dumps(bronze_numbers(value), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def bronze_sha(payload):
    """Hash exact immutable bytes."""
    return hashlib.sha256(payload).hexdigest()


def bronze_id(kind, *parts):
    """Hash a versioned typed tuple using the Sprint 1 identity contract."""
    return bronze_sha(bronze_json([kind, *parts]))


def bronze_constant(value):
    """Reject nonstandard JSON constants rather than accepting NaN/Infinity."""
    raise BronzeError("Invalid JSON constant")


def bronze_decode(payload):
    """Decode bounded evidence as strict UTF-8 JSON."""
    try:
        return json.loads(payload.decode("utf-8"), parse_constant=bronze_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise BronzeError("Invalid UTF-8/JSON evidence") from exc


def bronze_time(value):
    """Parse an aware persisted timestamp without using the host timezone."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("Naive timestamp")
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError) as exc:
        raise BronzeError("Invalid UTC evidence timestamp") from exc


def bronze_millis(value):
    """Convert representable epoch milliseconds, preserving null and pre-1970 times."""
    if value is None:
        return None
    if type(value) is not int or not -62135596800000 <= value <= 253402300799999:
        raise BronzeError("Unrepresentable epoch timestamp")
    from datetime import timedelta
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)


def bronze_b64(value):
    """Preserve null separately from empty bytes and reject damaged raw evidence."""
    try:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("Non-string base64")
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise BronzeError("Invalid base64 landing evidence") from exc


def bronze_version(feature):
    """Reproduce the producer's source-version contract before typed validation."""
    if not isinstance(feature, dict):
        return None
    props, event_id = feature.get("properties"), feature.get("id")
    if (not isinstance(props, dict) or not isinstance(event_id, str) or not event_id
            or type(props.get("updated")) is not int or props["updated"] < 0):
        return None
    geometry = feature.get("geometry") or {}
    if not isinstance(geometry, dict):
        return None
    projection = {key: props.get(key) for key in BRONZE_SEMANTIC_FIELDS}
    projection["coordinates"] = geometry.get("coordinates")
    return bronze_id("source-version-v1", "usgs", event_id, props["updated"], bronze_sha(bronze_json(projection)))


def bronze_finite(value):
    """Reject bool and numbers too large for downstream DOUBLE fields."""
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def bronze_feature_errors(feature):
    """Validate identity and basic types; keep nullable impact values and negative depths."""
    if not isinstance(feature, dict):
        return ["FEATURE_NOT_OBJECT"]
    errors = []
    if feature.get("type") != "Feature":
        errors.append("FEATURE_TYPE")
    if not isinstance(feature.get("id"), str) or not feature["id"].strip():
        errors.append("EVENT_ID")
    props = feature.get("properties")
    if not isinstance(props, dict):
        return errors + ["PROPERTIES"]
    updated = props.get("updated")
    if type(updated) is not int or not 0 <= updated <= 253402300799999:
        errors.append("UPDATED_TIME")
    deleted = props.get("status") == "deleted"
    event_time = props.get("time")
    if not (deleted and event_time is None) and (
            type(event_time) is not int or not -62135596800000 <= event_time <= 253402300799999):
        errors.append("EVENT_TIME")
    geometry = feature.get("geometry")
    if not (deleted and geometry is None):
        coords = geometry.get("coordinates") if isinstance(geometry, dict) else None
        if (not isinstance(geometry, dict) or geometry.get("type") != "Point"
                or not isinstance(coords, list) or len(coords) != 3
                or any(not bronze_finite(n) for n in coords)):
            errors.append("GEOMETRY")
        elif not -180 <= coords[0] <= 180 or not -90 <= coords[1] <= 90:
            errors.append("COORDINATE_RANGE")
    for key in ("mag", "cdi", "mmi"):
        value = props.get(key)
        if value is not None and (not bronze_finite(value)):
            errors.append("NUMERIC_" + key.upper())
    for key in ("felt", "sig"):
        value = props.get(key)
        if value is not None and (type(value) is not int or not 0 <= value <= 9223372036854775807):
            errors.append("INTEGER_" + key.upper())
    for key in ("magType", "place", "status", "type"):
        if props.get(key) is not None and not isinstance(props[key], str):
            errors.append("STRING_" + key.upper())
    if props.get("alert") not in (None, "green", "yellow", "orange", "red"):
        errors.append("PAGER_ALERT")
    if props.get("tsunami") is not None and (type(props["tsunami"]) is not int or props["tsunami"] not in (0, 1)):
        errors.append("TSUNAMI_FLAG")
    return errors


def bronze_verify_batch(manifest, data, definition):
    """Prove exact sealed bytes, contiguous offsets and every raw delivery identity."""
    if not isinstance(manifest, dict) or not isinstance(definition, dict):
        raise BronzeError("Malformed handoff metadata")
    sid = definition.get("stream_id")
    generation, topic = definition.get("stream_generation"), definition.get("topic")
    if (not isinstance(generation, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", generation)
            or topic != "earthquake_events_dev" or definition.get("environment") != "dev"
            or definition.get("manifest_version") != 1
            or sid != bronze_id("stream-v1", "dev", topic, generation)
            or definition.get("partitions") not in ([0], [0, 1])):
        raise BronzeError("Unsupported stream definition")
    bid = manifest.get("batch_id")
    if not isinstance(bid, str) or not re.fullmatch("[0-9a-f]{64}", bid):
        raise BronzeError("Invalid batch identity")
    required = {"manifest_version": 1, "stream_id": sid, "stream_generation": generation,
                "topic": topic, "state": "LANDED",
                "records_relative_path": f"handoff/{sid}/batches/{bid}/records.jsonl"}
    if any(manifest.get(key) != value for key, value in required.items()):
        raise BronzeError("Unexpected landing manifest")
    first, partition = manifest.get("offset_first"), manifest.get("partition")
    if type(first) is not int or first < 0 or type(partition) is not int or partition not in definition["partitions"]:
        raise BronzeError("Invalid broker range")
    if len(data) > 4194304 or bronze_sha(data) != manifest.get("data_sha256"):
        raise BronzeError("Landing checksum or byte limit")
    items = [bronze_decode(line) for line in data.splitlines()]
    if not items or len(items) != manifest.get("row_count") or len(data) != manifest.get("data_bytes"):
        raise BronzeError("Landing count mismatch")
    for index, row in enumerate(items):
        expected = {"stream_generation": generation, "topic": topic, "partition": partition,
                    "offset": first + index,
                    "delivery_id": bronze_id("delivery-v1", generation, topic, partition, first + index)}
        if not isinstance(row, dict) or any(row.get(key) != value for key, value in expected.items()):
            raise BronzeError("Landing identity or offset mismatch")
        value = bronze_b64(row.get("value_b64"))
        bronze_b64(row.get("key_b64"))
        if row.get("message_sha256") != (bronze_sha(value) if value is not None else None):
            raise BronzeError("Message checksum mismatch")
        headers = row.get("headers_b64")
        if not isinstance(headers, list):
            raise BronzeError("Malformed landing headers")
        for pair in headers:
            if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[0], str):
                raise BronzeError("Malformed landing header")
            bronze_b64(pair[1])
        bronze_millis(row.get("kafka_timestamp_ms"))
    last = first + len(items) - 1
    if (manifest.get("offset_last") != last or manifest.get("next_offset") != last + 1
            or bid != bronze_id("landing-v1", sid, partition, first, last, bronze_sha(data))):
        raise BronzeError("Batch identity mismatch")
    bronze_time(manifest["landed_at"])
    return items


class BronzeCaptureCache:
    """Load and independently verify frozen source bytes once per referenced capture."""

    def __init__(self, store):
        """Keep a bounded in-memory source cache backed only by the cloud store."""
        self.store, self.cache, self.total_bytes = store, {}, 0

    def get(self, capture_id):
        """Resolve known ledger lineage and verify its original source manifest and feed."""
        if not isinstance(capture_id, str) or not re.fullmatch("[0-9a-f]{64}", capture_id):
            return None
        if capture_id in self.cache:
            return self.cache[capture_id]
        rows = self.store.lookup("ops.captures", "capture_id", [capture_id])
        if not rows:
            return None
        if len(rows) != 1:
            raise BronzeError("Duplicate capture ledger key")
        row = rows[0]
        collection = row["collection_id"]
        if not isinstance(collection, str) or not re.fullmatch("[0-9a-f]{64}", collection):
            raise BronzeError("Invalid capture collection identity")
        prefix = f"captures/{collection}/{capture_id}"
        raw = self.store.read(prefix + "/ready.json")
        manifest = bronze_decode(raw)
        payload = self.store.read(prefix + "/response.json", 16777216)
        self.total_bytes += len(payload)
        if len(self.cache) >= 256 or self.total_bytes > 33554432:
            raise BronzeError("Capture verification budget exceeded; reduce processing batch size")
        if (row["manifest_uri"] != BRONZE_ROOT + "/" + prefix + "/ready.json"
                or row["response_uri"] != BRONZE_ROOT + "/" + prefix + "/response.json"
                or manifest.get("contract_version") != BRONZE_CONTRACT
                or manifest.get("response_relative_path") != prefix + "/response.json"
                or manifest.get("capture_id") != capture_id or manifest.get("collection_id") != collection
                or manifest.get("response_sha256") != bronze_sha(payload)
                or row["response_sha256"] != bronze_sha(payload)
                or manifest.get("response_bytes") != len(payload)
                or capture_id != bronze_id("capture-v1", collection, bronze_sha(payload))):
            raise BronzeError("Frozen source capture integrity failure")
        for field in ("source_id", "source_revision", "expected_item_count"):
            if manifest.get(field) != row[field]:
                raise BronzeError("Capture ledger disagrees with frozen manifest")
        if bronze_time(manifest["fetched_at"]) != row["fetched_at"]:
            # Spark returns naive UTC timestamps in collected rows.
            if bronze_time(manifest["fetched_at"]).replace(tzinfo=None) != row["fetched_at"]:
                raise BronzeError("Capture timestamp mismatch")
        feed = bronze_decode(payload)
        features, meta = feed.get("features"), feed.get("metadata")
        if (feed.get("type") != "FeatureCollection" or not isinstance(features, list)
                or not isinstance(meta, dict) or meta.get("count") != len(features)
                or meta.get("status") != 200 or type(meta.get("generated")) is not int
                or len(features) != row["expected_item_count"]):
            raise BronzeError("Frozen source count failure")
        self.cache[capture_id] = (manifest, features)
        return self.cache[capture_id]


def bronze_classify(row, manifest, sources):
    """Classify one delivery; trust capture attribution only after exact source-item proof."""
    raw = bronze_b64(row["value_b64"])
    delivery = {"delivery_id": row["delivery_id"], "batch_id": manifest["batch_id"],
        "stream_generation": row["stream_generation"], "topic": row["topic"], "partition": row["partition"],
        "offset": row["offset"], "kafka_timestamp": bronze_millis(row.get("kafka_timestamp_ms")),
        "capture_id": None, "source_item_id": None, "source_item_index": None,
        "source_version_id": None, "message_sha256": row["message_sha256"],
        "record_sha256": bronze_sha(bronze_json(row)), "raw_feature_sha256": None,
        "received_at": bronze_time(manifest["landed_at"]), "disposition": "QUARANTINED"}
    errors, feature, envelope = [], None, None
    if raw is None:
        errors = ["NULL_MESSAGE"]
    else:
        try:
            envelope = bronze_decode(raw)
        except BronzeError:
            errors = ["MESSAGE_JSON"]
    if not errors:
        if not isinstance(envelope, dict):
            errors = ["ENVELOPE_OBJECT"]
        elif any(envelope.get(key) != value for key, value in {
                "envelope_version": 1, "contract_version": BRONZE_CONTRACT, "environment": "dev",
                "stream_generation": row["stream_generation"], "topic": row["topic"]}.items()):
            errors = ["ENVELOPE_CONTRACT"]
        else:
            source = sources.get(envelope.get("capture_id"))
            if source is None:
                errors = ["UNKNOWN_CAPTURE"]
            else:
                capture, features = source
                index = envelope.get("source_item_index")
                if type(index) is not int or not 0 <= index < len(features):
                    errors = ["SOURCE_ITEM_INDEX"]
                else:
                    feature = features[index]
                    item_id = bronze_id("source-item-v1", capture["capture_id"], index)
                    event_id = feature.get("id") if isinstance(feature, dict) else None
                    event_id = event_id if isinstance(event_id, str) and event_id else None
                    expected = {"capture_id": capture["capture_id"], "collection_id": capture["collection_id"],
                        "source_id": capture["source_id"], "source_revision": capture["source_revision"],
                        "source_item_id": item_id, "source_event_id": event_id,
                        "captured_at": capture["fetched_at"], "source_version_id": bronze_version(feature),
                        "raw_feature_sha256": bronze_sha(bronze_json(feature))}
                    if (any(envelope.get(key) != value for key, value in expected.items())
                            or "feature" not in envelope or bronze_json(envelope["feature"]) != bronze_json(feature)
                            or bronze_b64(row["key_b64"]) != (event_id or item_id).encode("utf-8")):
                        errors = ["SOURCE_ITEM_PROOF"]
                    else:
                        delivery.update(capture_id=capture["capture_id"], source_item_id=item_id,
                                        source_item_index=index, raw_feature_sha256=expected["raw_feature_sha256"])
                        errors = bronze_feature_errors(feature)
    version = None
    if not errors:
        props = feature["properties"]
        projection = {key: props.get(key) for key in BRONZE_SEMANTIC_FIELDS}
        projection["coordinates"] = (feature.get("geometry") or {}).get("coordinates")
        version = {"source_version_id": bronze_version(feature), "source_system": "usgs",
            "source_event_id": feature["id"], "source_updated_ms": props["updated"],
            "event_time_ms": props.get("time"), "payload_sha256": bronze_sha(bronze_json(projection)),
            "raw_feature_sha256": bronze_sha(bronze_json(feature)), "raw_event_json": bronze_json(feature).decode(),
            "contract_version": BRONZE_CONTRACT, "first_capture_id": delivery["capture_id"],
            "first_delivery_id": delivery["delivery_id"], "first_batch_id": manifest["batch_id"],
            "first_ingested_at": delivery["received_at"]}
        delivery.update(source_version_id=version["source_version_id"], disposition="ACCEPTED")
    quarantine = None
    if errors:
        quarantine = {"quarantine_id": bronze_id("quarantine-v1", row["delivery_id"], "BRONZE", BRONZE_REVISION),
            "delivery_id": row["delivery_id"], "batch_id": manifest["batch_id"], "stage_name": "BRONZE",
            "contract_or_normalizer_revision": BRONZE_REVISION, "error_codes": errors,
            "raw_value_b64": row["value_b64"],
            "raw_reference_uri": BRONZE_ROOT + "/" + manifest["records_relative_path"],
            "rejected_at": delivery["received_at"], "resolution_status": "OPEN", "resolved_at": None, "replay_run_id": None}
    unknown = []
    if isinstance(feature, dict):
        unknown += ["feature." + key for key in set(feature) - {"type", "id", "properties", "geometry", "bbox"}]
        props = feature.get("properties")
        if isinstance(props, dict):
            unknown += ["properties." + key for key in set(props) - BRONZE_PROP_FIELDS]
    return delivery, version, quarantine, sorted(unknown)


def bronze_unique(rows, key, compare):
    """Deduplicate MERGE input and fail if the same logical key has conflicting meaning."""
    unique = {}
    for row in rows:
        old = unique.get(row[key])
        if old is not None and any(old.get(field) != row.get(field) for field in compare):
            raise BronzeError("Conflicting immutable input key: " + key)
        unique.setdefault(row[key], row)
    return list(unique.values())


class BronzeEngine:
    """Coordinate independent Delta writes, reconciliation and final receipt creation."""

    def __init__(self, store, run_id, fail_at="none"):
        """Bind one serialized processing attempt and an optional development fault."""
        self.store, self.run_id, self.fail_at = store, run_id, fail_at
        self.sources = BronzeCaptureCache(store)

    def process(self, bundles):
        """Process a bounded group atomically per table, then acknowledge verified batches."""
        plans, versions, deliveries, quarantines = [], [], [], []
        for manifest, data, definition, raw_manifest in bundles:
            if bronze_decode(raw_manifest) != manifest:
                raise BronzeError("Parsed manifest differs from stored bytes")
            items = bronze_verify_batch(manifest, data, definition)
            expected_uri = BRONZE_ROOT + "/" + manifest["records_relative_path"].rsplit("/", 1)[0] + "/manifest.json"
            ledger = self.store.lookup("ops.batches", "batch_id", [manifest["batch_id"]])
            if (len(ledger) != 1 or ledger[0]["manifest_sha256"] != bronze_sha(raw_manifest)
                    or ledger[0]["manifest_uri"] != expected_uri or ledger[0]["row_count"] != len(items)
                    or any(ledger[0].get(field) != manifest[field] for field in
                           ("stream_generation", "topic", "partition", "offset_first", "offset_last"))):
                raise BronzeError("Batch is missing from handoff ledger or disagrees with evidence")
            ds, vs, qs, unknown = [], [], [], set()
            for row in items:
                delivery, version, quarantine, extra = bronze_classify(row, manifest, self.sources)
                ds.append(delivery)
                if version:
                    vs.append(version)
                if quarantine:
                    qs.append(quarantine)
                unknown.update(extra)
            plans.append((manifest, raw_manifest, ds, vs, qs, unknown))
            deliveries.extend(ds)
            versions.extend(vs)
            quarantines.extend(qs)
        if len(deliveries) > 10000:
            raise BronzeError("Bronze planning exceeds 10000 deliveries; lower bronze_max_batches")
        version_fields = ("source_event_id", "source_updated_ms", "payload_sha256", "contract_version")
        delivery_fields = ("record_sha256", "capture_id", "source_item_id", "source_version_id", "disposition")
        versions = bronze_unique(versions, "source_version_id", version_fields)
        deliveries = bronze_unique(deliveries, "delivery_id", delivery_fields)
        quarantines = bronze_unique(quarantines, "quarantine_id", ("delivery_id", "error_codes", "raw_value_b64"))
        # Each target commits independently. No receipt may precede all three verified writes.
        self.store.merge("bronze.event_versions", versions, version_fields)
        bronze_fault(self.fail_at, "after-versions")
        self.store.merge("bronze.quarantine", quarantines, ("delivery_id", "error_codes", "raw_value_b64"))
        bronze_fault(self.fail_at, "after-quarantine")
        self.store.merge("bronze.deliveries", deliveries, delivery_fields)
        bronze_fault(self.fail_at, "after-deliveries")
        self.store.prove("bronze.event_versions", versions, version_fields)
        self.store.prove("bronze.quarantine", quarantines, ("delivery_id", "error_codes", "raw_value_b64"))
        self.store.prove("bronze.deliveries", deliveries, delivery_fields)
        persisted_versions = {row["source_version_id"]: row for row in self.store.lookup(
            "bronze.event_versions", "source_version_id", [row["source_version_id"] for row in versions])}
        for manifest, raw_manifest, ds, vs, qs, unknown in plans:
            bid = manifest["batch_id"]
            if len(ds) != manifest["row_count"] or len(vs) + len(qs) != len(ds):
                raise BronzeError("Bronze dispositions do not account for every input")
            new_ids = {row["source_version_id"] for row in vs
                       if persisted_versions[row["source_version_id"]]["first_batch_id"] == bid}
            self.store.check(self.run_id, "BATCH", bid, "delivery_accounting", "PASS", len(ds), str(manifest["row_count"]))
            self.store.check(self.run_id, "BATCH", bid, "schema_observation", "WARN" if unknown else "PASS",
                             len(unknown), "0 unknown fields", {"unknown_fields": sorted(unknown)})
            self.store.ack_batch(bid, len(vs), len(qs), len({v["source_version_id"] for v in vs}), len(new_ids))
            bronze_fault(self.fail_at, "after-batch-ledger")
            receipt = {"receipt_version": 1, "processor_revision": BRONZE_REVISION, "batch_id": bid,
                "manifest_sha256": bronze_sha(raw_manifest), "row_count": len(ds),
                "accepted_delivery_count": len(vs), "quarantined_delivery_count": len(qs),
                "delivery_set_sha256": bronze_sha(bronze_json(sorted(row["delivery_id"] for row in ds))),
                "state": "BRONZE_COMMITTED"}
            self.store.put_receipt("receipts/bronze/" + bid + ".json", bronze_json(receipt))
            bronze_fault(self.fail_at, "after-batch-receipt")
        return len(plans)

    def account_captures(self):
        """Count distinct proven source ordinals, including durable feature rejections."""
        pending_published = []
        for capture in self.store.pending_captures():
            cid = capture["capture_id"]
            source = self.sources.get(cid)
            if source is None:
                raise BronzeError("Capture disappeared while accounting")
            manifest, features = source
            items = self.store.accounted_items(cid)
            expected = {bronze_id("source-item-v1", cid, i): i for i in range(len(features))}
            if any(item not in expected or expected[item] != index for item, index in items.items()):
                raise BronzeError("Invalid source ordinal in persisted accounting")
            count = len(items)
            self.store.check(self.run_id, "CAPTURE", cid, "source_item_accounting",
                             "PASS" if count == len(features) else "WARN", count, str(len(features)))
            # Empty captures still require a completed dispatch before being ACCOUNTED.
            complete = count == len(features) and capture["state"] in ("PUBLISH_ACKED", "ACCOUNTED")
            self.store.ack_capture(cid, count, complete)
            if capture["state"] == "PUBLISH_ACKED" and not complete:
                pending_published.append(cid)
            bronze_fault(self.fail_at, "after-capture-ledger")
            if complete:
                receipt = {"receipt_version": 1, "processor_revision": BRONZE_REVISION,
                    "capture_id": cid, "response_sha256": manifest["response_sha256"],
                    "expected_item_count": len(features), "accounted_item_count": count, "state": "ACCOUNTED"}
                self.store.put_receipt("receipts/captures/" + cid + ".json", bronze_json(receipt))
                bronze_fault(self.fail_at, "after-capture-receipt")
        return pending_published


class SparkBronzeStore:
    """Use public Spark Connect APIs and bounded managed-Volume operations."""

    def __init__(self, spark_session, utilities, root=BRONZE_ROOT):
        """Confine all reads/writes to the established development namespace."""
        if root.rstrip("/") != BRONZE_ROOT:
            raise BronzeError("Sprint 4 requires the development Volume")
        self.spark, self.utils, self.root = spark_session, utilities, BRONZE_ROOT
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def table(self, name):
        """Allow only known project tables rather than arbitrary SQL identifiers."""
        if name not in set(BRONZE_KEYS) | {"ops.captures", "ops.batches"}:
            raise BronzeError("Unknown Bronze store table")
        return self.spark.table("earthquake_dev." + name)

    def path(self, relative):
        """Reject absolute paths and traversal before accessing any cloud object."""
        if (not relative or relative.startswith("/") or "\\" in relative
                or any(part in ("", ".", "..") for part in relative.split("/"))):
            raise BronzeError("Unsafe Volume-relative path")
        return self.root + "/" + relative

    def read(self, relative, limit=4194304):
        """Read a bounded binary file using the same tested route as Sprint 3."""
        rows = self.spark.read.format("binaryFile").load(self.path(relative)).select("length", "content").collect()
        if len(rows) != 1 or rows[0]["length"] > limit:
            raise BronzeError("Missing, ambiguous or oversized Volume file")
        return bytes(rows[0]["content"])

    def names(self, relative=""):
        """List direct children and propagate authentication/connection errors."""
        path = self.path(relative) if relative else self.root
        entries = self.utils.fs.ls(path)
        if len(entries) > 1024:
            raise BronzeError("Directory inventory exceeds bounded development scan")
        return {entry.name.rstrip("/"): entry.isDir() for entry in entries}

    def optional(self, relative):
        """Treat only a directory-listed absent file as absent; never swallow read failures."""
        parts = relative.split("/")
        parent = ""
        for part in parts:
            entries = self.names(parent)
            if part not in entries:
                return None
            parent = part if not parent else parent + "/" + part
        return self.read(relative)

    def put_receipt(self, relative, payload):
        """Write immutable ASCII-safe proof, verify readback and handle a lost PUT reply."""
        previous = self.optional(relative)
        if previous is not None:
            if previous != payload:
                raise BronzeError("Conflicting immutable Bronze receipt")
            return
        self.utils.fs.mkdirs(self.path(relative.rsplit("/", 1)[0]))
        try:
            self.utils.fs.put(self.path(relative), payload.decode("utf-8"), False)
        except Exception:
            # A lost response may follow a successful write. Readback decides, not the exception.
            if self.optional(relative) != payload:
                raise
        if self.read(relative) != payload:
            raise BronzeError("Bronze receipt readback differs")

    def lookup(self, name, key, values):
        """Read only bounded keys, checking that the result has no duplicate logical keys."""
        from pyspark.sql import functions as F
        if not values:
            return []
        if len(values) > 10000 or key not in self.table(name).columns:
            raise BronzeError("Oversized or invalid key lookup")
        rows = self.table(name).where(F.col(key).isin(list(set(values)))).limit(20001).collect()
        if len(rows) > 20000:
            raise BronzeError("Scoped lookup exceeded result budget")
        result = [row.asDict(recursive=True) for row in rows]
        if len({row[key] for row in result}) != len(result):
            raise BronzeError("Duplicate target key in " + name)
        return result

    def frame(self, name, rows):
        """Use the existing explicit table schema, including null-only columns."""
        return self.spark.createDataFrame(rows, self.table(name).schema)

    def prove(self, name, rows, compare):
        """Prove every expected immutable key and field exists after the Delta commit."""
        if not rows:
            return
        key = BRONZE_KEYS[name]
        existing = {row[key]: row for row in self.lookup(name, key, [row[key] for row in rows])}
        for row in rows:
            other = existing.get(row[key])
            if other is None or any(other.get(field) != row.get(field) for field in compare):
                raise BronzeError("Post-write reconciliation failed for " + name)

    def merge(self, name, rows, compare):
        """Pre-deduplicate source keys and insert immutable facts in one Delta statement."""
        if not rows:
            return
        key = BRONZE_KEYS[name]
        rows = bronze_unique(rows, key, compare)
        existing = {row[key]: row for row in self.lookup(name, key, [row[key] for row in rows])}
        for row in rows:
            old = existing.get(row[key])
            if old is not None and any(old.get(field) != row.get(field) for field in compare):
                raise BronzeError("Immutable target conflict in " + name)
        self.frame(name, rows).createOrReplaceTempView("s4_insert_input")
        self.spark.sql(f"""MERGE INTO earthquake_dev.{name} t USING s4_insert_input s ON t.{key}=s.{key}
            WHEN NOT MATCHED THEN INSERT *""")

    def upsert_ops(self, name, rows, update_fields):
        """Update mutable attempt/check metadata using fixed application-owned columns."""
        if not rows:
            return
        key = BRONZE_KEYS[name]
        if len({row[key] for row in rows}) != len(rows):
            raise BronzeError("Duplicate operational input")
        self.frame(name, rows).createOrReplaceTempView("s4_ops_input")
        assignments = ", ".join(f"t.{field}=s.{field}" for field in update_fields)
        self.spark.sql(f"""MERGE INTO earthquake_dev.{name} t USING s4_ops_input s ON t.{key}=s.{key}
            WHEN MATCHED THEN UPDATE SET {assignments} WHEN NOT MATCHED THEN INSERT *""")

    def check(self, run_id, scope_type, scope_id, rule, status, observed, expected, details=None):
        """Persist one inspectable rule result per scope and actual processing attempt."""
        row = {"check_id": bronze_id("check-v1", run_id, scope_type, scope_id, rule, BRONZE_REVISION),
            "run_id": run_id, "scope_type": scope_type, "scope_id": scope_id, "rule_id": rule,
            "rule_version": BRONZE_REVISION, "severity": "ERROR" if rule == "delivery_accounting" else "WARNING",
            "status": status, "observed_value": float(observed), "expected_value": expected,
            "evaluated_at": datetime.now(timezone.utc), "resolved_at": None,
            "details_json": bronze_json(details).decode() if details else None}
        self.upsert_ops("ops.check_results", [row], ("status", "observed_value", "evaluated_at", "details_json"))

    def ack_batch(self, batch_id, accepted, rejected, distinct_versions, inserted):
        """Advance the durable data stage without regressing later Silver progress."""
        rows = self.lookup("ops.batches", "batch_id", [batch_id])
        if len(rows) != 1:
            raise BronzeError("Missing batch ledger")
        current = rows[0]
        if accepted + rejected != current["row_count"]:
            raise BronzeError("Batch ledger count mismatch")
        if current["data_stage"] not in ("LANDED", "BRONZE_COMMITTED", "SILVER_COMMITTED"):
            raise BronzeError("Unknown batch data stage")
        if current["data_stage"] != "LANDED":
            if (current["accepted_delivery_count"] != accepted or current["quarantined_delivery_count"] != rejected
                    or current["distinct_source_version_count"] != distinct_versions):
                raise BronzeError("Existing Bronze batch reconciliation differs")
            return
        updates = self.spark.createDataFrame([(batch_id, accepted, rejected, distinct_versions, inserted)],
            "batch_id STRING, accepted BIGINT, rejected BIGINT, distinct_versions BIGINT, inserted BIGINT")
        updates.createOrReplaceTempView("s4_batch_ack")
        self.spark.sql("""MERGE INTO earthquake_dev.ops.batches t USING s4_batch_ack s ON t.batch_id=s.batch_id
            WHEN MATCHED AND t.data_stage='LANDED' THEN UPDATE SET
            t.accepted_delivery_count=s.accepted, t.quarantined_delivery_count=s.rejected,
            t.distinct_source_version_count=s.distinct_versions, t.bronze_insert_count=s.inserted,
            t.bronze_committed_at=current_timestamp(), t.data_stage='BRONZE_COMMITTED', t.last_attempt_status='SUCCESS'""")
        verified = self.lookup("ops.batches", "batch_id", [batch_id])[0]
        if verified["data_stage"] not in ("BRONZE_COMMITTED", "SILVER_COMMITTED"):
            raise BronzeError("Batch ledger acknowledgement failed")

    def pending_captures(self):
        """Revisit all bounded captures, including receipt repair after an interrupted ack."""
        rows = self.table("ops.captures").orderBy("fetched_at", "capture_id").limit(257).collect()
        if len(rows) > 256:
            raise BronzeError("Capture inventory exceeds 256; incremental recovery required")
        return [row.asDict(recursive=True) for row in rows]

    def accounted_items(self, capture_id):
        """Count only proven accepted/rejected deliveries in Bronze-committed batches."""
        from pyspark.sql import functions as F
        ds = self.table("bronze.deliveries").where(F.col("capture_id") == capture_id).alias("d")
        batches = self.table("ops.batches").where("data_stage IN ('BRONZE_COMMITTED','SILVER_COMMITTED')").select("batch_id")
        ds = ds.join(batches, "batch_id", "inner")
        versions = self.table("bronze.event_versions").select("source_version_id").withColumn("version_present", F.lit(True))
        rejects = self.table("bronze.quarantine").where("stage_name='BRONZE'").select("delivery_id").distinct().withColumn("reject_present", F.lit(True))
        ds = ds.join(versions, "source_version_id", "left").join(rejects, "delivery_id", "left")
        bad = ds.where("(disposition='ACCEPTED' AND version_present IS NULL) OR "
                       "(disposition='QUARANTINED' AND reject_present IS NULL) OR "
                       "disposition NOT IN ('ACCEPTED','QUARANTINED') OR source_item_id IS NULL OR source_item_index IS NULL")
        if bad.limit(1).count():
            raise BronzeError("Broken persisted delivery disposition/lineage")
        rows = ds.select("source_item_id", "source_item_index").distinct().limit(10001).collect()
        if len(rows) > 10000:
            raise BronzeError("Capture accounting exceeds 10000 source items")
        result = {}
        for row in rows:
            if row.source_item_id in result and result[row.source_item_id] != row.source_item_index:
                raise BronzeError("Source item maps to conflicting ordinals")
            result[row.source_item_id] = row.source_item_index
        return result

    def ack_capture(self, capture_id, count, complete):
        """Advance source counts monotonically and preserve an existing completion time."""
        old = self.lookup("ops.captures", "capture_id", [capture_id])[0]
        if count < old["accounted_item_count"] or count > old["expected_item_count"]:
            raise BronzeError("Capture accounting cannot regress or exceed its source count")
        if old["state"] == "ACCOUNTED" and not complete:
            raise BronzeError("Existing completed capture lost accounting proof")
        self.spark.createDataFrame([(capture_id, count, complete)],
            "capture_id STRING, item_count BIGINT, complete BOOLEAN").createOrReplaceTempView("s4_capture_ack")
        self.spark.sql("""MERGE INTO earthquake_dev.ops.captures t USING s4_capture_ack s ON t.capture_id=s.capture_id
            WHEN MATCHED THEN UPDATE SET t.accounted_item_count=s.item_count,
            t.state=CASE WHEN s.complete THEN 'ACCOUNTED' ELSE t.state END,
            t.accounted_at=CASE WHEN s.complete THEN coalesce(t.accounted_at,current_timestamp()) ELSE t.accounted_at END""")

    def bundles(self, recheck=False, max_batches=32):
        """Inventory sealed manifests independently of offsets, selecting bounded unacknowledged work."""
        if "handoff" not in self.names():
            raise BronzeError("No handoff files; run the Sprint 3 workflow first")
        ledgers = self.table("ops.batches").limit(129).collect()
        if len(ledgers) > 128:
            raise BronzeError("Development ledger exceeds 128 batches; incremental inventory required")
        ledger = {row.batch_id: row.asDict(recursive=True) for row in ledgers}
        if len(ledger) != len(ledgers):
            raise BronzeError("Duplicate batch ledger keys")
        candidates, sealed = [], set()
        for sid, is_dir in sorted(self.names("handoff").items()):
            if not is_dir or not re.fullmatch("[0-9a-f]{64}", sid):
                raise BronzeError("Unexpected handoff directory")
            prefix = "handoff/" + sid
            definition = bronze_decode(self.read(prefix + "/definition.json"))
            if definition.get("stream_id") != sid:
                raise BronzeError("Stream folder/definition mismatch")
            if "batches" not in self.names(prefix):
                continue
            for bid, batch_dir in sorted(self.names(prefix + "/batches").items()):
                if not batch_dir or not re.fullmatch("[0-9a-f]{64}", bid):
                    raise BronzeError("Unexpected batch directory")
                bp = prefix + "/batches/" + bid
                if "manifest.json" not in self.names(bp):
                    continue
                sealed.add(bid)
                if len(sealed) > 128:
                    raise BronzeError("Sealed batch inventory exceeds 128")
                raw = self.read(bp + "/manifest.json")
                manifest = bronze_decode(raw)
                if (manifest.get("batch_id") != bid or manifest.get("stream_id") != sid
                        or manifest.get("records_relative_path") != bp + "/records.jsonl"):
                    raise BronzeError("Manifest folder/path identity mismatch")
                row = ledger.get(bid)
                if row is None or row["manifest_sha256"] != bronze_sha(raw):
                    raise BronzeError("Run the handoff ledger before Bronze; manifest is missing or changed")
                receipt_raw = self.optional("receipts/bronze/" + bid + ".json")
                if receipt_raw is not None:
                    receipt = bronze_decode(receipt_raw)
                    if (receipt.get("batch_id") != bid or receipt.get("manifest_sha256") != bronze_sha(raw)
                            or receipt.get("processor_revision") != BRONZE_REVISION
                            or receipt.get("state") != "BRONZE_COMMITTED"
                            or receipt.get("row_count") != row["row_count"]
                            or receipt.get("accepted_delivery_count") != row["accepted_delivery_count"]
                            or receipt.get("quarantined_delivery_count") != row["quarantined_delivery_count"]
                            or row["data_stage"] not in ("BRONZE_COMMITTED", "SILVER_COMMITTED")):
                        raise BronzeError("Invalid existing Bronze receipt or ledger state")
                    if not recheck:
                        continue
                candidates.append((manifest, definition, raw))
        if set(ledger) - sealed:
            raise BronzeError("Ledger references missing sealed batch evidence")
        selected, total_bytes = [], 0
        for manifest, definition, raw in candidates[:max_batches]:
            if total_bytes + manifest["data_bytes"] > 16777216 and selected:
                break
            data = self.read(manifest["records_relative_path"])
            total_bytes += len(data)
            selected.append((manifest, data, definition, raw))
        return selected, len(candidates) - len(selected)

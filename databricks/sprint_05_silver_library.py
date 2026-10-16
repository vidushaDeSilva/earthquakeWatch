# Databricks notebook source
"""Normalize verified Bronze observations into recoverable Silver state.

Load sprint_04_bronze_library in a separate cell before this library. Identity
uses explicit USGS aliases; input, history, current state and receipts are proved
independently. No network clients, local data files or deletion are introduced.
"""

from collections import defaultdict
from datetime import date, timedelta
import uuid

if "SparkBronzeStore" not in globals():
    raise RuntimeError("Run sprint_04_bronze_library in a separate cell first")

SILVER_REVISION = "silver-v1"
NORMALIZER_REVISION = 1
SILVER_BOUNDARIES = ("none", "after-aliases", "after-history", "after-quarantine",
                     "after-current", "after-silver-ledger", "after-silver-receipt")
EVENT_COLUMNS = """source_event_id STRING, semantic_payload_sha256 STRING NOT NULL, source_updated_at TIMESTAMP NOT NULL,
    event_time TIMESTAMP, event_date DATE, longitude DOUBLE, latitude DOUBLE, depth_km DOUBLE,
    magnitude DOUBLE, magnitude_type STRING, place STRING, pager_alert STRING, mmi DOUBLE, cdi DOUBLE,
    felt_count BIGINT, significance BIGINT, tsunami_flag INT, review_status STRING, event_type STRING,
    source_url STRING, is_deleted BOOLEAN NOT NULL"""
SILVER_SCHEMAS = {
    "silver.event_aliases": """source_system STRING NOT NULL, alias_event_id STRING NOT NULL,
        event_key STRING NOT NULL, is_preferred BOOLEAN NOT NULL, evidence_source_version_id STRING NOT NULL,
        first_seen_at TIMESTAMP NOT NULL, last_confirmed_at TIMESTAMP NOT NULL""",
    "silver.event_versions": """silver_version_id STRING NOT NULL, event_key STRING NOT NULL,
        source_version_id STRING NOT NULL, normalizer_revision INT NOT NULL,""" + EVENT_COLUMNS + """,
        selection_status STRING NOT NULL, normalized_at TIMESTAMP NOT NULL""",
    "silver.events_current": """event_key STRING NOT NULL, silver_version_id STRING NOT NULL,
        source_version_id STRING NOT NULL, normalizer_revision INT NOT NULL,""" + EVENT_COLUMNS + """,
        first_observed_at TIMESTAMP NOT NULL, last_confirmed_at TIMESTAMP NOT NULL,
        has_unresolved_conflict BOOLEAN NOT NULL""",
}


class SilverError(BronzeError):
    """Reject incomplete Silver evidence without certifying a batch."""


def s56_utc(value):
    """Normalize Spark's naive UTC results and aware Python timestamps for comparison."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    if isinstance(value, list):
        return [s56_utc(item) for item in value]
    if isinstance(value, dict):
        return {key: s56_utc(item) for key, item in value.items()}
    return value


def s56_fact(value):
    """Make timestamps/dates JSON-safe for stable immutable-fact digests."""
    if isinstance(value, datetime):
        return s56_utc(value).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [s56_fact(item) for item in value]
    if isinstance(value, dict):
        return {key: s56_fact(item) for key, item in value.items()}
    return value


def s56_digest(rows, keys):
    """Hash a deterministic row set without depending on Spark collection order."""
    return bronze_sha(bronze_json(s56_fact(sorted(rows, key=lambda r: tuple(str(r[k]) for k in keys)))))


def silver_aliases(feature):
    """Read exact source identifiers; never infer identity from location or event time."""
    preferred = feature["id"]
    supplied = feature["properties"].get("ids")
    if supplied is not None and not isinstance(supplied, str):
        raise SilverError("Malformed explicit alias list")
    tokens = [preferred, *((supplied or "").split(","))]
    aliases = {item.strip() for item in tokens if item.strip()}
    if (len(aliases) > 128 or preferred != preferred.strip()
            or any(not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", item) for item in aliases)):
        raise SilverError("Invalid explicit alias token")
    return sorted(aliases)


def silver_normalize(feature, event_key, source_version_id, now):
    """Type one accepted source state while preserving nullable values and negative depth."""
    if bronze_feature_errors(feature) or bronze_version(feature) != source_version_id:
        raise SilverError("Accepted Bronze feature no longer validates")
    p = feature["properties"]
    event_time = bronze_millis(p.get("time"))
    coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
    semantic = {key: p.get(key) for key in BRONZE_SEMANTIC_FIELDS}
    semantic["coordinates"] = (feature.get("geometry") or {}).get("coordinates")
    row = {"silver_version_id": bronze_id("silver-version-v1", event_key, source_version_id, NORMALIZER_REVISION),
        "event_key": event_key, "source_version_id": source_version_id, "normalizer_revision": NORMALIZER_REVISION,
        "source_event_id": feature["id"], "semantic_payload_sha256": bronze_sha(bronze_json(semantic)),
        "source_updated_at": bronze_millis(p["updated"]),
        "event_time": event_time, "event_date": event_time.date() if event_time else None,
        "longitude": float(coords[0]) if coords[0] is not None else None,
        "latitude": float(coords[1]) if coords[1] is not None else None,
        "depth_km": float(coords[2]) if coords[2] is not None else None,
        "source_url": "https://earthquake.usgs.gov/earthquakes/eventpage/" + feature["id"],
        "is_deleted": p.get("status") == "deleted", "selection_status": "ACCEPTED", "normalized_at": now}
    mapping = {"mag": "magnitude", "magType": "magnitude_type", "place": "place", "alert": "pager_alert",
        "mmi": "mmi", "cdi": "cdi", "felt": "felt_count", "sig": "significance", "tsunami": "tsunami_flag",
        "status": "review_status", "type": "event_type"}
    for original, target in mapping.items():
        value = p.get(original)
        row[target] = float(value) if target in ("magnitude", "mmi", "cdi") and value is not None else value
    return row


def silver_identity_plan(observations, old_aliases):
    """Resolve connected explicit alias sets before any identity writes occur."""
    parents, observed, errors = {}, {}, {}
    for observation in observations:
        did = observation["delivery"]["delivery_id"]
        try:
            aliases = silver_aliases(observation["feature"])
        except SilverError:
            errors[did] = "INVALID_ALIAS"
            continue
        observed[did] = aliases
        for alias in aliases:
            parents.setdefault(alias, alias)

    def root(alias):
        """Find and compress an explicit-identity component."""
        while parents[alias] != alias:
            parents[alias] = parents[parents[alias]]
            alias = parents[alias]
        return alias

    for aliases in observed.values():
        for alias in aliases[1:]:
            parents[root(alias)] = root(aliases[0])
    components = defaultdict(list)
    for alias in parents:
        components[root(alias)].append(alias)
    known = {row["alias_event_id"]: row for row in old_aliases}
    targets, affected = {}, set()
    for aliases in components.values():
        existing = {known[a]["event_key"] for a in aliases if a in known}
        if len(existing) > 1:
            affected.update(existing)
            for did, evidence in observed.items():
                if evidence[0] in aliases:
                    errors[did] = "ALIAS_COLLISION"
            continue
        group = [o for o in observations if o["delivery"]["delivery_id"] in observed
                 and observed[o["delivery"]["delivery_id"]][0] in aliases]
        first = min(group, key=lambda o: (o["observed_at"], o["delivery"]["capture_id"],
                                         o["delivery"]["source_item_index"], o["feature"]["id"]))
        key = next(iter(existing)) if existing else bronze_id("event-key-v1", "usgs", first["feature"]["id"])
        for alias in aliases:
            targets[alias] = key
    return observed, targets, errors, affected


class SilverEngine:
    """Apply deterministic Silver plans with repair after any individual table commit."""

    def __init__(self, store, run_id, fail_at="none"):
        """Bind a serialized writer and optional development interruption."""
        if fail_at not in SILVER_BOUNDARIES:
            raise SilverError("Unknown Silver fault boundary")
        self.store, self.run_id, self.fail_at = store, run_id, fail_at

    def process(self, batches):
        """Prove input, persist aliases/history/current, then certify completed batches."""
        self.store.silver_sources = BronzeCaptureCache(self.store)
        self.store.silver_input_bytes = 0
        observations, batch_proofs = [], []
        for batch in batches:
            items, proof = self.store.silver_input(batch)
            observations.extend(items)
            batch_proofs.append(proof)
        if len(observations) > 10000:
            raise SilverError("Silver observation budget exceeded")
        observations = list({o["delivery"]["delivery_id"]: o for o in observations}.values())
        all_aliases = self.store.rows("silver.event_aliases", 20000)
        evidence, targets, errors, affected = silver_identity_plan(observations, all_aliases)
        old_aliases = {r["alias_event_id"]: r for r in all_aliases}
        aliases, versions, seen = {}, {}, defaultdict(list)
        now = self.store.now()
        for observation in observations:
            delivery, feature, at = observation["delivery"], observation["feature"], observation["observed_at"]
            did = delivery["delivery_id"]
            if did in errors:
                continue
            event_key = targets[evidence[did][0]]
            row = silver_normalize(feature, event_key, delivery["source_version_id"], now)
            versions[row["silver_version_id"]] = row
            seen[event_key].append(at)
            for alias in evidence[did]:
                old = aliases.get(alias) or old_aliases.get(alias)
                candidate = {"source_system": "usgs", "alias_event_id": alias, "event_key": event_key,
                    "is_preferred": alias == feature["id"], "evidence_source_version_id": delivery["source_version_id"],
                    "first_seen_at": min(at, old["first_seen_at"]) if old else at,
                    "last_confirmed_at": max(at, old["last_confirmed_at"]) if old else at}
                # Replaying old evidence must not change the latest preferred-ID observation.
                if old and at <= old["last_confirmed_at"]:
                    candidate.update(is_preferred=old["is_preferred"], evidence_source_version_id=old["evidence_source_version_id"])
                aliases[alias] = candidate
        self.store.merge_rows("silver.event_aliases", list(aliases.values()), ("source_system", "alias_event_id"),
            ("event_key",), ("is_preferred", "evidence_source_version_id", "first_seen_at", "last_confirmed_at"))
        bronze_fault(self.fail_at, "after-aliases")
        keys = sorted(set(seen) | affected)
        history = {r["silver_version_id"]: r for r in self.store.event_history(keys)}
        for key, row in versions.items():
            if key in history:
                row["normalized_at"] = history[key]["normalized_at"]
            history[key] = row
        by_event, conflicts = defaultdict(list), set()
        for row in history.values():
            by_event[row["event_key"]].append(row)
        for event_key, states in by_event.items():
            timestamps = defaultdict(set)
            for row in states:
                timestamps[row["source_updated_at"]].add(row["semantic_payload_sha256"])
            ambiguous = {at for at, ids in timestamps.items() if len(ids) > 1}
            if ambiguous:
                conflicts.add(event_key)
            for row in states:
                if row["source_updated_at"] in ambiguous:
                    row["selection_status"] = "CONFLICT"
        version_rows = list(history.values())
        immutable = tuple(k for k in next(iter(version_rows), {}) if k not in ("normalized_at", "selection_status"))
        self.store.merge_rows("silver.event_versions", version_rows, ("silver_version_id",), immutable, ("selection_status",))
        bronze_fault(self.fail_at, "after-history")
        rejects = []
        for observation in observations:
            delivery = observation["delivery"]
            did = delivery["delivery_id"]
            key = targets.get(evidence.get(did, [None])[0])
            reason = errors.get(did) or ("SAME_TIME_CONFLICT" if key in conflicts else None)
            if reason:
                rejects.append({"quarantine_id": bronze_id("silver-quarantine-v1", did, NORMALIZER_REVISION),
                    "delivery_id": did, "batch_id": delivery["batch_id"], "stage_name": "SILVER",
                    "contract_or_normalizer_revision": str(NORMALIZER_REVISION), "error_codes": [reason],
                    "raw_value_b64": None, "raw_reference_uri": observation["response_uri"],
                    "rejected_at": now, "resolution_status": "OPEN", "resolved_at": None, "replay_run_id": None})
        self.store.merge_rows("bronze.quarantine", rejects, ("quarantine_id",),
            ("delivery_id", "error_codes", "raw_reference_uri"), ())
        bronze_fault(self.fail_at, "after-quarantine")
        old_current = {r["event_key"]: r for r in self.store.current_rows(keys)}
        current = []
        for key in keys:
            old = old_current.get(key)
            candidates = [r for r in by_event.get(key, []) if r["selection_status"] == "ACCEPTED"]
            # The ID tie-break applies only to equivalent semantic payloads, never conflicting facts.
            candidate = max(candidates, key=lambda r: (r["source_updated_at"], r["normalizer_revision"],
                                                       r["source_event_id"])) if candidates else None
            if old and (candidate is None or old["source_updated_at"] > candidate["source_updated_at"]):
                candidate = old
            if candidate is None:
                continue
            row = {k: v for k, v in candidate.items() if k not in ("selection_status", "normalized_at")}
            times = seen.get(key, [])
            if old:
                times += [old["first_observed_at"], old["last_confirmed_at"]]
            row.update(first_observed_at=min(times), last_confirmed_at=max(times),
                has_unresolved_conflict=key in conflicts or key in affected or bool(old and old["has_unresolved_conflict"]))
            if old and row["source_updated_at"] < old["source_updated_at"]:
                raise SilverError("Current source revision would regress")
            current.append(row)
        self.store.merge_rows("silver.events_current", current, ("event_key",), (),
            tuple(k for k in next(iter(current), {}) if k != "event_key"))
        bronze_fault(self.fail_at, "after-current")
        for name, rows, keys_for_table in (("silver.event_aliases", list(aliases.values()), ("source_system", "alias_event_id")),
                ("silver.event_versions", version_rows, ("silver_version_id",)),
                ("silver.events_current", current, ("event_key",))):
            self.store.prove_rows(name, rows, keys_for_table, tuple(k for k in next(iter(rows), {}) if k != "normalized_at"))
        if errors or conflicts or affected:
            self.store.stage_check(self.run_id, "SILVER", "identity_and_revision", "FAIL", len(errors) + len(conflicts), "0")
            raise SilverError("Unresolved Silver identity/revision conflict; batches remain uncertified")
        for proof in batch_proofs:
            self.store.ack_silver(proof["batch_id"])
            bronze_fault(self.fail_at, "after-silver-ledger")
            self.store.put_receipt("receipts/silver/" + proof["batch_id"] + ".json", bronze_json(proof))
            bronze_fault(self.fail_at, "after-silver-receipt")
        self.store.stage_check(self.run_id, "SILVER", "silver_completed", "PASS", len(batch_proofs), str(len(batch_proofs)))
        return {"processed_batches": len(batch_proofs), "observations": len(observations)}


class SparkSilverStore(SparkBronzeStore):
    """Extend the tested bounded cloud adapter with explicit Silver schemas and checks."""

    def table(self, name):
        """Permit only project-owned layer tables."""
        if name in SILVER_SCHEMAS:
            return self.spark.table("earthquake_dev." + name)
        return super().table(name)

    def now(self):
        """Return an aware processing clock, never source freshness."""
        return datetime.now(timezone.utc)

    def rows(self, name, limit=10000):
        """Fail visibly instead of silently truncating any table inventory."""
        result = self.table(name).limit(limit + 1).collect()
        if len(result) > limit:
            raise SilverError("Table inventory exceeds development bound: " + name)
        return [s56_utc(row.asDict(recursive=True)) for row in result]

    def lookup(self, name, key, values):
        """Normalize collected timestamps from the inherited bounded key lookup."""
        return [s56_utc(row) for row in super().lookup(name, key, values)]

    def merge_rows(self, name, rows, keys, immutable, mutable):
        """Pre-deduplicate and verify immutable facts before one atomic table MERGE."""
        if not rows:
            return
        fields = set(self.table(name).columns)
        if any(set(row) != fields for row in rows) or not set(keys + immutable + mutable) <= fields:
            raise SilverError("Unexpected explicit schema: " + name)
        keyed = {}
        for row in rows:
            key = tuple(row[k] for k in keys)
            if key in keyed and any(keyed[key][f] != row[f] for f in immutable + mutable):
                raise SilverError("Conflicting duplicate MERGE source")
            keyed[key] = row
        rows = list(keyed.values())
        from pyspark.sql import functions as F
        collected = self.table(name).where(F.col(keys[0]).isin(list({r[keys[0]] for r in rows}))).limit(20001).collect()
        if len(collected) > 20000:
            raise SilverError("MERGE target scope exceeds development bound")
        old_rows = [s56_utc(r.asDict(recursive=True)) for r in collected]
        old = {tuple(r[k] for k in keys): r for r in old_rows}
        if len(old) != len(old_rows):
            raise SilverError("Duplicate target logical key")
        for row in rows:
            previous = old.get(tuple(row[k] for k in keys))
            if previous and any(previous[f] != row[f] for f in immutable):
                raise SilverError("Immutable Silver/Gold fact conflict: " + name)
        self.frame(name, rows).createOrReplaceTempView("s56_merge_input")
        condition = " AND ".join(f"t.{k}=s.{k}" for k in keys)
        update = "WHEN MATCHED THEN UPDATE SET " + ",".join(f"t.{k}=s.{k}" for k in mutable) if mutable else ""
        self.spark.sql(f"MERGE INTO earthquake_dev.{name} t USING s56_merge_input s ON {condition} "
                       f"{update} WHEN NOT MATCHED THEN INSERT *")

    def prove_rows(self, name, rows, keys, compare):
        """Reconcile every planned key after commit, tolerating no duplicate target rows."""
        if not rows:
            return
        # The first key can be publication_id: multiple rows with that key are legitimate.
        from pyspark.sql import functions as F
        actual = self.table(name).where(F.col(keys[0]).isin(list({r[keys[0]] for r in rows}))).limit(20001).collect()
        if len(actual) > 20000:
            raise SilverError("Post-write proof exceeds bound")
        actual = [s56_utc(r.asDict(recursive=True)) for r in actual]
        indexed = {tuple(r[k] for k in keys): r for r in actual}
        if len(indexed) != len(actual):
            raise SilverError("Duplicate persisted logical key")
        for row in rows:
            other = indexed.get(tuple(row[k] for k in keys))
            if other is None or any(other[f] != row[f] for f in compare):
                raise SilverError("Post-write Silver/Gold reconciliation failed: " + name)

    def event_history(self, keys):
        """Read bounded version history for only the affected event identities."""
        from pyspark.sql import functions as F
        if not keys:
            return []
        result = self.table("silver.event_versions").where(F.col("event_key").isin(keys)).limit(20001).collect()
        if len(result) > 20000:
            raise SilverError("Affected history exceeds 20000 revisions")
        return [s56_utc(r.asDict(recursive=True)) for r in result]

    def current_rows(self, keys):
        """Read unique current rows without scanning unrelated events."""
        return self.lookup("silver.events_current", "event_key", keys)

    def silver_batches(self, recheck=False, maximum=32):
        """Include missing-receipt repairs even after the ledger advanced to Silver."""
        all_batches = self.rows("ops.batches", 10000)
        pending = []
        for batch in sorted(all_batches, key=lambda b: (b["landed_at"], b["batch_id"])):
            if batch["data_stage"] == "LANDED":
                raise SilverError("Run Bronze before Silver; LANDED batches remain")
            if batch["data_stage"] not in ("BRONZE_COMMITTED", "SILVER_COMMITTED"):
                raise SilverError("Unknown input data stage")
            retired = self.optional("receipts/retired/batches/" + batch["batch_id"] + ".json")
            if retired is not None and recheck:
                raise SilverError("Landing recheck expired; use Bronze replay into a recovery namespace")
            raw = self.optional("receipts/silver/" + batch["batch_id"] + ".json")
            if raw is not None:
                receipt = bronze_decode(raw)
                if (receipt.get("batch_id") != batch["batch_id"] or receipt.get("state") != "SILVER_COMMITTED"
                        or receipt.get("manifest_sha256") != batch["manifest_sha256"]
                        or receipt.get("normalizer_revision") != NORMALIZER_REVISION
                        or batch["data_stage"] != "SILVER_COMMITTED"):
                    raise SilverError("Conflicting Silver completion receipt")
                if not recheck:
                    continue
            pending.append(batch)
        return pending[:maximum], max(0, len(pending) - maximum)

    def silver_input(self, batch):
        """Reopen sealed landing and exact source observations, including later alias variants."""
        bid = batch["batch_id"]
        uri = batch["manifest_uri"]
        if not uri.startswith(BRONZE_ROOT + "/"):
            raise SilverError("Input manifest outside development Volume")
        path = uri[len(BRONZE_ROOT) + 1:]
        raw = self.read(path)
        if bronze_sha(raw) != batch["manifest_sha256"]:
            raise SilverError("Input manifest checksum changed")
        manifest = bronze_decode(raw)
        sid = manifest["stream_id"]
        definition = bronze_decode(self.read("handoff/" + sid + "/definition.json"))
        data = self.read(manifest["records_relative_path"])
        self.silver_input_bytes = getattr(self, "silver_input_bytes", 0) + len(data)
        if self.silver_input_bytes > 16777216:
            raise SilverError("Silver input exceeds 16 MiB; lower silver_max_batches")
        records = bronze_verify_batch(manifest, data, definition)
        if manifest["batch_id"] != bid or len(records) != batch["row_count"]:
            raise SilverError("Input batch identity/count mismatch")
        bronze_receipt = self.optional("receipts/bronze/" + bid + ".json")
        if bronze_receipt is None:
            raise SilverError("Missing Bronze receipt; run Bronze repair first")
        receipt = bronze_decode(bronze_receipt)
        for field, expected in {"batch_id": bid, "state": "BRONZE_COMMITTED", "processor_revision": BRONZE_REVISION,
                "manifest_sha256": batch["manifest_sha256"], "row_count": batch["row_count"],
                "accepted_delivery_count": batch["accepted_delivery_count"],
                "quarantined_delivery_count": batch["quarantined_delivery_count"]}.items():
            if receipt.get(field) != expected:
                raise SilverError("Conflicting Bronze receipt")
        existing = {r["delivery_id"]: r for r in self.lookup("bronze.deliveries", "delivery_id", [r["delivery_id"] for r in records])}
        sources = getattr(self, "silver_sources", None)
        if sources is None:
            self.silver_sources = sources = BronzeCaptureCache(self)
        result, accepted, versions, quarantines = [], 0, [], []
        for record in records:
            delivery, version, quarantine, _ = bronze_classify(record, manifest, sources)
            old = existing.get(delivery["delivery_id"])
            fields = ("record_sha256", "capture_id", "source_item_id", "source_item_index",
                      "source_version_id", "raw_feature_sha256", "disposition")
            if old is None or any(old[f] != delivery[f] for f in fields):
                raise SilverError("Persisted Bronze delivery proof differs")
            if version:
                versions.append(version)
                capture, features = sources.get(delivery["capture_id"])
                result.append({"delivery": old, "feature": features[delivery["source_item_index"]],
                    "observed_at": bronze_time(capture["fetched_at"]),
                    "response_uri": BRONZE_ROOT + "/" + capture["response_relative_path"]})
                accepted += 1
            else:
                quarantines.append(quarantine)
        # Reconcile entire bounded sets; avoid one Spark action per earthquake.
        self.prove("bronze.event_versions", versions, ("source_event_id", "source_updated_ms", "payload_sha256"))
        self.prove("bronze.quarantine", quarantines, ("delivery_id", "error_codes"))
        if accepted != batch["accepted_delivery_count"] or len(records) - accepted != batch["quarantined_delivery_count"]:
            raise SilverError("Bronze batch disposition counts differ")
        proof = {"receipt_version": 1, "processor_revision": SILVER_REVISION, "normalizer_revision": NORMALIZER_REVISION,
            "batch_id": bid, "manifest_sha256": batch["manifest_sha256"], "row_count": len(records),
            "accepted_delivery_count": accepted, "quarantined_delivery_count": len(records) - accepted,
            "accepted_delivery_set_sha256": bronze_sha(bronze_json(sorted(o["delivery"]["delivery_id"] for o in result))),
            "state": "SILVER_COMMITTED"}
        return result, proof

    def ack_silver(self, batch_id):
        """Advance exactly one certified batch while preserving its original completion time."""
        self.spark.createDataFrame([(batch_id,)], "batch_id STRING").createOrReplaceTempView("s5_ack")
        self.spark.sql("""MERGE INTO earthquake_dev.ops.batches t USING s5_ack s ON t.batch_id=s.batch_id
            WHEN MATCHED AND t.data_stage='BRONZE_COMMITTED' THEN UPDATE SET
            t.data_stage='SILVER_COMMITTED', t.silver_committed_at=current_timestamp(), t.last_attempt_status='SUCCESS'""")
        if self.lookup("ops.batches", "batch_id", [batch_id])[0]["data_stage"] != "SILVER_COMMITTED":
            raise SilverError("Silver ledger advancement was not confirmed")

    def stage_check(self, run_id, stage, rule, status, observed, expected, details=None):
        """Write stage-specific quality evidence without raw features or connection details."""
        revision = SILVER_REVISION if stage == "SILVER" else "gold-v1"
        row = {"check_id": bronze_id("check-v1", run_id, stage, rule, revision), "run_id": run_id,
            "scope_type": "STAGE", "scope_id": stage, "rule_id": rule, "rule_version": revision,
            "severity": "ERROR", "status": status, "observed_value": float(observed), "expected_value": expected,
            "evaluated_at": self.now(), "resolved_at": None, "details_json": bronze_json(details).decode() if details else None}
        self.upsert_ops("ops.check_results", [row], ("status", "observed_value", "evaluated_at", "details_json"))


def s56_attempt(store, stage, operation):
    """Persist run/attempt lifecycle for a bounded operation and sanitize terminal errors."""
    now = store.now()
    run_id = bronze_id("s56-run-v1", stage, uuid.uuid4().hex)
    run = {"run_id": run_id, "run_kind": "RECOVERY", "github_run_id": None, "github_attempt": None,
        "databricks_run_id": None, "window_start": now, "window_end": now, "started_at": now, "finished_at": None,
        "status": "RUNNING", "code_commit": "sprint-05-06-v1", "contract_version": BRONZE_CONTRACT, "error_class": None}
    attempt = {"attempt_id": bronze_id("s56-attempt-v1", run_id), "run_id": run_id, "scope_type": "STAGE",
        "scope_id": stage, "stage_name": stage, "attempt_number": 1, "status": "RUNNING", "started_at": now,
        "finished_at": None, "retryable": False, "http_status": None, "error_code": None,
        "sanitized_message": None, "metrics": {}}
    store.upsert_ops("ops.runs", [run], ("status", "finished_at", "error_class"))
    store.upsert_ops("ops.stage_attempts", [attempt], ("status", "finished_at", "error_code", "metrics"))
    try:
        metrics = operation(run_id)
        run.update(status="SUCCESS", finished_at=store.now())
        attempt.update(status="SUCCESS", finished_at=run["finished_at"], metrics={k: str(v) for k, v in metrics.items()})
        print("PASS Sprint " + ("5 Silver" if stage == "SILVER" else "6 Gold") + " run_id=" + run_id)
        return metrics
    except Exception as exc:
        run.update(status="FAILED", finished_at=store.now(), error_class=type(exc).__name__)
        attempt.update(status="FAILED", finished_at=run["finished_at"], error_code=type(exc).__name__)
        safe_message = str(exc) if isinstance(exc, BronzeError) else "External operation failed; inspect run/check metadata"
        attempt["sanitized_message"] = safe_message
        try:
            store.stage_check(run_id, stage, "stage_completed", "FAIL", 0, "1")
        except Exception:
            print("DIAGNOSTIC_WRITE_FAILED")
        raise SilverError(stage + " failed; " + safe_message + "; run_id=" + run_id) from None
    finally:
        store.upsert_ops("ops.runs", [run], ("status", "finished_at", "error_class"))
        store.upsert_ops("ops.stage_attempts", [attempt], ("status", "finished_at", "error_code", "sanitized_message", "metrics"))

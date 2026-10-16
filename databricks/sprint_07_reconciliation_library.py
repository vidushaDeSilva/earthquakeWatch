# Databricks notebook source
"""Verify source recovery, export bounded event checks and certify reconciliation.

Load the Bronze, Silver and Gold libraries in separate cells first. This library
uses only managed Volume evidence and Delta tables; external HTTP stays in Actions.
"""

if "SparkGoldStore" not in globals():
    raise RuntimeError("Load Bronze, Silver and Gold libraries before Sprint 7")

S78_SCHEMAS = {
    "ops.source_checks": """request_id STRING NOT NULL, kind STRING NOT NULL, requested_event_id STRING,
        capture_id STRING, outcome STRING NOT NULL, fetched_at TIMESTAMP NOT NULL,
        window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL, http_status INT NOT NULL,
        original_sha256 STRING NOT NULL, report_uri STRING NOT NULL""",
    "ops.reconciliations": """reconciliation_id STRING NOT NULL, run_id STRING NOT NULL,
        checked_at TIMESTAMP NOT NULL, status STRING NOT NULL, issues ARRAY<STRING> NOT NULL,
        silver_table_version BIGINT NOT NULL, checked_events BIGINT NOT NULL,
        pending_captures BIGINT NOT NULL, historical_dates ARRAY<STRING> NOT NULL""",
    "ops.recovery_snapshots": """snapshot_id STRING NOT NULL, snapshot_date DATE NOT NULL,
        publication_id STRING NOT NULL, created_at TIMESTAMP NOT NULL,
        manifest_sha256 STRING NOT NULL, manifest_uri STRING NOT NULL""",
    "ops.maintenance_plans": """plan_id STRING NOT NULL, created_at TIMESTAMP NOT NULL,
        action_count BIGINT NOT NULL, manifest_sha256 STRING NOT NULL, manifest_uri STRING NOT NULL""",
    "ops.maintenance_actions": """action_id STRING NOT NULL, plan_id STRING NOT NULL,
        kind STRING NOT NULL, scope_id STRING NOT NULL, status STRING NOT NULL,
        started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_class STRING""",
    "ops.data_holds": """hold_id STRING NOT NULL, scope_type STRING NOT NULL, scope_id STRING NOT NULL,
        reason STRING NOT NULL, created_at TIMESTAMP NOT NULL, expires_at TIMESTAMP, active BOOLEAN NOT NULL""",
    "ops.rollback_actions": """action_id STRING NOT NULL, from_publication_id STRING NOT NULL,
        target_publication_id STRING NOT NULL, expected_generation BIGINT NOT NULL,
        reason STRING NOT NULL, state STRING NOT NULL, created_at TIMESTAMP NOT NULL,
        finished_at TIMESTAMP""",
}


class RecoveryError(GoldError):
    """Reject uncertain source coverage or lifecycle evidence without changing serving."""


def s78_id(value):
    """Require a digest identity before constructing a cloud path or SQL predicate."""
    if not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value):
        raise RecoveryError("Expected a SHA-256 identity")
    return value


def s78_receipt(raw, fields):
    """Require exact receipt fields; a missing receipt never counts as completion."""
    if raw is None:
        raise RecoveryError("Required completion receipt is missing")
    value = bronze_decode(raw)
    if not isinstance(value, dict) or any(value.get(k) != v for k, v in fields.items()):
        raise RecoveryError("Completion receipt disagrees with durable evidence")
    return value


def s78_candidates(events, previous, thresholds):
    """Track current eligible and previously served recent IDs, without fuzzy identity matching."""
    now, ids = previous["now"], set()
    for e in events:
        if (not e["is_deleted"] and e["event_type"] == "earthquake" and e["event_time"] is not None
                and now - timedelta(days=30) <= e["event_time"] < now
                and gold_reasons(e, thresholds)[0]):
            ids.add(e["source_event_id"])
    ids.update(previous.get("event_ids", []))
    if len(ids) > 64:
        raise RecoveryError("Tracked-event scope exceeds 64; continue with a paged policy before publishing")
    if any(not re.fullmatch("[A-Za-z0-9_-]{1,80}", value) for value in ids):
        raise RecoveryError("Unsupported USGS event identifier")
    return sorted(ids)


class SparkRecoveryStore(SparkGoldStore):
    """Add allowlisted operations tables and verified source-request evidence."""

    def table(self, name):
        """Restrict dynamic identifiers to explicitly declared lifecycle tables."""
        if name in S78_SCHEMAS:
            return self.spark.table("earthquake_dev." + name)
        return super().table(name)

    def directory(self, relative):
        """Return immediate directory entries only inside the established Volume."""
        return self.utils.fs.ls(self.path(relative))

    def optional_limit(self, relative, limit=16777216):
        """Distinguish absence from an oversized valid source response without a four-MiB fallback."""
        parent, name = relative.rsplit("/", 1)
        return self.read(relative, limit) if name in self.names(parent) else None

    def sync_captures(self):
        """Import all selected captures while recognizing receipted intentional raw expiry."""
        from pyspark.sql import functions as F
        selections = self.spark.read.option("mode", "FAILFAST").json(self.root + "/collections/*/selected.json")
        manifests = self.spark.read.option("mode", "FAILFAST").json(self.root + "/captures/*/*/ready.json")
        selected = selections.select("collection_id", "capture_id").join(manifests, ["collection_id", "capture_id"])
        if selected.count() != selections.count():
            raise RecoveryError("A selected capture lacks its ready manifest")
        rows = selected.limit(10001).collect()
        if len(rows) > 10000:
            raise RecoveryError("Selected capture metadata inventory exceeds 10000")
        old = {r["capture_id"]: r for r in self.rows("ops.captures", 10000)}
        output = []
        for r in rows:
            m = r.asDict(recursive=True)
            cid, collection = s78_id(m["capture_id"]), s78_id(m["collection_id"])
            prefix = f"captures/{collection}/{cid}"
            if (m["environment"] != "dev" or m["contract_version"] != BRONZE_CONTRACT
                    or m["response_relative_path"] != prefix + "/response.json"
                    or cid != bronze_id("capture-v1", collection, m["response_sha256"])):
                raise RecoveryError("Selected capture identity is invalid")
            known = old.get(cid)
            if known:
                if any(known[k] != m[k] for k in ("collection_id", "source_id", "response_sha256", "expected_item_count")):
                    raise RecoveryError("Existing capture ledger differs from selected evidence")
                retired = self.optional("receipts/retired/captures/" + cid + ".json")
                if retired is not None:
                    self.verify_retired_capture(known, retired)
                continue  # New inputs get full byte verification; recovery reopens pending evidence.
            payload = self.read(prefix + "/response.json", 16777216)
            feed = bronze_decode(payload)
            if (bronze_sha(payload) != m["response_sha256"] or len(payload) != m["response_bytes"]
                    or feed.get("type") != "FeatureCollection" or len(feed["features"]) != m["expected_item_count"]
                    or feed["metadata"]["count"] != len(feed["features"]) or feed["metadata"]["status"] != 200):
                raise RecoveryError("New source capture bytes/count disagree")
            output.append({"capture_id": cid, "collection_id": collection, "source_id": m["source_id"],
                "source_revision": int(m["source_revision"]), "run_id": m["first_run_id"],
                "response_uri": self.root + "/" + prefix + "/response.json",
                "manifest_uri": self.root + "/" + prefix + "/ready.json", "response_sha256": m["response_sha256"],
                "fetched_at": bronze_time(m["fetched_at"]), "source_generated_at": bronze_time(m["source_generated_at"]),
                "expected_item_count": int(m["expected_item_count"]), "accounted_item_count": 0,
                "dispatch_chunk_count": 0, "state": "CAPTURED", "accounted_at": None})
        self.merge_rows("ops.captures", output, ("capture_id",),
            ("collection_id", "source_id", "response_sha256", "expected_item_count"), ())
        return len(output)

    def verify_retired_capture(self, capture, raw):
        """Validate retirement against the retained capture accounting receipt."""
        cid = capture["capture_id"]
        receipt = self.optional("receipts/captures/" + cid + ".json")
        s78_receipt(receipt, {"capture_id": cid, "state": "ACCOUNTED",
            "response_sha256": capture["response_sha256"], "expected_item_count": capture["expected_item_count"],
            "accounted_item_count": capture["expected_item_count"]})
        s78_receipt(raw, {"capture_id": cid, "state": "RETIRED", "response_sha256": capture["response_sha256"],
            "capture_receipt_sha256": bronze_sha(receipt)})

    def request_reports(self):
        """Independently verify exact external response bytes and the source wrapper mapping."""
        reports = []
        for rid, is_dir in sorted(self.names("reconciliation/requests").items()):
            if not is_dir:
                raise RecoveryError("Unexpected catalog-request file")
            s78_id(rid)
            rp = "reconciliation/requests/" + rid
            raw = self.optional(rp + "/ready.json")
            if raw is None:
                continue
            report = bronze_decode(raw)
            if (report.get("request_id") != rid or report.get("kind") not in ("EVENT_CHECK", "BACKFILL")
                    or report.get("original_relative_path") != rp + "/original.json"
                    or not report["url"].startswith("https://earthquake.usgs.gov/fdsnws/event/1/query?")
                    or rid != bronze_id("catalog-request-v1", report["kind"], report["epoch"], report["url"])):
                raise RecoveryError("Invalid catalog request identity")
            retired = self.optional("receipts/retired/captures/" + report["capture_id"] + ".json") if report["capture_id"] else None
            original = None if retired is not None else self.read(rp + "/original.json", 16777216)
            if original is not None and bronze_sha(original) != report["original_sha256"]:
                raise RecoveryError("Exact original catalog response hash failed")
            if report["capture_id"]:
                cid = s78_id(report["capture_id"])
                capture = self.lookup("ops.captures", "capture_id", [cid])
                if len(capture) != 1:
                    raise RecoveryError("Source check capture has not been imported")
                retired = self.optional("receipts/retired/captures/" + cid + ".json")
                if retired is not None:
                    self.verify_retired_capture(capture[0], retired)
                else:
                    m, features = BronzeCaptureCache(self).get(cid)
                    if m.get("request_id") != rid or m["source_url"] != report["url"]:
                        raise RecoveryError("Source wrapper is not bound to its catalog request")
                    original_json = bronze_decode(original) if original else None
                    expected = [original_json] if original_json and original_json.get("type") == "Feature" else (
                        original_json.get("features", []) if original_json else [])
                    if bronze_json(expected) != bronze_json(features):
                        raise RecoveryError("Catalog wrapper differs from exact original event features")
                    if report["kind"] == "EVENT_CHECK":
                        feature = features[0] if len(features) == 1 else {}
                        aliases = set(silver_aliases(feature)) if feature else set()
                        if report["requested_event_id"] not in aliases:
                            raise RecoveryError("Catalog event does not explicitly link the requested ID")
                    elif (report["count_before"] != len(features) or report["count_after"] != len(features)):
                        raise RecoveryError("Catalog count proofs differ from captured slice")
            reports.append(report)
            if len(reports) > 10000:
                raise RecoveryError("Catalog request metadata inventory exceeds 10000")
        return reports

    def event_check_plan(self):
        """Freeze current/previous candidate IDs so the runner can request bounded updates."""
        policies = self.lookup("ref.impact_policies", "policy_version", ["impact-v1"])
        if len(policies) != 1:
            raise RecoveryError("Missing impact-v1 policy")
        _, thresholds = gold_policy(policies[0])
        events = self.rows("silver.events_current", 10000)
        pointer = self.pointer()
        previous_ids = []
        if pointer["publication_id"]:
            previous_ids = [r["source_event_id"] for r in self.rows("gold.watchlist_snapshots", 100000)
                            if r["publication_id"] == pointer["publication_id"]]
        ids = s78_candidates(events, {"now": self.now(), "event_ids": previous_ids}, thresholds)
        at, version = self.now(), self.table_version("silver.events_current")
        plan_id = bronze_id("event-check-plan-v1", version, s56_fact(at), ids)
        plan = {"plan_id": plan_id, "created_at": s56_fact(at), "silver_table_version": version, "event_ids": ids}
        self.put_receipt("reconciliation/plans/" + plan_id + "/plan.json", bronze_json(plan))
        return plan

    def reconcile(self, run_id, mode="verify"):
        """Prove recent feed observations and tracked updates, keeping unresolved gaps visible."""
        if mode not in ("plan", "verify"):
            raise RecoveryError("reconcile_mode must be plan or verify")
        plan = self.event_check_plan()
        if mode == "plan":
            return {"planned_events": len(plan["event_ids"]), "mode": mode}
        reports = self.request_reports()
        capture_rows = self.rows("ops.captures", 10000)
        captures = {r["capture_id"]: r for r in capture_rows}
        issues, historical, intervals = [], set(), []
        check_rows = []
        for r in reports:
            captured = captures.get(r["capture_id"])
            outcome = "ACCOUNTED" if captured and captured["state"] == "ACCOUNTED" else r["outcome"]
            check_rows.append({"request_id": r["request_id"], "kind": r["kind"],
                "requested_event_id": r["requested_event_id"], "capture_id": r["capture_id"], "outcome": outcome,
                "fetched_at": bronze_time(r["fetched_at"]), "window_start": bronze_time(r["window_start"]),
                "window_end": bronze_time(r["window_end"]), "http_status": r["http_status"],
                "original_sha256": r["original_sha256"], "report_uri": self.root + "/reconciliation/requests/" + r["request_id"] + "/ready.json"})
            if r["kind"] == "BACKFILL" and outcome == "ACCOUNTED":
                first, end = bronze_time(r["window_start"]), bronze_time(r["window_end"])
                intervals.append((first, end))
        if intervals:
            first_day = min(a for a, _ in intervals).replace(hour=0, minute=0, second=0, microsecond=0)
            last_day = max(b for _, b in intervals)
            while first_day < last_day:
                stop, cursor = first_day + timedelta(days=1), first_day
                for a, b in sorted(intervals):
                    if b <= cursor or a >= stop:
                        continue
                    if a > cursor:
                        break
                    cursor = max(cursor, min(b, stop))
                if cursor == stop:
                    historical.add(first_day.date().isoformat())
                elif any(a < stop and b > first_day for a, b in intervals):
                    issues.append("PARTIAL_DAY_BACKFILL:" + first_day.date().isoformat())
                first_day = stop
        for gid, is_dir in self.names("reconciliation/gaps").items():
            if not is_dir:
                raise RecoveryError("Invalid Kafka gap entry")
            gap_raw = self.read("reconciliation/gaps/" + s78_id(gid) + "/plan.json")
            gap = bronze_decode(gap_raw)
            receipt = self.optional("reconciliation/gaps/" + gid + "/ready.json")
            if (bronze_id("offset-gap-v1", gap) != gid or receipt is None
                    or bronze_decode(receipt).get("plan_sha256") != bronze_sha(gap_raw)
                    or any(captures.get(cid, {}).get("state") != "ACCOUNTED" for cid in gap["capture_ids"])):
                issues.append("UNRESOLVED_KAFKA_GAP:" + gid)
        self.merge_rows("ops.source_checks", check_rows, ("request_id",),
            tuple(k for k in next(iter(check_rows), {}) if k != "outcome"), ("outcome",))
        latest_checks = {}
        for row in check_rows:
            if row["kind"] == "EVENT_CHECK":
                old = latest_checks.get(row["requested_event_id"])
                if old is None or (row["fetched_at"], row["outcome"] != "ACCOUNTED") > (old["fetched_at"], old["outcome"] != "ACCOUNTED"):
                    latest_checks[row["requested_event_id"]] = row
        for event_id in plan["event_ids"]:
            row = latest_checks.get(event_id)
            if (row is None or row["outcome"] != "ACCOUNTED"
                    or self.now() - row["fetched_at"] > timedelta(hours=24)):
                issues.append("TRACKED_EVENT_UNVERIFIED:" + event_id)
        sources = BronzeCaptureCache(self)
        for source_id in ("usgs-m2p5-day", "usgs-m2p5-month", "usgs-significant-month"):
            candidates = [c for c in capture_rows if c["source_id"] == source_id and c["state"] == "ACCOUNTED"]
            if not candidates:
                issues.append("MISSING_RECONCILE_SOURCE:" + source_id)
                continue
            latest = max(candidates, key=lambda c: (c["fetched_at"], c["capture_id"]))
            if self.optional("receipts/retired/captures/" + latest["capture_id"] + ".json") is not None:
                issues.append("SOURCE_EVIDENCE_EXPIRED:" + source_id)
                continue
            _, features = sources.get(latest["capture_id"])
            expected_versions = []
            for feature in features:
                if bronze_feature_errors(feature):
                    issues.append("INVALID_RECONCILE_SOURCE_ITEM:" + source_id)
                    continue
                expected_versions.append(bronze_version(feature))
            present = self.lookup("silver.event_versions", "source_version_id", sorted(set(expected_versions)))
            if set(expected_versions) - {r["source_version_id"] for r in present}:
                issues.append("MISSING_SILVER_SOURCE_VERSION:" + source_id)
        pending = sum(c["state"] != "ACCOUNTED" for c in capture_rows)
        if pending:
            issues.append("PENDING_CAPTURES")
        batches = self.rows("ops.batches", 10000)
        if any(b["data_stage"] != "SILVER_COMMITTED" for b in batches):
            issues.append("PENDING_SILVER_BATCHES")
        if self.table("bronze.quarantine").where("resolution_status='OPEN'").limit(1).count():
            issues.append("OPEN_QUARANTINE")
        if len(historical) > 366:
            raise RecoveryError("Historical daily correction scope exceeds 366 dates")
        at = self.now()
        row = {"reconciliation_id": bronze_id("reconciliation-v1", run_id), "run_id": run_id,
            "checked_at": at, "status": "BLOCKED" if issues else "SUCCESS", "issues": sorted(set(issues)),
            "silver_table_version": self.table_version("silver.events_current"), "checked_events": len(plan["event_ids"]),
            "pending_captures": pending, "historical_dates": sorted(historical)}
        self.merge_rows("ops.reconciliations", [row], ("reconciliation_id",), tuple(row), ())
        return {"mode": mode, "status": row["status"], "issues": len(row["issues"]), "checked_events": row["checked_events"]}

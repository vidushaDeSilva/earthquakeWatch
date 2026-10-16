# Databricks notebook source
"""Build pinned Gold snapshots and activate a fully checked publication.

Load the Bronze and Silver libraries first, each in a separate notebook cell.
Preview persists inspectable BUILDING output; publish requires current source
coverage and a conditional pointer update. Failed builds never change serving.
"""

if "SparkSilverStore" not in globals():
    raise RuntimeError("Run the Bronze and Silver libraries in separate cells first")

GOLD_REVISION = "gold-v1"
GOLD_BOUNDARIES = ("none", "after-building", "after-watchlist", "after-windows", "after-daily",
                   "after-ready", "after-pointer")
WINDOW_COLUMNS = """window_end TIMESTAMP NOT NULL, event_count BIGINT NOT NULL,
    watchlist_count BIGINT NOT NULL, max_magnitude DOUBLE, pager_yellow_count BIGINT NOT NULL,
    pager_orange_count BIGINT NOT NULL, pager_red_count BIGINT NOT NULL,
    as_of_utc TIMESTAMP NOT NULL, is_partial BOOLEAN NOT NULL"""
GOLD_SCHEMAS = {
    "gold.watchlist_snapshots": """publication_id STRING NOT NULL, event_key STRING NOT NULL,
        silver_version_id STRING NOT NULL, policy_version STRING NOT NULL,""" + EVENT_COLUMNS + """,
        inclusion_reasons ARRAY<STRING> NOT NULL, impact_label STRING NOT NULL, quality_flags ARRAY<STRING> NOT NULL,
        rank_priority INT NOT NULL, as_of_utc TIMESTAMP NOT NULL""",
    "gold.window_snapshots": """publication_id STRING NOT NULL, window_grain STRING NOT NULL,
        window_start TIMESTAMP NOT NULL,""" + WINDOW_COLUMNS,
    "gold.daily_history": """event_date DATE NOT NULL, publication_id STRING NOT NULL,
        policy_version STRING NOT NULL, event_count BIGINT NOT NULL, watchlist_count BIGINT NOT NULL,
        max_magnitude DOUBLE, pager_yellow_count BIGINT NOT NULL, pager_orange_count BIGINT NOT NULL,
        pager_red_count BIGINT NOT NULL, as_of_utc TIMESTAMP NOT NULL,
        coverage_status STRING NOT NULL, is_partial BOOLEAN NOT NULL""",
    "ops.publications": """publication_id STRING NOT NULL, run_id STRING NOT NULL,
        processing_generation STRING NOT NULL, policy_version STRING NOT NULL, policy_sha256 STRING NOT NULL,
        normalizer_revision INT NOT NULL, silver_table_version BIGINT NOT NULL, input_batch_ids ARRAY<STRING> NOT NULL,
        as_of_utc TIMESTAMP NOT NULL, source_checked_through TIMESTAMP, coverage_start TIMESTAMP NOT NULL,
        coverage_end TIMESTAMP NOT NULL, source_checks_json STRING NOT NULL, coverage_status STRING NOT NULL,
        blocking_issues ARRAY<STRING> NOT NULL, expected_pointer_generation BIGINT NOT NULL,
        expected_watchlist_rows BIGINT NOT NULL, expected_window_rows BIGINT NOT NULL,
        state STRING NOT NULL, created_at TIMESTAMP NOT NULL, ready_at TIMESTAMP, is_daily_snapshot BOOLEAN NOT NULL,
        output_table_versions MAP<STRING,BIGINT> NOT NULL, output_hashes MAP<STRING,STRING> NOT NULL""",
    "ops.serving_pointer": """product_key STRING NOT NULL, publication_id STRING,
        previous_publication_id STRING, pointer_generation BIGINT NOT NULL, updated_at TIMESTAMP, run_id STRING, excluded_publication_ids ARRAY<STRING>""",
}
GOLD_KEYS = {"gold.watchlist_snapshots": ("publication_id", "event_key"),
    "gold.window_snapshots": ("publication_id", "window_grain", "window_start"),
    "gold.daily_history": ("publication_id", "event_date")}


class GoldError(SilverError):
    """Reject invalid releases or activation while preserving the previous pointer."""


def gold_policy(row):
    """Validate the supported versioned rule grammar instead of silently ignoring new rules."""
    value = bronze_decode(row["rules_json"].encode())
    # Sprint 1 seeded rules_json as {"any": [...]}; preserve that immutable seed.
    if isinstance(value, dict) and set(value) == {"any"}:
        value = {"policy_version": row["policy_version"], "rules": value}
    if not isinstance(value, dict) or value.get("policy_version") != row["policy_version"]:
        raise GoldError("Policy identity differs from stored rules")
    rules = value.get("rules", {}).get("any")
    expected = {("significance", ">"), ("pager_alert", "in"), ("magnitude", ">="), ("mmi", ">="), ("tsunami_flag", "=")}
    if not isinstance(rules, list) or len(rules) != 5:
        raise GoldError("Unsupported impact policy grammar")
    indexed = {(r.get("field"), r.get("operator")): r.get("value") for r in rules if isinstance(r, dict)}
    if set(indexed) != expected:
        raise GoldError("Unsupported or duplicate impact policy rule")
    if indexed[("pager_alert", "in")] != ["yellow", "orange", "red"] or indexed[("tsunami_flag", "=")] != 1:
        raise GoldError("Unsupported categorical impact policy")
    for field, op in (("significance", ">"), ("magnitude", ">="), ("mmi", ">=")):
        if not bronze_finite(indexed[(field, op)]) or indexed[(field, op)] < 0:
            raise GoldError("Invalid impact threshold")
    return value, {field: threshold for (field, _), threshold in indexed.items()}


def gold_reasons(event, thresholds):
    """Return explainable null-safe reasons and editorial priority for one earthquake."""
    reasons, priorities = [], []
    pager = event.get("pager_alert")
    if pager in ("yellow", "orange", "red"):
        reasons.append("PAGER_" + pager.upper())
        priorities.append({"red": 0, "orange": 1, "yellow": 2}[pager])
    for field, prefix, priority, strict in (("significance", "SIG_GT_", 6, True),
            ("magnitude", "MAG_GTE_", 5, False), ("mmi", "SHAKEMAP_MMI_GTE_", 4, False)):
        value, threshold = event.get(field), thresholds[field]
        if value is not None and (value > threshold if strict else value >= threshold):
            reasons.append(prefix + format(threshold, "g"))
            priorities.append(priority)
    if event.get("tsunami_flag") == 1:
        reasons.append("TSUNAMI_FLAG")
        priorities.append(3)
    return sorted(reasons), min(priorities) if priorities else None


def gold_coverage(captures, batches, rejections, as_of):
    """Freeze honest feed coverage using fetch/generation evidence rather than newest event time."""
    issues, summaries, fetched = list(rejections.get("reconciliation_issues", [])), [], []
    if any(c["state"] != "ACCOUNTED" for c in captures):
        issues.append("PENDING_CAPTURES")
    if any(b["data_stage"] != "SILVER_COMMITTED" for b in batches):
        issues.append("PENDING_SILVER_BATCHES")
    if rejections.get("silver"):
        issues.append("UNRESOLVED_SILVER_CONFLICTS")
    if rejections.get("bronze"):
        issues.append("UNRESOLVED_BRONZE_REJECTIONS")
    for source, hours in (("usgs-m2p5-day", 3), ("usgs-m2p5-month", 36), ("usgs-significant-month", 36)):
        candidates = [c for c in captures if c["source_id"] == source and c["state"] == "ACCOUNTED"
                      and c["fetched_at"] <= as_of]
        if not candidates:
            issues.append("MISSING_SOURCE:" + source)
            continue
        latest = max(candidates, key=lambda c: (c["fetched_at"], c["capture_id"]))
        generated = latest.get("source_generated_at")
        if (generated is None or generated > latest["fetched_at"] + timedelta(minutes=5)
                or as_of - latest["fetched_at"] > timedelta(hours=hours)
                or as_of - generated > timedelta(hours=hours)):
            issues.append("STALE_OR_UNVERIFIED_SOURCE:" + source)
        if latest["accounted_item_count"] != latest["expected_item_count"]:
            issues.append("ACCOUNTING_MISMATCH:" + source)
        fetched.append(latest["fetched_at"])
        summaries.append({"source_id": source, "capture_id": latest["capture_id"], "fetched_at": latest["fetched_at"],
            "source_generated_at": generated, "expected_item_count": latest["expected_item_count"],
            "accounted_item_count": latest["accounted_item_count"]})
    snapshot = {"sources": summaries, "pending_capture_ids": sorted(c["capture_id"] for c in captures if c["state"] != "ACCOUNTED"),
        "historical_dates": rejections.get("historical_dates", []),
        "scope": "M2.5+ and significant rolling feeds; bounded targeted checks when reconciliation evidence exists"}
    return sorted(set(issues)), snapshot, min(fetched) if len(fetched) == 3 else None


def gold_outputs(events, publication, thresholds):
    """Build all deterministic outputs from one retained Silver snapshot and fixed UTC interval."""
    pid, as_of = publication["publication_id"], publication["as_of_utc"]
    start = as_of - timedelta(days=30)
    if len({e["event_key"] for e in events}) != len(events):
        raise GoldError("Pinned Silver snapshot has duplicate current keys")
    if any(e["last_confirmed_at"] > as_of + timedelta(seconds=5) for e in events):
        raise GoldError("Silver contains observations after as-of; use the original retry ID or a later as-of")
    if any(e["has_unresolved_conflict"] for e in events):
        raise GoldError("Pinned Silver snapshot contains unresolved current conflicts")
    scoped = [e for e in events if not e["is_deleted"] and e["event_type"] == "earthquake"
              and e["event_time"] is not None and start <= e["event_time"] < as_of]
    watchlist = []
    for event in scoped:
        reasons, priority = gold_reasons(event, thresholds)
        if not reasons:
            continue
        row = {k: v for k, v in event.items() if k not in ("source_version_id", "normalizer_revision",
            "first_observed_at", "last_confirmed_at", "has_unresolved_conflict")}
        row.update(publication_id=pid, policy_version=publication["policy_version"], inclusion_reasons=reasons,
            impact_label="ESTIMATED_IMPACT" if event["pager_alert"] in ("yellow", "orange", "red") else "POTENTIAL_IMPACT",
            quality_flags=["FEED_SNAPSHOTS_ONLY", *publication["blocking_issues"]], rank_priority=priority, as_of_utc=as_of)
        watchlist.append(row)
    watch_keys = {r["event_key"] for r in watchlist}
    windows, daily = [], []
    for grain, step in (("HOUR", timedelta(hours=1)), ("DAY", timedelta(days=1))):
        first = start.replace(minute=0, second=0, microsecond=0)
        if grain == "DAY":
            first = first.replace(hour=0)
        buckets = defaultdict(list)
        for event in scoped:
            bucket = event["event_time"].replace(minute=0, second=0, microsecond=0)
            if grain == "DAY":
                bucket = bucket.replace(hour=0)
            buckets[bucket].append(event)
        current = first
        while current < as_of:
            data = buckets[current]
            mags = [e["magnitude"] for e in data if e["magnitude"] is not None]
            row = {"publication_id": pid, "window_grain": grain, "window_start": current, "window_end": current + step,
                "event_count": len(data), "watchlist_count": sum(e["event_key"] in watch_keys for e in data),
                "max_magnitude": max(mags) if mags else None,
                "pager_yellow_count": sum(e["pager_alert"] == "yellow" for e in data),
                "pager_orange_count": sum(e["pager_alert"] == "orange" for e in data),
                "pager_red_count": sum(e["pager_alert"] == "red" for e in data), "as_of_utc": as_of,
                "is_partial": current < start or current + step > as_of}
            windows.append(row)
            if grain == "DAY":
                day = {k: v for k, v in row.items() if k not in ("window_grain", "window_start", "window_end")}
                day.update(event_date=current.date(), policy_version=publication["policy_version"],
                    coverage_status=publication["coverage_status"])
                daily.append(day)
            current += step
    if any(sum(w["event_count"] for w in windows if w["window_grain"] == grain) != len(scoped) for grain in ("HOUR", "DAY")):
        raise GoldError("Window counts do not reconcile with pinned events")
    historic = bronze_decode(publication["source_checks_json"].encode()).get("historical_dates", [])
    if len(historic) > 366:
        raise GoldError("Historical daily revision scope exceeds 366 dates")
    for text in sorted(set(historic)):
        day = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        if start.date() <= day.date() <= as_of.date():
            continue  # Existing 30-day rows already cover these dates, including clipped boundaries.
        if day >= as_of:
            raise GoldError("Historical correction cannot include a future day")
        data = [e for e in events if not e["is_deleted"] and e["event_type"] == "earthquake"
                and e["event_time"] is not None and day <= e["event_time"] < day + timedelta(days=1)]
        mags = [e["magnitude"] for e in data if e["magnitude"] is not None]
        daily.append({"event_date": day.date(), "publication_id": pid, "policy_version": publication["policy_version"],
            "event_count": len(data), "watchlist_count": sum(bool(gold_reasons(e, thresholds)[0]) for e in data),
            "max_magnitude": max(mags) if mags else None, "pager_yellow_count": sum(e["pager_alert"] == "yellow" for e in data),
            "pager_orange_count": sum(e["pager_alert"] == "orange" for e in data),
            "pager_red_count": sum(e["pager_alert"] == "red" for e in data), "as_of_utc": as_of,
            "coverage_status": "BOUNDED_CATALOG_BACKFILL", "is_partial": False})
    return {"gold.watchlist_snapshots": watchlist, "gold.window_snapshots": windows, "gold.daily_history": daily}


class GoldEngine:
    """Retry deterministic snapshots and expose them only through a validated publication pointer."""

    def __init__(self, store, run_id, fail_at="none"):
        """Bind the same serialized Job writer used by Bronze and Silver."""
        if fail_at not in GOLD_BOUNDARIES:
            raise GoldError("Unknown Gold fault boundary")
        self.store, self.run_id, self.fail_at = store, run_id, fail_at

    def process(self, action="preview", as_of=None, retry_id=None, policy_version="impact-v1", generation="v1"):
        """Build a preview or publish a release; retries reuse every persisted input pin."""
        if action not in ("preview", "publish", "auto") or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", generation):
            raise GoldError("Invalid Gold action or processing generation")
        existing = self.store.publication(retry_id) if retry_id else None
        if retry_id and existing is None:
            raise GoldError("Unknown retry publication")
        if existing and existing["state"] == "EXPIRED":
            raise GoldError("Publication output retention expired; build a new candidate")
        if existing:
            policy_version = existing["policy_version"]
        policies = self.store.lookup("ref.impact_policies", "policy_version", [policy_version])
        if len(policies) != 1:
            raise GoldError("Unknown policy version")
        policy, thresholds = gold_policy(policies[0])
        policy_hash = bronze_sha(bronze_json(policy))
        if existing and policy_hash != existing["policy_sha256"]:
            raise GoldError("Pinned policy content changed")
        if existing:
            publication = existing.copy()
        else:
            at = s56_utc(as_of or self.store.now())
            if at > self.store.now() + timedelta(seconds=5):
                raise GoldError("Publication as-of cannot be in the future")
            version = self.store.table_version("silver.events_current")
            captures, batches, rejects = self.store.coverage_inputs()
            issues, coverage, checked = gold_coverage(captures, batches, rejects, at)
            coverage_json = bronze_json(s56_fact(coverage)).decode()
            batch_ids = sorted(b["batch_id"] for b in batches if b["data_stage"] == "SILVER_COMMITTED")
            pid = bronze_id("publication-v1", generation, version, policy_version, policy_hash,
                            s56_fact(at), bronze_sha(coverage_json.encode()), batch_ids, issues)
            old = self.store.publication(pid)
            publication = old or {"publication_id": pid, "run_id": self.run_id, "processing_generation": generation,
                "policy_version": policy_version, "policy_sha256": policy_hash, "normalizer_revision": NORMALIZER_REVISION,
                "silver_table_version": version, "input_batch_ids": batch_ids, "as_of_utc": at,
                "source_checked_through": checked, "coverage_start": at - timedelta(days=30), "coverage_end": at,
                "source_checks_json": coverage_json, "coverage_status": "PARTIAL" if issues else "FEED_SNAPSHOTS_ONLY",
                "blocking_issues": issues, "expected_pointer_generation": self.store.pointer()["pointer_generation"],
                "expected_watchlist_rows": 0, "expected_window_rows": 0, "state": "BUILDING",
                "created_at": self.store.now(), "ready_at": None, "is_daily_snapshot": False,
                "output_table_versions": {}, "output_hashes": {}}
        events = self.store.snapshot(publication["silver_table_version"])
        outputs = gold_outputs(events, publication, thresholds)
        publication.update(expected_watchlist_rows=len(outputs["gold.watchlist_snapshots"]),
                           expected_window_rows=len(outputs["gold.window_snapshots"]))
        self.store.save_publication(publication)
        bronze_fault(self.fail_at, "after-building")
        for name, boundary in (("gold.watchlist_snapshots", "after-watchlist"),
                ("gold.window_snapshots", "after-windows"), ("gold.daily_history", "after-daily")):
            rows = outputs[name]
            if publication["state"] != "READY":
                self.store.merge_rows(name, rows, GOLD_KEYS[name], tuple(next(iter(rows), {})), ())
            self.store.verify_output(name, publication["publication_id"], rows)
            bronze_fault(self.fail_at, boundary)
            publication["output_table_versions"][name] = self.store.table_version(name)
            publication["output_hashes"][name] = s56_digest(rows, GOLD_KEYS[name])
        issues = publication["blocking_issues"]
        if action == "auto":
            action = "preview" if issues else "publish"
        self.store.stage_check(self.run_id, "GOLD", "publication_coverage", "WARN" if issues else "PASS",
                               len(issues), "0", {"publication_id": publication["publication_id"], "issues": issues})
        if action == "preview":
            self.store.save_publication(publication)
            print("GOLD_PREVIEW publication_id=" + publication["publication_id"] + " blocking_issues=" + str(len(issues)))
            return {"publication_id": publication["publication_id"], "action": action, "blocking_issues": len(issues)}
        self.store.activation_gate(publication)
        publication.update(state="READY", ready_at=publication["ready_at"] or self.store.now())
        self.store.save_publication(publication)
        bronze_fault(self.fail_at, "after-ready")
        self.store.activate(publication, self.run_id)
        bronze_fault(self.fail_at, "after-pointer")
        print("GOLD_ACTIVE publication_id=" + publication["publication_id"])
        return {"publication_id": publication["publication_id"], "action": action, "watchlist_rows": len(outputs["gold.watchlist_snapshots"])}


class SparkGoldStore(SparkSilverStore):
    """Persist Gold facts with exact readback and a conditional serving-pointer MERGE."""

    def table(self, name):
        """Allow only known publication and reference tables in addition to Silver."""
        if name in GOLD_SCHEMAS or name == "ref.impact_policies":
            return self.spark.table("earthquake_dev." + name)
        return super().table(name)

    def table_version(self, name):
        """Capture a concrete Delta version using a public SQL history command."""
        self.table(name)
        return int(self.spark.sql("DESCRIBE HISTORY earthquake_dev." + name + " LIMIT 1").first()["version"])

    def snapshot(self, version):
        """Fail on expired time travel; never silently substitute the current Silver table."""
        if type(version) is not int or version < 0:
            raise GoldError("Invalid Silver version pin")
        rows = self.spark.read.option("versionAsOf", version).table("earthquake_dev.silver.events_current").limit(10001).collect()
        if len(rows) > 10000:
            raise GoldError("Gold snapshot exceeds 10000 current events")
        return [s56_utc(r.asDict(recursive=True)) for r in rows]

    def coverage_inputs(self):
        """Inventory bounded capture/batch coverage and unresolved Silver quarantine."""
        captures, batches = self.rows("ops.captures", 10000), self.rows("ops.batches", 10000)
        open_stages = {r["stage_name"] for r in self.table("bronze.quarantine")
                       .where("resolution_status='OPEN'").select("stage_name").distinct().limit(3).collect()}
        rejects = {"silver": "SILVER" in open_stages, "bronze": "BRONZE" in open_stages}
        for batch in batches:
            if batch["data_stage"] == "SILVER_COMMITTED" and self.optional("receipts/silver/" + batch["batch_id"] + ".json") is None:
                raise GoldError("Missing Silver completion receipt; run Silver recovery")
        if self.spark.catalog.tableExists("earthquake_dev.ops.reconciliations"):
            reports = self.spark.table("earthquake_dev.ops.reconciliations").orderBy("checked_at", ascending=False).limit(1).collect()
            if not reports:
                rejects["reconciliation_issues"] = ["MISSING_RECONCILIATION"]
            else:
                report = s56_utc(reports[0].asDict(recursive=True))
                rejects["reconciliation_issues"] = list(report["issues"])
                if self.now() - report["checked_at"] > timedelta(hours=3):
                    rejects["reconciliation_issues"].append("STALE_RECONCILIATION")
                if report["silver_table_version"] != self.table_version("silver.events_current"):
                    rejects["reconciliation_issues"].append("RECONCILIATION_SILVER_CHANGED")
                rejects["historical_dates"] = report["historical_dates"]
        return captures, batches, rejects

    def publication(self, publication_id):
        """Read one immutable publication identity, rejecting ambiguous ledger rows."""
        if publication_id is None:
            return None
        if not re.fullmatch("[0-9a-f]{64}", publication_id):
            raise GoldError("Invalid publication identifier")
        rows = self.lookup("ops.publications", "publication_id", [publication_id])
        return rows[0] if rows else None

    def save_publication(self, row):
        """Preserve input pins and prevent any READY publication from moving backwards."""
        old = self.publication(row["publication_id"])
        if old and old["state"] == "READY" and row["state"] != "READY":
            raise GoldError("Cannot regress a READY release")
        mutable = ("state", "ready_at", "expected_watchlist_rows", "expected_window_rows", "output_table_versions", "output_hashes")
        immutable = tuple(k for k in row if k not in mutable)
        self.merge_rows("ops.publications", [row], ("publication_id",), immutable, mutable)
        self.prove_rows("ops.publications", [row], ("publication_id",), tuple(row))

    def verify_output(self, name, publication_id, expected):
        """Check the whole release partition, including unexpected rows and valid empty output."""
        from pyspark.sql import functions as F
        rows = self.table(name).where(F.col("publication_id") == publication_id).limit(10001).collect()
        if len(rows) > 10000:
            raise GoldError("Output release exceeds 10000 rows")
        actual = [s56_utc(r.asDict(recursive=True)) for r in rows]
        if len(actual) != len(expected) or s56_digest(actual, GOLD_KEYS[name]) != s56_digest(expected, GOLD_KEYS[name]):
            raise GoldError("Gold output content/count reconciliation failed: " + name)

    def pointer(self):
        """Require exactly one explicitly seeded product pointer."""
        rows = self.lookup("ops.serving_pointer", "product_key", ["earthquake_watchlist"])
        if len(rows) != 1:
            raise GoldError("Missing or duplicate serving pointer; run setup")
        return rows[0]

    def activation_gate(self, publication):
        """Recheck live coverage and age before exposing an otherwise immutable release."""
        pointer = self.pointer()
        if publication["publication_id"] in (pointer.get("excluded_publication_ids") or []):
            raise GoldError("Publication was excluded by rollback; build a repaired release")
        if pointer["publication_id"] == publication["publication_id"] and publication["state"] == "READY":
            return
        if publication["blocking_issues"]:
            raise GoldError("Pinned release has blocking coverage issues; build a new candidate after recovery")
        age = self.now() - publication["as_of_utc"]
        if age < timedelta(seconds=-5) or age > timedelta(hours=3):
            raise GoldError("Release is outside the three-hour activation freshness limit")
        captures, batches, rejects = self.coverage_inputs()
        issues, _, _ = gold_coverage(captures, batches, rejects, self.now())
        if issues:
            raise GoldError("Live source coverage blocks publication")
        if self.table_version("silver.events_current") != publication["silver_table_version"]:
            raise GoldError("A newer Silver version exists; build a new candidate")
        previous = self.publication(pointer["publication_id"])
        if previous and previous["as_of_utc"] >= publication["as_of_utc"]:
            raise GoldError("Older/equal-time release cannot supersede the active publication")
        if pointer["pointer_generation"] != publication["expected_pointer_generation"]:
            raise GoldError("Serving generation changed; candidate must be rebuilt")

    def audit_pointer(self, pointer):
        """Repair an activation audit after a lost reply before any later activation."""
        if pointer["publication_id"] is None:
            return
        at = pointer["updated_at"]
        row = {"attempt_id": bronze_id("pointer-audit-v1", pointer["product_key"], pointer["pointer_generation"], pointer["publication_id"]),
            "run_id": pointer["run_id"], "scope_type": "PUBLICATION", "scope_id": pointer["publication_id"],
            "stage_name": "PUBLISH_POINTER", "attempt_number": 1, "status": "SUCCESS", "started_at": at,
            "finished_at": at, "retryable": False, "http_status": None, "error_code": None, "sanitized_message": None,
            "metrics": {"from_publication": pointer["previous_publication_id"] or "", "to_publication": pointer["publication_id"],
                        "pointer_generation": str(pointer["pointer_generation"])}}
        self.upsert_ops("ops.stage_attempts", [row], ("status", "finished_at", "metrics"))

    def activate(self, publication, run_id):
        """Advance one seeded pointer conditionally; readback resolves a lost MERGE acknowledgement."""
        self.activation_gate(publication)
        previous = self.pointer()
        self.audit_pointer(previous)
        if previous["publication_id"] == publication["publication_id"]:
            return
        row = {"product_key": "earthquake_watchlist", "publication_id": publication["publication_id"],
            "previous_publication_id": previous["publication_id"], "pointer_generation": previous["pointer_generation"] + 1,
            "updated_at": self.now(), "run_id": run_id,
            "excluded_publication_ids": previous.get("excluded_publication_ids") or []}
        self.frame("ops.serving_pointer", [row]).createOrReplaceTempView("s6_pointer")
        try:
            self.spark.sql("""MERGE INTO earthquake_dev.ops.serving_pointer t USING s6_pointer s ON t.product_key=s.product_key
                WHEN MATCHED AND t.pointer_generation=s.pointer_generation-1 THEN UPDATE SET
                t.publication_id=s.publication_id, t.previous_publication_id=s.previous_publication_id,
                t.pointer_generation=s.pointer_generation, t.updated_at=s.updated_at, t.run_id=s.run_id""")
        except Exception:
            if self.pointer()["publication_id"] != publication["publication_id"]:
                raise
        actual = self.pointer()
        if actual["publication_id"] != publication["publication_id"] or actual["pointer_generation"] != row["pointer_generation"]:
            raise GoldError("Serving pointer compare-and-set did not activate the release")
        self.audit_pointer(actual)

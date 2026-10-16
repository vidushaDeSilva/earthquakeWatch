# Databricks notebook source
"""Plan and audit bounded retention, preserve daily releases, replay and rollback.

Raw file expiry has retained retirement receipts; managed Delta rows are deleted
through table transactions. Serving rollback changes one conditional pointer.
"""

if "SparkRecoveryStore" not in globals():
    raise RuntimeError("Load the Sprint 7 library before Sprint 8")

LIFECYCLE_TABLE_KEYS = {
    "bronze.event_versions": ("source_version_id",), "bronze.deliveries": ("delivery_id",),
    "bronze.quarantine": ("quarantine_id",), "silver.event_versions": ("silver_version_id",),
    "ops.runs": ("run_id",), "ops.stage_attempts": ("attempt_id",),
}


def retention_publications(publications, pointer, daily_rows, now):
    """Protect serving, its predecessor, recent candidates and daily archive dependencies."""
    keep = {p for p in (pointer["publication_id"], pointer["previous_publication_id"]) if p}
    per_day, latest_dates = {}, {}
    for p in publications:
        age = now - p["created_at"]
        if age < timedelta(days=7):
            keep.add(p["publication_id"])
        if p["is_daily_snapshot"] and p["state"] == "READY" and age < timedelta(days=730):
            key = p["as_of_utc"].date()
            if key not in per_day or p["as_of_utc"] > per_day[key]["as_of_utc"]:
                per_day[key] = p
    keep.update(p["publication_id"] for p in per_day.values())
    ready = {p["publication_id"] for p in publications if p["state"] == "READY"}
    excluded = set(pointer.get("excluded_publication_ids") or [])
    for row in daily_rows:
        if row["publication_id"] not in ready or row["publication_id"] in excluded:
            continue
        if (now.date() - row["event_date"]).days >= 730:
            continue
        old = latest_dates.get(row["event_date"])
        if old is None or (row["as_of_utc"], row["publication_id"]) > (old["as_of_utc"], old["publication_id"]):
            latest_dates[row["event_date"]] = row
    keep.update(r["publication_id"] for r in latest_dates.values())
    return keep


def retention_row_candidates(tables, now, protected_runs):
    """Select expired facts, keeping current source identity and required publication audits."""
    current = tables["silver.events_current"]
    current_sources = {r["source_version_id"] for r in current}
    current_versions = {r["silver_version_id"] for r in current}
    recent_sources = {r["source_version_id"] for r in tables["bronze.deliveries"]
                      if now - r["received_at"] < timedelta(days=90)}
    selected = {}
    selected["bronze.event_versions"] = [r for r in tables["bronze.event_versions"]
        if now - r["first_ingested_at"] >= timedelta(days=90)
        and r["source_version_id"] not in current_sources | recent_sources]
    selected["bronze.deliveries"] = [r for r in tables["bronze.deliveries"]
        if now - r["received_at"] >= timedelta(days=90)]
    selected["bronze.quarantine"] = [r for r in tables["bronze.quarantine"]
        if r["resolution_status"] in ("RESOLVED", "IGNORED") and r["resolved_at"] is not None
        and now - r["resolved_at"] >= timedelta(days=30)]
    selected["silver.event_versions"] = [r for r in tables["silver.event_versions"]
        if now - r["normalized_at"] >= timedelta(days=365) and r["silver_version_id"] not in current_versions]
    selected["ops.runs"] = [r for r in tables["ops.runs"] if r["status"] in ("SUCCESS", "FAILED")
        and r["finished_at"] is not None and now - r["finished_at"] >= timedelta(days=365)
        and r["run_id"] not in protected_runs]
    selected["ops.stage_attempts"] = [r for r in tables["ops.stage_attempts"]
        if r["finished_at"] is not None and now - r["finished_at"] >= timedelta(days=365)
        and r["run_id"] not in protected_runs and r["stage_name"] != "PUBLISH_POINTER"]
    return selected


class LifecycleEngine:
    """Execute one immutable reviewed plan with recovery after an uncertain delete acknowledgement."""

    def __init__(self, store, run_id, fail_at="none"):
        """Bind the serialized writer and explicit reversible-test fault boundaries."""
        if fail_at not in ("none", "after-retirement", "after-delete", "after-pointer"):
            raise RecoveryError("Unknown lifecycle fault boundary")
        self.store, self.run_id, self.fail_at = store, run_id, fail_at

    def plan(self):
        """Save exact deletion candidates and protected asset counts without deleting anything."""
        self.store.guard_maintenance()
        actions, summary = self.store.retention_candidates()
        content = {"plan_version": 1, "created_at": s56_fact(self.store.now()), "actions": actions, "summary": summary}
        pid = bronze_sha(bronze_json(content))
        self.store.put_receipt("lifecycle/plans/" + pid + "/plan.json", bronze_json(content))
        self.store.save_plan(pid, content)
        return {"plan_id": pid, "action_count": len(actions), **summary}

    def apply(self, plan_id):
        """Revalidate each planned action immediately before its separately atomic mutation."""
        s78_id(plan_id)
        raw = self.store.read("lifecycle/plans/" + plan_id + "/plan.json", 16777216)
        if bronze_sha(raw) != plan_id:
            raise RecoveryError("Retention plan bytes changed")
        plan = bronze_decode(raw)
        if self.store.now() - bronze_time(plan["created_at"]) > timedelta(hours=24):
            raise RecoveryError("Retention plan expired; create and inspect a fresh plan")
        self.store.guard_maintenance()
        for action in plan["actions"]:
            self.store.start_action(plan_id, action)
            self.store.perform_action(action, self.fail_at)
            self.store.finish_action(plan_id, action)
        return {"plan_id": plan_id, "applied_actions": len(plan["actions"])}

    def rollback(self, target_id, expected_generation, reason):
        """Restore serving to retained READY facts and atomically exclude the replaced bad release."""
        s78_id(target_id)
        if not reason.strip() or len(reason) > 200:
            raise RecoveryError("Rollback needs a concise nonsecret reason of at most 200 characters")
        target = self.store.publication(target_id)
        if target is None or target["state"] != "READY":
            raise RecoveryError("Rollback target must be a retained READY publication")
        self.store.verify_release(target)
        pointer = self.store.pointer()
        aid = bronze_id("rollback-v1", target_id, expected_generation)
        existing = self.store.rollback_record(aid)
        if existing is not None:
            if existing["reason"] != reason:
                raise RecoveryError("Rollback token was already bound to another reason")
            if pointer["publication_id"] == target_id and pointer["pointer_generation"] == expected_generation + 1:
                self.store.complete_rollback(existing)
                return {"action_id": aid, "publication_id": target_id, "status": "ALREADY_APPLIED"}
            if existing["state"] == "APPLIED":
                raise RecoveryError("This rollback was superseded; do not replay it over a newer release")
        if pointer["pointer_generation"] != expected_generation or pointer["publication_id"] is None:
            raise RecoveryError("Serving generation changed or no public release exists")
        if pointer["publication_id"] == target_id:
            return {"publication_id": target_id, "status": "ALREADY_ACTIVE"}
        if target_id in (pointer.get("excluded_publication_ids") or []):
            raise RecoveryError("A previously excluded publication cannot be restored as known-good")
        self.store.audit_pointer(pointer)
        record = existing or {"action_id": aid, "from_publication_id": pointer["publication_id"],
            "target_publication_id": target_id, "expected_generation": expected_generation,
            "reason": reason, "state": "PREPARED", "created_at": self.store.now(), "finished_at": None}
        self.store.save_rollback(record)
        self.store.rollback_pointer(record, self.run_id)
        bronze_fault(self.fail_at, "after-pointer")
        self.store.complete_rollback(record)
        return {"action_id": aid, "publication_id": target_id, "status": "APPLIED"}


class SparkLifecycleStore(SparkRecoveryStore):
    """Persist bounded lifecycle plans, exact retirement proofs and pointer recovery."""

    def guard_maintenance(self):
        """Require accounted inputs and no unresolved quality issue or active lifecycle hold."""
        from pyspark.sql import functions as F
        if self.table("ops.captures").where("state <> 'ACCOUNTED'").limit(1).count():
            raise RecoveryError("Pending captures protect raw data; complete Sprint 7 recovery first")
        if self.table("ops.batches").where("data_stage <> 'SILVER_COMMITTED'").limit(1).count():
            raise RecoveryError("Pending batches protect raw data")
        if self.table("bronze.quarantine").where("resolution_status='OPEN'").limit(1).count():
            raise RecoveryError("Unresolved quarantine protects raw evidence")
        if self.table("ops.data_holds").where(F.col("active") & (F.col("expires_at").isNull() |
                (F.col("expires_at") > F.current_timestamp()))).limit(1).count():
            raise RecoveryError("An active data hold blocks cleanup; resolve it explicitly")

    def verify_release(self, publication):
        """Verify retained release rows directly, without requiring old Silver time travel."""
        if publication["state"] != "READY":
            raise RecoveryError("Recovery requires a retained READY release")
        pid = publication["publication_id"]
        for name, keys in GOLD_KEYS.items():
            from pyspark.sql import functions as F
            rows = [s56_utc(r.asDict(recursive=True)) for r in self.table(name)
                    .where(F.col("publication_id") == pid).limit(10001).collect()]
            if len(rows) > 10000 or s56_digest(rows, keys) != publication["output_hashes"].get(name):
                raise RecoveryError("Release snapshot is missing, expired or changed: " + name)
            expected = publication["expected_watchlist_rows"] if name == "gold.watchlist_snapshots" else (
                publication["expected_window_rows"] if name == "gold.window_snapshots" else len(rows))
            if len(rows) != expected:
                raise RecoveryError("Release snapshot count mismatch")

    def snapshot_daily(self):
        """Protect a verified active release and save a manifest separate from Delta history."""
        pointer = self.pointer()
        if pointer["publication_id"] is None:
            return {"snapshot": "NO_ACTIVE_PUBLICATION"}
        publication = self.publication(pointer["publication_id"])
        self.verify_release(publication)
        day = publication["as_of_utc"].date()
        sid = bronze_id("daily-release-snapshot-v1", day.isoformat(), publication["publication_id"])
        manifest = {"snapshot_version": 1, "snapshot_id": sid, "snapshot_date": day.isoformat(),
            "publication_id": publication["publication_id"], "silver_table_version": publication["silver_table_version"],
            "policy_version": publication["policy_version"], "policy_sha256": publication["policy_sha256"],
            "as_of_utc": s56_fact(publication["as_of_utc"]), "output_hashes": publication["output_hashes"],
            "output_table_versions": publication["output_table_versions"], "source_checks_json": publication["source_checks_json"],
            "input_batch_ids": publication["input_batch_ids"]}
        raw = bronze_json(manifest)
        path = "lifecycle/snapshots/" + sid + "/manifest.json"
        self.put_receipt(path, raw)
        old = self.lookup("ops.recovery_snapshots", "snapshot_id", [sid])
        row = {"snapshot_id": sid, "snapshot_date": day, "publication_id": publication["publication_id"],
            "created_at": old[0]["created_at"] if old else self.now(), "manifest_sha256": bronze_sha(raw),
            "manifest_uri": self.root + "/" + path}
        self.merge_rows("ops.recovery_snapshots", [row], ("snapshot_id",), tuple(row), ())
        self.spark.sql("UPDATE earthquake_dev.ops.publications SET is_daily_snapshot=true WHERE publication_id='" + s78_id(publication["publication_id"]) + "'")
        return {"snapshot_id": sid, "publication_id": publication["publication_id"]}

    def retention_candidates(self):
        """Return at most 32 exact actions, with dependency and age protections applied."""
        now, actions = self.now(), []
        captures = self.rows("ops.captures", 10000)
        batches = self.rows("ops.batches", 10000)
        publications = self.rows("ops.publications", 10000)
        daily = self.rows("gold.daily_history", 100000)
        protected = retention_publications(publications, self.pointer(), daily, now)
        protected_runs = {p["run_id"] for p in publications if p["publication_id"] in protected}
        protected_runs.update(c["run_id"] for c in captures)
        protected_runs.update(b["run_id"] for b in batches)
        active_pins = [p for p in publications if p["state"] == "BUILDING" and now - p["created_at"] < timedelta(days=7)]
        for b in sorted(batches, key=lambda r: r["landed_at"]):
            if len(actions) >= 32:
                break
            if now - b["landed_at"] < timedelta(days=30):
                continue
            bid = s78_id(b["batch_id"])
            mpath = b["manifest_uri"].removeprefix(self.root + "/")
            raw = self.read(mpath)
            if bronze_sha(raw) != b["manifest_sha256"]:
                raise RecoveryError("Landing manifest changed before cleanup")
            m = bronze_decode(raw)
            data_path = m["records_relative_path"]
            data = self.optional(data_path)
            if data is None:
                s78_receipt(self.optional("receipts/retired/batches/" + bid + ".json"),
                    {"batch_id": bid, "state": "RETIRED", "manifest_sha256": b["manifest_sha256"]})
                continue
            bronze_raw = self.optional("receipts/bronze/" + bid + ".json")
            silver_raw = self.optional("receipts/silver/" + bid + ".json")
            s78_receipt(bronze_raw, {"batch_id": bid, "state": "BRONZE_COMMITTED", "manifest_sha256": b["manifest_sha256"]})
            s78_receipt(silver_raw, {"batch_id": bid, "state": "SILVER_COMMITTED", "manifest_sha256": b["manifest_sha256"]})
            if bronze_sha(data) != m["data_sha256"]:
                raise RecoveryError("Landing payload changed before cleanup")
            actions.append({"kind": "LANDING_FILE", "scope_id": bid, "path": data_path,
                "sha256": bronze_sha(data), "bytes": len(data), "retirement": {"state": "RETIRED", "batch_id": bid,
                "manifest_sha256": b["manifest_sha256"], "bronze_receipt_sha256": bronze_sha(bronze_raw),
                "silver_receipt_sha256": bronze_sha(silver_raw)}, "retirement_path": "receipts/retired/batches/" + bid + ".json"})
        for c in sorted(captures, key=lambda r: r["fetched_at"]):
            if len(actions) >= 32:
                break
            if now - c["fetched_at"] < timedelta(days=90):
                continue
            cid = s78_id(c["capture_id"])
            path = c["response_uri"].removeprefix(self.root + "/")
            data = self.optional_limit(path, 16777216)
            if data is None:
                self.verify_retired_capture(c, self.optional("receipts/retired/captures/" + cid + ".json"))
                continue
            receipt = self.optional("receipts/captures/" + cid + ".json")
            s78_receipt(receipt, {"capture_id": cid, "state": "ACCOUNTED", "response_sha256": c["response_sha256"],
                "expected_item_count": c["expected_item_count"], "accounted_item_count": c["expected_item_count"]})
            if bronze_sha(data) != c["response_sha256"]:
                raise RecoveryError("Capture bytes changed before cleanup")
            manifest = bronze_decode(self.read(c["manifest_uri"].removeprefix(self.root + "/")))
            related = []
            if manifest.get("request_id"):
                rid = s78_id(manifest["request_id"])
                for filename in ("original.json", "response-checkpoint.json"):
                    extra_path = "reconciliation/requests/" + rid + "/" + filename
                    extra_raw = self.optional_limit(extra_path, 16777216)
                    if extra_raw is not None:
                        related.append({"path": extra_path, "sha256": bronze_sha(extra_raw), "bytes": len(extra_raw)})
            actions.append({"kind": "CAPTURE_FILE", "scope_id": cid, "path": path, "sha256": bronze_sha(data),
                "bytes": len(data), "related_files": related, "retirement": {"state": "RETIRED", "capture_id": cid,
                "response_sha256": c["response_sha256"], "capture_receipt_sha256": bronze_sha(receipt)},
                "retirement_path": "receipts/retired/captures/" + cid + ".json"})
        # Incomplete batches are never recursive-delete targets; inspect their only payload file.
        for sid, is_dir in self.names("handoff").items():
            if not is_dir or len(actions) >= 32:
                continue
            prefix = "handoff/" + s78_id(sid) + "/batches"
            for bid, batch_dir in self.names(prefix).items():
                if not batch_dir or len(actions) >= 32:
                    continue
                bp = prefix + "/" + s78_id(bid)
                if self.optional(bp + "/manifest.json") is not None:
                    continue
                for entry in self.directory(bp):
                    if entry.name == "records.jsonl" and now.timestamp() * 1000 - entry.modificationTime >= 7 * 86400000:
                        data = self.read(bp + "/records.jsonl")
                        actions.append({"kind": "ORPHAN_FILE", "scope_id": bid, "path": bp + "/records.jsonl",
                            "sha256": bronze_sha(data), "bytes": len(data), "absent_manifest": bp + "/manifest.json"})
        for collection, is_dir in self.names("captures").items():
            if not is_dir or len(actions) >= 32:
                continue
            collection = s78_id(collection)
            selected_raw = self.optional("collections/" + collection + "/selected.json")
            selected_id = bronze_decode(selected_raw).get("capture_id") if selected_raw else None
            for cid, capture_dir in self.names("captures/" + collection).items():
                if not capture_dir or len(actions) >= 32:
                    continue
                cp = "captures/" + collection + "/" + s78_id(cid)
                if self.optional(cp + "/ready.json") is not None or selected_id == cid:
                    continue
                for entry in self.directory(cp):
                    if entry.name == "response.json" and now.timestamp() * 1000 - entry.modificationTime >= 7 * 86400000:
                        data = self.read(cp + "/response.json", 16777216)
                        actions.append({"kind": "ORPHAN_FILE", "scope_id": cid, "path": cp + "/response.json",
                            "sha256": bronze_sha(data), "bytes": len(data), "absent_manifest": cp + "/ready.json"})
        tables = {name: self.rows(name, 100000) for name in [*LIFECYCLE_TABLE_KEYS, "silver.events_current"]}
        candidates = retention_row_candidates(tables, now, protected_runs)
        for name, rows in candidates.items():
            if len(actions) >= 32:
                break
            if not rows:
                continue
            keys = LIFECYCLE_TABLE_KEYS[name]
            rows = sorted(rows, key=lambda r: tuple(r[k] for k in keys))[:1000]
            actions.append({"kind": "TABLE_ROWS", "scope_id": name, "table": name,
                "keys": list(keys), "values": [[r[k] for k in keys] for r in rows],
                "row_count": len(rows), "sha256": s56_digest(rows, keys)})
        for pub in sorted(publications, key=lambda r: r["created_at"]):
            if len(actions) >= 32:
                break
            if pub["publication_id"] in protected or pub["state"] == "EXPIRED":
                continue
            actions.append({"kind": "PUBLICATION", "scope_id": pub["publication_id"], "original_state": pub["state"],
                "sha256": bronze_sha(bronze_json(s56_fact(pub)))})
        for action in actions:
            action["action_id"] = bronze_id("retention-action-v1", action)
        return actions, {"protected_publications": len(protected), "active_pin_holds": len(active_pins),
            "raw_file_bytes": sum(a.get("bytes", 0) for a in actions), "bounded_actions": 32,
            "identity_metadata_held": True}

    def save_plan(self, pid, content):
        """Record a plan digest so review and execution refer to identical bytes."""
        row = {"plan_id": pid, "created_at": bronze_time(content["created_at"]), "action_count": len(content["actions"]),
            "manifest_sha256": pid, "manifest_uri": self.root + "/lifecycle/plans/" + pid + "/plan.json"}
        self.merge_rows("ops.maintenance_plans", [row], ("plan_id",), tuple(row), ())

    def start_action(self, pid, action):
        """Persist the mutation intention before deleting a byte or a row."""
        old = self.lookup("ops.maintenance_actions", "action_id", [action["action_id"]])
        row = old[0] if old else {"action_id": action["action_id"], "plan_id": pid, "kind": action["kind"],
            "scope_id": action["scope_id"], "status": "RUNNING", "started_at": self.now(),
            "finished_at": None, "error_class": None}
        if old and row["plan_id"] != pid:
            # The same byte-identical action may appear in a refreshed review plan.
            return
        self.merge_rows("ops.maintenance_actions", [row], ("action_id",), ("kind", "scope_id"), ())

    def finish_action(self, pid, action):
        """Finish an existing action only after the desired postcondition was read back."""
        row = self.lookup("ops.maintenance_actions", "action_id", [action["action_id"]])[0]
        row.update(status="SUCCESS", finished_at=row["finished_at"] or self.now())
        self.merge_rows("ops.maintenance_actions", [row], ("action_id",), ("kind", "scope_id"), ("status", "finished_at"))

    def perform_action(self, action, fail_at):
        """Recheck content and eligibility, then perform one recoverable mutation."""
        self.guard_maintenance()
        aid = action["action_id"]
        body = {k: v for k, v in action.items() if k != "action_id"}
        if aid != bronze_id("retention-action-v1", body):
            raise RecoveryError("Retention action identity changed")
        kind = action["kind"]
        if kind in ("LANDING_FILE", "CAPTURE_FILE", "ORPHAN_FILE"):
            data = self.optional_limit(action["path"], 16777216)
            if data is None and kind == "ORPHAN_FILE":
                return
            if data is None:
                s78_receipt(self.optional(action["retirement_path"]), action["retirement"])
            if data is not None and (bronze_sha(data) != action["sha256"] or len(data) != action["bytes"]):
                raise RecoveryError("Planned raw bytes changed")
            if data is not None:
                eligible, _ = self.retention_candidates()
                if aid not in {r["action_id"] for r in eligible}:
                    raise RecoveryError("Raw deletion is no longer eligible; make a new plan")
            if kind != "ORPHAN_FILE":
                self.put_receipt(action["retirement_path"], bronze_json(action["retirement"]))
                bronze_fault(fail_at, "after-retirement")
            if data is not None:
                self.utils.fs.rm(self.path(action["path"]), False)
            for related in action.get("related_files", []):
                extra = self.optional_limit(related["path"], 16777216)
                if extra is not None:
                    if bronze_sha(extra) != related["sha256"]:
                        raise RecoveryError("Related source evidence changed since planning")
                    self.utils.fs.rm(self.path(related["path"]), False)
                if self.optional(related["path"]) is not None:
                    raise RecoveryError("Related raw source deletion did not complete")
            bronze_fault(fail_at, "after-delete")
            if self.optional(action["path"]) is not None:
                raise RecoveryError("Cloud file deletion did not complete")
        elif kind == "TABLE_ROWS":
            name = action["table"]
            if name not in LIFECYCLE_TABLE_KEYS or tuple(action["keys"]) != LIFECYCLE_TABLE_KEYS[name]:
                raise RecoveryError("Untrusted table deletion scope")
            keys = LIFECYCLE_TABLE_KEYS[name]
            from pyspark.sql import functions as F
            values = [v[0] for v in action["values"]]
            actual = [s56_utc(r.asDict(recursive=True)) for r in self.table(name).where(F.col(keys[0]).isin(values)).collect()]
            if not actual:
                return  # The persisted action intent allows recovery after a lost DELETE reply.
            if len(actual) != action["row_count"] or s56_digest(actual, keys) != action["sha256"]:
                raise RecoveryError("Planned table rows changed or are only partly missing")
            eligible, _ = self.retention_candidates()
            if aid not in {r["action_id"] for r in eligible}:
                raise RecoveryError("Row deletion is no longer eligible")
            schema = self.table(name).select(*keys).schema
            self.spark.createDataFrame([tuple(v) for v in action["values"]], schema).createOrReplaceTempView("s8_delete_keys")
            self.spark.sql(f"MERGE INTO earthquake_dev.{name} t USING s8_delete_keys s ON t.{keys[0]}=s.{keys[0]} WHEN MATCHED THEN DELETE")
            bronze_fault(fail_at, "after-delete")
            if self.table(name).where(F.col(keys[0]).isin(values)).limit(1).count():
                raise RecoveryError("Table deletion readback failed")
        elif kind == "PUBLICATION":
            pid = s78_id(action["scope_id"])
            pub = self.publication(pid)
            original = dict(pub, state=action["original_state"])
            keep = retention_publications(self.rows("ops.publications", 10000), self.pointer(),
                self.rows("gold.daily_history", 100000), self.now())
            if pid in keep or bronze_sha(bronze_json(s56_fact(original))) != action["sha256"]:
                raise RecoveryError("Publication is protected or changed since planning")
            # Expire first so a retry cannot mistake partially removed output for a usable release.
            self.spark.sql("UPDATE earthquake_dev.ops.publications SET state='EXPIRED' WHERE publication_id='" + pid + "'")
            for table in GOLD_KEYS:
                self.spark.sql(f"DELETE FROM earthquake_dev.{table} WHERE publication_id='{pid}'")
            bronze_fault(fail_at, "after-delete")
        else:
            raise RecoveryError("Unknown retention action kind")

    def rollback_record(self, aid):
        """Load one immutable rollback intention."""
        rows = self.lookup("ops.rollback_actions", "action_id", [aid])
        return rows[0] if rows else None

    def save_rollback(self, record):
        """Persist expected generation and target before attempting the pointer change."""
        immutable = tuple(k for k in record if k not in ("state", "finished_at"))
        self.merge_rows("ops.rollback_actions", [record], ("action_id",), immutable, ("state", "finished_at"))

    def rollback_pointer(self, record, run_id):
        """CAS the target and excluded-release array in the same single-table transaction."""
        row = {"product_key": "earthquake_watchlist", "publication_id": record["target_publication_id"],
            "previous_publication_id": record["from_publication_id"], "pointer_generation": record["expected_generation"] + 1,
            "updated_at": self.now(), "run_id": run_id, "excluded_publication_ids": []}
        self.frame("ops.serving_pointer", [row]).createOrReplaceTempView("s8_rollback_pointer")
        try:
            self.spark.sql("""MERGE INTO earthquake_dev.ops.serving_pointer t USING s8_rollback_pointer s
                ON t.product_key=s.product_key WHEN MATCHED AND t.pointer_generation=s.pointer_generation-1
                AND t.publication_id=s.previous_publication_id THEN UPDATE SET t.publication_id=s.publication_id,
                t.previous_publication_id=s.previous_publication_id,t.pointer_generation=s.pointer_generation,
                t.updated_at=s.updated_at,t.run_id=s.run_id,
                t.excluded_publication_ids=array_union(coalesce(t.excluded_publication_ids,cast(array() as array<string>)),array(s.previous_publication_id))""")
        except Exception:
            pointer = self.pointer()
            if pointer["publication_id"] != record["target_publication_id"] or pointer["pointer_generation"] != record["expected_generation"] + 1:
                raise
        pointer = self.pointer()
        if pointer["publication_id"] != record["target_publication_id"] or pointer["pointer_generation"] != record["expected_generation"] + 1:
            raise RecoveryError("Rollback pointer compare-and-set failed")
        self.audit_pointer(pointer)

    def complete_rollback(self, record):
        """Repair audit metadata after pointer success, including an interrupted acknowledgement."""
        self.audit_pointer(self.pointer())
        record.update(state="APPLIED", finished_at=record["finished_at"] or self.now())
        self.save_rollback(record)

    def vacuum(self, apply=False):
        """Preview or apply default-safe Delta vacuum with no retention override."""
        self.guard_maintenance()
        stale = self.table("ops.publications").where("state='BUILDING' AND created_at < current_timestamp()-INTERVAL 6 DAYS")
        if stale.limit(1).count():
            raise RecoveryError("Expire old unfinished publications before vacuum; their pins are not durable backups")
        results = {}
        for table in ("bronze.event_versions", "bronze.deliveries", "bronze.quarantine", *SILVER_SCHEMAS,
                      "gold.watchlist_snapshots", "gold.window_snapshots", "gold.daily_history"):
            properties = {r["key"]: r["value"] for r in self.spark.sql("SHOW TBLPROPERTIES earthquake_dev." + table).collect()}
            if properties.get("delta.deletedFileRetentionDuration") != "interval 7 days":
                raise RecoveryError("Unexpected table retention; review rather than overriding it")
            command = "VACUUM earthquake_dev." + table + ("" if apply else " DRY RUN")
            rows = self.spark.sql(command).limit(1001).collect()
            if len(rows) > 1000:
                raise RecoveryError("Vacuum preview exceeds 1000 files; inspect the table with SQL before proceeding")
            results[table] = {"listed_rows": len(rows), "dry_run": not apply,
                "files": [r.asDict(recursive=True) for r in rows]}
        return results

    def replay_bronze(self, replay_id):
        """Rebuild typed retained source history into isolated tables, preserving live serving."""
        if not re.fullmatch("[a-z][a-z0-9_]{0,23}", replay_id):
            raise RecoveryError("Replay ID must be a lowercase identifier up to 24 characters")
        aliases = {r["alias_event_id"]: r["event_key"] for r in self.rows("silver.event_aliases", 20000)}
        versions, seen = [], defaultdict(list)
        captures = {r["capture_id"]: r for r in self.rows("ops.captures", 10000)}
        deliveries = self.rows("bronze.deliveries", 100000)
        for delivery in deliveries:
            if delivery["disposition"] == "ACCEPTED" and delivery["capture_id"] in captures:
                seen[delivery["source_version_id"]].append(captures[delivery["capture_id"]]["fetched_at"])
        for raw in self.rows("bronze.event_versions", 20000):
            feature = bronze_decode(raw["raw_event_json"].encode())
            if bronze_version(feature) != raw["source_version_id"] or bronze_feature_errors(feature):
                raise RecoveryError("Retained Bronze replay input failed source validation")
            key = aliases.get(raw["source_event_id"])
            if key is None or not seen.get(raw["source_version_id"]):
                raise RecoveryError("Replay lacks retained alias or observation-time evidence")
            row = silver_normalize(feature, key, raw["source_version_id"], raw["first_ingested_at"])
            versions.append(row)
        by_event = defaultdict(list)
        for row in versions:
            by_event[row["event_key"]].append(row)
        current = []
        for key, states in by_event.items():
            order = defaultdict(set)
            for row in states:
                order[row["source_updated_at"]].add(row["semantic_payload_sha256"])
            if any(len(values) > 1 for values in order.values()):
                raise RecoveryError("Retained Bronze replay contains unresolved same-time conflict")
            chosen = max(states, key=lambda r: (r["source_updated_at"], r["normalizer_revision"], r["source_event_id"]))
            row = {k: v for k, v in chosen.items() if k not in ("normalized_at", "selection_status")}
            times = [at for v in states for at in seen[v["source_version_id"]]]
            row.update(first_observed_at=min(times), last_confirmed_at=max(times), has_unresolved_conflict=False)
            current.append(row)
        schema_name = "replay_" + replay_id
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS earthquake_dev." + schema_name)
        for name, rows in (("event_versions", versions), ("events_current", current)):
            table = f"earthquake_dev.{schema_name}.{name}"
            source = "silver." + name
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({SILVER_SCHEMAS[source]}) USING DELTA")
            old_rows = [s56_utc(r.asDict(recursive=True)) for r in self.spark.table(table).limit(20001).collect()]
            keys = ("silver_version_id",) if name == "event_versions" else ("event_key",)
            if old_rows:
                if s56_digest(old_rows, keys) != s56_digest(rows, keys):
                    raise RecoveryError("Replay namespace contains different inputs; use a new replay ID")
            elif rows:
                self.spark.createDataFrame(rows, self.spark.table(table).schema).write.mode("append").saveAsTable(table)
        return {"replay_schema": schema_name, "event_versions": len(versions), "current_events": len(current),
            "scope": "retained Bronze and delivery evidence; no claim of a complete one-year rebuild"}

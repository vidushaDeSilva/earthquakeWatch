"""Build an immutable Gold candidate and conditionally activate its serving pointer.

Run as a dependent task after Bronze/Silver in one single-concurrency Databricks
Job. The pointer is the only consumer-visible commit boundary; recovery of a
lost acknowledgement reads it back before deciding whether activation occurred.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from earthquake_watch.contracts import ContractError
from earthquake_watch.gold_policy import build_rows, load_policy, reasons


CATALOG = "earthquake_dev"
PRODUCT = "usgs-watchlist"


def _table(suffix: str) -> str:
    """Resolve reviewed development tables without arbitrary SQL identifiers."""
    return f"{CATALOG}.{suffix}"


def _rows(spark: Any, table: str, predicate: Any) -> list[dict[str, Any]]:
    """Read bounded control rows or one small publication for verification."""
    return [row.asDict(recursive=True) for row in spark.table(table).where(predicate).collect()]


def _merge(spark: Any, table: str, records: list[dict[str, Any]], keys: tuple[str, ...]) -> None:
    """Insert unique immutable rows; reject a conflicting retry before activation."""
    if not records:
        return
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    unique = {tuple(row[key] for key in keys): row for row in records}
    if len(unique) != len(records):
        raise ContractError(f"Duplicate candidate key in {table}")
    target = DeltaTable.forName(spark, table)
    incoming = spark.createDataFrame(list(unique.values()), schema=target.toDF().schema)
    condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    existing = target.toDF().alias("t").join(incoming.alias("s"),
        [F.col(f"t.{key}") == F.col(f"s.{key}") for key in keys], "inner")
    # Compare every business field rather than trusting a count-only idempotency check.
    def normalized(value: Any) -> Any:
        """Compare Spark's UTC-naive timestamp results with aware input values."""
        if isinstance(value, datetime):
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        if isinstance(value, list):
            return [normalized(item) for item in value]
        if isinstance(value, dict):
            return {name: normalized(item) for name, item in value.items()}
        return value

    for key, source in unique.items():
        selected = existing.where(__import__("functools").reduce(
            lambda a, b: a & b, [F.col(f"s.{name}") == F.lit(value) for name, value in zip(keys, key)]))
        for result in selected.select("t.*").collect():
            actual = result.asDict(recursive=True)
            if normalized(actual) != normalized(source):
                raise ContractError(f"Existing candidate row differs in {table}")
    target.alias("t").merge(incoming.alias("s"), condition).whenNotMatchedInsertAll().execute()


def _update_publication(spark: Any, publication_id: str, values: dict[str, Any]) -> None:
    """Move one persisted candidate through BUILDING, READY and ACTIVE."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    target = DeltaTable.forName(spark, _table("ops.publications"))
    target.update(F.col("publication_id") == publication_id,
                  {name: F.lit(value) for name, value in values.items()})


def _check(spark: Any, run_id: str, pub_id: str, rule: str,
           passed: bool, observed: str, expected: str) -> None:
    """Persist a deterministic blocking check for an auditable rejection."""
    from delta.tables import DeltaTable

    table = _table("ops.quality_checks")
    row = dict(check_id=hashlib.sha256(f"{pub_id}|{rule}".encode()).hexdigest(),
               pipeline_run_id=run_id, stage_name="GOLD", rule_id=rule,
               severity="BLOCKER", check_status="PASS" if passed else "FAIL",
               observed_value=observed[:500], expected_value=expected,
               details_json=json.dumps({"publication_id": pub_id}),
               checked_at=datetime.now(timezone.utc))
    target = DeltaTable.forName(spark, table)
    incoming = spark.createDataFrame([row], schema=target.toDF().schema)
    target.alias("t").merge(incoming.alias("s"), "t.check_id = s.check_id") \
        .whenMatchedUpdate(set={name: f"s.{name}" for name in ("check_status", "observed_value", "checked_at")}) \
        .whenNotMatchedInsertAll().execute()
    if not passed:
        raise ContractError(f"Gold gate failed: {rule}")


def _gold_stage(spark: Any, run_id: str, pub_id: str, status: str,
                records_in: int, records_out: int | None = None,
                delta_version: int | None = None) -> None:
    """Keep a single retry-stable Gold stage row for the publication attempt."""
    from delta.tables import DeltaTable

    table = _table("ops.stage_runs")
    now = datetime.now(timezone.utc)
    row = dict(stage_run_id=hashlib.sha256(f"{run_id}|GOLD|{pub_id}".encode()).hexdigest(),
               pipeline_run_id=run_id, stage_name="GOLD", attempt_number=1,
               stage_status=status, input_fingerprint=pub_id, records_in=records_in,
               records_out=records_out, records_quarantined=0, output_table=_table("gold.watchlist_snapshots"),
               output_delta_version=delta_version, retryable=None, error_code=None,
               started_at=now, finished_at=now if status == "COMMITTED" else None)
    target = DeltaTable.forName(spark, table)
    source = spark.createDataFrame([row], schema=target.toDF().schema)
    target.alias("t").merge(source.alias("s"), "t.stage_run_id = s.stage_run_id") \
        .whenMatchedUpdate(set={key: f"s.{key}" for key in ("stage_status", "records_in", "records_out", "output_delta_version", "finished_at")}) \
        .whenNotMatchedInsertAll().execute()


def _pointer(spark: Any, publication_id: str, run_id: str, as_of: datetime) -> int:
    """Switch one product pointer with generation compare-and-swap and readback."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    table = _table("ops.serving_pointer")
    prior = _rows(spark, table, F.col("product_key") == PRODUCT)
    if len(prior) > 1:
        raise ContractError("Serving pointer key is not unique")
    old = prior[0] if prior else None
    if old and old["active_publication_id"] == publication_id:
        # Rebuild a missing audit row after a crash between pointer and audit.
        _merge(spark, _table("ops.pointer_history"), [dict(
            product_key=PRODUCT, pointer_generation=old["pointer_generation"],
            publication_id=publication_id, previous_publication_id=old["previous_publication_id"],
            pipeline_run_id=old["pipeline_run_id"], activated_at=old["updated_at"])],
            ("product_key", "pointer_generation"))
        return old["pointer_generation"]
    if old and old["active_publication_id"]:
        active = _rows(spark, _table("ops.publications"),
                       F.col("publication_id") == old["active_publication_id"])
        if len(active) != 1 or active[0]["publication_status"] not in ("READY", "ACTIVE"):
            raise ContractError("Existing serving pointer is invalid")
        # An older retry is never allowed to supersede a newer release.
        if active[0]["created_at"] >= as_of:
            raise ContractError("Candidate is not newer than the active publication")
    generation = old["pointer_generation"] + 1 if old else 1
    now = datetime.now(timezone.utc)
    if old:
        DeltaTable.forName(spark, table).update(
            condition=(F.col("product_key") == PRODUCT) &
                      (F.col("pointer_generation") == old["pointer_generation"]) &
                      (F.col("active_publication_id").eqNullSafe(old["active_publication_id"])),
            set={"active_publication_id": F.lit(publication_id),
                 "previous_publication_id": F.lit(old["active_publication_id"]),
                 "pointer_generation": F.lit(generation), "updated_at": F.lit(now),
                 "pipeline_run_id": F.lit(run_id)})
    else:
        _merge(spark, table, [dict(product_key=PRODUCT, active_publication_id=publication_id,
                                  previous_publication_id=None, pointer_generation=generation,
                                  updated_at=now, pipeline_run_id=run_id)], ("product_key",))
    current = _rows(spark, table, F.col("product_key") == PRODUCT)
    if len(current) != 1 or current[0]["active_publication_id"] != publication_id or current[0]["pointer_generation"] != generation:
        raise ContractError("Serving pointer compare-and-swap was not confirmed")
    _merge(spark, _table("ops.pointer_history"), [dict(
        product_key=PRODUCT, pointer_generation=generation, publication_id=publication_id,
        previous_publication_id=old["active_publication_id"] if old else None,
        pipeline_run_id=run_id, activated_at=current[0]["updated_at"])],
        ("product_key", "pointer_generation"))
    return generation


def publish(spark: Any, batch_id: str, pipeline_run_id: str, revision: str,
            policy_bytes: bytes) -> dict[str, Any]:
    """Pin Silver, build candidate, gate it and expose only a validated release."""
    from pyspark.sql import functions as F

    if any(len(identifier) != 64 or any(c not in "0123456789abcdef" for c in identifier)
           for identifier in (batch_id, pipeline_run_id)):
        raise ContractError("Invalid batch or pipeline run ID")
    policy, policy_hash = load_policy(policy_bytes)
    parent = _rows(spark, _table("ops.pipeline_runs"), F.col("pipeline_run_id") == pipeline_run_id)
    if (len(parent) != 1 or parent[0]["run_status"] != "RUNNING"
            or parent[0]["processing_revision"] != revision or parent[0]["batch_ids"] != [batch_id]
            or parent[0]["submitted_at"] is None):
        raise ContractError("Gold needs one RUNNING parent for exactly this batch and revision")
    as_of = parent[0]["submitted_at"].replace(tzinfo=timezone.utc)
    stages = _rows(spark, _table("ops.stage_runs"), F.col("pipeline_run_id") == pipeline_run_id)
    committed = [s for s in stages if s["stage_name"] == "SILVER" and s["stage_status"] == "COMMITTED"]
    if len(committed) != 1 or committed[0]["output_delta_version"] is None:
        raise ContractError("No unique committed Silver stage to pin")
    silver_version = committed[0]["output_delta_version"]
    # Once a candidate exists, retries reuse its original Silver snapshot even
    # if an upstream rerun emitted a later Delta commit for the same batch.
    prior_candidates = _rows(spark, _table("ops.publications"),
                             (F.col("pipeline_run_id") == pipeline_run_id) &
                             (F.col("product_key") == PRODUCT))
    prior_candidates = [p for p in prior_candidates if p["input_batch_ids"] == [batch_id]
                        and p["processing_revision"] == revision]
    if len(prior_candidates) > 1:
        raise ContractError("Multiple candidates exist for the same pipeline input")
    if prior_candidates:
        if prior_candidates[0]["publication_status"] == "REJECTED":
            raise ContractError("Rejected candidate requires a new processing revision")
        if json.loads(prior_candidates[0]["validation_summary"])["policy_sha256"] != policy_hash:
            raise ContractError("Policy bytes changed under the same processing revision")
        silver_version = prior_candidates[0]["silver_delta_version"]
    publication_id = hashlib.sha256(json.dumps([
        pipeline_run_id, batch_id, silver_version, revision, policy_hash, as_of.isoformat()],
        separators=(",", ":")).encode()).hexdigest()
    pub_table = _table("ops.publications")
    existing = _rows(spark, pub_table, F.col("publication_id") == publication_id)
    if len(existing) > 1 or (existing and existing[0]["publication_status"] == "REJECTED"):
        raise ContractError("Candidate is conflicted or already REJECTED; use a new revision")
    now = datetime.now(timezone.utc)
    if not existing:
        _merge(spark, pub_table, [dict(
            publication_id=publication_id, pipeline_run_id=pipeline_run_id, product_key=PRODUCT,
            policy_version=policy["policy_version"], processing_revision=revision,
            silver_delta_version=silver_version, input_batch_ids=[batch_id],
            validation_status="PENDING", publication_status="BUILDING",
            validation_summary=json.dumps({"policy_sha256": policy_hash, "as_of_utc": as_of.isoformat()}),
            created_at=as_of, validated_at=None, activated_at=None)], ("publication_id",))
    else:
        candidate = existing[0]
        if (candidate["silver_delta_version"] != silver_version or candidate["processing_revision"] != revision
                or candidate["input_batch_ids"] != [batch_id]
                or json.loads(candidate["validation_summary"])["policy_sha256"] != policy_hash):
            raise ContractError("Existing publication identity has different lineage")
        active = _rows(spark, _table("ops.serving_pointer"), F.col("product_key") == PRODUCT)
        if candidate["publication_status"] == "ACTIVE" and len(active) == 1 and active[0]["active_publication_id"] == publication_id:
            generation = _pointer(spark, publication_id, pipeline_run_id, as_of)
            _gold_stage(spark, pipeline_run_id, publication_id, "COMMITTED", 0,
                        spark.table(_table("gold.watchlist_snapshots")).where(F.col("publication_id") == publication_id).count(),
                        int(spark.sql(f"DESCRIBE HISTORY {_table('gold.watchlist_snapshots')} LIMIT 1").first()["version"]))
            return {"publication_id": publication_id, "pointer_generation": generation,
                    "silver_delta_version": silver_version, "already_active": True}
    try:
        _gold_stage(spark, pipeline_run_id, publication_id, "RUNNING", 0)
        _check(spark, pipeline_run_id, publication_id, "fresh_source",
               0 <= (now - as_of).total_seconds() <= 180 * 60, str((now - as_of).total_seconds()), "0..10800 seconds")
        captures = spark.table(_table("ops.captures")).join(
            spark.table(_table("ops.landing_batches")).where(F.col("landing_batch_id") == batch_id)
                 .select(F.explode("capture_ids").alias("capture_id")), "capture_id")
        generated = [r[0] for r in captures.select("source_generated_at").collect()]
        _check(spark, pipeline_run_id, publication_id, "capture_freshness",
               bool(generated) and all(g is not None and 0 <= (as_of - g.replace(tzinfo=timezone.utc)).total_seconds() <= 10800
                                       for g in generated), str(len(generated)), "all captures generated within 3h")
        landing = _rows(spark, _table("ops.landing_batches"), F.col("landing_batch_id") == batch_id)
        _check(spark, pipeline_run_id, publication_id, "landing_complete",
               len(landing) == 1 and landing[0]["landing_status"] == "VERIFIED" and
               landing[0]["expected_record_count"] == landing[0]["landed_record_count"],
               str(len(landing)), "one verified complete landing")
        # A bounded landing pass may split a capture. Do not publish a partial feed.
        coverage = [(r["capture_id"], r["expected_event_count"]) for r in captures.collect()]
        physical = spark.table(_table("bronze.event_deliveries"))
        covered = [(capture_id, physical.where(F.col("capture_id") == capture_id)
                    .select("usgs_event_id").distinct().count(), expected)
                   for capture_id, expected in coverage]
        _check(spark, pipeline_run_id, publication_id, "capture_accounting",
               bool(covered) and all(actual == expected for _, actual, expected in covered),
               str(covered), "all capture feature IDs present in Bronze")
        stage_names = {s["stage_name"] for s in stages if s["stage_status"] == "COMMITTED"}
        _check(spark, pipeline_run_id, publication_id, "upstream_stages",
               {"BRONZE", "SILVER"} <= stage_names, str(sorted(stage_names)), "BRONZE,SILVER")
        upstream = _rows(spark, _table("ops.quality_checks"), F.col("pipeline_run_id") == pipeline_run_id)
        required = {(s, r) for s, r in (("BRONZE", "physical_row_count"),
                                         ("SILVER", "logical_version_count"),
                                         ("SILVER", "quarantine_count"))}
        passing = {(q["stage_name"], q["rule_id"]) for q in upstream if q["check_status"] == "PASS"}
        _check(spark, pipeline_run_id, publication_id, "upstream_quality",
               required <= passing and not any(q["check_status"] == "FAIL" and q["severity"] == "BLOCKER" for q in upstream),
               str(sorted(passing)), str(sorted(required)))
        quarantine = spark.table(_table("ops.quarantine_records")).where(
            (F.col("landing_batch_id") == batch_id) & (F.col("resolution_status") == "OPEN")).count()
        _check(spark, pipeline_run_id, publication_id, "open_quarantine", quarantine == 0, str(quarantine), "0")
        # Read a fixed Delta version, not the mutable current-events view.
        versions = spark.read.option("versionAsOf", silver_version).table(_table("silver.event_versions"))
        from pyspark.sql import Window
        ranked = versions.where(F.col("validation_status") == "VALID").withColumn(
            "current_rank", F.row_number().over(Window.partitionBy("usgs_event_id").orderBy(
                F.col("usgs_updated_ms").desc(), F.col("last_seen_at").desc(), F.col("event_version_id").desc())))
        start = as_of - timedelta(hours=24)
        frame = ranked.where((F.col("current_rank") == 1) & (F.col("event_time") >= start) &
                             (F.col("event_time") < as_of) & (F.col("event_type") == "earthquake"))
        events = [r.asDict(recursive=True) for r in frame.collect()]
        for event in events:
            # Spark returns UTC timestamps as naive Python datetime objects.
            event["event_time"] = event["event_time"].replace(tzinfo=timezone.utc)
        conflicts = sum(bool(r["version_conflict"]) for r in events)
        _check(spark, pipeline_run_id, publication_id, "version_conflict", conflicts == 0, str(conflicts), "0")
        watchlist, daily = build_rows(events, publication_id, as_of, policy)
        _merge(spark, _table("gold.watchlist_snapshots"), watchlist, ("publication_id", "usgs_event_id"))
        _merge(spark, _table("gold.daily_metrics"), daily, ("publication_id", "metric_date"))
        saved_watch = _rows(spark, _table("gold.watchlist_snapshots"), F.col("publication_id") == publication_id)
        saved_daily = _rows(spark, _table("gold.daily_metrics"), F.col("publication_id") == publication_id)
        _check(spark, pipeline_run_id, publication_id, "watchlist_parity",
               len(saved_watch) == len(watchlist) and {r["usgs_event_id"] for r in saved_watch} ==
               {r["usgs_event_id"] for r in watchlist}, str(len(saved_watch)), str(len(watchlist)))
        _check(spark, pipeline_run_id, publication_id, "daily_parity",
               len(saved_daily) == len(daily) and all(
                   a["event_count"] == b["event_count"] and a["watchlist_count"] == b["watchlist_count"]
                   for a, b in zip(sorted(saved_daily, key=lambda x: x["metric_date"]), daily)),
               str(len(saved_daily)), str(len(daily)))
        _check(spark, pipeline_run_id, publication_id, "metric_accounting",
               sum(r["event_count"] for r in saved_daily) == len(events) and
               sum(r["watchlist_count"] for r in saved_daily) == len(saved_watch),
               str((sum(r["event_count"] for r in saved_daily),
                    sum(r["watchlist_count"] for r in saved_daily))),
               str((len(events), len(saved_watch))))
        expected_versions = {r["usgs_event_id"]: r["event_version_id"] for r in watchlist}
        _check(spark, pipeline_run_id, publication_id, "pinned_silver_lineage",
               all(expected_versions.get(r["usgs_event_id"]) == r["event_version_id"] for r in saved_watch),
               str(len(saved_watch)), "all version IDs match pinned Silver")
        _check(spark, pipeline_run_id, publication_id, "rule_semantics",
               all(r["selection_reasons"] == reasons(next(e for e in events if e["usgs_event_id"] == r["usgs_event_id"]), policy)
                   for r in saved_watch), str(len(saved_watch)), "all rows match impact-v1")
        digest = hashlib.sha256(json.dumps({"watchlist": sorted(watchlist, key=lambda r: r["usgs_event_id"]),
                                          "daily": daily}, sort_keys=True, default=str).encode()).hexdigest()
        _update_publication(spark, publication_id, {"validation_status": "PASS", "publication_status": "READY",
            "validation_summary": json.dumps({"policy_sha256": policy_hash, "as_of_utc": as_of.isoformat(),
                                              "output_sha256": digest, "watchlist_count": len(watchlist),
                                              "daily_bucket_count": len(daily)}),
            "validated_at": datetime.now(timezone.utc)})
    except ContractError:
        # Never demote a release already activated after an uncertain write response.
        pointer = _rows(spark, _table("ops.serving_pointer"), F.col("product_key") == PRODUCT)
        if not pointer or pointer[0]["active_publication_id"] != publication_id:
            _update_publication(spark, publication_id, {"validation_status": "FAIL", "publication_status": "REJECTED"})
        raise
    generation = _pointer(spark, publication_id, pipeline_run_id, as_of)
    _update_publication(spark, publication_id, {"publication_status": "ACTIVE",
                                                   "activated_at": datetime.now(timezone.utc)})
    gold_version = int(spark.sql(f"DESCRIBE HISTORY {_table('gold.watchlist_snapshots')} LIMIT 1").first()["version"])
    _gold_stage(spark, pipeline_run_id, publication_id, "COMMITTED", len(events), len(watchlist), gold_version)
    return {"publication_id": publication_id, "pointer_generation": generation,
            "silver_delta_version": silver_version, "watchlist_count": len(watchlist),
            "daily_bucket_count": len(daily)}


def main() -> None:
    """Accept the same three Job parameters as Bronze/Silver plus fixed policy path."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--policy-path", required=True)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    result = publish(SparkSession.builder.getOrCreate(), args.batch_id,
                     args.pipeline_run_id, args.revision, Path(args.policy_path).read_bytes())
    print(json.dumps(result))


if __name__ == "__main__":
    main()

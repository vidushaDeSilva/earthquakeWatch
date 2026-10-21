"""Plan and apply guarded Delta row retention without changing VACUUM policy.

The maintenance task is manual. A stored key plan and unchanged serving
pointer generation are required before deleting Bronze, Silver or old Gold.
Ops metadata remains longer to preserve audit lineage.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from earthquake_watch.contracts import ContractError, json_bytes, stamp
from earthquake_watch.retention import (BRONZE_DAYS, GOLD_DAYS, SILVER_DAYS,
                                        plan_sha256, safe_candidates)


ROOT = Path("/Volumes/earthquake_dev/ops/pipeline_files")
BRONZE = "earthquake_dev.bronze.event_deliveries"
SILVER = "earthquake_dev.silver.event_versions"
WATCH = "earthquake_dev.gold.watchlist_snapshots"
DAILY = "earthquake_dev.gold.daily_metrics"
MAX_KEYS = 10000


def _rows(spark: Any, table: str) -> list[dict[str, Any]]:
    """Bound maintenance metadata and avoid unbounded driver collections."""
    result = spark.table(table).limit(100001).collect()
    if len(result) > 100000:
        raise ContractError("Retention metadata exceeds the reviewed bound")
    return [r.asDict(recursive=True) for r in result]


def _aware(value: datetime) -> datetime:
    """Interpret Spark timestamps as UTC for retention cutoffs."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def table_candidates(spark: Any, now: datetime) -> dict[str, Any]:
    """Select only aged rows whose active and retained Gold lineage is safe."""
    from pyspark.sql import functions as F

    captures = _rows(spark, "earthquake_dev.ops.captures")
    batches = _rows(spark, "earthquake_dev.ops.landing_batches")
    runs = _rows(spark, "earthquake_dev.ops.pipeline_runs")
    stages = _rows(spark, "earthquake_dev.ops.stage_runs")
    pubs = _rows(spark, "earthquake_dev.ops.publications")
    pointers = [r for r in _rows(spark, "earthquake_dev.ops.serving_pointer")
                if r["product_key"] == "usgs-watchlist"]
    if len(pointers) > 1:
        raise ContractError("Duplicate serving pointer")
    if any(r["run_status"] not in ("SUCCEEDED", "FAILED") for r in runs):
        raise ContractError("Pause maintenance until every pipeline run is terminal")
    active_id = pointers[0]["active_publication_id"] if pointers else None
    generation = pointers[0]["pointer_generation"] if pointers else 0
    quarantine = _rows(spark, "earthquake_dev.ops.quarantine_records")
    open_batches = {r["landing_batch_id"] for r in quarantine
                    if r["resolution_status"] == "OPEN" and r["landing_batch_id"]}
    _, safe_batches = safe_candidates(now, captures, batches, runs, stages, pubs,
                                      active_id, open_batches)
    bronze_rows = []
    if safe_batches:
        physical = spark.table(BRONZE).where(
            F.col("landing_batch_id").isin(sorted(safe_batches)) &
            (F.col("ingested_at") <= now - timedelta(days=BRONZE_DAYS)))
        bronze_rows = [{"topic": r.topic, "partition_id": r.partition_id,
                        "offset": r.offset, "feature_sha256": r.feature_sha256,
                        "landing_batch_id": r.landing_batch_id}
                       for r in physical.select("topic", "partition_id", "offset",
                                                "feature_sha256", "landing_batch_id").limit(MAX_KEYS + 1).collect()]
    removable_pub_ids = sorted(p["publication_id"] for p in pubs if
                             p["publication_id"] != active_id and
                             p["publication_status"] in ("SUPERSEDED", "REJECTED") and
                             _aware(p["created_at"]) <= now - timedelta(days=GOLD_DAYS))
    retained = [p for p in pubs if p["publication_id"] not in removable_pub_ids and
                p["publication_status"] in ("ACTIVE", "READY", "SUPERSEDED")]
    # Retained daily metrics include earthquakes that never reached the
    # watchlist. Protect every Silver event in each retained 24-hour window.
    protected_windows = [(_aware(p["created_at"]) - timedelta(hours=24),
                          _aware(p["created_at"])) for p in retained]
    protected_versions = {r.event_version_id for r in spark.table(WATCH).where(
        ~F.col("publication_id").isin(removable_pub_ids)
    ).select("event_version_id").distinct().limit(MAX_KEYS + 1).collect()}
    silver_rows = []
    old_silver = spark.table(SILVER).where(
        F.col("last_seen_at") <= now - timedelta(days=SILVER_DAYS)).select(
            "event_version_id", "event_time").limit(MAX_KEYS + 1).collect()
    for row in old_silver:
        event_time = _aware(row.event_time)
        if row.event_version_id not in protected_versions and not any(
                start <= event_time < end for start, end in protected_windows):
            silver_rows.append(row.event_version_id)
    if max(len(bronze_rows), len(old_silver), len(protected_versions), len(removable_pub_ids)) > MAX_KEYS:
        raise ContractError("Retention plan exceeds bounded key limit")
    bronze_rows.sort(key=lambda r: (r["topic"], r["partition_id"], r["offset"]))
    return {"pointer_generation": generation, "active_publication_id": active_id,
            "bronze": bronze_rows, "silver": sorted(silver_rows),
            "gold_publications": removable_pub_ids}


def _delete_keys(spark: Any, table: str, column: str, keys: list[str]) -> None:
    """Delete bounded logical keys; a missing key is a safe repeat."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if keys:
        DeltaTable.forName(spark, table).delete(F.col(column).isin(keys))


def apply_tables(spark: Any, reviewed: dict[str, Any], current: dict[str, Any]) -> dict[str, int]:
    """Reject changed lineage, then perform repeatable keyed Delta deletes."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if (current["pointer_generation"] != reviewed["pointer_generation"] or
            current["active_publication_id"] != reviewed["active_publication_id"]):
        raise ContractError("Serving pointer changed since retention review")
    for field in ("bronze", "silver", "gold_publications"):
        existing = {json.dumps(item, sort_keys=True) for item in current[field]}
        if any(json.dumps(item, sort_keys=True) not in existing for item in reviewed[field]):
            # A prior partial apply is allowed only when its physical key is
            # already absent. Reclassification of an existing key is blocked.
            if field == "bronze":
                source = spark.createDataFrame([
                    (r["topic"], r["partition_id"], r["offset"]) for r in reviewed[field]],
                    "topic string, partition_id int, offset long")
                present = {(r.topic, r.partition_id, r.offset) for r in spark.table(BRONZE).join(
                    source, ["topic", "partition_id", "offset"], "inner").select(
                        "topic", "partition_id", "offset").collect()}
                if any((r["topic"], r["partition_id"], r["offset"]) in present and
                       json.dumps(r, sort_keys=True) not in existing for r in reviewed[field]):
                    raise ContractError("Bronze cleanup key changed eligibility")
            elif field == "silver":
                present = {r.event_version_id for r in spark.table(SILVER).where(
                    F.col("event_version_id").isin(reviewed[field])).select("event_version_id").collect()}
                if any(key in present and json.dumps(key) not in existing for key in reviewed[field]):
                    raise ContractError("Silver cleanup key changed eligibility")
            else:
                eligible = set(current[field])
                if any(key not in eligible for key in reviewed[field]):
                    raise ContractError("Gold cleanup publication changed eligibility")
    # The reviewed Gold publications are old and nonactive; remove both
    # consumer tables, then the independent Bronze and Silver key sets.
    _delete_keys(spark, WATCH, "publication_id", reviewed["gold_publications"])
    _delete_keys(spark, DAILY, "publication_id", reviewed["gold_publications"])
    if reviewed["bronze"]:
        source = spark.createDataFrame([
            (r["topic"], r["partition_id"], r["offset"]) for r in reviewed["bronze"]],
            "topic string, partition_id int, offset long")
        DeltaTable.forName(spark, BRONZE).alias("t").merge(source.alias("s"),
            "t.topic = s.topic AND t.partition_id = s.partition_id AND t.offset = s.offset") \
            .whenMatchedDelete().execute()
    _delete_keys(spark, SILVER, "event_version_id", reviewed["silver"])
    return {"bronze_keys": len(reviewed["bronze"]), "silver_keys": len(reviewed["silver"]),
            "gold_publications": len(reviewed["gold_publications"])}


def maintain(spark: Any, root: Path, expected_digest: str | None = None) -> dict[str, Any]:
    """Write an immutable dry run or apply only a digest reviewed earlier."""
    current = table_candidates(spark, datetime.now(timezone.utc))
    if expected_digest is None:
        digest = plan_sha256([current])
        report = {"report_version": 1, "created_at": stamp(datetime.now(timezone.utc)),
                  "plan_sha256": digest, "candidates": current}
        path = root / "retention" / f"tables-{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            previous = json.loads(path.read_bytes())
            if previous.get("candidates") != current or previous.get("plan_sha256") != digest:
                raise ContractError("Existing table retention report conflicts")
        else:
            with path.open("xb") as stream:
                stream.write(json_bytes(report))
        return {"mode": "DRY_RUN", "plan_sha256": digest, "counts": {
            name: len(current[name]) for name in ("bronze", "silver", "gold_publications")},
            "plan_path": str(path)}
    if len(expected_digest) != 64 or any(c not in "0123456789abcdef" for c in expected_digest):
        raise ContractError("Invalid table retention plan digest")
    path = root / "retention" / f"tables-{expected_digest}.json"
    if not path.is_file():
        raise ContractError("Reviewed table retention plan does not exist")
    report = json.loads(path.read_bytes())
    if report.get("plan_sha256") != expected_digest or plan_sha256([report["candidates"]]) != expected_digest:
        raise ContractError("Reviewed table retention report checksum differs")
    return {"mode": "APPLIED", "plan_sha256": expected_digest,
            **apply_tables(spark, report["candidates"], current)}


def main() -> None:
    """Require an explicit reviewed digest for row deletion."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply-plan-sha256")
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    print(json.dumps(maintain(SparkSession.builder.getOrCreate(), ROOT,
                              args.apply_plan_sha256)))


if __name__ == "__main__":
    main()

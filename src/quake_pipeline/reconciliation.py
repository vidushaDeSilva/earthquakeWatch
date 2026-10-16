"""Recover frozen captures and collect bounded, auditable USGS catalog evidence.

Source bytes stay in memory and the Databricks Volume. Catalog responses are
frozen before Kafka; single-event wrappers retain their exact original bytes.
The CLI runs under the existing GitHub concurrency lock and serialized Job.
"""

import argparse
import base64
import os
import re
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlsplit

from .capture import Collector
from .config import Settings
from .contracts import (CONTRACT_VERSION, SOURCES, collection_window, decode_json,
                        digest, json_bytes, parse_utc, stable_id, utc_text, validate_feed)
from .errors import BudgetExceeded, ContractError, PipelineError
from .http import HTTPClient, Response
from .jobs import sync_ledger
from .kafka_client import KafkaClient
from .kafka_config import KafkaSettings
from .landing import land
from .outbox import bind_stream, dispatch, stream_id
from .volume import Volume

MAX_INVENTORY = 10000
API_ROOT = "https://earthquake.usgs.gov/fdsnws/event/1/"


@dataclass(frozen=True)
class RequestSource:
    """Describe a frozen allowlisted feed or catalog request for the existing collector."""

    source_id: str
    url: str
    feed_key: str = "catalog"
    lookback_days: int = 0
    revision: int = 1


def checked_source(manifest):
    """Confine old and new capture dispatch to the known USGS endpoints."""
    for sources in SOURCES.values():
        for source in sources:
            if manifest.get("source_id") == source.source_id and manifest.get("source_url") == source.url:
                return source
    url = manifest.get("source_url", "")
    parsed = urlsplit(url)
    if (manifest.get("source_id") not in ("usgs-event-check", "usgs-catalog-m2p5")
            or parsed.scheme != "https" or parsed.netloc != "earthquake.usgs.gov"
            or parsed.path != "/fdsnws/event/1/query" or parsed.fragment):
        raise ContractError("Unknown capture source; do not dispatch untrusted request metadata")
    return RequestSource(manifest["source_id"], url)


def inventory(volume, maximum=MAX_INVENTORY):
    """Discover selected captures and recover an interrupted selection without refetching."""
    collections = volume.child_directories("captures")
    if len(collections) > maximum:
        raise BudgetExceeded("Capture inventory exceeds 10000; archive/index migration required")
    result = []
    for collection in collections:
        selected = volume.read_optional(f"collections/{collection}/selected.json")
        candidates = []
        if selected is not None:
            selection = decode_json(selected)
            if selection.get("collection_id") != collection:
                raise ContractError("Capture selection identity mismatch")
            ids = [selection.get("capture_id")]
        else:
            ids = volume.child_directories("captures/" + collection)
        for cid in ids:
            if not isinstance(cid, str) or not re.fullmatch("[0-9a-f]{64}", cid):
                raise ContractError("Invalid capture directory identity")
            raw = volume.read_optional(f"captures/{collection}/{cid}/ready.json")
            if raw is None:
                if selected is not None:
                    raise ContractError("Selected capture is missing its ready manifest")
                continue
            m = decode_json(raw)
            if m.get("capture_id") != cid or m.get("collection_id") != collection:
                raise ContractError("Frozen capture folder identity mismatch")
            checked_source(m)
            candidates.append(m)
        if not candidates:
            continue
        m = min(candidates, key=lambda c: (parse_utc(c["fetched_at"]), c["capture_id"]))
        if selected is None:
            volume.put_immutable(f"collections/{collection}/selected.json", json_bytes(
                {"manifest_version": 1, "collection_id": collection, "capture_id": m["capture_id"]}))
        receipt = volume.read_optional("receipts/captures/" + m["capture_id"] + ".json")
        if receipt is not None:
            r = decode_json(receipt)
            if any(r.get(k) != v for k, v in {"capture_id": m["capture_id"],
                    "state": "ACCOUNTED", "response_sha256": m["response_sha256"],
                    "expected_item_count": m["expected_item_count"],
                    "accounted_item_count": m["expected_item_count"]}.items()):
                raise ContractError("Accounted capture receipt differs from frozen manifest")
        else:
            result.append(m)
    return sorted(result, key=lambda m: (m["fetched_at"], m["capture_id"]))


def recover_captures(settings, client, volume, kafka, stream, emit, maximum=16, fail_at="none"):
    """Dispatch pending captures by ID, preserving chunk receipts and source ordinals."""
    if not 1 <= maximum <= 32:
        raise ContractError("Recovery max captures must be 1..32")
    pending = inventory(volume)
    total, selected_count = 0, 0
    for m in pending[:maximum]:
        source = checked_source(m)
        verified = Collector(settings, client, volume)._load(source, m["collection_id"], m["capture_id"])
        if total + verified["expected_item_count"] > stream.max_land_records:
            if selected_count:
                break
            raise BudgetExceeded("One capture exceeds the source-item budget")
        dispatch(volume, kafka, stream, verified, emit, fail_at)
        total += verified["expected_item_count"]
        selected_count += 1
    return {"selected_captures": selected_count, "remaining_captures": len(pending) - selected_count,
            "source_items": total}


def catalog_count(payload):
    """Parse the documented GeoJSON count object and the API's plain-text integer form."""
    value = decode_json(payload)
    value = value.get("count") if isinstance(value, dict) else value
    if type(value) is not int or value < 0:
        raise ContractError("Catalog count response lacks a nonnegative integer count")
    return value


class CatalogCollector:
    """Freeze catalog queries, exact original bytes and explicit source-check outcomes."""

    def __init__(self, settings, client, volume, epoch, now=None, sleeper=time.sleep, clock=time.monotonic):
        """Bind a retry epoch, injectable pacing and a fixed request budget."""
        if not re.fullmatch("[A-Za-z0-9_-]{1,32}", epoch):
            raise ContractError("Recovery epoch must be a short identifier")
        self.settings, self.client, self.volume, self.epoch = settings, client, volume, epoch
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep, self.clock, self.previous, self.calls = sleeper, clock, None, 0

    def request(self, url, expected=(200,)):
        """Pace USGS calls at one per second in addition to bounded HTTP retry handling."""
        if self.calls >= 80:
            raise BudgetExceeded("Catalog API budget of 80 requests reached; continue in another run")
        if self.previous is not None:
            self.sleep(max(0, 1 - (self.clock() - self.previous)))
        self.previous, self.calls = self.clock(), self.calls + 1
        return self.client.request("usgs", "GET", url, expected=expected)

    def collect(self, params, kind, run_id, start, end):
        """Seal one query or unresolved check; retries never replace selected source bytes."""
        if kind not in ("EVENT_CHECK", "BACKFILL"):
            raise ContractError("Unsupported catalog request kind")
        params = {"format": "geojson", "includedeleted": "true", "jsonerror": "true", **params}
        url = API_ROOT + "query?" + urlencode(sorted(params.items()))
        rid = stable_id("catalog-request-v1", kind, self.epoch, url)
        rp = "reconciliation/requests/" + rid
        ready = self.volume.read_optional(rp + "/ready.json")
        if ready is not None:
            report = decode_json(ready)
            self.verify(report, rid, url)
            return report
        before = after = None
        checkpoint = self.volume.read_optional(rp + "/response-checkpoint.json")
        saved = decode_json(checkpoint) if checkpoint is not None else None
        if kind == "BACKFILL":
            count_params = {k: v for k, v in params.items() if k not in ("limit", "orderby", "jsonerror")}
            count_url = API_ROOT + "count?" + urlencode(sorted(count_params.items()))
            before = base64.b64decode(saved["count_before_b64"], validate=True) if saved else self.request(count_url).body
            if catalog_count(before) >= 10000:
                return {"request_id": rid, "outcome": "SPLIT_REQUIRED"}
        if saved:
            if saved["request_id"] != rid:
                raise ContractError("HTTP checkpoint identity mismatch")
            response = Response(saved["status"], base64.b64decode(saved["body_b64"], validate=True))
            fetched = parse_utc(saved["fetched_at"])
        else:
            response = self.request(url, (200, 204, 404, 409))
            fetched = self.now()
            if len(response.body) > 8 * 1024 * 1024:
                if kind == "BACKFILL":
                    return {"request_id": rid, "outcome": "SPLIT_REQUIRED"}
                raise BudgetExceeded("Event-check response exceeds eight MiB")
            self.volume.put_immutable(rp + "/response-checkpoint.json", json_bytes({"request_id": rid,
                "status": response.status, "body_b64": base64.b64encode(response.body).decode(),
                "fetched_at": utc_text(fetched), "count_before_b64": base64.b64encode(before or b"").decode()}))
        report = {"request_version": 1, "request_id": rid, "kind": kind, "epoch": self.epoch,
            "url": url, "params": params, "requested_event_id": params.get("eventid"),
            "window_start": utc_text(start), "window_end": utc_text(end), "fetched_at": utc_text(fetched),
            "http_status": response.status, "original_sha256": digest(response.body),
            "original_relative_path": rp + "/original.json", "capture_id": None,
            "collection_id": None, "outcome": "UNRESOLVED", "generated_basis": None,
            "count_before": None, "count_after": None}
        self.volume.put_immutable(rp + "/original.json", response.body)
        feed = None
        if response.status == 200:
            original = decode_json(response.body)
            if kind == "EVENT_CHECK":
                if original.get("type") == "Feature":
                    features = [original]
                    generated = int(fetched.timestamp() * 1000)
                    report["generated_basis"] = "RESPONSE_RECEIVED_NOT_SOURCE_GENERATION"
                    feed = {"type": "FeatureCollection", "metadata": {"status": 200, "count": 1,
                        "generated": generated}, "features": features}
                else:
                    feed = validate_feed(response.body)
                    report["generated_basis"] = "USGS_METADATA"
                if len(feed["features"]) != 1:
                    raise ContractError("Event-ID query must return exactly one event")
                item = feed["features"][0]
                aliases = {item.get("id"), *str(item.get("properties", {}).get("ids", "")).strip(",").split(",")}
                if params["eventid"] not in aliases:
                    raise ContractError("Event response does not explicitly link the requested source ID")
            else:
                feed = validate_feed(response.body)
                after = self.request(count_url).body
                if catalog_count(before) != len(feed["features"]) or catalog_count(after) != catalog_count(before):
                    raise ContractError("Catalog changed count or truncated; retry with a new epoch or smaller slices")
                ids = [f.get("id") for f in feed["features"]]
                if len(ids) != len(set(ids)):
                    raise ContractError("Catalog slice contains duplicate IDs")
                report.update(count_before=catalog_count(before), count_after=catalog_count(after), generated_basis="USGS_METADATA")
        elif response.status == 204 and kind == "BACKFILL":
            after = self.request(count_url).body
            if catalog_count(before) != 0 or catalog_count(after) != 0:
                raise ContractError("Catalog empty response disagrees with count")
            feed = {"type": "FeatureCollection", "metadata": {"status": 200, "count": 0,
                "generated": int(fetched.timestamp() * 1000)}, "features": []}
            report.update(count_before=0, count_after=0, generated_basis="RESPONSE_RECEIVED_NOT_SOURCE_GENERATION")
        if feed is not None:
            # Preserve the exact catalog response separately when the existing source contract needs a wrapper.
            payload = response.body if decode_json(response.body or b"null") == feed else json_bytes(feed)
            collection = stable_id("catalog-collection-v1", rid)
            cid = stable_id("capture-v1", collection, digest(payload))
            prefix = f"captures/{collection}/{cid}"
            m = {"manifest_version": 1, "contract_version": CONTRACT_VERSION, "collection_id": collection,
                "capture_id": cid, "environment": self.settings.environment,
                "source_id": "usgs-event-check" if kind == "EVENT_CHECK" else "usgs-catalog-m2p5",
                "source_revision": 1, "feed_key": "catalog", "run_kind": kind, "first_run_id": run_id,
                "window_start": utc_text(start), "window_end": utc_text(end), "fetched_at": utc_text(fetched),
                "source_generated_at": utc_text(datetime.fromtimestamp(feed["metadata"]["generated"] / 1000, timezone.utc)),
                "source_url": url, "response_sha256": digest(payload), "response_bytes": len(payload),
                "response_relative_path": prefix + "/response.json", "expected_item_count": len(feed["features"]),
                "source_stale": False, "state": "CAPTURED", "request_id": rid,
                "generated_basis": report["generated_basis"]}
            existing_manifest = self.volume.read_optional(prefix + "/ready.json")
            if existing_manifest is not None:
                m = decode_json(existing_manifest)
            self.volume.put_immutable(prefix + "/response.json", payload)
            self.volume.put_immutable(prefix + "/ready.json", json_bytes(m))
            self.volume.put_immutable(f"collections/{collection}/selected.json", json_bytes(
                {"manifest_version": 1, "collection_id": collection, "capture_id": cid}))
            report.update(capture_id=cid, collection_id=collection, outcome="CAPTURED", fetched_at=m["fetched_at"])
        self.volume.put_immutable(rp + "/ready.json", json_bytes(report))
        self.verify(report, rid, url)
        return report

    def verify(self, report, rid, url):
        """Check frozen request identity and original bytes before accepting a retry."""
        if report.get("request_id") != rid or report.get("url") != url:
            raise ContractError("Catalog request identity differs")
        raw = self.volume.read_optional(report["original_relative_path"])
        if raw is None or digest(raw) != report["original_sha256"]:
            raise ContractError("Catalog original response missing or corrupt")
        if report["capture_id"]:
            source = RequestSource("usgs-event-check" if report["kind"] == "EVENT_CHECK" else "usgs-catalog-m2p5", url)
            Collector(self.settings, self.client, self.volume)._load(source, report["collection_id"], report["capture_id"])

    def backfill(self, start, end, run_id):
        """Split capped one-day slices instead of paging a mutating live catalog."""
        if (start >= end or end > self.now() or end - start > timedelta(days=7)
                or self.now() - start > timedelta(days=365)
                or any(t != t.replace(hour=0, minute=0, second=0, microsecond=0) for t in (start, end))):
            raise ContractError("Backfill requires 1..7 complete UTC days within the previous year")
        queue, cursor, results = [], start, []
        while cursor < end:
            stop = min(cursor + timedelta(days=1), end)
            queue.append((cursor, stop))
            cursor = stop
        while queue:
            first, last = queue.pop(0)
            # USGS endpoints are inclusive; subtract one millisecond for our half-open slices.
            params = {"starttime": utc_text(first), "endtime": utc_text(last - timedelta(milliseconds=1)),
                      "minmagnitude": "2.5", "limit": "10000", "orderby": "time-asc"}
            report = self.collect(params, "BACKFILL", run_id, first, last)
            if report["outcome"] == "SPLIT_REQUIRED":
                if last - first <= timedelta(minutes=1):
                    raise BudgetExceeded("Dense one-minute catalog slice exceeds the accepted bound")
                middle = first + timedelta(milliseconds=int((last - first).total_seconds() * 500))
                queue[0:0] = [(first, middle), (middle, last)]
            else:
                results.append(report)
        return results


def latest_plan(volume):
    """Select the newest frozen event-check plan with a bounded metadata inventory."""
    ids = volume.child_directories("reconciliation/plans")
    if len(ids) > MAX_INVENTORY:
        raise BudgetExceeded("Reconciliation plan inventory exceeds 10000")
    plans = [decode_json(volume.read_optional(f"reconciliation/plans/{pid}/plan.json")) for pid in ids]
    if not plans:
        raise ContractError("No reconciliation plan; run the preparation Job")
    return max(plans, key=lambda p: (p["created_at"], p["plan_id"]))


def gap_plan(volume, kafka, stream):
    """Freeze expired offset ranges and every pending selected source before a reset."""
    bounds = []
    for partition in kafka.partitions():
        beginning, end, committed = kafka.bounds(partition)
        if committed is not None and committed > end:
            raise ContractError("Checkpoint is beyond topic end; use a new stream generation after recreation")
        if (committed is None and beginning > 0) or (committed is not None and committed < beginning):
            bounds.append({"partition": partition, "retained_start": beginning,
                "exclusive_end": end, "old_committed_next": committed})
    if not bounds:
        raise ContractError("No expired offset gap exists")
    pending = inventory(volume)
    body = {"gap_version": 1, "stream_id": stream_id(stream), "bounds": bounds,
        "capture_ids": sorted(m["capture_id"] for m in pending), "scope": "ALL_SELECTED_FROZEN_CAPTURES"}
    gid = stable_id("offset-gap-v1", body)
    volume.put_immutable(f"reconciliation/gaps/{gid}/plan.json", json_bytes(body))
    return gid


def repair_gap(settings, client, volume, kafka, stream, gid, run_id, emit):
    """Reset only a reviewed range after its replayable captures have been fully verified."""
    if not re.fullmatch("[0-9a-f]{64}", gid or ""):
        raise ContractError("Repair requires the exact --gap-plan-id printed by gap-plan")
    raw = volume.read_optional(f"reconciliation/gaps/{gid}/plan.json")
    if raw is None:
        raise ContractError("Unknown reviewed Kafka gap plan")
    plan = decode_json(raw)
    if stable_id("offset-gap-v1", plan) != gid or plan["stream_id"] != stream_id(stream):
        raise ContractError("Kafka gap plan does not match this stream")
    completed = volume.read_optional(f"reconciliation/gaps/{gid}/ready.json")
    if completed is not None:
        if decode_json(completed).get("plan_sha256") != digest(raw):
            raise ContractError("Kafka gap receipt differs from its plan")
        return
    by_id = {m["capture_id"]: m for m in inventory(volume)}
    if set(by_id) - set(plan["capture_ids"]):
        raise ContractError("New pending captures appeared; drain them before planning the gap again")
    verified = []
    for cid in plan["capture_ids"]:
        m = by_id.get(cid)
        if m is None:
            # An already-accounted capture is an idempotent completed source outcome.
            receipt = volume.read_optional("receipts/captures/" + cid + ".json")
            if receipt is None or decode_json(receipt).get("state") != "ACCOUNTED":
                raise ContractError("Gap source disappeared without accounting proof")
            continue
        verified.append(Collector(settings, client, volume)._load(checked_source(m), m["collection_id"], cid))
    if sum(m["expected_item_count"] for m in verified) > stream.max_land_records:
        raise BudgetExceeded("Gap replay exceeds 10000 source items; review a staged recovery plan")
    for bound in plan["bounds"]:
        beginning, end, committed = kafka.bounds(bound["partition"])
        if beginning != bound["retained_start"] or end < bound["exclusive_end"]:
            raise ContractError("Broker retained range changed; make a new gap plan")
        if committed == bound["old_committed_next"]:
            try:
                kafka.commit(bound["partition"], beginning, gid)
            except Exception:
                if kafka.bounds(bound["partition"])[2] != beginning:
                    raise
            if kafka.bounds(bound["partition"])[2] != beginning:
                raise ContractError("Reviewed Kafka checkpoint reset did not take effect")
        elif committed is None or committed < beginning:
            raise ContractError("Kafka checkpoint differs from the reviewed gap plan")
    replay_stream = replace(stream, dispatch_generation="gap_" + gid[:24])
    for m in verified:
        dispatch(volume, kafka, replay_stream, m, emit)
    land(volume, kafka, stream, run_id, emit)
    volume.put_immutable(f"reconciliation/gaps/{gid}/ready.json", json_bytes({"gap_id": gid,
        "plan_sha256": digest(raw), "state": "SOURCE_REPLAY_LANDED", "scope": "SELECTED_CAPTURE_EVIDENCE_ONLY"}))


def main(argv=None):
    """Run recovery/cycle/backfill in one bounded cloud workflow without local data files."""
    parser = argparse.ArgumentParser(description="Sprint 7 source recovery and reconciliation")
    parser.add_argument("--action", choices=("recover", "cycle", "backfill", "gap-plan", "repair-gap"), default="recover")
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--gap-plan-id", default="")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--max-captures", type=int, default=16)
    args = parser.parse_args(argv)
    kafka, volume, report, events = None, None, None, []
    succeeded = False
    try:
        settings = replace(Settings.from_env(), max_requests=6000, run_seconds=8400)
        settings.require_cloud()
        if settings.environment != "dev":
            raise ContractError("Sprint 7 requires the development environment")
        stream = KafkaSettings.from_env(os.environ)
        events, run_id = [], stable_id("reconciliation-run-v1", args.epoch, os.getenv("GITHUB_RUN_ID", uuid.uuid4().hex), os.getenv("GITHUB_RUN_ATTEMPT", "1"))
        client = HTTPClient(settings, events.append)
        volume, kafka = Volume(settings, client), KafkaClient(stream)
        now_report = utc_text(datetime.now(timezone.utc))
        report = {"report_version": 1, "run_id": run_id, "run_kind": "RECOVERY",
            "github_run_id": os.getenv("GITHUB_RUN_ID", "manual"), "github_attempt": int(os.getenv("GITHUB_RUN_ATTEMPT", "1")),
            "code_commit": os.getenv("GITHUB_SHA", "unrecorded"), "started_at": now_report,
            "window_start": now_report, "window_end": now_report}
        bind_stream(volume, kafka, stream)
        raw_job = os.environ.get("EQ_HANDOFF_LEDGER_JOB_ID", "")
        if not raw_job.isdigit():
            raise ContractError("Set EQ_HANDOFF_LEDGER_JOB_ID to the existing serialized Job")
        job_id = int(raw_job)

        def launch(phase):
            """Use stable per-phase launch tokens and force maintenance off during ingestion."""
            return sync_ledger(settings, job_id, stable_id("s7-job-v1", run_id, phase), client,
                wait_seconds=2400, parameters={"gold_action": "preview", "reconcile_mode": phase,
                    "maintenance_action": "none", "volume_root": settings.volume_root})

        if args.action == "gap-plan":
            print("GAP_PLAN_ID=" + gap_plan(volume, kafka, stream), flush=True)
            succeeded = True
            return 0
        if args.action == "repair-gap":
            repair_gap(settings, client, volume, kafka, stream, args.gap_plan_id, run_id,
                lambda event: print(json_bytes(event).decode(), flush=True))
            launch("verify")
            print("PASS Sprint 7 reviewed offset-gap recovery")
            succeeded = True
            return 0
        if args.action in ("cycle", "backfill"):
            # Drain pre-existing frozen evidence before requesting new source observations.
            recover_captures(settings, client, volume, kafka, stream,
                lambda event: print(json_bytes(event).decode(), flush=True), args.max_captures)
            land(volume, kafka, stream, run_id, lambda event: print(json_bytes(event).decode(), flush=True))
            launch("plan")
            catalog = CatalogCollector(settings, client, volume, args.epoch)
            now = datetime.now(timezone.utc)
            if args.action == "cycle":
                # A new epoch is an explicit new observation, while retrying it reuses its captures.
                collector = Collector(replace(settings, collection_generation=args.epoch[:32]), client, volume)
                for mode, sources in SOURCES.items():
                    first, last = collection_window(mode, None, now)
                    for source in sources:
                        collector.collect(source, mode, first, last, run_id, True)
                plan = latest_plan(volume)
                if now - parse_utc(plan["created_at"]) > timedelta(hours=1):
                    raise ContractError("Event-check plan is too old; prepare another run")
                for event_id in plan["event_ids"]:
                    catalog.collect({"eventid": event_id}, "EVENT_CHECK", run_id, now, now)
            else:
                if not args.start or not args.end:
                    raise ContractError("Backfill requires --start and --end UTC timestamps")
                catalog.backfill(parse_utc(args.start), parse_utc(args.end), run_id)
        metrics = recover_captures(settings, client, volume, kafka, stream,
            lambda event: print(json_bytes(event).decode(), flush=True), args.max_captures)
        land(volume, kafka, stream, run_id, lambda event: print(json_bytes(event).decode(), flush=True))
        launch("verify")
        print("PASS Sprint 7 recovery " + json_bytes(metrics).decode())
        if metrics["remaining_captures"]:
            raise BudgetExceeded("Recovery made bounded progress; rerun to drain remaining captures")
        succeeded = True
        return 0
    except Exception as error:
        code = error.code if isinstance(error, PipelineError) else type(error).__name__
        message = str(error) if isinstance(error, PipelineError) else "Unexpected runtime failure; inspect this workflow and retry the frozen work"
        print(json_bytes({"event": "RECONCILIATION_FAILED", "error_code": code, "message": message}).decode(), flush=True)
        return 1
    finally:
        if volume is not None and report is not None:
            report.update(status="SUCCESS" if succeeded else "FAILED", attempts=events.copy(),
                finished_at=utc_text(datetime.now(timezone.utc)), error_class=None if succeeded else "RECOVERY_INCOMPLETE")
            try:
                volume.put_immutable("runs/" + report["run_id"] + "/report.json", json_bytes(report))
            except Exception:
                print("DIAGNOSTIC_UPLOAD_FAILED; inspect this Actions run", flush=True)
        if kafka is not None:
            try:
                kafka.close()
            except Exception:
                print("KAFKA_CLOSE_FAILED; offsets are committed only after verified landing", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())

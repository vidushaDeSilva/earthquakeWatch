"""Run configuration preflight, read-only feed checks, or immutable cloud capture.

This CLI logs only operational metadata. Live capture reports are saved in the
Volume so the Databricks ledger notebook can import them when compute is available.
"""

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from .capture import Collector
from .config import Settings
from .contracts import SOURCES, collection_window, json_bytes, stable_id, utc_text
from .errors import PipelineError
from .http import HTTPClient
from .volume import Volume


def emit(event: dict[str, Any]) -> None:
    """Print a structured, payload-free operational record."""
    print(json_bytes(event).decode("utf-8"), flush=True)


def main(argv: list[str] | None = None) -> int:
    """Execute one bounded collection request and return a meaningful process status."""
    parser = argparse.ArgumentParser(description="Sprint 1–2 source capture; no Kafka dispatch")
    parser.add_argument("--action", choices=("preflight", "dry-run", "capture"), default="preflight")
    parser.add_argument("--mode", choices=tuple(SOURCES), default="hourly")
    parser.add_argument("--window-end", default=None, help="Aligned ISO UTC end; blank chooses current interval")
    args = parser.parse_args(argv)
    started = datetime.now(timezone.utc)
    run_id = stable_id("run-v1", os.getenv("GITHUB_RUN_ID", uuid.uuid4().hex),
                       os.getenv("GITHUB_RUN_ATTEMPT", "1"), args.action, args.mode)
    events: list[dict[str, Any]] = []
    volume, report = None, None
    try:
        settings = Settings.from_env()
        start, end = collection_window(args.mode, args.window_end, started)
        if args.action != "dry-run":
            settings.require_cloud()
        emit({"event": "PREFLIGHT_PASS", "run_id": run_id, "environment": settings.environment,
              "window_start": utc_text(start), "window_end": utc_text(end), "mode": args.mode,
              "volume_root": settings.volume_root})
        if args.action == "preflight":
            print("PASS Sprint 1 configuration preflight (no network or writes)")
            return 0
        client = HTTPClient(settings, events.append)
        volume = Volume(settings, client) if args.action == "capture" else None
        collector = Collector(settings, client, volume)
        report = {"report_version": 1, "run_id": run_id, "run_kind": args.mode.upper(),
                  "action": args.action, "github_run_id": os.getenv("GITHUB_RUN_ID", "manual"),
                  "github_attempt": int(os.getenv("GITHUB_RUN_ATTEMPT", "1")),
                  "code_commit": os.getenv("GITHUB_SHA", "unrecorded"),
                  "window_start": utc_text(start), "window_end": utc_text(end),
                  "started_at": utc_text(started), "status": "RUNNING", "captures": []}
        for source in SOURCES[args.mode]:
            manifest, reused = collector.collect(source, args.mode, start, end, run_id,
                                                 args.action == "capture")
            report["captures"].append(manifest["capture_id"])
            emit({"event": "CAPTURE_VERIFIED" if volume else "FEED_VALIDATED", "source_id": source.source_id,
                  "capture_id": manifest["capture_id"], "collection_id": manifest["collection_id"],
                  "expected_item_count": manifest["expected_item_count"], "reused": reused,
                  "source_stale": manifest["source_stale"]})
        report.update(status="SUCCESS", finished_at=utc_text(datetime.now(timezone.utc)), attempts=events.copy())
        if volume:
            volume.put_immutable(f"runs/{run_id}/report.json", json_bytes(report))
        print("PASS Sprint 2 cloud source capture" if volume else "PASS Sprint 2 read-only USGS validation")
        return 0
    except PipelineError as error:
        emit({"event": "PIPELINE_FAILED", "run_id": run_id, "error_code": error.code, "message": str(error)})
        if volume and report:
            report.update(status="FAILED", error_class=error.code,
                          finished_at=utc_text(datetime.now(timezone.utc)), attempts=events.copy())
            try:
                volume.put_immutable(f"runs/{run_id}/report.json", json_bytes(report))
            except PipelineError:
                emit({"event": "DIAGNOSTIC_UPLOAD_FAILED", "run_id": run_id})
        return 75 if error.code in {"RETRY_DEFERRED", "BUDGET_EXCEEDED"} else 1


if __name__ == "__main__":
    sys.exit(main())

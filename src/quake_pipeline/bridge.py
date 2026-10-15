"""Run bounded Kafka dispatch, landing, recovery, or metadata inspection in the cloud.

Only metadata is printed. All source/delivery bytes remain in memory or the
Databricks managed Volume; failures never enable automatic Kafka offset reset.
"""

import argparse
import logging
import os
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from .capture import Collector
from .config import Settings
from .contracts import SOURCES, collection_window, json_bytes, stable_id, utc_text
from .errors import PipelineError
from .http import HTTPClient
from .kafka_client import KafkaClient
from .kafka_config import KafkaSettings
from .landing import land, recover
from .outbox import bind_stream, dispatch, load_capture, stream_id
from .volume import Volume


def emit(event: dict[str, Any]) -> None:
    """Print safe event metadata, never raw broker messages or credentials."""
    print(json_bytes(event).decode(), flush=True)


def main(argv: list[str] | None = None) -> int:
    """Execute one serial operation and write its terminal cloud run report."""
    parser = argparse.ArgumentParser(description="Sprint 3 durable Kafka cloud handoff")
    parser.add_argument("--action", choices=("dispatch", "land", "recover", "inspect"), required=True)
    parser.add_argument("--mode", choices=tuple(SOURCES), default="hourly")
    parser.add_argument("--window-end", default=None)
    parser.add_argument("--fail-at", choices=("none", "after-produce", "after-data", "after-manifest", "after-commit"), default="none")
    args = parser.parse_args(argv)
    started = datetime.now(timezone.utc)
    run_id = stable_id("bridge-run-v1", os.getenv("GITHUB_RUN_ID", uuid.uuid4().hex),
                       os.getenv("GITHUB_RUN_ATTEMPT", "1"), args.action, args.mode)
    kafka, volume, report, identity = None, None, None, None
    status = 1
    try:
        settings = replace(Settings.from_env(), max_requests=1500)
        settings.require_cloud()
        if settings.environment != "dev":
            raise ValueError("Sprint 3 dev environment required")
        stream = KafkaSettings.from_env(os.environ)
        identity = stream_id(stream)
        start, end = collection_window(args.mode, args.window_end, started)
        attempts = []
        report = {"report_version": 1, "run_id": run_id, "run_kind": "RECOVERY" if args.action == "recover" else args.mode.upper(),
                  "action": args.action, "github_run_id": os.getenv("GITHUB_RUN_ID", "manual"),
                  "github_attempt": int(os.getenv("GITHUB_RUN_ATTEMPT", "1")), "code_commit": os.getenv("GITHUB_SHA", "unrecorded"),
                  "started_at": utc_text(started), "window_start": utc_text(start), "window_end": utc_text(end),
                  "stream_id": identity, "status": "RUNNING", "events": []}
        client = HTTPClient(settings, attempts.append)
        volume = Volume(settings, client)
        kafka = KafkaClient(stream)
        identity, topology = bind_stream(volume, kafka, stream, persist=args.action != "inspect")
        emit({"event": "STREAM_VERIFIED", "stream_id": identity, "topic": stream.topic,
              "partitions": topology["partitions"], "consumer_group": stream.group,
              "stream_generation": stream.generation, "topic_config": topology["topic_config"]})

        def record_event(value: dict[str, Any]) -> None:
            """Keep bounded operational events for the next Delta ledger sync."""
            report["events"].append(value)
            emit(value)

        if args.action == "dispatch":
            collector = Collector(settings, client, volume)
            for source in SOURCES[args.mode]:
                manifest = load_capture(collector, source, args.mode, start, end)
                dispatch(volume, kafka, stream, manifest, record_event, args.fail_at)
        elif args.action == "land":
            land(volume, kafka, stream, run_id, record_event, args.fail_at)
        elif args.action == "recover":
            recover(volume, kafka, stream, record_event, args.fail_at)
        else:
            for partition in topology["partitions"]:
                beginning, end_offset, committed = kafka.bounds(partition)
                emit({"event": "OFFSET_INSPECTION", "partition": partition,
                      "retained_start": beginning, "exclusive_end": end_offset,
                      "committed_next": committed,
                      "retained_lag": end_offset - max(beginning, committed or 0)})
        report.update(status="SUCCESS", finished_at=utc_text(datetime.now(timezone.utc)), attempts=attempts.copy())
        if args.action != "inspect":
            volume.put_immutable(f"handoff/{identity}/runs/{run_id}/report.json", json_bytes(report))
        print("PASS Sprint 3 " + args.action, flush=True)
        status = 0
    except Exception as error:
        if isinstance(error, PipelineError):
            code, message = error.code, str(error)
        else:
            # Upstream Kafka/SDK exceptions can contain connection/auth details.
            code, message = "KAFKA_OR_RUNTIME_FAILURE", "External operation failed; inspect configuration and retry safely"
        emit({"event": "BRIDGE_FAILED", "run_id": run_id, "error_code": code, "message": message})
        if volume is not None and report is not None and args.action != "inspect":
            report.update(status="FAILED", error_class=code, finished_at=utc_text(datetime.now(timezone.utc)),
                          attempts=attempts.copy())
            try:
                volume.put_immutable(f"handoff/{identity}/runs/{run_id}/report.json", json_bytes(report))
            except Exception:
                emit({"event": "DIAGNOSTIC_UPLOAD_FAILED", "run_id": run_id})
    finally:
        if kafka is not None:
            try:
                kafka.close()
            except Exception:
                logging.getLogger(__name__).warning("Kafka close failed; no automatic offset commit was attempted")
    return status


if __name__ == "__main__":
    raise SystemExit(main())

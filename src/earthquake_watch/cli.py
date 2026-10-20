"""Small, explicit entry points for capture, publish, and bounded landing."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path

from .capture import capture_once
from .contracts import ContractError, SourceConfig
from .volume_client import DatabricksVolume


CONFIG = Path(__file__).resolve().parents[2] / "config" / "source_usgs_all_day.json"


def main() -> None:
    """Run one phase action; never chain phases or launch a Databricks Job."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("validate-config", "capture", "publish-capture", "land-once"))
    parser.add_argument("--scheduled-time", help="UTC ISO-8601 schedule time, e.g. 2026-10-19T19:17:00Z")
    parser.add_argument("--capture-id", help="64-character frozen capture identity")
    args = parser.parse_args()
    cfg = SourceConfig.load(CONFIG)
    for name in ("impact_policy_v1.json", "retention_v1.json"):
        value = json.loads((CONFIG.parent / name).read_text(encoding="utf-8"))
        if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
            raise ContractError(f"Unsupported {name}")
    if args.command == "validate-config":
        print("Phase 0 configuration contracts valid")
        return
    volume = DatabricksVolume(os.environ.get("DATABRICKS_HOST", ""), os.environ.get("DATABRICKS_TOKEN", ""))
    if args.command == "capture":
        if not args.scheduled_time:
            parser.error("capture requires --scheduled-time")
        scheduled = datetime.fromisoformat(args.scheduled_time.replace("Z", "+00:00"))
        result = capture_once(scheduled, cfg, volume)
        print(json.dumps(result.__dict__, sort_keys=True))
    elif args.command == "publish-capture":
        if not args.capture_id:
            parser.error("publish-capture requires --capture-id")
        from confluent_kafka import Producer
        from .kafka_transport import kafka_config, publish_capture

        receipt = publish_capture(volume, args.capture_id, cfg, Producer(kafka_config()))
        print(json.dumps(receipt, sort_keys=True))
    else:
        from confluent_kafka import Consumer, TopicPartition
        from .kafka_transport import GROUP, kafka_config
        from .landing import land_once

        consumer = Consumer(kafka_config(GROUP))
        try:
            manifest = land_once(volume, consumer, TopicPartition)
            print(json.dumps(manifest if manifest is not None else {"status": "NO_RECORDS"}, sort_keys=True))
        finally:
            consumer.close()


if __name__ == "__main__":
    main()

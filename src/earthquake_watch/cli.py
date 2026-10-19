"""Offline contract validation and explicit one-shot cloud capture."""

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
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("validate-config", "capture"))
    parser.add_argument("--scheduled-time", help="UTC ISO-8601 schedule time, e.g. 2026-10-19T19:17:00Z")
    args = parser.parse_args()
    cfg = SourceConfig.load(CONFIG)
    for name in ("impact_policy_v1.json", "retention_v1.json"):
        value = json.loads((CONFIG.parent / name).read_text(encoding="utf-8"))
        if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
            raise ContractError(f"Unsupported {name}")
    if args.command == "validate-config":
        print("Phase 0 configuration contracts valid")
        return
    if not args.scheduled_time:
        parser.error("capture requires --scheduled-time")
    scheduled = datetime.fromisoformat(args.scheduled_time.replace("Z", "+00:00"))
    volume = DatabricksVolume(os.environ.get("DATABRICKS_HOST", ""), os.environ.get("DATABRICKS_TOKEN", ""))
    result = capture_once(scheduled, cfg, volume)
    print(json.dumps(result.__dict__, sort_keys=True))


if __name__ == "__main__":
    main()

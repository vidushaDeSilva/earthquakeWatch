"""Plain-language explanations sourced from the repository's reviewed config."""

from __future__ import annotations

import json
from pathlib import Path


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def load_context(config_dir: Path = CONFIG_DIR) -> tuple[dict, dict]:
    """Read the source and impact policy bundled with this repository."""
    source = json.loads((config_dir / "source_usgs_all_day.json").read_text(encoding="utf-8"))
    policy = json.loads((config_dir / "impact_policy_v1.json").read_text(encoding="utf-8"))
    if source.get("source_id") != "usgs-all-day" or policy.get("policy_version") != "impact-v1":
        raise ValueError("Unsupported source or impact policy for dashboard explanation")
    return source, policy


def describe_rule(rule: dict) -> str:
    """Translate each reviewed policy rule without changing its comparison."""
    field = rule["field"]
    if field == "pager_alert" and "equals" in rule:
        return f"USGS PAGER alert is {rule['equals']}"
    if field == "tsunami_flag" and rule.get("equals") is True:
        return "USGS tsunami flag is set"
    if field == "max_mmi" and "gte" in rule:
        return f"maximum ShakeMap intensity (MMI) ≥ {rule['gte']}"
    if field == "magnitude" and "gte" in rule:
        return f"magnitude ≥ {rule['gte']}"
    if field == "significance" and "gt" in rule:
        return f"USGS significance > {rule['gt']}"
    raise ValueError(f"Unrecognized watchlist rule: {rule.get('code', 'unknown')}")


def watchlist_explanation(policy: dict) -> tuple[str, list[str]]:
    """Describe the 24-hour event scope and the OR between policy triggers."""
    window = policy["window_hours"]
    scope = (f"The active publication considers unique current Silver records of type "
             f"`earthquake` whose origin time falls in the rolling {window}-hour "
             f"window ending at publication time (start included, end excluded). "
             "An event enters the watchlist if **any one** of these signals matches:")
    rules = [describe_rule(rule) for rule in policy["rules"]]
    return scope, rules

"""Evaluate impact-v1 and build deterministic watchlist and daily metric rows.

This is editorial prioritization, not an estimate of actual damage or an alert.
"""

from datetime import datetime, time, timedelta, timezone
import hashlib
import json
from typing import Any

from .contracts import ContractError


EXPECTED_RULES = (
    ("PAGER_RED", "pager_alert", "equals", "red", 0),
    ("PAGER_ORANGE", "pager_alert", "equals", "orange", 1),
    ("PAGER_YELLOW", "pager_alert", "equals", "yellow", 2),
    ("TSUNAMI_FLAG", "tsunami_flag", "equals", True, 3),
    ("SHAKEMAP_MMI_GTE_6", "max_mmi", "gte", 6, 4),
    ("MAG_GTE_6", "magnitude", "gte", 6, 5),
    ("SIG_GT_600", "significance", "gt", 600, 6),
)


def load_policy(raw: bytes) -> tuple[dict[str, Any], str]:
    """Reject unknown or modified policy semantics and return the exact byte hash."""
    try:
        policy = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ContractError("Invalid impact policy JSON") from exc
    if (not isinstance(policy, dict) or policy.get("schema_version") != 1
            or policy.get("policy_version") != "impact-v1"
            or policy.get("product_key") != "usgs-watchlist"
            or policy.get("window_hours") != 24
            or policy.get("max_source_age_minutes") != 180):
        raise ContractError("Unsupported impact policy")
    rules = policy.get("rules")
    if (not isinstance(rules, list) or len(rules) != len(EXPECTED_RULES)
            or any(not isinstance(rule, dict) or set(rule) != {"code", "field", operator, "priority"}
                   or (rule["code"], rule["field"], rule["priority"]) != (code, field, priority)
                   or rule[operator] != value
                   for rule, (code, field, operator, value, priority) in zip(rules, EXPECTED_RULES))):
        raise ContractError("Impact policy differs from reviewed v1")
    return policy, hashlib.sha256(raw).hexdigest()


def reasons(event: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    """Return every matching reason in priority order with null-safe comparisons."""
    result = []
    for rule in policy["rules"]:
        value = event.get(rule["field"])
        if value is None:
            continue
        if "equals" in rule:
            matched = type(value) is type(rule["equals"]) and value == rule["equals"]
        else:
            matched = (type(value) in (int, float) and
                       (value >= rule["gte"] if "gte" in rule else value > rule["gt"]))
        if matched:
            result.append(rule["code"])
    return result


def build_rows(events: list[dict[str, Any]], publication_id: str,
               as_of: datetime, policy: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Use origin time in a half-open rolling window and emit zero-count dates."""
    if as_of.tzinfo is None:
        raise ContractError("Publication time must be timezone-aware")
    as_of = as_of.astimezone(timezone.utc)
    start = as_of - timedelta(hours=policy["window_hours"])
    current = []
    seen = set()
    for event in events:
        occurred = event["event_time"]
        if occurred.tzinfo is None:
            raise ContractError("Event time must be timezone-aware")
        if not start <= occurred.astimezone(timezone.utc) < as_of or event.get("event_type") != "earthquake":
            continue
        if event["usgs_event_id"] in seen:
            raise ContractError("Duplicate current Silver event ID")
        seen.add(event["usgs_event_id"])
        current.append(event)

    watchlist = []
    for event in current:
        matched = reasons(event, policy)
        if not matched:
            continue
        watchlist.append({
            "publication_id": publication_id, "usgs_event_id": event["usgs_event_id"],
            "event_version_id": event["event_version_id"], "policy_version": policy["policy_version"],
            "event_time": event["event_time"], "magnitude": event["magnitude"],
            "place": event["place"], "longitude": event["longitude"], "latitude": event["latitude"],
            "depth_km": event["depth_km"], "significance": event["significance"],
            "pager_alert": event["pager_alert"], "max_mmi": event["max_mmi"],
            "tsunami_flag": event["tsunami_flag"], "selection_reasons": matched,
            "impact_label": "ESTIMATED_IMPACT" if any(x.startswith("PAGER_") for x in matched)
                            else "POTENTIAL_IMPACT",
            "rank_priority": next(r["priority"] for r in policy["rules"] if r["code"] == matched[0]),
            "as_of_utc": as_of,
        })
    watchlist.sort(key=lambda r: (r["rank_priority"], -(r["significance"] if r["significance"] is not None else -1),
                                  -(r["magnitude"] if r["magnitude"] is not None else -100),
                                  -r["event_time"].timestamp(), r["usgs_event_id"]))
    eligible = {r["usgs_event_id"] for r in watchlist}
    metrics = []
    day = start.date()
    while day <= as_of.date():
        bucket = [r for r in current if r["event_time"].astimezone(timezone.utc).date() == day]
        magnitudes = [r["magnitude"] for r in bucket if r["magnitude"] is not None]
        day_start = datetime.combine(day, time.min, timezone.utc)
        metrics.append({
            "publication_id": publication_id, "metric_date": day,
            "event_count": len(bucket), "watchlist_count": sum(r["usgs_event_id"] in eligible for r in bucket),
            "max_magnitude": max(magnitudes) if magnitudes else None,
            "pager_yellow_count": sum(r["pager_alert"] == "yellow" for r in bucket),
            "pager_orange_count": sum(r["pager_alert"] == "orange" for r in bucket),
            "pager_red_count": sum(r["pager_alert"] == "red" for r in bucket),
            "tsunami_flag_count": sum(r["tsunami_flag"] is True for r in bucket),
            "coverage_status": ("FULL_DAY" if start <= day_start and
                                day_start + timedelta(days=1) <= as_of else "PARTIAL_DAY"),
            "as_of_utc": as_of,
        })
        day += timedelta(days=1)
    return watchlist, metrics

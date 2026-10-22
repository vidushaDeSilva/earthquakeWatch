"""Regression tests for status and delivery-count semantics."""

from datetime import datetime, timezone

from dashboard.metrics import duration_minutes, landing_coverage, summarize
from dashboard.content import describe_rule, watchlist_explanation


def test_submitted_run_is_not_counted_as_success() -> None:
    """A queued dispatch remains nonterminal until the finalizer writes status."""
    runs = [
        {"run_status": "SUCCEEDED", "submitted_at": "2026-10-22T08:00:00Z",
         "finished_at": "2026-10-22T08:03:00Z"},
        {"run_status": "RUNNING", "submitted_at": "2026-10-22T09:00:00Z",
         "finished_at": None},
    ]
    summary = summarize(runs, [], [])
    assert summary["latest_run"]["run_status"] == "RUNNING"
    assert summary["latest_success"]["run_status"] == "SUCCEEDED"
    assert (summary["terminal_count"], summary["success_count"],
            summary["pending_count"]) == (1, 1, 1)


def test_duration_and_landing_counts_keep_their_meaning() -> None:
    """Duration is UTC elapsed time; landing evidence is deliveries, not events."""
    assert duration_minutes({"started_at": datetime(2026, 10, 22, 9, tzinfo=timezone.utc),
                             "finished_at": "2026-10-22T09:02:30Z"}) == 2.5
    assert duration_minutes({"started_at": None, "finished_at": None}) is None
    assert landing_coverage({"landed_record_count": 200,
                             "expected_record_count": 200}) == "200 / 200"


def test_watchlist_copy_preserves_window_and_strict_threshold() -> None:
    """User-facing criteria must retain the reviewed policy's OR and > semantics."""
    policy = {"window_hours": 24, "rules": [
        {"field": "magnitude", "gte": 6},
        {"field": "significance", "gt": 600},
    ]}
    scope, rules = watchlist_explanation(policy)
    assert "rolling 24-hour" in scope and "any one" in scope
    assert rules == ["magnitude ≥ 6", "USGS significance > 600"]
    assert describe_rule({"field": "pager_alert", "equals": "yellow"}) == \
        "USGS PAGER alert is yellow"

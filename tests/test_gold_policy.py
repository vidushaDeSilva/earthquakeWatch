"""Regression tests for deterministic impact-v1 semantics and calendar boundaries."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from earthquake_watch.contracts import ContractError
from earthquake_watch.gold_policy import build_rows, load_policy, reasons
import importlib.util


POLICY_PATH = Path(__file__).resolve().parents[1] / "config" / "impact_policy_v1.json"
AS_OF = datetime(2026, 10, 20, 12, 17, tzinfo=timezone.utc)


def event(event_id: str, occurred: datetime, **overrides):
    """Make one normalized Silver row with only policy-relevant fields."""
    item = dict(usgs_event_id=event_id, event_version_id=event_id + "-v1",
                event_time=occurred, event_type="earthquake", magnitude=3.0,
                place="test", longitude=0.0, latitude=0.0, depth_km=5.0,
                significance=None, pager_alert=None, max_mmi=None, tsunami_flag=False)
    item.update(overrides)
    return item


def policy():
    """Read the versioned policy file tested in the deployment."""
    return load_policy(POLICY_PATH.read_bytes())[0]


def test_databricks_timestamp_normalization_handles_spark_naive_utc():
    """Spark UTC-naive and Python aware timestamps compare without TypeError."""
    path = Path(__file__).resolve().parents[1] / "databricks" / "publish_gold.py"
    spec = importlib.util.spec_from_file_location("publish_gold", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    naive = datetime(2026, 10, 21, 12, 0)
    aware = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
    assert module._utc_datetime(naive) == aware
    assert module._utc_datetime(aware) == aware


def test_policy_checksum_and_all_rules():
    """The JSON is immutable in semantics and collects every matching reason."""
    settings, checksum = load_policy(POLICY_PATH.read_bytes())
    assert len(checksum) == 64
    item = event("a", AS_OF - timedelta(minutes=1), pager_alert="red", magnitude=6,
                 significance=601, max_mmi=6, tsunami_flag=True)
    assert reasons(item, settings) == ["PAGER_RED", "TSUNAMI_FLAG", "SHAKEMAP_MMI_GTE_6",
                                        "MAG_GTE_6", "SIG_GT_600"]
    assert reasons(event("b", AS_OF, magnitude=None, max_mmi=None), settings) == []


def test_policy_rejects_unreviewed_threshold():
    """Changing a threshold requires deliberate versioned policy work."""
    with pytest.raises(ContractError):
        load_policy(POLICY_PATH.read_bytes().replace(b'"gt": 600', b'"gt": 500'))


def test_half_open_window_zero_bucket_and_priority():
    """Corrected origin times move counts, and a candidate can be empty."""
    start = AS_OF - timedelta(hours=24)
    rows, daily = build_rows([
        event("start", start, magnitude=6),
        event("red", AS_OF - timedelta(hours=1), pager_alert="red", magnitude=6),
        event("excluded", AS_OF, magnitude=9),
        event("blast", AS_OF - timedelta(hours=2), magnitude=7, event_type="quarry blast"),
    ], "publication", AS_OF, policy())
    assert [r["usgs_event_id"] for r in rows] == ["red", "start"]
    assert rows[0]["impact_label"] == "ESTIMATED_IMPACT"
    assert rows[1]["impact_label"] == "POTENTIAL_IMPACT"
    assert [r["event_count"] for r in daily] == [1, 1]
    assert [r["watchlist_count"] for r in daily] == [1, 1]
    assert all(r["coverage_status"] == "PARTIAL_DAY" for r in daily)
    empty_rows, empty_days = build_rows([], "empty", AS_OF, policy())
    assert empty_rows == [] and len(empty_days) == 2
    assert all(r["event_count"] == 0 for r in empty_days)


def test_duplicate_current_event_is_blocked():
    """A duplicate Silver current key is a blocking integrity error."""
    row = event("same", AS_OF - timedelta(hours=1))
    with pytest.raises(ContractError):
        build_rows([row, row], "candidate", AS_OF, policy())

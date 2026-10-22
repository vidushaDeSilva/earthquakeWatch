"""Pure dashboard summaries; keeping status semantics independent of the UI."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def as_utc(value: Any) -> datetime | None:
    """Normalize a SQL timestamp to aware UTC without changing the instant."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def latest_by(rows: list[dict], field: str) -> dict | None:
    """Find the latest non-null timestamp across bounded source rows."""
    valid = [row for row in rows if as_utc(row.get(field)) is not None]
    return max(valid, key=lambda row: as_utc(row[field])) if valid else None


def summarize(runs: list[dict], captures: list[dict], batches: list[dict]) -> dict:
    """Keep submitted and terminal states distinct; avoid join multiplication."""
    terminal = [r for r in runs if r.get("run_status") in {"SUCCEEDED", "FAILED"}]
    succeeded = [r for r in terminal if r["run_status"] == "SUCCEEDED"]
    return {
        "latest_capture": latest_by(captures, "fetched_at"),
        "latest_batch": latest_by(batches, "created_at"),
        "latest_run": latest_by(runs, "submitted_at"),
        "latest_terminal": latest_by(terminal, "finished_at"),
        "latest_success": latest_by(succeeded, "finished_at"),
        "terminal_count": len(terminal),
        "success_count": len(succeeded),
        "pending_count": len(runs) - len(terminal),
    }


def duration_minutes(row: dict) -> float | None:
    """Report elapsed processing time when both endpoints are recorded."""
    start, finish = as_utc(row.get("started_at")), as_utc(row.get("finished_at"))
    if start is None or finish is None or finish < start:
        return None
    return round((finish - start).total_seconds() / 60, 2)


def landing_coverage(batch: dict) -> str:
    """Display landed versus expected deliveries without claiming uniqueness."""
    landed, expected = batch.get("landed_record_count"), batch.get("expected_record_count")
    return f"{landed:,} / {expected:,}" if landed is not None and expected is not None else "Unavailable"

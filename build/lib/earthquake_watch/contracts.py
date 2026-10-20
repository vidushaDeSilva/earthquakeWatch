"""Validate the source contract and the exact frozen USGS response."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    """Untrusted data or configuration violates a versioned contract."""


def utc(dt: datetime) -> datetime:
    """Require timezone-aware input and normalize it to UTC."""
    if dt.tzinfo is None:
        raise ContractError("UTC-aware time required")
    return dt.astimezone(timezone.utc)


def stamp(dt: datetime) -> str:
    """Render a stable millisecond-resolution UTC timestamp."""
    return utc(dt).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class SourceConfig:
    """Immutable identity and safety bounds for the USGS all-day feed."""
    source_id: str
    source_url: str
    max_response_bytes: int
    socket_timeout_seconds: int
    max_future_seconds: int

    @classmethod
    def load(cls, path: Path) -> "SourceConfig":
        """Load the exact versioned USGS source configuration."""
        data = json.loads(path.read_text(encoding="utf-8"))
        expected = {"schema_version", "source_id", "source_url", "max_response_bytes", "socket_timeout_seconds", "max_future_seconds"}
        if set(data) != expected or type(data["schema_version"]) is not int or data["schema_version"] != 1:
            raise ContractError("Unsupported source configuration")
        if data["source_id"] != "usgs-all-day" or data["source_url"] != "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson":
            raise ContractError("Unexpected source identity or URL")
        for name, value in [("max_response_bytes", 16777216), ("socket_timeout_seconds", 30), ("max_future_seconds", 300)]:
            if type(data[name]) is not int or data[name] != value:
                raise ContractError(f"Unexpected {name}")
        return cls(**{key: data[key] for key in expected - {"schema_version"}})


@dataclass(frozen=True)
class ValidatedFeed:
    """Evidence measured directly from the unmodified source response."""
    sha256: str
    size: int
    count: int
    generated_at: datetime


def validate_feed(raw: bytes, cfg: SourceConfig, now: datetime) -> ValidatedFeed:
    """Validate top-level source evidence without changing the bytes to be archived."""
    if not raw or len(raw) > cfg.max_response_bytes:
        raise ContractError("Empty or oversized source response")
    try:
        doc: Any = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError("Response is not UTF-8 JSON") from exc
    if not isinstance(doc, dict) or doc.get("type") != "FeatureCollection" or not isinstance(doc.get("features"), list):
        raise ContractError("Response is not a GeoJSON FeatureCollection")
    meta = doc.get("metadata")
    if not isinstance(meta, dict) or type(meta.get("generated")) is not int or type(meta.get("count")) is not int or type(meta.get("status")) is not int:
        raise ContractError("Missing or invalid USGS metadata")
    if meta["status"] != 200 or meta["generated"] < 0 or meta["count"] < 0 or meta["count"] != len(doc["features"]):
        raise ContractError("USGS metadata is inconsistent")
    try:
        generated = datetime.fromtimestamp(meta["generated"] / 1000, timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ContractError("Invalid source generation time") from exc
    if generated > utc(now) + timedelta(seconds=cfg.max_future_seconds):
        raise ContractError("Source generation time is in the future")
    return ValidatedFeed(hashlib.sha256(raw).hexdigest(), len(raw), meta["count"], generated)


def capture_id(source_id: str, scheduled_time: datetime, checksum: str) -> tuple[str, datetime]:
    """Bind a response checksum to the UTC hourly schedule slot."""
    slot = utc(scheduled_time).replace(minute=0, second=0, microsecond=0)
    if len(checksum) != 64 or any(c not in "0123456789abcdef" for c in checksum):
        raise ContractError("Invalid response checksum")
    identity = f"{source_id}|{stamp(slot)}|{checksum}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest(), slot


def json_bytes(value: dict[str, Any]) -> bytes:
    """Serialize a small manifest or envelope as deterministic UTF-8 JSON."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")

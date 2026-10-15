"""Define source feeds, stable identifiers, JSON contracts, and UTC collection windows.

Collection windows coordinate retries; live feeds retain their own moving event-time
coverage. Raw item validation and quarantine are added with Bronze in Sprint 4.
"""

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ConfigurationError, ContractError

UTC = timezone.utc
CONTRACT_VERSION = "usgs-summary-v1"
SEMANTIC_FIELDS = (
    "time", "updated", "mag", "magType", "place", "felt", "cdi", "mmi",
    "alert", "status", "tsunami", "sig", "type",
)


@dataclass(frozen=True)
class Source:
    """One immutable source configuration revision."""

    source_id: str
    feed_key: str
    lookback_days: int
    revision: int = 1

    @property
    def url(self) -> str:
        """Return an allowlisted USGS summary endpoint."""
        return f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{self.feed_key}.geojson"


SOURCES = {
    "hourly": (Source("usgs-m2p5-day", "2.5_day", 1),),
    "reconcile": (
        Source("usgs-m2p5-month", "2.5_month", 30),
        Source("usgs-significant-month", "significant_month", 30),
    ),
}


def utc_text(value: datetime) -> str:
    """Serialize an aware datetime as a precise UTC ISO timestamp."""
    if value.tzinfo is None:
        raise ConfigurationError("Timestamps must specify UTC or an explicit offset")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    """Reject ambiguous naive input and normalize an ISO timestamp to UTC."""
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("timezone required")
        return result.astimezone(UTC)
    except ValueError as exc:
        raise ConfigurationError("Use an ISO timestamp with Z or an explicit UTC offset") from exc


def _normalize_numbers(value: Any) -> Any:
    """Make semantically equal JSON numbers such as 6 and 6.0 hash equally."""
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractError("Non-finite JSON numbers are not valid")
        return int(value) if value.is_integer() else value
    if isinstance(value, dict):
        return {key: _normalize_numbers(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_numbers(item) for item in value]
    return value


def json_bytes(value: Any) -> bytes:
    """Encode deterministic UTF-8 JSON; never write payloads to local files."""
    return json.dumps(_normalize_numbers(value), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(payload: bytes) -> str:
    """Return the SHA-256 of exact bytes."""
    return hashlib.sha256(payload).hexdigest()


def stable_id(kind: str, *parts: Any) -> str:
    """Hash a typed JSON tuple, avoiding ambiguous string concatenation."""
    return digest(json_bytes([kind, *parts]))


def _invalid_constant(value: str) -> None:
    """Reject Python's permissive JSON NaN/Infinity extension."""
    raise ValueError(value)


def decode_json(payload: bytes) -> Any:
    """Decode strict UTF-8 JSON with safe contract errors."""
    try:
        return json.loads(payload.decode("utf-8"), parse_constant=_invalid_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ContractError("Malformed or nonstandard JSON response") from exc


def validate_feed(payload: bytes) -> dict[str, Any]:
    """Validate the collection envelope while preserving all items for later quarantine."""
    feed = decode_json(payload)
    if not isinstance(feed, dict) or feed.get("type") != "FeatureCollection":
        raise ContractError("USGS response must be a GeoJSON FeatureCollection")
    metadata, features = feed.get("metadata"), feed.get("features")
    if not isinstance(metadata, dict) or not isinstance(features, list):
        raise ContractError("USGS metadata and features are required")
    count, generated = metadata.get("count"), metadata.get("generated")
    if type(count) is not int or count < 0 or count != len(features):
        raise ContractError("USGS metadata count does not match the response items")
    if type(generated) is not int or generated < 0 or metadata.get("status") != 200:
        raise ContractError("USGS generated timestamp or metadata status is invalid")
    return feed


def source_version_id(feature: dict[str, Any]) -> str:
    """Identify a valid source state consistently across summary and detail wrappers."""
    props = feature.get("properties")
    if not isinstance(props, dict) or not isinstance(feature.get("id"), str) or not feature["id"]:
        raise ContractError("Event identity is missing")
    if type(props.get("updated")) is not int or props["updated"] < 0:
        raise ContractError("Source revision timestamp is missing")
    geometry = feature.get("geometry") or {}
    if not isinstance(geometry, dict):
        raise ContractError("Event geometry must be an object")
    projection = {key: props.get(key) for key in SEMANTIC_FIELDS}
    projection["coordinates"] = geometry.get("coordinates")
    semantic_hash = digest(json_bytes(projection))
    return stable_id("source-version-v1", "usgs", feature["id"], props["updated"], semantic_hash)


def collection_window(mode: str, requested_end: str | None, now: datetime) -> tuple[datetime, datetime]:
    """Return an aligned fixed interval used only as the collection retry identity."""
    if mode not in SOURCES:
        raise ConfigurationError("Mode must be hourly or reconcile")
    end = parse_utc(requested_end) if requested_end else now.astimezone(UTC)
    aligned = end.replace(minute=0, second=0, microsecond=0)
    if mode == "reconcile":
        aligned = aligned.replace(hour=0)
    if requested_end and end != aligned:
        raise ConfigurationError("Window end must align to a UTC hour or reconciliation midnight")
    if aligned > now:
        raise ConfigurationError("Future collection windows are invalid")
    duration = timedelta(hours=1) if mode == "hourly" else timedelta(days=1)
    return aligned - duration, aligned

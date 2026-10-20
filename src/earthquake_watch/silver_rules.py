"""Pure, explicit USGS normalization and version identity rules for Silver."""

from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any

from .contracts import ContractError
from .kafka_transport import ENVELOPE_VERSION, canonical_feature


def _number(value: Any, name: str, required: bool = False) -> float | None:
    """Reject booleans and nonfinite coordinates or magnitudes."""
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ContractError(f"Invalid {name}")
    return float(value)


def _integer(value: Any, name: str, required: bool = False) -> int | None:
    """Keep integer source fields separate from permissive JSON numerics."""
    if value is None and not required:
        return None
    if type(value) is not int:
        raise ContractError(f"Invalid {name}")
    return value


def _time(milliseconds: int, name: str) -> datetime:
    """Convert USGS epoch milliseconds to an aware UTC timestamp."""
    try:
        return datetime.fromtimestamp(milliseconds / 1000, timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ContractError(f"Invalid {name}") from exc


def normalize(row: dict[str, Any], now: datetime, revision: str) -> dict[str, Any]:
    """Return one Silver version or raise a stable record-level data error."""
    try:
        envelope = json.loads(row["raw_payload"])
    except (KeyError, ValueError, TypeError) as exc:
        raise ContractError("Invalid event envelope JSON") from exc
    if not isinstance(envelope, dict) or envelope.get("schema_version") != ENVELOPE_VERSION:
        raise ContractError("Unsupported event envelope version")
    feature = envelope.get("feature")
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ContractError("Invalid GeoJSON feature")
    event_id = feature.get("id")
    if not isinstance(event_id, str) or not event_id or event_id != envelope.get("usgs_event_id"):
        raise ContractError("Missing or conflicting USGS event ID")
    if row.get("key") != event_id:
        raise ContractError("Kafka key and event ID disagree")
    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, dict) or not isinstance(geometry, dict) or geometry.get("type") != "Point":
        raise ContractError("Invalid feature properties or geometry")
    updated = _integer(properties.get("updated"), "updated time", required=True)
    occurred = _integer(properties.get("time"), "event time", required=True)
    assert updated is not None and occurred is not None
    if updated != envelope.get("usgs_updated_ms"):
        raise ContractError("Envelope and feature updated time disagree")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 3:
        raise ContractError("Missing point coordinates")
    longitude = _number(coordinates[0], "longitude", required=True)
    latitude = _number(coordinates[1], "latitude", required=True)
    depth = _number(coordinates[2], "depth", required=True)
    assert longitude is not None and latitude is not None and depth is not None
    if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
        raise ContractError("Coordinates outside Earth bounds")
    payload_hash = hashlib.sha256(canonical_feature(feature)).hexdigest()
    if payload_hash != envelope.get("feature_sha256"):
        raise ContractError("GeoJSON feature checksum mismatch")
    tsunami = _integer(properties.get("tsunami"), "tsunami flag", required=True)
    if tsunami not in (0, 1):
        raise ContractError("Unexpected tsunami flag")
    alert = properties.get("alert")
    if alert not in (None, "green", "yellow", "orange", "red"):
        raise ContractError("Unexpected PAGER alert")
    version_id = hashlib.sha256(f"{event_id}|{updated}|{payload_hash}".encode()).hexdigest()
    capture_id = envelope.get("capture_id")
    if (not isinstance(capture_id, str) or len(capture_id) != 64
            or any(c not in "0123456789abcdef" for c in capture_id)):
        raise ContractError("Missing capture lineage")
    if envelope.get("source_id") != "usgs-all-day":
        raise ContractError("Unexpected event source")
    return {
        "event_version_id": version_id, "usgs_event_id": event_id,
        "usgs_updated_ms": updated, "payload_sha256": payload_hash,
        "event_time": _time(occurred, "event time"), "updated_at": _time(updated, "updated time"),
        "magnitude": _number(properties.get("mag"), "magnitude"),
        "place": properties.get("place") if isinstance(properties.get("place"), str) else None,
        "longitude": longitude, "latitude": latitude, "depth_km": depth,
        "significance": _integer(properties.get("sig"), "significance"),
        "pager_alert": alert, "max_mmi": _number(properties.get("mmi"), "MMI"),
        "max_cdi": _number(properties.get("cdi"), "CDI"),
        "felt_count": _integer(properties.get("felt"), "felt count"),
        "tsunami_flag": bool(tsunami),
        "event_status": properties.get("status") if isinstance(properties.get("status"), str) else None,
        "event_type": properties.get("type") if isinstance(properties.get("type"), str) else None,
        "source_url": properties.get("url") if isinstance(properties.get("url"), str) else None,
        "first_seen_at": now, "last_seen_at": now, "first_capture_id": capture_id,
        "version_conflict": False, "validation_status": "VALID", "processing_revision": revision,
    }

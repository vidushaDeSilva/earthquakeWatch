"""Freeze exact source bytes, verify them, then commit the capture manifest last."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import time
from typing import Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .contracts import ContractError, SourceConfig, capture_id, json_bytes, stamp, utc, validate_feed
from .volume_client import VolumeError


class Volume(Protocol):
    def read_optional(self, relative: str) -> bytes | None: ...
    def put_immutable(self, relative: str, data: bytes) -> None: ...


def fetch_source(cfg: SourceConfig) -> bytes:
    """Bounded retries for transient HTTP/transport errors; no silent source fallback."""
    for attempt, delay in enumerate((0, 5, 20), start=1):
        if delay:
            time.sleep(delay)
        try:
            req = Request(cfg.source_url, headers={"Accept": "application/geo+json, application/json"})
            with urlopen(req, timeout=cfg.socket_timeout_seconds) as response:
                if response.status != 200:
                    raise ContractError(f"Unexpected USGS HTTP {response.status}")
                raw = response.read(cfg.max_response_bytes + 1)
                if len(raw) > cfg.max_response_bytes:
                    raise ContractError("USGS response too large")
                return raw
        except HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 3:
                raise ContractError(f"USGS HTTP {exc.code}") from exc
        except URLError as exc:
            if attempt == 3:
                raise ContractError("USGS unavailable") from exc
    raise AssertionError("Unreachable")


@dataclass(frozen=True)
class CaptureResult:
    capture_id: str
    manifest_path: str
    reused: bool


def freeze_capture(raw: bytes, scheduled_time: datetime, fetched_at: datetime,
                   cfg: SourceConfig, volume: Volume) -> CaptureResult:
    """Manifest presence is the only complete-capture boundary."""
    valid = validate_feed(raw, cfg, fetched_at)
    identifier, slot = capture_id(cfg.source_id, scheduled_time, valid.sha256)
    response_path = f"captures/{identifier}/response.json"
    manifest_path = f"captures/{identifier}/manifest.json"
    previous = volume.read_optional(manifest_path)
    if previous is not None:
        try:
            manifest = json.loads(previous)
        except (ValueError, UnicodeDecodeError) as exc:
            raise VolumeError("Existing capture manifest is invalid") from exc
        stored = volume.read_optional(response_path)
        if (manifest.get("capture_id") != identifier or manifest.get("response_sha256") != valid.sha256
                or manifest.get("response_bytes") != valid.size or manifest.get("state") != "FROZEN"
                or stored is None or hashlib.sha256(stored).hexdigest() != valid.sha256):
            raise VolumeError("Existing capture conflicts with expected identity")
        return CaptureResult(identifier, manifest_path, True)

    # Interrupted attempts can leave response.json without a manifest; exact replays reuse it.
    volume.put_immutable(response_path, raw)
    manifest = {
        "manifest_version": 1, "capture_id": identifier, "source_id": cfg.source_id,
        "source_url": cfg.source_url, "schedule_slot_start": stamp(slot),
        "fetched_at": stamp(fetched_at), "source_generated_at": stamp(valid.generated_at),
        "response_sha256": valid.sha256, "response_bytes": valid.size,
        "expected_event_count": valid.count, "response_relative_path": response_path,
        "state": "FROZEN",
    }
    volume.put_immutable(manifest_path, json_bytes(manifest))
    return CaptureResult(identifier, manifest_path, False)


def capture_once(scheduled_time: datetime, cfg: SourceConfig, volume: Volume,
                 fetch: Callable[[SourceConfig], bytes] = fetch_source,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> CaptureResult:
    raw = fetch(cfg)
    return freeze_capture(raw, utc(scheduled_time), clock(), cfg, volume)

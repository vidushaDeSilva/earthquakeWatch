"""Freeze USGS responses in a managed Volume and reuse verified captures on retry.

The selected manifest is written last. No Kafka messages or Delta business rows
are created in Sprints 1–2; source evidence stays ready for Sprint 3 dispatch.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .config import Settings
from .contracts import (CONTRACT_VERSION, Source, decode_json, digest, json_bytes,
                        parse_utc, stable_id, utc_text, validate_feed)
from .errors import ConfigurationError, ContentConflict, ContractError
from .http import HTTPClient
from .volume import Volume


class Collector:
    """Capture one configured feed for a stable collection interval."""

    def __init__(self, settings: Settings, client: HTTPClient, volume: Volume | None,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        """Bind source HTTP, cloud storage and an injectable UTC clock."""
        self.settings, self.client, self.volume, self.now = settings, client, volume, now

    def identity(self, source: Source, mode: str, start: datetime, end: datetime) -> str:
        """Generate the stable identity for this source request interval."""
        return stable_id("collection-v1", self.settings.environment, source.source_id,
                         source.revision, mode, utc_text(start), utc_text(end), source.url,
                         CONTRACT_VERSION, self.settings.collection_generation)

    def _load(self, source: Source, collection_id: str, capture_id: str) -> dict[str, Any]:
        """Verify every saved source byte before treating a selected capture as reusable."""
        if not isinstance(capture_id, str) or len(capture_id) != 64 or any(
                char not in "0123456789abcdef" for char in capture_id):
            raise ContractError("Invalid saved capture identity")
        prefix = f"captures/{collection_id}/{capture_id}"
        ready = self.volume.read_optional(prefix + "/ready.json")
        payload = self.volume.read_optional(prefix + "/response.json")
        if ready is None or payload is None:
            raise ContractError("Selected capture is incomplete; never refetch it silently")
        manifest = decode_json(ready)
        feed = validate_feed(payload)
        if not isinstance(manifest, dict) or any((
                manifest.get("collection_id") != collection_id,
                manifest.get("capture_id") != capture_id,
                manifest.get("source_id") != source.source_id,
                manifest.get("source_revision") != source.revision,
                manifest.get("contract_version") != CONTRACT_VERSION,
                manifest.get("environment") != self.settings.environment,
                manifest.get("source_url") != source.url,
                manifest.get("state") != "CAPTURED",
                manifest.get("response_relative_path") != prefix + "/response.json",
                manifest.get("response_sha256") != digest(payload),
                manifest.get("response_bytes") != len(payload),
                manifest.get("expected_item_count") != len(feed["features"]),
                capture_id != stable_id("capture-v1", collection_id, digest(payload)))):
            raise ContractError("Capture manifest does not match its source response")
        for field in ("fetched_at", "source_generated_at", "window_start", "window_end"):
            if not isinstance(manifest.get(field), str):
                raise ContractError("Capture manifest timestamp is missing")
            parse_utc(manifest[field])
        return manifest

    def _selected(self, source: Source, collection_id: str) -> dict[str, Any] | None:
        """Read a sealed selection, failing closed if its integrity is uncertain."""
        value = self.volume.read_optional(f"collections/{collection_id}/selected.json")
        if value is None:
            return None
        selection = decode_json(value)
        if not isinstance(selection, dict) or selection.get("collection_id") != collection_id:
            raise ContractError("Collection selection is invalid")
        return self._load(source, collection_id, selection.get("capture_id"))

    def _recover_ready(self, source: Source, collection_id: str) -> dict[str, Any] | None:
        """Recover a sealed response when the final collection-selection write was interrupted."""
        candidates = []
        for capture_id in self.volume.child_directories(f"captures/{collection_id}"):
            ready = self.volume.read_optional(f"captures/{collection_id}/{capture_id}/ready.json")
            if ready is not None:
                manifest = self._load(source, collection_id, capture_id)
                candidates.append((parse_utc(manifest["fetched_at"]), capture_id, manifest))
        if not candidates:
            return None
        manifest = min(candidates, key=lambda item: (item[0], item[1]))[2]
        selection = {"manifest_version": 1, "collection_id": collection_id,
                     "capture_id": manifest["capture_id"]}
        try:
            self.volume.put_immutable(f"collections/{collection_id}/selected.json", json_bytes(selection))
        except ContentConflict:
            return self._selected(source, collection_id)
        return manifest

    def collect(self, source: Source, mode: str, start: datetime, end: datetime,
                run_id: str, persist: bool) -> tuple[dict[str, Any], bool]:
        """Fetch once or reuse a frozen capture; a live feed cannot recreate old history."""
        collection_id = self.identity(source, mode, start, end)
        if persist:
            if self.volume is None:
                raise ConfigurationError("Capture requires a managed Volume client")
            existing = self._selected(source, collection_id)
            if existing is not None:
                return existing, True
            recovered = self._recover_ready(source, collection_id)
            if recovered is not None:
                return recovered, True
        max_age = timedelta(hours=2) if mode == "hourly" else timedelta(days=1, hours=2)
        if self.now() - end > max_age:
            raise ConfigurationError("No frozen capture for this old interval; historical backfill starts in Sprint 7")
        response = self.client.request("usgs", "GET", source.url, scope=collection_id)
        fetched = self.now()
        feed = validate_feed(response.body)
        try:
            generated = datetime.fromtimestamp(feed["metadata"]["generated"] / 1000, timezone.utc)
        except (ValueError, OverflowError, OSError) as exc:
            raise ContractError("USGS generation timestamp is out of range") from exc
        if generated > fetched + timedelta(minutes=5):
            raise ContractError("USGS generation timestamp is unexpectedly in the future")
        capture_id = stable_id("capture-v1", collection_id, digest(response.body))
        prefix = f"captures/{collection_id}/{capture_id}"
        manifest = {
            "manifest_version": 1, "contract_version": CONTRACT_VERSION,
            "collection_id": collection_id, "capture_id": capture_id,
            "environment": self.settings.environment, "source_id": source.source_id,
            "source_revision": source.revision, "feed_key": source.feed_key,
            "run_kind": mode.upper(), "first_run_id": run_id,
            "window_start": utc_text(start), "window_end": utc_text(end),
            "fetched_at": utc_text(fetched), "source_generated_at": utc_text(generated),
            "source_url": source.url, "response_sha256": digest(response.body),
            "response_bytes": len(response.body), "expected_item_count": len(feed["features"]),
            "response_relative_path": prefix + "/response.json",
            "source_stale": fetched - generated > timedelta(hours=2),
            "state": "CAPTURED",
        }
        if not persist:
            return manifest, False
        self.volume.put_immutable(prefix + "/response.json", response.body)
        try:
            self.volume.put_immutable(prefix + "/ready.json", json_bytes(manifest))
        except ContentConflict:
            # A previous attempt may have saved the same bytes with an earlier fetch time.
            manifest = self._load(source, collection_id, capture_id)
        selection = {"manifest_version": 1, "collection_id": collection_id, "capture_id": capture_id}
        try:
            self.volume.put_immutable(f"collections/{collection_id}/selected.json", json_bytes(selection))
        except ContentConflict:
            # If a concurrent writer won selection, use its frozen response, not this orphan.
            chosen = self._selected(source, collection_id)
            if chosen is None:
                raise
            return chosen, True
        return self._load(source, collection_id, capture_id), False

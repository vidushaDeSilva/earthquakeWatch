"""Phase 0/1 tests: bytes and manifest boundary are the recovery contract."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest

from earthquake_watch.capture import capture_once, freeze_capture
from earthquake_watch.contracts import ContractError, SourceConfig, capture_id, validate_feed
from earthquake_watch.volume_client import MemoryVolume, VolumeError


ROOT = Path(__file__).resolve().parents[1]
CFG = SourceConfig.load(ROOT / "config/source_usgs_all_day.json")
NOW = datetime(2026, 10, 19, 19, 17, tzinfo=timezone.utc)


def feed(features=None, generated=1792437420000, count=None, status=200):
    features = [] if features is None else features
    return json.dumps({"type": "FeatureCollection", "metadata": {
        "generated": generated, "count": len(features) if count is None else count, "status": status,
    }, "features": features}, separators=(",", ":")).encode()


def test_source_contract_and_identity():
    raw = feed()
    validated = validate_feed(raw, CFG, NOW)
    identifier, slot = capture_id(CFG.source_id, NOW, validated.sha256)
    assert slot.minute == 0 and len(identifier) == 64
    assert capture_id(CFG.source_id, NOW.replace(minute=59), validated.sha256)[0] == identifier
    assert capture_id(CFG.source_id, NOW.replace(hour=20), validated.sha256)[0] != identifier
    assert capture_id(CFG.source_id, NOW, hashlib.sha256(raw + b" ").hexdigest())[0] != identifier


@pytest.mark.parametrize("raw", [b"", b"\xff", b"{}", feed(count=1), feed(status=500), feed(generated=1792439000000)])
def test_invalid_top_level_never_writes(raw):
    volume = MemoryVolume()
    with pytest.raises(ContractError):
        freeze_capture(raw, NOW, NOW, CFG, volume)
    assert volume.files == {}


def test_exact_bytes_and_manifest_last_and_idempotent_retry():
    raw = feed([{"id": "us1"}]) + b" \n"
    volume = MemoryVolume()
    a = freeze_capture(raw, NOW, NOW, CFG, volume)
    assert volume.writes == [f"captures/{a.capture_id}/response.json", a.manifest_path]
    assert volume.files[volume.writes[0]] == raw
    manifest = json.loads(volume.files[a.manifest_path])
    assert manifest["response_sha256"] == hashlib.sha256(raw).hexdigest()
    assert manifest["response_bytes"] == len(raw) and manifest["state"] == "FROZEN"
    b = freeze_capture(raw, NOW, NOW.replace(minute=18), CFG, volume)
    assert b.capture_id == a.capture_id and b.reused and len(volume.writes) == 2


def test_interruption_before_manifest_recovers_without_overwrite():
    raw, volume = feed(), MemoryVolume()
    identifier, _ = capture_id(CFG.source_id, NOW, hashlib.sha256(raw).hexdigest())
    volume.put_immutable(f"captures/{identifier}/response.json", raw)
    result = freeze_capture(raw, NOW, NOW, CFG, volume)
    assert volume.writes[-1] == result.manifest_path and len(volume.writes) == 2


def test_corrupt_existing_file_blocks_commit():
    raw, volume = feed(), MemoryVolume()
    identifier, _ = capture_id(CFG.source_id, NOW, hashlib.sha256(raw).hexdigest())
    volume.put_immutable(f"captures/{identifier}/response.json", b"different")
    with pytest.raises(VolumeError):
        freeze_capture(raw, NOW, NOW, CFG, volume)
    assert all(not key.endswith("manifest.json") for key in volume.files)


def test_changed_feed_same_hour_is_another_capture():
    volume = MemoryVolume()
    a = freeze_capture(feed(), NOW, NOW, CFG, volume)
    b = freeze_capture(feed([{"id": "us1"}]), NOW, NOW, CFG, volume)
    assert a.capture_id != b.capture_id and len(volume.writes) == 4


def test_one_shot_capture_is_not_a_pipeline_run():
    volume = MemoryVolume()
    out = capture_once(NOW, CFG, volume, fetch=lambda _: feed(), clock=lambda: NOW)
    assert out.manifest_path in volume.files
    assert all(key.startswith("captures/") for key in volume.files)

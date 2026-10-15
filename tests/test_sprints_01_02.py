"""Verify source contracts, bounded HTTP, lost acknowledgements and capture replay offline.

The fake cloud stores bytes only in memory. Tests need no credentials, network,
Kafka broker, Databricks workspace, or persistent earthquake data files.
"""

import io
import ssl
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from urllib.error import URLError
from urllib.parse import unquote, urlsplit

from quake_pipeline.capture import Collector
from quake_pipeline.config import Settings
from quake_pipeline.contracts import (SOURCES, collection_window, decode_json,
                                      json_bytes, stable_id)
from quake_pipeline.contracts import source_version_id, validate_feed
from quake_pipeline.errors import (BudgetExceeded, ConfigurationError, ContentConflict,
                                   ContractError, DeferredError, HTTPFailure,
                                   TransportFailure)
from quake_pipeline.http import HTTPClient, Response, retry_after, transport
from quake_pipeline.volume import Volume
from quake_pipeline.jobs import sync_ledger

NOW = datetime(2026, 10, 15, 8, 17, tzinfo=timezone.utc)
START, END = collection_window("hourly", None, NOW)


def event():
    """Return a small valid summary event for state-identity tests."""
    return {"type": "Feature", "id": "test123", "properties": {
        "time": 1792051200000, "updated": 1792051500000, "mag": 6.0, "mmi": None},
        "geometry": {"type": "Point", "coordinates": [12.5, 45.5, -0.5]}}


def feed(items=None):
    """Return a strict feed with a valid generated timestamp and accurate item count."""
    features = [event()] if items is None else items
    return json_bytes({"type": "FeatureCollection", "metadata": {
        "generated": int(NOW.timestamp() * 1000), "count": len(features), "status": 200},
        "features": features})


def settings():
    """Use fake credentials that must never reach logs or another service."""
    return Settings.from_env({"DATABRICKS_HOST": "https://dbc-example.cloud.databricks.com/",
                              "DATABRICKS_TOKEN": "fake-secret-for-test"})


class FakeCloud:
    """Emulate USGS and Databricks Files API semantics in memory."""

    def __init__(self, payload=None):
        """Initialize immutable cloud files and fault-injection switches."""
        self.payload = feed() if payload is None else payload
        self.files, self.calls = {}, []
        self.fetches = 0
        self.lose_put_ack = False
        self.fail_selection_once = False

    def __call__(self, method, url, headers, body, timeout, limit):
        """Serve requests and optionally fail after a write has durably succeeded."""
        parsed = urlsplit(url)
        self.calls.append((method, url, headers))
        if parsed.hostname == "earthquake.usgs.gov":
            if "Authorization" in headers:
                raise AssertionError("Databricks token leaked to USGS")
            self.fetches += 1
            return Response(200, self.payload)
        path = unquote(parsed.path)
        if path.startswith("/api/2.0/fs/directories"):
            if method == "PUT":
                return Response(204)
            parent = path.removeprefix("/api/2.0/fs/directories") + "/"
            names = {key[len(parent):].split("/")[0] for key in self.files
                     if key.startswith(parent) and "/" in key[len(parent):]}
            return Response(200, json_bytes({"contents": [
                {"is_directory": True, "name": name, "path": parent + name} for name in sorted(names)]}))
        file_path = path.removeprefix("/api/2.0/fs/files")
        if method == "GET":
            return Response(200, self.files[file_path]) if file_path in self.files else Response(404)
        if method == "PUT":
            if not parsed.query == "overwrite=false":
                raise AssertionError("An immutable upload attempted overwrite")
            if file_path.endswith("/selected.json") and self.fail_selection_once:
                self.fail_selection_once = False
                raise BudgetExceeded("Injected interruption before selection")
            if file_path in self.files:
                return Response(409)
            self.files[file_path] = body
            if self.lose_put_ack:
                self.lose_put_ack = False
                raise TransportFailure()
            return Response(204)
        raise AssertionError("Unexpected request")


class ContractTests(unittest.TestCase):
    """Check response shape, stable hashing, timezone and configuration boundaries."""

    def test_valid_empty_feed_is_success(self):
        """No earthquakes is distinct from an invalid blank response."""
        self.assertEqual(validate_feed(feed([]))["features"], [])

    def test_count_mismatch_is_rejected(self):
        """Truncation cannot be mistaken for a complete source snapshot."""
        value = decode_json(feed())
        value["metadata"]["count"] = 2
        with self.assertRaises(ContractError):
            validate_feed(json_bytes(value))

    def test_malformed_blank_and_nonstandard_json_are_rejected(self):
        """Prevent empty bytes, HTML, NaN and invalid UTF-8 passing as data."""
        for payload in (b"", b"<html>bad gateway</html>", b'{"n":NaN}', b"\xff"):
            with self.subTest(payload=payload), self.assertRaises(ContractError):
                validate_feed(payload)

    def test_boolean_count_is_not_an_integer_count(self):
        """JSON true must not satisfy an expected count of one."""
        value = decode_json(feed())
        value["metadata"]["count"] = True
        with self.assertRaises(ContractError):
            validate_feed(json_bytes(value))

    def test_bad_individual_items_remain_available_for_future_quarantine(self):
        """Collection preserves rather than silently discards malformed source items."""
        self.assertEqual(len(validate_feed(feed([{}, None]))["features"]), 2)

    def test_summary_detail_and_numeric_format_hash_equally(self):
        """Detail-only arrays and equivalent JSON numbers do not create false revisions."""
        summary, detail = event(), event()
        detail["properties"].update(products={"shakemap": [1]}, url="https://example.com", ids=",alias,")
        detail["properties"]["mag"] = 6
        self.assertEqual(source_version_id(summary), source_version_id(detail))

    def test_revised_source_state_has_different_identity(self):
        """Magnitude corrections remain distinguishable even at the same source timestamp."""
        revised = event()
        revised["properties"]["mag"] = 5.5
        self.assertNotEqual(source_version_id(event()), source_version_id(revised))

    def test_ids_use_unambiguous_tuples(self):
        """Different tuple boundaries and numeric/string values cannot collide structurally."""
        self.assertNotEqual(stable_id("x", "ab", "c"), stable_id("x", "a", "bc"))
        self.assertNotEqual(stable_id("x", 1), stable_id("x", "1"))

    def test_utc_interval_alignment_and_naive_rejection(self):
        """Collection identity is independent of locale and rejects ambiguous timestamps."""
        self.assertEqual(END.minute, 0)
        with self.assertRaises(ConfigurationError):
            collection_window("hourly", "2026-10-15T08:00:00", NOW)
        with self.assertRaises(ConfigurationError):
            collection_window("hourly", "2026-10-15T08:01:00Z", NOW)
        with self.assertRaises(ConfigurationError):
            collection_window("hourly", "2026-10-15T09:00:00Z", NOW)

    def test_config_keeps_secret_private_and_handles_empty_optional_variable(self):
        """Empty GitHub variables use defaults and repr never includes the token."""
        value = Settings.from_env({"EQ_COLLECTION_GENERATION": "", "DATABRICKS_TOKEN": "sensitive"})
        self.assertEqual(value.collection_generation, "v1")
        self.assertNotIn("sensitive", repr(value))

    def test_host_and_volume_boundary(self):
        """Reject smoke-test paths, traversal and credential-bearing workspace URLs."""
        for values in ({"EQ_VOLUME_ROOT": "/Volumes/earthquake_compat/default/smoke_test"},
                       {"DATABRICKS_HOST": "https://user:secret@dbc-x.cloud.databricks.com"},
                       {"DATABRICKS_HOST": "https://dbc-x.cloud.databricks.com/path"},
                       {"DATABRICKS_HOST": "https://not-databricks.example"}):
            with self.subTest(values=values), self.assertRaises(ConfigurationError):
                Settings.from_env(values)


class HTTPTests(unittest.TestCase):
    """Exercise retries, backpressure, certificate failure and strict response limits."""

    def client(self, sender, **changes):
        """Build a bounded fake-time client with request telemetry and no actual sleeps."""
        self.events, self.waits = [], []
        return HTTPClient(replace(settings(), **changes), self.events.append, sender,
                          self.waits.append, jitter=lambda: 0)

    def test_429_and_503_retry_then_success(self):
        """Transient service failures recover without leaking auth or response text."""
        sender = Mock(side_effect=[Response(429, headers={"retry-after": "3"}),
                                   Response(503), Response(200, b"ok")])
        result = self.client(sender).request("usgs", "GET", "https://example.invalid")
        self.assertEqual(result.body, b"ok")
        self.assertEqual(self.waits, [3, 4])
        self.assertEqual([item["status"] for item in self.events], ["FAILED", "FAILED", "SUCCESS"])

    def test_long_retry_after_defers_without_sleeping(self):
        """Respect throttling instead of shortening the upstream requested wait."""
        sender = Mock(return_value=Response(429, headers={"retry-after": "3600"}))
        with self.assertRaises(DeferredError):
            self.client(sender).request("usgs", "GET", "https://example.invalid")
        self.assertEqual(sender.call_count, 1)
        self.assertEqual(self.waits, [])

    def test_http_date_retry_after(self):
        """HTTP-date Retry-After is interpreted in UTC."""
        self.assertEqual(retry_after("Thu, 15 Oct 2026 08:17:10 GMT", NOW), 10)

    def test_auth_failure_does_not_retry(self):
        """Wrong credentials require correction, not repeated identical requests."""
        sender = Mock(return_value=Response(401, b"never print this"))
        with self.assertRaises(HTTPFailure) as caught:
            self.client(sender).request("databricks", "GET", "https://example.invalid")
        self.assertEqual(sender.call_count, 1)
        self.assertNotIn("never print", str(caught.exception))

    def test_timeout_retry_exhaustion_and_request_budget(self):
        """Connectivity failures are bounded even when every attempt fails."""
        sender = Mock(side_effect=TransportFailure())
        with self.assertRaises(HTTPFailure):
            self.client(sender, max_attempts=3).request("usgs", "GET", "https://example.invalid")
        self.assertEqual(sender.call_count, 3)
        sender.reset_mock()
        with self.assertRaises(BudgetExceeded):
            self.client(sender, max_requests=1).request("usgs", "GET", "https://example.invalid")
        self.assertEqual(sender.call_count, 1)

    def test_certificate_errors_are_terminal(self):
        """Do not retry invalid trust chains or weaken TLS verification."""
        sender = Mock(side_effect=TransportFailure(False))
        with self.assertRaises(HTTPFailure):
            self.client(sender).request("databricks", "GET", "https://example.invalid")
        self.assertEqual(sender.call_count, 1)

    def test_response_body_limit(self):
        """Reject oversized network streams before they grow beyond the memory budget."""
        opened = io.BytesIO(b"abcdef")
        opened.status, opened.headers = 200, {}
        with patch("urllib.request.build_opener") as build:
            build.return_value.open.return_value = opened
            with self.assertRaises(BudgetExceeded):
                transport("GET", "https://example.invalid", {}, None, 1, 5)

    def test_transport_redacts_original_certificate_exception(self):
        """The transport preserves failure classification without exposing URL/token text."""
        with patch("urllib.request.build_opener") as build:
            build.return_value.open.side_effect = URLError(ssl.SSLCertVerificationError("private detail"))
            with self.assertRaises(TransportFailure) as caught:
                transport("GET", "https://example.invalid", {}, None, 1, 5)
        self.assertFalse(caught.exception.retryable)
        self.assertNotIn("private detail", str(caught.exception))


class CaptureTests(unittest.TestCase):
    """Test the full HTTP-to-Volume capture protocol against an in-memory cloud."""

    def setup_cloud(self, cloud=None):
        """Construct real collection/storage clients around the fake HTTP transport."""
        self.cloud = cloud or FakeCloud()
        self.events = []
        self.http = HTTPClient(settings(), self.events.append, self.cloud, lambda _: None,
                               jitter=lambda: 0)
        self.volume = Volume(settings(), self.http)
        return Collector(settings(), self.http, self.volume, now=lambda: NOW)

    def test_capture_then_replay_reuses_frozen_bytes(self):
        """A newer live feed cannot replace a selected source snapshot on retry."""
        collector = self.setup_cloud()
        first, reused = collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run-one", True)
        self.assertFalse(reused)
        self.cloud.payload = feed([])
        again, reused = collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run-two", True)
        self.assertTrue(reused)
        self.assertEqual(again, first)
        self.assertEqual(self.cloud.fetches, 1)
        self.assertEqual(first["expected_item_count"], 1)

    def test_read_only_action_writes_nothing(self):
        """Dry-run validates a source without populating Volume files."""
        collector = self.setup_cloud()
        collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run", False)
        self.assertEqual(self.cloud.files, {})

    def test_lost_put_ack_is_verified_as_success(self):
        """A 409 after an uncertain successful upload is not a data failure."""
        collector = self.setup_cloud()
        self.cloud.lose_put_ack = True
        manifest, _ = collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run", True)
        self.assertEqual(manifest["expected_item_count"], 1)

    def test_ready_manifest_recovers_after_interrupted_selection(self):
        """Recover the already sealed response rather than fetching changed source data."""
        collector = self.setup_cloud()
        self.cloud.fail_selection_once = True
        with self.assertRaises(BudgetExceeded):
            collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run-one", True)
        self.cloud.payload = feed([])
        recovered, reused = collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run-two", True)
        self.assertTrue(reused)
        self.assertEqual(recovered["expected_item_count"], 1)
        self.assertEqual(self.cloud.fetches, 1)

    def test_corrupt_selected_response_fails_without_refetch(self):
        """Never silently replace evidence whose checksum no longer matches."""
        collector = self.setup_cloud()
        collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run", True)
        response_path = next(key for key in self.cloud.files if key.endswith("response.json"))
        self.cloud.files[response_path] = feed([])
        with self.assertRaises(ContractError):
            collector.collect(SOURCES["hourly"][0], "hourly", START, END, "run-two", True)
        self.assertEqual(self.cloud.fetches, 1)

    def test_different_existing_file_is_not_overwritten(self):
        """Immutable conflicts preserve original cloud content."""
        self.setup_cloud()
        self.volume.put_immutable("diagnostics/example.json", b"first")
        with self.assertRaises(ContentConflict):
            self.volume.put_immutable("diagnostics/example.json", b"second")
        self.assertEqual(self.volume.read_optional("diagnostics/example.json"), b"first")

    def test_traversal_path_is_rejected(self):
        """Generated cloud paths stay inside their configured Volume."""
        self.setup_cloud()
        with self.assertRaises(ConfigurationError):
            self.volume.read_optional("../secret")

    def test_old_missing_window_requires_backfill(self):
        """A current feed cannot impersonate an uncaptured historical interval."""
        collector = self.setup_cloud()
        with self.assertRaises(ConfigurationError):
            collector.collect(SOURCES["hourly"][0], "hourly", START - timedelta(days=4),
                              END - timedelta(days=4), "run", True)
        self.assertEqual(self.cloud.fetches, 0)

    def test_pagination_continues_after_empty_page(self):
        """The token, not the number of entries, determines directory-list completion."""
        sender = Mock(side_effect=[Response(200, json_bytes({"contents": [], "next_page_token": "next"})),
                                   Response(200, json_bytes({"contents": [{"name": "a" * 64, "is_directory": True}]}))])
        client = HTTPClient(settings(), lambda _: None, sender)
        volume = Volume(settings(), client)
        self.assertEqual(volume.child_directories("captures/" + "b" * 64), ["a" * 64])
        self.assertEqual(sender.call_count, 2)


class JobTests(unittest.TestCase):
    """Confirm that automatic ledger submission uses idempotent launches and real run states."""

    def test_ambiguous_launch_retries_same_token(self):
        """Losing a successful launch ACK cannot create an unkeyed second job."""
        sender = Mock(side_effect=[TransportFailure(), Response(200, b'{"run_id":123}'),
                                   Response(200, b'{"state":{"life_cycle_state":"TERMINATED","result_state":"SUCCESS"}}')])
        client = HTTPClient(settings(), lambda _: None, sender, lambda _: None, jitter=lambda: 0)
        self.assertEqual(sync_ledger(settings(), 1, "a" * 64, client), "123")
        first = decode_json(sender.call_args_list[0].args[3])
        second = decode_json(sender.call_args_list[1].args[3])
        self.assertEqual(first["idempotency_token"], second["idempotency_token"])

    def test_failed_ledger_is_not_reported_success(self):
        """A durable capture and a successful processing job are distinct outcomes."""
        sender = Mock(side_effect=[Response(200, b'{"run_id":123}'),
                                   Response(200, b'{"state":{"life_cycle_state":"TERMINATED","result_state":"FAILED"}}')])
        client = HTTPClient(settings(), lambda _: None, sender)
        from quake_pipeline.errors import PipelineError
        with self.assertRaises(PipelineError):
            sync_ledger(settings(), 1, "a" * 64, client)


if __name__ == "__main__":
    unittest.main()

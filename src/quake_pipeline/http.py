"""Provide bounded, retryable standard-library HTTP with redacted telemetry.

Redirects are disabled, TLS verification stays enabled, and response bytes remain
in memory. Retry-After longer than the run's wait budget defers the work.
"""

import http.client
import random
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable

from .config import Settings
from .errors import BudgetExceeded, DeferredError, HTTPFailure, TransportFailure


@dataclass(frozen=True)
class Response:
    """A bounded in-memory HTTP response with lower-case header names."""

    status: int
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Prevent credentials or request bodies being forwarded to another origin."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        """Return no replacement request so redirects become explicit HTTP failures."""
        return None


def transport(method: str, url: str, headers: dict[str, str], body: bytes | None,
              timeout: float, limit: int) -> Response:
    """Read an HTTPS response incrementally with a strict memory bound."""
    opener = urllib.request.build_opener(NoRedirect())
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        try:
            opened = opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            opened = error  # HTTP errors also expose the bounded response stream.
        with opened as response:
            chunks, total = [], 0
            deadline = time.monotonic() + timeout
            reader = getattr(response, "read1", response.read)
            while True:
                if time.monotonic() >= deadline:
                    raise TransportFailure()
                chunk = reader(min(65536, limit + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    raise BudgetExceeded("Response exceeds the configured in-memory size limit")
                chunks.append(chunk)
            return Response(response.status, b"".join(chunks),
                            {key.lower(): value for key, value in response.headers.items()})
    except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
        cause = getattr(exc, "reason", exc)
        raise TransportFailure(not isinstance(cause, ssl.SSLCertVerificationError)) from None


def retry_after(value: str | None, now: datetime) -> float | None:
    """Interpret Retry-After delta-seconds or HTTP dates; ignore malformed values."""
    if not value:
        return None
    if value.isdigit():
        return float(value)
    try:
        moment = parsedate_to_datetime(value)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return max(0.0, (moment - now).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return None


class HTTPClient:
    """Share one request/time budget across USGS and Databricks requests."""

    def __init__(self, settings: Settings, emit: Callable[[dict[str, Any]], None],
                 sender: Callable[..., Response] = transport,
                 sleeper: Callable[[float], None] = time.sleep,
                 monotonic: Callable[[], float] = time.monotonic,
                 jitter: Callable[[], float] = random.random) -> None:
        """Inject transport and clocks so failure handling can be verified offline."""
        self.settings, self.emit, self.sender = settings, emit, sender
        self.sleep, self.clock, self.jitter = sleeper, monotonic, jitter
        self.deadline = monotonic() + settings.run_seconds
        self.requests = 0
        self.sequence = 0

    def request(self, service: str, method: str, url: str, *, body: bytes | None = None,
                headers: dict[str, str] | None = None,
                expected: tuple[int, ...] = (200,), scope: str = "RUN") -> Response:
        """Retry safe GET/HEAD/PUT calls and explicitly tokenized Jobs POST calls."""
        self.sequence += 1
        sequence = self.sequence
        if method not in {"GET", "HEAD", "PUT"} and method != "POST":
            raise ValueError("Unsupported method")
        if method == "POST":
            # The only POST use must supply a Databricks job idempotency token.
            from .contracts import decode_json
            if not body or not decode_json(body).get("idempotency_token"):
                raise ValueError("POST retries require an explicit idempotency token")
        for attempt in range(1, self.settings.max_attempts + 1):
            remaining = self.deadline - self.clock()
            if remaining <= 0 or self.requests >= self.settings.max_requests:
                raise BudgetExceeded("Run request or wall-time budget exhausted")
            self.requests += 1
            started = self.clock()
            started_at = datetime.now(timezone.utc).isoformat()
            response, transient = None, True
            try:
                response = self.sender(method, url, {"User-Agent": "earthquake-impact/0.2", **(headers or {})},
                                       body, min(self.settings.timeout_seconds, remaining),
                                       self.settings.max_response_bytes)
                success = response.status in expected
                transient = response.status == 429 or response.status == 408 or 500 <= response.status <= 599
            except TransportFailure as error:
                success, transient = False, error.retryable
            status = response.status if response else None
            self.emit({"request_sequence": sequence, "service": service, "scope_id": scope,
                       "attempt_number": attempt, "status": "SUCCESS" if success else "FAILED",
                       "http_status": status, "retryable": transient and not success,
                       "started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
                       "duration_ms": round((self.clock() - started) * 1000), "stage_name": "HTTP"})
            if success:
                return response
            if not transient or attempt == self.settings.max_attempts:
                raise HTTPFailure(service, status)
            requested_wait = retry_after(response.headers.get("retry-after") if response else None,
                                         datetime.now(timezone.utc))
            delay = max(requested_wait or 0, min(2 ** attempt, self.settings.max_retry_wait) + self.jitter())
            if delay > self.settings.max_retry_wait or delay >= self.deadline - self.clock():
                raise DeferredError("Retry-After exceeds this run's wait budget; retry the collection later", delay)
            self.sleep(delay)
        raise HTTPFailure(service, None)

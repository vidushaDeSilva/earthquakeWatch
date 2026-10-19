"""Minimal Databricks Files API adapter; only immutable creates and read-back."""

import hashlib
import json
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class VolumeError(RuntimeError):
    """A remote file is missing, different, or cannot be verified."""


class DatabricksVolume:
    def __init__(self, host: str, token: str, root: str = "/Volumes/earthquake_dev/ops/pipeline_files"):
        if not re.fullmatch(r"https://[^/]+", host) or not token:
            raise VolumeError("A secure Databricks host and token are required")
        self.host, self.token, self.root = host, token, root

    def _path(self, relative: str) -> str:
        parts = relative.split("/")
        if not parts or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", p) or p in (".", "..") for p in parts):
            raise VolumeError("Unsafe Volume path")
        return self.root + "/" + relative

    def _request(self, method: str, uri: str, body: bytes | None = None) -> tuple[int, bytes]:
        req = Request(self.host + uri, data=body, method=method, headers={
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/octet-stream",
        })
        try:
            with urlopen(req, timeout=30) as res:
                return res.status, res.read()
        except HTTPError as exc:
            if exc.code in (404, 409):
                return exc.code, b""
            raise VolumeError(f"Files API returned HTTP {exc.code}") from exc
        except URLError as exc:
            raise VolumeError("Files API unavailable; retry the same identity") from exc

    def read_optional(self, relative: str) -> bytes | None:
        status, body = self._request("GET", "/api/2.0/fs/files" + quote(self._path(relative), safe="/"))
        if status == 404:
            return None
        if status != 200:
            raise VolumeError(f"Unexpected Files API read status: {status}")
        return body

    def put_immutable(self, relative: str, data: bytes) -> None:
        full = self._path(relative)
        directory = full.rsplit("/", 1)[0]
        status, _ = self._request("PUT", "/api/2.0/fs/directories" + quote(directory, safe="/"))
        if status not in (200, 201, 204):
            raise VolumeError(f"Cannot create capture directory: HTTP {status}")
        try:
            status, _ = self._request("PUT", "/api/2.0/fs/files" + quote(full, safe="/") + "?overwrite=false", data)
            if status not in (200, 201, 204, 409):
                raise VolumeError(f"Cannot create immutable file: HTTP {status}")
        except VolumeError:
            # An acknowledged upload can lose its response. Verify before deciding it failed.
            existing = self.read_optional(relative)
            if existing is None or existing != data:
                raise
        existing = self.read_optional(relative)
        if existing is None or len(existing) != len(data) or hashlib.sha256(existing).digest() != hashlib.sha256(data).digest():
            raise VolumeError("Immutable file conflicts with, or differs from, expected bytes")


class MemoryVolume:
    """Offline test double with the same no-overwrite contract."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.writes: list[str] = []

    def read_optional(self, relative: str) -> bytes | None:
        return self.files.get(relative)

    def put_immutable(self, relative: str, data: bytes) -> None:
        previous = self.files.get(relative)
        if previous is not None and previous != data:
            raise VolumeError("Immutable content conflict")
        if previous is None:
            self.files[relative] = data
            self.writes.append(relative)

"""Confined Files API access for immutable captures, receipts, and landing files."""

import hashlib
import json
from pathlib import Path
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class VolumeError(RuntimeError):
    """A remote file is missing, different, or cannot be verified."""


class DatabricksVolume:
    """Read and create files under one configured Unity Catalog Volume."""

    def __init__(self, host: str, token: str, root: str = "/Volumes/earthquake_dev/ops/pipeline_files"):
        """Require a workspace HTTPS host and a nonempty access token."""
        if not re.fullmatch(r"https://[^/]+", host) or not token:
            raise VolumeError("A secure Databricks host and token are required")
        self.host, self.token, self.root = host, token, root

    def _path(self, relative: str) -> str:
        """Reject traversal and unexpected path components before an API call."""
        parts = relative.split("/")
        if not parts or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", p) or p in (".", "..") for p in parts):
            raise VolumeError("Unsafe Volume path")
        return self.root + "/" + relative

    def _request(self, method: str, uri: str, body: bytes | None = None) -> tuple[int, bytes]:
        """Keep authentication errors distinct from a genuine missing file."""
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
        """Return None only for a genuine 404."""
        status, body = self._request("GET", "/api/2.0/fs/files" + quote(self._path(relative), safe="/"))
        if status == 404:
            return None
        if status != 200:
            raise VolumeError(f"Unexpected Files API read status: {status}")
        return body

    def put_immutable(self, relative: str, data: bytes) -> None:
        """Create without overwrite and verify exact bytes after ambiguous responses."""
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

    def list_entries(self, relative: str) -> list[tuple[str, bool]]:
        """List every directory page; a short page is not end of listing."""
        base = "/api/2.0/fs/directories" + quote(self._path(relative), safe="/")
        entries: list[tuple[str, bool]] = []
        token: str | None = None
        seen: set[str] = set()
        while True:
            query = "?page_token=" + quote(token, safe="") if token else ""
            status, body = self._request("GET", base + query)
            if status == 404 and token is None:
                return []
            if status != 200:
                raise VolumeError("Directory listing failed during pagination")
            try:
                page = json.loads(body)
                items = page.get("contents", [])
                if not isinstance(items, list):
                    raise ValueError("contents must be an array")
                for item in items:
                    name = item["name"]
                    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
                        raise ValueError("unsafe listing entry")
                    entries.append((name, item["is_directory"] is True))
                next_token = page.get("next_page_token")
            except (KeyError, TypeError, ValueError) as exc:
                raise VolumeError("Invalid Files API directory listing") from exc
            if not next_token:
                return sorted(set(entries))
            if not isinstance(next_token, str) or next_token in seen:
                raise VolumeError("Repeated or invalid Files API page token")
            seen.add(next_token)
            token = next_token


class MemoryVolume:
    """Offline test double with the same no-overwrite contract."""

    def __init__(self):
        """Keep immutable files in memory for boundary tests."""
        self.files: dict[str, bytes] = {}
        self.writes: list[str] = []

    def read_optional(self, relative: str) -> bytes | None:
        """Return exact stored bytes or None."""
        return self.files.get(relative)

    def put_immutable(self, relative: str, data: bytes) -> None:
        """Reject a new byte sequence at an occupied path."""
        previous = self.files.get(relative)
        if previous is not None and previous != data:
            raise VolumeError("Immutable content conflict")
        if previous is None:
            self.files[relative] = data
            self.writes.append(relative)

    def list_entries(self, relative: str) -> list[tuple[str, bool]]:
        """List immediate files and folders like the remote Files API."""
        prefix = relative.rstrip("/") + "/"
        result = {(remainder.split("/", 1)[0], "/" in remainder)
                  for name in self.files if name.startswith(prefix)
                  if (remainder := name[len(prefix):])}
        return sorted(result)


class MountedVolume:
    """Read-only access to a Unity Catalog Volume mounted in Databricks jobs."""

    def __init__(self, root: str = "/Volumes/earthquake_dev/ops/pipeline_files"):
        """Bind every read to the one configured project Volume."""
        self.root = Path(root)

    def _path(self, relative: str) -> Path:
        """Reject traversal before interpreting an evidence path."""
        parts = relative.split("/")
        if not parts or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part) or part in (".", "..")
                            for part in parts):
            raise VolumeError("Unsafe mounted Volume path")
        return self.root.joinpath(*parts)

    def read_optional(self, relative: str) -> bytes | None:
        """Read the raw evidence bytes, treating only absence as optional."""
        path = self._path(relative)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None

    def list_entries(self, relative: str) -> list[tuple[str, bool]]:
        """Return immediate evidence names and directories without mutation."""
        directory = self._path(relative)
        try:
            return sorted((entry.name, entry.is_dir()) for entry in directory.iterdir())
        except FileNotFoundError:
            return []

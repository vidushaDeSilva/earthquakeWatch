"""Store and verify immutable files through the Databricks Files API.

The final ready/selection manifest is the logical visibility boundary. This module
does not assume a multi-file transaction or write anything to a local data file.
"""

import re
from urllib.parse import quote

from .config import Settings
from .contracts import decode_json, digest
from .errors import ConfigurationError, ContentConflict, ContractError
from .http import HTTPClient


class Volume:
    """A confined client for one configured Unity Catalog managed Volume."""

    def __init__(self, settings: Settings, client: HTTPClient) -> None:
        """Bind credentials to the validated Databricks workspace origin."""
        settings.require_cloud()
        self.settings, self.client = settings, client

    def path(self, relative: str) -> str:
        """Resolve a generated path while rejecting traversal and unsafe components."""
        if (not relative or relative.startswith("/") or
                any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part) or part in {".", ".."}
                    for part in relative.split("/"))):
            raise ConfigurationError("Unsafe Volume-relative path")
        return f"{self.settings.volume_root}/{relative}"

    def read_optional(self, relative: str) -> bytes | None:
        """Return bytes or a genuine 404; authorization and network errors still fail."""
        uri = self.settings.host + "/api/2.0/fs/files" + quote(self.path(relative), safe="/")
        result = self.client.request("databricks", "GET", uri,
                                     headers={"Authorization": f"Bearer {self.settings.token}"},
                                     expected=(200, 404))
        return None if result.status == 404 else result.body

    def put_immutable(self, relative: str, payload: bytes) -> None:
        """Create and read back a file; an identical existing file is a safe retry."""
        full = self.path(relative)
        parent = full.rsplit("/", 1)[0]
        auth = {"Authorization": f"Bearer {self.settings.token}"}
        self.client.request("databricks", "PUT", self.settings.host + "/api/2.0/fs/directories" +
                            quote(parent, safe="/"), headers=auth, expected=(200, 201, 204))
        self.client.request("databricks", "PUT", self.settings.host + "/api/2.0/fs/files" +
                            quote(full, safe="/") + "?overwrite=false", body=payload,
                            headers={**auth, "Content-Type": "application/octet-stream"},
                            expected=(200, 201, 204, 409))
        # A lost successful PUT response may be retried and return 409. Verify bytes.
        existing = self.read_optional(relative)
        if existing is None or digest(existing) != digest(payload):
            raise ContentConflict("Immutable Volume content differs or cannot be verified")

    def child_directories(self, relative: str) -> list[str]:
        """List all immediate directories, following tokens even on a short or empty page."""
        uri = self.settings.host + "/api/2.0/fs/directories" + quote(self.path(relative), safe="/")
        names, token, seen = [], None, set()
        while True:
            query = "?page_token=" + quote(token, safe="") if token else ""
            result = self.client.request("databricks", "GET", uri + query,
                                         headers={"Authorization": f"Bearer {self.settings.token}"},
                                         expected=(200, 404))
            if result.status == 404:
                if token:
                    raise ContractError("Volume directory disappeared during pagination")
                return []
            page = decode_json(result.body)
            if not isinstance(page, dict) or not isinstance(page.get("contents", []), list):
                raise ContractError("Invalid directory listing response")
            for item in page.get("contents", []):
                if not isinstance(item, dict):
                    raise ContractError("Invalid directory entry")
                if item.get("is_directory"):
                    name = item.get("name", "")
                    if not isinstance(name, str) or not re.fullmatch(r"[0-9a-f]{64}", name):
                        raise ContractError("Unexpected source capture directory name")
                    names.append(name)
            token = page.get("next_page_token")
            if not token:
                return sorted(set(names))
            if not isinstance(token, str) or token in seen:
                raise ContractError("Directory pagination token repeated or invalid")
            seen.add(token)

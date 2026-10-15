"""Validate runtime configuration and isolate development capture from the smoke test."""

import os
import re
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import urlsplit

from .errors import ConfigurationError


@dataclass(frozen=True)
class Settings:
    """Validated nonsecret settings; the token is excluded from repr and logs."""

    environment: str = "dev"
    volume_root: str = "/Volumes/earthquake_dev/ops/landing"
    host: str = ""
    token: str = field(default="", repr=False)
    collection_generation: str = "v1"
    max_attempts: int = 5
    max_requests: int = 100
    max_response_bytes: int = 16 * 1024 * 1024
    run_seconds: int = 1200
    timeout_seconds: int = 30
    max_retry_wait: int = 60

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "Settings":
        """Read a small documented set of environment variables and validate bounds."""
        env = os.environ if values is None else values
        environment = env.get("EQ_ENV", "dev").strip() or "dev"
        host = env.get("DATABRICKS_HOST", "").strip().rstrip("/")
        result = cls(environment=environment,
                     volume_root=env.get("EQ_VOLUME_ROOT", "").strip() or
                     f"/Volumes/earthquake_{environment}/ops/landing",
                     host=host, token=env.get("DATABRICKS_TOKEN", "").strip(),
                     collection_generation=env.get("EQ_COLLECTION_GENERATION", "v1").strip() or "v1")
        if result.environment not in {"dev", "prod"}:
            raise ConfigurationError("EQ_ENV must be dev or prod")
        expected = f"/Volumes/earthquake_{environment}/ops/landing"
        if result.volume_root != expected:
            raise ConfigurationError(f"EQ_VOLUME_ROOT must be {expected} for this environment")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", result.collection_generation):
            raise ConfigurationError("EQ_COLLECTION_GENERATION must be a short alphanumeric identifier")
        if host:
            parsed = urlsplit(host)
            try:
                valid_port = parsed.port in (None, 443)
            except ValueError:
                valid_port = False
            if (parsed.scheme != "https" or not parsed.hostname or not valid_port or
                    not parsed.hostname.endswith((".databricks.com", ".azuredatabricks.net")) or
                    parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment):
                raise ConfigurationError("DATABRICKS_HOST must be the HTTPS workspace origin only")
        return result

    def require_cloud(self) -> None:
        """Fail before network activity when required capture credentials are absent."""
        if not self.host or not self.token:
            raise ConfigurationError("Set DATABRICKS_HOST and secret DATABRICKS_TOKEN for capture")

"""Databricks SQL read adapter with a small and explicit configuration surface."""

from __future__ import annotations

import os
from urllib.parse import urlparse


def _setting(name: str) -> str:
    """Read deployment secrets or environment variables without logging values."""
    value = os.environ.get(name)
    if not value:
        import streamlit as st

        try:
            value = st.secrets.get(name)
        except (FileNotFoundError, KeyError):
            value = None
    if not value:
        raise RuntimeError(f"Missing {name}; see dashboard/README.md")
    return str(value)


def connection_settings() -> tuple[str, str, str]:
    """Validate the workspace host, warehouse path, and read-only token."""
    host = _setting("DATABRICKS_HOST")
    parsed = urlparse(host)
    if parsed.scheme == "https" and parsed.hostname and not parsed.path.strip("/"):
        hostname = parsed.hostname
    elif "://" not in host and "/" not in host and host:
        hostname = host
    else:
        raise RuntimeError("DATABRICKS_HOST must be a workspace hostname or HTTPS root URL")
    http_path = _setting("DATABRICKS_HTTP_PATH")
    if not http_path.startswith("/sql/"):
        raise RuntimeError("DATABRICKS_HTTP_PATH must be a SQL warehouse path")
    return hostname, http_path, _setting("DATABRICKS_TOKEN")


def fetch_rows(query: str, parameters: tuple[str, ...] = ()) -> list[dict]:
    """Execute a fixed SELECT with bound values and close the connection."""
    from databricks import sql

    host, http_path, token = connection_settings()
    with sql.connect(server_hostname=host, http_path=http_path,
                     access_token=token) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            names = [field[0] for field in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]

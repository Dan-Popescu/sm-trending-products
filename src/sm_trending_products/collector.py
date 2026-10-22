"""Bounded Rust process adapter. Source errors are persisted as safe categories."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from typing import Any

from .contracts import canonical
from .settings import Settings


class CollectionError(RuntimeError):
    def __init__(
        self, category: str, retry_after_seconds: float = 60, artifact: dict | None = None
    ):
        self.category = category if re.fullmatch(r"[a-z_]+", category) else "collection_error"
        self.retry_after_seconds = max(1, min(float(retry_after_seconds), 86400))
        self.artifact = redact(artifact) if artifact is not None else None
        super().__init__(self.category)


def redact(value: Any) -> Any:
    """Never persist authentication material, including accidental source echoes."""
    secret_keys = {
        "authorization",
        "access_token",
        "refresh_token",
        "client_secret",
        "password",
        "token",
        "secret",
        "api_key",
        "database_url",
    }
    secrets = [
        value
        for key, value in os.environ.items()
        if any(word in key.upper() for word in ("TOKEN", "SECRET", "PASSWORD")) and len(value) > 4
    ]

    def clean(item: Any) -> Any:
        if isinstance(item, dict):
            return {
                str(key): "[REDACTED]" if str(key).lower() in secret_keys else clean(content)
                for key, content in item.items()
            }
        if isinstance(item, list):
            return [clean(content) for content in item]
        if isinstance(item, str):
            for secret in secrets:
                item = item.replace(secret, "[REDACTED]")
            return re.sub(
                r"(?i)(access_token|refresh_token|client_secret|api_key)=([^&\s]+)",
                r"\1=[REDACTED]",
                item,
            )
        return item

    return clean(value)


class Collector:
    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, request: dict, *, normalize: bool = False) -> dict:
        args = [self.settings.collector_binary, *(["normalize"] if normalize else [])]
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            try:
                result = subprocess.run(
                    args,
                    input=canonical(request).encode(),
                    stdout=output,
                    stderr=errors,
                    timeout=self.settings.collector_timeout_seconds,
                    check=False,
                    env={
                        **os.environ,
                        "DATABASE_URL": self.settings.database_url.get_secret_value(),
                    },
                )
            except FileNotFoundError:
                raise CollectionError("collector_binary_missing", 300) from None
            except subprocess.TimeoutExpired:
                raise CollectionError("collector_timeout", 120) from None
            if output.tell() > 32 * 1024 * 1024:
                raise CollectionError("collector_output_limit")
            output.seek(0)
            try:
                body = json.load(output)
            except (ValueError, UnicodeDecodeError):
                raise CollectionError("invalid_collector_output") from None
            if result.returncode or "error" in body:
                error = body.get("error", {})
                code, category = error.get("code"), error.get("category", "collection_error")
                if code == "missing_access_token":
                    category = "credentials_missing"
                elif code == "quota_exhausted":
                    category = "quota_exhausted"
                elif category == "authorization":
                    category = "access_denied"
                elif category == "configuration":
                    category = "invalid_request"
                elif category == "rate_limit":
                    category = "rate_limited"
                raise CollectionError(
                    category, error.get("retry_after_seconds") or 60, artifact=error.get("artifact")
                )
            if not isinstance(body.get("records"), list):
                raise CollectionError("invalid_collector_contract")
            return redact(body)

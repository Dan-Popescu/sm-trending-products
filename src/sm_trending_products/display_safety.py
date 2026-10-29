"""Pure bounded display copies and an explicit current-credential adapter."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SECRET_KEY = re.compile(r"token|password|secret|authorization|credential|database_url|dsn", re.I)
TOKEN_VALUE = re.compile(r"\b(?:act|clt)\.[A-Za-z0-9._~-]+|Bearer\s+\S+", re.I)


def display_safe(value: Any) -> Any:
    """Remove secret-bearing metadata before rendering a public facade result."""
    if isinstance(value, Mapping):
        return {
            str(key): "[redacted]" if SECRET_KEY.search(str(key)) else display_safe(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [display_safe(item) for item in value]
    if isinstance(value, str):
        if value.startswith(("postgres://", "postgresql://")):
            return "[connection details redacted]"
        result = TOKEN_VALUE.sub("[redacted]", value)
        if result.startswith(("https://", "http://")):
            try:
                parsed = urlsplit(result)
                netloc = parsed.netloc.rsplit("@", 1)[-1]
                query = urlencode(
                    [
                        (
                            key,
                            "[redacted]"
                            if SECRET_KEY.search(key) or "signature" in key.lower()
                            else item,
                        )
                        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
                    ]
                )
                return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))
            except ValueError:
                return "[invalid URL]"
        return result
    return value


_URL = re.compile(r"(?:https?|postgres(?:ql)?)://[^\s<>\"']+", re.I)
_SECRET_ASSIGNMENT = re.compile(
    r"(access_token|refresh_token|client_secret|api_key)=([^&\s]+)", re.I
)


def _sensitive_key(key: str) -> bool:
    return bool(SECRET_KEY.search(key)) or key.lower() in {"api_key", "access_key"}


def _redactor(secret_values: Sequence[str]) -> Callable[[str], str]:
    values = tuple(
        sorted({value for value in secret_values if value}, key=lambda value: (-len(value), value))
    )
    used = set("".join(values))
    # A marker character absent from every secret cannot restore another secret
    # inside the marker or across either replacement boundary.
    marker = next((char for char in ("…", "■", "□", "\ue000") if char not in used), None)
    if marker is None:
        raise ValueError("brief_presentation_redaction_unavailable")

    def scrub(value: str) -> str:
        for secret in values:
            value = value.replace(secret, marker)
        return value

    return scrub


def _safe_url(url: str, scrub: Callable[[str], str]) -> str:
    if url.lower().startswith(("postgres://", "postgresql://")):
        return "[connection details redacted]"
    try:
        parsed = urlsplit(url)
        query = urlencode(
            [
                (
                    scrub(key),
                    scrub(
                        "[redacted]" if _sensitive_key(key) or "signature" in key.lower() else value
                    ),
                )
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            ]
        )
        return urlunsplit(
            (parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, query, parsed.fragment)
        )
    except ValueError:
        return "[invalid URL]"


def sanitize_display(
    value: Any,
    *,
    secret_values: Sequence[str] = (),
    max_bytes: int = 4_000_000,
    max_nodes: int = 200_000,
) -> Any:
    """Copy bounded JSON-like display data without reading runtime state.

    Dates and decimals become display strings. A sensitive scalar becomes a
    redacted string, so subsequent JSON serialization retains valid syntax.
    The original value, report identity and executable plans remain unchanged.
    """
    if (
        type(max_bytes) is not int
        or not 1 <= max_bytes <= 4_000_000
        or type(max_nodes) is not int
        or not 1 <= max_nodes <= 200_000
    ):
        raise ValueError("Invalid bounded display configuration")
    scrub = _redactor(secret_values)
    nodes = size = 0

    def visit(value: Any, depth: int = 0) -> Any:
        nonlocal nodes, size
        nodes += 1
        if nodes > max_nodes or depth > 32:
            raise ValueError("brief_presentation_budget_exceeded")
        if isinstance(value, dict):
            return {
                str(visit(str(key), depth + 1)): scrub("[redacted]")
                if _sensitive_key(str(key))
                else visit(item, depth + 1)
                for key, item in value.items()
            }
        if isinstance(value, (tuple, list)):
            return [visit(item, depth + 1) for item in value]
        if isinstance(value, str):
            size += len(value.encode("utf-8"))
            if size > max_bytes:
                raise ValueError("brief_presentation_budget_exceeded")
            result = _SECRET_ASSIGNMENT.sub(r"\1=[redacted]", value)
            result = _URL.sub(lambda match: _safe_url(match[0], scrub), scrub(result))
            return scrub(display_safe(result))
        if isinstance(value, date | datetime | Decimal):
            return visit(
                value.isoformat() if isinstance(value, date | datetime) else str(value), depth + 1
            )
        if value is None or type(value) in (bool, int, float):
            encoded = json.dumps(value, allow_nan=False)
            return scrub("[redacted]") if scrub(encoded) != encoded else value
        raise TypeError("Unsupported display value")

    return visit(value)


def scrub_display_text(value: str, *, secret_values: Sequence[str] = ()) -> str:
    """Scrub the final generated text or HTML, after all representation changes."""
    return _redactor(secret_values)(value)


def display_json(
    value: Any,
    *,
    secret_values: Sequence[str] = (),
    max_bytes: int = 4_000_000,
    max_nodes: int = 200_000,
) -> str:
    """Return final bounded JSON text for display without another serialization.

    JSON encoding can restore an escaped credential after a safe dictionary copy.
    Keep Unicode literal, scrub after encoding, and refuse invalid JSON. Callers
    pass the returned string directly to JSON widgets and keep executable-plan
    equality checks against the original value separate.
    """
    safe = sanitize_display(
        value, secret_values=secret_values, max_bytes=max_bytes, max_nodes=max_nodes
    )
    encoded = scrub_display_text(
        json.dumps(
            safe, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ),
        secret_values=secret_values,
    )
    if len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError("display_json_output_budget_exceeded")
    try:
        json.loads(encoded)
    except (ValueError, RecursionError):
        raise ValueError("display_json_unsafe_encoding") from None
    return encoded


def current_display_secrets() -> tuple[str, ...]:
    """Read current runtime values explicitly at an adapter's requested action."""
    return tuple(
        value
        for key, value in os.environ.items()
        if any(
            part in key.upper()
            for part in (
                "TOKEN",
                "PASSWORD",
                "SECRET",
                "CREDENTIAL",
                "API_KEY",
                "ACCESS_KEY",
                "DATABASE_URL",
                "DSN",
            )
        )
        and value
    )

"""Explicit current-secret display boundaries shared by research adapters."""

import json
from datetime import UTC, datetime
from urllib.parse import unquote_plus

import pytest


def test_shared_display_scrubs_decoded_queries_metadata_and_scalars_without_mutation():
    from sm_trending_products.display_safety import sanitize_display, scrub_display_text

    source = {
        "reference": "https://user:password@example.test/?q=review%2Bsecret%2642",
        "quantity": 42,
        "ordinary": 17,
        "missing": None,
        "at": datetime(2026, 10, 28, tzinfo=UTC),
        "api_key": "private-metadata",
    }
    safe = sanitize_display(source, secret_values=("review+secret&42", "42"))
    assert safe["ordinary"] == 17 and safe["missing"] is None
    assert isinstance(safe["quantity"], str)
    output = scrub_display_text(json.dumps(safe), secret_values=("review+secret&42", "42"))
    assert json.loads(output) == safe
    assert "review+secret&42" not in unquote_plus(output)
    assert "password" not in output and "private-metadata" not in output
    assert source["quantity"] == 42 and "user:password" in source["reference"]


def test_current_display_secrets_is_an_explicit_fresh_reader(monkeypatch):
    from sm_trending_products.display_safety import current_display_secrets

    monkeypatch.setenv("RESEARCH_API_KEY", "first-current-key")
    monkeypatch.setenv("RESEARCH_ACCESS_KEY", "access-value")
    monkeypatch.setenv("RESEARCH_CREDENTIAL", "credential-value")
    monkeypatch.setenv("RESEARCH_DSN", "dsn-value")
    assert {"first-current-key", "access-value", "credential-value", "dsn-value"}.issubset(
        current_display_secrets()
    )
    monkeypatch.setenv("RESEARCH_API_KEY", "second-current-key")
    assert "second-current-key" in current_display_secrets()
    assert "first-current-key" not in current_display_secrets()


def test_shared_display_refuses_excessive_copy_without_partial_output():
    from sm_trending_products.display_safety import sanitize_display

    with pytest.raises(ValueError):
        sanitize_display({"large": "x" * 11}, max_bytes=10)
    with pytest.raises(ValueError):
        sanitize_display({"nodes": [1, 2, 3]}, max_nodes=3)


def test_final_json_display_cannot_restore_credentials_during_serialization():
    from sm_trending_products.display_safety import display_json

    secrets = (r"\u2026", r"\n", r"\"", "42")
    source = {
        "private": r"\u2026",
        "newline": "\n",
        "quote": '"',
        "metric": 42,
        "ordinary": "Français",
        "count": 17,
    }
    output = display_json(source, secret_values=secrets)
    assert all(secret not in output for secret in secrets)
    restored = json.loads(output)
    assert restored["ordinary"] == "Français" and restored["count"] == 17
    assert source["private"] == r"\u2026" and source["metric"] == 42
    with pytest.raises(ValueError):
        display_json({"key": "ordinary"}, secret_values=('":',))
    with pytest.raises(ValueError):
        display_json({"key": "ordinary"}, max_bytes=12)

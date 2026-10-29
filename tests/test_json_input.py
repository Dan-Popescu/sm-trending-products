"""Shared decoding admits encoded bytes before adapter-specific validation."""

import builtins
import importlib
import json
from types import SimpleNamespace

import pytest

from sm_trending_products.content_research_cli import _object_bytes
from sm_trending_products.decision_brief_cli import object_from_bytes as brief_object
from sm_trending_products.json_input import MAX_INPUT_BYTES, object_from_bytes

DECODERS = (object_from_bytes, brief_object, _object_bytes)


@pytest.mark.parametrize("decode", DECODERS)
@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16", "utf-32"])
def test_decoder_preserves_json_unicode_and_existing_byte_encodings(decode, encoding):
    source = {"name": "Français 日本語 🛒", "missing": None, "quantity": 17}
    assert decode(json.dumps(source, ensure_ascii=False).encode(encoding)) == source


@pytest.mark.parametrize("decode", DECODERS)
def test_admission_counts_encoded_bytes_and_accepts_the_exact_limit(decode):
    content = b'{"name":"' + "é".encode() * ((MAX_INPUT_BYTES - 11) // 2) + b'"}'
    content += b" " * (MAX_INPUT_BYTES - len(content))
    assert len(content) == MAX_INPUT_BYTES
    assert decode(content)["name"].startswith("é")
    with pytest.raises(ValueError):
        decode(content + b" ")


def test_oversized_input_is_refused_before_any_json_decoding(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Oversized input reached JSON decoding")

    monkeypatch.setattr(json, "loads", forbidden)
    for decode in DECODERS:
        with pytest.raises(ValueError):
            decode(b"x" * (MAX_INPUT_BYTES + 1))


@pytest.mark.parametrize("decode", DECODERS)
@pytest.mark.parametrize("content", [b"[]", b"null", b"true", b"17", b'"text"'])
def test_top_level_json_must_be_an_object(decode, content):
    with pytest.raises(ValueError):
        decode(content)


@pytest.mark.parametrize("decode", DECODERS)
@pytest.mark.parametrize("content", [b'{"name":"\xff"}', b"\xff\xfe{", b'{"name":"\x00"}', b"{"])
def test_bad_encoding_and_invalid_json_keep_strict_decoder_errors(decode, content):
    with pytest.raises((UnicodeDecodeError, json.JSONDecodeError)):
        decode(content)


@pytest.mark.parametrize(
    ("decode", "size_error", "shape_error"),
    [
        (brief_object, "brief_input_too_large", "brief_input_requires_object"),
        (_object_bytes, "Input exceeds the bounded file size", "A JSON object is required"),
    ],
)
def test_compatibility_parsers_keep_their_existing_errors(decode, size_error, shape_error):
    with pytest.raises(ValueError, match=f"^{size_error}$"):
        decode(b"x" * (MAX_INPUT_BYTES + 1))
    with pytest.raises(ValueError, match=f"^{shape_error}$"):
        decode(b"[]")


def test_dashboards_decode_uploaded_objects_without_importing_cli_adapters(monkeypatch):
    original_import = builtins.__import__

    def without_cli(name, *args, **kwargs):
        if name.endswith(("decision_brief_cli", "content_research_cli")):
            pytest.fail("JSON decoding imported a CLI adapter")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_cli)
    for name in (
        "decision_brief_dashboard",
        "decision_changes_dashboard",
        "content_research_dashboard",
    ):
        module = importlib.reload(importlib.import_module(f"sm_trending_products.{name}"))
        if name == "content_research_dashboard":
            body = '{"name":"Français"}'.encode()
            assert module._read_upload(
                SimpleNamespace(size=len(body), getvalue=lambda body=body: body)
            ) == {"name": "Français"}

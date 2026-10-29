"""Transport-neutral bounded decoding for authored JSON-object input."""

from __future__ import annotations

import json

MAX_INPUT_BYTES = 128 * 1024


class InputTooLarge(ValueError):
    """The encoded input exceeds the shared admission limit."""


class ObjectRequired(ValueError):
    """The decoded JSON is not an object."""


def object_from_bytes(content: bytes) -> dict:
    """Bound encoded bytes before strict JSON decoding, without model validation."""
    if len(content) > MAX_INPUT_BYTES:
        raise InputTooLarge("JSON input exceeds 128 KiB")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ObjectRequired("A JSON object is required")
    return payload

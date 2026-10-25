"""Physical JSON encoding for bounded immutable analytical objects."""

from __future__ import annotations

import gzip
import io
import json
from typing import Any


def compress_json(encoded: bytes) -> bytes:
    return gzip.compress(encoded, compresslevel=6, mtime=0)


def read_json(encoded: bytes, *, compressed: bool, max_bytes: int | None = None) -> Any:
    """Bound decompression independently of the physical cache reservation."""
    if compressed:
        with gzip.GzipFile(fileobj=io.BytesIO(encoded)) as stream:
            encoded = stream.read(max_bytes + 1 if max_bytes is not None else -1)
    if max_bytes is not None and len(encoded) > max_bytes:
        raise ValueError("Analytical JSON exceeds its decoded byte budget")
    return json.loads(encoded)

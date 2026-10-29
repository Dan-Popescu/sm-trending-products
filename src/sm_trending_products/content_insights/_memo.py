"""Bounded, operation-owned storage for pure content calculations only."""

from __future__ import annotations

import json
from sys import getsizeof
from types import TracebackType

_Key = tuple[str, str | None, str, str, str]
# Charge a complete one-entry dictionary per entry, conservatively covering the
# actual dictionary's table allocation as it grows (including its empty header).
_ENTRY_OVERHEAD = getsizeof({None: None})


class _ClassificationMemo:
    """Retain immutable values; each read reconstructs a caller-owned result.

    The byte limit counts retained key/value objects, conservatively counting
    shared strings again for every entry, plus dictionary allocation overhead.
    Neither limit is an RSS or total comparison-memory guarantee.
    Pressure declines admission without changing the calculation's result.
    """

    def __init__(self, *, max_entries: int = 256, max_bytes: int = 4 * 1024 * 1024):
        self.max_entries, self.max_bytes = max_entries, max_bytes
        self.retained_bytes = 0
        self._entries: dict[_Key, tuple[bytes, str]] = {}

    def __enter__(self) -> _ClassificationMemo:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._entries.clear()
        self.retained_bytes = 0

    def get(self, key: _Key) -> tuple[dict, str] | None:
        value = self._entries.get(key)
        return (json.loads(value[0]), value[1]) if value is not None else None

    def remember(self, key: _Key, result: dict, script_key: str) -> None:
        if len(self._entries) >= self.max_entries or key in self._entries:
            return
        key_bytes = _ENTRY_OVERHEAD + getsizeof(key) + sum(getsizeof(part) for part in key)
        if self.retained_bytes + key_bytes > self.max_bytes:
            return
        # Preserve insertion order as well as values for ordinary JSON callers.
        value = (
            json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(),
            script_key,
        )
        retained = key_bytes + getsizeof(value) + sum(getsizeof(part) for part in value)
        if self.retained_bytes + retained <= self.max_bytes:
            self._entries[key] = value
            self.retained_bytes += retained

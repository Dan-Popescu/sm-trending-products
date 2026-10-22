"""Short-lived verified file pins independent of any analytical engine."""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from .history import History


@contextmanager
def pin_cached(history: History, obj: dict[str, Any]) -> Iterator[Path]:
    """Keep one verified cached inode alive while eviction continues elsewhere.

    ``History.cached`` verifies bytes under its writer lock. Reacquiring that lock
    before creating the hardlink closes the eviction race; a missing source is
    refetched at most three times. Only the pin is removed when reading finishes.
    """
    root = history.settings.cache_dir.resolve()
    pins = root / ".pins"
    pins.mkdir(mode=0o700, exist_ok=True)
    if pins.is_symlink() or pins.resolve().parent != root:
        raise ValueError("Cache pin directory escaped its configured root")
    target = pins / (uuid4().hex + ".parquet")
    created = False
    try:
        for _ in range(3):
            source = history.cached(obj)
            with (root / ".cache.lock").open("a") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                if source.is_symlink() or source.parent.resolve() != root:
                    raise ValueError("Verified cache file escaped its configured root")
                try:
                    os.link(source, target, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                created = True
                break
        if not created:
            raise ValueError("Cache eviction repeatedly removed the selected input")
        yield target
    finally:
        if created:
            target.unlink(missing_ok=True)

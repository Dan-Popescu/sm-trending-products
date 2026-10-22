"""Bounded verified cache with independent fills and crash-recoverable pins.

The global lock protects only names and accounting. Remote reads and checksums
hold an object lock, while pins/reservations hold crash-released kernel locks.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO
from uuid import uuid4

if TYPE_CHECKING:
    from .history import History


class CacheCapacityError(ValueError):
    """Active readers/downloads leave insufficient shared-cache space."""


def file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _open(path: Path) -> BinaryIO:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    return os.fdopen(descriptor, "r+b")


def _regular(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except FileNotFoundError:
        return False


class _Cache:
    def __init__(self, history: History):
        self.history = history
        self.root = history.settings.cache_dir.resolve()
        self.limit = history.settings.cache_max_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        self.pins = self._directory(".pins")
        self.downloads = self._directory(".downloads")
        self.locks = self._directory(".object-locks")

    def _directory(self, name: str) -> Path:
        path = self.root / name
        path.mkdir(mode=0o700, exist_ok=True)
        if path.is_symlink() or path.resolve().parent != self.root:
            raise ValueError("Cache control directory escaped its configured root")
        return path

    @contextmanager
    def global_lock(self) -> Iterator[None]:
        with _open(self.root / ".cache.lock") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            yield

    @contextmanager
    def object_lock(self, key: str) -> Iterator[None]:
        name = hashlib.sha256(key.encode()).hexdigest()
        # A bounded set of stripes avoids one permanent lock inode per remote key.
        # Warm reads do not acquire this lock; only colliding cold fills serialize.
        with _open(self.locks / name[:2]) as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            yield

    def _sweep(self) -> int:
        """Under the global lock, reclaim dead holders and count reservations.

        Pin creation takes its shared lock before releasing the global lock, so
        an exclusive nonblocking lock proves no reader uses that pinned inode.
        Multiple pins for one inode can survive together until its last reader exits.
        """
        for pin in self.pins.iterdir():
            if not _regular(pin):
                raise ValueError("Invalid cache pin")
            with _open(pin) as handle:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                pin.unlink(missing_ok=True)
        reserved = 0
        for record in self.downloads.glob("*.reserve"):
            if not _regular(record):
                raise ValueError("Invalid cache reservation")
            with _open(record) as handle:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    amount = int(handle.read())
                    if not 0 <= amount <= self.limit:
                        raise ValueError("Invalid cache reservation size") from None
                    reserved += amount
                    continue
                record.with_suffix(".part").unlink(missing_ok=True)
                record.unlink(missing_ok=True)
        return reserved

    def _inodes(self) -> dict[tuple[int, int], int]:
        sizes: dict[tuple[int, int], int] = {}
        paths = [path for path in self.root.iterdir() if not path.name.startswith(".")]
        paths.extend(self.pins.iterdir())
        for path in paths:
            if _regular(path):
                info = path.stat(follow_symlinks=False)
                sizes[(info.st_dev, info.st_ino)] = info.st_size
        return sizes

    def _make_room(self, amount: int, *, keep: Path | None = None) -> None:
        reserved = self._sweep()
        sizes = self._inodes()
        used = sum(sizes.values())
        if used + reserved + amount <= self.limit:
            return
        pinned = {
            (info.st_dev, info.st_ino)
            for path in self.pins.iterdir()
            if _regular(path)
            for info in (path.stat(follow_symlinks=False),)
        }
        candidates = sorted(
            (
                path
                for path in self.root.iterdir()
                if not path.name.startswith(".") and _regular(path)
            ),
            key=lambda path: path.stat(follow_symlinks=False).st_mtime_ns,
        )
        names: dict[tuple[int, int], int] = {}
        for path in candidates:
            info = path.stat(follow_symlinks=False)
            inode = (info.st_dev, info.st_ino)
            names[inode] = names.get(inode, 0) + 1
        for path in candidates:
            if used + reserved + amount <= self.limit:
                return
            info = path.stat(follow_symlinks=False)
            inode = (info.st_dev, info.st_ino)
            if path != keep and inode not in pinned:
                path.unlink(missing_ok=True)
                names[inode] -= 1
                if names[inode] == 0:
                    used -= sizes[inode]
        if used + reserved + amount > self.limit:
            raise CacheCapacityError(
                "Shared cache budget is in use by active readers or downloads; "
                "retry or narrow inputs"
            )

    def _pin(self, source: Path) -> tuple[Path, BinaryIO]:
        if not _regular(source):
            raise ValueError("Verified cache file is not a regular file")
        target = self.pins / (uuid4().hex + ".parquet")
        os.link(source, target, follow_symlinks=False)
        try:
            handle = _open(target)
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
        except BaseException:
            target.unlink(missing_ok=True)
            raise
        return target, handle

    def release(self, target: Path, handle: BinaryIO) -> None:
        try:
            with self.global_lock():
                target.unlink(missing_ok=True)
        finally:
            handle.close()

    def _try_cached(self, local: Path, expected: str) -> tuple[Path, Path, BinaryIO] | None:
        with self.global_lock():
            if local.is_symlink():
                raise ValueError("Cache file escaped its configured root")
            pinned = self._pin(local) if _regular(local) else None
        if pinned is None:
            return None
        target, handle = pinned
        try:
            if file_digest(target) == expected:
                with self.global_lock():
                    if _regular(local) and local.stat().st_ino == target.stat().st_ino:
                        local.touch()
                self.history.metrics["cache_hits"] += 1
                return local, target, handle
            with self.global_lock():
                if _regular(local) and local.stat().st_ino == target.stat().st_ino:
                    local.unlink(missing_ok=True)
        except BaseException:
            self.release(target, handle)
            raise
        self.release(target, handle)
        return None

    def obtain(self, obj: dict[str, Any]) -> tuple[Path, Path, BinaryIO]:
        key, expected = obj["key"], obj["sha256"]
        local = self.root / hashlib.sha256(key.encode()).hexdigest()
        declared = obj.get("bytes")
        if declared is not None and (
            isinstance(declared, bool) or not isinstance(declared, int) or declared < 0
        ):
            raise ValueError("Invalid object size")
        amount = self.limit if declared is None else declared
        if amount > self.limit:
            raise ValueError("Object exceeds cache budget; compact into smaller partitions")
        hit = self._try_cached(local, expected)
        if hit is not None:
            return hit
        with self.object_lock(key):
            hit = self._try_cached(local, expected)
            if hit is not None:
                return hit

            record = self.downloads / (uuid4().hex + ".reserve")
            temporary = record.with_suffix(".part")
            reservation: BinaryIO | None = None
            try:
                with self.global_lock():
                    self._make_room(amount)
                    reservation = _open(record)
                    fcntl.flock(reservation.fileno(), fcntl.LOCK_EX)
                    reservation.write(str(amount).encode())
                    reservation.flush()
                self.history.metrics["get_requests"] += 1
                response = self.history.client.get_object(Bucket=self.history.bucket, Key=key)
                hasher, size = hashlib.sha256(), 0
                with _open(temporary) as output:
                    stream = response["Body"]
                    try:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                            size += len(chunk)
                            if size > amount:
                                raise ValueError("Download exceeds its reserved cache budget")
                            output.write(chunk)
                            hasher.update(chunk)
                    finally:
                        stream.close()
                    output.flush()
                    os.fsync(output.fileno())
                self.history.metrics["download_bytes"] += size
                if declared is not None and size != declared:
                    raise ValueError("Object size differs from its completed manifest")
                if hasher.hexdigest() != expected:
                    raise ValueError("Object checksum mismatch")
                with self.global_lock():
                    os.replace(temporary, local)
                    record.unlink()
                    target, handle = self._pin(local)
                return local, target, handle
            finally:
                if reservation is not None:
                    with self.global_lock():
                        temporary.unlink(missing_ok=True)
                        record.unlink(missing_ok=True)
                    reservation.close()


@contextmanager
def pin_cached(history: History, obj: dict[str, Any]) -> Iterator[Path]:
    """Hold verified bytes within the global budget for the reader's lifetime."""
    cache = _Cache(history)
    _local, target, handle = cache.obtain(obj)
    try:
        yield target
    finally:
        cache.release(target, handle)


def cached_path(history: History, obj: dict[str, Any]) -> Path:
    """Return an evictable cache name; readers should use ``pin_cached`` instead."""
    cache = _Cache(history)
    local, target, handle = cache.obtain(obj)
    cache.release(target, handle)
    return local


def evict_cache(history: History, keep: Path) -> None:
    cache = _Cache(history)
    with cache.global_lock():
        cache._make_room(0, keep=keep)


@contextmanager
def cache_object_lock(history: History, key: str) -> Iterator[None]:
    """Fence remote deletion/cache invalidation against an in-flight fill."""
    with _Cache(history).object_lock(key):
        yield

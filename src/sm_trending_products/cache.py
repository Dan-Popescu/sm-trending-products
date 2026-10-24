"""Bounded verified cache with independent fills and crash-recoverable pins.

The global lock protects only names and accounting. Remote reads and checksums
hold an object lock, while pins/reservations hold crash-released kernel locks.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import stat
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO
from uuid import uuid4

from psycopg.errors import DeadlockDetected, LockNotAvailable, QueryCanceled

if TYPE_CHECKING:
    from .history import History


class CacheCapacityError(ValueError):
    """Active readers/downloads leave insufficient shared-cache space."""


# Operation-local reuse must not pin a large analytical corpus. These are upper
# bounds, further reduced to one quarter of the shared cache's payload budget.
_SESSION_MAX_BYTES = 256 * 1024 * 1024
_SESSION_MAX_PINS = 64
_Identity = tuple[int, int, int, int, int]
_LOCAL_PREFIX = "window-"


def _identity(path: Path) -> _Identity:
    info = path.stat(follow_symlinks=False)
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _declared_size(obj: dict[str, Any]) -> int | None:
    declared = obj.get("bytes")
    if declared is not None and (
        isinstance(declared, bool) or not isinstance(declared, int) or declared < 0
    ):
        raise ValueError("Invalid object size")
    return declared


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
        self.verified_identity: _Identity | None = None

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

    def paths(self, key: str) -> tuple[Path, ...]:
        """Keep the original opaque name readable during a cache upgrade."""
        name = hashlib.sha256(key.encode()).hexdigest()
        legacy = self.root / name
        return (
            (self.root / (_LOCAL_PREFIX + name), legacy)
            if key.startswith("local-feature-state/")
            else (legacy,)
        )

    def discard(self, key: str) -> None:
        with self.global_lock():
            for path in self.paths(key):
                path.unlink(missing_ok=True)

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
                    if amount < 0:
                        raise ValueError("Invalid cache reservation size") from None
                    if amount > self.limit:
                        raise CacheCapacityError(
                            "Existing downloads exceed the reduced cache budget; "
                            "retry after completion"
                        ) from None
                    reserved += amount
                    continue
                record.with_suffix(".part").unlink(missing_ok=True)
                record.unlink(missing_ok=True)
        return reserved

    def _snapshot(
        self,
    ) -> tuple[dict[tuple[int, int], int], set[tuple[int, int]], list[tuple[Path, os.stat_result]]]:
        """Stat each payload once while the shared name/accounting lock is held."""
        sizes: dict[tuple[int, int], int] = {}
        pinned: set[tuple[int, int]] = set()
        candidates: list[tuple[Path, os.stat_result]] = []
        for directory in (self.root, self.pins):
            with os.scandir(directory) as entries:
                for entry in entries:
                    if directory == self.root and entry.name.startswith("."):
                        continue
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISREG(info.st_mode):
                        inode = info.st_dev, info.st_ino
                        sizes[inode] = info.st_size
                        if directory == self.pins:
                            pinned.add(inode)
                        else:
                            candidates.append((Path(entry.path), info))
        return sizes, pinned, candidates

    def _make_room(self, amount: int, *, keep: Path | None = None) -> None:
        reserved = self._sweep()
        sizes, pinned, candidates = self._snapshot()
        used = sum(sizes.values())
        if used + reserved + amount <= self.limit:
            return
        candidates.sort(
            key=lambda item: (item[0].name.startswith(_LOCAL_PREFIX), item[1].st_mtime_ns)
        )
        names: dict[tuple[int, int], int] = {}
        for _path, info in candidates:
            inode = (info.st_dev, info.st_ino)
            names[inode] = names.get(inode, 0) + 1
        for path, info in candidates:
            if used + reserved + amount <= self.limit:
                return
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

    def _try_cached(
        self, local: Path, expected: str, *, refresh: bool = True
    ) -> tuple[Path, Path, BinaryIO] | None:
        with self.global_lock():
            if local.is_symlink():
                raise ValueError("Cache file escaped its configured root")
            pinned = self._pin(local) if _regular(local) else None
        if pinned is None:
            return None
        target, handle = pinned
        try:
            before = _identity(target)
            if file_digest(target) == expected:
                after = _identity(target)
                with self.global_lock():
                    if _regular(local) and local.stat().st_ino == target.stat().st_ino:
                        if refresh:
                            local.touch()
                    # Ordinary reads preserve their existing verification path.
                    # Sessions can reuse only a checksum whose inode metadata
                    # stayed unchanged throughout verification and afterwards.
                    if not refresh and before == after == _identity(target):
                        self.verified_identity = after
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

    def _reconcile_budget(self, local: Path) -> None:
        configuration = (self.root, self.limit)
        if getattr(self.history, "_cache_budget_configuration", None) != configuration:
            with self.global_lock():
                if getattr(self.history, "_cache_budget_configuration", None) != configuration:
                    # Reconcile once after restart or a reduced cache limit.
                    keep = local if _regular(local) and local.stat().st_size <= self.limit else None
                    self._make_room(0, keep=keep)
                    self.history._cache_budget_configuration = configuration

    def obtain_local(self, obj: dict[str, Any]) -> tuple[Path, Path, BinaryIO] | None:
        """An optional local state never invokes a provider on a miss."""
        key, expected = obj["key"], obj["sha256"]
        if not key.startswith("local-feature-state/") or obj.get("local_only") is not True:
            raise ValueError("Not a local projection descriptor")
        local, legacy = self.paths(key)
        self._reconcile_budget(local if _regular(local) else legacy)
        hit = self._try_cached(local, expected)
        if hit is not None:
            return hit
        hit = self._try_cached(legacy, expected)
        if hit is not None:
            # Never recreate a name removed by retirement. An existing reader
            # may finish on its pin even when its original name has disappeared.
            try:
                # Retirement also removes names under this lock. Promotion is
                # only a rename of a still-present, verified inode, so it need
                # not wait behind a distinct cold fill on the same stripe.
                with self.global_lock():
                    if (
                        not local.exists()
                        and _regular(legacy)
                        and legacy.stat().st_ino == hit[1].stat().st_ino
                    ):
                        legacy.rename(local)
                        return local, hit[1], hit[2]
            except BaseException:
                self.release(hit[1], hit[2])
                raise
        return hit

    def admit_local(
        self, body: bytes, descriptor: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Admit under the caller's object stripe and SQL retirement fence."""
        checksum = hashlib.sha256(body).hexdigest()
        key = f"local-feature-state/{checksum}.parquet"
        obj = descriptor or {"key": key, "sha256": checksum, "bytes": len(body), "local_only": True}
        if (
            not obj["key"].startswith("local-feature-state/")
            or obj.get("local_only") is not True
            or obj["sha256"] != checksum
            or obj["bytes"] != len(body)
        ):
            raise ValueError("Invalid local projection descriptor")
        key = obj["key"]
        if len(body) > self.limit:
            return None
        local = self.paths(key)[0]
        self._reconcile_budget(local)
        hit = self._try_cached(local, checksum)
        if hit is not None:
            self.release(hit[1], hit[2])
            return obj
        record = self.downloads / (uuid4().hex + ".reserve")
        temporary = record.with_suffix(".part")
        reservation: BinaryIO | None = None
        try:
            with self.global_lock():
                self._make_room(len(body))
                reservation = _open(record)
                fcntl.flock(reservation.fileno(), fcntl.LOCK_EX)
                reservation.write(str(len(body)).encode())
                reservation.flush()
            with _open(temporary) as output:
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            with self.global_lock():
                os.replace(temporary, local)
                record.unlink()
            return obj
        finally:
            if reservation is not None:
                with self.global_lock():
                    temporary.unlink(missing_ok=True)
                    record.unlink(missing_ok=True)
                reservation.close()

    def obtain(self, obj: dict[str, Any], *, refresh: bool = True) -> tuple[Path, Path, BinaryIO]:
        self.verified_identity = None
        key, expected = obj["key"], obj["sha256"]
        local = self.root / hashlib.sha256(key.encode()).hexdigest()
        self._reconcile_budget(local)
        declared = _declared_size(obj)
        amount = self.limit if declared is None else declared
        if amount > self.limit:
            raise ValueError("Object exceeds cache budget; compact into smaller partitions")
        hit = self._try_cached(local, expected, refresh=refresh)
        if hit is not None:
            return hit
        with self.object_lock(key):
            hit = self._try_cached(local, expected, refresh=refresh)
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


@dataclass
class _ReadPin:
    local: Path
    target: Path
    handle: BinaryIO
    identity: _Identity
    expected: str
    declared: int | None
    users: int = 0
    retained: bool = True


class _ReadSession:
    def __init__(self, history: History):
        self.history = history
        self.owner = threading.get_ident()
        self.cache = _Cache(history)
        self.limit = min(_SESSION_MAX_BYTES, self.cache.limit // 4)
        self.entries: OrderedDict[str, _ReadPin] = OrderedDict()
        self.used = 0

    def _dispose(self, entry: _ReadPin) -> None:
        try:
            # Refresh ordinary eviction recency only after reuse has ended.
            # Touching on every hit would itself invalidate the saved identity.
            with self.cache.global_lock():
                if _regular(entry.local) and _identity(entry.local) == entry.identity:
                    entry.local.touch()
        finally:
            self.cache.release(entry.target, entry.handle)

    def _forget(self, key: str) -> None:
        entry = self.entries.pop(key)
        self.used -= entry.identity[2]
        entry.retained = False
        if not entry.users:
            self._dispose(entry)

    def _release_idle(self) -> None:
        for key, entry in list(self.entries.items()):
            if not entry.users:
                self._forget(key)

    def close(self) -> None:
        for key in list(self.entries):
            self._forget(key)

    def _unchanged(self, entry: _ReadPin) -> bool:
        # Never resurrect an object retired/replaced through its ordinary name.
        # A retained pin keeps bytes safe for an existing reader, but is not
        # authority to admit another read after retention removes that name.
        with self.cache.global_lock():
            if entry.local.is_symlink():
                raise ValueError("Cache file escaped its configured root")
            return (
                _regular(entry.local)
                and _regular(entry.target)
                and _identity(entry.local) == entry.identity
                and _identity(entry.target) == entry.identity
            )

    def _retain(self, key: str, entry: _ReadPin) -> bool:
        size = entry.identity[2]
        if size > self.limit or not self._unchanged(entry):
            return False
        for old_key, old in list(self.entries.items()):
            if self.used + size <= self.limit and len(self.entries) < _SESSION_MAX_PINS:
                break
            if not old.users:
                self._forget(old_key)
        if self.used + size > self.limit or len(self.entries) >= _SESSION_MAX_PINS:
            return False
        self.entries[key] = entry
        self.used += size
        return True

    @contextmanager
    def pin(self, obj: dict[str, Any]) -> Iterator[Path]:
        declared = _declared_size(obj)
        key, expected = obj["key"], obj["sha256"]
        entry = self.entries.get(key)
        if entry is not None:
            if entry.expected == expected and entry.declared == declared and self._unchanged(entry):
                self.entries.move_to_end(key)
                self.history.metrics["cache_hits"] += 1
            else:
                self._forget(key)
                entry = None
        if entry is None:
            try:
                local, target, handle = self.cache.obtain(obj, refresh=False)
            except CacheCapacityError:
                # Idle session pins are optional. Release them before the normal
                # admission retry; genuine active readers retain their fences.
                self._release_idle()
                local, target, handle = self.cache.obtain(obj, refresh=False)
            verified = self.cache.verified_identity
            if verified is None:
                # Cold downloads already verify their stream, but rename/pin
                # changes inode metadata afterwards. Keep the ordinary lifetime;
                # the next warm read can establish a stable checksum identity.
                try:
                    yield target
                finally:
                    self.cache.release(target, handle)
                return
            entry = _ReadPin(local, target, handle, verified, expected, declared)
            try:
                entry.retained = self._retain(key, entry)
            except BaseException:
                self.cache.release(target, handle)
                raise
        entry.users += 1
        try:
            yield entry.target
        finally:
            entry.users -= 1
            if not entry.retained and not entry.users:
                self._dispose(entry)


_read_session: ContextVar[_ReadSession | None] = ContextVar("verified_read_session", default=None)


@contextmanager
def read_session(history: History) -> Iterator[None]:
    """Reuse verified, unchanged pins within one bounded analytical operation."""
    current = _read_session.get()
    if (
        current is not None
        and current.history is history
        and current.owner == threading.get_ident()
    ):
        yield
        return
    session = _ReadSession(history)
    token = _read_session.set(session)
    try:
        yield
    finally:
        _read_session.reset(token)
        session.close()


@contextmanager
def pin_cached(history: History, obj: dict[str, Any]) -> Iterator[Path]:
    """Hold verified bytes within the global budget for the reader's lifetime."""
    session = _read_session.get()
    if (
        session is not None
        and session.history is history
        and session.owner == threading.get_ident()
    ):
        if (
            session.cache.root == history.settings.cache_dir.resolve()
            and session.cache.limit == history.settings.cache_max_bytes
        ):
            with session.pin(obj) as target:
                yield target
            return
        session._release_idle()
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
def cache_object_lock(history: History, key: str) -> Iterator[Callable[[], None]]:
    """Fence deletion against fills; the yielded operation removes every alias."""
    cache = _Cache(history)
    with cache.object_lock(key):
        yield lambda: cache.discard(key)


def discard_retired_local(history: History, key: str) -> None:
    """Reclaim an obsolete version; durable retirement remains independently retryable."""
    if not key.startswith("local-feature-state/"):
        return
    try:
        with history.catalog.transaction() as connection:
            history.catalog.guard_retention(connection)
            with cache_object_lock(history, key) as discard:
                retired = connection.execute(
                    "SELECT 1 FROM retention_objects WHERE object_key=%s AND local_only", (key,)
                ).fetchone()
                if retired is not None:
                    discard()
    except (OSError, ValueError, DeadlockDetected, LockNotAvailable, QueryCanceled):
        # The existing outbox is still authoritative, including cleanup failure.
        return


def local_cache_descriptor(body: bytes, identity: str) -> dict[str, Any]:
    """Bind an immutable local state to both its bytes and publication ownership."""
    checksum = hashlib.sha256(body).hexdigest()
    name = hashlib.sha256((identity + ":" + checksum).encode()).hexdigest()
    return {
        "key": f"local-feature-state/{name}.parquet",
        "sha256": checksum,
        "bytes": len(body),
        "local_only": True,
    }


def store_local_cached(
    history: History,
    body: bytes,
    *,
    descriptor: dict[str, Any] | None = None,
    connection: Any = None,
) -> dict[str, Any] | None:
    """Admit disposable verified state without crossing a retirement tombstone."""
    obj = descriptor or local_cache_descriptor(body, "standalone")
    try:
        transaction = (
            history.catalog.transaction() if connection is None else nullcontext(connection)
        )
        with transaction as current:
            history.catalog.guard_retention(current)
            cache = _Cache(history)
            with cache.object_lock(obj["key"]):
                # Check after acquiring the stripe: an admission that waited
                # behind retirement must not resurrect the removed original.
                retired = current.execute(
                    "SELECT 1 FROM retention_objects WHERE object_key=%s", (obj["key"],)
                ).fetchone()
                if retired is not None:
                    return None
                return cache.admit_local(body, obj)
    except (CacheCapacityError, OSError, ValueError):
        return None


@contextmanager
def pin_local_cached(history: History, obj: dict[str, Any]) -> Iterator[Path | None]:
    """Pin existing state or report a miss, with no remote fallback or fake GET."""
    cache: _Cache | None = None
    hit: tuple[Path, Path, BinaryIO] | None = None
    try:
        cache = _Cache(history)
        hit = cache.obtain_local(obj)
    except (CacheCapacityError, OSError, ValueError):
        pass
    if hit is None:
        yield None
        return
    assert cache is not None
    try:
        yield hit[1]
    finally:
        cache.release(hit[1], hit[2])

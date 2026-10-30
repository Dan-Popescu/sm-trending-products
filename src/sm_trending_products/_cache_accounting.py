"""Reconstructible conservative charges for the existing shared file cache.

The caller holds the existing global flock. A charge includes every admitted
reservation, even after deletion, until pressure rebuilds the inode inventory.
An ordered filesystem watch detects writers which do not maintain this record;
directory timestamps alone are insufficient during a mixed-version release.
Unavailable/lost watches and corrupt records fall back to the ordinary inventory.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import struct
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from .history import History


_RECORD = ".cache-accounting"
_EVENT = struct.Struct("iIII")
_MODIFY = 0x00000002
_NAMES = 0x00000100 | 0x00000200 | 0x00000040 | 0x00000080
_LOST = 0x00000400 | 0x00000800 | 0x00008000
_OVERFLOW = 0x00004000


def _encoded(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


@dataclass
class _Charge:
    # An overestimate is safe. A missing estimate requires an inode inventory.
    bytes: int | None = None


class _Accounting:
    def __init__(self, directories: tuple[Path, Path, Path]):
        self.directories = directories
        self.process = os.getpid()
        self.descriptor = -1
        # Retained checksums must notice writes even when another transaction
        # already drained the event queue and inode timestamps have coalesced.
        self.payload_generation = 0
        self.watches: dict[int, int] = {}
        self.established = False
        self.finalizer: weakref.finalize | None = None
        try:
            library = ctypes.CDLL(None, use_errno=True)
            initialize = library.inotify_init1
            initialize.argtypes, initialize.restype = [ctypes.c_int], ctypes.c_int
            add = library.inotify_add_watch
            add.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            add.restype = ctypes.c_int
            descriptor = initialize(os.O_NONBLOCK | os.O_CLOEXEC)
            if descriptor < 0:
                return
            self.descriptor = descriptor
            self.finalizer = weakref.finalize(self, os.close, descriptor)
            for index, directory in enumerate(directories):
                watch = add(descriptor, os.fsencode(directory), _MODIFY | _NAMES | _LOST)
                if watch < 0:
                    self.close()
                    return
                self.watches[watch] = index
        except (AttributeError, OSError):
            self.close()

    def close(self) -> None:
        if self.descriptor >= 0:
            self.payload_generation += 1
        if self.finalizer is not None:
            self.finalizer()
        self.descriptor = -1

    def _events(self) -> tuple[bool, bool]:
        """Return (last mutations committed, immutable payload was modified)."""
        clean, uncertain, modified = self.established, False, False
        while self.descriptor >= 0:
            try:
                data = os.read(self.descriptor, 65536)
            except BlockingIOError:
                break
            except OSError:
                self.close()
                break
            if not data:
                self.close()
                break
            position = 0
            while position < len(data):
                watch, mask, _cookie, length = _EVENT.unpack_from(data, position)
                position += _EVENT.size
                name = data[position : position + length].split(b"\0", 1)[0]
                position += length
                if mask & _OVERFLOW:
                    uncertain = True
                    modified = True
                    self.payload_generation += 1
                    continue
                if mask & _LOST or watch not in self.watches:
                    self.close()
                    return False, True
                directory = self.watches[watch]
                if directory == 0 and name == _RECORD.encode():
                    if mask & _MODIFY:
                        clean = True
                    elif mask & _NAMES:
                        clean = False
                elif directory != 0 or not name.startswith(b"."):
                    # Streamed temporary bytes are already covered by the
                    # locked reservation. Its name was created under the flock.
                    if directory == 2 and name.endswith(b".part") and not mask & _NAMES:
                        continue
                    clean = False
                    if directory != 2 and mask & _MODIFY:
                        modified = True
                        self.payload_generation += 1
        return clean and not uncertain and self.descriptor >= 0, modified

    def _versions(self) -> list[list[int]]:
        result = []
        for directory in self.directories:
            info = directory.stat(follow_symlinks=False)
            result.append([info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns])
        return result

    @staticmethod
    def _write(handle: BinaryIO, body: bytes) -> None:
        handle.seek(0)
        handle.truncate()
        handle.write(body)
        handle.flush()

    @contextmanager
    def transaction(self) -> Iterator[_Charge]:
        descriptor = os.open(
            self.directories[0] / _RECORD, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "r+b") as record:
            clean, _modified = self._events()
            charge = _Charge()
            try:
                saved = json.loads(record.read(2049))
                if not isinstance(saved, dict):
                    raise ValueError("Invalid cache accounting record")
                checksum = saved.pop("checksum")
                amount = saved["bytes"]
                if (
                    clean
                    and self.established
                    and saved["version"] == 1
                    and type(amount) is int
                    and amount >= 0
                    and saved["directories"] == self._versions()
                    and checksum == hashlib.sha256(_encoded(saved)).hexdigest()
                ):
                    charge.bytes = amount
            except (ValueError, KeyError, TypeError):
                pass
            # A hard process exit at any later point cannot leave a reusable
            # pre-mutation estimate. Buffered data is flushed before yielding.
            self._write(record, b"")
            try:
                yield charge
            finally:
                _clean, modified = self._events()
                if modified:
                    # Cache payloads are immutable. An in-place resize or write
                    # is not an admission and must be reconciled independently.
                    charge.bytes = None
                if charge.bytes is not None and self.descriptor >= 0:
                    saved = {"version": 1, "bytes": charge.bytes, "directories": self._versions()}
                    saved["checksum"] = hashlib.sha256(_encoded(saved)).hexdigest()
                    self._write(record, _encoded(saved))
                    self.established = True


_owners: weakref.WeakKeyDictionary[History, _Accounting] = weakref.WeakKeyDictionary()


def accounting(history: History, directories: tuple[Path, Path, Path]) -> _Accounting:
    """Keep one bounded watcher alive across short-lived _Cache instances."""
    current = _owners.get(history)
    if current is None or current.directories != directories or current.process != os.getpid():
        if current is not None:
            current.close()
        current = _Accounting(directories)
        _owners[history] = current
    return current

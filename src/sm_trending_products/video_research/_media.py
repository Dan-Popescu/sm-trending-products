"""Owned job files and verified media, without source or durable job-state decisions."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .contracts import ExecutionConfig, VideoResearchDenied


def file_hash(path: Path, maximum: int) -> tuple[str, int]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum:
            raise VideoResearchDenied("invalid_owned_file")
        total, checksum = 0, hashlib.sha256()
        while block := stream.read(1024 * 1024):
            total += len(block)
            if total > maximum:
                raise VideoResearchDenied("file_byte_limit")
            checksum.update(block)
        after = os.fstat(stream.fileno())
        linked = path.stat(follow_symlinks=False)
        if (
            (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or (before.st_dev, before.st_ino) != (linked.st_dev, linked.st_ino)
            or total != before.st_size
        ):
            raise VideoResearchDenied("file_changed_during_verification")
        return checksum.hexdigest(), total


class OwnedJobFiles:
    """Own only a directory created by this invocation, never an existing job's bytes."""

    def __init__(self, config: ExecutionConfig, job_id: str):
        if re.fullmatch(r"[a-f0-9]{64}", job_id) is None:
            raise VideoResearchDenied("media_path_escaped")
        self._config = config
        self.directory = config.work_dir / job_id
        self._created = False

    def create(self) -> None:
        self.directory.mkdir(mode=0o700)
        self._created = True

    def discard(self) -> None:
        """Caller must have an acknowledged durable failure before requesting cleanup."""
        if self._created and self.directory.is_dir() and not self.directory.is_symlink():
            try:
                shutil.rmtree(self.directory)
            except OSError:
                pass  # Keep the original failure; capacity admission bounds leftover files.

    def media(self, expected: dict | None = None) -> VerifiedMedia:
        if (
            self.directory.is_symlink()
            or self.directory.resolve().parent != self._config.work_dir.resolve()
        ):
            raise VideoResearchDenied("media_path_escaped")
        return VerifiedMedia.inspect(
            self.directory / "video.mp4", self._config.max_media_bytes, expected
        )


@dataclass(frozen=True)
class VerifiedMedia:
    """A bounded file tied to its recorded identity; callers need no location rules."""

    _path: Path
    _maximum: int
    sha256: str
    bytes: int

    @classmethod
    def inspect(cls, path: Path, maximum: int, expected: dict | None = None) -> VerifiedMedia:
        checksum, size = file_hash(path, maximum)
        if expected is not None and (
            checksum != expected["sha256"] or size != expected.get("bytes", size)
        ):
            raise VideoResearchDenied("media_checksum_mismatch")
        return cls(path, maximum, checksum, size)

    def metadata(self) -> dict:
        return {"sha256": self.sha256, "bytes": self.bytes}

    def transcription_input(self) -> dict:
        return {"media_path": str(self._path), "media_sha256": self.sha256}

    @contextmanager
    def stage_export(self, destination: Path):
        temporary = destination.with_name("." + destination.name + "." + uuid.uuid4().hex)
        created = False
        try:
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            created = True
            with os.fdopen(descriptor, "wb") as target:
                original = os.open(self._path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(original, "rb") as origin:
                    shutil.copyfileobj(origin, target, length=1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            checksum, size = file_hash(temporary, self._maximum)
            if (checksum, size) != (self.sha256, self.bytes):
                raise VideoResearchDenied("media_changed_during_export")
            yield StagedExport(temporary, destination, checksum, size)
        finally:
            if created:
                temporary.unlink(missing_ok=True)


@dataclass(frozen=True)
class StagedExport:
    """Verified private output awaiting the caller's final source authorization."""

    _temporary: Path
    _destination: Path
    sha256: str
    bytes: int

    def publish(self) -> dict:
        os.link(self._temporary, self._destination)  # Atomic, never overwrites a path.
        return {"bytes": self.bytes, "sha256": self.sha256, "exported": True}


@contextmanager
def transcription_media(payload: dict, config: ExecutionConfig):
    """Verify the input on entry and again after the complete lazy transcription."""
    media = VerifiedMedia.inspect(
        Path(payload["media_path"]),
        config.max_media_bytes,
        {"sha256": payload["media_sha256"]},
    )
    yield str(media._path)
    if file_hash(media._path, config.max_media_bytes) != (media.sha256, media.bytes):
        raise VideoResearchDenied("media_changed_during_transcription")

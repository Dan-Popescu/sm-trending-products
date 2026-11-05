"""Private owned workspaces and isolated optional workers, independent of capture."""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import signal
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from ..contracts import canonical
from .contracts import ExecutionConfig, VideoRequest, VideoResearchDenied


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
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) or total != before.st_size:
            raise VideoResearchDenied("file_changed_during_verification")
        return checksum.hexdigest(), total


@contextmanager
def workspace(config: ExecutionConfig):
    root = config.work_dir
    if root != root.resolve() or root.is_symlink():
        raise VideoResearchDenied("private_workspace_must_not_be_symlinked")
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    marker = root / ".video-research-owned"
    if not marker.exists():
        if any(root.iterdir()):
            raise VideoResearchDenied("private_workspace_must_be_empty_on_first_use")
        with marker.open("x") as output:
            output.write("selected-video-research-v1\n")
    if marker.is_symlink() or marker.read_text() != "selected-video-research-v1\n":
        raise VideoResearchDenied("invalid_workspace_ownership")
    if root.stat().st_mode & 0o077:
        raise VideoResearchDenied("workspace_permissions_must_be_private")
    handle = os.open(root / ".operation.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise VideoResearchDenied("video_research_worker_busy") from None
        yield root
    finally:
        os.close(handle)


def admit_workspace(config: ExecutionConfig) -> None:
    """Check capacity only when admitting new work; cached reads consume no space."""
    root = config.work_dir
    used, entries = 0, 0
    for parent, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            entries += 1
            item = Path(parent) / name
            details = item.lstat()
            if entries > 20000 or stat.S_ISLNK(details.st_mode):
                raise VideoResearchDenied("workspace_inventory_untrusted")
            if stat.S_ISREG(details.st_mode):
                used += details.st_size
    if used + config.max_media_bytes + 2_000_000 > config.max_workspace_bytes:
        raise VideoResearchDenied("video_workspace_capacity")
    free = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
    if free < config.max_media_bytes + 16_000_000:
        raise VideoResearchDenied("video_disk_headroom")


def _inventory(root: Path) -> tuple[int, int]:
    total, entries = 0, 0
    for parent, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            entries += 1
            if entries > 20000:
                raise VideoResearchDenied("workspace_inventory_limit")
            try:
                details = (Path(parent) / name).lstat()
            except FileNotFoundError:
                continue  # Browser temporary files can disappear between enumeration and stat.
            if stat.S_ISREG(details.st_mode):
                total += details.st_size
    return total, entries


def _session(
    pid: int, known: dict[int, tuple[int, int]] | None = None
) -> dict[int, tuple[int, int]]:
    """Track stable owned identities and descendants, including detached browsers."""
    known = known or {}
    snapshot = {}
    page_size = os.sysconf("SC_PAGE_SIZE")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if fields[0] != "Z":
                snapshot[int(entry.name)] = (
                    int(fields[1]),
                    int(fields[3]),
                    int(fields[19]),
                    int(fields[21]) * page_size,
                )
        except (OSError, ValueError, IndexError):
            continue
    root = snapshot.get(pid)
    root_matches = root is not None and (pid not in known or root[2] == known[pid][0])
    members = {
        child: (row[2], row[3])
        for child, row in snapshot.items()
        if (child in known and known[child][0] == row[2]) or (root_matches and row[1] == pid)
    }
    while True:
        descendants = {
            child: (row[2], row[3])
            for child, row in snapshot.items()
            if child not in members and row[0] in members
        }
        if not descendants:
            return members
        members.update(descendants)


def _signal_owned(pid: int, start: int, sig: int) -> None:
    """A pidfd binds the signal to one process even if Linux recycles its PID."""
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        try:
            actual = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])
        except (OSError, ValueError, IndexError):
            return
        if actual == start:
            try:
                signal.pidfd_send_signal(descriptor, sig)
            except ProcessLookupError:
                pass
    finally:
        os.close(descriptor)


def _stop(process: subprocess.Popen, known: dict[int, tuple[int, int]]) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        current = _session(process.pid, known)
        known.update(current)
        for pid, row in current.items():
            _signal_owned(pid, row[0], sig)
        deadline = time.monotonic() + (0.5 if sig == signal.SIGTERM else 3)
        while time.monotonic() < deadline:
            process.poll()
            current = _session(process.pid, known)
            known.update(current)
            if not current:
                process.wait(timeout=1)
                return
            time.sleep(0.02)


class ProcessExecutor:
    """Optional SDKs/models run only in a bounded private child session."""

    def preflight(self, request: VideoRequest, config: ExecutionConfig) -> None:
        if request.selection.synthetic:
            raise VideoResearchDenied("synthetic_external_execution_refused")
        package = (
            "faster_whisper"
            if request.operation == "transcribe"
            else "yt_dlp"
            if request.operation == "download"
            else "TikTokApi"
            if request.provider == "unofficial"
            else None
        )
        if package and importlib.util.find_spec(package) is None:
            raise VideoResearchDenied("video_research_dependency_missing")
        if (
            not Path("/proc/self/stat").is_file()
            or not hasattr(os, "sched_getaffinity")
            or not hasattr(os, "pidfd_open")
            or not hasattr(signal, "pidfd_send_signal")
        ):
            raise VideoResearchDenied("linux_resource_supervision_required")
        from ..release_capacity import _host_limits

        try:
            limits = _host_limits()
        except (OSError, ValueError, KeyError):
            raise VideoResearchDenied("host_resource_measurements_unavailable") from None
        if limits["memory_available_bytes"] < (config.memory_mb + 256) * 1024**2:
            raise VideoResearchDenied("video_worker_memory_headroom_required")
        if request.operation == "comments" and request.provider == "research":
            if config.research is None or not os.environ.get(config.research.token_env):
                raise VideoResearchDenied("research_token_required")
        if request.operation == "transcribe":
            if config.model_dir is None or config.model_sha256 is None:
                raise VideoResearchDenied("provisioned_large_v3_model_required")
            for name in ("model.bin", "config.json", "tokenizer.json", "preprocessor_config.json"):
                path = config.model_dir / name
                if path.is_symlink() or not path.is_file():
                    raise VideoResearchDenied("complete_offline_model_required")

    def execute(self, payload: dict, directory: Path, config: ExecutionConfig) -> dict:
        operation = payload["request"]["operation"]
        duration = (
            config.transcription_timeout_seconds
            if operation == "transcribe"
            else config.timeout_seconds
        )
        environment = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "PATH",
                "PYTHONPATH",
                "HOME",
                "LANG",
                "LC_ALL",
                "SSL_CERT_FILE",
                "SSL_CERT_DIR",
                "REQUESTS_CA_BUNDLE",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "NO_PROXY",
                "http_proxy",
                "https_proxy",
                "no_proxy",
            }
        }
        if operation == "comments":
            key = (
                config.research.token_env
                if payload["request"]["provider"] == "research" and config.research
                else config.unofficial_token_env
            )
            if key in os.environ:
                environment[key] = os.environ[key]
        environment.update(
            {
                key: str(config.cpu_threads)
                for key in (
                    "POLARS_MAX_THREADS",
                    "RAYON_NUM_THREADS",
                    "OMP_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                )
            }
        )
        environment.update(TMPDIR=str(directory), HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
        encoded = canonical({**payload, "config": config.model_dump(mode="json")}).encode()
        if len(encoded) > 64000:
            raise VideoResearchDenied("worker_request_limit")
        process = subprocess.Popen(
            [sys.executable, "-m", "sm_trending_products.video_research.worker", str(directory)],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
            start_new_session=True,
        )
        known: dict[int, tuple[int, int]] = {}
        try:
            known.update(_session(process.pid))
            assert process.stdin is not None
            process.stdin.write(encoded)
            process.stdin.close()
            deadline = time.monotonic() + duration
            next_disk_check = 0.0
            while process.poll() is None:
                current = _session(process.pid, known)
                known.update(current)
                if not current and process.poll() is not None:
                    break
                if (
                    not current
                    or len(current) > 128
                    or sum(row[1] for row in current.values()) > config.memory_mb * 1024**2
                ):
                    raise VideoResearchDenied("video_worker_memory_or_process_limit")
                if time.monotonic() >= next_disk_check:
                    used, _ = _inventory(config.work_dir)
                    free = (
                        os.statvfs(config.work_dir).f_bavail * os.statvfs(config.work_dir).f_frsize
                    )
                    if used > config.max_workspace_bytes or free < 16_000_000:
                        raise VideoResearchDenied("video_workspace_runtime_limit")
                    next_disk_check = time.monotonic() + 1
                if time.monotonic() >= deadline:
                    raise VideoResearchDenied("video_worker_timeout")
                time.sleep(0.05)
            if process.returncode != 0:
                raise VideoResearchDenied("video_worker_failed")
            result = directory / "result.json"
            file_hash(result, 2_000_000)
            outcome = json.loads(result.read_bytes())
            if not isinstance(outcome, dict) or outcome.get("ok") is not True:
                reason = outcome.get("reason") if isinstance(outcome, dict) else None
                if isinstance(reason, str) and re_safe_reason(reason):
                    raise VideoResearchDenied(reason)
                raise VideoResearchDenied("video_worker_failed")
            return outcome["result"]
        finally:
            _stop(process, known)


def re_safe_reason(value: str) -> bool:
    return 0 < len(value) <= 96 and all(c in "abcdefghijklmnopqrstuvwxyz_" for c in value)

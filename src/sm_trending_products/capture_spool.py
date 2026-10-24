"""Bounded local receipt maintenance under authoritative removal policy."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .collector import CollectionError
from .contracts import now, utc
from .source_contracts import SOURCES

MAX_RECEIPT_BYTES = 32 * 1024 * 1024


def read_receipt(path: Path) -> dict:
    """Verify the actual stored response span without changing float encoding."""
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_RECEIPT_BYTES:
        raise ValueError("Invalid receipt file")
    with path.open("rb") as stream:
        payload = stream.read(MAX_RECEIPT_BYTES + 1)
    if len(payload) > MAX_RECEIPT_BYTES:
        raise ValueError("Receipt file too large")
    text = payload.decode("utf-8")
    decoder = json.JSONDecoder()
    position = 1
    if not text.startswith("{"):
        raise ValueError("Invalid receipt object")
    receipt, response_hash = {}, None
    while True:
        while position < len(text) and text[position].isspace():
            position += 1
        if position < len(text) and text[position] == "}":
            position += 1
            break
        key, position = decoder.raw_decode(text, position)
        if not isinstance(key, str) or key in receipt:
            raise ValueError("Invalid receipt field")
        while position < len(text) and text[position].isspace():
            position += 1
        if text[position : position + 1] != ":":
            raise ValueError("Invalid receipt field separator")
        position += 1
        while position < len(text) and text[position].isspace():
            position += 1
        start = position
        value, position = decoder.raw_decode(text, position)
        receipt[key] = value
        if key == "response":
            response_hash = hashlib.sha256(text[start:position].encode()).hexdigest()
        while position < len(text) and text[position].isspace():
            position += 1
        if text[position : position + 1] == ",":
            position += 1
        elif text[position : position + 1] != "}":
            raise ValueError("Invalid receipt delimiter")
    if text[position:].strip():
        raise ValueError("Trailing receipt content")
    if receipt.get("capture_version") == "1":
        if response_hash is None or response_hash != receipt.get("response_sha256"):
            raise ValueError("Receipt checksum mismatch")
    elif receipt.get("reservation_version") != "1":
        raise ValueError("Unsupported receipt protocol")
    capture, request = receipt["capture"], receipt["request"]
    if (
        capture["capture_id"] != path.stem
        or not re.fullmatch("[0-9a-f]{64}", capture["task_id"])
        or request["source"] not in SOURCES
        or not isinstance(request["account_id"], str)
        or not request["account_id"]
        or not isinstance(request["synthetic"], bool)
    ):
        raise ValueError("Invalid receipt scope")
    utc(capture.get("captured_at", capture.get("started_at")))
    return receipt


class CaptureSpool:
    def __init__(self, settings, catalog, history):
        self.settings, self.catalog, self.history = settings, catalog, history
        self.directory = settings.spool_dir / "captures"
        self._entries: Any = None
        self._scan_lock = threading.Lock()

    def close(self) -> None:
        with self._scan_lock:
            if self._entries is not None:
                self._entries.close()
                self._entries = None

    @contextmanager
    def _lock(self, capture_id: str):
        path = self.directory / f"capture-{capture_id[:2]}.lock"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "a+b") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    def _remove_files(self, capture_id: str, suffixes: tuple[str, ...]) -> None:
        descriptor = os.open(
            self.directory / ".budget.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "a+b") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            for suffix in suffixes:
                path = self.directory / f"{capture_id}{suffix}"
                try:
                    if not stat.S_ISREG(path.lstat().st_mode):
                        raise ValueError("Invalid spool file")
                    path.unlink()
                except FileNotFoundError:
                    continue
            directory = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def remove_committed(self, capture_id: str, receipt: dict) -> None:
        path = self.directory / f"{capture_id}.json"
        if not path.exists():
            return
        with self._lock(capture_id):
            try:
                stored = read_receipt(path)
            except FileNotFoundError:
                return
            if stored != receipt:
                raise ValueError("Committed receipt differs from local input")
            self._remove_files(capture_id, (".json", ".reserve"))

    def request_removal(
        self,
        source: str,
        account_id: str,
        synthetic: bool,
        reason: str,
        *,
        captured_before: datetime | None = None,
    ) -> dict:
        cutoff = utc(captured_before) if captured_before is not None else now()
        if (
            source not in SOURCES
            or not account_id
            or len(account_id) > 256
            or not isinstance(synthetic, bool)
            or not re.fullmatch("[a-z_]{1,80}", reason)
            or cutoff > now()
        ):
            raise ValueError("Invalid scoped spool removal")
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection, exclusive=True)
            connection.execute(
                "INSERT INTO capture_spool_revocations"
                "(source,account_id,synthetic,captured_before,reason) "
                "VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (source, account_id, synthetic, cutoff, reason),
            )
        return {
            "source": source,
            "account_id": account_id,
            "synthetic": synthetic,
            "captured_before": cutoff.isoformat(),
            "reason": reason,
        }

    def _authority(self, connection, receipt: dict) -> str | None:
        request, capture = receipt["request"], receipt["capture"]
        acquired_at = utc(capture.get("captured_at", capture.get("started_at")))
        started_at = utc(capture.get("started_at", acquired_at))
        revoked = connection.execute(
            "SELECT 1 FROM capture_spool_revocations WHERE source=%s AND account_id=%s "
            "AND synthetic=%s AND captured_before>=%s LIMIT 1",
            (
                request["source"],
                request["account_id"],
                request["synthetic"],
                min(started_at, acquired_at),
            ),
        ).fetchone()
        if revoked:
            return "capture_source_removed"
        policy = connection.execute(
            "SELECT retention_days FROM source_policies WHERE source=%s AND verified",
            (request["source"],),
        ).fetchone()
        # A reservation's timestamp precedes body completion. Allow one full
        # collector timeout before inferring expiry for incomplete inputs.
        grace = self.settings.collector_timeout_seconds if "reservation_version" in receipt else 0
        if (
            policy
            and acquired_at + timedelta(days=policy["retention_days"], seconds=grace) <= now()
        ):
            return "capture_retention_expired"
        dataset = connection.execute(
            "SELECT state FROM datasets WHERE dataset_id=%s", (capture["capture_id"],)
        ).fetchone()
        if dataset and dataset["state"] in {"invalidated", "deleted"}:
            return "capture_source_removed"
        return None

    def assert_publishable(self, connection, receipt: dict) -> None:
        self.catalog.guard_retention(connection)
        reason = self._authority(connection, receipt)
        if reason:
            raise CollectionError(reason, 3600)
        request = receipt["request"]
        if not request["synthetic"]:
            row = connection.execute(
                "SELECT state FROM source_capabilities WHERE source=%s AND account_id=%s "
                "AND capability='collect' ORDER BY available_at DESC,id DESC LIMIT 1",
                (request["source"], request["account_id"]),
            ).fetchone()
            policy = connection.execute(
                "SELECT verified FROM source_policies WHERE source=%s", (request["source"],)
            ).fetchone()
            if not row or row["state"] != "available" or not policy or not policy["verified"]:
                raise CollectionError("source_access_unverified", 3600)

    def _retire(self, path: Path) -> bool:
        with self._lock(path.stem):
            receipt = read_receipt(path)
            with self.catalog.transaction() as connection:
                self.catalog.guard_retention(connection, exclusive=True)
                reason = self._authority(connection, receipt)
                if not reason:
                    return False
                capture = receipt["capture"]
                connection.execute(
                    "UPDATE collection_tasks SET state='cancelled',"
                    "lease_owner=NULL,lease_until=NULL,lease_version=lease_version+1,"
                    "last_error=%s WHERE task_id=%s AND page_index=%s "
                    "AND capture_attempt=%s AND state IN ('pending','leased','quarantined')",
                    (
                        reason,
                        capture["task_id"],
                        capture["page_index"],
                        capture.get("capture_attempt", 0),
                    ),
                )
                dataset = connection.execute(
                    "SELECT state FROM datasets WHERE dataset_id=%s", (path.stem,)
                ).fetchone()
            # Cancellation is durable before touching files. A stale publisher
            # is fenced by task ownership and the publication-time policy guard.
            if dataset and dataset["state"] in {"staged", "committed"}:
                self.catalog.invalidate_dataset(path.stem, reason)
            self._remove_files(path.stem, (".json", ".part", ".reserve"))
            return True

    def scan(
        self, limit: int = 20, *, apply: bool = False, retention: bool = True, remove_committed=None
    ) -> dict:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("Spool maintenance limit must be between 1 and 1000")
        result = {
            "scanned": 0,
            "complete": False,
            "committed": 0,
            "retired": 0,
            "protected": 0,
            "invalid": 0,
            "released": 0,
            "apply": apply,
        }
        if not self.directory.exists():
            result["complete"] = True
            return result
        with self._scan_lock:
            if self._entries is None:
                self._entries = os.scandir(self.directory)
            for _ in range(limit):
                entry = next(self._entries, None)
                if entry is None:
                    self._entries.close()
                    self._entries = None
                    result["complete"] = True
                    break
                result["scanned"] += 1
                path = Path(entry.path)
                if path.suffix not in {".json", ".part", ".reserve"} or not re.fullmatch(
                    "[0-9a-f]{64}", path.stem
                ):
                    continue
                try:
                    if path.suffix == ".reserve" and not any(
                        path.with_suffix(suffix).exists() for suffix in (".json", ".part")
                    ):
                        with self._lock(path.stem):
                            if apply:
                                self._remove_files(path.stem, (".reserve",))
                            result["released"] += 1
                        continue
                    receipt = read_receipt(path)
                    dataset = self.catalog.one(
                        "SELECT * FROM datasets WHERE dataset_id=%s", (path.stem,)
                    )
                    with self.catalog.transaction() as connection:
                        self.catalog.guard_retention(connection)
                        reason = self._authority(connection, receipt) if retention else None
                    if reason:
                        if not apply or self._retire(path):
                            result["retired"] += 1
                        continue
                    if path.suffix == ".json" and dataset and dataset["state"] == "committed":
                        remote = self.history.raw(dataset)
                        if receipt != remote:
                            raise ValueError("Committed receipt differs from local input")
                        if apply:
                            (remove_committed or self.remove_committed)(path.stem, remote)
                        result["committed"] += 1
                        continue
                    result["protected"] += 1
                except FileNotFoundError:
                    continue
                except BlockingIOError:
                    result["protected"] += 1
                except Exception:
                    # Unknown/corrupt/unreadable inputs remain; transient S3
                    # failures cannot block the independent acquisition process.
                    result["invalid"] += 1
        return result

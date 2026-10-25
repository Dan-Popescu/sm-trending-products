#!/usr/bin/env python3
"""Verified immutable object backups with bounded base/delta resolution."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
import time
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig

CHUNK_SIZE = 4 * 1024 * 1024
MAX_CHAIN_DEPTH = 32


def client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def file_hash(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_SIZE):
            result.update(chunk)
    return result.hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


class ResolvedSnapshot:
    """Validate a complete immutable chain into a temporary disk-backed key index.

    Only the latest full inventory is restored. Entries point at their verified
    owner, so unchanged objects are neither recopied nor loaded into Python RAM.
    """

    def __init__(self, snapshot: Path, *, scratch_dir: Path | None = None):
        self.snapshot = snapshot.resolve()
        self.root = self.snapshot.parent.parent
        self._temporary = tempfile.TemporaryDirectory(prefix="sm-backup-index-", dir=scratch_dir)
        self.db = sqlite3.connect(Path(self._temporary.name) / "inventory.sqlite")
        self.db.execute("CREATE TABLE objects(key TEXT PRIMARY KEY,record TEXT,owner TEXT)")
        self.metadata: dict[str, Any] = {}

    def __enter__(self):
        try:
            visited: set[Path] = set()
            self.metadata = self._load(self.snapshot, visited)
            self.depth = len(visited)
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, *_args):
        self.close()

    def close(self) -> None:
        self.db.close()
        self._temporary.cleanup()

    def get(self, key: str) -> tuple[dict[str, Any], Path] | None:
        row = self.db.execute("SELECT record,owner FROM objects WHERE key=?", (key,)).fetchone()
        return (json.loads(row[0]), Path(row[1])) if row else None

    def records(self):
        for row in self.db.execute("SELECT record,owner FROM objects ORDER BY key"):
            yield json.loads(row[0]), Path(row[1])

    def _load(self, snapshot: Path, visited: set[Path]) -> dict[str, Any]:
        if snapshot in visited or len(visited) >= MAX_CHAIN_DEPTH:
            raise ValueError("Cyclic or excessive backup chain")
        visited.add(snapshot)
        metadata = json.loads((snapshot / "snapshot.json").read_text())
        if metadata.get("schema_version") not in (1, 2) or metadata.get("complete") is not True:
            raise ValueError("Incomplete or unsupported snapshot")
        if file_hash(snapshot / "objects.jsonl") != metadata["inventory_sha256"]:
            raise ValueError("Snapshot inventory checksum mismatch")
        base = metadata.get("base")
        if base:
            if metadata["schema_version"] != 2 or Path(base["path"]).is_absolute():
                raise ValueError("Invalid backup base reference")
            base_path = (snapshot / base["path"]).resolve()
            if not base_path.is_relative_to(self.root):
                raise ValueError("Backup base escapes its backup family")
            if file_hash(base_path / "snapshot.json") != base["snapshot_sha256"]:
                raise ValueError("Backup base checksum mismatch")
            parent = self._load(base_path, visited)
            if parent.get("copied_objects") is not True or any(
                parent.get(field) != metadata.get(field)
                for field in ("source_bucket", "source_endpoint", "namespace")
            ):
                raise ValueError("Backup base scope mismatch")
        else:
            self.db.execute("DELETE FROM objects")
        self.db.execute("CREATE TABLE next_objects(key TEXT PRIMARY KEY,record TEXT,owner TEXT)")
        count = total = 0
        with (snapshot / "objects.jsonl").open() as inventory:
            for line in inventory:
                item = json.loads(line)
                name = hashlib.sha256(item["key"].encode()).hexdigest()
                owner = snapshot
                if metadata["copied_objects"]:
                    if item.get("file") != name:
                        raise ValueError("Unexpected snapshot filename")
                    if item.get("from_base"):
                        previous = self.get(item["key"]) if base else None
                        if not previous or any(
                            previous[0].get(field) != item.get(field)
                            for field in ("sha256", "size")
                        ):
                            raise ValueError("Missing or incompatible base object")
                        owner = previous[1]
                    else:
                        path = snapshot / "objects" / name
                        if path.stat().st_size != item["size"] or file_hash(path) != item["sha256"]:
                            raise ValueError("Snapshot object checksum mismatch")
                self.db.execute(
                    "INSERT INTO next_objects VALUES(?,?,?)",
                    (item["key"], json.dumps(item), str(owner)),
                )
                count += 1
                total += item["size"]
        if count != metadata["object_count"] or total != metadata["total_bytes"]:
            raise ValueError("Snapshot count or total size mismatch")
        self.db.execute("DROP TABLE objects")
        self.db.execute("ALTER TABLE next_objects RENAME TO objects")
        return metadata


@contextmanager
def _checkpoint_guard(enabled: bool):
    """A session lock avoids keeping an MVCC transaction open during the checkpoint."""
    if not enabled:
        yield None
        return
    import psycopg

    from sm_trending_products.catalog import RETENTION_GUARD_KEY

    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as connection:
        connection.execute("SET statement_timeout='30s'")
        connection.execute("SELECT pg_advisory_lock_shared(%s)", (RETENTION_GUARD_KEY,))
        yield connection
        connection.execute("SELECT pg_advisory_unlock_shared(%s)", (RETENTION_GUARD_KEY,))


@contextmanager
def _online_checkpoint(_enabled: bool):
    """A shared database snapshot allows writers to continue during a dump.

    Do not acquire the retention advisory lock: an exclusive waiter behind a
    long backup would otherwise queue subsequent acquisition publications.
    Concurrent physical deletion makes this recovery set fail incomplete.
    """
    import psycopg

    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as connection:
        connection.execute("SET statement_timeout='30s'")
        # Catalog.initialize takes this key exclusively before any schema work.
        # Acquire it before starting the snapshot so a preceding migration is
        # visible, and a waiting migration cannot take relation locks mid-dump.
        connection.execute("SELECT pg_advisory_lock_shared(492371204)")
        try:
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                yield connection
        finally:
            connection.execute("SELECT pg_advisory_unlock_shared(492371204)")


def _expected_objects(connection, db: sqlite3.Connection) -> None:
    """Read authoritative checksums, including committed derived dependencies."""
    from sm_trending_products.contracts import canonical

    db.execute(
        "CREATE TABLE expected(key TEXT PRIMARY KEY,sha TEXT,bytes INTEGER,"
        "required INTEGER,seen INTEGER)"
    )
    db.execute("CREATE TABLE retired(key TEXT PRIMARY KEY)")

    def expect(key, checksum, size, required):
        old = db.execute("SELECT sha,bytes FROM expected WHERE key=?", (key,)).fetchone()
        if old and any(
            a is not None and b is not None and a != b
            for a, b in zip(old, (checksum, size), strict=True)
        ):
            raise ValueError("Conflicting catalog object checksums or sizes")
        db.execute(
            "INSERT INTO expected VALUES(?,?,?,?,0) ON CONFLICT(key) DO UPDATE SET "
            "sha=coalesce(expected.sha,excluded.sha),bytes=coalesce(expected.bytes,excluded.bytes),"
            "required=max(required,excluded.required)",
            (key, checksum, size, required),
        )

    with connection.transaction():
        with connection.cursor(name="backup_expected") as cursor:
            cursor.itersize = 128
            cursor.execute(
                "SELECT manifest,state FROM datasets WHERE state IN ('committed','staged')"
            )
            for manifest, state in cursor:
                descriptors = [manifest.get("raw"), *manifest.get("normalized", [])]
                if manifest.get("manifest_key"):
                    body = canonical(manifest).encode()
                    descriptors.append(
                        {
                            "key": manifest["manifest_key"],
                            "sha256": hashlib.sha256(body).hexdigest(),
                            "bytes": len(body),
                        }
                    )
                for obj in descriptors:
                    if obj:
                        expect(obj["key"], obj["sha256"], obj.get("bytes"), state == "committed")
            cursor.execute("SELECT object_key,run_id FROM assessment_runs WHERE NOT invalidated")
            for key, checksum in cursor:
                # Legacy plain schema-2 bytes hash to their logical run id.
                # Compressed/shared archives use authoritative physical hashes
                # below; a run id must never be compared to gzip bytes.
                expect(key, checksum if key.endswith(".json") else None, None, True)
            # to_jsonb supports pre-019 recovery images whose old rows/schema do
            # not yet carry checksums. Existence remains mandatory for all
            # committed nodes, including transitive assessment provenance.
            cursor.execute(
                "SELECT object_key,to_jsonb(d)->>'object_sha256',"
                "to_jsonb(d)->>'object_bytes' FROM derived_publications d WHERE state='committed'"
            )
            for key, checksum, size in cursor:
                expect(key, checksum, int(size) if size is not None else None, True)
            # Pending deletion can continue outside retention's exclusive SQL transaction.
            cursor.execute("SELECT object_key FROM retention_objects")
            db.executemany("INSERT INTO retired VALUES(?)", cursor)


def _wait_signal(path: Path, deadline: float, connection=None) -> None:
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Backup checkpoint deadline exceeded")
        if connection is not None:
            connection.execute("SELECT 1")
        time.sleep(0.1)


def _checkpoint_record(item: dict[str, Any], expected: tuple | None) -> dict[str, Any]:
    """Freeze LIST identity without a per-object remote round trip."""
    size = item["Size"]
    if expected and expected[1] is not None and size != expected[1]:
        raise ValueError("Catalog checkpoint object size mismatch")
    return {
        "key": item["Key"],
        "size": size,
        "etag": item["ETag"],
        "last_modified": item["LastModified"].isoformat(),
        "expected_sha256": expected[0] if expected else None,
    }


def _reuse_catalog_record(record: dict[str, Any], old: dict[str, Any] | None) -> bool:
    """Only authoritative catalog hashes can replace fresh metadata inspection.

    A legacy or unmanaged record still needs HEAD after the checkpoint. ETags
    alone are not a content hash, and old snapshots without a LIST timestamp
    acquire that fingerprint through the normal inspection path once.
    """
    return bool(
        old is not None
        and record["expected_sha256"]
        and record["expected_sha256"] == old.get("sha256")
        and record["expected_sha256"] == old.get("metadata", {}).get("sha256")
        and all(record.get(field) == old.get(field) for field in ("size", "etag", "last_modified"))
    )


def _inspect_checkpoint_record(s3, bucket: str, record: dict[str, Any]) -> None:
    """Inspect after writers resume; a changed LIST identity fails closed."""
    head = s3.head_object(Bucket=bucket, Key=record["key"], IfMatch=record["etag"])
    # HEAD's HTTP date has whole-second precision; LIST can retain fractions.
    # Keep LIST's original precision in the persisted/reuse fingerprint.
    head_modified = head["LastModified"].astimezone(UTC).replace(microsecond=0)
    checkpoint_modified = (
        datetime.fromisoformat(record["last_modified"]).astimezone(UTC).replace(microsecond=0)
    )
    if any(
        left != right
        for left, right in (
            (head["ContentLength"], record["size"]),
            (head["ETag"], record["etag"]),
            (head_modified, checkpoint_modified),
        )
    ):
        raise ValueError("Checkpoint object changed before metadata inspection")
    if record["expected_sha256"] and (
        head.get("Metadata", {}).get("sha256") != record["expected_sha256"]
    ):
        raise ValueError("Catalog checkpoint object checksum metadata mismatch")
    record.update(
        version_id=head.get("VersionId"),
        metadata=head.get("Metadata", {}),
        content_type=head.get("ContentType", "application/octet-stream"),
        content_encoding=head.get("ContentEncoding"),
        expected_sha256=record["expected_sha256"] or head.get("Metadata", {}).get("sha256"),
    )


def capture(
    output: Path,
    *,
    copy_objects: bool,
    base: Path | None = None,
    checkpoint_signal: Path | None = None,
    continue_signal: Path | None = None,
    deadline_seconds: int = 3600,
    online: bool = False,
    catalog_state: Path | None = None,
) -> dict[str, Any]:
    if not 1 <= deadline_seconds <= 86400:
        raise ValueError("Backup deadline must be between one second and one day")
    if base is not None and not copy_objects:
        raise ValueError("Incremental snapshots require object copies")
    if (checkpoint_signal is None) != (continue_signal is None):
        raise ValueError("Checkpoint and continuation signals are required together")
    if online and (checkpoint_signal is None or catalog_state is None):
        raise ValueError("Online capture requires a dump handshake and catalog state destination")
    deadline = time.monotonic() + deadline_seconds
    checkpoint_started = time.monotonic()
    bucket = os.environ["S3_BUCKET"]
    s3 = client()
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    objects = output / "objects"
    if copy_objects:
        objects.mkdir()
    metadata: dict[str, Any] = {
        "schema_version": 2,
        "captured_at": datetime.now(UTC).isoformat(),
        "source_bucket": bucket,
        "source_endpoint": os.environ.get("S3_ENDPOINT_URL") or "aws",
        "namespace": os.environ.get("PIPELINE_NAMESPACE", "development"),
        "copied_objects": copy_objects,
        "complete": False,
        "inventory_scope": "catalog_snapshot" if online else "dedicated_bucket",
    }
    metadata_path = output / "snapshot.json"
    _write_json(metadata_path, metadata)
    with ExitStack() as stack:
        temporary = stack.enter_context(
            tempfile.TemporaryDirectory(prefix="sm-backup-checkpoint-", dir=output)
        )
        db = sqlite3.connect(Path(temporary) / "checkpoint.sqlite")
        stack.callback(db.close)
        count = total_size = list_requests = 0
        checkpoint = _online_checkpoint if online else _checkpoint_guard
        with checkpoint(checkpoint_signal is not None) as connection:
            snapshot_id = None
            if online:
                from sm_trending_products.catalog import Catalog

                snapshot_id = connection.execute("SELECT pg_export_snapshot()").fetchone()[0]
                catalog = Catalog(os.environ["DATABASE_URL"])
                try:
                    assert catalog_state is not None
                    _write_json(catalog_state, catalog.export_state(snapshot_id))
                finally:
                    catalog.close()
            if connection is not None:
                _expected_objects(connection, db)
            with (output / "checkpoint.jsonl").open("x") as inventory:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    list_requests += 1
                    for item in page.get("Contents", []):
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Backup inventory deadline exceeded")
                        key = item["Key"]
                        if (
                            connection is not None
                            and db.execute("SELECT 1 FROM retired WHERE key=?", (key,)).fetchone()
                        ):
                            continue
                        expected = (
                            db.execute(
                                "SELECT sha,bytes FROM expected WHERE key=?", (key,)
                            ).fetchone()
                            if connection is not None
                            else None
                        )
                        if online and expected is None:
                            # A page published after the SQL snapshot belongs to
                            # a later recovery set, not this checkpoint.
                            continue
                        record = _checkpoint_record(item, expected)
                        inventory.write(json.dumps(record, sort_keys=True) + "\n")
                        count += 1
                        total_size += record["size"]
                        if connection is not None:
                            db.execute("UPDATE expected SET seen=1 WHERE key=?", (key,))
                if (
                    connection is not None
                    and db.execute(
                        "SELECT key FROM expected WHERE required AND NOT seen LIMIT 1"
                    ).fetchone()
                ):
                    raise ValueError("Committed catalog object is absent from checkpoint")
            if checkpoint_signal is not None:
                _write_json(
                    checkpoint_signal,
                    {"ready": True, "object_count": count, "snapshot_id": snapshot_id},
                )
                assert continue_signal is not None
                _wait_signal(continue_signal, deadline, connection)
        checkpoint_seconds = time.monotonic() - checkpoint_started
        previous = (
            stack.enter_context(ResolvedSnapshot(base, scratch_dir=output))
            if base is not None
            else None
        )
        if previous:
            if previous.depth >= MAX_CHAIN_DEPTH:
                raise ValueError("Backup chain limit reached; create a new full base")
            if (
                any(
                    previous.metadata.get(field) != metadata.get(field)
                    for field in ("source_bucket", "source_endpoint", "namespace")
                )
                or previous.metadata.get("copied_objects") is not True
            ):
                raise ValueError("Backup base scope mismatch")
            if not previous.snapshot.is_relative_to(output.parent.parent):
                raise ValueError("Backup base must remain in the same backup family")
            metadata["base"] = {
                "path": os.path.relpath(previous.snapshot, output),
                "snapshot_sha256": file_hash(previous.snapshot / "snapshot.json"),
            }
        # The catalog/inventory checkpoint is fixed. Writers can now resume.
        # Missing/deleted/changed source versions fail closed instead of completing
        # a recovery set containing bytes from a different checkpoint.
        downloaded = copied = reused = head_requests = catalog_reused = 0
        with (
            (output / "checkpoint.jsonl").open() as source_inventory,
            (output / "objects.jsonl").open("x") as inventory,
        ):
            for line in source_inventory:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Backup copy deadline exceeded")
                record = json.loads(line)
                key = record["key"]
                old = previous.get(key) if previous else None
                if copy_objects and _reuse_catalog_record(record, old[0] if old else None):
                    assert old is not None
                    record.update(
                        version_id=old[0].get("version_id"),
                        metadata=old[0].get("metadata", {}),
                        content_type=old[0].get("content_type", "application/octet-stream"),
                        content_encoding=old[0].get("content_encoding"),
                    )
                    catalog_reused += 1
                else:
                    head_requests += 1
                    _inspect_checkpoint_record(s3, bucket, record)
                if copy_objects:
                    name = hashlib.sha256(key.encode()).hexdigest()
                    same = (
                        old is not None
                        and all(
                            old[0].get(field) == record.get(field)
                            for field in ("size", "etag", "version_id")
                        )
                        and (
                            not record["expected_sha256"]
                            or record["expected_sha256"] == old[0]["sha256"]
                        )
                    )
                    if same:
                        assert old is not None
                        record.update(file=name, sha256=old[0]["sha256"], from_base=True)
                        reused += 1
                    else:
                        parameters: dict[str, Any] = {
                            "Bucket": bucket,
                            "Key": key,
                            "IfMatch": record["etag"],
                        }
                        if record["version_id"]:
                            parameters["VersionId"] = record["version_id"]
                        response = s3.get_object(**parameters)
                        content_hash = hashlib.sha256()
                        written = 0
                        with (objects / name).open("xb") as destination:
                            try:
                                while chunk := response["Body"].read(CHUNK_SIZE):
                                    if time.monotonic() >= deadline:
                                        raise TimeoutError("Backup copy deadline exceeded")
                                    destination.write(chunk)
                                    content_hash.update(chunk)
                                    written += len(chunk)
                            finally:
                                response["Body"].close()
                        checksum = content_hash.hexdigest()
                        if written != record["size"] or (
                            record["expected_sha256"] and checksum != record["expected_sha256"]
                        ):
                            raise ValueError("Checkpoint object content mismatch")
                        record.update(file=name, sha256=checksum)
                        downloaded += written
                        copied += 1
                inventory.write(json.dumps(record, sort_keys=True) + "\n")
        if time.monotonic() >= deadline:
            raise TimeoutError("Backup copy deadline exceeded")
        metadata.update(
            complete=True,
            object_count=count,
            total_bytes=total_size,
            inventory_sha256=file_hash(output / "objects.jsonl"),
            copied_object_count=copied,
            reused_object_count=reused,
            downloaded_bytes=downloaded,
            list_requests=list_requests,
            head_requests=head_requests,
            catalog_reused_object_count=catalog_reused,
            checkpoint_seconds=checkpoint_seconds,
        )
        _write_json(metadata_path, metadata)
    return metadata


def verify(snapshot: Path) -> dict[str, Any]:
    with ResolvedSnapshot(snapshot) as resolved:
        return resolved.metadata


def restore(snapshot: Path, *, bucket: str, create_bucket: bool) -> dict[str, Any]:
    # Verify the complete chain and every owned file before creating or mutating
    # the destination. No partially verified delta is uploaded.
    with ResolvedSnapshot(snapshot) as resolved:
        metadata = resolved.metadata
        if metadata["copied_objects"] is not True:
            raise ValueError("Object copies are required for restore")
        # Old snapshots can contain verified bytes alongside incompatible source
        # checksum headers. Reject those before creating a destination: restoring
        # the header would break the application's later immutable-write checks.
        for item, _owner in resolved.records():
            declared = item.get("metadata", {}).get("sha256")
            expected = item.get("expected_sha256")
            if (declared is not None and declared != item["sha256"]) or (
                expected is not None and (expected != item["sha256"] or declared != expected)
            ):
                raise ValueError("Snapshot checksum metadata mismatch")
        endpoint = os.environ.get("S3_ENDPOINT_URL") or "aws"
        if bucket == metadata["source_bucket"] and endpoint == metadata["source_endpoint"]:
            raise ValueError("Refusing restore over source bucket")
        s3 = client()
        if create_bucket:
            region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            parameters: dict[str, Any] = {"Bucket": bucket}
            if region != "us-east-1":
                parameters["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3.create_bucket(**parameters)
        if s3.list_objects_v2(Bucket=bucket, MaxKeys=1).get("KeyCount", 0):
            raise ValueError("Restore destination must be empty")
        for item, owner in resolved.records():
            extra = {"Metadata": item.get("metadata", {}), "ContentType": item["content_type"]}
            if item.get("content_encoding"):
                extra["ContentEncoding"] = item["content_encoding"]
            s3.upload_file(
                str(owner / "objects" / item["file"]),
                bucket,
                item["key"],
                ExtraArgs=extra,
                Config=TransferConfig(max_concurrency=2),
            )
    return {"restored_objects": metadata["object_count"], "target_bucket": bucket}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture_parser = commands.add_parser("capture")
    capture_parser.add_argument("--output", type=Path, required=True)
    capture_parser.add_argument("--copy-objects", action="store_true")
    capture_parser.add_argument("--base", type=Path)
    capture_parser.add_argument("--checkpoint-signal", type=Path)
    capture_parser.add_argument("--continue-signal", type=Path)
    capture_parser.add_argument("--deadline-seconds", type=int, default=3600)
    capture_parser.add_argument("--online", action="store_true")
    capture_parser.add_argument("--catalog-state", type=Path)
    commands.add_parser("verify").add_argument("snapshot", type=Path)
    restore_parser = commands.add_parser("restore")
    restore_parser.add_argument("snapshot", type=Path)
    restore_parser.add_argument("--bucket", required=True)
    restore_parser.add_argument("--create-bucket", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "capture":
            result = capture(
                args.output,
                copy_objects=args.copy_objects,
                base=args.base,
                checkpoint_signal=args.checkpoint_signal,
                continue_signal=args.continue_signal,
                deadline_seconds=args.deadline_seconds,
                online=args.online,
                catalog_state=args.catalog_state,
            )
        elif args.command == "verify":
            result = verify(args.snapshot)
        else:
            result = restore(args.snapshot, bucket=args.bucket, create_bucket=args.create_bucket)
    except Exception as error:
        parser.exit(
            1, f"Object snapshot failed ({type(error).__name__}); check access and inputs.\n"
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

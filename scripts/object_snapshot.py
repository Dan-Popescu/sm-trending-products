#!/usr/bin/env python3
"""Stream a dedicated S3 bucket into a verifiable, restorable local snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig

CHUNK_SIZE = 4 * 1024 * 1024


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


def capture(output: Path, *, copy_objects: bool) -> dict[str, Any]:
    bucket = os.environ["S3_BUCKET"]
    s3 = client()
    output.mkdir(parents=True, exist_ok=False)
    objects = output / "objects"
    if copy_objects:
        objects.mkdir()
    metadata: dict[str, Any] = {
        "schema_version": 1,
        "captured_at": datetime.now(UTC).isoformat(),
        "source_bucket": bucket,
        "source_endpoint": os.environ.get("S3_ENDPOINT_URL") or "aws",
        "namespace": os.environ.get("PIPELINE_NAMESPACE", "development"),
        "copied_objects": copy_objects,
        "complete": False,
    }
    metadata_path = output / "snapshot.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    count = total_size = 0
    with (output / "objects.jsonl").open("x") as inventory:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for item in page.get("Contents", []):
                key = item["Key"]
                head = s3.head_object(Bucket=bucket, Key=key)
                record: dict[str, Any] = {
                    "key": key,
                    "size": head["ContentLength"],
                    "etag": head["ETag"],
                    "version_id": head.get("VersionId"),
                    "metadata": head.get("Metadata", {}),
                    "content_type": head.get("ContentType", "application/octet-stream"),
                    "content_encoding": head.get("ContentEncoding"),
                }
                if copy_objects:
                    name = hashlib.sha256(key.encode()).hexdigest()
                    parameters = {"Bucket": bucket, "Key": key}
                    if record["version_id"]:
                        parameters["VersionId"] = record["version_id"]
                    response = s3.get_object(**parameters)
                    content_hash = hashlib.sha256()
                    written = 0
                    with (objects / name).open("xb") as destination:
                        try:
                            while chunk := response["Body"].read(CHUNK_SIZE):
                                destination.write(chunk)
                                content_hash.update(chunk)
                                written += len(chunk)
                        finally:
                            response["Body"].close()
                    if written != record["size"]:
                        raise ValueError("Object size changed during capture")
                    record.update(file=name, sha256=content_hash.hexdigest())
                inventory.write(json.dumps(record, sort_keys=True) + "\n")
                count += 1
                total_size += record["size"]
    metadata.update(
        complete=True,
        object_count=count,
        total_bytes=total_size,
        inventory_sha256=file_hash(output / "objects.jsonl"),
    )
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def verify(snapshot: Path) -> dict[str, Any]:
    metadata = json.loads((snapshot / "snapshot.json").read_text())
    if metadata.get("schema_version") != 1 or metadata.get("complete") is not True:
        raise ValueError("Incomplete or unsupported snapshot")
    if file_hash(snapshot / "objects.jsonl") != metadata["inventory_sha256"]:
        raise ValueError("Snapshot inventory checksum mismatch")
    count = total = 0
    with (snapshot / "objects.jsonl").open() as inventory:
        for line in inventory:
            item = json.loads(line)
            name = hashlib.sha256(item["key"].encode()).hexdigest()
            if metadata["copied_objects"]:
                if item.get("file") != name:
                    raise ValueError("Unexpected snapshot filename")
                path = snapshot / "objects" / name
                if path.stat().st_size != item["size"] or file_hash(path) != item["sha256"]:
                    raise ValueError("Snapshot object checksum mismatch")
            count += 1
            total += item["size"]
    if count != metadata["object_count"] or total != metadata["total_bytes"]:
        raise ValueError("Snapshot count or total size mismatch")
    return metadata


def restore(snapshot: Path, *, bucket: str, create_bucket: bool) -> dict[str, Any]:
    metadata = verify(snapshot)
    if metadata["copied_objects"] is not True:
        raise ValueError("Object copies are required for restore")
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
    with (snapshot / "objects.jsonl").open() as inventory:
        for line in inventory:
            item = json.loads(line)
            extra = {"Metadata": item.get("metadata", {}), "ContentType": item["content_type"]}
            if item.get("content_encoding"):
                extra["ContentEncoding"] = item["content_encoding"]
            s3.upload_file(
                str(snapshot / "objects" / item["file"]),
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
    commands.add_parser("verify").add_argument("snapshot", type=Path)
    restore_parser = commands.add_parser("restore")
    restore_parser.add_argument("snapshot", type=Path)
    restore_parser.add_argument("--bucket", required=True)
    restore_parser.add_argument("--create-bucket", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "capture":
            result = capture(args.output, copy_objects=args.copy_objects)
        elif args.command == "verify":
            result = verify(args.snapshot)
        else:
            result = restore(args.snapshot, bucket=args.bucket, create_bucket=args.create_bucket)
    except Exception as error:
        # SDK error strings can contain signed URLs or credential identifiers.
        parser.exit(
            1, f"Object snapshot failed ({type(error).__name__}); check access and inputs.\n"
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

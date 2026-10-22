"""Immutable S3 datasets, completed manifests, and a bounded verified local cache."""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import io
import json
import os
import tempfile
import uuid
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
import polars as pl
from botocore.config import Config
from botocore.exceptions import ClientError
from psycopg.types.json import Jsonb

from .cache import pin_cached
from .catalog import Catalog
from .contracts import Observation, canonical, digest, utc
from .settings import Settings


class History:
    def __init__(self, settings: Settings, catalog: Catalog):
        self.settings = settings
        self.catalog = catalog
        self._client = None
        self.usage_id = uuid.uuid4().hex
        self._flushed_metrics: dict[str, int] | None = None
        self.bucket = settings.s3_bucket
        settings.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metrics = {
            "get_requests": 0,
            "put_requests": 0,
            "head_requests": 0,
            "download_bytes": 0,
            "upload_bytes": 0,
            "cache_hits": 0,
            "http_attempts": 0,
        }

    @property
    def client(self):
        if self._client is None:
            self._client = self._make_client()
        return self._client

    def _make_client(self):
        settings = self.settings
        client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.aws_default_region,
            config=Config(
                signature_version="s3v4",
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

        def sent(**_kwargs):
            self.metrics["http_attempts"] += 1

        client.meta.events.register("before-send.s3", sent)
        return client

    def flush_usage(self) -> None:
        if not any(self.metrics.values()) or self._flushed_metrics == self.metrics:
            return
        self.catalog.execute(
            "INSERT INTO operational_usage(usage_id,counters) VALUES(%s,%s) "
            "ON CONFLICT(usage_id) DO UPDATE SET counters=EXCLUDED.counters,"
            "updated_at=clock_timestamp()",
            (self.usage_id, Jsonb(self.metrics)),
        )
        self._flushed_metrics = dict(self.metrics)

    def initialize(self, create_bucket: bool = False) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if not create_bucket or error.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                raise
            kwargs: dict[str, Any] = {"Bucket": self.bucket}
            if self.settings.aws_default_region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.settings.aws_default_region
                }
            self.client.create_bucket(**kwargs)

    def put(
        self,
        key: str,
        body: bytes,
        *,
        content_type: str = "application/octet-stream",
        connection=None,
    ) -> dict:
        if connection is not None:
            return self._guarded_put(connection, key, body, content_type=content_type)
        with self.catalog.transaction() as connection:
            return self._guarded_put(connection, key, body, content_type=content_type)

    def _guarded_put(self, connection, key: str, body: bytes, *, content_type: str) -> dict:
        self.catalog.guard_retention(connection)
        tombstone = connection.execute(
            "SELECT 1 FROM retention_objects WHERE object_key=%s", (key,)
        ).fetchone()
        if tombstone:
            raise ValueError("Object was retired and cannot be republished")
        return self._put(key, body, content_type=content_type)

    def _put(self, key: str, body: bytes, *, content_type: str) -> dict:
        checksum = hashlib.sha256(body).hexdigest()
        self.metrics["put_requests"] += 1
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
                Metadata={"sha256": checksum},
                IfNoneMatch="*",
            )
            self.metrics["upload_bytes"] += len(body)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise
            self.metrics["head_requests"] += 1
            existing = self.client.head_object(Bucket=self.bucket, Key=key)
            if existing.get("Metadata", {}).get("sha256") != checksum:
                raise ValueError("Immutable object collision") from None
        return {"key": key, "sha256": checksum, "bytes": len(body)}

    def cached(self, obj: dict) -> Path:
        key, expected = obj["key"], obj["sha256"]
        local = self.settings.cache_dir / hashlib.sha256(key.encode()).hexdigest()
        # Locking only coordinates cache writers, not authoritative publication.
        with (self.settings.cache_dir / ".cache.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if local.exists() and self._file_digest(local) == expected:
                local.touch()
                self.metrics["cache_hits"] += 1
                return local
            if obj.get("bytes", 0) > self.settings.cache_max_bytes:
                raise ValueError("Object exceeds cache budget; compact into smaller partitions")
            temporary: str | None = None
            try:
                self.metrics["get_requests"] += 1
                response = self.client.get_object(Bucket=self.bucket, Key=key)
                hasher = hashlib.sha256()
                size = 0
                with tempfile.NamedTemporaryFile(
                    dir=self.settings.cache_dir, delete=False
                ) as target:
                    temporary = target.name
                    stream = response["Body"]
                    try:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                            size += len(chunk)
                            if size > self.settings.cache_max_bytes:
                                raise ValueError("Download exceeds cache budget")
                            target.write(chunk)
                            hasher.update(chunk)
                    finally:
                        stream.close()
                    target.flush()
                    os.fsync(target.fileno())
                self.metrics["download_bytes"] += size
                if hasher.hexdigest() != expected:
                    raise ValueError("Object checksum mismatch")
                os.replace(temporary, local)
                temporary = None
                self._evict(keep=local)
            finally:
                if temporary:
                    Path(temporary).unlink(missing_ok=True)
        return local

    @staticmethod
    def _file_digest(path: Path) -> str:
        with path.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()

    def _evict(self, keep: Path) -> None:
        candidates = [
            path
            for path in self.settings.cache_dir.iterdir()
            if path.is_file() and not path.name.startswith(".")
        ]
        total = sum(path.stat().st_size for path in candidates)
        for path in sorted(candidates, key=lambda entry: entry.stat().st_mtime):
            if total <= self.settings.cache_max_bytes:
                break
            if path != keep:
                total -= path.stat().st_size
                path.unlink(missing_ok=True)

    def encode(self, observations: Sequence[Observation]) -> bytes:
        rows = []
        for obs in observations:
            row = obs.model_dump(mode="json")
            row["entity_key"] = obs.entity_key
            for key in ("metrics", "attributes", "missing_fields"):
                row[f"{key}_json"] = canonical(row.pop(key))
            rows.append(row)
        stream = io.BytesIO()
        pl.DataFrame(rows, infer_schema_length=None).write_parquet(stream, compression="zstd")
        return stream.getvalue()

    def publish(
        self,
        observations: Sequence[Observation],
        raw: dict,
        *,
        source: str,
        synthetic: bool,
        dataset_id: str | None = None,
        task: dict | None = None,
        next_payload: dict | None = None,
        transform_version: str = "1",
        supersedes: str | None = None,
        quarantine: Sequence[dict] = (),
        advance_task: bool = True,
    ) -> str:
        if any(obs.source != source or obs.synthetic != synthetic for obs in observations):
            raise ValueError("Datasets require one source and one real/synthetic class")
        dataset_id = dataset_id or digest(
            {"raw": raw, "version": transform_version, "synthetic": synthetic}
        )
        existing = self.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (dataset_id,))
        if existing and existing["state"] == "committed":
            if task:
                self.catalog.commit_dataset(
                    dataset_id,
                    existing["manifest"],
                    (),
                    task=task,
                    next_payload=next_payload,
                    advance_task=advance_task,
                )
            return dataset_id
        if existing and existing["state"] in ("invalidated", "deleted"):
            raise ValueError("Cannot republish invalidated evidence")
        rows = [obs.model_copy(update={"raw_dataset_id": dataset_id}) for obs in observations]
        date = (
            min(obs.observed_at for obs in rows).date().isoformat()
            if rows
            else utc(raw.get("observed_at", datetime.now().astimezone())).date().isoformat()
        )
        namespace = "synthetic" if synthetic else "live"
        prefix = f"{namespace}/source={source}/date={date}"
        raw_body = gzip.compress((canonical(raw) + "\n").encode(), mtime=0)
        raw_hash = hashlib.sha256(raw_body).hexdigest()
        raw_key = f"raw/{prefix}/{raw_hash}.jsonl.gz"
        objects: list[tuple[dict, bytes]] = []
        buckets: dict[int, list[Observation]] = defaultdict(list)
        for obs in rows:
            bucket = int(hashlib.sha256(obs.entity_key.encode()).hexdigest()[:8], 16)
            buckets[bucket % self.settings.history_partition_buckets].append(obs)
        for bucket, part in sorted(buckets.items()):
            body = self.encode(part)
            checksum = hashlib.sha256(body).hexdigest()
            key = f"normalized/{prefix}/bucket={bucket:04d}/{checksum}.parquet"
            objects.append(
                (
                    {
                        "key": key,
                        "sha256": checksum,
                        "bytes": len(body),
                        "bucket": bucket,
                        "entity_keys": sorted({r.entity_key for r in part}),
                    },
                    body,
                )
            )
        manifest = {
            "schema_version": "1",
            "dataset_id": dataset_id,
            "transform_version": transform_version,
            "source": source,
            "synthetic": synthetic,
            "partition_buckets": self.settings.history_partition_buckets,
            "raw": {"key": raw_key, "sha256": raw_hash, "bytes": len(raw_body)},
            "normalized": [obj for obj, _ in objects],
            "row_count": len(rows),
            "quarantine_count": len(quarantine),
            "quarantine": list(quarantine),
            "task_request": raw.get("request"),
            "supersedes": supersedes,
        }
        manifest_key = f"manifests/{namespace}/{dataset_id}.json"
        manifest["manifest_key"] = manifest_key
        self.catalog.stage_dataset(
            dataset_id, source, synthetic, manifest, task=task, supersedes=supersedes
        )
        self.put(raw_key, raw_body, content_type="application/gzip")
        for obj, body in objects:
            self.put(obj["key"], body)
        # Manifest publication is last; the catalog commit makes it discoverable.
        self.put(manifest_key, canonical(manifest).encode(), content_type="application/json")
        self.catalog.commit_dataset(
            dataset_id,
            manifest,
            rows,
            task=task,
            next_payload=next_payload,
            advance_task=advance_task,
        )
        return dataset_id

    def read(
        self,
        manifests: Sequence[dict],
        *,
        as_of: datetime,
        since: datetime | None = None,
        until: datetime | None = None,
        period_window: tuple[datetime, datetime] | None = None,
        entity_keys: set[str] | None = None,
        max_rows: int | None = None,
    ) -> list[Observation]:
        row_budget = max_rows if max_rows is not None else self.settings.analysis_max_rows_per_batch
        if isinstance(row_budget, bool) or row_budget < 1:
            raise ValueError("Analysis row budget must be positive")
        result: dict[str, Observation] = {}
        for dataset in manifests:
            if dataset["state"] != "committed":
                continue
            for obj in dataset["manifest"]["normalized"]:
                if entity_keys is not None and not entity_keys.intersection(obj["entity_keys"]):
                    continue
                with pin_cached(self, obj) as path:
                    query = pl.scan_parquet(path).filter(
                        pl.col("available_at").str.to_datetime(time_zone="UTC") <= utc(as_of)
                    )
                    observed = pl.col("observed_at").str.to_datetime(time_zone="UTC")
                    in_window = pl.lit(True)
                    if since:
                        in_window &= observed >= utc(since)
                    if until:
                        in_window &= observed <= utc(until)
                    if period_window:
                        # Late interval reports may describe an earlier outcome window.
                        # Keep these rows without relaxing the availability cutoff.
                        attributes = pl.col("attributes_json")
                        start = pl.coalesce(
                            attributes.str.json_path_match("$.period_start"),
                            attributes.str.json_path_match("$.source_period_start"),
                        ).str.to_datetime(time_zone="UTC", strict=False)
                        end = pl.coalesce(
                            attributes.str.json_path_match("$.period_end"),
                            attributes.str.json_path_match("$.source_period_end"),
                        ).str.to_datetime(time_zone="UTC", strict=False)
                        in_window |= (
                            (start >= utc(period_window[0]))
                            & (end <= utc(period_window[1]))
                            & (start < end)
                        ).fill_null(False)
                    query = query.filter(in_window)
                    if entity_keys is not None:
                        query = query.filter(pl.col("entity_key").is_in(sorted(entity_keys)))
                    # Push the hard materialization bound into the native query.
                    # Pins retain the verified inode during concurrent cache eviction.
                    frame = query.head(row_budget + 1).collect(engine="streaming")
                    if frame.height > row_budget:
                        raise ValueError(
                            "Analysis partition exceeds row budget; narrow the input range"
                        )
                    for row in frame.iter_rows(named=True):
                        row.pop("entity_key")
                        for name in ("metrics", "attributes", "missing_fields"):
                            row[name] = json.loads(row.pop(f"{name}_json"))
                        obs = Observation.model_validate(row)
                        prior = result.get(obs.observation_id)
                        if prior and prior.model_dump(exclude={"raw_dataset_id"}) != obs.model_dump(
                            exclude={"raw_dataset_id"}
                        ):
                            raise ValueError(
                                "Conflicting payloads under one observation identifier"
                            )
                        result.setdefault(obs.observation_id, obs)
                        if len(result) > row_budget:
                            raise ValueError(
                                "Analysis batch exceeds row budget; reduce product batch size"
                            )
        return sorted(result.values(), key=lambda obs: (obs.observed_at, obs.observation_id))

    def raw(self, dataset: dict) -> dict:
        with pin_cached(self, dataset["manifest"]["raw"]) as path:
            with gzip.open(path, "rt") as handle:
                return json.load(handle)

    def observations(
        self,
        as_of: datetime,
        synthetic: bool,
        *,
        entity_keys: set[str] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        period_window: tuple[datetime, datetime] | None = None,
    ) -> tuple[list[Observation], list[dict]]:
        manifests = self.catalog.manifests(synthetic=synthetic, as_of=as_of, since=since)
        return self.read(
            manifests,
            as_of=as_of,
            since=since,
            until=until,
            period_window=period_window,
            entity_keys=entity_keys,
            max_rows=self.settings.analysis_max_rows_per_batch,
        ), manifests

    def _verify_remote_object(self, obj: dict) -> None:
        # Bypass cache: verify the backup/remote object itself with bounded memory.
        self.metrics["get_requests"] += 1
        response = self.client.get_object(Bucket=self.bucket, Key=obj["key"])
        hasher = hashlib.sha256()
        with response["Body"] as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                hasher.update(chunk)
                self.metrics["download_bytes"] += len(chunk)
        if hasher.hexdigest() != obj["sha256"]:
            raise ValueError("checksum_mismatch")

    def verify(self, dataset_id: str | None = None) -> dict:
        rows = self.catalog.all(
            "SELECT * FROM datasets WHERE state='committed'"
            + (" AND dataset_id=%s" if dataset_id else ""),
            (dataset_id,) if dataset_id else (),
        )
        failures = []
        checked = 0
        for row in rows:
            manifest = row["manifest"]
            objects = [
                manifest["raw"],
                *manifest["normalized"],
                {
                    "key": manifest["manifest_key"],
                    "sha256": hashlib.sha256(canonical(manifest).encode()).hexdigest(),
                    "bytes": len(canonical(manifest).encode()),
                },
            ]
            for obj in objects:
                try:
                    self._verify_remote_object(obj)
                    checked += 1
                except (ClientError, ValueError, OSError):
                    failures.append(
                        {
                            "dataset_id": row["dataset_id"],
                            "object_key": obj["key"],
                            "category": "missing_or_corrupt_object",
                        }
                    )
        if dataset_id and not rows:
            failures.append({"dataset_id": dataset_id, "category": "unknown_committed_dataset"})
        assessments_checked = 0
        if dataset_id is None:
            for row in self.catalog.all(
                "SELECT run_id,object_key FROM assessment_runs WHERE NOT invalidated"
            ):
                try:
                    self._verify_remote_object({"key": row["object_key"], "sha256": row["run_id"]})
                    checked += 1
                    assessments_checked += 1
                except (ClientError, ValueError, OSError):
                    failures.append(
                        {
                            "run_id": row["run_id"],
                            "object_key": row["object_key"],
                            "category": "missing_or_corrupt_assessment",
                        }
                    )
        return {
            "ok": not failures,
            "datasets": len(rows),
            "objects_checked": checked,
            "assessments_checked": assessments_checked,
            "failures": failures,
        }

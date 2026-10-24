"""Immutable S3 datasets, completed manifests, and a bounded verified local cache."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any

import boto3
import polars as pl
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from psycopg.types.json import Jsonb

from .analysis_batches import RowBudgetExceeded
from .cache import cached_path, evict_cache, file_digest, pin_cached, read_session
from .catalog import Catalog
from .contracts import Observation, canonical, digest, observation_period, utc
from .settings import Settings


class HistoryIndexPending(RuntimeError):
    """A bounded legacy indexing slice completed; retry to continue migration."""


class History:
    def __init__(self, settings: Settings, catalog: Catalog):
        self.settings = settings
        self.catalog = catalog
        self._client = None
        self.usage_id = uuid.uuid4().hex
        self._flushed_metrics: dict[str, int] | None = None
        self._cache_budget_configuration: tuple[Path, int] | None = None
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
        # Computation reuse is distinct from filesystem/S3 traffic. A feature
        # hit can correctly perform zero file-cache reads and zero source GETs.
        self.analysis_metrics = {
            "projection_hits": 0,
            "projection_misses": 0,
            "delta_windows": 0,
            "full_windows": 0,
            "duplicate_rebuild_windows": 0,
        }

    def read_session(self) -> AbstractContextManager[None]:
        """Bound verification reuse to this operation without changing retention guards."""
        return read_session(self)

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
        namespace = key.split("/")[1] if "/" in key else ""
        self.catalog.guard_namespace(
            connection, True if namespace == "synthetic" else False if namespace == "live" else None
        )
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
        return cached_path(self, obj)

    @staticmethod
    def _file_digest(path: Path) -> str:
        return file_digest(path)

    def _evict(self, keep: Path) -> None:
        evict_cache(self, keep)

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

    def normalized_objects(
        self,
        rows: Sequence[Observation],
        prefix: str,
        partition_buckets: int | None = None,
        *,
        physical_buckets: int | None = None,
    ) -> tuple[list[tuple[dict, bytes]], int]:
        """Size physical files independently from the configured maximum hash fanout."""
        maximum = partition_buckets or self.settings.history_partition_buckets
        target = self.settings.history_target_rows_per_file
        physical_buckets = physical_buckets or min(
            maximum, max(1, (len(rows) + target - 1) // target)
        )
        buckets: dict[int, list[Observation]] = defaultdict(list)
        for obs in rows:
            bucket = int(hashlib.sha256(obs.entity_key.encode()).hexdigest()[:8], 16)
            buckets[bucket % physical_buckets].append(obs)
        objects = []
        for bucket, part in sorted(buckets.items()):
            periods = [observation_period(row) for row in part]
            valid_periods = [period for period in periods if period is not None]
            period_fields = {
                "period_start",
                "period_end",
                "source_period_start",
                "source_period_end",
            }
            # Malformed/ambiguous legacy period aliases remain conservative. The
            # Parquet row predicate retains its established permissive decoding.
            periods_indexed = all(
                period is not None or not period_fields.intersection(row.attributes)
                for row, period in zip(part, periods, strict=True)
            )
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
                        "entity_keys": sorted({row.entity_key for row in part}),
                        "min_observed_at": min(row.observed_at for row in part).isoformat(),
                        "max_observed_at": max(row.observed_at for row in part).isoformat(),
                        "min_available_at": min(row.available_at for row in part).isoformat(),
                        "max_available_at": max(row.available_at for row in part).isoformat(),
                        "periods_indexed": periods_indexed,
                        "max_period_start": max(period[0] for period in valid_periods).isoformat()
                        if valid_periods
                        else None,
                        "min_period_end": min(period[1] for period in valid_periods).isoformat()
                        if valid_periods
                        else None,
                    },
                    body,
                )
            )
        return objects, physical_buckets

    @staticmethod
    def recover_staged_objects(
        recorded: dict, raw_hash: str, objects: Sequence[tuple[dict, bytes]]
    ) -> list[tuple[dict, bytes]]:
        """Honor persisted recovery layout across deployments and sizing changes."""
        bodies = {obj["key"]: body for obj, body in objects}
        if recorded["raw"]["sha256"] != raw_hash or set(bodies) != {
            obj["key"] for obj in recorded["normalized"]
        }:
            raise ValueError("Staged dataset input or layout changed")
        recovered = [(obj, bodies[obj["key"]]) for obj in recorded["normalized"]]
        if any(hashlib.sha256(body).hexdigest() != obj["sha256"] for obj, body in recovered):
            raise ValueError("Staged dataset normalized payload changed")
        return recovered

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
        kind: str | None = None,
        capture: dict | None = None,
        publication_guard=None,
    ) -> str:
        if any(obs.source != source or obs.synthetic != synthetic for obs in observations):
            raise ValueError("Datasets require one source and one real/synthetic class")
        dataset_id = dataset_id or digest(
            {"raw": raw, "version": transform_version, "synthetic": synthetic}
        )
        if kind in {"raw_capture", "raw_capture_error"} or capture is not None:
            if (
                kind not in {"raw_capture", "raw_capture_error"}
                or observations
                or capture is None
                or raw.get("capture_version") != "1"
                or raw.get("capture") != capture
                or capture.get("capture_id") != dataset_id
            ):
                raise ValueError("Raw captures require one typed receipt and zero observations")
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
                    **({"publication_guard": publication_guard} if publication_guard else {}),
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
        recorded = existing["manifest"] if existing else None
        recorded_buckets = recorded.get("partition_buckets") if recorded else None
        physical = (
            recorded.get("physical_partition_buckets", recorded_buckets) if recorded else None
        )
        objects, physical_buckets = self.normalized_objects(
            rows, prefix, recorded_buckets, physical_buckets=physical
        )
        manifest = {
            "schema_version": "1",
            "dataset_id": dataset_id,
            "transform_version": transform_version,
            "source": source,
            "synthetic": synthetic,
            "partition_buckets": self.settings.history_partition_buckets,
            "physical_partition_buckets": physical_buckets,
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
        if kind is not None:
            manifest["kind"] = kind
        if capture is not None:
            manifest["acquisition_protocol"] = "1"
            manifest["capture"] = capture
        if recorded:
            # Staged evidence is a durable recovery contract. A deployment or
            # settings change cannot rewrite its immutable manifest/layout.
            if recorded["transform_version"] != transform_version:
                raise ValueError("Staged dataset transformation changed")
            objects = self.recover_staged_objects(recorded, raw_hash, objects)
            manifest = recorded
            raw_key = manifest["raw"]["key"]
            manifest_key = manifest["manifest_key"]
        self.catalog.stage_dataset(
            dataset_id,
            source,
            synthetic,
            manifest,
            task=task,
            supersedes=supersedes,
            **({"publication_guard": publication_guard} if publication_guard else {}),
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
            **({"publication_guard": publication_guard} if publication_guard else {}),
        )
        return dataset_id

    def read(
        self,
        manifests: Iterable[dict],
        *,
        as_of: datetime,
        since: datetime | None = None,
        until: datetime | None = None,
        period_window: tuple[datetime, datetime] | None = None,
        entity_keys: set[str] | None = None,
        max_rows: int | None = None,
    ) -> list[Observation]:
        iterator = iter(manifests)
        first = next(iterator, None)
        if first is None:
            return []
        with self.catalog.evidence_transaction(first.get("synthetic")):
            return self._read(
                chain((first,), iterator),
                as_of=as_of,
                since=since,
                until=until,
                period_window=period_window,
                entity_keys=entity_keys,
                max_rows=max_rows,
            )

    def _read(
        self,
        manifests: Iterable[dict],
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
        namespace = None
        for dataset in manifests:
            current_namespace = dataset.get("synthetic")
            if namespace is not None and current_namespace != namespace:
                raise ValueError("A history read cannot mix evidence namespaces")
            namespace = current_namespace
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
                        raise RowBudgetExceeded(row_budget)
                    for row in frame.iter_rows(named=True):
                        obs = self._decode(row)
                        prior = result.get(obs.observation_id)
                        if prior and self._payload(prior) != self._payload(obs):
                            raise ValueError(
                                "Conflicting payloads under one observation identifier"
                            )
                        result.setdefault(obs.observation_id, obs)
                        if len(result) > row_budget:
                            raise RowBudgetExceeded(row_budget)
        return sorted(result.values(), key=lambda obs: (obs.observed_at, obs.observation_id))

    def raw(self, dataset: dict) -> dict:
        with self.catalog.evidence_transaction(dataset.get("synthetic")):
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
        manifests: list[dict] = []

        def selected():
            for manifest in self.catalog.iter_manifests(
                synthetic=synthetic,
                as_of=as_of,
                since=since,
                until=until,
                period_window=period_window,
                entity_keys=entity_keys,
            ):
                manifests.append(manifest)
                yield manifest

        observations = self.read(
            selected(),
            as_of=as_of,
            since=since,
            until=until,
            period_window=period_window,
            entity_keys=entity_keys,
            max_rows=self.settings.analysis_max_rows_per_batch,
        )
        return observations, manifests

    def page(
        self,
        as_of: datetime,
        synthetic: bool,
        entity_keys: set[str],
        *,
        limit: int = 100,
        after: tuple[datetime, str] | None = None,
    ) -> tuple[list[Observation], tuple[datetime, str] | None]:
        """Select a page in SQL before loading the corresponding verified objects."""
        if not 1 <= limit <= 1000:
            raise ValueError("Evidence page limit must be between 1 and 1000")
        if not entity_keys:
            return [], None
        self._ensure_page_index(as_of, synthetic, entity_keys)
        with self.catalog.transaction() as connection:
            # A page's file reads share the same retention fence as its SQL
            # selection; retirement cannot delete an input halfway through it.
            self.catalog.guard_namespace(connection, synthetic)
            references = self.catalog.observation_page(
                synthetic, as_of, entity_keys, limit + 1, after, connection=connection
            )
            references.sort(key=lambda row: (row["observed_at"], row["observation_id"]))
            selected = references[:limit]
            expected = {row["observation_id"]: row for row in selected}
            objects = {row["object_key"]: row["descriptor"] for row in selected}
            observations: dict[str, Observation] = {}
            for obj in objects.values():
                with pin_cached(self, obj) as path:
                    query = self._page_query(path, as_of, entity_keys).filter(
                        pl.col("observation_id").is_in(list(expected))
                    )
                    fields = [
                        name for name in query.collect_schema().names() if name != "raw_dataset_id"
                    ]
                    frame = (
                        query.unique(subset=fields).head(2 * limit + 1).collect(engine="streaming")
                    )
                    for row in frame.iter_rows(named=True):
                        observation = self._decode(row)
                        reference = expected[observation.observation_id]
                        if (
                            self.catalog.observation_fingerprint(observation)
                            != reference["payload_sha256"]
                        ):
                            raise ValueError("Evidence index differs from verified object")
                        # Keep the oldest active delivery's original provenance.
                        if reference["object_key"] == obj["key"]:
                            observations.setdefault(observation.observation_id, observation)
            if len(observations) != len(selected):
                raise ValueError("Evidence index references missing observations")
            records = [observations[row["observation_id"]] for row in selected]
            cursor = (
                (records[-1].observed_at, records[-1].observation_id)
                if len(references) > limit
                else None
            )
            return records, cursor

    def _ensure_page_index(self, as_of: datetime, synthetic: bool, entity_keys: set[str]) -> None:
        """Backfill at most a configured number of legacy objects per request.

        Current publications index within their SQL commit. Legacy files are
        decoded in Arrow batches, never as an unbounded Python observation list.
        The explicit pending result prevents partial indexing from looking empty.
        """
        budget = self.settings.history_index_objects_per_call
        objects = self.catalog.unindexed_objects(synthetic, as_of, entity_keys, budget + 1)
        for obj in objects[:budget]:
            with self.catalog.transaction() as connection:
                self.catalog.guard_inputs(connection, [obj["dataset_id"]])
                state = connection.execute(
                    "SELECT observations_indexed FROM normalized_objects "
                    "WHERE dataset_id=%s AND object_key=%s FOR UPDATE",
                    (obj["dataset_id"], obj["object_key"]),
                ).fetchone()
                if not state or state["observations_indexed"]:
                    continue
                with pin_cached(self, obj["descriptor"]) as path:
                    for batch in pq.ParquetFile(path).iter_batches(batch_size=1000):
                        rows = [(obj["object_key"], self._decode(row)) for row in batch.to_pylist()]
                        self.catalog.index_object_rows(connection, obj["dataset_id"], rows)
                connection.execute(
                    "UPDATE normalized_objects SET observations_indexed=true "
                    "WHERE dataset_id=%s AND object_key=%s",
                    (obj["dataset_id"], obj["object_key"]),
                )
        if len(objects) > budget:
            raise HistoryIndexPending(
                "Historical evidence index is being prepared; retry this page"
            )

    @staticmethod
    def _page_query(path: Path, as_of: datetime, entity_keys: set[str]) -> pl.LazyFrame:
        return pl.scan_parquet(path).filter(
            (pl.col("available_at").str.to_datetime(time_zone="UTC") <= utc(as_of))
            & pl.col("entity_key").is_in(sorted(entity_keys))
        )

    @staticmethod
    def _decode(row: dict) -> Observation:
        row.pop("entity_key")
        for name in ("metrics", "attributes", "missing_fields"):
            row[name] = json.loads(row.pop(f"{name}_json"))
        return Observation.model_validate(row)

    @staticmethod
    def _payload(observation: Observation) -> dict:
        return observation.model_dump(exclude={"raw_dataset_id"})

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

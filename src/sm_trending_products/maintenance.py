"""Bounded compaction and policy-driven evidence deletion with publication fencing."""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import io
from typing import TYPE_CHECKING, Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from .cache import cache_object_lock
from .contracts import Observation, canonical, digest

if TYPE_CHECKING:
    from .pipeline import Pipeline

COMPACTION_MAX_ROWS = 100_000


class Maintenance:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.catalog = pipeline.catalog
        self.history = pipeline.history

    def compact(self, synthetic: bool = False, max_datasets: int = 100) -> dict[str, Any]:
        if not 2 <= max_datasets <= 1000:
            raise ValueError("Compaction dataset limit must be between 2 and 1000")
        row_limit = min(COMPACTION_MAX_ROWS, self.pipeline.settings.analysis_max_rows_per_batch)
        # Select a mergeable group in SQL before fetching manifests. Full outputs
        # and incompatible early groups cannot repeatedly occupy the candidate cap.
        group = self.catalog.one(
            "WITH ranked AS (SELECT source,transform_version,"
            "manifest->>'partition_buckets' AS buckets,row_count,created_at,"
            "row_number() OVER(PARTITION BY source,transform_version,"
            "manifest->>'partition_buckets' ORDER BY row_count,created_at,dataset_id) AS position "
            "FROM datasets WHERE state='committed' AND compacted_into IS NULL "
            "AND synthetic=%s AND row_count>0 AND row_count<%s AND quarantine_count=0) "
            "SELECT source,transform_version,buckets FROM ranked WHERE position<=2 "
            "GROUP BY source,transform_version,buckets HAVING count(*)=2 AND sum(row_count)<=%s "
            "ORDER BY min(created_at),source,transform_version LIMIT 1",
            (synthetic, row_limit, row_limit),
        )
        selected: list[dict] = []
        if group:
            candidates = self.catalog.all(
                "SELECT * FROM datasets WHERE state='committed' AND compacted_into IS NULL "
                "AND synthetic=%s AND row_count>0 AND row_count<%s AND quarantine_count=0 "
                "AND source=%s AND transform_version=%s AND manifest->>'partition_buckets'=%s "
                "ORDER BY row_count,created_at,dataset_id LIMIT %s",
                (
                    synthetic,
                    row_limit,
                    group["source"],
                    group["transform_version"],
                    group["buckets"],
                    max_datasets,
                ),
            )
            rows = 0
            for dataset in candidates:
                if rows + dataset["row_count"] <= row_limit:
                    selected.append(dataset)
                    rows += dataset["row_count"]
        if len(selected) < 2:
            return {
                "status": "no_work",
                "reason": "fewer_than_two_eligible_datasets",
                "row_limit": row_limit,
            }
        # Size determines membership, not delivery precedence. Deduplication
        # must retain the same oldest provenance as ordinary catalog reads,
        # including when recovering an already-uploaded legacy compaction.
        selected.sort(key=lambda dataset: (dataset["created_at"], dataset["dataset_id"]))
        cutoff = max(dataset["max_available_at"] for dataset in selected)
        observations = self.history.read(selected, as_of=cutoff, max_rows=row_limit)
        parent_ids = sorted(dataset["dataset_id"] for dataset in selected)
        transform_version = selected[0]["transform_version"]
        dataset_id = digest(
            {"kind": "compaction", "parents": parent_ids, "version": transform_version}
        )
        staged = self.catalog.one(
            "SELECT manifest FROM datasets WHERE dataset_id=%s AND state='staged'", (dataset_id,)
        )
        source = selected[0]["source"]
        partition_buckets = selected[0]["manifest"]["partition_buckets"]
        objects, raw_body, manifest = self._compaction_objects(
            observations,
            source,
            synthetic,
            parent_ids,
            dataset_id,
            partition_buckets,
            recorded=staged["manifest"] if staged else None,
            transform_version=transform_version,
        )
        self.catalog.stage_dataset(dataset_id, source, synthetic, manifest)
        self.history.put(manifest["raw"]["key"], raw_body, content_type="application/gzip")
        for obj, body in objects:
            self.history.put(obj["key"], body)
        self.history.put(
            manifest["manifest_key"], canonical(manifest).encode(), content_type="application/json"
        )
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            parents = connection.execute(
                "SELECT * FROM datasets WHERE dataset_id=ANY(%s) ORDER BY dataset_id FOR UPDATE",
                (parent_ids,),
            ).fetchall()
            child = connection.execute(
                "SELECT * FROM datasets WHERE dataset_id=%s FOR UPDATE", (dataset_id,)
            ).fetchone()
            if child and child["state"] == "committed":
                return {
                    "status": "compacted",
                    "dataset_id": dataset_id,
                    "parent_dataset_ids": parent_ids,
                    "rows": len(observations),
                }
            if (
                not child
                or child["state"] != "staged"
                or len(parents) != len(parent_ids)
                or any(
                    parent["state"] != "committed" or parent["compacted_into"] for parent in parents
                )
            ):
                raise ValueError("Compaction parents changed before publication; retry")
            connection.execute(
                "UPDATE datasets SET state='committed',manifest=%s,row_count=%s,"
                "min_available_at=%s,max_available_at=%s,committed_at=clock_timestamp() "
                "WHERE dataset_id=%s",
                (
                    Jsonb(manifest),
                    len(observations),
                    min(obs.available_at for obs in observations),
                    max(obs.available_at for obs in observations),
                    dataset_id,
                ),
            )
            self.catalog.index_observations(connection, dataset_id, manifest, observations)
            connection.execute(
                "UPDATE datasets SET compacted_into=%s WHERE dataset_id=ANY(%s)",
                (dataset_id, parent_ids),
            )
        return {
            "status": "compacted",
            "dataset_id": dataset_id,
            "parent_dataset_ids": parent_ids,
            "rows": len(observations),
            "normalized_objects_before": sum(
                len(item["manifest"]["normalized"]) for item in selected
            ),
            "normalized_objects_after": len(objects),
        }

    def _compaction_objects(
        self,
        observations: list[Observation],
        source: str,
        synthetic: bool,
        parent_ids: list[str],
        dataset_id: str,
        partition_buckets: int,
        *,
        recorded: dict | None = None,
        transform_version: str = "1",
    ) -> tuple[list[tuple[dict, bytes]], bytes, dict]:
        namespace = "synthetic" if synthetic else "live"
        date = min(obs.observed_at for obs in observations).date().isoformat()
        prefix = f"{namespace}/source={source}/date={date}"
        raw_stream = io.BytesIO()
        header = {
            "schema_version": "1",
            "kind": "compaction",
            "parent_dataset_ids": parent_ids,
            "observed_at": min(obs.observed_at for obs in observations).isoformat(),
        }
        with gzip.GzipFile(fileobj=raw_stream, mode="wb", mtime=0) as compressed:
            compressed.write(canonical(header).removesuffix("}").encode() + b',"records":[')
            for index, obs in enumerate(observations):
                if index:
                    compressed.write(b",")
                compressed.write(canonical(obs.model_dump(mode="json")).encode())
            compressed.write(b"]}\n")
        raw_body = raw_stream.getvalue()
        raw_hash = hashlib.sha256(raw_body).hexdigest()
        objects, physical_buckets = self.history.normalized_objects(
            observations,
            prefix,
            partition_buckets,
            physical_buckets=(
                recorded.get("physical_partition_buckets", partition_buckets) if recorded else None
            ),
        )
        manifest = {
            "schema_version": "1",
            "dataset_id": dataset_id,
            "transform_version": transform_version,
            "source": source,
            "synthetic": synthetic,
            "partition_buckets": partition_buckets,
            "physical_partition_buckets": physical_buckets,
            "parent_dataset_ids": parent_ids,
            "raw": {
                "key": f"raw/{prefix}/{raw_hash}.jsonl.gz",
                "sha256": raw_hash,
                "bytes": len(raw_body),
            },
            "normalized": [obj for obj, _ in objects],
            "row_count": len(observations),
            "quarantine_count": 0,
            "quarantine": [],
            "task_request": None,
            "supersedes": None,
            "manifest_key": f"manifests/{namespace}/{dataset_id}.json",
        }
        if recorded:
            objects = self.history.recover_staged_objects(recorded, raw_hash, objects)
            manifest = recorded
        return objects, raw_body, manifest

    @staticmethod
    def _expired_closure() -> str:
        # Indexed lineage and shared object references extend each bounded root batch.
        return """
            WITH RECURSIVE roots AS (
                SELECT d.dataset_id FROM datasets d
                JOIN source_policies p ON p.source=d.source AND p.verified
                WHERE d.state IN ('committed','staged')
                AND d.created_at < clock_timestamp()-p.retention_days*interval '1 day'
                ORDER BY d.created_at,d.dataset_id LIMIT 100
            ), affected(dataset_id) AS (
                SELECT dataset_id FROM roots
                UNION
                SELECT edge.dataset_id FROM affected current
                CROSS JOIN LATERAL (
                    SELECT child_dataset_id AS dataset_id FROM dataset_lineage
                    WHERE parent_dataset_id=current.dataset_id
                    UNION
                    SELECT shared.dataset_id FROM dataset_object_refs origin
                    JOIN dataset_object_refs shared ON shared.object_key=origin.object_key
                    WHERE origin.dataset_id=current.dataset_id
                ) edge
            )
            SELECT dataset_id FROM affected
        """

    def retention(self, *, apply: bool = False) -> dict[str, Any]:
        closure = self._expired_closure()
        preview = self.catalog.all(
            f"SELECT d.dataset_id,d.source,count(*) OVER() AS total "
            f"FROM datasets d JOIN ({closure}) targets USING(dataset_id) "
            "WHERE d.state<>'deleted' ORDER BY d.dataset_id LIMIT 1000"
        )
        result: dict[str, Any] = {
            "applied": apply,
            "dataset_count": preview[0]["total"] if preview else 0,
            "dataset_ids": [row["dataset_id"] for row in preview],
            "preview_truncated": bool(preview and preview[0]["total"] > len(preview)),
            "unverified_sources": [
                row["source"]
                for row in self.catalog.all(
                    "SELECT DISTINCT d.source FROM datasets d LEFT JOIN source_policies p "
                    "ON p.source=d.source WHERE d.state='committed' "
                    "AND (p.source IS NULL OR NOT p.verified) ORDER BY d.source"
                )
            ],
        }
        if not apply:
            pending = self.catalog.one(
                "SELECT count(*) AS count FROM retention_objects WHERE state='pending'"
            )
            result["pending_objects"] = pending["count"] if pending else 0
            return result
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection, exclusive=True)
            connection.execute("CREATE TEMP TABLE retention_targets ON COMMIT DROP AS " + closure)
            # Guarded writers cannot publish new derivatives between this closure and tombstones.
            connection.execute(
                "UPDATE datasets SET state='invalidated',invalid_reason='retention_expired' "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "AND state<>'deleted'"
            )
            connection.execute(
                "CREATE TEMP TABLE retention_derivations ON COMMIT DROP AS "
                "SELECT object_key FROM derived_publications WHERE state<>'deleted' "
                "AND evidence_datasets ?| ARRAY(SELECT dataset_id FROM retention_targets)"
            )
            connection.execute(
                "CREATE TEMP TABLE retention_assessments ON COMMIT DROP AS "
                "SELECT run_id FROM assessment_runs WHERE object_key IN ("
                "SELECT object_key FROM assessment_runs "
                "WHERE evidence_datasets ?| ARRAY(SELECT dataset_id FROM retention_targets) "
                "UNION SELECT object_key FROM retention_derivations)"
            )
            connection.execute(
                "CREATE TEMP TABLE retention_evaluations ON COMMIT DROP AS "
                "WITH RECURSIVE affected(evaluation_id) AS ("
                "SELECT evaluation_id FROM evaluation_dependencies "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "UNION SELECT child.evaluation_id FROM evaluation_runs child "
                "JOIN affected parent ON child.parent_id=parent.evaluation_id) "
                "SELECT evaluation_id FROM affected"
            )
            connection.execute(
                "INSERT INTO retention_objects(object_key,reason) "
                "SELECT object_key,'retention_expired' FROM dataset_object_refs "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "UNION SELECT object_key,'retention_expired' FROM assessment_runs "
                "WHERE run_id IN (SELECT run_id FROM retention_assessments) "
                "UNION SELECT object_key,'retention_expired' FROM retention_derivations "
                "ON CONFLICT DO NOTHING"
            )
            connection.execute(
                "UPDATE derived_publications SET state='invalidated' WHERE object_key IN "
                "(SELECT object_key FROM retention_derivations)"
            )
            connection.execute(
                "UPDATE assessment_runs SET invalidated=true "
                "WHERE run_id IN (SELECT run_id FROM retention_assessments)"
            )
            connection.execute(
                "DELETE FROM current_assessments WHERE run_id IN "
                "(SELECT run_id FROM retention_assessments)"
            )
            tombstone = Jsonb({"status": "invalidated", "reason": "retention_expired"})
            connection.execute(
                "UPDATE alerts SET invalidated=true,payload=%s WHERE run_id IN "
                "(SELECT run_id FROM retention_assessments)",
                (tombstone,),
            )
            connection.execute(
                "UPDATE evaluation_runs SET invalidated=true,payload=%s WHERE evaluation_id IN "
                "(SELECT evaluation_id FROM retention_evaluations)",
                (tombstone,),
            )
            connection.execute(
                "UPDATE datasets SET manifest=manifest-'quarantine'-'task_request' "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "AND state='invalidated'"
            )
            # If a mixed compacted child is removed, surviving originals become readable again.
            connection.execute(
                "UPDATE datasets SET compacted_into=NULL WHERE state='committed' "
                "AND compacted_into IN (SELECT dataset_id FROM retention_targets)"
            )
            result["restored_entity_origins"] = self._repair_entity_origins(connection)
            actual = connection.execute(
                "SELECT dataset_id,count(*) OVER() AS total FROM retention_targets "
                "ORDER BY dataset_id LIMIT 1000"
            ).fetchall()
            result.update(
                dataset_count=actual[0]["total"] if actual else 0,
                dataset_ids=[row["dataset_id"] for row in actual],
            )
        deleted, failed = self._delete_pending_objects()
        self.catalog.execute(
            "UPDATE datasets d SET state='deleted',manifest=jsonb_build_object("
            "'dataset_id',d.dataset_id,'status','deleted','reason','retention_expired') "
            "WHERE d.state='invalidated' AND d.invalid_reason='retention_expired' "
            "AND NOT EXISTS (SELECT 1 FROM dataset_object_refs r JOIN retention_objects o "
            "ON o.object_key=r.object_key WHERE r.dataset_id=d.dataset_id AND o.state='pending')"
        )
        self.catalog.execute(
            "UPDATE derived_publications SET state='deleted',evidence_datasets='[]' "
            "WHERE state='invalidated' AND object_key IN "
            "(SELECT object_key FROM retention_objects WHERE state='deleted')"
        )
        pending = self.catalog.one(
            "SELECT count(*) AS count FROM retention_objects WHERE state='pending'"
        )
        result.update(
            deleted_objects=deleted,
            failed_objects=failed,
            pending_objects=pending["count"] if pending else 0,
        )
        return result

    @staticmethod
    def _repair_entity_origins(connection: Connection[dict[str, Any]]) -> int:
        # Use manifest metadata only; do not materialize historical Parquet under this lock.
        connection.execute(
            "CREATE TEMP TABLE retention_entity_replacements ON COMMIT DROP AS "
            "SELECT e.entity_key,e.synthetic,replacement.dataset_id,replacement.account_id "
            "FROM entities e JOIN retention_targets expired "
            "ON expired.dataset_id=e.origin->>'dataset_id' "
            "CROSS JOIN LATERAL ("
            "SELECT d.dataset_id,CASE WHEN d.manifest->'task_request'->>'source'=e.source "
            "THEN COALESCE(NULLIF(d.manifest->'task_request'->>'account_id',''),e.account_id) "
            "ELSE e.account_id END AS account_id "
            "FROM datasets d JOIN source_policies policy ON policy.source=d.source "
            "WHERE d.state='committed' AND d.source=e.source AND d.synthetic=e.synthetic "
            "AND policy.verified AND d.created_at>=clock_timestamp()-"
            "policy.retention_days*interval '1 day' "
            "AND d.manifest @> jsonb_build_object('normalized',jsonb_build_array("
            "jsonb_build_object('entity_keys',jsonb_build_array(e.entity_key)))) "
            "ORDER BY COALESCE(d.manifest->'task_request'->>'source'=e.source "
            "AND NULLIF(d.manifest->'task_request'->>'account_id','') IS NOT NULL "
            "AND ((e.source IN ('tiktok_ads','tiktok_commercial') "
            "AND d.manifest->'task_request'->>'operation'='query') "
            "OR (e.source='tiktok_display' "
            "AND d.manifest->'task_request'->>'operation' IN ('query','list'))),false) DESC,"
            "d.max_available_at DESC NULLS LAST,d.committed_at DESC,d.dataset_id LIMIT 1"
            ") replacement"
        )
        restored = connection.execute(
            "UPDATE entities e SET account_id=replacement.account_id,"
            "title='',attributes='{}',origin=jsonb_build_object("
            "'dataset_id',replacement.dataset_id,'source',e.source) "
            "FROM retention_entity_replacements replacement "
            "WHERE e.entity_key=replacement.entity_key AND e.synthetic=replacement.synthetic"
        ).rowcount
        # Current fresh origins are untouched. Expired metadata has no remaining provenance.
        connection.execute(
            "UPDATE entities SET title='',attributes='{}',"
            "origin='{\"retention_purged\":true}'::jsonb "
            "WHERE origin->>'dataset_id' IN (SELECT dataset_id FROM retention_targets)"
        )
        return restored

    def _delete_pending_objects(self) -> tuple[int, int]:
        from botocore.exceptions import BotoCoreError, ClientError

        deleted = failed = 0
        pending = self.catalog.all(
            "SELECT object_key FROM retention_objects WHERE state='pending' "
            "ORDER BY created_at,object_key LIMIT 1000"
        )
        if not pending:
            return deleted, failed
        try:
            self.history.metrics["get_requests"] += 1
            versioning = self.history.client.get_bucket_versioning(Bucket=self.history.bucket)
            versioned = versioning.get("Status") in {"Enabled", "Suspended"}
        except (BotoCoreError, ClientError):
            for item in pending:
                self.catalog.execute(
                    "UPDATE retention_objects SET attempts=attempts+1,"
                    "last_error='versioning_verification_failed' WHERE object_key=%s",
                    (item["object_key"],),
                )
            return 0, len(pending)
        for item in pending:
            key = item["object_key"]
            try:
                with cache_object_lock(self.history, key):
                    if versioned:
                        self._delete_object_versions(key)
                    else:
                        self.history.metrics["delete_requests"] = (
                            self.history.metrics.get("delete_requests", 0) + 1
                        )
                        self.history.client.delete_object(Bucket=self.history.bucket, Key=key)
                    with (self.pipeline.settings.cache_dir / ".cache.lock").open("a") as lock:
                        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                        cached = (
                            self.pipeline.settings.cache_dir
                            / hashlib.sha256(key.encode()).hexdigest()
                        )
                        cached.unlink(missing_ok=True)
            except (BotoCoreError, ClientError, OSError):
                self.catalog.execute(
                    "UPDATE retention_objects SET attempts=attempts+1,"
                    "last_error='object_delete_failed' "
                    "WHERE object_key=%s",
                    (key,),
                )
                failed += 1
            else:
                self.catalog.execute(
                    "UPDATE retention_objects SET state='deleted',attempts=attempts+1,"
                    "deleted_at=clock_timestamp(),last_error=NULL WHERE object_key=%s",
                    (key,),
                )
                deleted += 1
        return deleted, failed

    def _delete_object_versions(self, key: str) -> None:
        from botocore.exceptions import ClientError

        for _batch in range(100):
            self.history.metrics["list_requests"] = self.history.metrics.get("list_requests", 0) + 1
            response = self.history.client.list_object_versions(
                Bucket=self.history.bucket, Prefix=key, MaxKeys=1000
            )
            versions = [
                {"Key": key, "VersionId": item["VersionId"]}
                for group in ("Versions", "DeleteMarkers")
                for item in response.get(group, [])
                if item["Key"] == key
            ]
            if not versions:
                return
            result = self.history.client.delete_objects(
                Bucket=self.history.bucket, Delete={"Objects": versions, "Quiet": True}
            )
            self.history.metrics["delete_requests"] = (
                self.history.metrics.get("delete_requests", 0) + 1
            )
            if result.get("Errors"):
                raise ClientError(
                    {"Error": {"Code": "ObjectVersionDeletionFailed"}}, "DeleteObjects"
                )
        raise ClientError({"Error": {"Code": "ObjectVersionBatchIncomplete"}}, "DeleteObjects")

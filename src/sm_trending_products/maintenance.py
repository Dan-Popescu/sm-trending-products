"""Bounded compaction and policy-driven evidence deletion with publication fencing."""

from __future__ import annotations

import gzip
import hashlib
import io
import math
from dataclasses import dataclass
from datetime import datetime
from time import monotonic
from typing import TYPE_CHECKING, Any

from psycopg import Connection, sql
from psycopg.errors import LockNotAvailable, QueryCanceled
from psycopg.types.json import Jsonb

from .cache import cache_object_lock
from .contracts import Observation, canonical, digest

if TYPE_CHECKING:
    from .pipeline import Pipeline

COMPACTION_MAX_ROWS = 100_000
RETENTION_ROOT_BATCH = 100
RETENTION_TARGET_LIMIT = 10_000
ARTIFACT_RELATION_BATCH = 1000
MAINTENANCE_PREVIEW_LIMIT = 1000


class _BudgetExhausted(Exception):
    pass


class _RetentionClosureTooLarge(Exception):
    pass


@dataclass
class _DrainBudget:
    max_seconds: float
    max_operations: int
    started: float
    operations: int = 0

    def check(self) -> None:
        if monotonic() - self.started >= self.max_seconds:
            raise _BudgetExhausted("deadline")

    def request(self) -> None:
        self.check()
        if self.operations >= self.max_operations:
            raise _BudgetExhausted("operation_limit")
        self.operations += 1


def _validate_drain_limits(max_batches: int, max_seconds: float) -> None:
    if (
        isinstance(max_batches, bool)
        or not isinstance(max_batches, int)
        or not 1 <= max_batches <= 1000
    ):
        raise ValueError("Maintenance batch limit must be between 1 and 1000")
    if (
        isinstance(max_seconds, bool)
        or not isinstance(max_seconds, (int, float))
        or not math.isfinite(max_seconds)
        or not 0 < max_seconds <= 3600
    ):
        raise ValueError("Maintenance deadline must be positive and at most 3600 seconds")


class Maintenance:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.catalog = pipeline.catalog
        self.history = pipeline.history

    def compact(
        self,
        synthetic: bool = False,
        max_datasets: int = 100,
        *,
        committed_before: datetime | None = None,
    ) -> dict[str, Any]:
        if not 2 <= max_datasets <= 1000:
            raise ValueError("Compaction dataset limit must be between 2 and 1000")
        row_limit = min(COMPACTION_MAX_ROWS, self.pipeline.settings.analysis_max_rows_per_batch)
        cutoff_sql = (
            "AND COALESCE(committed_at,created_at)<=%s " if committed_before is not None else ""
        )
        cutoff_params = (committed_before,) if committed_before is not None else ()
        # Select a mergeable group in SQL before fetching manifests. Full outputs
        # and incompatible early groups cannot repeatedly occupy the candidate cap.
        group = self.catalog.one(
            "WITH ranked AS (SELECT source,transform_version,"
            "manifest->>'partition_buckets' AS buckets,row_count,created_at,"
            "row_number() OVER(PARTITION BY source,transform_version,"
            "manifest->>'partition_buckets' ORDER BY row_count,created_at,dataset_id) AS position "
            "FROM datasets WHERE state='committed' AND compacted_into IS NULL "
            "AND synthetic=%s AND row_count>0 AND row_count<%s AND quarantine_count=0 "
            + cutoff_sql
            + ") SELECT source,transform_version,buckets FROM ranked WHERE position<=2 "
            "GROUP BY source,transform_version,buckets HAVING count(*)=2 AND sum(row_count)<=%s "
            "ORDER BY min(created_at),source,transform_version LIMIT 1",
            (synthetic, row_limit, *cutoff_params, row_limit),
        )
        selected: list[dict] = []
        if group:
            candidates = self.catalog.all(
                "SELECT * FROM datasets WHERE state='committed' AND compacted_into IS NULL "
                "AND synthetic=%s AND row_count>0 AND row_count<%s AND quarantine_count=0 "
                "AND source=%s AND transform_version=%s AND manifest->>'partition_buckets'=%s "
                + cutoff_sql
                + "ORDER BY row_count,created_at,dataset_id LIMIT %s",
                (
                    synthetic,
                    row_limit,
                    group["source"],
                    group["transform_version"],
                    group["buckets"],
                    *cutoff_params,
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
            target_rows_per_file=self.pipeline.settings.compaction_target_rows_per_file,
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
    def _expired_closure(root_limit: int = RETENTION_ROOT_BATCH) -> str:
        # Indexed lineage and shared object references extend each bounded root batch.
        return f"""
            WITH RECURSIVE roots AS (
                SELECT d.dataset_id FROM datasets d
                JOIN source_policies p ON p.source=d.source AND p.verified
                WHERE d.state IN ('committed','staged')
                AND d.created_at < clock_timestamp()-p.retention_days*interval '1 day'
                ORDER BY d.created_at,d.dataset_id LIMIT {root_limit}
            )
            SELECT dataset_id FROM dataset_publication_closure(ARRAY(SELECT dataset_id FROM roots))
        """

    def retention(self, *, apply: bool = False) -> dict[str, Any]:
        closure = self._expired_closure()
        roots = self.catalog.all(
            "SELECT d.dataset_id,d.source,count(*) OVER() AS total FROM datasets d "
            "JOIN source_policies p ON p.source=d.source AND p.verified "
            "WHERE d.state IN ('committed','staged') AND d.created_at<clock_timestamp()-"
            "p.retention_days*interval '1 day' ORDER BY d.created_at,d.dataset_id LIMIT 100"
        )
        wide = self.catalog.one(
            "SELECT source_invalidation_wide(%s) AS wide", ([row["dataset_id"] for row in roots],)
        )
        preview_incomplete = bool(wide and wide["wide"])
        preview = roots
        if not preview_incomplete:
            try:
                with self.catalog.transaction() as connection:
                    connection.execute("SET LOCAL statement_timeout='2s'")
                    preview = connection.execute(
                        f"SELECT d.dataset_id,d.source,count(*) OVER() AS total "
                        f"FROM datasets d JOIN ({closure} LIMIT {RETENTION_TARGET_LIMIT + 1}) "
                        "targets USING(dataset_id) "
                        "WHERE d.state<>'deleted' ORDER BY d.dataset_id LIMIT 1000"
                    ).fetchall()
            except QueryCanceled:
                preview_incomplete = True
                preview = roots
        result: dict[str, Any] = {
            "applied": apply,
            "dataset_count": preview[0]["total"] if preview else 0,
            "dataset_ids": [row["dataset_id"] for row in preview],
            "preview_truncated": preview_incomplete
            or bool(preview and preview[0]["total"] > len(preview)),
            "dataset_count_is_lower_bound": preview_incomplete
            or bool(preview and preview[0]["total"] > RETENTION_TARGET_LIMIT),
            "preview_scope": "expired_roots_only" if preview_incomplete else "bounded_closure",
            "unverified_sources": self._unverified_sources(),
        }
        if not apply:
            pending = self.catalog.one(
                "SELECT count(*) AS count FROM retention_objects WHERE state='pending'"
            )
            result["pending_objects"] = pending["count"] if pending else 0
            result.update(self.retention_backlog())
            return result
        result.update(self._retire_expired())
        deletion = self._delete_pending_objects()
        finalized = self._finish_retention()
        result.update(
            deleted_objects=deletion["deleted"],
            failed_objects=deletion["failed"],
            artifact_relations_deleted=finalized["artifact_relations_deleted"],
            artifact_gc_completed_parents=finalized["artifact_gc_completed_parents"],
            artifact_gc_blocked_parent_keys=finalized["artifact_gc_blocked_parent_keys"][:20],
            **self.retention_backlog(),
        )
        return result

    def _retire_expired(self, *, budget: _DrainBudget | None = None) -> dict[str, Any]:
        root_limit = RETENTION_ROOT_BATCH
        while True:
            if budget:
                budget.check()
            try:
                return self._retire_root_batch(root_limit)
            except LockNotAvailable:
                if budget is None:
                    raise
                raise _BudgetExhausted("database_busy") from None
            except (_RetentionClosureTooLarge, QueryCanceled):
                if root_limit == 1:
                    if budget is None:
                        raise
                    from .invalidation import SourceInvalidation

                    root = self.catalog.one(
                        "SELECT d.dataset_id FROM datasets d JOIN source_policies p "
                        "ON p.source=d.source AND p.verified "
                        "WHERE d.state IN ('committed','staged') "
                        "AND d.created_at<clock_timestamp()-p.retention_days*interval '1 day' "
                        "ORDER BY d.created_at,d.dataset_id LIMIT 1"
                    )
                    if root:
                        SourceInvalidation(self.catalog).begin(
                            root["dataset_id"], "retention_expired"
                        )
                        raise _BudgetExhausted("invalidation_pending") from None
                    raise
                root_limit = max(1, root_limit // 2)

    @staticmethod
    def _guard_retention_table(connection: Connection[dict[str, Any]], table: str) -> int:
        row = connection.execute(
            sql.SQL("SELECT count(*) AS count FROM {}").format(sql.Identifier(table))
        ).fetchone()
        count = row["count"] if row else 0
        if count > RETENTION_TARGET_LIMIT:
            raise _RetentionClosureTooLarge("retention_closure_limit")
        return count

    def _retire_root_batch(self, root_limit: int) -> dict[str, Any]:
        with self.catalog.transaction() as connection:
            connection.execute("SET LOCAL statement_timeout='2s'")
            connection.execute("SET LOCAL lock_timeout='250ms'")
            self.catalog.guard_retention(connection, exclusive=True)
            roots = connection.execute(
                "SELECT d.dataset_id FROM datasets d JOIN source_policies p "
                "ON p.source=d.source AND p.verified WHERE d.state IN ('committed','staged') "
                "AND d.created_at<clock_timestamp()-p.retention_days*interval '1 day' "
                "ORDER BY d.created_at,d.dataset_id LIMIT %s",
                (root_limit,),
            ).fetchall()
            root_ids = [root["dataset_id"] for root in roots]
            wide = connection.execute(
                "SELECT source_invalidation_wide(%s) AS wide", (root_ids,)
            ).fetchone()
            if wide and wide["wide"]:
                raise _RetentionClosureTooLarge("retention_work_limit")
            connection.execute(
                "CREATE TEMP TABLE retention_targets ON COMMIT DROP AS "
                "SELECT dataset_id FROM dataset_publication_closure(%s) "
                f"LIMIT {RETENTION_TARGET_LIMIT + 1}",
                (root_ids,),
            )
            if not self._guard_retention_table(connection, "retention_targets"):
                return {
                    "dataset_count": 0,
                    "dataset_ids": [],
                    "preview_truncated": False,
                    "restored_entity_origins": 0,
                }
            # Shared descendants can recur in later root chunks. Count each newly
            # retired dataset once, while retaining the full closure for fencing.
            connection.execute(
                "CREATE TEMP TABLE retention_new_targets ON COMMIT DROP AS "
                "SELECT d.dataset_id FROM datasets d JOIN retention_targets t USING(dataset_id) "
                "WHERE d.state<>'deleted' AND (d.state<>'invalidated' "
                "OR d.invalid_reason IS DISTINCT FROM 'retention_expired')"
            )
            # Guarded writers cannot publish new derivatives between this closure and tombstones.
            connection.execute(
                "UPDATE datasets SET state='invalidated',invalid_reason='retention_expired' "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "AND state<>'deleted'"
            )
            connection.execute(
                "CREATE TEMP TABLE retention_derivations ON COMMIT DROP AS "
                "SELECT p.object_id,p.object_key FROM analytical_artifact_ancestor_ids("
                "ARRAY(SELECT dataset_id FROM retention_targets)) affected "
                "JOIN derived_publications p USING(object_id) "
                f"LIMIT {RETENTION_TARGET_LIMIT + 1}"
            )
            self._guard_retention_table(connection, "retention_derivations")
            connection.execute(
                "CREATE TEMP TABLE retention_assessments ON COMMIT DROP AS "
                "SELECT run_id FROM assessment_runs WHERE object_key IN ("
                "SELECT object_key FROM assessment_runs "
                "WHERE evidence_datasets ?| ARRAY(SELECT dataset_id FROM retention_targets) "
                "UNION SELECT object_key FROM retention_derivations) "
                f"LIMIT {RETENTION_TARGET_LIMIT + 1}"
            )
            self._guard_retention_table(connection, "retention_assessments")
            connection.execute(
                "CREATE TEMP TABLE retention_evaluations ON COMMIT DROP AS "
                "WITH RECURSIVE affected(evaluation_id) AS ("
                "SELECT evaluation_id FROM evaluation_dependencies "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "UNION SELECT evaluation_id FROM evaluation_artifacts "
                "WHERE object_id IN (SELECT object_id FROM retention_derivations) "
                "UNION SELECT child.evaluation_id FROM evaluation_runs child "
                "JOIN affected parent ON child.parent_id=parent.evaluation_id) "
                "SELECT evaluation_id FROM affected "
                f"LIMIT {RETENTION_TARGET_LIMIT + 1}"
            )
            self._guard_retention_table(connection, "retention_evaluations")
            for table, query in (
                (
                    "retention_alerts",
                    "SELECT alert_id FROM alerts WHERE run_id IN "
                    "(SELECT run_id FROM retention_assessments)",
                ),
                (
                    "retention_current_assessments",
                    "SELECT product_id FROM current_assessments "
                    "WHERE run_id IN (SELECT run_id FROM retention_assessments)",
                ),
                (
                    "retention_entities",
                    "SELECT entity_key,synthetic FROM entities WHERE "
                    "origin->>'dataset_id' IN (SELECT dataset_id FROM retention_targets)",
                ),
                (
                    "retention_restored_parents",
                    "SELECT dataset_id FROM datasets "
                    "WHERE state='committed' AND compacted_into IN "
                    "(SELECT dataset_id FROM retention_targets)",
                ),
            ):
                connection.execute(
                    sql.SQL(
                        "CREATE TEMP TABLE {} ON COMMIT DROP AS "
                        + query
                        + f" LIMIT {RETENTION_TARGET_LIMIT + 1}"
                    ).format(sql.Identifier(table))
                )
                self._guard_retention_table(connection, table)
            connection.execute(
                "CREATE TEMP TABLE retention_object_targets ON COMMIT DROP AS "
                "SELECT object_key FROM dataset_object_refs "
                "WHERE dataset_id IN (SELECT dataset_id FROM retention_targets) "
                "UNION SELECT object_key FROM assessment_runs "
                "WHERE run_id IN (SELECT run_id FROM retention_assessments) "
                "UNION SELECT object_key FROM retention_derivations "
                f"LIMIT {RETENTION_TARGET_LIMIT + 1}"
            )
            self._guard_retention_table(connection, "retention_object_targets")
            connection.execute(
                "INSERT INTO retention_objects(object_key,reason) "
                "SELECT object_key,'retention_expired' FROM retention_object_targets "
                "ON CONFLICT DO NOTHING"
            )
            connection.execute(
                "UPDATE derived_publications SET state='invalidated' WHERE state<>'deleted' "
                "AND object_id IN "
                "(SELECT object_id FROM retention_derivations)"
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
            restored = self._repair_entity_origins(connection)
            actual = connection.execute(
                "SELECT dataset_id,count(*) OVER() AS total FROM retention_new_targets "
                "ORDER BY dataset_id LIMIT 1000"
            ).fetchall()
            count = actual[0]["total"] if actual else 0
            return {
                "dataset_count": count,
                "dataset_ids": [row["dataset_id"] for row in actual],
                "preview_truncated": count > len(actual),
                "restored_entity_origins": restored,
            }

    def _unverified_sources(self) -> list[str]:
        return [
            row["source"]
            for row in self.catalog.all(
                "SELECT DISTINCT d.source FROM datasets d LEFT JOIN source_policies p "
                "ON p.source=d.source WHERE d.state='committed' "
                "AND (p.source IS NULL OR NOT p.verified) ORDER BY d.source"
            )
        ]

    def retention_backlog(self) -> dict[str, Any]:
        """Describe durable work, independently from the bounded deletion preview."""
        expired = self.catalog.one(
            "SELECT count(*) AS count,GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-"
            "min(d.created_at+p.retention_days*interval '1 day')))::float8 AS oldest_age_seconds "
            "FROM datasets d JOIN source_policies p ON p.source=d.source AND p.verified "
            "WHERE d.state IN ('committed','staged') AND "
            "d.created_at<clock_timestamp()-p.retention_days*interval '1 day'"
        )
        pending = self.catalog.one(
            "SELECT count(*) AS count,GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-"
            "min(created_at)))::float8 AS oldest_age_seconds "
            "FROM retention_objects WHERE state='pending'"
        )
        finalizing = self.catalog.one(
            "SELECT (SELECT count(*) FROM datasets WHERE state='invalidated' "
            "AND invalid_reason='retention_expired')+(SELECT count(*) "
            "FROM derived_publications WHERE state='invalidated') AS count"
        )
        gc = self.catalog.one(
            "SELECT count(*) AS count FROM derived_publications "
            "WHERE state='deleted' AND NOT relations_purged"
        )
        return {
            "expired_root_count": expired["count"] if expired else 0,
            "oldest_expired_root_age_seconds": expired["oldest_age_seconds"]
            if expired and expired["count"]
            else None,
            "pending_objects": pending["count"] if pending else 0,
            "oldest_pending_object_age_seconds": pending["oldest_age_seconds"]
            if pending and pending["count"]
            else None,
            "pending_finalization_count": finalizing["count"] if finalizing else 0,
            "pending_artifact_gc_count": gc["count"] if gc else 0,
            "pending_source_invalidations": self.catalog.pending_invalidations(),
        }

    @staticmethod
    def _gc_artifact_relations(
        connection: Connection[dict[str, Any]],
        limit: int,
        *,
        after: tuple[str, bool] | None = None,
    ) -> dict[str, Any]:
        """Remove at most 1,000 edges/inputs/evaluation links in total.

        The pending-publication index owns progress. Keysets pass anomalous live
        evaluation links, while partially cleaned parents resume inclusively.
        """
        limit = min(limit, ARTIFACT_RELATION_BATCH)
        keyset = ""
        params: list[Any] = []
        if after is not None:
            keyset = "AND object_key" + (">=" if after[1] else ">") + "%s "
            params.append(after[0])
        params.append(limit)
        parents = connection.execute(
            "SELECT object_id,object_key FROM derived_publications WHERE state='deleted' "
            "AND NOT relations_purged "
            + keyset
            + "ORDER BY object_key LIMIT %s FOR UPDATE SKIP LOCKED",
            params,
        ).fetchall()
        keys = [row["object_key"] for row in parents]
        identities = [row["object_id"] for row in parents]
        result: dict[str, Any] = {
            "artifact_relations_deleted": 0,
            "artifact_gc_completed_parents": 0,
            "artifact_gc_blocked_parent_keys": [],
            "artifact_gc_next": None,
            "artifact_gc_end": True,
        }
        if not keys:
            return result
        for table, key, order, condition in (
            ("derived_artifact_edges", "parent_id", "child_id", ""),
            ("derived_artifact_inputs", "object_id", "dataset_id", ""),
            (
                "evaluation_artifacts",
                "object_id",
                "evaluation_id",
                "AND EXISTS(SELECT 1 FROM evaluation_runs e "
                "WHERE e.evaluation_id=r.evaluation_id AND e.invalidated) ",
            ),
        ):
            remaining = limit - result["artifact_relations_deleted"]
            if not remaining:
                break
            deleted = connection.execute(
                sql.SQL(
                    "WITH targets AS (SELECT r.ctid FROM {} r WHERE {}=ANY(%s) "
                    + condition
                    + "ORDER BY {},{} LIMIT %s FOR UPDATE OF r SKIP LOCKED) "
                    "DELETE FROM {} r USING targets t WHERE r.ctid=t.ctid"
                ).format(
                    sql.Identifier(table),
                    sql.Identifier(key),
                    sql.Identifier(key),
                    sql.Identifier(order),
                    sql.Identifier(table),
                ),
                (identities, remaining),
            ).rowcount
            result["artifact_relations_deleted"] += deleted
        states = connection.execute(
            "SELECT d.object_id,d.object_key,"
            "EXISTS(SELECT 1 FROM derived_artifact_edges r WHERE r.parent_id=d.object_id) "
            "AS edges,"
            "EXISTS(SELECT 1 FROM derived_artifact_inputs r WHERE r.object_id=d.object_id) "
            "AS inputs,"
            "EXISTS(SELECT 1 FROM evaluation_artifacts r WHERE r.object_id=d.object_id) "
            "AS evaluations,"
            "EXISTS(SELECT 1 FROM evaluation_artifacts r JOIN evaluation_runs e "
            "USING(evaluation_id) WHERE r.object_id=d.object_id AND e.invalidated) "
            "AS removable_evaluations "
            "FROM derived_publications d WHERE object_id=ANY(%s) ORDER BY object_key",
            (identities,),
        ).fetchall()
        complete, retry = [], []
        for row in states:
            if not row["edges"] and not row["inputs"] and not row["evaluations"]:
                complete.append(row["object_id"])
            elif row["edges"] or row["inputs"] or row["removable_evaluations"]:
                retry.append(row["object_key"])
            else:
                result["artifact_gc_blocked_parent_keys"].append(row["object_key"])
        if complete:
            result["artifact_gc_completed_parents"] = connection.execute(
                "UPDATE derived_publications SET relations_purged=true "
                "WHERE object_id=ANY(%s) AND state='deleted' AND NOT relations_purged",
                (complete,),
            ).rowcount
        if retry:
            result.update(artifact_gc_next=(retry[0], True), artifact_gc_end=False)
        elif len(keys) == limit:
            result.update(artifact_gc_next=(keys[-1], False), artifact_gc_end=False)
        return result

    def _finish_retention(
        self, limit: int = 1000, *, gc_after: tuple[str, bool] | None = None
    ) -> dict[str, Any]:
        """Finalize bounded tombstones after their referenced objects were removed."""
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            datasets = connection.execute(
                "WITH ready AS (SELECT d.dataset_id FROM datasets d "
                "WHERE d.state='invalidated' AND d.invalid_reason='retention_expired' "
                "AND NOT EXISTS (SELECT 1 FROM dataset_object_refs r JOIN retention_objects o "
                "ON o.object_key=r.object_key WHERE r.dataset_id=d.dataset_id "
                "AND o.state='pending') "
                "ORDER BY d.dataset_id LIMIT %s FOR UPDATE OF d SKIP LOCKED) "
                "UPDATE datasets d SET state='deleted',manifest=jsonb_build_object("
                "'dataset_id',d.dataset_id,'status','deleted','reason','retention_expired') "
                "FROM ready WHERE ready.dataset_id=d.dataset_id",
                (limit,),
            ).rowcount
            derivations = connection.execute(
                "WITH ready AS (SELECT d.object_id FROM derived_publications d "
                "JOIN retention_objects o USING(object_key) WHERE d.state='invalidated' "
                "AND o.state='deleted' ORDER BY d.object_key LIMIT %s "
                "FOR UPDATE OF d SKIP LOCKED) UPDATE derived_publications d "
                "SET state='deleted',evidence_datasets='[]' FROM ready "
                "WHERE d.object_id=ready.object_id",
                (limit,),
            ).rowcount
            gc = self._gc_artifact_relations(
                connection, limit, after=None if derivations else gc_after
            )
            gc["changed"] = (
                datasets
                + derivations
                + gc["artifact_relations_deleted"]
                + gc["artifact_gc_completed_parents"]
            )
        return gc

    def retention_drain(
        self,
        *,
        max_batches: int = 100,
        max_objects: int = 10_000,
        max_seconds: float = 120,
        max_operations: int = 20_000,
    ) -> dict[str, Any]:
        """Drain resumable retention work without enlarging atomic root/object chunks.

        Deadlines are cooperative between SQL transactions and remote operations;
        an in-flight request retains the configured storage timeout. Failed keys
        are attempted once per call and remain durable for the next invocation.
        """
        _validate_drain_limits(max_batches, max_seconds)
        for value, name, upper in (
            (max_objects, "object", 100_000),
            (max_operations, "operation", 1_000_000),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= upper:
                raise ValueError(f"Maintenance {name} limit must be between 1 and {upper}")
        budget = _DrainBudget(max_seconds, max_operations, monotonic())
        result: dict[str, Any] = {
            "applied": True,
            "batches": 0,
            "finalization_batches": 0,
            "dataset_count": 0,
            "dataset_ids": [],
            "restored_entity_origins": 0,
            "deleted_objects": 0,
            "failed_objects": 0,
            "attempted_objects": 0,
            "artifact_relations_deleted": 0,
            "artifact_gc_completed_parents": 0,
            "artifact_gc_blocked_parent_keys": [],
            "unverified_sources": self._unverified_sources(),
        }
        stop_reason = "no_work"
        try:
            continuation = self.catalog.resume_invalidations(
                # This is a separate bounded frontier, not the root retirement
                # batch count. One scheduled invocation can drain broad graphs.
                max_batches=1000,
                max_seconds=min(max_seconds, 120),
            )
            result["invalidation_progress"] = continuation
            if continuation["pending"]:
                raise _BudgetExhausted("invalidation_pending")
            for _ in range(max_batches):
                budget.check()
                batch = self._retire_expired(budget=budget)
                if not batch["dataset_count"]:
                    break
                result["batches"] += 1
                result["dataset_count"] += batch["dataset_count"]
                result["dataset_ids"].extend(
                    batch["dataset_ids"][: MAINTENANCE_PREVIEW_LIMIT - len(result["dataset_ids"])]
                )
                result["restored_entity_origins"] += batch["restored_entity_origins"]
            else:
                stop_reason = "batch_limit"
        except _RetentionClosureTooLarge:
            stop_reason = "retention_closure_limit"
        except _BudgetExhausted as error:
            stop_reason = str(error)

        cursor = None
        if stop_reason not in {"deadline", "retention_closure_limit", "invalidation_pending"}:
            while result["attempted_objects"] < max_objects:
                try:
                    budget.check()
                except _BudgetExhausted as error:
                    stop_reason = str(error)
                    break
                batch = self._delete_pending_objects(
                    limit=min(1000, max_objects - result["attempted_objects"]),
                    after=cursor,
                    budget=budget,
                )
                result["deleted_objects"] += batch["deleted"]
                result["failed_objects"] += batch["failed"]
                result["attempted_objects"] += batch["attempted"]
                if batch["stop_reason"]:
                    stop_reason = batch["stop_reason"]
                    break
                if not batch["cursor"]:
                    break
                cursor = batch["cursor"]
            else:
                stop_reason = "object_limit"

        gc_after = None
        try:
            for _ in range(max_batches):
                budget.check()
                finalized = self._finish_retention(gc_after=gc_after)
                result["artifact_relations_deleted"] += finalized["artifact_relations_deleted"]
                result["artifact_gc_completed_parents"] += finalized[
                    "artifact_gc_completed_parents"
                ]
                result["artifact_gc_blocked_parent_keys"] = sorted(
                    set(result["artifact_gc_blocked_parent_keys"])
                    | set(finalized["artifact_gc_blocked_parent_keys"])
                )[:20]
                gc_after = finalized["artifact_gc_next"]
                if not finalized["changed"] and finalized["artifact_gc_end"]:
                    break
                result["finalization_batches"] += 1
            else:
                if stop_reason == "no_work":
                    stop_reason = "batch_limit"
        except _BudgetExhausted as error:
            stop_reason = str(error)
        result.update(self.retention_backlog())
        outstanding = (
            result["expired_root_count"]
            or result["pending_source_invalidations"]
            or result["pending_objects"]
            or result["pending_finalization_count"]
            or result["pending_artifact_gc_count"]
        )
        if not outstanding:
            stop_reason = "no_work"
        elif result["artifact_gc_blocked_parent_keys"]:
            stop_reason = "artifact_gc_live_evaluation"
        elif stop_reason == "no_work":
            stop_reason = "remote_errors" if result["failed_objects"] else "pending_work"
        result.update(
            status="complete"
            if not outstanding
            else "blocked"
            if stop_reason in {"retention_closure_limit", "artifact_gc_live_evaluation"}
            else "partial",
            stop_reason=stop_reason,
            preview_truncated=result["dataset_count"] > len(result["dataset_ids"]),
            operations=budget.operations,
            elapsed_seconds=monotonic() - budget.started,
        )
        return result

    def compaction_backlog(
        self, synthetic: bool = False, *, committed_before: datetime | None = None
    ) -> dict[str, Any]:
        row_limit = min(COMPACTION_MAX_ROWS, self.pipeline.settings.analysis_max_rows_per_batch)
        cutoff_sql = (
            "AND COALESCE(committed_at,created_at)<=%s " if committed_before is not None else ""
        )
        cutoff_params = (committed_before,) if committed_before is not None else ()
        backlog = self.catalog.one(
            "WITH ranked AS (SELECT source,transform_version,manifest->>'partition_buckets' "
            "AS buckets,row_count,created_at,row_number() OVER(PARTITION BY source,"
            "transform_version,manifest->>'partition_buckets' ORDER BY row_count,created_at,"
            "dataset_id) AS position FROM datasets WHERE state='committed' "
            "AND compacted_into IS NULL AND synthetic=%s AND row_count>0 "
            "AND row_count<%s AND quarantine_count=0 "
            + cutoff_sql
            + "), groups AS (SELECT source,transform_version,buckets,count(*) AS candidates,"
            "min(created_at) AS oldest,count(*) FILTER(WHERE position<=2) AS first_count,"
            "sum(row_count) FILTER(WHERE position<=2) AS first_rows FROM ranked "
            "GROUP BY source,transform_version,buckets) SELECT COALESCE(sum(candidates),0)::bigint "
            "AS candidate_dataset_count,count(*) FILTER(WHERE first_count=2 AND first_rows<=%s) "
            "AS mergeable_group_count,GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-"
            "min(oldest)))::float8 AS oldest_candidate_age_seconds FROM groups",
            (synthetic, row_limit, *cutoff_params, row_limit),
        )
        return (
            dict(backlog)
            if backlog
            else {
                "candidate_dataset_count": 0,
                "mergeable_group_count": 0,
                "oldest_candidate_age_seconds": None,
            }
        )

    def compact_drain(
        self,
        synthetic: bool = False,
        max_datasets: int = 100,
        *,
        max_batches: int = 24,
        max_seconds: float = 120,
    ) -> dict[str, Any]:
        """Compact groups committed before this pass, excluding newly recovered outputs."""
        _validate_drain_limits(max_batches, max_seconds)
        if not 2 <= max_datasets <= 1000:
            raise ValueError("Compaction dataset limit must be between 2 and 1000")
        started = monotonic()
        clock = self.catalog.one("SELECT clock_timestamp() AS cutoff")
        if not clock:
            raise RuntimeError("Database did not provide compaction cutoff")
        cutoff = clock["cutoff"]
        result: dict[str, Any] = {
            "batches": 0,
            "rows": 0,
            "dataset_ids": [],
            "parent_dataset_count": 0,
            "normalized_objects_before": 0,
            "normalized_objects_after": 0,
        }
        reason = "no_work"
        for _ in range(max_batches):
            if monotonic() - started >= max_seconds:
                reason = "deadline"
                break
            batch = self.compact(
                synthetic=synthetic, max_datasets=max_datasets, committed_before=cutoff
            )
            if batch["status"] == "no_work":
                break
            result["batches"] += 1
            result["rows"] += batch["rows"]
            result["parent_dataset_count"] += len(batch["parent_dataset_ids"])
            result["dataset_ids"].append(batch["dataset_id"])
            result["normalized_objects_before"] += batch.get("normalized_objects_before", 0)
            result["normalized_objects_after"] += batch.get("normalized_objects_after", 0)
        else:
            reason = "batch_limit"
        remaining = self.compaction_backlog(synthetic, committed_before=cutoff)
        result.update(
            status="partial" if remaining["mergeable_group_count"] else "complete",
            stop_reason=("remaining_work" if reason == "no_work" else reason)
            if remaining["mergeable_group_count"]
            else "no_work",
            pass_backlog=remaining,
            active_backlog=self.compaction_backlog(synthetic),
            elapsed_seconds=monotonic() - started,
        )
        return result

    @staticmethod
    def _repair_entity_origins(connection: Connection[dict[str, Any]]) -> int:
        from .retirement import repair_entity_origins

        return repair_entity_origins(connection)

    def _delete_pending_objects(
        self,
        *,
        limit: int = 1000,
        after: tuple[datetime, str] | None = None,
        budget: _DrainBudget | None = None,
    ) -> dict[str, Any]:
        from botocore.exceptions import BotoCoreError, ClientError

        result: dict[str, Any] = {
            "deleted": 0,
            "failed": 0,
            "attempted": 0,
            "cursor": None,
            "stop_reason": None,
        }
        cursor_sql = "AND (created_at,object_key)>(%s,%s) " if after else ""
        cursor_params = tuple(after) if after else ()
        pending = self.catalog.all(
            "SELECT object_key,created_at,COALESCE((to_jsonb(retention_objects)->>'local_only')"
            "::boolean,false) AS local_only FROM retention_objects WHERE state='pending' "
            + cursor_sql
            + "ORDER BY created_at,object_key LIMIT %s",
            (*cursor_params, limit),
        )
        if not pending:
            return result
        remote_keys = [item["object_key"] for item in pending if not item["local_only"]]
        versioned = versioning_failed = False
        if remote_keys:
            try:
                if budget:
                    budget.request()
                self.history.metrics["get_requests"] += 1
                versioning = self.history.client.get_bucket_versioning(Bucket=self.history.bucket)
                versioned = versioning.get("Status") in {"Enabled", "Suspended"}
            except _BudgetExhausted as error:
                result["stop_reason"] = str(error)
                return result
            except (BotoCoreError, ClientError):
                self.catalog.execute(
                    "UPDATE retention_objects SET attempts=attempts+1,"
                    "last_error='versioning_verification_failed' WHERE object_key=ANY(%s)",
                    (remote_keys,),
                )
                versioning_failed = True
        for item in pending:
            key = item["object_key"]
            if versioning_failed and not item["local_only"]:
                result["failed"] += 1
                result["attempted"] += 1
                result["cursor"] = (item["created_at"], key)
                continue
            try:
                if budget:
                    budget.check()
                with cache_object_lock(self.history, key) as discard:
                    if not item["local_only"]:
                        if versioned:
                            self._delete_object_versions(key, budget=budget)
                        else:
                            if budget:
                                budget.request()
                            self.history.metrics["delete_requests"] = (
                                self.history.metrics.get("delete_requests", 0) + 1
                            )
                            self.history.client.delete_object(Bucket=self.history.bucket, Key=key)
                    discard()
            except _BudgetExhausted as error:
                result["stop_reason"] = str(error)
                break
            except (BotoCoreError, ClientError, OSError):
                self.catalog.execute(
                    "UPDATE retention_objects SET attempts=attempts+1,"
                    "last_error='object_delete_failed' "
                    "WHERE object_key=%s",
                    (key,),
                )
                result["failed"] += 1
            else:
                self.catalog.execute(
                    "UPDATE retention_objects SET state='deleted',attempts=attempts+1,"
                    "deleted_at=clock_timestamp(),last_error=NULL WHERE object_key=%s",
                    (key,),
                )
                result["deleted"] += 1
            result["attempted"] += 1
            result["cursor"] = (item["created_at"], key)
        return result

    def _delete_object_versions(self, key: str, *, budget: _DrainBudget | None = None) -> None:
        from botocore.exceptions import ClientError

        for _batch in range(100):
            if budget:
                budget.request()
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
            if budget:
                budget.request()
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

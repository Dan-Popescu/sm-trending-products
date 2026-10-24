"""Bounded durable source-removal progress behind the catalog's namespace fence.

Evidence lifecycles remain authoritative. The work relation is disposable progress,
not an alternate evidence index. No namespace gate expires or clears on error.
"""

from __future__ import annotations

from time import monotonic, sleep
from typing import TYPE_CHECKING, Any

from psycopg.errors import DeadlockDetected, LockNotAvailable, QueryCanceled
from psycopg.types.json import Jsonb

from .contracts import digest

if TYPE_CHECKING:
    from .catalog import Catalog

WORK_LIMIT = 1000

# Each query yields immediate dependents only. The stable keyset stays on one
# parent until its fanout is exhausted; even one wide node cannot fill Python RAM.
SUCCESSORS = {
    "dataset": (
        (
            "dataset",
            "SELECT child_dataset_id AS target_key FROM dataset_lineage "
            "WHERE parent_dataset_id=%(key)s AND child_dataset_id>%(after)s "
            "ORDER BY child_dataset_id",
        ),
        (
            "shared_object",
            "SELECT object_key AS target_key FROM dataset_object_refs "
            "WHERE dataset_id=%(key)s AND object_key>%(after)s ORDER BY object_key",
        ),
        (
            "artifact",
            "SELECT i.object_id AS target_key FROM derived_artifact_inputs i "
            "JOIN derived_publications p ON p.object_id=i.object_id "
            "WHERE i.dataset_id=%(key)s AND p.state<>'deleted' "
            "AND p.object_id>COALESCE(NULLIF(%(after)s,''),'0')::bigint "
            "AND i.object_id>COALESCE(NULLIF(%(after)s,''),'0')::bigint ORDER BY i.object_id",
        ),
        (
            "artifact",
            "SELECT object_id AS target_key FROM derived_publications "
            "WHERE evidence_datasets ? %(key)s AND state<>'deleted' "
            "AND object_id>COALESCE(NULLIF(%(after)s,''),'0')::bigint ORDER BY object_id",
        ),
        (
            "assessment",
            "SELECT run_id AS target_key FROM assessment_runs "
            "WHERE evidence_datasets ? %(key)s AND run_id>%(after)s ORDER BY run_id",
        ),
        (
            "evaluation",
            "SELECT evaluation_id AS target_key FROM evaluation_dependencies "
            "WHERE dataset_id=%(key)s AND evaluation_id>%(after)s ORDER BY evaluation_id",
        ),
        (
            "entity",
            "SELECT entity_key AS target_key FROM entities WHERE synthetic=%(synthetic)s "
            "AND origin->>'dataset_id'=%(key)s AND %(physical)s "
            "AND entity_key>%(after)s ORDER BY entity_key",
        ),
        (
            "object",
            "SELECT object_key AS target_key FROM dataset_object_refs "
            "WHERE dataset_id=%(key)s AND %(physical)s AND object_key>%(after)s "
            "ORDER BY object_key",
        ),
        (
            "restore",
            "SELECT dataset_id AS target_key FROM datasets WHERE compacted_into=%(key)s "
            "AND state='committed' AND dataset_id>%(after)s ORDER BY dataset_id",
        ),
    ),
    "shared_object": (
        (
            "dataset",
            "SELECT dataset_id AS target_key FROM dataset_object_refs "
            "WHERE object_key=%(key)s AND dataset_id>%(after)s ORDER BY dataset_id",
        ),
    ),
    "artifact": (
        (
            "artifact",
            "SELECT e.parent_id AS target_key FROM derived_artifact_edges e "
            "JOIN derived_publications p ON p.object_id=e.parent_id "
            "WHERE e.child_id=%(key)s::bigint AND p.state<>'deleted' "
            "AND p.object_id>COALESCE(NULLIF(%(after)s,''),'0')::bigint "
            "AND e.parent_id>COALESCE(NULLIF(%(after)s,''),'0')::bigint ORDER BY e.parent_id",
        ),
        (
            "assessment",
            "SELECT r.run_id AS target_key FROM assessment_runs r "
            "JOIN derived_publications p ON p.object_key=r.object_key "
            "WHERE p.object_id=%(key)s::bigint AND r.run_id>%(after)s ORDER BY r.run_id",
        ),
        (
            "evaluation",
            "SELECT evaluation_id AS target_key FROM evaluation_artifacts "
            "WHERE object_id=%(key)s::bigint AND evaluation_id>%(after)s ORDER BY evaluation_id",
        ),
        (
            "object",
            "SELECT object_key AS target_key FROM derived_publications "
            "WHERE object_id=%(key)s::bigint AND %(physical)s AND object_key>%(after)s "
            "ORDER BY object_key",
        ),
    ),
    "assessment": (
        (
            "alert",
            "SELECT alert_id AS target_key FROM alerts "
            "WHERE run_id=%(key)s AND alert_id>%(after)s ORDER BY alert_id",
        ),
        (
            "current",
            "SELECT product_id AS target_key FROM current_assessments "
            "WHERE run_id=%(key)s AND product_id>%(after)s ORDER BY product_id",
        ),
        (
            "object",
            "SELECT object_key AS target_key FROM assessment_runs "
            "WHERE run_id=%(key)s AND %(physical)s AND object_key>%(after)s ORDER BY object_key",
        ),
    ),
    "evaluation": (
        (
            "evaluation",
            "SELECT evaluation_id AS target_key FROM evaluation_runs "
            "WHERE parent_id=%(key)s AND evaluation_id>%(after)s ORDER BY evaluation_id",
        ),
    ),
}


class SourceInvalidation:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def begin(self, dataset_id: str, reason: str) -> str:
        identifier = digest({"dataset": dataset_id, "reason": reason})
        with self.catalog.transaction() as connection:
            connection.execute("SET LOCAL statement_timeout='2s'")
            connection.execute("SET LOCAL lock_timeout='250ms'")
            try:
                self.catalog.guard_retention(connection, exclusive=True)
            except (LockNotAvailable, QueryCanceled) as error:
                from .catalog import InvalidationPending

                raise InvalidationPending(
                    "Removal registration is busy; retry the request"
                ) from error
            row = connection.execute(
                "SELECT synthetic,state FROM datasets WHERE dataset_id=%s", (dataset_id,)
            ).fetchone()
            if row is None:
                raise ValueError("Source invalidation requires an existing dataset")
            pending = connection.execute(
                "SELECT invalidation_id FROM source_invalidations "
                "WHERE synthetic=%s AND phase<>'complete'",
                (row["synthetic"],),
            ).fetchone()
            if pending:
                if pending["invalidation_id"] != identifier:
                    from .catalog import InvalidationPending

                    raise InvalidationPending(
                        "Another source invalidation is already pending in this namespace"
                    )
                return identifier
            connection.execute(
                "INSERT INTO source_invalidations(invalidation_id,dataset_id,synthetic,reason) "
                "VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (identifier, dataset_id, row["synthetic"], reason),
            )
            connection.execute(
                "INSERT INTO source_invalidation_work(invalidation_id,kind,target_key) "
                "SELECT %s,'dataset',%s WHERE EXISTS (SELECT 1 FROM source_invalidations "
                "WHERE invalidation_id=%s AND phase<>'complete') ON CONFLICT DO NOTHING",
                (identifier, dataset_id, identifier),
            )
        return identifier

    def status(self) -> list[dict[str, Any]]:
        return self.catalog.all(
            "SELECT invalidation_id,dataset_id,synthetic,reason,phase,"
            "discovered_count,applied_count,"
            "created_at,updated_at,completed_at FROM source_invalidations "
            "WHERE phase<>'complete' ORDER BY created_at,invalidation_id LIMIT 100"
        )

    def resume(
        self, *, max_batches: int = 10, max_seconds: float = 1, synthetic: bool | None = None
    ) -> dict[str, Any]:
        if (
            isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 1000
        ):
            raise ValueError("Invalidation batch limit must be between 1 and 1000")
        if isinstance(max_seconds, bool) or not 0 < max_seconds <= 120:
            raise ValueError("Invalidation deadline must be positive and at most 120 seconds")
        started = monotonic()
        batches = processed = 0
        stop_reason = "no_work"
        while batches < max_batches and monotonic() - started < max_seconds:
            try:
                batch = self._batch(synthetic, started + max_seconds)
            except (DeadlockDetected, LockNotAvailable, QueryCanceled):
                stop_reason = "database_busy"
                break
            if batch is None:
                break
            if batch == -1:
                stop_reason = "database_busy"
                sleep(min(0.025, max(0, started + max_seconds - monotonic())))
                continue
            batches += 1
            processed += batch
        else:
            stop_reason = "batch_limit" if batches >= max_batches else "deadline"
        remaining = self.status()
        if synthetic is not None:
            remaining = [job for job in remaining if job["synthetic"] == synthetic]
        return {
            "status": "partial" if remaining else "complete",
            "stop_reason": stop_reason if remaining else "no_work",
            "batches": batches,
            "processed": processed,
            "pending": remaining,
            "elapsed_seconds": monotonic() - started,
        }

    def _batch(self, synthetic: bool | None, deadline: float) -> int | None:
        with self.catalog.transaction() as connection:
            lock = connection.execute(
                "SELECT pg_try_advisory_xact_lock(492371205) AS acquired"
            ).fetchone()
            if not lock or not lock["acquired"]:
                return -1
            # Respect the caller's time budget even when a single SQL branch
            # encounters contention or a newly large adjacency list.
            remaining_ms = max(1, min(5000, int((deadline - monotonic()) * 1000)))
            connection.execute(
                "SELECT set_config('statement_timeout',%s,true)", (str(remaining_ms),)
            )
            connection.execute("SET LOCAL lock_timeout='250ms'")
            job = connection.execute(
                "SELECT * FROM source_invalidations WHERE phase<>'complete' "
                "AND (%s::boolean IS NULL OR synthetic=%s) "
                "ORDER BY created_at,invalidation_id FOR UPDATE SKIP LOCKED LIMIT 1",
                (synthetic, synthetic),
            ).fetchone()
            if job is None:
                return None
            connection.execute(
                "SELECT set_config('pipeline.invalidation_owner',%s,true)",
                (job["invalidation_id"],),
            )
            if job["phase"] == "discover":
                return self._discover(connection, job, deadline)
            return self._apply(connection, job)

    def _discover(self, connection, job, deadline: float) -> int:
        # Collapse terminal frontier rows in SQL. A 50,000-leaf fanout must not
        # cost four Python/SQL round trips per leaf after its bounded keyset walk.
        physical = job["reason"] == "retention_expired"
        terminal = connection.execute(
            "WITH candidates AS (SELECT kind,target_key FROM source_invalidation_work "
            "WHERE invalidation_id=%s AND NOT discovered "
            "AND kind IN ('alert','current','entity','object','restore') LIMIT %s) "
            "UPDATE source_invalidation_work w SET discovered=true FROM candidates c "
            "WHERE w.invalidation_id=%s AND w.kind=c.kind AND w.target_key=c.target_key",
            (job["invalidation_id"], WORK_LIMIT, job["invalidation_id"]),
        ).rowcount
        used = terminal
        room = (WORK_LIMIT - used) // (2 if physical else 1)
        if room:
            leaves = connection.execute(
                "SELECT w.target_key,p.object_key FROM source_invalidation_work w "
                "JOIN derived_publications p ON p.object_id=CASE WHEN w.kind='artifact' "
                "THEN w.target_key::bigint END "
                "WHERE w.invalidation_id=%s AND w.kind='artifact' AND NOT w.discovered "
                "AND NOT EXISTS(SELECT 1 FROM derived_artifact_edges e "
                "WHERE e.child_id=p.object_id) "
                "AND NOT EXISTS(SELECT 1 FROM assessment_runs a WHERE a.object_key=p.object_key) "
                "AND NOT EXISTS(SELECT 1 FROM evaluation_artifacts e "
                "WHERE e.object_id=p.object_id) "
                "ORDER BY w.target_key LIMIT %s",
                (job["invalidation_id"], room),
            ).fetchall()
            if leaves:
                if physical:
                    with connection.pipeline():
                        for leaf in leaves:
                            connection.execute(
                                "INSERT INTO source_invalidation_work"
                                "(invalidation_id,kind,target_key) VALUES(%s,'object',%s) "
                                "ON CONFLICT DO NOTHING",
                                (job["invalidation_id"], leaf["object_key"]),
                            )
                connection.execute(
                    "UPDATE source_invalidation_work SET discovered=true WHERE invalidation_id=%s "
                    "AND kind='artifact' AND target_key=ANY(%s)",
                    (job["invalidation_id"], [leaf["target_key"] for leaf in leaves]),
                )
                used += len(leaves) * (2 if physical else 1)
                terminal += len(leaves)
        if terminal:
            connection.execute(
                "UPDATE source_invalidations SET discovered_count=discovered_count+%s,"
                "updated_at=clock_timestamp() WHERE invalidation_id=%s",
                (terminal, job["invalidation_id"]),
            )
        # Leave room to persist the cursor before the transaction deadline.
        seconds = (deadline - monotonic()) * 0.8
        if used < WORK_LIMIT and seconds > 0:
            # Fixed statements live in one place. Only their values are bound by
            # SQL; no source value is interpolated into the statement text.
            adjacency = {
                kind: [
                    [
                        target,
                        query.replace("%(key)s", "$1")
                        .replace("%(after)s", "$2")
                        .replace("%(synthetic)s", "$3")
                        .replace("%(physical)s", "$4"),
                    ]
                    for target, query in branches
                ]
                for kind, branches in SUCCESSORS.items()
            }
            advanced = connection.execute(
                "SELECT advance_source_invalidation_frontier(%s,%s,%s,%s) AS used",
                (job["invalidation_id"], Jsonb(adjacency), WORK_LIMIT - used, seconds),
            ).fetchone()
            used += advanced["used"] if advanced else 0
        return used

    def _apply(self, connection, job) -> int:
        rows = connection.execute(
            "SELECT kind,target_key FROM source_invalidation_work WHERE invalidation_id=%s "
            "AND NOT applied AND kind=(SELECT kind FROM source_invalidation_work "
            "WHERE invalidation_id=%s AND NOT applied ORDER BY kind,target_key LIMIT 1) "
            "ORDER BY target_key LIMIT %s FOR UPDATE",
            (job["invalidation_id"], job["invalidation_id"], WORK_LIMIT),
        ).fetchall()
        if not rows:
            connection.execute(
                "UPDATE source_invalidations SET phase='complete',completed_at=clock_timestamp(),"
                "updated_at=clock_timestamp() WHERE invalidation_id=%s",
                (job["invalidation_id"],),
            )
            return 0
        grouped: dict[str, list[str]] = {}
        for row in rows:
            grouped.setdefault(row["kind"], []).append(row["target_key"])
        physical = job["reason"] == "retention_expired"
        for kind, keys in grouped.items():
            if kind == "dataset":
                connection.execute(
                    "UPDATE datasets SET state='invalidated',invalid_reason=%s "
                    "WHERE dataset_id=ANY(%s) AND state<>'deleted'",
                    (job["reason"], keys),
                )
                if physical:
                    connection.execute(
                        "UPDATE datasets SET manifest=manifest-'quarantine'-'task_request' "
                        "WHERE dataset_id=ANY(%s)",
                        (keys,),
                    )
            elif kind == "artifact":
                connection.execute(
                    "UPDATE derived_publications SET state='invalidated' "
                    "WHERE object_id=ANY(%s) AND state<>'deleted'",
                    ([int(key) for key in keys],),
                )
            elif kind == "assessment":
                connection.execute(
                    "UPDATE assessment_runs SET invalidated=true WHERE run_id=ANY(%s)", (keys,)
                )
            elif kind == "evaluation":
                if physical:
                    connection.execute(
                        "UPDATE evaluation_runs SET invalidated=true,payload=%s "
                        "WHERE evaluation_id=ANY(%s)",
                        (Jsonb({"status": "invalidated", "reason": job["reason"]}), keys),
                    )
                else:
                    connection.execute(
                        "UPDATE evaluation_runs SET invalidated=true WHERE evaluation_id=ANY(%s)",
                        (keys,),
                    )
            elif kind == "alert":
                if physical:
                    connection.execute(
                        "UPDATE alerts SET invalidated=true,payload=%s WHERE alert_id=ANY(%s)",
                        (Jsonb({"status": "invalidated", "reason": job["reason"]}), keys),
                    )
                else:
                    connection.execute(
                        "UPDATE alerts SET invalidated=true WHERE alert_id=ANY(%s)", (keys,)
                    )
            elif kind == "current":
                if physical:
                    connection.execute(
                        "DELETE FROM current_assessments WHERE product_id=ANY(%s)", (keys,)
                    )
            elif kind == "restore":
                connection.execute(
                    "UPDATE datasets SET compacted_into=NULL WHERE dataset_id=ANY(%s) "
                    "AND state='committed'",
                    (keys,),
                )
            elif kind == "object":
                with connection.pipeline():
                    for key in keys:
                        connection.execute(
                            "INSERT INTO retention_objects(object_key,reason) "
                            "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                            (key, job["reason"]),
                        )
            elif kind == "entity":
                from .retirement import repair_entity_origins

                connection.execute(
                    "CREATE TEMP TABLE retention_targets ON COMMIT DROP AS "
                    "SELECT DISTINCT origin->>'dataset_id' AS dataset_id FROM entities "
                    "WHERE synthetic=%s AND entity_key=ANY(%s)",
                    (job["synthetic"], keys),
                )
                repair_entity_origins(connection, keys, job["synthetic"])
        with connection.pipeline():
            for row in rows:
                connection.execute(
                    "DELETE FROM source_invalidation_work "
                    "WHERE invalidation_id=%s AND kind=%s AND target_key=%s",
                    (job["invalidation_id"], row["kind"], row["target_key"]),
                )
        connection.execute(
            "UPDATE source_invalidations SET applied_count=applied_count+%s,"
            "updated_at=clock_timestamp() WHERE invalidation_id=%s",
            (len(rows), job["invalidation_id"]),
        )
        return len(rows)

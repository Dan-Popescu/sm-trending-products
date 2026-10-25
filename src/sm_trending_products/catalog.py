"""PostgreSQL operational catalog, fencing, and immutable evidence pointers."""

from __future__ import annotations

import hashlib
import math
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from importlib.resources import files
from typing import Any, cast

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .contracts import Observation, canonical, digest, utc
from .worker_control import WorkerControl

# Shared protocol with the literal in SQL migration 003_maintenance.sql triggers.
# Changing the key requires coordinating both Python writers and database guards.
RETENTION_GUARD_KEY = 492371205
ENTITY_WRITE_BATCH_SIZE = 500


class InvalidationPending(ValueError):
    """Evidence in this namespace is temporarily fenced during source removal."""


class InvalidationTooLarge(ValueError):
    """An atomic closure requires bounded durable continuation."""


class LeaseLost(RuntimeError):
    """A worker no longer owns the right to commit this task."""


class Catalog:
    def __init__(self, database_url: str, *, max_connections: int = 4):
        self.pool = cast(
            ConnectionPool[psycopg.Connection[dict[str, Any]]],
            ConnectionPool(
                database_url,
                min_size=1,
                max_size=max_connections,
                kwargs={
                    "row_factory": dict_row,
                    "connect_timeout": 10,
                    "prepare_threshold": None
                    if os.environ.get("PG_PREPARE_THRESHOLD") == "none"
                    else 5,
                },
                open=False,
                timeout=15,
            ),
        )
        self.pool.open(wait=True)

    def close(self) -> None:
        self.pool.close()

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection[dict[str, Any]]]:
        try:
            with self.pool.connection() as connection, connection.transaction():
                connection.execute("SET LOCAL statement_timeout='30s'")
                connection.execute("SET LOCAL lock_timeout='10s'")
                yield connection
        except psycopg.errors.ObjectNotInPrerequisiteState as error:
            # SQL triggers are authoritative even for a late or empty-input
            # publication. Expose the same retry contract after its rollback.
            if (error.diag.message_primary or "").startswith("Evidence invalidation pending;"):
                raise InvalidationPending(
                    "Evidence invalidation pending; retry after bounded maintenance"
                ) from None
            raise

    def initialize(self) -> None:
        with self.transaction() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(492371204)")
            exists = connection.execute(
                "SELECT to_regclass('schema_migrations') AS table_name"
            ).fetchone()
            applied = (
                {
                    row["version"]
                    for row in connection.execute(
                        "SELECT version FROM schema_migrations"
                    ).fetchall()
                }
                if exists and exists["table_name"]
                else set()
            )
            for path in sorted(
                files("sm_trending_products").joinpath("sql").iterdir(), key=lambda item: item.name
            ):
                if path.name.endswith(".sql"):
                    version = int(path.name.split("_", 1)[0])
                    if version not in applied:
                        connection.execute(path.read_text())

    def all(self, query: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            return list(connection.execute(query, params).fetchall())

    def stream(self, query: str, params: Sequence[Any] = ()) -> Iterator[dict[str, Any]]:
        """Read a large inventory in bounded batches without a long source snapshot.

        A held cursor spools the result in PostgreSQL at commit (using database
        temporary storage if necessary). Remote object checks then retain only
        256 rows in Python and no source-table MVCC snapshot.
        """
        with self.pool.connection() as connection:
            with connection.cursor(name="catalog_inventory", withhold=True) as cursor:
                cursor.itersize = 256
                with connection.transaction():
                    connection.execute("SET LOCAL statement_timeout='30s'")
                    connection.execute("SET LOCAL lock_timeout='10s'")
                    cursor.execute(query, params)
                yield from cursor

    def one(self, query: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        with self.transaction() as connection:
            return connection.execute(query, params).fetchone()

    @staticmethod
    def guard_namespace(connection, synthetic: bool | None) -> None:
        """The SQL guard and publication triggers share the authoritative fence."""
        try:
            connection.execute("SELECT assert_evidence_namespace_available(%s)", (synthetic,))
        except psycopg.errors.ObjectNotInPrerequisiteState as error:
            if "Evidence invalidation pending" not in str(error):
                raise
            raise InvalidationPending(
                "Evidence invalidation pending; retry after bounded maintenance"
            ) from None

    @contextmanager
    def evidence_transaction(
        self, synthetic: bool | None
    ) -> Iterator[psycopg.Connection[dict[str, Any]]]:
        # Attempt one bounded continuation outside the failed transaction. Never
        # wait indefinitely or clear a durable gate merely because time elapsed.
        guarded = False
        try:
            with self.transaction() as connection:
                self.guard_namespace(connection, synthetic)
                guarded = True
                yield connection
            return
        except InvalidationPending:
            if guarded:
                raise
        self.resume_invalidations(max_batches=1, max_seconds=0.1, synthetic=synthetic)
        with self.transaction() as connection:
            self.guard_namespace(connection, synthetic)
            yield connection

    def evidence_all(
        self, query: str, params: Sequence[Any] = (), *, synthetic: bool | None
    ) -> list[dict[str, Any]]:
        with self.evidence_transaction(synthetic) as connection:
            return list(connection.execute(query, params).fetchall())

    def evidence_one(
        self, query: str, params: Sequence[Any] = (), *, synthetic: bool | None
    ) -> dict[str, Any] | None:
        with self.evidence_transaction(synthetic) as connection:
            return connection.execute(query, params).fetchone()

    def pending_invalidations(self) -> list[dict[str, Any]]:
        from .invalidation import SourceInvalidation

        return SourceInvalidation(self).status()

    def resume_invalidations(
        self, *, max_batches: int = 10, max_seconds: float = 1, synthetic: bool | None = None
    ) -> dict[str, Any]:
        from .invalidation import SourceInvalidation

        return SourceInvalidation(self).resume(
            max_batches=max_batches, max_seconds=max_seconds, synthetic=synthetic
        )

    def execute(self, query: str, params: Sequence[Any] = ()) -> None:
        with self.transaction() as connection:
            connection.execute(query, params)

    def replay_origin(self, dataset_id: str) -> dict:
        """Resolve only committed replay aliases within one source/workspace."""
        with self.transaction() as connection:
            self.guard_retention(connection)
            visited: set[str] = set()
            scope = None
            for _ in range(100):
                if dataset_id in visited:
                    raise ValueError("Cyclic replay lineage")
                visited.add(dataset_id)
                row = connection.execute(
                    "SELECT * FROM datasets WHERE dataset_id=%s", (dataset_id,)
                ).fetchone()
                if not row or row["state"] != "committed":
                    raise ValueError("Replay parent is unavailable")
                self.guard_namespace(connection, row["synthetic"])
                current_scope = (row["source"], row["synthetic"])
                scope = current_scope if scope is None else scope
                if current_scope != scope:
                    raise ValueError("Replay lineage crosses source or workspace")
                if row["supersedes"] is None:
                    return row
                dataset_id = row["supersedes"]
            raise ValueError("Replay lineage exceeds the supported depth")

    def replay_available_at(
        self, replay_dataset_id: str, source_dataset_id: str, not_before: datetime
    ) -> datetime:
        """Freeze a canonical normalization clock without a long-held SQL lock."""
        with self.transaction() as connection:
            self.guard_inputs(connection, [source_dataset_id])
            connection.execute(
                "INSERT INTO replay_availability(replay_dataset_id,source_dataset_id,available_at) "
                "VALUES(%s,%s,GREATEST(clock_timestamp(),%s)) ON CONFLICT DO NOTHING",
                (replay_dataset_id, source_dataset_id, not_before),
            )
            row = connection.execute(
                "SELECT * FROM replay_availability WHERE replay_dataset_id=%s", (replay_dataset_id,)
            ).fetchone()
            if not row or row["source_dataset_id"] != source_dataset_id:
                raise ValueError("Replay identity collision")
            return row["available_at"]

    @staticmethod
    def guard_retention(
        connection: psycopg.Connection[dict[str, Any]], *, exclusive: bool = False
    ) -> None:
        """Fence evidence access/publication against retention before taking row locks."""
        statement = (
            "SELECT pg_advisory_xact_lock(%s)"
            if exclusive
            else "SELECT pg_advisory_xact_lock_shared(%s)"
        )
        connection.execute(statement, (RETENTION_GUARD_KEY,))

    def enqueue(
        self,
        source: str,
        account_id: str,
        operation: str,
        payload: dict,
        synthetic: bool,
        *,
        due_at: datetime,
        priority: float = 0,
        idempotency_key: str | None = None,
    ) -> str:
        task_id = digest(
            {
                "source": source,
                "account": account_id,
                "operation": operation,
                "synthetic": synthetic,
                "key": idempotency_key or payload,
            }
        )
        self.execute(
            "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic,"
            "due_at,priority) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (task_id, source, account_id, operation, Jsonb(payload), synthetic, due_at, priority),
        )
        return task_id

    def claim(
        self,
        worker_id: str,
        lease_seconds: int,
        synthetic: bool,
        max_attempts: int,
        *,
        credential_keys: dict[str, str] | None = None,
        worker_generation: int | None = None,
    ) -> dict[str, Any] | None:
        with self.transaction() as connection:
            if not WorkerControl.claim_admitted(
                connection, worker_id, synthetic, worker_generation
            ):
                return None
            self.guard_namespace(connection, synthetic)
            connection.execute(
                "WITH exhausted AS (SELECT task_id FROM collection_tasks "
                "WHERE synthetic=%s AND attempts>=%s AND "
                "(state='pending' OR (state='leased' AND lease_until<clock_timestamp())) "
                "FOR UPDATE SKIP LOCKED) "
                "UPDATE collection_tasks t SET state='quarantined',last_error='attempt_limit',"
                "lease_owner=NULL,lease_until=NULL FROM exhausted e WHERE t.task_id=e.task_id",
                (synthetic, max_attempts),
            )
            return connection.execute(
                "WITH next_task AS (SELECT t.task_id FROM collection_tasks t WHERE synthetic=%s "
                "AND attempts<%s AND due_at<=clock_timestamp() AND "
                "(state='pending' OR (state='leased' AND lease_until<clock_timestamp())) "
                "AND request_budget_ready(t.source,t.account_id,"
                "CASE WHEN t.synthetic THEN NULL ELSE %s::jsonb->>t.source END) "
                "ORDER BY priority DESC,due_at,t.task_id FOR UPDATE OF t SKIP LOCKED LIMIT 1) "
                "UPDATE collection_tasks t SET state='leased',lease_owner=%s,"
                "lease_version=t.lease_version+1,lease_until=clock_timestamp()+%s*interval '1 "
                "second',"
                "attempts=t.attempts+1 FROM next_task n WHERE t.task_id=n.task_id RETURNING t.*",
                (synthetic, max_attempts, Jsonb(credential_keys or {}), worker_id, lease_seconds),
            ).fetchone()

    def claim_capture_receipt(
        self,
        worker_id: str,
        lease_seconds: int,
        receipt: dict,
        *,
        worker_generation: int | None = None,
    ) -> dict[str, Any] | None:
        """Lease only the exact stored input; this admission cannot make HTTP requests."""
        from .source_contracts import raw_capture_identity

        capture, request = receipt["capture"], receipt["request"]
        with self.transaction() as connection:
            if not WorkerControl.claim_admitted(
                connection, worker_id, request["synthetic"], worker_generation
            ):
                return None
            self.guard_namespace(connection, request["synthetic"])
            task = connection.execute(
                "SELECT * FROM collection_tasks WHERE task_id=%s AND page_index=%s "
                "AND capture_attempt=%s AND source=%s AND account_id=%s AND synthetic=%s "
                "AND due_at<=clock_timestamp() AND (state='pending' "
                "OR (state='leased' AND lease_until<clock_timestamp()) "
                "OR (state='quarantined' AND last_error='attempt_limit')) "
                "FOR UPDATE SKIP LOCKED",
                (
                    capture["task_id"],
                    capture["page_index"],
                    capture.get("capture_attempt", 0),
                    request["source"],
                    request["account_id"],
                    request["synthetic"],
                ),
            ).fetchone()
            if task is None or raw_capture_identity(task) != capture["capture_id"]:
                return None
            return connection.execute(
                "UPDATE collection_tasks SET state='leased',lease_owner=%s,"
                "lease_version=lease_version+1,"
                "lease_until=clock_timestamp()+%s*interval '1 second' "
                "WHERE task_id=%s RETURNING *",
                (worker_id, lease_seconds, task["task_id"]),
            ).fetchone()

    def discount_capture_adoption(self, task: dict) -> None:
        """Undo only the new claim increment when Rust adopted an earlier receipt."""
        with self.transaction() as connection:
            self.assert_lease(connection, task)
            row = connection.execute(
                "UPDATE collection_tasks SET attempts=GREATEST(0,attempts-1) "
                "WHERE task_id=%s RETURNING attempts",
                (task["task_id"],),
            ).fetchone()
            if row is None:
                raise LeaseLost("Capture task no longer exists")
            task["attempts"] = row["attempts"]

    def capture_recovery_candidates(
        self, synthetic: bool, limit: int, after: str
    ) -> list[dict[str, Any]]:
        """Read one indexed active page, including not-yet-due entries for cursor progress."""
        return self.all(
            "SELECT *,due_at<=clock_timestamp() AND "
            "(state<>'leased' OR lease_until<clock_timestamp()) AS capture_recovery_due "
            "FROM collection_tasks WHERE synthetic=%s AND task_id>%s "
            "AND (state IN ('pending','leased') "
            "OR (state='quarantined' AND last_error='attempt_limit')) "
            "ORDER BY task_id LIMIT %s",
            (synthetic, after, limit),
        )

    def quota_waiting(
        self,
        synthetic: bool,
        max_attempts: int = 8,
        *,
        credential_keys: dict[str, str] | None = None,
    ) -> int:
        row = self.one(
            "SELECT count(*) AS count FROM collection_tasks t WHERE synthetic=%s "
            "AND attempts<%s AND due_at<=clock_timestamp() AND "
            "(state='pending' OR (state='leased' AND lease_until<clock_timestamp())) "
            "AND NOT request_budget_ready(t.source,t.account_id,"
            "CASE WHEN t.synthetic THEN NULL ELSE %s::jsonb->>t.source END)",
            (synthetic, max_attempts, Jsonb(credential_keys or {})),
        )
        return row["count"] if row else 0

    @staticmethod
    def assert_lease(connection: psycopg.Connection[dict[str, Any]], task: dict) -> None:
        row = connection.execute(
            "SELECT task_id FROM collection_tasks WHERE task_id=%s AND state='leased' "
            "AND lease_owner=%s AND lease_version=%s AND lease_until>clock_timestamp() FOR UPDATE",
            (task["task_id"], task["lease_owner"], task["lease_version"]),
        ).fetchone()
        if not row:
            raise LeaseLost("Collection lease expired or was superseded")

    def renew(self, task: dict, lease_seconds: int) -> None:
        with self.transaction() as connection:
            self.assert_lease(connection, task)
            connection.execute(
                "UPDATE collection_tasks SET lease_until=clock_timestamp()+%s*interval '1 second' "
                "WHERE task_id=%s",
                (lease_seconds, task["task_id"]),
            )

    def reschedule(
        self,
        task: dict,
        category: str,
        delay_seconds: float,
        *,
        quarantine: bool = False,
        no_attempt: bool = False,
    ) -> None:
        # Categories, never remote message text, enter persistent errors.
        category = category if category.replace("_", "").isalnum() else "collection_error"
        with self.transaction() as connection:
            self.assert_lease(connection, task)
            connection.execute(
                "UPDATE collection_tasks SET state=%s,due_at=clock_timestamp()+%s*interval '1 "
                "second',"
                "last_error=%s,lease_owner=NULL,lease_until=NULL,attempts=GREATEST(0,attempts-%s) "
                "WHERE task_id=%s",
                (
                    "quarantined" if quarantine else "pending",
                    max(1, delay_seconds),
                    category,
                    int(no_attempt),
                    task["task_id"],
                ),
            )

    def configure_budget(
        self, source: str, account_id: str, capacity: float, refill_per_second: float
    ) -> None:
        if (
            not math.isfinite(capacity)
            or not math.isfinite(refill_per_second)
            or capacity < 1
            or refill_per_second <= 0
        ):
            raise ValueError("Request budget must have positive capacity and refill")
        self.execute(
            "SELECT configure_request_budget(%s,%s,%s,%s)",
            (source, account_id, capacity, refill_per_second),
        )

    def block_budget(self, source: str, account_id: str, seconds: float) -> None:
        self.execute(
            "SELECT block_request_budget(%s,%s,%s)",
            (source, account_id, seconds),
        )

    def manifests(
        self,
        *,
        synthetic: bool,
        as_of: datetime | None = None,
        since: datetime | None = None,
        source: str | None = None,
        entity_keys: set[str] | None = None,
        dataset_ids: set[str] | None = None,
    ) -> list[dict]:
        """Compatibility list surface; scoped analytical readers iterate bounded pages."""
        return list(
            self.iter_manifests(
                synthetic=synthetic,
                as_of=as_of,
                since=since,
                source=source,
                entity_keys=entity_keys,
                dataset_ids=dataset_ids,
            )
        )

    def iter_manifests(
        self,
        *,
        synthetic: bool,
        as_of: datetime | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        period_window: tuple[datetime, datetime] | None = None,
        source: str | None = None,
        entity_keys: set[str] | None = None,
        dataset_ids: set[str] | None = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Select only relevant object descriptors before transferring metadata.

        Missing time bounds in historical descriptors are conservative matches.
        Keyset paging bounds each SQL response and preserves original dataset order.
        """
        if not 1 <= page_size <= 1000:
            raise ValueError("Manifest page size must be between 1 and 1000")
        if entity_keys is not None and not entity_keys:
            return
        if dataset_ids is not None and not dataset_ids:
            return
        clauses = ["d.state='committed'", "d.compacted_into IS NULL", "d.synthetic=%s"]
        params: list[Any] = [synthetic]
        if as_of:
            clauses.append("(d.min_available_at<=%s OR d.row_count=0)")
            params.append(as_of)
        if since and period_window is None:
            clauses.append("(d.max_available_at>=%s OR d.row_count=0)")
            params.append(since)
        if source:
            clauses.append("d.source=%s")
            params.append(source)
        if dataset_ids is not None:
            clauses.append("d.dataset_id=ANY(%s)")
            params.append(sorted(dataset_ids))
        projection = "d.manifest"
        joins = ""
        # The entity index provides the candidate set; per-object bounds and
        # descriptors are projected in SQL, not pruned after fetching whole manifests.
        if entity_keys is not None:
            joins = (
                "JOIN LATERAL (SELECT jsonb_agg(o.descriptor ORDER BY o.object_key) AS objects "
                "FROM normalized_objects o WHERE o.dataset_id=d.dataset_id "
                "AND EXISTS (SELECT 1 FROM normalized_object_entities e "
                "WHERE e.object_id=o.object_id "
                "AND e.entity_key=ANY(%s)) "
            )
            object_params: list[Any] = [sorted(entity_keys)]
            if as_of:
                joins += "AND (o.min_available_at IS NULL OR o.min_available_at<=%s) "
                object_params.append(as_of)
            observed_clauses = []
            if since:
                observed_clauses.append("(o.max_observed_at IS NULL OR o.max_observed_at>=%s)")
                object_params.append(since)
            if until:
                observed_clauses.append("(o.min_observed_at IS NULL OR o.min_observed_at<=%s)")
                object_params.append(until)
            if observed_clauses:
                observed_scope = " AND ".join(observed_clauses)
                if period_window is not None:
                    # Unknown legacy metadata must remain a conservative match.
                    # Extrema are necessary conditions for any contained report,
                    # with exact row-level period validation retained in History.
                    observed_scope = (
                        "(" + observed_scope + ") OR NOT o.periods_indexed OR "
                        "(o.max_period_start>=%s AND o.min_period_end<=%s)"
                    )
                    object_params.extend(period_window)
                joins += "AND (" + observed_scope + ") "
            joins += ") selected ON selected.objects IS NOT NULL "
            clauses.append(
                "EXISTS (SELECT 1 FROM normalized_object_entities matching "
                "JOIN normalized_objects matched ON matched.object_id=matching.object_id "
                "WHERE matched.dataset_id=d.dataset_id AND matching.entity_key=ANY(%s))"
            )
            params.append(sorted(entity_keys))
            projection = (
                "jsonb_build_object('dataset_id',d.dataset_id,'normalized',selected.objects)"
            )
        else:
            object_params = []
        last: tuple[datetime, str] | None = None
        while True:
            page_clauses = list(clauses)
            page_params = [*object_params, *params]
            if last:
                page_clauses.append("(d.created_at,d.dataset_id)>(%s,%s)")
                page_params.extend(last)
            page_params.append(page_size)
            rows = self.evidence_all(
                "SELECT d.dataset_id,d.source,d.synthetic,d.state,d.transform_version,"
                "d.created_at,d.committed_at,d.min_available_at,d.max_available_at,"
                "d.row_count,d.quarantine_count," + projection + " AS manifest "
                "FROM datasets d "
                + joins
                + "WHERE "
                + " AND ".join(page_clauses)
                + " ORDER BY d.created_at,d.dataset_id LIMIT %s",
                page_params,
                synthetic=synthetic,
            )
            yield from rows
            if len(rows) < page_size:
                return
            last = (rows[-1]["created_at"], rows[-1]["dataset_id"])

    @staticmethod
    def observation_fingerprint(observation: Observation) -> str:
        payload = observation.model_dump(mode="json", exclude={"raw_dataset_id"})
        for name in ("observed_at", "available_at", "published_at"):
            if payload[name] is not None:
                payload[name] = utc(payload[name]).isoformat()
        return digest(payload)

    def index_observations(
        self, connection, dataset_id: str, manifest: dict, observations: Sequence[Observation]
    ) -> None:
        """Index delivery metadata inside the authoritative publication transaction."""
        objects = manifest["normalized"]
        by_entity = {key: obj["key"] for obj in objects for key in obj["entity_keys"]}
        self.index_object_rows(
            connection, dataset_id, [(by_entity[row.entity_key], row) for row in observations]
        )
        connection.execute(
            "UPDATE normalized_objects SET observations_indexed=true WHERE dataset_id=%s",
            (dataset_id,),
        )

    def index_object_rows(
        self, connection, dataset_id: str, observations: Sequence[tuple[str, Observation]]
    ) -> None:
        objects = {
            row["object_key"]: row["object_id"]
            for row in connection.execute(
                "SELECT object_key,object_id FROM normalized_objects WHERE dataset_id=%s",
                (dataset_id,),
            ).fetchall()
        }
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO observation_deliveries(object_id,observation_id,"
                "entity_key,observed_at,available_at,payload_sha256) "
                "VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                [
                    (
                        objects[key],
                        row.observation_id,
                        row.entity_key,
                        row.observed_at,
                        row.available_at,
                        bytes.fromhex(self.observation_fingerprint(row)),
                    )
                    for key, row in observations
                ],
            )

    def unindexed_objects(
        self, synthetic: bool, as_of: datetime, entity_keys: set[str], limit: int
    ) -> list[dict]:
        return self.all(
            "SELECT o.dataset_id,o.object_key,o.descriptor FROM normalized_objects o "
            "JOIN datasets d ON d.dataset_id=o.dataset_id "
            "WHERE d.state='committed' AND d.compacted_into IS NULL AND d.synthetic=%s "
            "AND NOT o.observations_indexed "
            "AND (o.min_available_at IS NULL OR o.min_available_at<=%s) "
            "AND EXISTS (SELECT 1 FROM normalized_object_entities e "
            "WHERE e.object_id=o.object_id "
            "AND e.entity_key=ANY(%s)) ORDER BY d.created_at,d.dataset_id,o.object_key LIMIT %s",
            (synthetic, as_of, sorted(entity_keys), limit),
        )

    def observation_page(
        self,
        synthetic: bool,
        as_of: datetime,
        entity_keys: set[str],
        limit: int,
        after: tuple[datetime, str] | None,
        *,
        connection=None,
    ) -> list[dict]:
        """Select one snapshot using the same UTF-8 identifier order as Python."""
        if connection is None:
            with self.transaction() as transaction:
                self.guard_retention(transaction)
                return self.observation_page(
                    synthetic, as_of, entity_keys, limit, after, connection=transaction
                )
        after_clause = ""
        params: list[Any] = [synthetic, as_of, sorted(entity_keys)]
        if after:
            after_clause = "WHERE (observed_at,observation_id)>(%s,%s) "
            params.extend(after)
        params.append(limit)
        rows = list(
            connection.execute(
                "WITH eligible AS NOT MATERIALIZED ("
                'SELECT o.observation_id COLLATE "C" AS observation_id,n.dataset_id,'
                "n.object_key,o.entity_key,o.observed_at,o.available_at,"
                "encode(o.payload_sha256,'hex') AS payload_sha256,"
                "d.created_at,n.descriptor FROM observation_deliveries o "
                "JOIN normalized_objects n ON n.object_id=o.object_id "
                "JOIN datasets d ON d.dataset_id=n.dataset_id "
                "WHERE d.state='committed' AND d.compacted_into IS NULL AND d.synthetic=%s "
                "AND o.available_at<=%s AND o.entity_key=ANY(%s)), "
                "candidates AS (SELECT DISTINCT observed_at,observation_id FROM eligible "
                + after_clause
                + "ORDER BY observed_at,observation_id LIMIT %s), "
                "conflicts AS (SELECT e.observation_id FROM eligible e "
                "JOIN candidates c USING(observation_id) GROUP BY e.observation_id "
                "HAVING count(DISTINCT e.payload_sha256)>1) "
                "SELECT DISTINCT ON(e.observation_id) e.observation_id,e.observed_at,"
                "e.dataset_id,e.object_key,e.payload_sha256,e.descriptor,"
                "EXISTS(SELECT 1 FROM conflicts) AS conflicting FROM eligible e "
                "JOIN candidates c USING(observation_id) "
                "ORDER BY e.observation_id,e.created_at,e.dataset_id,e.object_key",
                params,
            ).fetchall()
        )
        if any(row["conflicting"] for row in rows):
            raise ValueError("Conflicting payloads under one observation identifier")
        return rows

    def stage_dataset(
        self,
        dataset_id: str,
        source: str,
        synthetic: bool,
        manifest: dict,
        *,
        task: dict | None = None,
        supersedes: str | None = None,
        publication_guard=None,
    ) -> dict:
        with self.transaction() as connection:
            if publication_guard:
                publication_guard(connection)
            self.guard_namespace(connection, synthetic)
            if task:
                self.assert_lease(connection, task)
            connection.execute(
                "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest,task_id,"
                "lease_version,supersedes,transform_version) "
                "VALUES(%s,%s,%s,'staged',%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (
                    dataset_id,
                    source,
                    synthetic,
                    Jsonb(manifest),
                    task.get("task_id") if task else None,
                    task.get("lease_version") if task else None,
                    supersedes,
                    manifest.get("transform_version", "1"),
                ),
            )
            row = connection.execute(
                "SELECT * FROM datasets WHERE dataset_id=%s FOR UPDATE", (dataset_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("Staged dataset disappeared")
            if row["source"] != source or row["synthetic"] != synthetic:
                raise ValueError("Dataset identity collision")
            return row

    @staticmethod
    def guard_inputs(
        connection: psycopg.Connection[dict[str, Any]], dataset_ids: Sequence[str]
    ) -> None:
        Catalog.guard_retention(connection)
        expected = sorted(set(dataset_ids))
        if expected:
            row = connection.execute(
                "SELECT count(*) AS count FROM datasets WHERE dataset_id=ANY(%s) "
                "AND state='committed'",
                (expected,),
            ).fetchone()
            if not row or row["count"] != len(expected):
                raise ValueError("Input evidence was invalidated during computation")

    def stage_derivation(self, object_key: str, dataset_ids: Sequence[str]) -> None:
        with self.transaction() as connection:
            self.guard_inputs(connection, dataset_ids)
            connection.execute(
                "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
                "VALUES(%s,%s,'staged') ON CONFLICT DO NOTHING",
                (object_key, Jsonb(sorted(set(dataset_ids)))),
            )

    @staticmethod
    def _entity_updates(observations: Sequence[Observation]) -> list[Observation]:
        """Fold frontier updates while preserving the original ordered SQL effects.

        The first observation establishes identity and handles an existing NULL
        frontier. The last maximum timestamp supplies the final mutable fields.
        Keeping both also preserves the legacy title comparison against NULL;
        all observations still enter the independent immutable delivery index.
        """
        selected: dict[tuple[str, bool], tuple[Observation, Observation, bool]] = {}
        for observation in observations:
            key = (observation.entity_key, observation.synthetic)
            previous = selected.get(key)
            if previous is None:
                selected[key] = (observation, observation, False)
            elif utc(observation.observed_at) >= utc(previous[1].observed_at):
                selected[key] = (previous[0], observation, True)
        result = []
        for first, latest, has_later_maximum in selected.values():
            result.append(first)
            if has_later_maximum:
                result.append(latest)
        return result

    def commit_dataset(
        self,
        dataset_id: str,
        manifest: dict,
        observations: Sequence,
        *,
        task: dict | None = None,
        next_payload: dict | None = None,
        advance_task: bool = True,
        publication_guard=None,
    ) -> None:
        with self.transaction() as connection:
            if publication_guard:
                publication_guard(connection)
            self.guard_retention(connection)
            if task:
                self.assert_lease(connection, task)
            row = connection.execute(
                "SELECT * FROM datasets WHERE dataset_id=%s FOR UPDATE", (dataset_id,)
            ).fetchone()
            if row:
                self.guard_namespace(connection, row["synthetic"])
            if not row or row["state"] in ("invalidated", "deleted"):
                raise ValueError("Dataset cannot be committed")
            if row["state"] != "committed":
                times = [obs.available_at for obs in observations]
                connection.execute(
                    "UPDATE datasets SET "
                    "state='committed',manifest=%s,row_count=%s,quarantine_count=%s,"
                    "min_available_at=%s,max_available_at=%s,committed_at=clock_timestamp() "
                    "WHERE dataset_id=%s",
                    (
                        Jsonb(manifest),
                        len(observations),
                        manifest.get("quarantine_count", 0),
                        min(times) if times else None,
                        max(times) if times else None,
                        dataset_id,
                    ),
                )
                # The catalog frontier needs at most two ordered writes per
                # entity, even when this delivery contains months of history.
                entity_updates = self._entity_updates(observations)
                # Bound each ordered pipeline; the outer transaction stays atomic.
                for start in range(0, len(entity_updates), ENTITY_WRITE_BATCH_SIZE):
                    with connection.pipeline():
                        for obs in entity_updates[start : start + ENTITY_WRITE_BATCH_SIZE]:
                            attributes = obs.attributes
                            title = str(attributes.get("title", attributes.get("description", "")))[
                                :1000
                            ]
                            connection.execute(
                                "INSERT INTO "
                                "entities(entity_key,synthetic,source,account_id,entity_type,title,"
                                "attributes,last_observed_at,origin) "
                                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                                "ON CONFLICT(entity_key,synthetic) DO UPDATE SET "
                                "last_observed_at=GREATEST(entities.last_observed_at,EXCLUDED.last_observed_at),"
                                "title=CASE WHEN "
                                "EXCLUDED.last_observed_at>=entities.last_observed_at "
                                "THEN "
                                "EXCLUDED.title ELSE entities.title END,"
                                "attributes=CASE WHEN entities.last_observed_at IS NULL OR "
                                "EXCLUDED.last_observed_at>=entities.last_observed_at "
                                "THEN EXCLUDED.attributes "
                                "ELSE entities.attributes END,"
                                "origin=CASE WHEN entities.last_observed_at IS NULL OR "
                                "EXCLUDED.last_observed_at>=entities.last_observed_at "
                                "THEN EXCLUDED.origin "
                                "ELSE entities.origin END,"
                                "account_id=CASE WHEN entities.last_observed_at IS NULL OR "
                                "EXCLUDED.last_observed_at>=entities.last_observed_at "
                                "THEN EXCLUDED.account_id "
                                "ELSE entities.account_id END",
                                (
                                    obs.entity_key,
                                    obs.synthetic,
                                    obs.source,
                                    obs.account_id or "",
                                    obs.entity_type,
                                    title,
                                    Jsonb(attributes),
                                    obs.observed_at,
                                    Jsonb({"dataset_id": dataset_id, "source": obs.source}),
                                ),
                            )
                self.index_observations(connection, dataset_id, manifest, observations)
            if task and advance_task:
                connection.execute(
                    "UPDATE collection_tasks SET "
                    "state=%s,payload=%s,lease_owner=NULL,lease_until=NULL,"
                    "completed_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,attempts=0,"
                    "page_index=page_index+%s,capture_attempt=0,last_error=%s "
                    "WHERE task_id=%s",
                    (
                        "quarantined"
                        if task.get("pagination_limit_reached")
                        else ("pending" if next_payload is not None else "done"),
                        Jsonb(next_payload if next_payload is not None else task["payload"]),
                        next_payload is None,
                        int(next_payload is not None),
                        "pagination_limit" if task.get("pagination_limit_reached") else None,
                        task["task_id"],
                    ),
                )

    def invalidate_dataset(self, dataset_id: str, reason: str) -> dict[str, Any] | None:
        from .invalidation import SourceInvalidation

        try:
            self._invalidate_dataset_atomic(dataset_id, reason)
        except (
            InvalidationTooLarge,
            InvalidationPending,
            psycopg.errors.QueryCanceled,
            psycopg.errors.LockNotAvailable,
        ):
            identifier = SourceInvalidation(self).begin(dataset_id, reason)
            result = self.resume_invalidations(max_batches=1, max_seconds=0.1)
            return {"invalidation_id": identifier, **result}
        return None

    def _invalidate_dataset_atomic(self, dataset_id: str, reason: str) -> None:
        with self.transaction() as connection:
            connection.execute("SET LOCAL statement_timeout='2s'")
            connection.execute("SET LOCAL lock_timeout='250ms'")
            self.guard_retention(connection, exclusive=True)
            namespace = connection.execute(
                "SELECT synthetic FROM datasets WHERE dataset_id=%s", (dataset_id,)
            ).fetchone()
            if namespace:
                self.guard_namespace(connection, namespace["synthetic"])
            wide = connection.execute(
                "SELECT source_invalidation_wide(ARRAY[%s]) AS wide", (dataset_id,)
            ).fetchone()
            if wide and wide["wide"]:
                raise InvalidationTooLarge("Source invalidation exceeds its atomic work budget")
            connection.execute(
                "CREATE TEMP TABLE invalidated_datasets ON COMMIT DROP AS "
                "SELECT dataset_id FROM dataset_publication_closure(ARRAY[%s]) LIMIT 10001",
                (dataset_id,),
            )
            dataset_count = connection.execute(
                "SELECT count(*) AS count FROM invalidated_datasets"
            ).fetchone()
            if dataset_count and dataset_count["count"] > 10000:
                raise InvalidationTooLarge("Source invalidation exceeds its atomic dataset budget")
            connection.execute(
                "CREATE TEMP TABLE invalidated_artifacts ON COMMIT DROP AS "
                "SELECT p.object_id,p.object_key FROM analytical_artifact_ancestor_ids("
                "ARRAY(SELECT dataset_id FROM invalidated_datasets)) affected "
                "JOIN derived_publications p USING(object_id) LIMIT 10001",
            )
            affected = connection.execute(
                "SELECT count(*) AS count FROM invalidated_artifacts"
            ).fetchone()
            if affected and affected["count"] > 10000:
                raise InvalidationTooLarge("Source invalidation exceeds its atomic artifact budget")
            connection.execute(
                "UPDATE datasets SET state='invalidated',invalid_reason=%s "
                "WHERE dataset_id IN (SELECT dataset_id FROM invalidated_datasets) "
                "AND state<>'deleted'",
                (reason,),
            )
            connection.execute(
                "UPDATE assessment_runs SET invalidated=true WHERE evidence_datasets ?| "
                "ARRAY(SELECT dataset_id FROM invalidated_datasets) "
                "OR object_key IN (SELECT object_key FROM invalidated_artifacts)",
            )
            connection.execute(
                "UPDATE derived_publications SET state='invalidated' WHERE state<>'deleted' "
                "AND object_id IN (SELECT object_id FROM invalidated_artifacts)"
            )
            connection.execute(
                "UPDATE alerts SET invalidated=true WHERE run_id IN "
                "(SELECT run_id FROM assessment_runs WHERE invalidated)",
            )
            connection.execute(
                "WITH RECURSIVE affected(evaluation_id) AS ("
                "SELECT evaluation_id FROM evaluation_dependencies "
                "WHERE dataset_id IN (SELECT dataset_id FROM invalidated_datasets) "
                "UNION SELECT evaluation_id FROM evaluation_artifacts "
                "WHERE object_id IN (SELECT object_id FROM invalidated_artifacts) "
                "UNION SELECT child.evaluation_id FROM evaluation_runs child "
                "JOIN affected parent ON child.parent_id=parent.evaluation_id) "
                "UPDATE evaluation_runs SET invalidated=true WHERE evaluation_id IN "
                "(SELECT evaluation_id FROM affected)",
            )
            connection.execute(
                "UPDATE datasets SET compacted_into=NULL WHERE state='committed' "
                "AND compacted_into IN (SELECT dataset_id FROM invalidated_datasets)"
            )

    def export_state(self, snapshot_id: str | None = None) -> dict[str, Any]:
        """Logical evidence verification summary; operational backup uses pg_dump."""
        tables: tuple[str, ...] = (
            "datasets",
            "products",
            "product_links",
            "family_memberships",
            "alerts",
            "evaluation_runs",
            "source_capabilities",
            "replay_availability",
        )
        result = {}
        with self.transaction() as connection:
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            if snapshot_id is not None:
                connection.execute(
                    sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot_id))
                )
            journal = connection.execute(
                "SELECT EXISTS(SELECT 1 FROM source_invalidations) OR "
                "EXISTS(SELECT 1 FROM source_invalidation_work) AS present"
            ).fetchone()
            # Empty new relations do not invalidate pre-migration fingerprints.
            # Any persisted job, including a completed tombstone, hashes both.
            if journal and journal["present"]:
                tables += ("source_invalidations", "source_invalidation_work")
            # Legal capture contracts and withdrawal certificates affect evidence
            # interpretation. Keep legacy fingerprints when these newer relations
            # are absent/empty, and inspect them through the same imported cut.
            for table in (
                "capture_policies",
                "capture_obligations",
                "capture_spool_revocations",
                "capture_spool_retirements",
            ):
                relation = connection.execute(
                    "SELECT to_regclass(%s) AS relation", (table,)
                ).fetchone()
                if relation and relation["relation"]:
                    populated = connection.execute(
                        sql.SQL("SELECT EXISTS(SELECT 1 FROM {}) AS present").format(
                            sql.Identifier(table)
                        )
                    ).fetchone()
                    if populated and populated["present"]:
                        tables += (table,)
            for table in tables:
                hasher = hashlib.sha256(b"[")
                separator = b""
                # Server-side cursors keep backup verification independent of history size.
                with connection.cursor(name="evidence_state") as cursor:
                    cursor.itersize = 256
                    cursor.execute(
                        f"SELECT row_to_json(t) AS row FROM {table} t ORDER BY row_to_json(t)::text"
                    )
                    for row in cursor:
                        hasher.update(separator)
                        hasher.update(canonical(row).encode())
                        separator = b","
                hasher.update(b"]")
                result[table] = hasher.hexdigest()
        return result

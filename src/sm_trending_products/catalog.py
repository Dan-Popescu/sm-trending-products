"""PostgreSQL operational catalog, fencing, and immutable evidence pointers."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from importlib.resources import files
from typing import Any, cast

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .contracts import canonical, digest

# Shared protocol with the literal in SQL migration 003_maintenance.sql triggers.
# Changing the key requires coordinating both Python writers and database guards.
RETENTION_GUARD_KEY = 492371205


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
        with self.pool.connection() as connection, connection.transaction():
            connection.execute("SET LOCAL statement_timeout='30s'")
            connection.execute("SET LOCAL lock_timeout='10s'")
            yield connection

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

    def one(self, query: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        with self.transaction() as connection:
            return connection.execute(query, params).fetchone()

    def execute(self, query: str, params: Sequence[Any] = ()) -> None:
        with self.transaction() as connection:
            connection.execute(query, params)

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
        self, worker_id: str, lease_seconds: int, synthetic: bool, max_attempts: int
    ) -> dict[str, Any] | None:
        with self.transaction() as connection:
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
                "WITH next_task AS (SELECT task_id FROM collection_tasks WHERE synthetic=%s "
                "AND attempts<%s AND due_at<=clock_timestamp() AND "
                "(state='pending' OR (state='leased' AND lease_until<clock_timestamp())) "
                "ORDER BY priority DESC,due_at,task_id FOR UPDATE SKIP LOCKED LIMIT 1) "
                "UPDATE collection_tasks t SET state='leased',lease_owner=%s,"
                "lease_version=t.lease_version+1,lease_until=clock_timestamp()+%s*interval '1 "
                "second',"
                "attempts=t.attempts+1 FROM next_task n WHERE t.task_id=n.task_id RETURNING t.*",
                (synthetic, max_attempts, worker_id, lease_seconds),
            ).fetchone()

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
        if capacity < 1 or refill_per_second <= 0:
            raise ValueError("Request budget must have positive capacity and refill")
        self.execute(
            "INSERT INTO request_budgets(source,account_id,capacity,refill_per_second,tokens) "
            "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(source,account_id) DO UPDATE SET "
            "capacity=EXCLUDED.capacity,refill_per_second=EXCLUDED.refill_per_second,"
            "tokens=LEAST(request_budgets.tokens,EXCLUDED.capacity)",
            (source, account_id, capacity, refill_per_second, capacity),
        )

    def block_budget(self, source: str, account_id: str, seconds: float) -> None:
        self.execute(
            "UPDATE request_budgets SET blocked_until=GREATEST(COALESCE(blocked_until,"
            "clock_timestamp()),clock_timestamp()+%s*interval '1 second') "
            "WHERE source=%s AND account_id=%s",
            (seconds, source, account_id),
        )
        self.execute(
            "UPDATE credential_budgets SET blocked_until=GREATEST(COALESCE(blocked_until,"
            "clock_timestamp()),clock_timestamp()+%s*interval '1 second') WHERE credential_key IN "
            "(SELECT credential_key FROM credential_bindings WHERE source=%s AND account_id=%s)",
            (seconds, source, account_id),
        )

    def manifests(
        self,
        *,
        synthetic: bool,
        as_of: datetime | None = None,
        since: datetime | None = None,
        source: str | None = None,
    ) -> list[dict]:
        clauses = ["state='committed'", "compacted_into IS NULL", "synthetic=%s"]
        params: list[Any] = [synthetic]
        if as_of:
            clauses.append("(min_available_at<=%s OR row_count=0)")
            params.append(as_of)
        if since:
            clauses.append("(max_available_at>=%s OR row_count=0)")
            params.append(since)
        if source:
            clauses.append("source=%s")
            params.append(source)
        return self.all(
            "SELECT * FROM datasets WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at,dataset_id",
            params,
        )

    def stage_dataset(
        self,
        dataset_id: str,
        source: str,
        synthetic: bool,
        manifest: dict,
        *,
        task: dict | None = None,
        supersedes: str | None = None,
    ) -> dict:
        with self.transaction() as connection:
            self.guard_retention(connection)
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

    def commit_dataset(
        self,
        dataset_id: str,
        manifest: dict,
        observations: Sequence,
        *,
        task: dict | None = None,
        next_payload: dict | None = None,
        advance_task: bool = True,
    ) -> None:
        with self.transaction() as connection:
            self.guard_retention(connection)
            if task:
                self.assert_lease(connection, task)
            row = connection.execute(
                "SELECT * FROM datasets WHERE dataset_id=%s FOR UPDATE", (dataset_id,)
            ).fetchone()
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
                for obs in observations:
                    attributes = obs.attributes
                    title = str(attributes.get("title", attributes.get("description", "")))[:1000]
                    connection.execute(
                        "INSERT INTO "
                        "entities(entity_key,synthetic,source,account_id,entity_type,title,"
                        "attributes,last_observed_at,origin) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(entity_key,synthetic) DO UPDATE SET "
                        "last_observed_at=GREATEST(entities.last_observed_at,EXCLUDED.last_observed_at),"
                        "title=CASE WHEN EXCLUDED.last_observed_at>=entities.last_observed_at THEN "
                        "EXCLUDED.title ELSE entities.title END,"
                        "attributes=CASE WHEN entities.last_observed_at IS NULL OR "
                        "EXCLUDED.last_observed_at>=entities.last_observed_at "
                        "THEN EXCLUDED.attributes "
                        "ELSE entities.attributes END,"
                        "origin=CASE WHEN entities.last_observed_at IS NULL OR "
                        "EXCLUDED.last_observed_at>=entities.last_observed_at THEN EXCLUDED.origin "
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
            if task and advance_task:
                connection.execute(
                    "UPDATE collection_tasks SET "
                    "state=%s,payload=%s,lease_owner=NULL,lease_until=NULL,"
                    "completed_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,attempts=0,"
                    "page_index=page_index+%s,last_error=%s "
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

    def invalidate_dataset(self, dataset_id: str, reason: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE datasets SET state='invalidated',invalid_reason=%s "
                "WHERE dataset_id=%s AND state<>'deleted'",
                (reason, dataset_id),
            )
            connection.execute(
                "UPDATE assessment_runs SET invalidated=true WHERE evidence_datasets ? %s",
                (dataset_id,),
            )
            connection.execute(
                "UPDATE alerts SET invalidated=true WHERE run_id IN "
                "(SELECT run_id FROM assessment_runs WHERE invalidated)",
            )
            connection.execute(
                "UPDATE evaluation_runs SET invalidated=true WHERE evaluation_id IN "
                "(SELECT evaluation_id FROM evaluation_dependencies WHERE dataset_id=%s)",
                (dataset_id,),
            )

    def export_state(self) -> dict[str, Any]:
        """Logical evidence verification summary; operational backup uses pg_dump."""
        tables = (
            "datasets",
            "products",
            "product_links",
            "family_memberships",
            "alerts",
            "evaluation_runs",
            "source_capabilities",
        )
        result = {}
        with self.transaction() as connection:
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

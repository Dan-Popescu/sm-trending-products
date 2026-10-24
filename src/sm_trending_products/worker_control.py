"""Fenced worker admission and acknowledged, non-destructive draining."""

from __future__ import annotations

import math
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import psycopg
from psycopg_pool import PoolTimeout

if TYPE_CHECKING:
    from .catalog import Catalog


class WorkerControl:
    """Drain acknowledgement is an operation boundary, not a heartbeat claim.

    A registered process owns one generation. Remote drain requests block future
    claims immediately; only its operation loop acknowledges quiescence. A
    remotely drained daemon stays alive so Compose cannot restart it into work.
    """

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    @staticmethod
    def _lock(
        connection: psycopg.Connection[dict[str, Any]],
        worker_id: str,
        *,
        deadline: float | None = None,
    ) -> dict | None:
        # Serialize registration with a legacy claim that found no registry row.
        # No transaction here spans a source request or object publication.
        if deadline is not None:
            WorkerControl._limit(connection, deadline)
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,492371209))", (worker_id,)
        )
        if deadline is not None:
            WorkerControl._limit(connection, deadline)
        return cast(
            dict[str, Any] | None,
            connection.execute(
                "SELECT * FROM collection_workers WHERE worker_id=%s FOR UPDATE", (worker_id,)
            ).fetchone(),
        )

    @staticmethod
    def _leases(connection: psycopg.Connection[dict[str, Any]], worker_id: str) -> dict:
        return cast(
            dict[str, Any],
            connection.execute(
                "SELECT count(*) AS owned_leases,"
                "count(*) FILTER (WHERE lease_until<clock_timestamp()) AS expired_leases,"
                "count(*) FILTER (WHERE lease_until>=clock_timestamp()) AS unexpired_leases "
                "FROM collection_tasks WHERE lease_owner=%s AND state='leased'",
                (worker_id,),
            ).fetchone(),
        )

    @staticmethod
    def _session(row: dict | None, generation: int) -> dict:
        if row is None or row["generation"] != generation:
            raise ValueError("Unknown worker generation")
        return row

    @staticmethod
    def claim_admitted(
        connection: psycopg.Connection[dict[str, Any]],
        worker_id: str,
        synthetic: bool,
        generation: int | None,
    ) -> bool:
        row = WorkerControl._lock(connection, worker_id)
        if row is None:
            return generation is None  # Existing unregistered callers remain supported.
        if (
            row["generation"] != generation
            or row["synthetic"] != synthetic
            or row["state"] != "running"
        ):
            return False
        connection.execute(
            "UPDATE collection_workers SET heartbeat_at=clock_timestamp() WHERE worker_id=%s",
            (worker_id,),
        )
        return True

    def register(
        self, worker_id: str, *, synthetic: bool, stale_after_seconds: float = 30
    ) -> dict[str, Any]:
        if not worker_id or len(worker_id) > 200:
            raise ValueError("Worker identity must contain 1..200 characters")
        if not math.isfinite(stale_after_seconds) or stale_after_seconds <= 0:
            raise ValueError("Worker stale heartbeat threshold must be positive")
        with self.catalog.transaction() as connection:
            row = self._lock(connection, worker_id)
            leases = self._leases(connection, worker_id)
            stale = cast(
                dict,
                connection.execute(
                    "SELECT %s::timestamptz<clock_timestamp()-%s*interval '1 second' AS stale",
                    (row["heartbeat_at"] if row else None, stale_after_seconds),
                ).fetchone(),
            )

            if leases["unexpired_leases"] or (
                row is not None and row["state"] != "stopped" and not stale["stale"]
            ):
                raise ValueError("Worker already registered or still owns unfinished work")
            return cast(
                dict[str, Any],
                connection.execute(
                    "INSERT INTO collection_workers(worker_id,generation,synthetic,state) "
                    "VALUES(%s,1,%s,'running') ON CONFLICT(worker_id) DO UPDATE SET "
                    "generation=collection_workers.generation+1,synthetic=EXCLUDED.synthetic,"
                    "state=CASE WHEN collection_workers.drain_requested_at IS NULL "
                    "THEN 'running' ELSE 'draining' END,"
                    "registered_at=clock_timestamp(),heartbeat_at=clock_timestamp(),"
                    "drain_acknowledged_at=NULL,"
                    "stopped_at=NULL RETURNING *",
                    (worker_id, synthetic),
                ).fetchone(),
            )

    def cancel_unregistered(self, worker_id: str, *, synthetic: bool) -> bool:
        """Fence an absent, never-registered identity with zero accepted work.

        The host controller must prove its fixed slot has no container first.
        No running worker is acknowledged or stopped here. The admission lock
        prevents a legacy claim racing the zero-lease check; a delayed startup
        inherits the durable pause when it eventually registers.
        Repeating after an interrupted cancellation accepts only the same
        already-stopped pause with zero leases; running identities are untouched.
        """
        if not worker_id or len(worker_id) > 200:
            raise ValueError("Invalid worker identity")
        with self.catalog.transaction() as connection:
            row = self._lock(connection, worker_id)
            if self._leases(connection, worker_id)["owned_leases"]:
                return False
            if row is not None:
                return bool(
                    row["synthetic"] == synthetic
                    and row["state"] == "stopped"
                    and row["drain_requested_at"] is not None
                    and row["stopped_at"] is not None
                )
            connection.execute(
                "INSERT INTO collection_workers(worker_id,generation,synthetic,state,"
                "drain_requested_at,stopped_at) "
                "VALUES(%s,1,%s,'stopped',clock_timestamp(),clock_timestamp())",
                (worker_id, synthetic),
            )
            return True

    def heartbeat(self, worker_id: str, generation: int) -> dict:
        with self.catalog.transaction() as connection:
            self._session(self._lock(connection, worker_id), generation)
            return cast(
                dict[str, Any],
                connection.execute(
                    "UPDATE collection_workers SET heartbeat_at=clock_timestamp() "
                    "WHERE worker_id=%s RETURNING *",
                    (worker_id,),
                ).fetchone(),
            )

    @staticmethod
    def _timeout(timeout_seconds: float) -> None:
        if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 3600:
            raise ValueError("Drain timeout must be positive and at most 3600 seconds")

    @staticmethod
    def _limit(connection: psycopg.Connection[dict[str, Any]], deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise psycopg.errors.QueryCanceled("Worker control deadline expired")
        milliseconds = f"{max(1, int(remaining * 1000))}ms"
        connection.execute(
            "SELECT set_config('lock_timeout',%s,true),set_config('statement_timeout',%s,true)",
            (milliseconds, milliseconds),
        )

    @contextmanager
    def _bounded_connection(self, deadline: float) -> Iterator[psycopg.Connection[dict[str, Any]]]:
        with self.catalog.pool.connection(
            timeout=max(0.001, deadline - time.monotonic())
        ) as connection:
            with connection.transaction():
                self._limit(connection, deadline)
                yield connection

    def _blocked(
        self,
        worker_id: str,
        generation: int,
        reason: str,
        timeout_seconds: float,
        *,
        worker: dict | None = None,
    ) -> dict:
        if worker is None:
            try:
                worker = self.status(worker_id, timeout_seconds=min(timeout_seconds, 0.1))
            except (psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled, PoolTimeout):
                pass  # A blocked diagnostics read cannot authorize retirement.
        return {
            "ok": False,
            "status": "blocked",
            "reason": reason,
            "worker_id": worker_id,
            "generation": generation,
            "timeout_seconds": timeout_seconds,
            "worker": worker,
        }

    def request_drain(self, worker_id: str, generation: int, *, timeout_seconds: float = 5) -> dict:
        """Return only after prior claim transactions have crossed the barrier."""
        self._timeout(timeout_seconds)
        deadline = time.monotonic() + timeout_seconds
        try:
            with self._bounded_connection(deadline) as connection:
                row = self._session(
                    self._lock(connection, worker_id, deadline=deadline), generation
                )
                if row["drain_requested_at"] is not None:
                    return row
                self._limit(connection, deadline)
                return cast(
                    dict[str, Any],
                    connection.execute(
                        "UPDATE collection_workers SET "
                        "state=CASE WHEN state='stopped' THEN 'stopped' ELSE 'draining' END,"
                        "drain_requested_at=clock_timestamp(),drain_acknowledged_at=NULL "
                        "WHERE worker_id=%s RETURNING *",
                        (worker_id,),
                    ).fetchone(),
                )
        except (psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled, PoolTimeout):
            return self._blocked(worker_id, generation, "admission_lock_timeout", timeout_seconds)

    def wait_drained(self, worker_id: str, generation: int, *, timeout_seconds: float = 30) -> dict:
        """Observe bounded acknowledgement; a timeout never terminates accepted work."""
        self._timeout(timeout_seconds)
        deadline = time.monotonic() + timeout_seconds
        row = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return self._blocked(
                    worker_id, generation, "drain_timeout", timeout_seconds, worker=row
                )
            try:
                row = self._session(self.status(worker_id, timeout_seconds=remaining), generation)
            except (psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled, PoolTimeout):
                return self._blocked(
                    worker_id, generation, "drain_timeout", timeout_seconds, worker=row
                )
            if (
                (row["state"] == "drained" and row["drain_acknowledged_at"] is not None)
                or (row["state"] == "stopped" and row["stopped_at"] is not None)
            ) and row["owned_leases"] == 0:
                return row
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(0.05, remaining))

    def acknowledge_drain(self, worker_id: str, generation: int) -> dict:
        """Called by the owning operation loop after it has settled its work."""
        with self.catalog.transaction() as connection:
            row = self._session(self._lock(connection, worker_id), generation)
            if (
                row["state"] == "draining"
                and not self._leases(connection, worker_id)["owned_leases"]
            ):
                row = cast(
                    dict[str, Any],
                    connection.execute(
                        "UPDATE collection_workers SET state='drained',"
                        "drain_acknowledged_at=clock_timestamp(),heartbeat_at=clock_timestamp() "
                        "WHERE worker_id=%s RETURNING *",
                        (worker_id,),
                    ).fetchone(),
                )
            return row

    def resume(self, worker_id: str, generation: int) -> dict:
        with self.catalog.transaction() as connection:
            row = self._session(self._lock(connection, worker_id), generation)
            if row["state"] == "stopped":
                raise ValueError("A stopped worker requires a new generation")
            if row["state"] == "running":
                return row
            return cast(
                dict[str, Any],
                connection.execute(
                    "UPDATE collection_workers SET state='running',drain_requested_at=NULL,"
                    "drain_acknowledged_at=NULL WHERE worker_id=%s RETURNING *",
                    (worker_id,),
                ).fetchone(),
            )

    def finish(self, worker_id: str, generation: int) -> dict:
        with self.catalog.transaction() as connection:
            self._session(self._lock(connection, worker_id), generation)
            unfinished = self._leases(connection, worker_id)["owned_leases"]
            return cast(
                dict[str, Any],
                connection.execute(
                    "UPDATE collection_workers SET state=%s,heartbeat_at=clock_timestamp(),"
                    "stopped_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END "
                    "WHERE worker_id=%s RETURNING *",
                    ("draining" if unfinished else "stopped", not unfinished, worker_id),
                ).fetchone(),
            )

    def status(self, worker_id: str, *, timeout_seconds: float = 30) -> dict | None:
        self._timeout(timeout_seconds)
        deadline = time.monotonic() + timeout_seconds
        with self._bounded_connection(deadline) as connection:
            self._limit(connection, deadline)
            row = cast(
                dict[str, Any],
                connection.execute(
                    "SELECT * FROM collection_workers WHERE worker_id=%s", (worker_id,)
                ).fetchone(),
            )
            if not row:
                return None
            self._limit(connection, deadline)
            return {**row, **self._leases(connection, worker_id)}

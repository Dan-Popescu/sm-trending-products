"""Fenced worker admission and acknowledged, non-destructive draining."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import psycopg

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
    def _lock(connection: psycopg.Connection[dict[str, Any]], worker_id: str) -> dict | None:
        # Serialize registration with a legacy claim that found no registry row.
        # No transaction here spans a source request or object publication.
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,492371209))", (worker_id,)
        )
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
                "count(*) FILTER (WHERE lease_until<clock_timestamp()) AS expired_leases "
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

    def register(self, worker_id: str, *, synthetic: bool) -> dict[str, Any]:
        if not worker_id or len(worker_id) > 200:
            raise ValueError("Worker identity must contain 1..200 characters")
        with self.catalog.transaction() as connection:
            row = self._lock(connection, worker_id)
            if (row is not None and row["state"] != "stopped") or self._leases(
                connection, worker_id
            )["owned_leases"]:
                raise ValueError("Worker already registered or still owns unfinished work")
            return cast(
                dict[str, Any],
                connection.execute(
                    "INSERT INTO collection_workers(worker_id,generation,synthetic,state) "
                    "VALUES(%s,1,%s,'running') ON CONFLICT(worker_id) DO UPDATE SET "
                    "generation=collection_workers.generation+1,synthetic=EXCLUDED.synthetic,"
                    "state='running',registered_at=clock_timestamp(),heartbeat_at=clock_timestamp(),"
                    "drain_requested_at=NULL,drain_acknowledged_at=NULL,"
                    "stopped_at=NULL RETURNING *",
                    (worker_id, synthetic),
                ).fetchone(),
            )

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

    def request_drain(self, worker_id: str) -> dict:
        """Return only after prior claim transactions have crossed the barrier."""
        with self.catalog.transaction() as connection:
            row = self._lock(connection, worker_id)
            if row is None:
                raise ValueError("Unknown worker; legacy admission cannot acknowledge draining")
            if row["state"] != "running":
                return row
            return cast(
                dict[str, Any],
                connection.execute(
                    "UPDATE collection_workers SET state='draining',"
                    "drain_requested_at=clock_timestamp(),drain_acknowledged_at=NULL "
                    "WHERE worker_id=%s RETURNING *",
                    (worker_id,),
                ).fetchone(),
            )

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

    def status(self, worker_id: str) -> dict | None:
        with self.catalog.transaction() as connection:
            row = cast(
                dict[str, Any],
                connection.execute(
                    "SELECT * FROM collection_workers WHERE worker_id=%s", (worker_id,)
                ).fetchone(),
            )
            return {**row, **self._leases(connection, worker_id)} if row else None

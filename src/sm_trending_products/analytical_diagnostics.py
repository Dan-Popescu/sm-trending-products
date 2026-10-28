"""Bounded operational attempts, isolated from analytical content and hashes."""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from time import perf_counter
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from .contracts import canonical, now
from .module_schema import initialize_module

if TYPE_CHECKING:
    from .analysis_batches import WindowMetadataBudget
    from .history import History

logger = logging.getLogger(__name__)
_active: ContextVar[tuple[_Attempt, ...]] = ContextVar("analytical_attempts", default=())


_MIGRATIONS = {
    1: """
CREATE TABLE analytical_run_diagnostics (
    attempt_id text PRIMARY KEY CHECK(attempt_id ~ '^[0-9a-f]{32}$'),
    operation text NOT NULL CHECK(operation IN ('assessment','freeze')),
    synthetic boolean NOT NULL,
    cutoff timestamptz,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    state text NOT NULL CHECK(state IN ('running','succeeded','failed')),
    result_id varchar(128),
    error_type varchar(128),
    summary jsonb NOT NULL CHECK(octet_length(summary::text)<=32768),
    CHECK((state='running')=(finished_at IS NULL)),
    CHECK(state<>'succeeded' OR (cutoff IS NOT NULL AND result_id IS NOT NULL)),
    CHECK((state='failed')=(error_type IS NOT NULL))
);
CREATE INDEX analytical_run_diagnostics_recent
    ON analytical_run_diagnostics(synthetic,started_at DESC,attempt_id DESC);
"""
}


@dataclass
class _Attempt:
    history: History
    operation: str
    synthetic: bool
    cutoff: datetime | None
    attempt_id: str = field(default_factory=lambda: uuid4().hex)
    started_at: datetime = field(default_factory=now)
    finished_at: datetime | None = None
    state: str = "running"
    result_id: str | None = None
    error_type: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)


def _current(history: History) -> _Attempt | None:
    return next((run for run in reversed(_active.get()) if run.history is history), None)


def attempt_identifier(value: str) -> str:
    if not re.fullmatch("[0-9a-f]{32}", value):
        raise ValueError("Attempt identifier must contain 32 lowercase hexadecimal characters")
    return value


def record(history: History, group: str, name: str, value: int = 1) -> None:
    """Attribute an event to this context's innermost attempt for this History."""
    if (run := _current(history)) is not None:
        run.summary[group][name] += value


def storage(history: History, name: str, value: int = 1) -> None:
    history.metrics[name] += value
    record(history, "storage", name, value)


def analysis(history: History, name: str, value: int = 1) -> None:
    history.analysis_metrics[name] += value
    record(history, "analysis", name, value)


def _metadata_admitted(history: History, objects: int, encoded_bytes: int) -> None:
    if (run := _current(history)) is not None:
        work = run.summary["work"]
        work["metadata_partitions"] += 1
        work["metadata_objects"] += objects
        work["metadata_bytes"] += encoded_bytes
        work["metadata_peak_objects"] = max(work["metadata_peak_objects"], objects)
        work["metadata_peak_bytes"] = max(work["metadata_peak_bytes"], encoded_bytes)


@contextmanager
def metadata_selection(history: History, budget: WindowMetadataBudget) -> Iterator[None]:
    with phase(history, "metadata_selection"):
        try:
            yield
        finally:
            _metadata_admitted(history, budget.objects, budget.bytes)
            for reason, count in budget.declines.items():
                if count:
                    record(history, "reasons", reason, count)


@contextmanager
def phase(history: History, name: str) -> Iterator[None]:
    run = _current(history)
    if run is None:
        yield
        return
    started = perf_counter()
    try:
        yield
    finally:
        run.summary["phase_ms"][name] += round((perf_counter() - started) * 1000, 3)


class AnalyticalDiagnostics:
    def __init__(self, history: History):
        self.history = history

    def initialize(self) -> None:
        """Optional telemetry must not advance or block the capture schema."""
        try:
            initialize_module(self.history.catalog, "analytical_diagnostics", _MIGRATIONS)
        except Exception as error:
            logger.warning(
                "Analytical diagnostics initialization unavailable (%s)", type(error).__name__
            )

    def _save(self, run: _Attempt) -> None:
        """Best effort: a telemetry failure cannot change analytical behavior."""
        try:
            if len(canonical(run.summary).encode()) > 32768:
                raise ValueError("Diagnostic summary exceeds its byte budget")
            with self.history.catalog.pool.connection(timeout=0.2) as connection:
                with connection.transaction():
                    connection.execute("SET LOCAL statement_timeout='500ms'")
                    connection.execute("SET LOCAL lock_timeout='100ms'")
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended("
                        "current_schema()||':analytical-diagnostics:'||%s,492371260))",
                        (str(run.synthetic),),
                    )
                    connection.execute(
                        "INSERT INTO analytical_run_diagnostics("
                        "attempt_id,operation,synthetic,cutoff,"
                        "started_at,finished_at,state,result_id,error_type,summary) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(attempt_id) DO UPDATE SET finished_at=EXCLUDED.finished_at,"
                        "cutoff=EXCLUDED.cutoff,"
                        "state=EXCLUDED.state,result_id=EXCLUDED.result_id,"
                        "error_type=EXCLUDED.error_type,summary=EXCLUDED.summary",
                        (
                            run.attempt_id,
                            run.operation,
                            run.synthetic,
                            run.cutoff,
                            run.started_at,
                            run.finished_at,
                            run.state,
                            run.result_id,
                            run.error_type,
                            Jsonb(run.summary),
                        ),
                    )
                    connection.execute(
                        "DELETE FROM analytical_run_diagnostics WHERE attempt_id IN ("
                        "SELECT attempt_id FROM analytical_run_diagnostics WHERE synthetic=%s "
                        "ORDER BY started_at DESC,attempt_id DESC OFFSET %s)",
                        (run.synthetic, self.history.settings.analytical_diagnostics_max_runs),
                    )
        except Exception as error:
            logger.warning(
                "Analytical diagnostic %s unavailable (%s)", run.attempt_id, type(error).__name__
            )

    @contextmanager
    def attempt(
        self,
        operation: str,
        cutoff: datetime | None,
        synthetic: bool,
        *,
        admission: Callable[[], None] | None = None,
    ) -> Iterator[_Attempt]:
        """Attribute admission before persistence can affect its clock decisions."""
        run = _Attempt(self.history, operation, synthetic, cutoff)
        run.summary = {
            "version": 1,
            "elapsed_ms": 0,
            "phase_ms": Counter(),
            "storage": Counter({name: 0 for name in self.history.metrics}),
            "analysis": Counter({name: 0 for name in self.history.analysis_metrics}),
            "work": Counter(),
            "reasons": Counter(),
            "limits": {
                name: getattr(self.history.settings, name)
                for name in (
                    "analysis_product_batch_size",
                    "analysis_max_rows_per_batch",
                    "analysis_max_objects_per_batch",
                    "analysis_max_metadata_bytes_per_batch",
                    "analytical_diagnostics_max_runs",
                )
            },
        }
        token = _active.set((*_active.get(), run))
        started = perf_counter()
        persistence_seconds = 0.0
        try:
            if admission is not None:
                admission()
            saving = perf_counter()
            self._save(run)
            persistence_seconds = perf_counter() - saving
            yield run
        except BaseException as error:
            run.state, run.error_type = "failed", type(error).__name__[:128]
            raise
        else:
            run.state = "succeeded"
        finally:
            run.finished_at = now()
            run.summary["elapsed_ms"] = round(
                (perf_counter() - started - persistence_seconds) * 1000, 3
            )
            _active.reset(token)
            self._save(run)

    def list(self, synthetic: bool, limit: int = 50) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("Analytical run limit must be between 1 and 100")
        return self.history.catalog.all(
            "SELECT * FROM analytical_run_diagnostics WHERE synthetic=%s "
            "ORDER BY started_at DESC,attempt_id DESC LIMIT %s",
            (synthetic, limit),
        )

    def get(self, attempt_id: str, synthetic: bool) -> dict[str, Any] | None:
        attempt_identifier(attempt_id)
        return self.history.catalog.one(
            "SELECT * FROM analytical_run_diagnostics WHERE attempt_id=%s AND synthetic=%s",
            (attempt_id, synthetic),
        )

"""Durable capture planning cadence, independent of analytical orchestration."""

from __future__ import annotations

import argparse
import signal
import threading
from datetime import datetime, timedelta

from .catalog import InvalidationPending
from .contracts import canonical
from .pipeline import Pipeline


class CaptureCoordinator:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.catalog = pipeline.catalog

    def status(self, synthetic: bool = False) -> dict | None:
        return self.catalog.one(
            "SELECT * FROM capture_planner_state WHERE synthetic=%s", (synthetic,)
        )

    def tick(self, synthetic: bool = False, *, budget: int | None = None) -> dict:
        scope = "true" if synthetic else "false"
        lock = "hashtextextended(current_schema()||':capture-planner:'||%s,0)"
        with self.catalog.pool.connection() as owner:
            acquired = owner.execute(
                f"SELECT pg_try_advisory_lock({lock}) AS acquired", (scope,)
            ).fetchone()
            owner.commit()
            if not acquired or not acquired["acquired"]:
                return {"status": "coordinator_busy", "synthetic": synthetic}
            try:
                return self._plan(synthetic, budget)
            finally:
                owner.execute(f"SELECT pg_advisory_unlock({lock})", (scope,))
                owner.commit()

    def _plan(self, synthetic: bool, budget: int | None) -> dict:
        self.catalog.execute(
            "INSERT INTO capture_planner_state(synthetic) VALUES(%s) ON CONFLICT DO NOTHING",
            (synthetic,),
        )
        due = self.catalog.one(
            "UPDATE capture_planner_state SET last_started_at=clock_timestamp(),last_error=NULL "
            "WHERE synthetic=%s AND next_due_at<=clock_timestamp() RETURNING last_started_at",
            (synthetic,),
        )
        if not due:
            return {"status": "not_due", "synthetic": synthetic}
        try:
            result = self.pipeline.schedule(
                self.pipeline.settings.scheduler_request_budget if budget is None else budget,
                synthetic=synthetic,
            )
        except Exception as error:
            # Keep the original due boundary; a restarted planner retries it.
            self.catalog.execute(
                "UPDATE capture_planner_state SET last_error=%s WHERE synthetic=%s",
                (type(error).__name__, synthetic),
            )
            return {
                "status": "invalidation_pending"
                if isinstance(error, InvalidationPending)
                else "planning_failed",
                "synthetic": synthetic,
                "error": type(error).__name__,
            }
        started: datetime = due["last_started_at"]
        period = self.pipeline.settings.collection_schedule_minutes * 60
        next_due = started + timedelta(seconds=period - started.timestamp() % period)
        self.catalog.execute(
            "UPDATE capture_planner_state SET next_due_at=%s,last_completed_at=clock_timestamp(),"
            "last_error=NULL,last_scheduled_count=%s WHERE synthetic=%s",
            (next_due, result["scheduled"], synthetic),
        )
        return {"status": "planned", "synthetic": synthetic, "result": result}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=2)
    args = parser.parse_args(argv)
    if not 0 < args.poll_seconds <= 60:
        parser.error("poll-seconds must be positive and at most 60")
    stop = threading.Event()
    if args.loop:
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
    with Pipeline.from_env() as pipeline:
        coordinator = CaptureCoordinator(pipeline)
        while not stop.is_set():
            result = coordinator.tick(args.synthetic)
            print(canonical(result), flush=True)
            if not args.loop:
                return 1 if result["status"] == "planning_failed" else 0
            stop.wait(args.poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Capture planning has its own durable cadence, independent of analysis jobs."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from sm_trending_products.capture_coordinator import CaptureCoordinator
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_two_planners_only_admit_one_due_tick_and_restart_keeps_cadence(pipeline):
    def tick(_):
        with Pipeline(pipeline.settings) as independent:
            return CaptureCoordinator(independent).tick(synthetic=True, budget=0)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(tick, range(2)))
    assert [result["status"] for result in results].count("planned") == 1
    assert all(result["status"] in {"planned", "not_due", "coordinator_busy"} for result in results)
    before = CaptureCoordinator(pipeline).status(True)
    assert before["last_completed_at"] is not None
    with Pipeline(pipeline.settings) as restarted:
        coordinator = CaptureCoordinator(restarted)
        assert coordinator.tick(synthetic=True, budget=0)["status"] == "not_due"
        assert coordinator.status(True) == before
        assert coordinator.tick(synthetic=False, budget=0)["status"] == "planned"


def test_planning_failure_does_not_advance_the_due_window(pipeline, monkeypatch):
    coordinator = CaptureCoordinator(pipeline)

    def unavailable(*args, **kwargs):
        raise RuntimeError("never-persist-this-private-detail")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline, "schedule", unavailable)
        result = coordinator.tick(synthetic=True, budget=0)
    assert result == {"status": "planning_failed", "synthetic": True, "error": "RuntimeError"}
    state = coordinator.status(True)
    assert state["last_completed_at"] is None
    assert state["last_error"] == "RuntimeError"
    assert "private-detail" not in str(state)
    assert coordinator.tick(synthetic=True, budget=0)["status"] == "planned"


def test_held_planner_lock_never_blocks_a_replacement(pipeline):
    with pipeline.catalog.pool.connection() as holder:
        holder.execute(
            "SELECT pg_advisory_lock(hashtextextended(current_schema()||':capture-planner:true',0))"
        )
        holder.commit()
        try:
            result = CaptureCoordinator(pipeline).tick(synthetic=True, budget=0)
            assert result == {"status": "coordinator_busy", "synthetic": True}
        finally:
            holder.execute(
                "SELECT pg_advisory_unlock(hashtextextended("
                "current_schema()||':capture-planner:true',0))"
            )
            holder.commit()
    assert CaptureCoordinator(pipeline).tick(synthetic=True, budget=0)["status"] == "planned"

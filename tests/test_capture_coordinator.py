"""Capture planning has its own durable cadence, independent of analysis jobs."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import psycopg
import pytest

from sm_trending_products.capture_coordinator import CaptureCoordinator
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.scheduler import Scheduler

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
        patch.setattr(Scheduler, "schedule", unavailable)
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


def test_lost_planner_backend_cannot_mutate_after_replacement_commits(pipeline, monkeypatch):
    entered, release = Event(), Event()
    original = Scheduler.schedule
    owner_pids = []

    def pause_owner(self, *args, **kwargs):
        if not owner_pids:
            owner_pids.append(kwargs["connection"].info.backend_pid)
            entered.set()
            assert release.wait(8)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Scheduler, "schedule", pause_owner)
    with Pipeline(pipeline.settings) as old, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(CaptureCoordinator(old).tick, synthetic=True, budget=0)
        try:
            assert entered.wait(8)
            with psycopg.connect(
                pipeline.settings.database_url.get_secret_value(), autocommit=True
            ) as admin:
                admin.execute("SELECT pg_terminate_backend(%s)", (owner_pids[0],))
            replacement = CaptureCoordinator(pipeline)
            assert replacement.tick(synthetic=True, budget=0)["status"] == "planned"
            committed = replacement.status(True)
        finally:
            release.set()
        assert pending.result(timeout=8)["status"] == "planning_failed"
        assert replacement.status(True) == committed


def test_delayed_discovery_keeps_prior_due_instead_of_a_new_sampling_window(pipeline):
    policy = {
        "discovery_queries": [
            {
                "source": "tiktok_display",
                "account_id": "fixture-account",
                "operation": "list",
                "interval_hours": 1,
            }
        ]
    }
    initial = pipeline.schedule(1, synthetic=True, policy=policy)
    task_id = initial["task_ids"][0]
    # Simulate a legacy task admitted two hours ago, before original_due metadata
    # existed. A later coordinator must retain its one-hour-overdue next window.
    prior = pipeline.catalog.one(
        "UPDATE collection_tasks SET state='done',created_at=clock_timestamp()-interval '2 hours',"
        "schedule_reason=schedule_reason-'original_due_at' WHERE task_id=%s RETURNING created_at",
        (task_id,),
    )
    expected = prior["created_at"] + timedelta(hours=1)
    result = CaptureCoordinator(pipeline).tick(synthetic=True, budget=1)
    assert result["status"] == "planned" and result["result"]["scheduled"] == 1
    obligation = pipeline.capture_contracts.declare(result["result"]["task_ids"][0])
    assert obligation["original_due_at"] == expected
    assert obligation["deadline_at"] is None  # No invented retrospective contract.

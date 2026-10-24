"""Cancel an absent unregistered candidate only at the authoritative claim barrier."""

import pytest
from test_release_control import Evidence, Host, release

from sm_trending_products.contracts import now
from sm_trending_products.release_control import DeploymentController, ReleaseBlocked
from sm_trending_products.release_host import CatalogWorkerGate
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "condition",
    ["absent_empty", "absent_accepted_lease", "unregistered_container", "running_registry"],
)
def test_prepared_candidate_cancellation_requires_absence_and_zero_accepted_work(
    pipeline, condition
):
    class InterruptedStart(Host):
        def start(self, target, manifest, slot, worker_id):
            if condition == "unregistered_container":
                self.running[slot] = manifest.image
            raise ReleaseBlocked("fixture_start_failed")

    host = InterruptedStart()
    control = WorkerControl(pipeline.catalog)
    control.register("old", synthetic=True)
    controller = DeploymentController(
        pipeline.catalog, host, CatalogWorkerGate(control), Evidence(), allow_synthetic=True
    )
    plan = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=35,
        synthetic=True,
    )
    worker_id = plan["candidate_worker_id"]
    if condition == "running_registry":
        control.register(worker_id, synthetic=True)
    accepted = None
    if condition == "absent_accepted_lease":
        pipeline.catalog.enqueue(
            "tiktok_display", "fixture-account", "query", {}, True, due_at=now()
        )
        accepted = pipeline.catalog.claim(worker_id, 180, True, 8)
        assert accepted
    assert controller.apply(plan["release_id"])["phase"] == "prepared"
    result = controller.rollback(plan["release_id"])
    assert control.status("old")["state"] == "running" and "blue" in host.running
    assert "retire" not in host.events
    if condition == "absent_empty":
        assert result["status"] == "aborted"
        tombstone = control.status(worker_id)
        assert tombstone["state"] == "stopped" and tombstone["owned_leases"] == 0
        assert tombstone["drain_requested_at"] is not None
        late = control.register(worker_id, synthetic=True)
        assert late["state"] == "draining"
        pipeline.catalog.enqueue(
            "tiktok_display", "fixture-account", "query", {}, True, due_at=now()
        )
        assert (
            pipeline.catalog.claim(worker_id, 180, True, 8, worker_generation=late["generation"])
            is None
        )
    else:
        expected = (
            "candidate_identity_unavailable"
            if condition == "running_registry"
            else "candidate_worker_unavailable"
        )
        assert result["status"] == "blocked" and result["reason"] == expected
        if condition == "running_registry":
            assert control.status(worker_id)["state"] == "running"
        else:
            assert control.status(worker_id) is None
        if accepted:
            task = pipeline.catalog.one(
                "SELECT * FROM collection_tasks WHERE task_id=%s", (accepted["task_id"],)
            )
            assert task["state"] == "leased" and task["lease_owner"] == worker_id


def test_cancellation_resumes_after_pause_commit_before_abort_checkpoint(pipeline, monkeypatch):
    from test_release_control import ProcessDeath

    class AbsentHost(Host):
        def start(self, target, manifest, slot, worker_id):
            raise ReleaseBlocked("fixture_start_failed")

    host = AbsentHost()
    workers = WorkerControl(pipeline.catalog)
    workers.register("old", synthetic=True)
    controller = DeploymentController(
        pipeline.catalog, host, CatalogWorkerGate(workers), Evidence(), allow_synthetic=True
    )
    plan = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=35,
        synthetic=True,
    )
    assert controller.apply(plan["release_id"])["phase"] == "prepared"
    original = controller._record

    def death_before_abort_checkpoint(connection, row, fence, **changes):
        if changes.get("status") == "aborted":
            raise ProcessDeath()
        return original(connection, row, fence, **changes)

    monkeypatch.setattr(controller, "_record", death_before_abort_checkpoint)
    with pytest.raises(ProcessDeath):
        controller.rollback(plan["release_id"])
    tombstone = workers.status(plan["candidate_worker_id"])
    assert tombstone["state"] == "stopped" and tombstone["owned_leases"] == 0
    restarted = DeploymentController(
        pipeline.catalog, host, CatalogWorkerGate(workers), Evidence(), allow_synthetic=True
    )
    assert restarted.rollback(plan["release_id"])["status"] == "aborted"
    assert restarted.rollback(plan["release_id"])["status"] == "aborted"
    assert host.running == {"blue": release("a").manifest.image}
    assert "retire" not in host.events
    late = workers.register(plan["candidate_worker_id"], synthetic=True)
    assert late["state"] == "draining"
    pipeline.catalog.enqueue("tiktok_display", "fixture-account", "query", {}, True, due_at=now())
    assert (
        pipeline.catalog.claim(
            plan["candidate_worker_id"], 180, True, 8, worker_generation=late["generation"]
        )
        is None
    )

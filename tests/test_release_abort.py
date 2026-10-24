"""Cancel an absent unregistered candidate only at the authoritative claim barrier."""

import pytest
from test_release_control import Evidence, Host, release

from sm_trending_products.contracts import now
from sm_trending_products.release_control import DeploymentController, ReleaseBlocked
from sm_trending_products.release_host import CatalogWorkerGate
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "condition", ["absent_empty", "absent_accepted_lease", "unregistered_container"]
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
        assert result["status"] == "blocked" and result["reason"] == "candidate_worker_unavailable"
        assert control.status(worker_id) is None
        if accepted:
            task = pipeline.catalog.one(
                "SELECT * FROM collection_tasks WHERE task_id=%s", (accepted["task_id"],)
            )
            assert task["state"] == "leased" and task["lease_owner"] == worker_id

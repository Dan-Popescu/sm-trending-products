"""Rolling release state through its public deployment interface and native SQL."""

from contextlib import contextmanager

import pytest

from sm_trending_products.release_control import (
    DeploymentController,
    ReleaseBlocked,
    ReleaseManifest,
    VerifiedRelease,
)

pytestmark = pytest.mark.integration


def release(character):
    return VerifiedRelease.for_controlled_test(
        ReleaseManifest(
            image=f"ghcr.io/example/pipeline@sha256:{character * 64}",
            revision=character * 40,
            schema_min=28,
            schema_max=40,
            capture_protocol=1,
        )
    )


class Host:
    def __init__(self):
        self.events = []
        self.running = {"blue": release("a").manifest.image}
        self.fail = None

    @contextmanager
    def exclusive(self, target):
        yield

    def prepare(self, target, manifest, slot):
        self.events.append("prepare")

    def start(self, target, manifest, slot, worker_id):
        self.events.append("start")
        self.running[slot] = manifest.image

    def healthy(self, target, manifest, slot, worker_id):
        return (
            not (self.fail == "health" and slot == "green")
            and self.running.get(slot) == manifest.image
        )

    def absent(self, target, slot):
        return slot not in self.running

    def retire(self, target, manifest, slot, worker_id):
        self.events.append("retire")
        self.running.pop(slot, None)


class Workers:
    def __init__(self):
        self.drained = set()
        self.busy = False
        self.generations = {}

    def status(self, worker_id):
        return {"generation": self.generations.get(worker_id, 1), "state": "running"}

    def request_drain(self, worker_id, generation):
        assert generation == self.generations.get(worker_id, 1)
        self.drained.add(worker_id)
        return {"drained": not self.busy}

    def resume(self, worker_id, generation):
        assert generation == self.generations.get(worker_id, 1)
        self.drained.discard(worker_id)


class Evidence:
    def __init__(self):
        self.ready = False

    def verify(self, **kwargs):
        if not self.ready:
            return None
        return {"controlled_fixture": True}


def test_release_keeps_predecessor_until_capture_proof_and_drain(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    result = controller.apply(planned["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "capture_proof_missing"
    assert "blue" in host.running and not workers.drained
    evidence.ready = True
    workers.busy = True
    result = controller.apply(planned["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "predecessor_busy"
    assert set(host.running) == {"blue", "green"}
    workers.busy = False
    result = controller.apply(planned["release_id"])
    assert result["status"] == "completed"
    assert set(host.running) == {"green"}
    assert host.events.index("retire") > host.events.index("start")
    assert controller.apply(planned["release_id"])["status"] == "completed"


def test_unhealthy_replacement_does_not_drain_or_retire_old(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    host.fail = "health"
    evidence.ready = True
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    result = controller.apply(planned["release_id"])
    assert result["reason"] == "replacement_unhealthy"
    assert "blue" in host.running and not workers.drained
    assert "retire" not in host.events


class ProcessDeath(BaseException):
    pass


@pytest.mark.parametrize("boundary", ["prepare", "start", "retire"])
def test_restart_after_host_action_preserves_and_finishes_the_same_release(pipeline, boundary):
    class InterruptedHost(Host):
        crashed = False

        def interrupt(self, operation):
            if operation == boundary and not self.crashed:
                self.crashed = True
                raise ProcessDeath()

        def prepare(self, *args):
            super().prepare(*args)
            self.interrupt("prepare")

        def start(self, *args):
            super().start(*args)
            self.interrupt("start")

        def retire(self, *args):
            super().retire(*args)
            self.interrupt("retire")

    host, workers, evidence = InterruptedHost(), Workers(), Evidence()
    evidence.ready = True
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    with pytest.raises(ProcessDeath):
        controller.apply(planned["release_id"])
    if boundary != "retire":
        assert "blue" in host.running
    restarted = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    assert restarted.apply(planned["release_id"])["status"] == "completed"
    assert set(host.running) == {"green"}


def test_unverified_or_incompatible_release_is_rejected_before_host_mutation(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    controller = DeploymentController(pipeline.catalog, host, workers, evidence)
    with pytest.raises(ReleaseBlocked, match="release_trust_unconfigured"):
        controller.plan(
            release("b"),
            target="fixture",
            current=release("a"),
            current_slot="blue",
            current_worker_id="old",
            schema_version=32,
        )
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    with pytest.raises(ReleaseBlocked, match="incompatible_schema"):
        controller.plan(
            release("b"),
            target="fixture",
            current=release("a"),
            current_slot="blue",
            current_worker_id="old",
            schema_version=50,
            synthetic=True,
        )
    assert host.events == [] and not workers.drained


def test_application_rollback_plans_reverse_handover_without_rewinding_sql(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    evidence.ready = True
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    assert controller.apply(planned["release_id"])["status"] == "completed"
    pipeline.create_product("created-by-new-release", "Keep newer data", synthetic=True)
    reverse = controller.rollback(planned["release_id"])
    assert reverse["release_id"] != planned["release_id"]
    assert controller.apply(reverse["release_id"])["status"] == "completed"
    assert set(host.running) == {"blue"}
    assert pipeline.products(True)[0]["product_id"] == "created-by-new-release"


def test_failed_current_can_be_replaced_by_the_verified_prior_release(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    evidence.ready = True
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    assert controller.apply(planned["release_id"])["status"] == "completed"
    host.fail = "health"
    reverse = controller.rollback(planned["release_id"])
    assert controller.apply(reverse["release_id"])["status"] == "completed"
    assert set(host.running) == {"blue"}


def test_aborting_a_plan_never_starts_or_stops_a_capture_slot(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    assert controller.rollback(planned["release_id"])["status"] == "aborted"
    assert host.events == [] and set(host.running) == {"blue"}
    retry = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
        request_id="explicit-second-attempt",
    )
    assert retry["release_id"] != planned["release_id"]


def test_candidate_restart_rebinds_proof_and_never_drains_a_new_predecessor_generation(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    controller.apply(planned["release_id"])
    workers.generations[planned["candidate_worker_id"]] = 2
    evidence.ready = True
    result = controller.apply(planned["release_id"])
    assert result["status"] == "completed" and result["candidate_generation"] == 2

    reverse = controller.rollback(planned["release_id"])
    workers.generations[planned["candidate_worker_id"]] = 3
    result = controller.apply(reverse["release_id"])
    assert result["reason"] == "predecessor_generation_changed"
    assert result["status"] == "blocked" and "green" in host.running


def test_blocked_cancellation_is_durable_and_apply_never_resumes_forward_rollout(pipeline):
    host, workers, evidence = Host(), Workers(), Evidence()
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    planned = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    assert controller.apply(planned["release_id"])["reason"] == "capture_proof_missing"
    workers.busy = True
    cancellation = controller.rollback(planned["release_id"])
    assert cancellation["reason"] == "candidate_busy" and cancellation["abort_requested"]
    evidence.ready = True
    assert controller.apply(planned["release_id"])["reason"] == "candidate_busy"
    assert "old" not in workers.drained and "blue" in host.running
    workers.busy = False
    assert controller.apply(planned["release_id"])["status"] == "aborted"
    assert set(host.running) == {"blue"}

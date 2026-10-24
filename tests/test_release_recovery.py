from test_release_control import Evidence, Host, Workers, release

from sm_trending_products.release_control import DeploymentController


class StatefulWorkers(Workers):
    down = set()

    def status(self, worker_id):
        result = super().status(worker_id)
        result["state"] = (
            "stopped"
            if worker_id in self.down
            else ("draining" if worker_id in self.drained else "running")
        )
        return result


def test_replacement_dies_after_busy_drain_leaves_predecessor_unadmitted(pipeline):
    host, workers, evidence = Host(), StatefulWorkers(), Evidence()
    evidence.ready = True
    workers.busy = True
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    plan = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    first = controller.apply(plan["release_id"])
    assert first["reason"] == "predecessor_busy"
    workers.down = {plan["candidate_worker_id"]}
    host.running.pop("green")
    result = controller.apply(plan["release_id"])
    assert result["reason"] == "replacement_worker_unavailable"
    assert workers.status("old")["state"] == "running"


def test_candidate_restart_during_drain_retires_predecessor_with_old_proof(pipeline):
    host, workers, evidence = Host(), StatefulWorkers(), Evidence()
    evidence.ready = True
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    plan = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=32,
        synthetic=True,
    )
    original = workers.request_drain

    def restart_candidate(worker_id, generation):
        workers.generations[plan["candidate_worker_id"]] = 2
        return original(worker_id, generation)

    workers.request_drain = restart_candidate
    result = controller.apply(plan["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "replacement_generation_changed"
    assert result["candidate_generation"] == 1
    assert workers.status(plan["candidate_worker_id"])["generation"] == 2
    assert "blue" in host.running and workers.status("old")["state"] == "running"

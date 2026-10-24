"""Ordinary restart recovery at the two private checkpoint writes."""

import hashlib
from types import SimpleNamespace

import pytest
from psycopg.types.json import Jsonb
from test_release_authorization import authorized
from test_release_control import ProcessDeath, release

from sm_trending_products.release_cli import HostRuntime
from sm_trending_products.release_control import DeploymentRequest, release_manifest_bytes

pytestmark = pytest.mark.integration


def test_new_signed_request_supersedes_rollback_after_high_water_checkpoint_crash(
    pipeline, tmp_path
):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    evidence.ready = True
    controller.execution = authority.execution(row)
    assert controller.apply(row["release_id"])["status"] == "completed"
    row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (row["release_id"],)
    )
    authority.begin_rollback(row)
    reverse = controller.rollback(row["release_id"])
    reverse_row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (reverse["release_id"],)
    )
    authority.rollback(reverse_row, row)
    controller.execution = authority.execution(reverse_row)
    assert controller.apply(reverse["release_id"])["status"] == "completed"
    authority.complete_rollback(reverse["release_id"])
    assert authority.rollback_state()["completed"] is True
    active = pipeline.catalog.one("SELECT * FROM deployment_targets WHERE target='fixture'")
    plan = controller.plan(
        release("c"),
        target="fixture",
        current=release("a"),
        current_slot=active["active_slot"],
        current_worker_id=active["active_worker_id"],
        schema_version=35,
        synthetic=True,
        request_id="signed-2",
    )
    next_row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (plan["release_id"],)
    )
    request = DeploymentRequest(
        target="fixture",
        sequence=2,
        revision="c" * 40,
        manifest_sha256=hashlib.sha256(release_manifest_bytes(release("c").manifest)).hexdigest(),
    )
    proof = {"method": "cosign_bundle", "bundle": {"controlled_fixture": True}}
    write = authority._write

    def death_after_high_water(path, value):
        write(path, value)
        if path.name == "accepted-request.json":
            raise ProcessDeath()

    authority._write = death_after_high_water
    with pytest.raises(ProcessDeath):
        with pipeline.catalog.transaction() as connection:
            connection.execute(
                "INSERT INTO deployment_requests(target,sequence,request,proof,release_id) "
                "VALUES(%s,%s,%s,%s,%s)",
                (
                    "fixture",
                    2,
                    Jsonb(request.model_dump(mode="json")),
                    Jsonb(proof),
                    plan["release_id"],
                ),
            )
            authority.record_request(next_row, request, proof)
    authority._write = write
    assert pipeline.catalog.one("SELECT 1 FROM deployment_requests WHERE sequence=2") is None
    # The higher signed intent is durable. Polling must reach its recovery path,
    # rather than reject the completed old rollback before inspecting that intent.
    assert authority.rollback_state() is None
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture")
    runtime.authorization = authority
    runtime.trust = authority.trust
    runtime.controller = lambda current: controller
    runtime.pending_processing = lambda: False
    runtime.refresh_processing_release = lambda *args, **kwargs: {"status": "completed"}

    def no_network(*args):
        pytest.fail("The accepted signed intent must resume without another network request")

    runtime.reader = SimpleNamespace(request=no_network)
    result = runtime.pull()
    assert result["status"] == "completed" and result["release_id"] == plan["release_id"]
    assert pipeline.catalog.one("SELECT 1 FROM deployment_requests WHERE sequence=2")
    assert set(controller.host.running.values()) == {release("c").manifest.image}


def test_private_rollback_action_recovers_missing_routing_marker_before_processing(
    pipeline, tmp_path
):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    evidence.ready = True
    controller.execution = authority.execution(row)
    assert controller.apply(row["release_id"])["status"] == "completed"
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture")
    runtime.authorization = authority
    runtime.trust = authority.trust
    runtime.controller = lambda current: controller
    runtime.pending_processing = lambda: pytest.fail(
        "A durable rollback must precede blocked processing"
    )
    runtime.refresh_processing_release = lambda *args, **kwargs: {"status": "completed"}
    runtime.reader = SimpleNamespace(
        request=lambda *args: pytest.fail("Rollback must resume offline")
    )
    original = authority._write

    def death_before_marker(path, value):
        if path.name == "rollback-intent.json":
            raise ProcessDeath()
        original(path, value)

    authority._write = death_before_marker
    with pytest.raises(ProcessDeath):
        runtime.rollback(row["release_id"])
    authority._write = original
    assert authority.verify(row) == "rollback"
    assert authority.rollback_state()["release_id"] == row["release_id"]
    reverse = runtime.pull()
    assert reverse["release_id"] != row["release_id"]
    assert set(controller.host.running.values()) == {release("b").manifest.image}
    assert runtime.pull()["status"] == "completed"
    assert set(controller.host.running.values()) == {release("a").manifest.image}

"""An app-writable SQL journal cannot supply host deployment authority."""

import hashlib
import sys
from types import SimpleNamespace

import pytest
from psycopg.types.json import Jsonb
from test_release_control import Evidence, Host, Workers, release

from sm_trending_products.release_authorization import HostAuthorization
from sm_trending_products.release_cli import HostRuntime
from sm_trending_products.release_control import (
    DeploymentController,
    DeploymentRequest,
    ReleaseBlocked,
    ReleaseTrust,
    release_manifest_bytes,
)

pytestmark = pytest.mark.integration


def authorized(pipeline, tmp_path, *, before_seal=None):
    binary = tmp_path / "controlled-verifier"
    binary.write_text("#!" + sys.executable + "\nraise SystemExit(0)\n")
    binary.chmod(0o700)
    directory = tmp_path / "root-authority"
    directory.mkdir(mode=0o700)
    authority = HostAuthorization(
        directory,
        pipeline.catalog,
        ReleaseTrust("controlled-workflow", ("ghcr.io/example/pipeline",), str(binary)),
    )
    host, workers, evidence = Host(), Workers(), Evidence()
    controller = DeploymentController(
        pipeline.catalog, host, workers, evidence, allow_synthetic=True
    )
    plan = controller.plan(
        release("b"),
        target="fixture",
        current=release("a"),
        current_slot="blue",
        current_worker_id="old",
        schema_version=35,
        synthetic=True,
        request_id="signed-1",
    )
    row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (plan["release_id"],)
    )
    if before_seal is not None:
        before_seal(row)
        row = pipeline.catalog.one(
            "SELECT * FROM deployment_runs WHERE release_id=%s", (row["release_id"],)
        )
    request = DeploymentRequest(
        target="fixture",
        sequence=1,
        revision="b" * 40,
        manifest_sha256=hashlib.sha256(release_manifest_bytes(release("b").manifest)).hexdigest(),
    )
    proof = {"method": "cosign_bundle", "bundle": {"controlled_fixture": True}}
    pipeline.catalog.execute(
        "INSERT INTO deployment_requests(target,sequence,request,proof,release_id) "
        "VALUES(%s,%s,%s,%s,%s)",
        ("fixture", 1, Jsonb(request.model_dump(mode="json")), Jsonb(proof), row["release_id"]),
    )
    authority.record_request(row, request, proof)
    return authority, controller, row, evidence, request


def test_private_authority_controls_cancellation_and_explicit_reverse_handover(pipeline, tmp_path):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    assert authority.verify(row) == "apply"
    evidence.ready = True
    assert controller.apply(row["release_id"])["status"] == "completed"
    row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (row["release_id"],)
    )
    reverse = controller.rollback(row["release_id"])
    reverse_row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (reverse["release_id"],)
    )
    with pytest.raises(ReleaseBlocked, match="deployment_authorization_missing"):
        authority.verify(reverse_row)
    authority.rollback(reverse_row, row)
    assert authority.verify(reverse_row) == "apply"
    authority.cancel(reverse_row)
    assert authority.verify(reverse_row) == "abort"
    assert reverse_row["abort_requested"] is False  # SQL progress cannot undo private cancellation.


def test_private_sequence_survives_lost_sql_acceptance_and_refuses_changed_intent(
    pipeline, tmp_path
):
    authority, _, row, _, request = authorized(pipeline, tmp_path)
    pipeline.catalog.execute("DELETE FROM deployment_requests")
    with pytest.raises(ReleaseBlocked, match="deployment_request_sequence_conflict"):
        authority.sequence(request.model_copy(update={"revision": "c" * 40}))
    with pytest.raises(ReleaseBlocked, match="deployment_request_journal_changed"):
        authority.verify(row)
    changed = {**row, "prior_generation": 2}
    with pytest.raises(ReleaseBlocked, match="deployment_plan_identity_mismatch"):
        authority.record_request(changed, request, {"method": "cosign_bundle", "bundle": {}})


def test_authorized_source_handover_refreshes_all_roles_and_reverse_too(pipeline, tmp_path):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture", state_directory=authority.directory)
    runtime.authorization = authority
    runtime.controller = lambda current: controller
    refreshed = []
    options = []

    def processing(release=None, **kwargs):
        refreshed.append(release)
        options.append(kwargs)
        return {"status": "completed", "target": "fixture"}

    runtime.refresh_processing_release = processing
    evidence.ready = True
    result = runtime.apply(row["release_id"])
    assert result["status"] == "completed" and result["capture_phase"] == "retired"
    assert refreshed[-1].manifest == release("b").manifest
    assert options[-1]["previous"].manifest == release("a").manifest
    assert options[-1]["supersede"] is False
    # Repeating an accepted request repairs the source-to-processing crash gap.
    runtime.apply(row["release_id"])
    assert len(refreshed) == 2
    reverse = runtime.rollback(row["release_id"])
    runtime.pending_processing = lambda: True
    result = runtime.pull()
    assert result["release_id"] == reverse["release_id"]
    assert refreshed[-1].manifest == release("a").manifest
    assert options[-1]["previous"].manifest == release("b").manifest
    assert options[-1]["supersede"] is True
    result = runtime.pull()
    assert result["status"] == "completed" and refreshed[-1] is None


def test_sealed_execution_rejects_sql_intent_swap_between_cli_and_controller(pipeline, tmp_path):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    controller.host.running["green"] = release("b").manifest.image
    evidence.ready = True
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture", state_directory=authority.directory)
    runtime.authorization = authority

    def swapped(current):
        pipeline.catalog.execute(
            "UPDATE deployment_runs SET candidate_manifest=prior_manifest,"
            "prior_manifest=candidate_manifest,"
            "candidate_proof=prior_proof,prior_proof=candidate_proof,prior_slot='green',"
            "prior_worker_id=candidate_worker_id,candidate_worker_id=prior_worker_id,"
            "prior_generation=1,candidate_generation=1 WHERE release_id=%s",
            (row["release_id"],),
        )
        pipeline.catalog.execute(
            "UPDATE deployment_targets SET active_manifest=%s,active_proof=%s,active_slot='green',"
            "active_worker_id=%s WHERE target='fixture'",
            (
                Jsonb(row["candidate_manifest"]),
                Jsonb(row["candidate_proof"]),
                row["candidate_worker_id"],
            ),
        )
        return controller

    runtime.controller = swapped
    with pytest.raises(ReleaseBlocked, match="deployment_intent_changed"):
        runtime.apply(row["release_id"])
    assert controller.host.events == [] and set(controller.host.running) == {"blue", "green"}


@pytest.mark.parametrize("forgery", ["abort", "completed"])
def test_sql_progress_cannot_cancel_or_manufacture_a_completed_source_handover(
    pipeline, tmp_path, forgery
):
    authority, controller, row, _, _ = authorized(pipeline, tmp_path)
    controller.host.running["green"] = release("b").manifest.image
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture", state_directory=authority.directory)
    runtime.authorization = authority
    runtime.controller = lambda current: controller
    runtime.refresh_processing_release = lambda *args, **kwargs: pytest.fail(
        "Forged completion refreshed processing"
    )
    if forgery == "abort":
        pipeline.catalog.execute(
            "UPDATE deployment_runs SET abort_requested=true,phase='started',candidate_generation=1"
        )
    else:
        pipeline.catalog.execute("UPDATE deployment_runs SET phase='retired',status='completed'")
    result = runtime.apply(row["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "capture_proof_missing"
    assert (
        set(controller.host.running) == {"blue", "green"} and "retire" not in controller.host.events
    )


@pytest.mark.parametrize("boundary", ["prepare", "start", "committed_checkpoint", "retire"])
def test_private_execution_resumes_after_host_and_cross_journal_crashes(
    pipeline, tmp_path, boundary
):
    from test_release_control import ProcessDeath

    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    execution = authority.execution(row)
    controller.execution = execution
    evidence.ready = True
    if boundary == "committed_checkpoint":
        original = execution.checkpoint
        interrupted = []

        def checkpoint(current):
            if current["phase"] == "committed" and not interrupted:
                interrupted.append(True)
                raise ProcessDeath()
            original(current)

        execution.checkpoint = checkpoint
    else:
        original = getattr(controller.host, boundary)
        interrupted = []

        def action(*args):
            original(*args)
            if not interrupted:
                interrupted.append(True)
                raise ProcessDeath()

        setattr(controller.host, boundary, action)
    with pytest.raises(ProcessDeath):
        controller.apply(row["release_id"])
    fresh = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (row["release_id"],)
    )
    controller.execution = authority.execution(fresh)
    result = controller.apply(row["release_id"])
    assert result["status"] == "completed" and set(controller.host.running) == {"green"}


@pytest.mark.parametrize("forgery", ["completed", "candidate_identity"])
def test_first_private_seal_cannot_promote_forged_sql_progress_or_worker_identity(
    pipeline, tmp_path, forgery
):
    def forged(row):
        if forgery == "completed":
            pipeline.catalog.execute(
                "UPDATE deployment_runs SET phase='retired',status='completed'"
            )
        else:
            pipeline.catalog.execute(
                "UPDATE deployment_runs SET candidate_worker_id=prior_worker_id"
            )

    if forgery == "candidate_identity":
        with pytest.raises(ReleaseBlocked, match="deployment_plan_identity_mismatch"):
            authorized(pipeline, tmp_path, before_seal=forged)
        return
    authority, controller, row, _, _ = authorized(pipeline, tmp_path, before_seal=forged)
    controller.execution = authority.execution(row)
    result = controller.apply(row["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "capture_proof_missing"
    assert "retire" not in controller.host.events and "blue" in controller.host.running


def test_explicit_rollback_uses_private_phase_before_persisting_cancellation(pipeline, tmp_path):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    controller.execution = authority.execution(row)
    assert controller.apply(row["release_id"])["reason"] == "capture_proof_missing"
    pipeline.catalog.execute("UPDATE deployment_runs SET phase='retired',status='completed'")
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture", state_directory=authority.directory)
    runtime.authorization = authority
    runtime.controller = lambda current: controller
    controller.workers.busy = True
    assert runtime.rollback(row["release_id"])["reason"] == "candidate_busy"
    evidence.ready = True
    assert runtime.apply(row["release_id"])["reason"] == "candidate_busy"
    assert authority.verify(row) == "abort" and "blue" in controller.host.running


def test_rollback_after_sql_commit_gap_survives_another_crash_and_preempts_processing(
    pipeline, tmp_path
):
    from test_release_control import ProcessDeath

    authority, controller, row, evidence, request = authorized(pipeline, tmp_path)
    execution = authority.execution(row)
    controller.execution = execution
    evidence.ready = True
    checkpoint = execution.checkpoint

    def interrupted_checkpoint(current):
        if current["phase"] == "committed":
            raise ProcessDeath()
        checkpoint(current)

    execution.checkpoint = interrupted_checkpoint
    with pytest.raises(ProcessDeath):
        controller.apply(row["release_id"])
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture", state_directory=authority.directory)
    runtime.authorization = authority
    runtime.trust = authority.trust
    runtime.controller = lambda current: controller
    runtime.pending_processing = lambda: pytest.fail("Rollback must preempt blocked processing")
    refreshed = []

    def processing(release=None, **kwargs):
        refreshed.append(release)
        return {"status": "completed", "target": "fixture"}

    runtime.refresh_processing_release = processing
    apply = controller.apply
    interrupted = []

    def crash_after_private_rollback_intent(release_id):
        if not interrupted:
            interrupted.append(True)
            raise ProcessDeath()
        return apply(release_id)

    controller.apply = crash_after_private_rollback_intent
    with pytest.raises(ProcessDeath):
        runtime.rollback(row["release_id"])
    assert authority.verify(row) == "rollback"
    assert authority.rollback_state()["release_id"] == row["release_id"]
    assert "green" in controller.host.running  # B keeps acquiring throughout recovery.
    reverse = runtime.pull()
    assert reverse["release_id"] != row["release_id"]
    assert set(controller.host.running) == {"green"}
    result = runtime.pull()
    assert result["status"] == "completed" and set(controller.host.running) == {"blue"}
    assert refreshed[-1].manifest == release("a").manifest
    assert authority.rollback_state()["completed"] is True
    # The unchanged old signed B request must not undo the completed rollback.
    runtime.pending_processing = lambda: False
    runtime.reader = SimpleNamespace(request=lambda trust: (request, {}))
    result = runtime.pull()
    assert result["status"] == "completed" and refreshed[-1].manifest == release("a").manifest
    # A genuinely newer signed request can supersede the private rollback.
    active = runtime.active()
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
    newer = DeploymentRequest(
        target="fixture",
        sequence=2,
        revision="c" * 40,
        manifest_sha256=hashlib.sha256(release_manifest_bytes(release("c").manifest)).hexdigest(),
    )
    proof = {"method": "cosign_bundle", "bundle": {"controlled_fixture": True}}
    pipeline.catalog.execute(
        "INSERT INTO deployment_requests(target,sequence,request,proof,release_id) "
        "VALUES(%s,%s,%s,%s,%s)",
        ("fixture", 2, Jsonb(newer.model_dump(mode="json")), Jsonb(proof), plan["release_id"]),
    )
    authority.record_request(next_row, newer, proof)
    assert authority.rollback_state() is None
    old_reverse = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (reverse["release_id"],)
    )
    with pytest.raises(ReleaseBlocked, match="deployment_rollback_superseded"):
        authority.verify(old_reverse)
    result = runtime.pull()
    assert result["status"] == "completed" and refreshed[-1].manifest == release("c").manifest

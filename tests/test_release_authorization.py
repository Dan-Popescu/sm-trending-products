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


def authorized(pipeline, tmp_path):
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
    )
    row = pipeline.catalog.one(
        "SELECT * FROM deployment_runs WHERE release_id=%s", (plan["release_id"],)
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
    with pytest.raises(ReleaseBlocked, match="deployment_intent_changed"):
        authority.record_request(changed, request, {"method": "cosign_bundle", "bundle": {}})


def test_authorized_source_handover_refreshes_all_roles_and_reverse_too(pipeline, tmp_path):
    authority, controller, row, evidence, _ = authorized(pipeline, tmp_path)
    runtime = object.__new__(HostRuntime)
    runtime.catalog = pipeline.catalog
    runtime.config = SimpleNamespace(target="fixture")
    runtime.authorization = authority
    runtime.controller = lambda current: controller
    refreshed = []

    def processing(release=None):
        refreshed.append(release)
        return {"status": "completed", "target": "fixture"}

    runtime.refresh_processing_release = processing
    evidence.ready = True
    result = runtime.apply(row["release_id"])
    assert result["status"] == "completed" and result["capture_phase"] == "retired"
    assert refreshed[-1].manifest == release("b").manifest
    # Repeating an accepted request repairs the source-to-processing crash gap.
    runtime.apply(row["release_id"])
    assert len(refreshed) == 2
    reverse = runtime.rollback(row["release_id"])
    runtime.apply(reverse["release_id"])
    assert refreshed[-1].manifest == release("a").manifest
    runtime.pending_processing = lambda: True
    result = runtime.pull()
    assert result["status"] == "completed" and refreshed[-1] is None

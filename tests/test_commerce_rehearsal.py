"""Complete rehearsal through public modules, native PostgreSQL and HTTP S3."""

import json
import runpy
from pathlib import Path

import pytest

from sm_trending_products.commerce import LaunchDraft, LaunchService
from sm_trending_products.commerce_delivery import CommerceDelivery
from sm_trending_products.commerce_orders import CommerceOrders
from sm_trending_products.commerce_rehearsal import (
    RehearsalError,
    main,
    rehearse,
    verify_rehearsal,
)


def test_complete_synthetic_rehearsal_and_fresh_evidence_validation(pipeline, monkeypatch):
    report = rehearse(pipeline, synthetic=True, run_id="complete-run")
    assert report["status"] == "completed", report
    assert report["execution_mode"] == "fresh_run"
    assert report["stages"]["history"] == {"video_count": 3, "observation_count": 59}
    assert [row["window_hours"] for row in report["stages"]["trends"]] == [24, 72, 168]
    assert all(row["observed_growth_lower"] > 0 for row in report["stages"]["trends"])
    assert report["stages"]["matching"]["conflict_count"] >= 1
    assert report["stages"]["transcript"]["analysis_reused"]
    assert report["counts"]["controlled_http_methods"]["POST"] == 1
    assert report["counts"]["controlled_http_requests"] <= 40
    assert report["counts"]["live_source_requests"] == report["counts"]["paid_provider_calls"] == 0
    assert report["stages"]["financials"]["reviewed_profit"] == "40.00"
    assert report["stages"]["financials"]["late_status"] == "censored"
    assert report["stages"]["fulfillment"]["notification_delivered"] is False
    assert report["stages"]["fulfillment"]["carrier_verified"] is False

    def forbidden(*args, **kwargs):
        raise AssertionError("Read-only replay/verification attempted execution")

    monkeypatch.setattr("sm_trending_products.commerce_rehearsal._shop_server", forbidden)
    monkeypatch.setattr(pipeline, "mature", forbidden)
    replay = rehearse(pipeline, synthetic=True, run_id="complete-run")
    assert replay["execution_mode"] == "replayed_completed_run"
    assert replay["fresh_http_requests"] == 0
    assert replay["stages"] == report["stages"]
    checked = verify_rehearsal(pipeline, synthetic=True, run_id="complete-run")
    assert checked["status"] == "verified", checked
    assert checked["execution_mode"] == "fresh_evidence_validation"
    assert checked["controlled_http_requests"] == 0
    assert all(checked["checks"].values())
    saved_order = CommerceOrders(pipeline.catalog).order(report["connection_id"], "71", True)
    serialized = json.dumps({"report": report, "order": saved_order}, default=str)
    assert "synthetic-customer@example.test" not in serialized
    assert "synthetic-hint@example.test" not in serialized
    assert "Synthetic only" not in serialized


@pytest.mark.parametrize("action", [rehearse, verify_rehearsal])
def test_explicit_synthetic_mode_is_required_before_any_resources(action):
    with pytest.raises(RehearsalError, match="explicit_synthetic_mode_required"):
        action(None, synthetic=False)
    with pytest.raises(RehearsalError, match="invalid_rehearsal_run_id"):
        action(None, synthetic=True, run_id="bad run id")


def test_cli_requires_synthetic_before_environment_or_resource_access(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as caught:
        main(["--output", "/unused.json"])
    assert caught.value.code == 2


def test_cloud_storage_refused_before_module_initialization(pipeline, monkeypatch):
    monkeypatch.setattr(pipeline.settings, "s3_endpoint_url", "https://s3.amazonaws.com")
    with pytest.raises(RehearsalError, match="controlled_local_object_store_required"):
        rehearse(pipeline, synthetic=True)


def test_failed_run_does_not_repeat_effects_and_new_id_is_explicit_fresh_run(pipeline, monkeypatch):
    original = CommerceDelivery.prepare
    calls = []

    def fail_after_dispatch(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        calls.append(result["operation_id"])
        raise RuntimeError("private-person@example.test")

    with monkeypatch.context() as failed:
        failed.setattr(CommerceDelivery, "prepare", fail_after_dispatch)
        report = rehearse(pipeline, synthetic=True, run_id="failed-run")
        assert report["status"] == "failed"
        assert report["failed_stage"] == "controlled_draft_orders"
        replay = rehearse(pipeline, synthetic=True, run_id="failed-run")
    assert replay["status"] == "failed" and replay["execution_mode"] == "incomplete_run_no_retry"
    assert len(calls) == 1 and "private-person" not in str(report)
    assert CommerceDelivery(pipeline.catalog).operation(calls[0], True)["state"] == "succeeded"
    fresh = rehearse(pipeline, synthetic=True, run_id="new-explicit-run")
    assert fresh["status"] == "completed", fresh
    assert fresh["execution_mode"] == "fresh_run"


def test_fresh_run_refuses_a_namespace_containing_real_products(pipeline):
    pipeline.create_product("real-product", "Real product", synthetic=False)
    report = rehearse(pipeline, synthetic=True)
    assert report["status"] == "failed"
    assert report["error_code"] == "use_an_isolated_small_synthetic_workspace"
    assert pipeline.products(True) == []


def test_cli_writes_private_report_and_preserves_existing_output(pipeline, monkeypatch, tmp_path):
    report = rehearse(pipeline, synthetic=True, run_id="cli-run")
    assert report["status"] == "completed", report
    for name, value in {
        "S3_ENDPOINT_URL": pipeline.settings.s3_endpoint_url,
        "S3_BUCKET": pipeline.settings.s3_bucket,
        "PIPELINE_NAMESPACE": pipeline.settings.pipeline_namespace,
        "CACHE_DIR": str(tmp_path / "cli-cache"),
        "SPOOL_DIR": str(tmp_path / "cli-spool"),
    }.items():
        monkeypatch.setenv(name, value)
    path = tmp_path / "report.json"
    argv = ["--synthetic", "--run-id", "cli-run", "--output", str(path)]
    assert main(argv) == 0
    saved = path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(saved)["execution_mode"] == "replayed_completed_run"
    assert main(argv) == 2
    assert path.read_bytes() == saved


def test_verification_detects_missing_manual_record_and_changed_dossier(pipeline):
    report = rehearse(pipeline, synthetic=True, run_id="damaged-evidence")
    assert report["status"] == "completed", report
    # Deliberately damage restored-fixture metadata; the rehearsal itself creates
    # every business fact through its public modules rather than direct SQL.
    pipeline.catalog.execute(
        "DELETE FROM commerce_order_events WHERE connection_id=%s AND kind='notification'",
        (report["connection_id"],),
    )
    checked = verify_rehearsal(pipeline, synthetic=True, run_id="damaged-evidence")
    assert checked["status"] == "failed"
    assert checked["checks"]["manual_notification"] is False
    assert checked["checks"]["manual_handoff"] is True
    assert checked["checks"]["manual_tracking"] is True
    assert checked["checks"]["dossier_revision"] is True
    launches = LaunchService(pipeline.catalog)
    draft = LaunchDraft.model_validate(launches.dossier(report["dossier_id"], True)["draft"])
    launches.save(draft.model_copy(update={"title": "Changed synthetic title"}), 1, "change-title")
    checked = verify_rehearsal(pipeline, synthetic=True, run_id="damaged-evidence")
    assert checked["status"] == "failed"
    assert checked["checks"]["dossier_revision"] is False


def test_rehearsal_and_legacy_checkpoint_recovery_coexist(pipeline):
    # Import the real operator fixture, including its no-source-request assertion.
    fixture = runpy.run_path(str(Path(__file__).parents[1] / "scripts/recovery_fixture.py"))

    report = rehearse(pipeline, synthetic=True, run_id="legacy-recovery")
    assert report["status"] == "completed", report
    checkpoint = fixture["seed"](pipeline)
    recovered = fixture["resume"](pipeline.settings)
    assert recovered["status"] == "verified"
    assert recovered["dataset_id"] == checkpoint["dataset_id"]
    assert recovered["source_requests"] == 0
    checked = verify_rehearsal(pipeline, synthetic=True, run_id="legacy-recovery")
    assert checked["status"] == "verified", checked
    assert checked["controlled_http_requests"] == 0


@pytest.mark.parametrize("kind", [None, "declaration", "actual", "withdrawal"])
def test_verification_detects_lost_financial_publication_intents(pipeline, kind):
    report = rehearse(pipeline, synthetic=True, run_id="lost-financial-intents")
    assert report["status"] == "completed", report
    # Actual fixture corruption: source packs, reviews and frozen results remain
    # intact, but their durable publication/recovery metadata is no longer there.
    pipeline.catalog.execute(
        "DELETE FROM commercial_publications WHERE test_id=%s AND (%s::text IS NULL OR kind=%s)",
        (report["commercial_test_id"], kind, kind),
    )
    checked = verify_rehearsal(pipeline, synthetic=True, run_id="lost-financial-intents")
    assert checked["status"] == "failed", checked
    if kind != "declaration":
        assert checked["checks"]["financial_publications"] is False
    if kind in {None, "declaration"}:
        assert checked["checks"]["ledger_declaration"] is False
    assert checked["checks"]["old_cutoff_profit"] is True
    assert checked["checks"]["history_objects"] is True

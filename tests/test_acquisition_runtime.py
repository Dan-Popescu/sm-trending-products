"""Operator acquisition publishes immutable receipts before independent replay."""

from __future__ import annotations

import pytest
from test_raw_capture import one_shot_pages, page

from sm_trending_products.capture_contracts import CapturePolicy
from sm_trending_products.cli import dispatch, parser
from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def test_capture_cli_and_offline_normalizer_share_original_window_and_generation(pipeline):
    pipeline.settings.acquisition_enabled = True
    contract_id = pipeline.capture_contracts.register(
        CapturePolicy(
            source="tiktok_display",
            account_id="fixture-account",
            operation="list",
            version="pilot-runtime",
            cadence_seconds=60,
            lateness_seconds=30,
        ),
        synthetic=True,
    )
    with one_shot_pages([page("ephemeral")]) as (endpoint, requests):
        task_id = pipeline.enqueue(
            "tiktok_display",
            "fixture-account",
            "list",
            {"endpoint_override": endpoint},
            synthetic=True,
        )
        obligation = pipeline.capture_contracts.declare(task_id)
        result = dispatch(pipeline, parser().parse_args(["capture", "--synthetic"]))
        assert result["processed"] == len(requests) == 1
    raw_id = result["datasets"][0]
    raw = pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (raw_id,))
    assert raw["row_count"] == 0 and raw["manifest"]["kind"] == "raw_capture"
    assert raw["manifest"]["capture"]["worker_generation"] == 1
    assert pipeline.worker_control.status(pipeline.worker_id)["state"] == "stopped"
    receipt = pipeline.capture_contracts.declare(task_id)
    assert receipt["contract_id"] == contract_id
    assert receipt["original_due_at"] == obligation["original_due_at"]
    assert receipt["capture_dataset_id"] == raw_id

    published = dispatch(pipeline, parser().parse_args(["publish-captures", "--synthetic"]))
    assert published["processed"] == 1 and not published["errors"]
    child = pipeline.catalog.one(
        "SELECT * FROM datasets WHERE dataset_id=%s", (published["datasets"][0],)
    )
    assert child["supersedes"] == raw_id and child["row_count"] == 1
    assert (
        dispatch(pipeline, parser().parse_args(["publish-captures", "--synthetic"]))["processed"]
        == 0
    )
    status = pipeline.capture_contracts.status(as_of=now(), synthetic=True)
    assert status["obligations"][0]["status"] == "captured"


def test_independent_runtime_blocks_legacy_source_path_before_any_request(pipeline, monkeypatch):
    pipeline.settings.acquisition_enabled = True

    def forbidden(*args, **kwargs):
        raise AssertionError("Legacy source path was admitted")

    monkeypatch.setattr(pipeline._collection, "collect_batch", forbidden)
    with pytest.raises(ValueError, match="Independent acquisition"):
        pipeline.collect_batch(synthetic=True)


def test_operator_spool_revocation_requires_explicit_apply_and_preserves_workspace(pipeline):
    arguments = [
        "capture-spool-revoke",
        "--source",
        "tiktok_display",
        "--account-id",
        "fixture-account",
        "--reason",
        "source_removed",
        "--synthetic",
    ]
    preview = dispatch(pipeline, parser().parse_args(arguments))
    assert preview["preview"] is True and preview["synthetic"] is True
    assert pipeline.catalog.all("SELECT * FROM capture_spool_revocations") == []
    applied = dispatch(pipeline, parser().parse_args([*arguments, "--apply"]))
    assert applied
    rows = pipeline.catalog.all("SELECT * FROM capture_spool_revocations")
    assert len(rows) == 1 and rows[0]["synthetic"] is True
    assert rows[0]["source"] == "tiktok_display" and rows[0]["account_id"] == "fixture-account"

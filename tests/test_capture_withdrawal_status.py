"""Authorized removal remains distinct from physical capture and missed windows."""

from datetime import timedelta

import pytest
from test_capture_contracts import enqueue as enqueue_query
from test_capture_contracts import historical
from test_raw_capture import one_shot_pages, page, service

from sm_trending_products.capture_contracts import CaptureContracts, CapturePolicy
from sm_trending_products.contracts import now, utc

pytestmark = pytest.mark.integration


def policy(pipeline, *, lateness=120):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(
        CapturePolicy(
            source="tiktok_display",
            account_id="fixture-account",
            operation="list",
            version="withdrawal-fixture-v1",
            cadence_seconds=60,
            lateness_seconds=lateness,
            recoverability="snapshot",
        ),
        synthetic=True,
    )
    return contracts


def enqueue(pipeline, endpoint, key):
    return pipeline.enqueue(
        "tiktok_display",
        "fixture-account",
        "list",
        {"endpoint_override": endpoint},
        synthetic=True,
        idempotency_key=key,
    )


def local_capture(pipeline, endpoint, key):
    task_id = enqueue(pipeline, endpoint, key)
    task = pipeline.catalog.claim(pipeline.worker_id, 180, True, 8)
    assert task["task_id"] == task_id
    receipt = pipeline.collector.capture(
        {
            "source": "tiktok_display",
            "account_id": "fixture-account",
            "operation": "list",
            "synthetic": True,
            "payload": {},
            "endpoint_override": endpoint,
        },
        task,
    )
    return task_id, receipt


def row(contracts, *, as_of=None):
    return contracts.status(as_of=as_of or now(), synthetic=True)["obligations"][0]


def retain_local(*args, **kwargs):
    raise OSError("controlled_keep_local_receipt")


def test_authorized_local_withdrawal_preserves_physical_clock_and_original_miss(pipeline):
    contracts = policy(pipeline, lateness=0)
    acquisition = service(pipeline)
    with one_shot_pages([page()]) as (endpoint, requests):
        task_id, receipt = local_capture(pipeline, endpoint, "unpublished-withdrawal")
        assert len(requests) == 1
    original = contracts.declare(task_id)
    before_withdrawal = now()
    assert row(contracts)["status"] == "overdue"
    acquisition.request_spool_removal("tiktok_display", "fixture-account", True, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    withdrawn = row(contracts)
    assert withdrawn["status"] == "authorized_withdrawal"
    assert withdrawn["capture_status"] == "not_captured"
    assert withdrawn["capture_dataset_id"] is None and withdrawn["captured_at"] is None
    assert withdrawn["durable_at"] is None
    assert withdrawn["obligation_id"] == original["obligation_id"]
    assert withdrawn["original_due_at"] == original["original_due_at"]
    assert withdrawn["deadline_at"] == original["deadline_at"]
    marker = withdrawn["withdrawal"]
    assert marker["complete_receipt"] is True
    assert utc(marker["captured_at"]) == utc(receipt["capture"]["captured_at"])
    assert marker["authority_kind"] == "source_revocation"
    assert marker["reason"] == "capture_source_removed"
    assert withdrawn["late_seconds"] > 0
    later = row(contracts, as_of=now() + timedelta(minutes=1))
    assert later["late_seconds"] == withdrawn["late_seconds"]
    historical_status = row(contracts, as_of=before_withdrawal)
    assert historical_status["status"] == "overdue"
    assert historical_status["withdrawal"] is None


def test_cancelled_task_without_authority_remains_an_uncaptured_unknown_window(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    task_id = enqueue_query(pipeline, "ordinary-cancellation", now())
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET state='cancelled',last_error='capture_source_removed' "
        "WHERE task_id=%s",
        (task_id,),
    )
    unresolved = row(contracts)
    assert unresolved["status"] == "unqualified"
    assert unresolved["capture_status"] == "not_captured"
    assert unresolved["withdrawal"] is None
    assert unresolved["capture_dataset_id"] is None
    assert unresolved["late_seconds"] is None


def test_authorized_retirement_of_committed_receipt_keeps_its_actual_capture_proof(
    pipeline, monkeypatch
):
    contracts = policy(pipeline)
    acquisition = service(pipeline)
    monkeypatch.setattr(acquisition, "_remove_committed_spool", retain_local)
    with one_shot_pages([page()]) as (endpoint, _):
        task_id = enqueue(pipeline, endpoint, "committed-withdrawal")
        captured = acquisition.capture_batch(1, True)
        assert captured["processed"] == 1
    original = contracts.declare(task_id)
    acquisition.request_spool_removal("tiktok_display", "fixture-account", True, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    withdrawn = row(contracts)
    assert withdrawn["status"] == "authorized_withdrawal"
    assert withdrawn["capture_status"] == "captured"
    assert withdrawn["capture_dataset_id"] == original["capture_dataset_id"]
    assert withdrawn["captured_at"] == original["captured_at"]
    assert withdrawn["durable_at"] == original["durable_at"]
    assert withdrawn["dataset_state"] == "invalidated"
    assert withdrawn["withdrawal"]["capture_id"] == original["capture_dataset_id"]


def test_old_diagnostic_attempt_withdrawal_cannot_hide_a_new_successful_attempt(
    pipeline, monkeypatch
):
    contracts = policy(pipeline)
    acquisition = service(pipeline)
    monkeypatch.setattr(acquisition, "_remove_committed_spool", retain_local)
    error = {"error": {"code": "internal_error", "message": "retry later"}}
    with one_shot_pages([error, page("new-attempt")]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint, "retry-after-cutoff")
        assert acquisition.capture_batch(1, True)["processed"] == 0
        diagnostic = pipeline.catalog.one("SELECT * FROM datasets WHERE task_id=%s", (task_id,))
        acquisition.request_spool_removal(
            "tiktok_display", "fixture-account", True, "source_opt_out"
        )
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=%s", (task_id,)
        )
        succeeded = acquisition.capture_batch(1, True)
        assert succeeded["processed"] == 1 and succeeded["errors"] == []
        assert len(requests) == 2
    # Acquisition cleanup may already have advanced the shared bounded cursor
    # past the diagnostic; finish that scan and one subsequent full cycle.
    retired = sum(acquisition.maintain_spool(1000, apply=True)["retired"] for _ in range(2))
    assert retired == 1
    assert pipeline.catalog.one(
        "SELECT 1 FROM capture_spool_retirements WHERE capture_id=%s", (diagnostic["dataset_id"],)
    )
    captured = row(contracts)
    assert captured["status"] == "captured" and captured["withdrawal"] is None
    assert captured["capture_dataset_id"] == succeeded["datasets"][0]


def test_lawful_uncaptured_withdrawal_does_not_poison_other_verified_pause_frontier(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    acquisition = service(pipeline)
    with one_shot_pages([page()]) as (endpoint, _):
        task_id, _ = local_capture(pipeline, endpoint, "unknown-withdrawn-window")
    acquisition.request_spool_removal("tiktok_display", "fixture-account", True, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    assert contracts.declare(task_id)["capture_dataset_id"] is None
    contracts.register(historical(), synthetic=True)
    enqueue_query(pipeline, "authorized-remaining-query", now())
    assessment = contracts.assess_pause(
        as_of=now(),
        synthetic=True,
        pause_seconds=1,
        rollback_seconds=1,
        safety_seconds=1,
        catchup_requests_per_second=1,
        request_budget=100,
    )
    assert assessment["allowed"] is True and assessment["reasons"] == []

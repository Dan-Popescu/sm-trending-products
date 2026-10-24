"""Qualify the integrated capture protocol using actual Rust, SQL and S3 receipts."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

import pytest
from test_raw_capture import one_shot_pages, page

from sm_trending_products.capture_contracts import CaptureContracts, CapturePolicy
from sm_trending_products.contracts import now
from sm_trending_products.raw_capture import RawCaptureService
from sm_trending_products.release_evidence import CatalogCaptureVerifier
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


def capture_contract(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(
        CapturePolicy(
            source="tiktok_display",
            account_id="fixture-account",
            operation="list",
            version="offline-qualification-v1",
            cadence_seconds=30,
            lateness_seconds=120,
            recoverability="snapshot",
        ),
        synthetic=True,
    )
    return contracts


def acquisition(pipeline, worker_id):
    session = WorkerControl(pipeline.catalog).register(worker_id, synthetic=True)
    service = RawCaptureService(
        pipeline.settings,
        pipeline.catalog,
        pipeline.history,
        pipeline.collector,
        worker_id,
    )
    service.worker_generation = session["generation"]
    return service


def enqueue(pipeline, endpoint, key):
    return pipeline.enqueue(
        "tiktok_display",
        "fixture-account",
        "list",
        {"endpoint_override": endpoint},
        synthetic=True,
        idempotency_key=key,
    )


def qualify(pipeline, acquirer, start):
    return CatalogCaptureVerifier(pipeline.catalog).verify(
        worker_id=acquirer.worker_id,
        generation=acquirer.worker_generation,
        since=start,
        synthetic=True,
    )


def test_actual_paginated_raw_commits_qualify_original_obligations_without_normalization(
    pipeline, monkeypatch
):
    contracts = capture_contract(pipeline)
    candidate = acquisition(pipeline, "qualified-candidate")
    start = now()

    def forbidden_normalizer(*args, **kwargs):
        raise AssertionError("Acquisition invoked the observation normalizer")

    monkeypatch.setattr(pipeline.collector, "run", forbidden_normalizer)
    assert qualify(pipeline, candidate, start) is None
    with one_shot_pages([page(more=True, cursor=7, malformed=True), page("last")]) as (
        endpoint,
        requests,
    ):
        task_id = enqueue(pipeline, endpoint, "two-pages")
        initial = contracts.declare(task_id)
        result = candidate.capture_batch(2, True)
        assert result["processed"] == 2 and result["errors"] == []
        assert len(requests) == 2 and requests[1]["cursor"] == 7
    obligations = pipeline.catalog.all(
        "SELECT * FROM capture_obligations WHERE task_id=%s ORDER BY page_index", (task_id,)
    )
    assert [row["page_index"] for row in obligations] == [0, 1]
    assert len({row["obligation_id"] for row in obligations}) == 2
    for row in obligations:
        assert row["original_due_at"] == initial["original_due_at"]
        assert row["contract_id"] == initial["contract_id"]
        assert row["captured_at"] <= row["durable_at"]
        assert row["captured_at"] <= row["deadline_at"]
        dataset = pipeline.catalog.one(
            "SELECT * FROM datasets WHERE dataset_id=%s", (row["capture_dataset_id"],)
        )
        manifest = dataset["manifest"]
        assert dataset["state"] == "committed" and dataset["row_count"] == 0
        assert manifest["kind"] == "raw_capture" and manifest["acquisition_protocol"] == "1"
        assert manifest["capture"]["task_id"] == task_id
        assert manifest["capture"]["page_index"] == row["page_index"]
        assert manifest["capture"]["worker_id"] == candidate.worker_id
        assert manifest["capture"]["worker_generation"] == candidate.worker_generation
        assert manifest["task_request"]["payload"] == {
            key: value
            for key, value in row["request_payload"].items()
            if key != "endpoint_override"
        }
        assert pipeline.history.raw(dataset)["capture"] == manifest["capture"]
    evidence = qualify(pipeline, candidate, start)
    assert evidence is not None and evidence["deadline_breaches"] == 0
    # Readiness reports a bounded witness per registered path. Every page's raw
    # receipt and original deadline were verified above; pagination does not
    # require duplicating the entire audit trail in the handover summary.
    assert evidence["capture_dataset_ids"]
    assert set(evidence["capture_dataset_ids"]) <= set(result["datasets"])
    assert evidence["obligation_ids"]
    assert set(evidence["obligation_ids"]) <= {row["obligation_id"] for row in obligations}
    assert pipeline.history.observations(now(), True)[0] == []


def test_source_offline_spool_adoption_settles_window_without_qualifying_adopting_worker(
    pipeline, monkeypatch
):
    contracts = capture_contract(pipeline)
    predecessor = acquisition(pipeline, "original-acquirer")
    start = now()
    publish = pipeline.history.publish

    def interrupted(*args, **kwargs):
        raise RuntimeError("controlled_after_local_fsync_before_remote_publication")

    monkeypatch.setattr(pipeline.history, "publish", interrupted)
    with one_shot_pages([page("one-shot")]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint, "adopted-window")
        original = contracts.declare(task_id)
        failed = predecessor.capture_batch(1, True)
        assert failed["processed"] == 0 and len(failed["errors"]) == 1
        assert len(requests) == 1
    assert contracts.declare(task_id)["capture_dataset_id"] is None
    WorkerControl(pipeline.catalog).finish(predecessor.worker_id, predecessor.worker_generation)
    candidate = acquisition(pipeline, "adopting-candidate")
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    monkeypatch.setattr(pipeline.history, "publish", publish)
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=%s", (task_id,)
    )
    adopted = candidate.capture_batch(1, True)
    assert adopted["processed"] == 1 and adopted["errors"] == []
    settled = contracts.declare(task_id)
    assert settled["obligation_id"] == original["obligation_id"]
    assert settled["original_due_at"] == original["original_due_at"]
    dataset = pipeline.catalog.one(
        "SELECT * FROM datasets WHERE dataset_id=%s", (settled["capture_dataset_id"],)
    )
    capture = pipeline.history.raw(dataset)["capture"]
    assert capture["worker_id"] == predecessor.worker_id
    assert capture["worker_generation"] == predecessor.worker_generation
    assert qualify(pipeline, candidate, start) is None
    assert qualify(pipeline, predecessor, start) is not None

    # Only a physically new acquisition supplies the replacement's own proof.
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-token")
    with one_shot_pages([page("replacement-fresh")]) as (endpoint, requests):
        enqueue(pipeline, endpoint, "fresh-window")
        fresh = candidate.capture_batch(1, True)
        assert fresh["processed"] == 1 and fresh["errors"] == [] and len(requests) == 1
    evidence = qualify(pipeline, candidate, start)
    assert evidence is not None and evidence["capture_dataset_ids"] == fresh["datasets"]
    assert settled["capture_dataset_id"] not in evidence["capture_dataset_ids"]


def test_restart_generation_requires_fresh_physical_capture(pipeline):
    capture_contract(pipeline)
    candidate = acquisition(pipeline, "restarted-candidate")
    start = now()
    with one_shot_pages([page("generation-one")]) as (endpoint, _):
        enqueue(pipeline, endpoint, "generation-one")
        first = candidate.capture_batch(1, True)
        assert first["processed"] == 1
    assert qualify(pipeline, candidate, start) is not None
    WorkerControl(pipeline.catalog).finish(candidate.worker_id, candidate.worker_generation)
    restarted = acquisition(pipeline, candidate.worker_id)
    assert restarted.worker_generation > candidate.worker_generation
    assert qualify(pipeline, restarted, start) is None
    with one_shot_pages([page("generation-two")]) as (endpoint, _):
        enqueue(pipeline, endpoint, "generation-two")
        second = restarted.capture_batch(1, True)
        assert second["processed"] == 1
    evidence = qualify(pipeline, restarted, start)
    assert evidence is not None and evidence["capture_dataset_ids"] == second["datasets"]
    assert set(first["datasets"]).isdisjoint(evidence["capture_dataset_ids"])


def test_expired_registered_owner_is_fenced_while_replacement_adopts_one_shot_response(pipeline):
    contracts = capture_contract(pipeline)
    predecessor = acquisition(pipeline, "expired-acquirer")
    candidate = acquisition(pipeline, "lease-replacement")
    start = now()
    entered, release = threading.Event(), threading.Event()
    with one_shot_pages([page()], entered=entered, release=release) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint, "fenced-one-shot")
        original = contracts.declare(task_id)
        with ThreadPoolExecutor(2) as pool:
            old = pool.submit(predecessor.capture_batch, 1, True)
            assert entered.wait(10)
            pipeline.catalog.execute(
                "UPDATE collection_tasks SET lease_until=clock_timestamp()-interval '1 second' "
                "WHERE task_id=%s",
                (task_id,),
            )
            new = pool.submit(candidate.capture_batch, 1, True)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                task = pipeline.catalog.one(
                    "SELECT lease_owner FROM collection_tasks WHERE task_id=%s", (task_id,)
                )
                if task["lease_owner"] == candidate.worker_id:
                    break
                time.sleep(0.01)
            else:
                pytest.fail("Replacement did not reclaim the expired registered task")
            release.set()
            stale, adopted = old.result(timeout=15), new.result(timeout=15)
        assert stale["processed"] == 0 and stale["errors"][0]["category"] == "lease_lost"
        assert adopted["processed"] == 1 and adopted["errors"] == []
        assert len(requests) == 1
    settled = contracts.declare(task_id)
    assert settled["obligation_id"] == original["obligation_id"]
    assert settled["original_due_at"] == original["original_due_at"]
    dataset = pipeline.catalog.one(
        "SELECT * FROM datasets WHERE dataset_id=%s", (settled["capture_dataset_id"],)
    )
    assert dataset["manifest"]["capture"]["worker_id"] == predecessor.worker_id
    assert dataset["manifest"]["capture"]["worker_generation"] == predecessor.worker_generation
    assert qualify(pipeline, candidate, start) is None


def test_actual_fresh_capture_cannot_hide_an_older_unknown_pending_scope(pipeline):
    # An unqualified window exists before this release and stays uncaptured.
    pipeline.catalog.enqueue(
        "tiktok_display",
        "fixture-account",
        "query",
        {},
        True,
        due_at=now() - timedelta(minutes=1),
        idempotency_key="unknown-original-window",
    )
    assert pipeline.catalog.claim("pending-predecessor", 180, True, 8)
    capture_contract(pipeline)
    candidate = acquisition(pipeline, "qualified-other-scope")
    start = now()
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint, "known-scope")
        assert candidate.capture_batch(1, True)["processed"] == 1
    assert qualify(pipeline, candidate, start) is None


def test_actual_retired_handover_receipt_cannot_be_hidden_by_fresh_same_scope(pipeline):
    contracts = capture_contract(pipeline)
    candidate = acquisition(pipeline, "retired-window-candidate")
    start = now()
    with one_shot_pages([page("first"), page("second")]) as (endpoint, requests):
        first_task = enqueue(pipeline, endpoint, "retired-first-window")
        first = candidate.capture_batch(1, True)
        assert first["processed"] == 1
        pipeline.catalog.invalidate_dataset(first["datasets"][0], "controlled_source_removal")
        enqueue(pipeline, endpoint, "fresh-second-window")
        second = candidate.capture_batch(1, True)
        assert second["processed"] == 1 and len(requests) == 2
    cutoff = contracts.declare(first_task)["deadline_at"] + timedelta(seconds=1)
    with patch("sm_trending_products.release_evidence.now", return_value=cutoff):
        assert qualify(pipeline, candidate, start) is None


def test_http_business_error_does_not_acknowledge_and_retry_preserves_original_window(pipeline):
    contracts = capture_contract(pipeline)
    candidate = acquisition(pipeline, "business-retry-candidate")
    start = now()
    failure = {"error": {"code": "internal_error", "message": "retry later"}}
    with one_shot_pages([failure, page("successful-retry")]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint, "business-error-window")
        original = contracts.declare(task_id)
        first = candidate.capture_batch(1, True)
        assert first["processed"] == 0 and len(first["errors"]) == 1
        after_error = contracts.declare(task_id)
        assert after_error["capture_dataset_id"] is None
        assert after_error["obligation_id"] == original["obligation_id"]
        diagnostic = pipeline.catalog.one("SELECT * FROM datasets WHERE task_id=%s", (task_id,))
        assert diagnostic["manifest"]["kind"] == "raw_capture_error"
        assert qualify(pipeline, candidate, start) is None
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=%s", (task_id,)
        )
        recovered = candidate.capture_batch(1, True)
        assert recovered["processed"] == 1 and recovered["errors"] == []
        assert len(requests) == 2
    settled = contracts.declare(task_id)
    assert settled["obligation_id"] == original["obligation_id"]
    assert settled["original_due_at"] == original["original_due_at"]
    assert settled["capture_dataset_id"] != diagnostic["dataset_id"]
    evidence = qualify(pipeline, candidate, start)
    assert evidence is not None and evidence["capture_dataset_ids"] == recovered["datasets"]

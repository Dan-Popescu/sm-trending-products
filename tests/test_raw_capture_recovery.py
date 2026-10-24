"""Archived source failures retain their original page/window across retries."""

from __future__ import annotations

import pytest
from test_raw_capture import ACCOUNT, datasets, enqueue, one_shot_pages, page, service

pytestmark = pytest.mark.integration


def retry_now(pipeline, task_id):
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=%s", (task_id,)
    )
    pipeline.catalog.execute("UPDATE request_budgets SET blocked_until=NULL")


def test_business_rate_limit_archives_each_attempt_and_retries_original_page(pipeline):
    acquisition = service(pipeline)
    error = {"error": {"code": "rate_limit_exceeded", "http_status_code": 429}}
    with one_shot_pages([error, page(more=True, cursor=7), page("last")]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        original = pipeline.catalog.one(
            "SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,)
        )
        failed = acquisition.capture_batch(1, True)
        task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        assert failed["errors"][0]["category"] == "rate_limited"
        assert task["state"] == "pending" and task["page_index"] == 0
        assert task["capture_attempt"] == 1 and task["attempts"] == 0
        assert task["due_at"] >= original["due_at"]
        archived = datasets(pipeline)[0]
        assert archived["manifest"]["kind"] == "raw_capture_error"
        assert acquisition.publish_pending(1, True)["processed"] == 0
        assert acquisition.capture_batch(1, True)["processed"] == 0
        assert len(requests) == 1
        retry_now(pipeline, task_id)
        assert acquisition.capture_batch(2, True)["processed"] == 2
        assert len(requests) == 3 and "cursor" not in requests[1] and requests[2]["cursor"] == 7
    saved = datasets(pipeline)
    assert len(saved) == 3 and len({row["dataset_id"] for row in saved}) == 3
    assert saved[1]["manifest"]["capture"]["capture_attempt"] == 1
    assert saved[2]["manifest"]["capture"]["capture_attempt"] == 0
    assert (
        saved[0]["manifest"]["capture"]["request_fingerprint"]
        == saved[1]["manifest"]["capture"]["request_fingerprint"]
    )
    assert acquisition.publish_pending(5, True)["processed"] == 2


def test_archived_business_error_recovery_increments_attempt_once_without_refetch(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original = acquisition._reschedule_failure

    def interrupted(task, category, delay, *, error_dataset_id=None):
        if error_dataset_id:
            raise RuntimeError("controlled_after_error_commit_before_attempt_advance")
        return original(task, category, delay)

    monkeypatch.setattr(acquisition, "_reschedule_failure", interrupted)
    with one_shot_pages([{"error": {"code": "rate_limit_exceeded"}}, page()]) as (
        endpoint,
        requests,
    ):
        task_id = enqueue(pipeline, endpoint)
        assert acquisition.capture_batch(1, True)["processed"] == 0
        assert len(requests) == 1
        assert (
            pipeline.catalog.one(
                "SELECT capture_attempt FROM collection_tasks WHERE task_id=%s", (task_id,)
            )["capture_attempt"]
            == 0
        )
        monkeypatch.setattr(acquisition, "_reschedule_failure", original)
        retry_now(pipeline, task_id)
        assert acquisition.capture_batch(1, True)["errors"][0]["category"] == "rate_limited"
        assert len(requests) == 1
        assert (
            pipeline.catalog.one(
                "SELECT capture_attempt FROM collection_tasks WHERE task_id=%s", (task_id,)
            )["capture_attempt"]
            == 1
        )
        retry_now(pipeline, task_id)
        assert acquisition.capture_batch(1, True)["processed"] == 1 and len(requests) == 2
    assert len(datasets(pipeline)) == 2


def test_display_query_missing_member_diagnostics_are_derived_after_normalization(pipeline):
    acquisition = service(pipeline)
    with one_shot_pages(
        [{"data": {"videos": [{"id": "returned", "view_count": 10}]}, "error": {"code": "ok"}}]
    ) as (endpoint, _):
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "query",
            {"endpoint_override": endpoint, "filters": {"video_ids": ["returned", "missing"]}},
            synthetic=True,
        )
        parent_id = acquisition.capture_batch(1, True)["datasets"][0]
    parent = pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (parent_id,))
    assert "missing_entity_ids" not in pipeline.history.raw(parent)
    child_id = acquisition.publish_pending(1, True)["datasets"][0]
    child = pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (child_id,))
    derived = pipeline.history.raw(child)
    assert derived["requested_entity_ids"] == ["missing", "returned"]
    assert derived["missing_entity_ids"] == ["missing"]


def test_auth_business_error_blocks_then_allows_fresh_attempt_after_access_recovery(pipeline):
    acquisition = service(pipeline)
    with one_shot_pages([{"error": {"code": "access_token_invalid"}}, page()]) as (
        endpoint,
        requests,
    ):
        task_id = enqueue(pipeline, endpoint)
        result = acquisition.capture_batch(1, True)
        task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        assert result["errors"][0]["category"] == "access_denied"
        assert task["state"] == "quarantined" and task["capture_attempt"] == 1
        assert acquisition.capture_batch(1, True)["processed"] == 0 and len(requests) == 1
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET state='pending' WHERE task_id=%s", (task_id,)
        )
        retry_now(pipeline, task_id)
        assert acquisition.capture_batch(1, True)["processed"] == 1 and len(requests) == 2
    assert len(datasets(pipeline)) == 2


def test_adopting_final_receipt_retires_crash_leftover_disk_reservation(pipeline, monkeypatch):
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        task = pipeline.catalog.claim(pipeline.worker_id, 180, True, 8)
        request = service(pipeline).collection._request(task)
        receipt = pipeline.collector.capture(request, task)
    directory = pipeline.settings.spool_dir / "captures"
    reservation = directory / f"{receipt['capture']['capture_id']}.reserve"
    reservation.write_text("")
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    assert pipeline.collector.capture(request, task) == receipt
    assert not reservation.exists()

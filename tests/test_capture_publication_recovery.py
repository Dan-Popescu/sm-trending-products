"""Locally durable responses recover independently of fresh source admission."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import EndpointConnectionError
from test_raw_capture import ACCOUNT, datasets, enqueue, one_shot_pages, page, service

from sm_trending_products.contracts import now
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


def task(pipeline, task_id):
    return pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))


def due(pipeline, task_id):
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=%s", (task_id,)
    )


def advance_until_result(acquisition, *, attempts=100):
    # Directory-entry work stays bounded per call, including shard lock files.
    for _ in range(attempts):
        result = acquisition.capture_batch(20, True)
        if result["processed"] or result["errors"]:
            return result
    pytest.fail("Bounded recovery cursor did not reach a pending receipt")


@pytest.mark.parametrize("partial", [False, True])
def test_s3_outage_survives_attempt_limit_without_new_http_or_quota(pipeline, monkeypatch, partial):
    acquisition = service(pipeline, "original")
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 1, 0.000001)
    original_put = pipeline.history.client.put_object
    failed_puts = []

    def unavailable(**kwargs):
        failed_puts.append(kwargs["Key"])
        raise EndpointConnectionError(endpoint_url="http://controlled-s3-outage.invalid")

    monkeypatch.setattr(pipeline.history.client, "put_object", unavailable)
    with one_shot_pages([page()]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        # Preserve earlier real source failures; disk publication must not reset them.
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET attempts=2 WHERE task_id=%s", (task_id,)
        )
        assert (
            acquisition.capture_batch(1, True)["errors"][0]["category"]
            == "capture_publication_failed"
        )
        receipt_path = next((pipeline.settings.spool_dir / "captures").glob("*.json"))
        original_bytes = receipt_path.read_bytes()
        receipt = json.loads(original_bytes)
        assert datasets(pipeline)[0]["state"] == "staged"
        assert task(pipeline, task_id)["attempts"] == 3
        if partial:
            receipt_path.rename(receipt_path.with_suffix(".part"))
        pipeline.catalog.execute(
            "SELECT block_request_budget(%s,%s,3600)", ("tiktok_display", ACCOUNT)
        )
        for _ in range(10):
            due(pipeline, task_id)
            assert (
                advance_until_result(acquisition)["errors"][0]["category"]
                == "capture_publication_failed"
            )
            assert task(pipeline, task_id)["attempts"] == 3
            assert task(pipeline, task_id)["due_at"] > now()
        assert len(failed_puts) == 11 and len(requests) == 1
        # Backoff remains effective even while scanning local recovery candidates.
        assert acquisition.capture_batch(1000, True)["errors"] == []
        assert len(failed_puts) == 11
        # A prior image may already have exhausted the original task while S3 was down.
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET state='quarantined',last_error='attempt_limit',"
            "attempts=8,due_at=clock_timestamp() WHERE task_id=%s",
            (task_id,),
        )
        monkeypatch.setattr(pipeline.history.client, "put_object", original_put)
        monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
        replacement = service(pipeline, "replacement")
        recovered = advance_until_result(replacement)
        assert recovered["processed"] == 1 and recovered["errors"] == []
        assert len(requests) == 1
        saved = datasets(pipeline)[0]
        assert saved["state"] == "committed"
        assert pipeline.history.raw(saved)["capture"] == receipt["capture"]
        assert saved["manifest"]["capture"]["worker_id"] == "original"
        assert task(pipeline, task_id)["state"] == "done"
        assert not receipt_path.exists() and not receipt_path.with_suffix(".part").exists()
        assert pipeline.catalog.one(
            "SELECT blocked_until>clock_timestamp() AS blocked,tokens FROM request_budgets "
            "WHERE source=%s AND account_id=%s",
            ("tiktok_display", ACCOUNT),
        )["blocked"]


def test_recovery_does_not_reopen_access_quarantine_and_respects_worker_drain(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original_publish = pipeline.history.publish

    def unavailable(*args, **kwargs):
        raise RuntimeError("controlled_after_fsync_before_staging")

    monkeypatch.setattr(pipeline.history, "publish", unavailable)
    with one_shot_pages([page()]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        acquisition.capture_batch(1, True)
        receipt = json.loads(
            next((pipeline.settings.spool_dir / "captures").glob("*.json")).read_bytes()
        )
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET state='quarantined',last_error='access_denied',"
            "due_at=clock_timestamp() WHERE task_id=%s",
            (task_id,),
        )
        monkeypatch.setattr(pipeline.history, "publish", original_publish)
        replacement = service(pipeline, "replacement")
        assert replacement.capture_batch(1000, True)["processed"] == 0
        assert task(pipeline, task_id)["state"] == "quarantined"
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET state='pending' WHERE task_id=%s", (task_id,)
        )
        control = WorkerControl(pipeline.catalog)
        worker = control.register("replacement", synthetic=True)
        replacement.worker_generation = worker["generation"]
        control.request_drain("replacement", worker["generation"])
        assert replacement.capture_batch(1000, True)["processed"] == 0
        assert len(requests) == 1 and datasets(pipeline) == []
        control.resume("replacement", worker["generation"])
        assert advance_until_result(replacement)["processed"] == 1
        assert datasets(pipeline)[0]["manifest"]["capture"] == receipt["capture"]
        assert "worker_generation" not in receipt["capture"]
        assert len(requests) == 1


def test_normal_claim_adoption_does_not_count_another_source_failure(pipeline, monkeypatch):
    acquisition = service(pipeline)
    original_publish = pipeline.history.publish

    def unavailable(*args, **kwargs):
        raise RuntimeError("controlled_after_fsync_before_staging")

    monkeypatch.setattr(pipeline.history, "publish", unavailable)
    with one_shot_pages([{"error": {"code": "rate_limit_exceeded"}}]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET attempts=2 WHERE task_id=%s", (task_id,)
        )
        acquisition.capture_batch(1, True)
        assert task(pipeline, task_id)["attempts"] == 3
        # Model the ordinary claim winning before the bounded spool scan reaches its file.
        monkeypatch.setattr(acquisition.spool, "recovery_receipts", lambda *args: iter(()))
        for _ in range(3):
            due(pipeline, task_id)
            acquisition.capture_batch(1, True)
            assert task(pipeline, task_id)["attempts"] == 3
        monkeypatch.setattr(pipeline.history, "publish", original_publish)
        due(pipeline, task_id)
        recovered = acquisition.capture_batch(1, True)
        assert recovered["errors"][0]["category"] == "rate_limited"
        assert task(pipeline, task_id)["attempts"] == 2
        assert task(pipeline, task_id)["capture_attempt"] == 1
        assert len(requests) == 1


def test_committed_business_error_still_settles_while_source_budget_is_blocked(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original = acquisition._reschedule_failure

    def interrupted(task, category, delay, *, error_dataset_id=None):
        if error_dataset_id:
            raise RuntimeError("controlled_after_error_commit_before_attempt_advance")
        return original(task, category, delay)

    monkeypatch.setattr(acquisition, "_reschedule_failure", interrupted)
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 1, 0.000001)
    with one_shot_pages([{"error": {"code": "rate_limit_exceeded"}}]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET attempts=2 WHERE task_id=%s", (task_id,)
        )
        assert acquisition.capture_batch(1, True)["processed"] == 0
        assert datasets(pipeline)[0]["state"] == "committed"
        assert task(pipeline, task_id)["capture_attempt"] == 0
        assert task(pipeline, task_id)["attempts"] == 3
        pipeline.catalog.execute(
            "SELECT block_request_budget(%s,%s,3600)", ("tiktok_display", ACCOUNT)
        )
        monkeypatch.setattr(acquisition, "_reschedule_failure", original)
        replacement = service(pipeline, "replacement")
        due(pipeline, task_id)
        recovered = advance_until_result(replacement)
        assert recovered["errors"][0]["category"] == "rate_limited"
        assert task(pipeline, task_id)["capture_attempt"] == 1
        assert task(pipeline, task_id)["attempts"] == 2
        assert len(requests) == 1
        assert not list((pipeline.settings.spool_dir / "captures").glob("*.json"))


def test_preexisting_committed_error_recovers_after_old_image_removed_local_receipt(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original = acquisition._reschedule_failure

    def interrupted(task, category, delay, *, error_dataset_id=None):
        if error_dataset_id:
            raise RuntimeError("controlled_after_error_commit_before_attempt_advance")
        return original(task, category, delay)

    monkeypatch.setattr(acquisition, "_reschedule_failure", interrupted)
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 1, 0.000001)
    with one_shot_pages([{"error": {"code": "rate_limit_exceeded"}}]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET attempts=2 WHERE task_id=%s", (task_id,)
        )
        acquisition.capture_batch(1, True)
        next((pipeline.settings.spool_dir / "captures").glob("*.json")).unlink()
        pipeline.catalog.execute(
            "SELECT block_request_budget(%s,%s,3600)", ("tiktok_display", ACCOUNT)
        )
        due(pipeline, task_id)
        recovered = advance_until_result(service(pipeline, "replacement"))
        assert recovered["errors"][0]["category"] == "rate_limited"
        assert task(pipeline, task_id)["capture_attempt"] == 1
        assert task(pipeline, task_id)["attempts"] == 2
        assert len(requests) == 1


def test_invalidation_wait_does_not_erase_prior_source_failures(pipeline, monkeypatch):
    from sm_trending_products.catalog import InvalidationPending

    acquisition = service(pipeline)
    original_publish = pipeline.history.publish

    def invalidation(*args, **kwargs):
        raise InvalidationPending("controlled_namespace_pause_after_fsync")

    monkeypatch.setattr(pipeline.history, "publish", invalidation)
    with one_shot_pages([{"error": {"code": "rate_limit_exceeded"}}]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET attempts=2 WHERE task_id=%s", (task_id,)
        )
        with pytest.raises(InvalidationPending):
            acquisition.capture_batch(1, True)
        assert task(pipeline, task_id)["attempts"] == 3
        # An ordinary claim can adopt before the disk cursor visits the file.
        monkeypatch.setattr(acquisition.spool, "recovery_receipts", lambda *args: iter(()))
        for _ in range(3):
            due(pipeline, task_id)
            with pytest.raises(InvalidationPending):
                acquisition.capture_batch(1, True)
            assert task(pipeline, task_id)["attempts"] == 3
        monkeypatch.setattr(pipeline.history, "publish", original_publish)
        due(pipeline, task_id)
        assert acquisition.capture_batch(1, True)["errors"][0]["category"] == "rate_limited"
        assert task(pipeline, task_id)["attempts"] == 2
        assert len(requests) == 1


def test_future_active_prefix_does_not_starve_remote_error_recovery(pipeline, monkeypatch):
    acquisition = service(pipeline)
    original = acquisition._reschedule_failure

    def interrupted(task, category, delay, *, error_dataset_id=None):
        if error_dataset_id:
            raise RuntimeError("controlled_after_error_commit_before_attempt_advance")
        return original(task, category, delay)

    monkeypatch.setattr(acquisition, "_reschedule_failure", interrupted)
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 1, 0.000001)
    with one_shot_pages([{"error": {"code": "rate_limit_exceeded"}}]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        acquisition.capture_batch(1, True)
        next((pipeline.settings.spool_dir / "captures").glob("*.json")).unlink()
        due(pipeline, task_id)
        pipeline.catalog.execute(
            "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,"
            "synthetic,due_at) "
            "SELECT lpad(to_hex(n),64,'0'),'tiktok_display',%s,'list','{}',true,"
            "clock_timestamp()+interval '1 day' FROM generate_series(1,60) n",
            (ACCOUNT,),
        )
        replacement = service(pipeline, "replacement")
        for _ in range(6):
            result = replacement.capture_batch(20, True)
            if result["errors"]:
                break
        assert result["errors"][0]["category"] == "rate_limited"
        assert task(pipeline, task_id)["capture_attempt"] == 1
        assert len(requests) == 1


@pytest.mark.parametrize("batch_size", [1, 4])
def test_receipt_backlog_leaves_normal_source_admission_available(
    pipeline, monkeypatch, batch_size
):
    acquisition = service(pipeline)

    def unavailable(*args, **kwargs):
        raise RuntimeError("controlled_after_fsync_before_staging")

    monkeypatch.setattr(pipeline.history, "publish", unavailable)
    with one_shot_pages([page(str(n)) for n in range(7)]) as (endpoint, requests):
        pending = []
        for n in range(6):
            pending.append(
                pipeline.enqueue(
                    "tiktok_display",
                    ACCOUNT,
                    "list",
                    {"endpoint_override": endpoint},
                    synthetic=True,
                    idempotency_key=f"receipt-{n}",
                )
            )
            assert acquisition.capture_batch(1, True)["errors"]
        assert len(requests) == 6
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=ANY(%s)", (pending,)
        )
        fresh = pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint},
            synthetic=True,
            idempotency_key="fresh-priority",
        )
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET priority=1000 WHERE task_id=%s", (fresh,)
        )
        for _ in range(2):
            acquisition.capture_batch(batch_size, True)
            if len(requests) == 7:
                break
        assert len(requests) == 7
        assert task(pipeline, fresh)["last_error"] == "capture_publication_failed"

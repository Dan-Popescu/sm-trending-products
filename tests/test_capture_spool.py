"""Orphan receipts retire only under explicit scope or verified expiry."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import partial

import pytest
from test_raw_capture import ACCOUNT, datasets, enqueue, one_shot_pages, page, service

from sm_trending_products.collector import CollectionError
from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def local_capture(pipeline):
    task = pipeline.catalog.claim(pipeline.worker_id, 180, True, 8)
    receipt = pipeline.collector.capture(service(pipeline).collection._request(task), task)
    path = pipeline.settings.spool_dir / "captures" / f"{receipt['capture']['capture_id']}.json"
    return task, receipt, path


def test_verified_expiry_previews_then_retires_unshipped_receipt_and_fences_task(pipeline):
    acquisition = service(pipeline)
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        task, receipt, path = local_capture(pipeline)
    old = (now() - timedelta(days=2)).isoformat()
    payload = path.read_text().replace(receipt["capture"]["captured_at"], old)
    payload = payload.replace(receipt["capture"]["started_at"], old)
    path.write_text(payload)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,verified,retention_days) "
        "VALUES('tiktok_display',true,1)"
    )
    expired_receipt = json.loads(path.read_bytes())
    with pytest.raises(CollectionError, match="capture_retention_expired"):
        pipeline.history.publish(
            [],
            expired_receipt,
            source=task["source"],
            synthetic=True,
            dataset_id=path.stem,
            task=task,
            kind="raw_capture",
            capture=expired_receipt["capture"],
            publication_guard=partial(
                acquisition.spool.assert_publishable, receipt=expired_receipt
            ),
        )
    assert datasets(pipeline) == []
    preview = acquisition.maintain_spool(1000)
    assert preview["retired"] == 1 and path.exists()
    applied = acquisition.maintain_spool(1000, apply=True)
    assert applied["retired"] == 1 and not path.exists()
    retired = pipeline.catalog.one(
        "SELECT * FROM collection_tasks WHERE task_id=%s", (task["task_id"],)
    )
    assert retired["state"] == "cancelled" and retired["last_error"] == "capture_retention_expired"
    assert datasets(pipeline) == []


def test_credential_denial_preserves_input_but_explicit_scoped_revocation_removes_it(pipeline):
    acquisition = service(pipeline)
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        _, receipt, path = local_capture(pipeline)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state) "
        "VALUES('tiktok_display',%s,'collect','denied')",
        (ACCOUNT,),
    )
    assert acquisition.maintain_spool(1000, apply=True)["protected"] == 1 and path.exists()
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, False, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["protected"] == 1 and path.exists()
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    with pytest.raises(CollectionError, match="capture_source_removed"):
        with pipeline.catalog.transaction() as connection:
            acquisition.spool.assert_publishable(connection, receipt)
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1 and not path.exists()


def test_pending_prefix_cannot_starve_later_committed_spool_cleanup(pipeline):
    acquisition = service(pipeline)
    pending = []
    with one_shot_pages([page()] * 13) as (endpoint, _):
        for index in range(13):
            pipeline.enqueue(
                "tiktok_display",
                ACCOUNT,
                "list",
                {"endpoint_override": endpoint},
                synthetic=True,
                idempotency_key=f"cleanup-prefix-{index}",
            )
            task, receipt, path = local_capture(pipeline)
            if index < 12:
                pending.append(path)
        pipeline.history.publish(
            [],
            receipt,
            source=task["source"],
            synthetic=True,
            dataset_id=receipt["capture"]["capture_id"],
            task=task,
            kind="raw_capture",
            capture=receipt["capture"],
        )
    for _ in range(100):
        acquisition.cleanup_committed_spool(2)
        if not path.exists():
            break
    assert not path.exists() and all(item.exists() for item in pending)


def test_explicit_revocation_purges_partial_file_using_durable_prefetch_scope(pipeline):
    entered, release = threading.Event(), threading.Event()
    acquisition = service(pipeline)
    with one_shot_pages([page()], entered=entered, release=release) as (endpoint, _):
        enqueue(pipeline, endpoint)
        task = pipeline.catalog.claim(pipeline.worker_id, 180, True, 8)
        request = acquisition.collection._request(task)
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(pipeline.collector.capture, request, task)
            assert entered.wait(10)
            reservation = next((pipeline.settings.spool_dir / "captures").glob("*.reserve"))
            metadata = reservation.read_bytes()
            assert json.loads(metadata)["request"]["account_id"] == ACCOUNT
            release.set()
            receipt = future.result(timeout=15)
    path = reservation.with_suffix(".json")
    partial = reservation.with_suffix(".part")
    partial.write_bytes(path.read_bytes()[:120])
    path.unlink()
    reservation.write_bytes(metadata)
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    result = acquisition.maintain_spool(1000, apply=True)
    assert result["retired"] == 1 and not partial.exists() and not reservation.exists()
    assert receipt["capture"]["capture_id"] == partial.stem


def test_inflight_revoked_request_finishes_durably_but_cannot_publish(pipeline):
    entered, release = threading.Event(), threading.Event()
    acquisition = service(pipeline)
    with one_shot_pages([page()], entered=entered, release=release) as (endpoint, _):
        task_id = enqueue(pipeline, endpoint)
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(acquisition.capture_batch, 1, True)
            assert entered.wait(10)
            acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
            # A live writer keeps its scoped reservation and cannot be purged.
            assert acquisition.maintain_spool(1000, apply=True)["protected"] >= 1
            release.set()
            result = future.result(timeout=15)
        assert (
            result["processed"] == 0 and result["errors"][0]["category"] == "capture_source_removed"
        )
    assert datasets(pipeline) == []
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    assert (
        pipeline.catalog.one("SELECT state FROM collection_tasks WHERE task_id=%s", (task_id,))[
            "state"
        ]
        == "cancelled"
    )


def test_temporary_remote_cleanup_failure_does_not_block_next_capture(pipeline, monkeypatch):
    acquisition = service(pipeline)
    remove = acquisition._remove_committed_spool

    def unavailable(*args, **kwargs):
        raise OSError("controlled_cleanup_unavailable")

    with one_shot_pages([page(), page("second")]) as (endpoint, requests):
        enqueue(pipeline, endpoint)
        monkeypatch.setattr(acquisition, "_remove_committed_spool", unavailable)
        assert acquisition.capture_batch(1, True)["processed"] == 1
        monkeypatch.setattr(acquisition, "_remove_committed_spool", remove)
        original = pipeline.history.raw
        monkeypatch.setattr(pipeline.history, "raw", unavailable)
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint},
            synthetic=True,
            idempotency_key="second-task",
        )
        assert acquisition.capture_batch(1, True)["processed"] == 1 and len(requests) == 2
        monkeypatch.setattr(pipeline.history, "raw", original)
    for _ in range(5):
        acquisition.cleanup_committed_spool(1000)
    assert not list((pipeline.settings.spool_dir / "captures").glob("*.json"))


def test_registered_receipt_revocation_uses_existing_dataset_invalidation(pipeline, monkeypatch):
    acquisition = service(pipeline)

    def retain(*args, **kwargs):
        raise OSError("controlled_keep_committed_local_copy")

    monkeypatch.setattr(acquisition, "_remove_committed_spool", retain)
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        assert acquisition.capture_batch(1, True)["processed"] == 1
    assert acquisition.publish_pending(1, True)["processed"] == 1
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    result = acquisition.maintain_spool(1000, apply=True)
    assert result["retired"] == 1 and all(
        row["state"] == "invalidated" for row in datasets(pipeline)
    )
    assert not list((pipeline.settings.spool_dir / "captures").glob("*.json"))

"""Actual Rust acquisition preserves one-shot pages before normalization."""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sm_trending_products.catalog import LeaseLost
from sm_trending_products.collector import CollectionError
from sm_trending_products.contracts import now
from sm_trending_products.raw_capture import RawCaptureService

pytestmark = pytest.mark.integration
ACCOUNT = "fixture-account"


@contextmanager
def one_shot_pages(pages, *, entered=None, release=None, incomplete=False):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            if entered:
                entered.set()
            if release:
                assert release.wait(10)
            if len(requests) > len(pages):
                self.send_response(410)
                self.end_headers()
                return
            value = pages[len(requests) - 1]
            body = value if isinstance(value, bytes) else json.dumps(value).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body) + (10 if incomplete else 0)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        if release:
            release.set()
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()


def page(identifier="one", *, more=False, cursor=0, malformed=False):
    return {
        "data": {
            "videos": "schema-drift" if malformed else [{"id": identifier, "view_count": 10}],
            "has_more": more,
            "cursor": cursor,
        },
        "error": {"code": "ok"},
    }


def service(pipeline, worker=None):
    return RawCaptureService(
        pipeline.settings,
        pipeline.catalog,
        pipeline.history,
        pipeline.collector,
        worker or pipeline.worker_id,
    )


def enqueue(pipeline, endpoint):
    return pipeline.enqueue(
        "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
    )


def datasets(pipeline):
    return pipeline.catalog.all("SELECT * FROM datasets ORDER BY created_at,dataset_id")


def test_acquisition_advances_pages_with_broken_normalizer_and_replays_after_source_stops(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original = pipeline.collector.run

    def forbidden_normalizer(*args, **kwargs):
        raise AssertionError("Acquisition invoked record normalization")

    monkeypatch.setattr(pipeline.collector, "run", forbidden_normalizer)
    with one_shot_pages([page(more=True, cursor=7, malformed=True), page("last")]) as (
        endpoint,
        requests,
    ):
        task_id = enqueue(pipeline, endpoint)
        captured = acquisition.capture_batch(2, True)
        assert captured["processed"] == 2 and captured["errors"] == []
        assert len(requests) == 2 and requests[1]["cursor"] == 7
    task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    assert task["state"] == "done" and task["page_index"] == 1
    parents = datasets(pipeline)
    assert len(parents) == 2 and all(row["manifest"]["kind"] == "raw_capture" for row in parents)
    assert all(row["row_count"] == 0 for row in parents)
    assert pipeline.history.observations(now(), True)[0] == []
    assert not list((pipeline.settings.spool_dir / "captures").glob("*.json"))

    processed_after = now()
    monkeypatch.setattr(pipeline.collector, "run", original)
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    published = acquisition.publish_pending(10, True)
    assert published["processed"] == 2 and published["errors"] == []
    assert published["quarantined"] == 1
    observations, _ = pipeline.history.observations(now(), True)
    assert [row.entity_id for row in observations] == ["last"]
    assert observations[0].available_at >= processed_after
    assert acquisition.publish_pending(10, True)["processed"] == 0
    assert pipeline.verify()["ok"]
    pipeline.catalog.invalidate_dataset(parents[0]["dataset_id"], "source_deleted")
    assert all(
        row["state"] == "invalidated"
        for row in pipeline.catalog.all(
            "SELECT state FROM datasets WHERE supersedes=%s", (parents[0]["dataset_id"],)
        )
    )


def test_fsynced_capture_is_adopted_without_source_or_token_after_publication_failure(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original = pipeline.history.publish

    def interrupted(*args, **kwargs):
        raise RuntimeError("controlled_after_fsync_before_remote_publication")

    monkeypatch.setattr(pipeline.history, "publish", interrupted)
    with one_shot_pages([page()]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        result = acquisition.capture_batch(1, True)
        assert result["processed"] == 0 and len(result["errors"]) == 1
        assert len(requests) == 1
    spool = list((pipeline.settings.spool_dir / "captures").glob("*.json"))
    assert len(spool) == 1
    receipt = json.loads(spool[0].read_bytes())
    # Also exercise the crash window after file fsync but before final rename.
    partial = spool[0].with_suffix(".part")
    spool[0].rename(partial)
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    monkeypatch.setattr(pipeline.history, "publish", original)
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp() WHERE task_id=%s", (task_id,)
    )
    recovered = service(pipeline, "replacement").capture_batch(1, True)
    assert recovered["processed"] == 1 and recovered["errors"] == []
    saved = datasets(pipeline)[0]
    assert saved["manifest"]["capture"] == receipt["capture"]
    assert saved["manifest"]["capture"]["worker_id"] == pipeline.worker_id
    assert not spool[0].exists() and not partial.exists()
    assert service(pipeline).publish_pending(1, True)["processed"] == 1


def test_expired_source_worker_cannot_publish_and_replacement_uses_same_response(pipeline):
    entered, release = threading.Event(), threading.Event()
    first, second = service(pipeline, "first"), service(pipeline, "replacement")
    with one_shot_pages([page()], entered=entered, release=release) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        with ThreadPoolExecutor(2) as pool:
            old = pool.submit(first.capture_batch, 1, True)
            assert entered.wait(10)
            pipeline.catalog.execute(
                "UPDATE collection_tasks SET lease_until=clock_timestamp()-interval '1 second' "
                "WHERE task_id=%s",
                (task_id,),
            )
            new = pool.submit(second.capture_batch, 1, True)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                row = pipeline.catalog.one(
                    "SELECT lease_owner FROM collection_tasks WHERE task_id=%s", (task_id,)
                )
                if row["lease_owner"] == "replacement":
                    break
                time.sleep(0.01)
            else:
                pytest.fail("Replacement did not reclaim the expired task")
            release.set()
            previous, recovered = old.result(timeout=15), new.result(timeout=15)
        assert len(requests) == 1
    assert previous["processed"] == 0 and previous["errors"][0]["category"] == "lease_lost"
    assert recovered["processed"] == 1 and recovered["errors"] == []
    assert datasets(pipeline)[0]["manifest"]["capture"]["worker_id"] == "first"


def test_normalizer_fencing_rejects_expired_owner_before_dataset_mutation(pipeline):
    first, second = service(pipeline, "first"), service(pipeline, "replacement")
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        assert first.capture_batch(1, True)["processed"] == 1
    old = first._claim_publication(True)
    pipeline.catalog.execute(
        "UPDATE capture_publication_leases SET lease_until=clock_timestamp()-interval '1 second'"
    )
    new = second._claim_publication(True)
    assert new["fence"] > old["fence"]
    with pytest.raises(LeaseLost):
        first.replay.replay(old["dataset_id"], publication_guard=first._publication_guard(old))
    assert len(pipeline.datasets()) == 1
    second.replay.replay(new["dataset_id"], publication_guard=second._publication_guard(new))
    assert len(pipeline.datasets()) == 2


def test_failed_normalizer_retries_later_without_starving_other_capture(pipeline, monkeypatch):
    acquisition = service(pipeline)
    with one_shot_pages([page("first", more=True, cursor=7), page("second")]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        assert acquisition.capture_batch(2, True)["processed"] == 2
    original = pipeline.collector.run

    def fail_one(request, **kwargs):
        if request["raw"]["data"]["videos"][0]["id"] == "first":
            raise CollectionError("collector_binary_missing", 300)
        return original(request, **kwargs)

    monkeypatch.setattr(pipeline.collector, "run", fail_one)
    result = acquisition.publish_pending(2, True)
    assert result["processed"] == 1 and len(result["errors"]) == 1
    assert [row.entity_id for row in pipeline.history.observations(now(), True)[0]] == ["second"]
    assert acquisition.publish_pending(1, True)["processed"] == 0


def test_complete_invalid_body_is_preserved_beyond_old_diagnostic_limit(pipeline):
    body = b'{"unparseable":"' + b"x" * 70000 + b"fixture-token\xff\xfe"
    with one_shot_pages([body]) as (endpoint, requests):
        enqueue(pipeline, endpoint)
        result = service(pipeline).capture_batch(1, True)
        assert len(requests) == 1 and result["processed"] == 0
    saved = pipeline.history.raw(datasets(pipeline)[0])
    recovered = bytes.fromhex(saved["response"]["invalid_json_hex"])
    assert len(recovered) > 65536 and b"fixture-token" not in recovered
    assert b"[REDACTED]" in recovered
    assert recovered.endswith(b"\xff\xfe")
    assert saved["response"]["truncated"] is False


def test_spool_capacity_refuses_before_request_and_preserves_pending_capture(pipeline):
    directory = pipeline.settings.spool_dir / "captures"
    directory.mkdir(parents=True)
    pending = directory / ("f" * 64 + ".reserve")
    pending.write_text("")
    with one_shot_pages([page()]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        task = pipeline.catalog.claim(pipeline.worker_id, 180, True, 8)
        assert task["task_id"] == task_id
        with pytest.raises(CollectionError, match="capture_spool_capacity"):
            pipeline.collector.capture(
                service(pipeline).collection._request(task), task, max_spool_bytes=32 * 1024 * 1024
            )
        assert requests == [] and pending.exists()


def test_incomplete_http_body_never_becomes_a_complete_receipt_or_advances_cursor(pipeline):
    with one_shot_pages([page(more=True, cursor=7)], incomplete=True) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        result = service(pipeline).capture_batch(1, True)
        assert len(requests) == 1 and result["processed"] == 0
        assert result["errors"][0]["category"] == "transport"
    task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    assert task["state"] == "pending" and task["page_index"] == 0
    assert datasets(pipeline) == []
    directory = pipeline.settings.spool_dir / "captures"
    assert not list(directory.glob("*.json")) and not list(directory.glob("*.reserve"))


def test_committed_cleanup_failure_retries_without_failing_or_refetching_task(
    pipeline, monkeypatch
):
    acquisition = service(pipeline)
    original = acquisition._remove_committed_spool

    def unavailable_cleanup(*args, **kwargs):
        raise OSError("controlled_cleanup_unavailable")

    monkeypatch.setattr(acquisition, "_remove_committed_spool", unavailable_cleanup)
    with one_shot_pages([page()]) as (endpoint, requests):
        enqueue(pipeline, endpoint)
        result = acquisition.capture_batch(1, True)
        assert len(requests) == 1 and result["processed"] == 1
        assert result["errors"] == [] and result["cleanup_pending"] == 1
        directory = pipeline.settings.spool_dir / "captures"
        assert len(list(directory.glob("*.json"))) == 1
        with os.scandir(directory) as scan:
            entries = sorted(scan, key=lambda entry: entry.name.endswith(".json"))
        # Model filesystems that return lock/metadata entries before receipts.
        # A one-entry maintenance budget must make progress across later batches.
        assert not entries[0].name.endswith(".json")
        acquisition.spool._entries = (entry for entry in entries)
        monkeypatch.setattr(acquisition, "_remove_committed_spool", original)
        for index in range(len(entries) + 2):
            idle = acquisition.capture_batch(1, True)
            assert idle["processed"] == 0 and idle["errors"] == []
            assert len(requests) == 1
            remaining = list(directory.glob("*.json"))
            if index == 0:
                assert remaining  # The first bounded scan only visits metadata.
            if not remaining:
                break
        else:
            pytest.fail("Committed spool cleanup did not finish within its bounded directory scan")


@pytest.mark.parametrize("source", ["tiktok_ads", "tiktok_commercial"])
def test_commercial_capture_preserves_request_scope_without_record_normalizer(
    pipeline, monkeypatch, source
):
    acquisition = service(pipeline)

    def forbidden_normalizer(*args, **kwargs):
        raise AssertionError("Acquisition invoked record normalization")

    monkeypatch.setattr(pipeline.collector, "run", forbidden_normalizer)
    pipeline.catalog.configure_budget(source, "business-account", 1000, 1000)
    pages = [
        {
            "data": {
                "unknown_future_schema": {"ratio": 0.8455124082255701},
                "has_more": True,
                "search_id": "search-continuation",
            },
            "error": {"code": "ok"},
        },
        {"data": {"unknown_future_schema": [1, 2], "has_more": False}, "error": {"code": "ok"}},
    ]
    filters = {"country_code": "FR"}
    with one_shot_pages(pages) as (endpoint, requests):
        pipeline.enqueue(
            source,
            "business-account",
            "query",
            {"endpoint_override": endpoint, "filters": filters},
            synthetic=True,
        )
        result = acquisition.capture_batch(2, True)
        assert result["processed"] == 2 and result["errors"] == []
        assert requests[1]["search_id"] == "search-continuation"
        assert all(request["filters"] == filters for request in requests)
    raw = pipeline.history.raw(datasets(pipeline)[0])
    assert raw["request"]["account_id"] == "business-account"
    assert raw["request"]["payload"]["filters"] == filters
    assert (
        raw["response"]["data"]["unknown_future_schema"]
        == pages[0]["data"]["unknown_future_schema"]
    )


def test_acquisition_generation_is_original_and_unchanged_when_adopting_spool(
    pipeline, monkeypatch
):
    with one_shot_pages([page()]) as (endpoint, requests):
        enqueue(pipeline, endpoint)
        task = pipeline.catalog.claim("actual-acquirer", 180, True, 8)
        request = service(pipeline).collection._request(task)
        acquired = pipeline.collector.capture(request, task, worker_generation=7)
        assert len(requests) == 1
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    adopted = pipeline.collector.capture(
        request,
        {**task, "lease_owner": "adopting-worker", "lease_version": 99},
        worker_generation=8,
    )
    assert acquired == adopted
    assert adopted["capture"]["worker_id"] == "actual-acquirer"
    assert adopted["capture"]["worker_generation"] == 7

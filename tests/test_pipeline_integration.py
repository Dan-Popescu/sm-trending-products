"""Evidence-to-decision scenarios against PostgreSQL and an HTTP S3 test service."""

from __future__ import annotations

import json
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sm_trending_products.catalog import LeaseLost
from sm_trending_products.contracts import AssessmentPolicy, Observation, now, utc
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration
ACCOUNT = "fixture-account"


@contextmanager
def controlled_source(responses):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            status, body, headers = responses[min(len(requests) - 1, len(responses) - 1)]
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def video_observation(entity_id, timestamp, count, *, synthetic=True):
    return {
        "source": "fixture_import",
        "entity_type": "video",
        "entity_id": entity_id,
        "observed_at": timestamp.isoformat(),
        "available_at": timestamp.isoformat(),
        "published_at": (timestamp - timedelta(days=1)).isoformat(),
        "synthetic": synthetic,
        "metrics": {"view_count": {"lower": count, "upper": count}},
        "attributes": {"creator_id": "independent-creator", "title": "Useful physical product"},
    }


def test_history_bounds_native_reads_but_keeps_available_late_interval_reports(pipeline):
    start = utc("2026-01-02T00:00:00Z")
    end = start + timedelta(days=1)
    late = end + timedelta(days=2)
    records = [
        video_observation("bounded", start - timedelta(days=1), 10),
        video_observation("bounded", start, 20),
        video_observation("bounded", late, 30),
        video_observation("bounded", late + timedelta(days=1), 40),
    ]
    records[2]["attributes"].update(period_start=start.isoformat(), period_end=end.isoformat())
    records[3]["attributes"].update(period_start=start.isoformat(), period_end=end.isoformat())
    pipeline.import_records(records, synthetic=True)
    manifests = pipeline.catalog.manifests(synthetic=True, as_of=late)
    rows = pipeline.history.read(
        manifests,
        as_of=late,
        since=start,
        until=end,
        period_window=(start, end),
        max_rows=2,
    )
    assert [row.observed_at for row in rows] == [start, late]
    plain, _ = pipeline.history.observations(late, True, since=start, until=end)
    assert [row.observed_at for row in plain] == [start]


def test_verification_detects_corrupted_assessment_objects_without_using_cache(pipeline):
    cutoff = now()
    pipeline.import_records([video_observation("verify", cutoff, 100)], synthetic=True)
    pipeline.create_product("verify-product", "Verified product", synthetic=True)
    pipeline.review_link("verify-product", "fixture_import:video:verify")
    assessment = pipeline.assess(now() + timedelta(seconds=1), synthetic=True)
    assert pipeline.verify()["assessments_checked"] == 1
    key = f"assessments/synthetic/{assessment['run_id']}.json"
    pipeline.history.client.put_object(Bucket=pipeline.history.bucket, Key=key, Body=b"corrupt")
    result = pipeline.verify()
    assert result["ok"] is False
    assert result["failures"] == [
        {
            "run_id": assessment["run_id"],
            "object_key": key,
            "category": "missing_or_corrupt_assessment",
        }
    ]


def test_import_review_assess_replay_and_restart_preserve_the_same_evidence(pipeline):
    cutoff = now()
    records = [
        video_observation("video-1", cutoff - timedelta(hours=2), 100),
        video_observation("video-1", cutoff - timedelta(hours=1), 200),
    ]
    original = pipeline.import_records(records, synthetic=True)
    assert pipeline.import_records(records, synthetic=True) == original
    pipeline.create_product("reviewed-product", "Reviewed product", synthetic=True)
    pipeline.review_link("reviewed-product", "fixture_import:video:video-1")
    decision_time = now() + timedelta(seconds=1)
    report = pipeline.assess(decision_time, synthetic=True)
    assert len(report["rows"]) == 1
    assert report["rows"][0]["features"]["growth_per_hour"] == pytest.approx(100, rel=1e-12)
    assert report["rows"][0]["demand"]["status"] == "unknown"
    assert len(pipeline.evidence("reviewed-product", decision_time, synthetic=True)) == 2
    assert len(pipeline.datasets()) == 1
    replay = pipeline.replay(original[0])
    assert replay["records"] == 2
    assert pipeline.verify()["ok"] is True
    assert len(pipeline.evidence("reviewed-product", decision_time, synthetic=True)) == 2
    with Pipeline(pipeline.settings) as restarted:
        assert restarted.latest_assessments(synthetic=True)[0]["features"][
            "growth_per_hour"
        ] == pytest.approx(100, rel=1e-12)
        assert len(restarted.evidence("reviewed-product", decision_time, synthetic=True)) == 2
        assert restarted.latest_assessments(synthetic=False) == []


def test_real_import_cannot_backdate_when_the_system_acquired_evidence(pipeline):
    old = now() - timedelta(days=30)
    started = now()
    pipeline.import_records([video_observation("old-source-video", old, 250, synthetic=False)])
    pipeline.create_product("real-product", "Real imported product")
    pipeline.review_link("real-product", "fixture_import:video:old-source-video")
    visible = pipeline.evidence("real-product", now() + timedelta(seconds=1))
    assert len(visible) == 1
    assert utc(visible[0]["observed_at"]) == old
    assert utc(visible[0]["available_at"]) >= started
    assert pipeline.evidence("real-product", old + timedelta(days=1)) == []
    assert pipeline.status()["pending_live_acceptance"] is True


def test_rust_http_pagination_publishes_both_pages_before_advancing_the_cursor(
    pipeline, monkeypatch
):
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-access-token")
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 20, 1)
    first = {
        "data": {"videos": [{"id": "video-a", "view_count": 7}], "has_more": True, "cursor": 123},
        "error": {"code": "ok"},
    }
    second = {
        "data": {"videos": [{"id": "video-b", "view_count": 11}], "has_more": False},
        "error": {"code": "ok"},
    }
    with controlled_source([(200, first, {}), (200, second, {})]) as (endpoint, requests):
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint, "fixture_observed_at": now().isoformat()},
            synthetic=True,
        )
        result = pipeline.collect_batch(batch_size=2, synthetic=True)
    assert result["errors"] == []
    assert result["processed"] == 2
    assert len(result["datasets"]) == 2
    assert len(requests) == 2
    assert requests[1]["cursor"] == 123
    pipeline.create_product("two-page-product", "Product across two pages", synthetic=True)
    for entity in ("video-a", "video-b"):
        pipeline.review_link("two-page-product", f"tiktok_display:video:{entity}")
    evidence = pipeline.evidence("two-page-product", now() + timedelta(seconds=1), synthetic=True)
    assert {item["entity_id"] for item in evidence} == {"video-a", "video-b"}
    assert pipeline.verify()["ok"] is True
    assert pipeline.collect_batch(batch_size=1, synthetic=True)["processed"] == 0
    assert pipeline.latest_assessments(synthetic=False) == []


@pytest.mark.parametrize("retry_after", [120, 172800])
def test_source_rate_limit_preserves_the_task_without_publishing_or_retrying_ahead(
    pipeline, monkeypatch, retry_after
):
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-access-token")
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 20, 1)
    # Synthetic Rust requests intentionally do not bind real credential keys.
    # Seed the established shared-budget relationship through its native SQL API.
    pipeline.catalog.one(
        "SELECT * FROM reserve_request('tiktok_display',%s,'controlled-shared-credential')",
        (ACCOUNT,),
    )
    started = now()
    with controlled_source([(429, {}, {"Retry-After": str(retry_after)})]) as (endpoint, requests):
        task_id = pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        result = pipeline.collect_batch(batch_size=3, synthetic=True)
    assert result["processed"] == 0
    assert result["errors"][0]["category"] == "rate_limited"
    assert len(requests) == 1
    task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    assert task["due_at"] >= started + timedelta(seconds=retry_after)
    budget = pipeline.catalog.one(
        "SELECT blocked_until FROM request_budgets WHERE source='tiktok_display' AND account_id=%s",
        (ACCOUNT,),
    )
    assert budget["blocked_until"] >= started + timedelta(seconds=retry_after)
    shared = pipeline.catalog.one("SELECT blocked_until FROM credential_budgets")
    assert shared["blocked_until"] >= started + timedelta(seconds=retry_after)
    pipeline.catalog.configure_budget("tiktok_display", "other-alias", 20, 1)
    reservation = pipeline.catalog.one(
        "SELECT * FROM reserve_request('tiktok_display','other-alias',"
        "'controlled-shared-credential')"
    )
    assert reservation["allowed"] is False
    assert pipeline.datasets() == []
    queues = pipeline.status()["queues"]
    assert any(row["state"] == "pending" and row["count"] == 1 for row in queues)
    assert pipeline.collect_batch(batch_size=1, synthetic=True)["processed"] == 0


def test_unrepresentable_source_retry_window_is_visible_and_not_automatically_retried(pipeline):
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 20, 1)
    with controlled_source([(429, {}, {"Retry-After": "1e300"})]) as (endpoint, requests):
        task_id = pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        result = pipeline.collect_batch(3, synthetic=True)
    assert result["processed"] == 0
    assert result["errors"] == [{"task_id": task_id, "category": "invalid_retry_hint"}]
    assert len(requests) == 1
    assert pipeline.datasets() == []
    task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    assert task["state"] == "quarantined" and task["last_error"] == "invalid_retry_hint"
    assert pipeline.collect_batch(1, synthetic=True)["processed"] == 0


def test_collector_timeout_terminates_the_process_and_retains_retryable_work(pipeline, monkeypatch):
    arrived, release = threading.Event(), threading.Event()

    class SuspendedSource(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            arrived.set()
            release.wait(15)
            try:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"data":{"videos":[]},"error":{"code":"ok"}}')
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), SuspendedSource)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    children = []
    original_popen = subprocess.Popen

    def record_process(*args, **kwargs):
        child = original_popen(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(subprocess, "Popen", record_process)
    settings = pipeline.settings.model_copy(update={"collector_timeout_seconds": 5})
    try:
        with Pipeline(settings) as worker:
            task_id = worker.enqueue(
                "tiktok_display",
                ACCOUNT,
                "list",
                {"endpoint_override": f"http://127.0.0.1:{server.server_port}"},
                synthetic=True,
            )
            result = worker.collect_batch(1, synthetic=True)
            assert arrived.is_set()
            assert result["errors"] == [{"task_id": task_id, "category": "collector_timeout"}]
            assert len(children) == 1 and children[0].poll() is not None
            assert worker.datasets() == []
            task = worker.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
            assert task["state"] == "pending" and task["last_error"] == "collector_timeout"
            assert task["lease_owner"] is None and task["due_at"] > now()
            assert worker.collect_batch(1, synthetic=True)["processed"] == 0
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_cached_commercial_search_id_does_not_collapse_distinct_pages(pipeline, monkeypatch):
    monkeypatch.setenv("TIKTOK_CCL_ACCESS_TOKEN", "fixture-commercial-token")
    pipeline.catalog.configure_budget("tiktok_ads", ACCOUNT, 20, 1)
    pages = [
        (
            200,
            {
                "data": {
                    "ads": [{"ad": {"id": f"ad-{number}"}}],
                    "has_more": number < 3,
                    "search_id": "same-cached-search",
                },
                "error": {"code": "ok"},
            },
            {},
        )
        for number in (1, 2, 3)
    ]
    with controlled_source(pages) as (endpoint, requests):
        pipeline.enqueue(
            "tiktok_ads",
            ACCOUNT,
            "query",
            {"endpoint_override": endpoint, "filters": {}},
            synthetic=True,
        )
        result = pipeline.collect_batch(batch_size=3, synthetic=True)
    assert result["errors"] == []
    assert result["processed"] == 3
    assert len(requests) == 3
    assert len(set(result["datasets"])) == 3
    assert requests[1]["search_id"] == requests[2]["search_id"] == "same-cached-search"
    assert pipeline.collect_batch(batch_size=1, synthetic=True)["processed"] == 0


def test_runaway_pagination_stops_at_the_configured_limit_and_keeps_collected_evidence(
    pipeline, monkeypatch
):
    monkeypatch.setenv("TIKTOK_CCL_ACCESS_TOKEN", "fixture-commercial-token")
    settings = pipeline.settings.model_copy(update={"collection_max_pages": 2})
    pages = [
        (
            200,
            {
                "data": {
                    "ads": [{"ad": {"id": f"limited-ad-{number}"}}],
                    "has_more": True,
                    "search_id": "runaway-search",
                },
                "error": {"code": "ok"},
            },
            {},
        )
        for number in (1, 2, 3)
    ]
    with Pipeline(settings) as limited, controlled_source(pages) as (endpoint, requests):
        limited.enqueue(
            "tiktok_ads",
            ACCOUNT,
            "query",
            {"endpoint_override": endpoint, "filters": {}},
            synthetic=True,
        )
        result = limited.collect_batch(batch_size=5, synthetic=True)
        assert result["processed"] == 2
        assert result["errors"] == []
        assert len(requests) == 2
        assert len(limited.datasets()) == 2
        assert any(
            row["state"] == "quarantined" and row["count"] == 1
            for row in limited.status()["queues"]
        )
        assert limited.status()["queue_errors"][0]["last_error"] == "pagination_limit"
        assert limited.verify()["ok"] is True


def test_shared_quota_stops_a_second_process_before_its_http_request(pipeline, monkeypatch):
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-access-token")
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 1, 0.0001)
    response = {
        "data": {"videos": [{"id": "quota-video", "view_count": 9}], "has_more": False},
        "error": {"code": "ok"},
    }
    with controlled_source([(200, response, {})]) as (endpoint, requests):
        for number in (1, 2):
            pipeline.enqueue(
                "tiktok_display",
                ACCOUNT,
                "list",
                {"endpoint_override": endpoint},
                synthetic=True,
                idempotency_key=f"quota-task-{number}",
            )
        first = pipeline.collect_batch(batch_size=1, synthetic=True)
        with Pipeline(pipeline.settings) as independent_worker:
            second = independent_worker.collect_batch(batch_size=1, synthetic=True)
    assert first["processed"] == 1
    assert second["processed"] == 0
    assert second["errors"][0]["category"] == "quota_exhausted"
    assert len(requests) == 1
    assert len(pipeline.datasets()) == 1


def test_expired_worker_cannot_publish_and_a_replacement_can_complete_the_task(
    pipeline, monkeypatch
):
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-access-token")
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 20, 1)
    response = {
        "data": {"videos": [{"id": "recovered-video", "view_count": 13}], "has_more": False},
        "error": {"code": "ok"},
    }
    with controlled_source([(200, response, {})]) as (endpoint, requests):
        task_id = pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        abandoned = pipeline.catalog.claim("abandoned-worker", 1, True, 8)
        assert abandoned["task_id"] == task_id
        time.sleep(1.1)
        with pytest.raises(LeaseLost):
            pipeline.catalog.renew(abandoned, 180)
        recovered = pipeline.collect_batch(batch_size=1, synthetic=True)
    assert recovered["processed"] == 1
    assert recovered["errors"] == []
    assert len(requests) == 1
    assert len(pipeline.datasets()) == 1
    assert pipeline.verify()["ok"] is True


def test_only_the_new_lease_can_publish_after_a_worker_is_replaced(pipeline):
    task_id = pipeline.enqueue("tiktok_display", ACCOUNT, "list", {}, synthetic=True)
    old = pipeline.catalog.claim("old-worker", 1, True, 8)
    assert old["task_id"] == task_id
    time.sleep(1.1)
    new = pipeline.catalog.claim("new-worker", 180, True, 8)
    record = video_observation("leased-video", now(), 13)
    record["source"] = "tiktok_display"
    record["observation_id"] = "lease-observation"
    observation = Observation.model_validate(record)
    with pytest.raises(LeaseLost):
        pipeline.history.publish(
            [observation],
            {"records": [record]},
            source="tiktok_display",
            synthetic=True,
            dataset_id="stale-worker-result",
            task=old,
        )
    assert pipeline.datasets() == []
    pipeline.history.publish(
        [observation],
        {"records": [record]},
        source="tiktok_display",
        synthetic=True,
        dataset_id="replacement-worker-result",
        task=new,
    )
    assert pipeline.datasets()[0]["state"] == "committed"
    assert pipeline.verify()["ok"] is True


def test_repeated_assessment_does_not_duplicate_frozen_alerts(pipeline):
    cutoff = now()
    pipeline.import_records(
        [
            video_observation("alert-video", cutoff - timedelta(hours=2), 10),
            video_observation("alert-video", cutoff - timedelta(hours=1), 1000),
        ],
        synthetic=True,
    )
    pipeline.create_product("alert-product", "Alertable product", synthetic=True)
    pipeline.review_link("alert-product", "fixture_import:video:alert-video")
    decision = now() + timedelta(seconds=1)
    policy = AssessmentPolicy(alert_threshold=0)
    first = pipeline.assess(decision, synthetic=True, policy=policy)
    second = pipeline.assess(decision, synthetic=True, policy=policy)
    assert first["run_id"] == second["run_id"]
    alerts = pipeline.alerts(synthetic=True)
    assert len(alerts) == 1
    assert alerts[0]["payload"]["evidence_refs"] == first["rows"][0]["evidence_refs"]
    assert pipeline.alerts(synthetic=False) == []


def test_invalidated_input_hides_current_results_and_marks_frozen_alerts(pipeline):
    cutoff = now()
    dataset_ids = pipeline.import_records(
        [
            video_observation("invalidated-video", cutoff - timedelta(hours=2), 10),
            video_observation("invalidated-video", cutoff - timedelta(hours=1), 1000),
        ],
        synthetic=True,
    )
    pipeline.create_product("invalidated-product", "Product with removed evidence", synthetic=True)
    pipeline.review_link("invalidated-product", "fixture_import:video:invalidated-video")
    pipeline.assess(
        now() + timedelta(seconds=1), synthetic=True, policy=AssessmentPolicy(alert_threshold=0)
    )
    frozen_payload = pipeline.alerts(synthetic=True)[0]["payload"]
    pipeline.catalog.invalidate_dataset(dataset_ids[0], "source_retention_expired")
    assert pipeline.latest_assessments(synthetic=True) == []
    alert = pipeline.alerts(synthetic=True)[0]
    assert alert["invalidated"] is True
    assert alert["payload"] == frozen_payload
    assert pipeline.verify()["invalidated_current_assessments"] == 1


def test_verification_checks_remote_evidence_even_after_local_cache_warmup(pipeline):
    timestamp = now() - timedelta(hours=1)
    dataset_ids = pipeline.import_records(
        [video_observation("missing-object-video", timestamp, 500)], synthetic=True
    )
    pipeline.create_product("missing-object-product", "Product with storage loss", synthetic=True)
    pipeline.review_link("missing-object-product", "fixture_import:video:missing-object-video")
    assert len(pipeline.evidence("missing-object-product", now(), synthetic=True)) == 1
    manifest = pipeline.catalog.manifests(synthetic=True)[0]["manifest"]
    pipeline.history.client.delete_object(
        Bucket=pipeline.settings.s3_bucket, Key=manifest["normalized"][0]["key"]
    )
    verification = pipeline.verify(dataset_ids[0])
    assert verification["ok"] is False
    assert verification["failures"][0]["category"] == "missing_or_corrupt_object"


def test_missing_credentials_cannot_promote_live_access_after_synthetic_collection(
    pipeline, monkeypatch
):
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN", raising=False)
    result = pipeline.verify_live(
        "tiktok_display",
        ACCOUNT,
        "list",
        {},
        evidence="Approved-app documentation still requires a usable token",
        retention_days=7,
    )
    assert result["state"] == "unverified"
    assert result["ok"] is False
    assert pipeline.status()["pending_live_acceptance"] is True
    assert pipeline.datasets() == []


def test_empty_authorized_response_does_not_complete_live_observation_acceptance(
    pipeline, monkeypatch
):
    monkeypatch.setattr(
        pipeline.collector,
        "run",
        lambda request: {
            "raw": {"data": {"videos": []}},
            "records": [],
            "observed_at": now().isoformat(),
        },
    )
    result = pipeline.verify_live(
        "tiktok_display", ACCOUNT, "list", {}, evidence="controlled approval", retention_days=7
    )
    assert result["ok"] is False
    assert result["state"] == "unverified"
    assert result["details"]["endpoint_accessible"] is True
    assert result["details"]["acceptance_observation_count"] == 0
    assert pipeline.status()["pending_live_acceptance"] is True


def test_top_level_schema_drift_archives_safe_evidence_without_advancing_the_task(
    pipeline, monkeypatch
):
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-access-token")
    response = {
        "unexpected_layout": {"videos": [{"id": "drift-video"}]},
        "access_token": "fixture-access-token",
        "error": {"code": "ok"},
    }
    with controlled_source([(200, response, {})]) as (endpoint, requests):
        pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        result = pipeline.collect_batch(batch_size=1, synthetic=True)
    assert result["processed"] == 0
    assert result["errors"][0]["category"] == "schema"
    assert len(requests) == 1
    datasets = pipeline.datasets()
    assert len(datasets) == 1
    assert datasets[0]["row_count"] == 0
    assert datasets[0]["quarantine_count"] == 1
    assert any(
        row["state"] == "quarantined" and row["count"] == 1 for row in pipeline.status()["queues"]
    )
    assert pipeline.collect_batch(batch_size=1, synthetic=True)["processed"] == 0
    assert pipeline.status()["queue_errors"][0]["last_error"] == "schema"
    manifest = pipeline.catalog.manifests(synthetic=True)[0]
    archived = pipeline.history.raw(manifest)
    assert "unexpected_layout" in json.dumps(archived)
    assert "fixture-access-token" not in json.dumps(archived)
    assert pipeline.verify()["ok"] is True

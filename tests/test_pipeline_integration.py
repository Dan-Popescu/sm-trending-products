"""Evidence-to-decision scenarios against PostgreSQL and an HTTP S3 test service."""

from __future__ import annotations

import json
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


def test_source_rate_limit_preserves_the_task_without_publishing_or_retrying_ahead(
    pipeline, monkeypatch
):
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "fixture-access-token")
    pipeline.catalog.configure_budget("tiktok_display", ACCOUNT, 20, 1)
    with controlled_source([(429, {}, {"Retry-After": "120"})]) as (endpoint, requests):
        pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        result = pipeline.collect_batch(batch_size=3, synthetic=True)
    assert result["processed"] == 0
    assert result["errors"][0]["category"] == "rate_limited"
    assert len(requests) == 1
    assert pipeline.datasets() == []
    queues = pipeline.status()["queues"]
    assert any(row["state"] == "pending" and row["count"] == 1 for row in queues)
    assert pipeline.collect_batch(batch_size=1, synthetic=True)["processed"] == 0


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
    assert second["errors"] == []
    assert pipeline.catalog.quota_waiting(synthetic=True) == 1
    waiting = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE state='pending'")
    assert waiting["attempts"] == 0
    assert waiting["lease_version"] == 0
    assert len(requests) == 1
    assert len(pipeline.datasets()) == 1


def test_batched_entity_upserts_preserve_latest_time_and_delivery_order(pipeline):
    timestamp = now()
    records = []
    for index in range(503):
        record = video_observation("same-entity", timestamp, index)
        record["attributes"]["title"] = f"delivery-{index}"
        record["account_id"] = f"account-{index}"
        records.append(record)
    records.insert(501, video_observation("same-entity", timestamp - timedelta(hours=1), 1))
    pipeline.import_records(records, synthetic=True)
    current = pipeline.catalog.one(
        "SELECT * FROM entities WHERE entity_key='fixture_import:video:same-entity'"
    )
    assert current["title"] == "delivery-502"
    assert current["account_id"] == "account-502"
    assert current["last_observed_at"] == timestamp
    assert current["attributes"]["title"] == "delivery-502"
    older = video_observation("same-entity", timestamp - timedelta(hours=2), 0)
    older["attributes"]["title"] = "late older delivery"
    pipeline.import_records([older], synthetic=True)
    after = pipeline.catalog.one(
        "SELECT * FROM entities WHERE entity_key='fixture_import:video:same-entity'"
    )
    assert after["title"] == current["title"]
    assert after["account_id"] == current["account_id"]
    assert after["attributes"] == current["attributes"]


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


def test_commercial_list_alias_is_canonical_in_new_and_recovered_tasks(pipeline):
    for source in ("tiktok_ads", "tiktok_commercial"):
        task_id = pipeline.enqueue(source, ACCOUNT, "list", {"filters": {}}, synthetic=True)
        task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        assert task["operation"] == "query"
        # Existing persisted list tasks also get canonical provenance when read.
        task["operation"] = "list"
        assert pipeline._collection._request(task)["operation"] == "query"


def test_commercial_verification_alias_records_discovery_without_inventing_access(
    pipeline, monkeypatch
):
    monkeypatch.setattr(
        pipeline.collector,
        "run",
        lambda request: {
            "records": [],
            "raw": {"data": {}},
            "observed_at": now().isoformat(),
        },
    )
    for source in ("tiktok_ads", "tiktok_commercial"):
        result = pipeline.verify_live(
            source, ACCOUNT, "list", {"filters": {}}, evidence="approved-fixture", retention_days=7
        )
        assert result["state"] == "unverified"
        capabilities = pipeline.catalog.all(
            "SELECT capability,state,details FROM source_capabilities WHERE source=%s", (source,)
        )
        assert {row["capability"] for row in capabilities} == {"collect", "discovery"}
        assert all(row["state"] == "unverified" for row in capabilities)
        assert all(row["details"]["verified_operation"] == "query" for row in capabilities)


def test_grouped_display_refresh_uses_one_http_request_and_records_missing_ids(pipeline):
    keys = [f"tiktok_display:video:batched-{index}" for index in range(3)]
    for key in keys:
        pipeline.catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
            "VALUES(%s,true,'tiktok_display',%s,'video')",
            (key, ACCOUNT),
        )
    response = {
        "data": {
            "videos": [{"id": "batched-0", "view_count": 10}, {"id": "batched-2", "view_count": 20}]
        },
        "error": {"code": "ok"},
    }
    with controlled_source([(200, response, {})]) as (endpoint, requests):
        # Refresh compatibility includes the endpoint context in synthetic mode.
        origin = {
            "request": {
                "source": "tiktok_display",
                "account_id": ACCOUNT,
                "operation": "list",
                "payload": {"endpoint_override": endpoint},
            },
            "observed_at": now().isoformat(),
        }
        dataset = pipeline.history.publish([], origin, source="tiktok_display", synthetic=True)
        from psycopg.types.json import Jsonb

        pipeline.catalog.execute("UPDATE entities SET origin=%s", (Jsonb({"dataset_id": dataset}),))
        planned = pipeline.schedule(1, synthetic=True)
        assert planned["scheduled"] == 1
        assert planned["refresh_entities_scheduled"] == 3
        collected = pipeline.collect_batch(1, synthetic=True)
    assert collected["processed"] == 1
    assert collected["errors"] == []
    assert len(requests) == 1
    assert set(requests[0]["filters"]["video_ids"]) == {"batched-0", "batched-1", "batched-2"}
    published = pipeline.catalog.one(
        "SELECT * FROM datasets WHERE dataset_id=%s", (collected["datasets"][0],)
    )
    raw = pipeline.history.raw(published)
    assert raw["missing_entity_ids"] == ["batched-1"]
    assert raw["requested_entity_ids"] == ["batched-0", "batched-1", "batched-2"]
    rows = pipeline.history.read([published], as_of=now())
    assert {row.entity_key for row in rows} == {keys[0], keys[2]}


def test_grouped_commercial_refresh_paginates_once_and_blocks_new_members_of_active_query(pipeline):
    from psycopg.types.json import Jsonb

    first = {
        "data": {"ads": [{"ad": {"id": "page-one"}}], "has_more": True, "search_id": "same-cycle"},
        "error": {"code": "ok"},
    }
    final = {
        "data": {"ads": [{"ad": {"id": "page-two"}}], "has_more": False, "search_id": "same-cycle"},
        "error": {"code": "ok"},
    }
    with controlled_source([(200, first, {}), (200, final, {})]) as (endpoint, requests):
        origin = {
            "request": {
                "source": "tiktok_ads",
                "account_id": ACCOUNT,
                "operation": "list",
                "payload": {"filters": {"country_code": "FR"}, "endpoint_override": endpoint},
            },
            "observed_at": now().isoformat(),
        }
        dataset = pipeline.history.publish([], origin, source="tiktok_ads", synthetic=True)
        for index in range(10):
            pipeline.catalog.execute(
                "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,origin) "
                "VALUES(%s,true,'tiktok_ads',%s,'ad',%s)",
                (f"tiktok_ads:ad:member-{index}", ACCOUNT, Jsonb({"dataset_id": dataset})),
            )
        planned = pipeline.schedule(10, synthetic=True)
        assert planned["scheduled"] == 1
        assert planned["refresh_entities_scheduled"] == 10
        first_page = pipeline.collect_batch(1, synthetic=True)
        assert first_page["processed"] == 1
        pipeline.catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,origin) "
            "VALUES('tiktok_ads:ad:new-member',true,'tiktok_ads',%s,'ad',%s)",
            (ACCOUNT, Jsonb({"dataset_id": dataset})),
        )
        # The task's continuation gained max_count/search_id. Its physical query
        # identity still matches a newly due entity from the same origin query.
        during_cycle = pipeline.schedule(10, synthetic=True)
        assert during_cycle["scheduled"] == 0
        second_page = pipeline.collect_batch(1, synthetic=True)
    assert second_page["processed"] == 1
    assert second_page["errors"] == []
    assert len(requests) == 2
    assert "search_id" not in requests[0]
    assert requests[1]["search_id"] == "same-cycle"
    assert pipeline.catalog.one("SELECT count(*) AS count FROM collection_tasks")["count"] == 1
    task = pipeline.catalog.one("SELECT state,page_index,schedule_reason FROM collection_tasks")
    assert task["state"] == "done" and task["page_index"] == 1
    assert len(task["schedule_reason"]["entity_keys"]) == 10


@pytest.mark.parametrize("synthetic", [False, True])
def test_collection_hints_current_credentials_and_reports_wait_without_spawning(
    pipeline, monkeypatch, synthetic
):
    import hashlib

    old_token = "old-fixture-credential"
    rotated_token = "rotated-fixture-credential"
    next_token = "next-fixture-credential"
    old_key = hashlib.sha256(old_token.encode()).hexdigest()
    current_token = old_token if synthetic else rotated_token
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", current_token)
    pipeline.catalog.execute(
        "INSERT INTO credential_budgets(credential_key,capacity,refill_per_second,tokens,"
        "blocked_until) "
        "VALUES(%s,1000,1000,1000,clock_timestamp()+interval '1 hour')",
        (old_key,),
    )
    pipeline.catalog.execute(
        "INSERT INTO credential_bindings(source,account_id,credential_key) "
        "VALUES('tiktok_display',%s,%s)",
        (ACCOUNT, old_key),
    )
    if not synthetic:
        pipeline.catalog.execute(
            "INSERT INTO source_capabilities(source,account_id,capability,state) "
            "VALUES('tiktok_display',%s,'collect','available')",
            (ACCOUNT,),
        )
        pipeline.catalog.execute(
            "INSERT INTO source_policies(source,retention_days,verified) "
            "VALUES('tiktok_display',7,true)"
        )
    task_ids = [
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "query",
            {"filters": {"video_ids": [f"quota-{index}"]}},
            synthetic=synthetic,
            idempotency_key=f"current-credential-{index}",
        )
        for index in range(2)
    ]
    claims, waits, requests = [], [], []
    original_claim = pipeline.catalog.claim
    original_wait = pipeline.catalog.quota_waiting

    def claim(*args, **kwargs):
        claims.append(kwargs["credential_keys"])
        return original_claim(*args, **kwargs)

    def waiting(*args, **kwargs):
        waits.append(kwargs["credential_keys"])
        return original_wait(*args, **kwargs)

    def collect(request):
        requests.append(request)
        # A source block learned after one request must prevent launching any
        # remaining task. The next claim also observes the newly rotated token.
        pipeline.catalog.block_budget("tiktok_display", ACCOUNT, 60)
        monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", next_token)
        return {"records": [], "raw": {"data": {"videos": []}}, "observed_at": now().isoformat()}

    monkeypatch.setattr(pipeline.catalog, "claim", claim)
    monkeypatch.setattr(pipeline.catalog, "quota_waiting", waiting)
    monkeypatch.setattr(pipeline.collector, "run", collect)
    result = pipeline.collect_batch(10, synthetic=synthetic)
    assert result["processed"] == 1
    assert result["errors"] == []
    assert result["quota_waiting"] == 1 and result["waiting"] is True
    assert len(requests) == 1
    assert len(claims) == 2 and len(waits) == 1
    if synthetic:
        assert claims == [{}, {}] and waits == [{}]
    else:
        assert claims[0]["tiktok_display"] == hashlib.sha256(rotated_token.encode()).hexdigest()
        assert claims[1]["tiktok_display"] == hashlib.sha256(next_token.encode()).hexdigest()
        assert waits[0] == claims[1]
        assert claims[0]["tiktok_display"] != old_key
    tasks = pipeline.catalog.all(
        "SELECT state,attempts FROM collection_tasks WHERE task_id=ANY(%s)", (task_ids,)
    )
    assert sorted(row["state"] for row in tasks) == ["done", "pending"]
    assert all(row["attempts"] == 0 for row in tasks)

"""Public maintenance behaviors against SQL and HTTP object storage."""

import json
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from botocore.exceptions import ClientError

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def record(entity_id, observed_at, views):
    return {
        "source": "maintenance_fixture",
        "entity_type": "video",
        "entity_id": entity_id,
        "observed_at": observed_at.isoformat(),
        "available_at": observed_at.isoformat(),
        "synthetic": True,
        "metrics": {"view_count": {"lower": views, "upper": views}},
        "attributes": {"creator_id": "independent-creator", "title": "Useful product"},
    }


def test_compaction_preserves_evidence_and_original_provenance(pipeline):
    timestamp = now()
    first = pipeline.import_records(
        [record("video", timestamp - timedelta(hours=2), 100)], synthetic=True
    )[0]
    second = pipeline.import_records(
        [record("video", timestamp - timedelta(hours=1), 200)], synthetic=True
    )[0]
    pipeline.create_product("maintenance-product", "Maintenance product", synthetic=True)
    pipeline.review_link("maintenance-product", "maintenance_fixture:video:video")
    cutoff = now() + timedelta(seconds=1)
    before = pipeline.evidence("maintenance-product", cutoff, synthetic=True)
    result = pipeline.compact(synthetic=True, max_datasets=10)
    assert result["status"] == "compacted"
    assert set(result["parent_dataset_ids"]) == {first, second}
    assert result["rows"] == 2
    after = pipeline.evidence("maintenance-product", cutoff, synthetic=True)
    assert after == before
    assert {item["raw_dataset_id"] for item in after} == {first, second}
    assert pipeline.verify()["ok"] is True
    assert pipeline.compact(synthetic=True)["status"] == "no_work"
    replay = pipeline.replay(result["dataset_id"])
    assert replay["records"] == 2
    assert len(pipeline.evidence("maintenance-product", cutoff, synthetic=True)) == 2


def test_retention_preview_preserves_data_and_apply_removes_dependent_evidence(pipeline):
    timestamp = now()
    datasets = pipeline.import_records(
        [
            record("expired-video", timestamp - timedelta(hours=2), 100),
            record("expired-video", timestamp - timedelta(hours=1), 200),
        ],
        synthetic=True,
    )
    pipeline.create_product("expired-product", "Expired product", synthetic=True)
    pipeline.review_link("expired-product", "maintenance_fixture:video:expired-video")
    cutoff = now() + timedelta(seconds=1)
    pipeline.assess(cutoff, synthetic=True)
    pipeline.freeze(cutoff, synthetic=True)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        ("maintenance_fixture",),
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' "
        "WHERE dataset_id=ANY(%s)",
        (datasets,),
    )
    before = pipeline.catalog.export_state()
    preview = pipeline.retention()
    assert preview["applied"] is False
    assert set(preview["dataset_ids"]) == set(datasets)
    assert pipeline.catalog.export_state() == before
    assert pipeline.verify()["ok"] is True
    assert pipeline.latest_assessments(synthetic=True)
    assert pipeline.evaluations(synthetic=True)
    result = pipeline.retention(apply=True)
    assert result["applied"] is True
    assert result["pending_objects"] == 0
    assert pipeline.evidence("expired-product", now(), synthetic=True) == []
    assert pipeline.latest_assessments(synthetic=True) == []
    invalidated = pipeline.evaluations(synthetic=True)
    assert invalidated and all(item["invalidated"] for item in invalidated)
    assert all(item["payload"]["status"] == "invalidated" for item in invalidated)
    assert pipeline.alerts(synthetic=True) == []
    assert pipeline.verify()["ok"] is True
    with pytest.raises(ValueError):
        pipeline.replay(datasets[0])


def test_expired_parent_invalidates_compaction_but_preserves_fresh_original(pipeline):
    timestamp = now()
    expired = pipeline.import_records(
        [record("mixed-video", timestamp - timedelta(hours=2), 100)], synthetic=True
    )[0]
    fresh = pipeline.import_records(
        [record("mixed-video", timestamp - timedelta(hours=1), 200)], synthetic=True
    )[0]
    pipeline.create_product("mixed-product", "Mixed product", synthetic=True)
    pipeline.review_link("mixed-product", "maintenance_fixture:video:mixed-video")
    compacted = pipeline.compact(synthetic=True)["dataset_id"]
    replayed = pipeline.replay(compacted)["dataset_id"]
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        ("maintenance_fixture",),
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (expired,),
    )
    preview = pipeline.retention()
    assert {expired, compacted, replayed} <= set(preview["dataset_ids"])
    assert fresh not in preview["dataset_ids"]
    pipeline.retention(apply=True)
    remaining = pipeline.evidence("mixed-product", now(), synthetic=True)
    assert len(remaining) == 1
    assert remaining[0]["raw_dataset_id"] == fresh
    assert remaining[0]["metrics"]["view_count"]["lower"] == 200
    assert pipeline.verify()["ok"] is True


def test_failed_object_deletion_is_hidden_then_retried_and_staged_derivatives_are_removed(pipeline):
    dataset = pipeline.import_records(
        [record("retry-video", now() - timedelta(hours=1), 100)], synthetic=True
    )[0]
    pipeline.create_product("retry-product", "Retry product", synthetic=True)
    pipeline.review_link("retry-product", "maintenance_fixture:video:retry-video")
    derived_key = "assessments/synthetic/interrupted-publication.json"
    pipeline.catalog.stage_derivation(derived_key, [dataset])
    pipeline.history.put(derived_key, b'{"restricted":"derived evidence"}')
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        ("maintenance_fixture",),
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (dataset,),
    )

    class DeniedObjectStore(BaseHTTPRequestHandler):
        def do_DELETE(self):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"<Error><Code>AccessDenied</Code></Error>")

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), DeniedObjectStore)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    denied_settings = pipeline.settings.model_copy(
        update={"s3_endpoint_url": f"http://127.0.0.1:{server.server_port}"}
    )
    try:
        with Pipeline(denied_settings) as denied:
            result = denied.retention(apply=True)
        assert result["failed_objects"] > 0
        assert result["pending_objects"] > 0
        assert pipeline.evidence("retry-product", now(), synthetic=True) == []
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
    retried = pipeline.retention(apply=True)
    assert retried["pending_objects"] == 0
    assert retried["deleted_objects"] > 0
    with pytest.raises(ClientError) as missing:
        pipeline.history.client.head_object(Bucket=pipeline.history.bucket, Key=derived_key)
    assert missing.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    with pytest.raises(ValueError):
        pipeline.history.put(derived_key, b"cannot republish restricted evidence")


def test_retention_removes_all_source_versions_in_a_versioned_bucket(pipeline):
    client = pipeline.history.client
    bucket = pipeline.history.bucket
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    dataset = pipeline.import_records(
        [record("versioned-video", now() - timedelta(hours=1), 100)], synthetic=True
    )[0]
    metadata = next(
        item for item in pipeline.catalog.manifests(synthetic=True) if item["dataset_id"] == dataset
    )
    raw_key = metadata["manifest"]["raw"]["key"]
    response = client.get_object(Bucket=bucket, Key=raw_key)
    body = response["Body"].read()
    response["Body"].close()
    client.put_object(Bucket=bucket, Key=raw_key, Body=body, Metadata=response["Metadata"])
    assert len(client.list_object_versions(Bucket=bucket, Prefix=raw_key)["Versions"]) == 2
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        ("maintenance_fixture",),
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (dataset,),
    )
    result = pipeline.retention(apply=True)
    assert result["pending_objects"] == 0
    remaining = client.list_object_versions(Bucket=bucket, Prefix=raw_key)
    assert not remaining.get("Versions")
    assert not remaining.get("DeleteMarkers")


@pytest.mark.parametrize(
    ("source", "entity_type"),
    [("tiktok_ads", "ad"), ("tiktok_commercial", "commercial_content")],
)
def test_retention_rebinds_legacy_expired_origin_to_surviving_refresh_context(
    pipeline, source, entity_type
):
    timestamp = now()
    requests = []

    class AdSource(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            if source == "tiktok_ads":
                data = {
                    "ads": [
                        {
                            "ad": {"id": 123, "reach": {"unique_users_seen": "10000-19999"}},
                            "advertiser": {"business_id": len(requests)},
                        }
                    ]
                }
            else:
                data = {
                    "commercial_contents": [
                        {
                            "id": 123,
                            "creator": {"username": f"creator-{len(requests)}"},
                            "brand_names": ["Physical product"],
                        }
                    ]
                }
            body = json.dumps(
                {
                    "data": {**data, "has_more": False},
                    "error": {"code": "ok"},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), AdSource)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        ids = []
        for hours in (2, 1):
            pipeline.enqueue(
                source,
                "fixture-account",
                "query",
                {
                    "filters": {},
                    "endpoint_override": f"http://127.0.0.1:{server.server_port}",
                    "fixture_observed_at": (timestamp - timedelta(hours=hours)).isoformat(),
                },
                synthetic=True,
            )
            ids.extend(pipeline.collect_batch(batch_size=1, synthetic=True)["datasets"])
        assert len(ids) == 2
        pipeline.create_product("ad-product", "Ad product", synthetic=True)
        pipeline.review_link("ad-product", f"{source}:{entity_type}:123")
        pipeline.catalog.execute(
            "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
            (source,),
        )
        pipeline.catalog.execute(
            "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' "
            "WHERE dataset_id=%s",
            (ids[0],),
        )
        # Reproduce the legacy first-observation origin, independently of new catalog upserts.
        pipeline.catalog.execute(
            "UPDATE entities SET origin="
            "jsonb_build_object('dataset_id',%s::text,'source',%s::text) "
            "WHERE entity_key=%s",
            (ids[0], source, f"{source}:{entity_type}:123"),
        )
        pipeline.retention(apply=True)
        evidence = pipeline.evidence("ad-product", now(), synthetic=True)
        assert len(evidence) == 1
        assert evidence[0]["raw_dataset_id"] == ids[1]
        scheduled = pipeline.schedule(1, synthetic=True)
        assert scheduled["scheduled"] == 1
        assert not any(
            item["category"] == "refresh_origin_unavailable" for item in scheduled["warnings"]
        )
        refreshed = pipeline.collect_batch(batch_size=1, synthetic=True)
        assert refreshed["processed"] == 1
        assert refreshed["errors"] == []
        assert len(requests) == 3
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

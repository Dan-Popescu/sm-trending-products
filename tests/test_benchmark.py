"""The measured workload traverses collection, history, analysis, and decisions."""

from __future__ import annotations

import json
from urllib.parse import urlsplit

import pytest

from sm_trending_products.benchmark import _group_videos, benchmark
from sm_trending_products.contracts import utc

pytestmark = pytest.mark.integration


def test_large_identity_assignment_evaluates_each_video_once():
    evaluated = set()

    def product_for(video):
        assert video not in evaluated
        evaluated.add(video)
        return 0 if video < 6000 else video % 100

    groups = _group_videos(10000, product_for)
    assert len(evaluated) == 10000
    assert len(groups[0]) == 6040
    assert sorted(video for group in groups.values() for video in group) == list(range(10000))


def test_failed_object_verification_fails_the_benchmark(pipeline, tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "verify", lambda: {"ok": False})
    report = benchmark(pipeline, records=4, product_count=1, output=tmp_path / "failed.json")
    assert report["checks"]["objects_verified"] is False
    assert report["ok"] is False
    assert "objects_verified" in report["failed_checks"]


def test_parallel_skewed_workload_compacts_without_changing_the_published_records(
    pipeline, tmp_path
):
    report = benchmark(
        pipeline,
        records=80,
        product_count=4,
        output=tmp_path / "parallel.json",
        workers=3,
        source_latency_ms=2,
        hot_fraction=0.6,
        compaction=True,
    )
    assert report["ok"] is True
    assert report["workload"]["workers"] == 3
    assert report["workload"]["source_latency_ms"] == 2
    assert report["compaction"]["status"] == "compacted"
    assert report["measurements"]["actual_records"] == 80
    assert report["measurements"]["queue_final"] == [{"state": "done", "count": 4}]
    assert report["measurements"]["storage_traffic"]["put_requests"] > 0
    assert pipeline.verify()["ok"] is True
    assert pipeline.latest_assessments(synthetic=False) == []


def test_exhausted_budget_returns_a_failed_bounded_report_with_retryable_work(pipeline, tmp_path):
    report = benchmark(
        pipeline,
        records=80,
        product_count=4,
        output=tmp_path / "budget.json",
        workers=2,
        request_capacity=1,
        requests_per_second=0.0001,
    )
    assert report["ok"] is False
    assert any(error["category"] == "quota_exhausted" for error in report["errors"])
    assert report["checks"]["collection_completed"] is False
    assert any(row["state"] == "pending" for row in pipeline.status()["queues"])


def test_benchmark_measures_actual_evidence_and_preserves_shared_cache(pipeline, tmp_path):
    sentinel = pipeline.settings.cache_dir / "other-worker-cache-entry"
    sentinel.write_bytes(b"Existing cache entries belong to other work")
    output = tmp_path / "reports" / "benchmark.json"
    report = benchmark(pipeline, records=40, output=output, product_count=4)

    assert json.loads(output.read_text()) == report
    assert report["synthetic"] is True
    assert report["operating_envelope"] == "measured_sample_only"
    assert report["errors"] == []
    assert all(
        report["checks"][key] is True
        for key in (
            "objects_verified",
            "repeated_analysis_same_run",
            "forward_decision_created",
            "requested_records_published",
        )
    )
    measurements = report["measurements"]
    assert measurements["processed_pages"] == 2
    assert measurements["error_count"] == 0
    assert measurements["elapsed_seconds"] > 0
    assert measurements["cpu_seconds_parent"] > 0
    assert measurements["peak_parent_rss_kib"] > 0
    assert measurements["peak_child_rss_kib"] > 0
    assert measurements["cold_analysis_seconds"] > 0
    assert measurements["warm_analysis_seconds"] > 0
    traffic = measurements["storage_traffic"]
    assert traffic["put_requests"] > 0
    assert traffic["get_requests"] > 0
    assert traffic["upload_bytes"] > 0
    assert traffic["download_bytes"] > 0
    assert traffic["cache_hits"] > 0

    datasets = pipeline.datasets()
    assert len(datasets) == 2
    assert sum(row["row_count"] for row in datasets) == 40
    assert sum(row["quarantine_count"] for row in datasets) == 0
    assert measurements["actual_records"] == 40
    assert measurements["quarantine_count"] == 0
    assert measurements["actual_records_per_second"] == pytest.approx(
        40 / measurements["elapsed_seconds"]
    )
    products = pipeline.latest_assessments(synthetic=True)
    assert len(products) == 4
    assert all(row["score"] is not None for row in products)
    assert len({reference for row in products for reference in row["evidence_refs"]}) == 40
    forward = [row for row in pipeline.evaluations(synthetic=True) if row["kind"] == "forward"]
    assert len(forward) == 1
    assert len(forward[0]["payload"]["selected"]) == 4
    assert pipeline.latest_assessments(synthetic=False) == []
    assert pipeline.evaluations(synthetic=False) == []
    assert pipeline.verify()["ok"] is True
    assert sentinel.read_bytes() == b"Existing cache entries belong to other work"

    # Source request provenance proves this workload used the controlled loopback endpoint.
    manifests = pipeline.catalog.manifests(synthetic=True, as_of=utc(products[0]["as_of"]))
    assert len(manifests) == 2
    for manifest in manifests:
        raw = pipeline.history.raw(manifest)
        request = raw["request"]
        endpoint = urlsplit(request["endpoint_override"])
        assert request["synthetic"] is True
        assert request["account_id"] == report["benchmark_id"]
        assert endpoint.scheme == "http"
        assert endpoint.hostname == "127.0.0.1"

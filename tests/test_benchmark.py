"""The measured workload traverses collection, history, analysis, and decisions."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from sm_trending_products.benchmark import _disk_usage, _group_videos, benchmark
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
    assert report["checks"]["post_compaction_semantics_unchanged"] is True
    assert report["checks"]["post_cold_warm_same_run"] is True
    assert report["analysis_runs"]["pre"]["cold"] != report["analysis_runs"]["post"]["cold"]
    for label in ("pre", "post"):
        cold = report["phases"][f"analysis_{label}_cold"]
        warm = report["phases"][f"analysis_{label}_warm"]
        assert cold["cache_before"]["file_bytes"] == 0
        assert cold["storage_traffic"]["download_bytes"] > 0
        assert warm["storage_traffic"]["download_bytes"] == 0
        assert warm["storage_traffic"]["cache_hits"] > 0
        assert warm["cache_before"] == cold["cache_usage"]
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
    assert all(error["category"] == "quota_exhausted" for error in report["errors"])
    assert report["checks"]["collection_completed"] is False
    states = {row["state"]: row["count"] for row in report["measurements"]["queue_final"]}
    assert states == {"pending": 3, "done": 1}
    assert sum(states.values()) == report["measurements"]["queue_initial_tasks"] == 4
    assert report["measurements"]["processed_pages"] == 1
    # One racing worker may already have delayed its task after the atomic denial.
    assert 2 <= pipeline.catalog.quota_waiting(synthetic=True) <= 3
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
    assert measurements["collection_seconds"] < measurements["elapsed_seconds"]
    assert measurements["collection_records_per_second"] == pytest.approx(
        40 / measurements["collection_seconds"]
    )
    latency = measurements["task_latency_seconds"]
    assert latency["count"] == 2
    assert 0 < latency["min"] <= latency["p50"] <= latency["p95"] <= latency["max"]
    assert latency["max"] < measurements["elapsed_seconds"]
    assert measurements["queue_samples"][-1]["states"] == {"done": 2}
    records = measurements["source_record_bytes"]
    responses = measurements["source_response_bytes"]
    assert records["count"] == 40
    assert responses["count"] == 2
    assert records["min"] <= records["mean"] <= records["max"]
    assert responses["total"] > records["total"] > 0
    assert report["phases"]["analysis_pre_cold"]["cache_before"]["file_bytes"] == 0
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


def test_paced_arrival_drains_through_quota_refill_without_losing_requested_work(
    pipeline, tmp_path
):
    report = benchmark(
        pipeline,
        80,
        tmp_path / "paced.json",
        product_count=4,
        workers=2,
        arrival_rate=10,
        drain_seconds=4,
        request_capacity=1,
        requests_per_second=4,
    )
    assert report["ok"] is True
    assert report["complete"] is True
    assert report["status"] == "completed"
    assert report["measurements"]["queue_final"] == [{"state": "done", "count": 4}]
    assert report["measurements"]["actual_records"] == 80
    assert report["measurements"]["quota_waiting_tasks"] == 0
    assert report["measurements"]["drain_timed_out"] is False
    assert report["measurements"]["queue_sample_count"] >= 4


def test_paced_overload_writes_complete_timeout_and_partial_quota_accounting(pipeline, tmp_path):
    output = tmp_path / "overload.json"
    report = benchmark(
        pipeline,
        80,
        output,
        product_count=4,
        workers=2,
        arrival_rate=50,
        drain_seconds=0.25,
        request_capacity=1,
        requests_per_second=0.0001,
    )
    assert json.loads(output.read_text()) == report
    assert report["ok"] is False
    assert report["complete"] is True
    assert report["status"] == "drain_timeout"
    assert report["measurements"]["drain_timed_out"] is True
    assert report["measurements"]["actual_records"] == 20
    assert 2 <= report["measurements"]["quota_waiting_tasks"] <= 3
    assert report["measurements"]["quota_waiting_tasks"] == pipeline.catalog.quota_waiting(True)
    assert report["measurements"]["quota_deferred_tasks"] == 3
    assert report["measurements"]["task_latency_seconds"]["count"] == 1
    states = {row["state"]: row["count"] for row in report["measurements"]["queue_final"]}
    assert states == {"done": 1, "pending": 3}
    assert report["phases"]["collection"]["storage_traffic"]["put_requests"] > 0


def test_occupied_synthetic_workspace_is_rejected_before_queue_mutation(pipeline, tmp_path):
    pipeline.create_product("unrelated", "Unrelated synthetic evidence", synthetic=True)
    budgets = pipeline.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id")
    with pytest.raises(ValueError, match="empty isolated synthetic workspace"):
        benchmark(pipeline, 4, tmp_path / "occupied.json", product_count=1)
    assert pipeline.catalog.all("SELECT * FROM collection_tasks") == []
    assert (
        pipeline.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id") == budgets
    )


def test_live_workspace_and_nested_shared_cache_are_preserved(pipeline, tmp_path):
    pipeline.create_product("live-unrelated", "Live product", synthetic=False)
    nested = pipeline.settings.cache_dir / "unrelated" / "nested"
    nested.mkdir(parents=True)
    sentinel = nested / "payload"
    sentinel.write_bytes(b"unrelated cache")
    report = benchmark(pipeline, 4, tmp_path / "isolated.json", product_count=1)
    assert report["ok"] is True
    assert report["measurements"]["cache_usage"]["file_bytes"] >= len(b"unrelated cache")
    assert report["measurements"]["cache_usage"]["allocated_bytes"] > 0
    assert sentinel.read_bytes() == b"unrelated cache"
    assert all(row["product_id"] != "live-unrelated" for row in pipeline.latest_assessments(True))


def test_recursive_disk_usage_counts_hardlinked_payload_once_and_reservations_separately(tmp_path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"a" * 8192)
    pins = tmp_path / ".pins"
    pins.mkdir()
    os.link(payload, pins / "reader")
    downloads = tmp_path / ".downloads"
    downloads.mkdir()
    reservation = downloads / "future.reserve"
    reservation.write_text("99999")
    (tmp_path / "ignored-symlink").symlink_to(payload)
    usage = _disk_usage(tmp_path)
    assert usage["file_bytes"] == 8192 + len("99999")
    assert usage["reservation_declared_bytes"] == 99999
    assert usage["allocated_bytes"] >= 8192


def test_workflow_exception_retains_completed_collection_and_phase_telemetry(
    pipeline, tmp_path, monkeypatch
):
    def failed_verification():
        raise RuntimeError("unavailable object store")

    monkeypatch.setattr(pipeline, "verify", failed_verification)
    output = tmp_path / "exception.json"
    report = benchmark(pipeline, 4, output, product_count=1)
    assert report["ok"] is False
    assert report["complete"] is True
    assert report["status"] == "failed"
    assert report["checks"]["collection_completed"] is True
    assert report["checks"]["workflow_completed"] is False
    assert report["measurements"]["actual_records"] == 4
    assert report["measurements"]["task_latency_seconds"]["count"] == 2
    assert report["phases"]["verification"]["elapsed_seconds"] > 0
    assert report["phases"]["analysis_pre_cold"]["storage_traffic"]["get_requests"] > 0
    assert report["errors"] == [{"category": "benchmark_failed", "exception_type": "RuntimeError"}]
    assert json.loads(output.read_text()) == report


def test_existing_live_objects_are_rejected_before_global_verification_or_mutation(
    pipeline, tmp_path, monkeypatch
):
    observed = utc("2026-01-01T00:00:00Z")
    pipeline.import_records(
        [
            {
                "source": "unrelated-live",
                "entity_type": "video",
                "entity_id": "live-video",
                "observed_at": observed.isoformat(),
                "available_at": observed.isoformat(),
                "metrics": {"view_count": {"lower": 10, "upper": 10}},
            }
        ],
        synthetic=False,
    )
    before = pipeline.catalog.export_state()
    traffic = dict(pipeline.history.metrics)
    budgets = pipeline.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id")
    with pytest.raises(ValueError, match="verification inputs"):
        benchmark(pipeline, 4, tmp_path / "unrelated-live.json", product_count=1)
    assert pipeline.catalog.export_state() == before
    assert pipeline.history.metrics == traffic
    assert (
        pipeline.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id") == budgets
    )


def test_existing_live_empty_assessment_is_rejected_as_a_global_verification_input(
    pipeline, tmp_path
):
    pipeline.assess(synthetic=False)
    assert pipeline.catalog.one("SELECT count(*) AS count FROM datasets")["count"] == 0
    before = dict(pipeline.history.metrics)
    with pytest.raises(ValueError, match="verification inputs"):
        benchmark(pipeline, 4, tmp_path / "unrelated-assessment.json", product_count=1)
    assert pipeline.history.metrics == before
    assert pipeline.catalog.all("SELECT * FROM collection_tasks") == []


def test_paced_producer_failure_stops_workers_before_executor_join_and_reports_admitted_work(
    pipeline, tmp_path
):
    settings = pipeline.settings.model_dump(mode="json")
    settings["database_url"] = pipeline.settings.database_url.get_secret_value()
    output = tmp_path / "producer-failed.json"
    environment = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
        "BENCHMARK_FAILURE_SETTINGS": json.dumps(settings),
        "BENCHMARK_FAILURE_OUTPUT": str(output),
    }
    code = """
import os
import time
from pathlib import Path
from sm_trending_products.benchmark import benchmark
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.settings import Settings

settings = Settings.model_validate_json(os.environ['BENCHMARK_FAILURE_SETTINGS'])
with Pipeline(settings) as application:
    original_enqueue = application.enqueue
    calls = 0
    def failing_enqueue(*args, **kwargs):
        global calls
        calls += 1
        if calls == 2:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if application.catalog.one(
                    "SELECT count(*) AS count FROM collection_tasks WHERE state='done'"
                )['count'] == 1:
                    raise RuntimeError('injected producer failure after durable publication')
                time.sleep(0.01)
            raise AssertionError('first admitted task did not publish')
        return original_enqueue(*args, **kwargs)
    application.enqueue = failing_enqueue
    result = benchmark(application, 80, Path(os.environ['BENCHMARK_FAILURE_OUTPUT']),
                       product_count=4, workers=2, arrival_rate=100, drain_seconds=1)
    assert result['complete'] and not result['ok']
"""
    # A regression hangs in executor shutdown; a process deadline prevents hanging pytest.
    result = subprocess.run(
        [sys.executable, "-c", code], env=environment, capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(output.read_text())
    assert report["complete"] is True
    assert report["status"] == "failed"
    assert report["checks"]["collection_completed"] is False
    assert report["checks"]["workflow_completed"] is False
    measurements = report["measurements"]
    assert measurements["requested_tasks"] == 4
    assert measurements["admitted_tasks"] == 1
    assert measurements["not_enqueued_tasks"] == 3
    assert measurements["queue_final"] == [{"state": "done", "count": 1}]
    assert measurements["actual_records"] == 20
    assert measurements["storage_traffic"]["put_requests"] > 0
    assert report["phases"]["collection"]["storage_traffic"]["put_requests"] > 0

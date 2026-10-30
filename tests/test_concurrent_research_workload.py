"""Actual Rust acquisition overlaps complete public reports in owned disposable state."""

import importlib.util
import os
from pathlib import Path

import pytest

from sm_trending_products.concurrent_research_workload import Campaign, useful_overlap
from sm_trending_products.research_workload import Workload

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "concurrent_runner", ROOT / "scripts/concurrent-research-workload.py"
)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def campaign(**kwargs):
    return Campaign(collector_binary=str(ROOT / "collector/target/debug/sm-collector"), **kwargs)


def test_useful_overlap_requires_entire_first_publication_inside_report_call():
    task = {
        "task_id": "one",
        "completed": 2.5,
        "normalized_datasets": 1,
        "publish_call_started": 1.5,
        "publish_call_completed": 2,
        "first_normalization": True,
    }
    report = {
        "iteration": 0,
        "report_call_started": 1,
        "report_call_completed": 3,
        "reports": 2,
        "sql_execute_calls": 4,
    }
    assert useful_overlap([task], [report]) == [{"task_id": "one", "report_iteration": 0}]
    assert not useful_overlap([task], [{**report, "report_call_completed": 1.9}])
    assert not useful_overlap([task], [{**report, "sql_execute_calls": 0}])
    assert not useful_overlap([{**task, "normalized_datasets": 0}], [report])
    assert not useful_overlap([{**task, "first_normalization": False}], [report])
    # A delayed snapshot/checkpoint can observe an earlier commit inside a later
    # report. Observation time is only an upper bound, never proof of overlap.
    assert not useful_overlap(
        [{**task, "publish_call_started": 0.2, "publish_call_completed": 0.8}], [report]
    )
    # A call straddling the report start does not bound its commit inside it.
    assert not useful_overlap([{**task, "publish_call_started": 0.8}], [report])


def test_explicit_larger_profile_and_existing_binary_required():
    with pytest.raises(ValueError, match="existing executable"):
        Campaign(collector_binary="/missing/collector")
    with pytest.raises(ValueError, match="opt-in"):
        campaign(arrivals=17)
    with pytest.raises(ValueError, match="opt-in"):
        campaign(retained_days=30)


def test_refuses_live_database_before_owned_state(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:secret@cloud.example/prod")
    result = runner.run_campaign(
        campaign(), Workload(case="research"), output=tmp_path / "refused.json"
    )
    control = result["cases"]["collection_only"]
    assert not result["ok"] and control["status"] == "refused"
    assert control["cleanup"]["schema_dropped"] is None
    assert "secret" not in (tmp_path / "refused.json").read_text()


def test_native_actual_capture_reports_accounting_and_cleanup(tmp_path, monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.runner.existing, "memory_headroom", lambda: 512 * runner.runner.MIB)
    result = runner.run_campaign(
        campaign(arrivals=4, source_latency_seconds=0.15),
        Workload(case="research"),
        output=tmp_path / "matched.json",
        timeout=90,
    )
    assert result["ok"], result
    for name, case in result["cases"].items():
        assert case["checks"]["all_arrivals_durable"]
        assert case["accounting"]["current_rows"] == 8
        assert case["accounting"]["missing_rows"] == case["accounting"]["duplicate_rows"] == 0
        assert case["admitted_arrivals"] == len(case["collection"]) == 4
        assert case["complete_bytes_preserved"] and case["identity_preserved"]
        assert len(case["source_http"]) == 4
        assert case["queue_samples"]
        assert case["cleanup"] == {"schema_dropped": True, "files_removed": True, "errors": []}
        assert case["process_scope"]["postgresql_rss_bytes"] is None
        assert case["worker_lifetime_reaped_children_cpu_seconds"] > 0
        if name == "concurrent":
            assert case["useful_overlap"]
            for overlap in case["useful_overlap"]:
                task = next(t for t in case["collection"] if t["task_id"] == overlap["task_id"])
                read = next(
                    p for p in case["phases"] if p.get("iteration") == overlap["report_iteration"]
                )
                assert task["first_normalization"]
                assert read["report_call_started"] <= task["publish_call_started"]
                assert task["publish_call_completed"] <= read["report_call_completed"]
            reads = [p for p in case["phases"] if p.get("iteration", -1) >= 0]
            assert len(reads) >= 2
            assert reads[0]["storage"]["get_requests"] > 0
            assert reads[1]["storage"]["cache_hits"] > 0


def test_native_deadline_cleans_owned_state_and_keeps_usage_unknown(tmp_path, monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.runner.existing, "memory_headroom", lambda: 512 * runner.runner.MIB)
    result = runner.run_campaign(
        campaign(), Workload(case="research"), output=tmp_path / "deadline.json", timeout=0.01
    )
    case = result["cases"]["collection_only"]
    assert not result["ok"] and case["status"] == "case_deadline"
    assert case["cleanup"]["schema_dropped"] and case["cleanup"]["files_removed"]
    assert not case["usage_accounting_complete"] and case["usage"]["get_requests"] is None


def test_direct_worker_refuses_operator_schema_before_initialize(pipeline, tmp_path, monkeypatch):
    from sm_trending_products.concurrent_research_workload import worker

    initialized = []
    monkeypatch.setattr(type(pipeline), "initialize", lambda *a, **kw: initialized.append(True))
    values = pipeline.settings.model_dump(mode="json")
    values["database_url"] = pipeline.settings.database_url.get_secret_value()
    payload = {
        "checkpoint": str(tmp_path / "worker.json"),
        "owned": str(tmp_path),
        "settings": values,
        "campaign": campaign().model_dump(mode="json"),
        "workload": Workload(case="research").model_dump(),
        "max_upload_bytes": runner.runner.MIB,
    }
    assert (
        worker(
            payload,
            write_report=runner.runner.existing.write_report,
            sql_counts=runner.runner.sql_counts,
        )
        == 1
    )
    assert initialized == []


def test_exact_normalized_receipt_not_raw_task_completion(pipeline):
    from test_raw_capture import enqueue, one_shot_pages, page

    from sm_trending_products.concurrent_research_workload import task_snapshot

    with one_shot_pages([page("first"), page("second")]) as (endpoint, _requests):
        enqueue(pipeline, endpoint)
        pipeline.enqueue(
            "tiktok_display",
            "fixture-account",
            "list",
            {"endpoint_override": endpoint, "cursor": 1},
            synthetic=True,
        )
        assert pipeline.capture_batch(2, True)["processed"] == 2
    rows = task_snapshot(pipeline)
    assert len(rows) == 2 and all(row["state"] == "done" for row in rows)
    assert not any(row["normalized_dataset_id"] for row in rows)
    assert pipeline.publish_captures(1, True)["processed"] == 1
    rows = task_snapshot(pipeline)
    assert sum(bool(row["normalized_dataset_id"]) for row in rows) == 1
    assert sum(row["normalized_rows"] or 0 for row in rows) == 1


def test_native_shared_cache_pressure_preserves_reports_and_receipts(tmp_path, monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.runner.existing, "memory_headroom", lambda: 512 * runner.runner.MIB)
    result = runner.run_campaign(
        campaign(
            arrivals=4,
            source_latency_seconds=0.15,
            cache_bytes=1_000_000,
            source_padding_bytes=450000,
        ),
        # Real report work must exceed a complete first normalization interval;
        # one tiny product can phase-lock all four publications across report boundaries.
        # Four products remain a small 272-row fixture under the existing 1,000-row guard.
        Workload(case="research", products=4),
        output=tmp_path / "pressure.json",
        timeout=90,
    )
    assert result["ok"], result
    treatment = result["cases"]["concurrent"]
    assert treatment["shared_cache"]["captured_raw_bytes"] > 1_000_000
    assert treatment["shared_cache"]["captured_raw_names_evicted_after_replay"] > 0
    assert treatment["useful_overlap"] and treatment["complete_bytes_preserved"]


def test_native_report_refusal_checkpoints_known_traffic(tmp_path, monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.runner.existing, "memory_headroom", lambda: 512 * runner.runner.MIB)
    result = runner.run_campaign(
        campaign(),
        Workload(case="research", max_rows=1),
        output=tmp_path / "refused-report.json",
        timeout=60,
    )
    case = result["cases"]["collection_only"]
    assert not result["ok"] and case["status"] == "refused"
    refused = next(p for p in case["phases"] if p["name"] == "historical_report")
    assert refused["status"] == "refused" and refused["error_type"] == "RowBudgetExceeded"
    assert refused["storage"]["get_requests"] > 0
    assert case["checkpointed_usage"]["get_requests"] >= refused["storage"]["get_requests"]
    assert not case["usage_accounting_complete"] and case["usage"]["get_requests"] is None
    assert case["cleanup"]["schema_dropped"] and case["cleanup"]["files_removed"]


def test_native_sigterm_preserves_last_durable_checkpoint_before_cleanup(tmp_path):
    import json
    import signal
    import subprocess
    import sys
    import time

    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    output = tmp_path / "signal.json"
    code = """
import importlib.util
from pathlib import Path
from sm_trending_products.concurrent_research_workload import Campaign
from sm_trending_products.research_workload import Workload
spec = importlib.util.spec_from_file_location(
    'controlled_runner', 'scripts/concurrent-research-workload.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
runner.run_campaign(Campaign(collector_binary=str(Path('collector/target/debug/sm-collector').resolve()),
                             arrivals=16, source_latency_seconds=1.0),
                    Workload(case='research'), output=Path(__import__('sys').argv[1]), timeout=90)
"""
    environment = {**os.environ, "TMPDIR": str(tmp_path)}
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(output)],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    known = None
    try:
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline and process.poll() is None:
            for path in tmp_path.glob("sm-research-workload-*/checkpoint.json"):
                try:
                    checkpoint = json.loads(path.read_text())
                except (OSError, ValueError):
                    continue
                if checkpoint.get("collection") and checkpoint.get("setup", {}).get(
                    "storage", {}
                ).get("put_requests", 0):
                    known = checkpoint
                    break
            if known is not None:
                break
            time.sleep(0.02)
        assert known is not None, "Actual normalized arrival checkpoint was not reached"
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=20)
        report = json.loads(output.read_text())["cases"]["collection_only"]
        assert report["status"] == "runner_interrupted" and not report["ok"]
        assert (
            report["checkpointed_usage"]["put_requests"]
            >= known["setup"]["storage"]["put_requests"]
            > 0
        )
        assert len(report["phases"]) >= len(known["phases"])
        assert len(report["collection"]) >= len(known["collection"]) >= 1
        assert not report["usage_accounting_complete"] and report["usage"]["upload_bytes"] is None
        assert report["cleanup"] == {"schema_dropped": True, "files_removed": True, "errors": []}
        assert not list(tmp_path.glob("sm-research-workload-*"))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=20)

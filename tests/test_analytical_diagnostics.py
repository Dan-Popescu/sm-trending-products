"""Bounded operational attempts stay separate from analytical result identity."""

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event
from time import monotonic, sleep

import pytest
from test_feature_projection import video

import sm_trending_products.pipeline as pipeline_module
from sm_trending_products.analysis_batches import MetadataBudgetExceeded
from sm_trending_products.cli import main
from sm_trending_products.contracts import AssessmentPolicy, now


def seed(pipeline, *, products=1, samples=3):
    cutoff = now() + timedelta(minutes=1)
    for product in range(products):
        identifier = f"p{product}"
        pipeline.create_product(identifier, identifier.upper(), synthetic=True)
        for sample in range(samples):
            row = video(
                identifier,
                cutoff - timedelta(hours=samples - sample - 1),
                100 + 10 * sample,
                cutoff - timedelta(days=40),
            )
            pipeline.import_records([row.model_dump(mode="json")], synthetic=True)
        pipeline.review_link(identifier, row.entity_key)
    return cutoff


def test_repeated_results_have_distinct_inspectable_attempts_without_changing_their_hash(pipeline):
    cutoff = seed(pipeline)
    first = pipeline.assess(cutoff, True)
    second = pipeline.assess(cutoff, True)
    assert first == second
    attempts = pipeline.analytical_runs(True)
    assert len(attempts) == 2
    assert len({row["attempt_id"] for row in attempts}) == 2
    for attempt in attempts:
        assert attempt["operation"] == "assessment"
        assert attempt["state"] == "succeeded"
        assert attempt["result_id"] == first["run_id"]
        assert attempt["finished_at"] >= attempt["started_at"]
        assert attempt == pipeline.analytical_run(attempt["attempt_id"], True)
        assert pipeline.analytical_run(attempt["attempt_id"], False) is None
    assert pipeline.analytical_runs(False) == []


def test_cold_and_unchanged_runs_explain_their_work_and_storage_without_cumulative_leakage(
    pipeline,
):
    cutoff = seed(pipeline)
    before = dict(pipeline.history.metrics)
    pipeline.assess(cutoff, True)
    cold = pipeline.analytical_runs(True)[0]["summary"]
    assert cold["storage"] == {
        key: value - before[key] for key, value in pipeline.history.metrics.items()
    }
    assert cold["storage"]["get_requests"] >= 3
    assert cold["analysis"]["projection_misses"] == 1
    assert cold["work"]["source_rows_decoded"] == 3
    assert cold["work"]["source_objects_read"] == 3
    assert cold["work"]["metadata_objects"] == 3
    assert cold["work"]["metadata_bytes"] > 0
    assert {
        "metadata_selection",
        "payload_read",
        "calculation",
        "normalization",
        "publication",
    } <= cold["phase_ms"].keys()
    assert all(value >= 0 for value in cold["phase_ms"].values())
    pipeline.assess(cutoff, True)
    warm = pipeline.analytical_runs(True)[0]["summary"]
    assert warm["analysis"]["projection_hits"] == 1
    assert warm["analysis"]["projection_misses"] == 0
    assert warm["work"].get("source_rows_decoded", 0) == 0
    assert warm["work"].get("metadata_objects", 0) == 0


def test_frozen_decisions_and_failed_admission_have_separate_honest_diagnostics(pipeline):
    cutoff = seed(pipeline)
    frozen = pipeline.freeze(cutoff, True)
    attempt = pipeline.analytical_runs(True)[0]
    assert attempt["operation"] == "freeze"
    assert attempt["result_id"] == frozen["evaluation_id"]
    assert attempt["summary"]["work"]["source_rows_decoded"] == 3
    # A new context forces required-input admission without changing source data.
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    with pytest.raises(MetadataBudgetExceeded):
        pipeline.assess(cutoff, True)
    failed = pipeline.analytical_runs(True)[0]
    assert failed["state"] == "failed"
    assert failed["result_id"] is None
    assert failed["error_type"] == "MetadataBudgetExceeded"
    assert failed["summary"]["reasons"]["metadata_object_budget"] == 1
    assert failed["summary"]["work"].get("source_rows_decoded", 0) == 0


def test_adaptive_split_reports_successful_admission_and_actual_repeated_work(pipeline):
    cutoff = seed(pipeline, products=2, samples=2)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    assert len(pipeline.assess(cutoff, True)["rows"]) == 2
    stats = pipeline.analytical_runs(True)[0]["summary"]
    assert stats["work"]["partition_splits"] == 1
    assert stats["reasons"]["metadata_object_budget"] == 1
    assert stats["work"]["metadata_objects"] == 4
    assert stats["work"]["metadata_peak_objects"] == 2
    assert stats["work"]["source_rows_decoded"] == 4


def test_concurrent_attempts_are_visible_while_running_and_attribute_only_their_work(pipeline):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Requires independent native PostgreSQL backends")
    cutoff = seed(pipeline)
    before = dict(pipeline.history.metrics)
    with ThreadPoolExecutor(max_workers=2) as executor:
        with pipeline.catalog.transaction() as fence:
            pipeline.catalog.guard_retention(fence, exclusive=True)
            current = executor.submit(pipeline.assess, cutoff, True)
            historical = executor.submit(pipeline.assess, cutoff - timedelta(days=2), True)
            deadline = monotonic() + 10
            while monotonic() < deadline:
                running = pipeline.analytical_runs(True)
                if len(running) == 2:
                    break
                sleep(0.01)
            else:
                pytest.fail("Both attempts should be visible before their evidence reads")
            assert all(row["state"] == "running" for row in running)
        assert len(current.result(timeout=20)["rows"]) == 1
        assert historical.result(timeout=20)["rows"] == []
    attempts = {row["cutoff"]: row for row in pipeline.analytical_runs(True)}
    assert attempts[cutoff]["summary"]["work"]["source_rows_decoded"] == 3
    old = attempts[cutoff - timedelta(days=2)]["summary"]
    assert old["work"].get("source_rows_decoded", 0) == 0
    assert old["analysis"]["projection_misses"] == 0
    for metric, total in pipeline.history.metrics.items():
        assert (
            sum(row["summary"]["storage"][metric] for row in attempts.values())
            == total - before[metric]
        )


def test_nested_public_attempts_restore_the_parent_context(pipeline, monkeypatch):
    cutoff = seed(pipeline)
    before = dict(pipeline.history.metrics)
    get_object = pipeline.history.client.get_object
    nested = False

    def source_get(**arguments):
        nonlocal nested
        if not nested:
            nested = True
            assert pipeline.assess(cutoff - timedelta(days=2), True)["rows"] == []
        return get_object(**arguments)

    monkeypatch.setattr(pipeline.history.client, "get_object", source_get)
    pipeline.assess(cutoff, True)
    attempts = {row["cutoff"]: row for row in pipeline.analytical_runs(True)}
    assert len(attempts) == 2
    assert attempts[cutoff]["summary"]["work"]["source_rows_decoded"] == 3
    assert (
        attempts[cutoff - timedelta(days=2)]["summary"]["work"].get("source_rows_decoded", 0) == 0
    )
    for metric, total in pipeline.history.metrics.items():
        assert (
            sum(row["summary"]["storage"][metric] for row in attempts.values())
            == total - before[metric]
        )


def test_diagnostic_write_failure_preserves_success_and_the_original_analytical_failure(pipeline):
    cutoff = seed(pipeline)
    expected = pipeline.assess(cutoff, True)
    pipeline.catalog.execute(
        "ALTER TABLE analytical_run_diagnostics RENAME TO unavailable_diagnostics"
    )
    try:
        assert pipeline.assess(cutoff, True) == expected
        pipeline.history.settings = pipeline.settings.model_copy(
            update={"analysis_max_objects_per_batch": 2}
        )
        with pytest.raises(MetadataBudgetExceeded):
            pipeline.assess(cutoff, True, AssessmentPolicy(min_link_confidence=1.0))
    finally:
        pipeline.catalog.execute(
            "ALTER TABLE unavailable_diagnostics RENAME TO analytical_run_diagnostics"
        )
    assert len(pipeline.analytical_runs(True)) == 1


def test_retention_and_namespace_limits_bound_diagnostics(pipeline):
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analytical_diagnostics_max_runs": 2}
    )
    cutoff = now()
    for offset in range(3):
        pipeline.assess(cutoff + timedelta(seconds=offset), True)
    pipeline.assess(cutoff, False)
    assert len(pipeline.analytical_runs(True)) == 2
    assert len(pipeline.analytical_runs(False)) == 1
    assert len(pipeline.analytical_runs(True, limit=1)) == 1
    assert min(row["cutoff"] for row in pipeline.analytical_runs(True)) > cutoff
    assert all(
        len(json.dumps(row["summary"]).encode()) < 32768 for row in pipeline.analytical_runs(True)
    )
    with pytest.raises(ValueError, match="limit"):
        pipeline.analytical_runs(True, limit=101)


def test_cli_lists_and_selects_diagnostics_without_loading_source_evidence(
    pipeline, monkeypatch, capsys
):
    cutoff = seed(pipeline)
    pipeline.assess(cutoff, True)
    attempt = pipeline.analytical_runs(True)[0]
    monkeypatch.setenv("S3_BUCKET", pipeline.history.bucket)
    assert main(["analysis-runs", "--synthetic", "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["attempt_id"] == attempt["attempt_id"]
    assert main(["analysis-run", attempt["attempt_id"], "--synthetic"]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "succeeded"


def test_changed_and_oversized_optional_windows_explain_delta_then_full_work(pipeline):
    cutoff = seed(pipeline)
    pipeline.assess(cutoff, True)
    later = cutoff + timedelta(hours=1)
    row = video("p0", later, 140, cutoff - timedelta(days=40))
    pipeline.import_records([row.model_dump(mode="json")], synthetic=True)
    pipeline.assess(later, True)
    delta = pipeline.analytical_runs(True)[0]["summary"]
    assert delta["analysis"]["delta_windows"] == 1
    assert delta["work"]["source_rows_decoded"] == 1
    assert delta["work"]["local_rows_decoded"] == 3
    pipeline.catalog.execute(
        "UPDATE feature_product_windows w SET covered_object_ids=(SELECT array_agg(i) "
        "FROM unnest(w.covered_object_ids) AS i CROSS JOIN generate_series(1,100))"
    )
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 10}
    )
    row = video("p0", later + timedelta(hours=1), 150, cutoff - timedelta(days=40))
    pipeline.import_records([row.model_dump(mode="json")], synthetic=True)
    pipeline.assess(later + timedelta(hours=1), True)
    full = pipeline.analytical_runs(True)[0]["summary"]
    assert full["analysis"]["full_windows"] == 1
    assert full["work"]["source_rows_decoded"] == 5
    assert full["reasons"]["optional_coverage_object_budget"] == 1


def test_row_split_counts_work_discarded_by_the_failed_read_before_retrying(pipeline):
    cutoff = now() + timedelta(minutes=1)
    pipeline.settings.history_partition_buckets = 1
    for identifier in ("a", "b"):
        pipeline.create_product(identifier, identifier.upper(), synthetic=True)
        batch = [
            video(
                identifier,
                cutoff - timedelta(hours=59 - sample),
                100 + sample,
                cutoff - timedelta(days=40),
            )
            for sample in range(60)
        ]
        pipeline.import_records([row.model_dump(mode="json") for row in batch], synthetic=True)
        pipeline.review_link(identifier, batch[0].entity_key)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_rows_per_batch": 100}
    )
    pipeline.assess(cutoff, True)
    stats = pipeline.analytical_runs(True)[0]["summary"]
    assert stats["work"]["partition_splits"] == 1
    assert stats["reasons"]["row_budget"] == 1
    # 101 decoded before refusal, then two independently admitted 60-row reads.
    assert stats["work"]["source_rows_decoded"] == 221
    assert stats["work"]["source_rows_materialized"] == 240


def test_validation_failure_records_a_type_without_copying_its_sensitive_message(pipeline):
    cutoff = now()
    with pytest.raises(ValueError):
        pipeline.freeze(cutoff, True, {"horizon_days": ["sensitive-input-example"]})
    row = pipeline.analytical_runs(True)[0]
    assert row["state"] == "failed"
    assert row["error_type"] == "ValidationError"
    assert "sensitive-input-example" not in json.dumps(row, default=str)


def test_killed_cli_leaves_an_incomplete_running_marker_without_claiming_failure(
    pipeline, monkeypatch
):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Requires independent native PostgreSQL backends")
    monkeypatch.setenv("S3_BUCKET", pipeline.history.bucket)
    with pipeline.catalog.transaction() as fence:
        pipeline.catalog.guard_retention(fence, exclusive=True)
        process = subprocess.Popen(
            [sys.executable, "-m", "sm_trending_products", "assess", "--synthetic"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = monotonic() + 10
            while monotonic() < deadline:
                attempts = pipeline.analytical_runs(True)
                if attempts:
                    break
                assert process.poll() is None
                sleep(0.01)
            else:
                pytest.fail("CLI should publish its running marker before waiting for Evidence")
            process.kill()
            assert process.wait(timeout=5) != 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    attempt = pipeline.analytical_runs(True)[0]
    assert attempt["state"] == "running"
    assert attempt["finished_at"] is None
    assert attempt["result_id"] is None


def test_cli_limits_and_identifiers_fail_before_loading_configuration(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as error:
        main(["analysis-runs", "--limit", "0"])
    assert error.value.code == 2
    assert "between 1 and 100" in capsys.readouterr().err
    with pytest.raises(SystemExit) as error:
        main(["analysis-run", "not-an-attempt"])
    assert error.value.code == 2
    assert "Find identifiers with" in capsys.readouterr().err


def test_default_assessment_clock_still_resolves_after_the_retention_guard(pipeline, monkeypatch):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Requires independent native PostgreSQL backends")
    initial = now()
    after_guard = initial + timedelta(hours=1)
    released = Event()
    monkeypatch.setattr(
        pipeline_module, "now", lambda: after_guard if released.is_set() else initial
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pipeline.catalog.transaction() as fence:
            pipeline.catalog.guard_retention(fence, exclusive=True)
            future = executor.submit(pipeline.assess, synthetic=True)
            deadline = monotonic() + 5
            while monotonic() < deadline:
                if pipeline.analytical_runs(True):
                    break
                sleep(0.01)
            else:
                pytest.fail("An attempt should be visible before its evidence guard completes")
            released.set()
        result = future.result(timeout=10)
    assert result["as_of"] == after_guard
    assert pipeline.analytical_runs(True)[0]["cutoff"] == after_guard

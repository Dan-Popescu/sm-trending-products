"""Real retained-history supervision, accounting, semantic checks and stop guards."""

import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "retained_history", ROOT / "scripts/retained-history.py"
)
assert SPEC and SPEC.loader
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


@pytest.mark.parametrize(
    "arguments",
    [
        ["--products", "10001"],
        ["--history-days", "365"],
        ["--refreshes-per-day", "7"],
        ["--max-rss-bytes", "1"],
    ],
)
def test_invalid_workloads_fail_before_creating_measurement_state(tmp_path, arguments):
    with pytest.raises(SystemExit):
        RUNNER.arguments([*arguments, "--output", str(tmp_path / "invalid.json")])
    assert list(tmp_path.iterdir()) == []


def test_spill_fd_budget_rejects_unavailable_shapes_before_allocating_state(tmp_path, monkeypatch):
    monkeypatch.setattr(RUNNER.resource, "getrlimit", lambda _limit: (16384, 16384))
    options = RUNNER.arguments(
        ["--products", "10000", "--history-days", "90", "--output", str(tmp_path / "big.json")]
    )
    with pytest.raises(ValueError, match="does not raise system limits"):
        RUNNER.run(options)
    assert list(tmp_path.iterdir()) == []


def test_shared_and_hot_imports_enter_the_conservative_fd_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(RUNNER.resource, "getrlimit", lambda _limit: (16384, 16384))
    options = RUNNER.arguments(
        [
            "--products",
            "3000",
            "--hot-products",
            "3000",
            "--hot-product-batch-rows",
            "700",
            "--output",
            str(tmp_path / "big.json"),
        ]
    )
    with pytest.raises(ValueError, match="open descriptors"):
        RUNNER.adapter_budget(options)
    options.hot_products = 0
    options.shared_products_per_import = 100
    budget = RUNNER.adapter_budget(options)
    assert budget["estimated_source_datasets"] == 1440
    assert budget["estimated_spill_fd_upper_bound"] < 16384


def test_owned_file_observer_is_incremental_and_does_not_double_count_pins(tmp_path):
    payload = tmp_path / "payload"
    payload.write_bytes(b"a" * 4096)
    os.link(payload, tmp_path / "reader-pin")
    for index in range(3000):
        (tmp_path / f"empty-{index}").touch()
    accountant = RUNNER.DirectoryAccountant(tmp_path)
    first = accountant.sample()
    assert first["scan_in_progress"]
    second = accountant.sample()
    assert not second["scan_in_progress"]
    assert second["bytes"] == payload.stat().st_blocks * 512
    assert second["complete_pass_age_seconds"] is not None


def test_configuration_disk_failure_happens_before_a_schema_is_created(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://fixture:fixture@127.0.0.1/test")
    original = Path.write_text
    connections = []

    def write(path, *args, **kwargs):
        if path.name == "configuration.json":
            raise OSError("fixture disk full during configuration")
        return original(path, *args, **kwargs)

    def connect(*args, **kwargs):
        connections.append(args)
        pytest.fail("No database resource may exist before local configuration is durable")

    monkeypatch.setattr(Path, "write_text", write)
    monkeypatch.setattr(RUNNER.psycopg, "connect", connect)
    with pytest.raises(OSError, match="disk full"):
        RUNNER.run(RUNNER.arguments(["--products", "1", "--output", str(tmp_path / "case.json")]))
    assert connections == []
    assert not list(tmp_path.glob("retained-*"))


@pytest.mark.integration
@pytest.mark.parametrize("days", [30, 90])
@pytest.mark.parametrize("shared", [False, True])
def test_retained_history_counts_real_rows_and_checks_warm_delta_evicted_results(
    pipeline, tmp_path, days, shared
):
    output = tmp_path / "retained.json"
    options = RUNNER.arguments(
        [
            "--products",
            "3",
            "--videos-per-product",
            "2",
            "--history-days",
            str(days),
            "--refresh-waves",
            "1",
            "--deadline-seconds",
            "90",
            "--shared-products-per-import",
            "3" if shared else "1",
            "--hot-products",
            "1" if shared else "0",
            "--output",
            str(output),
        ]
    )
    report = RUNNER.run(options)
    assert report["ok"] and report["status"] == "complete"
    assert report["seeded_observations"] == 3 * 2 * days * 4
    assert report["seed_inventory"]["datasets"]["observations"] == report["seeded_observations"]
    assert report["phases"]["cold"]["selected_observations"] == 3 * 2 * min(days * 4, 121)
    assert report["phases"]["refresh_1"]["selected_observations"] == 3 * 2 * 121
    assert report["phases"]["warm_same_cutoff"]["metrics"]["get_requests"] == 0
    assert report["phases"]["warm_same_cutoff"]["analysis_work"]["projection_hits"] == 3
    assert report["phases"]["refresh_1"]["analysis_work"]["delta_windows"] == 3
    assert report["phases"]["evicted"]["analysis_work"]["full_windows"] == 3
    assert report["checks"]["sampled_complete_oracle"]
    assert report["cleanup"] == {"schema_removed": True, "working_directory_removed": True}
    assert json.loads(output.read_text())["seeded_products"] == 3
    assert os.environ["TEST_DATABASE_URL"] not in output.read_text()
    assert not list(tmp_path.glob("retained-*"))


@pytest.mark.integration
def test_deadline_preserves_requested_work_and_partial_report_without_silent_downscale(
    pipeline, tmp_path
):
    output = tmp_path / "stopped.json"
    report = RUNNER.run(
        RUNNER.arguments(["--products", "1000", "--deadline-seconds", "1", "--output", str(output)])
    )
    assert not report["ok"] and report["status"] == "deadline"
    assert report["requested"]["products"] == 1000
    assert report["seeded_products"] < 1000
    assert report["supervisor"]["exit_code"] != 0
    assert report["cleanup"]["schema_removed"]
    assert not list(tmp_path.glob("retained-*"))


def test_existing_report_is_not_overwritten_by_a_new_measurement(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://fixture:fixture@127.0.0.1/test")
    output = tmp_path / "previous.json"
    output.write_text('{"ok": true, "status": "complete"}')
    original = output.read_bytes()
    with pytest.raises(ValueError, match="new --output"):
        RUNNER.run(RUNNER.arguments(["--products", "1", "--output", str(output)]))
    assert output.read_bytes() == original
    assert list(tmp_path.iterdir()) == [output]

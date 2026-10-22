"""Owned process cleanup and native runner evidence, without running the load matrix."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "offline_benchmark", ROOT / "scripts/offline-benchmark.py"
)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def running(pid: int) -> bool:
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def test_owned_session_cleanup_kills_resistant_descendant_and_preserves_sibling(tmp_path):
    marker = tmp_path / "grandchild"
    descendant = "import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)"
    code = (
        "import subprocess,sys,time,pathlib;"
        f"child=subprocess.Popen([sys.executable,'-c',{descendant!r}]);"
        f"pathlib.Path({str(marker)!r}).write_text(str(child.pid));time.sleep(60)"
    )
    sibling = subprocess.Popen(
        [sys.executable, "-c", "import time;time.sleep(60)"], start_new_session=True
    )
    process = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    try:
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        child_pid = int(marker.read_text())
        runner.stop_session(process)
        deadline = time.monotonic() + 3
        while running(child_pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not running(child_pid)
        assert sibling.poll() is None
    finally:
        runner.stop_session(process)
        runner.stop_session(sibling)


def test_missing_proc_child_links_never_claim_complete_session_rss(monkeypatch):
    original = Path.read_text

    def read(path, *args, **kwargs):
        if path.name == "children":
            raise FileNotFoundError(str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    sample = runner.session_resources(os.getsid(0), {os.getpid()})
    # pytest may not be its session leader; the known owned PID still supplies one process.
    assert sample["process_count"] >= 1
    assert sample["discovered_rss_bytes"] > 0
    assert sample["descendant_discovery_complete"] is False
    assert sample["session_rss_bytes"] is None


def test_runner_rejects_unbounded_input_before_creating_state(tmp_path):
    with pytest.raises(ValueError, match="10000"):
        runner.run_case(
            ["--records", "10001", "--output", str(tmp_path / "report.json")],
            collector_binary=tmp_path / "absent",
        )
    assert not list(tmp_path.iterdir())


def test_native_case_reports_real_binary_default_layout_and_cleans_only_owned_state(tmp_path):
    database_url = os.environ.get("TEST_DATABASE_URL")
    binary = Path(
        os.environ.get("TEST_BENCHMARK_COLLECTOR", ROOT / "collector/target/debug/sm-collector")
    )
    if not database_url or not binary.exists():
        pytest.skip("Native test database and compiled collector required")
    sentinel = "runner_sentinel_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(sentinel)))
    output = tmp_path / "report.json"
    profile = "release" if "release" in binary.parts else "debug"
    try:
        report = runner.run_case(
            ["--records", "4", "--product-count", "1", "--output", str(output)],
            collector_binary=binary,
            collector_profile=profile,
            case_timeout=30,
        )
        assert report["ok"] is True
        assert report["environment"]["effective_settings"]["history_partition_buckets"] == 64
        assert report["environment"]["collector"] == runner.collector_identity(binary, profile)
        assert report["runner"]["cleanup_errors"] == []
        assert report["final_database_snapshot"]["queue"] == [{"state": "done", "count": 2}]
        assert report["published_data"]["original_published_rows"] == 4
        assert report["published_data"]["active_normalized_objects"]["count"] == 2
        assert database_url not in json.dumps(report)
        assert "database_url" not in report["environment"]["effective_settings"]
        assert json.loads(output.read_text()) == report
        with psycopg.connect(database_url, autocommit=True) as connection:
            assert connection.execute("SELECT to_regnamespace(%s)", (sentinel,)).fetchone()[0]
            assert (
                connection.execute(
                    "SELECT to_regnamespace(%s)", (report["runner"]["schema"],)
                ).fetchone()[0]
                is None
            )
        assert not Path(report["environment"]["effective_settings"]["cache_dir"]).parent.exists()
    finally:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(sentinel)))


def test_native_deadline_retains_final_queue_and_cleans_schema(tmp_path):
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Native test database required")
    binary = tmp_path / "slow-collector"
    binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(60)\n")
    binary.chmod(0o700)
    output = tmp_path / "deadline.json"
    report = runner.run_case(
        ["--records", "40", "--product-count", "4", "--output", str(output)],
        collector_binary=binary,
        case_timeout=4,
    )
    assert report["ok"] is False
    assert report["status"] == "case_deadline"
    assert report["runner"]["cleanup_errors"] == []
    assert report["runner"]["samples"]
    states = {row["state"]: row["count"] for row in report["final_database_snapshot"]["queue"]}
    assert sum(states.values()) == 2 and states.get("leased") == 1
    with psycopg.connect(database_url, autocommit=True) as connection:
        assert (
            connection.execute(
                "SELECT to_regnamespace(%s)", (report["runner"]["schema"],)
            ).fetchone()[0]
            is None
        )
    assert not Path(report["environment"]["effective_settings"]["cache_dir"]).parent.exists()


def test_matrix_refuses_low_headroom_without_starting_case(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "offline_envelope", ROOT / "scripts/offline-envelope.py"
    )
    assert spec and spec.loader
    matrix = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(matrix)
    monkeypatch.setitem(
        matrix.HELPER,
        "hardware_facts",
        lambda directory: {
            "memory_headroom_bytes": runner.GIB,
            "disk_free_bytes": 20 * runner.GIB,
        },
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("A guarded case must not start")

    monkeypatch.setitem(matrix.HELPER, "run_case", forbidden)
    summary = matrix.run_matrix(tmp_path, tmp_path / "unused", "release")
    assert summary["ok"] is False and summary["complete"] is False
    assert summary["stop_reason"] == "insufficient_memory_headroom"
    assert summary["cases"] == []
    assert json.loads((tmp_path / "summary.json").read_text()) == summary


@pytest.mark.parametrize("start_gib", [2, 3, 4])
def test_matrix_admission_profile_keeps_the_execution_stop_floor(tmp_path, monkeypatch, start_gib):
    spec = importlib.util.spec_from_file_location(
        "offline_envelope_profile", ROOT / "scripts/offline-envelope.py"
    )
    assert spec and spec.loader
    matrix = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(matrix)
    monkeypatch.setitem(
        matrix.HELPER,
        "hardware_facts",
        lambda directory: {
            "memory_headroom_bytes": 7 * runner.GIB // 2,
            "disk_free_bytes": 20 * runner.GIB,
        },
    )
    monkeypatch.setattr(matrix, "matrix_cases", lambda: [("small", [], False)])
    admitted = []

    def run_case(*args, **kwargs):
        admitted.append(kwargs)
        return {"ok": True, "status": "completed"}

    monkeypatch.setitem(matrix.HELPER, "run_case", run_case)
    if start_gib == 2:
        with pytest.raises(ValueError, match="stop floor"):
            matrix.run_matrix(tmp_path, tmp_path / "unused", "release", start_memory_headroom_gib=2)
        assert admitted == []
        return
    summary = matrix.run_matrix(
        tmp_path, tmp_path / "unused", "release", start_memory_headroom_gib=start_gib
    )
    assert bool(admitted) is (start_gib == 3)
    assert summary["ok"] is (start_gib == 3)
    assert summary["bounds"]["start_memory_headroom_bytes"] == start_gib * runner.GIB
    assert summary["bounds"]["stop_memory_headroom_bytes"] == 2 * runner.GIB

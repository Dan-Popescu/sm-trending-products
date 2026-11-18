"""Owned native scratch resources and real process-session cleanup."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest

from sm_trending_products.qualification import QualificationResources, stop_session


def native_database() -> str:
    database = os.environ.get("TEST_DATABASE_URL")
    if not database:
        pytest.skip("Dedicated native PostgreSQL required")
    return database


def running(pid: int) -> bool:
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        return False


def test_owned_worker_stops_before_scratch_cleanup_and_preserves_sibling(monkeypatch):
    sent = []
    killpg = os.killpg

    def recorded(group, number):
        sent.append((group, number))
        return killpg(group, number)

    monkeypatch.setattr(os, "killpg", recorded)
    resources = QualificationResources(
        native_database(), schema_prefix="benchmark_", directory_prefix="sm-qualification-"
    )
    marker = resources.directory / "descendant.pid"
    descendant = "import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)"
    code = (
        "import subprocess,sys,time,pathlib;"
        f"child=subprocess.Popen([sys.executable,'-c',{descendant!r}]);"
        f"pathlib.Path({str(marker)!r}).write_text(str(child.pid));time.sleep(60)"
    )
    sibling = subprocess.Popen(
        [sys.executable, "-c", "import time;time.sleep(60)"], start_new_session=True
    )
    try:
        with resources:
            resources.start()
            resources.launch([sys.executable, "-c", code], {}, environment=dict(os.environ))
            deadline = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            child = int(marker.read_text())
            resources.stop_worker()
            signals_after_stop = len(sent)
            resources.stop_worker()
            assert len(sent) == signals_after_stop
            deadline = time.monotonic() + 3
            while running(child) and time.monotonic() < deadline:
                time.sleep(0.02)
            assert not running(child)
            # The caller can read its terminal checkpoint before context cleanup.
            assert marker.exists()
            with psycopg.connect(resources.database_url) as connection:
                assert connection.execute("SELECT current_schema()").fetchone()[0] == (
                    resources.schema
                )
        assert not resources.directory.exists()
        assert len(sent) == signals_after_stop
        assert sibling.poll() is None
        assert resources.cleanup == {"schema_dropped": True, "files_removed": True, "errors": []}
        with psycopg.connect(native_database()) as connection:
            assert (
                connection.execute("SELECT to_regnamespace(%s)", (resources.schema,)).fetchone()[0]
                is None
            )
    finally:
        resources.close()
        stop_session(sibling)


def test_partial_object_store_start_still_retires_owned_schema_and_files(monkeypatch):
    from moto.server import ThreadedMotoServer

    start = ThreadedMotoServer.start
    endpoint = []

    def partially_started(server):
        start(server)
        endpoint.append(server.get_host_and_port())
        raise RuntimeError("Injected failure after the actual server starts")

    monkeypatch.setattr(ThreadedMotoServer, "start", partially_started)
    resources = QualificationResources(
        native_database(), schema_prefix="benchmark_", directory_prefix="sm-qualification-"
    )
    with pytest.raises(RuntimeError, match="Injected failure"):
        with resources:
            resources.start()
    assert endpoint
    with pytest.raises(OSError):
        with socket.create_connection(endpoint[0], timeout=0.5):
            pass
    assert resources.cleanup == {"schema_dropped": True, "files_removed": True, "errors": []}
    assert not resources.directory.exists()
    with psycopg.connect(native_database()) as connection:
        assert (
            connection.execute("SELECT to_regnamespace(%s)", (resources.schema,)).fetchone()[0]
            is None
        )


def test_cleanup_failure_is_recorded_and_other_owned_cleanup_still_runs(monkeypatch):
    from moto.server import ThreadedMotoServer

    stop = ThreadedMotoServer.stop

    def stopped_then_failed(server):
        stop(server)
        raise OSError("Injected post-stop failure")

    monkeypatch.setattr(ThreadedMotoServer, "stop", stopped_then_failed)
    resources = QualificationResources(
        native_database(), schema_prefix="benchmark_", directory_prefix="sm-qualification-"
    )
    with resources:
        resources.start()
    assert resources.cleanup == {
        "schema_dropped": True,
        "files_removed": True,
        "errors": ["OSError"],
    }
    resources.close()
    assert resources.cleanup["errors"] == ["OSError"]


def test_automatic_cleanup_keeps_stores_available_until_worker_shutdown(tmp_path):
    resources = QualificationResources(
        native_database(), schema_prefix="benchmark_", directory_prefix="sm-qualification-"
    )
    shutdown = tmp_path / "shutdown.json"
    ready = resources.directory / "ready"
    with resources:
        resources.start()
        code = f"""
import json,pathlib,signal,time,urllib.request
import psycopg
def shutdown(signum, frame):
    with psycopg.connect({resources.database_url!r}) as connection:
        schema = connection.execute('SELECT current_schema()').fetchone()[0]
    response = urllib.request.urlopen({resources.endpoint!r}, timeout=1)
    pathlib.Path({str(shutdown)!r}).write_text(json.dumps({{
        'schema': schema, 'moto_status': response.status}}))
    raise SystemExit(0)
signal.signal(signal.SIGTERM, shutdown)
pathlib.Path({str(ready)!r}).write_text('ready')
while True:
    time.sleep(1)
"""
        resources.launch([sys.executable, "-c", code], {}, environment=dict(os.environ))
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
    assert json.loads(shutdown.read_text()) == {"schema": resources.schema, "moto_status": 200}
    assert resources.cleanup == {"schema_dropped": True, "files_removed": True, "errors": []}


def test_failed_worker_input_still_owns_and_reaps_the_started_process(tmp_path):
    resources = QualificationResources(
        native_database(), schema_prefix="benchmark_", directory_prefix="sm-qualification-"
    )
    marker = tmp_path / "worker.pid"
    code = (
        "import os,pathlib,time;"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()));"
        "os.close(0);time.sleep(60)"
    )
    with pytest.raises(BrokenPipeError):
        with resources:
            resources.start()
            resources.launch(
                [sys.executable, "-c", code],
                {"input": "x" * 1024**2},
                environment=dict(os.environ),
            )
    assert not running(int(marker.read_text()))
    assert resources.cleanup == {"schema_dropped": True, "files_removed": True, "errors": []}

"""Draining fences new claims while preserving already accepted publication."""

import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sm_trending_products.contracts import digest
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


def queued(pipeline, suffix):
    return pipeline.enqueue(
        "tiktok_display",
        "fixture-account",
        "list",
        {},
        synthetic=True,
        idempotency_key=f"drain-{suffix}",
    )


def start_worker_process(pipeline):
    settings = pipeline.settings.model_dump(mode="json")
    settings["database_url"] = pipeline.settings.database_url.get_secret_value()
    settings["worker_id"] = "process-worker"
    process = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            "import sys;from sm_trending_products.pipeline import Pipeline;"
            "from sm_trending_products.settings import Settings;"
            "from sm_trending_products.cli import dispatch,parser;"
            "settings=Settings.model_validate_json(sys.stdin.read());"
            "application=Pipeline(settings);"
            "dispatch(application,parser().parse_args(['worker','--loop','--synthetic',"
            "'--batch-size','20','--poll-seconds','0.1']));application.close()",
        ],
        env=dict(os.environ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.stdin.write(json.dumps(settings).encode())
    process.stdin.close()
    process.stdin = None
    return process


def test_acknowledged_drain_rejects_new_claims_but_allows_the_current_checkpoint(pipeline):
    control = WorkerControl(pipeline.catalog)
    worker = control.register("collector-a", synthetic=True)
    first, second = queued(pipeline, "first"), queued(pipeline, "second")
    claimed = pipeline.catalog.claim(
        "collector-a", 180, True, 8, worker_generation=worker["generation"]
    )
    assert claimed["task_id"] in {first, second}
    admitted = claimed["task_id"]
    assert control.request_drain("collector-a", worker["generation"])["state"] == "draining"
    assert control.acknowledge_drain("collector-a", worker["generation"])["state"] == "draining"
    assert control.status("collector-a")["owned_leases"] == 1
    assert (
        pipeline.catalog.claim("collector-a", 180, True, 8, worker_generation=worker["generation"])
        is None
    )
    pipeline.catalog.renew(claimed, 180)
    pipeline.history.publish(
        [],
        {"observed_at": claimed["created_at"].isoformat(), "next_payload": None},
        source="tiktok_display",
        synthetic=True,
        task=claimed,
        dataset_id=digest({"task": admitted, "page_index": 0}),
    )
    assert control.acknowledge_drain("collector-a", worker["generation"])["state"] == "drained"
    assert control.status("collector-a")["owned_leases"] == 0
    assert control.resume("collector-a", worker["generation"])["state"] == "running"
    next_task = pipeline.catalog.claim(
        "collector-a", 180, True, 8, worker_generation=worker["generation"]
    )
    assert next_task["task_id"] in {first, second} - {admitted}


def test_worker_scope_and_generation_cannot_be_bypassed_or_silently_reused(pipeline):
    control = WorkerControl(pipeline.catalog)
    old = control.register("reused-slot", synthetic=True)
    queued(pipeline, "generation")
    assert pipeline.catalog.claim("reused-slot", 180, True, 8) is None
    assert (
        pipeline.catalog.claim("reused-slot", 180, False, 8, worker_generation=old["generation"])
        is None
    )
    with pytest.raises(ValueError, match="already registered"):
        control.register("reused-slot", synthetic=True)
    assert control.finish("reused-slot", old["generation"])["state"] == "stopped"
    current = control.register("reused-slot", synthetic=True)
    assert current["generation"] > old["generation"]
    assert (
        pipeline.catalog.claim("reused-slot", 180, True, 8, worker_generation=old["generation"])
        is None
    )
    with pytest.raises(ValueError, match="generation"):
        control.resume("reused-slot", old["generation"])
    assert (
        pipeline.catalog.claim("reused-slot", 180, True, 8, worker_generation=current["generation"])
        is not None
    )


def test_claim_waits_for_the_drain_barrier_while_other_workers_keep_progressing(pipeline):
    control = WorkerControl(pipeline.catalog)
    worker = control.register("barrier", synthetic=True)
    queued(pipeline, "barrier-first")
    queued(pipeline, "barrier-second")
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pipeline.catalog.transaction() as connection:
            connection.execute(
                "UPDATE collection_workers SET state='draining' WHERE worker_id='barrier'"
            )
            blocked = pool.submit(
                pipeline.catalog.claim,
                "barrier",
                180,
                True,
                8,
                worker_generation=worker["generation"],
            )
            with pytest.raises(TimeoutError):
                blocked.result(timeout=0.1)
            # Legacy callers remain compatible and a different worker is not blocked.
            assert pool.submit(pipeline.catalog.claim, "other", 180, True, 8).result(timeout=2)
        assert blocked.result(timeout=2) is None


def test_unknown_worker_cannot_be_reported_as_safely_drained(pipeline):
    control = WorkerControl(pipeline.catalog)
    with pytest.raises(ValueError, match="Unknown worker"):
        control.request_drain("unknown", 1)


def test_failed_replacement_can_cancel_a_pending_drain_without_abandoning_accepted_work(pipeline):
    control = WorkerControl(pipeline.catalog)
    worker = control.register("rollback-old", synthetic=True)
    queued(pipeline, "rollback")
    task = pipeline.catalog.claim(
        "rollback-old", 180, True, 8, worker_generation=worker["generation"]
    )
    control.request_drain("rollback-old", worker["generation"])
    resumed = control.resume("rollback-old", worker["generation"])
    assert resumed["state"] == "running" and resumed["drain_requested_at"] is None
    assert control.resume("rollback-old", worker["generation"]) == resumed
    assert control.status("rollback-old")["owned_leases"] == 1
    pipeline.catalog.renew(task, 180)


def test_restart_requires_stale_heartbeat_and_expired_leases_and_fences_old_commands(pipeline):
    from sm_trending_products.catalog import LeaseLost

    control = WorkerControl(pipeline.catalog)
    old = control.register("restart-slot", synthetic=True)
    queued(pipeline, "restart")
    task = pipeline.catalog.claim("restart-slot", 180, True, 8, worker_generation=old["generation"])
    pipeline.catalog.execute(
        "UPDATE collection_workers SET heartbeat_at=clock_timestamp()-interval '1 hour'"
    )
    with pytest.raises(ValueError, match="unfinished work"):
        control.register("restart-slot", synthetic=True)
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET lease_until=clock_timestamp()-interval '1 second'"
    )
    pipeline.catalog.execute("UPDATE collection_workers SET heartbeat_at=clock_timestamp()")
    with pytest.raises(ValueError, match="already registered"):
        control.register("restart-slot", synthetic=True)
    pipeline.catalog.execute(
        "UPDATE collection_workers SET heartbeat_at=clock_timestamp()-interval '1 hour'"
    )
    current = control.register("restart-slot", synthetic=True)
    assert current["generation"] == old["generation"] + 1
    with pytest.raises(ValueError, match="generation"):
        control.request_drain("restart-slot", old["generation"])
    assert control.status("restart-slot")["state"] == "running"
    with pytest.raises(LeaseLost):
        pipeline.catalog.renew(task, 180)
    assert (
        pipeline.catalog.claim("restart-slot", 180, True, 8, worker_generation=old["generation"])
        is None
    )
    recovered = pipeline.catalog.claim(
        "restart-slot", 180, True, 8, worker_generation=current["generation"]
    )
    assert recovered["task_id"] == task["task_id"]
    assert recovered["lease_version"] > task["lease_version"]


@pytest.mark.parametrize("clean_stop", [False, True])
def test_restart_preserves_requested_drain_until_explicit_new_generation_resume(
    pipeline, clean_stop
):
    control = WorkerControl(pipeline.catalog)
    old = control.register("paused-slot", synthetic=True)
    control.request_drain("paused-slot", old["generation"])
    if clean_stop:
        control.acknowledge_drain("paused-slot", old["generation"])
        control.finish("paused-slot", old["generation"])
    else:
        pipeline.catalog.execute(
            "UPDATE collection_workers SET heartbeat_at=clock_timestamp()-interval '1 hour'"
        )
    current = control.register("paused-slot", synthetic=True)
    assert current["state"] == "draining" and current["drain_requested_at"] is not None
    queued(pipeline, "paused-restart")
    assert (
        pipeline.catalog.claim("paused-slot", 180, True, 8, worker_generation=current["generation"])
        is None
    )
    assert control.acknowledge_drain("paused-slot", current["generation"])["state"] == "drained"
    with pytest.raises(ValueError, match="generation"):
        control.resume("paused-slot", old["generation"])
    control.resume("paused-slot", current["generation"])
    assert pipeline.catalog.claim(
        "paused-slot", 180, True, 8, worker_generation=current["generation"]
    )


def test_drain_lock_and_settling_timeouts_are_bounded_and_keep_accepted_work(pipeline):
    control = WorkerControl(pipeline.catalog)
    worker = control.register("blocked-slot", synthetic=True)
    queued(pipeline, "bounded")
    task = pipeline.catalog.claim(
        "blocked-slot", 180, True, 8, worker_generation=worker["generation"]
    )
    with pipeline.catalog.transaction() as connection:
        connection.execute(
            "SELECT * FROM collection_workers WHERE worker_id='blocked-slot' FOR UPDATE"
        )
        started = time.monotonic()
        result = control.request_drain("blocked-slot", worker["generation"], timeout_seconds=0.1)
        assert time.monotonic() - started < 2
        assert result["status"] == "blocked" and result["reason"] == "admission_lock_timeout"
        assert result["worker"]["owned_leases"] == 1
    assert control.request_drain("blocked-slot", worker["generation"])["state"] == "draining"
    started = time.monotonic()
    result = control.wait_drained("blocked-slot", worker["generation"], timeout_seconds=0.1)
    assert time.monotonic() - started < 2
    assert result["status"] == "blocked" and result["reason"] == "drain_timeout"
    assert result["worker"]["state"] == "draining"
    assert result["worker"]["owned_leases"] == 1
    pipeline.catalog.renew(task, 180)


def test_drain_timeout_diagnostics_cannot_hang_behind_a_table_lock(pipeline):
    control = WorkerControl(pipeline.catalog)
    worker = control.register("table-blocked", synthetic=True)
    with pipeline.catalog.transaction() as connection:
        connection.execute("LOCK TABLE collection_workers IN ACCESS EXCLUSIVE MODE")
        started = time.monotonic()
        result = control.request_drain("table-blocked", worker["generation"], timeout_seconds=0.1)
        assert time.monotonic() - started < 2
        assert result["status"] == "blocked" and result["worker"] is None
    assert control.status("table-blocked")["state"] == "running"


def test_operator_commands_distinguish_requested_drain_from_acknowledged_drain(pipeline):
    from sm_trending_products.cli import dispatch, parser

    session = pipeline.start_worker(True)
    worker_id = pipeline.worker_id
    assert (
        dispatch(
            pipeline,
            parser().parse_args(
                [
                    "worker-drain",
                    "--worker-id",
                    worker_id,
                    "--generation",
                    str(session["generation"]),
                ]
            ),
        )["state"]
        == "draining"
    )
    status = dispatch(pipeline, parser().parse_args(["worker-status", "--worker-id", worker_id]))
    assert status["owned_leases"] == 0 and status["drain_acknowledged_at"] is None
    wait = parser().parse_args(
        [
            "worker-drain",
            "--worker-id",
            worker_id,
            "--generation",
            str(session["generation"]),
            "--wait-seconds",
            "0.1",
        ]
    )
    assert dispatch(pipeline, wait)["status"] == "blocked"
    assert (
        pipeline.worker_control.acknowledge_drain(worker_id, session["generation"])["state"]
        == "drained"
    )
    assert dispatch(pipeline, wait)["state"] == "drained"
    assert (
        dispatch(
            pipeline,
            parser().parse_args(
                [
                    "worker-resume",
                    "--worker-id",
                    worker_id,
                    "--generation",
                    str(session["generation"]),
                ]
            ),
        )["state"]
        == "running"
    )


@pytest.mark.parametrize("mode", ["signal", "remote", "crash"])
def test_actual_worker_finishes_its_request_and_quiesces_before_claiming_another(pipeline, mode):
    started, release = threading.Event(), threading.Event()
    requests = []

    class Source(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            started.set()
            assert release.wait(15)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            try:
                self.wfile.write(
                    json.dumps(
                        {
                            "data": {
                                "videos": [{"id": f"response-{len(requests)}", "view_count": 10}],
                                "has_more": False,
                            },
                            "error": {"code": "ok"},
                        }
                    ).encode()
                )
            except BrokenPipeError:
                pass  # A killed predecessor cannot receive its abandoned response.

        def log_message(self, *args):
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
    thread = threading.Thread(target=source.serve_forever, daemon=True)
    thread.start()
    process = None
    try:
        for index in range(3):
            pipeline.enqueue(
                "tiktok_display",
                "fixture-account",
                "list",
                {"endpoint_override": f"http://127.0.0.1:{source.server_port}"},
                synthetic=True,
                idempotency_key=f"slow-{index}",
            )
        process = start_worker_process(pipeline)
        assert started.wait(15), "Actual collector never reached the controlled source"
        control = WorkerControl(pipeline.catalog)
        if mode == "signal":
            process.send_signal(signal.SIGTERM)
        elif mode == "remote":
            generation = control.status("process-worker")["generation"]
            assert control.request_drain("process-worker", generation)["state"] == "draining"
        else:
            generation = control.status("process-worker")["generation"]
            process.kill()
            process.communicate(timeout=5)
            pipeline.catalog.execute(
                "UPDATE collection_workers SET heartbeat_at=clock_timestamp()-interval '1 hour'"
            )
            pipeline.catalog.execute(
                "UPDATE collection_tasks SET lease_until=clock_timestamp()-interval '1 second' "
                "WHERE state='leased'"
            )
            process = start_worker_process(pipeline)
        release.set()
        if mode == "remote":
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = control.status("process-worker")
                if state["state"] == "drained":
                    break
                time.sleep(0.05)
            assert state["state"] == "drained"
            assert process.poll() is None  # No restart-policy re-admission.
            time.sleep(0.15)
            assert len(requests) == 1
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=10)
            assert process.returncode == 0, errors.decode()
            process = start_worker_process(pipeline)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = control.status("process-worker")
                if state["generation"] > generation and state["state"] == "drained":
                    break
                time.sleep(0.05)
            assert state["generation"] > generation and state["state"] == "drained"
            assert state["drain_requested_at"] is not None
            assert len(requests) == 1 and process.poll() is None
            with pytest.raises(ValueError, match="generation"):
                control.request_drain("process-worker", generation)
            control.resume("process-worker", state["generation"])
        if mode in {"remote", "crash"}:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if (
                    pipeline.catalog.one(
                        "SELECT count(*) AS count FROM collection_tasks WHERE state='done'"
                    )["count"]
                    == 3
                ):
                    break
                time.sleep(0.05)
            assert len(requests) == (4 if mode == "crash" else 3)
            assert control.status("process-worker")["generation"] > generation
            process.send_signal(signal.SIGTERM)
        output, errors = process.communicate(timeout=15)
        assert process.returncode == 0, errors.decode()
        assert control.status("process-worker")["state"] == "stopped"
        assert len(requests) == {"signal": 1, "remote": 3, "crash": 4}[mode]
        completed = 1 if mode == "signal" else 3
        assert (
            pipeline.catalog.one(
                "SELECT count(*) AS count FROM collection_tasks WHERE state='done'"
            )["count"]
            == completed
        )
        assert len(pipeline.datasets()) == completed
        assert pipeline.verify()["ok"]
    finally:
        release.set()
        if process is not None and process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
        source.shutdown()
        source.server_close()
        thread.join(timeout=2)

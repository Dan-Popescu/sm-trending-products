"""Run the actual CLI loop in a subprocess with deterministic collection outcomes."""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


def _run_fixture(mode: str) -> None:
    from sm_trending_products import cli, pipeline

    class FixturePipeline:
        @classmethod
        def from_env(cls):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def collect_batch(self, batch_size, synthetic=False):
            time.sleep(0.01)  # Bound the fake busy worker and keep output small.
            result = {
                "processed": batch_size,
                "errors": [],
                "synthetic": synthetic,
                "called_at": time.monotonic(),
            }
            if mode == "empty":
                result["processed"] = 0
            elif mode == "partial":
                result["processed"] = batch_size - 1
            elif mode == "error":
                result["errors"] = [{"category": "publication_failed"}]
            elif mode == "quota":
                result["quota_waiting"] = 1
            elif mode == "waiting":
                result["waiting"] = True
            return result

    pipeline.Pipeline = FixturePipeline
    raise SystemExit(
        cli.main(
            [
                "worker",
                "--loop",
                "--synthetic",
                "--batch-size",
                "2",
                "--poll-seconds",
                "60" if mode == "busy" else "0.2",
            ]
        )
    )


def _read_result(process):
    assert select.select([process.stdout], [], [], 8)[0], "CLI worker stopped making progress"
    return json.loads(process.stdout.readline())


@pytest.mark.parametrize("mode", ["busy", "empty", "partial", "error", "quota", "waiting"])
def test_cli_drains_busy_batches_but_waits_on_idle_or_backpressure_and_stops_on_signal(mode):
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join((str(root / "src"), str(root / "tests")))
    process = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            "import sys; from test_worker_queue_loop import _run_fixture; "
            "_run_fixture(sys.argv[1])",
            mode,
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    try:
        first, second = _read_result(process), _read_result(process)
        assert first["synthetic"] is second["synthetic"] is True
        if mode == "busy":
            # The old loop sleeps 60 seconds, so it cannot produce this second
            # result within the bounded reader deadline even on a busy CI host.
            assert first["processed"] == second["processed"] == 2
        else:
            assert second["called_at"] - first["called_at"] >= 0.2
        process.send_signal(signal.SIGTERM if mode == "busy" else signal.SIGINT)
        output, errors = process.communicate(timeout=5)
        assert process.returncode == 0, errors.decode()
        assert json.loads(output.splitlines()[-1]) == {"status": "stopped"}
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)

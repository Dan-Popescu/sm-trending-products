"""Opt-in native archive comparison; run each case in a fresh pytest process.

The baseline class is loaded from immutable git 0f2500f. Current Provenance's
ordinary records still produce identical bytes. Source/feature work remains
outside the measured archive phase; HTTP Moto CPU is included for both cases.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import os
import resource
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import pytest
from test_sharded_results import seed

from sm_trending_products.contracts import canonical
from sm_trending_products.pipeline import Pipeline


def rss_bytes():
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("Linux resident memory counters unavailable")


@contextmanager
def phase_memory():
    stats = {"rss_before_bytes": rss_bytes(), "rss_peak_bytes": rss_bytes()}
    stop = threading.Event()

    def sample():
        while not stop.wait(0.002):
            stats["rss_peak_bytes"] = max(stats["rss_peak_bytes"], rss_bytes())

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        yield stats
    finally:
        stats["rss_peak_bytes"] = max(stats["rss_peak_bytes"], rss_bytes())
        stop.set()
        sampler.join()
        stats["rss_increase_bytes"] = max(0, stats["rss_peak_bytes"] - stats["rss_before_bytes"])


def baseline_archive(tmp_path):
    encoded = subprocess.run(
        ["git", "show", "0f2500f:src/sm_trending_products/assessment_archive.py"],
        capture_output=True,
        check=True,
    ).stdout
    path = tmp_path / "baseline_archive.py"
    path.write_bytes(encoded)
    name = "sm_trending_products._frozen_archive_benchmark"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module.AssessmentArchive


@pytest.mark.parametrize(
    ("mode", "scenario"),
    [
        (mode, scenario)
        for mode in ("baseline", "adaptive")
        for scenario in ("clock", "sparse5", "all")
    ],
    ids=[
        f"{mode}-{scenario}"
        for mode in ("baseline", "adaptive")
        for scenario in ("clock", "sparse5", "all")
    ],
)
def test_archive_resources(pipeline, tmp_path, monkeypatch, mode, scenario):
    products = int(os.environ.get("ADAPTIVE_BENCHMARK_PRODUCTS", "1000"))
    cutoff, _records, _datasets = seed(pipeline, products=products, videos=2, points=4)
    if mode == "baseline":
        pipeline._assessment_archive = baseline_archive(tmp_path)(
            pipeline.catalog, pipeline.history
        )
    report = {
        "baseline_revision": "0f2500f",
        "candidate_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, check=True, text=True
        ).stdout.strip(),
        "mode": mode,
        "scenario": scenario,
        "products": products,
        "interval_minutes": 60,
        "scope": (
            "archive stage/commit; native PostgreSQL + local HTTP Moto; source preparation excluded"
        ),
        "runs": [],
    }
    original_stage = pipeline._assessment_archive.stage
    original_commit = pipeline._assessment_archive.commit

    def stage(body, scope):
        started = time.process_time()
        logical = canonical(body).encode()
        plain_cpu = time.process_time() - started
        started = time.process_time()
        compressed = gzip.compress(logical, compresslevel=6, mtime=0)
        gzip_cpu = plain_cpu + time.process_time() - started
        before = dict(pipeline.history.metrics)
        with phase_memory() as memory:
            started, cpu = time.perf_counter(), time.process_time()
            publication = original_stage(body, scope)
            elapsed, used_cpu = time.perf_counter() - started, time.process_time() - cpu
        report["runs"].append(
            {
                "logical_bytes": len(logical),
                "simple_gzip_bytes": len(compressed),
                "simple_gzip_cpu_seconds": gzip_cpu,
                "upload_bytes": pipeline.history.metrics["upload_bytes"] - before["upload_bytes"],
                "put_requests": pipeline.history.metrics["put_requests"] - before["put_requests"],
                "stage_cpu_seconds": used_cpu,
                "stage_elapsed_seconds": elapsed,
                **memory,
            }
        )
        return publication

    def commit(connection, publication):
        started, cpu = time.perf_counter(), time.process_time()
        result = original_commit(connection, publication)
        report["runs"][-1]["commit_cpu_seconds"] = time.process_time() - cpu
        report["runs"][-1]["commit_elapsed_seconds"] = time.perf_counter() - started
        return result

    monkeypatch.setattr(pipeline._assessment_archive, "stage", stage)
    monkeypatch.setattr(pipeline._assessment_archive, "commit", commit)
    latest = None
    for run in range(4):
        as_of = cutoff + timedelta(hours=run)
        if run and scenario != "clock":
            pipeline.import_records(
                [
                    {
                        "observation_id": f"refresh-{product}-{run}",
                        "source": "provenance_fixture",
                        "entity_type": "video",
                        "entity_id": f"p{product}-v0",
                        "observed_at": as_of.isoformat(),
                        "available_at": as_of.isoformat(),
                        "synthetic": True,
                        "metrics": {
                            "view_count": {
                                "lower": 1000 * run + product,
                                "upper": 1000 * run + product,
                            }
                        },
                        "attributes": {"creator_id": f"p{product}-v0"},
                    }
                    for product in range(0, products, 20 if scenario == "sparse5" else 1)
                ],
                synthetic=True,
            )
        latest = pipeline.assess(as_of, True)
        key = pipeline.catalog.one(
            "SELECT object_key FROM assessment_runs WHERE run_id=%s", (latest["run_id"],)
        )["object_key"]
        response = pipeline.history.client.get_object(Bucket=pipeline.history.bucket, Key=key)
        with response["Body"] as stream:
            report["runs"][-1]["representation"] = json.loads(gzip.decompress(stream.read()))[
                "representation"
            ]
    report["cold_pages"] = []
    assert latest is not None
    for limit in (1, 200):
        with Pipeline(
            pipeline.settings.model_copy(update={"cache_dir": tmp_path / f"cold-{limit}"})
        ) as reader:
            offset = max(0, products - min(products, limit) - 1)
            with phase_memory() as memory:
                started, cpu = time.perf_counter(), time.process_time()
                page = reader.assessment_run_page(
                    latest["run_id"], True, offset=offset, limit=limit
                )
                elapsed, used_cpu = time.perf_counter() - started, time.process_time() - cpu
            assert page["rows"] == latest["rows"][offset : offset + limit]
            report["cold_pages"].append(
                {
                    "limit": limit,
                    "get_requests": reader.history.metrics["get_requests"],
                    "download_bytes": reader.history.metrics["download_bytes"],
                    "cpu_seconds": used_cpu,
                    "elapsed_seconds": elapsed,
                    **memory,
                }
            )
    assert canonical(pipeline.assessment_snapshot(latest["run_id"], True)) == canonical(latest)
    report["exact_snapshot_verified"] = True
    report["process_peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    directory = Path(os.environ["ADAPTIVE_BENCHMARK_DIRECTORY"])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"adaptive-archive-{products}-{mode}-{scenario}.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )

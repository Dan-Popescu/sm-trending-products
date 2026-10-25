"""Opt-in archive-only costs with real schema-2 rows, PostgreSQL and HTTP Moto.

Run explicitly with pytest and ARCHIVE_BENCHMARK_DIRECTORY set. The normal test
suite does not collect this file. Provider network latency is not modeled.
Source collection, feature preparation, current-row SQL and alerts are excluded
from the archive timings, and source/provenance inputs are retained unchanged.
"""

from __future__ import annotations

import gzip
import json
import os
import time
from datetime import timedelta
from pathlib import Path

import pytest
from test_sharded_results import seed

from sm_trending_products.contracts import canonical
from sm_trending_products.pipeline import Pipeline


@pytest.mark.parametrize("products", [100, 1000])
def test_archive_costs(pipeline, tmp_path, monkeypatch, products):
    cutoff, _records, _datasets = seed(pipeline, products=products, videos=2, points=4)
    report = {
        "products": products,
        "runs": [],
        "scope": "archive only; native PostgreSQL + HTTP Moto, real schema-2 assessment rows",
        "excluded": [
            "source collection and source object storage",
            "feature preparation and input provenance publication",
            "current assessment SQL writes and alerts",
            "provider network latency and service charges",
        ],
    }
    original_stage = pipeline._assessment_archive.stage
    original_commit = pipeline._assessment_archive.commit
    latest = None

    def measured_stage(body, scope):
        cpu = time.process_time()
        encoded = canonical(body).encode()
        plain_cpu = time.process_time() - cpu
        cpu = time.process_time()
        simple_gzip = gzip.compress(encoded, compresslevel=6, mtime=0)
        gzip_cpu = plain_cpu + time.process_time() - cpu
        before = dict(pipeline.history.metrics)
        stored_before = pipeline.catalog.one(
            "SELECT coalesce(sum(object_bytes),0) AS bytes,count(*) AS objects "
            "FROM derived_publications"
        )
        started, cpu = time.perf_counter(), time.process_time()
        publication = original_stage(body, scope)
        stage_cpu, stage_elapsed = time.process_time() - cpu, time.perf_counter() - started
        stored_after = pipeline.catalog.one(
            "SELECT coalesce(sum(object_bytes),0) AS bytes,count(*) AS objects "
            "FROM derived_publications"
        )
        report["runs"].append(
            {
                "logical_bytes": len(encoded),
                "simple_gzip_bytes": len(simple_gzip),
                "plain_encode_cpu_seconds": plain_cpu,
                "simple_gzip_cpu_seconds": gzip_cpu,
                "shared_new_physical_bytes": int(stored_after["bytes"] - stored_before["bytes"]),
                "shared_new_objects": stored_after["objects"] - stored_before["objects"],
                "shared_put_requests": pipeline.history.metrics["put_requests"]
                - before["put_requests"],
                "shared_upload_bytes": pipeline.history.metrics["upload_bytes"]
                - before["upload_bytes"],
                "shared_stage_cpu_seconds": stage_cpu,
                "shared_stage_elapsed_seconds": stage_elapsed,
            }
        )
        return publication

    def measured_commit(connection, publication):
        started, cpu = time.perf_counter(), time.process_time()
        result = original_commit(connection, publication)
        report["runs"][-1]["shared_commit_cpu_seconds"] = time.process_time() - cpu
        report["runs"][-1]["shared_commit_elapsed_seconds"] = time.perf_counter() - started
        return result

    monkeypatch.setattr(pipeline._assessment_archive, "stage", measured_stage)
    monkeypatch.setattr(pipeline._assessment_archive, "commit", measured_commit)
    for index, kind in enumerate(
        ("initial", "clock_only", "clock_only", "sparse_5_percent", "clock_only")
    ):
        as_of = cutoff + timedelta(minutes=15 * index)
        if kind == "sparse_5_percent":
            pipeline.import_records(
                [
                    {
                        "observation_id": f"sparse-new-{product}",
                        "source": "provenance_fixture",
                        "entity_type": "video",
                        "entity_id": f"p{product}-v0",
                        "observed_at": as_of.isoformat(),
                        "available_at": as_of.isoformat(),
                        "synthetic": True,
                        "metrics": {
                            "view_count": {"lower": 1000 + product, "upper": 1000 + product}
                        },
                        "attributes": {"creator_id": f"p{product}-v0"},
                    }
                    for product in range(0, products, 20)
                ],
                synthetic=True,
            )
        latest = pipeline.assess(as_of, True)
        report["runs"][-1]["kind"] = kind
    assert latest is not None
    report["totals"] = {
        field: sum(row[field] for row in report["runs"])
        for field in (
            "logical_bytes",
            "simple_gzip_bytes",
            "shared_new_physical_bytes",
            "shared_new_objects",
            "shared_put_requests",
            "shared_upload_bytes",
            "shared_stage_cpu_seconds",
            "shared_commit_cpu_seconds",
        )
    }
    report["cold_reads"] = []
    for limit in (1, 200):
        settings = pipeline.settings.model_copy(update={"cache_dir": tmp_path / f"cold-{limit}"})
        with Pipeline(settings) as reader:
            offset = max(0, products - min(products, limit) - 1)
            started, cpu = time.perf_counter(), time.process_time()
            page = reader.assessment_run_page(latest["run_id"], True, offset=offset, limit=limit)
            elapsed, used_cpu = time.perf_counter() - started, time.process_time() - cpu
            assert page["rows"] == latest["rows"][offset : offset + limit]
            report["cold_reads"].append(
                {
                    "requested_products": limit,
                    "returned_products": len(page["rows"]),
                    "get_requests": reader.history.metrics["get_requests"],
                    "download_bytes": reader.history.metrics["download_bytes"],
                    "cpu_seconds": used_cpu,
                    "elapsed_seconds": elapsed,
                }
            )
    report["exact_snapshot_verified"] = canonical(
        pipeline.assessment_snapshot(latest["run_id"], True)
    ) == canonical(latest)
    assert report["exact_snapshot_verified"]
    directory = Path(os.environ["ARCHIVE_BENCHMARK_DIRECTORY"])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"assessment-archive-{products}.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )

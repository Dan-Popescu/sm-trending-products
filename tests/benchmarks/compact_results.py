"""Opt-in native compact publication cost probe; not part of default test collection."""

from __future__ import annotations

import hashlib
import json
import os
import resource
import subprocess
import time
from datetime import timedelta
from pathlib import Path

import pytest

from sm_trending_products.contracts import canonical, now

OUT = Path(os.environ.get("COMPACT_BENCHMARK_OUTPUT", "output/compact-results-100.json"))


@pytest.mark.integration
def test_cost_100_products_20_videos_120_samples(pipeline):
    pipeline.settings.analysis_product_batch_size = 10
    cutoff = now() + timedelta(hours=1)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    result = {
        "commit": revision,
        "scope": {
            "products": 100,
            "videos_per_product": 20,
            "observations_per_video": 120,
            "window_days": 30,
            "sample_hours": 6,
            "product_batch_size": 10,
            "collection_model": "one synthetic import per product, not physical API pages",
            "history_partition_buckets": pipeline.settings.history_partition_buckets,
        },
        "phases": {},
    }

    def refs(product, points=120):
        return sorted(
            hashlib.sha256(f"p{product}-v{video}-{point}".encode()).hexdigest()
            for video in range(20)
            for point in range(points)
        )

    def facts(product, points=range(120), wave=False):
        for video in range(20):
            for point in points:
                observed = (
                    cutoff + timedelta(hours=1)
                    if wave
                    else cutoff - timedelta(hours=6 * (119 - point))
                )
                yield {
                    "observation_id": hashlib.sha256(
                        f"p{product}-v{video}-{point}".encode()
                    ).hexdigest(),
                    "source": "cost_probe",
                    "entity_type": "video",
                    "entity_id": f"p{product}-v{video}",
                    "observed_at": observed.isoformat(),
                    "available_at": observed.isoformat(),
                    "synthetic": True,
                    "metrics": {"view_count": {"lower": point * 100, "upper": point * 100}},
                    "attributes": {"creator_id": f"creator-{product}-{video}"},
                }

    started = time.perf_counter()
    for product in range(100):
        pipeline.create_product(f"p{product}", f"Product {product}", synthetic=True)
        pipeline.import_records(list(facts(product)), synthetic=True)
        for video in range(20):
            pipeline.review_link(f"p{product}", f"cost_probe:video:p{product}-v{video}")
        if product % 20 == 19:
            print(f"imported {product + 1} products", flush=True)
    result["import_seconds"] = time.perf_counter() - started
    result["source_inventory"] = pipeline.catalog.one(
        "SELECT count(*) AS datasets FROM datasets WHERE state='committed'"
    )
    result["source_inventory"].update(
        pipeline.catalog.one(
            "SELECT count(*) AS objects,sum((value->>'bytes')::bigint) AS normalized_bytes "
            "FROM datasets CROSS JOIN LATERAL jsonb_array_elements(manifest->'normalized') value "
            "WHERE state='committed'"
        )
    )

    def inventory():
        return pipeline.catalog.one(
            "SELECT count(*) AS nodes,COALESCE(sum(object_bytes),0) AS bytes "
            "FROM derived_publications WHERE object_key LIKE 'provenance/%%'"
        ) | pipeline.catalog.one(
            "SELECT (SELECT count(*) FROM derived_artifact_inputs) AS input_edges,"
            "(SELECT count(*) FROM derived_artifact_edges) AS child_edges"
        )

    def phase(name, at):
        metrics = dict(pipeline.history.metrics)
        before = inventory()
        started = time.perf_counter()
        payload = pipeline.assess(at, True)
        elapsed = time.perf_counter() - started
        after = inventory()
        assert len(payload["rows"]) == 100
        counters = {key: pipeline.history.metrics[key] - value for key, value in metrics.items()}
        result["phases"][name] = {
            "seconds": elapsed,
            "history": counters,
            "provenance_total": after,
            "provenance_new": {key: after[key] - before[key] for key in after},
            "assessment_bytes": len(
                canonical(
                    {key: value for key, value in payload.items() if key != "run_id"}
                ).encode()
            ),
            "row_total_bytes": sum(len(canonical(row).encode()) for row in payload["rows"]),
            "max_row_bytes": max(len(canonical(row).encode()) for row in payload["rows"]),
            "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
        OUT.write_text(json.dumps(result, indent=2, default=str))
        print(name, json.dumps(result["phases"][name], default=str), flush=True)
        return payload

    initial = phase("initial", cutoff)

    def expand(item, ids):
        if isinstance(item, dict):
            if item.get("format") == "sharded-provenance-v1":
                assert item["count"] == len(ids)
                return ids
            return {key: expand(child, ids) for key, child in item.items()}
        if isinstance(item, list):
            return [expand(child, ids) for child in item]
        return item

    legacy_rows = []
    for row in initial["rows"]:
        product = int(row["product_id"][1:])
        legacy_rows.append(expand(row, refs(product)))
    legacy_body = {
        "schema_version": "1",
        "as_of": cutoff,
        "synthetic": True,
        "policy": initial["policy"],
        "rows": legacy_rows,
        "datasets": [
            row["dataset_id"]
            for row in pipeline.catalog.all(
                "SELECT dataset_id FROM datasets WHERE state='committed' ORDER BY dataset_id"
            )
        ],
    }
    result["legacy_equivalent_body_bytes"] = len(canonical(legacy_body).encode())
    del legacy_body, legacy_rows, initial
    phase("unchanged_future_cutoff", cutoff + timedelta(minutes=1))
    for product in range(100):
        pipeline.import_records(list(facts(product, range(120, 121), wave=True)), synthetic=True)
    phase("one_refresh_wave", cutoff + timedelta(hours=1))
    result["assessment_sql_bytes"] = pipeline.catalog.one(
        "SELECT COALESCE(sum(pg_column_size(payload)),0) AS current_rows_bytes "
        "FROM current_assessments"
    )["current_rows_bytes"]
    result["all_derived_inventory"] = pipeline.catalog.one(
        "SELECT count(*) AS objects,sum(object_bytes) AS bytes FROM derived_publications "
        "WHERE state='committed'"
    )
    OUT.write_text(json.dumps(result, indent=2, default=str))
    assert result["phases"]["initial"]["assessment_bytes"] < 700000
    assert result["phases"]["unchanged_future_cutoff"]["provenance_new"]["nodes"] == 0
    assert result["phases"]["one_refresh_wave"]["provenance_new"]["nodes"] <= 405

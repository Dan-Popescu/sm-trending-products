"""Characterize unchanged source assessments across actual clock boundaries."""

import importlib.util
import json
import time
from datetime import timedelta
from pathlib import Path

import pytest

from sm_trending_products.contracts import now

SPEC = importlib.util.spec_from_file_location(
    "retained_clock", Path(__file__).resolve().parents[1] / "scripts/retained-history.py"
)
assert SPEC and SPEC.loader
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


@pytest.mark.integration
def test_retained_clock_boundary_rebuild_is_distinct_from_same_cutoff_warm_hit(pipeline, tmp_path):
    cutoff = now() + timedelta(hours=4)
    for product in range(3):
        pipeline.create_product(f"p{product}", f"Product {product}", synthetic=True)
        pipeline.import_records(
            [
                RUNNER.fact(product, video, point, cutoff, 120, 6)
                for point in range(120)
                for video in range(2)
            ],
            synthetic=True,
        )
        pipeline.identity.reassign_entities(
            [f"retained_fixture:video:p{product}-v{video}" for video in range(2)],
            f"p{product}",
            method="reviewed-clock-probe-v1",
        )
    measurements = {}

    def assess(label, minutes, expected_observations):
        before = dict(pipeline.history.metrics)
        work_before = dict(pipeline.history.analysis_metrics)
        started = time.perf_counter()
        payload = pipeline.assess(cutoff + timedelta(minutes=minutes), True)
        assert {row["product_id"] for row in payload["rows"]} == {"p0", "p1", "p2"}
        assert (
            sum(row["coverage"]["observations"] for row in payload["rows"]) == expected_observations
        )
        measurements[label] = {
            "wall_seconds": time.perf_counter() - started,
            "metrics": {
                key: pipeline.history.metrics[key] - value for key, value in before.items()
            },
            "work": {
                key: pipeline.history.analysis_metrics[key] - value
                for key, value in work_before.items()
            },
            "observations": expected_observations,
        }
        return measurements[label]["work"]

    assert assess("cold", 0, 720)["full_windows"] == 3
    assert assess("same_cutoff", 0, 720)["projection_hits"] == 3
    assert assess("initial_plus_15min", 15, 720)["delta_windows"] == 3
    assert assess("initial_plus_30min", 30, 720)["projection_hits"] == 3
    for product in range(3):
        pipeline.import_records(
            [RUNNER.fact(product, video, 120, cutoff, 120, 6) for video in range(2)],
            synthetic=True,
        )
    assert assess("new_wave_6h", 360, 726)["delta_windows"] == 3
    assert assess("new_wave_plus_15min", 375, 720)["delta_windows"] == 3
    assert assess("new_wave_plus_30min", 390, 720)["projection_hits"] == 3
    for label in ["initial_plus_15min", "new_wave_plus_15min"]:
        assert measurements[label]["metrics"]["get_requests"] == 0
    output = tmp_path / "clock-boundary-observed.json"
    output.write_text(json.dumps(measurements, sort_keys=True, indent=2) + "\n")

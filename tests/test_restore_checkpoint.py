"""The isolated restore probe exercises the actual fenced publication protocol."""

import runpy
from pathlib import Path

import pytest
from test_evaluation_integration import seed_product

pytestmark = pytest.mark.integration
fixture = runpy.run_path(str(Path(__file__).parents[1] / "scripts/recovery_fixture.py"))


def test_recovery_probe_preserves_existing_evidence_and_restarts_without_duplicates(pipeline):
    seed_product(pipeline, "before-restore")
    pipeline.assess(synthetic=True)
    original = pipeline.latest_assessments(True)
    staged = fixture["seed"](pipeline)
    result = fixture["resume"](pipeline.settings)
    assert result["dataset_id"] == staged["dataset_id"]
    assert result["source_requests"] == 0 and result["restart_duplicate_observations"] == 0
    assert pipeline.latest_assessments(True) == original


def test_synthetic_recovery_probe_refuses_live_state_before_claiming(pipeline):
    staged = fixture["seed"](pipeline)
    pipeline.create_product("live-product", "Live state", synthetic=False)
    before = pipeline.catalog.export_state()
    with pytest.raises(ValueError, match="refuses live state"):
        fixture["resume"](pipeline.settings)
    assert pipeline.catalog.export_state() == before
    assert (
        pipeline.catalog.one(
            "SELECT state FROM datasets WHERE dataset_id=%s", (staged["dataset_id"],)
        )["state"]
        == "staged"
    )

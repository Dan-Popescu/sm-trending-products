"""Hourly opt-in contracts preserve legacy frozen-decision identities."""

from datetime import timedelta

import pytest
from analytical_results import backtest_reports
from test_evaluation import catalog, observation
from test_evaluation_integration import record, seed_product

from sm_trending_products.contracts import now, utc
from sm_trending_products.evaluation import (
    EvaluationConfig,
    backtest,
    freeze_decision,
    mature_decision,
)
from sm_trending_products.evaluation_service import EvaluationService


@pytest.mark.parametrize(
    "hours", [[], [-1], [12, 12], [True], ["12"], [12.5], [8761], list(range(25))]
)
def test_hour_delays_require_bounded_unique_integer_values(hours):
    with pytest.raises(ValueError):
        EvaluationConfig(launch_delay_hours=hours)


def test_delay_units_are_explicit_and_roundtrip_without_legacy_payload_changes():
    legacy = EvaluationConfig(launch_delay_days=(0, 2)).model_dump(mode="json")
    assert "launch_delay_hours" not in legacy
    hourly = EvaluationConfig(launch_delay_hours=(12, 24, 48)).model_dump(mode="json")
    assert "launch_delay_days" not in hourly
    assert hourly["launch_delay_hours"] == [12, 24, 48]
    assert EvaluationConfig.model_validate(hourly).model_dump(mode="json") == hourly
    with pytest.raises(ValueError, match="not both"):
        EvaluationConfig(launch_delay_days=(2, 10), launch_delay_hours=(12,))


def test_legacy_snapshot_and_maturation_hashes_are_unchanged():
    product, link = catalog()
    rows = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation("v1", "2026-01-05T00:00:00Z", 4000),
    ]
    config = EvaluationConfig(
        top_k=1, horizon_days=(3,), launch_delay_days=(0, 2), max_boundary_gap_hours=48
    )
    snapshot = freeze_decision(
        rows, [product], [link], utc("2026-01-02T00:00:00Z"), config, synthetic=True
    )
    report = mature_decision(snapshot, rows, utc("2026-01-05T00:00:00Z"))
    assert snapshot["snapshot_id"] == (
        "b60f891858a747df2143dfdc2adb2984ed9064651428b878c29508d013cb9e16"
    )
    assert report["report_id"] == (
        "abaed4614bd375df7cf89eabeb123ab4fbafdf930d805123659e42a5d6c3e4d3"
    )


def test_hour_scenarios_measure_from_exact_launch_time_and_keep_horizon_anchor():
    product, link = catalog()
    decision = utc("2026-01-02T00:00:00Z")
    rows = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation("v1", "2026-01-02T12:00:00Z", 1000),
        observation("v1", "2026-01-03T00:00:00Z", 3000),
    ]
    config = EvaluationConfig(
        top_k=1, horizon_days=(1,), launch_delay_hours=(12, 24, 48), threshold=1500
    )
    snapshot = freeze_decision(rows, [product], [link], decision, config, synthetic=True)
    report = mature_decision(snapshot, rows, decision + timedelta(days=1))
    measured, at_horizon, after_horizon = report["scenarios"]
    assert measured["launch_delay_hours"] == 12
    assert measured["window_start"] == (decision + timedelta(hours=12)).isoformat()
    assert measured["methods"]["policy"]["outcomes"][0]["lower"] == 2000
    assert at_horizon["status"] == after_horizon["status"] == "not_actionable"
    assert all("launch_delay_days" not in scenario for scenario in report["scenarios"])
    summary = backtest(
        rows, [product], [link], [decision], decision + timedelta(days=1), config, synthetic=True
    )
    assert all("launch_delay_hours" in row for row in summary["comparisons"])


@pytest.mark.integration
def test_native_hour_delay_persistence_read_scope_and_frontier(pipeline, monkeypatch):
    entity, _, _ = seed_product(pipeline, "hourly")
    decision = now() + timedelta(minutes=1)
    pipeline.import_records(
        [
            record(entity, decision, 300),
            record(entity, decision + timedelta(hours=12), 1000),
            record(entity, decision + timedelta(days=1), 3000),
        ],
        synthetic=True,
    )
    config = {"top_k": 1, "horizon_days": [1], "launch_delay_hours": [12, 24, 48]}
    frozen = pipeline.freeze(decision, True, config)
    assert frozen["result"]["selected"] == ["hourly"]
    assert "launch_delay_days" not in frozen["result"]["config"]
    assert EvaluationService._initial_due(frozen["result"]) == decision + timedelta(days=1)
    frontier = pipeline.catalog.one(
        "SELECT next_due_at FROM forward_frontier WHERE evaluation_id=%s",
        (frozen["evaluation_id"],),
    )
    assert frontier["next_due_at"] == decision + timedelta(days=1)
    original = pipeline.history.observations
    reads = []

    def capture(*args, **kwargs):
        reads.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline.history, "observations", capture)
    matured = pipeline.mature(frozen["evaluation_id"], decision + timedelta(days=1))
    scenarios = matured["result"]["scenarios"]
    assert scenarios[0]["methods"]["policy"]["outcomes"][0]["lower"] == 2000
    assert scenarios[1]["status"] == scenarios[2]["status"] == "not_actionable"
    assert len(reads) == 1
    assert reads[0]["since"] == decision + timedelta(hours=12 - 24)
    repeated = pipeline.mature(frozen["evaluation_id"], decision + timedelta(days=1))
    assert repeated["evaluation_id"] == matured["evaluation_id"]
    replay = pipeline.backtest([decision], decision + timedelta(days=1), True, config)["result"]
    scenario = backtest_reports(pipeline, replay)[0]["scenarios"][0]
    assert scenario["launch_delay_hours"] == 12
    assert scenario["methods"]["policy"]["outcomes"][0]["lower"] == 2000

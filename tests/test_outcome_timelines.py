"""Outcome parity and operation bounds for prepared temporal evidence."""

from datetime import timedelta
from decimal import Decimal

import pytest
from test_evaluation import catalog, observation

from sm_trending_products.contracts import Measurement, ProductLink
from sm_trending_products.evaluation import EvaluationConfig, freeze_decision, mature_decision
from sm_trending_products.outcome_timeline import EntityTimeline


@pytest.mark.parametrize("kind", ["cumulative", "interval", "profit"])
def test_long_timeline_is_prepared_once_and_reports_the_first_confirmed_crossing(monkeypatch, kind):
    import sm_trending_products.outcome_timeline as timelines

    product, link = catalog()
    before = [
        observation("v1", "2026-01-01T00:00:00Z", 10),
        observation("v1", "2026-01-02T00:00:00Z", 100),
    ]
    cutoff = before[-1].observed_at
    count = 240
    step = timedelta(days=3) / count
    links = [link]
    evidence = list(before)
    for index in range(1, count + 1):
        end = cutoff + step * index
        changes = {}
        if kind == "interval":
            changes = {
                "metrics": {"view_count": Measurement(lower=1, upper=1, semantics="interval")},
                "attributes": {
                    "period_start": (end - step).isoformat(),
                    "period_end": end.isoformat(),
                },
            }
        elif kind == "profit":
            changes = {
                "entity_type": "cost",
                "metrics": {},
                "attributes": {
                    "period_start": (end - step).isoformat(),
                    "period_end": end.isoformat(),
                    "currency": "EUR",
                    "realized": True,
                    "costs_complete": True,
                    "realized_revenue": "2.00",
                    "realized_costs": {"goods": "1.00"},
                },
            }
        evidence.append(
            observation(
                "cost" if kind == "profit" else "v1", end.isoformat(), 100 + index, **changes
            )
        )
    if kind == "profit":
        links.append(
            ProductLink(
                product_id="p1", entity_key="fixture:cost:cost", available_at=before[0].available_at
            )
        )
    config = EvaluationConfig(
        target="profit" if kind == "profit" else "attention",
        currency="EUR" if kind == "profit" else None,
        threshold=count,
        horizon_days=(3,),
        launch_delay_days=(0,),
    )
    snapshot = freeze_decision(before, [product], links, cutoff, config, synthetic=True)
    calls = 0
    metric = timelines._metric

    def counted(row, target):
        nonlocal calls
        calls += 1
        return metric(row, target)

    monkeypatch.setattr(timelines, "_metric", counted)
    report = mature_decision(snapshot, evidence, cutoff + timedelta(days=3))
    result = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert result["status"] == "success"
    assert Decimal(str(result["lower"])) == count
    assert result["lead_time_hours"] == 72
    # Counting normalization calls catches a rescan at every candidate boundary
    # without asserting wall time on a shared runner.
    assert calls <= len(evidence) + 1


def test_interval_prefix_preserves_arbitrary_precision_counts_and_gap_diagnostics():
    base = observation("v1", "2026-01-01T00:00:00Z", 0).observed_at
    huge = 10**40 + 1
    rows = [
        observation(
            "v1",
            (base + timedelta(hours=index + 1)).isoformat(),
            0,
            metrics={"view_count": Measurement(lower=huge, upper=huge, semantics="interval")},
            attributes={
                "period_start": (base + timedelta(hours=index)).isoformat(),
                "period_end": (base + timedelta(hours=index + 1)).isoformat(),
            },
        )
        for index in range(2)
    ]
    timeline = EntityTimeline(rows, "attention")
    assert (
        timeline.outcome(base, base + timedelta(hours=2), timedelta(hours=1))["lower"] == 2 * huge
    )
    assert (
        timeline.outcome(base, base + timedelta(hours=3), timedelta(hours=1))["reason"]
        == "incomplete_interval_coverage"
    )
    gap = EntityTimeline(rows[1:], "attention")
    assert (
        gap.outcome(base, base + timedelta(hours=2), timedelta(hours=1))["reason"]
        == "interval_gap_or_overlap"
    )

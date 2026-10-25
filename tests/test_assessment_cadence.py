"""Same synthetic captures, different scoring times; no scheduler or alert writes."""

from datetime import UTC, datetime

import pytest

from sm_trending_products.analytics import assess
from sm_trending_products.contracts import (
    AssessmentPolicy,
    Measurement,
    Observation,
    Product,
    ProductLink,
)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 25, hour, minute, tzinfo=UTC)


@pytest.fixture
def scenario():
    products = [
        Product(product_id=name, name=name, created_at=at(7), synthetic=True)
        for name in ("A", "B", "C")
    ]
    links = [
        ProductLink(product_id=name, entity_key=f"cadence_fixture:video:{name}", available_at=at(7))
        for name in ("A", "B", "C")
    ]
    captures = [
        ("A", at(8), 100),
        ("A", at(9), 200),
        ("B", at(8), 100),
        ("B", at(9), 300),
        ("C", at(8), 100),
        ("C", at(9), 400),
        ("A", at(10, 8), 1200),
        ("B", at(10, 37), 3000),
    ]
    observations = tuple(
        Observation(
            observation_id=f"{name}-{timestamp.isoformat()}",
            source="cadence_fixture",
            entity_type="video",
            entity_id=name,
            observed_at=timestamp,
            available_at=timestamp,
            synthetic=True,
            metrics={"view_count": Measurement(lower=views, upper=views)},
        )
        for name, timestamp, views in captures
    )
    return observations, products, links, AssessmentPolicy()


def rows_at(scenario, cutoff):
    observations, products, links, policy = scenario
    return assess(observations, products, links, cutoff, synthetic=True, policy=policy)


@pytest.mark.parametrize(
    ("cutoff", "expected_score", "expected_rank"),
    [
        (at(10), 35.897436, 3),
        (at(10, 15), 85.986928, 1),
        (at(10, 30), 85.986928, 1),
        (at(10, 45), 63.764706, 2),
        (at(11), 63.764706, 2),
    ],
)
def test_documented_scores_use_the_real_assessor(scenario, cutoff, expected_score, expected_rank):
    rows = rows_at(scenario, cutoff)
    rank, product = next(
        (index, row) for index, row in enumerate(rows, 1) if row["product_id"] == "A"
    )
    assert product["score"] == pytest.approx(expected_score, abs=0.000001, rel=0)
    assert rank == expected_rank


def test_hourly_scoring_misses_a_transient_threshold_crossing_with_identical_captures(scenario):
    policy = scenario[-1]
    assert policy.alert_threshold == 70
    quarter_hour = (at(10), at(10, 15), at(10, 30), at(10, 45), at(11))
    hourly = (at(10), at(11))

    def eligible_ticks(ticks):
        return [
            cutoff
            for cutoff in ticks
            if next(row for row in rows_at(scenario, cutoff) if row["product_id"] == "A")["score"]
            >= policy.alert_threshold
        ]

    # These are threshold opportunities, not persisted alerts or a cooldown simulation.
    assert eligible_ticks(quarter_hour) == [at(10, 15), at(10, 30)]
    assert eligible_ticks(hourly) == []
    assert (
        next(row for row in rows_at(scenario, at(11)) if row["product_id"] == "A")["features"][
            "view_count"
        ]
        == 1200
    )


def test_later_captures_do_not_leak_into_a_historical_scoring_time(scenario):
    observations, products, links, policy = scenario
    cutoff = at(10, 15)
    known_then = tuple(row for row in observations if row.available_at <= cutoff)
    assert len(observations) == 8 and len(known_then) == 7
    assert rows_at(scenario, cutoff) == assess(
        known_then, products, links, cutoff, synthetic=True, policy=policy
    )

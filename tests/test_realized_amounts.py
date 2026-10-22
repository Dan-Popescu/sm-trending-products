"""Frozen cost-component completeness and prepared/direct monetary parity."""

from datetime import timedelta
from decimal import Decimal

import pytest

from sm_trending_products.contracts import Observation, utc
from sm_trending_products.outcome_timeline import EntityTimeline, _realized_profit

START = utc("2026-01-01T00:00:00Z")
END = START + timedelta(days=1)
REQUIRED = ("goods", "fees")


def ledger(identifier="original", *, start=START, end=END, **attributes):
    return Observation.model_validate(
        {
            "observation_id": identifier,
            "source": "cost_import",
            "entity_type": "cost",
            "entity_id": "ledger",
            "observed_at": end,
            "available_at": end,
            "synthetic": True,
            "attributes": {
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "currency": "EUR",
                "realized": True,
                "costs_complete": True,
                "realized_revenue": "100",
                "realized_costs": {"goods": "20", "fees": "0"},
                **attributes,
            },
        }
    )


def outcomes(rows, *, required=REQUIRED, start=START, end=END):
    direct = _realized_profit(rows, start, end, "EUR", required_cost_components=required)
    timeline = EntityTimeline(rows, "profit", "EUR", required_cost_components=required)
    prepared = timeline.outcome(start, end, timedelta(hours=1))
    assert prepared == direct
    without_refs = timeline.outcome(start, end, timedelta(hours=1), include_references=False)
    assert without_refs == {key: value for key, value in direct.items() if key != "evidence_refs"}
    return direct


@pytest.mark.parametrize(
    ("costs", "strict_profit", "legacy_profit"),
    [
        ({"goods": "20", "fees": "0"}, "80", "80"),
        ({"goods": "0", "fees": 0}, "100", "100"),
        ({"goods": "20", "fees": "0", "shipping": "5"}, "75", "75"),
        ({}, None, "100"),
        ("20", None, "80"),
        ({"goods": "20"}, None, "80"),
        ({"goods": "-10", "fees": "20"}, None, "90"),
        ({"goods": "20", "fees": "-1"}, None, "81"),
        ({"goods": "20", "fees": "0", "extra": "-1"}, None, "81"),
        ({"goods": "-20", "fees": "0"}, None, None),
        ({"goods": "20", "fees": "NaN"}, None, None),
        ({"goods": "20", "fees": "Infinity"}, None, None),
        ({"goods": "20", "fees": "-Infinity"}, None, None),
        ({"goods": "20", "fees": None}, None, None),
        ({"goods": "20", "fees": True}, None, None),
        ({"goods": "20", "fees": []}, None, None),
        ({"goods": "20", "fees": "unknown"}, None, None),
        ({"goods": "20", "fees": "0", "extra": "NaN"}, None, None),
        (None, None, None),
        ([], None, None),
    ],
)
def test_each_declared_and_additional_component_is_validated_without_rewriting_legacy_contracts(
    costs, strict_profit, legacy_profit
):
    row = ledger(realized_costs=costs)
    for required, expected in ((REQUIRED, strict_profit), (None, legacy_profit)):
        result = outcomes([row], required=required)
        if expected is None:
            assert result == {
                "status": "censored",
                "reason": "incomplete_realized_costs_or_revenue",
            }
        else:
            assert result == {
                "status": "measured",
                "lower": expected,
                "upper": expected,
                "precision": "exact",
                "evidence_refs": [row.observation_id],
                "unit": "EUR",
            }


@pytest.mark.parametrize(
    "attributes",
    [
        {"currency": "USD"},
        {"realized": False},
        {"costs_complete": False},
        {"realized_revenue": "NaN"},
        {"realized_revenue": "Infinity"},
        {"realized_revenue": None},
        {"realized_revenue": True},
    ],
)
def test_business_flags_currency_and_nonfinite_revenue_share_the_same_validator(attributes):
    assert outcomes([ledger(**attributes)])["reason"] == "incomplete_realized_costs_or_revenue"


@pytest.mark.parametrize(
    "attributes",
    [
        {"realized_costs": {"goods": "1e1000000", "fees": "0"}},
        {"realized_revenue": "1e1000000"},
    ],
)
def test_strict_decimal_arithmetic_overflow_censors_instead_of_aborting_maturation(attributes):
    assert outcomes([ledger(**attributes)])["reason"] == "incomplete_realized_costs_or_revenue"


def test_strict_period_aggregate_overflow_is_censored_in_prepared_and_direct_paths():
    rows = [
        ledger(
            str(index),
            start=START + timedelta(days=index),
            end=END + timedelta(days=index),
            realized_costs={"goods": "9e999999", "fees": "0"},
        )
        for index in range(2)
    ]
    assert (
        outcomes(rows, end=END + timedelta(days=1))["reason"]
        == "incomplete_realized_costs_or_revenue"
    )


@pytest.mark.parametrize("invalid_costs", [{}, "20", {"goods": "20"}, {"goods": "-1", "fees": "2"}])
def test_latest_invalid_revision_censors_the_period_instead_of_reviving_older_complete_costs(
    invalid_costs,
):
    original = ledger()
    revision = ledger("later-revision", realized_costs=invalid_costs).model_copy(
        update={"available_at": END + timedelta(hours=1)}
    )
    assert outcomes([original])["lower"] == "80"
    assert outcomes([original, revision])["reason"] == "incomplete_realized_costs_or_revenue"
    assert outcomes([revision, original])["reason"] == "incomplete_realized_costs_or_revenue"


def test_strict_period_coverage_gap_overlap_and_partial_horizon_keep_direct_prepared_parity():
    first = ledger()
    second = ledger("second", start=END, end=END + timedelta(days=1), realized_revenue="200")
    result = outcomes([second, first], end=END + timedelta(days=1))
    assert result["lower"] == "260"
    assert result["evidence_refs"] == [first.observation_id, second.observation_id]
    assert (
        outcomes([first], end=END + timedelta(days=1))["reason"]
        == "incomplete_realized_profit_coverage"
    )
    assert (
        outcomes([second], end=END + timedelta(days=1))["reason"]
        == "realized_profit_period_gap_or_overlap"
    )
    overlap = ledger("overlap", start=START + timedelta(hours=1), end=END + timedelta(days=1))
    assert (
        outcomes([first, overlap], end=END + timedelta(days=1))["reason"]
        == "realized_profit_period_gap_or_overlap"
    )


def test_strict_valid_timeline_parses_each_period_once_across_all_horizon_candidates(monkeypatch):
    import sm_trending_products.outcome_timeline as timelines

    rows = [
        ledger(
            str(index), start=START + timedelta(hours=index), end=START + timedelta(hours=index + 1)
        )
        for index in range(240)
    ]
    parsed = 0
    validate = timelines._realized_amount

    def counted(*args, **kwargs):
        nonlocal parsed
        parsed += 1
        return validate(*args, **kwargs)

    monkeypatch.setattr(timelines, "_realized_amount", counted)
    timeline = EntityTimeline(rows, "profit", "EUR", required_cost_components=REQUIRED)
    for index in range(1, 241):
        outcome = timeline.outcome(START, START + timedelta(hours=index), timedelta(hours=1))
        assert Decimal(outcome["lower"]) == 80 * index
    assert parsed == len(rows)

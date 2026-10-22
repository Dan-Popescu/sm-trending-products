"""A later incomplete ledger must not revive superseded complete profit."""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_evaluation_integration import outcome_config, seed_product

from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "correction",
    [
        {"costs_complete": False},
        {"realized": False},
        {"currency": "USD"},
        {"realized_revenue": None},
        {"realized_costs": {"goods": "unknown"}},
    ],
)
def test_later_incomplete_realized_ledger_censors_without_rewriting_old_reports(
    pipeline, correction
):
    decision = now() + timedelta(minutes=1)
    end = decision + timedelta(days=3)
    seed_product(pipeline, "ledger-product", base=decision)

    def ledger(identifier, observed, available, attributes):
        return {
            "observation_id": identifier,
            "source": "cost_import",
            "entity_type": "cost",
            "entity_id": "ledger",
            "observed_at": observed.isoformat(),
            "available_at": available.isoformat(),
            "synthetic": True,
            "attributes": attributes,
        }

    pipeline.import_records(
        [ledger("quote", decision - timedelta(hours=1), decision - timedelta(hours=1), {})],
        synthetic=True,
    )
    pipeline.review_link("ledger-product", "cost_import:cost:ledger")
    frozen = pipeline.freeze(
        decision,
        True,
        outcome_config(
            target="profit",
            currency="EUR",
            threshold=30,
            profit_contract={"ledgers": {"cost_import:cost:ledger": ["goods", "ad_spend", "fees"]}},
        ),
    )
    assert frozen["result"]["selected"] == ["ledger-product"]
    complete = {
        "period_start": decision.isoformat(),
        "period_end": end.isoformat(),
        "currency": "EUR",
        "realized": True,
        "costs_complete": True,
        "realized_revenue": "120.00",
        "realized_costs": {"goods": "50.00", "ad_spend": "20.00", "fees": "10.00"},
    }
    first_cutoff = end + timedelta(minutes=1)
    pipeline.import_records([ledger("actual", end, first_cutoff, complete)], synthetic=True)
    original = pipeline.mature(frozen["evaluation_id"], first_cutoff)
    measured = original["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert measured["status"] == "success" and measured["lower"] == "40.00"

    later_cutoff = end + timedelta(hours=1)
    pipeline.import_records(
        [ledger("correction", end, later_cutoff, {**complete, **correction})], synthetic=True
    )
    revised = pipeline.mature(frozen["evaluation_id"], later_cutoff)
    outcome = revised["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "censored"
    assert "lower" not in outcome
    assert pipeline.mature(frozen["evaluation_id"], first_cutoff) == original
    stored = pipeline.evaluation(original["evaluation_id"], True)
    assert stored["payload"] == original["result"]
    replayed = pipeline.backtest([decision], later_cutoff, True, frozen["result"]["config"])[
        "result"
    ]
    assert replayed["snapshots"] == [frozen["result"]]
    assert replayed["reports"] == [revised["result"]]

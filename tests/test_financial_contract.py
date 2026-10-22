"""Frozen financial definitions and transaction-baseline suitability."""

from copy import deepcopy

import pytest
from test_evaluation import catalog, observation

from sm_trending_products.contracts import Measurement, ProductLink, canonical, digest, utc
from sm_trending_products.evaluation import (
    EvaluationConfig,
    ProfitContract,
    backtest,
    freeze_decision,
    mature_decision,
)

DECISION = utc("2026-01-02T00:00:00Z")
LATER = utc("2026-01-06T00:00:00Z")
LEDGER = "fixture:cost:ledger"


def financial_case():
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation(
            "price-only",
            "2026-01-02T00:00:00Z",
            0,
            entity_type="offer",
            metrics={},
            attributes={"price": "100", "currency": "EUR"},
        ),
    ]
    links = [
        link,
        ProductLink(product_id="p1", entity_key=LEDGER, available_at=link.available_at),
        ProductLink(
            product_id="p1", entity_key="fixture:offer:price-only", available_at=link.available_at
        ),
    ]
    realized = observation(
        "ledger",
        LATER.isoformat(),
        0,
        entity_type="cost",
        metrics={},
        attributes={
            "period_start": DECISION.isoformat(),
            "period_end": "2026-01-05T00:00:00Z",
            "currency": "EUR",
            "realized": True,
            "costs_complete": True,
            "realized_revenue": "100.10",
            "realized_costs": {"goods": "40.10", "fees": "20"},
        },
    )
    config = EvaluationConfig(
        target="profit",
        currency="EUR",
        threshold=30,
        horizon_days=(3,),
        launch_delay_days=(0,),
        profit_contract=ProfitContract(ledgers={LEDGER: ("goods", "fees")}),
    )
    return product, links, evidence, realized, config


def test_new_profit_decisions_require_a_contract_even_without_decision_dates():
    config = EvaluationConfig(target="profit", currency="EUR")
    # Parsing old configuration remains supported; creating a new decision does not.
    assert config.profit_contract is None
    with pytest.raises(ValueError, match="declared profit_contract"):
        freeze_decision([], [], [], DECISION, config, synthetic=True)
    with pytest.raises(ValueError, match="declared profit_contract"):
        backtest([], [], [], [], LATER, config, synthetic=True)


@pytest.mark.parametrize(
    "ledgers",
    [
        {},
        {"fixture:video:v1": ("goods",)},
        {LEDGER: ()},
        {LEDGER: ("goods", "goods")},
        {LEDGER: (" ",)},
    ],
)
def test_financial_scope_requires_explicit_ledger_and_component_names(ledgers):
    with pytest.raises(ValueError):
        ProfitContract(ledgers=ledgers)


@pytest.mark.parametrize("state", ["missing", "future", "rejected", "other_workspace"])
def test_declared_ledgers_must_be_reviewed_in_the_decision_scope(state):
    product, links, evidence, _realized, config = financial_case()
    if state == "missing":
        links = [link for link in links if link.entity_key != LEDGER]
    elif state == "other_workspace":
        product = product.model_copy(update={"synthetic": False})
    else:
        changes = {"available_at": LATER} if state == "future" else {"status": "rejected"}
        links = [
            link.model_copy(update=changes) if link.entity_key == LEDGER else link for link in links
        ]
    with pytest.raises(ValueError, match="reviewed links in the decision scope"):
        freeze_decision(evidence, [product], links, DECISION, config, synthetic=True)


def test_known_ledger_scope_does_not_require_a_recent_quote_or_future_actuals():
    product, links, evidence, _realized, config = financial_case()
    old = utc("2025-10-01T00:00:00Z")
    product = product.model_copy(update={"created_at": old})
    links = [
        link.model_copy(update={"available_at": old}) if link.entity_key == LEDGER else link
        for link in links
    ]
    quote = observation(
        "ledger", old.isoformat(), 0, entity_type="cost", metrics={}, published_at=None
    )
    snapshot = freeze_decision(
        [quote, *evidence], [product], links, DECISION, config, synthetic=True
    )
    assert snapshot["selected"] == ["p1"]
    assert quote.observation_id not in snapshot["evidence_refs"]
    assert snapshot["config"]["profit_contract"]["ledgers"] == {LEDGER: ["goods", "fees"]}
    outcome = mature_decision(snapshot, evidence, LATER)["scenarios"][0]["methods"]["policy"][
        "outcomes"
    ][0]
    assert outcome["status"] == "censored"


def test_declared_profit_ignores_price_offers_but_retains_missing_required_ledgers():
    product, links, evidence, realized, config = financial_case()
    snapshot = freeze_decision(evidence, [product], links, DECISION, config, synthetic=True)
    outcome = mature_decision(snapshot, [*evidence, realized], LATER)["scenarios"][0]["methods"][
        "policy"
    ]["outcomes"][0]
    assert outcome["status"] == "success" and outcome["lower"] == "40.00"
    assert [row["entity_key"] for row in outcome["entities"]] == [LEDGER]

    second = "fixture:cost:another-ledger"
    links.append(
        ProductLink(product_id="p1", entity_key=second, available_at=links[0].available_at)
    )
    config = config.model_copy(
        update={
            "profit_contract": ProfitContract(
                ledgers={LEDGER: ("goods", "fees"), second: ("goods",)}
            )
        }
    )
    snapshot = freeze_decision(evidence, [product], links, DECISION, config, synthetic=True)
    outcome = mature_decision(snapshot, [*evidence, realized], LATER)["scenarios"][0]["methods"][
        "policy"
    ]["outcomes"][0]
    assert outcome["status"] == "censored"
    assert {row["entity_key"] for row in outcome["entities"]} == {LEDGER, second}


def test_legacy_snapshot_retains_its_unscoped_definition_and_original_json():
    product, links, evidence, realized, config = financial_case()
    snapshot = freeze_decision(evidence, [product], links, DECISION, config, synthetic=True)
    legacy = deepcopy(snapshot)
    del legacy["config"]["profit_contract"]
    del legacy["baseline_eligibility"]
    del legacy["snapshot_id"]
    legacy["snapshot_id"] = digest(legacy)
    original = canonical(legacy)
    report = mature_decision(legacy, [*evidence, realized], LATER)
    assert report["config"] == legacy["config"] and "profit_contract" not in report["config"]
    assert report["snapshot_id"] == legacy["snapshot_id"]
    assert canonical(legacy) == original
    # Old frozen_links includes the ordinary offer; new contracts select only the ledger.
    result = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert result["status"] == "censored"
    assert {row["entity_key"] for row in result["entities"]} == {LEDGER, "fixture:offer:price-only"}


@pytest.mark.parametrize(
    ("first_period", "second_period", "expected_reason"),
    [
        ("lifetime", "lifetime", None),
        ("2026-01", "2026-01", None),
        ("lifetime", "2026-01", "incompatible_sales_counter_periods"),
        ("unknown", "lifetime", "incomplete_exact_transaction_coverage"),
    ],
)
def test_transaction_baseline_requires_one_common_declared_period(
    first_period, second_period, expected_reason
):
    products, links, evidence = [], [], []
    for index, period in enumerate((first_period, second_period), 1):
        entity, product_id = f"v{index}", f"p{index}"
        product, link = catalog(entity, product_id)
        products.append(product)
        links.append(link)
        evidence.extend(
            [
                observation(entity, "2026-01-01T00:00:00Z", 100),
                observation(entity, DECISION.isoformat(), 200),
                observation(
                    f"o{index}",
                    DECISION.isoformat(),
                    0,
                    entity_type="offer",
                    metrics={"units_sold": Measurement(lower=index * 100, upper=index * 100)},
                    attributes={"sold_period": period},
                ),
            ]
        )
        links.append(
            ProductLink(
                product_id=product_id,
                entity_key=f"fixture:offer:o{index}",
                available_at=link.available_at,
            )
        )
    snapshot = freeze_decision(
        evidence, products, links, DECISION, EvaluationConfig(target="sales"), synthetic=True
    )
    assert ("sales_popularity" in snapshot["baselines"]) == (expected_reason is None)
    assert snapshot["baseline_eligibility"]["sales_popularity"]["reason"] == expected_reason


@pytest.mark.parametrize("extra_kind", ["price", "estimated", "bounded", "stale", "interval"])
def test_partial_transaction_coverage_blocks_baselines_but_price_only_offers_do_not(extra_kind):
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", DECISION.isoformat(), 200),
        observation(
            "exact",
            DECISION.isoformat(),
            0,
            entity_type="offer",
            metrics={"units_sold": Measurement(lower=10, upper=10)},
            attributes={"sold_period": "lifetime"},
        ),
    ]
    when = utc("2025-12-29T00:00:00Z") if extra_kind == "stale" else DECISION
    metric = (
        None
        if extra_kind == "price"
        else Measurement(
            lower=100,
            upper=200 if extra_kind in {"estimated", "bounded"} else 100,
            precision=extra_kind if extra_kind in {"estimated", "bounded"} else "exact",
            model_version="explicit-estimate-v1" if extra_kind == "estimated" else None,
            semantics="interval" if extra_kind == "interval" else "cumulative",
        )
    )
    evidence.append(
        observation(
            "other",
            when.isoformat(),
            0,
            entity_type="offer",
            published_at=None,
            metrics={"units_sold": metric} if metric else {},
            attributes={"sold_period": "lifetime", "price": "100", "currency": "EUR"},
        )
    )
    links = [
        link,
        *[
            ProductLink(
                product_id="p1", entity_key=f"fixture:offer:{key}", available_at=link.available_at
            )
            for key in ("exact", "other")
        ],
    ]
    snapshot = freeze_decision(
        evidence, [product], links, DECISION, EvaluationConfig(target="sales"), synthetic=True
    )
    assert ("sales_popularity" in snapshot["baselines"]) == (extra_kind == "price")
    if extra_kind != "price":
        assert snapshot["baseline_eligibility"]["sales_popularity"]["reason"] == (
            "incomplete_exact_transaction_coverage"
        )
    if extra_kind == "estimated":
        # The existing observed-subset feature remains available to assessment.
        assert snapshot["universe"][0]["features"]["units_sold"] == 10


def test_overflow_across_otherwise_finite_ledgers_is_censored_per_product():
    product, links, evidence, realized, config = financial_case()
    second = "fixture:cost:second"
    links.append(
        ProductLink(product_id="p1", entity_key=second, available_at=links[0].available_at)
    )
    config = config.model_copy(
        update={
            "profit_contract": ProfitContract(
                ledgers={LEDGER: ("goods", "fees"), second: ("goods",)}
            )
        }
    )
    attributes = {
        **realized.attributes,
        "realized_revenue": "9e999999",
        "realized_costs": {"goods": "0", "fees": "0"},
    }
    realized = realized.model_copy(update={"attributes": attributes})
    another = realized.model_copy(update={"entity_id": "second", "observation_id": "second-ledger"})
    snapshot = freeze_decision(evidence, [product], links, DECISION, config, synthetic=True)
    report = mature_decision(snapshot, [*evidence, realized, another], LATER)
    result = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert result["status"] == "censored"
    assert result["reason"] == "invalid_realized_product_amount"
    assert "lower" not in result


@pytest.mark.parametrize("measured", [False, True])
def test_profit_reporting_counts_policy_measurements_without_claiming_business_profit(measured):
    product, links, evidence, realized, config = financial_case()
    report = backtest(
        [*evidence, *([realized] if measured else [])],
        [product],
        links,
        [DECISION],
        LATER,
        config,
        synthetic=True,
    )
    assert report["methodology"]["target"] == "profit"
    assert report["methodology"]["measured_financial_outcome_count"] == int(measured)
    assert report["methodology"]["profit_claim"] is False
    assert report["synthetic"] is True
    empty = backtest([], [], [], [], LATER, config, synthetic=True)
    assert empty["status"] == "insufficient_history"
    assert empty["methodology"]["measured_financial_outcome_count"] == 0
    assert empty["methodology"]["profit_claim"] is False

"""Economic scenarios through real launch revisions and native PostgreSQL."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError
from test_launch_dossiers import draft

from sm_trending_products.commerce import LaunchError, LaunchService
from sm_trending_products.commercial_sensitivity import (
    CashOrders,
    CashPlan,
    EconomicInsights,
    JointScenario,
    PaymentPart,
    ScenarioGrid,
    ScenarioProfile,
)


def profile(**changes):
    values = {"scenarios": [{"name": "declared", "ad_cost": "5"}]}
    values.update(changes)
    return ScenarioProfile.model_validate(values)


def cash_plan(**changes):
    values = {
        "basis": "Ten operator-supplied orders; net of output-tax reserve; no forecast",
        "settlement_basis": "net_revenue_before_costs_and_returns",
        "horizon_days": 14,
        "opening_cash": "0",
        "orders": [{"day": 0, "quantity": 10}],
        "supplier_payments": [{"offset_days": 0, "fraction": "1"}],
        "settlements": [{"offset_days": 7, "fraction": "1"}],
        "cost_payment_days": {
            "shipping": 0,
            "payment_fees": 0,
            "nonrecoverable_taxes": 0,
            "returns_allowance": 0,
            "fulfillment": 0,
            "ad_cost": 0,
        },
    }
    values.update(changes)
    return values


@pytest.fixture
def insights(pipeline):
    pipeline.create_product("p1", "Fixture product", synthetic=True)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create")
    return EconomicInsights(launches)


def describe(insights, values=None):
    return insights.describe("launch-p1-fr", 1, values or profile(), synthetic=True)


def test_dossier_allowance_and_decision_breakpoints(insights):
    result = describe(insights)
    point = result["scenarios"][0]
    assert result["revision"] == 1
    assert result["scenario_only"] is True
    assert result["synthetic"] is True
    assert point["economics"] == {
        "net_revenue": "30.00",
        "total_costs_before_ads": "15.00",
        "pre_ad_contribution": "15.00",
        "post_ad_contribution": "10.00",
        "target_surplus": "5.00",
        "meets_target": True,
    }
    assert point["breakpoints"]["maximum_ad_cost"] == "10.00"
    assert point["breakpoints"]["maximum_goods_cost"] == "13.25"
    assert point["breakpoints"]["return_tolerance"]["status"] == "unknown"
    assert point["cash"] == {"status": "unknown", "unknown_reasons": ["cash_plan"]}
    assert result["summary"]["all_meet_target"] is True


def test_joint_ranges_are_monotone_and_returns_replace_allowance(insights):
    values = profile(
        scenarios=[
            {
                "name": "joint",
                "returns": {
                    "rate": "0",
                    "loss_per_return": "20",
                    "basis": "Refund loss including recovery; excludes normal costs",
                },
                "grid": {
                    "goods_costs": ["8.25", "18.25"],
                    "ad_costs": ["5", "10"],
                    "return_rates": ["0", "0.5"],
                },
            }
        ]
    )
    result = describe(insights, values)
    points = result["scenarios"]
    assert len(points) == 8
    assert [Decimal(point["economics"]["target_surplus"]) for point in points] == [
        Decimal("7"),
        Decimal("-3"),
        Decimal("2"),
        Decimal("-8"),
        Decimal("-3"),
        Decimal("-13"),
        Decimal("-8"),
        Decimal("-18"),
    ]
    assert points[0]["assumptions"]["costs"]["returns_allowance"] == "0"
    assert points[1]["assumptions"]["costs"]["returns_allowance"] == "10.0"
    tolerance = points[0]["breakpoints"]["return_tolerance"]
    assert tolerance["maximum_rate"] == "0.35"
    assert tolerance["ratio_numerator"] == "7.00"
    assert tolerance["ratio_denominator"] == "20"
    assert points[4]["breakpoints"]["return_tolerance"]["status"] == "unattainable"
    assert result["summary"]["all_meet_target"] is False


@pytest.mark.parametrize(
    "loss,ad,status,maximum",
    [
        ("0", "5", "no_sensitivity", "1"),
        ("0", "30", "no_sensitivity", None),
        ("1", "5", "all_rates", "1"),
        ("20", "12", "threshold", "0.00"),
    ],
)
def test_zero_and_nonzero_return_loss_edges(insights, loss, ad, status, maximum):
    result = describe(
        insights,
        profile(
            scenarios=[
                {
                    "name": "return-edge",
                    "ad_cost": ad,
                    "returns": {
                        "rate": "0",
                        "loss_per_return": loss,
                        "basis": "Declared total return loss",
                    },
                }
            ]
        ),
    )
    tolerance = result["scenarios"][0]["breakpoints"]["return_tolerance"]
    assert tolerance["status"] == status
    assert tolerance["maximum_rate"] == maximum


def test_positive_unit_contribution_still_needs_cash_before_staggered_payout(insights):
    result = describe(
        insights,
        profile(
            cash=cash_plan(
                settlements=[
                    {"offset_days": 3, "fraction": "0.4"},
                    {"offset_days": 7, "fraction": "0.6"},
                ]
            )
        ),
    )
    point = result["scenarios"][0]
    assert point["economics"]["post_ad_contribution"] == "10.00"
    cash = point["cash"]
    assert cash["status"] == "complete"
    assert Decimal(cash["additional_cash_required"]) == 200
    assert Decimal(cash["peak_cash_exposure"]) == 200
    assert Decimal(cash["closing_cash"]) == 100
    assert [row["day"] for row in cash["daily_flows"]] == [0, 3, 7]
    assert Decimal(cash["daily_flows"][1]["inflow"]) == 120
    assert "intraday" in cash["timing_resolution"]
    funded = describe(insights, profile(cash=cash_plan(opening_cash="250")))
    assert Decimal(funded["scenarios"][0]["cash"]["additional_cash_required"]) == 0
    assert Decimal(funded["scenarios"][0]["cash"]["minimum_cash"]) == 50


def test_horizon_retains_unsettled_flows_and_supplier_deposit_schedule(insights):
    result = describe(
        insights,
        profile(
            cash=cash_plan(
                horizon_days=5,
                orders=[{"day": 2, "quantity": 10}],
                supplier_payments=[
                    {"offset_days": -2, "fraction": "0.5"},
                    {"offset_days": 2, "fraction": "0.5"},
                ],
            )
        ),
    )
    cash = result["scenarios"][0]["cash"]
    assert Decimal(cash["beyond_horizon"]["inflow"]) == 300
    assert Decimal(cash["closing_cash"]) == -200
    assert [day["day"] for day in cash["daily_flows"]] == [0, 2, 4]
    assert Decimal(cash["daily_flows"][0]["outflow"]) == Decimal("41.25")


def test_cash_range_increases_funding_with_goods_and_ad_costs(insights):
    result = describe(
        insights,
        profile(
            scenarios=[
                {
                    "name": "cash-range",
                    "grid": {"goods_costs": ["8.25", "9.25"], "ad_costs": ["5", "6"]},
                }
            ],
            cash=cash_plan(),
        ),
    )
    assert [
        Decimal(point["cash"]["additional_cash_required"]) for point in result["scenarios"]
    ] == list(map(Decimal, [200, 210, 210, 220]))
    zero = describe(insights, profile(cash=cash_plan(orders=[{"day": 0, "quantity": 0}])))
    assert Decimal(zero["scenarios"][0]["cash"]["additional_cash_required"]) == 0
    assert Decimal(zero["scenarios"][0]["cash"]["closing_cash"]) == 0


def test_missing_tax_preserves_known_costs_and_missing_goods_still_has_a_cost_ceiling(insights):
    insights.launches.save(draft(tax_per_order=None), 1, "missing-tax")
    point = insights.describe("launch-p1-fr", 2, profile(), synthetic=True)["scenarios"][0]
    assert point["economics"]["total_costs_before_ads"] == "15.00"
    assert point["economics"]["net_revenue"] is None
    costs = dict(draft().costs)
    costs.pop("goods")
    insights.launches.save(draft(costs=costs), 2, "missing-goods")
    point = insights.describe("launch-p1-fr", 3, profile(), synthetic=True)["scenarios"][0]
    assert point["economics"]["total_costs_before_ads"] is None
    assert point["economics"]["target_surplus"] is None
    assert point["breakpoints"]["maximum_goods_cost"] == "13.25"
    assert point["breakpoints"]["maximum_ad_cost"] is None


def test_unknown_money_ad_cost_and_timing_stay_unknown(insights):
    unknown_ad = describe(insights, profile(scenarios=[{"name": "unknown-ad"}]))
    point = unknown_ad["scenarios"][0]
    assert point["economics"]["target_surplus"] is None
    assert point["breakpoints"]["maximum_ad_cost"] == "10.00"
    assert unknown_ad["summary"]["all_meet_target"] is None
    missing_timing = describe(insights, profile(cash=cash_plan(cost_payment_days={})))
    assert (
        "cash.cost_payment_days.shipping"
        in missing_timing["scenarios"][0]["cash"]["unknown_reasons"]
    )
    incomplete = draft(tax_per_order=None, costs={"goods": "0"})
    insights.launches.save(incomplete, 1, "missing")
    result = insights.describe("launch-p1-fr", 2, profile(cash=cash_plan()), synthetic=True)
    point = result["scenarios"][0]
    assert point["economics"]["net_revenue"] is None
    assert point["economics"]["pre_ad_contribution"] is None
    assert point["cash"]["status"] == "unknown"
    assert "tax_per_order" in point["unknown_reasons"]
    assert "costs.shipping" in point["unknown_reasons"]
    assert "costs.goods" not in point["unknown_reasons"]


def test_price_changes_require_declared_tax_instead_of_reusing_fixed_dossier_tax(insights):
    with pytest.raises(ValidationError, match="tax basis"):
        profile(scenarios=[{"name": "price", "retail_price": "48", "basis": "Alternative offer"}])
    point = describe(
        insights,
        profile(
            scenarios=[
                {
                    "name": "price",
                    "retail_price": "48",
                    "basis": "Operator tax scenario; not legal advice",
                    "tax_basis": {"prices_include_tax": True, "tax_per_order": "8"},
                    "ad_cost": "5",
                }
            ]
        ),
    )["scenarios"][0]
    assert point["economics"]["net_revenue"] == "40"
    assert point["economics"]["target_surplus"] == "15.00"


def test_revision_namespace_and_current_quote_freshness(insights):
    with pytest.raises(LaunchError) as error:
        insights.describe("launch-p1-fr", 1, profile())
    assert error.value.code == "not_found"
    insights.launches.save(draft(quote_expires_at=datetime(2020, 1, 1, tzinfo=UTC)), 1, "stale")
    with pytest.raises(LaunchError) as error:
        describe(insights)
    assert error.value.code == "revision_conflict"
    result = insights.describe("launch-p1-fr", 2, profile(), synthetic=True)
    assert result["source_facts"]["readiness"]["status"] == "blocked"
    assert "stale_quote" in {r["code"] for r in result["source_facts"]["readiness"]["reasons"]}
    for revision in (True, 0, -1, 2_147_483_648):
        with pytest.raises(LaunchError) as error:
            insights.describe("launch-p1-fr", revision, profile(), synthetic=True)
        assert error.value.code == "invalid_revision"
    with pytest.raises(LaunchError) as error:
        insights.describe("launch-p1-fr", 2, profile(), synthetic="true")
    assert error.value.code == "invalid_workspace"


@pytest.mark.parametrize("value", [1.25, True, "NaN", "Infinity", "-1", "0.0000001", "1e1000"])
def test_exact_bounded_amounts(value):
    with pytest.raises(ValidationError):
        profile(scenarios=[{"name": "bad", "ad_cost": value}])


def test_authoritative_boundary_revalidates_mutated_nested_models(insights):
    values = profile(scenarios=[{"name": "joint", "grid": {"goods_costs": ["8"]}}])
    values.scenarios[0].grid.goods_costs.append(1.5)
    with pytest.raises(ValidationError):
        describe(insights, values)
    bypassed = profile().model_copy(update={"cash": cash_plan(horizon_days=True)})
    with pytest.raises(ValidationError):
        describe(insights, bypassed)
    assert insights.launches.dossier("launch-p1-fr", True)["revision"] == 1


@pytest.mark.parametrize("bad", [Decimal("-100"), True, 1.5, Decimal("NaN")])
def test_public_boundary_revalidates_constructed_models_inside_tuples(insights, bad):
    scenario = JointScenario.model_construct(name="bypass", ad_cost=bad)
    bypassed = ScenarioProfile.model_construct(scenarios=(scenario,))
    with pytest.raises(ValidationError):
        describe(insights, bypassed)
    grid = ScenarioGrid.model_construct(goods_costs=(bad,))
    scenario = JointScenario.model_construct(name="bypass", ad_cost=Decimal("5"), grid=grid)
    with pytest.raises(ValidationError):
        describe(insights, ScenarioProfile.model_construct(scenarios=(scenario,)))


@pytest.mark.parametrize("container", [list, tuple, set, frozenset])
def test_public_boundary_revalidates_nested_cash_container_values(insights, container):
    order = CashOrders.model_construct(day=0, quantity=-1)
    plan = CashPlan.model_construct(**cash_plan(orders=container([order])))
    with pytest.raises(ValidationError):
        describe(insights, profile().model_copy(update={"cash": plan}))
    parts = tuple(
        [
            PaymentPart.model_construct(offset_days=0, fraction=Decimal("-1")),
            PaymentPart.model_construct(offset_days=1, fraction=Decimal("2")),
        ]
    )
    plan = CashPlan.model_construct(**cash_plan(supplier_payments=parts))
    with pytest.raises(ValidationError):
        describe(insights, profile().model_copy(update={"cash": plan}))


@pytest.mark.parametrize("location", ["scenarios", "grid", "orders", "settlements"])
def test_public_boundary_rejects_lazy_iterators_without_consuming(insights, location):
    def forbidden():
        pytest.fail("A lazy or unbounded sequence must be rejected before iteration")
        yield

    lazy = forbidden()
    values = profile().model_dump(mode="python")
    if location == "scenarios":
        values["scenarios"] = lazy
    elif location == "grid":
        values["scenarios"][0]["grid"] = {"goods_costs": lazy}
    else:
        values["cash"] = cash_plan(**{location: lazy})
    with pytest.raises(ValidationError):
        describe(insights, values)


def test_materialized_tuple_profile_and_cash_sequences_remain_supported(insights):
    values = profile().model_dump(mode="python")
    values["scenarios"] = tuple(values["scenarios"])
    values["scenarios"][0]["grid"] = {"goods_costs": ("8.25", "9.25")}
    plan = cash_plan()
    for field in ("orders", "supplier_payments", "settlements"):
        plan[field] = tuple(plan[field])
    values["cash"] = plan
    result = describe(insights, values)
    assert [
        Decimal(point["cash"]["additional_cash_required"]) for point in result["scenarios"]
    ] == [Decimal(200), Decimal(210)]


@pytest.mark.parametrize(
    "zero",
    [
        "0e-1000000",
        "0e+1000000",
        "-0e-1000000",
        Decimal("-0e+1000000"),
    ],
)
def test_extreme_zero_exponents_have_bounded_public_output(insights, zero):
    values = {
        "scenarios": [
            {
                "name": "zero-arithmetic",
                "basis": "Explicit zero-price scenario",
                "retail_price": zero,
                "tax_basis": {"prices_include_tax": True, "tax_per_order": zero},
                "cost_overrides": {
                    name: zero
                    for name in ("shipping", "payment_fees", "nonrecoverable_taxes", "fulfillment")
                },
                "returns": {"rate": zero, "loss_per_return": zero, "basis": "Explicit zero loss"},
                "grid": {"goods_costs": [zero], "ad_costs": [zero], "return_rates": [zero]},
            },
            {
                "name": "zero-allowance",
                "basis": "Explicit zero allowance",
                "cost_overrides": {"returns_allowance": zero},
                "ad_cost": zero,
            },
        ],
        "cash": cash_plan(
            opening_cash=zero,
            supplier_payments=[
                {"offset_days": 0, "fraction": zero},
                {"offset_days": 1, "fraction": "1"},
            ],
            settlements=[{"offset_days": 0, "fraction": zero}, {"offset_days": 1, "fraction": "1"}],
        ),
    }
    result = describe(insights, values)
    point = result["scenarios"][0]
    assert point["status"] == "complete"
    assert Decimal(point["economics"]["post_ad_contribution"]) == 0
    assert Decimal(point["cash"]["closing_cash"]) == 0
    assert point["assumptions"]["ad_cost"] == "0"
    assert result["profile"]["cash"]["opening_cash"] == "0"
    assert len(json.dumps(result)) < 20_000
    # Constructed instances still pass through the same numeric normalization.
    bypassed = ScenarioProfile.model_construct(
        scenarios=(JointScenario.model_construct(name="constructed-zero", ad_cost=Decimal(zero)),),
        cash=CashPlan.model_construct(**cash_plan(opening_cash=Decimal(zero))),
    )
    result = describe(insights, bypassed)
    assert result["scenarios"][0]["assumptions"]["ad_cost"] == "0"
    assert len(json.dumps(result)) < 12_000


@pytest.mark.parametrize("zero", ["0e-1000000", "-0e+1000000"])
def test_imported_dossier_zeros_do_not_expand_source_facts_or_launch_reads(insights, zero):
    original = draft()
    source = draft(
        retail_price=zero,
        tax_per_order=zero,
        desired_profit=zero,
        costs={name: zero for name in original.costs},
    )
    saved = insights.launches.save(source, 1, "imported-zero")
    assert len(json.dumps(saved)) < 12_000
    result = insights.describe("launch-p1-fr", 2, profile(cash=cash_plan()), synthetic=True)
    point = result["scenarios"][0]
    assert Decimal(point["economics"]["pre_ad_contribution"]) == 0
    assert Decimal(point["economics"]["target_surplus"]) == -5
    assert all(len(value) < 40 for value in result["source_facts"]["draft"]["costs"].values())
    assert len(json.dumps(result)) < 12_000


@pytest.mark.parametrize("amount", ["0.000001", "999999999999.999999", "1e-6", "-0.000000"])
def test_near_bound_nonzero_amounts_and_negative_zero_still_work(insights, amount):
    result = describe(insights, profile(scenarios=[{"name": "bound", "ad_cost": amount}]))
    assert Decimal(result["scenarios"][0]["assumptions"]["ad_cost"]) == Decimal(amount)
    assert len(json.dumps(result)) < 12_000


@pytest.mark.parametrize(
    "change",
    [
        {"horizon_days": 366},
        {"orders": [{"day": 0, "quantity": True}]},
        {"orders": [{"day": 0, "quantity": 1_000_001}]},
        {"settlements": [{"offset_days": -1, "fraction": "1"}]},
        {"supplier_payments": [{"offset_days": -1, "fraction": "1"}]},
        {"settlements": [{"offset_days": 1, "fraction": "0.9"}]},
        {"orders": [{"day": 15, "quantity": 1}]},
    ],
)
def test_bounded_explicit_cash_schedules(change):
    with pytest.raises(ValidationError):
        profile(cash=cash_plan(**change))


def test_bounded_ranges_and_no_duplicate_return_charge():
    with pytest.raises(ValidationError, match="128 points"):
        profile(
            scenarios=[
                {
                    "name": f"grid-{index}",
                    "returns": {"rate": "0", "loss_per_return": "10", "basis": "Declared"},
                    "grid": {
                        "goods_costs": ["1", "2", "3", "4", "5"],
                        "ad_costs": ["1", "2", "3", "4", "5"],
                        "return_rates": ["0", "0.1", "0.2", "0.3", "0.4"],
                    },
                }
                for index in range(2)
            ]
        )
    for scenario in (
        {"name": "duplicate", "grid": {"goods_costs": ["1", "1.0"]}},
        {"name": "rate-only", "grid": {"return_rates": ["0.1"]}},
        {
            "name": "duplicate-returns",
            "cost_overrides": {"returns_allowance": "1"},
            "returns": {"rate": "0.1", "loss_per_return": "10", "basis": "Declared"},
        },
        {
            "name": "invalid-rate",
            "returns": {"rate": "1.01", "loss_per_return": "10", "basis": "Declared"},
        },
    ):
        with pytest.raises(ValidationError):
            profile(scenarios=[scenario])

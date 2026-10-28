"""Bounded joint economics and timed cash scenarios over an immutable launch revision.

No traffic, tax law, return probability or realized sale is inferred here. Returns
are a replacement loss allowance, and settlements are explicitly net of output
tax reserved outside this cash model, before the declared costs and return loss.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, localcontext
from itertools import product
from math import prod
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    ConfigDict,
    Field,
    StrictBool,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .commerce import LaunchDraft, LaunchError, LaunchService
from .commerce.models import REQUIRED_COSTS, CostName, Money
from .contracts import Contract, digest


def _canonical_zero(value: Decimal) -> Decimal:
    # Decimal's digit/place limits permit 0E±arbitrary_exponent. Canonicalize it
    # before arithmetic or fixed-point serialization can expand that exponent.
    return Decimal(0) if value.is_zero() else value


EconomicMoney = Annotated[Money, AfterValidator(_canonical_zero)]
Rate = Annotated[EconomicMoney, Field(le=1)]
Day = Annotated[int, Field(ge=0, le=365, strict=True)]
OtherCost = Literal[
    "shipping",
    "payment_fees",
    "nonrecoverable_taxes",
    "returns_allowance",
    "fulfillment",
    "ad_cost",
]
Basis = Annotated[str, Field(min_length=1, max_length=2000, pattern=r"\S")]


class _EconomicContract(Contract):
    """Revalidate every nested model and admit only bounded materialized collections."""

    model_config = ConfigDict(revalidate_instances="always")

    @field_validator("*", mode="before")
    @classmethod
    def bounded_containers(cls, value: object, info: ValidationInfo) -> object:
        sequence_limits = {
            "scenarios": 16,
            "goods_costs": 5,
            "ad_costs": 5,
            "return_rates": 5,
            "orders": 366,
            "supplier_payments": 8,
            "settlements": 8,
        }
        maximum = sequence_limits.get(info.field_name or "")
        if maximum is not None and value is not None:
            if type(value) not in (list, tuple):
                raise ValueError("Scenario sequences require a materialized list or tuple")
            if len(value) > maximum:  # type: ignore[arg-type]
                raise ValueError(f"{info.field_name} is limited to {maximum} entries")
        if info.field_name in {"cost_overrides", "cost_payment_days"}:
            if type(value) is not dict or len(value) > 6:
                raise ValueError("Cost mappings require a dictionary with at most six entries")
        return value


class TaxBasis(_EconomicContract):
    """Operator-declared per-order output tax for this particular price."""

    prices_include_tax: StrictBool
    tax_per_order: EconomicMoney | None = None

    @model_validator(mode="after")
    def explicit_tax(self) -> TaxBasis:
        if self.prices_include_tax and self.tax_per_order is None:
            raise ValueError("An inclusive scenario requires its own tax_per_order")
        if not self.prices_include_tax and self.tax_per_order is not None:
            raise ValueError("An exclusive scenario declares net revenue without output tax")
        return self


class ReturnAssumptions(_EconomicContract):
    """Total incremental loss per return, including any refund and recovery.

    This replaces the dossier returns_allowance. Goods and other ordinary order
    costs remain charged once; loss_per_return must not repeat those costs.
    """

    rate: Rate
    loss_per_return: EconomicMoney
    basis: Basis


class ScenarioGrid(_EconomicContract):
    goods_costs: list[EconomicMoney] | None = Field(default=None, min_length=1, max_length=5)
    ad_costs: list[EconomicMoney] | None = Field(default=None, min_length=1, max_length=5)
    return_rates: list[Rate] | None = Field(default=None, min_length=1, max_length=5)

    @model_validator(mode="after")
    def distinct_points(self) -> ScenarioGrid:
        for axis in (self.goods_costs, self.ad_costs, self.return_rates):
            if axis is not None and len(axis) != len(set(axis)):
                raise ValueError("Grid axes must contain distinct points")
        return self


class JointScenario(_EconomicContract):
    name: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9][\w.:-]*$")
    basis: Basis | None = None
    retail_price: EconomicMoney | None = None
    tax_basis: TaxBasis | None = None
    cost_overrides: dict[CostName, EconomicMoney] = Field(default_factory=dict)
    ad_cost: EconomicMoney | None = None
    returns: ReturnAssumptions | None = None
    grid: ScenarioGrid | None = None

    @model_validator(mode="after")
    def explicit_basis(self) -> JointScenario:
        if (self.retail_price is not None or self.tax_basis is not None) and self.basis is None:
            raise ValueError("Price and tax changes require an explicit economics basis")
        if "returns_allowance" in self.cost_overrides and self.basis is None:
            raise ValueError("A changed return allowance requires an explicit economics basis")
        if self.retail_price is not None and self.tax_basis is None:
            raise ValueError("A changed price requires its own tax basis")
        if self.returns is not None and "returns_allowance" in self.cost_overrides:
            raise ValueError("Return loss replaces returns_allowance; do not declare both")
        if self.grid is not None and self.grid.return_rates is not None and self.returns is None:
            raise ValueError("Return ranges require an explicit replacement return-loss model")
        if (
            self.retail_price is not None
            and self.tax_basis is not None
            and self.tax_basis.tax_per_order is not None
            and self.tax_basis.tax_per_order > self.retail_price
        ):
            raise ValueError("Included tax cannot exceed the scenario price")
        if self.grid is not None:
            if self.grid.goods_costs is not None and "goods" in self.cost_overrides:
                raise ValueError("Declare goods as an override or a range, not both")
            if self.grid.ad_costs is not None and self.ad_cost is not None:
                raise ValueError("Declare acquisition cost as an amount or a range, not both")
        return self


class CashOrders(_EconomicContract):
    day: Day
    quantity: int = Field(ge=0, le=1_000_000, strict=True)


class PaymentPart(_EconomicContract):
    offset_days: int = Field(ge=-365, le=365, strict=True)
    fraction: Rate


class CashPlan(_EconomicContract):
    """Daily net cash after output-tax reserve, before costs and return losses.

    Fractions describe timing, not provider behavior. Same-day flows are netted;
    unknown intraday ordering is outside this declared daily scenario.
    """

    basis: Basis
    settlement_basis: Literal["net_revenue_before_costs_and_returns"]
    horizon_days: Day
    opening_cash: EconomicMoney
    orders: list[CashOrders] = Field(min_length=1, max_length=366)
    supplier_payments: list[PaymentPart] = Field(min_length=1, max_length=8)
    settlements: list[PaymentPart] = Field(min_length=1, max_length=8)
    cost_payment_days: dict[OtherCost, Day] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded_schedule(self) -> CashPlan:
        if len({order.day for order in self.orders}) != len(self.orders):
            raise ValueError("Combine order quantities for each day")
        if sum(order.quantity for order in self.orders) > 1_000_000:
            raise ValueError("Total scenario volume is limited to 1000000 orders")
        for schedule in (self.supplier_payments, self.settlements):
            if sum((part.fraction for part in schedule), Decimal(0)) != 1:
                raise ValueError("Each payment schedule must allocate exactly one whole amount")
            if len({part.offset_days for part in schedule}) != len(schedule):
                raise ValueError("Combine fractions at each payment offset")
        if any(part.offset_days < 0 for part in self.settlements):
            raise ValueError("Settlement cannot precede an order")
        for order in self.orders:
            if order.day > self.horizon_days:
                raise ValueError("Order days must lie within the declared horizon")
            if any(order.day + part.offset_days < 0 for part in self.supplier_payments):
                raise ValueError("Supplier payments must not precede the scenario start")
        return self


class ScenarioProfile(_EconomicContract):
    schema_version: Literal["1"] = "1"
    scenarios: list[JointScenario] = Field(min_length=1, max_length=16)
    cash: CashPlan | None = None

    @model_validator(mode="after")
    def bounded_expansion(self) -> ScenarioProfile:
        if len({scenario.name for scenario in self.scenarios}) != len(self.scenarios):
            raise ValueError("Scenario names must be unique")
        count = sum(
            prod(
                len(axis) if axis is not None else 1
                for axis in (
                    scenario.grid.goods_costs,
                    scenario.grid.ad_costs,
                    scenario.grid.return_rates,
                )
            )
            if scenario.grid is not None
            else 1
            for scenario in self.scenarios
        )
        if count > 128:
            raise ValueError("The joint scenario grid is limited to 128 points")
        return self


def _money(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _cash(
    plan: CashPlan | None,
    net: Decimal | None,
    costs: dict[str, Decimal],
    ad: Decimal | None,
    unknown: list[str],
) -> dict:
    if plan is None:
        return {"status": "unknown", "unknown_reasons": ["cash_plan"]}
    gaps = list(unknown)
    if ad is None:
        gaps.append("ad_cost")
    payable = {**costs, "ad_cost": ad}
    for name, amount in payable.items():
        if name != "goods" and amount and name not in plan.cost_payment_days:
            gaps.append(f"cash.cost_payment_days.{name}")
    if gaps:
        return {"status": "unknown", "unknown_reasons": sorted(set(gaps))}
    assert net is not None and ad is not None
    # One amount per daily bucket bounds work independently of scenario volume.
    flows: dict[int, list[Decimal]] = defaultdict(lambda: [Decimal(0), Decimal(0)])
    for order in plan.orders:
        for part in plan.settlements:
            flows[order.day + part.offset_days][0] += net * order.quantity * part.fraction
        for part in plan.supplier_payments:
            flows[order.day + part.offset_days][1] += (
                costs["goods"] * order.quantity * part.fraction
            )
        for name, amount in payable.items():
            if name != "goods" and amount:
                offset = plan.cost_payment_days[name]  # type: ignore[index]
                flows[order.day + offset][1] += amount * order.quantity
    cash = plan.opening_cash
    minimum = cash
    days = []
    later = [Decimal(0), Decimal(0)]
    for day, (inflow, outflow) in sorted(flows.items()):
        if day > plan.horizon_days:
            later[0] += inflow
            later[1] += outflow
            continue
        cash += inflow - outflow
        minimum = min(minimum, cash)
        days.append(
            {
                "day": day,
                "inflow": _money(inflow),
                "outflow": _money(outflow),
                "closing_cash": _money(cash),
            }
        )
    return {
        "status": "complete",
        "unknown_reasons": [],
        "daily_flows": days,
        "opening_cash": _money(plan.opening_cash),
        "horizon_days": plan.horizon_days,
        "minimum_cash": _money(minimum),
        "closing_cash": _money(cash),
        "additional_cash_required": _money(max(Decimal(0), -minimum)),
        "peak_cash_exposure": _money(max(Decimal(0), plan.opening_cash - minimum)),
        "beyond_horizon": {"inflow": _money(later[0]), "outflow": _money(later[1])},
        "total_orders": sum(order.quantity for order in plan.orders),
        "basis": plan.basis,
        "settlement_basis": plan.settlement_basis,
        "timing_resolution": "daily_net; intraday ordering unknown",
    }


def _point(
    draft: LaunchDraft,
    scenario: JointScenario,
    index: int,
    goods: Decimal | None,
    ad: Decimal | None,
    rate: Decimal | None,
    cash: CashPlan | None,
) -> dict:
    costs: dict[str, Decimal] = {name: amount for name, amount in draft.costs.items()}
    costs.update((name, amount) for name, amount in scenario.cost_overrides.items())
    if goods is not None:
        costs["goods"] = goods
    if scenario.returns is not None:
        assert rate is not None
        costs["returns_allowance"] = rate * scenario.returns.loss_per_return
    price = scenario.retail_price if scenario.retail_price is not None else draft.retail_price
    inclusive = (
        scenario.tax_basis.prices_include_tax if scenario.tax_basis else draft.prices_include_tax
    )
    tax = scenario.tax_basis.tax_per_order if scenario.tax_basis else draft.tax_per_order
    if inclusive and price is not None and tax is not None and tax > price:
        raise LaunchError("invalid_scenario_tax", "Included tax exceeds the scenario price")
    unknown = [f"costs.{name}" for name in REQUIRED_COSTS if name not in costs]
    if price is None:
        unknown.append("retail_price")
    if inclusive is None:
        unknown.append("prices_include_tax")
    if inclusive is True and tax is None:
        unknown.append("tax_per_order")
    net = (
        price - tax
        if price is not None and inclusive and tax is not None
        else (price if inclusive is False else None)
    )
    total = (
        sum(costs.values(), Decimal(0)) if all(name in costs for name in REQUIRED_COSTS) else None
    )
    before_ad = net - total if net is not None and total is not None else None
    after_ad = before_ad - ad if before_ad is not None and ad is not None else None
    surplus = after_ad - draft.desired_profit if after_ad is not None else None
    max_ad = before_ad - draft.desired_profit if before_ad is not None else None
    other_costs = [name for name in REQUIRED_COSTS if name != "goods"]
    max_goods = (
        net - sum((costs[name] for name in other_costs), Decimal(0)) - ad - draft.desired_profit
        if net is not None and ad is not None and all(name in costs for name in other_costs)
        else None
    )
    tolerance: dict = {"status": "unknown", "reason": "replacement_return_model_required"}
    if scenario.returns is not None and surplus is not None:
        loss = scenario.returns.loss_per_return
        zero_surplus = surplus + costs["returns_allowance"]
        if loss == 0:
            tolerance = {
                "status": "no_sensitivity",
                "maximum_rate": "1" if zero_surplus >= 0 else None,
                "reason": "declared_return_loss_is_zero",
            }
        elif zero_surplus < 0:
            tolerance = {"status": "unattainable", "maximum_rate": None}
        else:
            tolerance = {
                "status": "all_rates" if zero_surplus >= loss else "threshold",
                "maximum_rate": _money(min(Decimal(1), zero_surplus / loss)),
                "ratio_numerator": _money(zero_surplus),
                "ratio_denominator": _money(loss),
            }
    return {
        "scenario_id": f"{scenario.name}:{index}",
        "name": scenario.name,
        "status": "complete" if not unknown and ad is not None else "incomplete",
        "unknown_reasons": [*unknown, *(["ad_cost"] if ad is None else [])],
        "assumptions": {
            "retail_price": _money(price),
            "prices_include_tax": inclusive,
            "tax_per_order": _money(tax) if inclusive else None,
            "costs": {name: _money(amount) for name, amount in costs.items()},
            "ad_cost": _money(ad),
            "desired_profit": _money(draft.desired_profit),
            "return_rate": _money(rate),
            "return_loss_per_return": _money(scenario.returns.loss_per_return)
            if scenario.returns
            else None,
            "return_basis": scenario.returns.basis if scenario.returns else None,
            "return_treatment": "replacement_loss" if scenario.returns else "dossier_allowance",
            "economics_basis": scenario.basis,
        },
        "economics": {
            "net_revenue": _money(net),
            "total_costs_before_ads": _money(total),
            "pre_ad_contribution": _money(before_ad),
            "post_ad_contribution": _money(after_ad),
            "target_surplus": _money(surplus),
            "meets_target": surplus >= 0 if surplus is not None else None,
        },
        "breakpoints": {
            "maximum_ad_cost": _money(max_ad),
            "maximum_goods_cost": _money(max_goods),
            "return_tolerance": tolerance,
            "basis": "desired profit target; all other inputs held fixed",
        },
        "cash": _cash(cash, net, costs, ad, unknown),
    }


class EconomicInsights:
    """Read launch facts once and expose bounded, reproducible decision arithmetic."""

    def __init__(self, launches: LaunchService):
        self.launches = launches

    def describe(
        self,
        dossier_id: str,
        expected_revision: int,
        scenario_profile: ScenarioProfile | dict,
        synthetic: bool = False,
    ) -> dict:
        profile = ScenarioProfile.model_validate(scenario_profile)
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or not 1 <= expected_revision <= 2_147_483_647
        ):
            raise LaunchError("invalid_revision")
        # Validate identifiers before passing them through the public launch boundary.
        if (
            not isinstance(dossier_id, str)
            or not 1 <= len(dossier_id) <= 128
            or dossier_id != dossier_id.strip()
            or any(ord(c) < 32 for c in dossier_id)
        ):
            raise LaunchError("invalid_dossier_id")
        record = self.launches.dossier(dossier_id, synthetic=synthetic)
        if record["revision"] != expected_revision:
            raise LaunchError("revision_conflict")
        draft = LaunchDraft.model_validate(record["draft"])
        draft = draft.model_copy(
            update={
                **{
                    name: _canonical_zero(value) if value is not None else None
                    for name in ("retail_price", "tax_per_order", "desired_profit")
                    for value in (getattr(draft, name),)
                },
                "costs": {name: _canonical_zero(value) for name, value in draft.costs.items()},
            }
        )
        points: list[dict] = []
        # More than enough for the product/sums of bounded 18-digit inputs. Division
        # may repeat; the exact numerator and denominator accompany return thresholds.
        with localcontext() as context:
            context.prec = 60
            for scenario in profile.scenarios:
                grid = scenario.grid or ScenarioGrid()
                axes = (
                    grid.goods_costs if grid.goods_costs is not None else [None],
                    grid.ad_costs if grid.ad_costs is not None else [scenario.ad_cost],
                    grid.return_rates
                    if grid.return_rates is not None
                    else [scenario.returns.rate if scenario.returns else None],
                )
                points.extend(
                    _point(draft, scenario, index, goods, ad, rate, profile.cash)
                    for index, (goods, ad, rate) in enumerate(product(*axes), start=1)
                )
        known = [point for point in points if point["status"] == "complete"]
        surpluses = [Decimal(point["economics"]["target_surplus"]) for point in known]
        return {
            "schema_version": "1",
            "scenario_only": True,
            "dossier_id": dossier_id,
            "revision": record["revision"],
            "product_id": draft.product_id,
            "country": draft.country,
            "currency": draft.currency,
            "synthetic": synthetic,
            "evaluated_at": record["readiness"]["evaluated_at"],
            "source_facts": {
                "draft": record["draft"],
                "updated_at": record["updated_at"],
                "readiness": record["readiness"],
            },
            "profile": profile.model_dump(mode="json"),
            "profile_hash": digest(profile.model_dump(mode="json")),
            "scenarios": points,
            "summary": {
                "points": len(points),
                "complete_points": len(known),
                "incomplete_points": len(points) - len(known),
                "minimum_target_surplus": _money(min(surpluses)) if surpluses else None,
                "maximum_target_surplus": _money(max(surpluses)) if surpluses else None,
                "all_meet_target": all(value >= 0 for value in surpluses)
                if len(known) == len(points)
                else None,
            },
            "limitations": [
                "Operator scenarios, not realized profit, demand estimates or forecasts",
                "Ranges are declared joint points, not probabilities or confidence intervals",
                "Changing price requires a new explicit tax basis; no tax law is inferred",
                "Return loss replaces the allowance and includes any declared refund once",
                "Cash reserves output tax before settlement; no actual provider ledger is inferred",
                "Cash nets same-day flows; intraday liquidity and undeclared expenses are unknown",
            ],
        }

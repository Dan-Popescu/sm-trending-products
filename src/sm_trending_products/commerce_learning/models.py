"""Exact reviewed commercial facts, separate from launch-price scenarios."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BeforeValidator, Field, StrictBool, field_validator, model_validator

from ..contracts import Contract, utc


class LearningError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code.replace("_", " "))


def _exact(value: object) -> object:
    if isinstance(value, bool) or not isinstance(value, str | int | Decimal):
        raise ValueError("Actual amounts require exact decimal strings, never floats")
    return value


Money = Annotated[Decimal, BeforeValidator(_exact), Field(ge=0, max_digits=18, decimal_places=6)]
Revenue = Annotated[Decimal, BeforeValidator(_exact), Field(max_digits=18, decimal_places=6)]
Cost = Literal[
    "goods",
    "shipping",
    "payment_fees",
    "nonrecoverable_taxes",
    "returns",
    "fulfillment",
    "advertising",
]
ACTUAL_COSTS: tuple[Cost, ...] = (
    "goods",
    "shipping",
    "payment_fees",
    "nonrecoverable_taxes",
    "returns",
    "fulfillment",
    "advertising",
)
Reference = Annotated[str, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9][\w.:-]*$")]


class ActualCostContract(Contract):
    version: Literal["commercial-actual-costs-v1"] = "commercial-actual-costs-v1"
    required_costs: tuple[Cost, ...]
    revenue_basis: Literal["settled_payments_less_executed_refunds_excluding_output_taxes"]
    coverage_policy: str = Field(min_length=1, max_length=2000)

    @model_validator(mode="after")
    def exhaustive_contract(self) -> ActualCostContract:
        if len(self.required_costs) != len(ACTUAL_COSTS) or set(self.required_costs) != set(
            ACTUAL_COSTS
        ):
            raise ValueError(
                "Declare every actual-cost component exactly once; reviewed zero is explicit"
            )
        if not self.coverage_policy.strip():
            raise ValueError(
                "Declare how actual payment, refund, fee and cost coverage is reviewed"
            )
        return self


class TestPlan(Contract):
    selection: Literal["test", "watch", "skip"]
    reason: str = Field(min_length=1, max_length=4000)
    selection_policy: str = Field(min_length=1, max_length=4000)
    cost_contract: ActualCostContract
    assessment_run_id: Reference | None = None
    forward_evaluation_id: Reference | None = None

    @field_validator("reason", "selection_policy")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Selection reason and policy cannot be blank")
        return value


class FinancialCoverage(Contract):
    """Reviewer attestations, never inferred from an order's paid flag or total."""

    payments_complete: StrictBool = False
    refunds_complete: StrictBool = False
    fees_complete: StrictBool = False
    costs_complete: StrictBool = False


class ReviewedPeriod(Contract):
    period_start: datetime
    period_end: datetime
    country: str = Field(pattern=r"^[A-Z]{2}$")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    amount_kind: Literal["actual"] = "actual"
    realized_revenue: Revenue | None = None
    realized_costs: dict[Cost, Money | None] = Field(default_factory=dict)
    evidence_refs: tuple[Reference, ...] = Field(default=(), max_length=200)
    coverage: FinancialCoverage
    reviewer_note: str = Field(min_length=1, max_length=4000)

    @field_validator("period_start", "period_end")
    @classmethod
    def explicit_time(cls, value: datetime) -> datetime:
        return utc(value)

    @model_validator(mode="after")
    def bounded_period(self) -> ReviewedPeriod:
        if not timedelta(0) < self.period_end - self.period_start <= timedelta(days=366):
            raise ValueError("An exact reviewed period must be positive and at most 366 days")
        if not self.reviewer_note.strip():
            raise ValueError("A reviewer note is required")
        if len(set(self.evidence_refs)) != len(self.evidence_refs):
            raise ValueError("Evidence references must be unique opaque identifiers")
        return self

    def missing(self, contract: ActualCostContract) -> list[str]:
        reasons = []
        if self.realized_revenue is None:
            reasons.append("unknown_realized_revenue")
        reasons.extend(
            "unknown_actual_cost:" + name
            for name in contract.required_costs
            if self.realized_costs.get(name) is None
        )
        reasons.extend(
            "incomplete_coverage:" + name
            for name, complete in dict(self.coverage).items()
            if not complete
        )
        if not self.evidence_refs:
            reasons.append("missing_actual_evidence")
        return reasons

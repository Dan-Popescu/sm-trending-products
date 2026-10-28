"""Scenario arithmetic and transparent completeness, without legal inference."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from .models import REQUIRED_COSTS, LaunchDraft


def assess(draft: LaunchDraft, evaluated_at: datetime) -> tuple[dict, dict]:
    reasons: list[dict[str, str]] = []
    blocked = False

    def missing(field: str, message: str) -> None:
        reasons.append({"code": "missing_fact", "field": field, "message": message})

    for field in ("title", "description", "supplier_name", "supplier_reference", "return_terms"):
        if not getattr(draft, field).strip():
            missing(field, f"Declare {field.replace('_', ' ')}")
    for field in ("delivery_min_days", "delivery_max_days", "quote_expires_at"):
        if getattr(draft, field) is None:
            missing(field, f"Declare {field.replace('_', ' ')}")
    if draft.quote_expires_at is not None and draft.quote_expires_at <= evaluated_at:
        blocked = True
        reasons.append(
            {
                "code": "stale_quote",
                "field": "quote_expires_at",
                "message": "Refresh the expired supplier quote and associated facts",
            }
        )
    if draft.stock_status == "unknown":
        missing("stock_status", "Supplier availability remains unknown")
    elif draft.stock_status == "unavailable":
        blocked = True
        reasons.append(
            {
                "code": "unavailable_stock",
                "field": "stock_status",
                "message": "The declared stock is unavailable",
            }
        )
    if draft.decision != "test":
        blocked = blocked or draft.decision in {"hold", "reject"}
        reasons.append(
            {
                "code": "operator_decision",
                "field": "decision",
                "message": "An explicit test decision is required",
            }
        )

    unknown: list[str] = []
    if draft.retail_price is None:
        unknown.append("retail_price")
    if draft.prices_include_tax is None:
        unknown.append("prices_include_tax")
    if draft.prices_include_tax is True and draft.tax_per_order is None:
        unknown.append("tax_per_order")
    for name in REQUIRED_COSTS:
        if name not in draft.costs:
            unknown.append(f"costs.{name}")
    for field in unknown:
        missing(field, f"Declare {field.replace('_', ' ')}; missing money is not zero")

    net: Decimal | None = None
    if draft.retail_price is not None:
        if draft.prices_include_tax is False:
            net = draft.retail_price
        elif draft.prices_include_tax is True and draft.tax_per_order is not None:
            net = draft.retail_price - draft.tax_per_order
    total = (
        sum(draft.costs.values(), Decimal("0"))
        if all(name in draft.costs for name in REQUIRED_COSTS)
        else None
    )
    contribution = net - total if net is not None and total is not None else None
    acquisition = contribution - draft.desired_profit if contribution is not None else None
    if acquisition is not None and acquisition < 0:
        blocked = True
        reasons.append(
            {
                "code": "negative_acquisition_budget",
                "field": "costs",
                "message": "This scenario cannot meet the declared profit target",
            }
        )

    def money(value: Decimal | None) -> str | None:
        return format(value, "f") if value is not None else None

    economics = {
        "currency": draft.currency,
        "net_revenue": money(net),
        "total_costs": money(total),
        "pre_ad_contribution": money(contribution),
        "maximum_acquisition_cost": money(acquisition),
        "desired_profit": money(draft.desired_profit),
        "unknown_reasons": unknown,
        "scenario_only": True,
    }
    readiness = {
        "status": "blocked" if blocked else "incomplete" if reasons else "ready",
        "reasons": reasons,
        "evaluated_at": evaluated_at.isoformat(),
        "scenario_only": True,
    }
    return readiness, economics

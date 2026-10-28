"""Authoritative counter periods and explicitly preserved outcome definitions."""

from __future__ import annotations

import re
from typing import Literal

from .contracts import Observation

CounterPeriodContract = Literal["documented-counter-v1"]
DOCUMENTED_COUNTER_CONTRACT: CounterPeriodContract = "documented-counter-v1"


def counter_period(
    observation: Observation,
    metric: str,
    *,
    contract: CounterPeriodContract | None = DOCUMENTED_COUNTER_CONTRACT,
) -> str | None:
    """Interpret comparable periods without treating rolling totals as increments.

    An absent contract preserves the exact interpretation of already frozen
    outcomes. Current assessments and explicitly versioned outcomes share the
    documented contract; callers do not reconstruct aliases or period rules.
    """
    if metric != "units_sold":
        return str(observation.attributes.get("view_count_period", "lifetime"))
    if contract is None:
        period = observation.attributes.get(
            "units_sold_period", observation.attributes.get("sold_period")
        )
        return str(period) if period is not None else None
    declarations = [
        observation.attributes[name]
        for name in ("units_sold_period", "sold_period")
        if name in observation.attributes
    ]
    if not declarations:
        return "lifetime" if observation.attributes.get("counter_scope") == "lifetime" else None
    if any(not isinstance(value, str) for value in declarations):
        return None
    periods = {value.strip().lower() for value in declarations}
    if len(periods) != 1:
        return None
    period = periods.pop()
    if period in {"", "unknown", "unspecified", "none", "null", "?"}:
        return None
    if period.startswith(("rolling", "last_", "last ", "past_", "past ")) or re.fullmatch(
        r"\d+\s*(d|days?|h|hours?|w|weeks?|months?)", period
    ):
        return None
    return period

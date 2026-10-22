"""Prepared entity timelines for exact, bounded and censored outcome queries."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .contracts import Measurement, Observation, utc


def _metric(row: Observation, target: str) -> Measurement | None:
    aliases = ("view_count", "views") if target == "attention" else ("units_sold", "sold_count")
    return next((row.metrics[name] for name in aliases if name in row.metrics), None)


def _period(row: Observation) -> tuple[datetime, datetime] | None:
    start = row.attributes.get("period_start", row.attributes.get("source_period_start"))
    end = row.attributes.get("period_end", row.attributes.get("source_period_end"))
    if start is None or end is None:
        return None
    try:
        bounds = utc(start), utc(end)
        return bounds if bounds[0] < bounds[1] else None
    except (ValueError, TypeError, AttributeError):
        return None


def _counter_period(row: Observation, target: str) -> str | None:
    if target == "attention":
        return str(row.attributes.get("view_count_period", "lifetime"))
    period = row.attributes.get("units_sold_period", row.attributes.get("sold_period"))
    return str(period) if period is not None else None


def _realized_profit(
    rows: Sequence[Observation],
    start: datetime,
    end: datetime,
    currency: str | None,
) -> dict[str, Any]:
    periods: dict[tuple[datetime, datetime], Observation] = {}
    for row in rows:
        period = _period(row)
        if period is None or period[0] < start or period[1] > end or period[1] > row.observed_at:
            continue
        previous = periods.get(period)
        if previous is None or (row.available_at, row.observation_id) > (
            previous.available_at,
            previous.observation_id,
        ):
            periods[period] = row
    cursor, profit = start, Decimal(0)
    evidence: list[str] = []
    for period, row in sorted(periods.items()):
        if period[0] != cursor:
            return {"status": "censored", "reason": "realized_profit_period_gap_or_overlap"}
        if (
            row.attributes.get("currency") != currency
            or row.attributes.get("realized") is not True
            or row.attributes.get("costs_complete") is not True
        ):
            return {"status": "censored", "reason": "incomplete_realized_costs_or_revenue"}
        try:
            revenue = Decimal(str(row.attributes["realized_revenue"]))
            costs = row.attributes["realized_costs"]
            total_costs = (
                sum((Decimal(str(value)) for value in costs.values()), Decimal(0))
                if isinstance(costs, dict)
                else Decimal(str(costs))
            )
            if not revenue.is_finite() or not total_costs.is_finite() or total_costs < 0:
                raise ValueError("Invalid realized amount")
            profit += revenue - total_costs
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return {"status": "censored", "reason": "incomplete_realized_costs_or_revenue"}
        evidence.append(row.observation_id)
        cursor = period[1]
    if not periods or cursor != end:
        return {"status": "censored", "reason": "incomplete_realized_profit_coverage"}
    return {
        "status": "measured",
        "lower": str(profit),
        "upper": str(profit),
        "precision": "exact",
        "evidence_refs": evidence,
        "unit": currency,
    }


class _PeriodPrefix:
    """End-ordered interval aggregates for one fixed action boundary.

    Valid interval coverage is a single directed path from start to end. Since
    every edge advances time, degree counts detect gaps/overlaps without rescans
    or a mutable ordered tree. References are materialized only for final results.
    """

    def __init__(self, periods: list[tuple], start: datetime):
        self.rows = sorted(
            (record for record in periods if record[0][0] >= start),
            key=lambda record: (record[0][1], record[0][0]),
        )
        self.ends: list[datetime] = []
        self.states: list[tuple[int | Decimal, int | Decimal, str, bool, bool]] = []
        incoming: dict[datetime, int] = {}
        outgoing: dict[datetime, int] = {}
        sources: set[datetime] = set()
        sinks: set[datetime] = set()
        branches = 0
        lower: int | Decimal = 0
        upper: int | Decimal = 0
        estimated = bounded = invalid = False

        def degree(node: datetime, inward: int, outward: int) -> None:
            nonlocal branches
            old_in, old_out = incoming.get(node, 0), outgoing.get(node, 0)
            branches -= int(old_in > 1 or old_out > 1)
            sources.discard(node)
            sinks.discard(node)
            new_in, new_out = old_in + inward, old_out + outward
            incoming[node], outgoing[node] = new_in, new_out
            branches += int(new_in > 1 or new_out > 1)
            if not new_in and new_out:
                sources.add(node)
            if new_in and not new_out:
                sinks.add(node)

        for period, _row, value, precision in self.rows:
            degree(period[0], 0, 1)
            degree(period[1], 1, 0)
            if value is None:
                invalid = True
            else:
                lower += value[0]
                upper += value[1]
            estimated |= precision == "estimated"
            bounded |= precision == "bounded"
            chain = not branches and sources == {start} and len(sinks) == 1
            self.ends.append(period[1])
            self.states.append(
                (
                    lower,
                    upper,
                    "estimated" if estimated else "bounded" if bounded else "exact",
                    chain,
                    invalid,
                )
            )

    def query(self, end: datetime) -> tuple[int, tuple] | None:
        count = bisect_right(self.ends, end)
        return (count, self.states[count - 1]) if count else None


class EntityTimeline:
    """One product partition's reusable metric, boundary and correction indexes."""

    def __init__(self, rows: Sequence[Observation], target: str, currency: str | None = None):
        self.target, self.currency = target, currency
        self.rows = list(rows)
        cumulative: list[tuple[Observation, Measurement]] = []
        periods: dict[tuple[datetime, datetime], tuple[Observation, Measurement | None]] = {}
        for row in rows:
            metric, period = _metric(row, target), _period(row)
            if target != "profit" and metric is not None and metric.semantics == "cumulative":
                cumulative.append((row, metric))
            if period is None or period[1] > row.observed_at:
                continue
            if target != "profit" and (metric is None or metric.semantics != "interval"):
                continue
            previous = periods.get(period)
            if previous is None or (row.available_at, row.observation_id) > (
                previous[0].available_at,
                previous[0].observation_id,
            ):
                periods[period] = row, metric
        self.periods: list[tuple] = []
        for period, (row, metric) in periods.items():
            value: tuple[int | Decimal, int | Decimal] | None
            if target == "profit":
                value = self._profit_value(row)
                precision = "exact"
            else:
                assert metric is not None
                value = metric.lower, metric.upper
                precision = metric.precision
            self.periods.append((period, row, value, precision))
        cumulative.sort(
            key=lambda pair: (pair[0].observed_at, pair[0].available_at, pair[0].observation_id)
        )
        self.cumulative = [row for row, _ in cumulative]
        self.measurements = [metric for _, metric in cumulative]
        self.timestamps = [row.observed_at for row in self.cumulative]
        self.reset_prefix = [0]
        for index, metric in enumerate(self.measurements):
            reset = index > 0 and metric.upper < self.measurements[index - 1].lower
            self.reset_prefix.append(self.reset_prefix[-1] + int(reset))
        self._prefix_start: datetime | None = None
        self._prefix: _PeriodPrefix | None = None

    def _profit_value(self, row: Observation) -> tuple[Decimal, Decimal] | None:
        # Resolve the latest period revision before validating its business fields.
        # Filtering incomplete revisions first would revive an older profit claim.
        if (
            row.attributes.get("currency") != self.currency
            or row.attributes.get("realized") is not True
            or row.attributes.get("costs_complete") is not True
        ):
            return None
        try:
            revenue = Decimal(str(row.attributes["realized_revenue"]))
            costs = row.attributes["realized_costs"]
            total = (
                sum((Decimal(str(value)) for value in costs.values()), Decimal(0))
                if isinstance(costs, dict)
                else Decimal(str(costs))
            )
            if not revenue.is_finite() or not total.is_finite() or total < 0:
                return None
            return revenue - total, revenue - total
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return None

    def _period_outcome(
        self, start: datetime, end: datetime, include_references: bool
    ) -> dict[str, Any] | None:
        profit = self.target == "profit"
        if self._prefix_start != start:
            self._prefix_start, self._prefix = start, _PeriodPrefix(self.periods, start)
        assert self._prefix is not None
        selected = self._prefix.query(end)
        if selected is None:
            return (
                {"status": "censored", "reason": "incomplete_realized_profit_coverage"}
                if profit
                else None
            )
        count, (lower, upper, precision, chain, invalid) = selected
        if profit and invalid:
            # Preserve the established ordering of malformed-cost versus gap
            # diagnostics on exceptional inputs; valid periods use prefix sums.
            fallback = _realized_profit(self.rows, start, end, self.currency)
            if not include_references:
                fallback.pop("evidence_refs", None)
            return fallback
        if not chain:
            return {
                "status": "censored",
                "reason": (
                    "realized_profit_period_gap_or_overlap" if profit else "interval_gap_or_overlap"
                ),
            }
        if self._prefix.ends[count - 1] != end:
            return {
                "status": "censored",
                "reason": (
                    "incomplete_realized_profit_coverage"
                    if profit
                    else "incomplete_interval_coverage"
                ),
            }
        result: dict[str, Any] = {
            "status": "measured",
            "lower": str(lower) if profit else int(lower),
            "upper": str(upper) if profit else int(upper),
            "precision": precision,
            "unit": self.currency if profit else "count",
        }
        if include_references:
            result["evidence_refs"] = [
                row.observation_id for _, row, _, _ in self._prefix.rows[:count]
            ]
        return result

    def outcome(
        self,
        start: datetime,
        end: datetime,
        max_gap: timedelta,
        *,
        include_references: bool = True,
    ) -> dict[str, Any]:
        interval = self._period_outcome(start, end, include_references)
        if interval is not None:
            return interval
        dates = self.timestamps
        upper = bisect_right(dates, end + max_gap)
        before_start, before_end = bisect_right(dates, start) - 1, bisect_right(dates, end) - 1
        after_start, after_end = bisect_left(dates, start), bisect_left(dates, end)
        if min(before_start, before_end) < 0 or max(after_start, after_end) >= upper:
            return {"status": "censored", "reason": "missing_boundary_observation"}
        # At the earliest eligible timestamp, the latest available revision wins.
        after_start = bisect_right(dates, dates[after_start]) - 1
        after_end = bisect_right(dates, dates[after_end]) - 1
        positions = before_start, after_start, before_end, after_end
        first, first_after, last, last_after = [self.cumulative[index] for index in positions]
        if any(
            gap > max_gap
            for gap in (
                start - first.observed_at,
                first_after.observed_at - start,
                end - last.observed_at,
                last_after.observed_at - end,
            )
        ):
            return {"status": "censored", "reason": "stale_boundary_observation"}
        if first_after.observed_at > last.observed_at:
            return {"status": "censored", "reason": "insufficient_boundary_history"}
        boundaries = first, first_after, last, last_after
        periods = {_counter_period(row, self.target) for row in boundaries}
        if None in periods or len(periods) != 1:
            return {"status": "censored", "reason": "unknown_or_changed_counter_period"}
        metrics = [self.measurements[index] for index in positions]
        initial, initial_upper, final, final_upper = metrics
        if len({metric.unit for metric in metrics}) != 1:
            return {"status": "censored", "reason": "incompatible_counter_units"}
        lo, hi = bisect_left(dates, first.observed_at), bisect_right(dates, last_after.observed_at)
        if (
            self.measurements[lo].upper < initial.lower
            or self.reset_prefix[hi] != self.reset_prefix[lo + 1]
            or final_upper.upper < initial.lower
        ):
            return {"status": "censored", "reason": "counter_correction_or_reset"}
        precisions = {metric.precision for metric in metrics}
        exact = first.observed_at == first_after.observed_at == start and (
            last.observed_at == last_after.observed_at == end
        )
        result = {
            "status": "measured",
            "lower": max(0, final.lower - initial_upper.upper),
            "upper": final_upper.upper - initial.lower,
            "precision": (
                "estimated"
                if "estimated" in precisions
                else "exact"
                if precisions == {"exact"} and exact
                else "bounded"
            ),
            "unit": final.unit,
            "start_bracket": [first.observed_at.isoformat(), first_after.observed_at.isoformat()],
            "end_bracket": [last.observed_at.isoformat(), last_after.observed_at.isoformat()],
        }
        if include_references:
            result["evidence_refs"] = sorted({row.observation_id for row in boundaries})
        return result

"""Point-in-time product assessment over bounded evidence partitions.

The caller selects immutable history partitions before invoking this engine. Polars
performs counter window calculations in native code. The remaining Python work is
bounded by product/entity count and produces small inspectable review records.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

import polars as pl

from .contracts import AssessmentPolicy, Observation, Product, ProductLink, digest, utc
from .identity_rules import resolve_links

METRIC_ALIASES = {
    "view_count": ("view_count", "views"),
    "units_sold": ("units_sold", "sold_count"),
    "purchase_intent_count": ("purchase_intent_count",),
}


@dataclass(frozen=True)
class AssessmentIdentity:
    """One globally resolved ownership snapshot shared by bounded partitions.

    Resolving before partitioning retains newer owners outside a product batch.
    The grouped immutable view also avoids regrouping all global links for each
    partition. Its scope is checked at the analytical boundary before reuse.
    """

    cutoff: datetime
    synthetic: bool
    min_confidence: float
    owners: Mapping[str, ProductLink]
    by_product: Mapping[str, tuple[ProductLink, ...]]

    @classmethod
    def resolve(
        cls,
        products: Sequence[Product],
        links: Sequence[ProductLink],
        as_of: datetime,
        synthetic: bool,
        min_confidence: float,
    ) -> AssessmentIdentity:
        cutoff = utc(as_of)
        other_namespace_ids = {
            product.product_id for product in products if product.synthetic != synthetic
        }
        owners = resolve_links(
            [link for link in links if link.product_id not in other_namespace_ids],
            cutoff,
            min_confidence=min_confidence,
        )
        grouped: dict[str, list[ProductLink]] = defaultdict(list)
        for link in owners.values():
            grouped[link.product_id].append(link)
        return cls(
            cutoff,
            synthetic,
            min_confidence,
            MappingProxyType(owners),
            MappingProxyType(
                {
                    key: tuple(sorted(values, key=lambda link: link.entity_key))
                    for key, values in grouped.items()
                }
            ),
        )

    def validate(self, cutoff: datetime, synthetic: bool, min_confidence: float) -> None:
        if (self.cutoff, self.synthetic, self.min_confidence) != (
            cutoff,
            synthetic,
            min_confidence,
        ):
            raise ValueError("Prepared assessment identity has a different scope")


def _unknown(reason: str, status: str = "unknown") -> dict[str, Any]:
    return {"score": None, "status": status, "reasons": [reason], "evidence_refs": []}


def _metric_windows(
    observations: Sequence[Observation], metric: str
) -> dict[str, list[dict[str, Any]]]:
    rows = []
    for observation in observations:
        aliases = METRIC_ALIASES.get(metric, (metric,))
        value = next(
            (observation.metrics[key] for key in aliases if key in observation.metrics), None
        )
        if value is None:
            continue
        rows.append(
            {
                "entity_key": observation.entity_key,
                "observation_id": observation.observation_id,
                "observed_at": observation.observed_at,
                "available_at": observation.available_at,
                "lower": value.lower,
                "upper": value.upper,
                "precision": value.precision,
                "semantics": value.semantics,
                "counter_scope": str(observation.attributes.get("counter_scope", "unspecified")),
                "counter_period": _counter_period(observation, metric),
                "unit": value.unit,
            }
        )
    if not rows:
        return {}
    frame = pl.from_dicts(rows).sort(
        ["entity_key", "observed_at", "available_at", "observation_id"]
    )
    frame = frame.unique(subset=["entity_key", "observed_at"], keep="last", maintain_order=True)
    frame = (
        frame.with_columns(
            pl.col("lower").shift(1).over("entity_key").alias("previous_lower"),
            pl.col("upper").shift(1).over("entity_key").alias("previous_upper"),
            pl.col("semantics").shift(1).over("entity_key").alias("previous_semantics"),
            pl.col("precision").shift(1).over("entity_key").alias("previous_precision"),
            pl.col("counter_scope").shift(1).over("entity_key").alias("previous_scope"),
            pl.col("counter_period").shift(1).over("entity_key").alias("previous_period"),
            pl.col("unit").shift(1).over("entity_key").alias("previous_unit"),
            (pl.col("observed_at") - pl.col("observed_at").shift(1).over("entity_key"))
            .dt.total_milliseconds()
            .truediv(3_600_000)
            .alias("interval_hours"),
        )
        .with_columns(
            (
                (pl.col("semantics") == "cumulative")
                & (pl.col("previous_semantics") == "cumulative")
                & (pl.col("counter_scope") == pl.col("previous_scope"))
                & pl.col("counter_period").is_not_null()
                & (pl.col("counter_period") == pl.col("previous_period"))
                & (pl.col("unit") == pl.col("previous_unit"))
                & (pl.col("precision") != "estimated")
                & (pl.col("previous_precision") != "estimated")
                & (pl.col("interval_hours") > 0)
                & (pl.col("upper") >= pl.col("previous_lower"))
            )
            .fill_null(False)
            .alias("valid_interval"),
            (pl.col("upper") < pl.col("previous_lower"))
            .fill_null(False)
            .alias("counter_correction"),
        )
        .with_columns(
            pl.when(pl.col("valid_interval"))
            .then((pl.col("lower") - pl.col("previous_upper")).clip(lower_bound=0))
            .otherwise(None)
            .alias("delta_lower"),
            pl.when(pl.col("valid_interval"))
            .then((pl.col("upper") - pl.col("previous_lower")).clip(lower_bound=0))
            .otherwise(None)
            .alias("delta_upper"),
        )
        .with_columns(
            (pl.col("delta_lower") / pl.col("interval_hours")).alias("growth_per_hour"),
            (pl.col("delta_upper") / pl.col("interval_hours")).alias("growth_upper_per_hour"),
        )
    )
    output: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frame.iter_rows(named=True):
        output[row["entity_key"]].append(row)
    return dict(output)


def _counter_period(observation: Observation, metric: str) -> str | None:
    if metric != "units_sold":
        return str(observation.attributes.get("view_count_period", "lifetime"))
    # Sales counters are not intrinsically lifetime counters. Explicit period
    # declarations override generic counter scope, including an old lifetime hint.
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


def _attention(
    evidence: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
    *,
    clock: dict[str, datetime | None] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if clock is not None:
        clock["age_anchor"] = None
    videos = [row for row in evidence if row.entity_type == "video"]
    windows = _metric_windows(videos, "view_count")
    latest_observations = {
        row.entity_key: row
        for row in sorted(videos, key=lambda row: (row.observed_at, row.available_at))
    }
    features: dict[str, Any] = {
        "view_count": None,
        "growth_per_hour": None,
        "growth_upper_per_hour": None,
        "acceleration_per_hour_squared": None,
        "video_count": len(latest_observations),
        "creator_count": 0,
        "creator_concentration": None,
        "independent_spread_concentration": None,
        "verified_creative_count": 0,
        "repeated_creative_count": 0,
        "positive_interval_fraction": None,
        "counter_corrections": 0,
        "age_hours": None,
    }
    if not windows:
        return _unknown("No eligible video-view observations"), features
    usable = []
    latest_counts = []
    creators = set()
    interval_growth: list[bool] = []
    accelerations = []
    published_ages = []
    published_timestamps = []
    refs: list[str] = []
    for key, timeline in windows.items():
        features["counter_corrections"] += sum(row["counter_correction"] for row in timeline)
        latest = timeline[-1]
        observation = latest_observations[key]
        age = (as_of - latest["observed_at"]).total_seconds() / 3600
        if age > policy.max_age_hours:
            continue
        if latest["precision"] == "exact" and latest["semantics"] == "cumulative":
            latest_counts.append(latest["lower"])
        else:
            latest_counts.append(None)
        if len(timeline) < policy.min_observations or not latest["valid_interval"]:
            continue
        usable.append(latest)
        refs.extend(row["observation_id"] for row in timeline)
        creator = observation.attributes.get("creator_id") or observation.account_id
        if creator:
            creators.add(str(creator))
        valid = [row for row in timeline if row["valid_interval"]]
        interval_growth.extend(row["delta_lower"] > 0 for row in valid)
        if len(timeline) > 2 and timeline[-2]["valid_interval"]:
            previous = timeline[-2]
            accelerations.append(
                (latest["growth_per_hour"] - previous["growth_per_hour"]) / latest["interval_hours"]
            )
        if observation.published_at is not None:
            published_timestamps.append(utc(observation.published_at))
            published_ages.append(
                max(0.0, (as_of - observation.published_at).total_seconds() / 3600)
            )
    if latest_counts and all(count is not None for count in latest_counts):
        features["view_count"] = sum(latest_counts)
    if not usable:
        return _unknown(
            "Insufficient fresh comparable observations", "insufficient_history"
        ), features
    growth = sum(row["growth_per_hour"] for row in usable)
    upper_growth = sum(row["growth_upper_per_hour"] for row in usable)
    creator_growth: dict[str, float] = defaultdict(float)
    creative_growth: dict[str, float] = defaultdict(float)
    fingerprints: set[str] = set()
    fingerprinted_count = 0
    for row in usable:
        observation = latest_observations[row["entity_key"]]
        creator = observation.attributes.get("creator_id") or observation.account_id
        if creator:
            creator_growth[str(creator)] += row["growth_per_hour"]
        fingerprint = observation.attributes.get("creative_fingerprint")
        creative_key = observation.entity_key
        if fingerprint:
            fingerprints.add(str(fingerprint))
            fingerprinted_count += 1
            creative_key = f"verified:{fingerprint}"
        creative_growth[creative_key] += row["growth_per_hour"]
    known_growth = sum(creator_growth.values())
    concentration = (
        sum((value / known_growth) ** 2 for value in creator_growth.values())
        if known_growth > 0
        else None
    )
    creative_concentration = (
        sum((value / growth) ** 2 for value in creative_growth.values()) if growth > 0 else None
    )
    independent_concentration = (
        max(concentration, creative_concentration)
        if concentration is not None and creative_concentration is not None
        else None
    )
    features.update(
        {
            "growth_per_hour": growth,
            "growth_upper_per_hour": upper_growth,
            "acceleration_per_hour_squared": sum(accelerations) if accelerations else None,
            "creator_count": len(creators),
            "creator_concentration": concentration,
            "independent_spread_concentration": independent_concentration,
            "verified_creative_count": len(fingerprints),
            "repeated_creative_count": fingerprinted_count - len(fingerprints),
            "positive_interval_fraction": sum(interval_growth) / len(interval_growth)
            if interval_growth
            else None,
            "age_hours": sorted(published_ages)[len(published_ages) // 2]
            if published_ages
            else None,
            "usable_video_count": len(usable),
            "interval_hours": sorted({row["interval_hours"] for row in usable}),
        }
    )
    if clock is not None and published_timestamps:
        clock["age_anchor"] = sorted(published_timestamps, reverse=True)[
            len(published_timestamps) // 2
        ]
    reasons = [
        "Growth uses actual collection intervals; source counters do not identify exact view timing"
    ]
    if features["counter_corrections"]:
        reasons.append("Counter corrections are excluded from growth intervals")
    if growth != upper_growth:
        reasons.append("Growth is bounded; ranking uses its conservative lower bound")
    if not creators:
        reasons.append("Creator identity unavailable; independent creator spread is unknown")
    if features["repeated_creative_count"]:
        reasons.append(
            "Verified repeated creatives retain exposure counts "
            "but do not add independent diffusion"
        )
    return {
        "score": None,
        "status": "observed" if growth == upper_growth else "bounded",
        "reasons": reasons,
        "evidence_refs": sorted(set(refs)),
    }, features


def _cohort(age_hours: float | None) -> str:
    if age_hours is None:
        return "unknown_age"
    return "under_1d" if age_hours < 24 else "1_to_7d" if age_hours < 168 else "over_7d"


def _fresh_latest(
    evidence: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
) -> list[Observation]:
    latest: dict[str, Observation] = {}
    for observation in sorted(
        evidence, key=lambda row: (row.observed_at, row.available_at, row.observation_id)
    ):
        latest[observation.entity_key] = observation
    return [
        row
        for row in latest.values()
        if (as_of - row.observed_at).total_seconds() <= policy.max_age_hours * 3600
    ]


def _declared_sales_intervals(
    latest: Sequence[dict[str, Any]],
    offers: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
) -> list[dict[str, Any]]:
    by_id = {row.observation_id: row for row in offers}
    intervals = []
    for row in latest:
        if row["semantics"] != "interval" or row["precision"] == "estimated":
            continue
        observation = by_id[row["observation_id"]]
        try:
            start = utc(observation.attributes["period_start"])
            end = utc(observation.attributes["period_end"])
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
        hours = (end - start).total_seconds() / 3600
        if (
            hours <= 0
            or end > observation.observed_at
            or (as_of - end).total_seconds() > policy.max_age_hours * 3600
        ):
            continue
        intervals.append(
            {
                "entity_key": row["entity_key"],
                "lower": row["lower"],
                "upper": row["upper"],
                "hours": hours,
                "ending_at": end.isoformat(),
                "growth_per_hour": row["lower"] / hours,
                "growth_upper_per_hour": row["upper"] / hours,
                "origin": "declared_source_period",
            }
        )
    return intervals


def _demand(
    evidence: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
) -> tuple[dict[str, Any], dict[str, Any]]:
    offers = [row for row in evidence if row.entity_type == "offer"]
    fresh = {row.entity_key: row for row in _fresh_latest(offers, as_of, policy)}
    windows = _metric_windows(offers, "units_sold")
    latest = [
        rows[-1]
        for key, rows in windows.items()
        if key in fresh
        and (as_of - rows[-1]["observed_at"]).total_seconds() <= policy.max_age_hours * 3600
    ]
    features: dict[str, Any] = {
        "units_sold": None,
        "sold_lower": None,
        "sold_upper": None,
        "sold_delta_lower": None,
        "sold_delta_upper": None,
        "sold_period_seconds": None,
        "sold_growth_per_hour": None,
        "sold_growth_upper_per_hour": None,
        "sold_counter_periods": {},
        "sales_baseline_exclusion_reason": (
            "incomplete_exact_transaction_coverage"
            if windows
            else "missing_exact_transaction_counts"
        ),
    }
    if not latest:
        proxy_rows = [
            row
            for row in _fresh_latest(evidence, as_of, policy)
            if "purchase_intent_count" in row.metrics
        ]
        if proxy_rows:
            return {
                "score": None,
                "status": "proxy",
                "evidence_refs": [row.observation_id for row in proxy_rows],
                "reasons": [
                    "Purchase-intent labels are observed proxies, not transactions; "
                    "no calibrated demand score"
                ],
                "intent_lower": sum(
                    row.metrics["purchase_intent_count"].lower for row in proxy_rows
                ),
                "intent_upper": sum(
                    row.metrics["purchase_intent_count"].upper for row in proxy_rows
                ),
            }, features
        return _unknown("No compatible transaction or intent evidence"), features
    measured = [
        row
        for row in latest
        if row["precision"] != "estimated" and row["semantics"] == "cumulative"
    ]
    declared = _declared_sales_intervals(latest, offers, as_of, policy)
    if not measured and not declared:
        return {
            "score": None,
            "status": "estimated"
            if any(row["precision"] == "estimated" for row in latest)
            else "unknown",
            "evidence_refs": [row["observation_id"] for row in latest],
            "reasons": [
                "Estimated or undocumented-period counters cannot establish "
                "measured transaction growth"
            ],
        }, features
    if measured:
        features["sold_lower"] = sum(row["lower"] for row in measured)
        features["sold_upper"] = sum(row["upper"] for row in measured)
        features["sold_counter_periods"] = {
            row["entity_key"]: row["counter_period"] for row in measured
        }
    periods = {row["counter_period"] for row in measured}
    if (
        len(measured) == len(latest) == len(windows)
        and all(row["precision"] == "exact" for row in measured)
        and None not in periods
    ):
        features["sales_baseline_exclusion_reason"] = (
            None if len(periods) == 1 else "incompatible_sales_counter_periods"
        )
    if (
        all(row["precision"] == "exact" for row in measured)
        and len(periods) == 1
        and None not in periods
    ):
        features["units_sold"] = features["sold_lower"]
    intervals = [
        {
            "entity_key": row["entity_key"],
            "lower": row["delta_lower"],
            "upper": row["delta_upper"],
            "hours": row["interval_hours"],
            "ending_at": row["observed_at"].isoformat(),
            "growth_per_hour": row["growth_per_hour"],
            "growth_upper_per_hour": row["growth_upper_per_hour"],
            "origin": "observed_counter_difference",
        }
        for row in measured
        if row["valid_interval"]
    ]
    intervals.extend(declared)
    score = None
    if intervals:
        lower_rate = sum(row["growth_per_hour"] for row in intervals)
        upper_rate = sum(row["growth_upper_per_hour"] for row in intervals)
        features["sold_growth_per_hour"] = lower_rate
        features["sold_growth_upper_per_hour"] = upper_rate
        period_signatures = {(row["ending_at"], round(row["hours"], 9)) for row in intervals}
        if len(period_signatures) == 1:
            features["sold_delta_lower"] = sum(row["lower"] for row in intervals)
            features["sold_delta_upper"] = sum(row["upper"] for row in intervals)
            features["sold_period_seconds"] = round(intervals[0]["hours"] * 3600, 6)
        score = round(min(100.0, 20 * math.log1p(lower_rate)), 6)
    bounded = any(row["precision"] == "bounded" for row in latest) or any(
        row["lower"] != row["upper"] for row in intervals
    )
    reasons = [
        "Transaction counters describe observed offers only; overlapping listings "
        "and unobserved sellers limit coverage",
        "Demand score is the versioned heuristic "
        "min(100, 20 * log1p(conservative observed units/hour)); it is not a forecast",
        "Counter changes are collection-interval changes, not exact daily sales "
        "or precise purchase times",
    ]
    if len(intervals) != len(latest):
        reasons.append("Some counters lack a comparable fresh interval")
    if any(
        row["counter_period"] is None or row["counter_period"] != row["previous_period"]
        for row in measured
    ):
        reasons.append(
            "Unknown or changed sales-counter periods cannot establish transaction growth"
        )
    return {
        "score": score,
        "status": "bounded" if bounded else "observed",
        "reasons": reasons,
        "evidence_refs": sorted({row.observation_id for row in offers if row.entity_key in fresh}),
        "intervals": intervals,
    }, features


def _price_quote(observation: Observation) -> tuple[dict[str, Any] | None, str | None]:
    attributes = observation.attributes
    currency = str(attributes.get("currency", "")).upper()
    if len(currency) != 3 or not currency.isalpha():
        return None, "missing_or_invalid_currency"
    if str(attributes.get("price_currency", currency)).upper() != currency:
        return None, "conflicting_price_currency"
    precision = attributes.get("price_precision", "exact")
    if not isinstance(precision, str):
        return None, "invalid_price_precision"
    bounded = "price_lower" in attributes or "price_upper" in attributes
    try:
        if bounded:
            lower = Decimal(str(attributes["price_lower"]))
            upper = Decimal(str(attributes["price_upper"]))
            if "price_precision" not in attributes:
                precision = "bounded"
            if precision == "rounded":
                precision = "bounded"
        elif precision == "exact":
            lower = upper = Decimal(str(attributes["price"]))
        else:
            return None, "uncertain_price_requires_explicit_bounds"
    except (KeyError, InvalidOperation):
        return None, "missing_or_invalid_price"
    if (
        precision not in {"exact", "bounded"}
        or not lower.is_finite()
        or not upper.is_finite()
        or lower < 0
        or upper < lower
        or (precision == "exact" and lower != upper)
    ):
        return None, "invalid_or_estimated_price_bounds"
    return {
        "lower": lower,
        "upper": upper,
        "precision": precision,
        "currency": currency,
        "market": observation.market.upper() if observation.market else None,
        "source": observation.source,
        "entity_key": observation.entity_key,
        "observed_at": observation.observed_at,
        "observation_id": observation.observation_id,
        "price_basis": str(attributes.get("price_basis", "per_offer")),
        "variant_id": attributes.get("variant_id"),
    }, None


def _price_change(first: dict[str, Any], latest: dict[str, Any]) -> dict[str, Any]:
    lower = latest["lower"] - first["upper"]
    upper = latest["upper"] - first["lower"]
    direction = (
        "increase"
        if lower > 0
        else "decrease"
        if upper < 0
        else "unchanged"
        if lower == upper == 0
        else "uncertain"
    )
    return {
        "change_lower": str(lower),
        "change_upper": str(upper),
        "direction": direction,
        "change_precision": "exact"
        if first["precision"] == latest["precision"] == "exact"
        else "bounded",
        "relative_change_lower": str(latest["lower"] / first["upper"] - 1)
        if first["upper"] > 0
        else None,
        "relative_change_upper": str(latest["upper"] / first["lower"] - 1)
        if first["lower"] > 0
        else None,
        "interval_hours": (latest["observed_at"] - first["observed_at"]).total_seconds() / 3600,
        "evidence_refs": [first["observation_id"], latest["observation_id"]],
    }


def _price_evolution(
    evidence: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
) -> dict[str, Any]:
    timelines: dict[str, list[Observation]] = defaultdict(list)
    for observation in evidence:
        if observation.entity_type == "offer" and any(
            field in observation.attributes for field in ("price", "price_lower", "price_upper")
        ):
            timelines[observation.entity_key].append(observation)
    summaries = []
    refs: set[str] = set()
    for entity_key, timeline in sorted(timelines.items()):
        versions: dict[datetime, Observation] = {}
        for observation in sorted(
            timeline, key=lambda row: (row.observed_at, row.available_at, row.observation_id)
        ):
            versions[observation.observed_at] = observation
        ordered = list(versions.values())
        last_observation = ordered[-1]
        latest, reason = _price_quote(last_observation)
        common: dict[str, Any] = {
            "entity_key": entity_key,
            "source": last_observation.source,
            "market": last_observation.market,
            "status": "unknown",
            "observed_quote_count": len(ordered),
            "revision_count": len(timeline) - len(ordered),
            "evidence_refs": [last_observation.observation_id],
        }
        refs.add(last_observation.observation_id)
        if latest is None:
            summaries.append({**common, "reason": reason})
            continue
        common["currency"] = latest["currency"]
        common["market"] = latest["market"]
        common["latest_price"] = {
            "lower": str(latest["lower"]),
            "upper": str(latest["upper"]),
            "precision": latest["precision"],
            "observed_at": latest["observed_at"].isoformat(),
        }
        if (as_of - latest["observed_at"]).total_seconds() > policy.max_age_hours * 3600:
            summaries.append({**common, "reason": "stale_latest_quote"})
            continue
        if latest["market"] is None:
            summaries.append({**common, "reason": "unknown_market_scope"})
            continue
        first: dict[str, Any] | None = None
        previous: dict[str, Any] | None = None
        comparable_count = 0
        directions: dict[str, int] = defaultdict(int)
        latest_change: dict[str, Any] = {}
        excluded = 0
        for observation in ordered:
            quote, _ = _price_quote(observation)
            if quote is None or any(
                quote[field] != latest[field]
                for field in (
                    "source",
                    "entity_key",
                    "currency",
                    "market",
                    "price_basis",
                    "variant_id",
                )
            ):
                excluded += 1
                continue
            if first is None:
                first = quote
            if previous is not None:
                latest_change = _price_change(previous, quote)
                directions[latest_change["direction"]] += 1
            previous = quote
            comparable_count += 1
        if comparable_count < 2 or first is None:
            summaries.append(
                {**common, "reason": "insufficient_comparable_quotes", "excluded_quotes": excluded}
            )
            continue
        summary = {
            **common,
            **_price_change(first, latest),
            "status": "observed"
            if first["precision"] == latest["precision"] == "exact"
            else "bounded",
            "first_price": {
                "lower": str(first["lower"]),
                "upper": str(first["upper"]),
                "precision": first["precision"],
                "observed_at": first["observed_at"].isoformat(),
            },
            "comparable_quote_count": comparable_count,
            "excluded_quotes": excluded,
            "latest_change": latest_change,
            "observed_increases": directions["increase"],
            "observed_decreases": directions["decrease"],
            "uncertain_changes": directions["uncertain"],
        }
        refs.update(summary["evidence_refs"])
        refs.update(latest_change["evidence_refs"])
        summaries.append(summary)
    known = [row for row in summaries if row["status"] != "unknown"]
    return {
        "status": "unknown"
        if not known
        else "bounded"
        if any(row["status"] == "bounded" for row in known)
        else "observed",
        "offers": summaries,
        "evidence_refs": sorted(refs),
        "reasons": [
            "Price changes compare like offer, source, known market, currency, "
            "variant, and price basis",
            "Bounds preserve quote uncertainty; source revisions at one observation "
            "time are not extra price changes",
            "Quote intervals show observed changes, not exact change timing "
            "or causal evidence of a price war",
        ],
    }


def _competition(
    evidence: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
) -> dict[str, Any]:
    latest = _fresh_latest(evidence, as_of, policy)
    offers = [row for row in latest if row.entity_type == "offer"]
    ads = [row for row in latest if row.entity_type in {"ad", "commercial_content"}]
    creatives = [row for row in latest if row.entity_type in {"video", "ad", "commercial_content"}]
    fingerprinted = [
        row
        for row in creatives
        if isinstance(row.attributes.get("creative_fingerprint"), str)
        and row.attributes["creative_fingerprint"].strip()
    ]
    fingerprints = {str(row.attributes["creative_fingerprint"]) for row in fingerprinted}
    prices = _price_evolution(evidence, as_of, policy)
    extra = {
        "observed_creative_count": len(fingerprints) if fingerprinted else None,
        "creative_coverage": {
            "fingerprinted_entities": len(fingerprinted),
            "unknown_fingerprint_entities": len(creatives) - len(fingerprinted),
            "scope": "explicit_fingerprints_only_not_inferred_from_ad_ids",
        },
        "price_evolution": prices,
    }
    if not offers and not ads:
        return {**_unknown("No scoped competitive evidence"), **extra}
    seller_ids = {
        str(row.attributes["seller_id"]) for row in offers if row.attributes.get("seller_id")
    }
    advertiser_ids = {
        str(row.attributes["advertiser_id"]) for row in ads if row.attributes.get("advertiser_id")
    }
    by_currency: dict[str, list[str]] = defaultdict(list)
    for row in offers:
        quote, _ = _price_quote(row)
        if quote is not None:
            by_currency[quote["currency"]].extend([str(quote["lower"]), str(quote["upper"])])
    price_ranges = {
        currency: {"lower": str(min(map(Decimal, prices))), "upper": str(max(map(Decimal, prices)))}
        for currency, prices in sorted(by_currency.items())
    }
    observed_competitors = len(seller_ids) + len(advertiser_ids)
    return {
        "score": round(100 / (1 + observed_competitors), 6) if observed_competitors else None,
        "status": "proxy",
        "observed_offer_count": len(offers),
        "observed_ad_count": len(ads),
        "observed_seller_count": len(seller_ids),
        "observed_advertiser_count": len(advertiser_ids),
        "price_ranges": price_ranges,
        **extra,
        "market_coverage": "unknown",
        "reasons": [
            "Higher score means fewer observed named sellers/advertisers; "
            "this is a coverage-dependent proxy, not market saturation",
            "Missing sellers, ad launch dates, and market totals remain unknown; "
            "first observation is not first shown",
        ],
        "evidence_refs": sorted(
            {row.observation_id for row in offers + ads + fingerprinted}
            | set(prices["evidence_refs"])
        ),
    }


def _feasibility(
    evidence: Sequence[Observation],
    as_of: datetime,
    policy: AssessmentPolicy,
) -> dict[str, Any]:
    required = (
        "sale_price",
        "unit_cost",
        "shipping_cost",
        "payment_fee",
        "ad_cost",
        "tax_cost",
        "return_rate",
        "return_cost",
    )
    costs = [row for row in _fresh_latest(evidence, as_of, policy) if row.entity_type == "cost"]
    scenarios: list[dict[str, Any]] = []
    failures = []
    for row in costs:
        attributes = row.attributes
        missing = [name for name in (*required, "currency") if attributes.get(name) is None]
        if missing:
            failures.append(f"{row.entity_key}: missing {', '.join(missing)}")
            continue
        try:
            amounts = {name: Decimal(str(attributes[name])) for name in required}
        except InvalidOperation:
            failures.append(f"{row.entity_key}: invalid decimal cost")
            continue
        currency = str(attributes["currency"]).upper()
        if (
            len(currency) != 3
            or not currency.isalpha()
            or any(not value.is_finite() or value < 0 for value in amounts.values())
            or amounts["sale_price"] <= 0
            or amounts["return_rate"] > 1
        ):
            failures.append(f"{row.entity_key}: invalid currency or cost assumptions")
            continue
        currency_keys = [name for name in attributes if name.endswith("_currency")]
        if any(str(attributes[name]).upper() != currency for name in currency_keys):
            failures.append(
                f"{row.entity_key}: mixed currencies require an explicit conversion scenario"
            )
            continue
        margin = (
            amounts["sale_price"]
            - sum(
                amounts[name]
                for name in ("unit_cost", "shipping_cost", "payment_fee", "ad_cost", "tax_cost")
            )
            - amounts["return_rate"] * amounts["return_cost"]
        )
        margin_fraction = margin / amounts["sale_price"]
        scenarios.append(
            {
                "observation_id": row.observation_id,
                "currency": currency,
                "assumptions": {name: str(value) for name, value in amounts.items()},
                "contribution_margin": str(margin),
                "margin_fraction": str(margin_fraction),
                "score": float(max(Decimal(0), min(Decimal(1), margin_fraction)) * 100),
            }
        )
    if not scenarios:
        result = _unknown("Complete comparable cost assumptions unavailable")
        result["reasons"].extend(failures)
        return result
    # Multiple viable scenarios are a sensitivity range, not cherry-picked profit.
    return {
        "score": min(row["score"] for row in scenarios),
        "status": "scenario",
        "scenarios": scenarios,
        "reasons": [
            "Contribution margin is a stated cost scenario, not realized profitability; "
            "score uses the most conservative complete scenario",
            *failures,
        ],
        "evidence_refs": sorted(row["observation_id"] for row in scenarios),
    }


def _score_attention(rows: list[dict[str, Any]]) -> None:
    candidates = [row for row in rows if row["features"]["growth_per_hour"] is not None]
    if not candidates:
        return
    frame = pl.from_dicts(
        [
            {
                "product_id": row["product_id"],
                "growth": row["features"]["growth_per_hour"],
                "cohort": (
                    f"{row['market'] or 'unspecified'}:{_cohort(row['features']['age_hours'])}"
                ),
            }
            for row in candidates
        ]
    ).with_columns(
        (
            (pl.col("growth").rank(method="average").over("cohort") - 0.5) / pl.len().over("cohort")
        ).alias("percentile")
    )
    percentiles = {row["product_id"]: row["percentile"] for row in frame.iter_rows(named=True)}
    for row in candidates:
        features = row["features"]
        velocity = features["growth_per_hour"]
        percentile = percentiles[row["product_id"]]
        concentration = features["independent_spread_concentration"]
        persistence = features["positive_interval_fraction"]
        acceleration = features["acceleration_per_hour_squared"]
        components = {"cohort_growth_percentile": (percentile, 0.50)}
        if concentration is not None:
            components["creator_spread"] = (1 - concentration, 0.25)
        if persistence is not None:
            components["positive_interval_fraction"] = (persistence, 0.15)
        if acceleration is not None:
            components["positive_acceleration"] = (
                min(1.0, max(0.0, acceleration) / max(1.0, velocity)),
                0.10,
            )
        score = (
            100
            * sum(value * weight for value, weight in components.values())
            / sum(weight for _, weight in components.values())
        )
        row["attention"]["score"] = round(score, 6) if velocity > 0 else 0.0
        features["cohort_growth_percentile"] = percentile
        row["attention"]["components"] = {
            name: {"value": value, "weight": weight} for name, (value, weight) in components.items()
        }
        row["attention"]["reasons"].append(
            "Attention score combines within-age/market-cohort growth rank, known creator "
            "spread, persistence, and available acceleration; "
            "available component weights are renormalized"
        )


def assess(
    observations: Sequence[Observation],
    products: Sequence[Product],
    links: Sequence[ProductLink],
    as_of: datetime,
    synthetic: bool = False,
    policy: AssessmentPolicy | None = None,
    *,
    normalize: bool = True,
    identity: AssessmentIdentity | None = None,
    age_anchors: dict[str, datetime | None] | None = None,
) -> list[dict[str, Any]]:
    """Assess products from evidence and identity decisions known at ``as_of``.

    This is an observational heuristic, not a sales prediction. Products with an
    unknown weighted dimension remain unranked unless renormalization is explicit.
    """
    cutoff = utc(as_of)
    policy = policy or AssessmentPolicy()
    product_map = {
        product.product_id: product
        for product in products
        if product.synthetic == synthetic and utc(product.created_at) <= cutoff
    }
    identity = identity or AssessmentIdentity.resolve(
        products, links, cutoff, synthetic, policy.min_link_confidence
    )
    identity.validate(cutoff, synthetic, policy.min_link_confidence)
    link_map = identity.owners
    grouped: dict[str, list[Observation]] = defaultdict(list)
    excluded: dict[str, int] = defaultdict(int)
    since = cutoff - timedelta(days=policy.lookback_days)
    seen: set[str] = set()
    for observation in sorted(
        observations, key=lambda row: (row.observed_at, row.available_at, row.observation_id)
    ):
        if observation.synthetic != synthetic or utc(observation.available_at) > cutoff:
            continue
        if not since <= utc(observation.observed_at) <= cutoff:
            continue
        if observation.observation_id in seen:
            continue
        seen.add(observation.observation_id)
        link = link_map.get(observation.entity_key)
        if link is None or link.product_id not in product_map:
            continue
        product = product_map[link.product_id]
        if product.market and (observation.market or "").upper() != product.market.upper():
            excluded[product.product_id] += 1
            continue
        grouped[product.product_id].append(observation)
    rows = []
    for product_id, product in sorted(product_map.items()):
        evidence = grouped[product_id]
        resolved = identity.by_product.get(product_id, ())
        clock: dict[str, datetime | None] | None = {} if age_anchors is not None else None
        attention, features = _attention(evidence, cutoff, policy, clock=clock)
        if age_anchors is not None:
            assert clock is not None
            age_anchors[product_id] = clock["age_anchor"]
        demand, demand_features = _demand(evidence, cutoff, policy)
        features.update(demand_features)
        rows.append(
            {
                "product_id": product_id,
                "name": product.name,
                "market": product.market,
                "score": None,
                "as_of": cutoff.isoformat(),
                "synthetic": synthetic,
                "policy_version": policy.version,
                "identity": {
                    "version": digest([link.model_dump(mode="json") for link in resolved]),
                    "decision_refs": sorted(
                        link.decision_id for link in resolved if link.decision_id
                    ),
                },
                "features": features,
                "attention": attention,
                "demand": demand,
                "competition": _competition(evidence, cutoff, policy),
                "feasibility": _feasibility(evidence, cutoff, policy),
                "evidence_refs": sorted({row.observation_id for row in evidence}),
                "coverage": {
                    "observations": len(evidence),
                    "entities": len({row.entity_key for row in evidence}),
                    "market_excluded_observations": excluded[product_id],
                    "scope": "observed_candidates_only",
                    "global_coverage": "unknown",
                },
                "reasons": [],
            }
        )
    return normalize_assessments(rows, policy) if normalize else rows


def normalize_assessments(
    assessments: Sequence[dict[str, Any]],
    policy: AssessmentPolicy | None = None,
) -> list[dict[str, Any]]:
    """Normalize summaries globally after bounded per-product evidence batches.

    Call ``assess(..., normalize=False)`` for each bounded partition, concatenate
    its small product summaries, then invoke this function once. Normalizing each
    partition separately would create shard-dependent rankings.
    """
    policy = policy or AssessmentPolicy()
    rows = deepcopy(list(assessments))
    for row in rows:
        if row["policy_version"] != policy.version:
            raise ValueError("Assessment summaries and normalization policy versions differ")
        row["score"] = None
        row["reasons"] = []
    _score_attention(rows)
    for row in rows:
        known = [
            (row[name]["score"], weight) for name, weight in policy.weights.items() if weight > 0
        ]
        if policy.missing_data == "exclude" and any(score is None for score, _ in known):
            row["reasons"].append("A required assessment dimension is unknown")
        else:
            eligible = [(score, weight) for score, weight in known if score is not None]
            if eligible:
                row["score"] = round(
                    sum(score * weight for score, weight in eligible)
                    / sum(weight for _, weight in eligible),
                    6,
                )
            if len(eligible) != len(known):
                row["reasons"].append(
                    "Known dimensions were explicitly renormalized; "
                    "missing dimensions remain unknown"
                )
        row["ranking_policy"] = {"weights": policy.weights, "missing_data": policy.missing_data}
    return sorted(
        rows, key=lambda row: (row["score"] is None, -(row["score"] or 0), row["product_id"])
    )

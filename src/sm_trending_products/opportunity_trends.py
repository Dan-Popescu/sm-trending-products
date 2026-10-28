"""Disjoint, capture-time-normalized diagnostics over already eligible evidence.

The counter kernel owns revisions, deltas and resets. This module selects its
contained intervals and compares explicitly measured panels; it never backfills
missing counters or equates newly sampled content with newly published content.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .analytics import _counter_windows, _CounterWindow
from .contracts import Observation, utc

PREVIEW_LIMIT = 200
Context = tuple[str, str, str]


@dataclass
class _Measure:
    lower: int = 0
    upper: int = 0
    seconds: float = 0
    intervals: int = 0
    references: set[str] = field(default_factory=set)
    signatures: set[tuple[str, str, str]] = field(default_factory=set)
    contexts: set[Context] = field(default_factory=set)
    creators: set[str | None] = field(default_factory=set)
    creatives: set[str | None] = field(default_factory=set)

    @property
    def pace(self) -> tuple[float, float]:
        hours = self.seconds / 3600
        return self.lower / hours, self.upper / hours


@dataclass
class _Period:
    start: datetime
    end: datetime
    entities: dict[str, _Measure] = field(default_factory=dict)
    context_entities: dict[Context, dict[str, _Measure]] = field(default_factory=dict)
    corrections: int = 0
    exclusions: Counter[str] = field(default_factory=Counter)


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _context(row: Observation) -> Context:
    declared = _text(row.attributes.get("distribution_context")) or _text(
        row.attributes.get("distribution")
    )
    distribution = (
        "declared:" + declared
        if declared
        else "authorized_account"
        if row.source == "tiktok_display"
        else "commercial_content"
        if row.entity_type in {"ad", "commercial_content"}
        else "unknown_distribution"
    )
    return row.source, row.entity_type, distribution


def _creator(row: Observation) -> str | None:
    creator = _text(row.attributes.get("creator_id"))
    if creator:
        return f"{row.source}:creator:{creator}"
    account = _text(row.account_id)
    return f"{row.source}:account:{account}" if account else None


def _period(
    timelines: dict[str, _CounterWindow],
    observations: dict[str, Observation],
    start: datetime,
    end: datetime,
    gap: timedelta,
) -> _Period:
    result = _Period(start, end)
    for key, timeline in timelines.items():
        points = [row for row in timeline.rows if row["observed_at"] <= end]
        if not points or end - points[-1]["observed_at"] > gap:
            result.exclusions["no_recent_capture_at_period_end"] += 1
            continue
        measure = _Measure()
        for previous, row in zip(points, points[1:], strict=False):
            if not start < row["observed_at"] <= end:
                continue
            result.corrections += int(row["counter_correction"])
            seconds = (row["observed_at"] - previous["observed_at"]).total_seconds()
            if (
                previous["observed_at"] < start
                or not row["valid_interval"]
                or row["unit"] != "count"
                or seconds > gap.total_seconds()
            ):
                result.exclusions["unmeasured_or_incomparable_interval"] += 1
                continue
            before = observations[previous["observation_id"]]
            after = observations[row["observation_id"]]
            if _context(before) != _context(after):
                result.exclusions["changed_distribution_context"] += 1
                continue
            context = _context(after)
            contextual = result.context_entities.setdefault(context, {}).setdefault(key, _Measure())
            for target in (measure, contextual):
                target.lower += row["delta_lower"]
                target.upper += row["delta_upper"]
                target.seconds += seconds
                target.intervals += 1
                target.references.update((before.observation_id, after.observation_id))
                target.signatures.add((row["counter_scope"], row["counter_period"], row["unit"]))
                target.contexts.add(context)
                target.creators.update((_creator(before), _creator(after)))
                target.creatives.update(
                    (
                        _text(before.attributes.get("creative_fingerprint")),
                        _text(after.attributes.get("creative_fingerprint")),
                    )
                )
        if measure.intervals:
            result.entities[key] = measure
        else:
            result.exclusions["no_comparable_contained_interval"] += 1
    return result


def _summary(
    entities: dict[str, _Measure], *, preview_limit: int = PREVIEW_LIMIT
) -> dict[str, Any]:
    references = set().union(*(row.references for row in entities.values()))
    hours = sum(row.seconds for row in entities.values()) / 3600
    lower = sum(row.lower for row in entities.values())
    upper = sum(row.upper for row in entities.values())
    return {
        "entity_count": len(entities),
        "observed_growth_lower": lower if entities else None,
        "observed_growth_upper": upper if entities else None,
        "measured_entity_hours": hours,
        "growth_per_measured_entity_hour_lower": lower / hours if hours else None,
        "growth_per_measured_entity_hour_upper": upper / hours if hours else None,
        "panel_pace_lower": sum(row.pace[0] for row in entities.values()) if entities else None,
        "panel_pace_upper": sum(row.pace[1] for row in entities.values()) if entities else None,
        "comparable_interval_count": sum(row.intervals for row in entities.values()),
        "evidence_count": len(references),
        "evidence_preview": sorted(references)[:preview_limit],
        "evidence_preview_truncated": len(references) > preview_limit,
    }


def _period_report(period: _Period, known_entities: set[str]) -> dict[str, Any]:
    summary = _summary(period.entities)
    return {
        **summary,
        "window_start": period.start.isoformat(),
        "window_end": period.end.isoformat(),
        "known_entity_count": len(known_entities),
        "missing_entity_count": len(known_entities.difference(period.entities)),
        "mean_entity_coverage_fraction": (
            summary["measured_entity_hours"]
            / (len(known_entities) * (period.end - period.start).total_seconds() / 3600)
            if known_entities
            else None
        ),
        "counter_correction_count": period.corrections,
        "exclusion_counts": dict(sorted(period.exclusions.items())),
    }


def _comparison(
    previous: dict[str, _Measure],
    current: dict[str, _Measure],
    *,
    preview_limit: int = PREVIEW_LIMIT,
) -> dict[str, Any]:
    before = _summary(previous, preview_limit=preview_limit)
    after = _summary(current, preview_limit=preview_limit)
    lower = (
        after["panel_pace_lower"] / before["panel_pace_upper"]
        if current and previous and before["panel_pace_upper"] > 0
        else None
    )
    upper = (
        after["panel_pace_upper"] / before["panel_pace_lower"]
        if current and previous and before["panel_pace_lower"] > 0
        else None
    )
    return {
        "entity_count": len(current),
        "previous": before,
        "current": after,
        "pace_ratio_lower": lower,
        "pace_ratio_upper": upper,
        "comparison_status": (
            "insufficient_comparable_panel"
            if not current or not previous
            else "zero_or_uncertain_previous_pace"
            if upper is None
            else "measured"
        ),
    }


def _stable_identity(measure: _Measure, attribute: str) -> str | None:
    values = getattr(measure, attribute)
    return next(iter(values)) if len(values) == 1 and None not in values else None


def _growth_share(
    keys: set[str], current: dict[str, _Measure]
) -> tuple[float | None, float | None]:
    lower = sum(current[key].lower for key in keys)
    upper = sum(current[key].upper for key in keys)
    others = current.keys() - keys
    other_lower = sum(current[key].lower for key in others)
    other_upper = sum(current[key].upper for key in others)
    if not upper + other_upper:
        return None, None
    return (
        lower / (lower + other_upper) if lower + other_upper else 0,
        upper / (upper + other_lower) if upper + other_lower else 0,
    )


def _identity_report(
    previous: dict[str, _Measure], current: dict[str, _Measure], attribute: str
) -> dict[str, Any]:
    groups: dict[str, set[str]] = defaultdict(set)
    unknown: set[str] = set()
    for key, measure in current.items():
        identity = _stable_identity(measure, attribute)
        if identity is None or identity != _stable_identity(previous[key], attribute):
            unknown.add(key)
        else:
            groups[identity].add(key)
    ordered = sorted(
        groups,
        key=lambda identity: (-sum(current[key].lower for key in groups[identity]), identity),
    )
    largest = ordered[0] if ordered else None
    retained = set(current).difference(unknown, groups[largest] if largest else set())
    unknown_share = _growth_share(unknown, current)
    largest_share = _growth_share(groups[largest], current) if largest else (None, None)
    return {
        "scope": "continuing_panel",
        "largest_identity_selection": "largest_current_observed_lower_growth",
        "stress_check_status": (
            "no_known_identity"
            if largest is None
            else "no_measured_panel_after_removal"
            if not retained
            else "measured"
        ),
        "known_identity_count": len(groups),
        "unknown_identity_entity_count": len(unknown),
        "unknown_identity_growth_lower": sum(current[key].lower for key in unknown),
        "unknown_identity_growth_upper": sum(current[key].upper for key in unknown),
        "unknown_identity_growth_share_lower": unknown_share[0],
        "unknown_identity_growth_share_upper": unknown_share[1],
        "largest_identity": largest,
        "largest_identity_growth_share_lower": largest_share[0],
        "largest_identity_growth_share_upper": largest_share[1],
        "leave_largest_out": _comparison(
            {key: previous[key] for key in sorted(retained)},
            {key: current[key] for key in sorted(retained)},
        ),
        "identity_preview": ordered[:PREVIEW_LIMIT],
        "identity_preview_truncated": len(ordered) > PREVIEW_LIMIT,
    }


def _contexts(observations: Sequence[Observation]) -> dict[str, Any]:
    latest: dict[str, Observation] = {}
    for row in observations:
        previous = latest.get(row.entity_key)
        if previous is None or (utc(row.observed_at), utc(row.available_at), row.observation_id) > (
            utc(previous.observed_at),
            utc(previous.available_at),
            previous.observation_id,
        ):
            latest[row.entity_key] = row
    grouped: dict[Context, list[Observation]] = defaultdict(list)
    for row in latest.values():
        grouped[_context(row)].append(row)
    reports = []
    for context, rows in sorted(grouped.items())[:PREVIEW_LIMIT]:
        reach = [
            {
                "entity_key": row.entity_key,
                "observation_id": row.observation_id,
                "observed_at": utc(row.observed_at).isoformat(),
                "available_at": utc(row.available_at).isoformat(),
                "metric": name,
                "measurement": value.model_dump(),
            }
            for row in sorted(rows, key=lambda value: value.entity_key)
            for name, value in sorted(row.metrics.items())
            if "reach" in name or name == "unique_users_seen"
        ]
        reports.append(
            {
                "source": context[0],
                "entity_type": context[1],
                "distribution_context": context[2],
                "organic_distribution": "not_established",
                "entity_count": len(rows),
                "reach_measurements_preview": reach[:PREVIEW_LIMIT],
                "reach_measurements_preview_truncated": len(reach) > PREVIEW_LIMIT,
                "interpretation": "Reach retains its source meaning; it is not video views",
            }
        )
    return {"contexts": reports, "contexts_truncated": len(grouped) > PREVIEW_LIMIT}


def describe_diagnostics(
    observations: Sequence[Observation],
    known_video_keys: set[str],
    cutoff: datetime,
    windows_hours: Sequence[int],
    gap: timedelta,
) -> dict[str, Any]:
    """Summarize an eligible product partition; acquisition stays with the caller."""
    videos = [row for row in observations if row.entity_type == "video"]
    timelines = _counter_windows(videos, "view_count", retain_rows=True)
    by_id = {row.observation_id: row for row in videos}
    first_seen: dict[str, datetime] = {}
    for row in videos:
        first_seen[row.entity_key] = min(
            first_seen.get(row.entity_key, utc(row.observed_at)), utc(row.observed_at)
        )
    comparisons = []
    for hours in windows_hours:
        boundary = cutoff - timedelta(hours=hours)
        previous = _period(timelines, by_id, boundary - timedelta(hours=hours), boundary, gap)
        current = _period(timelines, by_id, boundary, cutoff, gap)
        continuing = {
            key
            for key in previous.entities.keys() & current.entities.keys()
            if previous.entities[key].signatures == current.entities[key].signatures
            and len(current.entities[key].signatures) == 1
            and previous.entities[key].contexts == current.entities[key].contexts
            and len(current.entities[key].contexts) == 1
        }
        before = {key: previous.entities[key] for key in sorted(continuing)}
        after = {key: current.entities[key] for key in sorted(continuing)}
        newly_observed = {
            key: measure for key, measure in current.entities.items() if first_seen[key] > boundary
        }
        other = {
            key: measure
            for key, measure in current.entities.items()
            if key not in continuing and key not in newly_observed
        }
        context_reports = []
        contexts = previous.context_entities.keys() | current.context_entities.keys()
        for context in sorted(contexts)[:PREVIEW_LIMIT]:
            context_reports.append(
                {
                    "source": context[0],
                    "entity_type": context[1],
                    "distribution_context": context[2],
                    "organic_distribution": "not_established",
                    "previous": _summary(
                        previous.context_entities.get(context, {}),
                        preview_limit=0,
                    ),
                    "current": _summary(
                        current.context_entities.get(context, {}),
                        preview_limit=0,
                    ),
                    "continuing_panel": _comparison(
                        {key: row for key, row in before.items() if row.contexts == {context}},
                        {key: row for key, row in after.items() if row.contexts == {context}},
                        preview_limit=0,
                    ),
                }
            )
        comparisons.append(
            {
                "window_hours": hours,
                "previous": _period_report(previous, known_video_keys),
                "current": _period_report(current, known_video_keys),
                "continuing_panel": _comparison(before, after),
                "newly_observed": _summary(newly_observed),
                "other_current_evidence": _summary(other),
                "creator_robustness": _identity_report(before, after, "creators"),
                "verified_creative_robustness": _identity_report(before, after, "creatives"),
                "source_distribution_contexts": context_reports,
                "source_distribution_contexts_truncated": len(contexts) > PREVIEW_LIMIT,
            }
        )
    return {
        "version": "disjoint-trend-diagnostics-v1",
        "comparisons": comparisons,
        "source_distribution": _contexts(observations),
        "predictive_confidence": None,
        "reasons": [
            "Adjacent disjoint periods include only wholly contained comparable capture intervals",
            "Panel pace sums each video's growth divided by its own measured hours",
            "Missing time and missing entities remain unknown; they are never zero-filled",
            "Newly observed means first sampled in this bounded read, regardless of publication",
            "Creator identifiers are source-scoped accounts, not independently verified people",
            "Leave-largest-out excludes unknown identities and the largest known contributor",
            "Authorized account access does not establish organic distribution",
            "Attention does not establish sales, demand, profitability or future traction",
        ],
    }

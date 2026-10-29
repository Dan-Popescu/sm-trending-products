"""Pure bounded grouping; counter differences and rates come from the shared kernel."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

from ..contracts import digest, utc
from ..distribution_context import known_distribution_context

MIN_GROUP_VIDEOS = 2
MIN_GROUP_CREATORS = 2
MIN_COVERAGE = 0.5
DURATION_RATIO = 0.95
ALIGNMENT_FRACTION = 0.05
MAX_INTERVAL_GAP_HOURS = 24


def _age(published: datetime | None, start: datetime) -> str:
    if published is None:
        return "unknown"
    hours = (start - utc(published)).total_seconds() / 3600
    return (
        "under_1d"
        if hours < 24
        else "1_7d"
        if hours < 168
        else "7_30d"
        if hours < 720
        else "30d_plus"
    )


def _video(timeline: dict, start: datetime, cutoff: datetime) -> tuple[dict | None, str | None]:
    points = timeline["points"]
    contained = [p for p in points if start <= p["observed_at"] <= cutoff]
    if not contained:
        return None, "no_comparable_count_interval"
    contexts = {digest(p["context"]) for p in contained}
    signatures = {(p["unit"], p["counter_scope"], p["counter_period"]) for p in contained}
    if len(contexts) != 1 or len(signatures) != 1:
        return None, "changed_video_context"
    intervals = [
        (before, after)
        for before, after in zip(points, points[1:], strict=False)
        if start <= before["observed_at"] < after["observed_at"] <= cutoff
        and after["valid_interval"]
        and after["unit"] == "count"
        and (after["observed_at"] - before["observed_at"])
        <= timedelta(hours=MAX_INTERVAL_GAP_HOURS)
    ]
    if not intervals:
        return None, "no_comparable_count_interval"
    first, last = intervals[0][0], intervals[-1][1]
    hours = sum(after["interval_hours"] for _, after in intervals)
    context = last["context"]
    # Only aggregate kernel-supplied differences/rates. No raw-counter subtraction.
    return {
        "entity_key": timeline["entity_key"],
        **context,
        "age_stratum": _age(context["published_at"], first["observed_at"]),
        "counter_scope": last["counter_scope"],
        "counter_period": last["counter_period"],
        "unit": last["unit"],
        "measured_start": first["observed_at"],
        "measured_end": last["observed_at"],
        "outcome_available_at": max(p["system_available_at"] for pair in intervals for p in pair),
        "measured_hours": hours,
        "coverage_fraction": hours / ((cutoff - start).total_seconds() / 3600),
        "comparable_interval_count": len(intervals),
        "counter_correction_count": sum(p["counter_correction"] for p in contained),
        "observed_growth_lower": sum(p["delta_lower"] for _, p in intervals),
        "observed_growth_upper": sum(p["delta_upper"] for _, p in intervals),
        "growth_per_hour_lower": sum(
            p["growth_per_hour"] * p["interval_hours"] for _, p in intervals
        )
        / hours,
        "growth_per_hour_upper": sum(
            p["growth_upper_per_hour"] * p["interval_hours"] for _, p in intervals
        )
        / hours,
        "evidence_refs": sorted({p["observation_id"] for pair in intervals for p in pair}),
    }, None


def outcomes(
    timelines: list[dict], texts: list[dict], start: datetime, cutoff: datetime
) -> tuple[list[dict], Counter, list[dict]]:
    exclusions: Counter[str] = Counter()
    details = []
    videos = {}
    for timeline in timelines:
        value, reason = _video(timeline, start, cutoff)
        if reason:
            exclusions[reason] += 1
            details.append(
                {"unit": "video", "entity_key": timeline["entity_key"], "reason": reason}
            )
        elif value is not None:
            videos[timeline["entity_key"]] = value
    text_groups: dict[tuple, list[dict]] = defaultdict(list)
    for text in texts:
        text_groups[(text["entity_key"], text["source_kind"], text["language"])].append(text)
    result = []
    for (entity, kind, language), selected in sorted(
        text_groups.items(), key=lambda pair: str(pair[0])
    ):
        if entity not in videos:
            exclusions["text_without_comparable_video"] += 1
            details.extend(
                {
                    "unit": "text_context",
                    "entity_key": entity,
                    "transcript_id": t["transcript_id"],
                    "reason": "text_without_comparable_video",
                }
                for t in selected
            )
            continue
        if len(selected) != 1:
            exclusions["multiple_text_versions_for_video"] += 1
            details.append(
                {
                    "unit": "text_context_group",
                    "entity_key": entity,
                    "reason": "multiple_text_versions_for_video",
                    "transcript_ids": [t["transcript_id"] for t in selected],
                }
            )
            continue
        text, video = selected[0], videos[entity]
        creator, attested = video["creator_id"], text["creator_id"]
        if creator and attested and creator != attested:
            exclusions["conflicting_creator_identity"] += 1
            details.append(
                {
                    "unit": "text_context",
                    "entity_key": entity,
                    "transcript_id": text["transcript_id"],
                    "reason": "conflicting_creator_identity",
                }
            )
            continue
        creator = creator or attested
        role = text["creator_role"] if attested else video["creator_role"]
        result.append(
            {
                **video,
                "content_key": text["content_key"],
                "transcript_id": text["transcript_id"],
                "source_kind": kind,
                "language": language or "unknown",
                "creator_id": creator,
                "creator_role": role,
                "creator_key": (video["source"], "creator", creator) if creator else None,
                "text_available_at": text["available_at"],
                "context_known_at": text["context_known_at"],
                "timing_scope": "text_known_by_interval_start"
                if utc(text["available_at"]) <= video["measured_start"]
                else "text_imported_after_interval_start",
            }
        )
    return result, exclusions, details


STRATUM_FIELDS = (
    "source",
    "distribution_context",
    "account_id",
    "market",
    "source_kind",
    "language",
    "age_stratum",
    "creator_role",
    "counter_scope",
    "counter_period",
    "unit",
    "timing_scope",
    "taxonomy_timing_scope",
)


def classification(text: dict, theme_id: str) -> str:
    coverage = text["theme_coverage"][theme_id]
    if coverage == "unsupported_language":
        return "unsupported_language"
    if coverage != "complete":
        return "incomplete_scan"
    mentions = [m for m in text["mentions"] if m["theme_id"] == theme_id]
    if any(m["status"] != "candidate" for m in mentions):
        return "negated_or_ambiguous"
    return "present" if mentions else "absent"


def _summary(rows: list[dict]) -> dict:
    return {
        "video_count": len(rows),
        "known_creator_count": len({r["creator_key"] for r in rows if r["creator_key"]}),
        "unknown_creator_video_count": sum(r["creator_key"] is None for r in rows),
        "measured_video_hours": sum(r["measured_hours"] for r in rows),
        "minimum_measured_hours": min((r["measured_hours"] for r in rows), default=None),
        "maximum_measured_hours": max((r["measured_hours"] for r in rows), default=None),
        "minimum_coverage_fraction": min((r["coverage_fraction"] for r in rows), default=None),
        "comparable_interval_count": sum(r["comparable_interval_count"] for r in rows),
        "observed_growth_lower": sum(r["observed_growth_lower"] for r in rows) if rows else None,
        "observed_growth_upper": sum(r["observed_growth_upper"] for r in rows) if rows else None,
        "mean_growth_lower": sum(r["observed_growth_lower"] for r in rows) / len(rows)
        if rows
        else None,
        "mean_growth_upper": sum(r["observed_growth_upper"] for r in rows) / len(rows)
        if rows
        else None,
        "mean_growth_per_hour_lower": sum(r["growth_per_hour_lower"] for r in rows) / len(rows)
        if rows
        else None,
        "mean_growth_per_hour_upper": sum(r["growth_per_hour_upper"] for r in rows) / len(rows)
        if rows
        else None,
        "entity_keys": [r["entity_key"] for r in rows],
    }


def compare(
    rows: list[dict], texts: dict[str, dict], theme_id: str, window_hours: int
) -> dict[str, Any]:
    groups: dict[str, list[dict]] = {"present": [], "absent": []}
    excluded: Counter[str] = Counter()
    for row in rows:
        status = classification(texts[row["content_key"]], theme_id)
        if status in groups:
            groups[status].append(row)
        else:
            excluded[status] += 1
    present, absent = _summary(groups["present"]), _summary(groups["absent"])
    reasons = []
    usable = groups["present"] + groups["absent"]
    if any(r["creator_key"] is None for r in rows):
        reasons.append("unknown_creator_identity")
    if any(
        g["video_count"] < MIN_GROUP_VIDEOS or g["known_creator_count"] < MIN_GROUP_CREATORS
        for g in (present, absent)
    ):
        reasons.append("insufficient_videos_or_known_creators")
    if {r["creator_key"] for r in groups["present"]}.intersection(
        r["creator_key"] for r in groups["absent"]
    ) - {None}:
        reasons.append("shared_creator_between_groups")
    if rows[0]["age_stratum"] == "unknown":
        reasons.append("unknown_video_age")
    if not known_distribution_context(rows[0]["distribution_context"]):
        reasons.append("unknown_distribution_context")
    if usable:
        if min(r["coverage_fraction"] for r in usable) < MIN_COVERAGE:
            reasons.append("sparse_measured_coverage")
        if (
            min(r["measured_hours"] for r in usable) / max(r["measured_hours"] for r in usable)
            < DURATION_RATIO
        ):
            reasons.append("incomparable_measured_durations")
        if any(
            (max(r[k] for r in usable) - min(r[k] for r in usable)).total_seconds()
            > window_hours * 3600 * ALIGNMENT_FRACTION
            for k in ("measured_start", "measured_end")
        ):
            reasons.append("incomparable_window_alignment")
        if any(r["counter_correction_count"] for r in usable):
            reasons.append("counter_corrections_in_window")
    result = {field: rows[0][field] for field in STRATUM_FIELDS}
    result.update(
        {
            "status": "unavailable" if reasons else "observed_association",
            "reasons": reasons,
            "present": present,
            "absent": absent,
            "excluded_classification_counts": dict(sorted(excluded.items())),
            "difference_in_mean_growth_lower": None
            if reasons
            else present["mean_growth_lower"] - absent["mean_growth_upper"],
            "difference_in_mean_growth_upper": None
            if reasons
            else present["mean_growth_upper"] - absent["mean_growth_lower"],
            "difference_in_mean_rate_lower": None
            if reasons
            else present["mean_growth_per_hour_lower"] - absent["mean_growth_per_hour_upper"],
            "difference_in_mean_rate_upper": None
            if reasons
            else present["mean_growth_per_hour_upper"] - absent["mean_growth_per_hour_lower"],
        }
    )
    return result

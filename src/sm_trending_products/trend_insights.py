"""Bounded descriptive trend windows over one product's already available evidence.

These reports do not change ranking policy or predict sales. Growth belongs to
actual comparable capture intervals; edge gaps are shown rather than interpolated.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from .analytics import _counter_windows, _CounterWindow
from .catalog import Catalog
from .contracts import Capability, Observation, digest, utc
from .evaluation import eligible_evidence
from .history import History
from .identity_rules import LINK_STATE_CTES

MAX_METADATA_ROWS = 10000
EVIDENCE_PREVIEW_LIMIT = 200


def _window(
    timelines: dict[str, _CounterWindow],
    observations: dict[str, Observation],
    known_entities: set[str],
    cutoff: datetime,
    hours: int,
    max_gap: timedelta,
) -> dict[str, Any]:
    start = cutoff - timedelta(hours=hours)
    lower = upper = intervals = positive = corrections = 0
    covered_seconds = positive_seconds = 0.0
    entity_coverage: list[float] = []
    entity_gains: dict[str, int] = {}
    creator_gains: dict[str, int] = defaultdict(int)
    creative_gains: dict[str, int] = defaultdict(int)
    creators: set[str] = set()
    fingerprints: set[str] = set()
    references: set[str] = set()
    fingerprinted = unknown_creators = 0
    exclusions: list[dict[str, str]] = [
        {"entity_key": key, "reason": "no_eligible_metric_history"}
        for key in sorted(known_entities.difference(timelines))[:20]
    ]
    entity_peaks: dict[str, float] = {}
    adequately_sampled: set[str] = set()
    for key, timeline in timelines.items():
        captured = 0.0
        gain = 0
        entity_intervals = 0
        rates: list[float] = []
        latest = observations[timeline.latest["observation_id"]]
        if cutoff - utc(latest.observed_at) > max_gap:
            if len(exclusions) < 20:
                exclusions.append({"entity_key": key, "reason": "stale_latest_capture"})
            continue
        for index, row in enumerate(timeline.rows):
            if not index or not start <= row["observed_at"] <= cutoff:
                continue
            previous = timeline.rows[index - 1]
            corrections += int(row["counter_correction"])
            if (
                previous["observed_at"] < start
                or not row["valid_interval"]
                or row["unit"] != "count"
            ):
                continue
            seconds = (row["observed_at"] - previous["observed_at"]).total_seconds()
            if seconds > max_gap.total_seconds():
                continue
            intervals += 1
            entity_intervals += 1
            captured += seconds
            covered_seconds += seconds
            lower += row["delta_lower"]
            upper += row["delta_upper"]
            gain += row["delta_lower"]
            rates.append(row["growth_per_hour"])
            if row["delta_lower"] > 0:
                positive += 1
                positive_seconds += seconds
            references.update((previous["observation_id"], row["observation_id"]))
        if not captured:
            if len(exclusions) < 20:
                exclusions.append({"entity_key": key, "reason": "no_comparable_contained_interval"})
            continue
        entity_coverage.append(captured / (hours * 3600))
        entity_gains[key] = gain
        if gain:
            entity_peaks[key] = max(rates) / (gain / (captured / 3600))
        if entity_intervals >= 3 and captured / (hours * 3600) >= 0.5:
            adequately_sampled.add(key)
        creator = latest.attributes.get("creator_id") or latest.account_id
        if creator:
            creators.add(str(creator))
            creator_gains[str(creator)] += gain
        else:
            unknown_creators += 1
        fingerprint = latest.attributes.get("creative_fingerprint")
        if isinstance(fingerprint, str) and fingerprint.strip():
            fingerprints.add(fingerprint)
            fingerprinted += 1
            creative_gains[fingerprint] += gain

    measured_entities = len(entity_gains)
    quality = (
        "insufficient"
        if not intervals
        else "complete"
        if measured_entities == len(known_entities)
        and all(abs(value - 1) < 1e-9 for value in entity_coverage)
        and not corrections
        else "partial"
    )
    persistence = positive_seconds / covered_seconds if covered_seconds else None
    # Compare each entity with its own pace: stable differences between videos
    # are diffusion, not evidence of a temporal spike.
    peak_ratio = (
        sum(entity_gains[key] * ratio for key, ratio in entity_peaks.items()) / lower
        if lower
        else None
    )
    adequately_sampled_growth = sum(entity_gains[key] for key in adequately_sampled)
    adequate = (
        measured_entities > 0
        and bool(adequately_sampled)
        and (not lower or adequately_sampled_growth / lower >= 0.5)
        and not corrections
    )
    shape = (
        "insufficient_history"
        if not adequate
        else "no_observed_growth"
        if not upper
        else "uncertain_growth"
        if not lower
        else "concentrated_growth"
        if peak_ratio is not None and peak_ratio >= 3
        else "sustained_growth"
        if persistence is not None and persistence >= 0.75
        else "mixed_growth"
    )
    known_creator_gain = sum(creator_gains.values())
    creator_concentration = (
        sum((gain / known_creator_gain) ** 2 for gain in creator_gains.values())
        if known_creator_gain
        else None
    )
    reasons = [
        "Growth includes only observed comparable intervals wholly inside this window",
        "Unobserved boundary gaps are not interpolated; zero missing growth is not assumed",
        "Shape and evidence quality are descriptive, not a calibrated probability of sales",
    ]
    if adequate:
        reasons.append(
            "Temporal shape needs at least three comparable capture intervals over half the window"
        )
    if shape == "concentrated_growth":
        reasons.append("Growth-weighted peaks are at least three times each video's own mean pace")
    if shape == "sustained_growth":
        reasons.append("Positive comparable intervals span at least 75% of measured capture time")
    if intervals and lower != upper:
        reasons.append(
            "Shape uses lower-bound growth; bounded counters admit other possible histories"
        )
    if corrections:
        reasons.append("Counter corrections/resets are excluded and limit shape confidence")
    if measured_entities < len(known_entities):
        reasons.append("Some linked videos have no current comparable evidence in this window")
    if unknown_creators:
        reasons.append("Some measured entities lack creator identity")
    if fingerprinted < measured_entities:
        reasons.append("Creative independence is unknown where a verified fingerprint is missing")
    return {
        "window_hours": hours,
        "window_start": start.isoformat(),
        "window_end": cutoff.isoformat(),
        "observed_growth_lower": lower if intervals else None,
        "observed_growth_upper": upper if intervals else None,
        "unit": "count",
        "growth_precision": None if not intervals else "exact" if lower == upper else "bounded",
        "measured_entity_count": measured_entities,
        "known_entity_count": len(known_entities),
        "missing_entity_count": len(known_entities) - measured_entities,
        "comparable_interval_count": intervals,
        "positive_interval_count": positive,
        "positive_observed_time_fraction": persistence,
        "growth_weighted_temporal_peak_ratio": peak_ratio,
        "adequately_sampled_entity_count": len(adequately_sampled),
        "minimum_entity_coverage_fraction": min(entity_coverage) if entity_coverage else None,
        "mean_entity_coverage_fraction": (
            sum(entity_coverage) / len(known_entities) if known_entities else None
        ),
        "known_creator_count": len(creators),
        "growing_creator_count": sum(gain > 0 for gain in creator_gains.values()),
        "unknown_creator_entities": unknown_creators,
        "creator_growth_concentration": creator_concentration,
        "verified_creative_count": len(fingerprints),
        "growing_verified_creative_count": sum(gain > 0 for gain in creative_gains.values()),
        "repeated_creative_entities": fingerprinted - len(fingerprints),
        "unknown_fingerprint_entities": measured_entities - fingerprinted,
        "evidence_quality": quality,
        "shape": shape,
        "counter_correction_count": corrections,
        "predictive_confidence": None,
        "reasons": reasons,
        "evidence_count": len(references),
        "evidence_preview": sorted(references)[:EVIDENCE_PREVIEW_LIMIT],
        "evidence_preview_truncated": len(references) > EVIDENCE_PREVIEW_LIMIT,
        "exclusions_preview": exclusions,
        "exclusions_preview_truncated": len(known_entities) - measured_entities > len(exclusions),
    }


class TrendInsights:
    """Describe one product with one bounded source read reused across windows."""

    def __init__(self, catalog: Catalog, history: History):
        self.catalog, self.history = catalog, history

    def describe(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        windows_hours: Sequence[int] = (24, 72, 168),
        max_boundary_gap_hours: float = 24,
        min_link_confidence: float = 0.9,
        max_rows: int = 50000,
    ) -> dict[str, Any]:
        cutoff = utc(as_of)
        if (
            not 1 <= len(windows_hours) <= 8
            or any(
                isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 8760
                for value in windows_hours
            )
            or len(set(windows_hours)) != len(windows_hours)
            or not math.isfinite(max_boundary_gap_hours)
            or not 0 < max_boundary_gap_hours <= 168
            or not math.isfinite(min_link_confidence)
            or not 0 <= min_link_confidence <= 1
            or isinstance(max_rows, bool)
            or not isinstance(max_rows, int)
            or not 1 <= max_rows <= 100000
        ):
            raise ValueError("Invalid bounded trend-insight configuration")
        product = self.catalog.evidence_one(
            "SELECT product_id,name,market FROM products WHERE product_id=%s "
            "AND synthetic=%s AND created_at<=%s",
            (product_id, synthetic, cutoff),
            synthetic=synthetic,
        )
        if product is None:
            raise ValueError("Product is unavailable in this workspace and cutoff")
        candidates = self.catalog.evidence_all(
            "SELECT DISTINCT entity_key FROM product_links WHERE product_id=%s "
            "AND available_at<=%s ORDER BY entity_key LIMIT %s",
            (product_id, cutoff, MAX_METADATA_ROWS + 1),
            synthetic=synthetic,
        )
        if len(candidates) > MAX_METADATA_ROWS:
            raise ValueError("Product identity exceeds the trend metadata budget")
        # Scope histories to this product's candidate entities while preserving
        # every competing owner's decisions. The shared resolver reads the CTE
        # named product_links, so none of its temporal ownership rules diverge.
        links = (
            self.catalog.evidence_all(
                "WITH product_links AS (SELECT * FROM product_links WHERE entity_key=ANY(%s)), "
                + LINK_STATE_CTES
                + "SELECT entity_key,decision_id,confidence FROM resolved_link_owners "
                "WHERE product_id=%s AND confidence>=%s ORDER BY entity_key LIMIT %s",
                (
                    [row["entity_key"] for row in candidates],
                    synthetic,
                    cutoff,
                    cutoff,
                    product_id,
                    min_link_confidence,
                    MAX_METADATA_ROWS + 1,
                ),
                synthetic=synthetic,
            )
            if candidates
            else []
        )
        if len(links) > MAX_METADATA_ROWS:
            raise ValueError("Product identity exceeds the trend metadata budget")
        keys = {row["entity_key"] for row in links if row["entity_key"].split(":", 2)[1] == "video"}
        gap = timedelta(hours=max_boundary_gap_hours)
        since = cutoff - timedelta(hours=max(windows_hours)) - gap
        with self.history.read_session():
            observations = (
                self.history.read(
                    self.catalog.iter_manifests(
                        synthetic=synthetic,
                        as_of=cutoff,
                        entity_keys=keys,
                        since=since,
                        until=cutoff,
                    ),
                    as_of=cutoff,
                    since=since,
                    until=cutoff,
                    entity_keys=keys,
                    max_rows=min(max_rows, self.history.settings.analysis_max_rows_per_batch),
                )
                if keys
                else []
            )
        accounts = sorted({(row.source, row.account_id or "") for row in observations})
        if len(accounts) > MAX_METADATA_ROWS:
            raise ValueError("Source account scope exceeds the trend metadata budget")
        capabilities = (
            self.catalog.evidence_all(
                "SELECT DISTINCT ON(c.source,c.account_id,c.capability) "
                "c.source,c.account_id,c.capability,c.state,c.available_at,c.details "
                "FROM source_capabilities c JOIN unnest(%s::text[],%s::text[]) "
                "AS selected(source,account_id) USING(source,account_id) "
                "WHERE c.available_at<=%s "
                "ORDER BY c.source,c.account_id,c.capability,c.available_at DESC,c.id DESC "
                "LIMIT %s",
                (
                    [pair[0] for pair in accounts],
                    [pair[1] for pair in accounts],
                    cutoff,
                    MAX_METADATA_ROWS + 1,
                ),
                synthetic=synthetic,
            )
            if accounts
            else []
        )
        # A scoped source with no grant for these accounts must remain denied.
        # Read one actual declaration per otherwise unseen source, rather than
        # sorting capability histories for every unrelated account of that source.
        unscoped = sorted(
            {pair[0] for pair in accounts}.difference(row["source"] for row in capabilities)
        )
        if unscoped:
            capabilities.extend(
                self.catalog.evidence_all(
                    "SELECT c.* FROM unnest(%s::text[]) AS selected(source) CROSS JOIN LATERAL ("
                    "SELECT source,account_id,capability,state,available_at,details "
                    "FROM source_capabilities WHERE source=selected.source "
                    "ORDER BY account_id,capability,available_at DESC,id DESC LIMIT 1) c",
                    (unscoped,),
                    synthetic=synthetic,
                )
            )
        if len(capabilities) > MAX_METADATA_ROWS:
            raise ValueError("Source capability metadata exceeds the trend budget")
        eligible = eligible_evidence(
            observations,
            cutoff,
            synthetic,
            product["market"],
            [Capability.model_validate(row) for row in capabilities],
            since=since,
        )
        videos = [row for row in eligible if row.entity_type == "video"]
        timelines = _counter_windows(videos, "view_count", retain_rows=True)
        by_id = {row.observation_id: row for row in videos}
        reports = [
            _window(timelines, by_id, keys, cutoff, hours, gap) for hours in sorted(windows_hours)
        ]
        result = {
            "version": "descriptive-trends-v1",
            "product_id": product_id,
            "name": product["name"],
            "market": product["market"],
            "as_of": cutoff.isoformat(),
            "synthetic": synthetic,
            "input_observation_count": len(observations),
            "eligible_video_observation_count": len(videos),
            "identity": {
                "version": digest(links),
                "decision_count": len(links),
                "decision_refs_preview": [
                    row["decision_id"] for row in links[:EVIDENCE_PREVIEW_LIMIT]
                ],
                "preview_truncated": len(links) > EVIDENCE_PREVIEW_LIMIT,
            },
            "windows": reports,
            "interpretation": "Observed attention patterns; no sales or profitability prediction",
        }
        result["report_id"] = digest(result)
        return result

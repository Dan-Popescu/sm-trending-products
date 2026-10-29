"""One guarded product/text population and one bounded authoritative timeline read."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from ..catalog import Catalog
from ..content_insights import DEFAULT_TAXONOMY, ContentInsights, ContentTaxonomy
from ..contracts import canonical, digest, utc
from ..enrichment import TranscriptInsights
from ..enrichment.contracts import IDENTIFIER, revalidate
from ..history import History
from ..provenance import Provenance
from ..trend_insights import TrendInsights
from ._groups import (
    ALIGNMENT_FRACTION,
    DURATION_RATIO,
    MAX_INTERVAL_GAP_HOURS,
    MIN_COVERAGE,
    MIN_GROUP_CREATORS,
    MIN_GROUP_VIDEOS,
    STRATUM_FIELDS,
    classification,
    compare,
    outcomes,
)

MAX_REPORT_BYTES = 4_000_000


def validate_scope(
    product_id: str,
    synthetic: bool,
    window_hours: int,
    max_rows: int,
    max_texts: int,
    max_strata: int,
) -> None:
    if (
        not isinstance(product_id, str)
        or not re.fullmatch(IDENTIFIER, product_id)
        or type(synthetic) is not bool
        or any(
            type(value) is not int or not 1 <= value <= cap
            for value, cap in (
                (window_hours, 8760),
                (max_rows, 100000),
                (max_texts, 200),
                (max_strata, 128),
            )
        )
    ):
        raise ValueError("Invalid bounded content-progression configuration")


class ContentProgression:
    """Describe measured associations; never execute extraction or source acquisition."""

    def __init__(self, catalog: Catalog, history: History, provenance: Provenance):
        self.catalog = catalog
        self.content = ContentInsights(catalog, provenance)
        self.texts = TranscriptInsights(catalog, provenance)
        self.trends = TrendInsights(catalog, history)

    def describe(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        window_hours: int = 168,
        max_rows: int = 50000,
        max_texts: int = 200,
        max_strata: int = 128,
        taxonomy: ContentTaxonomy = DEFAULT_TAXONOMY,
    ) -> dict:
        validate_scope(product_id, synthetic, window_hours, max_rows, max_texts, max_strata)
        cutoff = utc(as_of)
        taxonomy = revalidate(ContentTaxonomy, taxonomy)
        if taxonomy.reviewed_at > cutoff:
            raise ValueError("Content taxonomy was not reviewed at this cutoff")
        content = self.content.aggregate(
            product_id, synthetic=synthetic, as_of=cutoff, limit=max_texts, taxonomy=taxonomy
        )
        if content["next_cursor"]:
            raise ValueError("Complete text population exceeds the bounded candidate budget")
        texts = content["texts"]
        trend = self.trends.describe(
            product_id,
            cutoff,
            synthetic,
            windows_hours=(window_hours,),
            max_rows=max_rows,
            include_video_intervals=True,
            video_entity_keys=sorted({t["entity_key"] for t in texts}),
        )
        selected, exclusions, exclusion_details = outcomes(
            trend["video_timelines"], texts, cutoff - timedelta(hours=window_hours), cutoff
        )
        for outcome in selected:
            outcome["taxonomy_timing_scope"] = (
                "reviewed_by_interval_start"
                if taxonomy.reviewed_at <= outcome["measured_start"]
                else "reviewed_after_interval_start"
            )
        grouped: dict[tuple, list[dict]] = defaultdict(list)
        for outcome in selected:
            grouped[tuple(outcome[field] for field in STRATUM_FIELDS)].append(outcome)
        if len(grouped) > max_strata:
            raise ValueError("Content progression exceeds the bounded stratum budget")
        by_content = {text["content_key"]: text for text in texts}
        themes: list[dict] = []
        for rule in taxonomy.themes:
            counts = Counter(classification(text, rule.theme_id) for text in texts)
            themes.append(
                {
                    "theme_id": rule.theme_id,
                    "label": rule.label,
                    "classification_counts": {
                        key: counts[key]
                        for key in (
                            "present",
                            "absent",
                            "negated_or_ambiguous",
                            "unsupported_language",
                            "incomplete_scan",
                        )
                    },
                    "classification_count_unit": "canonical_supplied_texts",
                    "strata": [
                        compare(rows, by_content, rule.theme_id, window_hours)
                        for _, rows in sorted(grouped.items(), key=lambda pair: str(pair[0]))
                    ],
                    "status": "reviewed_theme" if selected else "unavailable",
                }
            )
        result = {
            "version": "quoted-content-progression-v1",
            "product_id": product_id,
            "synthetic": synthetic,
            "as_of": cutoff.isoformat(),
            "window_hours": window_hours,
            "window_start": (cutoff - timedelta(hours=window_hours)).isoformat(),
            "scope": "complete_bounded_eligible_supplied_text_population",
            "status": "available" if texts else "not_available",
            "comparison_status": "available"
            if any(s["status"] == "observed_association" for t in themes for s in t["strata"])
            else "unavailable",
            "taxonomy_version": content["taxonomy_version"],
            "taxonomy_sha256": content["taxonomy_sha256"],
            "taxonomy_reviewed_at": content["taxonomy_reviewed_at"],
            "taxonomy_review_reference": content["taxonomy_review_reference"],
            "distinct_text_count": len(texts),
            "candidate_rows_scanned": content["scanned"],
            "eligible_video_count": len({r["entity_key"] for r in selected}),
            "input_observation_count": trend["input_observation_count"],
            "stratum_count": len(grouped),
            "texts": texts,
            "outcomes": [
                {key: value for key, value in row.items() if key != "evidence_refs"}
                for row in selected
            ],
            "video_evidence": [
                {"entity_key": key, "evidence_refs": refs}
                for key, refs in sorted(
                    {r["entity_key"]: r["evidence_refs"] for r in selected}.items()
                )
            ],
            "themes": themes,
            "exclusion_counts": dict(sorted(exclusions.items())),
            "exclusions": exclusion_details,
            "source_unavailable_count": len(content["unavailable"]),
            "source_unavailable": content["unavailable"],
            "comparability_policy": {
                "minimum_group_videos": MIN_GROUP_VIDEOS,
                "maximum_interval_gap_hours": MAX_INTERVAL_GAP_HOURS,
                "minimum_group_named_creators": MIN_GROUP_CREATORS,
                "minimum_measured_coverage_fraction": MIN_COVERAGE,
                "minimum_measured_duration_ratio": DURATION_RATIO,
                "maximum_endpoint_spread_window_fraction": ALIGNMENT_FRACTION,
                "calibrated_confidence_thresholds": False,
            },
            "independent_people": "not_established",
            "causal_effect": None,
            "sales_effectiveness": "not_measured",
            "market_meaning": "source_observation_market_not_audience_geography",
            "predictive_confidence": None,
            "interpretation": (
                "Descriptive observed associations in supplied texts and measured attention; "
                "text imported after interval start was not known before that outcome. "
                "Named source-scoped creator IDs do not verify independent people. "
                "No causal, sales or calibrated predictive claim."
            ),
        }
        # Both final guards and finalization share one retention fence. A text
        # can depend on an older dataset absent from the measured-video refs.
        # Acquire this only after hydration, and reuse it for the metric query.
        with self.catalog.evidence_transaction(synthetic) as connection:
            fresh = self.texts.eligible_texts(
                product_id, synthetic=synthetic, as_of=cutoff, limit=max_texts
            )
            if fresh["next_cursor"] or not {t["transcript_id"] for t in texts}.issubset(
                {t["transcript_id"] for t in fresh["items"]}
            ):
                raise ValueError("Content evidence changed during report composition")
            references = sorted({ref for row in selected for ref in row["evidence_refs"]})
            if references:
                entities = {
                    ref: row["entity_key"] for row in selected for ref in row["evidence_refs"]
                }
                delivered = connection.execute(
                    "SELECT DISTINCT o.observation_id "
                    "FROM unnest(%s::text[],%s::text[]) selected(observation_id,entity_key) "
                    "JOIN observation_deliveries o USING(observation_id,entity_key) "
                    "JOIN normalized_objects n USING(object_id) JOIN datasets d USING(dataset_id) "
                    "WHERE d.synthetic=%s "
                    "AND o.available_at<=%s AND d.state='committed'",
                    (references, [entities[ref] for ref in references], synthetic, cutoff),
                ).fetchall()
                if {row["observation_id"] for row in delivered} != set(references):
                    raise ValueError("Measurement evidence changed during report composition")
            if len(canonical(result).encode()) > MAX_REPORT_BYTES:
                raise ValueError("Content progression exceeds the bounded output budget")
            result["report_id"] = digest(result)
            return result

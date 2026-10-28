"""One bounded, private decision brief over the existing evidence-owning modules."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from .catalog import Catalog
from .commerce import LaunchError, LaunchService
from .commercial_sensitivity import EconomicInsights, ScenarioProfile
from .competition_insights import CompetitionInsights
from .content_insights import DEFAULT_TAXONOMY, ContentInsights, ContentTaxonomy
from .contracts import digest, utc
from .enrichment.contracts import revalidate
from .history import History
from .matching import MatchingIndexesNotReady, MatchingProfile, ProductMatcher
from .module_schema import require_module
from .product_cohorts import CohortInsights
from .provenance import Provenance
from .trend_insights import TrendInsights


def _identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value):
        raise ValueError(f"Invalid {label}")


def _module_ready(catalog: Catalog, module: str, version: int = 1) -> bool:
    try:
        require_module(catalog, module, version)
    except ValueError as error:
        if str(error) != "Optional module is not initialized; run its init command":
            raise
        return False
    return True


def _unavailable(reason: str, *, status: str = "not_available") -> dict:
    return {"status": status, "reason": reason, "report": None}


def _available(report: dict) -> dict:
    return {"status": "available", "reason": None, "report": report}


class DecisionBrief:
    """Compose reports without persisting a second report or inventing a winner score.

    A namespace/retention fence spans the report, preventing retirement between
    its component reads. Existing module boundaries still enforce source scopes,
    availability and ownership. No source payload or brief is cached here.
    """

    def __init__(self, catalog: Catalog, history: History, provenance: Provenance):
        self.catalog, self.history, self.provenance = catalog, history, provenance

    def describe(
        self,
        product_id: str,
        target_country: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        taxonomy_id: str | None = None,
        category_id: str | None = None,
        dossier_id: str | None = None,
        expected_revision: int | None = None,
        scenario_profile: ScenarioProfile | dict | None = None,
        matching_profile: MatchingProfile | dict | None = None,
        content_taxonomy: ContentTaxonomy = DEFAULT_TAXONOMY,
        max_rows: int = 50000,
        max_products: int = 1000,
        text_limit: int = 100,
        query_scope: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        _identifier(product_id, "product_id")
        if type(synthetic) is not bool:
            raise ValueError("Invalid workspace")
        if not isinstance(target_country, str) or not re.fullmatch(r"[A-Z]{2}", target_country):
            raise ValueError("Target country requires an explicit uppercase two-letter code")
        for budget, maximum in ((max_rows, 100000), (max_products, 10000), (text_limit, 200)):
            if type(budget) is not int or not 1 <= budget <= maximum:
                raise ValueError("Invalid decision brief budget")
        for value, label in ((taxonomy_id, "taxonomy_id"), (category_id, "category_id")):
            if value is not None:
                _identifier(value, label)
        for value, maximum in ((source, 128), (query_scope, 1000)):
            if value is not None and (
                not isinstance(value, str)
                or not 1 <= len(value) <= maximum
                or value != value.strip()
                or not value.isprintable()
            ):
                raise ValueError("Invalid competition scope")
        if category_id is not None and taxonomy_id is None:
            raise ValueError("A category requires an explicit taxonomy")
        requested_commercial = any(
            value is not None for value in (dossier_id, expected_revision, scenario_profile)
        )
        if requested_commercial and any(
            value is None for value in (dossier_id, expected_revision, scenario_profile)
        ):
            raise ValueError("Select a dossier, exact revision and scenario profile together")
        if dossier_id is not None:
            _identifier(dossier_id, "dossier_id")
            if type(expected_revision) is not int or not 1 <= expected_revision <= 2147483647:
                raise ValueError("Invalid dossier revision")
        profile = (
            ScenarioProfile.model_validate(scenario_profile)
            if scenario_profile is not None
            else None
        )
        matching = (
            MatchingProfile.model_validate(
                dict(matching_profile)
                if isinstance(matching_profile, MatchingProfile)
                else matching_profile
            )
            if matching_profile is not None
            else None
        )
        content_taxonomy = revalidate(ContentTaxonomy, content_taxonomy)
        cutoff = utc(as_of)
        with self.catalog.evidence_transaction(synthetic) as connection:
            self.catalog.guard_retention(connection)
            product = connection.execute(
                "SELECT product_id,name,market,created_at FROM products "
                "WHERE product_id=%s AND synthetic=%s AND created_at<=%s",
                (product_id, synthetic, cutoff),
            ).fetchone()
            if product is None:
                raise ValueError("Product is unavailable in this workspace and cutoff")
            sections = {
                "trends": _available(
                    TrendInsights(self.catalog, self.history).describe(
                        product_id, cutoff, synthetic, diagnostics=True, max_rows=max_rows
                    )
                ),
                "competition": _available(
                    CompetitionInsights(self.catalog, self.history).describe(
                        product_id,
                        cutoff,
                        synthetic,
                        country=target_country,
                        source=source,
                        query_scope=query_scope,
                        max_rows=max_rows,
                    )
                ),
                "comparables": self._cohorts(
                    product_id,
                    target_country,
                    cutoff,
                    synthetic,
                    taxonomy_id,
                    category_id,
                    max_products,
                ),
                "themes": self._themes(product_id, cutoff, synthetic, content_taxonomy, text_limit),
                "variants": self._variants(product_id, cutoff, synthetic, matching),
                "economics": self._economics(
                    product_id,
                    target_country,
                    cutoff,
                    synthetic,
                    dossier_id,
                    expected_revision,
                    profile,
                ),
            }
            result = {
                "version": "product-decision-brief-v1",
                "product_id": product_id,
                "name": product["name"],
                "target_country": target_country,
                "product_target_market": product["market"],
                "country_basis": "operator_selected_target_market",
                "observed_audience_country": None,
                "audience_coverage": "unknown",
                "as_of": cutoff.isoformat(),
                "synthetic": synthetic,
                "sections": sections,
                "selection": {
                    "taxonomy_id": taxonomy_id,
                    "category_id": category_id,
                    "dossier_id": dossier_id,
                    "expected_revision": expected_revision,
                    "scenario_profile": profile.model_dump(mode="json") if profile else None,
                    "scenario_profile_hash": digest(profile.model_dump(mode="json"))
                    if profile
                    else None,
                    "matching_profile": matching.model_dump(mode="json") if matching else None,
                    "matching_profile_hash": digest(matching.model_dump(mode="json"))
                    if matching
                    else None,
                    "content_taxonomy_hash": digest(content_taxonomy.model_dump(mode="json")),
                    "content_taxonomy": content_taxonomy.model_dump(mode="json"),
                    "source": source,
                    "query_scope": query_scope,
                },
                "budgets": {
                    "history_rows_per_report": max_rows,
                    "cohort_products": max_products,
                    "supplied_texts": text_limit,
                    "matching_candidates": 100,
                    "matching_results": 20,
                },
                **_interpret(sections),
                "limitations": [
                    "Descriptive evidence and operator scenarios, "
                    "not a calibrated sales probability",
                    "Ready to review requests human investigation; "
                    "it does not launch or prove a winner",
                    "Target country is not observed audience country or measured local demand",
                    "Distribution, independent people and customer intent remain unverified",
                    "The brief uses available bounded panels, not a global market census",
                    "No new source, paid analysis, advertising, "
                    "payment, shipment or email is invoked",
                    "References identify this read; "
                    "replay refreshes source withdrawal and current revisions",
                ],
            }
            result["report_id"] = digest(result)
            return result

    def _cohorts(self, product, country, cutoff, synthetic, taxonomy, category, limit):
        if taxonomy is None:
            return _unavailable("select_reviewed_category_taxonomy", status="not_requested")
        if not _module_ready(self.catalog, "product_cohorts"):
            return _unavailable("product_cohorts_not_initialized", status="not_initialized")
        service = CohortInsights(self.catalog)
        if service.taxonomy(taxonomy, cutoff) is None:
            return _unavailable("taxonomy_not_available_at_cutoff")
        report = service.compare(
            cutoff,
            synthetic,
            taxonomy_id=taxonomy,
            category_id=category,
            country=country,
            max_products=limit,
        )
        row = next((row for row in report["rows"] if row["product_id"] == product), None)
        # The brief exposes one product, retaining the panel's method and coverage.
        report["rows"] = [row] if row is not None else []
        report["panel_report_id"] = report.pop("report_id")
        report["selected_product_status"] = "in_panel" if row else "not_in_requested_panel"
        report["report_id"] = digest(report)
        return _available(report)

    def _themes(self, product, cutoff, synthetic, taxonomy, limit):
        if not _module_ready(self.catalog, "transcript_insights", 2):
            return _unavailable("transcript_insights_not_initialized", status="not_initialized")
        if taxonomy.reviewed_at > cutoff:
            return _unavailable("content_taxonomy_not_available_at_cutoff")
        return _available(
            ContentInsights(self.catalog, self.provenance).aggregate(
                product,
                as_of=cutoff,
                synthetic=synthetic,
                taxonomy=taxonomy,
                limit=limit,
            )
        )

    def _variants(self, product, cutoff, synthetic, profile):
        if profile is None:
            return _unavailable("select_reviewed_variant_profile", status="not_requested")
        try:
            report = ProductMatcher(self.catalog).suggest(
                product,
                cutoff,
                synthetic,
                profile=profile,
                candidate_limit=100,
                limit=20,
            )
        except MatchingIndexesNotReady:
            return _unavailable(
                "matching_identifier_indexes_not_initialized", status="not_initialized"
            )
        report["profile_hash"] = digest(profile.model_dump(mode="json"))
        report["report_id"] = digest(report)
        return _available(report)

    def _economics(self, product, country, cutoff, synthetic, dossier, revision, profile):
        if dossier is None:
            return _unavailable(
                "select_dossier_revision_and_assumption_profile", status="not_requested"
            )
        if not _module_ready(self.catalog, "commerce"):
            return _unavailable("commerce_not_initialized", status="not_initialized")
        result = EconomicInsights(LaunchService(self.catalog)).describe(
            dossier,
            revision,
            profile,
            synthetic=synthetic,
        )
        if result["product_id"] != product or result["country"] != country:
            raise LaunchError("brief_dossier_scope_mismatch")
        if utc(result["source_facts"]["updated_at"]) > cutoff:
            return _unavailable("selected_dossier_revision_not_available_at_cutoff")
        result["report_id"] = digest(result)
        return _available(result)


def _interpret(sections: dict) -> dict:
    support: list[dict] = []
    contrary: list[dict] = []
    missing: list[dict] = []

    def note(bucket, dimension, code, report=None, **values):
        bucket.append(
            {
                "dimension": dimension,
                "code": code,
                "reference": (report or {}).get("report_id") or (report or {}).get("aggregate_id"),
                **values,
            }
        )

    for dimension, section in sections.items():
        if section["status"] != "available":
            note(missing, dimension, section["reason"])
    trends = sections["trends"]["report"]
    positive = False
    for window in trends["windows"]:
        if window["observed_growth_lower"] is not None and window["observed_growth_lower"] > 0:
            positive = True
            note(
                support,
                "trends",
                "positive_observed_attention",
                trends,
                window_hours=window["window_hours"],
                growth_lower=window["observed_growth_lower"],
                growth_upper=window["observed_growth_upper"],
                unit=window["unit"],
            )
        if window["evidence_quality"] != "complete":
            note(
                missing,
                "trends",
                "incomplete_observed_window",
                trends,
                window_hours=window["window_hours"],
                missing_entities=window["missing_entity_count"],
            )
        if window["counter_correction_count"]:
            note(
                contrary,
                "trends",
                "counter_corrections_limit_comparison",
                trends,
                window_hours=window["window_hours"],
            )
    for comparison in trends["diagnostics"]["comparisons"]:
        creator = comparison["creator_robustness"]
        if (
            creator["largest_identity_growth_share_lower"] is not None
            and creator["largest_identity_growth_share_lower"] >= 0.5
        ):
            note(
                contrary,
                "trends",
                "dominant_creator_contribution",
                trends,
                window_hours=comparison["window_hours"],
                share_lower=creator["largest_identity_growth_share_lower"],
            )
        if creator["stress_check_status"] != "measured":
            note(
                missing,
                "trends",
                "creator_removal_not_measurable",
                trends,
                window_hours=comparison["window_hours"],
            )
    competition = sections["competition"]["report"]
    if competition["status"] != "observed":
        note(missing, "competition", competition["reason"], competition)
    cohorts = sections["comparables"]["report"]
    if cohorts:
        row = next(iter(cohorts["rows"]), None)
        if row is None or row["percentile"] is None:
            note(
                missing,
                "comparables",
                row["reason"] if row else "selected_product_not_in_panel",
                cohorts,
            )
    themes = sections["themes"]["report"]
    if themes:
        for gap in themes["gaps"]:
            note(missing, "themes", gap, themes)
        if themes["distinct_text_count"]:
            note(
                support,
                "themes",
                "supplied_text_mentions_available_not_buyer_demand",
                themes,
                distinct_text_count=themes["distinct_text_count"],
            )
    variants = sections["variants"]["report"]
    if variants:
        conflicts = [row for row in variants["suggestions"] if row["conflicts"]]
        incomplete = [row for row in variants["suggestions"] if row["missing_facts"]]
        if incomplete:
            note(
                missing,
                "variants",
                "variant_facts_incomplete",
                variants,
                candidate_count=len(incomplete),
                entity_keys=[row["entity_key"] for row in incomplete],
            )
        if conflicts:
            note(
                contrary,
                "variants",
                "candidate_identity_or_variant_conflicts_require_review",
                variants,
                candidate_count=len(conflicts),
                entity_keys=[row["entity_key"] for row in conflicts],
            )
        if variants["candidate_truncated"] or variants["ownership_truncated"]:
            note(missing, "variants", "bounded_identity_panel_incomplete", variants)
        if not variants["suggestions"]:
            note(missing, "variants", "no_current_matching_metadata", variants)
    economics = sections["economics"]["report"]
    commercially_reviewable = False
    if economics:
        summary = economics["summary"]
        readiness = economics["source_facts"]["readiness"]
        if summary["incomplete_points"]:
            note(missing, "economics", "scenario_costs_incomplete", economics)
        if summary["all_meet_target"] is False:
            note(contrary, "economics", "declared_scenarios_miss_profit_target", economics)
        if readiness["status"] != "ready":
            note(missing, "economics", "dossier_not_ready", economics, reasons=readiness["reasons"])
        if any(point["cash"]["status"] == "unknown" for point in economics["scenarios"]):
            note(missing, "economics", "cash_volume_or_timing_unknown", economics)
        commercially_reviewable = (
            readiness["status"] == "ready" and summary["all_meet_target"] is True
        )
        if commercially_reviewable:
            note(
                support,
                "economics",
                "declared_scenarios_meet_target_not_actual_profit",
                economics,
                complete_points=summary["complete_points"],
            )
    note(missing, "demand", "actual_purchases_and_product_market_fit_not_established")
    note(missing, "audience", "target_country_audience_coverage_unknown")
    # This is a navigation state, never a commercial recommendation or fitted model.
    status = (
        "ready_to_review"
        if positive and commercially_reviewable
        else "investigate"
        if positive
        else "watch"
    )
    next_facts = []
    actions = {
        "economics": "Supply or review exact supplier, cost, tax, return and cash-timing facts",
        "variants": "Review product identifiers and variant conflicts before pooling evidence",
        "trends": "Inspect additional comparable captures and independent known contributors",
        "comparables": "Review category assignment and enough compatible product assessments",
        "themes": "Supply permissioned texts and inspect quoted context and language coverage",
        "competition": "Document comparable query, country, distribution and price contexts",
        "demand": "Define a future reviewed commercial test; "
        "attention alone cannot establish purchases",
        "audience": "Obtain directly observed audience geography before claiming local demand",
    }
    for dimension in (
        "economics",
        "variants",
        "trends",
        "comparables",
        "themes",
        "competition",
        "demand",
        "audience",
    ):
        codes = [row["code"] for row in missing + contrary if row["dimension"] == dimension]
        if codes:
            next_facts.append(
                {
                    "dimension": dimension,
                    "action": actions[dimension],
                    "reasons": sorted(set(codes)),
                }
            )
    return {
        "status": status,
        "status_policy": {
            "version": "review-navigation-v1",
            "watch": "No positive lower-bound growth in the requested observed windows",
            "investigate": "Positive observed growth; "
            "no complete currently ready selected commercial scenario",
            "ready_to_review": "Positive observed growth and a currently ready dossier "
            "whose complete declared scenarios meet its target",
            "meaning": "Navigation for operator review; "
            "all listed contrary evidence and gaps still apply",
            "launch_authorized": False,
            "predictive_probability": None,
        },
        "support": support,
        "contrary": contrary,
        "missing": missing,
        "next_useful_facts": next_facts,
    }

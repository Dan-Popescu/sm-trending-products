"""Compact, guarded differences between two selected-product evidence cutoffs.

DecisionBrief and its owning kernels remain authoritative. This module keeps no
history and never returns source quotations or whole report copies.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from .catalog import Catalog
from .commerce import LaunchError
from .commercial_sensitivity import ScenarioProfile
from .content_insights import DEFAULT_TAXONOMY, ContentTaxonomy
from .contracts import canonical, digest, utc
from .decision_brief import DecisionBrief, _identifier, _unavailable
from .distribution_context import known_distribution_context
from .history import History
from .matching import MatchingProfile
from .provenance import Provenance

PREVIEW_LIMIT = 20
MAX_OUTPUT_BYTES = 128 * 1024
_ECONOMIC_FACT_FIELDS = (
    "currency",
    "retail_price",
    "prices_include_tax",
    "tax_per_order",
    "desired_profit",
    "costs",
    "quote_expires_at",
    "stock_status",
    "delivery_min_days",
    "delivery_max_days",
    "fulfillment_mode",
    "decision",
)


class ChangesSelection(BaseModel):
    """Explicit inputs for one brief; budgets belong to the complete comparison."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    taxonomy_id: str | None = None
    category_id: str | None = None
    dossier_id: str | None = None
    expected_revision: int | None = None
    scenario_profile: ScenarioProfile | None = None
    matching_profile: MatchingProfile | None = None
    content_taxonomy: ContentTaxonomy = DEFAULT_TAXONOMY
    source: str | None = None
    query_scope: str | None = None

    @model_validator(mode="before")
    @classmethod
    def exact_revision(cls, value: Any) -> Any:
        if isinstance(value, dict) and value.get("expected_revision") is not None:
            revision = value["expected_revision"]
            if type(revision) is not int or not 1 <= revision <= 2_147_483_647:
                raise ValueError("Invalid exact revision")
        return value

    @model_validator(mode="after")
    def valid_scope(self) -> ChangesSelection:
        for field in ("taxonomy_id", "category_id", "dossier_id"):
            value = getattr(self, field)
            if value is not None:
                _identifier(value, field)
        if self.category_id is not None and self.taxonomy_id is None:
            raise ValueError("Select a taxonomy for the category")
        commercial = (self.dossier_id, self.expected_revision, self.scenario_profile)
        if any(value is not None for value in commercial) and any(
            value is None for value in commercial
        ):
            raise ValueError("Select exact dossier revision and scenarios together")
        if self.source is not None and not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.source):
            raise ValueError("Invalid source")
        if self.query_scope is not None and (
            not 1 <= len(self.query_scope) <= 1000
            or self.query_scope != self.query_scope.strip()
            or not self.query_scope.isprintable()
        ):
            raise ValueError("Invalid query scope")
        return self

    def brief_arguments(self) -> dict:
        # Revalidation also checks mutable nested models supplied by a caller.
        return ChangesSelection.model_validate(self.model_dump(mode="json")).__dict__.copy()


class _ChangesBrief(DecisionBrief):
    def _economics(self, *args, **kwargs):
        try:
            return super()._economics(*args, **kwargs)
        except LaunchError as error:
            if error.code not in {"revision_conflict", "not_found"}:
                raise
            # The public dossier boundary serves the current exact revision.
            # Do not reconstruct an older draft from unsupported SQL or audit text.
            return _unavailable("selected_historical_dossier_revision_unavailable")


class DecisionChanges:
    """Two sequential briefs under one namespace/retention fence; no report cache."""

    def __init__(self, catalog: Catalog, history: History, provenance: Provenance):
        self.catalog, self.history, self.provenance = catalog, history, provenance

    def compare(
        self,
        product_id: str,
        target_country: str,
        previous_as_of: datetime,
        as_of: datetime,
        synthetic: bool = False,
        *,
        previous_selection: ChangesSelection | dict | None = None,
        current_selection: ChangesSelection | dict | None = None,
        max_rows: int = 50_000,
        max_products: int = 1000,
        text_limit: int = 100,
    ) -> dict:
        _identifier(product_id, "product_id")
        if not isinstance(target_country, str) or not re.fullmatch(r"[A-Z]{2}", target_country):
            raise ValueError("Select an explicit target country")
        if type(synthetic) is not bool:
            raise ValueError("Invalid workspace")
        for value, maximum in ((max_rows, 100_000), (max_products, 10_000), (text_limit, 200)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("Invalid comparison budget")
        previous_cutoff, cutoff = utc(previous_as_of), utc(as_of)
        if previous_cutoff >= cutoff:
            raise ValueError("Cutoffs must be strictly chronological")
        prior = _selection(previous_selection)
        current = _selection(current_selection) if current_selection is not None else prior
        prior_args, current_args = prior.brief_arguments(), current.brief_arguments()
        service = _ChangesBrief(self.catalog, self.history, self.provenance)
        with self.catalog.evidence_transaction(synthetic) as connection:
            self.catalog.guard_retention(connection)
            before = _compact(
                service.describe(
                    product_id,
                    target_country,
                    previous_cutoff,
                    synthetic,
                    **prior_args,
                    max_rows=max_rows,
                    max_products=max_products,
                    text_limit=text_limit,
                )
            )
            after = _compact(
                service.describe(
                    product_id,
                    target_country,
                    cutoff,
                    synthetic,
                    **current_args,
                    max_rows=max_rows,
                    max_products=max_products,
                    text_limit=text_limit,
                )
            )
            result = _compose(before, after)
            result["budgets"] = {
                "cutoffs": 2,
                "history_rows_per_component": max_rows,
                "maximum_history_reads": 4,
                "cohort_products_per_cutoff": max_products,
                "supplied_texts_per_cutoff": text_limit,
                "preview_items": PREVIEW_LIMIT,
                "maximum_output_bytes": MAX_OUTPUT_BYTES,
            }
            result["report_id"] = digest(result)
            if len(canonical(result).encode()) > MAX_OUTPUT_BYTES:
                raise ValueError("Decision changes output exceeds budget")
            return result


def _selection(value: ChangesSelection | dict | None) -> ChangesSelection:
    return ChangesSelection.model_validate(
        value.model_dump(mode="json")
        if isinstance(value, ChangesSelection)
        else {}
        if value is None
        else value
    )


def _compact(brief: dict) -> dict:
    """Drop first-read quotations before the second read, even in local memory."""
    for dimension, section in brief["sections"].items():
        report = section["report"]
        if report is None:
            continue
        if dimension == "themes":
            section["report"] = {
                key: report[key]
                for key in (
                    "aggregate_id",
                    "distinct_text_count",
                    "normalized_script_count",
                    "unsupported_text_count",
                    "truncated_text_count",
                    "scanned",
                    "next_cursor",
                )
            }
        elif dimension == "economics":
            draft = report["source_facts"]["draft"]
            report["source_facts"]["draft"] = {key: draft[key] for key in _ECONOMIC_FACT_FIELDS}
            report["source_facts"]["draft"]["declared_terms_reference"] = digest(
                {key: draft[key] for key in ("supplier_name", "supplier_reference", "return_terms")}
            )
        elif dimension == "variants":
            section["report"] = {
                key: report[key]
                for key in (
                    "report_id",
                    "candidate_truncated",
                    "ownership_truncated",
                )
            }
        elif dimension == "trends":
            report.pop("name", None)
            report["diagnostics"].pop("source_distribution", None)
    brief.pop("name", None)
    return brief


def _reference(section: dict) -> dict:
    report = section["report"] or {}
    return {
        "status": section["status"],
        "reason": section["reason"],
        "reference": report.get("report_id") or report.get("aggregate_id"),
    }


def _facts(report: dict, bucket: str) -> list[dict]:
    # Repeated brief codes describe distinct windows. References and measured
    # quantities can change while the same scoped support/gap remains present.
    return sorted(
        (
            {
                "dimension": row["dimension"],
                "code": row["code"],
                **({"window_hours": row["window_hours"]} if "window_hours" in row else {}),
            }
            for row in report[bucket]
        ),
        key=lambda row: (row["dimension"], row["code"], row.get("window_hours", 0)),
    )


def _coverage_quantities(dimension: str, section: dict) -> dict | None:
    report = section["report"]
    if report is None:
        return None
    fields = {
        "trends": ("input_observation_count", "eligible_video_observation_count"),
        "themes": (
            "distinct_text_count",
            "normalized_script_count",
            "unsupported_text_count",
            "truncated_text_count",
            "scanned",
        ),
        "competition": ("resolved_observation_count", "revision_count", "conflicting_entity_count"),
        "variants": ("candidate_truncated", "ownership_truncated"),
    }.get(dimension, ())
    values = {key: report[key] for key in fields}
    if dimension == "themes":
        values["next_page_available"] = report["next_cursor"] is not None
    if dimension == "comparables":
        selected = next(iter(report["rows"]), None)
        values["selected_product_status"] = report["selected_product_status"]
        values["selected_product_reason"] = selected["reason"] if selected else None
    return values


def _difference(before: Any, after: Any) -> dict:
    return {"changed": before != after, "previous": before, "current": after}


def _compose(before: dict, after: dict) -> dict:
    selection = {
        field: _difference(digest(before["selection"][field]), digest(after["selection"][field]))
        for field in before["selection"]
        if before["selection"][field] != after["selection"][field]
    }
    coverage = []
    for dimension in before["sections"]:
        prior, current = before["sections"][dimension], after["sections"][dimension]
        coverage.append(
            {
                "dimension": dimension,
                **_difference(_reference(prior), _reference(current)),
                "quantities": _difference(
                    _coverage_quantities(dimension, prior), _coverage_quantities(dimension, current)
                ),
                "meaning": "reference_change_alone_is_not_measured_behavior",
            }
        )
    gaps = {}
    for bucket in ("support", "contrary", "missing"):
        prior, current = _facts(before, bucket), _facts(after, bucket)
        added = [row for row in current if row not in prior]
        removed = [row for row in prior if row not in current]
        gaps[bucket] = {
            "added": added[:PREVIEW_LIMIT],
            "removed": removed[:PREVIEW_LIMIT],
            "added_count": len(added),
            "removed_count": len(removed),
            "preview_truncated": max(len(added), len(removed)) > PREVIEW_LIMIT,
        }
    return {
        "version": "product-decision-changes-v1",
        "product_id": before["product_id"],
        "target_country": before["target_country"],
        "synthetic": before["synthetic"],
        "previous_as_of": before["as_of"],
        "as_of": after["as_of"],
        "country_basis": "operator_selected_target_market",
        "observed_audience_country": None,
        "audience_coverage": "unknown",
        "brief_references": {"previous": before["report_id"], "current": after["report_id"]},
        "selection_changes": selection,
        "selections": {
            label: {
                **{
                    key: report["selection"][key]
                    for key in (
                        "taxonomy_id",
                        "category_id",
                        "dossier_id",
                        "expected_revision",
                        "scenario_profile_hash",
                        "matching_profile_hash",
                        "content_taxonomy_hash",
                    )
                },
                "competition_context_reference": digest(
                    {key: report["selection"][key] for key in ("source", "query_scope")}
                ),
            }
            for label, report in (("previous", before), ("current", after))
        },
        "review_state": {
            **_difference(before["status"], after["status"]),
            "meaning": "navigation_state_using_current_quote_readiness_not_historical_readiness",
            "launch_authorized": False,
        },
        "trajectory": _trajectory(before, after, bool(selection)),
        "economics": _economics(before, after),
        "competition": _competition(before, after, bool(selection)),
        "coverage": coverage,
        "evidence_changes": gaps,
        "limitations": [
            "Two fresh guarded reads, not stored historical snapshots or a backtest",
            "A changed digest, discovery, correction or assumption does not prove growth",
            "Missing historical dossier revisions and replaced compact projections "
            "stay unavailable",
            "Quote readiness is evaluated now; evidence cutoffs do not reconstruct past readiness",
            "Target-country selection does not supply measured audience geography",
            "Observed attention and competition do not establish purchases or market growth",
            "No source quotations, raw source text, execution plans or full report copies "
            "are returned",
        ],
    }


def _period_fingerprint(period: dict) -> str | None:
    if period["evidence_preview_truncated"]:
        return None
    # Known entity counts can grow through later discovery. Compare the measured
    # baseline itself; continuing-panel membership is decided only by the kernel.
    return digest(
        {
            key: value
            for key, value in period.items()
            if key
            not in {
                "known_entity_count",
                "missing_entity_count",
                "mean_entity_coverage_fraction",
                "exclusion_counts",
            }
        }
    )


def _measure(period: dict) -> dict:
    return {
        key: period[key]
        for key in (
            "window_start",
            "window_end",
            "observed_growth_lower",
            "observed_growth_upper",
            "measured_entity_hours",
            "comparable_interval_count",
            "counter_correction_count",
            "known_entity_count",
            "missing_entity_count",
            "exclusion_counts",
        )
    }


def _trajectory(before: dict, after: dict, selection_changed: bool) -> dict:
    prior = before["sections"]["trends"]["report"]
    current = after["sections"]["trends"]["report"]
    rows = []
    prior_periods = {row["window_hours"]: row for row in prior["diagnostics"]["comparisons"]}
    for row in current["diagnostics"]["comparisons"]:
        old = prior_periods[row["window_hours"]]["current"]
        baseline = row["previous"]
        panel = row["continuing_panel"]
        reason = (
            "changed_selection_or_configuration"
            if selection_changed
            else "requested_cutoffs_do_not_match_adjacent_kernel_windows"
            if baseline["window_end"] != before["as_of"]
            else "baseline_reference_preview_incomplete"
            if _period_fingerprint(old) is None or _period_fingerprint(baseline) is None
            else "baseline_evidence_changed_or_newly_available"
            if _period_fingerprint(old) != _period_fingerprint(baseline)
            else "counter_corrections_limit_comparison"
            if baseline["counter_correction_count"] or row["current"]["counter_correction_count"]
            else "source_context_unknown_or_incomplete"
            if row["source_distribution_contexts_truncated"]
            or any(
                not known_distribution_context(context["distribution_context"])
                and context["continuing_panel"]["entity_count"]
                for context in row["source_distribution_contexts"]
            )
            else None
            if panel["comparison_status"] == "measured"
            else panel["comparison_status"]
        )
        rows.append(
            {
                "window_hours": row["window_hours"],
                "previous_cutoff_measurement": _measure(old),
                "current_cutoff_measurement": _measure(row["current"]),
                "comparison_status": "measured" if reason is None else "unavailable",
                "reason": reason,
                "continuing_entity_count": panel["entity_count"],
                "pace_ratio_lower": panel["pace_ratio_lower"] if reason is None else None,
                "pace_ratio_upper": panel["pace_ratio_upper"] if reason is None else None,
                "newly_observed_entity_count": row["newly_observed"]["entity_count"],
                "other_current_entity_count": row["other_current_evidence"]["entity_count"],
                "newly_observed_meaning": "first_sampled_not_market_entry_or_growth",
                "reference": current["report_id"],
            }
        )
    return {
        "windows": rows,
        "identity_changed": prior["identity"]["version"] != current["identity"]["version"],
        "method": "existing_disjoint_trend_kernel_continuing_panel",
        "unit": "count_per_measured_entity_hour",
        "predictive_confidence": None,
    }


def _economic_facts(section: dict) -> dict | None:
    report = section["report"]
    if report is None:
        return None
    draft = report["source_facts"]["draft"]
    return {
        "dossier_id": report["dossier_id"],
        "revision": report["revision"],
        "currency": report["currency"],
        "facts": draft,
        "scenario_profile_reference": report["profile_hash"],
        "scenario_summary": report["summary"],
        "quote_readiness_evaluated_at": report["evaluated_at"],
        "current_quote_readiness": report["source_facts"]["readiness"]["status"],
        "current_quote_readiness_reasons": report["source_facts"]["readiness"]["reasons"],
        "facts_updated_at": report["source_facts"]["updated_at"],
        "reference": report["report_id"],
    }


def _economics(before: dict, after: dict) -> dict:
    prior = _economic_facts(before["sections"]["economics"])
    current = _economic_facts(after["sections"]["economics"])
    return {
        "status": "available" if prior is not None and current is not None else "unavailable",
        "previous": prior,
        "current": current,
        "availability": {
            "previous": _reference(before["sections"]["economics"]),
            "current": _reference(after["sections"]["economics"]),
        },
        "facts_changed": prior["facts"] != current["facts"] if prior and current else None,
        "assumptions_changed": (
            prior["scenario_profile_reference"] != current["scenario_profile_reference"]
            if prior and current
            else None
        ),
        "reason": "current_readiness_is_not_historical_readiness",
        "realized_profit_change": None,
    }


def _competition(before: dict, after: dict, selection_changed: bool) -> dict:
    prior = before["sections"]["competition"]["report"]
    current = after["sections"]["competition"]["report"]
    old_scopes = {row["scope_id"]: row for row in prior["scopes"]}
    scopes = []
    for row in current["scopes"][:PREVIEW_LIMIT]:
        old = old_scopes.get(row["scope_id"])
        fields = (
            "capture_count",
            "observed_entity_count",
            "fresh_entity_count",
            "stale_entity_count",
            "observed_seller_count",
            "observed_advertiser_count",
            "comparable_interval_count",
            "observed_price_decrease_count",
            "observed_price_increase_count",
        )
        scopes.append(
            {
                "scope_reference": row["scope_id"],
                "status": "observed"
                if old and old["status"] == row["status"] == "observed" and not selection_changed
                else "unavailable",
                "reason": "observed_panel_counts_not_market_growth"
                if old
                else "newly_observed_scope_not_market_entry",
                "facts": {key: _difference(old[key] if old else None, row[key]) for key in fields},
            }
        )
    return {
        "previous_status": prior["status"],
        "current_status": current["status"],
        "previous_reference": prior["report_id"],
        "current_reference": current["report_id"],
        "scopes": scopes,
        "scope_preview_truncated": len(current["scopes"]) > PREVIEW_LIMIT,
        "previous_scope_count": len(prior["scopes"]),
        "current_scope_count": len(current["scopes"]),
        "previous_scopes_not_observed_currently": sorted(
            set(old_scopes) - {row["scope_id"] for row in current["scopes"]}
        )[:PREVIEW_LIMIT],
        "previous_scopes_not_observed_currently_count": len(
            set(old_scopes) - {row["scope_id"] for row in current["scopes"]}
        ),
        "scope_change_count": current["scope_change_count"],
        "revision_count": current["revision_count"],
        "conflicting_entity_count": current["conflicting_entity_count"],
        "market_growth": None,
        "meaning": "changed_or_missing_captures_never_establish_entry_departure_or_saturation",
    }


def explain_changes(report: dict) -> list[str]:
    """Pure French reading path shared by the private view and text CLI."""
    states = {
        "watch": "À surveiller",
        "investigate": "À approfondir",
        "ready_to_review": "Prêt pour examen",
    }
    state = report["review_state"]
    lines = [
        f"Produit {report['product_id']} ; pays cible {report['target_country']}.",
        f"Du {report['previous_as_of']} au {report['as_of']}.",
        f"État de lecture : {states.get(state['previous'], 'État non reconnu')} → "
        f"{states.get(state['current'], 'État non reconnu')}.",
        "Cet état guide l’examen. Il ne prouve pas la demande et n’autorise aucun lancement.",
    ]
    if report["selection_changes"]:
        lines.append(
            "La sélection ou les hypothèses ont changé. Les différences de contexte "
            "ne constituent pas une croissance mesurée."
        )
    measured = [
        row for row in report["trajectory"]["windows"] if row["comparison_status"] == "measured"
    ]
    if measured:
        for row in measured:
            lines.append(
                f"Sur {row['window_hours']} heures, le rythme du même panel de "
                f"{row['continuing_entity_count']} vidéo(s) est multiplié par "
                f"{row['pace_ratio_lower']:.3g} à {row['pace_ratio_upper']:.3g}, "
                "sur les seuls intervalles mesurés comparables."
            )
    else:
        lines.append(
            "Le changement de rythme entre les deux dates n’est pas comparable avec "
            "les preuves disponibles. Les quantités observées restent consultables."
        )
        reasons = {
            "changed_selection_or_configuration": "La sélection ou les hypothèses diffèrent.",
            "requested_cutoffs_do_not_match_adjacent_kernel_windows": (
                "Ces dates ne délimitent pas deux périodes adjacentes de 24, 72 ou 168 heures."
            ),
            "baseline_reference_preview_incomplete": (
                "Les références de la première période dépassent la limite de comparaison."
            ),
            "baseline_evidence_changed_or_newly_available": (
                "Des preuves de la première période ont changé "
                "ou sont devenues disponibles plus tard."
            ),
            "counter_corrections_limit_comparison": (
                "Des corrections ou remises à zéro des compteurs limitent la comparaison."
            ),
            "source_context_unknown_or_incomplete": (
                "Le contexte de diffusion est inconnu ou le panel est incomplet."
            ),
            "insufficient_comparable_panel": (
                "Les mêmes vidéos n’ont pas assez d’intervalles comparables aux deux périodes."
            ),
            "zero_or_uncertain_previous_pace": (
                "Le rythme précédent est nul ou trop incertain pour calculer un rapport."
            ),
        }
        separation = utc(report["as_of"]) - utc(report["previous_as_of"])
        matching = next(
            (
                row
                for row in report["trajectory"]["windows"]
                if timedelta(hours=row["window_hours"]) == separation
            ),
            None,
        )
        reason = (
            matching["reason"]
            if matching
            else "requested_cutoffs_do_not_match_adjacent_kernel_windows"
        )
        lines.append(
            reasons.get(reason, "La raison détaillée reste disponible dans les références.")
        )
    if any(row["newly_observed_entity_count"] for row in report["trajectory"]["windows"]):
        lines.append(
            "Des vidéos ont été observées pour la première fois dans la fenêtre. "
            "Cela ne prouve ni leur apparition sur le marché ni une croissance."
        )
    if report["economics"]["status"] == "unavailable":
        lines.append(
            "La comparaison économique est indisponible pour au moins une date ou "
            "révision sélectionnée. Aucun ancien fait n’est reconstitué."
        )
    else:
        lines.append(
            "Les faits économiques et les hypothèses sont comparés séparément. "
            "La validité des devis est vérifiée maintenant, pas à la date passée."
        )
        if report["economics"]["facts_changed"]:
            lines.append("Les faits économiques déclarés dans les dossiers sélectionnés diffèrent.")
        if report["economics"]["assumptions_changed"]:
            lines.append(
                "Les hypothèses économiques ont changé ; elles ne sont pas des ventes mesurées."
            )
    missing = report["evidence_changes"]["missing"]
    if missing["added_count"] or missing["removed_count"]:
        lines.append(
            f"Informations manquantes : {missing['added_count']} point(s) désormais signalé(s), "
            f"{missing['removed_count']} point(s) qui ne sont plus signalé(s)."
        )
    lines.append(
        "La concurrence décrit les candidats observés dans le périmètre accessible. "
        "Elle ne mesure pas la croissance globale du marché."
    )
    lines.append("L’audience du pays cible et les achats réels restent inconnus.")
    return lines

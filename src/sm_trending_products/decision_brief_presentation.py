"""A bounded, pure French reading view of an authoritative decision brief.

This module reads no environment, source, database or session. Adapters redact
current credentials before calling it. It never recomputes an analytical result.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from html import escape
from typing import Any

from .display_safety import sanitize_display, scrub_display_text

MAX_PRESENTATION_BYTES = 2 * 1024 * 1024
MAX_PRESENTATION_NODES = 40_000

_STATES = {
    "watch": "À surveiller : aucune croissance observée strictement positive en borne basse.",
    "investigate": "À approfondir : croissance observée ; "
    "les conditions déclarées de préparation commerciale ne sont pas réunies.",
    "ready_to_review": "Prêt à examiner : croissance observée et scénarios déclarés conformes "
    "à leur objectif dans un dossier actuellement prêt.",
}
_DIMENSIONS = {
    "trends": "Attention observée",
    "competition": "Concurrence observée",
    "comparables": "Produits comparables",
    "themes": "Textes fournis",
    "variants": "Identité et variantes",
    "economics": "Hypothèses économiques",
    "demand": "Achats réels",
    "audience": "Audience",
}
_REASONS = {
    "positive_observed_attention": "Croissance positive mesurée sur des intervalles comparables.",
    "incomplete_observed_window": "Fenêtre partiellement mesurée ; les périodes absentes "
    "ne valent pas zéro.",
    "counter_corrections_limit_comparison": "Des corrections de compteurs limitent la comparaison.",
    "dominant_creator_contribution": "Un identifiant de créateur concentre au moins la moitié "
    "de la croissance mesurée ; son indépendance n’est pas vérifiée.",
    "creator_removal_not_measurable": "L’effet du retrait du principal contributeur "
    "n’est pas mesurable.",
    "supplied_text_mentions_available_not_buyer_demand": "Des mentions candidates sont "
    "effectivement citées dans les textes fournis. Elles ne prouvent ni le bénéfice "
    "décrit ni une demande d’acheteurs.",
    "supplied_texts_without_candidate_theme_mentions": "Des textes fournis sont disponibles "
    "sans mention candidate interprétée. Cela ne prouve pas une absence de demande.",
    "negated_theme_mentions_require_context_review": "Des mentions sont marquées comme "
    "niées. Examiner les citations et leur contexte ; elles ne sont pas des éléments "
    "favorables ni une preuve d’absence de demande.",
    "ambiguous_theme_mentions_require_context_review": "Des mentions sont ambiguës. "
    "Examiner les citations et leur contexte avant de les interpréter.",
    "variant_facts_incomplete": "Des informations sur les variantes sont incomplètes.",
    "candidate_identity_or_variant_conflicts_require_review": "Des candidats présentent des "
    "conflits d’identité ou de variante à examiner.",
    "bounded_identity_panel_incomplete": "La limite de lecture laisse des candidats non examinés.",
    "no_current_matching_metadata": "Aucune métadonnée actuelle ne permet ce rapprochement.",
    "scenario_costs_incomplete": "Certains scénarios ont des coûts inconnus ; leur résultat "
    "reste inconnu.",
    "declared_scenarios_miss_profit_target": "Au moins un scénario calculable manque "
    "l’objectif de contribution déclaré.",
    "dossier_not_ready": "Le dossier sélectionné n’est pas actuellement prêt à être examiné.",
    "cash_volume_or_timing_unknown": "Le volume ou le calendrier de trésorerie reste inconnu.",
    "declared_scenarios_meet_target_not_actual_profit": "Tous les scénarios déclarés complets "
    "atteignent leur objectif ; ce calcul ne mesure pas un bénéfice réalisé.",
    "actual_purchases_and_product_market_fit_not_established": "Achats réels et adéquation "
    "au marché non établis.",
    "target_country_audience_coverage_unknown": "La géographie de l’audience reste inconnue.",
    "select_reviewed_category_taxonomy": "Comparaison non demandée : "
    "choisir une catégorie révisée.",
    "select_dossier_revision_and_assumption_profile": "Économie non demandée : sélectionner "
    "un dossier, sa révision exacte et des scénarios.",
    "select_reviewed_variant_profile": "Rapprochement non demandé : choisir un profil révisé.",
    "no_eligible_supplied_text_at_cutoff": "Aucun texte fourni éligible à cette date.",
    "unsupported_or_unknown_language": "Certains textes ont une langue non prise en charge "
    "ou inconnue ; ils ne prouvent pas l’absence d’un thème.",
    "theme_language_coverage_incomplete": "La couverture des langues est incomplète ; "
    "l’absence d’un thème ne peut pas être déduite pour les textes non lus.",
    "text_mention_limit_reached": "La limite de lecture des mentions est atteinte.",
    "source_evidence_unavailable": "Une partie des sources est actuellement indisponible.",
    "bounded_page_has_more": "La page bornée ne couvre pas tous les textes éligibles.",
    "no_fresh_declared_comparable_scope": "Aucun périmètre récent et déclaré ne permet "
    "une comparaison de concurrence.",
    "unknown_observed_country": "Le pays observé manque ; le pays cible ne le remplace pas.",
    "undeclared_query_scope": "Le périmètre de recherche observé manque.",
    "undeclared_coverage_scope": "La couverture observée manque.",
    "unknown_distribution_scope": "Le contexte de diffusion observé manque.",
    "no_eligible_observations": "Aucune observation éligible dans le périmètre sélectionné.",
    "selected_product_not_in_panel": "Ce produit ne figure pas dans le panel sélectionné.",
    "selected_dossier_revision_not_available_at_cutoff": "La révision sélectionnée du dossier "
    "n’était pas disponible à la date de coupure.",
    "taxonomy_not_available_at_cutoff": "La catégorie révisée n’était pas disponible à cette date.",
    "content_taxonomy_not_available_at_cutoff": "Les règles de lecture des textes "
    "n’étaient pas disponibles à cette date.",
}
_ACTIONS = {
    "economics": "Vérifier le fournisseur, les coûts, les taxes, les retours et les échéances "
    "de trésorerie dans la révision exacte du dossier.",
    "variants": "Vérifier les identifiants et les conflits de variantes avant de regrouper "
    "les observations.",
    "trends": "Examiner des captures comparables supplémentaires et les contributeurs connus.",
    "comparables": "Vérifier la catégorie et disposer d’assez de produits comparables.",
    "themes": "Fournir des textes autorisés ; vérifier citations, contexte et langues couvertes.",
    "competition": "Documenter des périmètres comparables de recherche, pays, diffusion et prix.",
    "demand": "Définir un futur test commercial à examiner ; l’attention seule ne prouve "
    "aucun achat. Ce dossier n’exécute pas ce test.",
    "audience": "Obtenir une géographie directement observée avant d’affirmer une demande locale.",
}
_COST_LABELS = {
    "goods": "marchandises",
    "shipping": "transport",
    "payment_fees": "frais de paiement",
    "nonrecoverable_taxes": "taxes non récupérables",
    "returns_allowance": "provision pour retours",
    "fulfillment": "préparation et livraison",
}


@dataclass(frozen=True)
class BriefPresentation:
    """The complete display contract shared by UI, text and HTML adapters."""

    title: str
    state: str
    context: tuple[str, ...]
    groups: tuple[tuple[str, tuple[str, ...]], ...]
    technical: dict[str, Any]


def _value(value: Any) -> str:
    return "inconnu" if value is None else str(value)


def _reason(code: Any) -> str:
    if code in {
        "product_cohorts_not_initialized",
        "transcript_insights_not_initialized",
        "matching_identifier_indexes_not_initialized",
        "commerce_not_initialized",
    }:
        return "Module non initialisé ; aucune donnée de remplacement n’est inventée."
    return _REASONS.get(
        str(code),
        "Information non interprétée dans cette version ; "
        "consulter le code et la référence dans les détails techniques.",
    )


def _note(row: dict) -> str:
    line = f"{_DIMENSIONS.get(str(row.get('dimension')), 'Autre information')} : "
    line += _reason(row.get("code"))
    quantities = []
    for key, label in (
        ("window_hours", "fenêtre en h"),
        ("missing_entities", "vidéos absentes"),
        ("candidate_count", "candidats"),
        ("distinct_text_count", "textes distincts"),
        ("candidate_mention_count", "mentions candidates citées"),
        ("mention_count", "mentions citées"),
        ("complete_points", "scénarios complets"),
        ("share_lower", "part minimale du contributeur"),
    ):
        if key in row:
            quantities.append(f"{label} : {_value(row[key])}")
    if "growth_lower" in row:
        quantities.append(
            f"croissance : {_value(row['growth_lower'])} à "
            f"{_value(row.get('growth_upper'))} {_value(row.get('unit'))}"
        )
    return line + (" (" + "; ".join(quantities) + ")" if quantities else "")


def present_brief(report: dict, *, secret_values: Sequence[str] = ()) -> BriefPresentation:
    """Compose from authoritative structure, then redact only displayed values.

    Adapters supply current-secret values on each explicit action. This
    pure function additionally strips URL credentials and secret-bearing metadata.
    Oversized reports fail explicitly; they are never shortened into a success.
    """
    technical = sanitize_display(
        report,
        secret_values=secret_values,
        max_bytes=MAX_PRESENTATION_BYTES,
        max_nodes=MAX_PRESENTATION_NODES,
    )
    selection = report.get("selection", {})
    context = (
        f"Pays cible : {_value(report.get('target_country'))}. Pays de l’audience : "
        f"{_value(report.get('observed_audience_country'))}. Sa couverture reste inconnue.",
        f"Date de coupure des observations : {_value(report.get('as_of'))}.",
        f"Marché déclaré du produit : {_value(report.get('product_target_market'))}. "
        "Il peut différer du pays cible sélectionné.",
        f"Dossier sélectionné : {_value(selection.get('dossier_id'))} ; "
        f"révision exacte : {_value(selection.get('expected_revision'))}.",
        "Données synthétiques : simulation, sans preuve de demande réelle."
        if report.get("synthetic") is True
        else "Données du périmètre réel accessible, couverture limitée."
        if report.get("synthetic") is False
        else "Nature réelle ou synthétique des données inconnue.",
        "Ces états guident l’examen humain. Ils n’autorisent aucun lancement et ne prédisent "
        "ni achats ni bénéfice. Les limites et éléments défavorables restent applicables.",
    )
    groups: list[tuple[str, tuple[str, ...]]] = []
    measured = []
    sections = report.get("sections", {})
    trends = sections.get("trends", {}).get("report") or {}
    for window in trends.get("windows", []):
        measured.append(
            f"{_value(window.get('window_hours'))} h : croissance observée de "
            f"{_value(window.get('observed_growth_lower'))} à "
            f"{_value(window.get('observed_growth_upper'))} "
            f"{_value(window.get('unit'))} "
            f"{'(vues vidéo)' if window.get('unit') == 'count' else '(unité originale)'} ; "
            f"vidéos mesurées : {_value(window.get('measured_entity_count'))} / "
            f"{_value(window.get('known_entity_count'))} connues ; "
            f"intervalles comparables : {_value(window.get('comparable_interval_count'))}."
        )
    if measured:
        measured.append(
            "Fenêtres agrégées qui se chevauchent, pas une série chronologique. "
            "Aucune interpolation des périodes absentes ni addition avec la portée publicitaire."
        )
        groups.append(("Quantités observées", tuple(measured)))
    competition = sections.get("competition", {}).get("report") or {}
    if competition:
        comparable = competition.get("status") == "observed"
        lines = [
            "Concurrence : périmètres comparables observés."
            if comparable
            else "Concurrence : comparabilité non établie.",
            f"Observations éligibles : {_value(competition.get('eligible_observation_count'))}. "
            "Les comptes portent sur les candidats observés, pas sur tout le marché.",
        ]
        for scope in competition.get("scopes", []):
            lines.append(
                f"Pays observé : {_value(scope.get('observed_country'))} ; "
                f"diffusion : {_value(scope.get('distribution'))} ; "
                f"captures : {_value(scope.get('capture_count'))} ; "
                f"vendeurs observés : {_value(scope.get('observed_seller_count'))} ; "
                f"annonceurs observés : {_value(scope.get('observed_advertiser_count'))}. "
                "Ces identifiants ne prouvent pas l’indépendance des personnes."
            )
        groups.append(("Périmètre de concurrence", tuple(lines)))
    themes = sections.get("themes", {}).get("report") or {}
    if themes:
        groups.append(
            (
                "Couverture des textes",
                (
                    f"Textes distincts : {_value(themes.get('distinct_text_count'))} ; "
                    "langue inconnue ou non couverte : "
                    f"{_value(themes.get('unsupported_text_count'))} ; "
                    f"textes partiellement lus : {_value(themes.get('truncated_text_count'))}.",
                    "Les mentions citées décrivent les textes fournis. Elles ne mesurent "
                    "ni achats, ni efficacité commerciale ; des identifiants de créateur "
                    "ne prouvent pas des personnes indépendantes.",
                ),
            )
        )
    economics = sections.get("economics", {}).get("report") or {}
    if economics:
        currency = _value(economics.get("currency"))
        lines = [
            f"État actuel des devis évalué à : {_value(economics.get('evaluated_at'))}. "
            "Cette date ne reconstitue pas leur validité historique."
        ]
        for point in economics.get("scenarios", []):
            values = point.get("economics", {})
            assumptions = point.get("assumptions", {})
            target = values.get("meets_target")
            result = (
                "objectif atteint"
                if target is True
                else "objectif manqué"
                if target is False
                else "résultat inconnu"
            )
            lines.append(
                f"Scénario {_value(point.get('scenario_id'))} : "
                f"{result} ; "
                f"contribution après publicité : {_value(values.get('post_ad_contribution'))} "
                f"{currency}/commande ; objectif déclaré : "
                f"{_value(assumptions.get('desired_profit'))} {currency}/commande ; "
                f"écart à cet objectif : {_value(values.get('target_surplus'))} "
                f"{currency}/commande. "
                f"Hypothèses : prix {_value(assumptions.get('retail_price'))}, "
                f"publicité {_value(assumptions.get('ad_cost'))} {currency}/commande. "
                "Il s’agit d’hypothèses, pas de ventes ou de bénéfices réalisés."
            )
            costs = assumptions.get("costs", {})
            tax = assumptions.get("prices_include_tax")
            tax_label = "oui" if tax is True else "non" if tax is False else "inconnu"
            lines.append(
                f"Hypothèses de coût du scénario {_value(point.get('scenario_id'))} : "
                + "; ".join(
                    f"{label} : {_value(costs.get(key))} {currency}/commande"
                    for key, label in _COST_LABELS.items()
                )
                + f" ; taxe incluse : {tax_label} ; "
                f"taxe par commande : {_value(assumptions.get('tax_per_order'))} {currency}. "
                "Les valeurs absentes restent inconnues."
            )
        groups.append(("Scénarios déclarés", tuple(lines)))
    for key, title in (
        ("support", "Éléments favorables mesurés"),
        ("contrary", "Éléments défavorables connus"),
        ("missing", "Informations manquantes"),
    ):
        note_lines = tuple(_note(row) for row in report.get(key, []))
        groups.append(
            (
                title,
                note_lines
                or (
                    "Aucun élément signalé dans ce rapport ; "
                    "cela ne prouve pas son absence dans le marché.",
                ),
            )
        )
    actions = tuple(
        _ACTIONS.get(
            str(row.get("dimension")),
            "Vérification non interprétée ; examiner ses motifs dans les détails techniques.",
        )
        for row in report.get("next_useful_facts", [])
    )
    groups.append(
        ("Vérifications utiles", actions or ("Aucune vérification proposée dans ce rapport.",))
    )
    # Redaction can change keys and scalar types. Never route through a sanitized
    # report, or sanitize the internal keys used to retrieve generated prose.
    human = sanitize_display(
        (
            f"Dossier de décision — {_value(report.get('name'))} "
            f"({_value(report.get('product_id'))})",
            _STATES.get(
                str(report.get("status")),
                "État non interprété dans cette version ; examiner les détails techniques.",
            ),
            context,
            groups,
        ),
        secret_values=secret_values,
        max_bytes=MAX_PRESENTATION_BYTES,
        max_nodes=MAX_PRESENTATION_NODES,
    )
    return BriefPresentation(
        title=human[0],
        state=human[1],
        context=tuple(human[2]),
        groups=tuple((title, tuple(lines)) for title, lines in human[3]),
        technical=technical,
    )


def _bounded(value: str) -> str:
    if len(value.encode("utf-8")) > MAX_PRESENTATION_BYTES:
        raise ValueError("brief_presentation_budget_exceeded")
    return value


def render_text(view: BriefPresentation, *, secret_values: Sequence[str] = ()) -> str:
    """Produce a bounded standalone French text with secondary provenance."""
    paragraphs = [view.title, view.state, *view.context]
    for title, lines in view.groups:
        paragraphs.extend(("", title, *(f"- {line}" for line in lines)))
    paragraphs.extend(
        (
            "",
            "Détails techniques (codes et références)",
            json.dumps(view.technical, ensure_ascii=False, sort_keys=True, default=str),
        )
    )
    return _bounded(scrub_display_text("\n".join(paragraphs), secret_values=secret_values))


def render_html(view: BriefPresentation, *, secret_values: Sequence[str] = ()) -> str:
    """Produce inert standalone HTML; escape every source-derived string."""
    parts = [
        '<!doctype html><html lang="fr"><meta charset="utf-8">',
        f"<title>{escape(view.title)}</title><body><h1>{escape(view.title)}</h1>",
        f"<p>{escape(view.state)}</p>",
    ]
    parts.extend(f"<p>{escape(line)}</p>" for line in view.context)
    for title, lines in view.groups:
        parts.append(f"<h2>{escape(title)}</h2><ul>")
        parts.extend(f"<li>{escape(line)}</li>" for line in lines)
        parts.append("</ul>")
    parts.extend(
        (
            "<details><summary>Détails techniques (codes et références)</summary><pre>",
            escape(json.dumps(view.technical, ensure_ascii=False, sort_keys=True, default=str)),
            "</pre></details></body></html>",
        )
    )
    return _bounded(scrub_display_text("".join(parts), secret_values=secret_values))

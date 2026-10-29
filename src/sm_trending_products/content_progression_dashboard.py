"""Private explicit French presentation; no source-derived session state."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st

from .display_safety import (
    current_display_secrets,
    display_json,
    sanitize_display,
    scrub_display_text,
)
from .product_display import product_labels

if TYPE_CHECKING:
    from .pipeline import Pipeline


_LABELS = {
    "time": "Temps et effort",
    "ease": "Facilité d’utilisation",
    "price": "Prix et valeur",
    "reliability": "Fiabilité",
    "portability": "Transport et poids",
    "demonstration": "Démonstration",
    "comparison": "Comparaison",
}
_KINDS = {
    "human_transcript": "Transcription fournie",
    "subtitles": "Sous-titres",
    "description": "Description",
}
_REASONS = {
    "unknown_creator_identity": "Identité de créateur inconnue",
    "insufficient_videos_or_known_creators": "Trop peu de vidéos ou de créateurs nommés",
    "shared_creator_between_groups": "Créateurs partagés entre les groupes",
    "unknown_video_age": "Âge de vidéo inconnu",
    "unknown_distribution_context": "Contexte de diffusion inconnu",
    "sparse_measured_coverage": "Couverture mesurée insuffisante",
    "incomparable_measured_durations": "Durées mesurées trop différentes",
    "incomparable_window_alignment": "Périodes mesurées trop décalées",
    "counter_corrections_in_window": "Compteur corrigé pendant la période",
}


def render_patterns(
    pipeline: Pipeline, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Thèmes cités et progression mesurée des vidéos")
    st.caption(
        "Compare les textes fournis avec l’attention observée sur les vidéos. "
        "Une association ne démontre aucune causalité, efficacité publicitaire ou vente. "
        "Un texte importé après le début de la mesure reste dans un groupe distinct. "
        "Les identifiants de créateurs ne prouvent pas qu’il s’agit de personnes indépendantes."
    )
    if not products:
        st.info("Sélectionnez un produit avec des vidéos validées et des textes fournis.")
        return
    try:
        navigation_secrets = current_display_secrets()
        identifiers = [row["product_id"] for row in products]
        labels = product_labels(identifiers, secret_values=navigation_secrets)
    except (ValueError, TypeError, RecursionError):
        st.error("La sélection est indisponible. Vérifiez les identifiants et leur affichage.")
        return
    product = st.selectbox(
        "Produit pour les thèmes et la progression",
        identifiers,
        key="research-patterns-product",
        format_func=labels.__getitem__,
    )
    hours = st.number_input(
        "Fenêtre observée, en heures", 1, 8760, 168, key="research-patterns-hours"
    )
    limit = st.number_input(
        "Maximum de textes candidats", 1, 200, 200, key="research-patterns-texts"
    )
    if st.button("Examiner les thèmes et la progression", key="research-patterns-inspect"):
        from .content_progression import ContentProgression

        output = st.empty()
        try:
            report = ContentProgression(
                pipeline.catalog, pipeline.history, pipeline.provenance
            ).describe(
                product,
                as_of,
                synthetic,
                window_hours=int(hours),
                max_texts=int(limit),
            )
            secrets = current_display_secrets()
            details = display_json(report, secret_values=secrets)
            with output.container():
                st.write(
                    scrub_display_text(
                        f"{report['distinct_text_count']} textes ; "
                        f"{report['eligible_video_count']} vidéos mesurées ; "
                        f"{report['stratum_count']} groupes de contexte.",
                        secret_values=secrets,
                    )
                )
                st.caption(
                    "Tous les thèmes examinés restent visibles. Les absences exigent une lecture "
                    "complète dans une langue prise en charge. "
                    "Les comparaisons insuffisantes restent indisponibles."
                )
                rows = []
                for theme in report["themes"]:
                    for group in theme["strata"] or [None]:
                        rows.append(
                            {
                                "Thème": _LABELS.get(theme["theme_id"], theme["label"]),
                                "Comparaison": "Association observée"
                                if group and group["status"] == "observed_association"
                                else "Indisponible",
                                "Explication": "; ".join(
                                    _REASONS.get(reason, "Condition inconnue")
                                    for reason in group["reasons"]
                                )
                                if group
                                else "Aucune mesure comparable",
                                "Vidéos avec le thème": group["present"]["video_count"]
                                if group
                                else 0,
                                "Vidéos sans le thème": group["absent"]["video_count"]
                                if group
                                else 0,
                                "Créateurs nommés avec / sans": (
                                    f"{group['present']['known_creator_count']} / "
                                    f"{group['absent']['known_creator_count']}"
                                )
                                if group
                                else "0 / 0",
                                "Texte connu au début": "Oui"
                                if group and group["timing_scope"] == "text_known_by_interval_start"
                                else "Non"
                                if group
                                else "Inconnu",
                                "Source du texte": _KINDS.get(group["source_kind"], "Inconnue")
                                if group
                                else "Inconnue",
                                "Langue": group["language"] if group else "Inconnue",
                                "Source des mesures": group["source"] if group else "Inconnue",
                                "Couverture minimale avec le thème": group["present"][
                                    "minimum_coverage_fraction"
                                ]
                                if group
                                else None,
                                "Couverture minimale sans le thème": group["absent"][
                                    "minimum_coverage_fraction"
                                ]
                                if group
                                else None,
                                "Écart moyen de vues par heure, borne basse": group[
                                    "difference_in_mean_rate_lower"
                                ]
                                if group
                                else None,
                                "Écart moyen de vues par heure, borne haute": group[
                                    "difference_in_mean_rate_upper"
                                ]
                                if group
                                else None,
                            }
                        )
                st.dataframe(sanitize_display(rows, secret_values=secrets), hide_index=True)
                st.caption(
                    "Les durées et la couverture doivent être proches pour comparer les groupes. "
                    "Ces règles de compatibilité ne constituent pas "
                    "un niveau de confiance statistique."
                )
                with st.expander("Citations exactes, dates, mesures et exclusions"):
                    st.json(details)
        except Exception:
            output.empty()
            st.error(
                "Cette comparaison est indisponible. Vérifiez l’initialisation des textes, "
                "la date, l’accès aux sources et les limites de lecture."
            )
            return

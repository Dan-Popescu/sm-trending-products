"""Selected-product briefs with explicit inputs and no retained source reports."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st

from .decision_brief_presentation import (
    MAX_PRESENTATION_BYTES,
    MAX_PRESENTATION_NODES,
    present_brief,
)
from .display_safety import current_display_secrets, display_json
from .json_input import MAX_INPUT_BYTES, object_from_bytes
from .product_display import product_labels


def render_brief(pipeline: Any, products: list[dict], as_of: datetime, synthetic: bool) -> None:
    st.subheader("Dossier de décision produit")
    st.caption(
        "Éléments mesurés, éléments défavorables et informations manquantes "
        "pour un pays cible explicite. "
        "Un dossier prêt appelle un examen humain ; il ne démontre pas une réussite commerciale."
    )
    if not products:
        st.info("Créer un produit et vérifier ses liens avec les observations avant de continuer.")
        return
    navigation_secrets = current_display_secrets()
    identifiers = [row["product_id"] for row in products]
    try:
        labels = product_labels(identifiers, secret_values=navigation_secrets)
    except (ValueError, TypeError, RecursionError):
        st.error("La sélection de produit ne peut pas être affichée en sécurité.")
        return
    product_id = st.selectbox(
        "Produit à examiner",
        identifiers,
        key="research-brief-product",
        format_func=labels.__getitem__,
    )
    country = st.text_input("Pays cible (par exemple FR)")
    st.caption(
        "Ce pays décrit le marché de vente visé. "
        "La géographie de l’audience doit être observée séparément."
    )
    with st.expander("Contexte révisé facultatif et limites de lecture"):
        taxonomy_id = st.text_input("Identifiant des catégories révisées (facultatif)")
        category_id = st.text_input("Identifiant de catégorie (facultatif)")
        dossier_id = st.text_input("Identifiant exact du dossier commercial (facultatif)")
        revision = int(
            st.number_input(
                "Révision exacte attendue du dossier", min_value=1, max_value=2_147_483_647, value=1
            )
        )
        scenarios = st.file_uploader(
            "Scénarios économiques JSON (facultatif, 128 Kio maximum)",
            type="json",
            key="brief_scenarios",
        )
        matching = st.file_uploader(
            "Profil révisé des variantes JSON (facultatif, 128 Kio maximum)",
            type="json",
            key="brief_matching",
        )
        rules = st.file_uploader(
            "Règles révisées des thèmes JSON (facultatif, 128 Kio maximum)",
            type="json",
            key="brief_rules",
        )
        query = st.text_input("Périmètre de recherche observé (facultatif)")
        source = st.text_input("Source observée (facultatif)")
        max_rows = int(
            st.number_input(
                "Limite de lignes historiques par composant",
                min_value=1,
                max_value=100_000,
                value=50_000,
            )
        )
        max_products = int(
            st.number_input(
                "Limite de produits comparables", min_value=1, max_value=10_000, value=1000
            )
        )
        text_limit = int(
            st.number_input(
                "Limite de textes fournis par page", min_value=1, max_value=200, value=100
            )
        )
    if not st.button("Construire le dossier de décision"):
        return
    try:
        from .commercial_sensitivity import ScenarioProfile
        from .content_insights import DEFAULT_TAXONOMY, ContentTaxonomy
        from .matching import MatchingProfile

        if (bool(dossier_id) != (scenarios is not None)) or (category_id and not taxonomy_id):
            raise ValueError(
                "Provide exact dossier/revision/scenarios together and category taxonomy"
            )

        def uploaded(item):
            if item.size > MAX_INPUT_BYTES:
                raise ValueError("Brief input is too large")
            return object_from_bytes(item.getvalue())

        profile = ScenarioProfile.model_validate(uploaded(scenarios)) if scenarios else None
        match_profile = MatchingProfile.model_validate(uploaded(matching)) if matching else None
        taxonomy = ContentTaxonomy.model_validate(uploaded(rules)) if rules else DEFAULT_TAXONOMY
        report = pipeline.decision_brief().describe(
            product_id,
            country,
            as_of,
            synthetic,
            taxonomy_id=taxonomy_id or None,
            category_id=category_id or None,
            dossier_id=dossier_id or None,
            expected_revision=revision if dossier_id else None,
            scenario_profile=profile,
            matching_profile=match_profile,
            content_taxonomy=taxonomy,
            query_scope=query or None,
            source=source or None,
            max_rows=max_rows,
            max_products=max_products,
            text_limit=text_limit,
        )
    except Exception:
        st.error(
            "Dossier indisponible. Vérifier le pays cible, la révision exacte du dossier, "
            "les fichiers JSON, la disponibilité des sources et les limites de lecture."
        )
        return
    try:
        secrets = current_display_secrets()
        view = present_brief(report, secret_values=secrets)
        technical_json = display_json(
            view.technical,
            secret_values=secrets,
            max_bytes=MAX_PRESENTATION_BYTES,
            max_nodes=MAX_PRESENTATION_NODES,
        )
    except (ValueError, TypeError, RecursionError):
        st.error(
            "Présentation indisponible : le dossier dépasse les limites de lecture "
            "ou son format est inconnu."
        )
        return
    st.text(view.title)
    st.write(view.state)
    for line in view.context:
        st.text(line)
    for title, lines in view.groups:
        st.subheader(title)
        for line in lines:
            st.text(line)
    with st.expander("Détails techniques : codes, références, hypothèses et provenance"):
        st.json(technical_json, expanded=False)

"""On-request private comparison; derived reports never enter session state."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import streamlit as st

from .decision_brief_cli import MAX_INPUT_BYTES, object_from_bytes
from .display_safety import current_display_secrets, sanitize_display, scrub_display_text
from .product_display import product_labels


def render_changes(pipeline: Any, products: list[dict], as_of: datetime, synthetic: bool) -> None:
    # An output container is explicitly cleared before every interaction, including
    # unsuccessful actions. Never retain a report or source text in session state.
    output = st.empty()
    output.empty()
    st.subheader("Ce qui a changé entre deux analyses")
    st.caption(
        "Choisissez un produit, un pays cible et deux dates de connaissance. "
        "Les données manquantes restent inconnues ; le pays cible ne décrit pas l’audience."
    )
    if not products:
        st.info("Créez un produit et examinez ses liens de preuve pour commencer.")
        return
    try:
        navigation_secrets = current_display_secrets()
        product_ids = [row["product_id"] for row in products]
        labels = product_labels(product_ids, secret_values=navigation_secrets)
    except Exception:
        st.error(
            "Sélection indisponible : les libellés protégés ne permettent pas "
            "de distinguer les produits."
        )
        return
    product = st.selectbox(
        "Produit à comparer",
        product_ids,
        key="changes_product",
        format_func=labels.__getitem__,
    )
    country = st.text_input("Pays cible, par exemple FR", key="changes_country")
    previous = st.text_input(
        "Première date de connaissance (avec fuseau horaire)",
        value=(as_of - timedelta(hours=24)).isoformat(),
        key="changes_previous",
    )
    st.caption(
        f"Seconde date de connaissance : {as_of.isoformat()}. "
        "La première date proposée est 24 heures plus tôt ; vous pouvez la modifier."
    )
    with st.expander("Sélections explicites et limites de lecture"):
        prior_upload = st.file_uploader(
            "Sélection de la première analyse (JSON, 128 Kio maximum)",
            type="json",
            key="changes_prior_selection",
        )
        current_upload = st.file_uploader(
            "Sélection de la seconde analyse (facultatif)",
            type="json",
            key="changes_current_selection",
        )
        st.caption(
            "Sans second fichier, la même sélection est utilisée aux deux dates. "
            "Un dossier économique exige son identifiant, sa révision exacte et ses hypothèses."
        )
        max_rows = int(
            st.number_input(
                "Lignes historiques par composant",
                min_value=1,
                max_value=100_000,
                value=50_000,
                key="changes_rows",
            )
        )
        max_products = int(
            st.number_input(
                "Produits du panel de comparaison",
                min_value=1,
                max_value=10_000,
                value=1000,
                key="changes_products",
            )
        )
        text_limit = int(
            st.number_input(
                "Textes fournis par date",
                min_value=1,
                max_value=200,
                value=100,
                key="changes_texts",
            )
        )
    if not st.button("Comparer les deux analyses", key="changes_build"):
        return
    try:
        from .contracts import canonical, utc
        from .decision_changes import (
            MAX_OUTPUT_BYTES,
            ChangesSelection,
            DecisionChanges,
            explain_changes,
        )

        def selection(upload):
            if upload is None:
                return None
            if upload.size > MAX_INPUT_BYTES:
                raise ValueError("Selection too large")
            return ChangesSelection.model_validate(object_from_bytes(upload.getvalue()))

        report = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
            product,
            country,
            utc(previous),
            as_of,
            synthetic,
            previous_selection=selection(prior_upload),
            current_selection=selection(current_upload),
            max_rows=max_rows,
            max_products=max_products,
            text_limit=text_limit,
        )
        secrets = current_display_secrets()
        lines = [
            scrub_display_text(sanitize_display(line, secret_values=secrets), secret_values=secrets)
            for line in explain_changes(report)
        ]
        report = sanitize_display(report, secret_values=secrets, max_bytes=MAX_OUTPUT_BYTES)
        details = scrub_display_text(canonical(report), secret_values=secrets)
        if len(details.encode()) > MAX_OUTPUT_BYTES:
            raise ValueError("Decision changes display exceeds budget")
        json.loads(details)
    except Exception:
        st.error(
            "Comparaison indisponible. Vérifiez les dates, le pays, la sélection exacte, "
            "les autorisations des sources et les limites de lecture."
        )
        return
    with output.container():
        for line in lines:
            st.write(line)
        with st.expander("Références, quantités et raisons détaillées"):
            st.json(details, expanded=False)

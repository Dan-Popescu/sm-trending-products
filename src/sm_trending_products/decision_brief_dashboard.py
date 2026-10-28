"""Selected-product briefs with explicit inputs and no retained source reports."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st

from .decision_brief_cli import MAX_INPUT_BYTES, object_from_bytes


def render_brief(pipeline: Any, products: list[dict], as_of: datetime, synthetic: bool) -> None:
    st.subheader("Product decision brief")
    st.caption(
        "Measured support, contrary evidence and missing facts for an explicit target country. "
        "Readiness means ready for human review; it does not establish a winning product."
    )
    if not products:
        st.info("Create a product and review its evidence links first.")
        return
    product_id = st.selectbox("Product for decision brief", [row["product_id"] for row in products])
    country = st.text_input("Target country (for example FR)")
    st.caption("This country describes the intended sales market, not the measured audience.")
    with st.expander("Optional reviewed context and bounded reads"):
        taxonomy_id = st.text_input("Reviewed category taxonomy ID (optional)")
        category_id = st.text_input("Category ID (optional)")
        dossier_id = st.text_input("Exact launch dossier ID (optional)")
        revision = int(
            st.number_input(
                "Expected dossier revision", min_value=1, max_value=2_147_483_647, value=1
            )
        )
        scenarios = st.file_uploader(
            "Economic scenario JSON (optional, 128 KiB maximum)", type="json", key="brief_scenarios"
        )
        matching = st.file_uploader(
            "Reviewed variant profile JSON (optional, 128 KiB maximum)",
            type="json",
            key="brief_matching",
        )
        rules = st.file_uploader(
            "Reviewed theme taxonomy JSON (optional, 128 KiB maximum)",
            type="json",
            key="brief_rules",
        )
        query = st.text_input("Observed query scope (optional)")
        source = st.text_input("Observed source (optional)")
        max_rows = int(
            st.number_input(
                "Brief history row budget", min_value=1, max_value=100_000, value=50_000
            )
        )
        max_products = int(
            st.number_input("Brief peer panel budget", min_value=1, max_value=10_000, value=1000)
        )
        text_limit = int(
            st.number_input("Brief supplied-text page size", min_value=1, max_value=200, value=100)
        )
    if not st.button("Build decision brief"):
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
            "Brief unavailable. Check the target country, exact dossier revision, "
            "JSON inputs, source availability and read budgets."
        )
        return
    from .collector import redact
    from .dashboard import display_safe

    report = display_safe(redact(report))
    st.write("Review state:", report["status"])
    for field, title in (
        ("support", "Measured support"),
        ("contrary", "Contrary evidence"),
        ("missing", "Missing information"),
        ("next_useful_facts", "Next useful facts"),
    ):
        st.markdown(f"**{title}**")
        st.json(display_safe(report.get(field, [])), expanded=False)
    with st.expander("Complete evidence, assumptions and cutoffs"):
        st.json(display_safe(report), expanded=False)

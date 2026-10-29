"""One private entry point for on-demand decision research."""

from __future__ import annotations

from datetime import datetime
from importlib import import_module
from typing import Any

import streamlit as st

VIEWS = {
    "Decision brief": ("decision_brief_dashboard", "render_brief"),
    "Changes between analyses": ("decision_changes_dashboard", "render_changes"),
    "Comparable products": ("cohort_research_dashboard", "render_cohorts"),
    "Economic scenarios": ("economic_research_dashboard", "render_economics"),
    "Competition and prices": ("competition_research_dashboard", "render_competition"),
    "Quoted content themes": ("content_research_dashboard", "render_content"),
    "Useful supplied texts": ("text_selection_dashboard", "render_text_selection"),
    "Governed validation": ("research_validation_dashboard", "render_validation"),
}


def render_research(pipeline: Any, products: list[dict], as_of: datetime, synthetic: bool) -> None:
    st.header("Decision research")
    st.caption(
        "Reports run on request. Missing evidence stays unknown; "
        "attention does not establish sales."
    )
    view = st.selectbox("Research report", list(VIEWS))
    module, function = VIEWS[view]
    render = getattr(import_module(f".{module}", __package__), function)
    if view == "Governed validation":
        render(pipeline, synthetic)
    elif view == "Economic scenarios":
        render(pipeline, products, synthetic)
    else:
        render(pipeline, products, as_of, synthetic)

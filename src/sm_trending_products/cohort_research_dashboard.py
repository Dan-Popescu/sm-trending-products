"""On-demand cohort research and explicit category review for the private dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st

from .cohort_research_cli import METRICS, parse_taxonomy_categories
from .product_cohorts import CohortInsights

if TYPE_CHECKING:
    from .pipeline import Pipeline


def _safe(value):
    from .collector import redact
    from .dashboard import display_safe

    return display_safe(redact(value))


def _safe_details(value) -> None:

    with st.expander("Revision, coverage and evidence details"):
        st.json(_safe(value))


def render_cohorts(
    pipeline: Pipeline, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Reviewed categories and observed product peers")
    st.caption(
        "Country is the declared product target market. Audience country and coverage are unknown. "
        "Source and distribution remain mixed or unknown assessment aggregates. "
        "Percentiles describe this observed panel; "
        "they are not market demand or sales probabilities."
    )
    service = CohortInsights(pipeline.catalog)
    task = st.radio(
        "Cohort task",
        ["Compare products", "Review taxonomy", "Assign category", "Initialize cohorts"],
    )
    if task == "Initialize cohorts":
        st.info(
            "Initialization adds optional cohort tables. Reading a report never initializes them."
        )
        if st.button("Initialize cohort module"):
            try:
                result = service.initialize()
                st.success("Cohort module initialized.")
                _safe_details(result)
            except Exception:
                st.error("Cohort initialization is unavailable. Check the database configuration.")
        return
    taxonomy_id = st.text_input("Taxonomy ID", value="physical")
    if task == "Compare products":
        category_id = st.text_input("Category filter (blank = all)")
        country = st.text_input("Target-country filter (blank = all, for example FR)")
        metric = st.selectbox("Cohort metric", METRICS)
        maximum = int(
            st.number_input(
                "Maximum panel products", min_value=1, max_value=10000, value=1000, step=1
            )
        )
        minimum = int(
            st.number_input(
                "Minimum independent peers", min_value=2, max_value=1000, value=3, step=1
            )
        )
        age = float(
            st.number_input(
                "Maximum assessment age in hours", min_value=0.1, max_value=8760.0, value=72.0
            )
        )
        if st.button("Compare observed panel"):
            try:
                report = service.compare(
                    as_of,
                    synthetic,
                    taxonomy_id=taxonomy_id,
                    category_id=category_id or None,
                    country=country or None,
                    metric=metric,
                    max_products=maximum,
                    min_peers=minimum,
                    max_assessment_age_hours=age,
                )
                report = _safe(report)
            except Exception:
                st.error(
                    "Comparison unavailable. Check taxonomy initialization, cutoff and bounds."
                )
                return
            st.caption(
                "Eligibility belongs to each assessment cutoff; no live source-grant "
                "reassessment is claimed. Current retirement guards still apply."
            )
            if metric in {"growth_per_hour", "view_count"}:
                st.warning(
                    "Original counter units are uncertified. These values receive no percentile."
                )
            if not report["rows"]:
                st.info("No declared products match this bounded panel.")
            if any(row["percentile"] is None for row in report["rows"]):
                st.warning(
                    "Some comparisons are unavailable. Missing, stale, tied or sparse peers "
                    "remain explicit below."
                )
            st.dataframe(
                [
                    {
                        "Product": row["product_id"],
                        "Category": row["category_id"],
                        "Assignment revision": row["assignment_revision"],
                        "Target country": row["target_country"] or "Unknown",
                        "Audience country": "Unknown",
                        "Age bucket": row["age_bucket"],
                        "Metric": row["metric"],
                        "Unit": row["metric_unit"],
                        "Value": row["value"],
                        "Observed-panel percentile": row["percentile"],
                        "Independent peers": row["peer_count"],
                        "Comparability": row["comparability"],
                        "Unavailable reason": row["reason"],
                        "Assessment cutoff": row["assessed_at"],
                    }
                    for row in report["rows"]
                ],
                hide_index=True,
            )
            _safe_details(report)
        return
    if task == "Review taxonomy":
        if st.button("Show taxonomy revision"):
            try:
                record = service.taxonomy(taxonomy_id, as_of)
                record = _safe(record)
                if record is None:
                    st.info(
                        "No taxonomy was available at this cutoff. "
                        "A new taxonomy expects revision 0."
                    )
                else:
                    st.write(f"Current taxonomy revision: {record['revision']}")
                    st.dataframe(record["categories"], hide_index=True)
                    _safe_details(record)
            except Exception:
                st.error("Taxonomy unavailable. Initialize the optional module explicitly first.")
        categories = st.text_area(
            "Reviewed category JSON", value='[{"category_id":"tools","name":"Tools"}]'
        )
        revision = int(
            st.number_input(
                "Expected taxonomy revision", min_value=0, max_value=2_147_483_646, value=0, step=1
            )
        )
        request = st.text_input("Taxonomy request ID", value="taxonomy-review-1")
        reviewer = st.text_input("Taxonomy reviewer")
        rationale = st.text_area("Taxonomy review rationale")
        st.caption(
            "Keep the request ID to replay the same save. Change it for a deliberate new revision."
        )
        if st.button("Save reviewed taxonomy"):
            try:
                parsed = parse_taxonomy_categories(categories.encode())
                record = service.save_taxonomy(
                    taxonomy_id, parsed, revision, request, reviewer=reviewer, rationale=rationale
                )
                record = _safe(record)
                st.success(f"Saved taxonomy revision {record['revision']}.")
                _safe_details(record)
            except Exception:
                st.error(
                    "Taxonomy save unavailable. Check JSON, reviewer, request ID and expected "
                    "revision; a concurrent change requires reviewing the current revision."
                )
        return
    if not products:
        st.info("Create a product before reviewing its category.")
        return
    product = st.selectbox(
        "Product for category review", [row["product_id"] for row in products], format_func=_safe
    )
    taxonomy_revision = int(
        st.number_input(
            "Reviewed taxonomy revision", min_value=1, max_value=2_147_483_647, value=1, step=1
        )
    )
    withdraw = st.checkbox("Withdraw category assignment")
    category = st.text_input("Reviewed category ID", disabled=withdraw)
    revision = int(
        st.number_input(
            "Expected assignment revision", min_value=0, max_value=2_147_483_646, value=0, step=1
        )
    )
    request = st.text_input("Assignment request ID", value="category-review-1")
    reviewer = st.text_input("Assignment reviewer")
    rationale = st.text_area("Assignment review rationale")
    st.caption(
        "Use revision 0 for a new assignment. The comparison panel shows current assignment "
        "revisions. Reuse a request ID only to replay identical input."
    )
    if st.button("Save category assignment"):
        try:
            record = service.assign(
                product,
                taxonomy_id,
                None if withdraw else category,
                revision,
                request,
                taxonomy_revision=taxonomy_revision,
                synthetic=synthetic,
                reviewer=reviewer,
                rationale=rationale,
            )
            record = _safe(record)
            st.success(f"Saved assignment revision {record['revision']}.")
            _safe_details(record)
        except Exception:
            st.error(
                "Assignment save unavailable. Check category, reviewed taxonomy revision, "
                "request ID and expected assignment revision."
            )

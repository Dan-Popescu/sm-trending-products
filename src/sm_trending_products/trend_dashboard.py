"""On-demand descriptive trend windows for the private review workspace."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st

if TYPE_CHECKING:
    from .trend_insights import TrendInsights


def render_trends(
    insights: TrendInsights, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Product growth over 1, 3 and 7 days")
    st.caption(
        "Uses already collected observations. Missing intervals remain unknown. "
        "This does not predict sales."
    )
    if not products:
        st.info("Create a product and review its video links first.")
        return
    product = st.selectbox("Product for trend details", [row["product_id"] for row in products])
    diagnostics = st.checkbox("Compare periods and inspect dominant contributors", value=False)
    if st.button("Inspect multi-day growth"):
        try:
            report = insights.describe(product, as_of, synthetic, diagnostics=diagnostics)
        except Exception:
            st.error(
                "Trend details are unavailable. Check evidence availability "
                "and the bounded history budget."
            )
            return
        st.dataframe(
            [
                {
                    "Hours": row["window_hours"],
                    "Measured growth (lower)": row["observed_growth_lower"],
                    "Measured growth (upper)": row["observed_growth_upper"],
                    "Shape": row["shape"],
                    "Evidence quality": row["evidence_quality"],
                    "Known videos": row["known_entity_count"],
                    "Measured videos": row["measured_entity_count"],
                }
                for row in report["windows"]
            ],
            hide_index=True,
        )
        if diagnostics:
            st.caption(
                "Continuing-video comparisons use measured time only. Removing a contributor "
                "is a robustness check, not a prediction of what would have happened."
            )
            from .dashboard import display_safe

            st.json(display_safe(report["diagnostics"]))
        with st.expander("Coverage, identity and evidence details"):
            from .dashboard import display_safe

            st.json(display_safe(report))

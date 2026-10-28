"""Optional matching explanations, computed only on an explicit review action."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st

if TYPE_CHECKING:
    from .matching import ProductMatcher


def render_matching(
    matcher: ProductMatcher, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Multilingual match explanations")
    st.caption(
        "Suggestions need your review. Scores are text heuristics, "
        "not verified ownership or sales probabilities."
    )
    if not products:
        st.info("Create a product first.")
        return
    product_id = st.selectbox("Product to match", [row["product_id"] for row in products])
    aliases = st.text_area("Alternative product names or translations (one per line)")
    brand = st.text_input("Known brand (optional)")
    ean = st.text_input("Known EAN barcode (optional)")
    capacity = st.text_input("Known capacity (optional, for example 350 ml)")
    if st.button("Explain candidate matches"):
        from .matching import MatchingProfile

        try:
            profile = MatchingProfile(
                aliases=tuple(line.strip() for line in aliases.splitlines() if line.strip()),
                brand=brand or None,
                identifiers={"ean": ean} if ean else {},
                variants={"capacity": capacity} if capacity else {},
            )
            report = matcher.suggest(product_id, as_of, synthetic, profile=profile, limit=20)
        except Exception:
            st.error(
                "Candidates could not be loaded. Check the profile and cutoff. "
                "EAN lookup requires explicit sm-trends match-products --initialize-indexes."
            )
            return
        if report.get("candidate_truncated") or report.get("ownership_truncated"):
            st.warning(
                "This search is partial. Missing or unresolved candidates "
                "are not a verified absence."
            )
        from .dashboard import display_safe

        st.json(display_safe(report))

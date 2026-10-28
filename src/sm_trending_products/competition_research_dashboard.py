"""On-demand competition coverage and price-pressure inspection."""

from __future__ import annotations

from datetime import datetime
from itertools import islice
from typing import TYPE_CHECKING

import streamlit as st

from .competition_insights import CompetitionInsights

PRICE_INTERVAL_PREVIEW_LIMIT = 200

if TYPE_CHECKING:
    from .pipeline import Pipeline


def _safe(value):
    from .collector import redact
    from .dashboard import display_safe

    return display_safe(redact(value))


def render_competition(
    pipeline: Pipeline, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Observed competition and compatible price changes")
    st.caption(
        "Newly observed sellers and advertisers are not market entrants. Missing or stale captures "
        "do not establish departure. Country, query, coverage and distribution must be explicit; "
        "filters cannot supply missing evidence. "
        "Market coverage and account independence are unknown."
    )
    if not products:
        st.info("Create a product and review its offer or advertising links first.")
        return
    product = st.selectbox(
        "Product for competition research",
        [row["product_id"] for row in products],
        format_func=_safe,
    )
    family = st.checkbox("Include the known product family")
    country = st.text_input("Observed-country filter (blank = all, for example FR)")
    source = st.text_input("Source filter (blank = all)")
    query = st.text_input("Declared query-scope filter (blank = all)")
    days = int(
        st.number_input("Competition history days", min_value=1, max_value=366, value=30, step=1)
    )
    age = float(
        st.number_input(
            "Maximum capture age or gap in hours", min_value=0.1, max_value=8760.0, value=72.0
        )
    )
    maximum = int(
        st.number_input(
            "Maximum competition observation rows",
            min_value=1,
            max_value=100000,
            value=50000,
            step=1,
        )
    )
    confidence = float(
        st.number_input("Minimum reviewed-link confidence", min_value=0.0, max_value=1.0, value=0.9)
    )
    if not st.button("Inspect observed competition"):
        return
    try:
        report = CompetitionInsights(pipeline.catalog, pipeline.history).describe(
            product,
            as_of,
            synthetic,
            family=family,
            country=country or None,
            source=source or None,
            query_scope=query or None,
            window_days=days,
            max_age_hours=age,
            max_rows=maximum,
            min_link_confidence=confidence,
        )
        report = _safe(report)
    except Exception:
        st.error(
            "Competition inspection unavailable. Check scope, source availability and row budget."
        )
        return
    if report["status"] != "observed":
        st.warning("Competition comparison is unavailable: no fresh declared comparable scope.")
    if report["source_excluded_observation_count"]:
        st.warning("Some captured observations are outside the eligible source scope.")
    if not report["scopes"]:
        st.info(
            "No eligible observations match the requested scope. "
            "Missing evidence is not zero competition."
        )
    st.caption(
        "Competition growth and market saturation remain unknown. Prices describe compatible "
        "retail quotes; supplier margin, delivered cost and profit are not established."
    )
    st.dataframe(
        [
            {
                "Observed country": scope["observed_country"] or "Unknown",
                "Source": scope["source"],
                "Source account": scope["source_account"] or "Unknown",
                "Distribution": scope["distribution"] or "Unknown",
                "Query scope": scope["query_scope"] or "Unknown",
                "Coverage scope": scope["coverage_scope"] or "Unknown",
                "Status": scope["status"],
                "Unavailable reason": scope["reason"],
                "Last capture age (hours)": scope["latest_observation_age_hours"],
                "Observed named sellers": scope["observed_seller_count"],
                "Observed named advertisers": scope["observed_advertiser_count"],
                "Stale entities": scope["stale_entity_count"],
                "Comparable capture intervals": scope["comparable_interval_count"],
                "Compatible price intervals": scope["price_interval_count"],
            }
            for scope in report["scopes"]
        ],
        hide_index=True,
    )
    for index, scope in enumerate(report["scopes"], 1):
        st.write(f"Scope {index}: observed captures and retail prices")
        if scope["status"] != "observed":
            st.warning(
                "Scope unavailable: " + (scope["reason"] or "unknown context").replace("_", " ")
            )
        st.dataframe(scope["timeline_preview"], hide_index=True)
        if scope["timeline_preview_truncated"]:
            st.warning("Capture preview is truncated; totals describe the complete bounded input.")
        if not scope["price_interval_count"]:
            st.info(
                "No compatible retail-price intervals. Missing variant, currency, tax, shipping "
                "or collection context remains unavailable."
            )
        if scope["price_offers_preview"]:
            st.caption(
                "Offer totals count compatible intervals across their own price contexts. "
                "The latest valid quote context does not relabel earlier changes."
            )
            st.dataframe(
                [
                    {
                        "Offer": offer["entity_key"],
                        "Status": offer["status"],
                        "Unavailable reason": offer["reason"],
                        "Compatible intervals": offer["comparable_interval_count"],
                        "Observed decreases": offer["observed_decrease_count"],
                        "Observed increases": offer["observed_increase_count"],
                        "Uncertain changes": offer["uncertain_change_count"],
                    }
                    for offer in scope["price_offers_preview"]
                ],
                hide_index=True,
            )
            intervals = list(
                islice(
                    (
                        {
                            "Offer": offer["entity_key"],
                            "Currency": interval["price_context"]["currency"],
                            "Variant": interval["price_context"]["variant_id"],
                            "Price basis": interval["price_context"]["price_basis"],
                            "Tax basis": interval["price_context"]["tax_basis"],
                            "Shipping basis": interval["price_context"]["shipping_basis"],
                            "Measured change (lower)": interval["change_lower"],
                            "Measured change (upper)": interval["change_upper"],
                            "Direction": interval["direction"],
                            "Measured interval (hours)": interval["interval_hours"],
                        }
                        for offer in scope["price_offers_preview"]
                        for interval in offer["intervals_preview"]
                    ),
                    PRICE_INTERVAL_PREVIEW_LIMIT + 1,
                )
            )
            if intervals:
                st.dataframe(intervals[:PRICE_INTERVAL_PREVIEW_LIMIT], hide_index=True)
            if len(intervals) > PRICE_INTERVAL_PREVIEW_LIMIT or any(
                offer["intervals_preview_truncated"] for offer in scope["price_offers_preview"]
            ):
                st.warning(
                    "Price-interval table is a bounded preview. Totals include all compatible "
                    "intervals in the bounded input."
                )
        if scope["price_preview_truncated"]:
            st.warning("Offer preview is truncated; price counts include all bounded offers.")
    if report["scope_change_count"]:
        st.warning("Collection contexts changed. Broader discovery is not competition growth.")
    with st.expander("Complete bounded coverage, identity and price evidence"):
        st.json(report)

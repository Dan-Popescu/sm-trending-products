"""Private, explicit supplied-text inspection without a cached source-derived report."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st

from .display_safety import current_display_secrets, display_json, sanitize_display
from .json_input import MAX_INPUT_BYTES, object_from_bytes

if TYPE_CHECKING:
    from .pipeline import Pipeline


def _read_upload(uploaded) -> dict:
    if uploaded.size > MAX_INPUT_BYTES:
        raise ValueError("The uploaded JSON exceeds 128 KiB")
    return object_from_bytes(uploaded.getvalue())


def render_content(
    pipeline: Pipeline, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Quoted content themes")
    st.caption(
        "Inspect a bounded page of supplied transcripts, subtitles or descriptions. "
        "Quotes are candidate message interpretations, not verified claims or buyer demand. "
        "Unknown speaker roles, unsupported languages and partially scanned themes stay explicit. "
        "No source request, media download or paid analysis runs here."
    )
    if not products:
        st.info("Select a product with reviewed video links and supplied text first.")
        return
    product = st.selectbox(
        "Product for quoted themes",
        [row["product_id"] for row in products],
        key="research-content-product",
    )
    limit = st.number_input("Maximum transcript rows in this panel", 1, 200, 100)
    after = st.text_input("Transcript continuation cursor (optional)", max_chars=64)
    taxonomy_file = st.file_uploader(
        "Reviewed content taxonomy JSON (optional, at most 128 KiB)",
        type=["json"],
        max_upload_size=1,
    )
    if st.button("Inspect quoted themes", key="research-content-inspect"):
        from .content_insights import DEFAULT_TAXONOMY, ContentInsights, ContentTaxonomy
        from .content_research_cli import _cursor

        try:
            taxonomy = (
                ContentTaxonomy.model_validate(_read_upload(taxonomy_file))
                if taxonomy_file
                else DEFAULT_TAXONOMY
            )
            result = ContentInsights(pipeline.catalog, pipeline.provenance).aggregate(
                product,
                synthetic=synthetic,
                as_of=as_of,
                limit=int(limit),
                after=_cursor(after or None),
                taxonomy=taxonomy,
            )
            secrets = current_display_secrets()
            gaps = (
                sanitize_display(
                    "Evidence gaps: " + ", ".join(result["gaps"]), secret_values=secrets
                )
                if result["gaps"]
                else None
            )
            details = display_json(result, secret_values=secrets)
        except Exception:
            st.error(
                "The content panel is unavailable. Check transcript initialization, "
                "taxonomy review time, cursor, namespace and current source access."
            )
        else:
            st.caption(
                "Counts cover this page only. Continuing pages can repeat canonical text versions; "
                "do not add page totals as an independent audience count."
            )
            if gaps is not None:
                st.warning(gaps)
            st.json(details)

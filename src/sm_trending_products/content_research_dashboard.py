"""Private, explicit supplied-text inspection without a cached source-derived report."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import streamlit as st

if TYPE_CHECKING:
    from .pipeline import Pipeline


def _read_upload(uploaded) -> dict:
    from .content_research_cli import MAX_INPUT_BYTES, _object_bytes

    if uploaded.size > MAX_INPUT_BYTES:
        raise ValueError("The uploaded JSON exceeds 128 KiB")
    return _object_bytes(uploaded.getvalue())


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
        from .collector import redact
        from .content_insights import DEFAULT_TAXONOMY, ContentInsights, ContentTaxonomy
        from .content_research_cli import _cursor
        from .dashboard import display_safe

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
            if result["gaps"]:
                st.warning("Evidence gaps: " + ", ".join(result["gaps"]))
            st.json(display_safe(redact(result)))

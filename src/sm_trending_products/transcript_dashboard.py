"""Selected text import and local phrase inspection in the private workspace."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import streamlit as st

if TYPE_CHECKING:
    from .enrichment import TranscriptInsights


def render_transcripts(service: TranscriptInsights, products: list[dict], synthetic: bool) -> None:
    st.subheader("Selected-video transcript insights")
    st.caption(
        "Import text you may use for one reviewed product video. Descriptions, subtitles and "
        "human transcripts stay distinct. Analysis uses free local rules and quoted text; "
        "it does not download videos or infer unrecorded speech."
    )
    if not products:
        st.info("Create a product and confirm its video link first.")
        return
    product = st.selectbox(
        "Product for transcript insights", [row["product_id"] for row in products]
    )
    with st.form(f"transcript-import:{synthetic}:{product}"):
        entity = st.text_input("Confirmed video entity key", max_chars=512)
        dataset = st.text_input("Committed source dataset ID", max_chars=128)
        observation = st.text_input("Source observation ID", max_chars=256)
        kind = st.selectbox("Text source", ["human_transcript", "subtitles", "description"])
        permission = st.text_input("Permission reference", max_chars=512)
        language = st.text_input("Language (optional, for example fr)", max_chars=12)
        text = st.text_area("Supplied text", max_chars=16000)
        save = st.form_submit_button("Import selected text")
    if save:
        from .enrichment import TranscriptInput

        try:
            result = service.import_transcript(
                TranscriptInput.model_validate(
                    {
                        "product_id": product,
                        "entity_key": entity,
                        "dataset_id": dataset,
                        "observation_id": observation,
                        "synthetic": synthetic,
                        "source_kind": kind,
                        "permission_reference": permission,
                        "language": language or None,
                        "text": text,
                    }
                )
            )
        except Exception:
            st.error(
                "Text was not imported. Check initialization, source references, "
                "ownership and permission."
            )
        else:
            from .dashboard import display_safe

            st.json(display_safe(result))
    with st.form(f"transcript-analyze:{synthetic}:{product}"):
        transcript_id = st.text_input("Imported transcript ID", max_chars=128)
        request_key = f"transcript-request:{synthetic}:{product}"
        remembered_key = f"_transcript-request:{synthetic}:{product}"
        # Streamlit removes hidden widget keys when the product/view changes.
        # Keep only the stable operation ID separately; never cache source-derived results.
        if remembered_key not in st.session_state:
            st.session_state[remembered_key] = uuid4().hex
        request = st.text_input(
            "Analysis request ID (reuse to read the same request)",
            key=request_key,
            value=st.session_state[remembered_key],
            max_chars=128,
        )
        analyze = st.form_submit_button("Analyze selected text with local rules")
        show = st.form_submit_button("Read existing analysis request")
    if analyze or show:
        st.session_state[remembered_key] = request
        try:
            result = (
                service.analyze(request, product, transcript_id, synthetic=synthetic)
                if analyze
                else service.result(request, product, synthetic=synthetic)
            )
        except Exception:
            st.error("Analysis did not complete. Check source availability and the daily budget.")
        else:
            from .dashboard import display_safe

            st.json(display_safe(result))

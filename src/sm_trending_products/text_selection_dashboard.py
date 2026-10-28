"""Preview bounded supplied-text queues and explicitly apply a reviewed saved plan."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import streamlit as st

from .content_research_dashboard import _read_upload

if TYPE_CHECKING:
    from .pipeline import Pipeline


def render_text_selection(
    pipeline: Pipeline, products: list[dict], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Useful supplied-text selection")
    st.caption(
        "Preview inspects supplied-text metadata only. Applying a reviewed saved plan uses "
        "free local rules and the existing daily limits. It never enables a source fetch, "
        "audio extraction or paid provider. Known themes and roles are operator attestations; "
        "unknown metadata remains unknown."
    )
    if not products:
        st.info("Create a product with eligible supplied text first.")
        return
    selected = st.multiselect(
        "Product shortlist for supplied text (maximum ten)",
        [row["product_id"] for row in products],
        max_selections=10,
        key="research-text-shortlist",
    )
    config_file = st.file_uploader(
        "Selection configuration JSON (optional, at most 128 KiB)",
        type=["json"],
        key="research-text-config",
        max_upload_size=1,
    )
    after_text = st.text_area(
        "Continuation cursors JSON (optional: product ID to transcript cursor)", max_chars=2000
    )
    if st.button("Preview useful supplied texts", key="research-text-preview"):
        import json

        from .collector import redact
        from .content_research_cli import _cursor
        from .contracts import canonical
        from .dashboard import display_safe
        from .text_selection import TextSelection, TextSelectionConfig

        try:
            config = (
                TextSelectionConfig.model_validate(_read_upload(config_file))
                if config_file
                else TextSelectionConfig()
            )
            after = json.loads(after_text) if after_text.strip() else {}
            if not isinstance(after, dict) or len(after) > 10:
                raise ValueError("A bounded cursor object is required")
            for cursor in after.values():
                if not isinstance(cursor, str):
                    raise ValueError("A cursor must be a string")
                _cursor(cursor)
            plan = TextSelection(pipeline.catalog, pipeline.provenance).preview(
                selected, config=config, synthetic=synthetic, as_of=as_of, after=after
            )
        except Exception:
            st.error(
                "The preview is unavailable. Check the selected products, bounded configuration, "
                "cursors, transcript initialization and current source access."
            )
        else:
            safe_plan = display_safe(redact(plan))
            st.json(safe_plan)
            encoded = canonical(plan)
            if canonical(safe_plan) != encoded:
                st.warning(
                    "This plan contains private metadata and cannot be exported. "
                    "Correct its source metadata before creating an apply-ready plan."
                )
            elif len(encoded.encode()) <= 131_072:
                st.download_button(
                    "Download complete selection plan",
                    encoded,
                    "text-selection-plan.json",
                    mime="application/json",
                    key="research-text-download",
                    on_click="ignore",
                )
            else:
                st.warning("Use a smaller candidate panel to create an apply-ready 128 KiB plan.")
    st.caption(
        "Review the complete downloaded plan, then upload that same file below. "
        "Application checks its current source access and exact scope again."
    )
    plan_file = st.file_uploader(
        "Reviewed complete selection plan JSON (at most 128 KiB)",
        type=["json"],
        key="research-text-plan",
        max_upload_size=1,
    )
    remembered_key = f"_research-text-request:{synthetic}"
    if remembered_key not in st.session_state:
        st.session_state[remembered_key] = uuid4().hex
    prefix = st.text_input(
        "Stable local selection request prefix",
        value=st.session_state[remembered_key],
        max_chars=64,
        key=f"research-text-request:{synthetic}",
    )
    if st.button("Apply reviewed plan with free local rules", key="research-text-apply"):
        from .collector import redact
        from .dashboard import display_safe
        from .text_selection import TextSelection, TextSelectionPlan

        st.session_state[remembered_key] = prefix
        try:
            if plan_file is None:
                raise ValueError("Upload the reviewed complete plan")
            reviewed_plan = TextSelectionPlan.model_validate(_read_upload(plan_file))
            if reviewed_plan.synthetic != synthetic or set(reviewed_plan.product_ids) != set(
                selected
            ):
                raise ValueError("The plan must match the visible namespace and shortlist")
            result = TextSelection(pipeline.catalog, pipeline.provenance).execute(
                reviewed_plan, request_prefix=prefix
            )
        except Exception:
            st.error(
                "The local plan was not applied. Check its exact namespace and shortlist, "
                "source eligibility, stable request prefix and remaining daily budget."
            )
        else:
            if result["state"] != "completed":
                st.warning("Some selected texts were denied. Inspect the per-text reasons.")
            st.json(display_safe(redact(result)))

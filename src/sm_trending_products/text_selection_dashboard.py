"""Preview bounded supplied-text queues and explicitly apply a reviewed saved plan."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import streamlit as st

from .content_research_dashboard import _read_upload
from .display_safety import current_display_secrets, display_json
from .product_display import product_labels
from .product_shortlist import MAX_SHORTLIST_PRODUCTS, shortlist_ids, shortlist_workspace

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
    try:
        workspace = shortlist_workspace(pipeline.catalog, synthetic)
        # Changing the actual database/schema drops old selection IDs. Retain both
        # real/synthetic selections only within this one current workspace.
        for key in list(st.session_state):
            if (
                isinstance(key, str)
                and key.startswith(("_research-text-shortlist:", "research-text-shortlist:"))
                and f":{workspace}:" not in key
            ):
                del st.session_state[key]
        saved_key = f"_research-text-shortlist:{workspace}:{synthetic}"
        widget_key = f"research-text-shortlist:{workspace}:{synthetic}"
        saved = shortlist_ids(st.session_state.get(saved_key, []))
        current = pipeline.products_by_ids(saved, synthetic, as_of=as_of)
        retained = [row["product_id"] for row in current]
        if len(products) > 200:
            raise ValueError("The product page exceeds its bounded scope")
        page_ids = [row["product_id"] for row in products]
        if len(page_ids) != len(set(page_ids)):
            raise ValueError("Duplicate product page identifiers")
        for identifier in page_ids:
            shortlist_ids((identifier,))
        secrets = current_display_secrets()
        labels = product_labels(list(dict.fromkeys([*retained, *page_ids])), secret_values=secrets)
    except Exception:
        st.error("La sélection est indisponible. Vérifier les produits et leur espace de travail.")
        return
    if tuple(retained) != saved:
        st.warning("Des produits devenus indisponibles ont été retirés de la sélection.")
    st.session_state[saved_key] = retained
    if not products and not retained:
        st.info("Create a product with eligible supplied text first.")
        return
    if st.button("Effacer la sélection de cet espace", key=f"research-text-clear:{synthetic}"):
        retained = []
        st.session_state[saved_key] = []
        st.session_state[widget_key] = []
    st.caption(
        "La sélection conserve au plus dix identifiants dans cet espace, même sur une autre "
        "page du catalogue. Retirez une étiquette pour exclure un produit. "
        "Les produits conservés restent visibles parmi les choix."
    )
    # Off-page selections remain widget options. Never retain product rows or names.
    options = list(dict.fromkeys([*retained, *page_ids]))
    st.session_state[widget_key] = retained

    def remember_selection() -> None:
        st.session_state[saved_key] = list(shortlist_ids(st.session_state[widget_key]))

    selected = st.multiselect(
        "Product shortlist for supplied text (maximum ten)",
        options,
        max_selections=MAX_SHORTLIST_PRODUCTS,
        key=widget_key,
        on_change=remember_selection,
        format_func=labels.__getitem__,
    )
    st.session_state[saved_key] = list(shortlist_ids(selected))

    def current_selection() -> tuple[str, ...]:
        identifiers = shortlist_ids(selected)
        resolved = pipeline.products_by_ids(identifiers, synthetic, as_of=as_of)
        if not identifiers or {row["product_id"] for row in resolved} != set(identifiers):
            raise ValueError("The complete selected product scope must remain available")
        return identifiers

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
                current_selection(), config=config, synthetic=synthetic, as_of=as_of, after=after
            )
            displayed_plan = display_json(redact(plan), secret_values=current_display_secrets())
            encoded = canonical(plan)
        except Exception:
            st.error(
                "The preview is unavailable. Check the selected products, bounded configuration, "
                "cursors, transcript initialization and current source access."
            )
        else:
            st.json(displayed_plan)
            if canonical(json.loads(displayed_plan)) != encoded:
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
        from .text_selection import TextSelection, TextSelectionPlan

        st.session_state[remembered_key] = prefix
        try:
            if plan_file is None:
                raise ValueError("Upload the reviewed complete plan")
            reviewed_plan = TextSelectionPlan.model_validate(_read_upload(plan_file))
            if reviewed_plan.synthetic != synthetic or set(reviewed_plan.product_ids) != set(
                current_selection()
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
            try:
                displayed_result = display_json(
                    redact(result), secret_values=current_display_secrets()
                )
            except (ValueError, TypeError, RecursionError):
                st.error(
                    "Le résultat ne peut pas être affiché en sécurité. "
                    "Vérifiez avec le même identifiant de requête."
                )
            else:
                st.json(displayed_result)

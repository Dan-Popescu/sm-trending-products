"""Private launch preparation; reads never initialize or contact a storefront."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import streamlit as st

from .commerce import REQUIRED_COSTS

if TYPE_CHECKING:
    from .commerce import LaunchService


def render_launches(service: LaunchService, synthetic: bool) -> None:
    st.subheader("Launch dossiers")
    st.caption(
        "Prepare one product for one country and currency. Saving does not publish a shop, "
        "charge a customer or contact a supplier. Amounts below are scenarios."
    )
    page = int(st.number_input("Dossier page", min_value=1, value=1, step=1))
    try:
        dossiers = service.dossiers(synthetic=synthetic, limit=100, offset=(page - 1) * 100)
    except Exception:
        st.error("Launch preparation is unavailable. Run sm-trends commerce init explicitly.")
        return
    options = ["New dossier", *[row["dossier_id"] for row in dossiers]]
    selected = st.selectbox("Launch dossier", options)
    dossier = next((row for row in dossiers if row["dossier_id"] == selected), None)
    draft = dossier["draft"] if dossier else {}
    if dossier:
        st.write(f"Revision {dossier['revision']} · {dossier['country']} · {dossier['currency']}")
        st.write(f"Readiness: {dossier['readiness']['status']}")
        for reason in dossier["readiness"]["reasons"]:
            st.warning(reason["message"])
        st.json(dossier["economics"])
        try:
            html = service.draft_page(selected, synthetic=synthetic)
        except Exception:
            st.error("The draft page could not be loaded.")
        else:
            st.download_button(
                "Download private product-page draft",
                html,
                file_name="product-draft.html",
                mime="text/html",
            )
    # Widget keys include the current revision so a saved revision reloads its own facts.
    revision = dossier["revision"] if dossier else 0
    with st.form(f"launch-edit:{synthetic}:{selected}:{revision}"):
        identifier = st.text_input(
            "Dossier ID", value=draft.get("dossier_id", ""), disabled=bool(dossier)
        )
        product = st.text_input(
            "Existing product ID", value=draft.get("product_id", ""), disabled=bool(dossier)
        )
        country = st.text_input(
            "Country (ISO code, for example FR)",
            value=draft.get("country", ""),
            disabled=bool(dossier),
        )
        currency = st.text_input(
            "Currency (ISO code, for example EUR)",
            value=draft.get("currency", ""),
            disabled=bool(dossier),
        )
        modes = ["dropship", "stock", "external"]
        mode = st.selectbox(
            "Fulfillment mode", modes, index=modes.index(draft.get("fulfillment_mode", "dropship"))
        )
        title = st.text_input("Product title", value=draft.get("title", ""))
        description = st.text_area("Product description", value=draft.get("description", ""))
        supplier_name = st.text_input("Supplier name", value=draft.get("supplier_name", ""))
        supplier = st.text_input(
            "Supplier or quote reference", value=draft.get("supplier_reference") or ""
        )
        quote = st.text_input(
            "Quote valid until (ISO timestamp with timezone)",
            value=draft.get("quote_expires_at") or "",
        )
        stock_states = ["unknown", "available", "unavailable"]
        stock = st.selectbox(
            "Stock status",
            stock_states,
            index=stock_states.index(draft.get("stock_status", "unknown")),
        )
        delivery_min = st.text_input(
            "Minimum delivery days (blank = unknown)",
            value="" if draft.get("delivery_min_days") is None else str(draft["delivery_min_days"]),
        )
        delivery_max = st.text_input(
            "Maximum delivery days (blank = unknown)",
            value="" if draft.get("delivery_max_days") is None else str(draft["delivery_max_days"]),
        )
        returns = st.text_area("Return terms", value=draft.get("return_terms") or "")
        st.caption(
            "Enter exact decimal amounts, for example 12.50. "
            "Blank means unknown; zero means a known zero cost."
        )
        price = st.text_input("Retail price", value=draft.get("retail_price") or "")
        tax_options = ["Unknown", "Tax included", "Tax excluded"]
        tax_value = draft.get("prices_include_tax")
        tax_basis = st.selectbox(
            "Retail price tax basis",
            tax_options,
            index=0 if tax_value is None else (1 if tax_value else 2),
        )
        tax = st.text_input("Tax per order", value=draft.get("tax_per_order") or "")
        costs = {
            field: st.text_input(
                field.replace("_", " ").capitalize(), value=draft.get("costs", {}).get(field) or ""
            )
            for field in REQUIRED_COSTS
        }
        desired = st.text_input(
            "Desired profit per order after advertising", value=draft.get("desired_profit", "0")
        )
        decisions = ["watch", "test", "hold", "reject"]
        decision = st.selectbox(
            "Commercial decision", decisions, index=decisions.index(draft.get("decision", "watch"))
        )
        note = st.text_area("Decision note", value=draft.get("note") or "")
        submitted = st.form_submit_button("Save new revision")
    if submitted:
        from .commerce import LaunchDraft

        try:
            updated = LaunchDraft.model_validate(
                {
                    **draft,
                    "dossier_id": identifier,
                    "product_id": product,
                    "country": country,
                    "currency": currency,
                    "synthetic": synthetic,
                    "fulfillment_mode": mode,
                    "title": title,
                    "description": description,
                    "supplier_name": supplier_name,
                    "supplier_reference": supplier,
                    "quote_expires_at": quote or None,
                    "stock_status": stock,
                    "delivery_min_days": int(delivery_min) if delivery_min else None,
                    "delivery_max_days": int(delivery_max) if delivery_max else None,
                    "return_terms": returns,
                    "retail_price": price or None,
                    "prices_include_tax": None
                    if tax_basis == "Unknown"
                    else tax_basis == "Tax included",
                    "tax_per_order": tax or None,
                    "costs": {key: value for key, value in costs.items() if value},
                    "desired_profit": desired,
                    "decision": decision,
                    "note": note,
                }
            )
            service.save(updated, expected_revision=revision, request_id=uuid4().hex)
        except Exception:
            st.error(
                "The revision was not saved. Check the fields or reload if another edit was saved."
            )
        else:
            st.session_state["_review_notice"] = "Launch revision saved."
            st.rerun()

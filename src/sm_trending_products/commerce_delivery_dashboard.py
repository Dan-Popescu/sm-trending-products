"""Explicit private draft actions through the durable delivery boundary."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import streamlit as st

if TYPE_CHECKING:
    from .commerce_delivery import CommerceDelivery


def render_delivery(delivery: CommerceDelivery, dossier: dict, synthetic: bool) -> None:
    st.subheader("Configured store draft")
    st.caption(
        "No platform is selected by default. A private connection file must explicitly enable "
        "its store. Preparation creates only a draft; customer checkout remains disabled here."
    )
    prefix = f"store-draft:{synthetic}:{dossier['dossier_id']}"
    # Hidden widgets lose their state. Keep bounded intent outside widget keys.
    intents = st.session_state.setdefault("_store-delivery-intents", {})
    if prefix not in intents:
        if len(intents) >= 100:
            intents.pop(next(iter(intents)))
        intents[prefix] = {"request_id": uuid4().hex, "revision": str(dossier["revision"])}
    request_key, revision_key = prefix + ":request-input", prefix + ":revision-input"
    if request_key not in st.session_state:
        st.session_state[request_key] = intents[prefix]["request_id"]
        st.session_state[revision_key] = intents[prefix]["revision"]
    if st.button("Use latest dossier revision for a new request", key=prefix + ":new"):
        st.session_state[request_key] = uuid4().hex
        st.session_state[revision_key] = str(dossier["revision"])
        intents[prefix] = {
            "request_id": st.session_state[request_key],
            "revision": str(dossier["revision"]),
        }
        st.rerun()
    with st.form(prefix):
        path = st.text_input("Private store connection file")
        request_id = st.text_input(
            "Draft request ID (keep for replay after errors)", key=request_key
        )
        expected_revision = st.text_input("Dossier revision to prepare", key=revision_key)
        operation_id = st.text_input("Existing draft operation ID (for inspection or recovery)")
        preview = st.form_submit_button("Inspect connection without contacting store")
        prepare = st.form_submit_button("Prepare product draft")
        inspect = st.form_submit_button("Inspect draft operation")
        recover = st.form_submit_button("Recover uncertain draft operation")
        listing = st.form_submit_button("Find stored draft operations")
    if not any((preview, prepare, inspect, recover, listing)):
        return
    intents[prefix] = {"request_id": request_id, "revision": expected_revision}
    try:
        from .commerce_connection import load_connection
        from .dashboard import display_safe

        if inspect:
            result = delivery.operation(operation_id, synthetic=synthetic)
        else:
            provider = load_connection(Path(path))
            if listing:
                result = {
                    "operations": delivery.operations(
                        provider.config.connection_id,
                        dossier["dossier_id"],
                        synthetic=synthetic,
                    )
                }
            elif preview:
                result = {
                    "connection_id": provider.config.connection_id,
                    "provider": provider.config.provider,
                    "enabled": provider.config.enabled,
                    "synthetic": provider.config.test_mode,
                    "network_contacted": False,
                    "readiness": dossier["readiness"],
                }
            elif recover:
                result = delivery.recover(operation_id, provider, synthetic=synthetic)
            else:
                result = delivery.prepare(
                    dossier["dossier_id"],
                    provider,
                    int(expected_revision),
                    request_id,
                    synthetic=synthetic,
                )
        st.json(display_safe(result))
    except Exception:
        st.error(
            "The draft action did not complete. Keep the displayed request ID and revision: "
            "repeat the same request to inspect its result, "
            "or find stored operations before recovery. "
            "Check connection, revision conflicts and explicit delivery-init setup."
        )

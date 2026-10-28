"""Private on-demand research desk; session state retains request intent only."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import streamlit as st

from .research_validation_cli import (
    MAX_PROFILE_BYTES,
    ProfileError,
    decode_profile,
    safe_error,
    service_for,
    synthetic_clock,
)

if TYPE_CHECKING:
    from .pipeline import Pipeline


def _safe(value: Any) -> Any:
    from .collector import redact
    from .dashboard import display_safe

    return display_safe(redact(value))


def _error(error: Exception) -> None:
    body = safe_error(error)
    st.error(body["error"].replace("_", " "))
    st.json(_safe(body))


def _request(label: str, scope: str, action: str) -> tuple[str, str]:
    remembered = f"_research-request:{scope}:{action}"
    if remembered not in st.session_state:
        st.session_state[remembered] = uuid4().hex
    request = st.text_input(
        label,
        value=st.session_state[remembered],
        max_chars=128,
        key=f"research-request-widget:{scope}:{action}",
    )
    return request, remembered


def render_validation(pipeline: Pipeline, synthetic: bool) -> None:
    st.subheader("Governed product-selection research")
    st.caption(
        "Compare declared products using existing evidence. The protocol records exclusions, "
        "chronology and policy locks. Synthetic results establish software behavior; "
        "no collection, commercial launch or paid analysis is performed."
    )
    service = service_for(pipeline)
    workspace = str(synthetic)
    if st.button("Initialize or upgrade research module", key=f"research-init:{workspace}"):
        try:
            service.initialize()
        except Exception as error:
            _error(error)
        else:
            st.success("Optional research tables are initialized.")
    clock_text = (
        st.text_input(
            "Synthetic clock (optional ISO timestamp with timezone)",
            max_chars=64,
            key=f"research-clock:{workspace}",
        )
        if synthetic
        else ""
    )
    uploaded = st.file_uploader(
        "Protocol JSON (maximum 128 KiB)", type=["json"], key=f"research-upload:{workspace}"
    )
    with st.form(f"research-register:{workspace}"):
        register_request, register_key = _request("Registration request ID", workspace, "register")
        register = st.form_submit_button("Register immutable protocol")
    selection_key = f"research-protocol-widget:{workspace}"
    remembered_protocol = f"_research-protocol:{workspace}"
    inspect_key = f"_research-inspect-protocol:{workspace}"
    if register:
        st.session_state[register_key] = register_request
        try:
            if uploaded is None:
                raise ProfileError("protocol_file_required")
            if getattr(uploaded, "size", 0) > MAX_PROFILE_BYTES:
                raise ProfileError("protocol_file_too_large")
            uploaded.seek(0)
            protocol = decode_profile(uploaded.read(MAX_PROFILE_BYTES + 1))
            result = service.register(
                protocol,
                register_request,
                synthetic=synthetic,
                as_of=synthetic_clock(clock_text, synthetic),
            )
        except Exception as error:
            _error(error)
        else:
            identifier = result["protocol_id"]
            st.session_state[remembered_protocol] = identifier
            st.session_state[selection_key] = identifier
            st.session_state[inspect_key] = identifier
            st.success("Immutable protocol registered.")
    selected = st.text_input(
        "Protocol ID",
        value=st.session_state.get(remembered_protocol, ""),
        max_chars=128,
        key=selection_key,
    )
    if st.button("Inspect protocol", key=f"research-inspect:{workspace}"):
        st.session_state[remembered_protocol] = selected
        st.session_state[inspect_key] = selected
    current = None
    if selected and st.session_state.get(inspect_key) == selected:
        try:
            current = service.protocol(selected, synthetic)
        except Exception as error:
            _error(error)
    if current is None:
        st.info(
            "Upload a protocol or inspect an existing protocol ID. Reads never initialize tables."
        )
        return
    st.write(
        _safe(
            {
                "registered_at": current["registered_at"],
                "locked_at": current["locked_at"],
                "selected_policy": current["locked_policy_id"],
                "registration_timing": current["registration_timing"],
                "heldout_timing": current["heldout_timing"],
            }
        )
    )
    if "retrospective_governed" in current["heldout_timing"].values():
        st.warning(
            "retrospective_governed: registration or selection was after the decision date, "
            "or the policy is not yet locked. This is not evidence of blind validation."
        )
    else:
        st.info(
            "prospective_predeclared describes recorded chronology. "
            "This interface cannot prove that an operator never viewed outcomes elsewhere."
        )
    st.dataframe(
        _safe(
            [
                {
                    "Partition": name,
                    "Declared products": len(partition["product_ids"]),
                    "First decision": partition["decision_times"][0],
                    "Last decision": partition["decision_times"][-1],
                    "Decision count": len(partition["decision_times"]),
                }
                for name, partition in current["protocol"]["partitions"].items()
            ]
        ),
        hide_index=True,
    )
    with st.expander("Protocol, exact policies and limits"):
        st.json(_safe(current))
    scope = f"{workspace}:{selected}"
    policies = list(current["protocol"]["policies"])
    with st.form(f"research-lock:{scope}"):
        policy = st.selectbox("Policy to lock", policies)
        request, request_key = _request("Policy lock request ID", scope, "lock")
        lock = st.form_submit_button("Lock selected policy")
    if lock:
        st.session_state[request_key] = request
        try:
            service.lock(
                selected, policy, request, synthetic, as_of=synthetic_clock(clock_text, synthetic)
            )
        except Exception as error:
            _error(error)
        else:
            st.rerun()
    with st.form(f"research-run:{scope}"):
        partition = st.selectbox("Partition to execute", ["development", "test", "holdout"])
        policy = st.selectbox("Policy to execute", policies)
        request, request_key = _request("Execution request ID", scope, "run")
        run = st.form_submit_button("Execute declared research subset")
    attempt_selection = f"research-attempt-widget:{workspace}"
    remembered_attempt = f"_research-attempt:{workspace}"
    inspect_attempt = f"_research-inspect-attempt:{workspace}"
    if run:
        st.session_state[request_key] = request
        try:
            result = service.run(
                selected,
                partition,
                policy,
                request,
                synthetic=synthetic,
                as_of=synthetic_clock(clock_text, synthetic),
            )
        except Exception as error:
            _error(error)
        else:
            st.session_state[remembered_attempt] = result["attempt_id"]
            st.session_state[attempt_selection] = result["attempt_id"]
            st.session_state[inspect_attempt] = result["attempt_id"]
    attempt_id = st.text_input(
        "Attempt ID",
        value=st.session_state.get(remembered_attempt, ""),
        max_chars=128,
        key=attempt_selection,
    )
    if st.button("Inspect attempt", key=f"research-inspect-attempt:{scope}"):
        st.session_state[remembered_attempt] = attempt_id
        st.session_state[inspect_attempt] = attempt_id
    if attempt_id and st.session_state.get(inspect_attempt) == attempt_id:
        try:
            attempt = service.attempt(attempt_id, synthetic)
        except Exception as error:
            _error(error)
        else:
            st.write(
                _safe(
                    {
                        "attempt_state": attempt["state"],
                        "error_code": attempt["error_code"],
                        "attempt_clock": attempt["as_of"],
                        "evaluation_cutoff": attempt.get("evaluation_cutoff"),
                    }
                )
            )
            if attempt.get("result_unavailable"):
                st.warning("Evidence was withdrawn; the stored result is unavailable.")
            elif attempt["state"] != "completed":
                st.warning(
                    "This attempt has not completed successfully. Reuse its ID to inspect it."
                )
            st.json(_safe(attempt))
    page = int(st.number_input("Attempt page", min_value=1, max_value=7, value=1, step=1))
    list_key = f"_research-list-attempts:{workspace}"
    if st.button("List protocol attempts", key=f"research-list-attempts:{scope}"):
        st.session_state[list_key] = selected
    if st.session_state.get(list_key) == selected:
        try:
            attempts = service.attempts(selected, synthetic, limit=20, offset=(page - 1) * 20)
        except Exception as error:
            _error(error)
        else:
            st.dataframe(
                _safe(
                    [
                        {
                            key: row[key]
                            for key in (
                                "attempt_id",
                                "partition_name",
                                "policy_id",
                                "state",
                                "error_code",
                                "as_of",
                            )
                        }
                        for row in attempts
                    ]
                ),
                hide_index=True,
            )

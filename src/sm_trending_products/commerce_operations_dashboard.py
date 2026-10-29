"""Private structured order follow-up and reviewed actuals, with explicit effects."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import streamlit as st

from .commerce_learning import ACTUAL_COSTS
from .display_safety import current_display_secrets, display_json

if TYPE_CHECKING:
    from .dashboard import DashboardPipeline

PAGE_SIZE = 20
COST_LABELS = {
    "goods": "Product sourcing",
    "shipping": "Shipping",
    "payment_fees": "Payment fees",
    "nonrecoverable_taxes": "Nonrecoverable taxes",
    "returns": "Return-related costs",
    "fulfillment": "Fulfillment",
    "advertising": "Advertising",
}
COVERAGE_LABELS = {
    "payments_complete": "All settled payments reviewed",
    "refunds_complete": "All executed refunds reviewed",
    "fees_complete": "All payment fees reviewed",
    "costs_complete": "All cost categories reviewed",
}


def _show(value) -> None:
    try:
        details = display_json(value, secret_values=current_display_secrets())
    except (ValueError, TypeError, RecursionError):
        st.error("Details unavailable. Check the report format and bounded display limits.")
        return
    st.json(details)


def _message(message: str, error: Exception) -> str:
    code = getattr(error, "code", "")
    safe_code = (
        f" ({code})" if isinstance(code, str) and re.fullmatch(r"[a-z_]{1,128}", code) else ""
    )
    return message + safe_code


def _error(message: str, error: Exception) -> None:
    st.error(_message(message, error))


def _read(operation, message: str, fallback):
    try:
        return operation()
    except Exception as error:
        _error(message, error)
        return fallback


def _intent(scope: str, revision: int, **metadata) -> dict:
    intents = st.session_state.setdefault("_commerce-operation-requests", {})
    if scope not in intents:
        if len(intents) >= 100:
            intents.pop(next(iter(intents)))
        intents[scope] = {"request_id": uuid4().hex, "revision": str(revision), **metadata}
    return intents[scope]


def _input(label: str, scope: str, name: str, default="", maximum=128, disabled=False):
    key = scope + ":" + name
    if key not in st.session_state:
        st.session_state[key] = "" if default is None else str(default)
    return st.text_input(label, key=key, max_chars=maximum, disabled=disabled)


def _request(scope: str, revision: int, label: str, **metadata) -> tuple[str, str]:
    intent = _intent(scope, revision, **metadata)
    return (
        _input(label + " request ID", scope, "request_id", intent["request_id"]),
        _input(label + " expected revision", scope, "revision", intent["revision"], maximum=10),
    )


def _new_request(scope: str, revision: int, label: str) -> None:
    if st.button("Start a new " + label.lower() + " request", key=scope + ":new"):
        intent = _intent(scope, revision)
        intent.update(request_id=uuid4().hex, revision=str(revision))
        st.session_state[scope + ":request_id"] = intent["request_id"]
        st.session_state[scope + ":revision"] = intent["revision"]
        st.rerun()


def _perform(scope: str, request: str, revision: str, operation, message: str, **metadata):
    # Only replay IDs, expected revisions and period bounds survive widget cleanup.
    # Persist before calling: a response failure can follow a durable write.
    _intent(scope, 0).update(request_id=request, revision=revision, **metadata)
    _refresh(operation, message)


def _refresh(operation, message: str) -> None:
    try:
        operation()
    except Exception as error:
        notice = (
            "error",
            _message(
                "Request may already be recorded. Keep its ID; inspect current records and retry.",
                error,
            ),
        )
    else:
        notice = ("success", message)
    # Refresh authoritative state even after a lost response or failed publication.
    st.session_state["_commerce-operation-notice"] = notice
    st.rerun()


def render_commerce_operations(pipeline: DashboardPipeline, synthetic: bool) -> None:
    st.subheader("Commerce operations")
    st.caption(
        "Private review only. Initialize the optional order/learning modules through the CLI. "
        "No payment, supplier purchase, shipment or email is sent here."
    )
    notice = st.session_state.pop("_commerce-operation-notice", None)
    if notice:
        getattr(st, notice[0])(notice[1])
    section = st.radio(
        "Commerce section",
        ["Orders and supplier follow-up", "Commercial tests and actual costs"],
        key="commerce-operation-section",
    )
    if section == "Orders and supplier follow-up":
        _orders(pipeline.commerce_orders(), synthetic)
    else:
        _financial(pipeline.launch_service(), pipeline.commercial_learning(), synthetic)


def _orders(orders, synthetic: bool) -> None:
    connection = st.text_input("Store connection ID", max_chars=128)
    order_id = st.text_input("Order ID to inspect", max_chars=200)
    cursor = st.text_input("Order or receipt page cursor (optional)", max_chars=200)
    if st.button("Read stored orders"):
        _show(
            _read(
                lambda: orders.orders(connection, synthetic, limit=PAGE_SIZE, after=cursor or None),
                "Orders unavailable. Check order-init and workspace.",
                {},
            )
        )
    pending_only = st.checkbox("Pending receipts only", value=True)
    if st.button("Read event receipts"):
        _show(
            _read(
                lambda: orders.inbox(
                    connection,
                    synthetic,
                    limit=PAGE_SIZE,
                    after=cursor or None,
                    pending_only=pending_only,
                ),
                "Receipts unavailable. Check order-init and workspace.",
                {},
            )
        )
    detail = None
    if connection and order_id:
        detail = _read(
            lambda: orders.order(connection, order_id, synthetic),
            "Stored order unavailable. Reconcile it explicitly if needed.",
            None,
        )
        if detail:
            st.caption(
                "Order totals are not received revenue or profit. "
                "Financial coverage stays incomplete."
            )
            _show(detail)
    with st.form("commerce-reconcile"):
        path = st.text_input("Private connection file for an explicit store read", max_chars=2048)
        resolve = st.checkbox(
            "Resolve a timestamp conflict using this current store read", value=False
        )
        revision = st.text_input(
            "Order revision for conflict resolution", value=str(detail["revision"] if detail else 0)
        )
        submitted = st.form_submit_button("Reconcile order from configured store")
    if submitted:
        try:
            from .commerce_connection import load_connection

            provider = load_connection(Path(path))
            if provider.config.connection_id != connection:
                raise ValueError("Connection file does not match the inspected store")
            orders.reconcile_order(
                provider,
                order_id,
                synthetic,
                resolve_conflict=resolve,
                expected_revision=int(revision) if resolve else None,
            )
        except Exception as error:
            _error(
                "Store read did not finish. Check connection, order-init and current revision.",
                error,
            )
        else:
            st.session_state["_commerce-operation-notice"] = (
                "success",
                "Authoritative store state read. No financial or fulfillment action was sent.",
            )
            st.rerun()
    if detail:
        _manual_order_records(orders, detail, synthetic)


def _manual_order_records(orders, detail: dict, synthetic: bool) -> None:
    events = {row["kind"]: row for row in detail["events"]}
    for kind in ("handoff", "tracking", "notification"):
        _manual_form(orders, detail, events, synthetic, kind)
    st.caption(
        "Tracking is operator-reported, not carrier-verified. "
        "Notification intents do not send email."
    )


def _manual_form(orders, detail: dict, events: dict, synthetic: bool, kind: str) -> None:
    connection, order_id = detail["connection_id"], detail["order_id"]
    labels = {"handoff": "Supplier", "tracking": "Tracking", "notification": "Notification"}
    label = labels[kind]
    current = events.get(kind, {})
    scope = f"commerce-{kind}:{synthetic}:{connection}:{order_id}"
    revision = current.get("revision", 0)
    _new_request(scope, revision, label)
    with st.form(scope):
        request, expected = _request(scope, revision, label)
        if kind == "handoff":
            supplier = st.text_input("Opaque supplier reference", max_chars=128)
            options = {row["line_id"] for row in detail["line_links"] if row["state"] == "matched"}
            options.update(current.get("payload", {}).get("line_ids", []))
            lines = st.multiselect("Owned order line IDs", sorted(options), max_selections=100)
            state = st.selectbox("Supplier preparation state", ["prepared", "cancelled"])
        elif kind == "tracking":
            handoff = events.get("handoff", {})
            handoff_revision = st.number_input(
                "Prepared handoff revision",
                min_value=1,
                value=max(handoff.get("revision", 1), 1),
                step=1,
            )
            carrier = st.text_input("Carrier reference", max_chars=128)
            tracking = st.text_input("Tracking reference", max_chars=128)
            status = st.selectbox(
                "Operator-reported tracking status",
                ["recorded", "in_transit", "delivered", "returned"],
            )
        else:
            notification = st.selectbox(
                "Notification intent type",
                ["order_confirmation", "shipping_update", "refund_update"],
            )
        submitted = st.form_submit_button("Record " + label.lower() + " intent")
    if submitted:

        def record():
            if kind == "handoff":
                return orders.handoff(
                    connection,
                    order_id,
                    request,
                    supplier,
                    lines,
                    expected_revision=int(expected),
                    synthetic=synthetic,
                    state=state,
                )
            if kind == "tracking":
                return orders.tracking(
                    connection,
                    order_id,
                    request,
                    int(handoff_revision),
                    carrier,
                    tracking,
                    expected_revision=int(expected),
                    synthetic=synthetic,
                    status=status,
                )
            return orders.request_notification(
                connection,
                order_id,
                request,
                notification,
                expected_revision=int(expected),
                synthetic=synthetic,
            )

        _perform(
            scope,
            request,
            expected,
            record,
            "Manual intent recorded. No external action or confirmed delivery.",
        )


def _financial(launches, learning, synthetic: bool) -> None:
    page = st.number_input(
        "Dossier page (20 per page)", min_value=1, max_value=50001, value=1, step=1
    )
    dossiers = _read(
        lambda: launches.dossiers(synthetic, limit=PAGE_SIZE, offset=(int(page) - 1) * PAGE_SIZE),
        "Launch dossiers unavailable. Run commerce init first.",
        [],
    )
    if dossiers:
        ids = [row["dossier_id"] for row in dossiers]
        dossier_id = st.selectbox("Dossier for selection", ids)
        dossier = next(row for row in dossiers if row["dossier_id"] == dossier_id)
        _plan(learning, dossier, synthetic)
    else:
        st.info("Create a launch dossier before recording a selection.")
    page = st.number_input(
        "Commercial test page (20 per page)", min_value=1, max_value=50001, value=1, step=1
    )
    tests = _read(
        lambda: learning.tests(synthetic, limit=PAGE_SIZE, offset=(int(page) - 1) * PAGE_SIZE),
        "Commercial tests unavailable. Initialize the learning module explicitly.",
        [],
    )
    if not tests:
        return
    selected = st.selectbox(
        "Commercial test",
        [row["test_id"] for row in tests],
        format_func=lambda key: next(
            row["dossier_id"] + " / " + row["plan"]["selection"] + " / " + key[:12]
            for row in tests
            if row["test_id"] == key
        ),
    )
    test = _read(
        lambda: learning.test(selected, synthetic), "Commercial test could not be read.", None
    )
    if test:
        _show(test)
        if test["plan"]["selection"] != "test":
            st.info(
                "Watch/skip choices declare no financial ledger "
                "and cannot receive actual-profit periods."
            )
        elif test["state"] != "ready":
            st.warning(
                "Ledger declaration is pending. Retry the original plan "
                "request with the same ID and values."
            )
        else:
            st.caption(
                "Ledger declared at "
                + test["ledger_declared_at"]
                + ". A future frozen financial decision must explicitly use "
                "this profit contract and a later cutoff."
            )
            _periods(learning, test, synthetic)


def _plan(learning, dossier: dict, synthetic: bool) -> None:
    from .commerce_learning import ActualCostContract, TestPlan

    scope = f"commerce-plan:{synthetic}:{dossier['dossier_id']}"
    _new_request(scope, dossier["revision"], "Plan")
    st.caption(
        f"Dossier revision {dossier['revision']}; readiness: {dossier['readiness']['status']}."
    )
    if _intent(scope, dossier["revision"])["revision"] != str(dossier["revision"]):
        st.warning(
            "This request retains an earlier dossier revision. Review the current dossier "
            "before starting a new plan request."
        )
    with st.form(scope):
        request, expected = _request(scope, dossier["revision"], "Plan")
        selection = st.selectbox("Selection decision", ["watch", "test", "skip"])
        reason = st.text_area("Why this decision?", max_chars=4000)
        policy = st.text_area("Selection policy used", max_chars=4000)
        st.caption(
            "Future actual-cost contract: " + ", ".join(COST_LABELS[key] for key in ACTUAL_COSTS)
        )
        confirmed = st.checkbox(
            "I will review all seven cost categories, including explicit reviewed zeros"
        )
        coverage = st.text_area(
            "How payment, refund, fee and cost coverage will be checked", max_chars=2000
        )
        assessment = st.text_input("Original assessment run ID (optional)", max_chars=256)
        evaluation = st.text_input("Original forward evaluation ID (optional)", max_chars=256)
        submitted = st.form_submit_button("Record selection decision")
    if submitted:

        def save():
            if not confirmed:
                raise ValueError("Confirm the full reviewed cost contract")
            contract = ActualCostContract(
                required_costs=ACTUAL_COSTS,
                revenue_basis="settled_payments_less_executed_refunds_excluding_output_taxes",
                coverage_policy=coverage,
            )
            plan = TestPlan.model_validate(
                {
                    "selection": selection,
                    "reason": reason,
                    "selection_policy": policy,
                    "cost_contract": contract,
                    "assessment_run_id": assessment or None,
                    "forward_evaluation_id": evaluation or None,
                }
            )
            return learning.plan_test(
                dossier["dossier_id"], int(expected), request, synthetic, plan=plan
            )

        _perform(
            scope, request, expected, save, "Selection recorded. Ledger readiness is shown below."
        )


def _periods(learning, test: dict, synthetic: bool) -> None:
    page = st.number_input(
        "Reviewed period page (20 revisions per page)",
        min_value=1,
        max_value=50001,
        value=1,
        step=1,
    )
    periods = _read(
        lambda: learning.periods(
            test["test_id"],
            synthetic,
            limit=PAGE_SIZE,
            offset=(int(page) - 1) * PAGE_SIZE,
            include_history=True,
        ),
        "Period history unavailable.",
        [],
    )
    for row in periods:
        if row["publication_kind"] == "withdrawal" and row["publication_state"] == "pending":
            st.warning(
                "A withdrawal is pending. The previous analytical export "
                "must not be treated as current."
            )
    choices = {row["period_id"] + ":" + str(row["revision"]): row for row in periods}
    selected = st.selectbox(
        "Period to review or publish",
        ["New period", *choices],
        format_func=lambda key: (
            key
            if key == "New period"
            else choices[key]["period"]["period_start"]
            + " / revision "
            + str(choices[key]["revision"])
            + (" / corrected history" if choices[key]["superseded"] else " / current")
        ),
    )
    period = choices.get(selected)
    if period:
        st.write(
            "Reviewed period profit: "
            + (period["realized_profit"] if period["realized_profit"] is not None else "Unknown")
        )
        st.caption(
            "Publication: " + period["publication_state"] + ". Values apply only to this period."
        )
        if period["publication_state"] == "pending":
            st.warning(
                "This revision has not finished publication. Its analytical export is unconfirmed."
            )
        _show(period)
        if period["dataset_id"] and (
            not period["superseded"] or period["publication_kind"] == "withdrawal"
        ):
            if st.button("Publish or resume this period revision"):
                _refresh(
                    lambda: learning.publish(
                        test["test_id"], period["period_id"], period["revision"], synthetic
                    ),
                    "This period revision was published. "
                    "Its reviewed completeness remains explicit.",
                )
        if period["superseded"]:
            st.info("Historical correction: choose the current revision to edit actuals.")
            return
    _review(learning, test, period, synthetic)


def _review(learning, test: dict, existing: dict | None, synthetic: bool) -> None:
    from .commerce_learning import FinancialCoverage, ReviewedPeriod

    payload = existing["period"] if existing else {}
    revision = existing["revision"] if existing else 0
    scope = (
        f"commerce-review:{synthetic}:{test['test_id']}:"
        f"{existing['period_id'] if existing else 'new'}"
    )
    metadata = _intent(
        scope,
        revision,
        period_start=payload.get("period_start", test["planned_at"]),
        period_end=payload.get("period_end", datetime.now(UTC).isoformat()),
    )
    _new_request(scope, revision, "Review")
    if metadata["revision"] != str(revision):
        st.warning(
            "This request retains an earlier period revision. Inspect the current review "
            "before starting a new request."
        )
    with st.form(scope):
        request, expected = _request(scope, revision, "Review")
        start = _input(
            "Period start (ISO timestamp with timezone)",
            scope,
            "start",
            metadata["period_start"],
            40,
            bool(existing),
        )
        end = _input(
            "Period end (ISO timestamp with timezone)",
            scope,
            "end",
            metadata["period_end"],
            40,
            bool(existing),
        )
        st.caption(
            "Country/currency fixed by the test: " + test["country"] + " / " + test["currency"]
        )
        st.caption(
            "Exact amounts use a decimal point. Blank means unknown; "
            "a reviewed 0.00 is a known zero."
        )
        revenue = _input(
            "Settled revenue minus executed refunds, excluding output taxes",
            scope,
            "revenue",
            payload.get("realized_revenue"),
            32,
        )
        costs = {
            key: _input(
                COST_LABELS[key] + " actual cost (blank = unknown)",
                scope,
                key,
                payload.get("realized_costs", {}).get(key),
                32,
            )
            for key in ACTUAL_COSTS
        }
        coverage = {
            key: st.checkbox(label, value=payload.get("coverage", {}).get(key, False))
            for key, label in COVERAGE_LABELS.items()
        }
        refs = st.text_area(
            "Opaque evidence IDs (one per line, no customer contact data)",
            value="\n".join(payload.get("evidence_refs", [])),
            max_chars=51400,
        )
        note = st.text_area(
            "Reviewer note (no customer contact data)",
            value=payload.get("reviewer_note", ""),
            max_chars=4000,
        )
        submitted = st.form_submit_button("Save reviewed actuals")
    if submitted:

        def save():
            value = ReviewedPeriod.model_validate(
                {
                    "period_start": start,
                    "period_end": end,
                    "country": test["country"],
                    "currency": test["currency"],
                    "realized_revenue": revenue.strip() or None,
                    "realized_costs": {key: value.strip() or None for key, value in costs.items()},
                    "coverage": FinancialCoverage(**coverage),
                    "evidence_refs": tuple(
                        line.strip() for line in refs.splitlines() if line.strip()
                    ),
                    "reviewer_note": note,
                }
            )
            return learning.review_period(test["test_id"], value, int(expected), request, synthetic)

        _perform(
            scope,
            request,
            expected,
            save,
            "Review recorded. Incomplete actuals keep profit unknown.",
            period_start=start,
            period_end=end,
        )

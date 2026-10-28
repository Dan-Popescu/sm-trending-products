"""Private structured commerce forms over native SQL and controlled store reads."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_commerce_learning import learning as learning
from test_commerce_learning import period, planned
from test_commerce_orders import context as context
from test_commerce_providers import ControlledStore, Reply, woo_order

from sm_trending_products.commerce_learning import ACTUAL_COSTS
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration

APP = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.commerce_operations_dashboard import render_commerce_operations
with Pipeline.from_env() as pipeline:
    render_commerce_operations(pipeline, True)
"""


@pytest.fixture(autouse=True)
def operator_environment(pipeline, monkeypatch):
    for key, value in {
        "S3_BUCKET": pipeline.settings.s3_bucket,
        "S3_ENDPOINT_URL": pipeline.settings.s3_endpoint_url,
        "CACHE_DIR": str(pipeline.settings.cache_dir),
        "SPOOL_DIR": str(pipeline.settings.spool_dir),
        "COLLECTOR_BINARY": pipeline.settings.collector_binary,
        "HISTORY_PARTITION_BUCKETS": str(pipeline.settings.history_partition_buckets),
    }.items():
        monkeypatch.setenv(key, value)


def widget(app, kind, label):
    return next(item for item in getattr(app, kind) if item.label == label)


def app_view() -> AppTest:
    return AppTest.from_string(APP).run(timeout=30)


def financial(app):
    widget(app, "radio", "Commerce section").set_value("Commercial tests and actual costs").run()


def test_view_never_initializes_modules_or_reads_the_unselected_section(pipeline, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Unselected commerce section was accessed")

    real_learning = Pipeline.commercial_learning
    monkeypatch.setattr(Pipeline, "commercial_learning", forbidden)
    app = app_view()
    assert not app.exception and not app.json
    assert (
        pipeline.catalog.one("SELECT to_regclass('commerce_order_heads') AS table")["table"] is None
    )
    monkeypatch.setattr(Pipeline, "commercial_learning", real_learning)
    monkeypatch.setattr(Pipeline, "commerce_orders", forbidden)
    financial(app)
    assert not app.exception and app.error
    assert pipeline.catalog.one("SELECT to_regclass('commercial_tests') AS table")["table"] is None
    assert pipeline.catalog.one("SELECT to_regclass('commerce_dossiers') AS table")["table"] is None


def fill_plan(app, selection="watch"):
    widget(app, "selectbox", "Selection decision").set_value(selection)
    widget(app, "text_area", "Why this decision?").set_value(
        "Reviewed a multi-day product opportunity"
    )
    widget(app, "text_area", "Selection policy used").set_value("Manual pilot policy version one")
    widget(
        app, "text_area", "How payment, refund, fee and cost coverage will be checked"
    ).set_value("Review all payment, refund and supplier statements; allocate advertising per test")
    widget(
        app,
        "checkbox",
        "I will review all seven cost categories, including explicit reviewed zeros",
    ).check()


def test_operator_records_watch_then_test_and_incomplete_actuals_remain_unknown(learning, pipeline):
    app = app_view()
    financial(app)
    assert not app.exception and not app.error
    fill_plan(app)
    watch_request = widget(app, "text_input", "Plan request ID").value
    widget(app, "button", "Record selection decision").click().run()
    assert not app.exception and not app.error
    watch = learning.tests(True)[0]
    assert watch["plan"]["selection"] == "watch" and watch["ledger_key"] is None
    assert watch["plan"]["cost_contract"]["required_costs"] == list(ACTUAL_COSTS)
    widget(app, "radio", "Commerce section").set_value("Orders and supplier follow-up").run()
    financial(app)
    assert widget(app, "text_input", "Plan request ID").value == watch_request
    assert widget(app, "text_input", "Plan expected revision").value == "1"
    widget(app, "button", "Start a new plan request").click().run()
    fill_plan(app, "test")
    widget(app, "button", "Record selection decision").click().run()
    assert not app.exception and not app.error
    test = next(row for row in learning.tests(True) if row["plan"]["selection"] == "test")
    assert test["state"] == "ready" and test["ledger_declared_at"]
    widget(app, "selectbox", "Commercial test").set_value(test["test_id"]).run()
    assert not app.exception
    assert widget(app, "text_input", "Review expected revision").value == "0"
    request = widget(app, "text_input", "Review request ID").value
    widget(
        app, "text_input", "Settled revenue minus executed refunds, excluding output taxes"
    ).set_value("120.00")
    widget(app, "text_area", "Reviewer note (no customer contact data)").set_value(
        "Supplier and ad statements are still missing"
    )
    widget(app, "button", "Save reviewed actuals").click().run()
    assert not app.exception and not app.error
    row = learning.periods(test["test_id"], True)[0]
    assert row["complete"] is False and row["realized_profit"] is None
    assert set(row["period"]["realized_costs"]) == set(ACTUAL_COSTS)
    assert all(value is None for value in row["period"]["realized_costs"].values())
    assert row["publication_state"] == "not_publishable"
    assert widget(app, "text_input", "Review request ID").value == request
    assert widget(app, "text_input", "Review expected revision").value == "0"


def test_actual_review_correction_exposes_pending_withdrawal_and_explicit_retry(
    learning, pipeline, monkeypatch
):
    from sm_trending_products.contracts import utc
    from sm_trending_products.history import History

    test = planned(learning)
    start = utc(test["planned_at"])
    reviewed = learning.review_period(
        test["test_id"], period(start, start + timedelta(microseconds=1)), 0, "complete", True
    )
    learning.publish(test["test_id"], reviewed["period_id"], 1, True)
    app = app_view()
    financial(app)
    period_key = reviewed["period_id"] + ":1"
    widget(app, "selectbox", "Period to review or publish").set_value(period_key).run()
    assert not app.exception and not app.error
    widget(
        app, "text_input", "Settled revenue minus executed refunds, excluding output taxes"
    ).set_value("")
    widget(app, "text_area", "Reviewer note (no customer contact data)").set_value(
        "Revenue coverage is now incomplete"
    )
    real_publish = History.publish

    def unavailable(*args, **kwargs):
        raise RuntimeError("Bearer fixture-private must never reach the interface")

    monkeypatch.setattr(History, "publish", unavailable)
    widget(app, "button", "Save reviewed actuals").click().run()
    assert not app.exception and app.error
    assert "fixture-private" not in str(app.error)
    assert any("withdrawal is pending" in warning.value for warning in app.warning)
    latest = learning.periods(test["test_id"], True)[0]
    assert latest["revision"] == 2 and latest["realized_profit"] is None
    assert latest["publication_state"] == "pending" and latest["publication_kind"] == "withdrawal"
    monkeypatch.setattr(History, "publish", real_publish)
    widget(app, "selectbox", "Period to review or publish").set_value(
        reviewed["period_id"] + ":2"
    ).run()
    assert any("Unknown" in value.value for value in app.markdown)
    widget(app, "button", "Publish or resume this period revision").click().run()
    assert not app.exception and not app.error
    assert learning.periods(test["test_id"], True)[0]["publication_state"] == "published"


def test_manual_supplier_tracking_and_notification_records_do_not_claim_external_effects(
    context,
):
    orders, provider, _operation = context
    orders.reconcile_order(provider, "71", True)
    app = app_view()
    widget(app, "text_input", "Store connection ID").set_value("local-store")
    widget(app, "text_input", "Order ID to inspect").set_value("71")
    app.run()
    assert not app.exception and not app.error
    before = provider.calls
    widget(app, "text_input", "Opaque supplier reference").set_value("supplier-fixture")
    widget(app, "multiselect", "Owned order line IDs").set_value(["81"])
    widget(app, "button", "Record supplier intent").click().run()
    assert not app.exception and not app.error
    widget(app, "text_input", "Carrier reference").set_value("carrier-fixture")
    widget(app, "text_input", "Tracking reference").set_value("TRACK-81")
    widget(app, "button", "Record tracking intent").click().run()
    assert not app.exception and not app.error
    widget(app, "button", "Record notification intent").click().run()
    assert not app.exception and not app.error
    events = {row["kind"]: row for row in orders.order("local-store", "71", True)["events"]}
    assert events["handoff"]["payload"]["external_action"] == "none"
    assert events["tracking"]["payload"]["carrier_verified"] is False
    assert events["notification"]["payload"]["state"] == "requested"
    assert events["notification"]["payload"]["confirmed_delivered"] is False
    assert provider.calls == before
    assert all("email sent" not in value.value.lower() for value in app.success)
    request = widget(app, "text_input", "Notification request ID").value
    financial(app)
    widget(app, "radio", "Commerce section").set_value("Orders and supplier follow-up").run()
    widget(app, "text_input", "Store connection ID").set_value("local-store")
    widget(app, "text_input", "Order ID to inspect").set_value("71")
    app.run()
    assert widget(app, "text_input", "Notification request ID").value == request
    assert widget(app, "text_input", "Notification expected revision").value == "0"
    widget(app, "button", "Record notification intent").click().run()
    assert not app.exception and not app.error
    replay = next(
        row
        for row in orders.order("local-store", "71", True)["events"]
        if row["kind"] == "notification"
    )
    assert replay["revision"] == 1


def test_order_reconciliation_reads_configured_store_only_after_click_and_masks_secrets(
    pipeline, tmp_path, monkeypatch
):
    orders = pipeline.commerce_orders()
    orders.initialize()
    store = ControlledStore()
    try:
        config = {
            "provider": "woocommerce",
            "connection_id": "controlled-store",
            "base_url": store.url,
            "enabled": True,
            "test_mode": True,
        }
        connection = tmp_path / "store.json"
        connection.write_text(json.dumps(config))
        monkeypatch.setenv("COMMERCE_CONSUMER_KEY", "fixture-key")
        monkeypatch.setenv("COMMERCE_CONSUMER_SECRET", "fixture-consumer-secret")
        monkeypatch.setenv("COMMERCE_WEBHOOK_SECRET", "fixture-webhook-secret")
        store.replies.append(Reply(woo_order(refunds=[])))
        app = app_view()
        widget(app, "text_input", "Store connection ID").set_value("controlled-store")
        widget(app, "text_input", "Order ID to inspect").set_value("71")
        widget(app, "text_input", "Private connection file for an explicit store read").set_value(
            str(connection)
        )
        assert store.requests == []
        widget(app, "button", "Reconcile order from configured store").click().run()
        assert not app.exception and not app.error, [error.value for error in app.error]
        assert len(store.requests) == 1 and store.requests[0][0] == "GET"
        output = " ".join(item.value for item in app.json)
        assert "private@example.test" not in output and "DO NOT RETAIN" not in output
        assert "fixture-consumer-secret" not in output
        assert orders.order("controlled-store", "71", True)["cash_complete"] is False
        app.run()
        assert len(store.requests) == 1
    finally:
        store.close()


def test_order_and_pending_receipt_pages_are_bounded_and_do_not_poll_store(context):
    from test_commerce_orders import signed

    from sm_trending_products.commerce_orders import OrderError

    orders, provider, _operation = context
    for index in range(71, 92):
        orders.reconcile_order(provider, str(index), True)

    def fail_read():
        raise TimeoutError("fixture connection interruption")

    provider.callback = fail_read
    raw, headers = signed(provider, {"id": 999})
    with pytest.raises(OrderError):
        orders.ingest(provider, raw, headers, True)
    provider.callback = None
    before = provider.calls
    app = app_view()
    widget(app, "text_input", "Store connection ID").set_value("local-store")
    widget(app, "button", "Read stored orders").click().run()
    assert not app.exception and not app.error
    page = json.loads(app.json[0].value)
    assert len(page["items"]) == 20 and page["next_cursor"]
    widget(app, "text_input", "Order or receipt page cursor (optional)").set_value(
        page["next_cursor"]
    )
    widget(app, "button", "Read stored orders").click().run()
    assert len(json.loads(app.json[0].value)["items"]) == 1
    widget(app, "text_input", "Order or receipt page cursor (optional)").set_value("")
    widget(app, "button", "Read event receipts").click().run()
    receipts = json.loads(app.json[0].value)
    assert len(receipts["items"]) == 1 and receipts["items"][0]["order_id"] == "999"
    assert receipts["items"][0]["state"] == "pending"
    assert provider.calls == before

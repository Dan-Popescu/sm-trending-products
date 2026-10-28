"""Native order inbox/reconciliation and no-effect fulfillment preparation."""

import base64
import hmac
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier, Event

import pytest
from test_commerce_providers import ControlledStore, Reply, woo_order
from test_launch_dossiers import draft

from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_delivery import CommerceDelivery
from sm_trending_products.commerce_orders import CommerceOrders, OrderError
from sm_trending_products.commerce_providers import (
    ConnectionConfig,
    DraftLookup,
    OrderLine,
    OrderPage,
    OrderSnapshot,
    PaymentSnapshot,
    PreparedProduct,
    ProviderCapabilities,
    ProviderError,
    RefundSnapshot,
    WooCommerceProvider,
)


class ControlledProvider:
    capabilities = ProviderCapabilities()

    def __init__(self):
        self.config = ConnectionConfig(
            "woocommerce",
            "local-store",
            "http://127.0.0.1:9",
            enabled=True,
            test_mode=True,
            consumer_key="PRIVATE-key",
            consumer_secret="PRIVATE-secret",
            webhook_secret="PRIVATE-hook",
        )
        self.product = None
        self.calls = 0
        self.callback = None
        self.page = OrderPage(("71",), None)
        self.snapshot = OrderSnapshot(
            "local-store",
            "woocommerce",
            "71",
            "2026-10-28T10:00:00Z",
            "EUR",
            "72.00",
            False,
            "unknown",
            (),
            (),
            (),
            True,
            "2026-10-28T09:00:00Z",
        )

    def lookup_draft(self, launch_id, sku):
        return DraftLookup("found", self.product) if self.product else DraftLookup("absent")

    def prepare_draft(self, value):
        self.product = PreparedProduct("51", None, value.sku, "http://127.0.0.1:9/product/51")
        self.snapshot = replace(self.snapshot, lines=(OrderLine("81", "51", None, value.sku, 2),))
        return self.product

    def update_draft(self, product_id, value):
        return self.prepare_draft(value)

    def read_order(self, order_id):
        self.calls += 1
        snapshot = self.snapshot
        if self.callback:
            self.callback()
        return replace(snapshot, order_id=order_id)

    def list_order_ids(self, updated_after, cursor=None):
        return self.page

    def verify_webhook(self, raw, headers):
        return WooCommerceProvider(self.config).verify_webhook(raw, headers)


@pytest.fixture
def context(pipeline):
    pipeline.create_product("p1", "Selected product", synthetic=True)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    original = draft()
    launches.save(original, 0, "dossier")
    delivery = CommerceDelivery(pipeline.catalog)
    delivery.initialize()
    provider = ControlledProvider()
    operation = delivery.prepare(original.dossier_id, provider, 1, "prepare", synthetic=True)
    service = CommerceOrders(pipeline.catalog, delivery)
    service.initialize()
    service.register_listing(operation["operation_id"], synthetic=True)
    return service, provider, operation


def signed(provider, body, **headers):
    raw = json.dumps(body, sort_keys=True).encode()
    signature = base64.b64encode(
        hmac.digest(provider.config.webhook_secret.encode(), raw, "sha256")
    ).decode()
    return raw, {"X-WC-Webhook-Signature": signature, **headers}


def ingest(service, provider, body=None, **headers):
    raw, headers = signed(provider, body or {"id": 71}, **headers)
    return service.ingest(provider, raw, headers, synthetic=True)


def read(service, provider, order="71", **kwargs):
    return service.reconcile_order(provider, order, synthetic=True, **kwargs)


def test_signed_hints_are_durable_and_unsigned_delivery_headers_cannot_hide_new_business_state(
    context, pipeline
):
    service, provider, _operation = context

    def inspect():
        assert pipeline.catalog.one("SELECT state FROM commerce_order_inbox")["state"] == "pending"
        sessions = pipeline.catalog.all(
            "SELECT state FROM pg_stat_activity WHERE datname=current_database() "
            "AND query LIKE '%%commerce-order%%'"
        )
        assert not any(row["state"] == "idle in transaction" for row in sessions)

    provider.callback = inspect
    first = ingest(
        service,
        provider,
        {"id": 71, "total": "999999.00"},
        **{"X-WC-Webhook-Delivery-ID": "same-forged-id", "X-WC-Webhook-Topic": "order.deleted"},
    )
    provider.callback = None
    assert first["payload"]["total"] == "72"
    assert first["payload"]["cancelled"] is False
    provider.snapshot = replace(
        provider.snapshot, updated_at="2026-10-28T11:00:00Z", cancelled=True
    )
    second = ingest(
        service,
        provider,
        {"id": 71, "updated": 2},
        **{"X-WC-Webhook-Delivery-ID": "same-forged-id"},
    )
    assert second["revision"] == 2 and second["payload"]["cancelled"] is True
    duplicate = ingest(
        service,
        provider,
        {"id": 71, "updated": 2},
        **{"X-WC-Webhook-Delivery-ID": "different-forged-id"},
    )
    assert duplicate["state"] == "duplicate" and provider.calls == 2
    assert len(pipeline.catalog.all("SELECT * FROM commerce_order_inbox")) == 2
    assert sorted(
        row["completed_revision"]
        for row in pipeline.catalog.all("SELECT completed_revision FROM commerce_order_inbox")
    ) == [1, 2]


def test_unsigned_or_tampered_raw_body_is_rejected_before_financial_state(context, pipeline):
    service, provider, _ = context
    raw, headers = signed(provider, {"id": 71})
    with pytest.raises(ProviderError, match="invalid_webhook_signature"):
        service.ingest(provider, raw + b" ", headers, synthetic=True)
    with pytest.raises(ProviderError):
        service.ingest(provider, raw, {}, synthetic=True)
    assert provider.calls == 0
    assert pipeline.catalog.all("SELECT * FROM commerce_order_inbox") == []


def test_older_provider_reads_never_regress_current_snapshot_and_equal_dates_conflict(
    context, pipeline
):
    service, provider, _ = context
    original = read(service, provider)
    provider.snapshot = replace(provider.snapshot, updated_at="2026-10-28T09:00:00Z", total="9.00")
    old = read(service, provider)
    assert old["revision"] == 1 and old["payload"] == original["payload"]
    assert old["reconciliation_state"] == "stale_read_ignored"
    provider.snapshot = replace(provider.snapshot, updated_at="2026-10-28T10:00:00Z", total="73.00")
    conflict = read(service, provider)
    assert conflict["state"] == "conflict" and conflict["payload"]["total"] == "72"
    assert pipeline.catalog.one("SELECT count(*) AS n FROM commerce_order_snapshots")["n"] == 1
    assert pipeline.catalog.one("SELECT count(*) AS n FROM commerce_order_conflicts")["n"] == 1
    # Reading either same-timestamp candidate again is not an automatic winner.
    provider.snapshot = replace(provider.snapshot, total="72.00")
    assert read(service, provider)["state"] == "conflict"
    with pytest.raises(OrderError, match="requires_revision"):
        read(service, provider, resolve_conflict=True)
    with pytest.raises(OrderError, match="order_revision_conflict"):
        read(service, provider, resolve_conflict=True, expected_revision=2)
    resolved = read(service, provider, resolve_conflict=True, expected_revision=1)
    assert resolved["state"] == "current" and resolved["revision"] == 2
    historic = service.order("local-store", "71", synthetic=True, revision=1)
    assert historic["payload"] == original["payload"] and historic["state"] == "historical"
    assert historic["reference"] == original["reference"]


def test_a_newer_authoritative_timestamp_clears_conflict_without_overwriting_old_revisions(context):
    service, provider, _ = context
    first = read(service, provider)
    provider.snapshot = replace(provider.snapshot, total="1")
    assert read(service, provider)["state"] == "conflict"
    provider.snapshot = replace(provider.snapshot, updated_at="2026-10-28T12:00:00Z")
    latest = read(service, provider)
    assert latest["state"] == "current" and latest["revision"] == 2
    assert service.order("local-store", "71", True, revision=1)["payload"] == first["payload"]


def test_crash_between_remote_read_and_commit_leaves_replayable_inbox(
    context, pipeline, monkeypatch
):
    service, provider, _ = context
    store = service._store

    def process_death(*args, **kwargs):
        raise SystemExit("controlled process death")

    monkeypatch.setattr(service, "_store", process_death)
    with pytest.raises(SystemExit):
        ingest(service, provider)
    assert pipeline.catalog.one("SELECT state FROM commerce_order_inbox")["state"] == "pending"
    assert pipeline.catalog.all("SELECT * FROM commerce_order_snapshots") == []
    monkeypatch.setattr(service, "_store", store)
    restarted = CommerceOrders(pipeline.catalog)
    assert ingest(restarted, provider)["revision"] == 1
    assert provider.calls == 2  # Repeating a read cannot purchase/refund/fulfill anything.
    assert pipeline.catalog.one("SELECT state FROM commerce_order_inbox")["state"] == "completed"


def test_parallel_workers_do_not_fetch_same_order_twice_and_keep_new_hint_pending(
    context, pipeline
):
    service, provider, _ = context
    entered, release = Event(), Event()

    def block():
        entered.set()
        assert release.wait(15)

    provider.callback = block
    with ThreadPoolExecutor(max_workers=2) as pool:
        active = pool.submit(ingest, service, provider)
        assert entered.wait(15)
        try:
            with pytest.raises(OrderError, match="order_busy"):
                ingest(service, provider, {"id": 71, "new_hint": 1})
            assert provider.calls == 1
        finally:
            release.set()
        assert active.result(15)["revision"] == 1
    assert sorted(
        row["state"] for row in pipeline.catalog.all("SELECT state FROM commerce_order_inbox")
    ) == ["completed", "pending"]
    provider.callback = None
    assert ingest(service, provider, {"id": 71, "new_hint": 1})["revision"] == 1
    assert {
        row["state"] for row in pipeline.catalog.all("SELECT state FROM commerce_order_inbox")
    } == {"completed"}


def test_process_death_after_snapshot_insert_rolls_back_before_commit(
    context, pipeline, monkeypatch
):
    service, provider, _ = context
    guard = service._fetch_guard

    class InterruptedConnection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, query, *args, **kwargs):
            result = self.connection.execute(query, *args, **kwargs)
            if query.startswith("INSERT INTO commerce_order_snapshots"):
                raise SystemExit("controlled death before database commit")
            return result

        def transaction(self):
            return self.connection.transaction()

    @contextmanager
    def interrupted(*args):
        with guard(*args) as connection:
            yield InterruptedConnection(connection)

    monkeypatch.setattr(service, "_fetch_guard", interrupted)
    with pytest.raises(SystemExit):
        ingest(service, provider)
    monkeypatch.setattr(service, "_fetch_guard", guard)
    assert pipeline.catalog.all("SELECT * FROM commerce_order_snapshots") == []
    assert pipeline.catalog.one("SELECT revision FROM commerce_order_heads")["revision"] == 0
    assert service.inbox("local-store", True)["items"][0]["state"] == "pending"
    assert ingest(service, provider)["revision"] == 1


def test_database_session_loss_during_http_cannot_publish_or_acknowledge_hint(context, pipeline):
    service, provider, _ = context

    def disconnect_guard():
        key = pipeline.catalog.one(
            "SELECT hashtextextended(current_schema()||':commerce-order:'||%s,492371253) AS key",
            ("local-store:71",),
        )["key"]
        pid = pipeline.catalog.one(
            "SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid::bigint=%s "
            "AND objid::bigint=%s AND granted",
            ((key >> 32) & 4294967295, key & 4294967295),
        )["pid"]
        assert pipeline.catalog.one("SELECT pg_terminate_backend(%s) AS killed", (pid,))["killed"]

    provider.callback = disconnect_guard
    with pytest.raises(OrderError, match="order_database_guard_unavailable"):
        ingest(service, provider)
    assert pipeline.catalog.all("SELECT * FROM commerce_order_snapshots") == []
    assert pipeline.catalog.one("SELECT revision FROM commerce_order_heads")["revision"] == 0
    receipt = service.inbox("local-store", True)["items"][0]
    assert receipt["state"] == "pending" and receipt["completed_revision"] is None
    provider.callback = None
    assert ingest(service, provider)["revision"] == 1
    assert service.inbox("local-store", True)["items"] == []


def test_database_session_loss_inside_final_transaction_rolls_back_insert(
    context, pipeline, monkeypatch
):
    service, provider, _ = context
    guard = service._fetch_guard

    class DisconnectedConnection:
        def __init__(self, connection):
            self.connection = connection

        def transaction(self):
            return self.connection.transaction()

        def execute(self, query, *args, **kwargs):
            result = self.connection.execute(query, *args, **kwargs)
            if query.startswith("INSERT INTO commerce_order_snapshots"):
                killed = pipeline.catalog.one(
                    "SELECT pg_terminate_backend(%s) AS killed", (self.connection.info.backend_pid,)
                )
                assert killed["killed"]
            return result

    @contextmanager
    def disconnected(*args):
        with guard(*args) as connection:
            yield DisconnectedConnection(connection)

    monkeypatch.setattr(service, "_fetch_guard", disconnected)
    with pytest.raises(OrderError, match="order_database_guard_unavailable"):
        ingest(service, provider)
    monkeypatch.setattr(service, "_fetch_guard", guard)
    assert pipeline.catalog.all("SELECT * FROM commerce_order_snapshots") == []
    assert pipeline.catalog.one("SELECT revision FROM commerce_order_heads")["revision"] == 0
    receipt = service.inbox("local-store", True)["items"][0]
    assert receipt["state"] == "pending" and receipt["completed_revision"] is None
    assert ingest(service, provider)["revision"] == 1


@pytest.mark.parametrize(
    "change", [{"sku": "different"}, {"variant_id": "61"}, {"product_id": "52"}]
)
def test_a_partial_listing_identity_match_never_claims_product_ownership(context, change):
    service, provider, _ = context
    provider.snapshot = replace(
        provider.snapshot, lines=(replace(provider.snapshot.lines[0], **change),)
    )
    assert read(service, provider)["line_links"] == [{"line_id": "81", "state": "unmatched"}]


def test_two_order_read_slots_bound_parallel_resource_use(context):
    service, provider, _ = context
    entered, release = Barrier(3), Event()

    def block():
        entered.wait(15)
        assert release.wait(15)

    provider.callback = block
    with ThreadPoolExecutor(max_workers=2) as pool:
        active = [pool.submit(read, service, provider, order) for order in ("71", "72")]
        entered.wait(15)
        try:
            with pytest.raises(OrderError, match="order_capacity_busy"):
                read(service, provider, "73")
            assert provider.calls == 2
        finally:
            release.set()
        assert all(item.result(15)["revision"] == 1 for item in active)


def test_read_failures_leave_safe_pending_work_for_restart_without_raw_error_persistence(context):
    service, provider, _ = context

    def fail():
        raise RuntimeError("private-customer@example.test")

    provider.callback = fail
    with pytest.raises(OrderError, match="order_provider_failed") as caught:
        ingest(service, provider)
    assert "private-customer" not in str(caught.value)
    pending = service.inbox("local-store", synthetic=True)
    assert pending["items"][0]["state"] == "pending"
    assert "private-customer" not in str(pending)
    provider.callback = None
    read(service, provider)
    assert service.inbox("local-store", synthetic=True)["items"] == []
    assert len(service.inbox("local-store", synthetic=True, pending_only=False)["items"]) == 1


def test_a_lost_fetch_token_cannot_publish_its_stale_read(context, pipeline):
    service, provider, _ = context

    def lose_claim():
        pipeline.catalog.execute("UPDATE commerce_order_heads SET fetch_token='different-worker'")

    provider.callback = lose_claim
    with pytest.raises(OrderError, match="fetch_claim_lost"):
        read(service, provider)
    assert pipeline.catalog.all("SELECT * FROM commerce_order_snapshots") == []
    provider.callback = None
    assert read(service, provider)["revision"] == 1


def test_configured_shop_and_namespace_cannot_be_rebound_by_headers_or_new_config(context):
    service, provider, _ = context
    read(service, provider)
    with pytest.raises(OrderError, match="workspace_mismatch"):
        service.reconcile_order(provider, "71", synthetic=False)
    with pytest.raises(OrderError, match="order_not_found"):
        service.order("local-store", "71", synthetic=False)
    original = provider.config
    provider.config = replace(original, base_url="http://127.0.0.1:8")
    with pytest.raises(OrderError, match="connection_identity_mismatch"):
        read(service, provider)
    provider.config = replace(original, consumer_secret="rotated-secret")
    assert read(service, provider)["revision"] == 1
    with pytest.raises(ProviderError, match="shop_mismatch"):
        ingest(service, provider, **{"X-WC-Webhook-Source": "https://other.example"})


def test_listing_registration_requires_actual_success_and_matching_dossier_identity(
    context, pipeline
):
    service, _provider, operation = context
    assert service.register_listing(operation["operation_id"], True)["dossier_revision"] == 1
    assert len(pipeline.catalog.all("SELECT * FROM commerce_order_listings")) == 1
    pipeline.catalog.execute("UPDATE commerce_delivery_operations SET state='uncertain'")
    with pytest.raises(OrderError, match="not_succeeded"):
        service.register_listing(operation["operation_id"], True)


def test_multiple_products_keep_unallocated_order_cash_and_ambiguous_lines_explicit(
    context, pipeline
):
    service, provider, _ = context
    pipeline.create_product("p2", "Second product", synthetic=True)
    launches = LaunchService(pipeline.catalog)
    launches.save(draft(dossier_id="launch-p2-fr", product_id="p2"), 0, "second")
    second_provider = ControlledProvider()
    second_provider.prepare_draft = lambda value: PreparedProduct("52", None, value.sku, None)
    operation = service.delivery.prepare(
        "launch-p2-fr", second_provider, 1, "second", synthetic=True
    )
    service.register_listing(operation["operation_id"], True)
    provider.snapshot = replace(
        provider.snapshot,
        total="100.00",
        lines=provider.snapshot.lines
        + (
            OrderLine("82", "52", None, operation["sku"], 1),
            OrderLine("83", "999", None, "foreign", 1),
        ),
        payments=(PaymentSnapshot("capture-1", "capture", "100.00", "EUR"),),
    )
    result = read(service, provider)
    assert [line["state"] for line in result["line_links"]] == ["matched", "matched", "unmatched"]
    assert {line.get("canonical_product_id") for line in result["line_links"]} == {"p1", "p2", None}
    assert all("amount" not in line and "total" not in line for line in result["line_links"])
    assert (
        result["financial_allocation"] == "unallocated_order_totals"
        and result["cash_complete"] is False
    )
    # Two owned operations with different dossier revisions cannot pick an arbitrary anchor.
    launches.save(draft(title="New revision"), 1, "revision-two")
    updated = service.delivery.prepare("launch-p1-fr", provider, 2, "updated", synthetic=True)
    service.register_listing(updated["operation_id"], True)
    provider.snapshot = replace(provider.snapshot, updated_at="2026-10-28T11:00:00Z")
    assert read(service, provider)["line_links"][0]["state"] == "ambiguous"


def test_edited_total_paid_timestamp_and_refund_record_are_not_confirmed_cash(context):
    service, provider, _ = context
    provider.snapshot = replace(
        provider.snapshot, total="999.99", refunds=(RefundSnapshot("91", "10.00", "EUR", False),)
    )
    result = read(service, provider)
    assert result["payload"]["paid_at"] is not None
    assert result["payload"]["payments"] == []
    assert result["payload"]["refunds"][0]["payment_confirmed"] is False
    assert result["cash_complete"] is False


def test_mixed_currency_or_conflicting_payment_ids_are_rejected_without_changing_snapshot(context):
    service, provider, _ = context
    original = read(service, provider)
    provider.snapshot = replace(
        provider.snapshot, payments=(PaymentSnapshot("t", "capture", "1", "USD"),)
    )
    with pytest.raises(OrderError, match="invalid_payment_currency"):
        read(service, provider)
    provider.snapshot = replace(
        provider.snapshot,
        payments=(
            PaymentSnapshot("t", "capture", "1", "EUR"),
            PaymentSnapshot("t", "capture", "2", "EUR"),
        ),
    )
    with pytest.raises(OrderError, match="business_identity_conflict"):
        read(service, provider)
    assert service.order("local-store", "71", True)["payload"] == original["payload"]


def test_currency_mismatch_does_not_assign_order_line_to_dossier(context):
    service, provider, _ = context
    provider.snapshot = replace(provider.snapshot, currency="USD")
    assert read(service, provider)["line_links"] == [
        {"line_id": "81", "state": "currency_mismatch"}
    ]


def test_bounded_one_page_reconciliation_and_cursor_order_pages(context):
    service, provider, _ = context
    provider.page = OrderPage(("71", "72", "72"), "next")
    result = service.reconcile_page(provider, "2026-10-01T00:00:00Z", synthetic=True)
    assert len(result["orders"]) == 2 and provider.calls == 2
    assert result["next_cursor"] == "next" and result["coverage_complete"] is False
    first = service.orders("local-store", True, limit=1)
    assert first["items"][0]["order_id"] == "71" and first["next_cursor"] == "71"
    assert (
        service.orders("local-store", True, after=first["next_cursor"])["items"][0]["order_id"]
        == "72"
    )
    provider.page = OrderPage(tuple(str(i) for i in range(51)), None)
    with pytest.raises(OrderError, match="invalid_order_page"):
        service.reconcile_page(provider, "2026-10-01T00:00:00Z", synthetic=True)
    for limit in (0, 101, True):
        with pytest.raises(OrderError):
            service.orders("local-store", True, limit=limit)


def test_manual_handoff_tracking_and_notification_are_idempotent_cas_intents(context, pipeline):
    service, provider, _ = context
    read(service, provider)
    handoff = service.handoff(
        "local-store", "71", "handoff", "supplier-quote-123", ["81"], synthetic=True
    )
    assert handoff["payload"]["external_action"] == "none" and handoff["revision"] == 1
    assert (
        service.handoff(
            "local-store", "71", "handoff", "supplier-quote-123", ["81"], synthetic=True
        )
        == handoff
    )
    with pytest.raises(OrderError, match="event_request_conflict"):
        service.handoff("local-store", "71", "handoff", "different", ["81"], synthetic=True)
    with pytest.raises(OrderError, match="event_revision_conflict"):
        service.handoff(
            "local-store", "71", "another", "supplier-quote-123", ["81"], synthetic=True
        )
    tracking = service.tracking(
        "local-store", "71", "tracking", 1, "carrier", "TRACK-123", synthetic=True
    )
    assert tracking["payload"]["carrier_verified"] is False
    assert (
        service.tracking("local-store", "71", "tracking", 1, "carrier", "TRACK-123", synthetic=True)
        == tracking
    )
    notification = service.request_notification(
        "local-store", "71", "notify", "shipping_update", synthetic=True
    )
    assert notification["payload"] == {
        "notification_kind": "shipping_update",
        "state": "requested",
        "external_action": "none",
        "confirmed_delivered": False,
    }
    assert provider.calls == 1
    with pytest.raises(OrderError, match="order_not_found"):
        service.handoff("local-store", "71", "live", "supplier", ["81"], synthetic=False)
    service.handoff(
        "local-store",
        "71",
        "cancel",
        "supplier-quote-123",
        ["81"],
        expected_revision=1,
        state="cancelled",
        synthetic=True,
    )
    with pytest.raises(OrderError, match="current_handoff"):
        service.tracking(
            "local-store",
            "71",
            "newtrack",
            1,
            "carrier",
            "TRACK-123",
            expected_revision=1,
            synthetic=True,
        )
    assert pipeline.catalog.one("SELECT count(*) AS n FROM commerce_order_events")["n"] == 4


def test_manual_handoff_cannot_prepare_unknown_unowned_or_cancelled_lines(context):
    service, provider, _ = context
    provider.snapshot = replace(
        provider.snapshot, lines=(OrderLine("81", "foreign", None, "foreign", 1),)
    )
    read(service, provider)
    with pytest.raises(OrderError, match="owned_lines"):
        service.handoff("local-store", "71", "h", "supplier", ["81"], synthetic=True)
    provider.snapshot = replace(
        provider.snapshot,
        updated_at="2026-10-28T11:00:00Z",
        cancelled=True,
        lines=(OrderLine("81", "51", None, provider.product.sku, 1),),
    )
    read(service, provider)
    with pytest.raises(OrderError, match="cancelled_order_handoff"):
        service.handoff("local-store", "71", "h", "supplier", ["81"], synthetic=True)


def test_parallel_manual_handoffs_accept_only_one_expected_revision(context):
    service, provider, _ = context
    read(service, provider)

    def attempt(index):
        try:
            return service.handoff(
                "local-store", "71", f"h-{index}", "supplier", ["81"], synthetic=True
            )["revision"]
        except OrderError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, range(2)))
    assert 1 in results and "event_revision_conflict" in results


def test_prepared_handoff_can_be_cancelled_when_later_order_evidence_conflicts(context):
    service, provider, _ = context
    read(service, provider)
    service.handoff("local-store", "71", "prepare", "supplier", ["81"], synthetic=True)
    provider.snapshot = replace(provider.snapshot, total="1")
    assert read(service, provider)["state"] == "conflict"
    cancelled = service.handoff(
        "local-store",
        "71",
        "cancel",
        "supplier",
        ["81"],
        expected_revision=1,
        synthetic=True,
        state="cancelled",
    )
    assert cancelled["payload"]["state"] == "cancelled"


def test_real_http_adapter_strips_customer_pii_and_does_not_persist_webhook_body(pipeline):
    store = ControlledStore()
    try:
        provider = WooCommerceProvider(store.config())
        service = CommerceOrders(pipeline.catalog)
        service.initialize()
        store.replies.append(Reply(woo_order(refunds=[])))
        result = ingest(service, provider, {"id": 71, "billing": {"email": "PII@example.test"}})
        assert result["payload"]["payments"] == []
        data = str(pipeline.catalog.all("SELECT * FROM commerce_order_snapshots")) + str(
            pipeline.catalog.all("SELECT * FROM commerce_order_inbox")
        )
        assert (
            "PII@example.test" not in data
            and "private@example.test" not in data
            and "DO NOT RETAIN" not in data
        )
        assert len(store.requests) == 1 and store.requests[0][0] == "GET"
    finally:
        store.close()

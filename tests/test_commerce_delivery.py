"""Native PostgreSQL outbox recovery through the private delivery interface."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import Any

import pytest
from test_launch_dossiers import draft as launch_draft

from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_delivery import CommerceDelivery, DeliveryError
from sm_trending_products.commerce_providers import (
    ConnectionConfig,
    DraftLookup,
    DraftProduct,
    PreparedProduct,
    ProviderCapabilities,
    ProviderError,
    WooCommerceProvider,
)
from sm_trending_products.contracts import digest


class ControlledProvider:
    """An explicitly synthetic in-memory remote with observable failure boundaries."""

    capabilities = ProviderCapabilities()

    def __init__(self) -> None:
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
        self.product: PreparedProduct | None = None
        self.calls: list[tuple[str, Any]] = []
        self.mode = "normal"
        self.started = Event()
        self.release = Event()
        self.before_write: Any = None

    def lookup_draft(self, launch_id: str, sku: str) -> DraftLookup:
        self.calls.append(("lookup", (launch_id, sku)))
        if self.mode == "unknown_lookup":
            return DraftLookup("unknown")
        if self.product is None:
            return DraftLookup("absent")
        assert self.product.sku == sku
        return DraftLookup("found", self.product)

    def prepare_draft(self, draft: DraftProduct) -> PreparedProduct:
        self.calls.append(("prepare", draft))
        if self.before_write is not None:
            self.before_write()
        if self.mode == "hold":
            self.started.set()
            assert self.release.wait(timeout=10)
        self.product = PreparedProduct("51", None, draft.sku, "http://127.0.0.1:9/product/51")
        if self.mode == "lost_response":
            raise ProviderError("transport_failed", ambiguous=True)
        if self.mode == "partial_resource":
            raise ProviderError("transport_failed", ambiguous=True, resource_id="51")
        if self.mode == "unexpected_exception":
            raise RuntimeError("PRIVATE-provider-error")
        if self.mode == "crash":
            raise SystemExit("synthetic worker crash")
        return self.product

    def update_draft(self, product_id: str, draft: DraftProduct) -> PreparedProduct:
        assert product_id == "51"
        self.calls.append(("update", draft))
        if self.before_write is not None:
            self.before_write()
        if self.mode == "hold":
            self.started.set()
            assert self.release.wait(timeout=10)
        if self.mode == "lost_response":
            raise ProviderError("transport_failed", ambiguous=True, resource_id="51")
        self.product = PreparedProduct("51", None, draft.sku, "http://127.0.0.1:9/product/51")
        return self.product


@pytest.fixture
def context(pipeline):
    pipeline.create_product("p1", "Fixture product", synthetic=True)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    original = launch_draft()
    launches.save(original, 0, "create-dossier")
    delivery = CommerceDelivery(pipeline.catalog)
    delivery.initialize()
    return launches, delivery, ControlledProvider(), original


def prepare(context, request_id="prepare-1", revision=1):
    launches, delivery, provider, original = context
    return delivery.prepare(original.dossier_id, provider, revision, request_id, synthetic=True)


def test_command_is_durable_before_http_and_holds_no_sql_transaction(context):
    launches, delivery, provider, original = context

    def inspect_boundary():
        row = launches.catalog.one("SELECT * FROM commerce_delivery_operations")
        assert row["state"] == "running" and row["attempt_token"]
        active = launches.catalog.all(
            "SELECT state FROM pg_stat_activity WHERE datname=current_database() "
            "AND backend_type='client backend' AND query LIKE '%%commerce-delivery%%'"
        )
        assert not any(row["state"] == "idle in transaction" for row in active)

    provider.before_write = inspect_boundary
    record = prepare(context)
    assert record["state"] == "succeeded" and record["dossier_revision"] == 1
    assert record["product_id"] == "51" and record["synthetic"]
    sent = next(call[1] for call in provider.calls if call[0] == "prepare")
    assert sent.price == "36.00" and sent.currency == "EUR" and sent.price_includes_tax is True
    assert sent.sku == record["sku"] and sent.launch_id == record["launch_id"]
    assert "PRIVATE" not in str(record)
    assert delivery.operation(record["operation_id"], True) == record
    with pytest.raises(DeliveryError, match="operation_not_found"):
        delivery.operation(record["operation_id"])


def test_exact_replay_is_read_only_and_request_conflict_is_rejected(context):
    first = prepare(context)
    calls = list(context[2].calls)
    replay = prepare(context)
    assert replay == first and context[2].calls == calls
    with pytest.raises(DeliveryError, match="request_conflict"):
        prepare(context, revision=2)
    with pytest.raises(DeliveryError, match="revision_already_applied"):
        prepare(context, request_id="another")


def test_response_lost_never_blindly_creates_again_and_recovery_updates_owned_id(context):
    _, delivery, provider, _ = context
    provider.mode = "lost_response"
    uncertain = prepare(context)
    assert uncertain["state"] == "uncertain"
    assert uncertain["error_code"] == "transport_failed"
    calls = len(provider.calls)
    assert prepare(context) == uncertain and len(provider.calls) == calls
    provider.mode = "normal"
    recovered = delivery.recover(uncertain["operation_id"], provider, synthetic=True)
    assert recovered["state"] == "succeeded" and recovered["product_id"] == "51"
    assert [kind for kind, _ in provider.calls].count("prepare") == 1
    assert provider.calls[-1][0] == "update"


def test_uncertain_absence_remains_uncertain_and_blocks_new_revision(context):
    launches, delivery, provider, original = context
    provider.mode = "lost_response"
    uncertain = prepare(context)
    provider.product = None
    provider.mode = "normal"
    launches.save(original.model_copy(update={"title": "New version"}), 1, "revise")
    with pytest.raises(DeliveryError, match="unresolved_operation"):
        prepare(context, request_id="newer", revision=2)
    still_uncertain = delivery.recover(uncertain["operation_id"], provider, True)
    assert still_uncertain["state"] == "uncertain"
    assert still_uncertain["error_code"] == "lookup_unresolved"
    assert [kind for kind, _ in provider.calls].count("prepare") == 1


def test_known_partial_resource_recovery_does_not_depend_on_search_index(context):
    _, delivery, provider, _ = context
    provider.mode = "partial_resource"
    uncertain = prepare(context)
    assert uncertain["product_id"] == "51"
    provider.mode = "unknown_lookup"
    recovered = delivery.recover(uncertain["operation_id"], provider, True)
    assert recovered["state"] == "succeeded"
    assert provider.calls[-1][0] == "update"


def test_crashed_running_command_requires_recovery_and_does_not_create_again(context):
    _, delivery, provider, _ = context
    provider.mode = "crash"
    with pytest.raises(SystemExit):
        prepare(context)
    operation = context[0].catalog.one("SELECT operation_id FROM commerce_delivery_operations")
    record = delivery.operation(operation["operation_id"], True)
    assert record["state"] == "running"
    provider.mode = "normal"
    recovered = delivery.recover(record["operation_id"], provider, True)
    assert recovered["state"] == "succeeded"
    assert [kind for kind, _ in provider.calls].count("prepare") == 1


def test_initial_unknown_read_creates_nothing_and_allows_a_new_safe_command(context):
    _, _, provider, _ = context
    provider.mode = "unknown_lookup"
    failed = prepare(context)
    assert failed["state"] == "failed" and failed["error_code"] == "lookup_unresolved"
    assert [kind for kind, _ in provider.calls] == ["lookup"]
    provider.mode = "normal"
    assert prepare(context, request_id="new-safe-attempt")["state"] == "succeeded"


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"synthetic": False}, "workspace_mismatch"),
        ({"enabled": False}, "connection_disabled"),
    ],
)
def test_wrong_workspace_and_disabled_connection_never_dispatch(context, change, code):
    _, delivery, provider, original = context
    synthetic = change.get("synthetic", True)
    if "enabled" in change:
        provider.config = replace(provider.config, enabled=change["enabled"])
    with pytest.raises(DeliveryError, match=code):
        delivery.prepare(original.dossier_id, provider, 1, "prepare-1", synthetic=synthetic)
    assert provider.calls == []


def test_stale_or_incomplete_dossier_and_revision_conflict_refuse_http(context):
    launches, _, provider, original = context
    with pytest.raises(DeliveryError, match="revision_conflict"):
        prepare(context, revision=2)
    stale = original.model_copy(update={"quote_expires_at": datetime.now(UTC) - timedelta(days=1)})
    launches.save(stale, 1, "stale")
    with pytest.raises(DeliveryError, match="dossier_not_ready"):
        prepare(context, revision=2)
    assert provider.calls == []


def test_connection_identity_binds_store_but_not_rotating_secrets(context):
    _, delivery, provider, _ = context
    first = prepare(context)
    provider.config = replace(provider.config, consumer_secret="rotated-PRIVATE-secret")
    assert prepare(context) == first
    provider.config = replace(provider.config, base_url="http://127.0.0.1:10")
    with pytest.raises(DeliveryError, match="connection_identity_mismatch"):
        prepare(context)
    with pytest.raises(DeliveryError, match="connection_identity_mismatch"):
        delivery.recover(first["operation_id"], provider, True)


def test_parallel_prepare_and_recover_refuse_active_worker_without_duplicate_write(context):
    _, delivery, provider, original = context
    provider.mode = "hold"
    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(prepare, context)
        try:
            assert provider.started.wait(timeout=5)
            operation = context[0].catalog.one(
                "SELECT operation_id FROM commerce_delivery_operations"
            )
            other = CommerceDelivery(context[0].catalog)
            with pytest.raises(DeliveryError, match="operation_busy"):
                other.prepare(original.dossier_id, provider, 1, "same", True)
            with pytest.raises(DeliveryError, match="operation_busy"):
                delivery.recover(operation["operation_id"], provider, True)
        finally:
            provider.release.set()
        assert future.result(timeout=5)["state"] == "succeeded"
    assert [kind for kind, _ in provider.calls].count("prepare") == 1


def test_completed_old_recovery_cannot_overwrite_newer_success(context):
    launches, delivery, provider, original = context
    first = prepare(context)
    revised = original.model_copy(
        update={"title": "New winning version", "description": "A < B & C"}
    )
    launches.save(revised, 1, "revise")
    second = prepare(context, request_id="prepare-2", revision=2)
    calls = list(provider.calls)
    assert first["sku"] == second["sku"] and first["launch_id"] == second["launch_id"]
    assert second["dossier_revision"] == 2 and provider.calls[-1][1].title == revised.title
    assert provider.calls[-1][1].description_html == "A &lt; B &amp; C"
    assert delivery.recover(first["operation_id"], provider, True) == first
    assert provider.calls == calls
    target = launches.catalog.one("SELECT applied_revision FROM commerce_delivery_targets")
    assert target["applied_revision"] == 2


def test_unknown_adapter_error_is_uncertain_and_sanitized(context):
    context[2].mode = "unexpected_exception"
    record = prepare(context)
    assert record["state"] == "uncertain"
    assert record["error_code"] == "provider_operation_failed"
    assert "PRIVATE" not in str(record)


def test_initialize_is_explicit_and_does_not_change_capture_migrations(pipeline):
    service = CommerceDelivery(pipeline.catalog)
    with pytest.raises(DeliveryError, match="dossiers_not_initialized"):
        service.initialize()
    LaunchService(pipeline.catalog).initialize()
    before = pipeline.catalog.all("SELECT * FROM schema_migrations ORDER BY version")
    with pytest.raises(DeliveryError, match="not_initialized"):
        service.operation("missing", True)
    assert service.initialize() == {
        "module": "commerce_delivery",
        "version": 1,
        "status": "initialized",
    }
    assert service.initialize()["version"] == 1
    assert pipeline.catalog.all("SELECT * FROM schema_migrations ORDER BY version") == before


def test_pending_command_recovery_can_make_its_first_safe_attempt(context, monkeypatch):
    _, delivery, provider, _ = context
    execute = delivery._execute
    monkeypatch.setattr(
        delivery, "_execute", lambda operation, *args, **kwargs: delivery._public(operation)
    )
    pending = prepare(context)
    assert pending["state"] == "pending" and provider.calls == []
    monkeypatch.setattr(delivery, "_execute", execute)
    recovered = delivery.recover(pending["operation_id"], provider, True)
    assert recovered["state"] == "succeeded"
    assert [kind for kind, _ in provider.calls].count("prepare") == 1


def test_crashed_unknown_create_with_absent_lookup_cannot_be_recreated(context):
    _, delivery, provider, _ = context
    provider.mode = "crash"
    with pytest.raises(SystemExit):
        prepare(context)
    provider.product = None
    provider.mode = "normal"
    operation = context[0].catalog.one("SELECT operation_id FROM commerce_delivery_operations")
    recovered = delivery.recover(operation["operation_id"], provider, True)
    assert recovered["state"] == "uncertain"
    assert [kind for kind, _ in provider.calls].count("prepare") == 1


def test_expired_frozen_quote_stops_uncertain_recovery_writes(context):
    launches, delivery, provider, _ = context
    provider.mode = "lost_response"
    uncertain = prepare(context)
    # Advance the persisted fixture quote rather than relying on wall-clock sleeps.
    with launches.catalog.transaction() as connection:
        connection.execute(
            "UPDATE commerce_dossier_revisions SET draft=jsonb_set(draft,'{quote_expires_at}',"
            "to_jsonb('2020-01-01T00:00:00Z'::text)) WHERE dossier_id=%s",
            (context[3].dossier_id,),
        )
    calls = list(provider.calls)
    provider.mode = "normal"
    result = delivery.recover(uncertain["operation_id"], provider, True)
    assert result["state"] == "uncertain" and result["error_code"] == "dossier_not_ready"
    assert provider.calls == calls


def test_dedicated_session_capacity_is_bounded_and_released(context):
    launches, delivery, _, original = context
    second_draft = original.model_copy(update={"dossier_id": "second-fr"})
    third_draft = original.model_copy(update={"dossier_id": "third-fr"})
    launches.save(second_draft, 0, "second")
    launches.save(third_draft, 0, "third")
    first, second, third = ControlledProvider(), ControlledProvider(), ControlledProvider()
    first.mode = second.mode = "hold"
    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(delivery.prepare, original.dossier_id, first, 1, "one", True)
        two = pool.submit(delivery.prepare, second_draft.dossier_id, second, 1, "two", True)
        try:
            assert first.started.wait(timeout=5) and second.started.wait(timeout=5)
            with pytest.raises(DeliveryError, match="delivery_capacity_busy"):
                delivery.prepare(third_draft.dossier_id, third, 1, "three", True)
            assert third.calls == []
        finally:
            first.release.set()
            second.release.set()
        assert one.result(timeout=5)["state"] == two.result(timeout=5)["state"] == "succeeded"
    assert delivery.prepare(third_draft.dossier_id, third, 1, "three", True)["state"] == "succeeded"


def test_durable_command_drives_real_controlled_woocommerce_http(context):
    from test_commerce_providers import (
        ControlledStore,
        Reply,
        currency_reply,
        tax_reply,
        woo_product,
    )

    launches, delivery, _, original = context
    store = ControlledStore()
    try:
        provider = WooCommerceProvider(store.config())
        namespace = launches.catalog.one("SELECT current_schema() AS namespace")["namespace"]
        stable = digest([namespace, provider.config.connection_id, original.dossier_id])
        product = woo_product(
            store,
            sku="SM-" + stable[:40],
            regular_price="36.00",
            meta_data=[{"key": "_sm_trending_launch_id", "value": "launch:" + stable}],
        )
        store.replies.extend([Reply([]), Reply([]), currency_reply(), tax_reply(), Reply(product)])
        result = delivery.prepare(original.dossier_id, provider, 1, "real-http", synthetic=True)
        assert result["state"] == "succeeded" and result["product_id"] == "51"
        posts = [request for request in store.requests if request[0] == "POST"]
        assert len(posts) == 1 and posts[0][1] == "/wp-json/wc/v3/products"
        assert posts[0][3]["status"] == "draft" and posts[0][3]["regular_price"] == "36.00"
        assert posts[0][3]["meta_data"][0]["value"] == result["launch_id"]
    finally:
        store.close()

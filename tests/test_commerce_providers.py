"""Real, isolated HTTP exchanges: no credentials or requests to a live store."""

import base64
import hmac
import json
import socket
import threading
from collections import deque
from collections.abc import Iterator
from dataclasses import asdict, dataclass, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from sm_trending_products.commerce_providers import (
    ConnectionConfig,
    DraftProduct,
    ProviderError,
    ShopifyProvider,
    WooCommerceProvider,
    create_provider,
)


@dataclass
class Reply:
    body: Any = None
    status: int = 200
    headers: dict[str, str] | None = None
    raw: bytes | None = None
    lose_response: bool = False


class ControlledStore:
    def __init__(self) -> None:
        self.replies: deque[Reply] = deque()
        self.requests: list[tuple[str, str, dict[str, str], Any]] = []
        store = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.respond()

            def do_PUT(self) -> None:
                self.respond()

            def do_POST(self) -> None:
                self.respond()

            def respond(self) -> None:
                size = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(size)
                store.requests.append(
                    (
                        self.command,
                        self.path,
                        {key.lower(): value for key, value in self.headers.items()},
                        json.loads(raw) if raw else None,
                    )
                )
                if not store.replies:
                    self.send_error(500)
                    return
                reply = store.replies.popleft()
                if reply.lose_response:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                body = reply.raw if reply.raw is not None else json.dumps(reply.body).encode()
                self.send_response(reply.status)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Shopify-API-Version", "2026-10")
                for key, value in (reply.headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=lambda: self.server.serve_forever(poll_interval=0.01), daemon=True
        )
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def config(self, provider: str = "woocommerce", **kwargs: Any) -> ConnectionConfig:
        return ConnectionConfig(
            provider=provider,  # type: ignore[arg-type]
            connection_id="controlled-store",
            base_url=self.url,
            enabled=True,
            test_mode=True,
            consumer_key="fixture-key",
            consumer_secret="fixture-consumer-secret",
            access_token="fixture-token",
            webhook_secret="fixture-webhook-secret",
            **kwargs,
        )

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@pytest.fixture
def store() -> Iterator[ControlledStore]:
    controlled = ControlledStore()
    try:
        yield controlled
    finally:
        controlled.close()


@pytest.fixture
def draft() -> DraftProduct:
    return DraftProduct(
        "launch-123", "SM-FR-123", "Useful product", "<p>Authored text</p>", "29.90", "EUR", True
    )


def woo_product(store: ControlledStore, **updates: Any) -> dict[str, Any]:
    return {
        "id": 51,
        "sku": "SM-FR-123",
        "status": "draft",
        "type": "simple",
        "regular_price": "29.90",
        "permalink": store.url + "/?post_type=product&p=51",
        "meta_data": [{"key": "_sm_trending_launch_id", "value": "launch-123"}],
        **updates,
    }


def shop_product(**updates: Any) -> dict[str, Any]:
    return {
        "id": "gid://shopify/Product/51",
        "status": "DRAFT",
        "handle": "useful-product",
        "metafield": {"value": "launch-123"},
        "variants": {
            "nodes": [{"id": "gid://shopify/ProductVariant/61", "sku": "SM-FR-123"}],
            "pageInfo": {"hasNextPage": False},
        },
        **updates,
    }


def shop_data(**payload: Any) -> Reply:
    if "products" in payload:
        payload.setdefault("skuMatches", {"nodes": [], "pageInfo": {"hasNextPage": False}})
    return Reply({"data": payload})


def tax_reply() -> Reply:
    return Reply({"id": "woocommerce_prices_include_tax", "value": "yes"})


def currency_reply() -> Reply:
    return Reply({"id": "woocommerce_currency", "value": "EUR"})


def test_woo_prepare_uses_rest_v3_explicit_draft_and_shop_currency(
    store: ControlledStore, draft: DraftProduct
) -> None:
    store.replies.extend([Reply([]), currency_reply(), tax_reply(), Reply(woo_product(store))])
    result = create_provider(store.config()).prepare_draft(draft)
    assert result.product_id == "51" and result.sku == draft.sku
    assert result.hosted_product_url == store.url + "/?post_type=product&p=51"
    lookup, currency_request, tax_request, create = store.requests
    assert lookup[:2] == ("GET", "/wp-json/wc/v3/products?sku=SM-FR-123&per_page=2&context=edit")
    assert currency_request[:2] == ("GET", "/wp-json/wc/v3/settings/general/woocommerce_currency")
    assert tax_request[:2] == ("GET", "/wp-json/wc/v3/settings/tax/woocommerce_prices_include_tax")
    assert create[:2] == ("POST", "/wp-json/wc/v3/products")
    assert (
        create[2]["authorization"]
        == "Basic " + base64.b64encode(b"fixture-key:fixture-consumer-secret").decode()
    )
    assert create[3]["status"] == "draft"
    assert create[3]["regular_price"] == "29.90"
    assert create[3]["meta_data"] == [{"key": "_sm_trending_launch_id", "value": draft.launch_id}]
    assert "consumer_secret" not in create[1]


def test_woo_existing_owned_draft_is_updated_without_duplicate_create(
    store: ControlledStore, draft: DraftProduct
) -> None:
    product = woo_product(store)
    store.replies.extend(
        [Reply([product]), Reply(product), currency_reply(), tax_reply(), Reply(product)]
    )
    result = WooCommerceProvider(store.config()).prepare_draft(draft)
    assert result.product_id == "51"
    assert [r[0] for r in store.requests] == ["GET", "GET", "GET", "GET", "PUT"]
    assert store.requests[-1][1] == "/wp-json/wc/v3/products/51"


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"meta_data": []}, "product_ownership_conflict"),
        ({"status": "publish"}, "product_not_draft"),
        ({"type": "variable"}, "unsupported_multiple_variants"),
        ({"permalink": "https://other.example/product/51"}, "unexpected_product_url"),
    ],
)
def test_existing_woo_conflicts_never_write(
    store: ControlledStore, draft: DraftProduct, change: dict[str, Any], code: str
) -> None:
    store.replies.append(Reply([woo_product(store, **change)]))
    with pytest.raises(ProviderError, match=code):
        WooCommerceProvider(store.config()).prepare_draft(draft)
    assert len(store.requests) == 1


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_disabled_and_multiple_variants_refuse_before_http(
    store: ControlledStore, draft: DraftProduct, provider: str
) -> None:
    config = store.config(provider)
    disabled = create_provider(replace(config, enabled=False))
    with pytest.raises(ProviderError, match="connection_disabled"):
        disabled.prepare_draft(draft)
    active = create_provider(config)
    assert not active.capabilities.multiple_variants
    with pytest.raises(ProviderError, match="unsupported_multiple_variants"):
        active.prepare_draft(replace(draft, variant_count=2))
    assert store.requests == []
    assert "fixture-token" not in repr(config)
    assert "fixture-consumer-secret" not in repr(config)


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_currency_mismatch_prevents_price_write(
    store: ControlledStore, draft: DraftProduct, provider: str
) -> None:
    if provider == "woocommerce":
        store.replies.extend([Reply([]), Reply({"id": "woocommerce_currency", "value": "GBP"})])
    else:
        store.replies.extend(
            [
                shop_data(products={"nodes": [], "pageInfo": {"hasNextPage": False}}),
                shop_data(shop={"currencyCode": "GBP", "myshopifyDomain": "127.0.0.1"}),
            ]
        )
    with pytest.raises(ProviderError, match="shop_currency_mismatch"):
        create_provider(store.config(provider)).prepare_draft(draft)
    assert len(store.requests) == 2
    assert all("mutation" not in str(request[3]) for request in store.requests)


def test_shopify_prepare_pins_version_and_updates_initial_variant_narrowly(
    store: ControlledStore, draft: DraftProduct
) -> None:
    initial = shop_product()
    initial["variants"]["nodes"][0]["sku"] = ""
    store.replies.extend(
        [
            shop_data(products={"nodes": [], "pageInfo": {"hasNextPage": False}}),
            shop_data(
                shop={"currencyCode": "EUR", "myshopifyDomain": "127.0.0.1", "taxesIncluded": True}
            ),
            shop_data(productCreate={"product": initial, "userErrors": []}),
            shop_data(
                productVariantsBulkUpdate={
                    "productVariants": [
                        {
                            "id": "gid://shopify/ProductVariant/61",
                            "price": "29.900",
                            "inventoryItem": {"sku": draft.sku},
                        }
                    ],
                    "userErrors": [],
                }
            ),
        ]
    )
    result = ShopifyProvider(store.config("shopify")).prepare_draft(draft)
    assert result.product_id == "gid://shopify/Product/51"
    assert result.variant_id == "gid://shopify/ProductVariant/61"
    assert not result.needs_update
    assert result.hosted_product_url == store.url + "/products/useful-product"
    assert all(r[:2] == ("POST", "/admin/api/2026-10/graphql.json") for r in store.requests)
    assert all(r[2]["x-shopify-access-token"] == "fixture-token" for r in store.requests)
    create, variant = store.requests[2][3], store.requests[3][3]
    assert create["variables"]["product"]["status"] == "DRAFT"
    assert create["variables"]["product"]["metafields"][0]["value"] == draft.launch_id
    assert "productSet" not in str(store.requests)
    assert variant["variables"] == {
        "productId": "gid://shopify/Product/51",
        "variants": [
            {
                "id": "gid://shopify/ProductVariant/61",
                "price": "29.90",
                "inventoryItem": {"sku": draft.sku},
            }
        ],
    }


def test_partial_shopify_create_retains_resource_id_and_reconciles_owned_marker(
    store: ControlledStore, draft: DraftProduct
) -> None:
    initial = shop_product()
    initial["variants"]["nodes"][0]["sku"] = ""
    store.replies.extend(
        [
            shop_data(products={"nodes": [], "pageInfo": {"hasNextPage": False}}),
            shop_data(
                shop={"currencyCode": "EUR", "myshopifyDomain": "127.0.0.1", "taxesIncluded": True}
            ),
            shop_data(productCreate={"product": initial, "userErrors": []}),
            Reply(lose_response=True),
            shop_data(products={"nodes": [initial], "pageInfo": {"hasNextPage": False}}),
        ]
    )
    provider = ShopifyProvider(store.config("shopify"))
    with pytest.raises(ProviderError) as caught:
        provider.prepare_draft(draft)
    assert caught.value.ambiguous
    assert caught.value.resource_id == "gid://shopify/Product/51"
    assert len(store.requests) == 4
    lookup = provider.lookup_draft(draft.launch_id, draft.sku)
    assert lookup.state == "found" and lookup.product is not None
    assert lookup.product.needs_update
    assert len([r for r in store.requests if "mutation CreateDraft" in str(r[3])]) == 1


def test_woo_lost_create_response_is_ambiguous_and_not_retried(
    store: ControlledStore, draft: DraftProduct
) -> None:
    store.replies.extend([Reply([]), currency_reply(), tax_reply(), Reply(lose_response=True)])
    with pytest.raises(ProviderError) as caught:
        WooCommerceProvider(store.config()).prepare_draft(draft)
    assert caught.value.code == "transport_failed" and caught.value.ambiguous
    assert len(store.requests) == 4
    assert store.requests[-1][0] == "POST"


@pytest.mark.parametrize(
    ("reply", "code"),
    [
        (Reply(status=429), "rate_limited"),
        (Reply(status=503), "provider_unavailable"),
        (Reply(raw=b'{"id":1,"id":2}'), "malformed_response"),
        (Reply(raw=b"not-json"), "malformed_response"),
        (Reply(raw=b"x" * 300), "response_too_large"),
        (Reply(status=302, headers={"Location": "http://127.0.0.1/steal"}), "redirect_refused"),
    ],
)
def test_controlled_response_failures_are_bounded_sanitized_and_never_follow_redirects(
    store: ControlledStore, reply: Reply, code: str
) -> None:
    store.replies.append(reply)
    provider = WooCommerceProvider(store.config(max_response_bytes=256))
    with pytest.raises(ProviderError) as caught:
        provider.read_order("71")
    assert caught.value.code == code and not caught.value.ambiguous
    assert "fixture" not in str(caught.value) and "127.0.0.1" not in str(caught.value)
    assert len(store.requests) == 1


def test_unknown_lookup_never_causes_create(store: ControlledStore, draft: DraftProduct) -> None:
    store.replies.append(Reply(status=429))
    provider = WooCommerceProvider(store.config())
    with pytest.raises(ProviderError, match="draft_lookup_unknown"):
        provider.prepare_draft(draft)
    assert len(store.requests) == 1


def test_shopify_wrong_shop_and_api_version_refuse_writes(
    store: ControlledStore, draft: DraftProduct
) -> None:
    store.replies.extend(
        [
            shop_data(products={"nodes": [], "pageInfo": {"hasNextPage": False}}),
            shop_data(shop={"currencyCode": "EUR", "myshopifyDomain": "other.myshopify.com"}),
        ]
    )
    with pytest.raises(ProviderError, match="shop_identity_mismatch"):
        ShopifyProvider(store.config("shopify")).prepare_draft(draft)
    wrong_version = shop_data(order=None)
    wrong_version.headers = {"X-Shopify-API-Version": "2026-07"}
    store.replies.append(wrong_version)
    with pytest.raises(ProviderError, match="shop_api_version_mismatch"):
        ShopifyProvider(store.config("shopify")).read_order("gid://shopify/Order/71")
    assert len(store.requests) == 3


def woo_order(**updates: Any) -> dict[str, Any]:
    return {
        "id": 71,
        "date_modified_gmt": "2026-10-28T10:00:00",
        "currency": "EUR",
        "total": "59.80",
        "status": "processing",
        "date_paid_gmt": "2026-10-28T09:00:00",
        "line_items": [
            {"id": 81, "product_id": 51, "variation_id": 0, "sku": "SM-FR-123", "quantity": 2}
        ],
        "refunds": [{"id": 91}],
        "billing": {"email": "private@example.test", "address_1": "DO NOT RETAIN"},
        **updates,
    }


def test_woo_authoritative_read_distinguishes_refund_record_from_gateway_refund(
    store: ControlledStore,
) -> None:
    store.replies.extend(
        [
            Reply(woo_order()),
            Reply({"id": 91, "amount": "10.00", "refunded_payment": False, "reason": "PRIVATE"}),
            Reply(woo_order(status="cancelled")),
            Reply({"id": 91, "amount": "10.00", "refunded_payment": True}),
        ]
    )
    provider = WooCommerceProvider(store.config())
    before = provider.read_order("71")
    after = provider.read_order("71")
    assert len(before.payments) == 0 and not before.refunds[0].payment_confirmed
    assert len(after.payments) == 1 and after.refunds[0].payment_confirmed
    assert before.paid_at == "2026-10-28T09:00:00Z"
    assert after.cancelled and after.synthetic
    assert after.updated_at == "2026-10-28T10:00:00Z"
    assert "PRIVATE" not in str(asdict(before))
    assert "private@example.test" not in str(asdict(before))
    assert after.lines[0].quantity == 2
    assert all(r[0] == "GET" for r in store.requests)


def shop_money(value: str) -> dict[str, Any]:
    return {"shopMoney": {"amount": value, "currencyCode": "EUR"}}


def transaction(identifier: int, kind: str, status: str, value: str) -> dict[str, Any]:
    return {
        "id": f"gid://shopify/OrderTransaction/{identifier}",
        "kind": kind,
        "status": status,
        "amountSet": shop_money(value),
    }


def shop_order(**updates: Any) -> dict[str, Any]:
    return {
        "id": "gid://shopify/Order/71",
        "updatedAt": "2026-10-28T10:00:00Z",
        "cancelledAt": None,
        "displayFulfillmentStatus": "UNFULFILLED",
        "totalPriceSet": shop_money("59.80"),
        "lineItems": {
            "nodes": [
                {
                    "id": "gid://shopify/LineItem/81",
                    "sku": "SM-FR-123",
                    "quantity": 2,
                    "product": {"id": "gid://shopify/Product/51"},
                    "variant": {"id": "gid://shopify/ProductVariant/61"},
                }
            ],
            "pageInfo": {"hasNextPage": False},
        },
        "transactions": [
            transaction(101, "AUTHORIZATION", "SUCCESS", "59.80"),
            transaction(102, "CAPTURE", "SUCCESS", "59.80"),
            transaction(103, "SALE", "FAILURE", "59.80"),
        ],
        "refunds": [
            {
                "id": "gid://shopify/Refund/91",
                "totalRefundedSet": shop_money("10.00"),
                "transactions": {
                    "nodes": [transaction(104, "REFUND", "PENDING", "10.00")],
                    "pageInfo": {"hasNextPage": False},
                },
            }
        ],
        "email": "PRIVATE@example.test",
        **updates,
    }


def test_shopify_only_successful_money_transactions_count_and_deduplicate(
    store: ControlledStore,
) -> None:
    order = shop_order()
    success = transaction(104, "REFUND", "SUCCESS", "10.00")
    after = shop_order()
    after["transactions"].append(success)
    after["refunds"][0]["transactions"]["nodes"] = [success]
    store.replies.extend([shop_data(order=order), shop_data(order=after)])
    provider = ShopifyProvider(store.config("shopify"))
    pending = provider.read_order("gid://shopify/Order/71")
    refunded = provider.read_order("gid://shopify/Order/71")
    assert len(pending.payments) == 1 and pending.payments[0].kind == "capture"
    assert not pending.refunds[0].payment_confirmed
    assert len(refunded.payments) == 2 and refunded.refunds[0].payment_confirmed
    assert "PRIVATE" not in str(asdict(refunded))
    query = store.requests[0][3]["query"]
    assert "email" not in query and "billingAddress" not in query
    assert "transactions(first: 101)" in query and "pageInfo" in query


def test_shopify_partial_success_does_not_confirm_entire_refund(store: ControlledStore) -> None:
    order = shop_order()
    order["refunds"][0]["transactions"]["nodes"] = [transaction(104, "REFUND", "SUCCESS", "4.00")]
    store.replies.append(shop_data(order=order))
    result = ShopifyProvider(store.config("shopify")).read_order("gid://shopify/Order/71")
    assert not result.refunds[0].payment_confirmed
    assert result.payments[-1].amount == "4.00"


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_changed_order_pages_are_bounded(store: ControlledStore, provider: str) -> None:
    if provider == "woocommerce":
        store.replies.append(Reply([{"id": 71}], headers={"X-WP-TotalPages": "2"}))
        expected_id, next_cursor = "71", "2"
    else:
        store.replies.append(
            shop_data(
                orders={
                    "nodes": [{"id": "gid://shopify/Order/71"}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "next-page"},
                }
            )
        )
        expected_id, next_cursor = "gid://shopify/Order/71", "next-page"
    result = create_provider(store.config(provider)).list_order_ids("2026-10-28T00:00:00Z")
    assert result.order_ids == (expected_id,) and result.next_cursor == next_cursor
    if provider == "woocommerce":
        query = parse_qs(urlsplit(store.requests[0][1]).query)
        assert query["per_page"] == ["50"] and query["dates_are_gmt"] == ["true"]
    else:
        assert "orders(first: 50" in store.requests[0][3]["query"]


def signature(config: ConnectionConfig, raw: bytes) -> str:
    return base64.b64encode(hmac.digest(config.webhook_secret.encode(), raw, "sha256")).decode()


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_raw_hmac_tamper_unsigned_replay_headers_and_authoritative_read(
    store: ControlledStore, provider: str
) -> None:
    config = store.config(provider)
    adapter = create_provider(config)
    if provider == "woocommerce":
        raw = b'{"id":71,"total":"999999.00","billing":{"email":"PRIVATE"}}'
        signature_name = "X-WC-Webhook-Signature"
        headers = {"X-WC-Webhook-Source": store.url, "X-WC-Webhook-Delivery-ID": "delivery-1"}
        store.replies.extend([Reply(woo_order(refunds=[])), Reply(woo_order(refunds=[]))])
        expected_id = "71"
    else:
        raw = b'{"admin_graphql_api_id":"gid://shopify/Order/71","total_price":"999999.00"}'
        signature_name = "X-Shopify-Hmac-SHA256"
        headers = {"X-Shopify-Shop-Domain": "127.0.0.1", "X-Shopify-Webhook-Id": "delivery-1"}
        store.replies.extend([shop_data(order=shop_order()), shop_data(order=shop_order())])
        expected_id = "gid://shopify/Order/71"
    headers[signature_name] = signature(config, raw)
    first = adapter.verify_webhook(raw, headers)
    assert first.order_id == expected_id and first.delivery_id == "delivery-1"
    assert store.requests == []
    assert "999999" not in str(asdict(first)) and "PRIVATE" not in str(asdict(first))
    with pytest.raises(ProviderError, match="invalid_webhook_signature"):
        adapter.verify_webhook(raw + b" ", headers)
    changed = {
        **headers,
        "X-Shopify-Topic": "refunds/create",
        "X-WC-Webhook-Topic": "order.deleted",
    }
    delivery_header = (
        "X-WC-Webhook-Delivery-ID" if provider == "woocommerce" else "X-Shopify-Webhook-Id"
    )
    changed[delivery_header] = "delivery-2"
    replay = adapter.verify_webhook(raw, changed)
    assert replay.order_id == first.order_id and replay.delivery_id != first.delivery_id
    # HMAC does not bind unsigned replay metadata. Fetching the configured shop's
    # state prevents an altered topic/price from fabricating refund/revenue.
    assert adapter.read_order(first.order_id) == adapter.read_order(replay.order_id)


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_webhook_wrong_shop_and_oversized_body_refused(
    store: ControlledStore, provider: str
) -> None:
    config = store.config(provider, max_webhook_bytes=128)
    adapter = create_provider(config)
    raw = b'{"order_id":71}'
    if provider == "woocommerce":
        headers = {
            "X-WC-Webhook-Signature": signature(config, raw),
            "X-WC-Webhook-Source": "https://other.test",
        }
    else:
        headers = {
            "X-Shopify-Hmac-SHA256": signature(config, raw),
            "X-Shopify-Shop-Domain": "other.myshopify.com",
        }
    with pytest.raises(ProviderError, match="webhook_shop_mismatch"):
        adapter.verify_webhook(raw, headers)
    with pytest.raises(ProviderError, match="webhook_too_large"):
        adapter.verify_webhook(b"x" * 129, {})


@pytest.mark.parametrize(
    ("url", "test_mode", "provider"),
    [
        ("http://public.example", False, "woocommerce"),
        ("http://localhost:8080", True, "woocommerce"),
        ("http://192.168.0.1", True, "woocommerce"),
        ("https://key:secret@store.example", False, "woocommerce"),
        ("https://store.example?token=secret", False, "woocommerce"),
        ("https://not-shopify.example", False, "shopify"),
        ("https://shop.myshopify.com/other", False, "shopify"),
    ],
)
def test_origin_validation_fails_closed(url: str, test_mode: bool, provider: str) -> None:
    with pytest.raises(ProviderError, match="invalid_configuration"):
        create_provider(
            ConnectionConfig(
                provider=provider, connection_id="test", base_url=url, test_mode=test_mode
            )
        )  # type: ignore[arg-type]


def test_live_dns_private_resolution_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *args, **kwargs: [(2, 1, 6, "", ("127.0.0.1", 443))]
    )
    config = ConnectionConfig(
        "woocommerce",
        "store",
        "https://store.example",
        enabled=True,
        consumer_key="fixture-key",
        consumer_secret="fixture-secret",
        webhook_secret="fixture-hook",
    )
    with pytest.raises(ProviderError, match="unsafe_store_address"):
        WooCommerceProvider(config).read_order("71")


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_unspecified_tax_basis_refuses_before_http(
    store: ControlledStore, draft: DraftProduct, provider: str
) -> None:
    with pytest.raises(ProviderError, match="price_tax_basis_required"):
        create_provider(store.config(provider)).prepare_draft(
            replace(draft, price_includes_tax=None)
        )
    assert store.requests == []


@pytest.mark.parametrize("provider", ["woocommerce", "shopify"])
def test_actual_catalog_tax_basis_mismatch_refuses_before_price_write(
    store: ControlledStore, draft: DraftProduct, provider: str
) -> None:
    if provider == "woocommerce":
        store.replies.extend(
            [
                Reply([]),
                currency_reply(),
                Reply({"id": "woocommerce_prices_include_tax", "value": "no"}),
            ]
        )
    else:
        store.replies.extend(
            [
                shop_data(products={"nodes": [], "pageInfo": {"hasNextPage": False}}),
                shop_data(
                    shop={
                        "currencyCode": "EUR",
                        "myshopifyDomain": "127.0.0.1",
                        "taxesIncluded": False,
                    }
                ),
            ]
        )
    with pytest.raises(ProviderError, match="shop_tax_basis_mismatch"):
        create_provider(store.config(provider)).prepare_draft(draft)
    assert not any(
        request[0] == "PUT" or "mutation" in str(request[3]) for request in store.requests
    )


@pytest.mark.parametrize(
    ("product", "sku_matches", "code"),
    [
        (shop_product(metafield={"value": "another-launch"}), [], "product_ownership_conflict"),
        (shop_product(status="ACTIVE"), [], "product_not_draft"),
        (
            None,
            [{"sku": "SM-FR-123", "product": {"id": "gid://shopify/Product/999"}}],
            "product_ownership_conflict",
        ),
    ],
)
def test_shopify_existing_ownership_and_unowned_sku_conflicts_refuse_writes(
    store: ControlledStore,
    draft: DraftProduct,
    product: dict[str, Any] | None,
    sku_matches: list[Any],
    code: str,
) -> None:
    store.replies.append(
        shop_data(
            products={
                "nodes": [] if product is None else [product],
                "pageInfo": {"hasNextPage": False},
            },
            skuMatches={"nodes": sku_matches, "pageInfo": {"hasNextPage": False}},
        )
    )
    with pytest.raises(ProviderError, match=code):
        ShopifyProvider(store.config("shopify")).prepare_draft(draft)
    assert len(store.requests) == 1


def test_shopify_direct_update_preserves_ownership_and_never_replaces_lists(
    store: ControlledStore, draft: DraftProduct
) -> None:
    store.replies.extend(
        [
            shop_data(product=shop_product()),
            shop_data(
                shop={"currencyCode": "EUR", "myshopifyDomain": "127.0.0.1", "taxesIncluded": True}
            ),
            shop_data(productUpdate={"product": shop_product(), "userErrors": []}),
            shop_data(
                productVariantsBulkUpdate={
                    "productVariants": [
                        {
                            "id": "gid://shopify/ProductVariant/61",
                            "price": "29.90",
                            "inventoryItem": {"sku": draft.sku},
                        }
                    ],
                    "userErrors": [],
                }
            ),
        ]
    )
    result = ShopifyProvider(store.config("shopify")).update_draft(
        "gid://shopify/Product/51", draft
    )
    assert not result.needs_update
    update = store.requests[2][3]["variables"]["product"]
    assert update["status"] == "DRAFT"
    assert set(update) == {"id", "title", "descriptionHtml", "status"}


@pytest.mark.parametrize(
    "change",
    [
        {"lineItems": {"nodes": [], "pageInfo": {"hasNextPage": True}}},
        {"totalPriceSet": {"shopMoney": {"amount": "NaN", "currencyCode": "EUR"}}},
        {"id": "gid://shopify/Order/999"},
        {"transactions": [transaction(102, "CAPTURE", "SUCCESS", "1.00")] * 101},
    ],
)
def test_shopify_incomplete_or_invalid_order_is_not_accepted_as_complete(
    store: ControlledStore, change: dict[str, Any]
) -> None:
    store.replies.append(shop_data(order=shop_order(**change)))
    with pytest.raises(ProviderError):
        ShopifyProvider(store.config("shopify")).read_order("gid://shopify/Order/71")


def test_graphql_http_200_throttle_is_not_a_success(store: ControlledStore) -> None:
    store.replies.append(
        Reply(
            {
                "errors": [
                    {"message": "PRIVATE server diagnostic", "extensions": {"code": "THROTTLED"}}
                ]
            }
        )
    )
    with pytest.raises(ProviderError) as caught:
        ShopifyProvider(store.config("shopify")).read_order("gid://shopify/Order/71")
    assert caught.value.code == "rate_limited" and caught.value.retryable
    assert "PRIVATE" not in str(caught.value)
    assert len(store.requests) == 1


def test_webhook_duplicate_case_headers_and_wrong_version_are_rejected(
    store: ControlledStore,
) -> None:
    config = store.config("shopify")
    provider = ShopifyProvider(config)
    raw = b'{"order_id":71}'
    signed = {"X-Shopify-Hmac-SHA256": signature(config, raw)}
    with pytest.raises(ProviderError, match="duplicate_webhook_header"):
        provider.verify_webhook(raw, {**signed, "x-shopify-hmac-sha256": "other"})
    with pytest.raises(ProviderError, match="webhook_api_version_mismatch"):
        provider.verify_webhook(raw, {**signed, "X-Shopify-API-Version": "2026-07"})
    assert store.requests == []


def test_woo_paid_timestamp_does_not_invent_cash_from_edited_order_total(
    store: ControlledStore,
) -> None:
    store.replies.append(Reply(woo_order(total="999.00", refunds=[])))
    order = WooCommerceProvider(store.config()).read_order("71")
    assert order.total == "999.00" and order.paid_at is not None
    assert order.payments == ()


def test_woo_wrong_product_identity_refuses_before_settings_or_put(
    store: ControlledStore, draft: DraftProduct
) -> None:
    store.replies.extend(
        [Reply(woo_product(store, id=52)), currency_reply(), tax_reply(), Reply(woo_product(store))]
    )
    with pytest.raises(ProviderError, match="resource_identity_mismatch") as caught:
        WooCommerceProvider(store.config()).update_draft("51", draft)
    assert not caught.value.ambiguous
    assert [(method, path) for method, path, *_ in store.requests] == [
        ("GET", "/wp-json/wc/v3/products/51?context=edit")
    ]

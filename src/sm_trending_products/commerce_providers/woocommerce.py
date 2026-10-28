"""Authenticated WooCommerce REST v3 drafts and authoritative order reads."""

import base64
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode, urlsplit

from ._http import HttpTransport, require_enabled
from ._validation import (
    amount,
    currency,
    header_hint,
    hosted_url,
    numeric_id,
    obj,
    optional_text,
    quantity,
    seq,
    text,
    timestamp,
    validate_draft,
    verified_body,
)
from .contracts import (
    ConnectionConfig,
    DraftLookup,
    DraftProduct,
    OrderLine,
    OrderPage,
    OrderSnapshot,
    PaymentSnapshot,
    PreparedProduct,
    ProviderCapabilities,
    ProviderError,
    RefundSnapshot,
    WebhookHint,
)

_MARKER = "_sm_trending_launch_id"


class WooCommerceProvider:
    capabilities = ProviderCapabilities()

    def __init__(self, config: ConnectionConfig) -> None:
        if config.provider != "woocommerce":
            raise ProviderError("provider_configuration_mismatch")
        self.config = config
        self._http = HttpTransport(config)

    def _request(
        self, method: str, suffix: str, payload: dict[str, Any] | None = None
    ) -> tuple[Any, dict[str, str]]:
        token = base64.b64encode(
            f"{self.config.consumer_key}:{self.config.consumer_secret}".encode()
        ).decode("ascii")
        return self._http.request(
            method,
            "/wp-json/wc/v3/" + suffix,
            {"Authorization": "Basic " + token},
            payload,
            write=method != "GET",
        )

    def _check_catalog_settings(self, draft: DraftProduct) -> None:
        # Core general setting, not an inferred Europe-wide/default currency.
        body, _ = self._request("GET", "settings/general/woocommerce_currency")
        setting = obj(body)
        if (
            setting.get("id") != "woocommerce_currency"
            or currency(setting.get("value")) != draft.currency
        ):
            raise ProviderError("shop_currency_mismatch")
        body, _ = self._request("GET", "settings/tax/woocommerce_prices_include_tax")
        setting = obj(body)
        value = setting.get("value")
        if (
            setting.get("id") != "woocommerce_prices_include_tax"
            or value not in ("yes", "no")
            or (value == "yes") != draft.price_includes_tax
        ):
            raise ProviderError("shop_tax_basis_mismatch")

    def _product(self, raw: Any, launch_id: str, sku: str) -> PreparedProduct:
        item = obj(raw)
        markers = [
            obj(entry).get("value")
            for entry in seq(item.get("meta_data"))
            if obj(entry).get("key") == _MARKER
        ]
        if markers != [launch_id] or item.get("sku") != sku:
            raise ProviderError("product_ownership_conflict")
        if item.get("status") != "draft":
            raise ProviderError("product_not_draft")
        if item.get("type") != "simple" or item.get("variations", []):
            raise ProviderError("unsupported_multiple_variants")
        return PreparedProduct(
            product_id=numeric_id(item.get("id")),
            variant_id=None,
            sku=sku,
            hosted_product_url=hosted_url(self.config, item.get("permalink")),
        )

    def lookup_draft(self, launch_id: str, sku: str) -> DraftLookup:
        require_enabled(self.config)
        text(launch_id)
        text(sku, 100)
        try:
            body, _ = self._request(
                "GET", "products?" + urlencode({"sku": sku, "per_page": 2, "context": "edit"})
            )
            products = seq(body, 2)
            if not products:
                return DraftLookup("absent")
            if len(products) != 1:
                raise ProviderError("product_ownership_conflict")
            return DraftLookup("found", self._product(products[0], launch_id, sku))
        except ProviderError as exc:
            if exc.retryable or exc.code in ("malformed_response", "response_too_large"):
                return DraftLookup("unknown")
            raise

    def prepare_draft(self, draft: DraftProduct) -> PreparedProduct:
        require_enabled(self.config)
        validate_draft(draft)
        existing = self.lookup_draft(draft.launch_id, draft.sku)
        if existing.state == "unknown":
            raise ProviderError("draft_lookup_unknown", retryable=True)
        if existing.product is not None:
            return self.update_draft(existing.product.product_id, draft)
        self._check_catalog_settings(draft)
        payload = self._payload(draft)
        payload["meta_data"] = [{"key": _MARKER, "value": draft.launch_id}]
        body, _ = self._request("POST", "products", payload)
        try:
            result = self._product(body, draft.launch_id, draft.sku)
            self._check_price(body, draft)
            return result
        except ProviderError as exc:
            raise ProviderError(exc.code, ambiguous=True) from None

    def update_draft(self, product_id: str, draft: DraftProduct) -> PreparedProduct:
        require_enabled(self.config)
        validate_draft(draft)
        product_id = numeric_id(product_id)
        body, _ = self._request("GET", "products/" + product_id + "?context=edit")
        existing = self._product(body, draft.launch_id, draft.sku)
        if existing.product_id != product_id:
            raise ProviderError("resource_identity_mismatch")
        self._check_catalog_settings(draft)
        body, _ = self._request("PUT", "products/" + product_id, self._payload(draft))
        try:
            result = self._product(body, draft.launch_id, draft.sku)
            self._check_price(body, draft)
            if result.product_id != product_id:
                raise ProviderError("resource_identity_mismatch")
            return result
        except ProviderError as exc:
            raise ProviderError(exc.code, ambiguous=True, resource_id=product_id) from None

    @staticmethod
    def _check_price(body: Any, draft: DraftProduct) -> None:
        if Decimal(amount(obj(body).get("regular_price"))) != Decimal(draft.price):
            raise ProviderError("variant_update_mismatch")

    @staticmethod
    def _payload(draft: DraftProduct) -> dict[str, Any]:
        return {
            "name": draft.title,
            "description": draft.description_html,
            "type": "simple",
            "status": "draft",
            "sku": draft.sku,
            "regular_price": draft.price,
        }

    def list_order_ids(self, updated_after: str, cursor: str | None = None) -> OrderPage:
        timestamp(updated_after)
        page = 1 if cursor is None else int(numeric_id(cursor))
        if page > 1_000_000:
            raise ProviderError("invalid_cursor")
        body, headers = self._request(
            "GET",
            "orders?"
            + urlencode(
                {
                    "modified_after": updated_after,
                    "dates_are_gmt": "true",
                    "orderby": "modified",
                    "order": "asc",
                    "per_page": 50,
                    "page": page,
                }
            ),
        )
        ids = tuple(numeric_id(obj(item).get("id")) for item in seq(body, 50))
        total_pages = headers.get("x-wp-totalpages")
        if total_pages is None or not total_pages.isdecimal():
            raise ProviderError("invalid_pagination")
        return OrderPage(ids, str(page + 1) if page < int(total_pages) else None)

    def read_order(self, order_id: str) -> OrderSnapshot:
        order_id = numeric_id(order_id)
        body, _ = self._request("GET", "orders/" + order_id)
        order = obj(body)
        if numeric_id(order.get("id")) != order_id:
            raise ProviderError("resource_identity_mismatch")
        order_currency = currency(order.get("currency"))
        total = amount(order.get("total"))
        status = text(order.get("status"), 40)
        if status not in {
            "pending",
            "processing",
            "on-hold",
            "completed",
            "cancelled",
            "refunded",
            "failed",
            "trash",
        }:
            raise ProviderError("unsupported_order_status")
        lines: list[OrderLine] = []
        for raw in seq(order.get("line_items")):
            item = obj(raw)
            lines.append(
                OrderLine(
                    numeric_id(item.get("id")),
                    None if item.get("product_id") == 0 else numeric_id(item.get("product_id")),
                    None if item.get("variation_id") == 0 else numeric_id(item.get("variation_id")),
                    optional_text(item.get("sku"), 100),
                    quantity(item.get("quantity")),
                )
            )
        payments: list[PaymentSnapshot] = []
        # Core WooCommerce exposes paid-state, not an immutable captured amount.
        # Current order.total may change after payment through order edits.
        paid_at = timestamp(order["date_paid_gmt"]) if order.get("date_paid_gmt") else None
        refunds: list[RefundSnapshot] = []
        for reference in seq(order.get("refunds")):
            refund_id = numeric_id(obj(reference).get("id"))
            raw, _ = self._request("GET", f"orders/{order_id}/refunds/{refund_id}")
            refund = obj(raw)
            if numeric_id(refund.get("id")) != refund_id:
                raise ProviderError("resource_identity_mismatch")
            refunded = refund.get("refunded_payment")
            if not isinstance(refunded, bool):
                raise ProviderError("invalid_provider_schema")
            refund_amount = amount(refund.get("amount"))
            refunds.append(RefundSnapshot(refund_id, refund_amount, order_currency, refunded))
            if refunded:
                payments.append(
                    PaymentSnapshot("refund:" + refund_id, "refund", refund_amount, order_currency)
                )
        return OrderSnapshot(
            self.config.connection_id,
            "woocommerce",
            order_id,
            timestamp(order.get("date_modified_gmt")),
            order_currency,
            total,
            status in {"cancelled", "trash"},
            # "completed" is an order status, not proof of carrier delivery.
            "order_completed" if status == "completed" else "unknown",
            tuple(lines),
            tuple(payments),
            tuple(refunds),
            self.config.test_mode,
            paid_at,
        )

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str]) -> WebhookHint:
        body, lowered = verified_body(self.config, raw_body, headers, "x-wc-webhook-signature")
        source = lowered.get("x-wc-webhook-source")
        if source is not None:
            actual, expected = urlsplit(source), urlsplit(self.config.base_url)
            if (actual.scheme, actual.netloc, actual.path.rstrip("/")) != (
                expected.scheme,
                expected.netloc,
                expected.path.rstrip("/"),
            ):
                raise ProviderError("webhook_shop_mismatch")
        # No dispatch based on unsigned topic/resource. A refund payload may use
        # order_id; otherwise the signed body ID is merely a lookup hint.
        order_id = numeric_id(body.get("order_id", body.get("id")))
        return WebhookHint(order_id, header_hint(lowered, "x-wc-webhook-delivery-id"))

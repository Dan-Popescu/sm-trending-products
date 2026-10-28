"""Narrow Shopify Admin GraphQL 2026-10 draft and order contracts.

Product search absence is not an idempotency guarantee. The durable caller must
not recreate a timed-out product just because a search index has no match yet.
"""

from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

from ._http import HttpTransport, require_enabled
from ._validation import (
    amount,
    currency,
    gid,
    header_hint,
    hosted_url,
    obj,
    optional_text,
    owned_marker,
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

_PRODUCT_FIELDS = """
id status handle
metafield(namespace: "sm_trending_products", key: "launch_id") { value }
variants(first: 2) { nodes { id sku } pageInfo { hasNextPage } }
"""
_TRANSACTION_FIELDS = "id kind status amountSet { shopMoney { amount currencyCode } }"
_ORDER_QUERY = """
query OrderSnapshot($id: ID!) {
  order(id: $id) {
    id updatedAt cancelledAt displayFulfillmentStatus
    totalPriceSet { shopMoney { amount currencyCode } }
    lineItems(first: 100) {
      nodes { id sku quantity product { id } variant { id } }
      pageInfo { hasNextPage }
    }
    transactions(first: 101) { TRANSACTION_FIELDS }
    refunds {
      id totalRefundedSet { shopMoney { amount currencyCode } }
      transactions(first: 100) {
        nodes { TRANSACTION_FIELDS }
        pageInfo { hasNextPage }
      }
    }
  }
}
""".replace("TRANSACTION_FIELDS", _TRANSACTION_FIELDS)


def _nodes(value: Any, maximum: int = 100) -> list[Any]:
    connection = obj(value)
    if obj(connection.get("pageInfo")).get("hasNextPage") is not False:
        raise ProviderError("unsupported_provider_volume")
    return seq(connection.get("nodes"), maximum)


def _money(value: Any, expected_currency: str | None = None) -> tuple[str, str]:
    money = obj(obj(value).get("shopMoney"))
    code = currency(money.get("currencyCode"))
    if expected_currency is not None and code != expected_currency:
        raise ProviderError("order_currency_mismatch")
    return amount(money.get("amount")), code


class ShopifyProvider:
    capabilities = ProviderCapabilities()

    def __init__(self, config: ConnectionConfig) -> None:
        if config.provider != "shopify":
            raise ProviderError("provider_configuration_mismatch")
        self.config = config
        self._http = HttpTransport(config)

    def _query(
        self, query: str, variables: dict[str, Any] | None = None, *, write: bool = False
    ) -> dict[str, Any]:
        body, headers = self._http.request(
            "POST",
            f"/admin/api/{self.config.api_version}/graphql.json",
            {"X-Shopify-Access-Token": self.config.access_token},
            {"query": query, "variables": variables or {}},
            write=write,
        )
        if headers.get("x-shopify-api-version") != self.config.api_version:
            raise ProviderError("shop_api_version_mismatch", ambiguous=write)
        try:
            result = obj(body)
            errors = result.get("errors")
            if errors:
                throttled = any(
                    obj(obj(error).get("extensions", {})).get("code") == "THROTTLED"
                    for error in seq(errors)
                )
                # A partial data result plus errors cannot prove a mutation failed.
                raise ProviderError(
                    "rate_limited" if throttled else "graphql_rejected",
                    retryable=throttled,
                    ambiguous=write,
                )
            return obj(result.get("data"))
        except ProviderError as exc:
            raise ProviderError(exc.code, retryable=exc.retryable, ambiguous=write) from None

    def _shop(self, draft: DraftProduct) -> None:
        data = self._query(
            "query ShopIdentity { shop { currencyCode myshopifyDomain taxesIncluded } }"
        )
        shop = obj(data.get("shop"))
        expected_host = urlsplit(self.config.base_url).hostname
        if shop.get("myshopifyDomain") != expected_host:
            raise ProviderError("shop_identity_mismatch")
        actual_currency = currency(shop.get("currencyCode"))
        if actual_currency != draft.currency:
            raise ProviderError("shop_currency_mismatch")
        if (
            not isinstance(shop.get("taxesIncluded"), bool)
            or shop["taxesIncluded"] != draft.price_includes_tax
        ):
            raise ProviderError("shop_tax_basis_mismatch")

    def _product(self, raw: Any, launch_id: str, sku: str) -> PreparedProduct:
        item = obj(raw)
        if obj(item.get("metafield")).get("value") != launch_id:
            raise ProviderError("product_ownership_conflict")
        if item.get("status") != "DRAFT":
            raise ProviderError("product_not_draft")
        variants = _nodes(item.get("variants"), 2)
        if len(variants) != 1:
            raise ProviderError("unsupported_multiple_variants")
        variant = obj(variants[0])
        actual_sku = variant.get("sku")
        if actual_sku not in (None, "", sku):
            raise ProviderError("product_ownership_conflict")
        handle = text(item.get("handle"), 255)
        if any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in handle):
            raise ProviderError("invalid_product_handle")
        return PreparedProduct(
            gid(item.get("id"), "Product"),
            gid(variant.get("id"), "ProductVariant"),
            sku if actual_sku == sku else "",
            hosted_url(self.config, self.config.base_url.rstrip("/") + "/products/" + handle),
            needs_update=actual_sku != sku,
        )

    def lookup_draft(self, launch_id: str, sku: str) -> DraftLookup:
        require_enabled(self.config)
        text(sku, 100)
        try:
            data = self._query(
                "query FindDraft($query: String!, $skuQuery: String!) { "
                "products(first: 2, query: $query) { nodes { "
                + _PRODUCT_FIELDS
                + " } pageInfo { hasNextPage } } "
                "skuMatches: productVariants(first: 2, query: $skuQuery) { "
                "nodes { sku product { id } } pageInfo { hasNextPage } } }",
                {
                    "query": "tag:" + owned_marker(launch_id),
                    "skuQuery": 'sku:"' + sku.replace("\\", "\\\\").replace('"', '\\"') + '"',
                },
            )
            products = _nodes(data.get("products"), 2)
            sku_matches = _nodes(data.get("skuMatches"), 2)
            expected_id = gid(obj(products[0]).get("id"), "Product") if len(products) == 1 else None
            for match in sku_matches:
                item = obj(match)
                if item.get("sku") == sku and (
                    expected_id is None
                    or gid(obj(item.get("product")).get("id"), "Product") != expected_id
                ):
                    raise ProviderError("product_ownership_conflict")
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
        self._shop(draft)
        result = self._query(
            "mutation CreateDraft($product: ProductCreateInput!) { "
            "productCreate(product: $product) { product { "
            + _PRODUCT_FIELDS
            + " } userErrors { field message } } }",
            {
                "product": {
                    "title": draft.title,
                    "descriptionHtml": draft.description_html,
                    "status": "DRAFT",
                    "tags": [owned_marker(draft.launch_id)],
                    "metafields": [
                        {
                            "namespace": "sm_trending_products",
                            "key": "launch_id",
                            "type": "single_line_text_field",
                            "value": draft.launch_id,
                        }
                    ],
                }
            },
            write=True,
        )
        try:
            created = obj(result.get("productCreate"))
            self._user_errors(created)
            product = self._product(created.get("product"), draft.launch_id, draft.sku)
        except ProviderError as exc:
            raise ProviderError(exc.code, ambiguous=True) from None
        # Persisted product ID is returned on any subsequent partial failure.
        try:
            return self._update_variant(product, draft)
        except ProviderError as exc:
            raise ProviderError(
                exc.code, retryable=exc.retryable, ambiguous=True, resource_id=product.product_id
            ) from None

    @staticmethod
    def _user_errors(payload: dict[str, Any]) -> None:
        if seq(payload.get("userErrors")):
            raise ProviderError("mutation_rejected")

    def update_draft(self, product_id: str, draft: DraftProduct) -> PreparedProduct:
        require_enabled(self.config)
        validate_draft(draft)
        product_id = gid(product_id, "Product")
        data = self._query(
            "query OwnedDraft($id: ID!) { product(id: $id) { " + _PRODUCT_FIELDS + " } }",
            {"id": product_id},
        )
        product = self._product(data.get("product"), draft.launch_id, draft.sku)
        if product.product_id != product_id:
            raise ProviderError("resource_identity_mismatch")
        self._shop(draft)
        try:
            data = self._query(
                "mutation UpdateDraft($product: ProductUpdateInput!) { "
                "productUpdate(product: $product) { product { "
                + _PRODUCT_FIELDS
                + " } userErrors { field message } } }",
                {
                    "product": {
                        "id": product_id,
                        "title": draft.title,
                        "descriptionHtml": draft.description_html,
                        "status": "DRAFT",
                    }
                },
                write=True,
            )
            updated = obj(data.get("productUpdate"))
            self._user_errors(updated)
            verified = self._product(updated.get("product"), draft.launch_id, draft.sku)
            if verified.product_id != product_id:
                raise ProviderError("resource_identity_mismatch")
            return self._update_variant(verified, draft)
        except ProviderError as exc:
            raise ProviderError(
                exc.code, retryable=exc.retryable, ambiguous=True, resource_id=product_id
            ) from None

    def _update_variant(self, product: PreparedProduct, draft: DraftProduct) -> PreparedProduct:
        data = self._query(
            "mutation UpdateDraftVariant($productId: ID!, "
            "$variants: [ProductVariantsBulkInput!]!) { "
            "productVariantsBulkUpdate(productId: $productId, variants: $variants) { "
            "productVariants { id price inventoryItem { sku } } userErrors { field message } } }",
            {
                "productId": product.product_id,
                "variants": [
                    {
                        "id": product.variant_id,
                        "price": draft.price,
                        "inventoryItem": {"sku": draft.sku},
                    }
                ],
            },
            write=True,
        )
        result = obj(data.get("productVariantsBulkUpdate"))
        self._user_errors(result)
        variants = seq(result.get("productVariants"), 1)
        if len(variants) != 1:
            raise ProviderError("resource_identity_mismatch", ambiguous=True)
        variant = obj(variants[0])
        if (
            gid(variant.get("id"), "ProductVariant") != product.variant_id
            or obj(variant.get("inventoryItem")).get("sku") != draft.sku
            or Decimal(amount(variant.get("price"))) != Decimal(draft.price)
        ):
            raise ProviderError("variant_update_mismatch", ambiguous=True)
        return PreparedProduct(
            product.product_id, product.variant_id, draft.sku, product.hosted_product_url
        )

    def list_order_ids(self, updated_after: str, cursor: str | None = None) -> OrderPage:
        timestamp(updated_after)
        if cursor is not None:
            text(cursor, 512)
        data = self._query(
            "query ChangedOrders($query: String!, $after: String) { "
            "orders(first: 50, sortKey: UPDATED_AT, query: $query, after: $after) { "
            "nodes { id } pageInfo { hasNextPage endCursor } } }",
            {"query": "updated_at:>='" + updated_after + "'", "after": cursor},
        )
        connection = obj(data.get("orders"))
        ids = tuple(gid(obj(item).get("id"), "Order") for item in seq(connection.get("nodes"), 50))
        page = obj(connection.get("pageInfo"))
        more = page.get("hasNextPage")
        if not isinstance(more, bool):
            raise ProviderError("invalid_pagination")
        return OrderPage(ids, text(page.get("endCursor"), 512) if more else None)

    def read_order(self, order_id: str) -> OrderSnapshot:
        order_id = gid(order_id, "Order")
        data = self._query(_ORDER_QUERY, {"id": order_id})
        order = obj(data.get("order"))
        if gid(order.get("id"), "Order") != order_id:
            raise ProviderError("resource_identity_mismatch")
        total, code = _money(order.get("totalPriceSet"))
        lines: list[OrderLine] = []
        for raw in _nodes(order.get("lineItems")):
            item = obj(raw)
            product, variant = item.get("product"), item.get("variant")
            lines.append(
                OrderLine(
                    gid(item.get("id"), "LineItem"),
                    None if product is None else gid(obj(product).get("id"), "Product"),
                    None if variant is None else gid(obj(variant).get("id"), "ProductVariant"),
                    optional_text(item.get("sku"), 100),
                    quantity(item.get("quantity")),
                )
            )
        payments: dict[str, PaymentSnapshot] = {}

        def ingest_transaction(raw: Any) -> PaymentSnapshot | None:
            item = obj(raw)
            transaction_id = gid(item.get("id"), "OrderTransaction")
            # Failed, pending and authorization-only transactions are not revenue.
            if item.get("status") != "SUCCESS" or item.get("kind") not in {
                "SALE",
                "CAPTURE",
                "REFUND",
            }:
                return None
            value, _ = _money(item.get("amountSet"), code)
            record = PaymentSnapshot(
                transaction_id, "refund" if item["kind"] == "REFUND" else "capture", value, code
            )
            if transaction_id in payments and payments[transaction_id] != record:
                raise ProviderError("transaction_identity_conflict")
            payments[transaction_id] = record
            return record

        for transaction in seq(order.get("transactions")):
            ingest_transaction(transaction)
        refunds: list[RefundSnapshot] = []
        for raw in seq(order.get("refunds")):
            refund = obj(raw)
            refund_total, _ = _money(refund.get("totalRefundedSet"), code)
            confirmed_amount = Decimal(0)
            for transaction in _nodes(refund.get("transactions")):
                record = ingest_transaction(transaction)
                if record is not None and record.kind == "refund":
                    confirmed_amount += Decimal(record.amount)
            refunds.append(
                RefundSnapshot(
                    gid(refund.get("id"), "Refund"),
                    refund_total,
                    code,
                    confirmed_amount > 0 and confirmed_amount == Decimal(refund_total),
                )
            )
        cancelled = order.get("cancelledAt")
        if cancelled is not None:
            timestamp(cancelled)
        return OrderSnapshot(
            self.config.connection_id,
            "shopify",
            order_id,
            timestamp(order.get("updatedAt")),
            code,
            total,
            cancelled is not None,
            text(order.get("displayFulfillmentStatus"), 50),
            tuple(lines),
            tuple(payments[key] for key in sorted(payments)),
            tuple(refunds),
            self.config.test_mode,
        )

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str]) -> WebhookHint:
        body, lowered = verified_body(self.config, raw_body, headers, "x-shopify-hmac-sha256")
        shop = lowered.get("x-shopify-shop-domain")
        if shop is not None and shop != urlsplit(self.config.base_url).hostname:
            raise ProviderError("webhook_shop_mismatch")
        version = lowered.get("x-shopify-api-version")
        if version is not None and version != self.config.api_version:
            raise ProviderError("webhook_api_version_mismatch")
        if body.get("order_id") is not None:
            from ._validation import numeric_id

            order_id = "gid://shopify/Order/" + numeric_id(body["order_id"])
        else:
            order_id = gid(body.get("admin_graphql_api_id"), "Order")
        return WebhookHint(
            order_id,
            header_hint(lowered, "x-shopify-webhook-id"),
            header_hint(lowered, "x-shopify-event-id"),
        )

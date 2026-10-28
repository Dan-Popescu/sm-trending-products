"""Platform-neutral, private commerce I/O records; no durable delivery guarantees.

An adapter performs one HTTP attempt. Its caller owns durable command identities,
serialization, reconciliation and business-event deduplication. A webhook is only
an object hint: call ``read_order`` before recording financial facts.
"""

from dataclasses import dataclass, field
from typing import Literal, Protocol

ProviderName = Literal["woocommerce", "shopify"]


class ProviderError(Exception):
    """Safe diagnostics, never a remote body, URL, credential or customer record."""

    def __init__(
        self,
        code: str,
        *,
        retryable: bool = False,
        ambiguous: bool = False,
        resource_id: str | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.ambiguous = ambiguous
        self.resource_id = resource_id


@dataclass(frozen=True)
class ConnectionConfig:
    provider: ProviderName
    connection_id: str
    base_url: str = field(repr=False)
    enabled: bool = False
    # Only explicit numeric loopback origins may use HTTP, solely for fixtures.
    test_mode: bool = False
    consumer_key: str = field(default="", repr=False)
    consumer_secret: str = field(default="", repr=False)
    access_token: str = field(default="", repr=False)
    webhook_secret: str = field(default="", repr=False)
    api_version: str = "2026-10"
    timeout_seconds: float = 10.0
    max_response_bytes: int = 1_048_576
    max_webhook_bytes: int = 262_144


@dataclass(frozen=True)
class ProviderCapabilities:
    single_sku_drafts: bool = True
    multiple_variants: bool = False
    publishes_live_products: bool = False
    checkout_owner: str = "platform"
    transactional_email_owner: str = "platform"
    creates_refunds: bool = False
    creates_supplier_orders: bool = False


@dataclass(frozen=True)
class DraftProduct:
    launch_id: str
    sku: str
    title: str
    # Authored/escaped content only; caller must not pass copied untrusted HTML.
    description_html: str
    price: str
    currency: str
    # Must be declared, and match the actual platform's catalog tax setting.
    price_includes_tax: bool | None = None
    variant_count: int = 1


@dataclass(frozen=True)
class PreparedProduct:
    product_id: str
    variant_id: str | None
    sku: str
    hosted_product_url: str | None
    status: Literal["draft"] = "draft"
    # True after a partially completed create, e.g. initial Shopify variant.
    needs_update: bool = False


@dataclass(frozen=True)
class DraftLookup:
    state: Literal["found", "absent", "unknown"]
    product: PreparedProduct | None = None


@dataclass(frozen=True)
class OrderLine:
    line_id: str
    product_id: str | None
    variant_id: str | None
    sku: str | None
    quantity: int


@dataclass(frozen=True)
class PaymentSnapshot:
    transaction_id: str
    kind: Literal["capture", "refund"]
    amount: str
    currency: str


@dataclass(frozen=True)
class RefundSnapshot:
    refund_id: str
    amount: str
    currency: str
    payment_confirmed: bool


@dataclass(frozen=True)
class OrderSnapshot:
    connection_id: str
    provider: ProviderName
    order_id: str
    updated_at: str
    currency: str
    total: str
    cancelled: bool
    fulfillment_status: str
    lines: tuple[OrderLine, ...]
    payments: tuple[PaymentSnapshot, ...]
    refunds: tuple[RefundSnapshot, ...]
    # Configured fixture connections cannot masquerade as live observations.
    synthetic: bool
    # Platform paid-state evidence, not a captured amount or bank settlement.
    paid_at: str | None = None


@dataclass(frozen=True)
class OrderPage:
    order_ids: tuple[str, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class WebhookHint:
    order_id: str
    # These headers are NOT covered by body HMAC; bookkeeping hints only.
    delivery_id: str | None = None
    event_id: str | None = None


class CommerceProvider(Protocol):
    config: ConnectionConfig
    capabilities: ProviderCapabilities

    def lookup_draft(self, launch_id: str, sku: str) -> DraftLookup: ...

    def prepare_draft(self, draft: DraftProduct) -> PreparedProduct: ...

    def update_draft(self, product_id: str, draft: DraftProduct) -> PreparedProduct: ...

    def read_order(self, order_id: str) -> OrderSnapshot: ...

    def list_order_ids(self, updated_after: str, cursor: str | None = None) -> OrderPage: ...

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str]) -> WebhookHint: ...

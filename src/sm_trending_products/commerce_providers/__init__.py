"""Disabled-by-default hosted commerce adapters, separate from acquisition.

``prepare_draft`` is for a fresh serialized durable command. On an ambiguous
exception, persist its optional ``resource_id`` and reconcile; never blindly
repeat a create because lookup reports absence. A found Shopify product with
``needs_update=True`` is a partial draft, not a completed prepare operation.

No method activates a platform, publishes a product, creates a refund, handles
cards, sends email or purchases supplier stock. Hosted URLs are product paths;
draft products may not be public. They are not private customer checkout tokens.
"""

from .contracts import (
    CommerceProvider,
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
from .shopify import ShopifyProvider
from .woocommerce import WooCommerceProvider

__all__ = [
    "CommerceProvider",
    "ConnectionConfig",
    "DraftLookup",
    "DraftProduct",
    "OrderLine",
    "OrderPage",
    "OrderSnapshot",
    "PaymentSnapshot",
    "PreparedProduct",
    "ProviderCapabilities",
    "ProviderError",
    "RefundSnapshot",
    "ShopifyProvider",
    "WebhookHint",
    "WooCommerceProvider",
    "create_provider",
]


def create_provider(config: ConnectionConfig) -> CommerceProvider:
    if config.provider == "woocommerce":
        return WooCommerceProvider(config)
    if config.provider == "shopify":
        return ShopifyProvider(config)
    raise ProviderError("unsupported_provider")

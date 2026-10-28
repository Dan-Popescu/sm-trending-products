# Hosted commerce provider API

`create_provider(ConnectionConfig(...))` returns `CommerceProvider`.
`WooCommerceProvider(config)` and `ShopifyProvider(config)` are also exported.
Connections default to `enabled=False`; dossier creation does not enable them.
Live configurations require HTTPS, credentials and a webhook secret. Shopify uses
the configured `*.myshopify.com` origin and Admin GraphQL **2026-10**. Only explicit
`test_mode=True` numeric loopback HTTP origins are supported for local fixtures;
their snapshots are always marked synthetic.

`DraftProduct(launch_id, sku, title, description_html, price, currency,
price_includes_tax=...)` describes one selected SKU. Prices are exact decimal
strings. The tax basis must be declared explicitly; currency and catalog tax
basis must match the actual shop before a price write. WooCommerce checks
`settings/general/woocommerce_currency` and
`settings/tax/woocommerce_prices_include_tax`; Shopify reads `shop.currencyCode`,
`shop.taxesIncluded` and `shop.myshopifyDomain`. These checks do not validate tax
rates, country-specific checkout, exemptions or legal configuration. No VAT or
currency conversion is performed. Pass authored/escaped HTML, not copied source
content. Multiple variants are rejected explicitly.

- `prepare_draft(draft)` creates or updates an owned **draft**. It never publishes.
- `update_draft(product_id, draft)` verifies ownership and draft state first.
- `lookup_draft(launch_id, sku)` returns `found`, `absent` or `unknown`.
- `read_order(order_id)` returns a strict snapshot without customer contact data.
- `list_order_ids(updated_after, cursor=None)` returns a bounded `OrderPage`.
- `verify_webhook(raw_body, headers)` verifies raw-body HMAC and returns only an
  order hint. Fetch the current order from this configured connection before
  recording money or fulfillment. Topic, shop and delivery headers are unsigned.

The caller owns a durable outbox, serialized commands, inbox and transaction
deduplication. There are no automatic HTTP retries. Persist ambiguous failures and
their optional `ProviderError.resource_id`; reconcile before proceeding. Search
absence does not prove a timed-out create never happened. Shopify may create its
initial variant before its price/SKU update completes; a found draft with
`needs_update=True` must be resumed through `update_draft`. Do not treat lookup as
completed price verification. Published or foreign-owned listings are rejected.

`OrderSnapshot.payments` contains confirmed money records, deduplicated by
transaction ID for Shopify successful SALE/CAPTURE/REFUND transactions. A refund
record alone is not confirmed cash. Partial successful refunds do not confirm the
entire refund record. WooCommerce core `paid_at` records platform paid-state;
its mutable order total is **not** converted into a capture. Missing captured
amounts require a future selected gateway import. WooCommerce gateway-confirmed
refunds are separate records. Processor settlement, fees, advertising, supplier
costs and profit are not inferred. Read totals are shop-currency order totals,
distinct from cash and net profit.

TLS certificates are verified, public DNS addresses are pinned per connection,
redirects are refused and bodies/socket I/O are bounded. System DNS resolution
also depends on host resolver timeouts. Rate-limit and server errors use sanitized
codes; remote bodies, secrets and customer details are not exception messages.
Orders beyond bounded nested-page limits are rejected rather than truncated.
Shopify default order access covers 60 days; older reconciliation needs approved
`read_all_orders` access. Page scans need overlapping windows and durable root
deduplication because remote collections can change while scanning.

Hosted platforms own checkout and transactional email. Returned URLs are product
paths, not private checkout tokens; a draft URL is not proof of public availability.
No method charges a customer, creates a refund, buys stock, creates a shipment or
sends mail. Source contracts are documented in
`docs/operations/commerce-provider-contracts.md` (research dated 2026-10-28).

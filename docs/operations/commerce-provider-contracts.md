# Commerce provider contracts

Research date: **2026-10-28**. Sources below are first-party provider documentation,
accessed on that date. This records implementation contracts, not live acceptance.
The owner selected platform-neutral preparation, Europe, dropshipping first and
the option of durable supplier/brand relationships later. No store, payment
gateway, supplier or paid service is activated by this research.

## Recommended first adapter scope

Implement two configurable adapters behind the same small interface: prepare a
draft listing, read a known listing/order, verify an inbound delivery and report
capabilities. Keep publication, charging, refunding, fulfillment submission and
customer notifications disabled until the corresponding integration is explicitly
configured. A provider store remains responsible for its checkout and configured
transactional emails. A successful local HTTP fixture proves wire-contract
behavior only; it does not prove granted scopes, installed plugins, gateway
configuration or deliverability.

Use immutable local launch/revision identifiers and persist provider product,
variant, order and transaction identifiers scoped to a configured connection.
Never treat a title, handle, SKU or webhook delivery ID as a universal transaction
identifier. An uncertain create outcome must reconcile before retrying. These are
application design recommendations, not provider idempotency guarantees.

## WooCommerce: current WordPress REST API

Use **`/wp-json/wc/v3`**. This is the current WordPress REST integration. The old
`/wc-api/v3` and the historical `v3.html` documentation describe the legacy API
and must not be used for new payload/header assumptions.
Sources: [current REST API](https://developer.woocommerce.com/docs/apis/rest-api/),
[versioned reference](https://woocommerce.github.io/woocommerce-rest-api-docs/).

REST consumer keys belong to a WordPress user and inherit that user's
capabilities. Keys can be read-only, write-only or read/write. Use a separate
read-only connection for ingestion where practical; draft creation requires write
permission. Use HTTPS Basic authentication with consumer key/secret in the
authorization header; never put credentials into query parameters or logged URLs.
Source: [authentication](https://developer.woocommerce.com/docs/apis/rest-api/authentication/).

For a first **simple product**, POST `products` with explicit `status: "draft"`
(the provider default is `publish`), `type: "simple"`, name, controlled HTML
description, SKU and decimal-string `regular_price`. Persist returned integer
`id` and `permalink`; update known products with PUT `products/{id}`. Prices do
not carry an independent per-product currency. List by exact `sku` or a persisted
ID for reconciliation, and reject ambiguous/mismatched results instead of
adopting another merchant listing. Do not infer stock from successful creation:
stock management and availability need their own explicit settings.
Source: [products](https://developer.woocommerce.com/docs/apis/rest-api/v3/products/).

Variable products use separate variation identifiers and
`products/{product_id}/variations`; do not represent several sellable variants as
one simple-product price. Source:
[product variations](https://developer.woocommerce.com/docs/apis/rest-api/v3/product-variations/).

Order reads are GET `orders/{id}` and bounded pages of `orders`. Preserve order
currency, decimal-string totals, line-item/product/variation identifiers, status,
`date_modified_gmt` and paid timestamps. The current reference supports
`modified_after`, `modified_before` and GMT interpretation for reconciliation.
An order state alone does not disclose all advertising, supplier or processor
costs; absent costs remain unknown in this application.
Source: [orders](https://developer.woocommerce.com/docs/apis/rest-api/v3/orders/).

Refund reads use `orders/{order_id}/refunds`. `api_refund` can actually invoke a
payment gateway and defaults to true on refund creation, so ingestion must never
POST that endpoint as a way to record an observed refund. Retain the provider's
refund ID and `refunded_payment` evidence rather than assuming every status change
proves an external money movement.
Source: [order refunds](https://developer.woocommerce.com/docs/apis/rest-api/v3/order-refunds/).

WooCommerce's current fulfillment endpoints are
`orders/{order_id}/fulfillments`. They exist only when
`woocommerce_feature_fulfillments_enabled` is enabled; otherwise the documented
response is `404 rest_no_route`. They contain shipment metadata and line-item
quantities, but do not place an order with a dropshipping supplier. Treat them as
an optional verified capability. `notify_customer` is an explicit write parameter,
not a default instruction to send another email.
Source: [order fulfillments](https://developer.woocommerce.com/docs/apis/rest-api/v3/order-fulfillments/).

### WooCommerce delivery verification and rate behavior

Require a configured webhook secret. Verify base64 HMAC-SHA256 of the **raw body**
against `X-WC-Webhook-Signature` before parsing. Current metadata includes
`X-WC-Webhook-ID`, **`X-WC-Webhook-Delivery-ID`**, source and topic. Do not copy the
legacy `X-WC-Delivery-ID` spelling from old documentation. Core order topics are
`order.created`, `order.updated`, `order.deleted`; refunds/shipments may require
order reconciliation or explicitly verified extension topics. Woo disables a
webhook after five consecutive failed deliveries, so monitor subscription health
and retain an API reconciliation path.
Source: [current webhooks](https://developer.woocommerce.com/docs/apis/rest-api/v3/webhooks/).

No general idempotency-key guarantee for arbitrary REST product creation was
established by the reviewed current reference. Persist intent locally, serialize
per launch/connection, reconcile uncertain POSTs and respect actual HTTP errors
and host/plugin limits. Do not apply the Store API's optional checkout limits to
the authenticated Admin REST API; they are different surfaces.
Source: [Store API rate limiting](https://developer.woocommerce.com/docs/apis/store-api/rate-limiting/).

## Shopify: versioned GraphQL Admin API

Pin **`2026-10`**, the latest stable API on the research date; `2027-01` is a release
candidate. Use HTTPS POST to
`https://{configured-shop}.myshopify.com/admin/api/2026-10/graphql.json` with
`X-Shopify-Access-Token`. Reject/flag an unexpected `X-Shopify-API-Version`
response rather than silently treating a fallen-forward schema as tested.
Sources: [API versioning](https://shopify.dev/docs/api/usage/versioning),
[Admin API reference](https://shopify.dev/docs/api/admin-graphql/2026-10).

Start with `write_products` for draft writes (it includes product reads) and
`read_orders` for financial ingestion. Order access normally covers only the last
60 days; older reads need approved `read_all_orders` alongside an order scope.
Customer-data fields can have additional access requirements. Tokens must come
from an actual installed/authorized app; a placeholder is not live access.
Sources: [scope management](https://shopify.dev/docs/apps/build/authentication-authorization/manage-access-scopes),
[authentication](https://shopify.dev/docs/apps/build/authentication-authorization).

Create drafts using `productCreate(product: ProductCreateInput)` with explicit
`status: DRAFT`. Request and persist the product GID and initial variant GID.
Creation supports the initial variant; multiple variants require a separate
variant operation. Updating title/description/status uses `productUpdate` with a
known product GID, not a new create. Always inspect `userErrors`, not only HTTP
status. Sources:
[productCreate](https://shopify.dev/docs/api/admin-graphql/2026-10/mutations/productCreate),
[productUpdate](https://shopify.dev/docs/api/admin-graphql/2026-10/mutations/productUpdate).

Price updates belong to `productVariantsBulkUpdate` using the known product and
variant GIDs. Its input uses decimal-string `price` and nested
`inventoryItem: { sku: ... }`; product updates do not update variant prices.
Query the store's `shop.currencyCode` before interpreting base prices. Market
presentment currency and taxes must not be inferred from the word “Europe.”
Sources: [variant mutation](https://shopify.dev/docs/api/admin-graphql/2026-10/mutations/productVariantsBulkUpdate),
[variant input](https://shopify.dev/docs/api/admin-graphql/2026-10/input-objects/ProductVariantsBulkInput),
[inventory item input](https://shopify.dev/docs/api/admin-graphql/2026-10/input-objects/InventoryItemInput),
[shop settings](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/Shop).

`productSet` can create/update from an external identifier, but list fields
including variants/metafields/collections reconcile by replacing omitted members.
It is therefore unsuitable as a casual partial patch. Initially prefer narrow
create/update operations and retained remote IDs. Do not assume an arbitrary
`Idempotency-Key` header makes product creation safe: Shopify documents
idempotency per supported mutation/directive.
Sources: [productSet](https://shopify.dev/docs/api/admin-graphql/2026-10/mutations/productSet),
[idempotent requests](https://shopify.dev/docs/api/usage/idempotent-requests),
[client implementation](https://shopify.dev/docs/apps/build/apis/graphql-admin/implementing-idempotency).

### Shopify ingestion and fulfillment

Read orders by immutable GID and bounded cursor pages, selecting only needed
amounts/currencies, item identifiers, financial/fulfillment states and timestamps.
Relevant subscriptions include order updates/paid states, refunds and
fulfillments, subject to granted scopes. Critically, `refunds/create` means a refund
record was created **independently of money movement**. A financial refund must be
supported by successful refund transaction evidence/reconciliation; do not add
cash refunds merely by counting that topic.
Sources: [Order](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/Order),
[webhook topics](https://shopify.dev/docs/api/webhooks/2026-10).

Shipping writes use `fulfillmentCreate` over fulfillment-order IDs/line items and
appropriate assigned/merchant-managed/third-party fulfillment scopes. It is not
equivalent to changing an order status, and does not itself buy goods from a
supplier. Notification preferences must be deliberate.
Source: [fulfillmentCreate](https://shopify.dev/docs/api/admin-graphql/2026-10/mutations/fulfillmentCreate).

### Shopify delivery verification and rate behavior

Verify `X-Shopify-Hmac-SHA256` using the app client secret and raw request bytes.
Deduplicate delivery IDs using `X-Shopify-Webhook-Id`; use `X-Shopify-Event-Id` to
correlate subscriptions generated by one merchant action. Persist receipt before
acknowledgement and process later: HTTPS has a one-second connect and five-second
total timeout. Shopify documents eight retries over four hours and may delete
API-created subscriptions after repeated failure.
Source: [verify deliveries](https://shopify.dev/docs/apps/build/webhooks/verify-deliveries).

Webhook ordering and delivery are not guaranteed. Keep immutable receipts and
perform periodic bounded API reconciliation using update cutoffs; never assume
that receipt order equals business-state order.
Source: [webhook behavior](https://shopify.dev/docs/apps/build/webhooks).

Admin GraphQL throttling is calculated cost per app/store, not a universal
request count. Consume `extensions.cost.throttleStatus` and inspect top-level
GraphQL `errors` even on HTTP 200; `THROTTLED` is not success. Limit requested
fields, paged work and retry attempts. Current documented plan rates range from
100 to 2,000 cost points/second, a single query is limited to 1,000 points and
array inputs to 250 elements. Do not hardcode those rates as granted capacity.
Source: [GraphQL rate limits](https://shopify.dev/docs/apps/build/apis/graphql-admin/rate-limits).

The reviewed generic and mutation pages disagree about high-volume variant
threshold numbers. Do not build a capacity claim from either figure. Actual
store limits, response metadata and the exact selected mutation govern admission;
confirm them during live acceptance before large catalog imports.

## Hosted checkout, emails and external prerequisites

Shopify's Storefront cart supplies a `checkoutUrl` that sends the buyer to its
web checkout. Admin product creation alone does not create a configured buyer
checkout. WooCommerce's existing storefront and configured gateway similarly own
the buyer payment flow; the administrative integration must not receive card
details or invent a paid order from a browser redirect.
Source: [Shopify Cart](https://shopify.dev/docs/api/storefront/2026-10/objects/Cart).

Both platforms already have configurable customer transaction notifications.
Choose one notification owner per event so this pipeline does not send duplicate
confirmation/shipping/refund emails. Configured notifications are not proof of
delivery; Woo mail delivery still depends on the installed mail configuration.
Sources: [Shopify notifications](https://help.shopify.com/en/manual/fulfillment/setup/notifications),
[Woo email settings](https://woocommerce.com/document/configuring-woocommerce-settings/emails/).

A generic `test=true` request flag is not a universal provider sandbox. Testing
payments requires a deliberately configured development/test store and gateway.
Local synthetic fixtures must stay separate from live store results. Activation
also needs a selected store/domain, credentials, countries/currencies, supplier
arrangements and merchant-reviewed tax/shipping/returns configuration. This note
does not determine European tax or consumer-law obligations.

## Application safeguards inferred from these contracts

- HMAC authenticates body bytes, not arbitrary metadata headers. Bind requests
  to a configured connection/secret and validate resource shape and merchant
  ownership. Reconcile a known remote object before consequential processing when
  needed; do not choose a financial action solely from a topic header.
- Deduplicate both delivery receipt and business object/version/transaction. A
  changed delivery ID must not create a second revenue or fulfillment entry.
- Acknowledge only after durable receipt; use bounded workers and an outbox for
  side effects. Preserve uncertain external outcomes instead of blind retries.
- Require HTTPS and configured host allowlists. Never follow redirects carrying
  credentials; reject userinfo, query credentials and unexpected/private origins
  except an explicit local-fixture path. Provider callbacks must not select a new
  outbound host from their payload.
- Keep customer contact/address data in the minimum private operational context.
  Trend/financial observations need identifiers, amounts and country/currency,
  not complete customer records. Preserve unknown cost/settlement fields rather
  than reporting them as zero or realized profit.

These safeguards are application recommendations derived from the documented
wire contracts. They require behavioral verification and do not claim universal
provider exactly-once execution, legal acceptance or live merchant authorization.

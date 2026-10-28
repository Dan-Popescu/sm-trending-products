# Inactive hosted-commerce connections

No platform has been selected. WooCommerce and Shopify adapters are available,
disabled by default. Hosted platforms own the storefront, checkout and native
transactional notifications. This slice prepares private single-SKU drafts; it
does not activate a store, publish a product, charge money, send mail, create a
refund or purchase supplier stock.

## Private configuration

```sh
sm-trends commerce connection-template --provider woocommerce > /private/store.json
# Or choose --provider shopify to prepare that provider's inactive template.
sm-trends commerce connection-inspect --connection /private/store.json
```

Inspection validates locally without DNS, HTTP or PostgreSQL. The file contains
environment-variable **names**, never token/password values. Supply secrets
through the selected private runtime environment. Default names are
`COMMERCE_CONSUMER_KEY`, `COMMERCE_CONSUMER_SECRET`, `COMMERCE_ACCESS_TOKEN` and
`COMMERCE_WEBHOOK_SECRET`. Rotation does not change store identity. Do not commit
private configuration or secret values.

Real configurations require verified HTTPS, and Shopify requires the selected
`myshopify.com` origin. Shopify API version is pinned to `2026-10` and checked in
responses. Synthetic HTTP providers must use numeric loopback addresses and
explicit `test_mode: true`; they cannot serve real dossiers. Connections remain
disabled until their configuration explicitly sets `enabled: true` and supplies
credentials. Merely creating a template or dossier changes no store.

## Durable draft preparation

Initialize the existing launch module first and save a ready dossier. Then:

```sh
sm-trends commerce delivery-init
sm-trends commerce prepare-draft --connection /private/store.json --id PRODUCT-FR --expected-revision 1 --request-id PREPARE-1
# The preceding command is a read-only preview. Execute a chosen configured draft:
sm-trends commerce prepare-draft --connection /private/store.json --id PRODUCT-FR --expected-revision 1 --request-id PREPARE-1 --apply
sm-trends commerce draft-operation --operation-id OPERATION
sm-trends commerce draft-operations --connection-id STORE --id PRODUCT-FR --limit 20
sm-trends commerce recover-draft --connection /private/store.json --operation-id OPERATION --apply
```

Add `--synthetic` only for a simulated dossier and loopback provider. The private
Launch preparation view exposes the same explicit actions. Reads and previews
never initialize tables or contact a store. Monetary writes require the dossier's
currency **and** tax basis to match actual shop settings; no VAT or currency
conversion is inferred. Product copy is escaped authored content. Multi-variant
products and live publication are explicitly unsupported by these draft adapters.

A durable operation is recorded before HTTP. One worker at a time controls a
store/dossier, with at most two active delivery sessions per schema. Remote calls
hold no SQL transaction. Request-ID replay reads the existing operation; it never
blindly creates again. A lost response or interrupted worker leaves the operation
uncertain. Recovery may resume an identified owned draft, but a search returning
no product does not prove the original request failed. Unresolved uncertainty
blocks newer revisions and may require investigation in the selected platform.
The UI keeps request/revision intent in a bounded non-widget cache for 100 recent
targets, including while another dossier is displayed. Persisted operation search
also supplies recovery IDs after the browser session has been lost.
Commands return exit code 1 for unresolved/failed execution and 2 for rejected
inputs; normal inspection is successful even when it describes uncertainty.

Provider order records distinguish mutable order totals, a paid timestamp and
confirmed financial transactions. WooCommerce's paid timestamp alone does not
establish the captured amount. Refund records do not automatically prove money
was returned. Reconciliation and complete actual-cost review are separate steps.

See the [documented provider contracts](commerce-provider-contracts.md),
[adapter API](../../src/sm_trending_products/commerce_providers/README.md) and
[delivery state machine](../../src/sm_trending_products/commerce_delivery/README.md).
Controlled HTTP tests prove software contracts; real API permissions, platform
availability, selected currency/taxes, supplier terms and customer acceptance
still need the chosen deployment and credentials.

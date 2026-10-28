# Commercial-learning delivery map

Approved specification: [product launch and commercial learning](../specs/commerce-learning.md).
Canonical parent: [#124](https://github.com/Dan-Popescu/sm-trending-products/issues/124).
Platform connections stay inactive; no platform has been selected. Initial target:
individual European countries, dropshipping, with later stock/supplier/brand support.

| Slice | Canonical issue | Engineering blockers |
| --- | --- | --- |
| C01: Versioned launch dossiers and readiness | [#125](https://github.com/Dan-Popescu/sm-trending-products/issues/125) | None |
| C02: Configurable WooCommerce/Shopify drafts | [#126](https://github.com/Dan-Popescu/sm-trending-products/issues/126) | None for adapters; C01 for dossier integration |
| C03: Reliable events and measured commercial feedback | [#127](https://github.com/Dan-Popescu/sm-trending-products/issues/127) | C01, C02 |
| C04: Multi-day trends and hourly launch-delay evaluation | [#128](https://github.com/Dan-Popescu/sm-trending-products/issues/128) | None |
| C05: Multilingual identity suggestions requiring review | [#129](https://github.com/Dan-Popescu/sm-trending-products/issues/129) | None |
| C06: Budgeted reusable transcript insights | [#130](https://github.com/Dan-Popescu/sm-trending-products/issues/130) | None |
| C07: Complete synthetic rehearsal and operations | [#131](https://github.com/Dan-Popescu/sm-trending-products/issues/131) | C01–C06 |

GitHub records authoritative status, pull requests and validation evidence. A
completed synthetic rehearsal does not activate a real store, establish source
access or prove commercial value. Actual activation needs explicit country,
currency, supplier facts, store configuration and credentials.

## Operator entry points

The optional modules remain independent from collection. Initialize only the
modules being used; a missing store, supplier or analysis provider does not stop
capture. The base capture SQL version and protocol are unchanged.

- [Launch preparation](../operations/launch-preparation.md): country-specific
  supplier facts, explicit missing costs, scenario margins and versioned decisions.
- [Store connections](../operations/store-connections.md): inactive WooCommerce or
  Shopify configuration, owned draft creation and uncertain-operation recovery.
- [Commercial workspace](../operations/commerce-workspace.md) and
  [commands](../operations/commerce-commands.md): authoritative order reads,
  manual fulfillment intents and reviewed actual costs.
- [Trend insights](../operations/trend-insights.md),
  [product matching](../operations/product-matching.md) and
  [transcript insights](../operations/transcript-insights.md): bounded optional
  reports and suggestions, preserving historical decisions and source controls.
- [Synthetic rehearsal](../operations/commerce-rehearsal.md): a complete local
  walkthrough and separate verification of persisted evidence after restore.

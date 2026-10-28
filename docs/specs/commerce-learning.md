# Product launch and commercial learning

Status: approved by the owner on 2026-10-28. Platform connections are prepared
without choosing or activating a platform. Start with European markets and
dropshipping; retain a path to stocked products, suppliers and durable brands.

Canonical parent: [#124](https://github.com/Dan-Popescu/sm-trending-products/issues/124).
Delivery: [C01–C07](../planning/commerce-learning.md).

## Problem Statement

The current application identifies and evaluates promising attention signals but
does not prepare a sale, operate a storefront, reconcile orders or learn from its
own commercial results. A promising product must pass commercial checks before
being treated as a viable launch. Large-scale enrichment must earn its recurring
cost rather than running indiscriminately on every video.

## Solution

Provide a private launch workspace linked to canonical products, configurable
hosted-commerce connections, reliable order and cost feedback, and better bounded
trend and content insights. Exercise the complete path with explicitly synthetic
local providers. Real activation remains unconfigured until the operator supplies
the selected store, country, supplier, terms and credentials.

## User Stories

1. As an operator, I want a launch dossier for a canonical product and country, so
   that popularity, practical feasibility and an actual commercial test stay distinct.
2. As an operator, I want quoted costs and known missing fields, so that an unknown
   tax, shipping cost or advertising cost is never silently counted as zero.
3. As an operator, I want margins and a maximum affordable acquisition cost, so
   that I can reject an economically implausible launch before spending money.
4. As an operator, I want versioned supplier, currency, stock and delivery facts,
   so that later revisions do not rewrite an earlier launch decision.
5. As an operator, I want an explicit country per launch, so that Europe is not
   treated as one tax, currency or delivery regime.
6. As an operator, I want dropshipping, owned stock and external fulfillment modes,
   so that a durable brand does not require replacing the commercial model.
7. As an operator, I want reusable product-page drafts with escaped content, so
   that each product does not require a new site implementation.
8. As an operator, I want WooCommerce and Shopify connections disabled by default,
   so that platform selection does not block independent engineering.
9. As an operator, I want stable publication identities and uncertain-operation
   recovery, so that a retry cannot blindly create a second listing or shipment.
10. As an operator, I want hosted checkout and native platform notifications, so
    that card handling and transactional mail are delegated to established systems.
11. As an operator, I want authenticated bounded event intake and reconciliation,
    so that duplicate, delayed or reordered callbacks cannot corrupt sales totals.
12. As an operator, I want paid, cancelled, refunded and fulfilled orders kept
    distinct, so that a refund record is not mistaken for an actual money movement.
13. As an operator, I want supplier handoff and tracking records, so that preparation
    can be verified before a particular supplier integration is selected.
14. As an operator, I want revenue and every declared actual cost by product and
    period, so that incomplete financial evidence remains explicitly incomplete.
15. As an operator, I want commercial outcomes connected to the original launch
    decision and policy, so that actual results can improve later selection.
16. As an operator, I want persistent growth compared over multiple durations,
    so that a short viral interval does not substitute for a multi-day trend.
17. As an operator, I want comparable category/market context and confidence,
    so that sparse or incomparable evidence does not receive false precision.
18. As an operator, I want evaluation delays measured in hours where useful,
    so that a 12/24/48-hour launch can be studied without rewriting old snapshots.
19. As an operator, I want bounded multilingual product-link suggestions and
    explicit conflicts, so that variants and unrelated products remain reviewable.
20. As an operator, I want repeated creatives to remain distinguishable from
    independent adoption, so that copied content cannot inflate confidence.
21. As an operator, I want reusable versioned transcript insights for selected
    videos, so that metrics refreshes do not trigger repeated transcription.
22. As an operator, I want enrichment selection and spending bounded explicitly,
    so that expensive analysis is restricted to a measured useful subset.
23. As an operator, I want the system to record what was tested, skipped and why,
    so that selection bias and unmeasured outcomes remain visible.
24. As an operator, I want all new work isolated from capture, so that storefront
    failures, processing updates or missing commercial credentials cannot stop it.

## Implementation Decisions

- Keep capture protocols, source quotas, evidence retirement and base SQL schema
  compatibility unchanged. Independent optional application modules own additive
  tables and checksummed migrations, initialized explicitly through a small shared
  module initializer; no capture table or migration is edited for commerce.
- Expose launch and commerce behavior through deep modules with typed records and
  a small operator interface. Private CLI and review views cross the same interface
  as scenario tests. Keep provider HTTP details and effect recovery internal.
- Dossiers use immutable revisions, optimistic expected-revision checks, bounded
  pages, real/synthetic isolation and explicit missing-data readiness reasons.
  A revision linked to an assessment never grants permission to publish raw source
  evidence or assume unavailable sales/rights/market data.
- Currency amounts are exact decimal strings. Monetary completeness, freshness,
  availability time, country and cost scenarios remain explicit. Actual profit
  requires a reviewed complete cost contract; scenario margins remain scenarios.
- Hosted commerce owns storefront checkout and transactional notifications.
  Provider adapters are configurable but inactive by default. All credentials
  stay in private runtime configuration; no production provider is contacted by
  tests or enabled merely by creating a dossier.
- Use durable operation identities and a transactional outbox. Verify provider
  ownership before adoption. Uncertain HTTP outcomes remain uncertain unless an
  authoritative lookup resolves them; unsupported provider idempotency is not
  fabricated. Do not hold database transactions during remote calls.
- Verify webhook signatures over raw bounded bytes, bind the configured shop,
  deduplicate deliveries and business revisions, and reconcile current order state.
  Do not trust unsigned topic metadata as financial authority. Keep customer
  personal data out of analytical observations and general CLI/log output.
- Supplier handoff and shipment records support manual/controlled adapters until a
  real supplier is selected. Platform-native email requests are distinct from
  confirmed delivery; do not claim a notification was received from a 2xx response.
- Reuse existing point-in-time evidence, counter semantics and frozen evaluation
  contracts. New trend insights and suggestions do not change historical scoring
  hashes or silently turn candidate links into confirmed ownership.
- Prefer reusable transcript imports and lightweight text analysis. Optional
  external enrichment must have an explicit provider, per-run/day budgets,
  deduplication and versioned outputs. No paid call is the default.
- Publish and merge focused slices autonomously after independent Standards/Spec
  reviews and successful CI, under the existing owner authorization.

## Testing Decisions

Use native PostgreSQL, controlled HTTP providers, deterministic clocks and the
existing pipeline fixtures. Verify behavior across the operator interface:
dossier creation and revision, readiness rejection, escaped draft page, hosted
product publication, authenticated duplicate/reordered events, financial
completeness, shipment handoff, transcript reuse, bounded selection and old
snapshot compatibility. Exercise rollback, competing workers, response-loss
uncertainty, namespace mismatches and stale revisions. Existing capture/rebuild,
hot CPU and backup/restore checks must stay green. Synthetic commerce events
must never be presented as real business performance.

## Out of Scope

Purchasing services or inventory, charging a real customer, enabling an unchosen
platform, sending real messages, spending on ads, deploying to an unspecified
host, guaranteeing product profitability, replacing a payment processor, or
assuming source permissions. Real legal/tax settings and supplier contracts are
operator-supplied facts, not inferred from a continent or generated by a model.

## Further Notes

Deliver in this order: launch dossier; hosted-commerce draft integration; order,
cost and fulfillment feedback; richer trend/evaluation insight; safer identity
suggestions; budgeted transcript insights; complete synthetic walkthrough and
operating guide. Independent later slices may be implemented in parallel while
earlier ones are reviewed. Real activation is a separate configuration-dependent
acceptance step, and does not block these engineering slices.

# Product requirements: emerging physical-product opportunities

Status: approved for autonomous implementation on 2026-10-22. Real source acceptance remains credential- and permission-dependent.

Tracking: [global PRD #2](https://github.com/Dan-Popescu/sm-trending-products/issues/2), [implementation PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

## Problem Statement

An operator needs to find physical products gaining traction early enough to assess and launch them. Popular-product lists conflate attention with purchasing, omit sampling limitations, and cannot establish whether their signals would have been actionable earlier. Collecting large volumes is useful only if evidence can be linked to products, interpreted honestly, and evaluated without future information.

## Solution

Build a continuously operating evidence pipeline that collects accessible TikTok data, tracks changes, groups evidence by product, and ranks opportunities with separate attention, demand, competition, and commercial-feasibility assessments. Make every assessment inspectable. Validate ranking policies historically when point-in-time history exists, then prospectively. Start on one VPS with S3 history and retain the ability to distribute workers later.

## User Stories

1. As an operator, I want to see which source capabilities are actually granted, so that I know what the system can observe.
2. As an operator, I want collection to work without credentials using clearly marked fixtures, so that engineering can proceed before account approval.
3. As an operator, I want real and synthetic records separated, so that simulated evidence cannot produce live opportunities.
4. As an operator, I want source observations preserved, so that unexpected assessments can be audited and recomputed.
5. As an operator, I want repeated counters and their timestamps, so that growth can be estimated across actual observation intervals.
6. As an operator, I want source delays and missing fields displayed, so that apparent quiet periods are not mistaken for zero activity.
7. As an operator, I want collection prioritized within quotas, so that promising candidates are refreshed without exhausting access.
8. As an operator, I want discovery kept distinct from refresh, so that I can see whether the candidate universe is expanding.
9. As an operator, I want related videos and offers linked to products, so that fragmented evidence can be assessed together.
10. As an operator, I want uncertain links reviewed and corrected, so that different products do not silently share signals.
11. As an operator, I want product links reconstructed as of a past decision, so that evaluation does not use later discoveries.
12. As an operator, I want attention growth compared with relevant peers, so that large established videos do not dominate every ranking.
13. As an operator, I want creator spread and concentration, so that a single viral creative is distinguishable from broader adoption.
14. As an operator, I want transaction evidence distinguished from intent proxies, so that views are not presented as sales.
15. As an operator, I want competition and coverage shown together, so that incomplete sampling is not mistaken for an empty market.
16. As an operator, I want sourcing costs and uncertainty recorded, so that attention can be assessed alongside practical feasibility.
17. As an operator, I want alerts with frozen reasons and inputs, so that later review reflects the original decision.
18. As an operator, I want historical comparisons against simple baselines, so that complexity must demonstrate useful improvement.
19. As an operator, I want launch delays modeled, so that a signal arriving too late is not counted as actionable success.
20. As an operator, I want forward outcomes measured after fixed horizons, so that policies are assessed on unseen evidence.
21. As an operator, I want collection to recover from interruptions, so that outages do not corrupt history or silently lose tasks.
22. As an operator, I want storage, requests, compute, and memory measured, so that scale decisions have a cost basis.
23. As an operator, I want deployment and restore instructions, so that the system can be operated on my VPS.

## Implementation Decisions

- Rust collectors and normalization; an adaptive application scheduler; Dagster OSS batch orchestration; PostgreSQL operational state; S3 raw JSONL with compression and normalized Parquet with Zstd; Polars and DuckDB analysis; a private Streamlit interface; Docker Compose deployment.
- Modules expose source capability/collection, observation publication/replay, product identity, assessment, evaluation, and operational control contracts. Engines exchange versioned records and dataset manifests, not shared mutable analytical files.
- Dataset contracts carry source and source entity identifiers, schema version, acquisition context, source timestamps and periods, collected and available timestamps, market scope where established, field semantics, precision, missingness, and real/synthetic classification.
- Repeated cumulative counts produce interval changes with reset/correction handling. They do not establish precise daily sales or view timing without supporting resolution.
- Initial sources are limited to granted official access and provenance-preserving imports. Display access concerns authorized accounts. Commercial Content Library access and permitted use must be verified. Research API data is excluded from this commercial pipeline. No generic API key guarantees global video discovery, product sales, viewer retention, or unrestricted comments.
- Capability states include unverified, available, denied, and degraded, with evidence and last verification time. Tests against fixtures do not promote a capability to available.
- Raw payload retention, permitted derived data, refresh, deletion, and export constraints are recorded per source. Storage volume does not override those constraints.
- Expensive media or language-model enrichment is optional, sampled, versioned, and budgeted. No paid API or provider is required to exercise the complete offline execution path.
- Prices, price ranges, sold counters, stock indicators, creator statistics, engagement, and comment signals are collected only where accessible and semantically documented. Unknown fields remain unknown.

## Testing Decisions

- Prefer scenario tests at the collection-to-visible-evidence boundary using controlled HTTP responses, real temporary PostgreSQL and S3-compatible services, and controlled time. Verify persistence, provenance, displayed results, and replay rather than internal function calls.
- Add focused tests for counter corrections, temporal joins, identity changes, and score normalization where scenario coverage would obscure edge cases.
- Validate source contracts separately against real credentials; live acceptance remains pending until those exist. There is no prior approved implementation or test suite to reuse.
- Repeated delivery, worker interruption, partial object publication, delayed data, quota exhaustion, and denied scopes are required scenarios.
- Performance measurements report hardware, input distribution, active concurrency, peak RSS, throughput, errors, request volume, and object-store traffic. No performance target is claimed before a representative baseline exists.

## Out of Scope

Advertising spend, purchasing inventory, unconfigured real product launches, resale of collected data, access circumvention, and cloud provisioning. The owner approved private launch preparation, inactive hosted-commerce connections and commercial learning on 2026-10-28; [the commercial-learning specification](../specs/commerce-learning.md) defines that expansion. Kubernetes, Kafka, a custom general-purpose orchestrator, distributed query infrastructure, and speculative machine-learning complexity are not prerequisites.

## Further Notes

Success first means trustworthy evidence and reproducible evaluation, then improved opportunity selection. Define top-K capacity, outcome thresholds, and target markets in versioned evaluation configurations before comparing policies. Report precision at K, coverage, lead time, alert duplication, and collection cost with uncertainty and sample size. A claimed profit advantage requires the operator's own outcome and cost data; attention growth alone cannot establish profitability.

Keys, source approvals, initial target market, and a representative load envelope remain external prerequisites. Their absence must be visible rather than filled with invented data. Detailed behaviors and acceptance criteria are in the six linked functional specifications and proposed tickets.

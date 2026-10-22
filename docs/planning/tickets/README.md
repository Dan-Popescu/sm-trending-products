# Delivery tickets

Status: approved for autonomous implementation; software is implemented and external acceptance remains pending where identified in each ticket. Published in [global PRD #2](https://github.com/Dan-Popescu/sm-trending-products/issues/2). T01–T15 are stable local identifiers; their actual GitHub issues are recorded below. See [implementation coverage](../../implementation/coverage.md) for actual files, test scenarios, and release gates; a test's existence is not a claim that native infrastructure or live access has been accepted.

Each ticket is a narrow complete behavior with an operator-visible outcome and acceptance evidence. Relevant specifications define the shared contracts; tickets do not establish competing definitions.

1. **[T01 — Inspect an authorized-video observation end to end](01-inspect-video-observation.md).** Blocked by: none. Collect one controlled Display-style response into durable history and show its provenance and access status.
2. **[T02 — Replay a completed dataset without source access](02-replay-datasets.md).** Blocked by: T01. Recompute normalized evidence from stored raw responses and expose dataset versions and drift quarantine.
3. **[T03 — Recover interrupted collection without duplicate results](03-recover-collection.md).** Blocked by: T01. Show recovery across worker and cross-store publication failures, preserving checkpoints and committed evidence.
4. **[T04 — Enforce shared quotas across collectors](04-enforce-shared-quotas.md).** Blocked by: T03. Run multiple workers under one source budget and expose wait, retry, and rate-limit states.
5. **[T05 — Link videos and offers to reviewed products](05-review-product-links.md).** Blocked by: T01. Review links, distinguish families, and reproduce previous grouping decisions.
6. **[T06 — Rank attention with inspectable reasons](06-rank-attention.md).** Blocked by: T05. Compare products using interval-aware attention features, actual coverage, and frozen alerts.
7. **[T07 — Discover and refresh candidates within a budget](07-prioritize-discovery.md).** Blocked by: T04, T06. Show discovery origins, protected exploration, adaptive refresh, and observed scheduling bias.
8. **[T08 — Inspect commercial-content evidence separately](08-inspect-commercial-content.md).** Blocked by: T01. Exercise a capability-gated commercial-content adapter and distinguish advertising evidence from organic reach.
9. **[T09 — Review demand, competition, and feasibility gaps](09-review-commercial-evidence.md).** Blocked by: T06, T08. Add documented offer and cost imports, explicit unknowns, and separately explained assessments.
10. **[T10 — Evaluate rankings with point-in-time replay](10-replay-attention-policies.md).** Blocked by: T06. Produce reproducible attention-target comparisons against baselines with no future observations or links.
11. **[T11 — Measure frozen decisions prospectively](11-run-forward-evaluation.md).** Blocked by: T10. Freeze decisions and report outcome maturity at 3, 7, and 14 days with launch-delay scenarios.
12. **[T12 — Compare commercial outcomes when evidence exists](12-evaluate-commercial-outcomes.md).** Blocked by: T09, T11. Evaluate demand/commercial targets only with eligible observations, retaining missing and censored outcomes.
13. **[T13 — Operate and restore the complete pipeline on a VPS](13-operate-and-restore.md).** Blocked by: T02, T03, T04, T07, T09, T11. Deploy the same services, restrict access, and demonstrate recovery of observations and frozen decisions in an isolated restore.
14. **[T14 — Establish a measured operating envelope](14-measure-operating-envelope.md).** Blocked by: T07, T10, T13. Publish repeatable load, memory, recovery, and object-store traffic measurements with a cost model.
15. **[T15 — Validate real source capabilities and enable live collection](15-verify-live-capabilities.md).** Engineering blockers: T01, T08. Human blocker: credentials, approved scopes and permitted use. Produce a verified capability matrix and at least one real observation from each actually available adapter; disable denied capabilities explicitly.

## Acceptance status

- T02 has controlled versioned replay acceptance, including a real v2 source
  transformation and immutable recovery; see the
  [criterion matrix](../../verification/replay-contract.md#controlled-t02-acceptance).
- T10/T11 have controlled replay/forward software acceptance with
  [criterion-by-criterion evidence](../../verification/evaluation-acceptance.md);
  no actual predictive advantage or elapsed market outcome is inferred.

- T01, T03, T04, T05 and T06 have agent-verified controlled engineering acceptance, with
  [criterion-by-criterion evidence](../../verification/foundation-acceptance.md).
  Their acceptance unlocks dependent engineering work without granting real
  TikTok capabilities or production-provider acceptance.
- T01–T12 have implemented software and behavioral test scenarios. The native PostgreSQL workflow passed worker/lease/quota scenarios; source fixtures do not establish live TikTok access.
- T13 has deployment, backup, and isolated-restore tooling. The Docker image build and full native container restore passed in CI; actual provider checks and VPS rollout remain pending.
- T14 has a measured synthetic sample and benchmark tooling. A production operating envelope, multiworker load capacity, and recurring provider cost remain unproven.
- T15 has capability-verification tooling, but live acceptance is pending credentials, approved scopes, and permission for the actual intended use. It is not marked complete.

The individual acceptance checklists retain the original delivery requirements. Use the coverage record and execution reports to decide which release gates are satisfied; do not treat an implemented feature as deployed or accepted merely because its code exists.

## Execution notes

T15 can run as soon as credentials exist and its two adapter tickets pass; it need not wait for the complete platform. Missing commercial-content access does not block engineering verification using isolated fixtures, but does limit real coverage and must remain visible.

T10 deliberately starts with an attention outcome. T12 extends evaluation to commercial targets after their inputs exist; this avoids falsely blocking all historical methodology on unavailable sales data.

No ticket purchases services, deploys to an unspecified machine, launches products, or spends an advertising budget. T13 supplies a supported deployment and isolated exercise; actual VPS rollout requires the machine details and access.

T14 must not promise a capacity number before measurement. This plan builds the mechanisms required to scale and then establishes their operating envelope.

## Published issues

| Local ticket | GitHub issue |
| --- | --- |
| T01 | [#9](https://github.com/Dan-Popescu/sm-trending-products/issues/9) |
| T02 | [#10](https://github.com/Dan-Popescu/sm-trending-products/issues/10) |
| T03 | [#11](https://github.com/Dan-Popescu/sm-trending-products/issues/11) |
| T04 | [#12](https://github.com/Dan-Popescu/sm-trending-products/issues/12) |
| T05 | [#13](https://github.com/Dan-Popescu/sm-trending-products/issues/13) |
| T06 | [#14](https://github.com/Dan-Popescu/sm-trending-products/issues/14) |
| T07 | [#15](https://github.com/Dan-Popescu/sm-trending-products/issues/15) |
| T08 | [#16](https://github.com/Dan-Popescu/sm-trending-products/issues/16) |
| T09 | [#17](https://github.com/Dan-Popescu/sm-trending-products/issues/17) |
| T10 | [#18](https://github.com/Dan-Popescu/sm-trending-products/issues/18) |
| T11 | [#19](https://github.com/Dan-Popescu/sm-trending-products/issues/19) |
| T12 | [#20](https://github.com/Dan-Popescu/sm-trending-products/issues/20) |
| T13 | [#21](https://github.com/Dan-Popescu/sm-trending-products/issues/21) |
| T14 | [#22](https://github.com/Dan-Popescu/sm-trending-products/issues/22) |
| T15 | [#23](https://github.com/Dan-Popescu/sm-trending-products/issues/23) |

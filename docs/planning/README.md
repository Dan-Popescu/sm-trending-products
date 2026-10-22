# Delivery plan

Status: architecture, defaults, specifications, test boundary, and autonomous implementation approved by the user on 2026-10-22. Core software is merged through PR #1. Follow-up offline corrections and acceptance proofs are recorded in [offline verification](../verification/offline-readiness.md). Representative capacity/fault limits, target deployment and live-source acceptance remain release gates. See the [implementation coverage](../implementation/coverage.md) for evidence and limits rather than interpreting this status as a production sign-off.

## Reading order

1. [Product requirements](../prd/overview.md): purpose, scope, source constraints, and success criteria.
2. [Domain vocabulary](../../CONTEXT.md): shared meanings of product, offer, observation, and opportunity.
3. [Architecture decisions](../adr/): accepted architectural tradeoffs.
4. Functional specifications:
   - [Reliable collection and replay](../specs/collection.md)
   - [Discovery and collection prioritization](../specs/discovery.md)
   - [Product identity and evidence](../specs/product-identity.md)
   - [Trend assessment](../specs/trend-assessment.md)
   - [Historical and forward validation](../specs/validation.md)
   - [Operations and deployment](../specs/operations.md)
5. [Repository setup record](setup.md): accepted choices and links to the active agent configuration.
6. [Delivery tickets](tickets/README.md): approved behavior, dependency map, and acceptance status.
7. [Implementation coverage](../implementation/coverage.md): actual software/test mapping and outstanding release gates.

## Working method

Use Matt Pocock's setup, domain-modeling, to-spec, to-tickets, implement, and code-review workflows. Start from externally visible behavior, specify acceptance criteria, and implement narrow vertical slices. A database layer, collector layer, or dashboard layer alone is not a delivery ticket.

GitHub Issues is the canonical tracker for Dan-Popescu/sm-trending-products. Keep reviewed product requirements in the repository; maintain the published umbrella issue and one issue per approved delivery ticket. Maintain native blocking links when the integration supports them; otherwise use explicit issue references. Do not mark a ticket ready-for-agent before its scope is approved and its human prerequisites are satisfied.

Read the domain glossary and relevant architecture decisions before implementing. For each ready ticket: select an unblocked slice, verify its acceptance criteria at the highest useful boundary, implement, run the relevant checks, and review the change against its specification. Use test-first development for nontrivial correctness rules. Record evidence in the pull request. Review, merge and continue autonomously under the user's standing authorization recorded in [AGENTS.md](../../AGENTS.md#autonomous-pull-request-delivery). There is no human pull-request approval step.

## Accepted setup and remaining acceptance

- Architecture: approved by the user.
- Agent instructions: AGENTS.md selected under the user's authorization to choose recommended defaults.
- Default triage labels: explicitly approved by the user.
- Configuration: activated in AGENTS.md and docs/agents/.
- Test boundary: collection-to-visible-evidence scenarios with native PostgreSQL and an HTTP S3 test service, a controlled HTTP source, and deterministic time. Smaller tests cover numerical and temporal invariants where helpful. PGlite remains an optional fallback; concurrency acceptance uses native PostgreSQL.
- Delivery breakdown: approved for autonomous execution; global PRD #2, specification issues #3–#8, and delivery issues #9–#23 are published.
- Live source acceptance: cannot be completed without approved credentials and scopes. Contract fixtures demonstrate software behavior, not granted access or market coverage.

## Current repository state

The initial implementation was merged through [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1), followed by autonomous correctness and acceptance work. The original GitHub permission blocker is resolved. The [publication record](../implementation/publication.md) preserves historical commit mappings; GitHub remains authoritative for current integration and issue state.

T01–T13 now have controlled engineering acceptance mapped in the
[ticket index](tickets/README.md). Follow-up work fixed shared quota/claim
contention, bounded history/cache/review access, versioned replay, historical
decision parity and incomplete-ledger revisions. Every PR runs Rust/Python/native
checks, the controlled benchmark/quota gate, image build and isolated recovery.
The strengthened recovery rehearsal resumes an interrupted checkpoint from copied
objects and verifies idempotency after reopening the restored application.

The [seven-case native measurement](../verification/controlled-envelope.md)
advances T14 with declared resource limits and explicit measurement gaps. The
remaining priority is target-VPS/private-access and production-object-store
acceptance, followed by representative retained-history/full-stack measurements
and dated provider prices. These need the specified infrastructure, independently
of TikTok access. T15 remains pending actual approved credentials, scopes and
intended use; synthetic tests cannot complete it. No production predictive value,
actual profit or universal capacity is established by controlled fixtures.

The preexisting local Python/SQLite scaffold was an abandoned experiment. The supported runtime now implements PostgreSQL operational state and S3 raw/Parquet history; the earlier experiment is outside that runtime. Implementation assumptions and substitutions are recorded in [the decisions recap](../implementation/assumptions.md).

All repository documentation and technical identifiers use English, including filenames, variables, functions, tables, fields, metrics, and issue titles. User-facing discussion can remain in French.

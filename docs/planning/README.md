# Delivery plan

Status: architecture, defaults, specifications, test boundary, and autonomous implementation approved by the user on 2026-10-22. The software delivery is implemented; native-service, deployment, capacity, and live-source acceptance remain separate release gates. See the [implementation coverage](../implementation/coverage.md) for evidence and limits rather than interpreting this status as a production sign-off.

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

Read the domain glossary and relevant architecture decisions before implementing. For each ready ticket: select an unblocked slice, verify its acceptance criteria at the highest useful boundary, implement, run the relevant checks, and review the change against its specification. Use test-first development for nontrivial correctness rules. Record evidence in the pull request. Do not merge or deploy automatically.

## Accepted setup and remaining acceptance

- Architecture: approved by the user.
- Agent instructions: AGENTS.md selected under the user's authorization to choose recommended defaults.
- Default triage labels: explicitly approved by the user.
- Configuration: activated in AGENTS.md and docs/agents/.
- Test boundary: collection-to-visible-evidence scenarios with PostgreSQL semantics and an HTTP S3 test service, a controlled HTTP source, and deterministic time. Smaller tests cover numerical and temporal invariants where helpful. Local SQL execution uses PGlite; native PostgreSQL remains required for concurrency acceptance.
- Delivery breakdown: approved for autonomous execution; global PRD #2, specification issues #3–#8, and delivery issues #9–#23 are published.
- Live source acceptance: cannot be completed without approved credentials and scopes. Contract fixtures demonstrate software behavior, not granted access or market coverage.

## Current repository state

The repository is published on `codex/project-planning` in [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1). The initial permissions blocker was resolved on 2026-10-22. The [publication record](../implementation/publication.md) maps original local commits and issue identifiers. PR #1 tracks the merge into `main`; its current GitHub state is the authoritative integration status.

The preexisting local Python/SQLite scaffold was an abandoned experiment. The supported runtime now implements PostgreSQL operational state and S3 raw/Parquet history; the earlier experiment is outside that runtime. Implementation assumptions and substitutions are recorded in [the decisions recap](../implementation/assumptions.md).

All repository documentation and technical identifiers use English, including filenames, variables, functions, tables, fields, metrics, and issue titles. User-facing discussion can remain in French.

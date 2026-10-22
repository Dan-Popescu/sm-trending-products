# T07 — Discover and refresh candidates within a budget

GitHub issue: [#15](https://github.com/Dan-Popescu/sm-trending-products/issues/15). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Software implemented; native PostgreSQL functional scenarios passed in CI. See the [coverage record](../../implementation/coverage.md).

## What to build

Turn accessible discovery results or explicit imports into a refresh queue that balances new candidates with promising known products and shows why each request was selected.

## Acceptance criteria

- [ ] Candidates retain origin, query/import context, first discovery time, scope, and actual source capability.
- [ ] An unavailable discovery endpoint produces a visible coverage limitation rather than a misleading global discovery claim.
- [ ] Policy versions protect a configured exploration share even with a large high-priority queue.
- [ ] Freshness, uncertainty, growth, and request cost affect future collection within the shared quota; planned and achieved intervals are visible.
- [ ] Priority changes can be reproduced from the same policy and observed evidence, and selection reasons are retained for bias analysis.
- [ ] Dagster schedules bounded batches while the application policy selects their candidate tasks; runs do not scale one-for-one with videos.

## Blocked by

T04 — Enforce shared quotas across collectors; T06 — Rank attention with inspectable reasons.

## Specification

[Functional specification](../../specs/discovery.md). Shared contracts are defined there; this ticket adds no alternative definitions.

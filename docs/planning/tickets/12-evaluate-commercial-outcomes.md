# T12 — Compare commercial outcomes when evidence exists

GitHub issue: [#20](https://github.com/Dan-Popescu/sm-trending-products/issues/20). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Software implemented; acceptance evidence is mapped in the [coverage record](../../implementation/coverage.md).

## What to build

Extend the existing evaluation report to supported commercial outcomes while preserving the distinction between attention success, transaction evidence, and actual business profit.

## Acceptance criteria

- [ ] Sales-related targets run only on period-compatible observed or bounded commercial evidence available under the evaluation contract.
- [ ] Candidates lacking a measurable commercial outcome remain visible in coverage and censoring counts.
- [ ] A transaction-popularity baseline is included only when suitable transaction evidence exists.
- [ ] Bounded counters and estimated outcomes are evaluated separately from exact observations and sensitivity assumptions are explicit.
- [ ] Actual profitability reporting requires imported realized revenue and complete defined cost inputs; attention-only success is never labeled profit.
- [ ] Point-in-time and prospective reports retain their original frozen definitions when commercial inputs are added later.

## Blocked by

T09 — Review demand, competition, and feasibility gaps; T11 — Measure frozen decisions prospectively.

## Specification

[Functional specification](../../specs/validation.md). Shared contracts are defined there; this ticket adds no alternative definitions.

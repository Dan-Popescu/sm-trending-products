# T12 — Compare commercial outcomes when evidence exists

GitHub issue: [#20](https://github.com/Dan-Popescu/sm-trending-products/issues/20). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled engineering acceptance verified against the original criteria;
see the [criterion matrix](../../verification/commercial-outcome-acceptance.md).
Real commercial outcomes and business profitability remain unvalidated.
The financial definition and baseline eligibility rules are specified in the
[validation contract](../../specs/validation.md); existing frozen definitions are
preserved when the stronger contract is used for new decisions.

## What to build

Extend the existing evaluation report to supported commercial outcomes while preserving the distinction between attention success, transaction evidence, and actual business profit.

## Acceptance criteria

- [x] Sales-related targets run only on period-compatible observed or bounded commercial evidence available under the evaluation contract.
- [x] Candidates lacking a measurable commercial outcome remain visible in coverage and censoring counts.
- [x] A transaction-popularity baseline is included only when suitable transaction evidence exists.
- [x] Bounded counters and estimated outcomes are evaluated separately from exact observations and sensitivity assumptions are explicit.
- [x] Actual profitability reporting requires imported realized revenue and complete defined cost inputs; attention-only success is never labeled profit.
- [x] Point-in-time and prospective reports retain their original frozen definitions when commercial inputs are added later.

## Blocked by

T09 — Review demand, competition, and feasibility gaps; T11 — Measure frozen decisions prospectively.

## Specification

[Functional specification](../../specs/validation.md). Shared contracts are defined there; this ticket adds no alternative definitions.

# T11 — Measure frozen decisions prospectively

GitHub issue: [#19](https://github.com/Dan-Popescu/sm-trending-products/issues/19). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled engineering acceptance verified by the agent; see the
[criterion matrix](../../verification/evaluation-acceptance.md#t11-prospective-decision-software).

## What to build

Freeze new decisions and expose their pending and matured attention outcomes at fixed horizons without changing the original decision after the fact.

## Acceptance criteria

- [x] Each decision snapshot retains eligible evidence references, candidate universe, policy and identity versions, cutoff, and frozen reasons.
- [x] Outcomes at 3, 7, and 14 days remain pending until the required evidence horizon matures.
- [x] Launch-delay scenarios of 2 and 10 days report their effect separately from immediate-action results.
- [x] A policy change applies to future decisions and cannot overwrite previously frozen decisions or redefine their success thresholds.
- [x] Missing, deleted, or unobserved outcomes remain explicitly censored or invalid according to the frozen configuration.
- [x] The operator view distinguishes backtest, forward-test, synthetic, and live results.

## Blocked by

T10 — Evaluate rankings with point-in-time replay.

## Specification

[Functional specification](../../specs/validation.md). Shared contracts are defined there; this ticket adds no alternative definitions.

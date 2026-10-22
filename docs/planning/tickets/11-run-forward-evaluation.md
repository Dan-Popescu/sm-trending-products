# T11 — Measure frozen decisions prospectively

**Status:** proposed; not published.

## What to build

Freeze new decisions and expose their pending and matured attention outcomes at fixed horizons without changing the original decision after the fact.

## Acceptance criteria

- [ ] Each decision snapshot retains eligible evidence references, candidate universe, policy and identity versions, cutoff, and frozen reasons.
- [ ] Outcomes at 3, 7, and 14 days remain pending until the required evidence horizon matures.
- [ ] Launch-delay scenarios of 2 and 10 days report their effect separately from immediate-action results.
- [ ] A policy change applies to future decisions and cannot overwrite previously frozen decisions or redefine their success thresholds.
- [ ] Missing, deleted, or unobserved outcomes remain explicitly censored or invalid according to the frozen configuration.
- [ ] The operator view distinguishes backtest, forward-test, synthetic, and live results.

## Blocked by

T10 — Evaluate rankings with point-in-time replay.

## Specification

[Functional specification](../../specs/validation.md). Shared contracts are defined there; this ticket adds no alternative definitions.

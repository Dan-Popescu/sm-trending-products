# T10 — Evaluate rankings with point-in-time replay

GitHub issue: [#18](https://github.com/Dan-Popescu/sm-trending-products/issues/18). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Software implemented; acceptance evidence is mapped in the [coverage record](../../implementation/coverage.md).

## What to build

Compare attention-ranking policies on the same recorded candidate universe and display a reproducible report that excludes future evidence.

## Acceptance criteria

- [ ] A frozen evaluation configuration defines universe, market scope, target, thresholds, top-K, horizons, versions, and action-delay scenarios before running.
- [ ] Delayed observations, later identity links, later source capabilities, and corrections do not enter earlier decisions.
- [ ] Contemporaneous popularity, recent observed growth, and a seeded matched random baseline use the same eligible universe.
- [ ] Temporal splits and product-family grouping avoid duplicate leakage where fitting or policy selection occurs; repeated alerts are reported as dependent observations.
- [ ] The report includes defined precision at K, coverage, lead-time distribution, missing outcomes, sample sizes, and uncertainty rather than only a winner list.
- [ ] Fixed inputs reproduce results; insufficient recorded history yields insufficient_history instead of fabricated backtest metrics.

## Blocked by

T06 — Rank attention with inspectable reasons.

## Specification

[Functional specification](../../specs/validation.md). Shared contracts are defined there; this ticket adds no alternative definitions.

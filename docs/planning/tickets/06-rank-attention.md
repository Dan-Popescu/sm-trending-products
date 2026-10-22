# T06 — Rank attention with inspectable reasons

GitHub issue: [#14](https://github.com/Dan-Popescu/sm-trending-products/issues/14). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled engineering acceptance verified by the agent; see the
[criterion matrix](../../verification/foundation-acceptance.md#t06-published-attention-and-frozen-alerts).

## What to build

Let an operator compare attention around reviewed products, inspect why they rank, and retain an alert snapshot that later evidence cannot rewrite.

## Acceptance criteria

- [x] Controlled timelines produce interval-adjusted growth and acceleration with age/cohort context and measured observation intervals.
- [x] Creator/video spread, concentration, and likely creative duplication distinguish one viral post from broader product diffusion.
- [x] Counter resets, corrections, stale evidence, insufficient sample size, and unknown fields are visible rather than interpreted as ordinary zeros.
- [x] The private view reads a published result version and traces each feature to eligible observations and identity decisions.
- [x] A versioned threshold creates a frozen alert with evidence, reasons, scope, cutoff, and cooldown behavior.
- [x] Synthetic test alerts are isolated from the live review queue and all four assessment dimensions remain separately represented.

## Blocked by

T05 — Link videos and offers to reviewed products.

## Specification

[Functional specification](../../specs/trend-assessment.md). Shared contracts are defined there; this ticket adds no alternative definitions.

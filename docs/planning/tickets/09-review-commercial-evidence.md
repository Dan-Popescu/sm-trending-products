# T09 — Review demand, competition, and feasibility gaps

**Status:** Software implemented; acceptance evidence is mapped in the [coverage record](../../implementation/coverage.md). Not published to GitHub.

## What to build

Extend product review with accessible offer observations and provenance-preserving cost imports, showing demand, competition, and feasibility alongside attention.

## Acceptance criteria

- [ ] An imported offer observation records source, market, currency, price, sold-counter semantics, stock indicators when present, acquisition time, and field precision.
- [ ] Rounded or cumulative sold counts produce documented interval bounds or unknowns rather than invented exact daily unit sales.
- [ ] Observed transactions, intent proxies, and optional modeled estimates remain separate and their assumptions are visible.
- [ ] Competition reflects observed offers, sellers, and commercial activity with measured coverage; absence of evidence is not zero competition.
- [ ] Cost, delivery, and return inputs retain provenance, freshness, and unknowns; profitability is presented only as a defined scenario until actual outcomes exist.
- [ ] Ranking policy exposes dimension weights, minimum evidence, missing-data behavior, and reasons without hiding absent commercial evidence.

## Blocked by

T06 — Rank attention with inspectable reasons; T08 — Inspect commercial-content evidence separately.

## Specification

[Functional specification](../../specs/trend-assessment.md). Shared contracts are defined there; this ticket adds no alternative definitions.

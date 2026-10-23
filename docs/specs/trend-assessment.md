# Trend assessment and opportunity review

GitHub issue: [#7](https://github.com/Dan-Popescu/sm-trending-products/issues/7). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

Status: approved for autonomous implementation on 2026-10-22.

## Problem Statement

Views alone cannot distinguish a useful emerging product from a viral creative, a mature market, or an unprofitable item.

## Solution

Show separately explained attention, demand, competition, and commercial-feasibility assessments with evidence, uncertainty, coverage, and an actionable review queue.

## User Stories

1. As an operator, I want exposure acceleration and creator spread, so that I can inspect early diffusion.
2. As an operator, I want age and cohort normalization, so that old viral content does not dominate every ranking.
3. As an operator, I want observed transactions distinguished from intent proxies, so that I can judge demand honestly.
4. As an operator, I want observed competition and sampling limits, so that missing sellers do not imply an empty market.
5. As an operator, I want cost assumptions and freshness, so that feasibility is reviewable.
6. As an operator, I want alert reasons frozen, so that subsequent updates cannot rewrite the original evidence.
7. As an operator, I want missing dimensions visible, so that a partial opportunity is not presented as fully validated.

## Implementation Decisions

- Attention features include interval-adjusted counter growth, acceleration, age-normalized performance, creator/video spread, concentration, and persistence across observations. Negative deltas are flagged as corrections rather than negative consumption.
- Demand inputs distinguish observed/bounded sold counters, independent intent signals where accessible, and unsupported estimates. Capture unit, period, rounding, and model version. Views never become measured unit sales through a fixed conversion assumption.
- Competition uses observed unique offers, sellers, creatives, ads, and price evolution with source/market scope. A commercial-library ad's first-observed time is not necessarily its actual launch time.
- Feasibility accepts provenance-preserving manual/imported costs, delivery constraints, and return assumptions. Contributions remain scenarios until actual costs and outcomes are available; missing values do not become zero.
- Polars computes versioned features in bounded partitions. Publish reusable results for Streamlit; interactive views do not scan the entire historical store. DuckDB queries immutable dataset versions without a shared writable database file.
- The private review surface loads only the selected view. Assessment, alert, and evaluation lists use bounded SQL projections; full snapshots are fetched only for the selected item. Product links are paged after resolving ownership at the selected cutoff. Evidence pages use cursors scoped to the selected product, cutoff, and workspace; historical preparation is shown explicitly and can be continued without advancing that cursor. Reads refresh on each interaction; successful mutations refresh the lists and advance the review cutoff. Database/object clients are session-scoped resources, replaced and closed when configuration or credentials change. Provider-file selectors and default shared AWS files participate through path/stat identity only; credential-file contents never enter the resource key.
- New persisted results use compact schema `2`: evidence identifiers live in
  immutable bounded provenance trees, with full input provenance retained even for
  excluded rows. Selected-product details page references through the authoritative
  publication lifecycle; new results never load an entire universe for review.
  Old stored results remain unchanged. See [ADR0008](../adr/0008-compact-analytical-provenance.md).
- Retain each dimension and confidence separately. A configurable ranking policy may combine them, but must expose weights, missing-data treatment, minimum evidence, scope, and reasons. Do not default unknown demand to zero or call an attention-only ranking a sales ranking.
- Alert deduplication and cooldown are explicit policies. Frozen alerts retain evidence references, coverage, availability cutoff, identity version, and scoring version.
- Published assessment rows carry an `identity` snapshot: a deterministic hash of
  the resolved, confidence-eligible link records and their nonempty immutable
  decision references. It uses the shared point-in-time ownership resolver;
  normalization does not resolve identity again. New results and alerts include
  this additive metadata. Previously published objects and alerts remain unchanged.

## Testing Decisions

Feed a controlled timeline through assessment to the review view. Distinguish a single viral creative from broad product spread, compare content of different ages, preserve unknown demand, handle rounded counters, and prevent a repeated creative from inflating independent spread. Inspect a frozen alert after later data changes. No existing approved tests exist.

## Out of Scope

Guaranteed sales prediction, unavailable retention/completion/swipe metrics, or automatic launches and advertising spend.

## Further Notes

Done means each displayed assessment can be traced to evidence and reproduced. Any new derived feature requires semantics, availability rules, and an intended decision before storage is expanded.

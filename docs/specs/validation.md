# Historical replay and forward validation

GitHub issue: [#8](https://github.com/Dan-Popescu/sm-trending-products/issues/8). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

Status: approved for autonomous implementation on 2026-10-22.

## Problem Statement

A ranking can look excellent after selecting known winners, using later links, or treating late-arriving counters as historical facts. It may still identify products too late to launch.

## Solution

Replay recorded knowledge at each decision time, compare policies with simple baselines, and freeze prospective decisions before their outcomes become available.

## User Stories

1. As an operator, I want explicit point-in-time eligibility, so that unavailable history is not silently fabricated.
2. As an operator, I want fair baselines, so that complex scoring must earn its cost.
3. As an operator, I want product families isolated in evaluation, so that duplicates do not exaggerate generalization.
4. As an operator, I want action delays included, so that theoretical lead time reflects launch constraints.
5. As an operator, I want future outcomes measured after fixed horizons, so that I can validate on unseen data.
6. As an operator, I want unsuccessful and disappearing candidates retained where allowed, so that survivors do not define the whole sample.
7. As an operator, I want insufficient history reported, so that uncertainty remains visible.

## Implementation Decisions

- Eligibility is based on availability time for observations, source capabilities, links, features, policies, and cost inputs. Source timestamps alone cannot establish historical eligibility. Corrections create later versions.
- Minimum source-history sampling points, features and frozen evidence references
  use the ranking policy's same lookback window in both raw and bounded persisted
  decisions. SQL reads prune that window, and the shared eligibility filter
  enforces it before counting or freezing references.
- An evaluation configuration freezes candidate universe, market scope, minimum evidence, top-K, ranking and identity versions, target metric, threshold, horizon, action delay, and missing-outcome treatment before measuring results.
- Initial outcome horizons are 3, 7, and 14 days; launch-delay scenarios include 2 and 10 days. These are scenarios, not profitability assumptions. Future attention or observed sales outcomes remain distinct targets.
- Compare with contemporaneous popularity, recent observed growth, and a seeded random baseline matched to the same eligible universe and coverage. Sales-popularity baselines require fresh exact cumulative counts on every transaction-bearing offer in the ranking window and one declared common reporting period across the candidate universe. Price-only offers do not add a transaction counter. Estimated, bounded, stale or unsupported counters exclude that baseline without changing the existing observed-subset assessment features; the frozen snapshot records its eligibility reason.
- Use temporal train/validation/test periods where fitting occurs, keep families grouped, report repeated-alert dependence, and preserve the same collection constraints across policies. A frozen final test is not repeatedly tuned against.
- Reports show precision at K for predefined observable targets, coverage, lead-time distributions, missing/censored outcomes, sample sizes, uncertainty intervals, and collection cost. Separate predictive comparison from evidence of business profit.
- A forward test stores immutable decision snapshots, matures each horizon, and records changes to future policy versions without rewriting prior decisions. Snapshots include evidence sufficient for audit within retention constraints.
- New persisted schema `2` decisions and outcomes reference bounded immutable
  audit trees. All loaded outcome inputs are dependencies, including incomplete
  corrections that censor a result. Backtests retain paged decision/report
  records and nested product/outcome collections, rather than duplicate whole
  universes inside one SQL value. Selected record and reference paths are
  inspectable without globally hydrating provenance. Stored schema `1`
  configuration, snapshot and report hashes retain exact legacy semantics.
  [ADR0008](../adr/0008-compact-analytical-provenance.md) defines the representation.
- Realized profit resolves the latest available revision of each ledger period
  before checking currency, realized status and cost completeness. A later
  incomplete or incompatible ledger censors that period rather than reviving a
  superseded complete profit claim. Earlier-cutoff reports remain immutable.
- New profit decisions and backtests require a versioned `profit_contract` in the
  frozen evaluation configuration. Its `ledgers` map declares reviewed `cost` or
  `offer` entity keys and a nonempty set of required actual-cost component names
  per ledger. The keys must already belong to reviewed links in the decision's
  workspace, market and availability scope. A recent quote or future actual
  result is not required to declare a known ledger. Invalid scope rejects before
  reading observation history. Ordinary offers remain evidence without becoming
  extra financial ledgers; every declared ledger must cover the outcome period
  or that product remains censored.
- Under that contract, realized costs are an explicit map containing every
  declared component. Every provided component must be finite and nonnegative;
  explicit zero is valid, missing is unknown, and additional actual components
  are included in the sum. No fee, tax, return or transaction is invented.
  The existing realized/completeness attestations, currency and contiguous source
  periods still apply. Invalid amounts, including arithmetic overflow, censor
  the result. Old snapshots without `profit_contract` retain their original
  monetary and entity-scope definitions; parsing or maturation never recreates
  their configuration, hashes or stored payloads.
- New backtest methodology reports the requested `target`, the contract version
  and `measured_financial_outcome_count` for policy outcomes only, counted per
  decision/horizon/delay scenario without repeating baseline outcomes. The
  compatibility field `profit_claim` is false: an imported monetary result is
  inspectable evidence, and neither an evaluation target nor synthetic results
  establish a real business-profit advantage.
- Persisted `freeze` and `backtest` accept the same `AssessmentPolicy` contract as
  scheduled assessment. The policy is passed through bounded preparation and
  frozen once in the existing snapshot; maturation reads that snapshot. Outcome
  configuration remains separate from ranking policy, with no second policy
  registry or recomputation from current defaults. CLI commands accept
  `--policy-file`; omitting it retains the established default.
- Read one selected product's bounded evidence once per maturation, reuse it across horizon/delay scenarios, and retain only outcome summaries before loading the next product. Prepared entity timelines reuse cumulative boundary/correction indexes and interval coverage/sums across threshold candidates. Reference lists are materialized for reported outcomes and the first confirmed crossing rather than for every unsuccessful candidate. These indexes preserve the same revisions, gaps, units, periods, censoring and availability rules as direct evaluation.

## Testing Decisions

Use deliberately delayed observations, future product links, corrected counters, duplicate families, and incomplete horizons. Verify that none enter a past decision, that a new snapshot cannot fabricate earlier history, and that repeated replay of a fixed version yields identical rankings and metrics. Verify outcome maturity with controlled time. No existing approved tests exist.

## Out of Scope

Claiming a backtest from present-day cumulative counters, causal attribution of sales to TikTok views, or profitability claims without actual costs and outcomes.

## Further Notes

Done means an evaluation is independently reproducible and clearly states what it can establish. Until sufficient history exists, the correct result may be insufficient_history; forward testing is still useful immediately after real collection begins.

A financial configuration example (component names are declared by the operator,
not supplied as defaults):

```json
{
  "target": "profit",
  "currency": "EUR",
  "profit_contract": {
    "version": "realized-profit-v1",
    "ledgers": {
      "cost_import:cost:ledger": ["goods", "fees"]
    }
  }
}
```

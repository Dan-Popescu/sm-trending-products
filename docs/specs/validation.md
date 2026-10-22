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
- An evaluation configuration freezes candidate universe, market scope, minimum evidence, top-K, ranking and identity versions, target metric, threshold, horizon, action delay, and missing-outcome treatment before measuring results.
- Initial outcome horizons are 3, 7, and 14 days; launch-delay scenarios include 2 and 10 days. These are scenarios, not profitability assumptions. Future attention or observed sales outcomes remain distinct targets.
- Compare with contemporaneous popularity, recent observed growth, and a seeded random baseline matched to the same eligible universe and coverage. Sales-popularity baselines run only when relevant transaction evidence exists.
- Use temporal train/validation/test periods where fitting occurs, keep families grouped, report repeated-alert dependence, and preserve the same collection constraints across policies. A frozen final test is not repeatedly tuned against.
- Reports show precision at K for predefined observable targets, coverage, lead-time distributions, missing/censored outcomes, sample sizes, uncertainty intervals, and collection cost. Separate predictive comparison from evidence of business profit.
- A forward test stores immutable decision snapshots, matures each horizon, and records changes to future policy versions without rewriting prior decisions. Snapshots include evidence sufficient for audit within retention constraints.
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

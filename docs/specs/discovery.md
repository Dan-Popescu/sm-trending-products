# Discovery and collection prioritization

GitHub issue: [#4](https://github.com/Dan-Popescu/sm-trending-products/issues/4). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

Status: approved for autonomous implementation on 2026-10-22.

## Problem Statement

Refreshing every candidate equally wastes requests, while refreshing only current winners can hide new trends and create self-confirming rankings.

## Solution

Separate discovery from refresh, allocate a bounded exploration budget, and refresh candidates according to evidence freshness, uncertainty, and growth under actual source quotas.

## User Stories

1. As an operator, I want to know where new candidates originate, so that discovery coverage is inspectable.
2. As an operator, I want promising candidates refreshed sooner, so that emerging changes can be noticed.
3. As an operator, I want exploration preserved, so that unknown candidates remain discoverable.
4. As an operator, I want priorities explained, so that I can detect scheduling bias.
5. As an operator, I want collection to stop cleanly at its budget, so that resource use stays bounded.
6. As an operator, I want a missing discovery capability reported, so that refresh is not sold as broad trend detection.

## Implementation Decisions

- Candidate origins include supported source discovery, authorized-account feeds, and explicit imports. Each retains query/context, coverage, cursor, and discovery time. Only granted capabilities can issue requests.
- Versioned policies allocate exploration, monitoring, and investigation budgets. Priority uses freshness, uncertainty, evidence growth, and observation cost; it does not silently exclude low-activity candidates forever.
- Optional versioned `request_costs` declare estimated request units by supported source; unspecified costs equal one. Cost-adjusted refresh scores are capped at 999, below the reserved exploration execution band of 1000 and above. Estimates adjust priority without changing actual Rust-enforced quota accounting or the protected exploration allocation. They are policy inputs, not measured provider prices.
- Configure target intervals by tier; initial example intervals of 1, 6, and 24 hours are tunable policies, not promised source freshness. Intraday polling is useful only if quota and source update resolution support it.
- Scheduling budgets count physical source requests. Compatible Display refresh intentions share requests of at most twenty IDs while keeping account, scheduling tier and fixture context separate. Ads/Commercial refresh intentions from the same origin query share one paginated cycle; compatible due discovery and refresh share that cycle. Each task retains every entity's policy inputs and planned interval, and each entity receives its own next due time. Active grouped tasks suppress duplicate refreshes for all members and the same in-flight commercial query, including continuation payloads. Later due cycles remain distinct.
- `scheduled` and `refresh_scheduled` report physical tasks; `refresh_entities_scheduled` reports the entity intentions covered. Discovery and refresh counts can overlap when one task serves both purposes.
- A zero effective task budget returns explicit backpressure after persisting policy activation, without scanning or deferring candidate entities. Policy changes remain effective while a queue is full.
- PostgreSQL coordinates due work and shared quotas. The application scheduler chooses what to collect; Dagster starts and observes bounded batches. Do not create one orchestration run per video.
- Planning admission and analytical publication have independent configurable
  cadences. The collection job does not rebuild the analytical universe after
  every small batch. An explicit workload profile reports required refresh
  requests versus the configured planning bound and any declared source budget;
  it never infers runtime capacity or granted access. Busy workers continue after
  successful full batches, while idle, failed or quota-deferred batches wait.
- Record planned versus achieved intervals, skipped work, source update delay, policy version, and selection reasons. Evaluation uses the actual sampled universe and intervals.
- The operator can inspect persisted plans through workspace-scoped summary and
  selected-member pages. Policy inputs remain frozen; current candidate timestamps
  and origin references are labeled separately. Review reads existing task and
  entity records and never invokes planning. Actual observation intervals remain
  part of the published assessment rather than being replaced by planned targets.

## Testing Decisions

With a controlled source, clock, and quota, show newly discovered candidates, queued priorities, their resulting observations, and explanations in the operator view. Verify exploration survives a large hot queue, multiple workers respect shared limits, outages do not cause retry storms, and denied discovery produces a clear coverage warning. No existing approved tests exist.

## Out of Scope

An unrestricted public TikTok search promise, guaranteed minute-level freshness, or a custom general-purpose workflow engine.

## Further Notes

Done means budgeted discovery or its explicit unavailability is visible, and scheduling changes produce measurable collection behavior. Policy history is retained for later bias analysis.

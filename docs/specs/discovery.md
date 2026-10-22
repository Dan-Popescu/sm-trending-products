# Discovery and collection prioritization

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
- Configure target intervals by tier; initial example intervals of 1, 6, and 24 hours are tunable policies, not promised source freshness. Intraday polling is useful only if quota and source update resolution support it.
- PostgreSQL coordinates due work and shared quotas. The application scheduler chooses what to collect; Dagster starts and observes bounded batches. Do not create one orchestration run per video.
- Record planned versus achieved intervals, skipped work, source update delay, policy version, and selection reasons. Evaluation uses the actual sampled universe and intervals.

## Testing Decisions

With a controlled source, clock, and quota, show newly discovered candidates, queued priorities, their resulting observations, and explanations in the operator view. Verify exploration survives a large hot queue, multiple workers respect shared limits, outages do not cause retry storms, and denied discovery produces a clear coverage warning. No existing approved tests exist.

## Out of Scope

An unrestricted public TikTok search promise, guaranteed minute-level freshness, or a custom general-purpose workflow engine.

## Further Notes

Done means budgeted discovery or its explicit unavailability is visible, and scheduling changes produce measurable collection behavior. Policy history is retained for later bias analysis.

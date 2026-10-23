# Bounded queue dispatch and scheduler writes

This slice removes avoidable queue/polling work while retaining PostgreSQL lease
fences, atomic source quotas, physical-request grouping and exploration rules.
It does not establish production pipeline throughput or source access.

## Shipped behavior

- Migration 018 adds partial indexes for priority-ordered active dispatch and
  attempt-limit cleanup. The claim and reservation statements remain unchanged.
  Existing tasks, quota balances, counters and lease versions are preserved.
- A looping worker immediately starts another bounded batch after a completely
  successful full batch. Empty/partial batches, errors and quota backpressure keep
  the configured interruptible polling wait. Every batch checks the signal event.
- Planning reads the assessment score and only the origin request it uses; it no
  longer transfers a complete unused assessment payload for every video.
- Selected member schedules and deferred inspection records use at most 500 rows
  per SQL write. The existing workspace planner transaction still owns all task
  inserts, per-entity policy details and due-time updates. No parallel scheduling
  state or publication path is introduced.

## Executed evidence

The [native dispatch samples](scale-dispatch.json) use an isolated PostgreSQL
schema with 50,000 tasks. Five complete `Catalog.claim` calls run before and after
applying migration 018 to that same queue. Each sample includes the exhaustion
cleanup, selection, lease update and transaction overhead.

| Queue | Before (median ms) | Migration 018 (median ms) |
| --- | ---: | ---: |
| All accounts ready | 385.710 | 2.008 |
| 20% higher-priority blocked, 20% future, 60% ready | 296.093 | 66.165 |

The remaining blocked-account cost is explicit: an ordered index still checks
quota readiness for blocked candidates. Indexing alone does not turn that path
into constant work. The mixed case claims only the ready account, and a separate
locked-head probe selects the next unlocked task. These are metadata-only queue
measurements on shared test hardware; there is no corresponding whole-pipeline
speedup claim.

`tests/test_dispatch_frontier.py` exercises 10,000 pending tasks together with
future work, unexpired leases, exhausted attempts, account and current-credential
blocks, another workspace and a locked highest-priority ready task. Its upgrade
scenario applies the migration twice without changing an existing task or spent
quota. Existing native contention scenarios retain cross-worker uniqueness and
strict credential budgets.

`tests/test_scheduler_bulk.py` plans 1,001 videos into 51 bounded physical requests
and checks every member's persisted reason, priority and next due time across
multiple write batches. Replanning active members defers all 1,001 without losing
their earlier policy details or changing the other workspace. Existing scheduler
scenarios retain grouping, origin-query continuation and protected exploration.

`tests/test_worker_queue_loop.py` runs the actual CLI entry point in separate
processes with deterministic collection outcomes. Full success advances without
a 60-second configured idle wait; empty, partial, error, quota and waiting cases
retain the poll interval. SIGTERM/SIGINT end the loop cleanly. The fixture replaces
collection only, so these checks establish loop behavior rather than HTTP speed.

The focused scheduler/dispatch/worker/native contention run passed 52 tests with
no skips. Ruff and mypy passed. Exact integrated CI and full native evidence are
recorded in the pull request before merge.

## Operational tradeoffs

The two indexes consume PostgreSQL storage and additional writes. They use the
existing transactional migration mechanism; on a large existing database, allow
for the index-build lock during the migration window. This deployment has no live
queue yet. A production online-index procedure requires its own verified plan.
Readiness over large blocked frontiers and history-dependent analytical work
remain separate optimization opportunities.

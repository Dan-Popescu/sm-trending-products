# Prelaunch massive-collection corrections

Status: approved by the owner on 2026-10-25 for autonomous implementation,
independent review, pull requests, CI and merging. Baseline:
`ac5ddcb9e8713e311b6772528e48b0ee8ade9b33`. This scope follows the fresh prelaunch
audit; it does not replace deployment or authorized-source acceptance.

## Durable capture recovery

A complete locally durable receipt must resume remote publication after more
than eight S3 publication failures, without resetting the real source-request
failure budget or requiring operator intervention. Existing source quota and
cooldown must not delay publication that makes no source request. Actual source
HTTP requests remain subject to all existing budgets and failure limits.

Recovery preserves the exact capture identity, original acquisition clocks and
worker generation, task/page/attempt, lease ownership and fencing, source access,
retention, revocation and immutable publication rules. Corrupt or incomplete
receipts must never trigger an unbudgeted replacement HTTP request. Retry work
and backoff remain bounded and fair. Business-error receipts and legacy delivery
recovery retain their existing semantics. Verify through native PostgreSQL,
controlled HTTP and object-store failure injection, including restart boundaries.

## Cache admission work

Ordinary cache admissions with available space must not enumerate every cached
file. Any shared accounting is reconstructible and subordinate to the existing
hard byte budget. Preserve cross-process reservations and pins, inode-aware
replacement, crash recovery, reduced budgets, legacy paths, retirement tombstones
and concurrent old/new release behavior. A process-local counter is insufficient.

Use bounded reproducible before/after fixtures with equal logical payload bytes
and budget, varying file count through 30,000. The objective is ordinary-admission
p95 below 5 ms on the declared test host; report actual results and remaining
slow paths, without converting an isolated result into a pipeline speed promise.
Retain the previously delivered feature-window cache priority.

## Pending normalization selection

Finding pending raw captures must scale with pending/retry work rather than all
retained completed history. A reconstructible indexed eligibility projection may
be introduced; the committed normalized child remains the sole authoritative
completion fact. Preserve SKIP LOCKED, parent-before-lease lock ordering, expired
owner fencing, retry fairness, invalidation/revocation/retirement and publication
guards. Support existing rows, crash recovery and writes by compatible old images.

Declare schema expansion and signed-release compatibility honestly. Do not weaken
release proofs or forge compatibility for a previously signed manifest. Verify
production claim behavior and query work against a retained-history fixture. The
objective is an empty claim below 5 ms at 500,000 completed parents on the declared
host; report actual results and any unmeasured busy catch-up limits.

## Resource evidence and delivery

Record reproducible CPU and memory measurements where feasible, separating
always-running processes, source collection/normalization and analytic refreshes.
State included processes, resource limits, duration and synthetic workload.
Configuration ceilings are not measured consumption. Do not infer a whole-VPS
capacity, live source grant, continuous-load maximum or memory/product coefficient
from partial-process or historical fixtures.

Deliver each correction as a focused independently reviewed pull request. Run
targeted regression checks and the integrated suite; merge only with passing
required CI. The owner does not require an additional human approval step.

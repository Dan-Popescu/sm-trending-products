# Hot analytical CPU modes

Status: approved by the owner on 2026-10-28. Delivery: [#122](https://github.com/Dan-Popescu/sm-trending-products/issues/122).

## Objective

Change an operating pipeline's analytical CPU ceilings without restarting
acquisition or changing source tasks, observations, sampling, quotas or leases.
Recurring infrastructure cost is the priority, while preserving responsive
exploration and hourly assessments. A lower ceiling is not itself a lower VPS
invoice or a demonstrated throughput qualification.

## Boundary

The trusted host controller exposes `standard` and `economy` profiles, a preview,
status and recovery. Only `dagster-daemon`, `dagster-webserver` and `dashboard`
CPU quotas may change. Capture slots, planners, normalizer, PostgreSQL, memory
limits, thread counts, schedules and data remain unchanged. Standard quotas come
from the existing bounded deployment. Economy halves these three ceilings,
subject to a 0.25-CPU daemon floor and 0.1-CPU web/dashboard floors, never raising
a ceiling above its standard value. No arbitrary quota or global CPU cap is
accepted by this mode.

Before reducing a Dagster role's CPU, verify that its running container and
effective future Compose configuration both explicitly enable independent
acquisition. A protected capture worker alone cannot rule out concurrent legacy
Dagster collection. Inspect only the relevant nonsecret setting. Refuse economy
with a missing or false setting; preserve increases back to standard. The timer
also checks this precondition while economy is active.

## Safety and recovery

- Use the existing host release lock. Refuse an unfinished deployment or
  processing handover before beginning a new CPU transition.
- Verify project/service labels and update exact immutable container IDs with
  `docker update --cpus`. A switch must not call Compose up/down, restart, stop,
  remove, kill, scale or recreate a service.
- Require a bounded, unambiguous running inventory and supported CPU settings.
  Check process identity, start time, restart count and unchanged protected
  resources before accepting a transition.
- Check acquisition health conservatively before lowering quotas. Retain the
  existing source deadlines: a heartbeat alone does not establish coverage and
  this feature does not issue artificial source requests to qualify itself.
  Inspect at most 1,001 originally due uncaptured obligations through the indexed
  frontier; more than 1,000 conservatively rejects economy. Future scheduled
  obligations do not consume this budget, and retry deferral cannot hide an
  original missed deadline. This sampled guard does not rescan all retained
  history or certify every completed capture.
- Recheck effective host CPU and aggregate running quotas plus reserve before
  increasing ceilings. Old capacity is not a permanent resource reservation.
  Pure reductions, including recovery to economy, remain available under pressure.
- Persist private intent, original quotas and verified container identities
  before the first Docker mutation. Verify the result before publishing the
  committed profile. Roll back a failed or uncertain transition; retain recovery
  intent if rollback cannot safely finish. On process death, reconcile that
  intent before allowing subsequent deployment mutations.
  If rolling back a standard increase would reduce CPU for legacy Dagster
  collection, complete that already authorized standard increase instead,
  subject to fresh capacity. Otherwise retain the safe partial state and intent.
  A missing analytical role after a committed transition must not block managed
  repair; preserve pins and defer that stable guard observation. Missing recorded
  roles during an unfinished transition must still block its recovery.
- Persist only allowlisted CPU settings under the controller's existing private
  writable state directory. They override inherited interpolation variables for
  subsequent managed service creation. Never copy application credentials into
  this state or rewrite the protected runtime environment.
- The existing controller timer reconciles interrupted changes and evaluates the
  economy capture guard before network access or deployment resumes. An unhealthy
  capture guard requests a capacity-checked return to standard. A blocked recovery
  remains visible and must not stop acquisition or delete pending data.

## Evidence and limits

Test preview/idempotency, unsafe values/inventories, lock contention, unfinished
handover rejection, persistence, partial Docker failures, uncertain outcomes,
controller death and resumed rollback. Use real Docker while a synthetic one-shot
source response is in flight; prove unchanged capture process/generation and
protected quotas, exactly one durable raw capture and subsequent successful work.
Verify the persisted profile survives later managed processing recreation.

The timer and trusted host controller upgrade are operational prerequisites.
Controlled tests do not establish live-source availability, arbitrary-volume
capacity or immunity to a whole-host failure. CPU quota enforcement limits
processor time over short intervals, rather than binding a process to a fixed
number of physical cores. RAM changes and dynamic thread-pool resizing are
outside this contract.

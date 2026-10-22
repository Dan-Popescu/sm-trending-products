# T13 — Operate and restore the complete pipeline on a VPS

GitHub issue: [#21](https://github.com/Dan-Popescu/sm-trending-products/issues/21). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled deployment/recovery acceptance is mapped in the
[criterion matrix](../../verification/operations-acceptance.md). Actual provider
acceptance and VPS rollout remain pending the specified machine and access.

## What to build

Provide a supported container deployment and demonstrate that collection, assessments, and frozen decisions can be operated and restored in a fresh environment.

## Acceptance criteria

- [x] The documented Docker Compose deployment produces an inspectable product assessment and forward decision from controlled evidence with all intended services.
- [x] The database and orchestration controls are private; the review surface has a documented restricted-access method and logs redact credentials.
- [x] Status distinguishes service health, stale evidence, denied access, queue age, storage pressure, and failed/quarantined batches.
- [x] An isolated restore from PostgreSQL backups and referenced object versions preserves committed observations, identity history, alerts, and forward decisions.
- [x] Restart after restore reconciles checkpoints and publication state without duplicated evidence.
- [x] Cache and compaction behavior preserve completed-manifest consistency and respect configured disk and memory limits.
- [x] The release includes operational and restore instructions; actual deployment waits for the user's specified VPS and access.

## Blocked by

T02 — Replay a completed dataset without source access; T03 — Recover interrupted collection without duplicate results; T04 — Enforce shared quotas across collectors; T07 — Discover and refresh candidates within a budget; T09 — Review demand, competition, and feasibility gaps; T11 — Measure frozen decisions prospectively.

## Specification

[Functional specification](../../specs/operations.md). Shared contracts are defined there; this ticket adds no alternative definitions.

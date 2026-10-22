# T03 — Recover interrupted collection without duplicate results

**Status:** Software implemented; native PostgreSQL concurrency acceptance pending. See the [coverage record](../../implementation/coverage.md). Not published to GitHub.

## What to build

Resume an interrupted batch and show exactly which observations were committed, retried, or quarantined, including recovery between object publication and catalog updates.

## Acceptance criteria

- [ ] A controlled crash before publication, after object write, and before checkpoint advancement can each be recovered without losing committed evidence.
- [ ] Repeated delivery of the same logical response does not create a duplicate observation; a later observation of the same video is retained.
- [ ] An expired lease is reclaimable and a stale worker cannot publish a successful completion after ownership changes.
- [ ] Checkpoints only advance over durably published data; reconciliation exposes and resolves orphaned staged objects.
- [ ] Retries and recovery states are visible to the operator and verification runs against real temporary storage services.

## Blocked by

T01 — Inspect an authorized-video observation end to end.

## Specification

[Functional specification](../../specs/collection.md). Shared contracts are defined there; this ticket adds no alternative definitions.

# Implementation handoff

The integration branch is `codex/project-planning`. All code, specifications,
tests, dependency locks, deployment configuration, and operating instructions
are included. No real credentials or collected live TikTok data are included.

## Recover the delivery

The delivery ZIP contains a readable source tree and `repository.bundle`, a
self-contained Git history. Extract the ZIP, then clone the bundle when you want
the commits and branch references:

```sh
git clone /absolute/path/to/repository.bundle sm-trending-products
cd sm-trending-products
git switch codex/project-planning
```

## Current published checkout

The project is now available in [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).
Use the normal GitHub checkout for continued work:

```sh
git clone --branch codex/project-planning https://github.com/Dan-Popescu/sm-trending-products.git
```

Original archive commits and published commits have identical trees but different
metadata/IDs, recorded in [publication](publication.md). Keep the bundle as recovery
history; do not force-push its old branch over the published branch. No main-branch
merge is implied by recovering an archive; the actual integration is tracked in
PR #1. No production deployment is implied by source publication.

## Check and operate

1. Read [coverage and release gates](coverage.md),
   [verification](../verification/implementation.md), and
   [autonomous decisions](assumptions.md).
2. Install locked dependencies and build the collector as described in
   [testing](../operations/testing.md). Use an isolated native PostgreSQL instance
   for full concurrency checks.
3. Build the image and run the isolated recovery rehearsal in
   [CI recovery](../../scripts/ci-recovery.sh) on a Docker-capable machine.
4. Configure the VPS and S3 destinations using the
   [runbook](../operations/runbook.md), then verify private access and provider
   behavior before treating it as production.
5. Supply the actual approved TikTok credentials/scopes and run `verify-live`
   separately for each permitted capability. A missing or denied source remains
   unavailable. Import eligible offer and cost evidence where the source does
   not supply it.

No-key tests exercise actual HTTP/SQL/object-storage boundaries with controlled
data. They do not validate live coverage, source permissions, sales, or profit.

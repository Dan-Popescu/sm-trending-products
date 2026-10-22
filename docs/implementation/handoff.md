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

The bundle's `origin` points to the local delivery file. To publish to the intended
repository, replace that remote after authenticating GitHub:

```sh
git remote set-url origin https://github.com/Dan-Popescu/sm-trending-products.git
git push origin codex/project-planning
```

For a still-empty remote, publish the existing local `main` initialization branch
as the PR base as well. Do not overwrite a remote branch that gained independent
work. Open a PR from the integration branch to `main`; no main-branch merge was
performed by this implementation.

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

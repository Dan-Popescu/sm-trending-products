# T14 — Establish a measured operating envelope

GitHub issue: [#22](https://github.com/Dan-Popescu/sm-trending-products/issues/22). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled native operating-envelope matrix verified: seven finite cases cover paced arrivals, quotas, cold/warm caches, compaction and actual lease-expiry recovery. Target-VPS retained-history capacity and priced provider costs remain pending. See the [measurement](../../verification/controlled-envelope.md) and [coverage record](../../implementation/coverage.md).

## What to build

Give the operator a repeatable capacity and cost report for the full supported workload so that scaling decisions follow measurements.

## Acceptance criteria

- [x] The workload declares hardware, record sizes, entity skew, source latency, request budget, retention, object sizes, concurrency, and analytical windows.
- [x] Report throughput, end-to-end freshness, queue growth, errors, peak RSS, CPU time, disk pressure, and S3 request/byte volume.
- [x] Cold-cache and warm-cache analytical runs are distinguished; compaction and object-store traffic are included.
- [x] Overload and interruption exercises demonstrate bounded buffers, controlled recovery, and reported degradation rather than silent data loss.
- [ ] An operating envelope and headroom recommendation follow the measurements; recurring cost scenarios show unit-price assumptions separately from measured usage.
- [x] Scaling triggers identify the measured bottleneck and a compatible next deployment step without claiming unlimited capacity or provisioning cloud resources.

Checked criteria apply to the declared controlled boundary. SQL creation-to-commit
latency is measured; live upstream freshness and full-stack concurrent RSS remain
unknown with explicit reasons. Fixture retention lasts for the temporary exercise.
The unpriced cost scenario separates units from missing provider prices. Production
capacity, actual retention growth and priced operating cost are not accepted by
these checks, and issue #22 remains open for that acceptance.

## Blocked by

T07 — Discover and refresh candidates within a budget; T10 — Evaluate rankings with point-in-time replay; T13 — Operate and restore the complete pipeline on a VPS.

## Specification

[Functional specification](../../specs/operations.md). Shared contracts are defined there; this ticket adds no alternative definitions.

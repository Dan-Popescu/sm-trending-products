# T14 — Establish a measured operating envelope

GitHub issue: [#22](https://github.com/Dan-Popescu/sm-trending-products/issues/22). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Benchmark software and a limited synthetic measurement implemented; production operating envelope and multiworker capacity acceptance pending. See the [coverage record](../../implementation/coverage.md).

## What to build

Give the operator a repeatable capacity and cost report for the full supported workload so that scaling decisions follow measurements.

## Acceptance criteria

- [ ] The workload declares hardware, record sizes, entity skew, source latency, request budget, retention, object sizes, concurrency, and analytical windows.
- [ ] Report throughput, end-to-end freshness, queue growth, errors, peak RSS, CPU time, disk pressure, and S3 request/byte volume.
- [ ] Cold-cache and warm-cache analytical runs are distinguished; compaction and object-store traffic are included.
- [ ] Overload and interruption exercises demonstrate bounded buffers, controlled recovery, and reported degradation rather than silent data loss.
- [ ] An operating envelope and headroom recommendation follow the measurements; recurring cost scenarios show unit-price assumptions separately from measured usage.
- [ ] Scaling triggers identify the measured bottleneck and a compatible next deployment step without claiming unlimited capacity or provisioning cloud resources.

## Blocked by

T07 — Discover and refresh candidates within a budget; T10 — Evaluate rankings with point-in-time replay; T13 — Operate and restore the complete pipeline on a VPS.

## Specification

[Functional specification](../../specs/operations.md). Shared contracts are defined there; this ticket adds no alternative definitions.

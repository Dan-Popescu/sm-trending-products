# Batched current assessment publication

Scope: ticket #114 and the batched-publication section of the
[resource-efficiency specification](../specs/resource-efficiency.md). Baseline:
`0f2500fbece79c7eb6a7a190f848ac831a2f0537`. No schema migration or source,
assessment-frequency, archive-format or analytical-formula change is introduced.

## Publication contract

`AssessmentPublisher.publish` owns one catalog transaction containing the existing
archive commit, current-row upserts and alert decisions. Pipeline retains staging
and returns the original complete body and ranking. Publication guards and the
shared retention fence remain in the archive's authoritative commit path; existing
SQL namespace triggers still guard every current/alert row. Failure in any batch
rolls back the run pointer, current rows and new alerts together. Staged immutable
objects remain recoverable through their existing lifecycle.

Current writes contain at most 200 rows and 1 MiB of encoded payload per statement.
An individually larger previously supported row is sent alone. The module sorts
references to existing rows, rather than constructing another whole-universe JSON
body. Current JSON retains its existing redaction; frozen alert payloads retain
their original existing behavior.

Existing product FK parents are locked in global product-ID order with
`FOR NO KEY UPDATE` before current upserts. This serializes overlapping new
publishers before the upsert statement snapshot, including concurrent first writes.
The lock remains compatible with ordinary FK key-share checks. The existing
`assessment_runs.as_of <= incoming cutoff` condition remains the current-row
winner rule; equal cutoffs retain last-publisher behavior. Alert advisory keys use
the existing `hashtext('alert:' || product_id)` namespace and are acquired in
globally sorted actual-key order, including collisions, through bounded calls.

Recent non-invalidated alerts are queried in bounded product batches before any
new alert JSON is serialized. Cooldown still uses the actual database clock and
the existing configured interval. A historical run rejected by the current-row
winner rule can still emit an alert when its policy and cooldown permit it; this
preexisting behavior is deliberately preserved. Alert IDs and conflict deduplication
are unchanged. Legacy publishers still use their earlier row-order protocol;
the new globally ordered publisher does not establish a new guarantee for legacy
writer overlap or whole-VPS availability.

## Native component measurement

The [reproducible results](batched-assessment-publication.json) use native
PostgreSQL with every publication guard enabled. A disposable schema contains
synthetic products and staged archive metadata. Both implementations execute the
real archive SQL commit. There are no source requests, object uploads or feature
calculations; those stages are outside this component measurement. The baseline
transaction is extracted directly from the immutable Pipeline source using AST,
rather than reimplemented by the benchmark.

Three repetitions alternate implementation order. Each first-alert run starts
empty; cooldown runs start with existing valid alerts; no-alert runs use a
threshold above every fixture score. Every measured run verifies all current
payloads and alert counts. Counts include archive and publication guards and
exclude the identical transaction timeout setters. The shared test host is not
an exclusive VPS or production latency qualification.

| Products | Alert scenario | SQL execute calls, old → new | Median elapsed, old → new | Client CPU, old → new |
| ---: | --- | ---: | ---: | ---: |
| 100 | First alerts | 407 → 13 | 205.94 → 53.31 ms | 49.68 → 15.13 ms |
| 100 | Active cooldown | 307 → 12 | 65.20 → 28.09 ms | 31.43 → 12.14 ms |
| 100 | No eligible alerts | 107 → 9 | 37.46 → 17.37 ms | 20.88 → 10.71 ms |
| 1,000 | First alerts | 4,007 → 37 | 954.03 → 229.40 ms | 403.43 → 120.68 ms |
| 1,000 | Active cooldown | 3,007 → 32 | 1,602.26 → 215.82 ms | 360.90 → 105.69 ms |
| 1,000 | No eligible alerts | 1,007 → 17 | 456.13 → 143.92 ms | 245.08 → 96.91 ms |

CPU is the benchmark client process only; PostgreSQL server CPU is not included.
The driver reports cumulative process peak RSS, not memory per product or a
whole-stack comparison. Extra encoded payload storage is bounded per batch;
the existing complete result plus small sorted references/lock keys remain
proportional to the number of returned products. No end-to-end speed multiplier
or continuous VPS consumption is inferred from these values.

Run with locked project dependencies and a dedicated native test database:

```sh
TEST_DATABASE_URL=postgresql://test-user:test-password@localhost/test-database \
PYTHONPATH=src python scripts/benchmark-assessment-publication.py \
  --products 100 1000 --repetitions 3 --output output/assessment-publication.json
```

The command creates and removes its own random schema and temporary local
directories. It does not initialize an S3 client or require source credentials.
Focused native tests cover exact complete results, batch-byte boundaries, reversed
rank order across batches, a blocked concurrent first writer, stale-run behavior,
cooldown/deduplication, invalidated alerts, null scores, empty runs and rollback.
Existing archive/source-withdrawal integration tests cover retained publication
lineage and invalidation through the same Pipeline entry point.

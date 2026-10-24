# Large source invalidation verification

The previous main revision `761b779` rejected a native closure containing 12,000
committed artifact-input associations in 0.045 seconds. The source and all 12,000
artifact states stayed committed: rejection was atomic, but removal could not
progress. That baseline is an observed limit, not a successful deletion.

The new continuation is exercised by `tests/test_large_invalidation.py` against
PostgreSQL 17 and a controlled HTTP object store. The source is a real imported
observation; the fanout consists of actual staged/input/committed SQL artifact
relations with legacy nullable descriptors. These SQL stress leaves do not claim
12,000/50,000 uploaded JSON objects or observed market products.

A first native pass removed 12,000 artifacts in 38 bounded resume batches and
2.52 seconds (2.72 seconds including request admission). After fixing the two-sided
artifact keyseek, the 50,000-artifact pass used 152 batches and 6.91 seconds
(6.99 seconds including admission). Native `EXPLAIN (ANALYZE,BUFFERS)` of first,
middle and last artifact pages shows at most 1,001 actual rows on every input
plan node, at 1.09/1.20/1.19 ms. Before the fix, the middle page scanned 26,001
publication rows despite correctly seeking its source-input index.

Two further native 12,000-dataset metadata graphs complete: a long lineage chain
in 88.18 seconds/181 batches, and a shared-object cluster in 47.72 seconds/170
batches. These contain empty-page dataset manifests, not 12,000 downloaded payload
files. The timings establish progress on these graph shapes, not a promise that
all broad namespace gates last only a few seconds. They are controlled database
measurements, not production latency or a capacity certificate.

Shared clusters of 2,500 and 5,000 datasets exercise work-based admission below the
old 10,000-row cap. Shared-object cursor first/middle/last plans inspect at most
1,001 rows. A source with only one immediate child hiding a 5,000-node shared
cluster enters durable continuation in 2.11 seconds through the atomic statement
budget. Sparse reverse evaluation lookup with 20 matches among 20,000 dependencies
uses indexed equality plus keyseek; first/middle/last fetch 20/10/1 rows at
0.031/0.027/0.017 ms. The independent pre-index probe scanned all 20,000 rows.

The tests additionally exercise every cached/published read facade, SQL writer
gates, active namespace versus the other namespace, operational visibility,
cooperative lock deadlines, rollback without lost progress, and durable gates
that remain active despite old timestamps. Late-gate race tests interpose an actual
committed journal after preflight for exploration, entity matching, scheduling,
leaf publication, collection and outcome maturation. A real CLI subprocess drains
12,000 artifact dependents and then fetches its queued controlled-source task once,
without restart; SIGTERM remains responsive. Physical retention additionally
verifies SQL feature-copy removal, local cached-window deletion, compacted-parent
restoration, a surviving alternative entity origin, and frozen outcome invalidation.

An opt-in native custom `pg_dump`/`pg_restore` probe copies a partially advanced
2,500-artifact job to a new isolated database. Initialization preserves its exact
catalog fingerprint; the restored namespace denies reads before continuation,
the opposite namespace is usable, and persisted cursors complete all retirement.
The observed native pending probe passes in 4.11 seconds. A second custom-dump
restore verifies that empty journals preserve the pre-migration eight-table
fingerprint, while creating a job makes both journal hashes mandatory; the two
restore cases and real Dagster pending-metadata regressions pass together in
8.27 seconds. Completed job tombstones also remain part of the fingerprint.
Normal immutable result compatibility continues to use the existing legacy
oracle and artifact tests.

Run in a task-owned native database:

```sh
PYTHONPATH=src pytest -q -s tests/test_large_invalidation.py
# Optional: the named container must host the task-owned TEST_DATABASE_URL.
TEST_POSTGRES_CONTAINER=your-test-postgres PYTHONPATH=src pytest -q tests/test_invalidation_restore.py
```

Advisory locks are database-scoped. Independent long-running benchmark or backup
probes should use separate task-owned databases, rather than changing the
application's fencing key to make concurrent test workloads appear independent.

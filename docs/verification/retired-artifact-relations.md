# Bounded retired-artifact relation collection

SQL 025 places cleanup progress on existing `derived_publications` tombstones.
It adds a partial index containing only deleted publications whose relationships
have not been fully collected. There is no additional queue or object identifier
mapping and the upgrade performs no object-store operation. Existing deleted
publications become eligible automatically.
The one-time atomic index build scans the existing publication table with a
ten-minute statement limit. Upgrade a large catalog in a maintenance window;
normal runtime transactions retain their existing 30-second statement limit.

`tests/test_artifact_gc.py` exercises the actual PostgreSQL schema, insertion
guards, maintenance service and HTTP object-store boundary. Its mixed-table
fixture starts with 600 outgoing edges, 600 input associations and three invalidated
evaluation associations. A single finalization pass removes exactly 1,000 rows
**across all three tables**, leaving 203 physical association rows and one pending
parent. The next drain removes those 203 rows, marks the parent collected and
reports completion. All 600 source publications, three evaluation tombstones and
artifact publication tombstones remain. A separate oversized private-call probe
also cannot remove more than 1,000 rows.

The real provenance fixture has two independently published sources, four combined
branches, eight outgoing edges and five input associations. Retiring the first
source removes 12 associations from eight deleted artifact parents. The surviving
leaf keeps its input relationship and returns the exact same public audit page.
Deleted artifacts retain their authoritative checksum/byte descriptors and
retention tombstones; object resurrection and restricted-page reads are rejected.
Retiring the surviving source then completes under a four-object atomic closure
limit, without charging old deleted ancestors against that limit.

Additional native cases cover populated pre-025 schema migration, transaction
rollback after relation deletion, concurrent parent locking with `SKIP LOCKED`,
the finalizer backend's actual shared publication fence, cooperative deadline
interruption, and a deleted artifact still linked by an active evaluation. That
inconsistent live link remains available for diagnosis, keeps the parent pending,
and produces a blocked result. Keyset continuation still collects later parents.
After the evaluation is invalidated, the next drain removes the final link.

These checks establish physical row removal and bounded resumable work, not a
production throughput or filesystem saving. PostgreSQL vacuum can reuse the dead
tuple space; ordinary SQL deletion does not immediately shrink relation files.
For a workload creating 24 million eligible relationships per month, the previous
implementation retained all 24 million rows indefinitely. Cleanup now removes
them after artifact retirement, subject to the configured maintenance budget and
measured drain capacity. Publication and object-retention tombstones still grow
with immutable artifact identities and are intentionally preserved.

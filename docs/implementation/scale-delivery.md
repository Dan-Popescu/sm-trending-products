# Approved scale and cost delivery

The user approved the complete high-impact scale/cost follow-up on 2026-10-23,
including compact published results, exact incremental calculation, smaller SQL
metadata and incremental backups. Baseline: main `2136e0485196a574a2745bc216c71d13f006f0df`.
GitHub remains the canonical tracker. Deliver in the following order, with
independent implementation/review work in parallel and exact-head CI before merge.

| Slice | Canonical issue | Status | Acceptance boundary |
| --- | --- | --- | --- |
| Dispatch and workload profile | [#57](https://github.com/Dan-Popescu/sm-trending-products/issues/57) | Merged in [PR62](https://github.com/Dan-Popescu/sm-trending-products/pull/62) | Priority-indexed claims, busy worker draining, bounded scheduler writes, one ownership resolution, separate planning/analysis and explicit capacity arithmetic. |
| Compact analytical results | [#58](https://github.com/Dan-Popescu/sm-trending-products/issues/58) | Merged in [PR63](https://github.com/Dan-Popescu/sm-trending-products/pull/63) | Immutable sharded provenance, compact new publications, paged details and unchanged legacy definitions. |
| History and maintenance | [#59](https://github.com/Dan-Popescu/sm-trending-products/issues/59) | [PR64](https://github.com/Dan-Popescu/sm-trending-products/pull/64) open; CI in progress | Compact internal references, conservative period-aware selection, resumable retention/compaction and visible backlog. |
| Exact incremental features | [#60](https://github.com/Dan-Popescu/sm-trending-products/issues/60) | Implemented; 47 native engine/caller cases covered, including independent probes; verified sessions and bounded node planning reviewed; full integration validation in progress | Changed-scope/delta updates, exact temporal invalidation and full-rebuild parity. |
| Incremental backups | [#61](https://github.com/Dan-Popescu/sm-trending-products/issues/61) | Implemented; integrated restore validation in progress | Verified base/delta recovery, legacy compatibility and a short coherent checkpoint pause. |

For #57, the [native dispatch measurement](../verification/scale-dispatch.md)
records complete claim timings and mixed-queue limits. The
[workload profile](../operations/workload-profile.md) distinguishes planning,
source quota and measured execution capacity. These measurements establish no
universal VPS capacity or provider cost.

Preserve immutable raw history, availability time, reviewed ownership,
real/synthetic isolation, atomic quotas, lease fencing, deletion invalidation and
the meaning and hashes of old stored snapshots. Projections are disposable
derivations, not another authoritative evidence store. Actual machine/provider
acceptance and real source permissions remain separate existing release gates.

For #58, [ADR0008](../adr/0008-compact-analytical-provenance.md) records the
versioned publication and read contracts. The [complete publication cost
measurement](../verification/compact-results.md) includes every new provenance
object and SQL dependency edge, rather than treating out-of-line bytes as free.

For #60, start with [ADR0009](../adr/0009-exact-feature-projections.md) and
[cache operations](../operations/cache.md). Compact feature rows and local rolling
windows are disposable caches; their contexts, source frontiers, provenance and
clock boundaries must remain consistent with the full analytical oracle. The
[bounded node-plan measurement](../verification/provenance-node-plan.json) records
transaction reduction separately from object bytes and whole-pipeline latency.
The cache guide also describes verified read sessions, their pin/byte limits and
fallback behavior. Revalidate these boundaries after changing either caller.

For #61, use [backup and restore operations](../operations/backup-and-restore.md)
for the recovery contract. The incremental implementation is ready for integration;
its restore validation and exact-head CI must complete before merging. Preserve
legacy recovery compatibility and independently verify the complete required
object set, base-chain integrity and writer-resumption behavior.

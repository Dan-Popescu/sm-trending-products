# Approved scale and cost delivery

The user approved the complete high-impact scale/cost follow-up on 2026-10-23,
including compact published results, exact incremental calculation, smaller SQL
metadata and incremental backups. Baseline: main `2136e0485196a574a2745bc216c71d13f006f0df`.
GitHub remains the canonical tracker. Deliver in the following order, with
independent implementation/review work in parallel and exact-head CI before merge.

| Slice | Canonical issue | Status | Acceptance boundary |
| --- | --- | --- | --- |
| Dispatch and workload profile | [#57](https://github.com/Dan-Popescu/sm-trending-products/issues/57) | Merged in [PR62](https://github.com/Dan-Popescu/sm-trending-products/pull/62) | Priority-indexed claims, busy worker draining, bounded scheduler writes, one ownership resolution, separate planning/analysis and explicit capacity arithmetic. |
| Compact analytical results | [#58](https://github.com/Dan-Popescu/sm-trending-products/issues/58) | Implemented; exact-head integration and pull request in progress | Immutable sharded provenance, compact new publications, paged details and unchanged legacy definitions. |
| History and maintenance | [#59](https://github.com/Dan-Popescu/sm-trending-products/issues/59) | Independent implementation and review in progress | Compact internal references, conservative period-aware selection, resumable retention/compaction and visible backlog. |
| Exact incremental features | [#60](https://github.com/Dan-Popescu/sm-trending-products/issues/60) | Independent implementation and review in progress | Changed-scope/delta updates, exact temporal invalidation and full-rebuild parity. |
| Incremental backups | [#61](https://github.com/Dan-Popescu/sm-trending-products/issues/61) | Independent implementation and review in progress | Verified base/delta recovery, legacy compatibility and a short coherent checkpoint pause. |

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

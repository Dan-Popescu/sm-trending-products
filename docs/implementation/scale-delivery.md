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
| History and maintenance | [#59](https://github.com/Dan-Popescu/sm-trending-products/issues/59) | Merged in [PR64](https://github.com/Dan-Popescu/sm-trending-products/pull/64) | Compact internal references, conservative period-aware selection, resumable retention/compaction and visible backlog. |
| Exact incremental features | [#60](https://github.com/Dan-Popescu/sm-trending-products/issues/60) | Merged in [PR65](https://github.com/Dan-Popescu/sm-trending-products/pull/65) | Changed-scope/delta updates, exact temporal invalidation and full-rebuild parity. |
| Incremental backups | [#61](https://github.com/Dan-Popescu/sm-trending-products/issues/61) | Merged in [PR66](https://github.com/Dan-Popescu/sm-trending-products/pull/66) | Verified base/delta recovery, legacy compatibility and a short coherent checkpoint pause. |

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
its restore validation and complete CI passed in PR66. Preserve
legacy recovery compatibility and independently verify the complete required
object set, base-chain integrity and writer-resumption behavior.

## Approved massive-collection follow-up

On 2026-10-24 the user approved all four further priorities below, including
independent review, PR creation, CI monitoring and autonomous merge. Deliver in
this order; implementation and evidence gathering may proceed in parallel.
GitHub issue and PR state is authoritative for current progress.

| Priority | Canonical issue | Acceptance boundary |
| --- | --- | --- |
| 1 | [#67](https://github.com/Dan-Popescu/sm-trending-products/issues/67) | Reproducible retained-history measurements at thousands of products, streamed seeding, explicit process/disk/deadline guards, cold/warm/dirty/evicted paths and complete measured-versus-assumed accounting. |
| 2 | [#68](https://github.com/Dan-Popescu/sm-trending-products/issues/68) | Measured changed-product CPU reduction through the real analytical callers, with exact immutable-oracle parity and bounded fallback. |
| 3 | [#69](https://github.com/Dan-Popescu/sm-trending-products/issues/69) | Safe bounded, recoverable large invalidation beyond the 10,000-element atomic cap; no readable retired descendants or publication races. |
| 4 | [#70](https://github.com/Dan-Popescu/sm-trending-products/issues/70) | Dated official VPS/object-store unit prices and a reproducible cost model that distinguishes measurements, assumptions and unknown costs. |

These tasks need neither VPS deployment nor real TikTok credentials. Controlled
measurements establish only their declared local hardware/workload boundary.
Actual target-VPS sizing, provider behavior and source grants remain separate
acceptance gates under T14/T15. Source-derived data and exact numerical/temporal
meaning must not change to make a load result look better. A stopped large case
must retain a partial report with the requested and completed work distinguished.

Code delivery for this follow-up is recorded in [PR72](https://github.com/Dan-Popescu/sm-trending-products/pull/72) (retained harness and historical entity writes), [PR73](https://github.com/Dan-Popescu/sm-trending-products/pull/73) (exact analytical CPU), [PR74](https://github.com/Dan-Popescu/sm-trending-products/pull/74) (large invalidation), and [PR76](https://github.com/Dan-Popescu/sm-trending-products/pull/76) (dated cost model). The measured backup checkpoint follow-up [#71](https://github.com/Dan-Popescu/sm-trending-products/issues/71) shipped in [PR75](https://github.com/Dan-Popescu/sm-trending-products/pull/75).

Cache-pressure measurements also identified [#77](https://github.com/Dan-Popescu/sm-trending-products/issues/77), implemented by [PR78](https://github.com/Dan-Popescu/sm-trending-products/pull/78). Consult its [same-budget evidence](../verification/cache-window-priority.md) before repeating the older different-budget comparison. Large-case acceptance remains a separate [#67](https://github.com/Dan-Popescu/sm-trending-products/issues/67) deliverable; the [retained evidence](../verification/retained/README.md) explicitly labels partial cases and source revisions. GitHub remains authoritative for open/merged state.

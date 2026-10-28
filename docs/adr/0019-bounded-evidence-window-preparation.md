# Prepare complete Evidence through one bounded module

Status: adopted under the owner's architecture-refactor authorization on 2026-10-28.
Delivery: #144. Extends ADR0009 and preserves ADR0003, ADR0008, ADR0010 and ADR0018.

## Decision

`EvidenceWindows.prepare` owns feature hits, adaptive raw-window reads, eligibility,
the existing unnormalized assessor, complete input provenance, and both guarded
feature/window writes. `AssessmentService` builds globally resolved product scopes
and normalizes the complete cohort once. Published and frozen callers retain their
interfaces. A concrete preparation operation hides source frontiers, coverage
identifiers and audit-root selection from application callers; no evaluator
callback or caller-managed load/finalize protocol is introduced.

Before any raw-window payload read, admit required object count and encoded
metadata bytes, including product scopes, entity keys, dataset headers and
object/product membership overhead. SQL selects individual indexed objects with
a count sentinel, computes admission totals, and only aggregates descriptors when
the selection fits. A single dataset no longer bypasses admission through an
unbounded per-dataset descriptor aggregate. Temporal predicates remain shared
with the existing manifest reader, including conservative legacy bounds.

Optional old window state is inspected through scalar cardinality/size headers
before transferring coverage arrays or descriptors. Required metadata has priority.
An oversized optional state or old-source collection declines reuse and takes the
admitted full-read path. New local-window writes also decline oversized coverage.
Metadata and row failures deterministically split product partitions; an oversized
singleton raises a typed error. No partial assessment is published as complete.

Source selection, generation frontiers, optional coverage and payload reads use
one repeatable-read metadata snapshot protected against retirement. PostgreSQL
would establish that snapshot before waiting for a transaction advisory lock.
Therefore the catalog first acquires the existing shared retention key as a session
lock, commits that admission transaction, and only then starts the read-only
snapshot on the same checked-out connection. Cleanup always unlocks before pool
return; a cleanup failure closes the connection. This uses one connection rather
than nesting pool acquisitions. Source publication remains concurrent, and cache
writes retain their authoritative generation recheck after reading.

Validate captured analytical identity even when every feature is a cache hit.
All excluded raw facts remain in ordinary publication lineage and local-window
retirement coverage. Formats, fingerprints, hashes, schemas and ranking formulas
remain unchanged.

## Tradeoffs and limits

Defaults are 10,000 selected source objects and 16 MiB of admitted encoded metadata
plus conservative representation charges per partition. These are configurable
application admission limits, not measured production capacity or a Python/native
RSS limit. PostgreSQL may inspect and serialize candidates before returning its
scalar refusal; statement/lock timeouts remain necessary. Holding the retention
fence through payload reads continues to delay retirement for that partition.

Shared source parsing is retained within each admitted partition; splitting can
repeat reads across partitions. Product identities, cohort summaries and global
normalization still grow with the product universe. Other History readers retain
their existing contracts. This change does not introduce distributed execution,
bound every pipeline operation, or make an oversized single product streamable.

Native behavioral regressions cover published/frozen singleton refusal before
payload traffic, aggregate bytes, one oversized descriptor and dataset, optional
legacy coverage, exact split/global-rank parity, partial-publication refusal and a
retirement that commits while the reader waits. Existing native cache, source
correction, expiry, provenance and historical-oracle regressions remain required.

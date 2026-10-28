# Bounded Evidence windows

Status: owner approved on 2026-10-28. Delivery: #144.
Baseline: main `a11ca80011c570d1c619556d01218e30f857ae9d`.

## Scope and ownership

Deepen the existing assessment preparation path. One concrete Evidence window
module owns feature hits, admitted raw-window reads, all-input provenance and
generation-fenced optional feature/window persistence. AssessmentService retains
global ownership scope construction and complete-universe normalization. Published
and frozen callers keep their current interfaces and meanings.

Use a concrete batch preparation operation rather than an evaluator callback or
a public load/finalize protocol. The implementation uses existing Catalog,
History, Provenance and FeatureProjection behavior. It must not move source
frontiers, coverage identifiers or freely selected audit roots into caller code.

## Bounded selection

Admit selected object count and encoded metadata bytes before transferring
descriptor collections or reading source/local-window payloads. SQL selection
must operate on individual normalized objects; a dataset-count page containing an
unbounded descriptor aggregate is insufficient. Include object/product membership
and bounded representation overhead in admission. Inspect optional previous
coverage cardinality and size before transferring its arrays. Oversized optional
state declines reuse and takes the admitted full-read path.

Use one coherent guarded metadata snapshot for selection and optional-state
inspection. Required metadata or observation-budget failures split a product
partition deterministically. A single oversized product fails with a typed
resource error. Never publish partial evidence as complete or hide a budget
failure behind an unrestricted fallback. Limits describe application admission;
they do not promise constant SQL planning time, native-query RSS or latency.

## Invariants and acceptance

- Preserve source order, duplicate conflicts and oldest-delivery provenance.
- Preserve raw-window filter independence and lineage for every excluded raw fact.
- Preserve source-generation, retention, staged local ownership and tombstone guards.
- Keep existing physical formats, cache fingerprints, historical hashes and schemas.
- Keep shared-file parsing when a partition fits, including the existing 100-product
  fixture. Splitting may repeat shared work across separately admitted partitions.
- Validate analytical identity for both cache hits and rebuilt results.
- Exercise published/frozen interfaces, many small objects, one large dataset,
  oversized descriptors and optional previous coverage, split versus singleton
  refusal, corruption/eviction, corrections, retirement and historical contracts.

Run focused native PostgreSQL/HTTP Moto regressions, formatting/lint/types, the
complete suite and actual-image CI. Review the exact revision on Standards and
Spec before autonomous merge. ADR0003, 0005, 0007, 0008, 0009, 0010 and 0018 remain
authoritative. No production rollout or new execution infrastructure is included.

The follow-up architectural assessment distinguishes existing frozen context and
telemetry from any genuinely useful additional run diagnostics. It does not
change ranking or add a central decision engine under an ambiguous dictation word.

# Analytical execution diagnostics

Status: owner approved on 2026-10-28. Delivery: #156.
Baseline: main `3202ba4a6ea242816b61e0af804c6a6c97a07d68`
(includes #155 and #157, integrated during implementation).

## Contract

Record one operational attempt for each public assessment and frozen decision,
including unsuccessful attempts. Identical analytical results may have multiple
attempts; attempt identifiers and timing never enter analytical payloads or hashes.
Preserve existing cumulative storage and analysis counters. Attribute work events
to the active execution and History instance, including concurrent executions;
do not subtract shared cumulative counters to estimate per-run work.

Capture elapsed time and inclusive summed phase durations, storage request/byte
counters, feature hit/miss/window modes, admitted metadata totals and partition
peaks, source/local rows decoded, source objects read, adaptive splits, and bounded
cache/fallback reasons. Failed reads and splits count work actually attempted.
Do not call decoded rows unique input facts or sum nested durations as total time.

Persist a running marker before work and completion on success or an exception.
An implicit assessment cutoff stays unknown until its existing retention guard
completes; instrumentation must preserve that clock-selection order.
Freeze live-window guard decisions must be captured at their original point,
before diagnostic persistence can delay them; rejected attempts are still recorded.
Store only a bounded error category/type, never exception messages, observations,
product/entity lists, SQL, credentials or tracebacks. Process termination can leave
a running marker with incomplete counters; do not invent a completed failure or
new recovery/cancellation machinery. Diagnostic persistence is best effort and
cannot change a successful result or mask the original analytical exception.

Retain at most a configured 1–10,000 attempts per namespace (default 1,000),
serialized counters no larger than 32 KiB, and fixed-size scalar headers. Serialize
admission/pruning so parallel starts and finishes preserve that cap. Old running
attempts are disposable too. Diagnostics do not participate in source-retirement
closure or acquire evidence locks.

## Inspection and acceptance

Expose namespace-scoped bounded list/detail through Pipeline and a noninteractive
JSON CLI with examples and immediate invalid-limit errors. Return no other
namespace's attempt on a selected lookup. Diagnostics must be discoverable without
expanding analytical provenance or reading evidence payloads.

Use the already approved public Pipeline/CLI seams, native PostgreSQL and HTTP
Moto. Verify cold, unchanged, changed, split and failed runs; exact existing result
identifiers; work reconciliation; concurrent/nested context isolation; failed
diagnostic writes; bounded retention, namespace isolation and CLI behavior.
Run formatting/lint/types, the full suite once at the final revision, independent
Standards/Spec review and actual-image CI before autonomous merge.

The existing frozen context remains authoritative. A durable source revision for
future distributed/restartable partitions is deferred until that execution model
is selected. No ranking change, per-observation event stream, scheduler, automatic
cancellation or production deployment is included. ADR0008/0009/0019 remain
authoritative for analytical compatibility, projection state and source windows.

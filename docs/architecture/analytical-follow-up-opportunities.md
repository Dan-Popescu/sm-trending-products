# Analytical architecture follow-up

Assessment date: 2026-10-28, against main
`a11ca80011c570d1c619556d01218e30f857ae9d` and delivery #144.
This assessment records the original proposals and their delivery status below.

## Recommended next measurement: one diagnostic record per analytical run

Implemented in #156 on the latest main, including the subsequent research-module integrations. Public assessments and
frozen decisions now have bounded, independently attributed operational attempts.
See the [operator guide](../operations/analytical-run-diagnostics.md) and
[ADR0020](../adr/0020-operational-analytical-run-diagnostics.md) for semantics,
validation boundaries and remaining limits. The rationale below is retained.

There is useful telemetry already: `History.metrics` tracks storage requests and
bytes, `History.analysis_metrics` tracks projection and window reuse, and the
benchmark records phase duration, CPU and counter deltas. The supervised envelope
experiment already has workload/time limits. These mechanisms should be extended
rather than replaced by an execution framework.

The remaining gap is attribution during ordinary analytical runs. `flush_usage`
persists storage counters per History instance, excluding analysis counters; it
does not describe the complete work or failure reason of one assessment/freeze.
A bounded diagnostic record could capture admitted metadata count/bytes, decoded
rows, splits, fallback reasons, and selection/read/calculation/normalization/
publication timings, including failed runs. Keep it outside content-addressed
analytical payloads so operational timing does not change historical result hashes.

Expected value is better evidence for choosing compaction, caching, partition size
or parallelism, and detecting repeated fallback before increasing concurrency.
Cost is instrumentation, a bounded operational lifecycle and precise definitions
for nested sessions and concurrent runs. Start with one representative cold,
unchanged, changed, split and failed run; verify diagnostics reconcile with existing
counters. Do not add per-observation events, automatic cancellation or a new
scheduler without a measured need. This is the most useful additional proposal,
with moderate implementation cost; its throughput benefit remains unmeasured.

## Preserve the existing frozen context; revisit revisions with parallel workers

`AssessmentIdentity` already captures ownership with cutoff, namespace and
confidence validation. `AssessmentEligibility` captures capability facts and owns
filtering and cache identity. Freeze stores policy, configuration, bindings,
families, capabilities and evidence; the snapshot is detached and hashed.
Evaluation persistence checks parent/root/lease state. Capture scheduling already
has atomic cadence and admission ownership.

A new central context object or decision engine would mostly repackage these
contracts today. Delivery #144 closes a smaller real gap: cached assessments now
validate their identity as rebuilt assessments already did. Its coherent window
snapshot is per admitted partition, not a durable run-wide distributed snapshot.

Before independently executing or restarting analytical partitions across workers,
decide whether they must share one durable source revision and captured metadata
revision. `_evaluation_inputs` currently obtains its inputs through several reads;
immutable arguments alone do not establish a shared database revision. A future
durable run manifest could solve that concrete requirement, but needs explicit
snapshot lifetime, retention and replay semantics. Defer it until that execution
model is selected. Introducing it now would add lifecycle costs without a current
consumer.

## Other opportunities from the reassessment

Transcript visibility/summary hydration can be improved independently if its
measured duplicate work matters. Acquisition failure settlement depends on how
long the legacy collector remains. A run-level cohort reducer becomes worthwhile
when measurements show global retained summaries or normalization dominate.
None requires a speculative central intelligence layer; each should retain one
authoritative contract and enter delivery with a measurable acceptance boundary.

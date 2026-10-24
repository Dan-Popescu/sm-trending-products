# Bound the work behind exact changed-product assessment

Status: adopted for massive-collection ticket #68 under the user's autonomous
implementation authorization. Baseline: main `761b7796f77d83a7c10cf818042c01e57cb5e999`.

## Decision

Keep `assess` and the shared `AssessmentService` interface unchanged. Build one
ephemeral view of each already ordered eligible product window, sharing its
latest observations across demand, competition and feasibility. Resolve metric
revisions once per entity and summarize their reference IDs, correction count,
comparable interval count and positive interval count. Compute the complete
counter arithmetic only for the latest two points needed by attention and the
latest point needed by demand. Polars continues to normalize the current cohort.
The generic private timeline adapter delegates to the same kernel when complete
interval inspection is needed; there is no second counter formula implementation.

This is exact full-window computation with less allocation and repeated work,
not a claim that every formula now updates in constant time. All observations,
including excluded facts, remain in the raw rolling window and audit scope.
There is no new persistent aggregate or alternate source of truth. Corrections,
revisions, period/unit/precision changes, stale evidence, backdated imports,
rolling expiry and ownership changes retain their existing meanings.

Preserve the old millisecond truncation and reciprocal-multiplication rounding
before growth division, including submillisecond intervals and large counters.
Timestamp ordering uses UTC, while observation timestamps retain their original
offsets for displayed price histories. Counter summaries count corrections even
when the latest entity sample is stale, and only usable video timelines
contribute persistence and evidence references.

Retain the compressed columnar raw-window format and its context version. Keep canonical nested JSON encoding unchanged, including key ordering and strict
non-finite rejection, so compressed working-set bytes remain identical. Decode
scalar columns and existing nested JSON values in
one Pydantic JSON validation pass instead of three Python JSON parses followed by
model validation. Scalars always pass through a JSON encoder; raw nested blobs
are already authenticated cache bytes and model validation still rejects malformed
values. Existing cache entries remain readable without a full-history rebuild.
Admission limits, checksum verification, local retirement outbox, source
membership checks and authoritative publication fencing remain unchanged.

## Evidence and tradeoffs

The independent pre-change module produces golden complete-assessment hashes for
500 deterministic cases with revisions, metric aliases, bounded/estimated counts,
period and unit changes, UTC offsets, millisecond boundaries and counters above
2^53 and 2^100. Existing native tests exercise real published/frozen callers,
corruption, eviction, source retirement and rolling expiry. A native price-history
round trip checks original offsets and whole analytical output without any
source-file read. A legacy columnar entry is read without rebuilding source data.

A whole-observation JSON cache with typed filter columns was evaluated and rejected:
it encoded faster but enlarged the realistic rolling state by approximately 16%,
reducing the shared cache margin. The selected columnar format and canonical writer keep those encoded bytes
identical to the previous layout. Native unsorted JSON serialization was also
rejected: although current normalized-source dictionaries arrive canonically
ordered, reordered equivalent attributes can otherwise greatly enlarge a cache
entry. Preserving writer behavior avoids making that compression property an
additional interface requirement. Performance claims must report component work,
complete pipeline behavior and competing cache bytes separately. The changed
path still scans its retained window and may remain limited by SQL or immutable
provenance publication; this decision does not establish a production capacity.

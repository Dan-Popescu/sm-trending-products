# Keep analytical summaries separate from immutable paged audit details

Status: adopted under the user's authorized scale and cost improvements.

## Context

A product's assessment previously repeated every observation identifier in both
its row and attention evidence. Forward decisions repeated them again at universe
scope. Retaining many refreshes of a long lookback therefore made analytical
JSON, PostgreSQL writes, serialization and live Python results grow with all
window observations, even when most history was unchanged. Partitioning the
history read alone did not bound the result retained across those partitions.

## Decision

New persisted assessment, forward, outcome and backtest results use schema `2`.
Product summaries retain metric values, uncertainty, coverage, identity and
reasons. Observation reference vectors become descriptors of immutable trees:
`{format: "sharded-provenance-v1", root: {key, sha256, bytes, count}, count}`.
Leaves contain at most 4,096 identifiers and every node is at most 256,000 JSON
bytes. Observed-day buckets isolate refresh changes; oversized buckets split by
identifier hash. Fixed fanout of 16 bounds tree nodes. The identifier is not
assumed to be a hexadecimal hash. Equivalent source scopes share content-addressed
objects across analytical results, including across process restarts.

All loaded raw inputs acquire provenance, including rows excluded by metrics,
market, capabilities, and incomplete corrections that censor an outcome. This
input provenance is separate from the displayed evidence that establishes a
measured result. Per-product scopes combine without materializing their leaves.
The existing globally resolved ownership context partitions products once per
cutoff, so an owner outside a requested partition still prevents false ownership.

Backtests persist decision and report headers in `sharded-records-v1` trees.
Large collections such as universes, bindings, identity decisions, financial
ledger declarations and outcomes are themselves record trees. Source positions
are retained for reproducible audit of the immutable snapshot/report hashes.
The process retains only bounded product evidence and compact comparison inputs
between dates. Individual records and reference identifiers must fit the node
byte budget; unsupported oversized records fail explicitly rather than creating
an unbounded object.

PostgreSQL's existing `derived_publications` staged, committed, invalidated and
deleted states remain authoritative. SQL019 adds bounded leaf input dependencies,
immutable parent-to-child artifact edges, and evaluation-to-root associations.
It does not copy all transitive raw dependencies onto every analytical result.
New derived objects record authoritative `object_sha256` and `object_bytes` before
upload, allowing backup inventory verification without a bucket listing.

The leaf/edge registration happens before immutable upload. A second fenced
check commits the publication after upload; no provenance-node upload holds a
SQL transaction across the network. Parents may acquire only committed children
while staged. Final analytical attachment and evaluation association recheck root
state under the shared retention fence. Reverse artifact closure invalidates all
ancestors on source removal, so checking the small exposed root set suffices for
later reads. Dataset removal closes over lineage and shared-object deliveries;
compacted/replayed descendants cannot keep removed source facts readable.
Policy retention queues every affected artifact through the existing deletion
outbox. Source invalidation rejects and rolls back a closure exceeding 10,000
datasets or artifacts; the bounded maintenance drain also limits its atomic
closure. Sharding does not imply unlimited removal fanout. A blocked single-root
closure requires a deliberately designed continuation protocol rather than a
partial invalidation that could leave readable descendants. The bounded local publication memo is a performance hint; final publish
and detail reads still check authoritative root state.

## Read contract and compatibility

`Pipeline.analytical_page(kind, identifier, synthetic, path=..., limit=100,
offset=0)` returns at most 200 references or records and the next offset.
`Pipeline.analytical_record(..., record_id, path=...)` follows only the selected
record's hash route. Paths may enter a record collection by its string record ID;
for example `("snapshots", snapshot_id, "universe", product_id, "evidence_refs")`.
Each page validates immutable object checksums and holds a shared retention fence
through its reads. No production API expands an entire provenance universe.
Workspace selection is checked before resolving a path. Existing selected-item
JSON remains readable through the legacy detail contract.

Existing stored schema `1` snapshots, configurations and hashes are unchanged.
The pure raw freeze/evaluation functions continue to produce and evaluate their
legacy representation by default. Only persisted production paths opt into the
new representation; exact legacy golden fixtures verify attention and financial
maturation. A compact snapshot requires its authoritative provenance guard and
partitioned evidence loader before maturation.

## Tradeoffs

This reduces duplicated analytical wire/SQL bytes and globally retained reference
lists. It adds immutable object PUTs, small dependency rows and selected-page
reads. Initial publication still enumerates all references in each bounded
product partition; provenance alone is not an incremental feature engine.
Unchanged roots need no new provenance PUTs, while one refresh rewrites its day
leaf and a small path. Actual object and SQL costs must include the entire tree,
not only the compact result header. Small day buckets favor append cost over the
lowest possible initial object count. Provider latency and request pricing can
therefore matter more than stored bytes for small workloads.

The artifact lifecycle and reverse closure also support independently disposable
feature projections; a feature cache must reference the input root that covers
all facts stored locally. ADR0009 owns projection invalidation and reuse rules.

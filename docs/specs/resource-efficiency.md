# Resource efficiency without loss of evidence

Approved by the user on 2026-10-25. Reduce compute, resident memory and stored
bytes without exchanging one material resource regression for a comparable
benefit elsewhere. Preserve exact source facts, historical availability,
assessment formulas, rank order, frozen decisions, source withdrawal, recovery
and concurrent publication safety. The user subsequently clarified that the
product goal is multi-day ecommerce trends, not intraday trading. Change the
default assessment interval to 60 minutes while preserving collection cadence,
formulas, lookback and separately scheduled prospective decisions. Keep an
explicit 15-minute override supported and explain the accepted timing tradeoff.

## Shared observation decoding and memory

Reuse the existing bounded columnar-window decoding optimization for normalized
history where appropriate. Keep exact model validation, unknown attributes,
missingness, large integer bounds, Unicode, original timestamp offsets and source
IDs. Avoid repeated JSON-to-Python-to-model conversion and whole-result temporary
copies. Keep existing row limits and source/projection invalidation authoritative.
Measure the actual decode/encode paths, process peak memory and CPU separately;
do not call a fixture's total RSS the VPS's continuous memory use.

## Batched current assessment publication

Publish current rows and alert decisions in bounded SQL batches instead of one
round trip per product. Keep one authoritative transaction with archive commit,
latest-result ordering, namespace/retention fences, exact visible JSON and alert
cooldown/concurrency behavior. A small private module should hide batching and
locking from Pipeline. No second registry and no speculative database schema.
Validate stale/concurrent runs, rollback, source withdrawal, alert deduplication,
large product batches and complete returned results. Measure SQL operations and
publication CPU/elapsed time rather than extrapolating whole-pipeline speedups.

## Bounded native Parquet read copies

Improve bounded compaction encoding and normalized reads using native Parquet
timestamps and suitable numeric fields when measured benefits support it.
Preserve arbitrary-size integer counters (including values above 2^100), all
measurement semantics and nullable/unknown fields, original timezone offsets,
raw evidence and exact reconstructed observations. Retain legacy readers.

Source capture bytes and identities stay unchanged. Recorded staged format/layout,
not current defaults, must govern every retry across upgrades. Do not remove originals
or voluntary history to obtain savings. Report bytes, full/selective reads,
encoding and decoding CPU, peak RSS and actual tradeoffs. Reject a candidate
whose measured regression outweighs its gain; retain a beneficial narrower slice.
The first native-column trials regressed bytes, encoding and reads. The selected
narrow slice keeps the exact legacy physical bytes and bounds compaction encoder
allocations, requiring no schema gate. A future native format needs its own
compatible reader handover and version gate after representative validation.

## Workload-aware analytical archive

Improve archive writes when changed products are scattered across existing hash
leaves. Evaluate a bounded compressed alternative/adaptive representation before
selecting it. Preserve logical schema-2 body, run ID, ordered pages of at most
200 returned products, bounded decoded nodes, legacy reads, authoritative
physical checksums, run-pointer winners and existing dependency lifecycle.
Do not select a whole-universe decoder merely to reduce PUTs; keep page and memory
bounds. New reader-dependent writes require SQL 040 and compatible
handover. Root owns signed compatibility metadata and rollout documentation.

Measure clock-only, scattered 5% updates and broad updates, including every new
node/header, PUTs, bytes, CPU/RSS and cold page reads. Include comparison with
simple gzip and the current shared representation. Do not claim universal savings
or provider cost without request/storage prices. Select only candidates with a
defensible benefit and no comparable serious regression. Preserve old objects.

## Delivery and evidence

Use separate worktrees for independent scopes. Independently review Standards
and Spec on immutable revisions, run relevant native tests and the integrated
suite, then open focused PRs, monitor their attached checks and merge autonomously
under AGENTS.md. This work does not deploy infrastructure, configure S3 cold
tiers, grant source access, change real retention policies or reduce collection
frequency. The hourly-cadence comparison must distinguish retained observations,
live alert latency, missed transient rankings and replay versus actual decisions.

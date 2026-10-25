# Historical results and storage

The complete ranking saved at a decision time is an immutable record. It does
not need periodic recomputation. A new current assessment can differ because
measurements, age, freshness, product links, product membership or the assessment
policy changed. Global percentile normalization can change every product's score
after only one product changes. A historical replay with different rules is a
separate analysis, not a replacement for the frozen original.

The dashboard reads current SQL assessments. Forward evaluation uses frozen
evaluation decisions and later source observations. Neither normally needs to
download every historical ranking snapshot. Backtests and future ML can still
need old source observations. There are no production access logs yet proving
how rarely any particular class of historical data will be read.

## Assessment schedule

`ASSESSMENT_SCHEDULE_MINUTES=60` means up to 24 scheduled analytical runs per
day. The default is in `PlanningSettings`, `.env.example` and Compose;
the Dagster assessment schedule builds its cron from that setting. Values must be
positive divisors of 60. Collection planning has its own five-minute schedule;
it does not require a ranking after every capture.

The initial implementation used a fixed `*/15` collection/analysis schedule
(commit `e2039c45`). Commit `abd73c007b6d7a8b91e8a56cac7b3bd5f7187b29`
separated collection and analysis and made the intervals configurable. This was
an engineering default, not a measured latency optimum or product requirement.
The original lossless-storage delivery left it unchanged. The later
[hourly-default decision](assessment-cadence.md), approved for multi-day commerce
decisions in [#117](https://github.com/Dan-Popescu/sm-trending-products/issues/117),
reduces nominal assessment triggers from 96 to 24 per day. It preserves collection,
scoring rules, lookbacks and the independent six-hour evaluation schedules.

An explicit `ASSESSMENT_SCHEDULE_MINUTES=15` remains available for faster alerts
with fresh measurements. An existing explicit deployment override remains in
effect; adopting the hourly default requires changing or removing that override
and reloading the analytics code-server deployment. For products refreshed only
four times a day, a 15-minute schedule may repeat substantial work. A
future selective schedule should account for source changes **and** clock-based
age/freshness/window transitions, changes to product membership or rules, plus a
periodic safety run. Skipping solely because no source generation changed would
freeze time-dependent results incorrectly. Measure analytical duration, freshness
and alert requirements before choosing the next interval.

## Preservation and cold storage

New assessments on SQL 038 use gzip-compressed shared archives. The logical
schema-2 result, run ID, rank order, clocks and scores remain exact. Stable
product parts share immutable objects; changing rank/age/normalized values use
separate blocks. The archive module reconstructs complete snapshots explicitly
and returns selected rank pages of at most 200 products. Reads follow selected
hash-leaf paths; each stable leaf holds at most 100 records and 256,000 decoded
bytes. Leaves can contain unrequested products, and a page can touch all stable
leaves of a small universe. This bounds individual reads and avoids full result
reconstruction; it does not promise exactly 200 decoded records. See the
[measured storage and request tradeoffs](../verification/assessment-archive.md).
Old plain JSON snapshots remain readable. Existing objects are not rewritten.
`assessment_runs.object_key` is the authoritative pointer; a logical run ID is
not the checksum of compressed bytes. Backup and verification use the physical
SHA256 and byte size registered in the existing publication catalog.

All source dependencies remain attached to the same authoritative graph. A
withdrawal invalidates affected archives and shared parts through the existing
retirement process. Compression and sharing do not grant an independent right
to retain source information. Follow the
[reader handover procedure](private-release-controller.md#additive-capture-schema-expansions)
before applying SQL 038 on an existing installation.

Preserve useful history; this delivery does not introduce voluntary age deletion
or an S3 lifecycle rule. Source-specific expiry requirements and withdrawals
remain authoritative. A 30-day analytical lookback limits one calculation's
inputs; it does not mean older data should be deleted.
The [source retention procedure](source-retention.md) explains explicitly
verified unlimited policies and the distinction from an unknown source policy.

Cheaper S3 storage with slower retrieval is a future option for genuinely inactive
archives. Old shared parts may still be referenced by a current or pending
result: object creation age alone cannot determine eligibility. Any transition
policy must check live references and retain the full reconstructible dependency
closure.

`verify` currently downloads committed source objects and valid historical
ranking roots. A fresh full object backup downloads the required remote objects.
For new shared archives, verification also checks reachable committed parts.
Those descriptors stream in batches of 256 from a held PostgreSQL cursor, so
Python does not retain the full shared inventory. PostgreSQL may use temporary
disk to materialize that inventory; its source snapshot ends before remote GETs.
Incremental backups can reuse verified unchanged bytes from a local base.
Restoring a complete local backup reads those copies and uploads them to the
destination; it does not require access to the original bucket. Before using a
tier requiring asynchronous restoration, design and test retrieval states,
deadlines and failures for verification, fresh backups and direct historical
access. Determine the provider's
actual storage, request, retrieval and minimum-duration charges for the selected
tier; a lower per-GB price is not a complete cost estimate.

The independently tunable [compacted Parquet layout](parquet-read-layout.md)
reduces active full-scan files while retaining original evidence. It does not
change the source format, delete historical objects or establish VPS capacity.

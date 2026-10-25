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

`ASSESSMENT_SCHEDULE_MINUTES=15` means up to 96 scheduled analytical runs per
day. The default is in `PlanningSettings`, `.env.example` and Compose;
`assessment_schedule` builds its Dagster cron from that setting. Values must be
positive divisors of 60. Collection planning has its own five-minute schedule;
it does not require a ranking after every capture.

The initial implementation used a fixed `*/15` collection/analysis schedule
(commit `e2039c45`). Commit `abd73c007b6d7a8b91e8a56cac7b3bd5f7187b29`
separated collection and analysis and made the intervals configurable. This was
an engineering default, not a measured latency optimum or product requirement.
The storage changes leave it unchanged so their effects can be assessed alone.

Fifteen minutes can be appropriate for fast alerts with fresh measurements. For
products refreshed only four times a day, it may repeat substantial work. A
future selective schedule should account for source changes **and** clock-based
age/freshness/window transitions, changes to product membership or rules, plus a
periodic safety run. Skipping solely because no source generation changed would
freeze time-dependent results incorrectly. Measure analytical duration, freshness
and alert requirements before choosing the next interval.

## Preservation and cold storage

Preserve useful history; this delivery does not introduce voluntary age deletion
or an S3 lifecycle rule. Source-specific expiry requirements and withdrawals
remain authoritative. A 30-day analytical lookback limits one calculation's
inputs; it does not mean older data should be deleted.

Cheaper S3 storage with slower retrieval is a future option for genuinely inactive
archives. Old shared parts may still be referenced by a current or pending
result: object creation age alone cannot determine eligibility. Any transition
policy must check live references and retain the full reconstructible dependency
closure.

`verify` currently downloads every valid historical ranking and its required
objects. A new full object backup and a full restoration also require synchronous
reads. Incremental backups can reuse verified unchanged bytes from a local base,
but cannot guarantee every future recovery avoids S3 reads. Before using a tier
requiring asynchronous restoration, add explicit retrieval states, deadlines,
failure handling and a tested recovery-time objective. Determine the provider's
actual storage, request, retrieval and minimum-duration charges for the selected
tier; a lower per-GB price is not a complete cost estimate.

The independently tunable [compacted Parquet layout](parquet-read-layout.md)
reduces active full-scan files while retaining original evidence. It does not
change the source format, delete historical objects or establish VPS capacity.

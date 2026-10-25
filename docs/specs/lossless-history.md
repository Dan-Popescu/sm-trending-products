# Lossless historical storage

Approved by the user on 2026-10-25. Preserve useful historical observations and
exact analytical results while reducing repeated storage and read overhead.
Do not change assessment frequency or configure an S3 lifecycle rule in this
delivery. Source access, revocation and retention obligations remain binding.

## Shared analytical archives

- Preserve the exact logical schema-2 snapshot, ordered rows and logical run ID.
  Age, timestamps, global cohort normalization and frozen evaluation decisions
  must reconstruct exactly, including after restart and backup/restore.
- Compress new archive storage and share unchanged product information across
  runs. Keep a small changing rank representation in bounded blocks; reading
  a page of at most 200 products must not hydrate the whole universe.
- Hide physical representation behind a dedicated archive module. Extend
  existing provenance records, publication inventory and dependency edges;
  do not introduce a second registry or deletion authority.
- Retain legacy readers. New writes require SQL 038 and a completed compatible
  reader handover; schemas 35–37 continue writing the legacy representation.
  Stored-byte hashes and sizes must describe physical bytes, independently of
  logical run IDs. Immutable keys include the physical hash. Retried or
  concurrent writers reuse the authoritative registered archive.
- Preserve staging, publication fences, source invalidation, retirement,
  verification and object backup/restore contracts. Never delete source history
  to obtain the advertised savings. Bound decompression and provenance nodes.
- Demonstrate exact reconstruction, rank boundaries, time-only and cohort
  changes, sparse updates, restart/retry and retention behavior. Record bounded
  storage/PUT/CPU measurements with limits; do not claim whole-pipeline speedup.

## Compacted Parquet read layout

- Add an independently configurable compaction output target of 5,000 rows.
  Keep source capture/normalization defaults at 1,000 rows and preserve the
  existing encoding, source identities and original immutable objects.
- Staged manifests own their recorded physical layout. A settings change or
  upgrade must not produce a different retry under the same immutable key.
- Verify compaction, replay, invalidation and integrity checks. Measure both
  full scans and selective reads; disclose the selective-read cost and that
  retained original files prevent an immediate reduction of total bucket size.

## Explicit source retention policy

- Support unlimited retention only when explicitly verified for the source.
  This is not the same as a missing or unverified source policy. Never silently
  change existing finite policies or resurrect retired evidence.
- Use NULL for the verified absence of a source expiry cap; keep positive
  finite limits valid. Gate unlimited writes on SQL 039, after compatible
  readers have completed handover. SQL 039 follows SQL 038.
- Require explicit source-wide retention evidence before removing or relaxing
  a trusted source cap. An account access probe must not silently relax a
  source-wide policy. A failed probe must not alter an already trusted cap.
- Skip age expiry for verified unlimited sources, while keeping revocation,
  invalidation and lawful retirement fully effective. SQL retirement-certificate
  validation must explicitly reject age-expiry authority for unlimited policies.
- Preserve policy state through native backup/restore and test finite expiry,
  unlimited retained history, failed probes and explicit withdrawals.

## Operational interpretation

An old frozen result does not require periodic recomputation. A new current
assessment can change with source measurements, time, product membership or
policy even if no new source rows arrived. The inherited 15-minute default is
configurable, not a measured product optimum. A later cadence change must model
clock transitions and alert requirements rather than only source generations.

Cold storage is a future option, not enabled here. Reference reachability must
protect old shared parts still used by current results. Full verification,
backup and restoration currently require synchronous object reads; asynchronous
retrieval needs an explicit operational design before archival S3 tiers are used.

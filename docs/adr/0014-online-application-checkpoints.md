# Export one application snapshot without pausing acquisition

Status: accepted for the controlled implementation of #86 under #80.

The paused-writer backup remains a supported legacy recovery path. An explicit
`--online` capture instead uses one PostgreSQL repeatable-read exported snapshot
for the application's logical fingerprints, referenced-object inventory and
`pg_dump --snapshot`. Publication continues against the normal live database;
rows and objects committed after the snapshot belong to a later recovery point.
The exporter stays alive until the dump handshake completes, then closes before
per-object inspection/copy and inherited-base hashing.

An online inventory includes catalog-known committed and available staged
dataset objects, valid assessment objects and committed derived publications.
Raw-only typed capture datasets are included even when they contain no normalized
observations. Unmanaged objects and objects belonging only to later catalog
commits are excluded from this application recovery set. Legacy whole-bucket
inventory behavior remains unchanged. Every required checkpoint object must be
present and copied or inherited with the existing verified hash, metadata and
conditional/version identity rules.

The exporter holds only the shared schema-migration advisory key used exclusively
by catalog initialization. It acquires that lock before establishing the snapshot,
so an earlier migration is visible and a later migration cannot queue relation
locks in the middle of the dump. Ordinary capture/publication does not use that
key. This does not provide compatibility for arbitrary external DDL that ignores
the application's migration protocol.

There is deliberately **no retention advisory fence** in the online path. Holding
one throughout `pg_dump` would let an exclusive retention waiter queue later
shared publication locks behind the backup. Instead, concurrent source deletion
or provider mutation may make the backup fail incomplete. That is preferable to
silently pausing acquisition or completing a recovery set with missing/mismatched
bytes. A captured database view is consistent, but does not reserve remote source
versions against deletion. Retention obligations apply to backup families too.

Dagster has a separate database and therefore a separately timed snapshot. It is
orchestration metadata rather than the authority for captured evidence, source
tasks or committed publications. Backup metadata records this distinction.
Restore startup must retain the existing idempotent job/cursor behavior; no claim
of cross-database transactional atomicity is made.

Successful online recovery restores the documented application cut, not all
observations captured afterwards. Those later observations remain in the live
catalog/object store and must not be erased by an application rollback. Zero-RPO
host/database disaster recovery requires the separately designed continuously
available capture/control dependency; a finite backup is not that guarantee.

Native tests hold the exported snapshot, capture a one-shot HTTP response with
actual Rust while an actual `pg_dump` is active, restore that custom dump and
copied objects into isolated destinations, and compare exact checkpoint
fingerprints. Further tests exercise post-snapshot inventory exclusion,
retention failure, source-free restoration and legacy full/incremental behavior.
These are controlled PostgreSQL/HTTP-object-store proofs, not target-provider or
VPS availability certification.

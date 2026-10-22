# Publish normalization revisions without rewriting source observations

Status: accepted under the user's T02 implementation authorization.

Source collection and omitted replay versions continue to use normalizer 1.
Version 1 retains its existing record payloads, identifiers and replay keys.
Explicit source replay version 2 reports unmapped source field paths, keeping
their original values in the archived raw response. It quarantines the unknown
fields without discarding otherwise valid known evidence; incompatible known
containers still quarantine the record. Neither version invents audience
geography, reach precision or commercial outcomes.

A changed transformation creates a distinct immutable observation revision:
source, entity and observation time remain fixed. An identifier derived from the
original delivery and transformation version, with normalization provenance,
distinguishes the revision. New knowledge is available at
the first successful normalization attempt, never at the old source timestamp.
The small PostgreSQL replay-availability record chooses that timestamp once per
canonical original delivery/version after decoding succeeds. Concurrent retries
converge through the unique key, then reuse the same timestamp and object hashes.
The transaction ends before object publication. It stores no observation payload
or competing publication lifecycle: staged/committed datasets and manifests
remain authoritative. Missing binaries and other configuration failures do not
reserve knowledge time.

Version 2 canonicalizes replay aliases through their committed, same-source,
same-workspace lineage. Replaying a replay therefore reuses the original revision
instead of creating conflicting identifiers or arbitrarily later knowledge.
Independent original deliveries may deduplicate under v1; their v2 revisions have
separate delivery contexts and clocks, so one revision ID never acquires two
different availability timestamps.
Traversal has explicit depth/cycle guards. Existing source-retention guards and
dataset lineage fence publication and invalidate dependent evidence; the tiny
availability tombstone cannot resurrect an invalidated parent.

Original versions remain auditable and eligible at their historical cutoffs.
Existing analytical timelines select the latest available revision at each
entity/timestamp. Minimum-history eligibility counts distinct sampling times,
while evidence references retain all eligible revision identifiers. Compaction
preserves the selected parents' transformation version. Imported or compacted
normalized records can be replayed under their existing version, but cannot be
relabeled as a new source transformation without an archived source response.

This adds one clock lookup/write per new versioned replay and small field-path
metadata. It avoids a parallel evidence store, duplicate raw blobs in normalized
attributes, live refetches and long-held SQL locks. Future semantic normalizers
must implement an explicit version and verify these invariants before release.

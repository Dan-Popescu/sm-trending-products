# Keep compact SQL delivery references and conservative source-period pruning

Status: adopted under the user's massive-volume cost/performance authorization.

PostgreSQL continues to index immutable history; Parquet remains authoritative for
observation payloads. Add an internal numeric identifier to the existing
`normalized_objects` relation. Delivery and entity-membership indexes reference
that identifier instead of repeating the dataset identifier and full object path
in every row and index. Guaranteed SHA256 payload fingerprints use 32-byte binary
values internally and are rendered as hexadecimal at the existing query boundary.
Arbitrary observation and entity identifiers remain text. Explicit `C` collation
continues to define observation keyset order independently of database locale.
There is no second entity registry, mutable analytical payload store or alternate
publication path. Foreign keys remove compact references when an object index is
retired, while immutable manifests, lineage and original provenance remain intact.
An unchanged manifest no longer rebuilds its object and membership indexes during
the staged-to-committed transition.

Migration 020 copies existing SQL references atomically and rebuilds their indexes
without fetching or rewriting S3 objects. The existing migration lock serializes
upgrades. An already large catalog needs a maintenance window and temporary disk
headroom for both reference layouts and indexes; the migration allows ten minutes
per statement, then restores the normal statement timeout. This is a one-time
catalog migration, not a claim of zero-downtime migration at arbitrary scale.

New immutable object descriptors additionally state whether source-period bounds
are known. Maximum valid period start and minimum valid period end provide
necessary, conservative conditions for a report contained in an outcome window.
The reader selects observed-window objects OR compatible report-period objects;
availability and exact period predicates still apply to each Parquet row. This
keeps delayed reports without loading every old cumulative counter. Unknown old
descriptors and malformed/ambiguous period aliases stay conservative matches.
They acquire precise metadata only through later normal publication/compaction;
the migration does not invent bounds or rewrite their manifests.

The shared contract parser owns valid explicit period interpretation. The older
timeline helper remains a compatibility wrapper. Integrity checking, cache pins,
retention fencing, conflict detection and oldest-delivery precedence retain their
existing boundaries. A separate object-driven analytical read design may reduce
repeated warm verification later; this change does not weaken checksum checks.

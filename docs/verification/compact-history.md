# Compact history metadata and bounded outcome reads

The implementation preserves immutable source objects and public identifiers.
`tests/test_compact_history.py` exercises a populated legacy SQL upgrade, arbitrary
Unicode/nonhexadecimal identifiers, replay provenance, paged public review,
idempotent reopening and unchanged manifests. Existing native history tests cover
conflicting payloads, non-C database collation, concurrent retirement, compaction
and staged-publication recovery. The migration itself performs no S3 requests.

The SQL-only footprint probe uses 10,000 simple synthetic video observations in
one publication, then reconstructs populated pre-020 reference tables and runs
the real migration. All three reference/mapping relations include heap, indexes
and TOAST in both measurements. The raw result is
[`compact-history-footprint.json`](compact-history-footprint.json).

| Relation | Before (bytes) | After (bytes) |
| --- | ---: | ---: |
| Entity/object membership | 7,004,160 | 1,802,240 |
| Object mapping and descriptors | 139,264 | 131,072 |
| Observation deliveries | 13,975,552 | 6,537,216 |
| **Total** | **21,118,976** | **8,470,528** |

This controlled fixture used **59.9% less reference/mapping storage**, including
the internal object mapping, while preserving all 10,000 delivery rows. Normalized
Parquet occupied 538,348 bytes and was unchanged. Actual sizes depend on identifier
lengths, page packing, source shape and compaction history. This is not a fixed
production bytes-per-row forecast or an elapsed collection-speed claim. WAL,
backups, replicas, other operational tables and temporary migration peak disk
are excluded. Those remain deployment measurements.

Outcome-read tests publish 31 daily historical files for one entity. A two-day
observed interval now selects its three daily samples instead of all 31 files. A valid
late report outside that observed-time window adds exactly its own file. Alias
periods, ambiguous null aliases and unknown legacy metadata remain readable under
their previous row-level rules. Old unknown descriptors intentionally retain
conservative extra reads. These are operation-count assertions, not extrapolated
wall-clock speedup claims.

The existing cache still verifies every pinned object. Repeated product batches
can therefore reread mixed compacted files even with zero S3 GETs; this slice
does not claim to remove that separate amplification.

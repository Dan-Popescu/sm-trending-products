# Bounded backup metadata amplification

Issue [#71](https://github.com/Dan-Popescu/sm-trending-products/issues/71) follows
the approved retained-history and priced-cost work. The previous checkpoint
issued one HEAD per retained key while application writers were paused.

## Native 1,001-object comparison

The [raw result](backup-inventory-1001.json) compares the immutable
`761b7796f77d83a7c10cf818042c01e57cb5e999` object snapshot script with the new
capture implementation. Script hashes are in every phase. PostgreSQL 17.11 and
HTTP Moto are real; source objects use the existing staged/committed catalog and
immutable storage publication protocol. They are small derived fixture objects,
not a claim about 1,001 products or retained product history. The process high
water RSS was 189,026,304 bytes, including Moto and baseline/treatment operations;
PostgreSQL resource use is outside that number.

| Phase | LIST pages | HEADs | GETs | HEADs inside checkpoint | Checkpoint span (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Prior full | 2 | 1,001 | 1,001 | 1,001 | 4.169 |
| New full | 2 | 1,001 | 1,001 | 0 | 0.234 |
| Prior unchanged increment | 2 | 1,001 | 0 | 1,001 | 3.842 |
| New unchanged increment | 2 | 0 | 0 | 0 | 0.211 |

The full captures copy the same 41,933 bytes. The unchanged capture reuses
locally verified base bytes. Source metadata inspection and payload copying run
after the checkpoint. This probe sends the SQL-dump/restart continuation signal
immediately; its spans measure the fenced inventory portion, **not** total writer
downtime including container startup, logical fingerprints or database dumps.
Shared development hardware and Moto timings do not forecast a provider's
latency or VPS capacity. The request-count reduction is the measured result.

## Native 10,000-object comparison

The [larger result](backup-inventory-10000.json) repeats the same native protocol
with 10,000 committed fixture objects and ten LIST pages per capture. It copied
428,890 bytes in each full base, and cleanup removed the schema and owned files.

| Phase | HEADs | GETs | HEADs inside checkpoint | Checkpoint span (s) | Whole capture (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Prior full | 10,000 | 10,000 | 10,000 | 45.343 | 86.926 |
| New full | 10,000 | 10,000 | 0 | 2.205 | 75.465 |
| Prior unchanged increment | 10,000 | 0 | 10,000 | 41.838 | 42.293 |
| New unchanged increment | 0 | 0 | 0 | 2.559 | 3.283 |

The process high water RSS was 252,690,432 bytes. Every committed object remained
present in each completed inventory. The larger new capture script also contains
the checksum-metadata restore preflight described below; the SHA in the raw report
identifies it. This remains a controlled small-object metadata amplification probe:
it does not model the bytes or streaming duration of a large retained product lake.

Reproduce with a task-owned native test database and the normal dev dependencies:

```sh
git show 761b779:scripts/object_snapshot.py > /tmp/backup-baseline-761.py
PYTHONPATH=src python tests/benchmarks/backup_inventory.py \
  --objects 1001 --baseline-script /tmp/backup-baseline-761.py \
  --output output/backup-inventory-1001.json
```

The probe caps object count at 10,000, payloads at one small record per object,
and a declared cooperative deadline; it streams catalog and inventory work through
disk-backed SQLite. It owns its schema, object service and files. Do not run
long baseline checkpoints in the same database as other timing probes: PostgreSQL
advisory retention locks are database-scoped even across separate schemas. Use a
separate task-owned database or serialize those measurements.

## Integrity and remaining work

Native tests preserve actual publication-to-restore behavior, v1/v2 chains,
conditional/versioned GET races, required missing/corrupt nodes and checkpoint
lock release. A process-level test runs the actual backup shell and snapshot
scripts over native SQL/HTTP, with controlled Docker-service and SQL-dump process
adapters; HEAD and GET must occur after the writer restart, and a managed unchanged
increment has neither. Independent probes also verified the final checksum
metadata protections.

Known source SHA must match both copied bytes and restorable checksum metadata.
Old base metadata incompatible with the catalog cannot take the zero-HEAD path;
fresh source metadata can repair the inventory without downloading already
verified payload bytes. Direct restore rejects incompatible checksum headers
before creating a destination.

LIST, authoritative SQL reads, full inventory serialization and base-chain
verification still grow with retained objects. Full bases still inspect and copy
every object. Unmanaged keys and legacy catalog records without known hashes
need one deferred HEAD each; the zero-HEAD claim applies to unchanged catalog-owned
objects with matching LIST fingerprints and verified base metadata. Counts are
logical SDK operations rather than transport retry attempts. Source-provider
version/conditional behavior and complete deployed downtime remain separate
acceptance checks.

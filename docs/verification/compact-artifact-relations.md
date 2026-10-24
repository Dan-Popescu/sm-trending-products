# Compact artifact association verification

SQL026 stores internal association references as bigint IDs on the existing
`derived_publications` relation. It preserves public keys and content hashes.

## Native storage measurement

The opt-in probe uses PostgreSQL 17.11 and actual 64-character SHA256 object names.
Run with a task-owned native database:

```sh
python tests/benchmarks/compact_artifact_relations.py \
  --output output/compact-artifact-relations.json
```

Both sides use `REINDEX TABLE` and `VACUUM (ANALYZE)` before measurement. This
avoids crediting SQL026 for incidental repacking of the unchanged public-key
index. Totals include the entire publication table, the new publication ID,
its unique index and sequence, and all three association tables and indexes.
Other unchanged application tables and fixture temporary tables are excluded.

| Child edges | Fixture | Publication rows | SQL019 bytes | SQL026 bytes | Reduction |
| ---: | --- | ---: | ---: | ---: | ---: |
| 100,000 | Unshared tree | 106,250 | 169,041,920 | 87,482,368 | 48.25% |
| 100,000 | Shared-child DAG | 10,346 | 77,635,584 | 18,571,264 | 76.08% |
| 500,000 | Unshared tree | 531,250 | 843,939,840 | 434,241,536 | 48.55% |
| 500,000 | Shared-child DAG | 35,346 | 371,056,640 | 79,454,208 | 78.59% |

Every parent has up to 16 children. The unshared case gives every child edge
a distinct child publication; the shared case reuses a pool of 4,096 children
across distinct parents. Each child has one source-input association and each
parent has one evaluation association. These are inspectable SQL row shapes,
not measured provider workloads or predicted production topology. Full graph
reduction is below 50% in the unshared case; it depends on publication reuse.
The edge table alone drops from 67,108,864 to 10,821,632 bytes at 100,000 edges.

The new publication column/index has a real cost: in the 500,000-edge unshared
case the publication relation grows from 194,912,256 to 210,993,152 bytes.
The total above includes that increase. The native SQL026 upgrade itself took
about 18 seconds for this fixture and about 5.4 seconds for the shared case;
these durations do not size a deployed maintenance window.

## Behavioral coverage

`tests/test_compact_artifact_relations.py` reconstructs the unchanged SQL019
layout from a populated native schema-v2 artifact graph, including assessments,
frozen decisions, maturation and backtests. SQL026 runs with storage GET, PUT,
HEAD and LIST forbidden. All public relation mappings, publication metadata,
old SQL019 ancestor closure, whole result payloads/hashes and verified audit
pages are unchanged. Unicode legacy keys and nullable legacy descriptors survive.
Reinitialization preserves assigned identities. Subsequent source removal
invalidates the correct results and prevents previously cached audit reads.

The trigger test observes its own backend's shared retention advisory lock and
rejects an unavailable child. Existing schema-v2 tests cover staged/upload races,
shared/compacted source closure, immutable detail reads and legacy golden hashes.
The SQL025 collector tests run against the new IDs: a mixed 1,203-row graph drains
as 1,000 then 203 rows, with safe restart, lock skipping, rollback and deadlines;
live evaluation links remain blocked and visible, while tombstones and descriptors
survive completed collection.

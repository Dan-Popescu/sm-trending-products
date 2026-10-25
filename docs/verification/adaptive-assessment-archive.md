# Adaptive assessment archive qualification

Measured on 2026-10-25. The workload uses 1,000 products, two videos and four
initial observations per video, then four actual assessments spaced one hour
apart. Cases cover no new observations, 5% of products updated at **every**
subsequent assessment, and all products updated at every assessment. Source
collection frequency and assessment formulas are unchanged by this archive work.

The immutable publisher baseline is commit `0f2500f`. Candidate encoding results
were measured on `4c52fda` (clock/sparse) and `50edc0f` (broad updates), with the
exact candidate revision recorded in those JSON files. Final `177e698` adds
explicit planning quotas/cleanup and an oversized-header fallback; `a1fc903`
preserves canonical identity for opaque metadata and numeric map keys. These
change neither the chosen format nor stored bytes for the measured ordinary-JSON
small plans. Their boundary behavior is covered by native tests. The measurements
do not assert a
whole-pipeline speedup, provider latency or VPS continuous resource use.

## Options evaluated before implementation

The [cost-only prototype](adaptive-archive-prototype.json) compared ordinary
shared hash partitions, full JSON blocks, templated full-row blocks, and
templated stable blocks with a small changing rank index. It used actual
assessment summaries with synthetic model links, without SQL or object-store
publication. These are preliminary encoder estimates, not production evidence.

At 1,000 products, ordinary full JSON blocks still needed about 142 KB and
19 objects, against about 147 KB and 19 objects for the old shared layout.
Templated full-row blocks reduced this to about 106 KB and 7 objects, but also
rewrote all those bytes during clock-only assessments. Templated **stable**
blocks retained quiet-run sharing: about 109 KB/8 objects after dispersed changes
and 3.36 KB/2 objects during quiet runs. That option qualified for native testing.

The shipped reader retains every existing format. SQL 040, after SQL 039 and a
compatible reader handover, enables adaptive writes. Schemas 35–37 keep plain
JSON writes; 38–39 keep ordinary shared writes. A pre-upload plan compares the real encoded parts and
their authoritative committed inventory. It selects the new layout only if
neither new bytes nor PUT count increases. It never uploads both candidates.

Repeated structure and values are represented once within each stable block.
Variable columns preserve exact JSON types, signed zero, unknown fields and
missing/null differences. Source provenance descriptors remain complete objects
so existing dependency discovery and retirement guards continue to own them.
Product IDs determine stable grouping independently of current rank; rank changes
remain in the small changing index. No chain of previous snapshots is needed.

## Native storage and requests

Totals below include **every new archive part, index and header** across four
assessments. KB means decimal kilobytes. Source objects and input-provenance
preparation are retained and excluded from both publisher measurements.

| Workload | Old shared bytes | Adaptive bytes | Reduction | Old PUTs | Adaptive PUTs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Clock only | 200.7 KB | 164.4 KB | 18.1% | 25 | 14 |
| 5% dispersed each hour | 769.2 KB | 622.7 KB | 19.0% | 76 | 32 |
| All products each hour | 877.6 KB | 684.2 KB | 22.0% | 108 | 32 |

A first packed assessment needs 8 PUTs. A subsequent unchanged assessment needs
2. Initial ordinary sharing needs 19 PUTs in this fixture. Broad updates sometimes
force old hash leaves to split, producing 35 PUTs; the packed layout needed 8.

For comparison, storing each whole logical snapshot in one gzip object would
use about 620 KB, 627 KB and 705 KB respectively across the three four-run cases,
with 4 PUTs each. That decoder would read the entire universe for a page, so it
is only a cost reference. Adaptive archives still make more PUTs than this
unbounded single-object alternative. Request/storage prices and actual retention
duration are required before asserting a provider cost saving.

## CPU and memory

The timed region is archive stage plus commit, excluding feature preparation,
current-assessment SQL and alert updates. Process CPU includes the local HTTP
Moto server and a 2 ms RSS sampler, consistently for both publishers. Each
benchmark case runs in a fresh pytest process and an isolated PostgreSQL database.

| Workload | Old CPU total | Adaptive CPU total | Old median stage | Adaptive median stage |
| --- | ---: | ---: | ---: | ---: |
| Clock only | 1.652 s | 1.817 s | 0.412 s | 0.326 s |
| 5% dispersed each hour | 1.948 s | 1.373 s | 0.520 s | 0.331 s |
| All products each hour | 2.404 s | 1.245 s | 0.544 s | 0.310 s |

The changed-data cases reduced observed archive CPU by about 30% and 48%.
Clock-only total CPU increased 10% in this small sample, including one measured
0.851 s stage; its median decreased. Both observations are retained rather than
claiming a universal CPU improvement. No sustained utilization estimate follows
from four short samples.

Whole-process peak RSS, including fixture construction and feature preparation,
was 368.5/370.9 MiB old/adaptive for clock-only, 372.9/371.1 MiB for dispersed
updates and 411.3/414.2 MiB for broad updates. This shows no material process-memory
increase at this size. The maximum sampled RSS rise during archive stage was
7.3–7.5 MiB adaptive versus 9.2–9.6 MiB old. RSS sampling can miss short peaks and
allocator reuse affects phase deltas; these are measurements, not hard limits.

Planning has independent hard admission limits: **4,000,000 charged bytes and
1,000 nodes per candidate**. Charges include compressed bytes, descriptors,
serialized dependencies and conservative metadata overhead, even for reused
nodes. Temporary spooling may be RAM-backed tmpfs; it is not advertised as disk
offload. If either candidate reaches its limit, both plans close and discard
their metadata **before** the existing bounded ordinary writer uploads anything.
Large universes therefore retain the old representation instead of growing
temporary planning storage without limit. The optimization is not a guarantee
that every larger workload will fit the adaptive planning budget.

## Cold historical pages

Both formats return at most 200 products. Every decoded object is bounded to
256,000 bytes. Wide blocks subdivide; the reader expands only requested rows,
never every row represented by a selected template block.

| Workload | Page | Old GETs / bytes | Adaptive GETs / bytes |
| --- | ---: | ---: | ---: |
| Clock only | 1 product | 4 / 16,080 B | 4 / 33,993 B |
| Clock only | 200 products | 19 / 189,491 B | 5 / 63,809 B |
| Dispersed updates | 1 product | 4 / 16,153 B | 4 / 34,591 B |
| Dispersed updates | 200 products | 19 / 192,324 B | 5 / 64,772 B |
| Broad updates | 1 product | 4 / 16,263 B | 4 / 34,518 B |
| Broad updates | 200 products | 19 / 193,412 B | 5 / 64,817 B |

The one-product tradeoff is explicit: about 18 KB more is downloaded, with the
same four requests. A 200-product page needs about 74% fewer GETs and 66% fewer
bytes. Local one-product CPU was 0.036–0.063 s adaptive versus 0.033–0.049 s old;
200-product CPU was 0.052–0.112 s versus 0.139–0.189 s. These do not predict cloud
latency. Full logical snapshot reconstruction was verified in every case.

## Evidence and reproduction

Reports:

- [Old clock](adaptive-archive-1000-baseline-clock.json) / [adaptive clock](adaptive-archive-1000-adaptive-clock.json)
- [Old dispersed](adaptive-archive-1000-baseline-sparse5.json) / [adaptive dispersed](adaptive-archive-1000-adaptive-sparse5.json)
- [Old broad](adaptive-archive-1000-baseline-all.json) / [adaptive broad](adaptive-archive-1000-adaptive-all.json)

Run every case in a separate process, with a task-owned `TEST_DATABASE_URL`:

```bash
source /workspace/work/performance-fixes/test-env.sh
export PYTHONPATH="$PWD/src:$PWD/tests"
ADAPTIVE_BENCHMARK_DIRECTORY="$PWD/docs/verification" \
  pytest -q 'tests/benchmarks/adaptive_archive.py::test_archive_resources[adaptive-sparse5]'
```

Available IDs are `baseline-clock`, `baseline-sparse5`, `baseline-all`, and their
`adaptive-*` counterparts. Native fixtures still separate each SQL schema and
bucket. Database-wide advisory locks mean concurrent suites should use separate
databases, as the recorded runs did.

The focused native suite on `177e698` passed 72 tests across adaptive and historical
archives, sharded provenance, object backup and feature projections. Specific
boundary tests cover exact numeric types/zero, numeric-key metadata, JSONB key
order, bounded selected expansion, byte/node planning exhaustion with cleanup
before fallback upload, near-limit rows and headers, source withdrawal, restart,
and object restore with an empty cache. The two additional native opaque/numeric
identity regressions passed on `a1fc903`. Existing run-pointer and publication
transaction contracts remain authoritative.

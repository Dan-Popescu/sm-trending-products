# Complete compact publication cost measurement

Measured on 2026-10-23 at source revision
`839d2cad22c0f6e98eb9f670ebf2df0c092ccb00`, before feature-projection integration.
[Raw counters and environment](compact-results-100.json) accompany this report.
[ADR0008](../adr/0008-compact-analytical-provenance.md) defines the representation.

## Workload and boundary

A native PostgreSQL 17.11 schema and loopback HTTP Moto store held 100 synthetic
products, 20 videos each, and 120 samples per video spaced six hours apart:
240,000 observations across a 30-day lookback. The source fixture imported one
2,400-row batch per product, producing 100 datasets and 300 normalized Parquet
objects, totaling 11,103,423 bytes. This represents retained history; it does not
reproduce physical API pagination, remote latency or TikTok access limits.

The assessment used ten products per evidence batch, two Polars/Rayon threads,
a four-bucket fixture partition setting, and the ordinary production publication
path. All three phases asserted exactly 100 output products. Product/link
creation preceded the synthetic decision cutoff. The final phase appended one
new sample to each video in 100 additional source batches. Import preparation
lasted 88.77 seconds and is separate from the assessment phase timings.

The machine exposed five CPU affinities with a four-CPU cgroup quota and a 16 GiB
memory limit. The filesystem was overlay. Other isolated test processes shared
the host and database, so timings are not an exclusive-machine capacity result.
Initial application file caches were empty; operating-system and provider caches
were not controlled. Later phases reused the same process and verified file
cache. No Rust rebuild, cloud deployment or external source requests were used.

## Results, including the out-of-line objects

| Assessment phase | Seconds | Result JSON bytes | New provenance nodes | Provenance bytes added | All PUTs | All uploaded bytes | Source GETs / bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Initial publication | 63.05 | 399,746 | 3,917 | 17,455,355 | 3,918 | 17,855,101 | 300 / 11,103,423 |
| Unchanged data, next cutoff | 23.65 | 399,746 | 0 | 0 | 1 | 399,746 | 0 / 0 |
| One appended refresh wave | 22.12 | 405,046 | 317 | 825,255 | 318 | 1,230,301 | 100 / 897,405 |

The equivalent legacy inline assessment body, reconstructed with the exact
identifiers and measured scalar values, is 32,514,905 bytes. The compact initial
header is **81.3 times smaller**. Counting all new immutable objects, initial
upload bytes fall only **1.82 times**. The initial result therefore trades many
small PUTs for a smaller reusable representation; a cheap header is not a free
audit tree. The 3,917 initial nodes include day leaves, their hash indexes and the
combined input root. Day placement and hash distribution affect exact counts.

Unchanged history adds no provenance objects or raw dependency edges. It still
adds one immutable assessment publication and 101 parent-to-root edges. The
appended wave rewrites its day leaf and a short index path for each product,
plus the combined root. Existing daily leaves remain shared. The write and
retained-byte totals above include all those new nodes, not just the result body.

| SQL/materialization measure | Initial | Unchanged phase added | Refresh phase added |
| --- | ---: | ---: | ---: |
| Leaf-to-source dependency rows | 3,100 | 0 | 200 |
| Artifact child-edge rows, including assessment attachments | 4,017 | 101 | 2,017 |

A purely linear illustration of the measured append pattern at 10,000 products
and four waves daily would add 3.8 million provenance nodes and 24.2 million child
edges in 30 days, before initial publication, additional assessment attachments
or reclamation. That arithmetic is not a capacity or cost forecast. It makes
request pricing, metadata reclamation and append fanout explicit follow-up
concerns, even with much smaller published payloads.

After all three results, the store retains 4,237 derived objects totaling
19,485,148 bytes, including previous tree versions and all three headers.
Current assessment payloads occupy 286,439 PostgreSQL column bytes in total;
this is not a measurement of old PostgreSQL TOAST storage. The largest initial
product JSON row is 3,990 bytes and the largest refreshed row is 4,043 bytes,
while their evidence scopes contain 2,400 and 2,420 identifiers respectively.
The process cumulative peak is 575,484 KiB, including imports, in-process Moto
and the legacy-byte comparison; PostgreSQL and other host processes are excluded.

## Interpretation and limits

This proves that published rows and retained global reference lists no longer
grow with every window observation. It proves reuse on an unchanged cutoff and
bounded new-object cost for this append distribution. It does **not** establish a
full pipeline speedup against the old implementation: there is no old native
runtime comparison, and 23.65 seconds of unchanged-history work remains here.
The separate exact feature-projection slice addresses repeated history reads,
reference enumeration and feature calculation. Remote object latency, smaller
source pages, different product skew, long retention and actual refresh cadence
can change the cost balance materially.

Prices are deliberately absent. Apply provider request, storage, transfer and
compute prices to the complete counters. Retained tree versions and backup copies
continue to cost storage until their authoritative lifecycle permits removal.
Source invalidation rejects atomic closures above 10,000 datasets or artifacts;
this finite workload does not establish unlimited deletion fanout.

The implementation's independent review also verified exact pre-change whole
attention/profit reports at three cutoffs, actual root/child corruption after
cache eviction, replay availability, source-removal closure and final-publication
races. A latest incomplete financial correction now remains an input dependency
even when it produces a censored outcome with no displayed successful evidence.
Earlier-cutoff successful reports remain immutable and valid when only that later
correction is invalidated.

## Repeat

After installing development dependencies and configuring an isolated native
test database, run the opt-in probe from the repository root:

```sh
COMPACT_BENCHMARK_OUTPUT=output/compact-results-100.json \
  PYTHONPATH=src pytest -q -s tests/benchmarks/compact_results.py
```

The ordinary test fixtures create and remove a fresh PostgreSQL schema, temporary
Moto bucket and local files. `TEST_DATABASE_URL` must point to the intended test
database. The probe is not collected by the default suite; allow several minutes
and adequate memory/disk headroom. It records exact revision, counters and all
retained object bytes. Repeat target-environment measurements before choosing VPS
size or extrapolating monthly cost.

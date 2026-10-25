# Shared assessment archive measurements

Measured on 2026-10-25 against native PostgreSQL and HTTP Moto. These are bounded
archive measurements, not provider latency, a production capacity limit, or a
whole-pipeline speedup. JSON reports preserve every individual measurement:
[100 products](assessment-archive-100.json) and
[1,000 products](assessment-archive-1000.json).

## Reproduction and fixture

```bash
source /workspace/work/performance-fixes/test-env.sh
export PYTHONPATH="$PWD/src:$PWD/tests"
ARCHIVE_BENCHMARK_DIRECTORY="$PWD/docs/verification" \
  pytest -q tests/benchmarks/assessment_archive.py
```

The test fixture imports four actual counter measurements for each of two
videos per product and runs the real assessment engine. The five publications
are initial, clock-only, clock-only, a new source measurement for 5% of products,
and clock-only. Their intervals are 15 minutes. Fixture videos have no published
timestamp, so this particular benchmark does not exercise changing ages;
behavioral tests separately verify exact age/cohort transitions and score changes.

The benchmark times only archive stage/commit. Source history, feature/input
provenance preparation, current-row SQL and alerts are excluded. Physical object
totals include every new stable/rank tree node and archive header produced by
the archive itself. Inputs remain in the source catalog and bucket. Baselines
encode the identical logical body as one plain JSON object or one gzip object
per run, with gzip level 6 and zero modification time. No baseline is credited
with shared storage. Exact full reconstruction is checked after each case.

## Five-publication storage

All sizes in this table are decimal bytes, with no rounding in the JSON files.

| Products | Plain JSON | Simple gzip | Shared gzip archive | Shared vs plain | Shared vs gzip |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 1,655,542 | 86,629 | 53,778 | -96.8% | -37.9% |
| 1,000 | 16,535,950 | 782,916 | 395,419 | -97.6% | -49.5% |

Compression accounts for most of the reduction from the original plain JSON.
The additional reduction from sharing is measured separately. It depends on
how many product parts change and how those products span hash partitions.

| Products | First shared publication | Clock-only shared publication | 5% sparse-update shared publication | Corresponding simple gzip |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 38,405 B / 19 PUTs | 1,097–1,190 B / 2 PUTs | 11,989 B / 7 PUTs | 17,130–17,620 B / 1 PUT |
| 1,000 | 189,410 B / 19 PUTs | 3,743–4,514 B / 2 PUTs | 194,007 B / 19 PUTs | 154,946–159,038 B / 1 PUT |

The first shared publication is larger than simple gzip because independent
bounded parts lose some compression across product boundaries and add indexes.
At 1,000 products, the 50 changed products span nearly all stable leaves, so the
sparse-update publication is also larger than one full gzip object. This is a
real tradeoff, not an exceptional case hidden from the totals. Clock-only
publications use about 35–41 times fewer bytes than simple gzip at this size.

Across all five runs, shared storage issued 32 PUTs for 100 products and 44 PUTs
for 1,000, compared with 5 PUTs for either single-object baseline. S3 request
charges and workload mix must be included before turning byte savings into a
cost claim. No provider price projection is asserted here.

## CPU and selected reads

Total archive stage plus commit process CPU across five publications was
0.349 seconds for 100 products and 1.771 seconds for 1,000 products. At 1,000,
individual stage CPU ranged from 0.259 to 0.444 seconds. This includes JSON
encoding, splitting, compression, HTTP client work and catalog operations;
the local HTTP Moto service shares the same process. It is not an isolated
compression microbenchmark or a production CPU utilization estimate.

Simple gzip baseline CPU includes canonical encoding plus gzip. Every CPU and
elapsed observation is recorded individually in the JSON reports. Shared
publication performs more work than one gzip object and promises no pipeline
acceleration.

Cold pages use a separate empty filesystem cache for each case:

| Universe | Requested / returned products | GETs | Downloaded bytes | CPU seconds | Elapsed seconds |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 1 / 1 | 4 | 5,010 | 0.031 | 0.035 |
| 100 | 200 / 100 | 19 | 38,890 | 0.107 | 0.112 |
| 1,000 | 1 / 1 | 4 | 16,928 | 0.042 | 0.065 |
| 1,000 | 200 / 200 | 19 | 194,006 | 0.197 | 0.207 |

A one-product lookup reads much less than a whole gzip snapshot. A 200-product
page can touch every stable hash leaf in this small universe and transfer more
than the full gzip baseline. The reader selects only requested paths, holds no
whole-universe record list, bounds decoded nodes to 256,000 bytes, and returns
at most 200 rows. Larger volumes and provider network latency were not measured.

## Correctness and recovery evidence

Native behavioral coverage verifies logical hash and full ordered row identity,
unknown fields and missing/null fields, rank boundaries 199/200, 1,000-product
restart/reuse, sparse updates and product additions, schema-37 legacy writes,
schema-38 shared writes, bounded decompression, physical checksum/size inventory,
different valid gzip encodings, retry/concurrent run-pointer selection, staged
upload failure/retry, source withdrawal and time/cohort changes with no collection.

The S3 snapshot restore regression reconstructs the archived assessment from an
empty restored cache and compares canonical logical bytes. The native
`test_online_custom_dump_restores_exact_catalog_cut_without_later_observations`
test additionally passed with `TEST_POSTGRES_CONTAINER=sm-trends-prelaunch-postgres`:
real custom `pg_dump`, `pg_restore`, object restore, catalog fingerprint, source
cut and exact reconstructed compressed assessment all survived restoration.

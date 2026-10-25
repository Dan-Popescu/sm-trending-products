# Compacted Parquet read layout

New compactions use a separate 5,000-row file target. Source captures keep the
1,000-row default, the same Zstandard encoder, and the same immutable object
bytes and identities. Existing Parquet files and manifests remain readable.
Staged compactions recover their recorded bucket layout even when the current
target changes in either direction. No schema migration or history deletion is
required.

## Measured scope

The [reproducible probe](../../scripts/benchmark-parquet-layout.py) calls the
production layout helper and encoder with two Polars threads. Each measurement
uses a fresh Linux Python process. It generates cumulative observations with two
numeric metrics, short video metadata, distinct observation IDs and original
raw-dataset provenance. There are 200 videos at 2,000 rows and 2,000 videos in the
larger cases. Raw results are in [parquet-read-layout.json](parquet-read-layout.json).

| Observations | Active files: 1k → 5k target | New compacted Parquet bytes | Full scan CPU | Process peak memory |
| --- | --- | --- | --- | --- |
| 2,000 | 2 → 1 | 114,109 → 105,783 (−7%) | 2.3 → 1.8 ms | 150 → 151 MiB |
| 20,000 | 20 → 4 | 1,549,773 → 1,110,052 (−28%) | 43 → 10 ms | 241 → 251 MiB |
| 100,000 | 64 → 20 | 7,923,849 → 7,064,995 (−11%) | 180 → 86 ms | 642 → 644 MiB |

The scan reads all columns from in-memory bytes with Polars. At a cold cache,
the file counts also describe how many normalized-object GETs a full scan needs.
These are bounded local measurements, not VPS capacity or production latency
claims. They exclude network latency, SQL work, object upload, and raw gzip
generation. Encoding CPU varied across runs; this change promises fewer active
files and smaller new copies, not faster compaction writes. Peak RSS includes
all materialized observations, encoded bytes and decoded frames; this is not
continuous service memory consumption.

One-video reads have a real cost: the selected whole-file download grows from
71,450 to 270,894 bytes at 20,000 rows, and 83,840 to 321,219 bytes at 100,000
rows. The latter local selected scan used 4.2 → 8.7 ms CPU. Reduce the separate
compaction target for a mostly selective workload. File size is not a strict row
cap: entity hashing, skew, and the maximum configured bucket count still apply.
The existing atomic compaction limit remains at most 100,000 observations.

Original raw and normalized history stays retained. Reducing the bytes written
for a new compacted generation does not shrink the existing bucket or eliminate
the extra delivery copy. The fixture does not model video/audio downloads or
future ML features.

## Reproduce

Run each case as a separate process with the project's installed environment:

```sh
POLARS_MAX_THREADS=2 PYTHONPATH=src python scripts/benchmark-parquet-layout.py --rows 20000 --target 1000
POLARS_MAX_THREADS=2 PYTHONPATH=src python scripts/benchmark-parquet-layout.py --rows 20000 --target 5000
```

Repeat with `--rows 2000` and `--rows 100000`. The command is read-only and does
not connect to PostgreSQL or S3. It verifies observation/source IDs and selected
row counts. Native maintenance tests additionally check complete observations,
provenance, immutable parent manifests, replay, remote verification, resumed
staged output in both sizing directions, and the existing invalidation paths.

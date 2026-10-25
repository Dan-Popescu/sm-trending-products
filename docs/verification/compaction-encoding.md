# Bounded compaction encoding and rejected native layouts

The selected change batches temporary Python row dictionaries for compaction
files larger than 8,192 observations. Small files call the original encoder.
Native Polars columns are concatenated and rechunked before the same Zstandard
writer. Source publication retains its original encoder. Every measured file,
including its SHA256, is byte-identical to the previous representation. This is
not a new format and needs no reader handover or schema migration. Staged
publication can therefore resume with either encoder and the recorded layout.

## Controlled measurements

[Raw results](compaction-encoding.json) record one fresh-process measurement for
each case with Python 3.12 and two Polars threads. The fixture uses 2,000 videos,
two counters (including occasional values above 2^100), stable SHA256 observation/raw
identifiers, timestamps and short source metadata. The benchmark encodes one
file, runs filtered full and selected-video reads, and validates every decoded
observation. There is no SQL, S3, provider latency or raw-gzip compaction work.

| Rows in one file | Identical bytes | Encoding CPU: old → bounded | Encoding-phase process peak: old → bounded |
| ---: | ---: | ---: | ---: |
| 20,000 | 915,745 | 0.404 → 0.380 s | 284 → 248 MiB (-12.5%) |
| 100,000 | 4,470,845 | 2.961 → 1.894 s | 906 → 696 MiB (-23.1%) |

These are component measurements. Encoding-phase peaks include fixture creation,
imports and the original Observation objects; they are sampled before reading.
At 100,000 rows the full lifetime peak remains approximately 1,008 MiB for both
encoders because the later full decoded result dominates. No decrease in VPS
continuous memory or whole-pipeline latency is inferred. The final native frame,
encoded bytes and original observations still scale with one bounded file;
only temporary Python row dictionaries are restricted to 8,192 rows.

The 100,000-row case is the existing atomic compaction cap, useful for a permitted
large-file setting or skew. The normal 5,000-row target often produces files
below the optimization threshold and keeps its exact existing path. The target
is not a strict cap when entities hash into the same bucket. Read bytes and
logical behavior are unchanged; differences in measured read timings are not
credited as a benefit of this writer.

## Native columns were measured and rejected

The benchmark also retains isolated, unshipped candidates: native UTC timestamps
with exact offset-microsecond companions, then native UInt64 metric bounds with
lossless decimal-string fallback for arbitrary integers and all metric metadata.
Both reconstruct the fixture exactly. No original facts were dropped.

| 20,000-row candidate | Bytes | Encode CPU | Full filter + decode CPU | Five-row selective filter + decode CPU | Lifetime process peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing representation | 915,745 | 0.404 s | 0.529 s | 0.021 s | 360 MiB |
| Native timestamps/offsets | 964,827 | 0.583 s | 0.614 s | 0.009 s | 409 MiB |
| Native timestamps and metrics | 1,067,059 | 0.759 s | 0.777 s | 0.011 s | 399 MiB |

Native time predicates improve this selective scan, but these candidates also
increase bytes, encoding work, complete-read CPU and lifetime memory. There is
no measured workload mix establishing a net benefit over the selected exact-byte
encoder. They therefore introduce neither production code nor a format gate.
This rejects these particular prototypes, not every possible native layout.
Unknown attributes, missingness, source periods and original timestamp offsets
remain preserved by the unchanged on-disk representation.

## Reproduce and verify

Run each case in a separate process:

```sh
POLARS_MAX_THREADS=2 PYTHONPATH=src python scripts/benchmark-compaction-encoding.py --encoder legacy --rows 20000
POLARS_MAX_THREADS=2 PYTHONPATH=src python scripts/benchmark-compaction-encoding.py --encoder bounded --rows 20000
POLARS_MAX_THREADS=2 PYTHONPATH=src python scripts/benchmark-compaction-encoding.py --encoder native-time --rows 20000
POLARS_MAX_THREADS=2 PYTHONPATH=src python scripts/benchmark-compaction-encoding.py --encoder native-metrics --rows 20000
```

Repeat the first two commands with `--rows 100000`. The measured code is the
production compaction encoder, the unchanged legacy writer and the shared
validated observation decoder. Native candidates live only in this script.
The behavioral tests compare complete physical bytes with large integer bounds,
Unicode, unknown nested values, missingness, offsets and optional fields first
appearing across a batch boundary. A native SQL/HTTP-object-store scenario checks
an interrupted 9,000-row publication, exact staged checksums, resumed legacy
encoding after a target change, original raw provenance, selected pages, replay
and remote verification. Existing compaction/retirement regressions remain
applicable because the layout and lifecycle are unchanged.

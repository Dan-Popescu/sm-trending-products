# Shared validated observation decoding

Ticket #113 extends the existing local-window native JSON validation path to
normalized source history. A single small module owns reconstruction, keeping
strict models, complete unknown attributes, missingness, original displayed
offsets and arbitrary-size metric integers. Writers, object bytes, hashes and
availability/invalidation contracts are unchanged.

Independent review found that the native JSON parser's nesting limit is lower
than the previous source decoder's. A 220-level unknown attribute survived the
existing Parquet writer but initially failed the new reader. The selected code
retains the original fully validated Python path only for that parser-limit
error. Ordinary model failures still fail; source-Parquet regressions and an
independent invalid-counter/deep-attribute check verify the distinction.

## Bounded component evidence

[Raw samples](observation-decoding.json) contain three fresh-process repetitions
per implementation in alternating order, plus an exploratory pair. Every run
retains 50,000 decoded observations with two counters, occasional bounds above
2^100, repeated video metadata, unknown nested values, missingness and immutable
SHA256 IDs. The full result hash is identical for every sample.

| Decoder | Median decode CPU, three repetitions | CPU range | Median process peak RSS |
| --- | ---: | ---: | ---: |
| Previous source reader | 1.955 s | 1.444–2.883 s | 409.3 MiB |
| Shared native reader | 1.515 s | 1.358–1.859 s | 394.1 MiB |

The process memory reduction is approximately 3.7% for this fixture. CPU varied
substantially on the shared host; the exploratory pair even reversed the order
(1.808 s previous, 2.087 s shared). These observations do not establish a fixed
CPU speedup or whole-pipeline/VPS consumption. Peak RSS includes imports, encoded
fixtures and retained output; only decoding is timed. Fixture generation,
verification, source acquisition, network, database and other services are
excluded. The memory gain is modest, not a claim of a large RAM reduction.

Reproduce each case in a separate process with locked dependencies:

```sh
PYTHONPATH=src python scripts/benchmark-observation-decode.py --decoder legacy --rows 50000
PYTHONPATH=src python scripts/benchmark-observation-decode.py --decoder shared --rows 50000
```

Repeat in alternating order; compare every `full_result_sha256`, not timings
alone. The benchmark's legacy function is the exact former source decoder and
is never a second production path except the narrowly required compatibility
fallback. The existing local-window algorithm is otherwise unchanged.

Ten pure regression cases cover complete evidence, all entity types, strict
measurement validation, huge integers, nested/escaped Unicode and the deep
source-Parquet case. Native window/source tests additionally cover source
provenance, replay, remote verification, existing cache compatibility and exact
price history offsets. The separate
[bounded compaction measurements](compaction-encoding.md) describe large-file
encoder savings; those component peaks must not be added as whole-server gains.

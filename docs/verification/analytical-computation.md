# Exact changed-product computation

Issue: #68. Baseline: `761b7796f77d83a7c10cf818042c01e57cb5e999`.
Treatment production code: `16b2a29` (the two production files are unchanged by
this measurement/documentation slice). Decision: [ADR0010](../adr/0010-bounded-analytical-computation.md).

## Reproduce

Extract the actual pre-change implementation, then run without database or source
credentials. The harness verifies both extracted module hashes before loading them:

```sh
git show 761b779:src/sm_trending_products/analytics.py > /tmp/analytics-761b779.py
git show 761b779:src/sm_trending_products/feature_projection.py > /tmp/windows-761b779.py
PYTHONPATH=src python scripts/benchmark-analytical-compute.py \
  --baseline-analytics /tmp/analytics-761b779.py \
  --baseline-windows /tmp/windows-761b779.py \
  --products 100 --videos 20 --samples 120 --repeats 3 \
  --max-seconds 300 --max-rss-mib 1024 --output output/compute-100.json
```

Use `--products 1 --samples 360 --repeats 7` for the longer-window comparison.
Every product has actual SHA256 observation and raw-dataset identifiers, four
samples/day, exact cumulative video counts and creator metadata. The 120/360
sample shapes span 29.75/89.75 days; policy lookback is 90 days. One product scope
is resident at a time; baseline and treatment decoded representations coexist
while their complete values are compared. Dataset generation and native warm-up
are outside the component timings, but process RSS includes imports and comparison
state. Admission is bounded; a cooperative deadline/RSS stop persists an incomplete
report rather than silently shrinking the requested workload. Checkpoints replace
the report atomically after each completed product. A zero exit means every
requested product and repeat passed all three parity checks.

The adapters alternate first position across product/repeat pairs. Elapsed and
process CPU are reported separately. These are shared-machine measurements with
native scheduling noise, not dedicated-core or provider latency measurements.
Library versions, CPU affinity/quota, exact module hashes, completed counts,
component totals/medians and whole-process peak RSS are in the raw reports.

## Measured component results

The [100-product report](analytical-compute-100.json) processes 240,000 actual
observations per complete pass, repeated three times for **each** adapter. The
[360-sample report](analytical-compute-360.json) processes one actual 7,200-row
product window seven times per adapter. Both completed without a resource stop.

| Shape | Assessor median, baseline → treatment | Decoder median, baseline → treatment | Complete encoded bytes |
| --- | --- | --- | --- |
| 100 products × 20 videos × 120 samples, 3 repeats | 32.02 → 11.32 ms/product (**2.83×**) | 35.70 → 33.11 ms/product | **11,570,384 → 11,570,384**, byte-identical |
| 1 product × 20 videos × 360 samples, 7 repeats | 128.10 → 37.76 ms/product (**3.39×**) | 190.00 → 142.98 ms/product | **411,151 → 411,151**, byte-identical |

Across all 300 paired scopes in the first case, assessor process CPU fell from
11.65 to 3.74 seconds. Including assessment, unchanged encoding and decoding,
process CPU fell from **36.48 to 27.59 seconds (24.4%)**. Whole-process peak RSS
was 243,539,968 bytes, including both adapters and comparison machinery; this is
not an isolated application-memory reduction claim.

The longer-window case used 316,362,752 peak process bytes. Its combined component
CPU fell from 2.96 to 2.40 seconds (18.9%). Decoder aggregate CPU was slightly
higher even though its median elapsed time was lower; native scheduling and
allocation noise are visible in this small case. The canonical encoder is the
same implementation and produced identical bytes; timing differences for encoding
are not credited as an optimization.

## Exactness and scope

The old module is an independent implementation, not a treatment-side formula
used as its own oracle. Every measured assessor result is canonically identical,
every decoded observation's complete JSON-mode model is identical, and every
encoded window is byte-identical. A separately captured immutable golden matrix
covers 500 complete assessments with aliases, revisions, bounds/estimates,
period/unit changes, timestamp offsets and submillisecond boundaries, and counts
above 2^53 and 2^100. Existing native caller tests retain source-retirement,
corruption, eviction, backdated imports, clock transitions and frozen decisions.
Native legacy-columnar and price-history tests verify zero source rebuilds and
preserved original offsets. Independent review also compares thousands of full
counter timelines and hundreds of complete assessments with the actual old code.

This measures the **changed-product computation**, not overall pipeline latency.
It excludes SQL round trips, S3, immutable provenance writes, collection, global
normalization of compact published rows and the full operational stack. Profiles
of real published refreshes still show important SQL/provenance and Parquet costs.
The retained-history harness must measure the combined path on the integrated
revision; this report does not multiply its component speedup into a VPS capacity
or monthly bill. Changed windows remain O(observations); historical counter rows
are summarized instead of expanded, and no mutable aggregate becomes evidence.

Whole-observation JSON and unsorted native JSON writers were evaluated and
rejected. The final writer preserves canonical key ordering and compression,
avoiding a faster encoder that reduces the shared cache's useful working set.

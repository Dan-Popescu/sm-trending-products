# Controlled measurements of the new research reports

Measured on 2026-10-29 UTC with dedicated synthetic native PostgreSQL schemas,
owned loopback HTTP Moto services and one fresh worker per attribution case.
Regular cases use clean code revision `17c9f3d6333708f4aa22d6a32224eb2664c9fdd0`;
the skew and sparse cases use clean revision
`743e92105ccfb3e496fdb67aca5c542c6e2a1ec7`. The latter fixes the declared skew
coefficient; regular-case behavior is unchanged. The reports retain code/tree
identity, versions, hardware scope, source fixture hashes, actual phases and
cleanup results. No source/provider connection or paid call was made.

```sh
for report in changes progression research; do
  PYTHONPATH=src:tests python scripts/research-workload.py \
    --case "$report" --text-characters 96 --timeout 60 --max-disk-mib 64 \
    --output "/tmp/u04-new-$report.json" || break
done
PYTHONPATH=src:tests python scripts/research-workload.py \
  --case research --shape skewed --text-characters 96 --timeout 60 \
  --max-disk-mib 64 --output /tmp/u04-new-skewed.json
PYTHONPATH=src:tests python scripts/research-workload.py \
  --case progression --samples 2 --text-characters 96 --timeout 60 \
  --max-disk-mib 64 --output /tmp/u04-new-sparse-refusal.json
# The final sparse command must return 1 with status report_refused.
```

The successful cases each admit one product, eight videos with eight metric
points, four offer rows and eight early supplied texts: 68 complete source rows
and 768 text characters. Context explicitly declares source, public-video
distribution, market, language, source kind, known creators and publication time.
The U02 report reaches a measured continuing panel of eight videos over adjacent
24-hour periods, with exact lower/upper pace ratio 2. U03 reaches an actual
`observed_association`: four candidate texts and four absent controls, four known
creator identifiers in each arm, aligned 24-hour durations and coverage fraction
1. Those identifiers do not establish independent people or causal effectiveness.

| Case | Cold wall s | Cold process CPU s | Warm wall s | Warm process CPU s | SQL execute calls cold / warm | Full worker CPU s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [Changes](research-workload-changes.json) | 0.4142 | 0.2101 | 0.2595 | 0.1200 | 551 / 551 | 1.9294 |
| [Progression](research-workload-progression.json) | 0.2586 | 0.1197 | 0.1802 | 0.0683 | 394 / 394 | 1.8694 |
| [Combined new reports](research-workload-combined.json) | 0.5054 | 0.2598 | 0.4183 | 0.1929 | 882 / 882 | 2.0909 |
| [Skewed combined](research-workload-skewed.json) | 0.7327 | 0.3400 | 0.5634 | 0.2031 | 882 / 882 | 2.3271 |

Each regular setup takes 924 driver execute calls, 19 logical object PUTs and
19,186 uploaded bytes. Regular cold execution needs nine GETs totaling 14,874
bytes; warm execution needs zero GETs. The combined case measures its own work,
using its own counters; it is not obtained by subtracting concurrent counters.
All successful cases compare complete canonical cold/warm bytes, hashes and public
IDs. None truncates evidence to achieve a smaller number. Setup, imports and
close/usage flush retain their separate phase/lifetime scopes.

The regular and skewed fixtures have different full source-row hashes. Their
observed `time` arm growth is respectively 20 and 218 views, with 52 in the absent
arm in both cases. Counts, contexts, coverage and complete cold/warm identities
remain intact. This verifies that a declared skew shape actually changes the
workload rather than silently exercising the regular fixture. It is not an
optimization or an estimate of a text's effect.

[The sparse refusal](research-workload-sparse-refusal.json) uses only two points
per video, 168 hours apart (20 source rows). It reads actual supplied text and
history, then records an unavailable comparison and refuses the case: cold wall
0.4104 s, process CPU 0.1704 s, 393 driver execute calls and nine GETs/14,461
bytes. Its complete result hash and public ID remain available for inspection;
no warm execution occurs. Checkpointed usage is complete for this normal refusal.
It must not be compared as a faster equivalent successful report.

All five cases removed their owned schema and files and stopped their Moto
service with no cleanup errors. Endpoint worker RSS after successful execution
was approximately 186–189 MB. These endpoint readings and process lifetime peaks
are not phase peaks. Descendant discovery is incomplete in this environment, so
aggregate session RSS and whole-host/process-tree peaks remain unknown. SQL
counts are `psycopg.Connection.execute` calls, not PostgreSQL CPU or individual
server statements. PostgreSQL, Moto and other host resources remain outside the
worker figures. Sampled disk excludes WAL, SQL temporary files, logs and Moto RAM.

No provider prices were supplied; measured usage stays separate from any optional
dated price scenario. These tiny local synthetic runs justify neither a runtime
optimization, production cost forecast nor target-VPS capacity certificate.
The [operator guide](../operations/research-workload.md) defines workload,
read/size/runtime bounds, safety admission, failure accounting and larger explicit
commands. The [existing-report baseline](research-workload.md) remains separate.

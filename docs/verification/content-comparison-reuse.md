# Bounded pure reuse within one decision comparison

C1 / #185 gives each `DecisionChanges.compare` call one private classification
and script-fingerprint memo. Each brief independently reads and hydrates its
eligible text before consulting the memo. Exact text, exact language (including
variants and unknown language), full canonical taxonomy content and both method
identities form the key. Context, attribution, transcript/source identities and
eligibility are reconstructed from each pass; none is memoized. Separate briefs
and progression operations receive no shared memo.

The memo admits at most 256 entries and 4 MiB of accounted retained objects.
Accounting includes the full text, language, taxonomy and method strings, key and
value tuples, immutable serialized classification, script hash and a conservative
one-entry dictionary allocation per entry. Shared strings are charged again for
each entry. These bounds do not describe total comparison memory or exact RSS.
Pressure declines admission while continuing to calculate and return every fact.
The context manager clears all retained entries and accounting on success and
exceptions, including source/read failures. A hit decodes a fresh result so
mutable returned reports cannot modify the stored classification. Serialization
preserves ordinary JSON insertion order as well as canonical values and IDs.

Regression coverage compares complete aggregate and comparison values, canonical
bytes and IDs with reuse disabled; checks ordinary aggregate JSON ordering;
exercises exact entry/byte boundaries, mutable outputs, Unicode offsets, unknown
and unsupported languages, capped mentions, text changes, language variants,
same-version taxonomy changes and method identities; and checks current owner
withdrawal between native brief passes. The existing content-normalization suite
continues to verify full-tail script fingerprints after the mention cap.

## Matched whole comparison measurements

The [recorded evidence](content-comparison-reuse.json) retains source hashes,
individual samples, public IDs, complete report hashes, cleanup results and the
local measurement harness. The independent control loads the actual content,
brief and changes modules from immutable baseline
`cddaeef8e5d7603083e7315f1c18b371d6550f58`. Candidate measurements use clean
implementation checkpoint `f3c78711d97c6fb2a10ca83877ee140670720fe9`. The baseline
already includes U09's shared classification/fingerprint fold.

Native runs use PostgreSQL 17.11 and owned loopback HTTP Moto state under
`/tmp/night-native-tests.lock`. Each worker has a 60-second runtime limit,
64-MiB owned disk/upload bounds, a minimum 256-MiB memory-headroom guard and
mandatory cleanup. These disk bounds are not worker memory limits. No collector,
source/provider execution or paid analysis runs. Each case admits the same 68
source observations: eight videos with eight metric points each and two offers
with two observations each. Eight eligible texts are read at both cutoffs.

The default workload contains only **two distinct strings**, repeated across its
eight logical texts. Classification calls fall from 16 to 2. That ratio includes
reuse within each brief and must not be generalized to eight distinct texts.
Three fresh-worker pairs alternate baseline/candidate, candidate/baseline, then
baseline/candidate at each size. Medians include the complete comparison worker
phase, with its fixed application and accounting work:

| Characters per text | Phase | Baseline CPU s | Candidate CPU s | Baseline wall s | Candidate wall s |
| ---: | --- | ---: | ---: | ---: | ---: |
| 16,000 | Cold | 0.247698 | 0.177414 | 0.386114 | 0.335034 |
| 16,000 | Warm | 0.223530 | 0.128542 | 0.358425 | 0.250895 |
| 96 | Cold | 0.147032 | 0.156072 | 0.286302 | 0.321246 |
| 96 | Warm | 0.121189 | 0.109816 | 0.260596 | 0.248485 |

The long case has lower CPU in every matched pair, with median reductions of
28.4% cold and 42.5% warm on this repetition-heavy fixture. Short cases overlap:
cold median CPU is 6.1% higher while warm median CPU is 9.4% lower, including a
warm pair that regresses. These observations establish no stable short-text
speedup.

A second variant gives all eight texts distinct fixed-length tails while
preserving four theme-present/four theme-absent prefixes and the same text sizes.
Its classification calls fall from **16 to 8**, demonstrating reuse across the
two cutoffs with distinct supplied texts. Six alternating warm pairs time the
actual whole comparisons in one native application; the cold warmup and separate
classification-count profiling are excluded:

| Characters per text | Baseline CPU s | Candidate CPU s | Baseline wall s | Candidate wall s |
| ---: | ---: | ---: | ---: | ---: |
| 16,000 | 0.199284 | 0.150122 | 0.275627 | 0.235349 |
| 96 | 0.101122 | 0.090471 | 0.178836 | 0.174279 |

The unique long-text median CPU is 24.7% lower, with reductions from 20.7% to
29.4% in all six pairs. Short-input pairs remain mixed: two have 14.1% and 21.4%
higher candidate CPU, despite a lower median. The ordinary-input benefit remains
uncertain. Misses pay bounded key, admission and serialization work; new text,
changed taxonomy/language/method, no overlap or pressure can eliminate reuse.

All fresh timing phases retain 551 driver SQL executes, nine cold object GETs
and zero warm GETs. All runs retain the measured eight-video 24-hour panel and
pace ratio 2. Separate within-namespace control/candidate checks preserve complete
canonical and ordinary JSON bytes and IDs at both sizes, including all unique
variant pairs. The whole changes report set is 10,836 bytes; fresh namespaces
have different provenance IDs and are not used as a byte-equality oracle.

The two-string long case retains two memo entries and 38,888 accounted bytes.
The unique-eight cases retain eight entries: 155,552 bytes at 16,000 characters
and 28,320 bytes at 96 characters. All recorded memos clear on exit. Endpoint RSS
and lifetime peaks do not identify whole-comparison, process-tree or host memory
peaks; no memory-saving claim follows. Worker CPU excludes PostgreSQL and Moto;
SQL counts do not measure database server CPU. These local synthetic observations
do not establish production capacity, profitability, monthly savings or continuous
VPS CPU/RAM.

The scoped native suite passes 83 cases, with 20 followup content reuse and
normalization cases passing after ordinary JSON ordering and exception-release
checks. Final unit coverage passes eight cases. Ruff lint and formatting pass all
356 source/test/script files, and mypy passes all 154 source files. Independent
delivery reviews and final composed CI remain separate requirements.

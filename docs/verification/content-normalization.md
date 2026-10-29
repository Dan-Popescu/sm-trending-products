# Reuse full text normalization within a content aggregate

U09 / #180 removes a measured duplicate fold from supplied-text aggregation.
The supported-language classifier privately returns its full normalized string
alongside the existing classification. Script fingerprints reuse that string;
unsupported languages still receive one fallback fold for fingerprinting. The
public `classify` function returns exactly its previous dictionary. Mention caps
do not shorten the fingerprint input. No cache, phrase cache, public flag, policy
version, admission limit, hydration path or source guard changes.

The [recorded evidence](content-normalization.json) identifies exact source bytes,
individual samples, report hashes and public IDs. The matched control starts at
`3f43c9dc3f4ef0c013163575b3edd1bb4a442b0c` and includes both qualified corrections:
U02 `c241a585cf0c5059c428b3dddf265a1cacfe8937` and U03
`0723e1ed8a1d5dfd5bc10a73037639c070b01712`. Its local revision is
`bceb78c1f0a1388e08e475dd976716535e2160b1`. Candidate measurements use clean code
and test checkpoint `076cad4f847113d9e6c3e70b29250986c962332a`; this documentation
does not affect the measured source bytes. Earlier measurements without the U03
correction are excluded from the reported comparison.

## Bounded native measurements

Measured on 2026-10-29 UTC with dedicated native PostgreSQL 17.11 and owned
loopback HTTP Moto state. Each fresh worker has a 60-second runtime limit,
64-MiB owned disk/upload bounds, a minimum 256-MiB memory-headroom guard and
mandatory cleanup. These disk bounds are not a worker memory cap. Source/provider
execution and paid analysis remain disabled.

For each text size, three pairs alternate baseline/candidate, candidate/baseline,
then baseline/candidate. Every execution admits the same 68 source rows, eight
videos with eight metric points, four offers and eight complete supplied texts.
The 16,000-character case admits 128,000 text characters; the ordinary
96-character case admits 768. Both run the real change and progression reports.

| Text characters | Phase | Baseline CPU s | Candidate CPU s | Baseline wall s | Candidate wall s |
| ---: | --- | ---: | ---: | ---: | ---: |
| 16,000 | Cold | 0.467790 | 0.396530 | 0.730355 | 0.631933 |
| 16,000 | Warm | 0.387910 | 0.317971 | 0.619366 | 0.528362 |
| 96 | Cold | 0.248683 | 0.238028 | 0.491076 | 0.462596 |
| 96 | Warm | 0.170689 | 0.179409 | 0.382501 | 0.401386 |

These are medians, with individual observations retained in the JSON. Long-text
CPU falls in every pair: median reductions are 15.2% cold and 18.0% warm, about
70 ms per warm report set. Short-text results vary: the fresh-worker warm CPU
median is 5.1% higher while cold CPU is 4.3% lower, with overlapping sample ranges.
A separate bounded same-worker check uses six alternating warm pairs through the
same actual whole reports and finds CPU medians 0.148264 versus 0.147240 seconds;
wall medians are 0.270460 versus 0.279188 seconds. This check preserves complete
bytes/IDs and is excluded from the fresh-worker timing table. No short-text
speedup or statistically established small regression is inferred.

Unsupported-language native aggregates also retain complete output equality.
Six alternating pairs at each size give baseline/candidate CPU medians
0.029417/0.028420 seconds at 96 characters and 0.047369/0.046668 seconds at
16,000 characters. Unsupported classification itself still skips normalization;
aggregation retains its one full-input fingerprint fold.

Every fresh timing worker retains 882 driver SQL execute calls per phase, nine
cold object GETs and zero warm GETs. All reach an actual measured eight-video
24-hour panel with pace ratio 2 and a descriptive progression association with
four known creators/videos in each group. Observed growth remains 20 views in
the theme-present group and 52 in the absent group. This is fixture behavior,
not evidence of a beneficial text effect or independent people. All workers
preserve complete cold/warm bytes and IDs and remove their owned schemas/files.

Separate targeted profiling attributes 24 duplicate aggregate folds, scanning
384,000 characters, to 0.0650 CPU seconds: 16.1% of that baseline warm public-report
CPU. The candidate eliminates those folds while retaining all 24 classifier
folds and all 360 short phrase folds. Phrase folding costs about 0.0013 CPU seconds
and is left unchanged. Profiling runs are excluded from the timing medians.

Endpoint worker RSS remains approximately 187–190 MB in both arms. Endpoint and
worker lifetime readings do not establish phase or complete process-tree/host
peaks; no memory-saving claim follows. Worker CPU excludes PostgreSQL and Moto,
and driver execute counts do not measure SQL server CPU. These small local
synthetic measurements do not establish production capacity or cost savings.

## Compatibility and guards

The actual immutable baseline content service is loaded independently and used
against the same native inputs. Complete change/progression results match the
candidate byte for byte, including IDs, in both cold and warm phases at both text
sizes (42,135 bytes per report set). This within-namespace comparison is separate
from timing: fresh namespaces have different provenance identities. It executes
both implementations and is excluded from speed measurements.

A separate native content oracle compares twelve complete bounded pages and IDs,
invalid page-budget refusals and post-withdrawal results. A deterministic corpus
of 149 cases / 115,000 characters matches complete classifier payloads and hashes,
including 875 exact quote spans, Unicode expansion/decomposition, punctuation,
negation, ambiguity, language variants, unsupported/unknown languages and capped
mention scans. Native regressions additionally verify identical full scripts
remain grouped while different 16,000-character tails remain distinct after the
24-mention cap. Withdrawal prevents text from reaching normalization at all.

The native workload command is unchanged. Run in each matched checkout, changing
the text size to 96 or 16000 and retaining the pair order above:

```sh
# Configure only a dedicated loopback TEST_DATABASE_URL and local test credentials.
ACQUISITION_ENABLED=false flock -w 300 /tmp/night-native-tests.lock \
  env PYTHONPATH="$PWD/src:$PWD/tests" python scripts/research-workload.py \
  --case research --text-characters 16000 --timeout 60 --max-disk-mib 64 \
  --output /tmp/content-normalization-run.json
```

The [workload guide](../operations/research-workload.md) defines admission,
resource scope and failure accounting. Timing assertions are absent from CI;
behavioral regressions enforce payloads, offsets, full-tail grouping, source
withdrawal and a full-input normalization work ceiling.

The final corrected-base scoped suite passes 97 native cases in 59.54 seconds.
Full Ruff formatting/lint checks pass (354 files), and mypy passes all 153 source
files. Independent specification and standards review remains a separate
delivery requirement.

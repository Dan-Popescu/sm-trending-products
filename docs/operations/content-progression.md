# Quoted themes and measured video progression

This on-demand report relates reviewed lexical themes in eligible supplied text
to observed video attention. It does not measure causal advertising effectiveness,
sales, profit, or calibrated predictive confidence. It performs no collection,
media download, ASR, LLM execution, paid analysis, or new source access.

Initialize and import authorized text through the existing transcript interface.
Then run the bounded report (the module is also directly runnable):

```sh
python -m sm_trending_products.content_progression_cli --product-id PRODUCT \
  --synthetic --as-of 2026-11-01T12:00:00Z --window-hours 168
```

The public interface shared by CLI and private UI is:

```python
from sm_trending_products.content_progression import ContentProgression

report = ContentProgression(catalog, history, provenance).describe(
    product_id, as_of, synthetic=False, window_hours=168,
    max_texts=200, max_rows=50000, max_strata=128,
)
```

The optional `taxonomy` argument uses the same reviewed `ContentTaxonomy` as
ContentInsights. All its themes remain visible, including unavailable comparisons.
The private `render_patterns(pipeline, products, as_of, synthetic)` entry point
uses French prose, executes only on an explicit button, and stores no source text
or report in session state. Reinteracting drops the prior report. CLI output is
JSON through the shared bounded display-safety adapter: URL normalization and
current-secret scrubbing run before presentation, then final JSON/text sinks are
scrubbed again so formatting cannot restore a credential. CLI and UI refresh the
current secret set on each explicit action. The analytical service still retains
faithful guarded quotations, offsets and identities. Parse and runtime errors contain fixed
codes and hints, without submitted values. `--help` and bounded input validation
do not require runtime configuration. Taxonomy JSON is limited to 128 KiB.
Product-selector labels use a separate current-secret snapshot on navigation,
before any report action. The selector retains canonical IDs for guarded reads;
the report action independently refreshes current secrets before display.
The shared product-label formatter disambiguates redaction collisions without
changing raw IDs; if labels cannot remain safe and distinct, navigation refuses
before any report action.
Presentation decisions use the original typed report, then generated strings and
tables are sanitized at their output boundaries. The sanitized report copy is
used only for technical JSON, so credential filtering cannot rename the fields
used by rendering logic or change the meaning of nulls and numbers. A rendering
failure clears the complete derived output and returns a fixed safe message.

## Population, counts and evidence

The report requires the complete bounded candidate page, at most 200 transcript
metadata rows. If a continuation exists it refuses rather than presenting the
page as the population. Unavailable source rows retain bounded reason references.
ContentInsights resolves each canonical text to its latest eligible context,
preserving exact quotations and offsets. Repeated requests and unchanged metric
refreshes do not create new samples or reserve extraction work.

Different supplied texts for one video/language/source-kind are not assumed to be
verified creative revisions: that video/context is excluded from comparison until
the ambiguity is resolved. Different languages and source kinds remain separate.
Counts are unique videos within each stratum; strata are never pooled into a
single uplift. The same video can appear in distinct source-kind/language strata.

TrendInsights has an additive `include_video_intervals=True` seam. Passing
`video_entity_keys` restricts its one longest-window history read to at most 200
text-selected linked videos. Complete retained rows come from the existing
counter kernel, including revision resolution, bounded differences, corrections,
units, counter scope and period. Existing default TrendInsights output and IDs
are unchanged. Evidence previews never supply the report population. History is
bounded by `max_rows` (at most 100,000 and the existing batch setting).
The final canonical report is limited to 4 MB; excessive output is refused.
Observation references are stored once per video in `video_evidence`, rather
than copied into every language/source-kind outcome.

Every video outcome exposes measured start/end, duration, coverage, interval count,
lower/upper growth, existing kernel rates and full bounded observation references.
Context changes, missing comparable count intervals and conflicting text/metric
creator identities have explicit exclusion counts. Missing evidence is not zero.
Text quotes and lifecycle availability cross the existing source, publication,
namespace and point-in-time ownership guards; the content metadata guard runs
again after the trend read. One shared retention fence spans that final text
check, the bounded observation-delivery check and report finalization, including
older text datasets absent from the measured references. The metric check reuses
the held connection; no additional report fence spans history hydration.
These checks reject evidence retired during composition. Outcome availability uses
the actual committed delivery clock, in addition to the kernel's recorded availability.
Reads never initialize optional SQL modules.

## Comparison rules and limits

Groups share a target window, source, declared distribution context, account, source market,
source kind, language, metric unit/scope/period, creator role and video-age band
(under one day, 1–7 days, 7–30 days, or at least 30 days). Source market does not
establish audience geography. Explicit `unknown`, `mixed`, `unspecified` and
`not_available` distribution declarations and normal missingness aliases such as
`n/a`, `none`, `null` and `not_applicable`, including case/spacing variations,
also remain unknown: matching sentinel labels cannot establish compatibility.
The two-cutoff and quoted-progression reports share this missingness gate without
rewriting source-specific context labels or changing the counter kernel.
Measured per-group quantities remain inspectable while contrasts are unavailable.
Unknown video age,
unknown distribution, unknown creator identity or overlapping named creator IDs
between present/absent groups make comparison unavailable. Creator IDs are scoped
to their source; operator attestations and source IDs do not verify independent
people. Age bands and measured contexts do not remove unmeasured confounding.

For a usable comparison, each group needs at least two videos and two named
creator IDs; every measured video needs at least half the target-window coverage;
the shortest measured duration must be at least 95% of the longest, and measured
endpoint spreads must be at most 5% of the target duration. These are explicit
compatibility rules, not calibrated confidence thresholds. Timestamps need not
match exactly. Counter corrections in the window prevent a comparison. Unknown
or insufficient controls return null contrasts, never zero effect.

Absence requires a completely scanned supported language and no mention of the
theme. Unsupported, incomplete, negated and ambiguous interpretations remain
separate classifications. Mean growth and mean rate contrasts retain lower and
upper bounds. There are no p-values, ranked winning themes or post-hoc selection.

Text available by the measured interval's start is separate from text imported
later. Outcome availability and attested context time remain explicit. A late
import can describe an earlier outcome but cannot establish historical predictive
knowledge. Even the earlier-text scope remains descriptive: no prospective test
or independent performance validation is implied.

Synthetic native tests establish software contracts, not actual source rights,
accurate speech, independent audiences, commercial demand or target-VPS capacity.

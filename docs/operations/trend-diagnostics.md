# Comparable and robust product traction

Use the opt-in diagnostics when investigating whether measured attention is
growing for a continuing set of videos, whether discovery changed the sample,
and whether a single known contributor explains the measured growth.

```python
from sm_trending_products.trend_insights import TrendInsights

report = TrendInsights(catalog, history).describe(
    product_id,
    as_of,
    synthetic=False,
    windows_hours=(24, 72, 168),
    diagnostics=True,
)
```

The result uses `descriptive-trends-v2` and adds `diagnostics`. Omitting the option
retains the existing v1 report, report identifier and ranking behavior. The
diagnostics do not write assessments, collect sources or change ranking policy.

## Compare periods before interpreting growth

Each item in `diagnostics.comparisons` compares two adjacent disjoint periods of
the requested duration. For 24 hours, these are `[as_of - 48h, as_of - 24h]` and
`[as_of - 24h, as_of]`. An interval ending at the shared boundary belongs to the
previous period. No capture interval contributes growth to both periods.

`previous` and `current` report comparable observed growth bounds, measured
entity-hours, linked-video missingness, coverage and counter correction counts.
An entity-hour is one hour actually supported by comparable captures for one
video. Two videos measured for six hours each provide twelve entity-hours. An
unobserved interval contributes neither assumed growth nor measured hours.
An empty growth value is null; it is not zero.

`continuing_panel` includes only videos measured in both periods under the same
counter scope, period, unit and distribution context. Its `panel_pace_lower` and
`panel_pace_upper` sum each video's observed growth divided by that video's own
measured hours. Its pace ratios compare those matched panels. This avoids
interpreting changes in capture duration as changes in pace. Bounded counters
produce bounded ratios. A zero or uncertain previous pace cannot produce an
unbounded percentage: the unavailable ratio remains null and the status explains
why. Ratios describe captured intervals; gaps can still hide a different pattern.

`newly_observed` contains measured videos whose first eligible sample in the
bounded read falls after the shared boundary. An old publication date does not
move their current growth into the continuing panel. This is first observation
within this read, not proof of publication, market entry or global novelty.
`other_current_evidence` retains measured current videos that lack a comparable
previous panel, including changed counter scopes and previously sampled videos
with insufficient earlier captures. Together these three groups partition the
current measured-video contribution.

Captures older than the configured boundary gap at a period's end are excluded.
Intervals crossing a period boundary, exceeding the permitted gap, changing
units/scopes or failing the shared counter kernel's comparability rules are
excluded. Resets and corrections remain counted and do not contribute a negative
or fabricated positive increment. Later valid intervals may still be measured.

## Read contributor stress checks

`creator_robustness` and `verified_creative_robustness` describe the continuing
panel. The largest known identity is selected by current observed lower growth;
the report names the identity and its selection rule. Share lower/upper fields
bound that identity's contribution using the other entities' opposing growth
bounds. Unknown-identity contribution receives its own growth and share bounds.
No share is claimed when all possible measured growth is zero.

`leave_largest_out` removes the selected identity's entire group in both periods
and compares the remaining matched videos. It also excludes unknown identities,
whose independence from the removed group cannot be established. Its measured
growth, hours, pace and ratios show what remains. `stress_check_status` identifies
missing known identities or an empty remaining panel; the nested comparison
status also identifies an unusable previous pace. A successful calculation is a
descriptive stress check, not proof that future traction will persist.

Creator IDs are scoped to their source. An explicit nonempty string is used;
otherwise a known account is a labeled account-identity fallback. Malformed,
missing or changing identities remain unknown. These identifiers do not prove
independent people, and identical strings across platforms are not merged.
Creative grouping requires an explicitly supplied nonempty
`creative_fingerprint`, as in the existing analytical contract. Copied
fingerprints form one group; titles, transcripts and script similarity do not
establish a verified creative identity. Missing or changing fingerprints remain
unknown.

## Preserve distribution and measurement meaning

`source_distribution_contexts` presents separate source, entity type and declared
distribution summaries and matched panels. Missing distribution remains unknown.
Distribution declarations remain declarations; authorized TikTok account access
does not establish organic distribution. No row is labeled proven organic.
Context-specific summaries omit repeated reference previews; bounded evidence
previews remain in the parent period reports.

`diagnostics.source_distribution.contexts` also includes eligible linked ads and
commercial content. Reach is shown separately as its original exact or bounded
measurement, with observation ID and capture/availability timestamps. It is not
added to video views, converted to unique people across platforms, or treated as
a measured interval of video growth. Incompatible view units do not contribute
to count-based growth. Attention is not observed purchases, profit or a
calibrated demand probability; predictive confidence stays null.

## Scope, freshness and resource limits

One product-scoped history read supplies all requested comparisons and legacy
windows. It spans twice the longest requested duration plus the permitted gap;
the defaults read at most 360 hours. The existing row budget defaults to 50,000,
is capped by the configured analysis budget, and accepts an explicit 1–100,000
rows. Exceeding the budget fails instead of truncating evidence. The existing
1–8 unique windows of 1–8,760 hours and gap up to 168 hours still apply.

Global temporal ownership resolution precedes the confidence filter. Every
competing owner for the selected product's candidate keys is considered, while
source objects are read only for the resolved selected entities. Workspace,
market, availability cutoff, source/account capability and source retirement
guards apply before diagnostics. Future permissions cannot grant historical
access. Reports are recomputed on read and are not frozen historical assessments
or authorization to retain withdrawn source evidence.

Identity and capability metadata retain their existing 10,000-item budgets.
Evidence and identity previews contain at most 200 references each, and source
context previews contain at most 200 rows with explicit truncation markers. No
new dependency, permanent service, global object-store scan, media download or
paid API is required. The bounded SQL candidate-enumeration caveat documented in
[trend insights](trend-insights.md) still applies.
# Private operator paths

`sm-trends trend-insights --product-id PRODUCT --diagnostics` requests the
versioned diagnostics explicitly. Without the flag, the existing v1 response and
identifier remain unchanged. All existing cutoff, namespace, row and gap flags
remain available. No source is contacted and no collection schedule is changed.

The private Opportunities view exposes **Compare periods and inspect dominant
contributors** inside the existing trend inspection. Checking it performs no
history read; **Inspect multi-day growth** requests the guarded report. A rerun
does not retain source-derived results in session state. Missing historical
panels and unknown identity/context remain explicit in the displayed diagnostics.

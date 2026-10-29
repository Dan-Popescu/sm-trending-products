# Product-scoped trend insight

`TrendInsights(catalog, history).describe(product_id, as_of, synthetic=False)`
describes the selected product over 24, 72 and 168 hours. Callers may supply
`windows_hours`, `max_boundary_gap_hours`, `min_link_confidence` and `max_rows`.
This service neither changes hourly ranking nor writes a new assessment.

Operator command: `sm-trends trend-insights --product-id PRODUCT --windows-hours 24 72 168`.
Use `--as-of` for an explicit UTC cutoff, `--synthetic` for fixtures, and `--max-rows`
to lower the history budget. The private Opportunities view includes an optional
**Inspect multi-day product growth** control; the bounded read runs only after
the operator clicks **Inspect multi-day growth**. Reports are recomputed rather
than retained in a dashboard cache after source or ownership changes.

One bounded history read supplies every duration. Global ownership rules apply
only to the selected product's candidate entity keys, including every competing
owner for those keys. Confidence is applied after ownership resolution. Evidence
must be available by `as_of`, in the requested workspace, in the product's market
when declared, and eligible under known source capabilities. Revision selection,
counter periods, counter scopes, units and resets use the existing counter kernel.

## Reading a result

- `observed_growth_lower` and `observed_growth_upper` sum comparable cumulative
  **count** intervals wholly contained in that window. They describe measured
  exposure, not unique people or sales. Missing intervals and boundary gaps are
  never filled by interpolation. A reset contributes no growth.
- Coverage measures the fraction of each window actually supported by those
  intervals. The mean includes linked videos with no measured history as zero
  observed coverage. Missing coverage does not mean zero growth. Stale videos
  are excluded. `evidence_quality` reports complete, partial or insufficient
  evidence; `predictive_confidence` remains null.
- Shape requires at least three comparable intervals covering half a window in
  an entity. When growth is positive, adequately sampled entities must account
  for at least half its measured lower bound. Corrections prevent a shape claim.
  Growth-weighted peaks of at least three times each video's own average pace
  produce `concentrated_growth`; otherwise positive growth during at least 75%
  of measured capture time produces `sustained_growth`. These versioned rules
  describe the capture intervals, not the exact timing inside an interval.
- Creator counts accept nonblank string creator identifiers, with a nonblank
  string account identity as the fallback. Surrounding whitespace is removed;
  malformed JSON values remain unknown unless that valid fallback exists.
  Identifiers are grouped by source, identifier kind and value: a creator and
  an account with the same local ID remain distinct, as do IDs from different
  sources. These counts do not establish independent human identities, even
  within one source. A verified creative means an explicitly supplied nonempty
  `creative_fingerprint`, consistent with the existing analytical contract.
  Copied fingerprints count once; missing fingerprints stay unknown. Separate
  growing counts exclude creators and creatives with no observed lower growth.
- Reasons and bounded references explain the result. Identity and report hashes
  identify the selected decisions and the descriptive result. These ephemeral
  reports are recomputed on read and are not frozen historical assessments.
  Existing ranking supplies its own market/age cohort context. No category
  percentile or sales probability is inferred without reviewed comparison data.

Assessment features, trend windows and optional diagnostics share that validated
creator extraction. This corrects earlier source/kind collisions and malformed
identifiers in newly computed results. Disposable computed features use
`exact-product-features-v4` to reject old cached counts; raw facts retain
`exact-raw-window-v1` and can be reused without a source payload read. Stored
assessment publications and frozen evaluation decisions retain their original
values and content IDs. Policy defaults and trend response schemas are unchanged;
new result IDs change when corrected content changes. Diagnostic identity labels
retain `source:kind:value`, with `%` and `:` inside parts escaped as `%25` and
`%3A` so embedded delimiters cannot merge labels.

## Cost limits

The defaults allow 50,000 observations, further limited by the existing configured
batch budget. An explicit budget may be between 1 and 100,000. Oversize reads fail
instead of silently truncating. There may be 1–8 unique integer windows of 1–8,760
hours; a capture interval and latest-capture age may be at most the selected gap
(default 24 hours, configurable up to 168). Identity candidate keys and source
capability metadata each have a 10,000-item budget. Evidence/identity previews
contain at most 200 references and exclusions at most 20; original lake evidence
is unchanged. CPU and memory remain proportional to selected-product history,
not to the complete product lake. No media download, paid API or GPU is used.

The base schema currently indexes identity history by entity key, not product ID.
Candidate enumeration can therefore scan SQL link metadata. The expensive owner
window sorts are confined to the selected candidate keys. A local native
PostgreSQL check with 100,000 decisions and 1,000 products confirmed an entity-key
index scan for this restricted owner resolution; the unscoped alternative sorted
all 100,000 decisions. Adding a product-key index is a separate base-schema change.

## Opt-in launch delays measured in hours

Supply, for example:

```json
{"horizon_days": [3, 7], "launch_delay_hours": [12, 24, 48]}
```

Use either `launch_delay_hours` or `launch_delay_days`, never both. Hour delays
are unique integers from 0 through 8,760, with at most 24 values. The outcome still
ends at decision time plus `horizon_days`; it starts at decision time plus the
chosen hours. A delay at or beyond that horizon is `not_actionable`. Frontier due
dates, censoring, frozen ownership and evidence availability retain their existing
meaning. All scenarios for a product still reuse one outcome-history read.

Legacy configurations omit the new field completely. Their serialized snapshots,
reports and identifiers are unchanged. Hour configurations omit the day field and
label scenario/comparison/lead-time delay values explicitly in hours. Older
application versions cannot read this new opt-in contract: upgrade analytical
readers before writing hour-based decisions. This does not require modifying or
restarting independent capture.

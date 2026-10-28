# Reviewed category and target-market comparisons

`CohortInsights(catalog)` owns the optional `product_cohorts` module. Run
`initialize()` explicitly; importing or reading reports never installs tables.
Capture SQL remains version 40. Categories describe physical-product types;
families describe related or substitutable products. They are separate decisions.

Use `save_taxonomy(taxonomy_id, categories, expected_revision, request_id,
reviewer=..., rationale=...)` with a complete list of `category_id` and `name`
objects. Use `assign(product_id, taxonomy_id, category_id, expected_revision,
request_id, taxonomy_revision=..., synthetic=..., reviewer=..., rationale=...)`
to record a reviewed assignment. A null category withdraws the assignment.
Every write has immutable revisions, server-recorded availability, optimistic
revision checks and request replay. Reusing a request with changed content fails.
A taxonomy change requires reviewing assignments against its new revision.

`compare(as_of, synthetic, taxonomy_id=..., category_id=None, country=None,
metric="attention_score", max_products=1000, min_peers=3,
max_assessment_age_hours=72)` returns a bounded observed-panel report. Supported
metrics also include `growth_per_hour`, `view_count`, `demand_score`,
`observed_seller_count` and `observed_advertiser_count`. The candidate budget is
an admission limit: exceeding it fails, rather than ranking a truncated panel.

The input is the compact current assessment projection. The report performs no
raw-history read and never rebuilds old cohorts from today's current pointers.
Assessment publication, namespace and retirement guards remain authoritative.
A pointer computed after the cutoff is unavailable, and old pointers become
stale. The report preserves missing assessments and categories as explicit gaps.
`eligibility_scope="assessment_cutoff_eligibility"` means source eligibility was
evaluated when each immutable assessment was computed. The report exposes the
requested cutoff and each assessment cutoff; it does not claim a live source-grant
reassessment or recompute old metrics after a capability change. Current source
withdrawal, namespace and evidence-retirement guards still apply.

Country means **declared product target market**. Viewer country remains unknown.
French text or a France target does not turn global views into France demand.
Source and distribution are labelled `assessment_aggregate_mixed_or_unknown`;
this report cannot establish channel-specific peers or audience coverage.

Peers share category revision, target country, age bucket, full assessment policy
(including lookback window), metric/unit and assessment cutoff. Each known family
has one representative chosen by product ID, independently from metric value.
A product's own family is excluded. Unknown families remain explicit.
The descriptive percentile is the fraction of independent peers below the
product plus half the fraction tied with it. Groups with too few peers, all-tied
metrics, bounded growth, unknown countries or unknown ages receive no percentile.
Even an available percentile has `comparability="limited"`: it describes this
observed panel, not a market-wide percentile, sales probability or saturation.

Original counter units are absent from compact assessment projections. Raw
`growth_per_hour` and `view_count` values therefore have unit `uncertified` and
receive no percentile. The default uses dimensionless versioned assessment-score
points, always labelled as limited comparison of the existing policy's outputs.
Named seller/advertiser cardinalities are counts. This does not change assessment
formulas, old hashes or ingestion; certifying original source units in a compact
projection is deferred.

## Operator commands and private view

The `sm-trends research cohorts` command uses PostgreSQL only. It does not create
an object-store client or read history files. Initialize the optional module
explicitly, then save a reviewed taxonomy and assignments:

```bash
sm-trends research cohorts init
sm-trends research cohorts taxonomy-save --taxonomy-id physical --file categories.json \
  --expected-revision 0 --request-id taxonomy-1 --reviewer operator \
  --rationale 'Reviewed physical-product categories'
sm-trends research cohorts assign --product-id LAMP --taxonomy-id physical \
  --category-id lighting --taxonomy-revision 1 --expected-revision 0 \
  --request-id lamp-category-1 --reviewer operator --rationale 'Reviewed specification'
sm-trends research cohorts show-taxonomy --taxonomy-id physical
sm-trends research cohorts compare --taxonomy-id physical --country FR \
  --metric attention_score --max-products 1000 --min-peers 3
```

`categories.json` is a regular JSON file of at most 128 KiB, containing
`[{"category_id":"lighting","name":"Lighting"}]` or the same list under
`{"categories": ...}`. Duplicate category identifiers and unknown fields fail
before settings or database access. To withdraw an assignment, replace
`--category-id lighting` with `--withdraw`, supply its current expected revision
and use a new request ID. Identical successful requests can be replayed safely;
changed content under an existing ID or stale revisions fail. `assign` and
`compare` accept `--synthetic`; real and synthetic assignments remain separate.
Read commands accept `--as-of` with a timezone. Exit status 0 means success;
2 means rejected input or unavailable operation. Errors omit raw authored input.
Every subcommand has a help example and never prompts interactively.

The private view exposes the same comparisons and explicit initialization,
taxonomy review and assignment controls. Expected revisions and request IDs
remain visible; a conflict never silently advances the revision. Work begins
only when its button is pressed. Reports are not retained in session state;
each inspection checks current retirement and namespace guards. The table
labels unknown audience country, uncertified counter units and unavailable
percentiles alongside the evidence cutoff.
Tables, revision details and product labels use a sanitized display copy that
removes configured secret values, authentication tokens and URL credentials;
the reviewed taxonomy and assignment records remain unchanged.

# Selected-product changes between two cutoffs

The change report explains two fresh decision briefs for one product, one explicit
target country and strictly chronological evidence cutoffs. It runs only on
request, through the CLI or private research view. It creates no report history,
source collection, paid interpretation or commercial action.

```bash
python -m sm_trending_products.decision_changes_cli --help
python -m sm_trending_products.decision_changes_cli --examples
sm-trends research changes --product-id LAMP --target-country FR \
  --previous-as-of 2026-10-27T12:00:00Z --as-of 2026-10-28T12:00:00Z
sm-trends research changes --product-id LAMP --target-country FR \
  --as-of 2026-10-28T12:00:00Z --format text
```

The default earlier cutoff is exactly 24 hours before the current cutoff; the
output always contains both resolved timestamps. An omitted current cutoff means
current UTC. The private view displays both cutoffs and permits editing the first.
The target country describes the intended market, never measured audience
geography. `--synthetic` explicitly selects the rehearsal namespace.

Both selections default to the same empty optional context. Supply reviewed
selection JSON with `--previous-selection` and, when different,
`--current-selection`. Omitting the second file reuses the first selection.
Each file must be an object no larger than 128 KiB. Unknown fields, incomplete
commercial selections, invalid revisions and malformed scopes fail before
environment configuration or service construction. Parser errors never echo
arguments or file content. Help and examples require no credentials.

```json
{
  "dossier_id": "LAMP-FR",
  "expected_revision": 3,
  "scenario_profile": {
    "scenarios": [{"name": "reviewed", "ad_cost": "5"}]
  },
  "source": "fixture_import",
  "query_scope": "reviewed-observed-query"
}
```

Other selection fields are `taxonomy_id`, `category_id`, `matching_profile` and
`content_taxonomy`, using the existing decision-brief contracts. The report keeps
exact dossier/revision/category identifiers and profile/context fingerprints,
without repeating full configuration or source text. Selection differences are
reported separately from measured behavior.

`DecisionChanges.compare(product_id, target_country, previous_as_of, as_of,
synthetic=False, *, previous_selection=None, current_selection=None,
max_rows=50000, max_products=1000, text_limit=100)` is the shared core. It delegates
to DecisionBrief and the existing trend, competition, cohort, content, matching
and economics modules; it does not rescore products or recompute prices/counters.
The CLI JSON is its compact payload; the French text CLI and private view share
the pure `explain_changes` reading path. Technical codes remain in private details.

The trend kernel compares continuing videos in adjacent disjoint windows of 24,
72 and 168 hours. A requested comparison is measurable only when its cutoffs
align with such a window, the earlier measured baseline has complete references
and remains unchanged, the kernel has a compatible continuing panel and known
distribution context, and corrections do not invalidate the comparison. Other
distribution declarations such as `unknown`, `mixed`, `unspecified` and
`not_available`, `unknown_distribution`, `unavailable`, `n/a`, `none`, `null` and
`not_applicable` remain unknown after case and whitespace/underscore/hyphen
normalization (including `not available`);
they cannot make the comparison measurable. Their interval quantities remain
visible. This comparison gate does not change existing trend-kernel outputs.
The shared `distribution_context.known_distribution_context` gate supplies this
missingness vocabulary for U02 and U03 without a whitelist of valid distributions.
Other requested cutoffs still return their observed quantities, with unavailable pace
comparison. Ratios are copied from the kernel, never calculated from changed
report hashes or aggregate-window subtraction. Truncated baseline references
cannot prove comparability. Newly observed videos stay distinct from continuing
growth; source publication dates never establish earlier system availability.

Economic facts and assumption-profile changes remain separate. Current quote
readiness includes its actual evaluation time and is never reconstructed at the
evidence cutoff. A replaced historical dossier revision is unavailable through
the public dossier boundary; the report does not read old revision SQL or invent
its facts. An unavailable current cohort projection likewise stays unavailable.
Observed competition counts and price-change counts cover sampled candidates;
they do not establish entry, departure, saturation or whole-market growth.

One namespace/retention fence spans both sequential briefs and final composition.
The existing brief readers recheck source eligibility. No quotation, source title,
supplier note, complete report copy or executable plan enters the difference
payload. The private renderer stores no report in session state and clears its
output on every interaction, including failed actions. Presentation refreshes
current secret redaction; original evidence-owning reports remain untouched.
The new change-report adapters share `display_safety`: they refresh API/access
keys, credentials, DSNs and other current secret values on every requested action,
sanitize copied JSON and generated prose, and scrub the final representation.
JSON syntax is checked before output. This display boundary leaves collector
redaction and existing default raw brief payloads unchanged.
Product selector labels also use current-secret sanitization during navigation.
Their raw selected identifiers remain unchanged for guarded report actions, which
refresh the current secret values again before presentation.
The shared `product_display.product_labels` policy disambiguates redacted label
collisions with sanitized ordinals and refuses remaining ambiguity. The stable
widget key preserves the original selected product across credential rotation.

Default admission allows at most four product-scoped history reads, each at most
50,000 rows, two cohort panels of at most 1,000 compact product projections and
two text pages of at most 100 entries. Existing module metadata, matching and
source-object budgets still apply. CLI maxima are 100,000 rows per component,
10,000 cohort products and 200 texts per cutoff. Compact previews contain at most
20 entries and the complete output refuses more than 128 KiB. Native tests prove
composition fits the default four-connection pool. These are application bounds,
not claims about actual market coverage or production VPS capacity.

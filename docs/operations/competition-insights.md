# Observed competition and price pressure

`CompetitionInsights(catalog, history).describe(product_id, as_of, synthetic,
family=False, country=None, source=None, query_scope=None, window_days=30,
max_age_hours=72, max_rows=50000)` reads one bounded product/family history window
and reuses it for seller, advertiser, offer, ad and retail-price diagnostics.
This module is read-only and needs no additional SQL initialization.

The report preserves global ownership resolution, known-at-cutoff identity and
family history, source/account grants, source withdrawal, retention and namespace
guards. It admits at most 1000 family products, 10000 identity metadata rows,
200 declared scopes and the configured history-row budget. Exceeding a budget
fails; it does not silently rank or compare a partial input. Output previews
have counts and truncation flags. The longest history window is read once.

Comparable observations explicitly supply all of:

- `market`: an uppercase observed country code, such as `FR`;
- `attributes.query_scope`: the stable query/filter identity;
- `attributes.coverage_scope`: the stable sampling, pagination and coverage-policy identity.
- `attributes.distribution`: an explicit printable context identifier of at most
  128 characters, such as `offer_listing` or `advertisement`.

A product target country, French text, requested filter or later query cannot
fill missing observed-country or collection declarations. The current native
collector does not provide these query/coverage declarations automatically.
Such evidence produces `not_comparable`, preserving the missing context instead
of reporting zero competition. Imports may carry declarations with provenance;
a declaration does not establish full market coverage.
An offer/ad entity type does not establish its distribution context. Missing,
malformed, overlong or explicitly unknown distribution remains unknown and
prevents comparable competition and price intervals.

Scopes remain separate by observed country, source/account, distribution, query
and coverage policy. Timelines show newly observed and continuing **observed**
identities within the report window. Their first capture is not market entry.
Capture counts are observations of linked candidates, not a complete query census.
Changed coverage starts another scope and breaks cross-context intervals.
Missing, old or uncaptured entities are not departures. `competition_growth`
and `market_saturation_percent` remain unknown. Observation age, actual capture
gaps, unknown identities and stale entities remain visible.
Missing source account IDs remain null with `source_account_scope="unknown"`.
An explicit query/coverage contract may still support a source-wide comparison;
account independence is never established by this report.

Price intervals reuse the existing exact-decimal/bounded quote kernel. An offer
must have matching explicit `currency`, `variant_id`, `price_basis`, query,
coverage, country and the following attributes at both ends:

- `tax_basis`: `tax_included`, `tax_excluded` or `not_applicable`;
- `shipping_basis`: `shipping_included`, `shipping_excluded` or `not_applicable`.

The comparison concerns the declared displayed-price basis. For example,
`shipping_excluded` prices do not establish delivered cost. No currency conversion,
tax estimate, supplier-margin estimate or precise price-change time is invented.
Different variant, currency, tax, shipping or price bases break the interval.
Unknown quotes and intervening scope changes also break continuity. Corrections
available later replace an earlier point only at the later knowledge cutoff;
revisions do not add price-change intervals. Simultaneous conflicting revisions
suppress the affected offer's price claims.
Every retained interval carries its own `price_context` (currency, variant,
price basis, tax basis and shipping basis), plus `collection_scope_id` linking
its unchanged country/source/query/coverage/distribution/account context to
the enclosing scope. The offer-level `price_context` is explicitly labelled
`price_context_scope="latest_valid_quote"`; it does not describe older intervals.
For EUR 20 → EUR 18 → USD 30 → USD 25, the report retains a EUR 2 decrease
and a USD 5 decrease, excludes the currency transition and never adds these
amounts together. Counts of compatible changes may cover multiple contexts.
Returning to an earlier variant or basis starts another adjacent interval;
it never bridges across the intervening incompatible quotes.

## Operator command and private view

Inspect one product, or explicitly its known family, on demand:

```bash
sm-trends research competition --product-id LAMP --country FR \
  --query-scope reviewed-query-v1 --window-days 30 --max-rows 50000
```

Optional flags are `--source`, `--family`, `--as-of` (timezone required),
`--synthetic`, `--max-age-hours` and `--min-link-confidence`. The row budget is
1–100000, the window is 1–366 days, the capture-age/gap limit is positive and at
most 8760 hours, and link confidence is between 0 and 1. Invalid bounds fail
before a pipeline, database or object-store client is created. `--help` works
without infrastructure and includes an invocation example. Successful output
is the same bounded JSON report as the public service; exit 2 indicates rejected
input or unavailable inspection, including a row-budget failure. Raw invalid
arguments and source text are not copied into errors.

The private view has the same filters and limits and performs no history read
until `Inspect observed competition` is pressed. It displays observed context,
unknown source accounts, stale entities, capture intervals and compatible retail
price intervals. Unknown competition and saturation remain explicit. Missing
scope fields cannot become facts through filters. No report is stored in session
state: a new inspection rechecks source withdrawal and retirement instead of
showing a previously assembled report.
The complete report is sanitized before rendering any scope, timeline or price
table as well as JSON details. Configured secret values, authentication tokens
and URL credentials are removed only from the display copy.
The private price-interval table shows each interval's own currency, variant
and price/tax/shipping bases. It previews at most 200 intervals per scope and
explicitly marks truncation; offer totals retain the complete bounded counts.

# Private product decision brief

`DecisionBrief(catalog, history, provenance).describe(product_id, target_country,
as_of, synthetic=False, ...)` builds one report on request. It does not save a
second analytical history or launch a commercial action. CLI and the private
dashboard call this same interface.

The target country is explicit and does not establish the audience's location or
local demand. A product with a different declared target market keeps that fact
visible. The brief never relabels its attention evidence as country-specific
views. Real and synthetic products cannot cross workspaces. Products, source
evidence, ownership, category revisions and supplied texts must be available at
the chosen cutoff. Retirement overrides historical access.

## Sections and exact selections

`sections` contains `trends`, `competition`, `comparables`, `themes`, `variants`
and `economics`. Each has `status`, a safe `reason` and its authoritative module
`report`, or null when unavailable. An uninitialized module is explicitly
`not_initialized`; a missing selection is `not_requested`. Reading a brief never
initializes optional SQL or replaces an unavailable result with an empty success.

- Trends use the opt-in disjoint-period diagnostics for 24, 72 and 168 hours.
  Their complete original report, identity version, observation references,
  measured periods, missingness and contributor-removal checks remain inspectable.
- Competition uses one selected product's bounded 30-day history and the explicit
  target-country filter. `source` and `query_scope` can narrow it further. A target
  filter does not supply missing observed geography, distribution or coverage.
  Incomparable scope remains unavailable inside the report.
- Comparables require `taxonomy_id`; `category_id` optionally narrows the panel.
  The compact panel is computed once. The brief includes its selected-product
  row, taxonomy revision, panel coverage and original `panel_report_id`, plus
  a new report ID for that projection. A missing selected row stays explicit.
  Scores retain the underlying assessment's limited comparison scope.
- Themes use eligible supplied text and a reviewed `content_taxonomy`, defaulting
  to the versioned EN/FR taxonomy. Review after the cutoff makes that taxonomy
  unavailable. Quotations retain offsets and provenance; seller persuasion is
  not measured buyer intent. Partial scans and unsupported languages stay gaps.
- Variants require an explicit `matching_profile`. The bounded matching report
  separates linked and unlinked candidates. A conflicting candidate is a review
  concern, not proof that the canonical product has that variant or is unsuitable.
  Matching uses eligible current metadata rather than reconstructed old metadata.
- Economics require all of `dossier_id`, `expected_revision` and
  `scenario_profile`. The selected dossier must belong to this exact product,
  workspace and target country. A stale revision fails; a current revision saved
  after the brief cutoff is unavailable. There is no search of the first dossier
  page or implicit choice of supplier, country or cost profile. Exact source
  facts, quote readiness, scenario/profile hash, decimal breakpoints and cash
  assumptions remain in the report. Evaluation time is current, separately
  visible from the evidence cutoff; an expired quote cannot regain readiness
  through a historical brief.

`selection` retains the complete supplied scenario, matching and content-taxonomy
configurations and their hashes even when an optional report is unavailable.
Distinct assumptions therefore cannot collapse to the same brief identity merely
because commercial tables were not initialized. Configuration copies are bounded
operator assumptions, not additional collected source history.

## Navigation and useful next facts

`support`, `contrary` and `missing` contain explicit codes and report references.
`next_useful_facts` gives at most eight deduplicated dimension-level actions,
ordered for reviewing commercial assumptions, identity, trend evidence,
comparables, text, competition, purchases and audience geography. This fixed
review order is transparent; it is not a learned value-of-information estimate.

`status_policy.version="review-navigation-v1"` defines three navigation states:

- `watch`: no positive lower-bound measured growth in the observed windows.
- `investigate`: positive observed growth, without a currently ready selected
  dossier whose complete supplied scenarios meet its declared profit target.
- `ready_to_review`: both of those facts are present. This means an operator can
  inspect a prepared case. Missing cash timing, absent audience data, concentration,
  contrary evidence and commercial uncertainty still apply. It neither approves
  a launch nor identifies a proven winner.

All states retain null predictive probability, `launch_authorized=false`, and
explicit missing real purchases/product-market-fit evidence. Defining a future
commercial test is a recommendation to obtain evidence, not its execution.
Each complete scenario that misses its declared target remains contrary evidence,
even when other scenarios are incomplete. The latter still contribute missing
information; they neither hide a calculated downside nor become assumed losses.

Text support requires actual candidate mentions from the authoritative quoted
content report. It reports their mention count and the number of distinct supplied
texts containing them. Merely available texts, complete scans with no mentions
and unsupported languages cannot create positive theme support. Availability
without interpreted candidate mentions is a missing-information note, without
inferring an absence of demand. Negated and ambiguous mentions retain separate
counts and context-review notes, including when candidates coexist in the same
text. Exact quotes, offsets, source eligibility and language gaps stay in the
underlying content report. No mention establishes the claimed product benefit,
buyer demand, sales or a commercial recommendation.

## Work and storage bounds

Defaults are at most 50,000 history rows per component report, 1,000 compact
cohort products, 100 supplied texts, 100 matching candidates and 20 matching
results. Hard maxima are 100,000, 10,000 and 200 for the first three budgets.
History's configured batch cap may lower the effective row admission. Trend
diagnostics reuse one longest-history read; offer/advertising competition uses
its own bounded selected-product read. No raw-history read runs once per peer.

A namespace and retention read fence spans composition, so retirement cannot
remove one component midway and leave its earlier quoted text in another.
The existing default four-connection catalog pool accommodates the bounded
module calls; the brief itself holds one connection during composition. Reads
are sequential, without an additional background worker. No source payload or
derived report is retained in the service or dashboard session. Every refresh
rechecks guards. A saved exported JSON is a point-in-time artifact, not an access
token: its references must pass current guards before retrieving source content.

Native acceptance and source-withdrawal scenarios are in
`tests/test_decision_brief.py`.

## French reading view and explicit exports

The private selected-product view presents the same guarded brief in French.
It explains the navigation state, target country, observation cutoff, exact
selected dossier revision, measured bounds and units, support, contrary evidence,
missing facts and useful next checks. Economic contributions remain declared
per-order scenarios, including known adverse results beside incomplete scenarios.
Quote readiness keeps its current evaluation time separate from the cutoff.
Overlapping aggregate windows are displayed as quantities rather than an invented
time series. Video views and advertising reach are never added together.

The pure interface is `present_brief(report, secret_values=())`, returning a
`BriefPresentation`. `render_text(view)` and `render_html(view)` produce standalone
French exports; the private UI consumes the same view. It performs no source reads,
analysis, writes or environment access. Adapters refresh runtime credential values
and apply redaction on each requested action before presentation. Copies strip
URL credentials, sensitive query fields and secret-bearing metadata. Query keys
and values are scrubbed after decoding and before URL encoding. Redaction is
reapplied after URL normalization and generated human prose; renderers accept
the same explicit `secret_values` for the final text/HTML output boundary. A
redaction marker cannot introduce a bound secret. HTML escapes
all source-derived strings and includes inert, collapsed technical JSON. Sanitizing
a copy never rewrites the analytical report or an executable plan.

Research adapters share `display_safety.sanitize_display(value, secret_values=...)`
and `scrub_display_text(text, secret_values=...)`. Only the explicit adapter reader
`current_display_secrets()` accesses the environment. Sensitive numeric, boolean
or null echoes become redacted strings before JSON serialization; technical UI
details receive the same protection as prose. The brief keeps its original
2 MiB/40,000-node presentation limits within the shared helper's 4 MB/200,000-node
ceiling.

Technical JSON widgets receive `display_safety.display_json(...)` as a final
string, so a widget cannot recreate a current credential by escaping Unicode
markers. The helper uses literal Unicode, reapplies current-value scrubbing after
JSON encoding, validates syntax and bounds final bytes. Its result is a display
copy; executable-plan equality and export refusal remain the caller's responsibility.

The product selector masks current credentials before any analysis runs, while
retaining the exact canonical ID for an explicit build. Refreshing its safe labels
preserves the selected product. Build-time credential redaction refreshes again
after the guarded report read.
All research selectors share the bounded `product_display.product_labels` mapping.
Masked labels remain distinct, while its map keys retain exact canonical IDs.
If safe disambiguation is impossible, the view refuses selection before analysis.

Presentation reads section keys, null availability and status codes from the
authoritative structure. It then sanitizes generated prose values and an
independent technical copy. A credential that happens to match a structural key
or scalar cannot hide contrary evidence, change readiness or cause a routing
exception.
Human adapters pass the original structure to presentation; legacy collector
redaction still applies to the unchanged default raw-JSON branch. The shared
display sanitizer retains bare credential-assignment masking after prose
composition, alongside URL, metadata and current-value protections.

Use the opt-in format flag and shell redirection for an explicit local export:

```bash
sm-trends research brief --product-id LAMP --target-country FR --format text
sm-trends research brief --product-id LAMP --target-country FR --format html > brief.html
```

The default `--format json` retains the existing canonical raw JSON and analytical
identifiers. Human presentation refuses inputs exceeding 40,000 nodes, 32 nested
levels or 2 MiB of string data, and exports exceeding 2 MiB. It never silently
truncates missing or adverse evidence to produce a successful export. Unknown
future codes receive an explicit uninterpreted explanation; their original codes
and references remain in secondary technical details.

No report runs merely by navigating. Every interaction clears the displayed
derived report; no report or source quotation is stored in session state. A new
explicit build calls the guarded service again, so withdrawn sources cannot be
recovered through a retained first report. Existing exported files remain
point-in-time private artifacts and do not grant access to source references.

Native, installed CLI, escaping, credential, language, mixed-scenario and private
view regression coverage is in `tests/test_decision_brief_presentation.py` and
`tests/test_decision_research_operator.py`.

## Operator entry points

`sm-trends research --help` discovers the optional commands without opening a
database or object store. Each command owns its configuration-free help and
bounded input validation. For example:

```bash
sm-trends research brief --product-id LAMP --target-country FR
sm-trends research brief --product-id LAMP --target-country FR \
  --dossier-id LAMP-FR --expected-revision 3 --scenarios /private/scenarios.json
sm-trends research validation --help
sm-trends research cohorts --help
sm-trends research economics --help
sm-trends research competition --help
sm-trends research content --help
sm-trends research select-texts --help
```

Use `--synthetic` to select the isolated simulated workspace. A historical cutoff
can be supplied with `--as-of`; it never restores withdrawn content. JSON files
must be regular files containing objects no larger than 128 KiB. The dossier ID,
expected revision and scenarios are supplied together. Category selection requires
the taxonomy ID. Invalid arguments and configuration failures return safe JSON
errors without repeating supplied secrets. Inspection neither initializes a module
nor runs a source request.

The private dashboard's **Decision research** view exposes the same modules. Select
a report, provide its explicit inputs and click its action button. Navigation
alone does not start analysis or initialize optional tables. The decision brief
uses the explicit target country and optional exact dossier selection. Displayed
reports are sanitized against current credentials and cleared on every new
interaction; no source quotes or derived reports are kept in session state.

The brief, changes and quoted-content adapters share the transport-neutral
`json_input.object_from_bytes` decoder. It admits at most 128 KiB of encoded
bytes before JSON parsing and requires an object; each adapter keeps its own
model validation and public input errors. The existing CLI parser aliases remain
available. Dashboards interpret the original typed report before sanitizing
displayed tables, prose and details. Selected research and commerce detail widgets
receive final `display_safety.display_json` strings, with current credentials
checked again after JSON serialization. Display copies never replace report
identities, action inputs or executable plans. Collector redaction and default
machine JSON outputs retain their existing behavior.

`Pipeline` supplies lazy factories for these optional modules. No factory adds a
permanent worker, source request or scheduled full-universe calculation. Cohort
and economic CLI operations construct SQL-only services and do not require S3.

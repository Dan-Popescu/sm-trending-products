# Governed product-selection research

`ResearchValidation(catalog, history, identity)` is an optional module. Initialize
it explicitly with `initialize()`; protocol and attempt reads never install SQL.
It uses the existing assessment, point-in-time ownership, frozen evaluation and
outcome kernels. It does not collect evidence, fit a model or launch a product.
Free-form `Pipeline.backtest` remains available with its original semantics.

## Protocol and execution

Create `ValidationProtocol` from JSON, then call:

```python
research.register(protocol, "register-pilot", synthetic=False)
research.run(protocol_id, "development", "attention", "development-attention")
research.lock(protocol_id, "attention", "select-attention")
research.run(protocol_id, "test", "attention", "test-attention")
research.run(protocol_id, "holdout", "attention", "holdout-attention")
research.attempts(protocol_id, limit=100, offset=0)
```

Every run actually computes over its declared product/date subset. All policies
must be registered before execution. Test and holdout accept only the immutable
selected policy, locked before either partition is opened. Development may compare
the bounded registered candidates; no automatic fitting or winner selection occurs.
Changing a protocol creates an explicit next revision with `previous_protocol_id`;
an opened test/holdout prevents revision of that protocol lineage. Changing a lock
requires a new protocol rather than overwriting its choice.

The first admitted test/holdout attempt freezes its entire named lineage to that
protocol, under one transaction lock shared by registration, policy locking and
attempt admission. Already registered ancestors or descendants cannot open a
different revision's heldout partition afterward, even when their policy locks
predate the opening. The owning protocol may replay its requests and continue its
declared heldout partitions. Rejected cross-revision attempts remain recorded.
Optional module migration 2 installs this fence and backfills prior openings;
an older initialized module requires the explicit init command before use.

Example profile (replace product IDs with existing reviewed products):

```json
{
  "name": "physical-products-pilot",
  "revision": 1,
  "config": {
    "target": "attention",
    "threshold": 1000,
    "top_k": 3,
    "horizon_days": [3],
    "launch_delay_hours": [12, 24, 48],
    "max_boundary_gap_hours": 24
  },
  "policies": {
    "attention": {"version": "attention-v1", "weights": {"attention": 1}},
    "attention-demand": {
      "version": "attention-demand-pilot-v1",
      "weights": {"attention": 0.7, "demand": 0.3},
      "missing_data": "exclude"
    }
  },
  "partitions": {
    "development": {
      "product_ids": ["development-product"],
      "decision_times": ["2027-01-01T00:00:00Z"]
    },
    "test": {
      "product_ids": ["test-product"],
      "decision_times": ["2027-01-06T00:00:00Z"]
    },
    "holdout": {
      "product_ids": ["holdout-product"],
      "decision_times": ["2027-01-11T00:00:00Z"]
    }
  },
  "measurement_sample_size": 2,
  "seed": 42,
  "max_attempts": 32
}
```

All three partitions are mandatory. Dates are unique, chronological and separated
by at least the largest outcome horizon plus boundary grace, even within one
partition. Products cannot cross partitions. The registry retains all known
historical family labels and confirmed entity ownership: transfers cannot erase
an overlap. Conflicting family assignments at one timestamp reject. Unknown
families use separate namespaced product fallbacks; this does not prove that the
products are truly independent. Every new run rechecks later available family and
entity decisions before computing.

For development and test runs, outcome evidence availability is capped strictly
before the next partition's first decision, including when the requested run
clock is later. The response distinguishes the attempt's `as_of` clock from
`evaluation_cutoff`. Corrections learned during test cannot leak back into
development outcomes, and holdout corrections cannot alter the test comparison.

New profit protocols require the existing complete `config.profit_contract` and
an explicit `profit_contract` for each partition's declared reviewed ledgers.
They use actual outcomes through the existing kernel, without allocating sales or
profit to unlaunched products. Measurement sampling requires the attention target.

## What the lock establishes

The response includes actual `registered_at`, `locked_at`, registration timing
and per-heldout timing. `prospective_predeclared` means the recorded registration
and, for heldout partitions, policy lock preceded the decision dates.
`retrospective_governed` identifies a protocol or selection registered later.
Neither label proves that the operator never inspected outcomes elsewhere.
Existing free-form replay and external access are outside this protocol's lock.
Synthetic clocks are available only with `synthetic=True`; live clocks cannot be
supplied. A synthetic lock cannot precede development evidence already opened by
the protocol. Synthetic results prove software behavior, not commercial value.

## Full selection accounting

Each governed snapshot adds `selection_audit`: bounded per-product stages, counts,
exclusion reasons and the exact decision cutoff. Discovered means canonical
products in the declared panel known by that cutoff, rather than a global market
census. Linked means a confidence-eligible reviewed owner. Eligible means the
existing minimum number of distinct-time observations; ranked additionally
requires a nonmissing policy score. Products filtered before the ordinary ranking
universe remain visible, including no reviewed link, inadequate history, missing
score, future creation and market mismatch. Selected means the family-deduplicated
top-K. Missing future outcomes are explicitly unknown, never zero.

Declared IDs remain in the audit when a retrospective evaluation cutoff precedes
their product creation. They have `not_discovered_at_cutoff` and false discovery,
eligibility and ranking stages; their later name, market and other product facts
never enter historical scoring. A future-only source capability declaration is
retained only as a controlled-source hint. It cannot grant earlier evidence access
or make an excluded attention sample measurable before a valid frozen grant.

When commercial learning is installed, reviewed means a recorded test/watch/skip
plan available by the cutoff. Planned tests remain separate from tested: tested
requires a reviewed actual period with evidence available by the cutoff. This is
evidence of a commercial observation, not a claim of positive revenue or profit.
If the module is unavailable, these stages are null with an explicit reason.

Optional seeded sampling chooses only from discovered in-market products outside
the policy selection, under a maximum of 25. It freezes their existing reviewed
links and measures attention using the ordinary outcome kernel. The audit records
sample size, population, seed, without-replacement method and inclusion probability
in `selection_audit.measurement_sample.methodology`, alongside its outcome report.
This complete sampling contract is retained in the persisted decision snapshot
and survives a fresh attempt read. An empty population has sample size zero and
unknown inclusion probability. Missing links, boundaries or outcomes remain
censored. Sampling does not collect new source data, spend money or create a
commercial test; it is not an estimate of counterfactual launch profit.

## Replay, retirement and capacity

Registration, locking and run requests have stable identities and conflict on
changed input. Every accepted run attempt is committed before computation;
successes, failures and interrupted running attempts remain inspectable. Replaying
the same request never launches duplicate work. A failed attempt stays failed;
use a new request identity for a deliberate retry. A process interrupted after
reservation remains running/uncertain rather than being silently retried.
Saved results retain normal immutable evaluation provenance and source guards;
withdrawn results return `result_unavailable` rather than exposing cached content.

Limits per protocol: 8 policies, 500 total products, 64 total dates, 128 attempts,
8 horizons (at most 366 days), 8 launch delays, 168 hours boundary grace and 366
days policy lookback. Scoped metadata permits 10,000 entity keys, 50,000 ownership
records, 10,000 family records and 10,000 source capability records; excess fails
explicitly. Each run scopes history by declared products and dates, reuses existing
feature projections and one-product outcome reads, and does not scan the global
S3 lake for each fold. Existing row, cache and provenance budgets still apply.
No new service, paid call or sub-hourly schedule is introduced.

## Private operator interfaces

The shared research command routes to the same module boundary:

```sh
sm-trends research validation example > protocol.json
sm-trends research validation init
sm-trends research validation register --file protocol.json --request-id REGISTER-1
sm-trends research validation show --protocol-id PROTOCOL
sm-trends research validation run --protocol-id PROTOCOL --partition development \
  --policy-id attention --request-id DEV-1
sm-trends research validation lock --protocol-id PROTOCOL \
  --policy-id attention --request-id LOCK-1
sm-trends research validation run --protocol-id PROTOCOL --partition holdout \
  --policy-id attention --request-id HOLDOUT-1
sm-trends research validation list-attempts --protocol-id PROTOCOL --limit 20 --offset 0
sm-trends research validation attempt --attempt-id ATTEMPT
```

Replace the example product IDs and decision dates before registering. The input
must be a regular JSON file of at most 128 KiB. File, model and argument validation
happen before application construction. Help and example generation need no
configuration. Invalid input produces bounded field paths and fixed error codes;
arbitrary input values and exception details are omitted. Output passes through
the existing credential redactor.

Use `--synthetic` on each command for the isolated fixture namespace. Only synthetic
registration, locking and execution accept `--as-of`; live commands use the
database clock. Keep the request ID and full input unchanged for a retry. A failed
attempt stays failed; use a deliberate new request ID to make another attempt.
Exit 0 means successful inspection or completed execution, exit 1 means a recorded
execution is failed, running or unavailable, and exit 2 means rejected input or an
operation that could not be admitted. Inspecting a failed stored attempt exits 0.

The private research view initializes tables only after the explicit initialization
button. Registration, locking and execution each require their own button and
visible stable request ID. Protocol and attempt IDs and request intent may remain
in session state; evidence reports do not. Once an ID is explicitly inspected,
each interaction reloads that report through its source guard. Withdrawn results
therefore disappear on the next interaction instead of surviving in a session
cache. Listing uses pages of 20 attempts, bounded by the protocol's 128-attempt
limit. The view labels retrospective chronology and never calls the stored
protocol evidence of blind validation outside its recorded scope.

## Illustrative candidates and read-only development comparisons

The additional candidate bundle is explicitly `UNVALIDATED`. No winning-product
criteria have been calibrated. Generate the dictionary and editable protocol with:

```sh
sm-trends research validation candidates
sm-trends research validation candidates --format protocol > candidates.json
```

The first command returns `status`, `criterion_dictionary` and `protocol`. The
second returns only a valid `ValidationProtocol`, ready for editing; it does not
register or execute anything. Replace all example product IDs and dates with a
reviewed, separated development/test/holdout design before registration. Declare
all policies together before examining their governed outcomes. The original
`example` command and existing command outputs retain their prior format.

| Illustrative policy | Retained history | Minimum distinct-time observations | Weighted dimensions | Missing weighted dimensions |
| --- | --- | --- | --- | --- |
| `attention-7d` | 7 days | 2 | attention 1 | exclude |
| `attention-demand-14d-strict` | 14 days | 3 | attention 0.7, demand 0.3 | exclude |
| `attention-demand-14d-available` | 14 days | 3 | attention 0.7, demand 0.3 | explicitly renormalize available dimensions |
| `commercial-30d-strict` | 30 days | 4 | attention 0.5, demand 0.2, competition 0.15, feasibility 0.15 | exclude |

These are investigation strategies with declared differences, rather than a
prediction of sales. A lookback retains available history; it does not require
observations on every day, positive growth on every day or a configurable minimum
persistence. The protocol evaluates the existing attention target at a three-day
horizon, with launch delays of 12, 24 and 48 hours. Those delays represent when a
launch could start; they do not measure shipping or supplier delivery time.

The dictionary distinguishes facts the existing assessment kernel can use from
requirements it cannot establish:

| Criterion | Existing facts or derivations | Unavailable or unmodeled claims |
| --- | --- | --- |
| Growth across days | Compatible interval growth, positive interval fraction and available acceleration within the retained window | No new minimum persistence or consecutive-growing-days rule |
| Diversity and coverage | Known creator spread/concentration, family deduplication, declared-panel selection audit | No configurable creator diversity threshold, unknown-creator independence or global market census |
| Demand | Existing intent proxies and compatible purchase counters, when supplied | Views do not establish sales; missing demand remains unknown |
| Competition | Named observed sellers/advertisers, offers, explicit creative fingerprints and comparable prices | Coverage-dependent proxy does not establish market saturation |
| Margin | Existing feasibility calculation for complete, currency-consistent stated cost scenarios | Missing costs remain unknown; scenario margin is not realized profit |
| Delivery | Declared launch-delay sensitivity | Supplier reliability, delivery time and geography are not modeled by `AssessmentPolicy` |

Compare **existing** attempt IDs after running the declared development policies:

```sh
sm-trends research validation compare --synthetic \
  --attempt-id DEVELOPMENT_ATTEMPT_A --attempt-id DEVELOPMENT_ATTEMPT_B
sm-trends research validation compare --synthetic \
  --attempt-id DEVELOPMENT_ATTEMPT_A --attempt-id DEVELOPMENT_ATTEMPT_B \
  --format summary
```

JSON is the default for this new command. `--format summary` produces concise
French operator prose, limited to six policy scenarios per attempt; the JSON
retains all admitted scenarios. The private research view provides the same
comparison and candidate dictionary after explicit protocol inspection. Enter
2–8 comma-separated development attempt IDs, then press **Compare development
attempts**. Only ID intent remains in session state. Every subsequent interaction
reloads the comparison through the ordinary evidence guards. Final JSON and text
use current credential redaction and bounded display serialization.

Comparison never creates an attempt, registers a candidate, changes a policy lock,
opens test/holdout results or installs optional SQL. It checks stored partition and
workspace identity before hydrating result metadata. Missing IDs, cross-workspace
IDs, test/holdout IDs, different protocols, different decision scopes and unequal
outcome evaluation cutoffs are refused. UTC representations of the same instant
are treated equally. The attempt's execution clock is shown separately from the
fenced outcome `evaluation_cutoff`.

Matching scopes return `comparable`, or `flagged` when observed coverage,
exclusions or outcome coverage/status differ. Intentional strict-missingness
exclusions remain visible alongside each policy's selected identities and
per-date audit, rather than hiding the rows. These differences do not support a
rate league or automatic winner. Existing kernel summaries retain selected and
observed denominators, status counts, coverage, precision bounds, uncertainty and
dependence. Seeded measurement samples retain their actual IDs, population,
inclusion probability, methodology and scenario summaries. Missing or pending
outcomes remain unknown; zero selected products does not mean zero demand.
`winner` is always null.

Failed, running or withdrawn attempts return `unavailable` without candidate
reports. An invalidation encountered during hydration can instead refuse the
read. Authoritative readers hydrate independently; a final namespace/retention
fence rechecks every collected provenance root and evaluation invalidation before
bounded output finalization. This avoids holding an outer read lock while a
nested reader waits behind a queued retirement writer. No new evaluator, counter
formula, persistent comparison table or report cache is introduced.

The comparison admits 2–8 unique attempts, up to the protocol's 64 dates and 500
products. Across the comparison it permits at most 10,000 hydrated records and
4,000,000 encoded bytes, and separately bounds the finalized output to 4,000,000
bytes. Reads use existing pages of at most 100 records. These are work/output
admission bounds, not a process RSS guarantee. Existing provenance-node and cache
budgets still apply. An over-budget comparison fails explicitly. Inspection of an
unavailable comparison exits 0; rejected inputs or incompatible scopes exit 2.

## Controlled native walkthrough

Use a disposable native PostgreSQL URL and the existing loopback HTTP object-store
fixtures; do not point the test database at deployed state. Run the controlled
operator and service walkthrough with:

```sh
PYTHONPATH=src:tests flock -w 300 /tmp/night-native-tests.lock \
  python -m pytest -q tests/test_research_comparison.py \
  tests/test_research_validation_operator.py
```

The fixture creates canonical synthetic development products `dev`, `empty` and
`thin`, separate test/holdout products, actual reviewed links and immutable
observations. `dev` has two distinct-time observations at the decision and a
future attention outcome; `thin` has only one observation and `empty` has no
reviewed link. The declared development universe and evaluation cutoff are held
constant while the preregistered policies differ.

The native checks establish these controlled outcomes:

- Attention-only and an explicitly renormalized attention/demand policy both
  select `dev`. Their existing policy outcome summaries show 1/1 observed and
  `success`, while both exclusions remain visible in the selection audit.
- A strict attention/demand policy selects no product because the fixture supplies
  no demand evidence. Its `score_unavailable` exclusion and zero selected
  denominator remain explicit; precision is null. This is useful evidence of an
  unmet information requirement, not evidence that demand is absent.
- Removing the future boundary evidence leaves observed outcomes unknown. Reading
  at the decision clock exposes the existing `pending` state, never an invented
  success, failure or zero sale.
- Different outcome cutoffs and different protocols are refused. A genuinely
  executed holdout attempt, available only after the ordinary policy lock, is
  refused before its result is hydrated. The existing heldout lock and immutable
  selected-policy restrictions remain enforced.
- Withdrawing a real fixture dataset removes candidate reports from the next
  service, CLI or private-view inspection. Concurrent withdrawal during metadata
  or descriptor hydration cannot return a stale comparison. Multi-date frozen
  chronology is preserved across hash-addressed record trees.
- Seeded measurement audits preserve sample size, population and inclusion
  probability. Record/byte budgets reject oversized reads, and unsupported,
  non-finite or oversized final display values are refused without echoing them.

This walkthrough verifies software behavior on synthetic observations. It does
not establish source access, real product opportunities, calibrated selection
criteria, supplier feasibility, profitable launches or counterfactual profit.
Real selection research requires authorized real history; profit evaluation also
requires complete reviewed actual commercial outcomes under the existing contract.

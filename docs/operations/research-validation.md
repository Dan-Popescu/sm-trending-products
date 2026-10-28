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
sample size, population and inclusion probability in the sample report's
methodology. Missing links, boundaries or outcomes remain censored. Sampling does
not collect new source data, spend money or create a commercial test; it is not an
estimate of counterfactual launch profit.

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

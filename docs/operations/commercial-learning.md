# Reviewed own-commerce learning

`CommercialLearning(catalog, history, identity)` records why a product was tested,
watched or skipped, then connects reviewed actual money to the existing frozen
`realized-profit-v1` contract. It does not launch a store, charge anyone, contact a
supplier or train a new score. Initialize the independent `commerce` module first,
then call `CommercialLearning.initialize()` explicitly. Reads never create tables.

## Small operator interface

```python
learning.plan_test(dossier_id, expected_revision, request_id, synthetic=False,
                   plan=TestPlan(...), as_of=None)
learning.review_period(test_id, ReviewedPeriod(...), expected_revision, request_id,
                       synthetic=False, as_of=None)
learning.publish(test_id, period_id, revision, synthetic=False)
learning.test(test_id, synthetic=False)
learning.tests(synthetic=False, limit=100, offset=0)
learning.periods(test_id, synthetic=False, limit=100, offset=0, include_history=False)
```

For `plan_test`, `expected_revision` is the exact launch-dossier revision. A plan
is immutable revision 1; use another request ID for another commercial test.
For `review_period`, `expected_revision` is that exact period's current revision:
zero for its first review, then one, two, etc. Concurrent edits cannot overwrite
one another. Same-input request replays retain their original economic revision;
transport/publication status can progress independently. Reusing an ID with
different input is rejected. Publication retries retain the original observation,
dataset, review time and raw-object identities, including after SQL response loss.

`TestPlan` requires `selection` (`test`, `watch`, `skip`), `reason`,
`selection_policy` and `cost_contract`. A test requires a ready, unexpired dossier.
Watch/skip decisions can record incomplete or blocked candidates without creating
a financial entity. These records expose selection choices; they do not establish
a randomly assigned control group or coverage of every candidate in the lake.

The cost contract explicitly declares all seven components: `goods`, `shipping`,
`payment_fees`, `nonrecoverable_taxes`, `returns`, `fulfillment`, `advertising`.
Its `revenue_basis` is
`settled_payments_less_executed_refunds_excluding_output_taxes` and its
`coverage_policy` describes how the operator reviews the complete money scope,
including any allocation of shared costs. These facts are not inferred from a
country, payment status or supplier scenario. The launch dossier's
`returns_allowance` is a forecast; actual `returns` covers reviewed return-related
costs, not a forecast or a refund already subtracted from realized revenue.

Optional `assessment_run_id` and `forward_evaluation_id` references must match the
product, workspace and known decision time. Assessment references currently bind
the product's current recorded assessment. Plans retain its exact policy and the
original frozen decision/hash when supplied; later decisions cannot rewrite them.

## Declare the ledger before freezing a financial decision

A `test` plan durably declares one own `commerce_actuals:cost:<test_id>` entity,
then confirms its product link through the existing identity boundary. This
declaration contains no actual revenue, costs or invented profit. The returned
`state` becomes `ready` only after durable source delivery and reviewed linkage;
`ledger_declared_at` is the actual link availability time. Freeze a subsequent
financial decision with the returned `profit_contract`, explicit currency and a
decision cutoff after this link became available. Existing decisions cannot gain
a newly created ledger retroactively. Ordinary offers stay outside this contract.

Plans, periods and datasets preserve their real/synthetic namespace. Real records
always use PostgreSQL time; supplying `as_of` for a real plan/review is rejected.
The optional explicit synthetic clock supports future multi-day rehearsals.
Period bounds cannot precede the plan, extend beyond the review, overlap another
period or change during a revision. Reviews advance availability time strictly.

## Review actuals and unknowns

`ReviewedPeriod` supplies exact UTC bounds, the plan's fixed country/currency,
`realized_revenue`, `realized_costs`, opaque `evidence_refs`, `reviewer_note` and
four coverage attestations: `payments_complete`, `refunds_complete`,
`fees_complete`, `costs_complete`. Enter amounts as exact decimal strings, up to
18 digits and six decimal places. Net revenue may be negative when executed
refunds exceed settlements in this period; actual cost components stay nonnegative.
Missing revenue or any component stays unknown;
an explicit reviewed `"0.00"` is a known zero. Mixed currencies, floating-point
JSON amounts and scenario values are rejected. No exchange rate is invented.

A complete review needs all amounts, all four attestations and actual evidence
references. An `order-snapshot:<hash>` reference can point to an immutable order
revision, but the service does not turn that order's total or paid flag into
received money. One reconciliation page does not prove complete period coverage.
Processor, refund, advertising, supplier and tax statements still require review.
References are review assertions, not automatic verification of external files.

Profit remains null for incomplete reviews. Complete reviewed periods can publish
exact realized amounts through `publish`; the result applies only to those bounds,
not the complete launch lifetime. A later incomplete revision automatically
publishes a **withdrawal marker without money**, censoring the earlier complete
amount at new cutoffs while preserving old outcomes. Superseded pending exports
cannot be published. A complete correction requires publishing its new revision.

Publication failures leave durable pending intents for retry. A pending withdrawal
means the analytical export still needs reconciliation; never present its prior
export as the current reviewed financial result. Review/publication states remain
visible so an interrupted delivery cannot be mistaken for completion. No learning
transaction stays open during object-store calls. Existing History retention locks
remain authoritative inside its shared publication implementation.

Only period money, country, currency, immutable pointers and opaque references
reach analytical source observations. Raw orders, customer details, reviewer notes,
supplier quotes and scenario amounts are not copied there. Keep customer personal
data out of authored notes and reference IDs. Private SQL retains the original
dossier and selection policy for manual comparison with actual reviewed outcomes;
it does not automatically revise a ranking or claim that a product will sell.

Lists default to 100 entries, cap at 1,000 and bound offsets to 1,000,000. Reviews
span at most 366 days and carry at most 200 references of at most 256 characters.
Initialization and tests require no selected commerce platform or paid API.

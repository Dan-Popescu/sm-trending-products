# Private order and financial-review commands

These commands reconcile existing orders and prepare manual records. They do not
charge or refund a customer, buy stock, create a shipment or send email. A hosted
commerce platform still owns checkout and its actual notifications. No platform
or supplier is selected by initializing these modules.

## Help, templates and configuration

Help and templates work before PostgreSQL or S3 credentials exist:

```sh
sm-trends commerce operations --help
sm-trends commerce operations reconcile-order --help
umask 077
sm-trends commerce operations plan-template > /private/plan.json
sm-trends commerce operations period-template > /private/period.json
```

Edit the templates before submitting them. They deliberately contain unknown
amounts, unchecked coverage and incomplete identifiers. Use exact decimal strings
such as `"12.50"`; missing money is not zero. A reviewed `"0.00"` is a known zero.

Runtime commands require the existing private `DATABASE_URL` and base schema.
Order commands use PostgreSQL and the configured shop; they do not need S3.
Financial publication additionally uses `S3_BUCKET`, optional `S3_ENDPOINT_URL`,
AWS authentication and `CACHE_DIR`, exactly like the existing history service.
Never put actual secret values in command arguments or versioned JSON files.

Prepare an inactive connection file with the existing C02 command:

```sh
sm-trends commerce connection-template --provider woocommerce > /private/store.json
sm-trends commerce connection-inspect --connection /private/store.json
```

Connection files reference environment-variable names for secrets. Their limit is
16 KiB. Read the [provider contracts](commerce-provider-contracts.md) before
selecting and enabling a real shop. The examples below use `--synthetic`; the
dossier, connection's test mode and workspace must agree. Removing this flag selects real data and does not
supply missing platform access or credentials.

## Initialize independent modules

```sh
sm-trends commerce init
sm-trends commerce delivery-init
sm-trends commerce operations order-init
sm-trends commerce operations learning-init
```

Initialization creates optional application tables explicitly. Reads never create
them implicitly. Collection protocols and workers are unchanged.

## Register an existing owned listing and reconcile orders

`OPERATION-ID` must name a succeeded, verified draft-delivery operation. Its exact
dossier revision, product, currency, shop and SKU determine line attribution.

```sh
sm-trends commerce operations register-listing --operation-id OPERATION-ID --synthetic
sm-trends commerce operations reconcile-order --connection /private/store.json \
  --order-id ORDER-ID --synthetic
sm-trends commerce operations orders --connection-id STORE-ID --limit 20 --synthetic
sm-trends commerce operations order --connection-id STORE-ID --order-id ORDER-ID --synthetic
sm-trends commerce operations order --connection-id STORE-ID --order-id ORDER-ID \
  --revision 1 --synthetic
```

An authenticated webhook intake uses the exact original body bytes and a JSON
object of header strings. Do not parse and reserialize the signed body first.
The body limit is 256 KiB; header JSON is at most 16 KiB and 100 headers. Raw bodies
and customer fields are not retained in order snapshots or command output.

```sh
sm-trends commerce operations ingest-event --connection /private/store.json \
  --body-file /private/webhook-body.bin --headers-file /private/webhook-headers.json --synthetic
sm-trends commerce operations inbox --connection-id STORE-ID --limit 20 --synthetic
sm-trends commerce operations inbox --connection-id STORE-ID --include-processed --synthetic
```

Repeated authenticated bodies are deduplicated. Totals in that body are hints:
the service rereads the configured shop before recording order facts. A failed
read leaves a pending receipt. Retry its order ID with `reconcile-order` or replay
the exact event. `--after CURSOR` pages order/inbox lists using their returned
`next_cursor`.

```sh
sm-trends commerce operations reconcile-page --connection /private/store.json \
  --updated-after 2026-10-01T00:00:00Z --synthetic
```

This processes at most 50 order IDs and returns the next cursor. Continue with
`--cursor NEXT-CURSOR`; an interrupted page can be retried. A page does not prove
complete coverage of a financial period. Use overlapping scans when operating a
real shop.

Conflicting payloads with the same provider update time stay explicitly
conflicted. Inspect them before requesting a fresh authoritative resolution:

```sh
sm-trends commerce operations reconcile-order --connection /private/store.json \
  --order-id ORDER-ID --resolve-conflict --expected-revision CURRENT-ORDER-REVISION --synthetic
```

Two order readers can run with healthy database sessions. A lost database guard
cannot commit an abandoned read or acknowledge its inbox. GET requests already
sent can still finish under their request deadlines after connection loss; the
two-reader limit is not an absolute remote-request limit during a network failure.

## Manual handoff, tracking and notification intents

Create `/private/handoff.json` with owned line IDs from the reconciled snapshot:

```json
{"supplier_reference":"SUPPLIER-QUOTE-ID","line_ids":["LINE-ID"]}
```

```sh
sm-trends commerce operations handoff --connection-id STORE-ID --order-id ORDER-ID \
  --request-id HANDOFF-1 --expected-revision 0 --file /private/handoff.json --synthetic
```

This records preparation only. To cancel that preparation, use a new request ID,
its current handoff revision and the same supplier/lines with `"state":"cancelled"`.
Cancelled, unowned or conflicted order lines cannot prepare a new handoff.

Create `/private/tracking.json` with the current prepared handoff revision:

```json
{"handoff_revision":1,"carrier":"CARRIER-ID","tracking_reference":"TRACKING-ID"}
```

```sh
sm-trends commerce operations tracking --connection-id STORE-ID --order-id ORDER-ID \
  --request-id TRACKING-1 --expected-revision 0 --file /private/tracking.json --synthetic
```

Tracking is operator-reported, with `carrier_verified=false`. Create
`/private/notification.json` containing `{"notification_kind":"shipping_update"}`:

```sh
sm-trends commerce operations notification-intent --connection-id STORE-ID --order-id ORDER-ID \
  --request-id NOTICE-1 --expected-revision 0 --file /private/notification.json --synthetic
```

This records `requested`, `external_action=none`, `confirmed_delivered=false`.
It sends no mail. The handoff, tracking and notification streams each have their
own revision counter: `--expected-revision` here is not the order's revision.
Same-input request replays return the original event; conflicting input or a stale
revision is rejected. Request IDs must be stable when retrying an interrupted call.

## Freeze a commercial plan before its financial evaluation

Fill `/private/plan.json` with the selection (`test`, `watch`, `skip`), reason,
selection policy and cost-coverage policy. Declare all actual cost components:
goods, shipping, payment fees, nonrecoverable taxes, returns, fulfillment and
advertising. Launch-price scenarios and a returns allowance are not actual costs.

```sh
sm-trends commerce operations plan-test --dossier-id PRODUCT-FR --expected-revision DOSSIER-REVISION \
  --request-id PLAN-1 --file /private/plan.json --synthetic
sm-trends commerce operations tests --limit 20 --synthetic
sm-trends commerce operations test --test-id TEST-ID --synthetic
```

A `test` selection needs the exact ready dossier revision and an unexpired quote.
Watch/skip choices retain their reasons without creating a financial ledger.
The immutable plan binds one country, currency and reviewed cost definition.

For a test, wait for `state=ready`, then use its returned `profit_contract` and
currency in an existing financial evaluation config. Freeze with a decision time
after `ledger_declared_at`, for example `sm-trends freeze --synthetic --config-file
/private/profit-evaluation.json`. A previously frozen decision cannot gain a
new ledger retroactively. No quote is exported as realized revenue or profit.

## Review actual money and publish it

Fill the period template with exact UTC bounds, country/currency, actual revenue
and every declared actual cost. Bounds must start no earlier than the plan, end
no later than the review, and not overlap another period. Revenue means settled
payments less executed refunds, excluding output taxes; it can be negative. Cost components
stay nonnegative. Do not subtract the same refund again as a returns cost.

Actual evidence references are opaque IDs, including `order-snapshot:<hash>`.
Review payments, refunds, fees and all costs explicitly; set the corresponding
four coverage flags only when verified. An edited order total, paid timestamp,
refund record or single reconciliation page does not prove received money or
complete coverage. Keep customer personal data out of references and notes.

```sh
sm-trends commerce operations review-period --test-id TEST-ID --expected-revision 0 \
  --request-id ACTUALS-1 --file /private/period.json --synthetic
sm-trends commerce operations periods --test-id TEST-ID --synthetic
sm-trends commerce operations publish-period --test-id TEST-ID --period-id PERIOD-ID \
  --revision 1 --synthetic
```

`--expected-revision` on a review refers to this exact period, starting at zero.
A complete review stays `pending` until published. An incomplete review stores
explicit missing reasons and a null profit; it cannot publish realized amounts.
Publication retries retain their original observation, dataset and availability
time. Operator JSON files are bounded to 256 KiB. Periods span at most 366 days
and carry at most 200 opaque references.

For a correction, retain the exact bounds, use its latest revision and a new
request ID. An incomplete correction automatically withdraws a previously known
amount. If that delivery fails, inspect `periods --include-history` and retry
`publish-period` for the pending withdrawal's original revision. Historical
withdrawals remain replayable even after a newer complete revision was published:
they repair their original incomplete interval without overriding newer actuals.
Old pending monetary exports that were replaced remain blocked. Previously stored
reports remain immutable; new evaluations use the evidence available at each cutoff.

The commands return JSON. Exit `0` means the command completed, including storing
an explicitly incomplete review; it does not certify profit. Exit `1` means a
returned financial review still has a pending publication. Exit `2` means an
operational/validation error (JSON on stderr), or an argument-usage error. Exit
`130` means interrupted. Read commands can succeed while showing pending work;
inspect their statuses before treating a workflow as finished. No command trains
a new ranking or automatically declares a winning product.

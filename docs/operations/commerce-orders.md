# Order reconciliation and fulfillment preparation

`CommerceOrders` is an optional application module, independent from acquisition.
It uses the existing configured commerce adapters and the succeeded operations of
`CommerceDelivery`. Nothing in this module activates a store, charges or refunds
a customer, purchases stock, creates a shipment, or sends email.

```python
from sm_trending_products.commerce_orders import CommerceOrders

orders = CommerceOrders(pipeline.catalog, delivery)
orders.initialize()
orders.register_listing(succeeded_operation_id, synthetic=True)
orders.ingest(provider, exact_raw_bytes, headers, synthetic=True)
current = orders.reconcile_order(provider, order_id, synthetic=True)
page = orders.orders(provider.config.connection_id, synthetic=True, limit=100)
detail = orders.order(provider.config.connection_id, order_id, synthetic=True)
```

The private CLI/UI must cross this service boundary. Providers remain configured
and disabled/enabled explicitly outside the service. A connection identity is
permanently bound to provider, canonical shop origin, test namespace and API
version through `commerce_delivery.connection_fingerprint`. Rotating secrets does
not change that identity; reusing its ID for another shop is rejected. Fixture
connections cannot be imported into the real workspace.

## Authenticated inbox and recovery

Webhook HMAC verification uses the exact bounded raw bytes through the existing
adapter. The inbox stores the configured connection, signed-body digest, order
hint, processing state and acknowledged snapshot revision; it never stores raw
customer data or headers. Signed
payload totals and unsigned delivery/topic/shop headers are not financial facts.
The service fetches the order from the configured shop before recording it.
Deduplication uses connection plus body hash, not a delivery ID that an unsigned
header can replace. Identical already-processed bodies return `duplicate` without
another read; polling still matters when a provider repeats an unchanged hint.

The inbox is committed before HTTP. After a failed read or interrupted process,
`inbox(connection_id, synthetic=False, limit=100, after=None)` lists pending hints
through indexed bounded pages. Retry their order IDs with `reconcile_order`.
`pending_only=False` includes processed receipts. Repeating a read has no remote
financial effect. A new hint received after a read started remains pending until
a subsequent read; the earlier snapshot does not silently acknowledge it.

With healthy database sessions, two order reads per PostgreSQL schema may run
simultaneously. Session advisory locks serialize the same order without holding a
SQL transaction during HTTP. A short final transaction uses the exact session
holding those locks; a lost session cannot commit through a fresh pooled connection.
Busy callers receive `order_busy` or `order_capacity_busy`, with their inbox hint
still durable. Process death or a database connection loss releases the session
lock. An adapter read already in progress can finish its bounded sequence of GETs
under the individual HTTP request deadlines even after that loss, while a
replacement reader is admitted. There is no operation-wide cancellation guarantee.
Therefore
the two-reader bound is not an absolute bound on remote requests during a network
partition. Such abandoned reads cannot publish or acknowledge inbox entries;
they do not perform financial writes. A persisted fetch token also prevents an
older worker from committing after another worker claims it.
Provider errors are sanitized; their raw bodies and diagnostics are not retained.

## Immutable revisions and conflicting dates

Each authoritative business snapshot has a canonical payload hash, source update
time, actual local availability time and an immutable numbered revision. Ordering
lines/transactions differently or formatting `10.00` as `10` does not create a
different business payload. Repeated transaction IDs within a snapshot must agree.
Older source timestamps cannot overwrite a newer snapshot.

Different business payloads with the same provider update time mark the order
`conflict`. Neither candidate becomes an arbitrary winner, and supplier intents
are blocked. A genuinely newer provider revision clears the conflict. Otherwise,
an operator can explicitly reread current authoritative state with
`reconcile_order(..., resolve_conflict=True, expected_revision=current_revision)`;
this records a new immutable resolution revision and retains the conflict audit.
Normal repeated reads of either equal-time candidate do not resolve it.

`order(..., revision=N)` returns an earlier snapshot and marks it `historical`
when a later revision exists. Its `reference` is an opaque stable
`order-snapshot:<hash>` of connection, order ID and revision. Financial reviews
can cite this reference and `payload_sha256` without copying customer information.
The current head state is explicit and must still be considered when reviewing
conflicting evidence. Snapshot contents remain unchanged after later events.

## Money and product attribution

Order totals, confirmed payment captures, confirmed cash refunds, refund records,
cancellation, fulfillment state and WooCommerce `paid_at` remain distinct.
WooCommerce's paid date does not establish the captured amount; an edited order
total is not converted into revenue. A refund record alone is not a cash movement.
No fees, advertising costs, supplier costs, taxes or profit are inferred.
`cash_complete=False` and `financial_allocation=unallocated_order_totals` stay
explicit. Reviewed complete financial periods are a separate module.

Only a succeeded, ownership-verified `CommerceDelivery.operation` can register a
listing. Its exact dossier revision, canonical product, remote product/variant,
SKU, currency and namespace are anchored. An order line must match all supplied
listing identities. Missing ownership, several conflicting dossier anchors, or a
currency mismatch remain explicitly unmatched/ambiguous. Multiple canonical
products may share an order; its entire total or capture is never allocated to
each product. Updating a listing to several registered dossier revisions remains
conservative until an explicit attribution policy is selected.

## Manual supplier, tracking and notification records

`handoff(connection_id, order_id, request_id, supplier_reference, line_ids,
expected_revision=0, synthetic=False, state="prepared")` records preparation for
known, unambiguously owned lines. Cancelled/conflicting orders cannot prepare a
handoff. `state="cancelled"` records a later cancellation of the preparation.
Supplier references are opaque business IDs, not customer addresses.

`tracking(..., request_id, handoff_revision, carrier, tracking_reference,
expected_revision=0, synthetic=False, status="recorded")` requires the current
prepared handoff. Tracking status is explicitly operator-reported with
`carrier_verified=False`; even an operator's `delivered` record is not a verified
carrier response.

`request_notification(..., request_id, notification_kind, expected_revision=0,
synthetic=False)` records an order/shipping/refund notification intent. Its state
is `requested`, `external_action=none`, `confirmed_delivered=False`. It does not
send a message or claim the platform delivered one. No supplier or mail provider
has been selected. Each manual stream uses immutable revisions and compare-and-
swap checks; replaying a request returns its original record, while redirecting
the request to another payload or overwriting a stale revision is rejected.

## Limits and live acceptance

Order snapshots contain at most 100 lines, 200 payment records and 100 refunds,
with exact nonnegative decimal strings and matching currencies. Listing matching
examines at most 1,000 registrations; overflow leaves lines `lookup_limit` rather
than making a partial ownership claim. Pages contain at most 100 order/inbox
metadata records. `reconcile_page(provider, updated_after, cursor=None,
synthetic=False)` processes at most 50 provider order IDs and returns compact
results and the next cursor. It does not declare period coverage complete. Use
overlapping scans and replay-safe business revisions; interrupted pages can be
read again. Shopify historical-access restrictions still apply.

Native PostgreSQL scenarios exercise authentic HMAC, controlled HTTP with
customer fields excluded, duplicates/reordered state, process interruptions,
concurrent workers, timestamp conflicts, multiple products/currencies, unconfirmed
refunds and idempotent manual records. Live deployment still requires the chosen
shop, HTTPS webhook hosting/retry policy, credentials/scopes, operational scans,
supplier agreement and platform notification configuration. Those are not
established by a successful synthetic test.

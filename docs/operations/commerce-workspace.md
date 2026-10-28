# Private commerce operations workspace

Select **Commerce operations** in the private dashboard, then choose one section:
**Orders and supplier follow-up** or **Commercial tests and actual costs**. Only
the selected section loads its services and records. Opening a section never
initializes optional tables, activates a platform or contacts a store.

Initialize the base application and the needed optional modules explicitly:

```sh
sm-trends commerce init
sm-trends commerce delivery-init
sm-trends commerce operations order-init
sm-trends commerce operations learning-init
```

Delivery initialization is needed for owned listing preparation/registration;
order and learning setup stay independent. No credentials, store, supplier or
shipping service is selected by running these commands. Real and synthetic
workspaces use the dashboard's existing switch and remain isolated.

## Orders and manual follow-up

Enter a connection ID to read stored orders or event receipts. Both lists show
at most **20 records** per call; copy `next_cursor` into the page-cursor field to
continue. Receipts default to pending only. Enter an order ID to inspect its
current stored snapshot, immutable reference and latest manual records. These
local reads do not poll the provider.

For an authoritative provider read, select a private connection-file path and
click **Reconcile order from configured store**. The connection file is loaded
only for this action, through the same bounded connection loader as the CLI.
Its connection ID must match the inspected connection. Credentials stay in
private environment variables, and errors never echo file contents or remote
diagnostics. The action performs provider reads, not payments, refunds or
fulfillment writes. A timestamp conflict needs the explicit resolution checkbox
and the exact current order revision. See [order reconciliation](commerce-orders.md)
for provider limits and signed receipt handling.

The manual forms record three independent versioned streams:

- **Supplier intent** records preparation/cancellation for explicitly selected,
  unambiguously owned order lines. A supplier reference is an opaque business ID,
  not a customer address. Nothing is purchased or sent to the supplier.
- **Tracking intent** references the current prepared handoff. The chosen status
  is operator-reported, with `carrier_verified=False`; it is not carrier evidence.
- **Notification intent** records an order, shipping or refund notification
  request. Its state is `requested`, `external_action=none` and
  `confirmed_delivered=False`. It does not send email.

An order total or paid date is never presented as received revenue or profit.
The stored snapshot keeps `cash_complete=False` and unallocated order totals.
Customer fields are excluded by the provider boundary before snapshots or the
private review output; no customer contact fields appear in these forms.

## Selection and an explicit financial contract

The financial section pages through **20 dossiers** and **20 commercial tests**
at a time. Choose a dossier and record `watch`, `test` or `skip`, with a decision
reason, selection policy and an explicit payment/refund/fee/cost coverage policy.
The operator confirms all seven public `ACTUAL_COSTS` categories: product sourcing,
shipping, payment fees, nonrecoverable taxes, return-related costs, fulfillment
and advertising. This declares what will need review; it supplies no actual cost
amount or assumed zero. Optional original assessment/evaluation IDs remain opaque
references checked by the service.

Watch/skip choices retain their reasons without creating a financial ledger. A
`test` requires a ready, unexpired dossier and declares a ledger with no invented
money. The detail shows its state, actual declaration time and `profit_contract`.
A later frozen financial decision must explicitly include that contract, the
currency and a cutoff after the declaration. A pending declaration requires
replaying the original plan request with its original values. Creating a plan
does not activate checkout, launch ads or prove commercial viability.

## Reviewing actual periods

Choose **New period**, or an existing current review to prepare a correction.
Provide exact ISO start/end timestamps with a timezone. Country and currency come
from the commercial test and cannot be changed by the review form. Amount fields
are strings using a decimal point; **blank means unknown**, while an explicit
reviewed `0.00` is a known zero. Net settled revenue may be negative if executed
refunds exceed settlements in the period. The form never fills amounts from an
order total, a launch quote or a paid flag.

Review all seven actual costs and four coverage attestations: settled payments,
executed refunds, fees and costs. Add opaque evidence IDs and a reviewer note;
do not enter customer contact data. Missing amounts, incomplete coverage or absent
evidence keep profit **Unknown**. The saved record lists the exact missing
reasons. See [reviewed commercial learning](commercial-learning.md) for accounting
scope, period bounds and immutable reference rules.

A complete period can be exported with **Publish or resume this period revision**.
A later incomplete correction produces a withdrawal without money, preserving
older historical outcomes while censoring that value at later cutoffs. Pending
withdrawals show a warning: the prior analytical export must not be treated as the
current reviewed result. The same explicit publication action can resume a pending
withdrawal, including a historical withdrawal revision. Corrected history is
labeled, and only current reviews can be edited.

Period lists show at most **20 revisions** per page, including corrected history.
Publication status is separate from reviewed completeness and profit. After a
mutation succeeds or fails, the view rereads SQL state; it does not keep a previous
profit result in session state after a durable correction or lost response.

## Safe retries and resource limits

Every plan, review and manual intent exposes its request ID and expected revision.
Zero is the initial period/manual revision; a plan targets its displayed dossier
revision. Reuse the same ID, expected revision and values to replay an attempt.
The ID and expected revision are not silently advanced after success or error.
For a deliberate new decision, use **Start a new … request**, inspect current
records and edit the intended values. A concurrent update reports a conflict
instead of overwriting it.

A bounded session cache retains replay IDs, expected revisions and period bounds
for up to **100 operation scopes**, separately from Streamlit widget state.
Navigation therefore does not discard them while the scope remains in that cache.
No source results, quoted evidence, order snapshots or financial output objects
are stored there. Old scopes can be evicted when the cap is reached; keep important
operation IDs outside the browser and use stored records/CLI for later recovery.
Draft form fields can need re-entry after navigation; retaining an ID is not
permission to change an earlier request payload.

Dossier/test/period pages bound offsets to **1,000,000** and expose only the chosen
page. Order snapshots remain bounded by their service contract. Forms cap source
IDs, notes and evidence-reference input; amounts are validated as exact decimals.
JSON is used to display safe records, not as the only editing interface. Secret
redaction applies to records and errors; HTML-looking authored values remain text.

Native Streamlit/PostgreSQL scenarios in
`tests/test_commerce_operations_dashboard.py` cover selected-section isolation,
explicit initialization, watch/test selection, unknown costs, interrupted
withdrawal and retry, manual intent truthfulness, replay metadata across navigation,
bounded order/receipt pages and a controlled HTTP provider read. They do not
establish real provider access, supplier arrangements, tax completeness or
commercial success.

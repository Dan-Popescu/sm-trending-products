# Durable draft delivery

Public imports: `CommerceDelivery`, `DeliveryError`, `connection_fingerprint`
from `sm_trending_products.commerce_delivery`.

Initialize the dossier module first, then call `CommerceDelivery(catalog).initialize()`
explicitly. No read creates tables or contacts a store. Capture migrations and
workers are unchanged.

```python
delivery.prepare(dossier_id, provider, expected_revision, request_id, synthetic=False)
delivery.operation(operation_id, synthetic=False)
delivery.recover(operation_id, provider, synthetic=False)
```

The CLI and dashboard use this interface. The provider is supplied from private
runtime configuration; this module stores no credentials. The same connection ID
cannot be reused for another provider, shop origin, API version or test workspace.
`connection_fingerprint(config)` exposes that binding while allowing secret
rotation. Synthetic dossiers require explicit loopback test-provider configuration;
live dossiers require live configuration. Neither mode publishes a product.

Preparation requires the current ready dossier and expected revision. The durable
operation references that exact immutable revision and records escaped authored
description, exact catalog price, currency and explicit tax basis before HTTP.
The provider verifies actual catalog settings. Stable per-connection/dossier SKU
and ownership marker survive revisions and retries. Successful output contains
operation/state, provider/connection fingerprint, canonical product/dossier/revision,
remote product/variant, SKU, hosted product URL, currency, workspace and timestamps.
It contains no authored draft text, customer details or credential configuration.

States are `pending`, `running`, `succeeded`, `failed` and `uncertain`.
Reusing an identical request ID only reads its existing operation. Reusing it with
a different revision/configuration is rejected. Only one unresolved operation may
exist for a target. Two active deliveries per database schema are allowed, using
dedicated PostgreSQL sessions; try-lock contention fails promptly. Connections
close in all exit paths and are never returned to the pool with a session lock.
HTTP runs without an open SQL transaction. Attempt tokens fence stale SQL writes.

An initial unknown lookup performs no remote write and records a failed safe
attempt; a new request may try again. After a response is lost or a worker crashes,
absence in a search index never permits another create. Explicit recovery updates
only a found owned draft or an already known provider ID; otherwise it stays
uncertain and blocks newer delivery. Completed old operations cannot overwrite a
newer successful revision. Expired frozen supplier quotes block recovery writes.
Unknown outcomes are deliberately not relabeled success or exactly-once execution.

Recovery should follow termination of a failed worker. PostgreSQL session locks
and attempt tokens coordinate application workers; providers without remote fencing
cannot prove that a remote request has stopped after a process/network failure.
Keep uncertain targets unresolved until ownership can be reconciled; manual
provider-side investigation is required when no resource can be found. There is
no automatic abandonment, blind retry, refund, mail, payment or supplier operation.

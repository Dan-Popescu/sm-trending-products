# Keep commercial operations independent from acquisition

Status: accepted under the owner's 2026-10-28 commercial-learning authorization.
Specification: [product launch and commercial learning](../specs/commerce-learning.md).

Launch dossiers, hosted-commerce effects and commercial outcomes use independent,
additive application tables with explicit checksummed module initialization.
The acquisition SQL migration/version protocol and source tables remain unchanged.
This keeps store failures and supplier or payment configuration out of capture,
and permits the new module to be installed without replacing a collecting worker.
One small migration initializer is shared by optional modules; each owns its
tables and immutable revisions rather than extending the collector's contracts.

Hosted platforms own customer checkout and transactional notifications. The
application owns launch decisions, reliable provider requests, reconciliation and
the link from measured outcomes back to selection. This trades some provider
integration work for substantially less custom payment, storefront and mail
infrastructure. Platform choice remains configurable and inactive by default.

European launches declare a country and currency explicitly. Dropshipping,
owned stock and external fulfillment share the commercial decision language;
supplier-specific transport stays behind the operational interface. Missing
costs, terms, stock, credentials or delivery facts remain unresolved requirements.

# Product trend intelligence

Identify emerging physical-product opportunities from accessible evidence, then measure whether the system finds useful opportunities early enough to act.

## Language

**Product**: A canonical physical item whose attention and commercial evidence are evaluated together. A product is distinct from any seller's listing. _Avoid_: using listing, SKU, and product interchangeably.

**Product family**: Related or substitutable products grouped to analyze diffusion and prevent near-duplicate items from leaking between evaluation groups.

**Offer**: A seller's listing for a product, with its own market, currency, price, availability, and source identifier.

**Video**: An individual published creative. Public content and advertisements retain their distinct source and distribution context.

**Observation**: A source response or imported record captured at a specific time, including provenance and missing-field reasons. Reobserving a counter creates another observation, not a replacement.

**Measurement**: An exact, bounded or estimated value from an observation, with an explicit unit and meaning. A measured count does not by itself establish interval sales.

**Counter period**: The reporting period summarized by a cumulative measurement. A rolling-period total is not comparable to a lifetime counter, and its change does not directly measure consumption between observations.

**Evidence**: An observation, or a reproducible derivation from observations, supporting a stated claim. Estimates are evidence with explicit assumptions, not observed facts.

**Availability time**: The earliest recorded time at which evidence was available to this system. A source's publication date or reporting period does not establish availability time. _Avoid_: treating old publication dates as historical availability.

**Coverage**: The accessible, actually sampled portion of a source, market, or candidate universe over an interval. Coverage is not an assertion that all TikTok content has been observed.

**Discovery**: Adding previously unknown candidate videos, products, or offers through a documented accessible source or explicit import. Refreshing known candidates is not discovery.

**Attention**: The observed growth and spread of exposure or engagement around a product. Attention alone does not establish purchases.

**Demand**: Evidence of purchase intent or purchases, with intent proxies distinguished from observed transactions. _Avoid_: converting views directly into reported sales.

**Competition**: Observed seller, offer, creator, or advertising activity competing for attention or purchases within measured coverage.

**Commercial feasibility**: A product's estimated ability to meet sourcing, delivery, cost, return, and market constraints. Missing costs remain unknown.

**Opportunity**: A product and target market considered actionable at a specified time, supported by evidence and explicit gaps.

**Alert**: A recorded notification that an opportunity met a versioned policy, including the evidence and assessment available at that time.

**Backtest**: Historical replay that uses only evidence and rules available at each simulated decision time. Retrospective exploration without such evidence is not a backtest.

**Forward test**: A prospective evaluation that freezes a decision and subsequently measures its predefined outcomes as they mature.

**Capture**: A complete source response preserved durably with its acquisition
context and time. A capture can exist before its contents become usable evidence.

**Capture obligation**: A declared authorized source scope and observation window
that must be captured by a recorded deadline. A retry retains the original window.

**Capture gap**: An obligation whose required observation was not secured within
its permitted window. A later counter value does not silently fill that gap.

**Replayability**: The verified ability to retrieve the same required historical
source evidence later, within its availability window and permitted request budget.
An undocumented recovery ability remains unknown.

**Launch dossier**: A versioned decision record for offering a canonical product
in one country, with commercial facts, assumptions and unresolved requirements.
Being ready to test is distinct from being proven profitable.

**Commercial test**: An actual attempt to sell under a recorded offer, launch
decision and measurement period. A synthetic rehearsal remains a rehearsal.

**Order**: A commerce platform's record of a customer's purchase request. An order
is distinct from a successful payment, completed fulfillment or realized profit.

**Fulfillment**: The supplier or warehouse work that delivers ordered products.
A handoff request is distinct from acceptance, shipment and delivery.

**Commercial outcome**: Measured sales and declared actual costs for a product,
country and period, with incomplete evidence identified explicitly.

**Transcript insight**: A reproducible interpretation of supplied spoken-text or
subtitle evidence, with its origin, version and uncertainty. A description is not
automatically a transcript of speech.

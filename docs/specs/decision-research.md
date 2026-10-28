# Evidence-backed product decision research

Status: approved by the owner on 2026-10-28. Implement all seven proposed research
lots and bounded automatic text selection, with independent review and autonomous
PR/CI/main delivery. No VPS deployment, platform activation, source connection,
paid enrichment, advertising, customer payment, shipment or email is authorized.

Baseline: `a11ca80011c570d1c619556d01218e30f857ae9d`.
This extends [commercial learning](commerce-learning.md), not its live activation.

## Intended decision

Help the operator decide which physical product and market deserve investigation
or a commercial test, why, which contrary evidence matters, and what information
could change that decision. Start with European dropshipping while retaining
stocked products and durable brands. Infrastructure economy matters more than
small latency gains. Existing hourly collection/ranking cadence is unchanged.

Existing acceleration, persistence, creator concentration, market/age cohorts,
price history, scenarios, replay, baselines, watch/skip reasons and reviewed
actual-profit contracts remain foundations. Do not duplicate those behaviors or
present new heuristics as sales probabilities.

## R01: Preregistered validation and selection accounting

- Record an immutable protocol with target, bounded candidate policies, declared
  decision dates, chronological development/test/holdout partitions, family
  separation and nonoverlapping outcome horizons. Policy selection is locked
  before opening a held-out partition. Preserve all attempted configurations,
  not only a winning result; requests are replay-safe and revisions explicit.
- An explicitly protocol-governed execution must enforce its partition at the
  authoritative computation interface. The existing free-form replay interface
  remains available and must not be relabeled as locked validation.
- Reuse existing scoring, counter semantics and frozen outcome computation.
  Do not rewrite old snapshots, identifiers or evaluation configurations.
- Account for discovered/linked/eligible/ranked/reviewed/tested candidates and
  missing outcomes with explicit stage reasons. Distinguish an observed audit
  population from the unobserved global market. A bounded, seeded measurement-only
  sample of excluded candidates may be defined without launching or spending.
- Retain exclusions and uncertainty. Do not invent profit for unlaunched products,
  fit calibrated sales probabilities without outcomes, or call simulator results
  evidence of commercial effectiveness. Report protocol/data limits explicitly.

## R02: Comparable and robust traction

- Add opt-in descriptive diagnostics over disjoint comparable time periods,
  separating continuing-video growth from newly observed videos. Normalize by
  actually measured time; never fill missing periods with zero.
- Report the growth remaining after removing the largest known creator and
  explicitly verified creative, their contribution shares, unknown-identity
  contribution and which stress checks cannot be performed.
- Keep source distribution contexts and incomparable units separate. Authorized
  account videos are not proof of organic distribution; ad reach is not views.
- Reuse one bounded longest-history read and the existing timeline kernel.
  Preserve default v1 reports and historical scoring; new diagnostics are opt-in
  and versioned. Honor corrections, resets, gaps, ownership, availability,
  retirement and the real/synthetic namespace.

## R03: Unified decision brief

- Expose one private, evidence-backed product/target-country brief: measured
  support, contrary evidence, missing dimensions, trend diagnostics, comparables,
  variant conflicts, reviewed themes, economic limits and the next useful fact.
- Explain watch/investigate/ready-to-review states without automatically launching
  anything or adding a calibrated prediction. Partial/uninitialized modules and
  unavailable source data remain explicit, not empty successful sections.
- Selected dossiers, profiles and evidence carry exact revisions, cutoffs and
  provenance. A live brief refreshes source permissions; stored references must
  not bypass withdrawal or expose cached source text after revocation.
- Provide bounded CLI and private dashboard paths through the same module
  interface, with stable request identities where any record is written.

## R04: Reviewed categories and comparable products

- Use a small versioned, reviewed category taxonomy and assignments. A product
  family is a substitution/identity grouping, not a category. Immutable changes
  use expected revisions and stable request identities; no read performs DDL.
- Compare bounded eligible product summaries by category, declared target market,
  age, policy, metric and unit where supported; provide panel size, missingness,
  family deduplication and descriptive within-panel percentiles.
- Target-market country is not measured audience country. Source/distribution
  metadata absent from an assessment remains mixed/unknown. Do not infer a
  country's demand from language, a search query or a launch-country choice.
- Sparse, stale, replaced, mixed-policy or incompatible groups do not receive an
  unsupported percentile. Label current-projection-only historical scope and
  bounded/truncated panels; do not claim a global market census.
- Reuse compact assessments/catalog metadata, not one raw-lake read per peer.

## R05: Economic breakpoints and cash exposure

- Extend the existing dossier contribution/max-acquisition-cost facts with bounded
  named joint scenarios and defensible break-even limits for returns, sourcing
  and advertising, using explicit operator assumptions and exact decimals.
- Compute timed cash exposure only when volumes, supplier-payment timing,
  settlement timing and cost-payment schedules are supplied. Positive unit
  contribution does not imply zero financing requirement.
- Missing costs, tax basis, return losses, timing or volumes remain unknown.
  Do not infer VAT rules/currency conversions or invent a probability distribution.
  Avoid double counting a refund and a return allowance. Price/tax changes require
  an explicit consistent basis; values remain scenarios, not reviewed actuals.
- Bound scenario expansion, dates and quantities. Pin dossier revision and quote
  freshness; reject bypassed/invalid models at the public mutation/read boundary.

## R06: Quoted content themes and message diffusion

- Consolidate problems, benefits, objections and selling angles in eligible supplied
  texts using bounded reviewed/versioned EN/FR themes/synonyms. Return quotations,
  offsets and ambiguous/negated mentions rather than untraceable generated claims.
- Count distinct eligible text/content versions, known creators and source kinds;
  repeated request IDs or metric refreshes cannot inflate recurrence.
- Show similar scripts and observed message spread without treating text similarity
  as a verified creative fingerprint or proof of independent people.
- Speaker role stays unknown unless explicit reviewed context supports it. Seller
  persuasion is not buyer demand; a quoted proof claim is not verified truth.
  Current contracts do not establish access to comment bodies or audio.
- Use an authoritative bounded text-read interface preserving all transcript
  source/observation/ownership/cutoff/retirement checks. Existing text/analysis
  identities remain compatible, including when optional reviewed context is added.

## R07: Observed competition and price pressure over time

- Report newly observed and continuing named sellers/advertisers/offers over
  comparable source/query/country windows, with observation age and coverage.
- First observation is not market entry, absence is not departure, and broader
  discovery is not competition growth. Unsupported scope comparisons remain
  explicitly unavailable rather than returning a saturation percentage.
- Compare prices only with compatible variant/currency/tax/shipping facts; do not
  convert observed retail price pressure into an actual supplier-margin claim.
- Bound product/family history and retain explicit unknown contexts.

## R08: Useful-text selection within a budget

- Select bounded eligible supplied texts for a bounded shortlist, emphasizing
  missing useful evidence, new content and known language/variant/creator diversity.
  Explain selection and unavailable/not-reviewed/unsupported reasons.
- Stable plan/configuration identities, cutoff, namespace and source eligibility
  survive replay. Revalidate any plan at execution; untrusted fields cannot widen
  a budget, change namespace or bypass a source guard.
- Preview is read-only. Any execution uses only the existing free local analyzer,
  explicit bounded application and atomic service budgets. Preserve unknown/running
  external-outcome barriers and canonical-text reuse; no ASR, media download,
  external source request or paid adapter is enabled.

## Architecture and acceptance

Use deep optional modules with a small public interface shared by CLI, dashboard
and native scenario tests. Keep invariants and versioning with the module that
owns the facts. Reuse PostgreSQL, S3/Parquet, DuckDB/Polars, provenance and existing
point-in-time guards. No new permanent service or dependency is required.

All work is bounded by selected products, rows, texts, scenarios, policies or
profiles. Shared reads and prepared features should be reused; avoid repeated
global scans and unbounded hourly materialization. Optional SQL uses checksummed
module initialization, never a capture-schema migration. Do not change source
quotas, capture protocol, CPU hot-switch guarantees or old snapshot hashes.

Native tests must cross real PostgreSQL/history interfaces, cover missing data,
mixed units, revisions, namespace mismatch, changing coverage, source withdrawal,
future evidence, cursor boundaries, rejected model bypasses and replay. Test
protocol temporal/family leakage rejection and economic arithmetic/monotonicity.
Each slice receives independent Standards/Spec review and exact-head required CI
before autonomous merge. The final combined application also runs the complete
capture, handover and backup/restore checks.

Synthetic evidence proves software behavior, not the real predictive usefulness
of a criterion. Source approval, target-VPS capacity, live commerce and actual
business outcomes remain separately configuration/data-dependent.

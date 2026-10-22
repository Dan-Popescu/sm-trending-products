# Controlled commercial-evidence acceptance

T08 verifies the commercial adapters and review contracts without live TikTok
access. Real endpoint fields, scopes, intended-use approval and market coverage
remain T15. Nothing in this acceptance purchases access or promotes fixtures to
live capabilities.

| T08 criterion | Executed evidence |
| --- | --- |
| Durable observations and explicit source/distribution | `tests/test_commercial_delivery.py` runs both Ads and disclosed-commercial-post responses through actual Rust, native PostgreSQL and HTTP object storage. Actual Streamlit shows two typed observations per source after the controlled server stops. |
| Reach, periods, creatives, advertiser and timestamps | Native delivery retains explicit 10000–19999 reach bounds, advertiser ID, source first/last-shown dates and creative references. Commercial posts retain publication time, creator and disclosure fields. Both retain observation/availability times and raw dataset references after restart. Earliest observation remains an observation, not a claimed launch date. |
| No false organic views or rounded precision | Neither source creates a video-view counter or an attention/demand score from reach. The rounded `11K` fixture retains raw text and an explicit undocumented-bounds reason. Streamlit preserves the source entity type and absent video metrics. |
| Live access stays gated and visible | Before and after a controlled denied-capability record, live enqueue rejects with `source_access_unverified`. The actual health view reports denied access and pending live acceptance. Synthetic collection itself creates no capability grant. |
| Pagination, missingness and provenance | Two source pages carry the same query filters and returned search ID. Missing commercial fields remain absent/explicitly unsupported. All observations retain synthetic classification, provenance and durable checksums; live assessments/evidence remain empty. Existing collector contracts separately cover shape validation and redaction. |
| Separate real acceptance and no spending | Controlled loopback responses and dummy fixture credentials are the sole source boundary. T15 remains open; no commercial source subscription, permission or business eligibility is inferred. |

The scenarios extend acceptance evidence without adding a second adapter,
normalizer, evidence type or scoring path. Full exact-head CI and its native,
benchmark, deployment and isolated-recovery results are recorded on the PR before
autonomous merge. Actual source retention, coverage and permitted use still need
the approved provider/account context.


## T09: commercial dimensions in published review

| Criterion | Executed evidence |
| --- | --- |
| Offer semantics and provenance | `tests/test_commercial_assessment_delivery.py` imports actual records into PostgreSQL/S3 history, then reviews them. Offers retain FR market, EUR price units, stock indication, availability and raw dataset references. Cost evidence retains its supplier quote, delivery assumption and explicit acquisition context. |
| Bounded counters and actual intervals | The native published result preserves a one-to-199 unit delta across the observed one-hour interval, while the absolute exact unit count stays null. Existing analytics cases reject changed, rolling, undocumented and stale reporting periods. No daily purchase series is invented. |
| Transactions, intent and modeled estimates | Separate products retain bounded transaction, uncalibrated intent-proxy and explicitly estimated statuses. Unknown/estimated demand scores stay null. The actual evidence view displays the estimate model version and conversion assumption rather than claiming transactions. |
| Competition and coverage | The native result reports one observed named seller and unknown market coverage. A product without commercial evidence has unknown competition rather than an empty-market score. Analytics cases also cover advertising, creative uncertainty and comparable price evolution. |
| Cost scenarios and missingness | The full imported assumptions yield a 7.5 EUR contribution-margin scenario, retaining source/delivery/return assumptions. A later incomplete cost quote makes feasibility unknown with an explicit missing-tax reason instead of using zero or an older complete quote. Earlier-cutoff evidence remains identical. |
| Explicit ranking policy | Actual Streamlit displays the stored four-dimension result, weights and missing-data behavior. With exclusion, missing commercial dimensions prevent ranking; explicit renormalization uses known attention while retaining null demand/feasibility and proxy labels. The native test checks the displayed result again after a later incomplete quote. |

This proof adds no conversion model, market census, cost resolver or alternate
assessment path. Manual estimates remain modeled evidence, quote margins remain
scenarios, and unknown supplier performance remains unvalidated. Source coverage,
actual costs and realized business profitability are separate from these
controlled engineering checks. T09's exact-head CI is recorded on its PR before
merge; T12 remains responsible for the commercial outcome-evaluation contract.

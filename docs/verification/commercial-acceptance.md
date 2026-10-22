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

# Native capture and release qualification

`tests/test_capture_release_qualification.py` joins the capture contracts, Rust
raw acquisition, immutable HTTP object history, worker-generation fencing and
`CatalogCaptureVerifier` at their public service boundaries. It uses native
PostgreSQL, an isolated HTTP Moto endpoint and one-shot loopback HTTP sources.
It does not substitute a readiness flag for captured evidence.

The declared fixture policy applies only to synthetic Display list requests.
Its cadence and tolerance are test inputs, not claims about TikTok replay or
live source access. Policies are registered before the tested original windows.

| Scenario | Required evidence |
| --- | --- |
| Broken record normalizer and valid pagination | Two actual raw commits have zero normalized rows, distinct task/page obligations, the same original due time and the acquiring generation. Qualification names those exact dataset and obligation IDs. |
| Publication fails after local fsync | With the source stopped and access token removed, another worker publishes the retained body without another source request. Its receipt retains the original acquirer and cannot qualify the adopting worker. A subsequent physical acquisition supplies that worker's own proof. |
| Candidate process generation changes | Finishing and registering the same worker creates a new generation. Existing actual receipts cease to qualify it until a fresh physical capture is committed. |
| Source lease expires during HTTP | Two real Rust subprocesses share the acquisition lock. The replacement reclaims the task while the old request is outstanding; exactly one response is fetched. SQL rejects the stale publisher and adoption preserves the original window and acquiring generation. |
| Older unknown pending window | A fresh actual capture in another qualified operation cannot hide the earlier window at release start. |
| Receipt retires during handover | A subsequent valid capture in the same declared scope cannot hide the retired original receipt once that obligation's deadline is crossed. |
| HTTP 200 source business error | The diagnostic archive cannot acknowledge the original window or supply release proof. A successful new physical attempt settles the unchanged obligation with a distinct dataset identity. |

The lease-expiry case controls only the SQL lease clock. The retired-receipt
case advances only the verifier's decision clock beyond the frozen deadline.
Every successful capture uses the actual Rust HTTP and receipt writer, S3
publication and PostgreSQL commit triggers. The publication interruption is an
exception at the Python publication boundary after Rust has fsynced its receipt;
it does not claim coverage of every possible operating-system crash instruction.

Run the focused native qualification with the test-only database URL supplied
through `TEST_DATABASE_URL` and a compiled collector at
`collector/target/debug/sm-collector`:

```bash
PYTHONPATH=src:tests POLARS_MAX_THREADS=2 \
  pytest -q tests/test_capture_release_qualification.py
```

Native verification on 2026-10-24 passed all seven scenarios in 7.98 seconds,
using the combined continuity core plus raw-error recovery revision `c1e989e1`
and release-evidence recovery revision `3f61724`. The business-error scenario
was first observed failing on the previous core because the diagnostic response
incorrectly acknowledged its capture obligation; the corrected protocol passes
without weakening that assertion.

Actual two-image Compose handover and host restart qualification are separate
checks under issue #89. These tests establish the single-VPS shared-spool
protocol boundary; they do not establish production TikTok acceptance, remote
VPS availability, source recoverability or protection against losing the entire
VPS disk before remote publication.

# Bounded supplied-text hydration

Delivery: U06 / #170, under the approved continued offline research work (#164).

`TranscriptInsights.read_text_page` reads one product's supplied text without
computing local-analysis visibility. The content aggregate uses this operation;
metadata selection and explicit transcript reads keep their existing interfaces.
It performs no source request, download, transcription or paid analysis.

Content aggregation resolves its effective cutoff once using the text module's
database clock (or preserves an explicit cutoff), then validates taxonomy
availability before selecting metadata or reading objects. The same cutoff
governs the hydrated page. An implicit cutoff requires a bounded database clock
read, but a future taxonomy performs no text hydration or S3 payload read.
An explicit unavailable taxonomy fails before SQL as well. Application clock
skew does not turn a default live read into a forbidden future cutoff.

The page admits between one and 200 transcript rows, plus one cursor sentinel.
Unavailable rows consume that budget. It retains the existing transcript-ID
cursor, availability cutoff, namespace and page-local canonical context choice.
Continuing pages can contain versions of the same logical text; page totals must
not be added as an independent audience count.

Selection and actual provenance-object hydration use one evidence transaction.
The individual and page readers share the transcript eligibility check, including
dataset/observation availability, point-in-time ownership, current live-source
verification and retention, and committed provenance. The retirement fence spans
the object read. Because policy changes and ownership writes are independent of
that fence, each canonical transcript is checked again immediately before its
object is hydrated. A prior metadata result is never a permission to read text.
These are the existing individual-read semantics, not a new durable database
snapshot or a distributed revocation protocol.

The metadata-only `eligible_texts` operation still checks local completed-result
availability and running/uncertain barriers. Canonical content reuse, retired
generation replacement, paid-provider barriers and daily reservations are
unchanged. The optimized page does not use or expose analysis state.

## Controlled measurement

Measured on 2026-10-28 using dedicated synthetic PostgreSQL schemas and an isolated
HTTP Moto object store. Each text has one completed free local analysis. The
baseline is the original metadata page plus explicit selected-text hydration;
the treatment is the combined guarded operation. Setup and analysis execution
are excluded. SQL counts instrument `psycopg.Connection.execute`, not server CPU
or every possible driver API. Scope and provenance counts instrument the
existing eligibility and publication guards. They are work counts, not provider
billing, end-to-end latency, target-VPS capacity or memory measurements.

| Texts | SQL baseline → treatment | Scope checks | Provenance guards | Analysis-state SQL |
| --- | --- | --- | --- | --- |
| 1 | 61 → 34 | 3 → 2 | 5 → 3 | 2 → 0 |
| 8 | 327 → 188 | 24 → 16 | 40 → 24 | 16 → 0 |

The measured additional-text slope decreases from 38 to 22 SQL executions.
Canonical complete aggregate bytes, including report IDs, remain identical at
the same cutoff. Transcript/context IDs, interpretation and bounded outputs are
unchanged. This deliberately retains the final authority recheck instead of
trading current eligibility for another reduction in database calls.

Reproduce the small native regression cases with the repository test environment:

```bash
PYTHONPATH=src:tests python -m pytest -q -s tests/test_text_hydration.py
```

The regressions compare complete reports with an independent adapter for the
previous reader, including context versions, unavailable records and cursor
pages. They also exercise future evidence, namespace and input budgets,
observation changes, ownership transfer, policy withdrawal/expiry and the native
retirement lock during actual object hydration. The existing transcript and text
selection suites remain the acceptance boundary for analysis reservations and
canonical reuse.

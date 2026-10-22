# Autonomous implementation decisions

The user authorized autonomous implementation, delegated sessions, integration,
and choosing recommended defaults on 2026-10-22. This record distinguishes those
implementation choices from external acceptance that software cannot grant.

## Chosen defaults

- Use AGENTS.md and the five approved standard triage labels. Repository and issue
  content use English; discussion with the operator can remain in French.
- Treat the approved PRD, six specifications, and fifteen delivery tickets as the
  working scope. Implement in a shared integration branch with explicit file
  ownership, then perform separate standards and specification reviews.
- Preserve the abandoned SQLite experiment outside the supported runtime. The
  supported deployment uses PostgreSQL, S3, Rust, Dagster, Polars, DuckDB, and
  Streamlit; no fallback silently replaces production storage with SQLite.
- Use GitHub Issues as the intended tracker. The integration rejects writes with
  HTTP 403, so local ticket files remain the publication backlog until access works.
- Default ranking is attention-only with all four dimensions displayed separately.
  Missing demand, competition, or cost inputs remain unknown. Commercial ranking
  and outcome evaluation require eligible imported or accessible evidence.
- Manual identity review precedes automated linking. Text matching proposes
  candidates only. Family changes and link transfers retain knowledge history.
- Start with a protected 20% exploration allocation and configurable 1/6/24-hour
  collection tiers. These are policy defaults, not claims about TikTok freshness.
- Enforce both endpoint/account and actual-credential budgets. A shared token uses
  the strictest configured budget encountered across its aliases. No configured
  budget means no live request. HTTP rate limits also block the shared credential.
- Process one bounded HTTP page per lease, with a durable page index independent
  of cursor contents. Default maximum is 1000 pages per task; reaching it records
  partial coverage and pauses the task instead of discarding collected evidence.
- Preserve rounded reach strings without numeric bounds when the source does not
  define a rounding rule. Require compatible sales periods and currencies.
- Evaluate 3/7/14-day outcomes and 2/10-day action-delay scenarios with frozen
  definitions. Asynchronous polls produce bounds or censoring; synthetic history
  never qualifies as real-market validation.
- Default controls bound collector requests, import batches, analytical product
  batches, row materialization, cache use, and DuckDB memory. Oversized work fails
  explicitly and requires repartitioning instead of claiming unlimited scale.
- Compaction preserves original observation provenance. Retention invalidates
  descendants, blocks re-publication, clears affected payloads and cache files,
  and purges applicable S3 object versions rather than merely adding delete markers.
- Use a test-only Moto HTTP service locally and external S3 in production. A
  pullable maintained MinIO image was not available from the examined official
  image location; this does not substitute Moto for durable production storage.
- Bind operator services to loopback and document SSH access. A single VPS is the
  initial deployment target; cloud provisioning and paid subscriptions are excluded.

## External acceptance

No TikTok keys or approved scopes were supplied. Live adapters remain disabled
until the explicit acceptance command verifies them. Commercial Content access
also requires permission for the actual intended use; the API name alone is not
approval for commercial product-trend research. No Research video API is used.

This workspace cannot run Docker or a native PostgreSQL service under an ordinary
database user. Functional SQL integration uses official PGlite plus HTTP Moto.
Native PostgreSQL concurrency, the Docker image build, and container backup/restore
are required checks in the provided CI and deployment workflow, not locally proven
facts. The user's VPS and S3 account have not been accessed or deployed.

Performance numbers apply only to the measured synthetic workload and environment.
No sales advantage, profitability, global TikTok coverage, or cloud capacity is
asserted from these engineering tests.

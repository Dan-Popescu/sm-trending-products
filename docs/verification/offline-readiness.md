# Offline engineering verification

Date: 2026-10-22. Scope: follow-up to merged PR #1, developed on
`codex/offline-readiness`. No actual TikTok credentials, approved capabilities,
production bucket or VPS were used.

## Executed application boundary

The full suite passed **159 tests, zero failures, zero errors and zero skips**
against native PostgreSQL 17.11 and temporary HTTP Moto 5.2.4 services in 42.03
seconds. [JUnit evidence](offline/tests.xml) retains the executed scenarios. The
[source manifest](offline/source-manifest.json) fingerprints runtime, test,
configuration and verification-driver files independently of documentation.
Rust collection uses the compiled locked collector and controlled loopback HTTP
responses. Ruff formatting/lint and mypy passed for the application.

New behavioral proofs include:

- `test_offline_delivery.py`: actual Dagster asset materialization invokes Rust
  collection, commits SQL/object evidence, publishes an assessment and frozen
  decision, then renders that real facade through Streamlit AppTest. Live queries
  remain empty and no source capability is promoted by synthetic work.
- `test_process_recovery.py`: separate worker processes terminate without cleanup
  before publication, after the raw object write and inside the final transaction
  before checkpoint update. After lease reclamation, the original page is recovered
  and pagination continues, leaving exactly two committed observations, no visible
  partial manifests and no duplicate delivery. All objects verify successfully.
- `test_scheduler.py`: A -> B -> A -> implicit selection retains A, preserves the
  definition's original availability and isolates live/synthetic activations.
  Four concurrent planners preserve the entire due frontier without duplication;
  versioned request-cost estimates affect persisted priorities.
- `test_dashboard.py`: real-shaped queue/source status populates the health cards
  and follows the selected workspace.
- `test_orchestration.py`: executed Dagster materializations distinguish fatal
  source failure from quota waiting, report actual retention counts, and fail
  pending deletion work explicitly. A native-storage regression records an actual
  Dagster PostgreSQL materialization with the checked-in module/driver configuration.
- Source acceptance tests retain an empty authorized response as unverified with
  an explicit observation count, rather than accepting T15 without observations.
- Benchmark tests cover failed object verification, parallel/skewed collection,
  compaction and bounded quota exhaustion with retained retryable work.

The first full run had 140 passes and sixteen setup errors because the newly
installed Cargo directory was missing from that command's PATH. Repeating with
the documented toolchain selection produced the complete clean result above.
A later full run also exposed a real UI cutoff precision bug: rounding the
default time down to whole seconds could hide a link created in that second.
The default now retains microseconds; the complete 159-test run passed afterward.
No application failure was hidden or counted as a passing test.

## Measured finite workload

[Native sample](offline/benchmark-1000.json): 1,000 observations, 50 products,
50 pages, four independent application workers, 10 ms configured source delay,
60% of source video identities assigned to the first product, and compaction.

All mandatory checks passed with zero collection errors or quarantined records.
All 50 tasks finished. Compaction reduced 200 normalized objects to four while
preserving 1,000 observations. The run took approximately 10.72 seconds, including
analysis, verification and decision freezing; cold/warm analysis took about
2.56/1.28 seconds. The report records request/byte traffic, process resource
accounting, disk/cache values, actual database version and explicit limitations.

This was a finite controlled workload on shared development infrastructure with
other verification work active. It is not a sustained throughput, full-stack
memory, production freshness, provider-cost or capacity guarantee. Worker-kill
tests establish the specified recovery windows separately from this measurement.

## Deployment boundary

The application image built successfully using an optional BuildKit CA secret
for the managed proxy. The full-service/recovery driver now starts worker,
Dagster daemon/webserver and Streamlit, probes their health, pauses/restarts
writers during backup, and runs isolated database/object restore. Execution
evidence for this expanded boundary is recorded separately when complete.

Full startup checks exposed deployment assumptions that application-only tests
could not detect: public bind mounts and copied source files must be readable by
unprivileged container users; internal Compose/restore hosts must be excluded from
external proxy routing; Dagster instance class references must use the modules
present in the locked runtime. The image/configuration/scripts address these.
The pinned Dagster PostgreSQL event storage requires the explicit psycopg2
SQLAlchemy dialect for its NOTIFY statement. PostgreSQL readiness uses TCP so
the temporary initialization socket cannot prematurely satisfy restore/startup.

The managed Docker daemon uses VFS. Multiple services exhausted the development
disk while old build snapshots were retained. Only build-cache records created
by this task were removed, preserving the application image and test database.
This environment constraint is not a measured production disk requirement.

## Acceptance and remaining work

This evidence advances T01/T03/T07/T13/T14 and the failure paths of the T15
verification tool. It does not automatically close their GitHub acceptance
checklists or claim a production sign-off.

- Reconcile each criterion with the reviewed tests and runtime evidence.
- Extend measured workloads to sustained queues, representative hardware,
  retention/compaction schedules, broader fault campaigns and service-level
  resource accounting; apply actual provider prices and required recovery goals.
- Verify the chosen production VPS, private access and real object-provider
  conditional-write/version/deletion behavior.
- Supply approved TikTok scope/intended-use evidence and tokens, verify actual
  response meanings/coverage and renewal/revocation behavior, and collect at least
  one inspectable real observation per enabled adapter.
- Collect real history and let frozen prospective outcomes mature before claiming
  market utility or realized profitability.

Reproduce the native suite with `scripts/offline-check.sh`; reproduce the isolated
sample with `scripts/offline-benchmark.py` as described in
[benchmarking](../operations/benchmarking.md). Use only an isolated test database.

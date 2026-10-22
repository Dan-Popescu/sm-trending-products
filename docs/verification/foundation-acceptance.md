# Foundational engineering acceptance

The agent reviewed T01 and T03 against their original criteria on 2026-10-22,
under the user's standing authority to verify and merge without human PR review.
These tickets specify controlled, no-key engineering scenarios. Real TikTok
acceptance remains T15; production-provider and sustained-load acceptance remain
T13/T14. They are not inferred from the checks below.

## T01: observation to visible evidence

| Criterion | Executed evidence |
| --- | --- |
| Same entity, counters, provenance, timestamps, missingness and synthetic class in review | [Offline delivery](../../tests/test_offline_delivery.py) executes Dagster through Rust, native PostgreSQL and HTTP object storage, then checks the actual Streamlit observation JSON: both counters, exact fixture timestamps, missing likes, source/class and raw dataset references. |
| Bounded collector through orchestration; review uses published output | The same scenario materializes the collection/assessment/decision assets. The controlled source is shut down before rendering the actual application facade in Streamlit. |
| Real temporary services and restart persistence | The scenario opens an independent restarted application and reads the two collected observations and published assessment. Native CI supplies PostgreSQL 17.11; fixtures start HTTP Moto. |
| Missing credentials/scopes remain explicit; fixtures never grant access | [Pipeline scenarios](../../tests/test_pipeline_integration.py) reject missing credentials and empty live responses; offline delivery asserts no capabilities were created and live assessments remain empty. |
| Secrets excluded; errors visible | [Collector contracts](../../tests/test_collector_contract.py) verify echoed-token redaction and authorization/rate/schema errors; [dashboard checks](../../tests/test_dashboard.py) verify safe rendering and unverified-access warnings. |
| Reproduction and supported runtime documented | [Native verification instructions](../operations/testing.md), `scripts/offline-check.sh`, locked Rust/Python dependencies and the declared PostgreSQL/S3 architecture define the supported boundary. |

## T03: interrupted collection

| Criterion | Executed evidence |
| --- | --- |
| Recovery at all three publication boundaries | [Process recovery](../../tests/test_process_recovery.py) terminates real worker processes before publication, after raw object write and before checkpoint advancement, then resumes two-page collection. |
| Stable delivery, later observation retained | Recovery yields exactly two distinct expected observations without duplicate datasets; [pipeline integration](../../tests/test_pipeline_integration.py) retains later observations and idempotent imports/replay. |
| Reclaim expired leases; fence stale completion | Native pipeline scenarios reject old owners after lease expiry/replacement. [Contention checks](../../tests/test_native_concurrency.py) retain active leases, isolate attempt exhaustion by workspace and skip locked exhausted rows while claiming runnable work. |
| Durable checkpoints; repair staged publication intent | Recovery checks unchanged page/cursor before retry, visible staging records, repair of the same dataset IDs, complete final manifests and no object keys outside the final reference graph. |
| Inspectable retries and actual services | The same checks observe public dataset/task state and remote objects; operator history exposes staging/quarantine metadata. PostgreSQL and the HTTP object store actually execute the transitions. |

## T04: shared quotas and bounded source failure

| Criterion | Executed evidence |
| --- | --- |
| Shared quotas across independent workers | Native contention checks use eight PostgreSQL backends and multiple account aliases of one credential; capacity is never exceeded. Pipeline integration executes independent Rust processes and prevents the second HTTP request after quota exhaustion. |
| Source retry hints take precedence | Pipeline tests exercise HTTP 429 with 120-second and two-day hints, checking task due time and both account/shared block times. Another alias cannot reserve during the source block. Impossible timestamp delays quarantine the task explicitly. |
| Quota exhaustion retains bounded retryable work | The native CI quota scenario accounts for every task (one completed, three pending); source-limit tests do not advance data or immediately retry, and quota waits do not consume failure attempts. |
| Bounded requests, timeouts, cancellation and queue | An actual Rust request waits on a suspended HTTP source; the configured timeout terminates and reaps its process, leaves no dataset and retains retryable work. Scheduler backpressure and pagination tests enforce bounded queue/page work. |
| Inspectable budgets, limits, oldest work and intervals | Streamlit renders actual budget counters/queue due times and the published one-hour observation interval in the controlled delivery scenario. Pipeline status exposes persisted rate-limit/timeout categories. |
| Credential material excluded | Rust source-contract tests and dashboard checks verify redaction. Credential sharing uses opaque keys in PostgreSQL; synthetic fixtures do not bind real credentials or promote source capabilities. |

Valid source retry windows are not shortened to a local refresh cap. The process
adapter validates that a finite nonnegative delay fits a durable timestamp;
invalid hints produce the existing quarantine state with `invalid_retry_hint`.
This adds no alternate scheduler, sleeping worker or persistence mechanism.

## Execution and remaining frontier

PR #24 passed 162 native tests and full-service backup/restore. The dedicated
recovery fix [PR #26](https://github.com/Dan-Popescu/sm-trending-products/pull/26)
passed 167 tests plus native benchmark/quota and deployment/recovery checks in
[CI](https://github.com/Dan-Popescu/sm-trending-products/actions/runs/39839205626).
The strengthened visible-evidence and staging/object-graph assertions above also
passed locally against native PostgreSQL and HTTP Moto. Their exact-head full CI
is the merge gate of the acceptance PR; its result is retained on that PR.

The established-credential alias deadlock is addressed in merged PR #27 with an
additive migration and an upgrade-preservation check. Its full CI passed 170
tests and deployment/recovery. Long retry-window and process-timeout checks add
the remaining T04 proofs; their exact-head CI is retained on the subsequent PR.
Next priorities are the replay/identity contracts and their dependent ranking
and evaluation criteria, ahead of additional operating-envelope variants.
Ticket acceptance does not close the umbrella PRD or specification parents.

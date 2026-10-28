# Attribute bounded operational diagnostics to analytical attempts

Status: adopted under the owner's follow-up authorization on 2026-10-28.
Delivery: #156. Preserves ADR0003/0008/0009/0019.

## Decision

Keep one disposable PostgreSQL diagnostic row per public assessment or frozen
decision attempt. Its random attempt ID is distinct from the deterministic result
ID. Timings, counters and errors remain outside analytical payloads, immutable
provenance and result hashes. No new execution framework or event stream is added.

`AnalyticalDiagnostics` owns attempt lifecycle, context attribution, bounded persistence,
pruning and inspection. A context variable binds events to the innermost active
attempt for the exact History instance. Source traffic and analysis events update
the existing cumulative counters through the same hooks that attribute their
increments. Shared-process counter subtraction would mix concurrent attempts;
per-attempt counters instead receive the actual events. Nested calls restore the
parent context after their own attempt finishes.

Phase hooks time existing operations. Decoded rows and object reads count physical
work, including repeated or discarded reads after a budget split, rather than
unique evidence. Metadata admission reports charged bytes and objects, including
partial admission before failure. Row counters are accumulated per native batch;
there is no per-observation diagnostic storage or unbounded list of labels.

An implicit assessment cutoff is resolved at the existing point after its initial
retention guard. A running marker can therefore have a null cutoff. Recording an
attempt earlier must not move the analytical clock across that guard.

Persist running state before analytical work and completion after normal return or
an exception. Error types are bounded; exception messages, SQL, tracebacks and raw
evidence are excluded. Ordinary diagnostic write failures log only attempt ID and
exception type, preserving the successful result or original analytical exception.
SIGKILL, a lost completion write or process loss can leave a running marker with
incomplete counters. It is not a lease or proof that work is still active.

## Bounds and tradeoffs

The configured limit is 1–10,000 attempts per live/synthetic namespace, default
1,000. Start and completion upserts use a short schema/namespace advisory fence and
prune in the same transaction, including old running markers. Ordering is by start
time and attempt ID, so a long-running old attempt can be evicted even if it
finishes later. Summary JSON has a 32 KiB SQL constraint and fixed-size scalar
headers. Limits bound retained logical records, not PostgreSQL MVCC/index bytes.

Persistence uses a 200 ms pool wait, 100 ms SQL lock timeout and 500 ms statement
timeout. These reduce contention; they are not an end-to-end latency guarantee.
Two small writes per attempt and counter/timing hooks add work. Their production
overhead and any throughput benefit remain unmeasured. Failed writes can make
diagnostics incomplete, which is preferable to making telemetry an analytical
availability dependency. No periodic heartbeat or recovery controller is added.

SQL041 adds only the disposable `analytical_run_diagnostics` table. It has no evidence foreign keys and no copied observations or product
identifiers. Its result ID is an operational label, not a required object-store
dependency. Source invalidation and backup object inventories keep their existing
authoritative boundaries. Inspection uses only SQL and preserves namespace scope.

The durable source revision discussed in the earlier reassessment remains deferred:
main #155/#157 add analytical explanations, not independently restartable distributed
partitions. That execution model still requires an explicit source/metadata
revision, retention lifetime and replay contract before implementation.

Public Pipeline/CLI/native regressions cover hash preservation, cold/warm/delta/
full paths, metadata and row splits, concurrent and nested attribution, diagnostic
storage failure, bounded retention, sensitive validation messages, actual CLI
termination and preservation of the original default-clock ordering.

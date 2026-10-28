# Inspect analytical execution work

Published assessments and frozen decisions produce disposable operational attempts
in PostgreSQL. Repeated identical results keep the same analytical ID and have
distinct attempt IDs. Other analytical operations, such as maturation, backtests
and product trend explanations, are outside this first diagnostic scope.

## Read recent or selected attempts

The normal explicit `sm-trends init` initializes this application module without
advancing the capture schema or changing signed-release compatibility. Module
initialization failures emit a warning and preserve ordinary capture setup.
Inspection does not initialize tables; retry explicit initialization after fixing
an unavailable module. Existing captures and analytical results remain authoritative.

Use the normal configured application connection. These commands read only SQL;
they do not load evidence or contact S3:

```sh
sm-trends analysis-runs --limit 20
sm-trends analysis-runs --synthetic --limit 20
sm-trends analysis-run 0123456789abcdef0123456789abcdef --synthetic
```

Output is JSON. The list limit is 1–100, default 50. Invalid limits and identifier
syntax fail before application configuration is loaded. A selected attempt absent
from the requested namespace returns `null`. `--help` includes examples.

Headers contain `attempt_id`, `operation` (`assessment` or `freeze`), `synthetic`,
`cutoff`, start/finish timestamps, state, result ID and bounded error type. A
completed success references its existing assessment/evaluation ID. Failure stores
the exception's type, never its message or traceback. An implicit assessment
cutoff is null until its existing retention guard finishes and selects the clock.

`running` means a start was recorded without a persisted completion. It can mean
active work, process loss, SIGKILL or a failed final diagnostic write. Its counters
remain the initial incomplete snapshot; there is no progress heartbeat. Confirm
the ordinary authoritative result/worker state before deciding to rerun anything.

## Interpret the summary

`summary.version` is 1. Counters are attributed to this execution and History
instance. Nested attempts own their own events; concurrent attempts cannot borrow
each other's traffic. Existing cumulative usage/benchmark counters retain their
meanings.
Diagnostic storage counters reattribute existing usage; adding both views would
double-count traffic.

| Field | Meaning |
| --- | --- |
| `elapsed_ms` | Analytical scope elapsed time, excluding its own start/completion diagnostic persistence. |
| `phase_ms` | Inclusive elapsed durations summed over repeated phase invocations. Phases can overlap; do not add them to infer total elapsed time. |
| `storage` | Existing GET/PUT/HEAD, download/upload bytes, file-cache hits and HTTP-attempt counters, attributed by event. These inherit existing storage accounting, not packet-level wire bytes. |
| `analysis` | Projection hits/misses and full/delta/duplicate-rebuild windows. A hit can perform no file read. |
| `work.products` | Products presented to assessment preparation. |
| `work.metadata_partitions` | Raw-window metadata admission attempts, including failed partitions. |
| `work.metadata_objects`, `metadata_bytes` | Admitted current/prior object metadata and charged bytes, including scope/membership overhead. Totals include repeated/partial admission before retries; bytes are not literal network traffic. |
| `work.metadata_peak_objects`, `metadata_peak_bytes` | Maximum admitted charge in one partition. These are not process/native RSS. |
| `work.source_objects_read` | Source-object read attempts, including verified cache reads and failures. |
| `work.source_rows_materialized`, `local_rows_materialized` | Rows returned by native frame collection before decode, including a budget sentinel. |
| `work.source_rows_decoded`, `local_rows_decoded` | Successfully decoded observations, including duplicates or discarded work before a later failure/retry. |
| `work.partition_splits` | Adaptive partition splits after row or metadata refusal. |
| `reasons` | Fixed mechanism names and counts; reasons can overlap and are not mutually exclusive product outcomes. |
| `limits` | Batch, row, object, metadata and retention limits captured at entry. |

Absent work/reason keys mean zero. Phase names cover input selection, feature
projection lookup, raw metadata selection (including fence wait), payload reads,
calculation, provenance, projection writes, normalization and final publication.
Elapsed time includes work not assigned to a phase; these are diagnostic regions,
not a complete CPU profiler.

Feature miss reasons start with `feature_` and use the existing projection
eligibility reasons. Raw reuse may decline for unavailable state/cache, context or
clock mismatch, changed source membership, decoder failure or duplicate delivery.
Optional metadata/coverage refusals name their budget. `feature_write_declined`
and `raw_write_declined` report an optional store refusal without guessing which
internal guard rejected it. Required refusals use `row_budget`,
`metadata_object_budget` or `metadata_byte_budget`.

For example, two 60-row source objects exceed a 100-row shared partition only
after decoding row 101. Retrying two separate products decodes 60 rows each. A
successful diagnostic therefore reports 221 decoded rows and one split, although
the final input contains 120 unique observations. This distinguishes work from
coverage and helps explain why increasing concurrency alone may add cost.

Compare cold, unchanged and changed attempts at stated cache conditions. A large
decoded count with few GETs can indicate repeated parsing of cached files; repeated
full windows or optional-cache refusals may indicate cache/metadata pressure.
Use benchmark CPU/RSS and SQL/service measurements to confirm a bottleneck before
selecting compaction, larger partitions or parallel execution. These records do not
establish a production capacity or an automatic optimization policy.

## Retention and availability

Set `ANALYTICAL_DIAGNOSTICS_MAX_RUNS` to 1–10,000; default 1,000 per namespace.
Compose forwards it to application services. Each successful diagnostic write
enforces that writer's configured cap atomically, keeping the latest starts.
Changing the limit takes effect on subsequent successful writes. Old running
attempts are disposable, and an evicted attempt can remain absent after it finishes.
Keep this configuration consistent across workers.

Summary JSON is limited to 32 KiB plus bounded headers. Logical row limits do not
bound MVCC/index disk usage; normal PostgreSQL vacuum/monitoring still applies.
No raw observations, source/entity/product lists, exception messages or credentials
are copied into diagnostics. Result IDs do not add object-store dependencies.

Diagnostic writes are best effort with short pool/statement/lock timeouts.
An unavailable diagnostic table or writer preserves the analytical result or its
original exception and emits a warning containing only attempt ID and error type.
There is no end-to-end timing or diagnostic-completeness guarantee during database
failure. The module adds two writes per attempt; measure overhead on representative
loads. See [ADR0020](../adr/0020-operational-analytical-run-diagnostics.md).

# Resource-budgeted maintenance

The maintenance service provides `retention_drain` and `compact_drain` for repeated
scheduled work. Existing `retention` preview/apply and single-group `compact`
operations remain available. A bounded call can finish part of a backlog; callers
must inspect its result rather than equating one successful invocation with an
empty backlog.

The deployed retention and compaction jobs now run every 15 minutes by default,
independently of collection and ranking. Configure `MAINTENANCE_SCHEDULE_MINUTES`
(a divisor of 60), `MAINTENANCE_MAX_SECONDS`, `RETENTION_MAX_BATCHES`,
`RETENTION_MAX_OBJECTS`, `RETENTION_MAX_OPERATIONS` and `COMPACTION_MAX_BATCHES`.
These are per-invocation bounds, not a measured production capacity claim. The
global Dagster concurrency cap still applies. Track backlog age and achieved
drain rate against input before changing resource limits.

The deployed coordinator has one run slot shared by all jobs. Configuration
therefore requires `2 * MAINTENANCE_MAX_SECONDS` to be strictly less than
`60 * MAINTENANCE_SCHEDULE_MINUTES`: retention and compaction must leave time
within their interval for other jobs. Defaults allocate two 120-second budgets
inside a 900-second interval. This admission check rejects profiles whose budgets
already fill or exceed the interval. The budgets remain cooperative, so atomic
SQL/storage work can overrun them; queue age and measured service times still
establish actual headroom. The retention job also records
`oldest_pending_object_age_seconds`, including when no expired roots remain but
physical deletions are waiting.

CLI examples:

```sh
sm-trends retention --apply --drain --max-seconds 60 --max-objects 5000
sm-trends compact --drain --max-batches 12 --max-seconds 60
```

Ordinary small retention previews and single-pass apply retain their existing
behavior. Wide previews report an explicit lower bound instead of computing an
unbounded shared-object closure. Use `--drain` to continue oversized removals.
The scheduled job distinguishes expected partial work from remote failures and
inconsistent artifact lifecycles. Budget backlog and pending invalidations remain
durable for the next iteration; deletion failures and inconsistent lifecycles fail
the job and retain uncompleted work for retry.

## Retention

`Maintenance.retention_drain` accepts these independent limits:

| Limit | Default | Boundary |
| --- | --- | --- |
| `max_batches` | 100 | At most this many retirement transactions, and this many finalization transactions. |
| `max_objects` | 10,000 | Maximum pending keys attempted during the invocation. |
| `max_seconds` | 120 | Cooperative deadline between transactions and remote operations. |
| `max_operations` | 20,000 | S3 API calls, including versioning inspection, version lists and deletions. |

Each retirement transaction starts with at most 100 expired roots. Its complete
lineage and shared-object closure is invalidated under the existing publication
fence before objects become eligible for deletion. Each affected metadata family
and object queue is limited to 10,000 rows per transaction. If that limit is
exceeded, the transaction rolls back and retries with half as many roots, down
to one. A bounded immediate-work probe also detects more than 1,000 adjacent
dependencies, and a two-second statement timeout catches expensive hidden
closures even below the output-count cap.

An oversized single root registers a durable source invalidation and returns
`status=partial`, `stop_reason=invalidation_pending`. Its live or synthetic
namespace denies evidence reads and publication until all affected lifecycles
are retired. Other products in that same namespace temporarily retry too; the
opposite namespace and operational status remain available. Unknown legacy
object-key namespaces conservatively share the gate. No timeout clears a gate.

The next worker or maintenance iteration discovers and applies bounded frontier
chunks of at most 1,000 work units. Discovery completes before lifecycle changes;
progress and each chunk's changes commit together. Each maintenance invocation
can resume up to 1,000 continuation chunks within its existing time budget,
independently of `max_batches` for ordinary root/finalization batches. Object
deletion continues through the existing retention outbox after retirement.
`invalidation_progress` reports this separate work; ordinary `dataset_count`
counts the synchronous retirement batches performed by the call.

The CLI collection daemon keeps running on typed `InvalidationPending`: it drains
bounded work, briefly yields while progress continues, and backs off on contention.
It neither exits for Docker restart backoff nor waits for a human to refresh a
page. An in-flight collection or outcome interrupted by a newly committed gate
releases its lease without consuming a failure attempt; an outcome keeps its
original due time. Status includes `pending_invalidations`. Normal PostgreSQL
dumps include both continuation relations, so a restored pending gate remains
closed before automatic continuation. Application initialization preserves the
checkpoint rather than silently altering its fingerprint.

Pending keys are read in stable `(created_at, object_key)` pages of at most 1,000.
A failed key remains pending with its safe error category and attempt count; the
same invocation moves on to later keys rather than spending the entire budget
retrying the oldest failure. The next invocation retries failed keys. Versioned
objects consume operation budget for every version-list and bulk-delete API call.
If the budget ends during a key, that key remains pending until a later invocation
confirms that all versions and delete markers have disappeared. Database
finalization also runs in bounded chunks after physical deletion.

After an artifact becomes `deleted`, finalization also removes its outgoing
artifact edges, source-input associations, and links from invalidated evaluations.
Each transaction deletes at most **1,000 association rows in total** across those
three tables. Incoming links from other artifacts are owned by their parents and
remain until those parents are deleted. Live artifact relationships and publication
and object-retention tombstones remain authoritative. A partial index on pending
deleted publications and bounded keyset pages avoid rescanning already cleaned
history. Concurrent finalizers skip locked parents; relation deletion and its
progress marker commit together under the publication fence.

A link from a live evaluation to a deleted artifact is retained. The drain reports
`status=blocked`, `stop_reason=artifact_gc_live_evaluation`, and up to 20 affected
`artifact_gc_blocked_parent_keys` for diagnosis, while continuing past that parent
to collect other eligible relations. Repair the inconsistent evaluation lifecycle
before retrying. SQL deletion makes dead row space reusable through PostgreSQL
vacuum; it does not immediately shrink relation files on disk.

The overall maintenance time limit remains cooperative between ordinary database
statements, cache-lock waits and storage calls. Continuation statements additionally
use the caller's remaining deadline, capped at five seconds, with a 250 ms row-lock
timeout and nonblocking advisory-lock acquisition. Existing network timeouts and
bounded SDK retries still apply. API-operation accounting excludes the SDK's
additional transport retry attempts; storage counters record HTTP attempts.

Results include processed dataset/key counts, `operations`, `elapsed_seconds`,
`status` and `stop_reason`. The following counters describe all remaining durable
work, independently from the at-most-1,000 returned dataset identifiers:

- `expired_root_count` and `oldest_expired_root_age_seconds` (age since expiry);
- `pending_objects` and `oldest_pending_object_age_seconds`;
- `pending_finalization_count`;
- `pending_artifact_gc_count` (deleted publications whose associations are not
  fully collected);
- `pending_source_invalidations` (namespace-gated source removals).

`artifact_relations_deleted` counts association rows removed during this call;
`artifact_gc_completed_parents` counts publication tombstones whose association
cleanup finished. These metrics are included in scheduled retention metadata.
An invocation with pending artifact cleanup cannot report `status=complete`.

`preview_truncated` describes whether affected dataset identifiers were omitted.
`dataset_count_is_lower_bound=true` and `preview_scope=expired_roots_only` identify
a wide preview whose reported count omits undiscovered descendants; it does not
claim to be a full closure count. A bounded ordinary preview uses
`preview_scope=bounded_closure` and still separately reports the global expired-root
backlog. Remote denial and exhausted budgets remain visible as partial work.
Retention applies to live and synthetic evidence under verified source policies.

## Compaction

`Maintenance.compact_drain` defaults to 24 groups and a 120-second cooperative
limit, with at most 100 parent datasets per group. The existing per-group row cap
remains the smaller of 100,000 and the configured analytical row limit. Every
group independently publishes and fences its immutable result; a deadline or
restart never requires repeating already committed groups.

The invocation captures a database commit-time cutoff and excludes newer
publications from its input groups (legacy missing commit times use creation
time). Consequently it does not immediately read and rewrite aggregates it just
created or recovered from a previously staged publication. A later invocation may
merge compatible aggregates when useful. Current collection can continue
independently and newer pages enter the next pass.

`pass_backlog` describes the remaining inputs eligible at the invocation cutoff;
`active_backlog` includes newly published outputs and new collection. Both report
candidate dataset count, mergeable compatibility groups and oldest candidate age.
`status=complete` means that this pass has no mergeable input group; the active
backlog may contain a future mergeable group made from this pass's outputs.
Compaction preserves original observation/availability/provenance and does not
retire original raw objects or promise storage savings from their deletion.

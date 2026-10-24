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

Retention preview and old single-pass apply remain unchanged unless `--drain` is
specified. The scheduled job distinguishes expected budget-limited partial work
from remote failures or an atomic closure that exceeds its safe limit. Budget
backlog is recorded for the next run; actual deletion failures and blocked
closures fail the job and retain all uncompleted work for retry.

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
to one. A single root exceeding the atomic limit rejects with `status=blocked`
and `stop_reason=retention_closure_limit`; it never retires an arbitrary subset
of a restricted lineage. Resolve that unusually large shared lineage before
retrying. The result does not claim restriction-driven deletion has completed.

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

The time limit does not cancel an already running database statement, cache-lock
wait or storage call. Existing statement/network timeouts and bounded SDK retries
still apply. API-operation accounting excludes the SDK's additional transport
retry attempts; the storage usage counters record HTTP attempts separately.

Results include processed dataset/key counts, `operations`, `elapsed_seconds`,
`status` and `stop_reason`. The following counters describe all remaining durable
work, independently from the at-most-1,000 returned dataset identifiers:

- `expired_root_count` and `oldest_expired_root_age_seconds` (age since expiry);
- `pending_objects` and `oldest_pending_object_age_seconds`;
- `pending_finalization_count`;
- `pending_artifact_gc_count` (deleted publications whose associations are not
  fully collected).

`artifact_relations_deleted` counts association rows removed during this call;
`artifact_gc_completed_parents` counts publication tombstones whose association
cleanup finished. These metrics are included in scheduled retention metadata.
An invocation with pending artifact cleanup cannot report `status=complete`.

`preview_truncated` describes whether affected dataset identifiers were omitted.
An ordinary read-only preview still covers one root batch and separately reports
the global expired-root backlog. Remote denial and exhausted budgets remain
visible as partial work, not successful deletion. Retention applies to both live
and synthetic evidence under verified source policies, as before.

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

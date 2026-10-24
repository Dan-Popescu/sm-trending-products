# Continue large source removal behind a durable namespace fence

Status: adopted for the approved massive-collection delivery, issue #69.

The previous synchronous closure rejected more than 10,000 affected datasets or
artifacts atomically. Removing this cap without changing eligibility would either
hold an unbounded transaction or expose descendants between retirement chunks.

Keep the existing synchronous path for small removals. Count bounded immediate
work before attempting a closure: a shared cluster below 10,000 result rows can
still generate quadratic recursive join work. A two-second atomic statement
budget also catches hidden dense descendants. On an oversized or expensive closure,
register a durable catalog continuation under the retention advisory fence. The
work relation records only progress; existing dataset/publication/evaluation
states remain authoritative. A pending continuation fences evidence reads and
publication in its live or synthetic namespace. The other namespace and operational
status remain available. Unrelated products in the fenced namespace temporarily
retry: this deliberately trades availability during a rare broad removal for
constant-cost eligibility checks instead of transitive graph walks on each read.
Unknown legacy object-key namespaces use a conservative gate, not a guessed scope.
Existing malformed cross-namespace relationships inserted through trusted raw SQL
are outside the supported publication contract. New known source/publication
associations reject cross-workspace dependencies at their authoritative SQL seam.

Discover all immediate dependents before changing their lifecycles. Stable private
branch cursors use native indexed keys; shared objects have their own frontier,
so one wide adjacency list never becomes a Python list or repeated global sort.
SQL handles the private frontier loop and terminal artifact batches, avoiding a
round trip for every empty branch in a long lineage. Both sides of artifact joins
carry the native key lower bound; a lower bound on only one side allowed the
planner to restart the other merge-join input from zero on every page. Reverse
evaluation dependencies use `(dataset_id,evaluation_id)` so unrelated history is
not scanned for each source. These indexes add write/storage cost in exchange for
bounded parent lookup; the work table keeps only active progress, not full history.
Each transaction admits/processes
at most 1,000 work units. Application occurs in bounded type groups and deletes
completed progress in the same transaction. A committed chunk is idempotent;
an interrupted chunk rolls back, and a durable gate never expires on a timer.

The processor uses a nonblocking exclusive advisory lock and short bounded
backoff. Ordinary namespace gates retain the shared fence through actual reads.
Publication triggers also gate staged/committed rows and dependency associations.
A private transaction-local processor context permits only the ongoing retirement;
it is not carried to callers or later pooled transactions. Database statement and
row-lock timeouts remain bounded. A continuation statement's timeout follows the
remaining caller deadline, capped at five seconds. The private SQL loop leaves
time to commit its cursor before that deadline.

The existing maintenance drain starts continuation for an oversized single root,
resumes pending work, then performs its normal object deletion/finalization/GC.
Collection iterations and evidence reads also attempt bounded progress before
returning a typed retry. The CLI loop actively continues pending work and yields
between calls instead of exiting or sleeping its ordinary idle poll interval.
Actual SQL/read transactions recheck the gate after preflight: manifest selection
alone cannot protect later exploration, entity matching or scheduling. Catalog's
transaction boundary translates only the pending-invalidation SQLSTATE/message
into typed backpressure after rollback, including empty-input publications.
Other SQL prerequisite failures retain their original error classification.
Late pending collection/outcome work releases leases without failure backoff.
Backup dumps include both journal relations; restoring a
pending checkpoint retains its gate. Starting the application does not silently
clear it or mutate checkpoint fingerprints. Normal workers/maintenance resume it.

Retirement still covers replay/compaction/shared-object lineage, immutable artifact
parents, frozen outcomes and copied feature payloads. SQL027 continues removing
feature copies and queuing local-window bytes. Retention queues physical objects
through the existing outbox and repairs current entity origins using the same
metadata-only helper as small retirement, restricted to the current entity batch.
No multi-host physical-cache acknowledgement protocol is introduced.

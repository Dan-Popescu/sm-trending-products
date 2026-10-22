# Select evidence through compact delivery references

Status: adopted under the user's performance-correction authorization.

PostgreSQL indexes normalized object descriptors, entity membership, and compact
observation delivery references. The references contain identity, observation and
availability times, a canonical payload fingerprint, and the immutable object
reference. Metric and attribute payloads remain in S3 Parquet. Publication and
compaction insert references in their existing fenced SQL transaction; they do
not create a separate asynchronous publication path. Invalidating a dataset
removes its lookup references along with its object index.

Analytical reads select relevant descriptors in SQL and stream metadata through
keyset pages. Missing time bounds in legacy descriptors are conservative matches.
Interval-report reads do not apply observed-time bounds to metadata, because a
report can describe a different period while remaining subject to availability.
Evidence review selects stable `(observed_at, observation_id)` pages in one SQL
statement snapshot, checks
conflicting known payload fingerprints, and verifies only the selected S3 objects.
Replayed deliveries deduplicate while the oldest active delivery preserves source
provenance. The availability cutoff and resolved product ownership remain fixed
for each cursor scope. The page retains a shared retention fence through its
verified file reads, so concurrent retirement cannot remove an input halfway
through the response or turn a partial reference lookup into a false final page.

Migration backfills object descriptors without reading Parquet. Existing object
contents acquire their observation references lazily, in Arrow batches of 1,000
rows and at most `HISTORY_INDEX_OBJECTS_PER_CALL` objects per request. An unfinished
slice reports `status=indexing`; it never presents partial history as an empty
result. The old list-returning evidence API raises an explicit pending error.
A large legacy object may still take substantial I/O time; the slice bounds
objects and materialization, not a promised wall-clock duration. Smaller legacy
compaction targets and measured operator timeouts remain deployment choices.
This adds reference/index storage and write work to PostgreSQL in exchange for
avoiding complete history scans on each operator page. Native regressions verify
upgrade, bounded progress, cutoff/identity behavior, conflicts, and selected-object
read counts.

Normalized layout uses `HISTORY_TARGET_ROWS_PER_FILE` and the configured maximum
bucket fanout. Small source pages retain one durable raw object, one normalized
file, and one manifest where they fit the target. Larger batches retain bounded
hash fanout, with exact entity memberships for pruning. Compaction applies the
same layout policy, chooses a mergeable group before fetching manifests, and runs
one bounded group per scheduled hour. Original observations, availability and raw
provenance are preserved. Provider cost and workload selectivity still require
measurement; a row target does not promise a fixed compressed byte size.

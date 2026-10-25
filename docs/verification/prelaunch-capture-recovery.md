# Durable capture recovery before deployment

Scope: approved prelaunch correction #99, baseline
`ac5ddcb9e8713e311b6772528e48b0ee8ade9b33`. This is controlled engineering
evidence, not deployed VPS or authorized live-source acceptance.

The recovery path publishes a previously captured body without new source HTTP
admission. Source quotas, cooldown and the source-attempt limit cannot strand
that body. Exact task identity, original acquisition generation/clocks, worker
admission, lease fences, legal removal and immutable publication remain checked.
The existing priority-ordered source path alternates with receipt recovery;
ordinary claims that adopt an older receipt discount only their own increment.

## Behavioral checks

Native PostgreSQL, the actual Rust capture binary, controlled one-shot HTTP pages
and HTTP S3 fixtures exercised:

- Eleven actual `boto3.put_object` failures after SQL staging, then recovery with
  an exhausted source token bucket and an hour-long cooldown. Only one source
  HTTP request occurs. Earlier source failures remain counted during retries.
- Exact `attempt_limit` quarantine recovery, final and complete partial-file
  crash boundaries, original acquisition metadata and removed-token adoption.
- A crash after committed business-error publication but before its task attempt
  settles, including a replacement after an older image already removed the
  local receipt. Rate limits settle once without erasing earlier source failures.
- Worker draining and unrelated access quarantines remain blocked. Repeated
  invalidation pauses and ordinary Rust adoption do not double-discount attempts.
- Future-due active task prefixes do not starve later eligible archived errors.
  Receipt backlogs during S3 failure still admit higher-priority fresh source
  work with batch sizes one and four.

Focused command, with the repository's native test dependencies and collector:

```sh
PYTHONPATH=src:tests pytest -q \
  tests/test_capture_publication_recovery.py \
  tests/test_raw_capture_recovery.py tests/test_raw_capture.py \
  tests/test_capture_spool.py tests/test_capture_retirement.py
```

Result at `edd331b73904867226f37ec29170e72b56794bc3`: **39 passed in 57.46
seconds**. Ruff formatting/lint and Mypy passed. Nine additional independent
native probes passed in 10.22 seconds, including twelve upload failures,
revocation between receipt read and task claim, original acquisition generation,
corrupt receipts, and both single-item and four-item batch fairness. The final
integrated revision and its complete checks are recorded in the pull request;
this note does not substitute for that CI result.

## Recovery task-index experiment

[Raw SQL plans, timings and fixture parameters](prelaunch-capture-recovery-index.json)
record the exact SQL called by `Catalog.capture_recovery_candidates`. A complete
migrated isolated PostgreSQL 17.11 schema contained 100,000 completed tasks,
50,000 pending tasks, 50,000 unexpired leased tasks and 1,000 exact attempt-limit
quarantines. Normal obligation triggers remained enabled. All active tasks were
future-due, intentionally testing a prefix that must still advance by bounded
pages. The shared PostgreSQL container had a 1 CPU / 2 GiB limit.

The fixed page size was 20. Without migration 036's partial index, the exact query
visited and filtered 100,000 completed tasks: 48.451 ms SQL execution. After index
creation it returned 20 potential tasks through the partial index, with four
shared blocks and no removed rows: 0.026 ms SQL execution. A later quarantine
cursor also returned 20 rows without filtering historical rows.

The full Python method's 11 samples had median 2.113 ms and maximum 39.422 ms on
the shared test host. These samples do not establish a latency percentile or an
end-to-end pipeline improvement. The stronger evidence is the bounded query
work: due/expiry eligibility is returned as a flag after selecting the indexed
page, rather than filtering an arbitrarily long future prefix first.

Building the partial index took 0.419 seconds and allocated 9,674,752 bytes in
this fixture. Seeding the 201,000 tasks and their obligations took 48.293 seconds,
outside index/query timers. Before/after SQL timings differ in cache warmth;
the row-work comparison, rather than a claimed speedup ratio, supports the
index. Schema construction on larger live tables can take longer and requires
write-lock and temporary-space planning. Recovery SQL still functions on schema
35 during a bridge release, but its indexed work bound requires migration 036.

No source collection throughput, S3 outage duration guarantee, whole-VPS CPU/RAM
maximum or live source permission follows from this task-metadata experiment.

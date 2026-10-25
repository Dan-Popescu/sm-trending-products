# Pending normalization selection

Delivery: [#101](https://github.com/Dan-Popescu/sm-trending-products/issues/101).
Scope: [prelaunch corrections](../specs/prelaunch-massive-collection.md).
Baseline: `ac5ddcb9e8713e311b6772528e48b0ee8ade9b33`.

## Behavior and ownership

Schema 037 adds `capture_publication_pending`, a disposable three-column
projection containing only potentially unfinished raw parents. PostgreSQL hooks
maintain it when parent or v1-child state, lineage, namespace or manifest changes.
They also cover compatible old-image writers and ordinary source-free replay.
A committed transformation-v1 child remains the sole completion fact; lease
cleanup and the projection never become an independent completion lifecycle.
Staged and v2 children do not settle v1 publication. Retiring a v1 child
reconsiders its parent; retiring a parent removes its pending eligibility.

The claim sorts pending candidates by the existing retry/creation/identity order,
then probes and locks one authoritative parent at a time. The per-parent probe
rechecks its namespace, immutable raw protocol, committed-child absence and
current lease eligibility. `SKIP LOCKED`, parent-before-lease acquisition,
takeover fences and existing publication guards remain in effect. Only the
selected parent is locked; an expired or concurrently acquired lease cannot
authorize overwriting another owner.

An independently reproduced snapshot race also required a mutation-time guard:
a claim that started before another worker's commit could otherwise replace its
live lease after the parent lock became available. New UPSERTs require actual
expiry and retry eligibility at the conflict boundary. Schema 037 also rejects
premature owner/fence changes by compatible old unconditional UPSERTs, while
allowing their ordinary same-fence failure/backoff updates. No parent lock is
acquired inside this lease guard.

Two `OFFSET 0` clauses intentionally prevent PostgreSQL from flattening the
pending selector and the child lookup into full-history joins. A simple join
looked fast with fresh statistics, but scanned all 10,000 completed parents in
a controlled empty-queue probe after backlog statistics became stale. A naive
outer sort around a locking lateral query also locked all pending parents;
the final query sorts before the per-parent lock and stops at the first result.
Behavioral tests preserve both properties, rather than asserting SQL spelling.

## Expansion and compatibility

Migration 037 installs the dataset hooks and backfills existing eligibility in
one transaction under a dataset lock that drains writers and row-locking claims
while retaining ordinary reads. Draining old claims before lease-trigger DDL
also avoids a parent-row/lease-table/FK lock cycle. This one-time work scales with
retained metadata. Ordinary claims never repeat that reconciliation. The
initialization transaction retains its 30-second statement and 10-second lock
limits; an expansion that exceeds them must be diagnosed and retried, not
silently treated as complete. Large deployed-history migration duration has
not been qualified by the small behavioral migration fixture.

The bridge image uses the previous claim only when the projection is absent
and the actual schema is 35 or 36. It rediscovers expansion on every claim and
fails explicitly if a newer schema is missing the projection. Compatible
signing ranges and the bridge-before-expansion procedure are owned by the
[private release guide](../operations/private-release-controller.md). Existing
signed manifests are never rewritten or presumed compatible with a new schema.

The projection is reconstructible from committed raw parents minus committed
v1 children. Rebuilding it requires the same guarded dataset-write transaction
as the migration; deleting it is not an operator shortcut. PostgreSQL backup
retains both the authoritative datasets and operational metadata. Ordinary
process crashes cannot commit only one half of a trigger-maintained transition.

## Controlled measurements

[Recorded fixture](fixtures/publication-frontier-20261025.json), 2026-10-25:
native PostgreSQL 17.11, a 1-CPU/2-GiB database container, a 4-CPU/16-GiB executor,
and concurrent isolated test activity. The fixture has **500,000 completed raw
parents and 500,000 committed v1 children**, with no pending parents. Publication
triggers are bypassed only while seeding the private metadata fixture; these
rows are not complete source captures, observations or granted source access.

| Measurement | Median | Empirical p95 |
| --- | ---: | ---: |
| Exact baseline selection SQL | 3,348.58 ms | 5,372.05 ms |
| New selection SQL | 1.08 ms | 3.08 ms |
| Actual new Python claim, including guards/schema check | 2.60 ms | 3.59 ms |
| Actual claim with stale statistics from a 5,000-row prior backlog | 1.62 ms | 3.37 ms |

Each row has five wall-time samples; p95 is the empirical maximum here, not a
statistical production latency bound. SQL `EXPLAIN ANALYZE` shows **zero visits
to retained parent and child rows** for both empty-queue cases. The reference
SQL runs against the same expanded schema and indexes as the treatment; this
isolates selection strategy rather than reproducing the entire old deployment.

The new committed-child lookup index occupies 62,398,464 bytes at this fixture
size. The pending projection occupies 1,802,240 bytes after inserting and deleting
the 5,000-row diagnostic backlog. These are fixture sizes, not universal storage
coefficients. Native tests also exercise real HTTP capture/S3 replay, retry
ordering, child retirement, legacy replay, migration backfill, concurrent parent
locks and expired publisher fencing.

Reproduce outside production:

```sh
TEST_DATABASE_URL=<isolated-native-test-url> PYTHONPATH=src \
  python scripts/benchmark-publication-frontier.py \
  --parents 1000 50000 500000 --repetitions 15 \
  --output output/publication-frontier.json
```

The script creates and drops one private schema; it requires native test-role
permission to bypass triggers during seeding and never selects a database
implicitly. It records full SQL plans, samples and index sizes in its JSON output.

## Remaining limits

Sorting and retry filtering still scale with actual pending/retry rows. This
change does not establish sustained catch-up throughput with a very large active
backlog. A prior peak backlog can also leave dead projection tuples until normal
PostgreSQL vacuum reclaims them. Source quotas, HTTP/S3 latency, normalization,
analytical CPU, whole-stack memory and target-VPS capacity remain separate
measurements. The isolated selection gain is not a promised pipeline speedup.

# Shared cache admission without repeated inventory walks

Scope: prelaunch massive-collection ticket #100 and
[approved specification](../specs/prelaunch-massive-collection.md). Baseline is
`ac5ddcb9e8713e311b6772528e48b0ee8ade9b33`. This is controlled local filesystem
evidence, not a VPS acceptance or full-pipeline speed claim.

## Behavior and recovery

The existing cache module retains ownership of its single shared byte budget,
kernel-locked reservations, inode-aware pins, checksums and eviction ordering.
A private accounting implementation maintains a **conservative shared charge**:
each admission charges its complete reservation before creating files. Promotion
from a temporary file to a payload does not add another charge. Deletion, pin
release and replacement do not immediately refund charges. When the estimate no
longer admits the requested bytes, the existing inventory reconstructs exact
inode/reservation usage and applies source-before-window eviction, including the
[bounded pressure follow-up](cache-pressure-headroom.md).
No disposable accounting value becomes authority for history or retention.

Every mutating cache transaction holds the same global flock as older images.
It clears the small accounting file before changing names and writes a checksummed
record only after covered changes. A hard process exit leaves either an invalid
record requiring inventory or a complete conservative record. A fresh process
always reconstructs once, so host restart does not trust an old disk record.
Directory identities and timestamps provide an additional consistency check.

Timestamps alone cannot establish compatibility when filesystem updates coalesce.
One Linux inotify descriptor and three directory watches per live History owner
also detect ordered namespace changes and in-place payload writes. A new writer's
accounting commit follows its mutations; an older writer never emits that commit.
Old pin/reservation/payload changes therefore invalidate the estimate and restore
the existing inventory path. Old/new overlap may retain the older scanning cost,
while preserving shared filenames and the same hard-budget protocol. A child
created by fork obtains its own watcher and first inventory. Garbage collection
closes the bounded descriptor; changing the cache root closes the old watcher.

Unavailable watches, watch loss, queue overflow, partial JSON, wrong JSON types,
or a changed counter checksum all force reconstruction. A reserve-covered
temporary file is named under the flock before streaming, so its later writes
need no inventory. Read-only inode verification uses an inspection lock without
rewriting accounting; it changes neither payload names nor sizes. Legal
retirement tombstones, original-file removal checks and local-only fallback
behavior remain unchanged.

## Measurements

The reproducible [raw results](cache-admission-accounting.json) vary ambient file
count while holding logical payload at 8 MiB and the cache budget at 128 MiB.
Each of 31 measured admissions writes a new 1 KiB local payload through the actual
cache, including its object stripe, a short-lived cache instance and file fsync.
Setup and the first reconciliation are excluded from ordinary-admission timing.
No ambient payload is evicted. The host has a four-CPU quota and 16 GiB memory
limit and was shared with other controlled work, so tail timings are not an
exclusive-host capacity measurement.

These initial accounting measurements precede the separately recorded pressure
follow-up; the raw module hashes identify that exact historical comparison.

| Ambient files | Baseline median | New median | Baseline p95 | New p95 | Inventories per 31 admissions |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 2.954 ms | 0.464 ms | 3.215 ms | 0.686 ms | 31 → 0 |
| 5,000 | 13.818 ms | 0.491 ms | 31.119 ms | 0.681 ms | 31 → 0 |
| 30,000 | 133.166 ms | 0.868 ms | 168.116 ms | 0.988 ms | 31 → 0 |

The objective of ordinary-admission p95 below 5 ms at 30,000 files is met in this
fixture. CPU median there changes from 133.171 ms to 0.870 ms. Initial
reconciliation still costs about 298 ms; capacity pressure and older writers can
still require a complete inventory. This does not promise a 153-fold improvement
for complete collection or analytics.

There is a measured tradeoff: an ordinary verified 1 KiB warm read at 30,000 files
changes from 0.151 ms to 0.503 ms median because pin name changes update shared
accounting. Bounded read-session reuse remains about 0.026 ms median in both
versions; its inspection path avoids those writes. Warm 128 KiB ordinary reads
change from 0.233 ms to 0.393 ms in this run. No source GET is introduced by a
warm hit. Timings and process-only cumulative peak RSS do not establish a memory
per product or continuous VPS load.

Run the same script against each immutable checkout:

```sh
git worktree add --detach /tmp/cache-admission-baseline ac5ddcb9e8713e311b6772528e48b0ee8ade9b33
PYTHONPATH=/tmp/cache-admission-baseline/src python scripts/measure-cache-admission.py --output output/cache-baseline.json
PYTHONPATH=src python scripts/measure-cache-admission.py --output output/cache-candidate.json
```

The script requires installed project dependencies and no database, cloud account
or source credentials. Each case cleans its temporary fixture. Cache modules'
SHA256 values identify the measured implementations independently of later
documentation commits.

Behavioral tests cover existing cache pressure, active/duplicate reader pins,
in-flight reservations, reduced limits, checksum failure, legacy window aliases,
retirement races and crash cleanup. Additional tests cover coalesced timestamp
versions, an older writer without accounting, pinned in-place growth, corrupt
records, lost/unavailable watches, overflow, descriptor lifetime, cross-process
charge sharing, process exit during mutation and forked watcher ownership.

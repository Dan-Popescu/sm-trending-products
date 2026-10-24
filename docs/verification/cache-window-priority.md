# Preserve reusable windows within the same cache budget

Issue [#77](https://github.com/Dan-Popescu/sm-trending-products/issues/77).
[Complete reports](cache-window-priority.json) preserve the traffic, selected-row
counts and direct current/obsolete-file inventories from the native comparison.

The cache budget is **1,000,000 bytes in both runs**. The fixture physically
imports six products, 20 distinct videos/product and 120 observations/video in
144 imports of 100 observations. A six-hour refresh adds six imports of 20
observations. The inclusive analytical lookback remains 30 days. Observations
contain one cumulative `view_count` and `creator_id`; these coefficients do not
cover price, sales, cost, transcript or media payloads.

| Refresh measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Source GETs | 150 | 6 |
| Download bytes | 1,869,492 | 56,554 |
| Full / delta windows | 6 / 0 | 0 / 6 |
| Current window files present | 3 / 6 | 6 / 6 |
| Declared current-window bytes | 648,374 | 648,371 |
| Obsolete local file bytes | 0 | 0 |
| Total payload inode bytes | 994,215 | 994,507 |
| Selected observations | 14,520 | 14,520 |

The candidate retains all six current windows after cold preparation, too:
643,573 bytes against 643,571 declared bytes in the baseline, where only three
files survive. Cold source GETs remain 144 in both cases. This isolates a cache
retention improvement rather than a reduction of requested source history.
Traffic falls by 25 times for refresh GETs in this fixture; it does not imply a
25-times reduction in complete pipeline runtime or a universal cloud bill.

The baseline application is immutable `3d022340071f707727f4429a697a1ba38cf18853`;
the candidate is `e85b7f3007db7f563e0bef044a8fc7cb3a1c6e71`. The same
`tests/test_cache_pressure.py` executes against each complete source checkout.
For the baseline, the test finishes its full-oracle check and writes the report
before failing the new six-current-window acceptance assertion. The candidate
passes. Separate timestamps and reviewed identities mean stored hashes are not
compared between runs: **each complete assessment, scalar, rank and expanded
reference list equals its own full six-product analytical oracle**. No oracle
expansion occurs between the measured cold and refresh phases.

This is a local native PostgreSQL/HTTP Moto test. Application and object-store
adapter share a process. Timings, provider latency, PostgreSQL CPU/RSS and total
production capacity are not inferred. The current/obsolete byte totals come
from descriptors and actual files, not subtraction of normalized source bytes;
provenance can also occupy the cache. Other work shared this machine.

The change prefers local windows before source files, retains LRU ordering within
each class, and reclaims an old local version only after its replacement stage
commits and its retirement tombstone is visible. Rejected and rolled-back stages
preserve the prior state. Cleanup failures leave the existing outbox retryable;
active pins still count toward the single budget. Legacy names remain readable,
and retirement removes both aliases. The [cache operations guide](../operations/cache.md)
documents the ownership and whole-group restart requirement.

Focused tests and independent reviews cover real processes, crashed reservations,
reduced-budget reconciliation, alias accounting, active readers, a real deferred
COMMIT failure, source invalidation and physical retention. Native PostgreSQL
contention after staging cannot turn optional cleanup into a failed assessment.
A legacy verified hit also finishes while a distinct cold fill sharing its stripe
is blocked. Independent six-product measurements reproduce the same GET reduction
and preserve whole-publication parity after cache eviction and full rebuilding.

Admission reuses one `scandir`/stat snapshot under the accounting lock. This removes
duplicate metadata reads and rescans during eviction, **while remaining O(number
of cached files)**. A corpus of many small files can therefore still make admission
expensive. No persisted cache registry, accounting database or size counter was
added. Descriptor locking and opportunistic cleanup also perform SQL metadata
work; this measurement makes no claim about their effect on complete pipeline CPU
or latency when a larger cache already retains every window.

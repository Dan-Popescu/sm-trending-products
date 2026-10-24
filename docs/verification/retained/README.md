# Retained-history evidence

These are native local PostgreSQL/HTTP Moto measurements for [#67](https://github.com/Dan-Popescu/sm-trending-products/issues/67), not a deployed-provider capacity certificate.

## 1,000 products: completed phases, final deadline stop

[Condensed report](retained-1000-30-partial.json) preserves every completed phase and request/byte counter from the original report. Its supervisor summary retains the sample count, peaks, final sample and original report hash. [Native source SQL inventory](retained-1000-30-source-sql.json) separately records per-table/index sizes and an actual `COUNT(*)` of the 2,400,000 observation deliveries.

The whole case is **not accepted**: its declared 2,400-second deadline stopped the final cache-evicted rebuild. The isolated schema and working directory were removed. The successful earlier phases remain scoped measurements; they do not establish that the whole suite passed.

Every observation has one cumulative `view_count` metric and a `creator_id` attribute. The fixture contains no price, sales, cost, transcript or media payload; its byte/CPU coefficients cover only that declared shape.

The workload physically imported 1,000 products × 20 distinct videos × 120 observations, spanning 29 days 18 hours at four observations/day. It then ingested two further waves of 20 observations/product. The analytical window is an inclusive 30 days, so the second wave retained 121 observations/video. Each completed assessment returned exactly 1,000 rows and the expected observation count. Sampled complete scalar/rank/reference oracle checks passed for this homogeneous fixture.

| Completed phase | Assessment wall seconds | Process CPU seconds | GET | PUT | Download bytes | Upload bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold | 615.18 | 438.42 | 1,000 | 40,256 | 101,416,778 | 178,839,426 |
| Warm, same cutoff | 2.42 | 1.58 | 0 | 1 | 0 | 0 |
| Refresh 1 | 311.17 | 277.63 | 1,000 | 3,247 | 9,348,466 | 13,009,035 |
| Refresh 2 | 389.65 | 353.19 | 1,000 | 4,249 | 9,348,508 | 9,788,580 |

Seed ingestion took 763.22 seconds. Refresh assessment timings exclude refresh ingestion, which this launch version did not time separately. The warm PUT attempted an existing publication and uploaded no new payload bytes. Each refresh used 1,000 delta windows with no full or duplicate rebuild windows. This is reuse of a local retained window; it is not a claim that all historical CPU work disappears.

The initial source inventory was 114,120,772 raw bytes, 101,416,778 normalized bytes, 1,898,800 canonical manifest bytes and 1,912,676,352 SQL relation/index bytes. `observation_deliveries` alone occupied 467,189,760 table bytes plus 1,407,918,080 index bytes. These physical coefficients describe a compacted fixture with 2,400 observations/import. Applying them to 20-observation source pages would ignore file/manifest overhead.

The local environment had an Intel Xeon Platinum 8573C, cgroup CPU quota equivalent to four cores, 16 GiB cgroup memory, Python 3.12.14 and PostgreSQL 17.11. The application and HTTP Moto shared one child. Peak discovered RSS was 1,003,253,760 bytes, maximum observed descriptors 2,017, and minimum free disk 12,163,133,440 bytes. Descendant discovery was incomplete; PostgreSQL CPU/RSS and Dagster/dashboard resources were outside that process measurement. The unchanged soft/hard descriptor limit was 16,384. Moto kept keys up to 32 KiB in RAM and spooled larger keys to owned files; this adapter choice changes its allocation, not the requested data or object layout.

Application code was fixed at `aadb7dd4070f2273a134d12a7931781955f5a687`. The launch harness was uncommitted, then recovered byte-for-byte: [source archive](retained-1000-30-runner.py.txt) hashes to the recorded `7c571bd22a0f8fc91981563266f6857fa84ffb5e43c7e7e61db389d97607d540`. Reproduction requires restoring that source to `scripts/retained-history.py` in that checkout because helper paths are repository-relative. This is precise source evidence, not a claim of a clean whole-tree launch revision.

For reproduction after a fresh clone, public commit [`765fcef`](https://github.com/Dan-Popescu/sm-trending-products/commit/765fcefafdb93f8d635bc6effd61bb08fb031d68) has exactly the same application `src` tree as local launch `aadb7dd`: `ccd8cd3f0858f20a44dce2e79bf677062a18a227`. Restore the archived harness on that public revision; the dirty harness overlay remains explicitly separate from the application source.

The cache-pressure cases below used local `045dc65`, whose complete tree `711e50778b027c7e47eefde3439c35cd3f9f78f3` is identical to public commit [`0c1fbe8`](https://github.com/Dan-Popescu/sm-trending-products/commit/0c1fbe842cd333a061ea1dde809ee36f17f244d8), including application `src` tree `bc5ab3a3b4fb2d4d7813447d5d81b39043a0b401`. These equivalences were checked from Git trees, not inferred from similar filenames.

WAL positions are server-global with other schemas active. No attributable WAL, dump size or PostgreSQL CPU/RSS measurement is inferred. HTTP Moto timings include local serialization and supervision overhead; they do not establish cloud request latency. A prior attempt with Moto's one-byte threshold failed at its descriptor limit and is not a pipeline capacity result.

## Clock advancement and unchanged-source cost

A separate [native three-product probe](clock-boundary-probe.json), exercised by `tests/test_retained_clock_boundaries.py`, distinguishes same-cutoff reuse from the actual 15-minute schedule. It uses two videos/product and 120 samples/video; it does not supply large-workload throughput coefficients.

At the initial cutoff all three products rebuild. Repeating that exact cutoff hits all three projections and uploads no new assessment bytes. Advancing by 15 minutes hits a conservative freshness boundary: all three products take a local delta path with zero object GETs. At 30 minutes all three projections hit. After ingesting a new six-hour wave, the next assessment selects 121 samples/video; 15 minutes later the oldest 30-day-boundary sample has expired, so all three products again take a local delta path, now with 120 samples/video and zero GETs. The following 30-minute assessment hits all projections.

Every advancing cutoff in this probe publishes a changed assessment body, including the stable projection-hit steps. The three-product stable step uploaded approximately 10.6 kB in one PUT. Thus a daily estimate cannot treat all between-wave evaluations as identical-cutoff, zero-byte publications, or assume no expiry work. Use measured moving-cutoff payload sizes, declare alignment/freshness assumptions, and separately model retention of these published snapshots. This is correct inclusive-window behavior; no production semantics were changed or speculative speedup claimed.

## Cache pressure and fragmented files

[Five complete small cases](cache-pressure.json) hold the actual logical workload fixed at six products, 20 videos/product and 120 samples/video, followed by one six-hour refresh. All returned the exact requested product/observation counts, matched sampled complete-row/reference oracles, respected their cache budget and removed owned state. These demonstrate safe fallback and its object-read cost; they are not a large-workload capacity measurement.

| Source layout | Cache bytes | Refresh full / delta windows | Refresh GET | Download bytes |
| --- | ---: | ---: | ---: | ---: |
| 2,400 observations/import | 1,000,000 | 6 / 0 | 11 | 562,780 |
| 2,400 observations/import | 2,000,000 | 1 / 5 | 6 | 55,888 |
| 2,400 observations/import | 3,000,000 | 0 / 6 | 6 | 55,887 |
| 100 observations/import | 1,000,000 | 6 / 0 | 150 | 1,852,392 |
| 100 observations/import | 8,000,000 | 0 / 6 | 6 | 55,890 |

The fragmented cases physically committed 144 historical imports, then six new imports of 20 observations each. The constrained case read all 150 normalized objects during refresh. With eight MB, the six valid local windows admitted a delta path and only the six new source objects were downloaded. The measured counts differ by 25×; no such factor is inferred for thousands of products or a cloud provider.

In these baseline cases, source objects and local feature windows share an undifferentiated bounded LRU cache. A missing or evicted window safely triggers a full rebuild, which explains the increased reads. The exact current-window total after a delta was not separately measured in these five reports, so they alone do not prove an improvement at the same budget. The subsequent [same-budget window-priority comparison](../cache-window-priority.md) measures the actual current and obsolete files, preserves reservations/pins/retirement, and verifies 150 → 6 GET at the same one-MB limit on its separately identified source revision.

All five cases used clean application revision `045dc658f83b3860f6cbc806e2845125a3dad8de`, analytical batch size one, and the exact archived harness hash `bef1fc190a07d27107cd7ff7d4d22262e84145273e5d18ca4427fea59a1d6a0c`. The JSON preserves the per-case original report hashes, requests, phase counters, import timings, versions and resource summaries. Separate timestamps can cause small compressed-byte differences between otherwise equal fixture shapes.

## 3,000 products: physical retained history and an RSS stop

[Condensed report](retained-3000-30-partial.json) preserves the original request for 3,000 products × 20 videos × 120 historical observations/video, followed by four refresh waves and a final cache-evicted rebuild. It is **not accepted as a complete suite**: the original two-GiB RSS guard stopped the common application/HTTP Moto process during the second refresh assessment. The guard was not raised retrospectively and no smaller workload was substituted.

The [native seed inventory](retained-3000-30-source-sql.json) independently counted **7,200,000 actual observation-delivery rows** after 3,000 normal imports and identity review of all 60,000 videos. Seed ingestion took 2,241.00 seconds. Raw objects occupied 342,344,420 bytes, normalized objects 304,307,895 bytes, canonical manifests 5,740,800 bytes and native SQL relations/indexes 5,829,795,840 bytes. The observation-delivery table/indexes alone occupied 5,719,588,864 bytes. Each observation contains the same explicitly scoped `view_count`/`creator_id` shape described above.

| Completed assessment phase | Wall seconds | Process CPU seconds | Work | GET | Download bytes | PUT | Upload bytes |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| Cold | 2,217.26 | 1,613.56 | 3,000 full windows | 3,000 | 304,307,895 | 120,366 | 536,415,184 |
| Warm, same cutoff | 9.04 | 5.85 | 3,000 projection hits | 0 | 0 | 1 | 0 |
| Refresh 1 | 1,528.00 | 1,453.38 | 3,000 delta windows | 3,000 | 28,091,865 | 9,402 | 38,940,757 |

Cold selected exactly 7,200,000 observations and returned all 3,000 requested products; warm publication was byte-for-byte equal. Refresh 1 selected 7,260,000 observations and passed the sampled complete-row/reference oracle. Its separate 60,000-observation ingestion took 111.09 seconds wall /75.34 seconds CPU, with 9,000 PUTs and 37,933,464 uploaded bytes. Thus measured ingestion plus assessment was 27.32 minutes, excluding the subsequent oracle; same-cutoff reuse is not the moving-clock schedule cost.

Refresh 2 also ingested all 60,000 observations: 128.86 seconds wall /84.38 seconds CPU, 9,000 PUTs and 37,932,856 uploaded bytes. A [second native source snapshot](retained-3000-30-two-ingest-source-sql.json) counted **7,320,000 physical rows** while those sources were stable and the second assessment was active. The assessment stopped before its final result/oracle, so no second-phase timing, score parity or completed second-wave claim is inferred. The third/fourth waves and final eviction did not execute.

The supervisor stopped after 8,289.20 seconds. Its sampled peak discovered RSS was 2,148,474,880 bytes (2,048.95 MiB), for the **common application plus HTTP Moto process**, against the original 2,147,483,648-byte guard. This does not measure application-only memory or establish a VPS memory requirement. The maximum observed descriptor count was 6,016 against the unchanged 16,384 limit; minimum free disk was 6,494,019,584 bytes. The stop was not a descriptor, disk or deadline failure. All named owned-file totals retain their rolling-observation scope.

The child exited under supervision, and native rechecks found no remaining `retained_probe_%` schema, worker process or owned configuration directory. Both cleanup flags are true. Free disk returned to 14,742,061,056 bytes. PostgreSQL CPU/RSS and attributable WAL remain unmeasured; the server-global WAL positions are not treated as this case's write volume.

This baseline used clean local launch `045dc65`, exactly reproducible on public [`0c1fbe8`](https://github.com/Dan-Popescu/sm-trending-products/commit/0c1fbe842cd333a061ea1dde809ee36f17f244d8), complete Git tree `711e50778b027c7e47eefde3439c35cd3f9f78f3`, application tree `bc5ab3a3b4fb2d4d7813447d5d81b39043a0b401`, and archived harness SHA256 `bef1fc190a07d27107cd7ff7d4d22262e84145273e5d18ca4427fea59a1d6a0c`. It precedes the later window-priority cache policy and explicit per-phase publication-hash check. The partial case neither certifies four daily waves nor transfers its timings to that newer code.

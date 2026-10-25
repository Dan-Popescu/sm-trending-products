# Always-on runtime footprint before the first pilot

On 2026-10-25, a bounded native probe ran all six persistent application roles:
capture, capture planner, normalizer, Dagster webserver, Dagster daemon and
Streamlit. Their descendant processes were included. A dedicated PostgreSQL
17.11 container isolated database resource accounting from other test sessions.
The local HTTP S3 emulator was measured separately and excluded from the runtime
CPU and application-memory figures below.

The [condensed measurements](runtime-footprint-20261025.json) contain phase and
per-role statistics, actual counts, source and collector hashes, readiness
checks, resource bounds and measurement limits. The application source was
local commit `d3ed19ad792d739d77670587d755bfefb6f92708`: main
`ac5ddcb9e8713e311b6772528e48b0ee8ade9b33` plus the narrow Dagster repository
loading correction tracked in [#102](https://github.com/Dan-Popescu/sm-trending-products/issues/102).
These measurements precede the other three prelaunch corrections and do not
establish their performance effects.

## Integrated corrections: fresh measurement

A second run used all three corrections, including the bounded source-cache
pressure policy, at local source commit
`8ed5fe5707750942cd49a16d38d18a133f283033`. Its
[condensed evidence and production-source hashes](runtime-footprint-integrated-20261025.json)
separate the measured implementation from subsequent documentation commits.
The same collector and harness hashes, isolated database limits and workload
parameters apply. Full native tests ran concurrently on the enclosing host.

The integrated run again completed **31 source requests, 31 raw captures,
31 normalized datasets and 620 observations**, including one naturally planned
refresh beyond the 30 injected tasks. No task remained unfinished or quarantined.
Complete repository loading and running GraphQL repository discovery passed.
Cleanup removed the probe's schema/database and processes; its separate
PostgreSQL container was then removed.

| Phase | Interval | Mean share of 4 CPU cores, PostgreSQL included | Synchronized memory estimate, p95 |
| --- | ---: | ---: | ---: |
| Idle roles | 59.73 s | 2.12% | 1.04 GiB |
| Capture and normalization | 60.01 s | 5.62% | 1.09 GiB |
| Drain overlapping scheduled empty work | 7.06 s | 14.07% | 1.37 GiB |

The 09:30 UTC schedule boundary occurred near the end of the active interval.
Scheduled work continued into the short drain, where an empty-live-namespace
`opportunity_rankings` run completed successfully. Its CPU/memory cost spans both
phases, unlike the phase placement in the earlier run. The two active percentages
are not an optimization comparison: scheduled
events, concurrent host work and phase boundaries differ. The seven-second
average is not a peak or a sustained massive-history forecast.

Memory totals use synchronized samples of application shared-page-adjusted
resident memory plus PostgreSQL cgroup memory, including its inactive file cache.
They exclude the operating system, other host cache, an active dashboard browser,
large historical analytics and media/audio processing. These figures establish
a small-workload baseline only. Keep the pilot's 4-CPU/16-GiB sizing provisional
until retained-history collection, analysis, maintenance and backup have been
measured together under the actual deployment limits.

## What was measured

After 30 seconds of warmup, the probe measured a 60-second idle interval and
offered one synthetic capture every two seconds for 60 seconds. Each response
reobserved the same 20 videos. The normal capture planner remained enabled and
added one naturally due refresh. All **31 requests produced 31 committed raw
captures, 31 normalized datasets and 620 observations**. No task remained pending,
leased or quarantined. Normalization finished within the additional 7-second
measured drain interval.

The active interval crossed a normal Dagster schedule boundary. Scheduled jobs
used the empty live namespace; the synthetic observations stayed separate. At
least one `opportunity_rankings` run completed successfully. This interval
therefore includes scheduled-process startup and empty analytical work, as well
as synthetic acquisition and normalization. It does not measure an analysis of
620 observations, and does not certify that every periodic job drained before
the probe stopped its own processes.

| Phase | Measured interval | Mean occupied CPU cores, including PostgreSQL | Equivalent share of 4 CPU cores | Application PSS, p95 | PostgreSQL cgroup memory, p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle roles | 59.70 s | 0.0767 | 1.92% | 940.9 MiB | 140.1 MiB |
| Capture, normalization and scheduled empty jobs | 59.98 s | 0.4625 | 11.56% | 1,135.0 MiB | 149.8 MiB |
| Remaining normalization drain | 7.03 s | 0.0775 | 1.94% | 998.7 MiB | 148.9 MiB |

Application PSS apportions resident shared pages between processes rather than
counting each page repeatedly as a sum of RSS would. PostgreSQL process PSS was
unavailable across container user IDs, so the database uses Docker's cgroup
accounting instead. Its p95 working set after subtracting inactive file cache
was 80.3 MiB idle and 88.8 MiB active. The last table column also includes that
inactive file cache. Summing application PSS and PostgreSQL cgroup memory at
each sampled time, then taking that series' p95, gives approximately
**1.06 GiB idle and 1.25 GiB active** for this short fixture. This combines two
accounting methods rather than measuring whole-host RAM. Host/kernel memory and
other host filesystem cache are excluded.

Full repository loading and a live GraphQL repository query both passed. An
HTTP 200 health endpoint alone was insufficient: the initial investigation found
the Dagster name conflict in #102 while its HTTP processes were still running.
The failed-repository preliminary run is excluded from these accepted figures.

## What this means for VPS planning

These measurements provide a concrete persistent-process baseline and one small
active interval. They cannot predict a deployment holding millions of historical
observations. They include neither a browser using the dashboard, a large
analytical run, compaction or backup over retained history, production network
latency, media extraction nor audio processing. CPU averages over one minute
also hide shorter bursts. A continuously arriving real workload may have
different payload sizes, source quotas, account counts and retry costs.

All native application processes shared one CPU affinity. PostgreSQL was capped
at 0.25 CPU and 512 MiB. The enclosing host had a four-CPU quota and 16 GiB memory
limit, with other agents running concurrently. The actual collector was a debug
build of the current Rust source. This is a controlled local measurement,
not a VPS speed comparison or a production maximum.

The existing [four-CPU pilot profile](../../deploy/production-4cpu.env.example)
sets **3 CPU cores and 7.25 GiB** as the sum of steady role ceilings. One
overlapping capture replacement raises those sums to **3.75 CPU cores and
8 GiB**. These settings are limits, not measured continuous consumption. The
profile gives PostgreSQL more resources than this test. It still requires
retained-history acceptance with simultaneous collection, normalization,
analysis, maintenance, backup and a replacement worker on the chosen VPS.

The historical retained fixtures also illustrate why daily averages and waiting
time are different measurements. The four dirty analytical updates consumed
1,366.58 process CPU seconds for 1,000 products and 6,871.57 seconds for 3,000
products. Dividing by 86,400 seconds and four CPU cores yields **0.40% and 1.99%**
of a four-CPU day for those analytical updates alone. Individual updates still
took 5–7 minutes and 24–38 minutes respectively. That older accounting includes
the analytical application and Moto but excludes PostgreSQL and the persistent
roles measured here. Do not add these percentages to claim a deployed full-stack
forecast. The 3,000-product suite completed its four updates but hit its overall
deadline during the final cache-eviction rebuild; it is not a complete capacity
acceptance.

## Reproduce the bounded probe

Use a separate local PostgreSQL container, the locked Python dependencies and a
collector built from the source being measured. The fixture credentials below
are disposable test values. The probe creates unique application schema and
Dagster database names, then removes only those names during cleanup. Never
point it at deployed data.

```sh
docker run -d --name sm-trends-runtime-envelope-postgres \
  -e POSTGRES_USER=runtime_test \
  -e POSTGRES_PASSWORD=runtime-test-only \
  -e POSTGRES_DB=runtime_test \
  -p 127.0.0.1:32769:5432 --memory=512m --cpus=0.25 \
  postgres:17.11-bookworm

PYTHONPATH=src python scripts/runtime-footprint.py \
  --database-url postgresql://runtime_test:runtime-test-only@127.0.0.1:32769/runtime_test \
  --postgres-container sm-trends-runtime-envelope-postgres \
  --collector-binary collector/target/debug/sm-collector \
  --output output/runtime-footprint.json \
  --warmup-seconds 30 --idle-seconds 60 --active-seconds 60 --drain-seconds 30

docker rm -f sm-trends-runtime-envelope-postgres
```

Wait for `pg_isready` in that container before running the Python command. The
probe is Linux-specific and reads its own descendants under `/proc` plus the
local Docker socket for database statistics. It stops if measured application
PSS exceeds its declared bound. An hourly/quarter-hour boundary can change which
ordinary jobs run during the window; compare declared events and actual task
counts before comparing CPU results. Retain the full output JSON when running
the probe against a later revision.

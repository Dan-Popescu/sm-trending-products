# Concurrent synthetic capture and research qualification (#194)

The runner exercises the existing Rust `capture` receipt boundary and
`publish_captures` normalization against an owned loopback HTTP fixture, while
`PublicDecisionChanges` and `ContentProgression` read retained synthetic history.
It never enables acquisition scheduling, real source capabilities, paid providers,
or a production execution path.

Run with an existing collector executable and an explicit loopback native test
PostgreSQL database whose name contains `test`. Docker/builds are not invoked.

```bash
PYTHONPATH=src python scripts/concurrent-research-workload.py \
  --collector collector/target/debug/sm-collector \
  --output /tmp/concurrent-small.json

PYTHONPATH=src python scripts/concurrent-research-workload.py \
  --collector collector/target/debug/sm-collector \
  --profile retained --allow-larger-profile --timeout 180 \
  --max-disk-mib 256 --output /tmp/concurrent-retained.json
```

The small profile declares one product, eight videos and eight supplied texts,
68 observations over seven logical retained days, twelve two-video arrivals,
20 ms arrival spacing and 80 ms source latency. The opt-in retained profile uses
four products, 32 videos/texts, 3,888 observations over 30 logical days, 48 two-video
arrivals at 100 ms spacing, and the same 80 ms source latency. Reports use complete
product scope, a measured 24-hour change panel and observed content association;
an unavailable report is a refusal. At most 16/32 report iterations run, respectively.
These finite campaigns are not long-duration endurance tests or capacity claims.

Each comparison runs a collection-only control followed by the concurrent case,
with the same inputs/bounds but fresh PostgreSQL schemas, HTTP Moto stores and
worker processes. The control runs reference and postdrain reports outside the
collection interval. Their collection-time ratio is descriptive; OS/database
caches and shared-host load are not controlled statistically.

Both cases first build declared historical fixtures in an empty owned synthetic
schema. The historical cutoff is fixed at `2026-10-30T00:00:00Z`, after the reviewed
content taxonomy and before actual fixture collection. Synthetic product/link,
imported dataset and supplied-text availability metadata explicitly describe the
retained fixture chronology. The existing workload's default 2099 cutoff and
outputs are unchanged. Actual Rust capture, observation and arrival availability
clocks are physical; they must all follow the historical cutoff. No real evidence
is backdated. Historical complete report bytes/IDs are compared before, during
and after arrivals. A postdrain admitted current read must contain every expected
arrival observation exactly once, while the same entity scope at the fixed cutoff
must contain none. Full object verification must also succeed.

Useful overlap requires the entire first successful `publish_captures` call to
fit inside the actual successful `reports()` call, excluding Pipeline construction,
serialization and checkpoint locking. Immediate start/end timestamps bound the
publication commit within real report work. A before-call authoritative child
snapshot, returned normalized dataset IDs and after-call exact child-to-raw-parent
-to-task correlation establish first publication; reused children cannot qualify.
Raw task completion or a delayed observation of an earlier commit does not qualify.
The report records the child dataset/row count, call intervals, admitted arrivals,
queue size/oldest age and source HTTP bytes/timestamps. Its later observed task
completion and arrival-to-durable latency are upper bounds, not exact commit clocks.
Source capture and normalization are both exercised. Collection replay and
concurrent reports share a cache directory and budget. The native cache-pressure
test archives incompressible bounded fixture padding and verifies actual raw cache
eviction while preserving reports/receipts.

The existing supervisor owns a random schema, temporary directory, Moto server
and worker process session. It refuses nonlocal/operator/live state, requires
empty scratch before initialization, caps each case at 180 seconds and 1–512 MiB
of owned disk/cumulative encoded uploads, and stops below 256 MiB memory or 512 MiB
filesystem headroom. Sampling is 250 ms; database/disk sampling is approximately
one second. Limits are admission/stop rules, not hard OS memory/CPU quotas.
Cleanup terminates only owned children, stops Moto, drops only its schema and
removes only its directory. Errors and interrupted accounting remain explicit;
checkpointed usage is a lower bound and unknown in-flight usage stays null.

Process/thread/supervisor scopes remain separate: report thread CPU and SQL calls
exclude collector/background/pool threads; worker lifetime CPU includes its Python
threads; reaped child CPU includes Rust. Per-PID RSS/CPU and discovered-session RSS
are samples and may miss short-lived children. Supervisor CPU includes Moto.
PostgreSQL CPU/RSS, Moto RSS, whole-host peak memory, WAL/temp/log disk and provider
invoices are unknown. Object counters are logical client traffic. The first
concurrent application cache starts absent, but reference reports have already
warmed OS/PostgreSQL/Moto; subsequent iterations reuse the shared application cache.

## Verification

Behavioral native tests cover actual Rust capture/report overlap and complete
accounting, matched controls, exact normalized-child attribution, shared-cache
pressure/eviction, live/operator refusal, forced-deadline cleanup, and actual SIGTERM after a normalized-arrival checkpoint
with known traffic preserved and unknown in-flight usage. Existing report-runner regressions retain default output and guard
coverage. Finite measurement evidence is recorded separately after committing the
implementation revision.

## Finite retained campaign, 2026-10-30

[Complete machine evidence](concurrent-research-retained.json) was produced from
clean immutable revision `8811cf70de336659532fc97e3718e20ce55ae2e6`, using the retained
command above, prebuilt Rust debug collector and native PostgreSQL 17.11. The two
cases completed in separate owned schemas, without cleanup errors. This is one
sequential matched comparison on a shared execution host, with both cases started
after available memory headroom recovered.

| Observation | Collection-only control | Concurrent research |
| --- | ---: | ---: |
| Retained observations / products / logical days | 3,888 / 4 / 30 | 3,888 / 4 / 30 |
| Admitted and durably normalized arrivals | 48 | 48 |
| Current verified arrival rows / missing / duplicate | 96 / 0 / 0 | 96 / 0 / 0 |
| Collection interval | 13.17 s | 16.71 s |
| Peak outstanding queue | 31 | 35 |
| Maximum sampled oldest outstanding age | 8.20 s | 11.63 s |
| Maximum observed arrival-to-normalized completion upper bound | 8.46 s | 12.00 s |
| Python worker lifetime CPU | 7.21 s | 17.02 s |
| Reaped Rust child CPU | 3.02 s | 3.44 s |
| Supervisor including Moto CPU | 2.35 s | 2.90 s |
| Sampled discovered process RSS peak | 208.34 MB | 211.18 MB |
| Sampled whole owned session RSS peak | unknown | unknown |
| Final owned files / schema relations | 1.29 / 5.55 MB | 1.77 / 5.47 MB |
| Logical History GET requests | 420 | 456 |
| Logical History download bytes | 1,391,628 | 1,473,314 |

All 48 exact normalized children completed. Of these, 46 entire first-publication
call intervals fit inside 12 actual public-report calls; two calls crossing report
boundaries/gaps are conservatively excluded. Each report call produced eight
complete reports (two modules across four products). The first concurrent phase
took 1.74 seconds; later phases took 1.32–1.53 seconds. The separate postdrain
cold-cache read took 1.41 seconds. Every historical result retained exact complete
bytes and public identities. Current scoped arrival reads contained all 96
observations with observation/availability clocks after the historical cutoff;
historical reads of those same arrival keys were empty. Both full object
verifications passed.

Collection elapsed time was 26.9% higher with research in this one run. The offered
arrival rate exceeded drain capacity even in the control, so queueing occurred
in both cases. This supports using bounded research frequency and measuring queue
age before choosing infrastructure; it does not establish a provider capacity,
production performance target or economic optimum. Report computation added
Python CPU while shared-cache reads limited extra logical GETs to 36 in this case.

The lowest measured available memory headroom was 1,444.43 MB during the concurrent
case (1,473.42 MB in the control). `/proc` descendant discovery was incomplete, so
full-session RSS stayed null; discovered RSS is not promoted to a process-tree or
host maximum. PostgreSQL/Moto RSS, PostgreSQL CPU, WAL/temp/log storage and
production disk capacity remain unknown. MB values here are decimal; runner limits
use MiB.

An [earlier attempt with the same code](concurrent-research-resource-refusal.json)
completed the control but correctly stopped treatment during setup when shared
available memory fell to 217.91 MB, below the unchanged 256 MiB headroom rule. Both
owned states were cleaned; completed control traffic remained recorded and
in-flight treatment totals remained null. The final evidence above reran both
cases fresh after headroom recovered, rather than combining attempts.

Fresh native regressions also verify that SIGTERM after a durable normalized
arrival preserves the last known setup/phase/traffic checkpoint before owned-state
removal. Supervisor interruption remains the terminal failure even if a child
checkpoint reports success; incomplete totals remain null. Report/setup refusals
and stopped collector threads also retain known traffic as a checkpointed lower
bound. All 34 focused native and existing workload checks passed on this revision.

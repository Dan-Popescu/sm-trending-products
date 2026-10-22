# Controlled native operating-envelope measurement

Executed on 2026-10-22: **all seven cases passed in 262.41 seconds**.
The [complete summary](operating-envelope/summary.json), per-case JSON below and
[artifact checksums](operating-envelope/checksums.json) preserve the measured evidence.
This is a finite synthetic native PostgreSQL/HTTP Moto integration envelope.

## Reproducibility and boundary

The measured clean local revision is `0bc331259e2b8a06e5f2875700ef18c18d12ec6b`,
tree `5d0e3f2729baf6096b68becc7861f4ebaac96b42`. The same exact tree is published
as [2e984789](https://github.com/Dan-Popescu/sm-trending-products/commit/2e98478993ddf6b005cedc9d6ffe6ecbfe683d2d).
The final delivery also strengthens progress-summary publication; the measured
case workloads, worker/collector code and phase instrumentation remain unchanged.

The Rust release binary SHA-256 is
`2638a6d46106c25ed43a2d645c199f7aa30ea2d8ac6e589dc63071ee9e9be7ed`.
It was built from main `c4811adc42554824eafe4b1dca27063b089043e1`; this measurement
slice changes no Rust source. Reports record each effective configuration and its
source tree. Reproduction uses an isolated database role allowed to create schemas:

```sh
cargo build --release --locked --manifest-path collector/Cargo.toml
TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test \
  uv run --locked python scripts/offline-envelope.py \
  --collector-binary collector/target/release/sm-collector \
  --collector-profile release --start-memory-headroom-gib 3 \
  --output-dir output/envelope
```

Hardware: INTEL(R) XEON(R) PLATINUM 8573C, 5 affinity CPUs,
**four CPUs of cgroup quota**, 16 GiB cgroup memory, overlayfs storage.
This is shared infrastructure. Database: PostgreSQL 17.11 (Debian 17.11-1.pgdg12+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14+deb12u1) 12.2.0, 64-bit.
The versions, free space and raw cgroup counters are recorded for every case.
History uses 64 buckets, 1,000 target rows/file, a 2,000,000,000-byte application
cache, two Polars/Rayon threads and a 512 MB DuckDB limit.

Source records in the six regular cases are 184–193 uncompressed JSON bytes.
The baseline is uniform. The parallel burst assigns 90% of 100 video identities
to one product, uses four workers and adds 50 ms per controlled HTTP request.
Paced cases offer 2 tasks/second for 60 tasks, or 8/second for 240 tasks; each task
contains 20 observations. Quota recovery offers 4/second for 80 tasks with a
one-request capacity/refill per second and a 100-second drain allowance.
The interruption probe has 20 tasks of three rows; its replacement and crashed
worker use an explicitly experimental 40-second lease and five-second collector
timeout. Ordinary cases use the 180/90-second deployment defaults.

## Results

SQL task latency is creation to committed completion. Collection throughput
includes enqueueing and deliberate pacing. Workflow time includes analysis and
verification; neither number measures live upstream freshness.

| Case | Observations | Workers | Collection rows/s | SQL task p95 (s) | Sampled peak backlog | Workflow (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [baseline](operating-envelope/baseline.json) | 1000 | 1 | 387.1 | 2.404 | 50 | 5.36 |
| [parallel_skew](operating-envelope/parallel_skew.json) | 4000 | 4 | 534.0 | 6.882 | 200 | 15.75 |
| [paced_one](operating-envelope/paced_one.json) | 1200 | 1 | 40.6 | 0.106 | 1 | 31.92 |
| [paced_four](operating-envelope/paced_four.json) | 1200 | 4 | 40.5 | 0.095 | 1 | 32.08 |
| [paced_higher](operating-envelope/paced_higher.json) | 4800 | 4 | 160.2 | 0.084 | 1 | 39.12 |
| [quota_recovery](operating-envelope/quota_recovery.json) | 1600 | 4 | 19.5 | 66.746 | 60 | 85.39 |
| [interruption](operating-envelope/interruption.json) | 60 | 1 | not measured | not measured | not sampled | 41.48 |

Every regular queue drained to its requested task count. Quota recovery reached
60 unfinished tasks, then completed all 80; its p95 latency reflects that deliberate
budget constraint. Recovered quota-denial events remain in the error/event list;
success means all requested work and publication checks completed, rather than
an absence of transient denials. Interruption recovered in 40.48 seconds after SIGKILL,
with 19 other tasks progressing before lease expiry, a new lease fence, exactly
60 unique observations and no staged dataset left behind. It establishes one
controlled fault boundary, not a production recovery objective.

The application worker high-water RSS was 210–253 MiB across the regular cases.
That includes Python/native imports and the controlled source. Supervisor RSS
is a separate process-lifetime high-water mark, including Moto and earlier cases.
Child RSS accounting can include fork-inherited pages; it is not isolated Rust
steady-state memory or a concurrent aggregate. Full session RSS is **unknown**
because this environment omits descendant-discovery links in `/proc`.
PostgreSQL/Dagster/dashboard resources are outside these process measurements.
All parent CPU, child CPU, supervisor CPU and sampled discovered-process lower
bounds remain separately available in the JSON.

The lowest sampled cgroup headroom was 3.67 GiB. Maximum sampled
owned files plus schema relations were 28.29 MiB; this excludes WAL,
database temporary files and remote object service memory. No execution guard
fired. The initial default 4 GiB admission profile
[refused to start](operating-envelope/initial-admission-refusal.json). The declared
3 GiB profile followed a successful smaller native regression measurement; the
2 GiB execution stop floor and all task/disk/deadline limits stayed enabled.
Each case has an independent schema/bucket/cache/spool and worker process. Schema
and local files were removed, temporary endpoints stopped, and cleanup errors are
empty for all seven cases. No live namespace or source credentials were used.

## Cache and compaction

Each cold/warm pair starts with its own empty application file cache; OS and
provider caches are outside that claim. All warm phases downloaded zero bytes
and recorded verified hits. For the baseline, cold normalized GETs fell from
50 to 1 after compaction,
and bytes from 540,221 to 61,069;
cold assessment elapsed 0.522 to 0.206 seconds.
In the 4,000-row case, active normalized objects fell from
200 to 4.
Assessment rows/evidence stayed identical. These are within-case observations,
not a comparison with an earlier software implementation.

Original objects remain retained for lineage. The baseline retains 51 normalized
objects totaling 601,290 bytes even though its active view reads one 61,069-byte
object. Descriptor summaries exclude manifests/derived objects. No fixture
retention policy is configured; these transient cases do not measure long-lived
history, deletion throughput or provider storage billing.

## Operating recommendation and cost assumptions

For this measured workload, start with one worker at two tasks/second. Both one
and four workers drained that offered rate with one sampled unfinished task;
four workers used more parent CPU (5.39 versus 3.37 seconds) without establishing
a useful latency advantage. Four workers also drained the finite eight-task/second
case; that is the largest paced offer tested, not a sustained production ceiling.
Respect the actual source budget before adding workers. Compact small active
files when measured GET overhead justifies its extra write and retained-storage
cost. Repeat these cases with representative retained history on the target VPS.

The [unpriced scenario](operating-envelope/cost-scenario.json) models 30 independent
baseline jobs and an always-on instance for 720 hours. It multiplies measured
request/byte units only; accumulated analytical history would be a different
workload. Monthly request units are 7,710 GETs,
60 HEADs and 4,710 PUTs. Monthly downloaded
payload is 79,863,840 bytes, with 43,786,440 uploaded bytes.
Apply operator-supplied dated unit prices separately:

`720 * instance_hour_price + retained_GB_month * storage_price + (GET + HEAD)/1000 * read_price + PUT/1000 * write_price + billed_egress_GB * egress_price`.

Actual provider prices, billable byte rounding/traffic, retained manifests,
database backups/WAL and full-stack instance requirements remain unknown.
No recurring currency amount is asserted.

Scale only after persistent queue growth under available quota, repeated cache
misses, unsafe measured memory or analysis time exceeding its actual freshness
objective identifies a bottleneck. First tune the measured batching/cache/compaction
boundary; move independent worker containers if the same bottleneck survives on
the target deployment. Production capacity, source coverage and live data update
resolution still require their own acceptance.

# Benchmarking and cost evidence

A benchmark measures a declared workload on stated hardware. It does not establish arbitrary scale, a universal VPS size, TikTok source throughput, or product profitability.

## Run

With isolated synthetic evidence and the required test services configured:

```sh
scripts/benchmark.sh 10000 output/benchmark.json
```

The wrapper invokes the application benchmark command and propagates its failure code. Increase record count only after confirming memory and disk headroom. Do not mix benchmark records into a live namespace or compare synthetic collection latency with TikTok latency.

On a configured Compose deployment, use `scripts/benchmark.sh --compose 10000 output/benchmark.json` so the application runs with the container's service endpoints. Host execution requires explicitly exported host-reachable database and S3 endpoints; a Compose `.env` is not automatically loaded by the host CLI.

Record CPU model/count, available memory, disk and filesystem, storage endpoint, database version, image revision, policy version, record size distribution, candidate/product skew, concurrency, quota, analytical lookback, and cache state alongside the output.

## Report

Measure input/output rows and bytes, collection and assessment throughput, achieved evidence freshness, queue age, end-to-end latency, peak resident memory, CPU time, errors/quarantine, local cache size/hits, object-store GET/HEAD/PUT requests, uploaded/downloaded bytes, and restore/recovery elapsed time where the exercise measured them.

Report cold-cache and warm-cache runs separately. Include partition count, object size distribution, manifest overhead, and compaction cost. More frequent polling does not improve source resolution if the upstream data is stale or rounded.

A synthetic in-process engine benchmark, a PostgreSQL/S3 integration benchmark, a multiworker load exercise, and a real API collection measurement answer different questions. Label the measured boundary and leave unsupported dimensions unknown. Do not merge their throughput numbers into a single advertised capacity.

## Cost scenarios

Estimate monthly cost from measured units multiplied by explicit current prices:

- VPS or cloud instance time, memory allocation, and local disk.
- PostgreSQL storage, backups, and measured growth.
- S3 bytes retained, request counts, cross-region traffic, and egress to the VPS.
- Optional external enrichment/provider requests that were actually enabled.

Keep provider prices and time assumptions outside the measured results, with a source/date. Report a range when cache hit ratio, retention, or request distribution is uncertain. Do not store or recalculate price estimates as observed operating costs.

## Scaling triggers

Persistent queue growth under available source quota, unsafe peak memory, analytical windows exceeding their freshness objective, cache thrashing, excessive object-store request overhead, and database contention identify different bottlenecks. Tune batch size, partitioning, compaction, and concurrency first, then repeat the representative workload.

Move independent collectors or analytical containers to additional hosts when measurements justify it. Keep PostgreSQL state and completed S3 manifests authoritative; no shared mutable DuckDB file is introduced. Choose distributed infrastructure only when a documented bottleneck survives these changes.

No numerical operating envelope or recurring cost is claimed until the corresponding benchmark has run on the actual target configuration.

## Controlled parallel workload without source credentials

Install the development dependencies and build the collector first. With an
isolated native test database, the helper creates a fresh schema and temporary
HTTP Moto service, runs the workload, and removes its test state:

```sh
TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test \
  uv run --locked python scripts/offline-benchmark.py \
  --records 1000 --product-count 50 --workers 4 --source-latency-ms 10 \
  --hot-fraction 0.6 --compact --output output/offline-benchmark.json
```

`--hot-fraction` assigns that fraction of source video identities to the first
product; other identities retain the deterministic distribution. Each worker
uses an independent application/catalog connection and the shared SQL queue and
budget. `--request-capacity` and `--requests-per-second` support a deliberately
constrained quota: incomplete collection returns a failed report with retryable
tasks rather than an infinite wait. The wrapper also forwards these options.

Mandatory check failures now set `ok: false` and a nonzero CLI exit code.
Reports include cold/warm analysis, optional compaction duration and object counts,
remaining queue state, cache/disk usage and aggregate worker storage traffic.
Process CPU/RSS does not include the full PostgreSQL/object-store service stack.
The finite workload and separate hard-crash tests do not establish sustained
queue stability, production capacity, restoration objectives or provider prices.

## Controlled operating-envelope matrix

Build the release collector, then run the sequential matrix against an isolated
native test database:

```sh
cargo build --release --locked --manifest-path collector/Cargo.toml
TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test \
  uv run --locked python scripts/offline-envelope.py \
  --collector-binary collector/target/release/sm-collector \
  --collector-profile release --output-dir output/envelope
```

The seven finite cases cover a uniform baseline, four-worker skew with controlled
source latency, matched paced arrivals with one and four workers, a higher arrival
rate, recovery under a constrained request budget, and termination after a raw
upload. The last case restarts ordinary collection and waits for the actual
experimental 40-second lease to expire. It does not modify SQL lease timestamps.
This is an integration measurement, with HTTP Moto and synthetic source responses.

Each case gets a fresh worker process, PostgreSQL schema, bucket, cache and spool.
The supervisor limits cases to four workers, 10,000 records, 500 tasks, 180 seconds
and 1 GiB of owned files plus schema relations. The suite admits cases only with
at least 4 GiB of memory headroom and 8 GiB of free disk, and lasts at most 15
minutes before bounded cleanup. A resource stop retains diagnostics and remaining
task counts. These limits are experiment safeguards, not measured production
capacity or a memory reservation on the shared host.

For this bounded experiment, `--start-memory-headroom-gib 3` permits a declared
3 GiB admission profile after checking the smaller baseline. The default remains
4 GiB, and both profiles stop execution below 2 GiB. The selected start and stop
limits are recorded in the summary; this option cannot disable the stop floor.

Reports record the code revision, binary hash and declared build profile, CPU
affinity and cgroup limits, database/library versions and effective nonsecret
settings. History partitioning uses the deployed default of 64 buckets. Parent
supervisor CPU includes Moto; the worker includes the controlled HTTP source.
PostgreSQL, Dagster, dashboard and other host processes are outside process
resource measurements. Sampled session RSS can miss brief peaks; when descendant
discovery is unavailable, aggregate RSS remains unknown rather than being reported
as the leader's memory.

Collection throughput uses the enqueue-and-collection phase duration. The legacy
whole-workflow throughput includes linking, assessment, compaction, verification
and freezing. Task latency uses actual SQL creation/completion times; future
logical fixture observation times do not measure live evidence freshness.
Queue sampling and disk scans are throttled to avoid measuring the sampler's work.

Both sides of compaction receive separate, initially empty application file
caches. Cold/warm phases record their own traffic and verified hits. They do not
claim to clear operating-system or provider caches. Assessment rows and evidence
identities must survive compaction; run IDs may change with dataset lineage.
Allocated cache/spool bytes deduplicate hard links, while promised download
reservations remain separate. Compaction reduces active read objects; original
objects retained for lineage still consume storage.

Use the case reports and `summary.json` to compare the declared workloads. A
failed or incomplete matrix establishes a stop condition, not a successful
operating envelope. Rerun on the target deployment with representative retained
history before choosing a VPS size or applying provider unit prices.

## Automated native regression workload

Every pull request and push to `main` runs `scripts/ci-benchmark.sh` against the
isolated native CI database and temporary loopback HTTP sources/object storage.
The first scenario publishes 1,000 observations with four workers, 10 ms source
delay, 60% hot-product skew and compaction. The second deliberately exhausts a
one-request budget: exit code 1 is required, the report must identify quota
exhaustion, and all four queued tasks must remain accounted for (one completed,
three retryable). Failure reports retain initial/final queue counts so completed
and deferred work can be inspected independently of error messages.

Both JSON reports and their diagnostic logs are retained in the workflow's
`verification-results` artifact. CI fails on an unexpected successful quota run,
an unrelated process error without a valid report, missing tasks, failed
publication checks or changed compaction row counts. The five-minute step limit
bounds the exercise; throughput has no machine-dependent pass threshold. This
protects reproducibility and degradation behavior, not a production operating
envelope. To repeat the same gate locally after building the collector:

```sh
TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test \
  bash scripts/ci-benchmark.sh
```

## Compact analytical result publication

The [100-product retained-history measurement](../verification/compact-results.md)
measures all immutable provenance nodes, uploads and SQL dependency edges for
240,000 observations, an unchanged cutoff and one appended refresh wave. Its
opt-in probe is separate from the ordinary suite. The measured small result
header must not be used as the total storage or request cost: cold publication
adds many objects, while repeated runs reuse them. Feature projection and actual
source collection require their own timing and traffic evidence.

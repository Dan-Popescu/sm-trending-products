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

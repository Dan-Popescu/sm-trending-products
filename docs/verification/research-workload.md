# Existing research report baseline

Measured on 2026-10-28 UTC at clean implementation revision
`524b1dee4d60752d86ada2c61a195a07f1d7b250`, using native PostgreSQL and an owned
loopback HTTP Moto server. [The complete successful report](research-workload-existing.json)
retains hardware/version facts, full-result hashes and IDs, phase counters,
sampled resource scope and cleanup outcomes. It was measured before U01/U02/U03
report adapters or the separate text hydration optimization arrive.

```sh
PYTHONPATH=src:tests python scripts/research-workload.py \
  --case combined --products 2 --entities 1 --samples 3 --texts 2 \
  --text-characters 96 --panel-size 2 --timeout 60 --max-disk-mib 64 \
  --output /tmp/night-workload-existing-baseline.json
```

The fixture contains 14 complete source rows, two products, two video entities,
four offers observed twice, and four supplied texts totaling 384 characters.
Combined execution returns eight complete reports through the public trend,
brief, content and metadata selection interfaces. Missing optional modules remain
explicit in brief panels. Setup and execution use distinct application caches;
cold/warm phases open separate application instances in the same owned worker.

| Phase | Wall seconds | Worker CPU seconds | Logical GETs | Logical PUTs | Download bytes | Upload bytes | Cache hits |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Setup | 1.0466 | 0.2438 | 0 | 9 | 0 | 13,314 | 0 |
| Cold | 1.2086 | 0.2142 | 5 | 0 | 10,925 | 0 | 9 |
| Warm | 1.1460 | 0.1788 | 0 | 0 | 0 | 0 | 14 |

Setup also made the bucket HEAD/create calls: 11 actual HTTP attempts versus nine
logical object PUTs. Execution counters have no retries in this sample. Bucket
bootstrap, retry billing and provider-specific rounding are separate from any
optional logical-request price scenario. No price file was supplied.

Cold and warm retained the same public IDs and the same hash of all eight full
outputs. The harness hashes complete reports before discarding the in-memory
results; it does not truncate evidence to reduce work. Endpoint worker RSS was
approximately 171.6 MB after setup, 186.6 MB after cold and 187.1 MB after warm.
These endpoint readings are not phase peaks. Complete session RSS is unknown
because this environment omits descendant-discovery links. PostgreSQL, Moto,
other processes and whole-VPS peak memory are outside those worker figures.

An earlier exact-revision attempt stopped at the actual memory admission floor
before setup when shared cgroup headroom was only 2,252,800 bytes.
[Its retained report](research-workload-memory-stop.json) records
`memory_headroom_limit`, null unaccounted in-flight usage and successful cleanup.
The successful run followed after headroom recovered; its guard was unchanged.
Neither run changed or cleared shared host caches.

Both runs removed their owned schema and temporary files and stopped their Moto
service, with no cleanup errors. The successful run's cold/warm cache reuse is
real; these tiny shared-host timings establish neither a production bottleneck
nor VPS capacity. No analytical optimization is justified from this sample.
Future comparisons should use explicit bounded text/history shapes and inspect
complete output identities alongside request counts and CPU, rather than treating
reduced evidence or a resource-stopped run as a speedup.

Validation for the implementation: ten native behavioral tests pass, full-source
mypy checks 138 modules, repository Ruff checks and format checks pass (321 files).
The [operator guide](../operations/research-workload.md) lists bounds, measurement
limits, independent case commands and optional dated price handling. Complete
repository CI and independent standards/spec review remain delivery gates.

A [follow-up measurement](research-workload-lifetime.json) at clean revision
`6f70d729e5cc72e918cd7a0ec11b82266e7310eb` uses the same command and fixture and
also records total fresh-worker CPU, including Python imports and application
close/flush outside phase timers. It completed with matching cold/warm identities,
the same storage quantities and clean teardown. Setup/cold/warm wall times were
0.8381/0.3695/0.3156 seconds and process CPU times were 0.2684/0.1738/0.1294
seconds. Full worker lifetime CPU was 2.0029 seconds, compared with 0.5716 seconds
inside those phase timers; reaped-child CPU was zero. This exposes startup and
other outside-phase work rather than silently omitting it from cost inspection.
Shared-host wall times vary between these runs, so they are not an optimization
comparison. Neither sample establishes how report CPU scales with large history.

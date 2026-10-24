# Retained-history measurement

GitHub delivery ticket: [#67](https://github.com/Dan-Popescu/sm-trending-products/issues/67).

Run a finite retained-history workload before choosing VPS hardware. The harness
publishes every requested synthetic observation through `Pipeline.import_records`
and reviews identity through the ordinary identity module. It never fabricates
observation-index rows or analytical outputs to reach a requested volume.

```sh
TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test \
  uv run --locked python scripts/retained-history.py \
  --products 1000 --videos-per-product 20 --history-days 30 \
  --refreshes-per-day 4 --refresh-waves 4 --deadline-seconds 14400 \
  --output output/retained-1000-30.json
```

This requires a dedicated native test database where the role can create schemas;
it does not use production namespaces, source credentials or a deployed object
store. Each case owns a fresh random schema and working directory. The supervisor
removes only these resources, retaining the result JSON and worker log. A deadline,
process-memory or disk stop retains the original request and partial progress;
it is an unsuccessful measurement, not an automatic smaller workload.
Use a fresh output path for each experiment; existing reports and harness archives are never overwritten.

Each new case also archives the exact harness source beside the JSON and records
its SHA256 plus dependency versions. The worker rejects a source change between
supervisor setup and its own startup. The archived script is source evidence;
restore it to `scripts/retained-history.py` in the recorded application checkout
to reproduce it, because its helper paths are repository-relative.

The default workload has 20 distinct videos/product, four observations/day and
120 samples/video over a nominal 30-day retained period (29 days 18 hours between
the first and last sample). A 90-day case retains 360 samples/video, spanning
89 days 18 hours. Analytics still uses its actual inclusive 30-day lookback.
All trajectories have equal growth and age; sampled complete-row oracle checks
therefore preserve the same cohort rank as the full homogeneous universe. This
fixture is not evidence of arbitrary heterogeneous ranking parity.

For a separate shared-file/skew exercise, use
`--shared-products-per-import 100 --hot-products 100 --hot-product-batch-rows 100`.
Ordinary groups pack complete product timelines into bounded import chunks;
neighbouring products can share a physical file. The first declared hot products
instead use many small files. This skews delivery/file counts while keeping the
same requested products, videos and observation trajectories. It is a physical
layout skew experiment, not a claim of heterogeneous creator or demand evidence.

Seeding defaults to at most 5,000 rows per call (the explicit maximum is 10,000).
The default physical layout retains
one timeline object per product when that fits a batch, resembling retained
compacted history. It does **not** measure one physical object or HTTP request
per upstream refresh. Provider grouping, quota, retry, discovery and source update
resolution remain separate workload assumptions. The case reports actual source
datasets, observation counts, object bytes, analytical artifact counts/bytes and
SQL relation/index bytes. Cold, unchanged, newly refreshed and completely evicted
analytical paths execute the real published-assessment module. Refresh ingestion
has separate wall/CPU times, request/byte deltas and completed product/observation
counts under `refresh_imports`; the `phases.refresh_N` timer covers assessment.
The supervisor's elapsed time also includes oracle verification and observation.
The example declares a four-hour guard so dense retained history can complete
rather than implicitly assuming a small-data deadline will suffice.

HTTP Moto retains payloads up to 32 KiB in RAM and spools larger payloads into
the owned working directory. The threshold is recorded, along with the unchanged
soft/hard descriptor limit and an estimated upper bound on spilled-object FDs.
Unsupported shapes are rejected before creating state; the harness never raises
system limits. The ordinary 20-video/four-daily day leaves stay below 32 KiB,
while larger raw/Parquet imports spill. Both actual RSS and open FD count are
observed. This exercises actual HTTP/storage
serialization; it does not establish any commercial provider's latency. Named
files and unlinked open spool files are accounted separately. The supervisor's
RSS guard covers the application/Moto child. PostgreSQL relation bytes are
measured, but server CPU/RSS and Dagster/dashboard resource use remain outside
that process measurement. Missing descendant-discovery support is reported.
WAL positions are server-global and cannot be attributed to this schema while
other test cases share the server.

The supervisor checks one-second samples against the declared process RSS,
owned-file plus schema-byte budget, free-disk floor and wall-clock deadline.
Owned directory accounting visits at most 2,500 entries per poll and begins a new
pass at most every 30 seconds. Its value is a rolling observation with an explicit
age, not an atomic filesystem total. The free-disk floor is checked every poll.
Individual atomic operations can finish between samples. It terminates only its
owned process session, then drops its schema and working directory. The default
2 GiB process limit and 24 GiB owned-data limit are guards, not recommendations
for a VPS. Set a smaller owned-data limit when the machine has less free disk;
the independent free-space floor remains enforced.

Use `--profile` only in a separate smaller diagnostic case. It profiles cold and
refresh operations and retains per-phase `.profile` files and the top cumulative
functions in JSON. Profiling and one-second observation add real overhead; keep
profiled timings separate from ordinary capacity measurements. Increase products
progressively (1,000, 5,000, 10,000) and history (30, 90 days), preserving failed
or stopped cases. Do not infer a successful 72-million-observation run from a
smaller test or multiply a single-product latency into a capacity certificate.

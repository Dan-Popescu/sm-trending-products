# Local verification measurements

Verified on 2026-10-22. The [machine-readable result](benchmark-local-1000.json)
contains a successful synthetic run, not a production capacity claim.
The measured application implementation is commit
`3d8778ef0ecdf95b418bb90eebbf79f0507c3793`; the run started at
`2026-10-22T09:43:41.338608+00:00`.

## Workload and checks

The release collector processed 1,000 observations in 50 controlled HTTP pages,
then the pipeline reviewed 50 products, published assessments, repeated the same
assessment, verified remote objects, and froze a forward-test decision.

All four report checks passed: every requested observation was published without
quarantine, remote object verification succeeded, repeated analysis produced the
same run identifier, and a forward decision was created. Collection reported zero
errors. The public `latest_assessments` boundary returned all 50 product
assessments; this measurement does not claim a browser-rendering check.

| Measurement | Observed value |
| --- | ---: |
| End-to-end elapsed time | 10.016 seconds |
| Observations per elapsed second | 99.836 |
| Cold analysis | 2.207 seconds |
| Warm analysis | 0.941 seconds |
| Parent process CPU time | 8.949 seconds |
| Peak parent RSS | 217,420 KiB |
| Peak child RSS accounting | 175,680 KiB |
| S3 API attempts | 804 |
| S3 GET / PUT / HEAD calls | 501 / 302 / 1 |
| Uploaded / downloaded bytes | 2,489,742 / 4,476,145 |
| Verified local cache hits | 400 |

## Execution context and limits

- Linux x86_64, Python 3.12.14, Rust release collector, one sequential collection
  worker, two Polars threads, four history partition buckets, and a 2 GB cache
  budget. The environment exposed nine logical CPUs; it is shared infrastructure,
  not a reserved VPS with a measured CPU allocation.
- The SQL service was an isolated in-memory PGlite server using
  `--max-connections 16`. `PG_PREPARE_THRESHOLD=none` disabled incompatible driver
  preparation in this fallback. PGlite multiplexes an embedded backend and does
  not establish native PostgreSQL concurrency, row-locking, or crash-durability
  behavior. Its connection limit must permit the Python catalog and Rust quota
  client to connect at the same time.
- The HTTP S3 service was Moto running in the same Python process. Its data was
  temporary. Source latency was loopback HTTP; no TikTok credentials, live source
  quota, provider network latency, or S3 billing was exercised.
- The measured path includes process startup, small-file publication, programmatic
  identity review, cold and warm analysis, remote-object verification, and decision
  freezing. It is not a sustained ingestion-throughput benchmark or a scaling
  operating envelope. Provider unit prices and retention remain separate inputs.
- Parent CPU/RSS accounting includes the local HTTP services. Child RSS is the
  process accounting high-water mark and can include memory inherited before
  executable replacement; it must not be presented as isolated Rust steady-state
  memory. The running embedded database has separate memory/CPU not represented
  by parent process accounting. These figures are not the complete service-stack
  resource requirement.

The collector release executable used SHA-256
`cccec66a1cc5213435990a13d10af4610b675782c4f7159bc66460a2d3146935`.
Its package artifacts were rebuilt and its fingerprint remained stable through
three subsequent Cargo builds before this run.

The harness started PGlite with an unused loopback port, started Moto, initialized
a fresh bucket/catalog, and invoked the public benchmark function in the same
process/network namespace. It used `records=1000`, `product_count=50`, and the
release collector path. All temporary services and data were removed afterward.
On a provisioned deployment, the supported entry point is
`sm-trends benchmark --records 1000 --product-count 50 --output benchmark.json`.

## Docker Compose configuration verification

The actual Docker Compose executable reported version 2.39.4. Its SHA-256 matched
the supplied checksum:
`7af95166a730b87e172d4fc9aefea8725d3c6c7327d59149267b452114ddb7d4`.

Both commands completed with exit code 0 and empty stderr, using placeholder
database credentials and bucket configuration supplied through the environment:

```bash
docker-compose --env-file .env.example config --quiet
docker-compose --env-file .env.example --profile local config --quiet
```

This verifies configuration parsing and profile/dependency resolution. No Docker
daemon was available, so these checks do not establish image builds, container
startup, PostgreSQL execution inside Docker, enforced resource limits, or volume
persistence. Those deployment checks remain separate from the successful local
functional benchmark.

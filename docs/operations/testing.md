# Verification environments

The common behavioral boundary is collection-to-visible evidence with a controlled HTTP source, temporary PostgreSQL, an HTTP S3-compatible service, and deterministic decision times. Numerical and temporal tests cover worked examples through public assessment/evaluation interfaces.

## Native service checks

CI starts PostgreSQL 17.11 and Moto 5.2.4 over real network connections, builds the Rust collector, installs locked dependencies, and runs the Python suite. It also validates the Compose configuration and builds the deployment image. No TikTok token is required or granted by these tests.

Set `DATABASE_URL`, `S3_ENDPOINT_URL`, `S3_BUCKET`, dummy AWS credentials, and `COLLECTOR_BINARY` to equivalent isolated local services to reproduce the checks. Never point tests at a production database or bucket. Integration test fixtures may create, truncate, or delete test state.

```sh
uv sync --locked --extra dev
cargo build --locked --manifest-path collector/Cargo.toml
make check
uv run --locked pytest -ra
```

## Constrained local SQL execution

The implementation environment lacks a Docker daemon and cannot start a standard native PostgreSQL service under a separate mapped user. An embedded PGLite PostgreSQL build may be used for local functional SQL checks through the project test bridge when available.

PGLite verifies SQL behavior at that boundary. It does not certify native PostgreSQL multi-process row locking, network failures, concurrent quota enforcement, production restore, or load performance. Those remain native-service CI/integration checks. Do not count a skipped native test as passed or report embedded-engine measurements as native database capacity.

## Source and deployment acceptance

Live TikTok capability acceptance requires the real approved accounts/scopes and permitted intended use. Source fixtures cannot establish this. Capture actual response semantics, scope failures, and quotas separately before enabling live collection.

A successful image build is not a successful VPS rollout. Record the target host, resource configuration, endpoint/provider acceptance, private access test, and successful isolated restore before production rollout.

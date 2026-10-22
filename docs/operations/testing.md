# Verification environments

The common behavioral boundary is collection-to-visible evidence with a controlled HTTP source, temporary PostgreSQL, an HTTP S3-compatible service, and deterministic decision times. Numerical and temporal tests cover worked examples through public assessment/evaluation interfaces.

## Native service checks

CI provides PostgreSQL 17.11, builds the Rust collector, installs locked dependencies, and runs the Python suite. Integration fixtures start their own Moto 5.2.4 HTTP server on an available loopback port and create an isolated bucket per test. CI also validates the Compose configuration and builds the deployment image. No TikTok token is required or granted by these tests.

Set `TEST_DATABASE_URL` to an isolated native PostgreSQL test database. Its role must be able to create and drop test schemas. The fixtures derive a scoped application `DATABASE_URL`, provide dummy credentials and tokens, and select their own S3 endpoint/buckets; setting an external `S3_ENDPOINT_URL` does not redirect these fixtures. Build the collector at its normal debug path before running the suite. Never use a production database: integration fixtures create and delete test state.

```sh
uv sync --locked --extra dev
cargo build --locked --manifest-path collector/Cargo.toml
make check
TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test \
  uv run --locked pytest -ra
```

## Constrained local SQL execution

Native PostgreSQL is now available in the managed Docker environment. Use
`scripts/offline-check.sh` to start an isolated PostgreSQL container and execute
the no-key suite, or supply an isolated `TEST_DATABASE_URL`. The early environment
used PGlite when native services were unavailable; that remains an optional
functional fallback and cannot certify native concurrency.

Set `TEST_PGLITE_BINARY` to the absolute path of `pglite-server` and leave `TEST_DATABASE_URL` unset to start the temporary in-memory bridge automatically. That fallback resets its ephemeral schema between tests. Native concurrency tests explicitly skip this engine because one multiplexed backend cannot validate contention. Without either variable, ordinary database fixtures skip and the native concurrency gate fails with the required configuration; inspect the summary rather than treating an unexecuted integration suite as a pass.

PGLite verifies SQL behavior at that boundary. It does not certify native PostgreSQL multi-process row locking, network failures, concurrent quota enforcement, production restore, or load performance. Those remain native-service CI/integration checks. Do not count a skipped native test as passed or report embedded-engine measurements as native database capacity.

CI runs `tests/test_native_concurrency.py` against its native PostgreSQL service. The scenarios contend on a shared credential across account aliases and claim one due-task frontier with multiple workers. After building the image, `scripts/ci-recovery.sh` creates a separate Compose project, seeds synthetic observations, a reviewed product, an alert, and a frozen decision, then invokes the real backup and isolated restore scripts. It overrides the job's external endpoints and leaves real TikTok tokens empty. That container exercise must actually run successfully before native recovery is marked verified; local static checks cannot satisfy it.

## Source and deployment acceptance

Live TikTok capability acceptance requires the real approved accounts/scopes and permitted intended use. Source fixtures cannot establish this. Capture actual response semantics, scope failures, and quotas separately before enabling live collection.

A successful image build is not a successful VPS rollout. Record the target host, resource configuration, endpoint/provider acceptance, private access test, and successful isolated restore before production rollout.

## Recorded execution

The [native CI report](../verification/native-ci.md) records 142 tests passed without
skips, the built image, and successful isolated recovery. Its artifact and tested
revision identify the proof boundary; production/live acceptance remains separate.

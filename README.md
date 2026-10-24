# sm-trending-products

An evidence pipeline for detecting emerging physical-product opportunities from accessible TikTok data.

The project separates attention, purchase demand, competition, and commercial feasibility. Source access and data quality must be verified before drawing business conclusions.

An independent planner admits due work through PostgreSQL; Rust capture workers
save raw responses durably before separate normalization. Immutable S3
raw/Parquet history supports reviewed product identity, four assessments,
historical replay and forward evaluation. Dagster coordinates downstream analysis
and maintenance; Polars and DuckDB process the evidence.

One application image runs as separately replaceable service containers, alongside
a separate PostgreSQL image and external S3 storage. A trusted host-local
controller applies signed releases while preserving the acquisition path. Operator
interfaces remain private through Tailscale. See the
[independent acquisition runtime](docs/operations/independent-acquisition.md).

Repository documentation and all technical identifiers use English. The earlier local SQLite experiment is outside the approved architecture.

Start with the [VPS runbook](docs/operations/runbook.md), [product requirements](docs/prd/overview.md), [delivery plan](docs/planning/README.md), and [autonomous implementation decisions](docs/implementation/assumptions.md).

To advance without TikTok access, install the documented Python/Rust tools and
run `scripts/offline-check.sh`. It starts a disposable native PostgreSQL container
unless `TEST_DATABASE_URL` already identifies an isolated test database. The suite
uses controlled loopback HTTP sources and temporary Moto buckets, including the
actual Dagster-to-review path and hard process-termination recovery scenarios.
See [offline verification](docs/verification/offline-readiness.md) for evidence
and remaining limits.

The [implementation coverage](docs/implementation/coverage.md),
[verification report](docs/verification/implementation.md), and
[handoff instructions](docs/implementation/handoff.md) distinguish implemented
software from the remaining live-source and deployment acceptance gates.

```bash
uv sync --locked --extra dev
cargo build --locked --manifest-path collector/Cargo.toml
uv run sm-trends --help
```

Local synthetic setup, Docker services, external S3 configuration, private access, and source acceptance are documented in the runbook. No paid provider or TikTok token is needed for the offline tests. Fixtures never establish live access or market results.

Use `sm-trends query --sql-file query.sql --synthetic` for bounded read-only DuckDB exploration of selected completed history. The `observations` view deduplicates replay copies; `observation_versions` preserves dataset versions for audit. The query surface cannot read arbitrary local files, contact external services, or modify deployment state.

See [verification environments](docs/operations/testing.md) and [backup and restore](docs/operations/backup-and-restore.md) before deployment. Live TikTok permissions, native PostgreSQL concurrency, the Docker image, and actual VPS rollout are separate acceptance steps. No live source or infrastructure has been enabled merely by implementing its adapter.

Native CI passed 142 tests without skips, built the deployment image, and verified
isolated database/object recovery. See the [execution report](docs/verification/native-ci.md)
for the tested revision and remaining production/live gates.

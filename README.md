# sm-trending-products

An evidence pipeline for detecting emerging physical-product opportunities from accessible TikTok data.

The project separates attention, purchase demand, competition, and commercial feasibility. Source access and data quality must be verified before drawing business conclusions.

The implementation includes Rust source adapters, a PostgreSQL control plane, immutable S3 raw/Parquet history, adaptive collection, reviewed product identity, four separate assessments, historical replay, forward evaluation, and a private operator interface. Dagster coordinates bounded batches; Polars and DuckDB process the evidence.

Repository documentation and all technical identifiers use English. The earlier local SQLite experiment is outside the approved architecture.

Start with the [VPS runbook](docs/operations/runbook.md), [product requirements](docs/prd/overview.md), [delivery plan](docs/planning/README.md), and [autonomous implementation decisions](docs/implementation/assumptions.md).

```bash
uv sync --locked --extra dev
cargo build --locked --manifest-path collector/Cargo.toml
uv run sm-trends --help
```

Local synthetic setup, Docker services, external S3 configuration, private access, and source acceptance are documented in the runbook. No paid provider or TikTok token is needed for the offline tests. Fixtures never establish live access or market results.

Use `sm-trends query --sql-file query.sql --synthetic` for bounded read-only DuckDB exploration of selected completed history. The `observations` view deduplicates replay copies; `observation_versions` preserves dataset versions for audit. The query surface cannot read arbitrary local files, contact external services, or modify deployment state.

See [verification environments](docs/operations/testing.md) and [backup and restore](docs/operations/backup-and-restore.md) before deployment. Live TikTok permissions, native PostgreSQL concurrency, the Docker image, and actual VPS rollout are separate acceptance steps. No live source or infrastructure has been enabled merely by implementing its adapter.

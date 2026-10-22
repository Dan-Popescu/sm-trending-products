# Delivery and verification

Use English for code, identifiers, documentation, issue titles, and filenames.
Use the glossary and accepted ADRs as the domain and architecture reference.

Implement an unblocked vertical ticket at the application boundary. Write a failing
behavior test, implement the behavior, then run the relevant test and static checks.
Do not replace an unavailable source capability with fabricated live data. Keep
fixtures synthetic, missing metrics explicit, and decision-time eligibility intact.

Install Python dependencies with `uv sync --locked --extra dev`. Build the collector
with `cargo build --locked --manifest-path collector/Cargo.toml`. Run:

```bash
uv run --locked ruff format --check src tests scripts
uv run --locked ruff check src tests scripts
uv run --locked mypy src
uv run --locked pytest -ra
cargo fmt --manifest-path collector/Cargo.toml --check
cargo clippy --locked --manifest-path collector/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path collector/Cargo.toml
```

Integration tests require a dedicated `TEST_DATABASE_URL`. They create and drop
random test schemas; never point them at an operator's production database. The
HTTP object-store fixture is an isolated Moto service. Native PostgreSQL CI is
required for concurrent locking, quotas, and fencing acceptance. The optional
`TEST_PGLITE_BINARY` fallback exercises SQL and protocol behavior but cannot prove
native backend concurrency; see the research note on test-engine limitations.

In restricted environments, mypy may need `--no-native-parser` because its native
parser enumerates unavailable host processes. Concurrent checks must use separate
cache directories. These are tooling accommodations, not reduced typing coverage.

Before merging, review standards separately from specification fit. Record exact
checks and remaining access-dependent acceptance in the pull request. A passing
fixture test is not evidence of real TikTok permissions, profitable opportunities,
or an executed VPS deployment.

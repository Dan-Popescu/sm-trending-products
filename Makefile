.PHONY: help setup test check rust-check build config up down status backup benchmark

help:
	@echo 'setup       Install locked Python dependencies and Rust collector'
	@echo 'check       Run Python formatting/lint/type checks and Rust checks'
	@echo 'test        Run tests (integration services configured through environment)'
	@echo 'config      Validate Compose configuration without printing interpolated secrets'
	@echo 'build/up    Build the image / start the supported deployment'
	@echo 'status      Print redacted operational status'
	@echo 'backup      BACKUP_DIR=/absolute/path make backup'
	@echo 'benchmark   RECORDS=10000 OUTPUT=output/benchmark.json make benchmark'

setup:
	uv sync --locked --extra dev
	cargo build --locked --manifest-path collector/Cargo.toml

check:
	uv run --locked ruff format --check src tests scripts
	uv run --locked ruff check src tests scripts
	uv run --locked mypy src
	$(MAKE) rust-check

rust-check:
	cargo fmt --manifest-path collector/Cargo.toml --check
	cargo clippy --locked --manifest-path collector/Cargo.toml --all-targets -- -D warnings
	cargo test --locked --manifest-path collector/Cargo.toml

test:
	uv run --locked pytest

config:
	docker compose config --quiet

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose run --rm --no-deps initialize sm-trends status --json

backup:
	./scripts/backup.sh "$(BACKUP_DIR)" --copy-objects

benchmark:
	./scripts/benchmark.sh "$(or $(RECORDS),10000)" "$(or $(OUTPUT),output/benchmark.json)"

#!/usr/bin/env bash
# Native-service verification with controlled HTTP sources; no live-source access.
set -euo pipefail
repository_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_directory"
command -v uv >/dev/null || { echo 'Install uv before running offline checks.' >&2; exit 2; }
command -v cargo >/dev/null || { echo 'Install the documented Rust toolchain first.' >&2; exit 2; }
uv sync --locked --extra dev
cargo build --locked --manifest-path collector/Cargo.toml
test_container=""
cleanup() {
  if [[ -n "$test_container" ]]; then docker rm -f "$test_container" >/dev/null; fi
}
trap cleanup EXIT
if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
  command -v docker >/dev/null || { echo 'Docker or an isolated TEST_DATABASE_URL is required.' >&2; exit 2; }
  test_container="sm-trends-offline-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
  docker run --detach --rm --name "$test_container" \
    --env POSTGRES_USER=offline_test --env POSTGRES_DB=offline_test \
    --env POSTGRES_PASSWORD=offline-test-only --publish 127.0.0.1::5432 \
    postgres:17.11-bookworm >/dev/null
  ready=false
  for _attempt in $(seq 1 60); do
    if docker exec "$test_container" pg_isready -U offline_test -d offline_test >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  [[ "$ready" == true ]] || { echo 'Isolated PostgreSQL did not become ready.' >&2; exit 1; }
  test_port="$(docker port "$test_container" 5432/tcp)"
  export TEST_DATABASE_URL="postgresql://offline_test:offline-test-only@${test_port}/offline_test"
fi
mkdir -p output
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-2}"
env -u TIKTOK_DISPLAY_ACCESS_TOKEN -u TIKTOK_CCL_ACCESS_TOKEN \
  uv run --locked pytest -ra --junitxml=output/offline-tests.xml "$@"
uv run --locked ruff format --check src tests scripts
uv run --locked ruff check src tests scripts
uv run --locked mypy --no-native-parser src

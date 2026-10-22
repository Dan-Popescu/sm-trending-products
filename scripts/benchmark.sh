#!/usr/bin/env bash
set -euo pipefail
run_in_compose=false
if [[ "${1:-}" == --compose ]]; then
  run_in_compose=true
  shift
fi
if [[ $# -lt 2 || ! "$1" =~ ^[1-9][0-9]*$ ]]; then
  echo 'Usage: scripts/benchmark.sh [--compose] RECORD_COUNT OUTPUT_JSON [BENCHMARK_OPTIONS...]' >&2
  exit 2
fi
mkdir -p "$(dirname "$2")"
record_count="$1"
output_path="$2"
shift 2
if [[ "$run_in_compose" == true ]]; then
  output_directory="$(cd "$(dirname "$output_path")" && pwd)"
  output_name="$(basename "$output_path")"
  exec docker compose run --rm --no-deps --user "$(id -u):$(id -g)" \
    --volume "$output_directory:/benchmark" initialize \
    sm-trends benchmark --records "$record_count" --output "/benchmark/$output_name" "$@"
fi
if command -v uv >/dev/null; then
  exec uv run --locked sm-trends benchmark --records "$record_count" --output "$output_path" "$@"
fi
exec sm-trends benchmark --records "$record_count" --output "$output_path" "$@"

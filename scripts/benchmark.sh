#!/usr/bin/env bash
set -euo pipefail
run_in_compose=false
if [[ "${1:-}" == --compose ]]; then
  run_in_compose=true
  shift
fi
if [[ $# -ne 2 || ! "$1" =~ ^[1-9][0-9]*$ ]]; then
  echo 'Usage: scripts/benchmark.sh [--compose] RECORD_COUNT OUTPUT_JSON' >&2
  exit 2
fi
mkdir -p "$(dirname "$2")"
if [[ "$run_in_compose" == true ]]; then
  output_directory="$(cd "$(dirname "$2")" && pwd)"
  output_name="$(basename "$2")"
  exec docker compose run --rm --no-deps --user "$(id -u):$(id -g)" \
    --volume "$output_directory:/benchmark" initialize \
    sm-trends benchmark --records "$1" --output "/benchmark/$output_name"
fi
if command -v uv >/dev/null; then
  exec uv run --locked sm-trends benchmark --records "$1" --output "$2"
fi
exec sm-trends benchmark --records "$1" --output "$2"

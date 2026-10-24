#!/usr/bin/env bash
# Run after the production image build on an isolated CI Docker daemon.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
mkdir -p output/continuity-evidence
task_docker=(env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH docker --host=unix:///var/run/docker.sock)
task_registry="sm-continuity-ci-${RANDOM}-${RANDOM}"
cleanup() {
  "${task_docker[@]}" stop --timeout=5 "$task_registry" >/dev/null 2>&1 || true
  "${task_docker[@]}" rm "$task_registry" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${task_docker[@]}" run --detach --name "$task_registry" \
  --publish 127.0.0.1:80:5000 registry:2.8.3 >/dev/null
uv run --locked python scripts/build-continuity-fixtures.py \
  --base-image "${CONTINUITY_BASE_IMAGE:-sm-trending-products:local}" \
  --output "$PWD/output/continuity-fixture-images.json"
PYTHONPATH=src:tests \
CONTINUITY_DOCKER_MANIFEST="$PWD/output/continuity-fixture-images.json" \
CONTINUITY_DOCKER_EVIDENCE_DIR="$PWD/output/continuity-evidence" \
uv run --locked pytest -q tests/test_docker_release_campaign.py \
  --junitxml=output/continuity-docker-tests.xml

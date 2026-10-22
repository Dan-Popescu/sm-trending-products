#!/usr/bin/env bash
# Native-container recovery verification. Requires the application image to be built.
set -euo pipefail
umask 077

command -v docker >/dev/null || { echo 'Docker Engine and Compose are required.' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'Python 3 is required.' >&2; exit 2; }
repository_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_directory"
mkdir -p output
recovery_report="$repository_directory/output/recovery-report.json"
recovery_workdir="$(mktemp -d)"
recovery_id="$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
recovery_password="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"

# Explicitly replace inherited CI/production values. This exercise never starts live workers.
export COMPOSE_PROJECT_NAME="sm-trends-ci-recovery-$recovery_id"
export COMPOSE_FILE="$repository_directory/compose.yaml"
export COMPOSE_PROFILES=local
export COMPOSE_ENV_FILES=/dev/null
export COMPOSE_DISABLE_ENV_FILE=true
export PIPELINE_IMAGE="${PIPELINE_IMAGE:-sm-trending-products:local}"
export POSTGRES_USER=sm_trends_ci
export POSTGRES_DB=sm_trends_ci
export POSTGRES_PASSWORD="$recovery_password"
export DATABASE_URL="postgresql://sm_trends_ci:$recovery_password@postgres:5432/sm_trends_ci"
export PIPELINE_NAMESPACE=ci-recovery
export S3_BUCKET="sm-trends-ci-recovery-$recovery_id"
export S3_ENDPOINT_URL=http://objectstore:5000
export AWS_ACCESS_KEY_ID=ci-testing
export AWS_SECRET_ACCESS_KEY=ci-testing
export AWS_SESSION_TOKEN=
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
export TIKTOK_DISPLAY_ACCESS_TOKEN=
export TIKTOK_CCL_ACCESS_TOKEN=
export CREATE_BUCKET_ON_INIT=true

cleanup() {
  recovery_status=$?
  trap - EXIT
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ "$recovery_status" -ne 0 ]]; then
    python3 - "$recovery_report" "$recovery_status" <<'PY'
import json
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(json.dumps({
    "status": "failed", "boundary": "native_container_recovery",
    "synthetic": True, "exit_code": int(sys.argv[2])
}, indent=2) + "\n")
PY
  fi
  rm -rf -- "$recovery_workdir"
  exit "$recovery_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 - "$recovery_report" <<'PY'
import json
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(json.dumps({
    "status": "running", "boundary": "native_container_recovery", "synthetic": True
}, indent=2) + "\n")
PY

docker compose config --quiet
docker compose up --detach --no-build --wait --wait-timeout 120 postgres objectstore
docker compose run --rm --no-deps initialize sm-trends init --create-bucket
docker compose run --rm --no-deps -T initialize python - <<'PY' > "$recovery_workdir/seed.json"
from datetime import timedelta

from sm_trending_products.contracts import AssessmentPolicy, canonical, now
from sm_trending_products.pipeline import Pipeline

timestamp = now()
with Pipeline.from_env() as application:
    records = [
        {
            "source": "ci_recovery",
            "entity_type": "video",
            "entity_id": "recovery-video",
            "observed_at": (timestamp - timedelta(hours=hours)).isoformat(),
            "available_at": (timestamp - timedelta(hours=hours)).isoformat(),
            "published_at": (timestamp - timedelta(days=1)).isoformat(),
            "synthetic": True,
            "metrics": {"view_count": {"lower": count, "upper": count}},
            "attributes": {"creator_id": "recovery-creator", "title": "Recovery product"},
        }
        for hours, count in [(2, 100), (1, 300)]
    ]
    dataset_ids = application.import_records(records, synthetic=True)
    application.create_product("recovery-product", "Recovery product", synthetic=True)
    application.review_link("recovery-product", "ci_recovery:video:recovery-video")
    cutoff = now() + timedelta(seconds=1)
    application.assess(
        cutoff, synthetic=True, policy=AssessmentPolicy(alert_threshold=0)
    )
    frozen = application.freeze(cutoff, synthetic=True)
    assert len(application.evidence("recovery-product", cutoff, synthetic=True)) == 2
    assert len(application.latest_assessments(synthetic=True)) == 1
    assert len(application.alerts(synthetic=True)) == 1
    assert any(row["evaluation_id"] == frozen["evaluation_id"]
               for row in application.evaluations(synthetic=True))
    assert application.verify()["ok"] is True
    print(canonical({
        "dataset_ids": dataset_ids, "observation_count": 2, "product_count": 1,
        "alert_count": 1, "forward_decision_id": frozen["evaluation_id"],
        "synthetic": True,
    }))
PY

scripts/backup.sh "$recovery_workdir/backup" --copy-objects
scripts/restore-check.sh "$recovery_workdir/backup"
python3 - "$recovery_workdir/seed.json" "$recovery_report" <<'PY'
import json
import sys
from pathlib import Path

seed = json.loads(Path(sys.argv[1]).read_text())
Path(sys.argv[2]).write_text(json.dumps({
    "status": "verified", "boundary": "native_container_recovery",
    "database": "PostgreSQL 17.11", "object_store": "HTTP Moto 5.2.4",
    "synthetic": True, "catalog_fingerprint_matched": True, "seed": seed,
}, indent=2) + "\n")
PY
echo 'Native PostgreSQL and HTTP object-store backup/restore rehearsal passed.'

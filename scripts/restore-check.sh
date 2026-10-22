#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -ne 1 || ! -f "$1/backup.json" ]]; then
  echo 'Usage: scripts/restore-check.sh /absolute/complete/backup-directory' >&2
  exit 2
fi
backup_directory="$(cd "$1" && pwd)"
python3 - "$backup_directory" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
metadata = json.loads((root / "backup.json").read_text())
if metadata.get("schema_version") != 1 or metadata.get("complete") is not True:
    raise SystemExit("Incomplete or unsupported backup")
for name, expected in metadata["sha256"].items():
    if Path(name).name != name:
        raise SystemExit("Invalid backup inventory filename")
    result = hashlib.sha256()
    with (root / name).open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            result.update(chunk)
    if result.hexdigest() != expected:
        raise SystemExit("Backup checksum mismatch")
objects = json.loads((root / "objects/snapshot.json").read_text())
if objects.get("copied_objects") is not True or objects.get("complete") is not True:
    raise SystemExit("Isolated restore requires --copy-objects backup")
PY

restore_id="$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
restore_password="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
restore_network="sm-trends-restore-$restore_id"
restore_database="sm-trends-restore-db-$restore_id"
restore_objects="sm-trends-restore-objects-$restore_id"
restore_bucket="sm-trends-restore-$restore_id"
pipeline_image="${PIPELINE_IMAGE:-sm-trending-products:local}"
restore_namespace="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["namespace"])' "$backup_directory/objects/snapshot.json")"

cleanup() {
  docker rm -f "$restore_database" "$restore_objects" >/dev/null 2>&1 || true
  docker network rm "$restore_network" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
docker network create --internal "$restore_network" >/dev/null
docker run --detach --rm --name "$restore_database" --network "$restore_network" \
  --label sm-trends.restore-isolated=true \
  --env POSTGRES_USER=restore --env POSTGRES_DB=restore \
  --env "POSTGRES_PASSWORD=$restore_password" \
  postgres:17.11-bookworm >/dev/null
docker run --detach --rm --name "$restore_objects" --network "$restore_network" \
  --label sm-trends.restore-isolated=true \
  --env MOTO_HOST=0.0.0.0 --env MOTO_PORT=5000 \
  motoserver/moto:5.2.4 >/dev/null

database_ready=false
for _attempt in $(seq 1 60); do
  if docker exec "$restore_database" pg_isready --username=restore --dbname=restore >/dev/null 2>&1; then
    database_ready=true
    break
  fi
  sleep 1
done
[[ "$database_ready" == true ]] || { echo 'Isolated database did not become ready.' >&2; exit 1; }
"$(dirname "$0")/restore-database.sh" --empty-container "$restore_database" "$backup_directory/pipeline.dump"

restore_environment=(
  --env "DATABASE_URL=postgresql://restore:$restore_password@$restore_database:5432/restore"
  --env "PIPELINE_NAMESPACE=$restore_namespace"
  --env "S3_ENDPOINT_URL=http://$restore_objects:5000"
  --env "S3_BUCKET=$restore_bucket"
  --env AWS_ACCESS_KEY_ID=restore
  --env "AWS_SECRET_ACCESS_KEY=$restore_password"
  --env AWS_DEFAULT_REGION=us-east-1
  --env CACHE_DIR=/tmp/cache
  --env SPOOL_DIR=/tmp/spool
)
objects_ready=false
for _attempt in $(seq 1 60); do
  if docker run --rm --network "$restore_network" "${restore_environment[@]}" "$pipeline_image" \
    python -c 'import os, urllib.request; urllib.request.urlopen(os.environ["S3_ENDPOINT_URL"] + "/", timeout=2).close()' >/dev/null 2>&1; then
    objects_ready=true
    break
  fi
  sleep 1
done
[[ "$objects_ready" == true ]] || { echo 'Isolated object store did not become ready.' >&2; exit 1; }
docker run --rm --user "$(id -u):$(id -g)" --network "$restore_network" "${restore_environment[@]}" \
  --volume "$backup_directory:/backup:ro" "$pipeline_image" \
  python scripts/object_snapshot.py restore /backup/objects --bucket "$restore_bucket" --create-bucket
docker run --rm --network "$restore_network" "${restore_environment[@]}" "$pipeline_image" \
  sm-trends verify --json
docker run --rm --network "$restore_network" "${restore_environment[@]}" "$pipeline_image" \
  sm-trends status --json
echo 'Isolated restore verified. No workers were started and no production destination was written.'

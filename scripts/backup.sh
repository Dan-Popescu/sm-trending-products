#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -lt 1 || $# -gt 2 || -z "$1" ]]; then
  echo 'Usage: scripts/backup.sh /absolute/new/backup-directory [--copy-objects]' >&2
  exit 2
fi
backup_directory="$1"
if [[ "$backup_directory" != /* || -e "$backup_directory" ]]; then
  echo 'Backup directory must be an absolute path that does not exist.' >&2
  exit 2
fi
copy_objects=false
if [[ $# -eq 2 ]]; then
  [[ "$2" == --copy-objects ]] || { echo 'Unknown option.' >&2; exit 2; }
  copy_objects=true
fi
command -v docker >/dev/null || { echo 'Docker Compose is required.' >&2; exit 1; }
mkdir -p "$backup_directory"
docker compose config --quiet
# Pause every writer in this Compose deployment while capturing its DB and objects.
# Other deployments sharing this store must be paused by the operator beforehand.
writer_services=()
while IFS= read -r service; do
  case "$service" in
    worker|dagster-daemon|dagster-webserver|dashboard) writer_services+=("$service") ;;
  esac
done < <(docker compose ps --services --status running)
resume_writers() {
  if [[ ${#writer_services[@]} -gt 0 ]]; then
    docker compose start "${writer_services[@]}" >/dev/null
  fi
}
trap resume_writers EXIT
if [[ ${#writer_services[@]} -gt 0 ]]; then
  docker compose stop "${writer_services[@]}" >/dev/null
fi
docker compose run --rm --no-deps initialize sm-trends verify --json > "$backup_directory/source-verification.json"
docker compose exec -T postgres sh -c \
  'exec pg_dump --format=custom --no-owner --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' \
  > "$backup_directory/pipeline.dump.partial"
mv "$backup_directory/pipeline.dump.partial" "$backup_directory/pipeline.dump"
docker compose exec -T postgres sh -c \
  'exec pg_dump --format=custom --no-owner --username="$POSTGRES_USER" --dbname=dagster' \
  > "$backup_directory/dagster.dump.partial"
mv "$backup_directory/dagster.dump.partial" "$backup_directory/dagster.dump"

snapshot_arguments=(capture --output /backup/objects)
if [[ "$copy_objects" == true ]]; then snapshot_arguments+=(--copy-objects); fi
docker compose run --rm --no-deps --user "$(id -u):$(id -g)" \
  --volume "$backup_directory:/backup" initialize \
  python scripts/object_snapshot.py "${snapshot_arguments[@]}" > "$backup_directory/object-summary.json"
python3 - "$backup_directory" <<'PY'
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

root = Path(sys.argv[1])
checksums = {}
for name in ("pipeline.dump", "dagster.dump", "source-verification.json", "object-summary.json"):
    result = hashlib.sha256()
    with (root / name).open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            result.update(chunk)
    checksums[name] = result.hexdigest()
metadata = {"schema_version": 1, "complete": True,
            "created_at": datetime.now(UTC).isoformat(), "sha256": checksums}
(root / "backup.json").write_text(json.dumps(metadata, indent=2) + "\n")
PY
echo "Backup completed: $backup_directory"
if [[ "$copy_objects" == false ]]; then
  echo 'Object inventory only: recovery depends on retained source S3 objects/versions.'
fi

#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# -lt 1 || -z "$1" ]]; then
  echo 'Usage: scripts/backup.sh /absolute/new/backup-directory [--copy-objects] [--base /absolute/completed/base-directory] [--online|--offline]' >&2
  exit 2
fi
backup_directory="$1"
shift
copy_objects=false
base_directory=
online=false
mode_given=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --copy-objects) copy_objects=true; shift ;;
    --online|--offline)
      [[ "$mode_given" == false ]] || { echo 'Choose one backup mode.' >&2; exit 2; }
      mode_given=true
      [[ "$1" == --online ]] && online=true || online=false
      shift ;;
    --base)
      [[ $# -ge 2 && -n "$2" ]] || { echo 'A base directory is required.' >&2; exit 2; }
      base_directory="$2"; shift 2 ;;
    *) echo 'Unknown backup option.' >&2; exit 2 ;;
  esac
done
if [[ "$backup_directory" != /* || -e "$backup_directory" ]]; then
  echo 'Backup directory must be an absolute path that does not exist.' >&2
  exit 2
fi
backup_parent="$(cd "$(dirname "$backup_directory")" && pwd)"
backup_name="$(basename "$backup_directory")"
if [[ -n "$base_directory" ]]; then
  [[ "$copy_objects" == true && -f "$base_directory/backup.json" ]] || { echo 'An incremental backup requires --copy-objects and a completed base.' >&2; exit 2; }
  base_directory="$(cd "$base_directory" && pwd)"
  [[ "$(dirname "$base_directory")" == "$backup_parent" ]] || { echo 'Backup chains must be siblings in one dedicated backup directory.' >&2; exit 2; }
fi
backup_deadline="${BACKUP_DEADLINE_SECONDS:-3600}"
[[ "$backup_deadline" =~ ^[0-9]+$ && "$backup_deadline" -ge 1 && "$backup_deadline" -le 86400 ]] || { echo 'BACKUP_DEADLINE_SECONDS must be between 1 and 86400.' >&2; exit 2; }
# Bound checkpoint commands and SQL dumps as well as the Python copy phase.
# GNU timeout signals this process group; the EXIT trap below restarts writers and
# removes the capture container. Allow at most 30 seconds for that cleanup.
if [[ "${SM_BACKUP_DEADLINE_ACTIVE:-}" != true ]]; then
  command -v timeout >/dev/null || { echo 'GNU timeout is required.' >&2; exit 1; }
  export SM_BACKUP_DEADLINE_ACTIVE=true
  bounded_arguments=("$backup_directory")
  if [[ "$copy_objects" == true ]]; then bounded_arguments+=(--copy-objects); fi
  if [[ -n "$base_directory" ]]; then bounded_arguments+=(--base "$base_directory"); fi
  if [[ "$online" == true ]]; then
    bounded_arguments+=(--online)
  else
    bounded_arguments+=(--offline)
  fi
  exec timeout --signal=TERM --kill-after=30s "$backup_deadline" bash "$0" "${bounded_arguments[@]}"
fi
command -v docker >/dev/null || { echo 'Docker Compose is required.' >&2; exit 1; }
mkdir -p "$backup_directory"
docker compose config --quiet
# Legacy checkpoints pause writers. Online mode instead exports one application
# snapshot shared by catalog verification, object requirements and pg_dump.
writer_services=()
if [[ "$online" != true ]]; then
  while IFS= read -r service; do
    case "$service" in
      worker|worker-green|capture-planner|normalizer|dagster-daemon|dagster-webserver|dashboard)
        writer_services+=("$service") ;;
    esac
  done < <(docker compose ps --services --status running)
fi
resume_writers() {
  if [[ ${#writer_services[@]} -gt 0 ]]; then
    docker compose start "${writer_services[@]}" >/dev/null
    writer_services=()
  fi
}
capture_pid=
capture_container="sm-trends-backup-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
cleanup() {
  backup_status=$?
  trap - EXIT
  if [[ -n "$capture_pid" ]]; then
    docker rm -f "$capture_container" >/dev/null 2>&1 || true
    wait "$capture_pid" || true
  fi
  resume_writers
  exit "$backup_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ ${#writer_services[@]} -gt 0 ]]; then
  docker compose stop "${writer_services[@]}" >/dev/null
fi

snapshot_arguments=(capture --output "/backups/$backup_name/objects"
  --checkpoint-signal "/backups/$backup_name/checkpoint-ready.json"
  --continue-signal "/backups/$backup_name/checkpoint-complete"
  --deadline-seconds "$backup_deadline")
if [[ "$copy_objects" == true ]]; then snapshot_arguments+=(--copy-objects); fi
if [[ -n "$base_directory" ]]; then snapshot_arguments+=(--base "/backups/$(basename "$base_directory")/objects"); fi
if [[ "$online" == true ]]; then
  snapshot_arguments+=(--online --catalog-state "/backups/$backup_name/catalog-state.json")
fi
echo 'Capturing a fenced object inventory and catalog checkpoint.'
docker compose run --rm --no-deps --name "$capture_container" --user "$(id -u):$(id -g)" \
  --volume "$backup_parent:/backups" initialize \
  python scripts/object_snapshot.py "${snapshot_arguments[@]}" > "$backup_directory/object-summary.json.partial" &
capture_pid=$!
checkpoint_started=$SECONDS
while [[ ! -f "$backup_directory/checkpoint-ready.json" ]]; do
  kill -0 "$capture_pid" 2>/dev/null || { wait "$capture_pid"; echo 'Object checkpoint failed.' >&2; exit 1; }
  [[ $((SECONDS-checkpoint_started)) -lt "$backup_deadline" ]] || { echo 'Object checkpoint timed out.' >&2; exit 1; }
  sleep 0.1
done

if [[ "$online" != true ]]; then
  docker compose run --rm --no-deps -T initialize python - <<'PY' > "$backup_directory/catalog-state.json"
from sm_trending_products.contracts import canonical
from sm_trending_products.pipeline import Pipeline

with Pipeline.from_env() as application:
    print(canonical(application.catalog.export_state()))
PY
fi
echo 'Capturing application and orchestration database dumps.'
if [[ "$online" == true ]]; then
  postgres_snapshot="$(python3 - "$backup_directory/checkpoint-ready.json" <<'PY'
import json
import re
import sys
value = json.load(open(sys.argv[1])).get("snapshot_id")
if not isinstance(value, str) or not re.fullmatch(r"[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+", value):
    raise SystemExit("Invalid exported PostgreSQL snapshot")
print(value)
PY
)"
  docker compose exec -T --env "BACKUP_SNAPSHOT_ID=$postgres_snapshot" postgres sh -c \
    'exec pg_dump --format=custom --no-owner --snapshot="$BACKUP_SNAPSHOT_ID" --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' \
    > "$backup_directory/pipeline.dump.partial"
else
  docker compose exec -T postgres sh -c \
    'exec pg_dump --format=custom --no-owner --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' \
    > "$backup_directory/pipeline.dump.partial"
fi
mv "$backup_directory/pipeline.dump.partial" "$backup_directory/pipeline.dump"
docker compose exec -T postgres sh -c \
  'exec pg_dump --format=custom --no-owner --username="$POSTGRES_USER" --dbname=dagster' \
  > "$backup_directory/dagster.dump.partial"
mv "$backup_directory/dagster.dump.partial" "$backup_directory/dagster.dump"
# LIST identity and catalog hashes fix the checkpoint's bytes. Per-object HEADs,
# conditional GETs and local base verification happen after writer resumption. Retention
# can now run. A disappearing uncopied source object leaves this backup incomplete;
# no long retention lock queues collection behind an exclusive maintenance waiter.
resume_writers
: > "$backup_directory/checkpoint-complete"
wait "$capture_pid"
capture_pid=
mv "$backup_directory/object-summary.json.partial" "$backup_directory/object-summary.json"
python3 - "$backup_directory" "$online" <<'PY'
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

root = Path(sys.argv[1])
objects = json.loads((root / "object-summary.json").read_text())
if objects.get("complete") is not True:
    raise SystemExit("Object backup did not complete")
(root / "source-verification.json").write_text(json.dumps({
    "ok": True if objects["copied_objects"] else None,
    "boundary": "checkpoint_content_copy" if objects["copied_objects"] else "checkpoint_inventory_only",
    "copied_objects": objects["copied_object_count"],
    "reused_verified_objects": objects["reused_object_count"],
}) + "\n")
def checksum(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            result.update(chunk)
    return result.hexdigest()
checksums = {name: checksum(root / name) for name in (
    "pipeline.dump", "dagster.dump", "source-verification.json", "object-summary.json", "catalog-state.json"
)}
metadata = {"schema_version": 2, "complete": True,
    "created_at": datetime.now(UTC).isoformat(), "sha256": checksums,
    "object_snapshot_sha256": checksum(root / "objects/snapshot.json")}
metadata["application_checkpoint"] = (
    "exported_snapshot" if sys.argv[2] == "true" else "paused_writers"
)
# PostgreSQL snapshots cannot be shared between separate databases. Dagster is
# orchestration metadata, not the authoritative evidence/task publication state.
metadata["dagster_checkpoint"] = "independent_database_snapshot"
(root / "backup.json").write_text(json.dumps(metadata, indent=2) + "\n")
PY
echo "Backup completed: $backup_directory"
if [[ "$copy_objects" == false ]]; then
  echo 'Object inventory only: recovery depends on retained source S3 objects/versions.'
fi

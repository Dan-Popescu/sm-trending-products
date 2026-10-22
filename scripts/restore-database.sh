#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 || "$1" != --empty-container || -z "$2" || ! -f "$3" ]]; then
  echo 'Usage: scripts/restore-database.sh --empty-container ISOLATED_CONTAINER pipeline.dump' >&2
  exit 2
fi
restore_container="$2"
dump_file="$3"
if [[ "$restore_container" != sm-trends-restore-* ]]; then
  echo 'Refusing destination: container name must start with sm-trends-restore-.' >&2
  exit 2
fi
if [[ "$(docker inspect --format '{{ index .Config.Labels "sm-trends.restore-isolated" }}' "$restore_container")" != true ]]; then
  echo 'Refusing destination: isolated-restore label is missing.' >&2
  exit 2
fi
table_count="$(docker exec "$restore_container" sh -c \
  'psql --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --tuples-only --no-align --command="SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('\''pg_catalog'\'', '\''information_schema'\'');"')"
[[ "$table_count" == 0 ]] || { echo 'Refusing destination: database is not empty.' >&2; exit 2; }
docker exec -i "$restore_container" sh -c \
  'exec pg_restore --exit-on-error --single-transaction --no-owner --no-privileges --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"' \
  < "$dump_file"
echo 'Database restored into the isolated empty destination.'

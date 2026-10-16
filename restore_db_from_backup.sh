#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--help" ]; then
  echo "Usage: $0 BACKUP.dump --confirm-database DATABASE_NAME"
  echo "Destructive legacy restore. Stop all Compose services except db and redis first."
  exit 0
fi
if [ "$#" -ne 3 ] || [ "${2:-}" != "--confirm-database" ]; then
  echo "Refusing restore: supply an explicit dump and --confirm-database DATABASE_NAME." >&2
  exit 2
fi
BACKUP_FILE="$1"
CONFIRMED_DB="$3"

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

DB_SERVICE="${DB_SERVICE:-db}"
DB_USER="${POSTGRES_USER:?POSTGRES_USER is required in .env or the environment}"
DB_NAME="${POSTGRES_DB:?POSTGRES_DB is required in .env or the environment}"
if [ "$CONFIRMED_DB" != "$DB_NAME" ]; then
  echo "Refusing restore: confirmation does not match the configured database." >&2
  exit 2
fi
if [ ! -f "$BACKUP_FILE" ]; then
  echo "Error: the explicit backup file does not exist." >&2
  exit 1
fi

RUNNING_SERVICES="$(docker compose ps --services --status running)"
while IFS= read -r service; do
  case "$service" in
    "$DB_SERVICE"|redis|"") ;;
    *) echo "Refusing restore: stop service '$service' and other application writers first." >&2; exit 2 ;;
  esac
done <<< "$RUNNING_SERVICES"

if ! docker compose ps -q "$DB_SERVICE" | grep -q .; then
  echo "Error: docker compose service '$DB_SERVICE' is not running."
  exit 1
fi

echo "Restoring '$DB_NAME' from: $BACKUP_FILE"
echo "This will drop and recreate objects inside the target database."

docker compose exec -T "$DB_SERVICE" \
  pg_restore \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  < "$BACKUP_FILE"

echo ""
echo "Restore complete."

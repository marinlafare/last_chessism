#!/usr/bin/env bash
set -euo pipefail

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

DB_SERVICE="${DB_SERVICE:-db}"
DB_USER="${POSTGRES_USER:?POSTGRES_USER is required in .env or the environment}"
DB_NAME="${POSTGRES_DB:?POSTGRES_DB is required in .env or the environment}"
BACKUP_DIR="${BACKUP_DIR:-./db_backup}"
BACKUP_FILE="${1:-}"

if [ -z "$BACKUP_FILE" ]; then
  BACKUP_FILE="$(ls -t "$BACKUP_DIR"/"${DB_NAME}"-*.dump 2>/dev/null | head -n 1 || true)"
fi

if [ -z "$BACKUP_FILE" ] || [ ! -f "$BACKUP_FILE" ]; then
  echo "Error: no backup file found. Pass a .dump path or create one with ./backup.sh."
  exit 1
fi

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

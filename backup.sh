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
OUTPUT_DIR="${BACKUP_DIR:-./db_backup}"
TIMESTAMP="$(date +%Y_%m_%d-%H_%M_%S)"

mkdir -p "$OUTPUT_DIR"

DUMP_PATH="$OUTPUT_DIR/${DB_NAME}-${TIMESTAMP}.dump"
LOG_PATH="$OUTPUT_DIR/${DB_NAME}-${TIMESTAMP}.pg_dump.log"
SHA_PATH="$DUMP_PATH.sha256"
TOC_PATH="$OUTPUT_DIR/${DB_NAME}-${TIMESTAMP}.toc"

echo "Starting backup of database '$DB_NAME' from docker compose service '$DB_SERVICE'."
echo "Writing custom-format dump to: $DUMP_PATH"

docker compose exec -T "$DB_SERVICE" \
  pg_dump \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  -F c \
  --no-owner \
  --no-privileges \
  --verbose \
  > "$DUMP_PATH" \
  2> "$LOG_PATH"

sha256sum "$DUMP_PATH" > "$SHA_PATH"
docker compose exec -T "$DB_SERVICE" pg_restore --list < "$DUMP_PATH" > "$TOC_PATH"

echo ""
echo "Backup complete."
echo "Dump:     $DUMP_PATH"
echo "Checksum: $SHA_PATH"
echo "TOC:      $TOC_PATH"
echo "Log:      $LOG_PATH"

#!/usr/bin/env bash
set -euo pipefail

BACKUP_FILE="${1:-backups/chessism_db-windows.backup}"
DB_NAME="chessism_db"
DB_USER="chessism_user"
CONTAINER_NAME="db"

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "Backup file not found: $BACKUP_FILE"
  exit 1
fi

echo "Ensuring Docker DB service is running and DB exists..."
if ! docker compose ps -q "$CONTAINER_NAME" | grep -q .; then
  echo "Docker service '$CONTAINER_NAME' is not running. Starting it..."
  docker compose up -d "$CONTAINER_NAME"
fi

docker compose exec -T "$CONTAINER_NAME" psql -U "$DB_USER" -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
docker compose exec -T "$CONTAINER_NAME" psql -U "$DB_USER" -d postgres -c "CREATE DATABASE $DB_NAME;"

echo "Restoring schema (pre-data)..."
set +o pipefail
cat "$BACKUP_FILE" | docker compose exec -T "$CONTAINER_NAME" pg_restore \
  --section=pre-data \
  --dbname="$DB_NAME" \
  --username="$DB_USER" \
  --no-owner
set -o pipefail

echo "Restoring data..."
cat "$BACKUP_FILE" | docker compose exec -T "$CONTAINER_NAME" pg_restore \
  --section=data \
  --dbname="$DB_NAME" \
  --username="$DB_USER" \
  --no-owner

echo "De-duping fen table..."
docker compose exec -T "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" <<'SQL'
BEGIN;
CREATE TABLE IF NOT EXISTS fen_dedup (LIKE fen INCLUDING DEFAULTS);
TRUNCATE fen_dedup;
INSERT INTO fen_dedup
SELECT DISTINCT ON (fen)
  fen, n_games, moves_counter, next_moves, score, wdl_win, wdl_draw, wdl_loss
FROM fen
ORDER BY fen,
         (wdl_win IS NULL),
         (wdl_draw IS NULL),
         (wdl_loss IS NULL),
         n_games DESC;
TRUNCATE fen;
INSERT INTO fen SELECT * FROM fen_dedup;
DROP TABLE fen_dedup;
COMMIT;
SQL

echo "Backfilling any missing FENs referenced by associations..."
docker compose exec -T "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" <<'SQL'
INSERT INTO fen (fen, n_games, moves_counter, next_moves, score, wdl_win, wdl_draw, wdl_loss)
SELECT DISTINCT
  gfa.fen_fen,
  0,
  '#0_0',
  NULL::text,
  NULL::double precision,
  NULL::double precision,
  NULL::double precision,
  NULL::double precision
FROM game_fen_association gfa
LEFT JOIN fen f ON f.fen = gfa.fen_fen
WHERE f.fen IS NULL;
SQL

echo "Restoring constraints, indexes, and FKs (post-data)..."
set +o pipefail
cat "$BACKUP_FILE" | docker compose exec -T "$CONTAINER_NAME" pg_restore \
  --section=post-data \
  --dbname="$DB_NAME" \
  --username="$DB_USER" \
  --no-owner
set -o pipefail

echo "Restore complete."

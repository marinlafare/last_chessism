#!/bin/bash

# --- Stop script on any error ---
set -e

# --- Configuration ---
CONTAINER_NAME="db"
DB_USER="chessism_user"
DB_NAME="chessism_db"
BACKUP_DIR="./backups"
BACKUP_PATTERN="$BACKUP_DIR/backup-${DB_NAME}-*.dump"

echo "Starting database restoration for '$DB_NAME'..."

# --- 1. Find the latest backup file ---
# ls -t lists by modification time, newest first. head -n 1 takes the newest file.
LATEST_BACKUP=$(ls -t $BACKUP_PATTERN 2>/dev/null | head -n 1)

if [ -z "$LATEST_BACKUP" ]; then
    echo "❌ Error: No backup files found matching pattern '$BACKUP_PATTERN'."
    exit 1
fi

echo "Found latest backup file: $(basename $LATEST_BACKUP)"
echo "Restoring database from $LATEST_BACKUP..."

# --- 2. Check if containers are running ---
# Check if the target container is running (returns non-zero if not running)
if ! docker compose ps -q $CONTAINER_NAME | grep -q .; then
    echo "❌ Error: Docker service '$CONTAINER_NAME' is not running. Please run 'docker compose up -d' first."
    exit 1
fi

# --- 3. Execute the Restore Command ---
# cat [file] | pipes the file content into the pg_restore command inside the container.
# --clean: drops existing tables before restoring them.
cat "$LATEST_BACKUP" | docker compose exec -T $CONTAINER_NAME pg_restore -U $DB_USER -d $DB_NAME --clean

# --- 4. Success Message ---
echo ""
echo "✅ Restoration complete! The database '$DB_NAME' is now restored."
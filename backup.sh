#!/bin/bash

# This script creates a compressed backup of your PostgreSQL database.
# It uses 'docker-compose exec' to run pg_dump inside the running 'db' container.
# --- MODIFIED: This script will now delete the single most recent backup in
#          the target directory before creating a new one. ---

# --- Stop script on any error ---
set -e

# --- Configuration ---
# Get these values from your docker-compose.yml
CONTAINER_NAME="db"
DB_USER="chessism_user"
DB_NAME="chessism_db"
OUTPUT_DIR="./backups" # We will create this directory

# --- Create backup directory if it doesn't exist ---
mkdir -p $OUTPUT_DIR

# --- NEW: Find and delete the most recent backup ---
echo "Checking for previous backup..."
# Find the newest file in the directory matching the pattern
# ls -t lists by modification time, newest first
# head -n 1 takes only the first line
# 2>/dev/null hides the "No such file or directory" error if no backups exist
LATEST_BACKUP=$(ls -t "$OUTPUT_DIR"/backup-"$DB_NAME"-*.dump 2>/dev/null | head -n 1)

if [ -f "$LATEST_BACKUP" ]; then
    echo "Found previous backup: $LATEST_BACKUP"
    echo "Deleting it..."
    rm "$LATEST_BACKUP"
    echo "Previous backup deleted."
else
    echo "No previous backups found."
fi
# --- END NEW SECTION ---


# --- Backup Filename ---
# Creates a filename like: backup-chessism_db-2025_11_10-22_15_30.dump
FILENAME="backup-${DB_NAME}-$(date +%Y_%m_%d-%H_%M_%S).dump"
FULL_PATH="$OUTPUT_DIR/$FILENAME"

echo "Starting new backup of database '$DB_NAME'..."

# --- The Backup Command ---
# docker-compose exec [service_name] [command_to_run]
#
# -T               : Disables pseudo-tty allocation (necessary for redirecting output)
# pg_dump          : The PostgreSQL backup utility
# -U $DB_USER    : The username to connect as
# -d $DB_NAME    : The database to dump
# -F c             : Format = Custom (compressed, efficient format)
# > $FULL_PATH   : Redirects the output from the command to a file on your host machine
docker-compose exec -T $CONTAINER_NAME pg_dump -U $DB_USER -d $DB_NAME -F c > $FULL_PATH

# --- Success Message ---
echo ""
echo "✅ New backup complete!"
echo "File created at: $FULL_PATH"
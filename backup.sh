#!/bin/bash

# This script creates a compressed backup of your PostgreSQL database.
# It uses 'docker-compose exec' to run pg_dump inside the running 'db' container.

# --- Stop script on any error ---
set -e

# --- Configuration ---
# Get these values from your docker-compose.yml
CONTAINER_NAME="db"
DB_USER="chessism_user"
DB_NAME="chessism_db"

# --- Backup Filename ---
# Creates a filename like: backup-chessism_db-2025_11_10-22_15_30.dump
FILENAME="backup-${DB_NAME}-$(date +%Y_%m_%d-%H_%M_%S).dump"
OUTPUT_DIR="./backups" # We will create this directory

# --- Create backup directory if it doesn't exist ---
mkdir -p $OUTPUT_DIR
FULL_PATH="$OUTPUT_DIR/$FILENAME"

echo "Starting backup of database '$DB_NAME'..."

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
echo "✅ Backup complete!"
echo "File created at: $FULL_PATH"
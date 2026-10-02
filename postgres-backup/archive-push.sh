#!/usr/bin/env bash
set -u

EXPECTED_UUID="${BACKUP_VOLUME_UUID:-1ebffac6-4b7a-4906-9e07-9586060d3825}"
VOLUME_ROOT="${BACKUP_VOLUME_ROOT:-/backup-volume}"
MARKER_PATH="${VOLUME_ROOT}/.chessism-backup-volume"
GAP_PATH="${CHESSISM_ARCHIVE_GAP_PATH:-/var/spool/pgbackrest/chessism-archive-gap}"
STANZA="${PGBACKREST_STANZA:-chessism}"
ARCHIVE_INFO="${VOLUME_ROOT}/chessism/database/pgbackrest/archive/${STANZA}/archive.info"
WAL_PATH="${1:-}"

mark_gap() {
  local reason="$1"
  local temporary="${GAP_PATH}.tmp.$$"
  umask 077
  {
    date --utc --iso-8601=seconds
    printf '%s\n' "$reason"
  } > "$temporary"
  mv "$temporary" "$GAP_PATH"
  printf 'Chessism backup warning: %s; WAL marked as intentionally unarchived.\n' "$reason" >&2
}

if [[ -z "$WAL_PATH" ]]; then
  mark_gap "archive-push received no WAL path"
  exit 0
fi

if [[ ! -r "$MARKER_PATH" ]] || [[ "$(tr -d '[:space:]' < "$MARKER_PATH")" != "$EXPECTED_UUID" ]]; then
  mark_gap "dedicated backup volume unavailable"
  exit 0
fi

if [[ ! -r "$ARCHIVE_INFO" ]]; then
  mark_gap "pgBackRest stanza is not initialized"
  exit 0
fi

if ! pgbackrest --stanza="$STANZA" archive-push "$WAL_PATH"; then
  mark_gap "pgBackRest archive-push failed"
  # Database availability wins over a backup chain. The next manual backup
  # sees the gap marker and starts a new full chain.
  exit 0
fi

exit 0

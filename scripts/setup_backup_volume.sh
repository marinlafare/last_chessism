#!/usr/bin/env bash
set -euo pipefail

EXPECTED_UUID="1ebffac6-4b7a-4906-9e07-9586060d3825"
MOUNT_POINT="/main-monitor-db-backups"
FSTAB_FILE="/etc/fstab"
SOURCE_BACKUPS="/home/jon/Desktop/workshop/db_backups/chessism"
FSTAB_ENTRY="UUID=${EXPECTED_UUID} ${MOUNT_POINT} ext4 defaults,nofail,x-systemd.automount,x-systemd.device-timeout=10s 0 2"

if [[ "${EUID}" -ne 0 ]]; then
  printf 'Run this script through sudo or pkexec.\n' >&2
  exit 1
fi

DEVICE="$(blkid -U "$EXPECTED_UUID" || true)"
if [[ -z "$DEVICE" ]] || [[ "$(blkid -s UUID -o value "$DEVICE")" != "$EXPECTED_UUID" ]]; then
  printf 'The expected backup partition UUID was not found. Nothing was changed.\n' >&2
  exit 1
fi

EXISTING_TARGET="$(findmnt -rn -S "UUID=${EXPECTED_UUID}" -o TARGET | head -n 1 || true)"
if [[ -n "$EXISTING_TARGET" ]] && [[ "$EXISTING_TARGET" != "$MOUNT_POINT" ]]; then
  printf 'The backup partition is already mounted at %s. Unmount it before setup.\n' "$EXISTING_TARGET" >&2
  exit 1
fi

install -d -m 0755 "$MOUNT_POINT"

TEMPORARY_READ_ONLY_MOUNT=0
cleanup_temporary_mount() {
  if [[ "$TEMPORARY_READ_ONLY_MOUNT" -eq 1 ]] && mountpoint -q "$MOUNT_POINT"; then
    umount "$MOUNT_POINT"
  fi
}
trap cleanup_temporary_mount EXIT

if ! mountpoint -q "$MOUNT_POINT"; then
  # Inspect the partition read-only before changing fstab or creating files.
  mount -o ro "UUID=${EXPECTED_UUID}" "$MOUNT_POINT"
  TEMPORARY_READ_ONLY_MOUNT=1
fi

MOUNTED_DEVICE="$(findmnt -rn -M "$MOUNT_POINT" -o SOURCE)"
MOUNTED_UUID="$(blkid -s UUID -o value "$MOUNTED_DEVICE")"
if [[ "$MOUNTED_UUID" != "$EXPECTED_UUID" ]]; then
  printf 'Mounted UUID mismatch. Refusing to create application directories.\n' >&2
  exit 1
fi

if [[ ! -d "${MOUNT_POINT}/timeshift" ]]; then
  printf 'The existing Timeshift directory was not found. Refusing to write to this filesystem.\n' >&2
  exit 1
fi

if [[ "$TEMPORARY_READ_ONLY_MOUNT" -eq 1 ]]; then
  umount "$MOUNT_POINT"
  TEMPORARY_READ_ONLY_MOUNT=0
fi

if ! grep -Eq "^[[:space:]]*UUID=${EXPECTED_UUID}[[:space:]]" "$FSTAB_FILE"; then
  FSTAB_BACKUP="${FSTAB_FILE}.before-chessism-$(date --utc +%Y%m%dT%H%M%SZ)"
  cp --preserve=all "$FSTAB_FILE" "$FSTAB_BACKUP"
  printf '\n# Dedicated recovery disk: Timeshift remains in /timeshift; app backups use separate top-level directories.\n%s\n' "$FSTAB_ENTRY" >> "$FSTAB_FILE"
  printf 'Saved the previous fstab as %s.\n' "$FSTAB_BACKUP"
fi

systemctl daemon-reload
if ! mountpoint -q "$MOUNT_POINT"; then
  mount "$MOUNT_POINT"
fi

MOUNTED_DEVICE="$(findmnt -rn -M "$MOUNT_POINT" -o SOURCE)"
MOUNTED_UUID="$(blkid -s UUID -o value "$MOUNTED_DEVICE")"
if [[ "$MOUNTED_UUID" != "$EXPECTED_UUID" ]]; then
  printf 'Mounted UUID mismatch after persistent mount. Refusing to write.\n' >&2
  exit 1
fi

# A previous read-only inspection mount can leave a per-mount VFS flag even
# though the ext4 superblock itself is writable. Enforce and verify the fstab
# mode before creating any application data.
mount -o remount,rw "$MOUNT_POINT"
VFS_OPTIONS="$(findmnt -rn -M "$MOUNT_POINT" -o VFS-OPTIONS)"
if [[ ",$VFS_OPTIONS," != *,rw,* ]]; then
  printf 'The backup filesystem is not mounted read-write. Refusing to continue.\n' >&2
  exit 1
fi

# This marker lets containers distinguish the recovery filesystem from an
# unmounted host directory. No path inside Timeshift is read or modified here.
printf '%s\n' "$EXPECTED_UUID" > "${MOUNT_POINT}/.chessism-backup-volume"
chmod 0444 "${MOUNT_POINT}/.chessism-backup-volume"

install -d -o 999 -g 999 -m 0750 \
  "${MOUNT_POINT}/chessism" \
  "${MOUNT_POINT}/chessism/database" \
  "${MOUNT_POINT}/chessism/database/pgbackrest" \
  "${MOUNT_POINT}/chessism/database/manifests" \
  "${MOUNT_POINT}/chessism/fen-analysis" \
  "${MOUNT_POINT}/chessism/research" \
  "${MOUNT_POINT}/chessism/research/matrices"

REQUESTING_UID="${SUDO_UID:-1000}"
REQUESTING_GID="${SUDO_GID:-1000}"
install -d -o "$REQUESTING_UID" -g "$REQUESTING_GID" -m 0750 \
  "${MOUNT_POINT}/real-butler"

if [[ -d "$SOURCE_BACKUPS" ]]; then
  while IFS= read -r -d '' source_file; do
    destination="${MOUNT_POINT}/chessism/fen-analysis/$(basename "$source_file")"
    if [[ ! -e "$destination" ]]; then
      cp --preserve=all "$source_file" "$destination"
    fi
  done < <(find "$SOURCE_BACKUPS" -maxdepth 1 -type f -name 'fen-analysis-*' -print0)

  if [[ -d "${SOURCE_BACKUPS}/research" ]]; then
    cp -a -n "${SOURCE_BACKUPS}/research/." "${MOUNT_POINT}/chessism/research/"
  fi
  chown -R 999:999 "${MOUNT_POINT}/chessism"
fi

CHESSISM_BYTES="$(du -sb "${MOUNT_POINT}/chessism" | awk '{print $1}')"
USAGE_TEMP="${MOUNT_POINT}/chessism/.backup-usage.json.tmp.$$"
printf '{"schema_version":1,"updated_at":"%s","total_bytes":%s}\n' \
  "$(date --utc --iso-8601=seconds)" "$CHESSISM_BYTES" > "$USAGE_TEMP"
chown 999:999 "$USAGE_TEMP"
chmod 0640 "$USAGE_TEMP"
mv "$USAGE_TEMP" "${MOUNT_POINT}/chessism/backup-usage.json"

STATUS_PATH="${MOUNT_POINT}/chessism/backup-status.json"
if [[ ! -e "$STATUS_PATH" ]]; then
  STATUS_TEMP="${MOUNT_POINT}/chessism/.backup-status.json.tmp.$$"
  printf '%s\n' \
    "{\"schema_version\":1,\"app_id\":\"chessism\",\"database_engine\":\"postgresql\",\"status\":\"stale\",\"trigger\":null,\"backup_id\":null,\"backup_type\":null,\"started_at\":null,\"completed_at\":null,\"last_verified_at\":null,\"restore_tested\":false,\"artifact_count\":0,\"total_bytes\":0,\"bytes_added\":0,\"destination_uuid\":\"${EXPECTED_UUID}\",\"relative_path\":\"main-monitor-db-backups/chessism/database\",\"manifest_sha256\":null,\"error_code\":null,\"error_message\":null,\"detail\":\"No database backup has run yet.\"}" \
    > "$STATUS_TEMP"
  chown 999:999 "$STATUS_TEMP"
  chmod 0640 "$STATUS_TEMP"
  mv "$STATUS_TEMP" "$STATUS_PATH"
fi

sync -f "$MOUNT_POINT"
printf 'Backup volume ready at %s. Timeshift was not modified.\n' "$MOUNT_POINT"
df -h "$MOUNT_POINT"

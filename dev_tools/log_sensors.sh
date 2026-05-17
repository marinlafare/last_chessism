#!/usr/bin/env bash
set -euo pipefail

# Log sensors output every 2 seconds with timestamps.
# Stop with Ctrl+C.
OUT_FILE="${1:-sensors_log.txt}"

while true; do
  date '+%Y-%m-%d %H:%M:%S' >> "$OUT_FILE"
  sensors >> "$OUT_FILE"
  echo '----' >> "$OUT_FILE"
  sleep 2
done

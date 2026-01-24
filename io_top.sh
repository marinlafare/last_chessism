#!/usr/bin/env bash
set -euo pipefail

interval="${1:-5}"
topn="${2:-15}"

tmp1="$(mktemp)"
tmp2="$(mktemp)"

snapshot() {
  for pid in /proc/[0-9]*; do
    io="${pid}/io"
    if [ -r "$io" ]; then
      rb=$(awk '/^read_bytes:/ {print $2}' "$io" 2>/dev/null || echo 0)
      wb=$(awk '/^write_bytes:/ {print $2}' "$io" 2>/dev/null || echo 0)
      comm=$(cat "${pid}/comm" 2>/dev/null || echo "?")
      echo "${pid##*/} ${comm} ${rb:-0} ${wb:-0}"
    fi
  done
}

snapshot > "$tmp1"
sleep "$interval"
snapshot > "$tmp2"

echo "PID COMM dr_bytes dw_bytes"
awk 'NR==FNR {rb[$1]=$3; wb[$1]=$4; next}
     {pid=$1; dr=$3-(rb[pid]+0); dw=$4-(wb[pid]+0);
      if (dr<0) dr=0; if (dw<0) dw=0;
      printf "%s %s %d %d\n", pid, $2, dr, dw
     }' "$tmp1" "$tmp2" \
  | sort -k4 -n \
  | tail -n "$topn"

rm -f "$tmp1" "$tmp2"

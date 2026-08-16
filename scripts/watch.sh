#!/usr/bin/env bash
#
# Read-only live view of the running overnight job. Safe to start and stop
# at any time; it only reads files and curls the metrics endpoint.
#
#   bash scripts/watch.sh
#
# Ctrl+C to leave. Stopping this does not affect the run.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
METRICS="http://localhost:9100/metrics"
INTERVAL="${INTERVAL:-5}"

RUN_DIR="$(ls -dt "$ROOT"/runs/*/ 2>/dev/null | head -1)"
[ -z "$RUN_DIR" ] && { echo "no run directory under $ROOT/runs"; exit 1; }

PID="$(cat "$RUN_DIR/pid" 2>/dev/null)"
STARTED="$(cat "$RUN_DIR/started.txt" 2>/dev/null)"

get() { awk -v k="$1" '$1==k {print $2}' /tmp/.watch-metrics 2>/dev/null; }

prev_decoded=""
prev_time=""

while true; do
  curl -s --max-time 3 "$METRICS" > /tmp/.watch-metrics 2>/dev/null

  decoded="$(get indexer_events_decoded_total)"
  lag="$(get indexer_commit_lag_slots)"
  errors="$(get indexer_db_write_errors_total)"
  dead="$(get indexer_dead_letters_total)"
  gaps="$(get indexer_gaps_detected_total)"
  dropped="$(get indexer_lookups_dropped_total)"
  failed="$(get indexer_skipped_failed_total)"

  now="$(date +%s)"
  rate="-"
  if [ -n "${prev_decoded:-}" ] && [ -n "${decoded:-}" ] && [ "$now" -gt "${prev_time:-$now}" ]; then
    rate=$(( (decoded - prev_decoded) / (now - prev_time) ))
  fi
  prev_decoded="${decoded:-}"
  prev_time="$now"

  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    alive="RUNNING  pid $PID"
    rss_kb="$(ps -o rss= -p "$PID" | tr -d ' ')"
    rss_mb=$(( ${rss_kb:-0} / 1024 ))
  else
    alive="NOT RUNNING"
    rss_mb="-"
  fi

  disk="$(df -g / | awk 'NR==2 {print $4}')"

  clear
  echo "  $alive"
  echo "  started   $STARTED"
  echo "  run dir   $RUN_DIR"
  echo
  printf "  %-14s %s MB\n"     "memory"    "$rss_mb"
  printf "  %-14s %s GB free\n" "disk"     "$disk"
  printf "  %-14s %s  (%s/s)\n" "decoded"  "${decoded:-?}" "$rate"
  printf "  %-14s %s slots\n"   "commit lag" "${lag:-?}"
  echo
  printf "  %-14s %s\n" "db errors"    "${errors:-?}"
  printf "  %-14s %s\n" "dead letters" "${dead:-?}"
  printf "  %-14s %s\n" "gaps"         "${gaps:-?}"
  printf "  %-14s %s\n" "lookups lost" "${dropped:-?}"
  printf "  %-14s %s\n" "failed txns"  "${failed:-?}"
  echo
  echo "  last watchdog line:"
  echo "  $(tail -1 "$RUN_DIR/watchdog.log" 2>/dev/null)"
  echo
  echo "  refreshing every ${INTERVAL}s, Ctrl+C to leave (run is unaffected)"

  sleep "$INTERVAL"
done

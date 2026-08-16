#!/usr/bin/env bash
#
# Runs the indexer unattended and stops it if anything looks dangerous.
# Everything for one run lands in runs/<timestamp>/.
#
#   ./scripts/overnight.sh
#
# Override any limit on the command line:
#   MAX_HOURS=2 MIN_DISK_GB=20 ./scripts/overnight.sh

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/solana-realtime-indexer"
RUN_DIR="$ROOT/runs/$(date +%Y%m%d-%H%M%S)"
METRICS="http://localhost:9100/metrics"

MAX_RSS_KB="${MAX_RSS_KB:-4000000}"
MIN_DISK_GB="${MIN_DISK_GB:-10}"
MAX_HOURS="${MAX_HOURS:-12}"
DB_PORT="${DB_PORT:-5433}"

say() { echo "$(date '+%F %T') $*"; }

fail() { say "PREFLIGHT FAILED: $*"; exit 1; }

[ -x "$BIN" ] || fail "no release binary at $BIN. Run: cargo build --release"
[ -f "$ROOT/.env" ] || fail "no .env in $ROOT"

grep -q '^DATABASE_URL=postgres' "$ROOT/.env" || fail "DATABASE_URL missing or malformed in .env"
grep -qE "^YELLOWSTONE_X_TOKEN=.+" "$ROOT/.env" || fail "YELLOWSTONE_X_TOKEN is empty in .env"

nc -z localhost "$DB_PORT" 2>/dev/null || fail "nothing listening on localhost:$DB_PORT. Run: docker compose start"

if pgrep -f 'release/solana-realtime-indexer' >/dev/null; then
  fail "an indexer is already running. Stop it first: pkill -f solana-realtime-indexer"
fi

free_gb="$(df -g / | awk 'NR==2 {print $4}')"
[ "$free_gb" -ge "$MIN_DISK_GB" ] || fail "only ${free_gb}GB free, need at least ${MIN_DISK_GB}GB"

mkdir -p "$RUN_DIR"
say "run directory: $RUN_DIR"
say "limits: max ${MAX_HOURS}h, stop under ${MIN_DISK_GB}GB disk, stop over ${MAX_RSS_KB}KB memory"
say "disk free at start: ${free_gb}GB"

PIPE="$RUN_DIR/pipe"
mkfifo "$PIPE"
grep --line-buffered -v '^pumpswap ' < "$PIPE" > "$RUN_DIR/run.log" &
FILTER_PID=$!

"$BIN" live > "$PIPE" 2>&1 &
BIN_PID=$!
echo "$BIN_PID" > "$RUN_DIR/pid"
say "indexer started, pid $BIN_PID"

STOPPED=""

stop_run() {
  reason="$1"
  [ -n "$STOPPED" ] && return
  STOPPED="yes"
  say "STOPPING: $reason"
  echo "$reason" > "$RUN_DIR/stop-reason.txt"

  curl -s --max-time 5 "$METRICS" > "$RUN_DIR/end-metrics.txt" 2>/dev/null

  kill -INT "$BIN_PID" 2>/dev/null
  for _ in $(seq 1 90); do
    kill -0 "$BIN_PID" 2>/dev/null || break
    sleep 1
  done
  kill -0 "$BIN_PID" 2>/dev/null && { say "did not exit in 90s, forcing"; kill -9 "$BIN_PID" 2>/dev/null; }

  sleep 1
  kill "$FILTER_PID" 2>/dev/null
  rm -f "$PIPE"

  date > "$RUN_DIR/ended.txt"
  say "done. summary in $RUN_DIR"
  exit 0
}

trap 'stop_run "interrupted by hand"' INT TERM

date > "$RUN_DIR/started.txt"
for _ in $(seq 1 30); do
  if curl -s --max-time 2 "$METRICS" > "$RUN_DIR/start-metrics.txt" 2>/dev/null; then
    [ -s "$RUN_DIR/start-metrics.txt" ] && break
  fi
  sleep 2
done

if [ -s "$RUN_DIR/start-metrics.txt" ]; then
  say "metrics endpoint answering"
else
  say "WARNING: metrics endpoint never answered on :9100"
fi

STARTED_AT="$(date +%s)"
WATCH_LOG="$RUN_DIR/watchdog.log"

while true; do
  kill -0 "$BIN_PID" 2>/dev/null || {
    say "indexer exited on its own"
    echo "indexer exited on its own" > "$RUN_DIR/stop-reason.txt"
    cp "$RUN_DIR/last-metrics.txt" "$RUN_DIR/end-metrics.txt" 2>/dev/null
    date > "$RUN_DIR/ended.txt"
    rm -f "$PIPE"
    exit 1
  }

  rss="$(ps -o rss= -p "$BIN_PID" | tr -d ' ')"
  [ -z "$rss" ] && { sleep 60; continue; }
  disk="$(df -g / | awk 'NR==2 {print $4}')"
  elapsed=$(( $(date +%s) - STARTED_AT ))
  hours=$(( elapsed / 3600 ))

  curl -s --max-time 3 "$METRICS" > "$RUN_DIR/last-metrics.txt.tmp" 2>/dev/null \
    && mv "$RUN_DIR/last-metrics.txt.tmp" "$RUN_DIR/last-metrics.txt"

  lag="$(awk '/^indexer_commit_lag_slots /{print $2}' "$RUN_DIR/last-metrics.txt" 2>/dev/null)"
  decoded="$(awk '/^indexer_events_decoded_total /{print $2}' "$RUN_DIR/last-metrics.txt" 2>/dev/null)"

  echo "$(date '+%F %T') rss=${rss}KB disk=${disk}GB elapsed=${elapsed}s lag=${lag:-?} decoded=${decoded:-?}" >> "$WATCH_LOG"

  [ "$rss"  -gt "$MAX_RSS_KB"  ] && stop_run "memory: ${rss}KB above ${MAX_RSS_KB}KB limit"
  [ "$disk" -lt "$MIN_DISK_GB" ] && stop_run "disk: ${disk}GB free, below ${MIN_DISK_GB}GB limit"
  [ "$hours" -ge "$MAX_HOURS"  ] && stop_run "reached ${MAX_HOURS} hours, planned end"

  sleep 60
done

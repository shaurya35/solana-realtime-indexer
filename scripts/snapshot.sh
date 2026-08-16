#!/usr/bin/env bash
#
# Periodically saves the full metrics text into the current run directory.
# Insurance against losing Prometheus, which has no volume and holds the only
# time series of the histograms.
#
#   bash scripts/snapshot.sh
#
# Read-only with respect to the run. Ctrl+C to stop; the run is unaffected.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
METRICS="http://localhost:9100/metrics"
EVERY="${EVERY:-900}"

RUN_DIR="$(ls -dt "$ROOT"/runs/*/ 2>/dev/null | head -1)"
[ -z "$RUN_DIR" ] && { echo "no run directory under $ROOT/runs"; exit 1; }

SNAPS="${RUN_DIR}snapshots"
mkdir -p "$SNAPS"
echo "saving a full metrics dump to $SNAPS every ${EVERY}s"
echo "Ctrl+C to stop, the run is not affected"

while true; do
  stamp="$(date +%Y%m%d-%H%M%S)"
  if curl -s --max-time 5 "$METRICS" > "$SNAPS/$stamp.txt" 2>/dev/null && [ -s "$SNAPS/$stamp.txt" ]; then
    echo "$(date '+%F %T') saved $stamp.txt"
  else
    rm -f "$SNAPS/$stamp.txt"
    echo "$(date '+%F %T') metrics endpoint did not answer"
  fi
  sleep "$EVERY"
done

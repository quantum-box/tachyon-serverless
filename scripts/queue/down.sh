#!/usr/bin/env bash
# scripts/queue/down.sh - stop the local NATS JetStream queue (PLT-4638).
#
# Usage:
#   scripts/queue/down.sh           # SIGTERM, wait up to 15 s (graceful: flushes and closes the store)
#   scripts/queue/down.sh --kill    # SIGKILL at once (crash simulation; the store must recover)
#   scripts/queue/down.sh --purge   # stop, then delete the state directory (messages, password)
#
# Environment: QUEUE_STATE_DIR (scripts/queue/lib.sh).
set -euo pipefail
# shellcheck source=scripts/queue/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

signal=TERM
purge=false
for arg in "$@"; do
  case "$arg" in
    --kill) signal=KILL ;;
    --purge) purge=true ;;
    -h | --help) sed -n '2,9p' "$0"; exit 0 ;;
    *) queue_die "unknown argument: $arg" ;;
  esac
done

if pid="$(queue_pid)"; then
  kill "-$signal" "$pid"
  deadline=$((SECONDS + 15))
  while kill -0 "$pid" 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || { kill -KILL "$pid" 2>/dev/null || true; break; }
    sleep 0.1
  done
  queue_log "nats-server (pid $pid) stopped with SIG$signal"
else
  queue_log "nats-server is not running"
fi
rm -f "$QUEUE_STATE_DIR/nats-server.pid"

if [ "$purge" = true ]; then
  rm -rf "${QUEUE_STATE_DIR:?}"
  queue_log "removed $QUEUE_STATE_DIR"
fi

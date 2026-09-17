#!/usr/bin/env bash
# scripts/queue/up.sh - start the local NATS JetStream queue as a plain process (PLT-4638).
#
# Downloads the pinned, checksum-verified nats-server (deploy/nats/versions.env), renders
# deploy/nats/nats-server.conf + a generated user password into the state directory and starts
# the server in the background. Idempotent: a running server is left alone. The stream itself is
# created by the gateway / adapter at startup (stream as code), not here.
#
# Usage:
#   scripts/queue/up.sh            # prints `export TACHYON_NATS_*=...` lines on stdout
#   eval "$(scripts/queue/up.sh)"
#
# Environment: QUEUE_STATE_DIR, QUEUE_PORT, QUEUE_HTTP_PORT, QUEUE_BIN_DIR (scripts/queue/lib.sh).
#
# This is ONE process on ONE host with its store on local disk. It is not HA and not replicated.
set -euo pipefail
# shellcheck source=scripts/queue/lib.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

case "${1:-}" in
  -h | --help) sed -n '2,15p' "$0"; exit 0 ;;
  "") ;;
  *) queue_die "unknown argument: $1" ;;
esac

if pid="$(queue_pid)"; then
  queue_log "nats-server already running (pid $pid)"
  queue_env
  exit 0
fi

bin="$(queue_binary)"
queue_render
"$bin" -c "$QUEUE_STATE_DIR/nats-server.conf" -t >/dev/null 2>&1 \
  || { "$bin" -c "$QUEUE_STATE_DIR/nats-server.conf" -t || true; queue_die "configuration test failed"; }

log="$QUEUE_STATE_DIR/nats-server.log"
nohup "$bin" -c "$QUEUE_STATE_DIR/nats-server.conf" >>"$log" 2>&1 &
echo $! >"$QUEUE_STATE_DIR/nats-server.pid"
if ! queue_wait_ready 30; then
  tail -n 40 "$log" >&2 || true
  queue_die "nats-server did not become ready (log: $log)"
fi
queue_log "nats-server $NATS_SERVER_VERSION ready on 127.0.0.1:$QUEUE_PORT (pid $(cat "$QUEUE_STATE_DIR/nats-server.pid"), store $QUEUE_STATE_DIR/jetstream)"
queue_env

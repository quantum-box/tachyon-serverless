#!/usr/bin/env bash
# scripts/queue/verify.sh - verify the local durable queue and object store end to end (PLT-4638).
#
# Starts a fresh nats-server (pinned, checksum-verified) in its own state directory and checks:
#   1. auth:      an anonymous client and a wrong password are refused by the server
#   2. restart:   publish N, ack some, leave some in flight, kill -9 the server, restart it on the
#                 same store: every unacked message (including the in-flight ones) is delivered,
#                 acked ones are not, and the dedup window still recognises the ids
#   3. capacity:  a stream at max_msgs refuses further publishes (discard new -> queue_full) and
#                 keeps what it has
#   4. max_age:   messages older than max_age are removed, acked or not
#   5. contract:  the EventQueue contract suite against JetStream (TACHYON_NATS_REQUIRED=1)
#   6. objects:   object store / GC / SQLite queue tests (restart, tamper, tenant crossing, size /
#                 quota, TTL GC never deleting referenced objects)
# and writes key=value results plus logs to the evidence directory.
#
# Usage:
#   scripts/queue/verify.sh [--evidence DIR] [--count N]
#
# Exit codes: 0 every check passed, 1 a check failed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE=""
COUNT=100

while [ $# -gt 0 ]; do
  case "$1" in
    --evidence) EVIDENCE="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    -h | --help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

EVIDENCE="${EVIDENCE:-$REPO_ROOT/target/queue/verify-$STAMP}"
mkdir -p "$EVIDENCE"
export QUEUE_STATE_DIR="$REPO_ROOT/target/queue/verify-state-$STAMP"
export QUEUE_PORT="${QUEUE_PORT:-24222}"
export QUEUE_HTTP_PORT="${QUEUE_HTTP_PORT:-28222}"
# shellcheck source=scripts/queue/lib.sh
. "$SCRIPT_DIR/lib.sh"

RESULTS="$EVIDENCE/results.txt"
: >"$RESULTS"
FAILED=0
# shellcheck disable=SC2317 # invoked by the EXIT trap
cleanup() {
  "$SCRIPT_DIR/down.sh" >/dev/null 2>&1 || true
  if [ -f "$QUEUE_STATE_DIR/nats-server.log" ]; then
    # the log carries no credentials; auth.conf / gateway.password are never copied
    cp "$QUEUE_STATE_DIR/nats-server.log" "$EVIDENCE/nats-server.log"
  fi
  rm -rf "${QUEUE_STATE_DIR:?}"
}
trap cleanup EXIT

record() { # name ok|FAIL detail
  printf '%-40s %s %s\n' "$1" "$2" "${3:-}" | tee -a "$RESULTS"
  if [ "$2" != ok ]; then FAILED=1; fi
}
kv() { # key file -> value
  sed -n "s/^$1=//p" "$2" | head -n 1
}

cd "$REPO_ROOT"
queue_log "building the probe"
cargo build -q -p tachyon-serverless-queue-nats --bin tachyon-queue-probe
PROBE="$REPO_ROOT/target/debug/tachyon-queue-probe"

eval "$("$SCRIPT_DIR/up.sh")"
{
  echo "nats_server_version=$NATS_SERVER_VERSION"
  echo "platform=$(queue_platform)"
  echo "sha256=$(queue_expected_sha "$(queue_platform)")"
  echo "stamp=$STAMP"
  echo "host=$(uname -srm)"
  echo "count=$COUNT"
} >"$EVIDENCE/environment.txt"
# the rendered server config without the auth include's secrets (it has none)
sed "s|$REPO_ROOT|<repo>|g" "$QUEUE_STATE_DIR/nats-server.conf" >"$EVIDENCE/nats-server.conf.rendered"
for f in "$QUEUE_STATE_DIR"/*; do
  # shellcheck disable=SC2012 # one known path at a time; only the mode column is kept
  printf '%s %s\n' "$(ls -ld "$f" | awk '{print $1}')" "$(basename "$f")"
done >"$EVIDENCE/state-dir-modes.txt"

probe() { # stream prefix [extra args...] command [command args...]
  local stream="$1" prefix="$2"
  shift 2
  "$PROBE" --url "$TACHYON_NATS_URL" --user "$TACHYON_NATS_USER" \
    --password-file "$TACHYON_NATS_PASSWORD_FILE" \
    --stream "$stream" --subject-prefix "$prefix" "$@"
}

# --- 1. authentication -------------------------------------------------------------------
out="$EVIDENCE/1-anonymous.txt"
if "$PROBE" --url "$TACHYON_NATS_URL" anonymous >"$out" 2>&1; then
  record auth.anonymous_refused ok "$(kv reason "$out")"
else
  record auth.anonymous_refused FAIL "$(cat "$out")"
fi
wrong="$EVIDENCE/.wrong-password"
umask 077
echo "definitely-not-the-password" >"$wrong"
out="$EVIDENCE/1-wrong-password.txt"
if "$PROBE" --url "$TACHYON_NATS_URL" --user gateway --password-file "$wrong" stats >"$out" 2>&1; then
  record auth.wrong_password_refused FAIL "$(cat "$out")"
elif [ "$(kv error_code "$out")" = unauthorized ]; then
  record auth.wrong_password_refused ok "$(kv error "$out")"
else
  record auth.wrong_password_refused FAIL "$(cat "$out")"
fi
rm -f "$wrong"

# --- 2. persistence across kill -9 --------------------------------------------------------
S=TACHYON_VERIFY_RESTART P=tachyon-verify.restart
out="$EVIDENCE/2-publish.txt"
probe "$S" "$P" --ack-wait-ms 2000 publish --count "$COUNT" --id-prefix run >"$out"
record restart.published "$([ "$(kv published "$out")" = "$COUNT" ] && echo ok || echo FAIL)" "published=$(kv published "$out")"
probe "$S" "$P" --ack-wait-ms 2000 consume --max 10 --wait-ms 2000 >"$EVIDENCE/2-consume-acked.txt"
probe "$S" "$P" --ack-wait-ms 2000 consume --max 10 --wait-ms 2000 --no-ack >"$EVIDENCE/2-consume-inflight.txt"
probe "$S" "$P" --ack-wait-ms 2000 stats >"$EVIDENCE/2-stats-before-kill.txt"
before="$(kv stream_messages "$EVIDENCE/2-stats-before-kill.txt")"
expected=$((COUNT - 10))
record restart.stored_before_kill "$([ "$before" = "$expected" ] && echo ok || echo FAIL)" \
  "stream_messages=$before ack_pending=$(kv ack_pending "$EVIDENCE/2-stats-before-kill.txt")"

pid="$(queue_pid)"
"$SCRIPT_DIR/down.sh" --kill
if kill -0 "$pid" 2>/dev/null; then
  record restart.killed FAIL "pid $pid still alive"
else
  record restart.killed ok "SIGKILL pid $pid"
fi
eval "$("$SCRIPT_DIR/up.sh")"
record restart.restarted ok "pid $(queue_pid)"
probe "$S" "$P" --ack-wait-ms 2000 stats >"$EVIDENCE/2-stats-after-restart.txt"
after="$(kv stream_messages "$EVIDENCE/2-stats-after-restart.txt")"
record restart.stored_after_restart "$([ "$after" = "$expected" ] && echo ok || echo FAIL)" "stream_messages=$after"
sleep 3 # past ack_wait: the in-flight deliveries from before the kill become deliverable again
out="$EVIDENCE/2-consume-after-restart.txt"
probe "$S" "$P" --ack-wait-ms 2000 consume --max "$COUNT" --wait-ms 3000 >"$out"
# The criterion is that every unacked message is delivered again. The redelivery
# counter of the messages that were in flight at the kill is informational only:
# JetStream's consumer state is not fsynced like the stream, so after SIGKILL the
# counter may come back reset (observed on Linux CI, kept on macOS). max_deliver
# therefore cannot bound retries across a crash; the ledger decides (ADR-0008).
record restart.all_unacked_delivered \
  "$([ "$(kv unique "$out")" = "$expected" ] && echo ok || echo FAIL)" \
  "received=$(kv received "$out") unique=$(kv unique "$out") redelivered=$(kv redelivered "$out") (redelivered is informational)"
probe "$S" "$P" --ack-wait-ms 2000 stats >"$EVIDENCE/2-stats-drained.txt"
record restart.drained "$([ "$(kv stream_messages "$EVIDENCE/2-stats-drained.txt")" = 0 ] && echo ok || echo FAIL)" \
  "stream_messages=$(kv stream_messages "$EVIDENCE/2-stats-drained.txt")"
out="$EVIDENCE/2-republish.txt"
probe "$S" "$P" --ack-wait-ms 2000 publish --count "$COUNT" --id-prefix run >"$out"
# Observed with nats-server v2.14.7: after a kill -9 the dedup window is rebuilt from the messages
# still in the stream, so the ids acked (removed) BEFORE the crash are accepted again; ids stored
# at crash time and ids seen after the restart stay deduplicated. The check pins exactly that, so a
# change in either direction is noticed. Dedup is a publisher-retry shield only; idempotency is the
# ledger's (docs/adr/0008).
record restart.dedup_after_restart \
  "$([ "$(kv duplicates "$out")" = "$expected" ] && [ "$(kv published "$out")" = 10 ] && echo ok || echo FAIL)" \
  "duplicates=$(kv duplicates "$out") accepted_again=$(kv published "$out") (the 10 acked before the kill)"

# --- 3. capacity boundary --------------------------------------------------------------------
S=TACHYON_VERIFY_CAPACITY P=tachyon-verify.capacity
out="$EVIDENCE/3-publish-over-capacity.txt"
probe "$S" "$P" --max-messages 5 publish --count 8 --id-prefix cap >"$out"
record capacity.discard_new_refuses \
  "$([ "$(kv published "$out")" = 5 ] && [ "$(kv queue_full "$out")" = 3 ] && echo ok || echo FAIL)" \
  "published=$(kv published "$out") queue_full=$(kv queue_full "$out") first_error=$(kv first_error "$out")"
out="$EVIDENCE/3-consume.txt"
probe "$S" "$P" --max-messages 5 consume --max 10 --wait-ms 1000 >"$out"
record capacity.existing_kept "$([ "$(kv unique "$out")" = 5 ] && echo ok || echo FAIL)" "unique=$(kv unique "$out")"
out="$EVIDENCE/3-oversize.txt"
if probe "$S" "$P" --max-messages 5 --max-message-bytes 1024 publish --count 1 --id-prefix big --payload-bytes 4096 >"$out"; then
  record capacity.message_too_large "$([ "$(kv published "$out")" = 0 ] && echo ok || echo FAIL)" "first_error=$(kv first_error "$out")"
else
  record capacity.message_too_large FAIL "$(cat "$out")"
fi

# --- 4. max_age retention ----------------------------------------------------------------------
S=TACHYON_VERIFY_AGE P=tachyon-verify.age
probe "$S" "$P" --max-age-secs 2 --duplicate-window-secs 1 publish --count 3 --id-prefix age >"$EVIDENCE/4-publish.txt"
probe "$S" "$P" --max-age-secs 2 --duplicate-window-secs 1 stats >"$EVIDENCE/4-stats-before.txt"
sleep 4
probe "$S" "$P" --max-age-secs 2 --duplicate-window-secs 1 stats >"$EVIDENCE/4-stats-after.txt"
record retention.max_age_expires \
  "$([ "$(kv stream_messages "$EVIDENCE/4-stats-before.txt")" = 3 ] && [ "$(kv stream_messages "$EVIDENCE/4-stats-after.txt")" = 0 ] && echo ok || echo FAIL)" \
  "before=$(kv stream_messages "$EVIDENCE/4-stats-before.txt") after=$(kv stream_messages "$EVIDENCE/4-stats-after.txt")"

# --- 5. JetStream contract suite ----------------------------------------------------------------
out="$EVIDENCE/5-nats-contract.txt"
if TACHYON_NATS_REQUIRED=1 cargo test -q -p tachyon-serverless-queue-nats --lib -- --test-threads 1 >"$out" 2>&1; then
  record contract.nats "ok" "$(grep -E '^test result' "$out" | head -n 1)"
else
  record contract.nats FAIL "see $(basename "$out")"
fi

# --- 6. object store, GC and SQLite queue ------------------------------------------------------
out="$EVIDENCE/6-objects-and-sqlite-queue.txt"
if cargo test -p tachyon-serverless-application --lib -- durable:: repository::object_contract_tests >"$out" 2>&1; then
  record objects_and_sqlite_queue ok "$(grep -E '^test result' "$out" | head -n 1)"
else
  record objects_and_sqlite_queue FAIL "see $(basename "$out")"
fi

echo
if [ "$FAILED" = 0 ]; then
  echo "verify: all checks passed (evidence: $EVIDENCE)"
else
  echo "verify: FAILED (evidence: $EVIDENCE)" >&2
fi
exit "$FAILED"

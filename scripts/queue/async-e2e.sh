#!/usr/bin/env bash
# scripts/queue/async-e2e.sh - invokeAsync and the transactional outbox end to end (PLT-4639).
#
# Starts the pinned nats-server (scripts/queue/up.sh) in its own state directory and a gateway
# built with the test-only `failpoints` feature ([queue] nats, [objects] filesystem, process
# provider), deploys example-hello, and checks:
#   1. accept:    small and large (object-stored) inputs answer 202 with a status URL; resending
#                 with the same Idempotency-Key answers the same invocation; another input is 409
#   2. crashes:   the gateway is SIGKILLed by a failpoint (after the object put, after COMMIT before
#                 the 202, after claiming before the publish, after the broker ACK before the row is
#                 marked sent) and restarted on the same data_dir; the client retries with its key
#   3. outage:    nats-server is stopped; acceptances land in the outbox until it is full, then are
#                 refused (503 queue_unavailable); nats-server is restarted and the outbox drains
#   4. orphans:   the object stored before the SIGKILL of (2) is collected after the orphan grace
#   5. converge:  every accepted invocation reaches `queued` and has a message in JetStream whose
#                 envelope names it; no message names anything that was not accepted; duplicates
#                 of one message id are counted (at-least-once) but every id maps to one invocation
# and writes key=value results plus logs to the evidence directory.
#
# It lives in scripts/queue/ (not scripts/e2e/) because it touches no execution provider and must
# not require the KVM gate (docs/ci.md §3).
#
# Usage:
#   scripts/queue/async-e2e.sh [--evidence DIR]
#
# Environment: TSLS_SKIP_BUILD=1 skips cargo build. Exit 0 only when every check passed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --evidence) EVIDENCE="$2"; shift 2 ;;
    -h | --help) sed -n '2,27p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

EVIDENCE="${EVIDENCE:-$REPO_ROOT/target/queue/async-e2e-$STAMP}"
mkdir -p "$EVIDENCE"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-async.XXXXXX")"
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
export QUEUE_STATE_DIR="$WORK_DIR/nats"
QUEUE_PORT="$(free_port)"
QUEUE_HTTP_PORT="$(free_port)"
export QUEUE_PORT QUEUE_HTTP_PORT
# shellcheck source=scripts/queue/lib.sh
. "$SCRIPT_DIR/lib.sh"

GATEWAY_PORT="$(free_port)"
API="http://127.0.0.1:$GATEWAY_PORT"
TOKEN="async-e2e-token-a"
TENANT="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TOKEN_B="async-e2e-token-b"
TENANT_B="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
MAX_PENDING=12
ORPHAN_GRACE=6
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
PROBE="$REPO_ROOT/target/debug/tachyon-queue-probe"
BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
HELLO_BIN="$REPO_ROOT/target/debug/example-hello"
GATEWAY_LOG="$EVIDENCE/gateway.log"
ACCEPTED="$EVIDENCE/accepted.txt"
RESULTS="$EVIDENCE/results.txt"
: >"$RESULTS"
: >"$ACCEPTED"
: >"$GATEWAY_LOG"
FAILED=0
GATEWAY_PID=""

# shellcheck disable=SC2317 # invoked by the EXIT trap
cleanup() {
  set +e
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill -TERM "$GATEWAY_PID" 2>/dev/null
    wait "$GATEWAY_PID" 2>/dev/null
  fi
  "$SCRIPT_DIR/down.sh" >/dev/null 2>&1
  if [ -f "$QUEUE_STATE_DIR/nats-server.log" ]; then
    cp "$QUEUE_STATE_DIR/nats-server.log" "$EVIDENCE/nats-server.log"
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

log() { printf '[async-e2e] %s\n' "$*" >&2; }
record() { # name ok|FAIL detail
  printf '%-44s %s %s\n' "$1" "$2" "${3:-}" | tee -a "$RESULTS"
  if [ "$2" != ok ]; then FAILED=1; fi
}
pass() { record "$1" ok "${2:-}"; }
fail() { record "$1" FAIL "${2:-}"; }

# ---------------------------------------------------------------------------
# build, queue, configuration
# ---------------------------------------------------------------------------

cd "$REPO_ROOT"
if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  log "building the gateway with the test-only failpoints feature"
  cargo build -q -p tachyon-serverless-gateway --features failpoints
  cargo build -q -p tachyon-serverless-queue-nats --bin tachyon-queue-probe
  cargo build -q -p tachyon-serverless-runtime-bridge -p example-hello
fi
for b in "$GATEWAY_BIN" "$PROBE" "$BRIDGE_BIN" "$HELLO_BIN"; do
  [ -x "$b" ] || { echo "missing binary: $b" >&2; exit 1; }
done

eval "$("$SCRIPT_DIR/up.sh")"
probe() {
  "$PROBE" --url "$TACHYON_NATS_URL" --password-file "$TACHYON_NATS_PASSWORD_FILE" "$@"
}

mkdir -p "$WORK_DIR/data"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$WORK_DIR/objects.key"
umask 022
CONFIG="$WORK_DIR/gateway.toml"
cat >"$CONFIG" <<EOF
listen = "127.0.0.1:$GATEWAY_PORT"
profile = "dev"
data_dir = "$WORK_DIR/data"

[provider]
kind = "process"

[provider.process]
bridge_binary = "$BRIDGE_BIN"
workdir = "$WORK_DIR/data/process"

[dispatcher]
instance = "async-e2e"

[[identity.tokens]]
token = "$TOKEN"
tenant_id = "$TENANT"
subject = "async-e2e"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B"
subject = "async-e2e-b"
roles = ["deploy", "invoke"]

[queue]
backend = "nats"

[queue.nats]
url = "$TACHYON_NATS_URL"
user = "$TACHYON_NATS_USER"
password_file = "$TACHYON_NATS_PASSWORD_FILE"
connect_timeout_ms = 2000
request_timeout_ms = 2000

[objects]
backend = "filesystem"
key_file = "$WORK_DIR/objects.key"
orphan_grace_seconds = $ORPHAN_GRACE
gc_interval_seconds = 2

[invoke_async]
inline_input_max_bytes = 1024
max_pending_events = $MAX_PENDING
claim_ttl_seconds = 3
retry_initial_ms = 200
retry_max_ms = 1000
publish_interval_ms = 100
EOF

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

# start_gateway [FAILPOINTS]
start_gateway() {
  printf '\n===== gateway start (failpoints: %s) =====\n' "${1:-none}" >>"$GATEWAY_LOG"
  LOG_FORMAT=json TSLS_FAILPOINTS="${1:-}" "$GATEWAY_BIN" --config "$CONFIG" >>"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  local i
  for i in $(seq 1 120); do
    kill -0 "$GATEWAY_PID" 2>/dev/null || { echo "gateway exited during startup (attempt $i)" >&2; return 1; }
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$API/healthz" || true)" = "200" ]; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

# wait_gateway_dead SECONDS -> 0 when the gateway exited (a failpoint killed it)
wait_gateway_dead() {
  local deadline=$((SECONDS + $1))
  while kill -0 "$GATEWAY_PID" 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.1
  done
  wait "$GATEWAY_PID" 2>/dev/null || true
  GATEWAY_PID=""
  return 0
}

stop_gateway() {
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill -TERM "$GATEWAY_PID"
    wait "$GATEWAY_PID" 2>/dev/null || true
  fi
  GATEWAY_PID=""
}

HTTP_CODE=""
HTTP_BODY=""
# http METHOD PATH TOKEN [BODY_FILE] [IDEMPOTENCY_KEY]
http() {
  local method="$1" path="$2" token="$3" body="${4:-}" key="${5:-}" out
  out="$WORK_DIR/http.out"
  : >"$out"
  local args=(-s -o "$out" -w '%{http_code}' --max-time 30 -X "$method" -H "authorization: Bearer $token")
  if [ -n "$body" ]; then args+=(-H 'content-type: application/json' --data-binary "@$body"); fi
  if [ -n "$key" ]; then args+=(-H "idempotency-key: $key"); fi
  HTTP_CODE="$(curl "${args[@]}" "$API$path" || true)"
  HTTP_BODY="$(cat "$out")"
}

payload() { # payload FILE N BYTES
  local file="$1"
  python3 -c 'import json,sys; print(json.dumps({"n": int(sys.argv[1]), "blob": "x" * int(sys.argv[2])}))' "$2" "$3" >"$file"
}

# accept N BYTES KEY -> sets HTTP_CODE / HTTP_BODY; appends the id to accepted.txt on 202
accept() {
  local f="$WORK_DIR/payload-$1.json"
  payload "$f" "$1" "$2"
  http POST "/v1/functions/$FUNCTION_ID:invokeAsync" "$TOKEN" "$f" "$3"
  if [ "$HTTP_CODE" = 202 ]; then
    printf '%s\n' "$HTTP_BODY" | jq -r .invocation_id >>"$ACCEPTED"
  fi
}

object_files() {
  find "$WORK_DIR/data/objects" -name '*.data' 2>/dev/null | wc -l | tr -d ' '
}

# wait_all_queued SECONDS -> 0 when every accepted invocation reads `queued`
wait_all_queued() {
  local deadline=$((SECONDS + $1)) pending id
  while :; do
    pending=0
    while IFS= read -r id; do
      http GET "/v1/invocations/$id" "$TOKEN"
      [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = queued ] || pending=$((pending + 1))
    done < <(sort -u "$ACCEPTED")
    [ "$pending" = 0 ] && return 0
    [ "$SECONDS" -lt "$deadline" ] || { log "$pending invocations not queued"; return 1; }
    sleep 0.5
  done
}

# ---------------------------------------------------------------------------
# 1. deploy and accept
# ---------------------------------------------------------------------------

start_gateway ""
printf '{"name":"async-hello","description":"PLT-4639 async e2e"}' >"$WORK_DIR/fn.json"
http POST /v1/functions "$TOKEN" "$WORK_DIR/fn.json"
FUNCTION_ID="$(printf '%s' "$HTTP_BODY" | jq -r .id)"
DIGEST="$(curl -s --max-time 30 -X POST -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/octet-stream' --data-binary "@$HELLO_BIN" "$API/v1/artifacts" | jq -r .digest)"
case "$(uname -m)" in arm64 | aarch64) ARCH=aarch64 ;; *) ARCH=x86_64 ;; esac
printf '{"artifact":{"kind":"binary","digest":"%s"},"architecture":"%s","publish_to_prod":true}' \
  "$DIGEST" "$ARCH" >"$WORK_DIR/rev.json"
http POST "/v1/functions/$FUNCTION_ID/revisions" "$TOKEN" "$WORK_DIR/rev.json"
REVISION_ID="$(printf '%s' "$HTTP_BODY" | jq -r .id)"
for _ in $(seq 1 100); do
  http GET "/v1/functions/$FUNCTION_ID/revisions/$REVISION_ID" "$TOKEN"
  [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = ready ] && break
  sleep 0.1
done
if [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = ready ]; then pass deploy.revision_ready "$REVISION_ID"; else fail deploy.revision_ready "$REVISION_ID"; fi

inline=0; objects=0; bad=0
for n in $(seq 1 8); do
  if [ $((n % 2)) = 0 ]; then accept "$n" 4096 "key-$n"; else accept "$n" 16 "key-$n"; fi
  if [ "$HTTP_CODE" != 202 ]; then bad=$((bad + 1)); continue; fi
  case "$(printf '%s' "$HTTP_BODY" | jq -r .input_storage)" in
    inline) inline=$((inline + 1)) ;;
    object) objects=$((objects + 1)) ;;
  esac
done
if [ "$bad" = 0 ]; then pass accept.202 "refused=$bad inline=$inline object=$objects"; else fail accept.202 "refused=$bad inline=$inline object=$objects"; fi
if [ "$objects" = 4 ] && [ "$inline" = 4 ]; then pass accept.objects_used ""; else fail accept.objects_used ""; fi
first="$(head -n 1 "$ACCEPTED")"
http GET "/v1/invocations/$first" "$TOKEN"
if [ "$HTTP_CODE" = 200 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .mode)" = async ]; then pass accept.status_url_readable "$(printf '%s' "$HTTP_BODY" | jq -c '{status, revision_id}')"; else fail accept.status_url_readable "$(printf '%s' "$HTTP_BODY" | jq -c '{status, revision_id}')"; fi
http GET "/v1/invocations/$first" "$TOKEN_B"
if [ "$HTTP_CODE" = 404 ]; then pass tenant.status_404_for_other_tenant "code=$HTTP_CODE"; else fail tenant.status_404_for_other_tenant "code=$HTTP_CODE"; fi

dups=0
for n in 2 3 4; do
  before="$(sed -n "${n}p" "$ACCEPTED")"
  if [ $((n % 2)) = 0 ]; then bytes=4096; else bytes=16; fi
  accept "$n" "$bytes" "key-$n"
  # accept() appended the replayed id; it must be the same one
  [ "$(printf '%s' "$HTTP_BODY" | jq -r .invocation_id)" = "$before" ] \
    && [ "$(printf '%s' "$HTTP_BODY" | jq -r .replayed)" = true ] && dups=$((dups + 1))
done
if [ "$dups" = 3 ]; then pass idempotency.same_key_same_invocation "matched=$dups/3"; else fail idempotency.same_key_same_invocation "matched=$dups/3"; fi
accept 2 99 "key-2"
if [ "$HTTP_CODE" = 409 ]; then pass idempotency.other_input_409 "code=$HTTP_CODE"; else fail idempotency.other_input_409 "code=$HTTP_CODE"; fi
if wait_all_queued 30; then pass converge.phase1_queued ""; else fail converge.phase1_queued ""; fi

# ---------------------------------------------------------------------------
# 2. SIGKILL at each failpoint, restart, retry
# ---------------------------------------------------------------------------

objects_before_orphan="$(object_files)"

# 2a. after the object put, before the transaction: nothing recorded, an orphan object
stop_gateway
start_gateway "accept.after_object_put=kill"
accept 100 4096 "key-orphan"
if [ "$HTTP_CODE" = 000 ] || [ -z "$HTTP_CODE" ]; then pass crash.after_object_put.no_response "code=$HTTP_CODE"; else fail crash.after_object_put.no_response "code=$HTTP_CODE"; fi
if wait_gateway_dead 10; then pass crash.after_object_put.killed ""; else fail crash.after_object_put.killed ""; fi
start_gateway ""
if [ "$(object_files)" = $((objects_before_orphan + 1)) ]; then pass crash.after_object_put.orphan_on_disk "files=$(object_files)"; else fail crash.after_object_put.orphan_on_disk "files=$(object_files)"; fi

# 2b. after COMMIT, before the 202: the retry with the same key answers the committed invocation
stop_gateway
start_gateway "accept.after_commit=kill"
accept 101 4096 "key-commit"
if [ "$HTTP_CODE" != 202 ]; then pass crash.after_commit.no_response "code=$HTTP_CODE"; else fail crash.after_commit.no_response "code=$HTTP_CODE"; fi
if wait_gateway_dead 10; then pass crash.after_commit.killed ""; else fail crash.after_commit.killed ""; fi
start_gateway ""
accept 101 4096 "key-commit"
if [ "$HTTP_CODE" = 202 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .replayed)" = true ]; then pass crash.after_commit.retry_replayed "code=$HTTP_CODE"; else fail crash.after_commit.retry_replayed "code=$HTTP_CODE"; fi

# 2c. claimed, before the publish
stop_gateway
start_gateway "outbox.before_publish=kill"
accept 102 16 "key-before-publish"
if [ "$HTTP_CODE" = 202 ]; then pass crash.before_publish.accepted "code=$HTTP_CODE"; else fail crash.before_publish.accepted "code=$HTTP_CODE"; fi
if wait_gateway_dead 10; then pass crash.before_publish.killed ""; else fail crash.before_publish.killed ""; fi
start_gateway ""

# 2d. after the broker's ACK, before the row is marked sent (republish -> dedup)
stop_gateway
start_gateway "outbox.after_publish=kill"
accept 103 4096 "key-after-publish"
if [ "$HTTP_CODE" = 202 ]; then pass crash.after_publish.accepted "code=$HTTP_CODE"; else fail crash.after_publish.accepted "code=$HTTP_CODE"; fi
if wait_gateway_dead 10; then pass crash.after_publish.killed ""; else fail crash.after_publish.killed ""; fi
start_gateway ""
if wait_all_queued 30; then pass converge.after_crashes_queued ""; else fail converge.after_crashes_queued ""; fi
# The row published before the SIGKILL was published again after its claim expired; inside the
# duplicate window JetStream answers `duplicate` and stores nothing.
if grep -q '"duplicates":1' "$GATEWAY_LOG" || grep -q 'already had' "$GATEWAY_LOG"; then
  pass crash.after_publish.republished_as_duplicate "$(grep -c 'already had' "$GATEWAY_LOG") log line(s)"
else
  fail crash.after_publish.republished_as_duplicate "no re-publish recorded"
fi

# ---------------------------------------------------------------------------
# 3. queue outage
# ---------------------------------------------------------------------------

"$SCRIPT_DIR/down.sh" >/dev/null 2>&1
accepted_during=0; refused=0; reason=""
for n in $(seq 200 $((200 + MAX_PENDING + 8))); do
  accept "$n" 16 "key-outage-$n"
  case "$HTTP_CODE" in
    202) accepted_during=$((accepted_during + 1)) ;;
    503) refused=$((refused + 1)); reason="$(printf '%s' "$HTTP_BODY" | jq -r .error.reason)" ;;
    *) log "outage: unexpected $HTTP_CODE $HTTP_BODY" ;;
  esac
  # Let the publisher fail its first publish against the stopped server, so the gateway knows
  # the queue is unavailable before the outbox fills.
  if [ "$n" = 200 ]; then sleep 4; else sleep 0.1; fi
done
record outage.outbox_pending_while_down ok "accepted=$accepted_during refused=$refused"
if [ "$accepted_during" -ge 1 ] && [ "$accepted_during" -le "$MAX_PENDING" ]; then pass outage.accepted_into_outbox "accepted=$accepted_during"; else fail outage.accepted_into_outbox "accepted=$accepted_during"; fi
if [ "$refused" -ge 1 ] && [ "$reason" = queue_unavailable ]; then pass outage.refused_queue_unavailable "refused=$refused reason=$reason"; else fail outage.refused_queue_unavailable "refused=$refused reason=$reason"; fi
eval "$("$SCRIPT_DIR/up.sh")"
if wait_all_queued 60; then pass converge.after_outage_queued ""; else fail converge.after_outage_queued ""; fi
accept 300 16 "key-after-outage"
if [ "$HTTP_CODE" = 202 ]; then pass outage.accepting_again "code=$HTTP_CODE"; else fail outage.accepting_again "code=$HTTP_CODE"; fi
if wait_all_queued 30; then pass converge.final_queued ""; else fail converge.final_queued ""; fi

# ---------------------------------------------------------------------------
# 4. orphan GC
# ---------------------------------------------------------------------------

sleep $((ORPHAN_GRACE + 4))
expected_objects=0
while IFS= read -r id; do
  http GET "/v1/invocations/$id" "$TOKEN"
  if [ "$(printf '%s' "$HTTP_BODY" | jq -r .input_size_bytes)" -gt 1024 ]; then
    expected_objects=$((expected_objects + 1))
  fi
done < <(sort -u "$ACCEPTED")
if [ "$(object_files)" = "$expected_objects" ]; then pass gc.orphan_collected_referenced_kept "files=$(object_files) referenced=$expected_objects"; else fail gc.orphan_collected_referenced_kept "files=$(object_files) referenced=$expected_objects"; fi

# ---------------------------------------------------------------------------
# 5. exactly one logical message per accepted invocation
# ---------------------------------------------------------------------------

stop_gateway
sort -u "$ACCEPTED" >"$WORK_DIR/accepted.sorted"
accepted_count="$(wc -l <"$WORK_DIR/accepted.sorted" | tr -d ' ')"
probe consume --max 100000 --wait-ms 3000 --print-ids >"$EVIDENCE/consume.txt"
grep '^message=' "$EVIDENCE/consume.txt" | sed 's/^message=\([^ ]*\).*/\1/' | sort >"$WORK_DIR/messages.all"
sort -u "$WORK_DIR/messages.all" >"$WORK_DIR/messages.unique"
received="$(wc -l <"$WORK_DIR/messages.all" | tr -d ' ')"
unique="$(wc -l <"$WORK_DIR/messages.unique" | tr -d ' ')"
missing="$(comm -23 "$WORK_DIR/accepted.sorted" "$WORK_DIR/messages.unique" | wc -l | tr -d ' ')"
unexpected="$(comm -13 "$WORK_DIR/accepted.sorted" "$WORK_DIR/messages.unique" | wc -l | tr -d ' ')"
bad_envelopes="$(grep '^message=' "$EVIDENCE/consume.txt" | grep -c -v 'envelope=ok' || true)"
if [ "$missing" = 0 ]; then pass converge.no_accepted_invocation_lost "accepted=$accepted_count missing=$missing"; else fail converge.no_accepted_invocation_lost "accepted=$accepted_count missing=$missing"; fi
if [ "$unexpected" = 0 ]; then pass converge.nothing_unaccepted_published "unexpected=$unexpected"; else fail converge.nothing_unaccepted_published "unexpected=$unexpected"; fi
if [ "$bad_envelopes" = 0 ]; then pass converge.envelopes_name_their_invocation "bad=$bad_envelopes"; else fail converge.envelopes_name_their_invocation "bad=$bad_envelopes"; fi
record converge.physical_messages ok "received=$received unique=$unique duplicates=$((received - unique)) (at-least-once; the ledger settles by invocation id)"

{
  echo "nats_server_version=$NATS_SERVER_VERSION"
  echo "platform=$(queue_platform)"
  echo "stamp=$STAMP"
  echo "host=$(uname -srm)"
  echo "git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "accepted=$accepted_count"
  echo "messages_received=$received"
  echo "messages_unique=$unique"
} >"$EVIDENCE/environment.txt"

if [ "$FAILED" = 0 ]; then
  log "PASS (evidence: $EVIDENCE)"
else
  log "FAIL (evidence: $EVIDENCE)"
fi
exit "$FAILED"

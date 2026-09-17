#!/usr/bin/env bash
# scripts/queue/async-dispatch-e2e.sh - the asynchronous dispatcher end to end (PLT-4640).
#
# Starts the pinned nats-server (scripts/queue/up.sh) and a gateway built with the test-only
# `failpoints` feature ([queue] nats, process provider), deploys examples/idempotent-async (a
# handler whose side effect is keyed by a business idempotency key) and checks:
#   1. retries:     an order that fails twice succeeds on its third attempt, with backoff; plain
#                   orders succeed on the first
#   2. dead ends:   an order that always fails ends in one `attempts_exhausted` dead letter; an
#                   oversized response (Runtime.ResponseTooLarge) is dead-lettered as `non_retryable`
#                   after one execution; an undecodable event published straight to JetStream is
#                   dead-lettered as `poison` and never executed
#   3. ACK lost:    the gateway is SIGKILLed after the terminal commit, before the ACK; after the
#                   restart JetStream redelivers and the dispatcher acks it without running again
#   4. mid-run:     the gateway is SIGKILLed after the handler's side effect, before the commit;
#                   after the restart the run is executed again, the idempotency key makes the
#                   second execution a no-op, and one terminal outcome is recorded
#   5. redrive:     without the `redrive` role 403, from another tenant 404, with it 202 with an
#                   audit record; the new invocation runs on the pinned revision and succeeds;
#                   a second redrive of the same entry is 409
#   6. converge:    every accepted invocation is terminal; at most one dead letter per invocation;
#                   no invocation has two terminal records; each side effect exists exactly once
# and writes key=value results plus logs to the evidence directory.
#
# Usage:
#   scripts/queue/async-dispatch-e2e.sh [--evidence DIR]
#   sudo -n env PATH="$PATH" HOME="$HOME" TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 \
#     scripts/queue/async-dispatch-e2e.sh                     # Firecracker (Linux/KVM, root)
#
# Environment: TSLS_SKIP_BUILD=1 skips cargo build. TSLS_PROVIDER=process (default) | firecracker
# (scripts/kvm/provider-lib.sh): with firecracker every run executes in a jailed microVM, the warm
# pool is on, and the handler's idempotency store is the external HTTP store of
# scripts/queue/effects-netns.sh, reached through egress `restricted` (a microVM cannot see the host
# file system). Exit 0 only when every check passed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --evidence) EVIDENCE="$2"; shift 2 ;;
    -h | --help) sed -n '2,31p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

EVIDENCE="${EVIDENCE:-$REPO_ROOT/target/queue/async-dispatch-e2e-$STAMP}"
mkdir -p "$EVIDENCE"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-dispatch.XXXXXX")"
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
export QUEUE_STATE_DIR="$WORK_DIR/nats"
QUEUE_PORT="$(free_port)"
QUEUE_HTTP_PORT="$(free_port)"
export QUEUE_PORT QUEUE_HTTP_PORT
# shellcheck source=scripts/queue/lib.sh
. "$SCRIPT_DIR/lib.sh"
# shellcheck source=scripts/kvm/provider-lib.sh
. "$REPO_ROOT/scripts/kvm/provider-lib.sh"
provider_init "$REPO_ROOT"

GATEWAY_PORT="$(free_port)"
API="http://127.0.0.1:$GATEWAY_PORT"
TOKEN="dispatch-e2e-token-a"
TOKEN_REDRIVE="dispatch-e2e-token-a-redrive"
TENANT="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TOKEN_B="dispatch-e2e-token-b"
TENANT_B="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
# JetStream ack wait (TSLS_ACK_WAIT_SECONDS). It must be longer than one run: a delivery redelivered
# while its run still holds the claim is acknowledged as `Skipped("claimed")` (the ledger's claim
# and reaper own the retry), so step 3 would then see no redelivery after the restart. A microVM
# cold start takes several seconds on a nested host, hence 30 s with firecracker.
if provider_is_fc; then ACK_WAIT="${TSLS_ACK_WAIT_SECONDS:-30}"; else ACK_WAIT="${TSLS_ACK_WAIT_SECONDS:-4}"; fi
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
PROBE="$REPO_ROOT/target/debug/tachyon-queue-probe"
BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
SAMPLE_BIN="$GUEST_DIR/example-idempotent-async"
EFFECTS="$WORK_DIR/effects"
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
  if provider_is_fc; then
    cp "$(sed -n 's/^EFFECTS_STORE_LOG=//p' "$WORK_DIR/effects.env" 2>/dev/null)" "$EVIDENCE/effects-store.log" 2>/dev/null
    "$SCRIPT_DIR/effects-netns.sh" down >/dev/null 2>&1
    provider_leftovers >"$EVIDENCE/leftovers-after.txt" 2>&1
  fi
  if [ -f "$QUEUE_STATE_DIR/nats-server.log" ]; then
    cp "$QUEUE_STATE_DIR/nats-server.log" "$EVIDENCE/nats-server.log"
  fi
  if [ -f "$EFFECTS/executions.log" ]; then
    cp "$EFFECTS/executions.log" "$EVIDENCE/executions.log"
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

log() { printf '[dispatch-e2e] %s\n' "$*" >&2; }
record() { # name ok|FAIL detail
  printf '%-48s %s %s\n' "$1" "$2" "${3:-}" | tee -a "$RESULTS"
  if [ "$2" != ok ]; then FAILED=1; fi
}
pass() { record "$1" ok "${2:-}"; }
fail() { record "$1" FAIL "${2:-}"; }
check() { # check NAME DETAIL CONDITION...
  local name="$1" detail="$2"
  shift 2
  if "$@"; then pass "$name" "$detail"; else fail "$name" "$detail"; fi
}

# ---------------------------------------------------------------------------
# build, queue, configuration
# ---------------------------------------------------------------------------

cd "$REPO_ROOT"
if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  log "building the gateway with the test-only failpoints feature"
  cargo build -q -p tachyon-serverless-gateway --features failpoints
  cargo build -q -p tachyon-serverless-queue-nats --bin tachyon-queue-probe
  cargo build -q -p tachyon-serverless-runtime-bridge -p example-idempotent-async
  if provider_is_fc; then
    cargo build -q --release --target "$(uname -m)-unknown-linux-musl" -p example-idempotent-async
  fi
fi
for b in "$GATEWAY_BIN" "$PROBE" "$BRIDGE_BIN" "$SAMPLE_BIN"; do
  [ -x "$b" ] || { echo "missing binary: $b" >&2; exit 1; }
done

eval "$("$SCRIPT_DIR/up.sh")"
probe() {
  "$PROBE" --url "$TACHYON_NATS_URL" --password-file "$TACHYON_NATS_PASSWORD_FILE" \
    --ack-wait-ms $((ACK_WAIT * 1000)) --max-deliver 1000 "$@"
}

mkdir -p "$WORK_DIR/data" "$EFFECTS"
# Where the handler keeps its idempotency records (see the header).
REVISION_ENV="[[\"IDEMPOTENT_ASYNC_DIR\",\"$EFFECTS\"]]"
REVISION_EGRESS=""
INIT_TIMEOUT=10
POOL_TOML=""
if provider_is_fc; then
  "$SCRIPT_DIR/effects-netns.sh" down >/dev/null 2>&1 || true
  "$SCRIPT_DIR/effects-netns.sh" up "$EFFECTS" >"$WORK_DIR/effects.env"
  STORE_URL="$(sed -n 's/^IDEMPOTENT_ASYNC_URL=//p' "$WORK_DIR/effects.env")"
  STORE_ALLOW="$(sed -n 's/^EFFECTS_ALLOW=//p' "$WORK_DIR/effects.env")"
  REVISION_ENV="[[\"IDEMPOTENT_ASYNC_URL\",\"$STORE_URL\"]]"
  REVISION_EGRESS=",\"egress\":\"restricted\",\"egress_allow\":[{\"cidr\":\"${STORE_ALLOW%:*}\",\"ports\":[${STORE_ALLOW##*:}]}]"
  # A cold microVM boot takes seconds on a nested host; the pool keeps later runs warm.
  INIT_TIMEOUT=60
  POOL_TOML=$'[pool]\nenabled = true\nmax_idle_per_revision = 4\nidle_ttl_seconds = 300'
fi
CONFIG="$WORK_DIR/gateway.toml"
cat >"$CONFIG" <<EOF
listen = "127.0.0.1:$GATEWAY_PORT"
profile = "dev"
data_dir = "$WORK_DIR/data"

$(provider_toml "$WORK_DIR/data" "$BRIDGE_BIN")

$POOL_TOML

[limits]
max_response_bytes = 65536

[invoke]
inline_output_max_bytes = 16384

[dispatcher]
instance = "dispatch-e2e"

[[identity.tokens]]
token = "$TOKEN"
tenant_id = "$TENANT"
subject = "dispatch-e2e"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "$TOKEN_REDRIVE"
tenant_id = "$TENANT"
subject = "dispatch-e2e-oncall"
roles = ["invoke", "redrive"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B"
subject = "dispatch-e2e-b"
roles = ["deploy", "invoke", "redrive"]

[queue]
backend = "nats"

[queue.nats]
url = "$TACHYON_NATS_URL"
user = "$TACHYON_NATS_USER"
password_file = "$TACHYON_NATS_PASSWORD_FILE"
connect_timeout_ms = 2000
request_timeout_ms = 2000

[invoke_async]
inline_input_max_bytes = 4096
claim_ttl_seconds = 3
retry_initial_ms = 200
retry_max_ms = 1000
publish_interval_ms = 100

[async_dispatch]
workers = 2
ack_wait_seconds = $ACK_WAIT
max_deliver = 1000
fetch_wait_ms = 200
claim_ttl_seconds = 4
admission_wait_ms = 2000
max_attempts = 3
backoff_initial_ms = 300
backoff_max_ms = 1000
backoff_floor_ms = 100
retry_budget = 0
reaper_interval_seconds = 1
stall_timeout_seconds = 60
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
# shellcheck disable=SC2317 # invoked through check()
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
# http METHOD PATH TOKEN [BODY_FILE]
http() {
  local method="$1" path="$2" token="$3" body="${4:-}" out
  out="$WORK_DIR/http.out"
  : >"$out"
  local args=(-s -o "$out" -w '%{http_code}' --max-time 30 -X "$method" -H "authorization: Bearer $token")
  if [ -n "$body" ]; then args+=(-H 'content-type: application/json' --data-binary "@$body"); fi
  HTTP_CODE="$(curl "${args[@]}" "$API$path" || true)"
  HTTP_BODY="$(cat "$out")"
}

jqr() { printf '%s' "$HTTP_BODY" | jq -r "$1"; }

# accept ORDER_ID JSON_EXTRA -> prints the invocation id (empty on refusal)
accept() {
  local f="$WORK_DIR/payload-$1.json"
  printf '{"order_id":"%s"%s}' "$1" "${2:-}" >"$f"
  http POST "/v1/functions/$FUNCTION_ID:invokeAsync" "$TOKEN" "$f"
  if [ "$HTTP_CODE" = 202 ]; then
    local id
    id="$(jqr .invocation_id)"
    printf '%s %s\n' "$1" "$id" >>"$ACCEPTED"
    printf '%s\n' "$id"
  fi
}

# wait_terminal ID SECONDS -> prints the final status (or the last one seen)
wait_terminal() {
  local deadline=$((SECONDS + $2)) status=""
  while :; do
    http GET "/v1/invocations/$1" "$TOKEN"
    status="$(jqr .status)"
    case "$status" in
      succeeded | failed | cancelled | outcome_unknown) break ;;
    esac
    [ "$SECONDS" -lt "$deadline" ] || break
    sleep 0.3
  done
  printf '%s\n' "$status"
}

executions() { # executions ORDER_ID [KIND] -> lines in executions.log
  local n
  n="$(grep -c "^$1 .*${2:-}\$" "$EFFECTS/executions.log" 2>/dev/null || true)"
  printf '%s\n' "${n:-0}"
}

effects() { # effects ORDER_ID -> 1 when the side effect exists
  if [ -f "$EFFECTS/effects/$1.json" ]; then echo 1; else echo 0; fi
}

# ---------------------------------------------------------------------------
# 0. deploy
# ---------------------------------------------------------------------------

start_gateway ""
printf '{"name":"idempotent-async","description":"PLT-4640 dispatch e2e"}' >"$WORK_DIR/fn.json"
http POST /v1/functions "$TOKEN" "$WORK_DIR/fn.json"
FUNCTION_ID="$(jqr .id)"
DIGEST="$(curl -s --max-time 30 -X POST -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/octet-stream' --data-binary "@$SAMPLE_BIN" "$API/v1/artifacts" | jq -r .digest)"
case "$(uname -m)" in arm64 | aarch64) ARCH=aarch64 ;; *) ARCH=x86_64 ;; esac
printf '{"artifact":{"kind":"binary","digest":"%s"},"architecture":"%s","execution":{"timeout_seconds":10,"initialization_timeout_seconds":%s,"max_concurrency":4},"env_vars":%s%s,"publish_to_prod":true}' \
  "$DIGEST" "$ARCH" "$INIT_TIMEOUT" "$REVISION_ENV" "$REVISION_EGRESS" >"$WORK_DIR/rev.json"
http POST "/v1/functions/$FUNCTION_ID/revisions" "$TOKEN" "$WORK_DIR/rev.json"
REVISION_ID="$(jqr .id)"
for _ in $(seq 1 100); do
  http GET "/v1/functions/$FUNCTION_ID/revisions/$REVISION_ID" "$TOKEN"
  [ "$(jqr .status)" = ready ] && break
  sleep 0.1
done
check deploy.revision_ready "$REVISION_ID" test "$(jqr .status)" = ready

# ---------------------------------------------------------------------------
# 1. retries and 2. dead ends
# ---------------------------------------------------------------------------

FLAKY="$(accept flaky-1 ',"fail_first":2')"
OK_IDS=()
for n in 1 2 3; do OK_IDS+=("$(accept "ok-$n")"); done
EXHAUST="$(accept exhaust-1 ',"fail_first":5')"
BIG="$(accept big-1 ',"response_bytes":100000')"
probe publish --count 1 --id-prefix poison-e2e >"$EVIDENCE/poison-publish.txt"
check accept.all_202 "$(wc -l <"$ACCEPTED" | tr -d ' ') accepted" test "$(wc -l <"$ACCEPTED" | tr -d ' ')" = 6
check poison.published "$(tr '\n' ' ' <"$EVIDENCE/poison-publish.txt")" grep -q '^published=1' "$EVIDENCE/poison-publish.txt"

status="$(wait_terminal "$FLAKY" 60)"
http GET "/v1/invocations/$FLAKY" "$TOKEN"
check retry.flaky_succeeded "status=$status attempts=$(jqr '.attempts | length') dispatch=$(jqr .dispatch.state)" \
  test "$status" = succeeded
check retry.flaky_three_attempts "attempts=$(jqr '.attempts | length') dispatch_attempts=$(jqr .dispatch.attempts)" \
  test "$(jqr '.attempts | length')" = 3
check retry.flaky_pinned_revision "$(jqr .revision_id)" test "$(jqr .revision_id)" = "$REVISION_ID"
check retry.flaky_side_effect_once "failed=$(executions flaky-1 failed) applied=$(executions flaky-1 applied)" \
  test "$(executions flaky-1 applied)" = 1
ok_done=0
for id in "${OK_IDS[@]}"; do
  [ "$(wait_terminal "$id" 30)" = succeeded ] && ok_done=$((ok_done + 1))
done
check retry.plain_orders_succeeded "succeeded=$ok_done/3" test "$ok_done" = 3

status="$(wait_terminal "$EXHAUST" 60)"
http GET "/v1/invocations/$EXHAUST" "$TOKEN"
EXHAUST_DLQ="$(jqr .dispatch.dead_letter_id)"
check deadletter.exhausted_failed "status=$status error=$(jqr .error.error_type) dlq=$EXHAUST_DLQ" \
  test "$status" = failed
check deadletter.exhausted_after_max_attempts "executions=$(executions exhaust-1 failed)" \
  test "$(executions exhaust-1 failed)" = 3
status="$(wait_terminal "$BIG" 60)"
http GET "/v1/invocations/$BIG" "$TOKEN"
check deadletter.non_retryable_once "status=$status error=$(jqr .error.error_type) executions=$(executions big-1 oversized)" \
  test "$(executions big-1 oversized)" = 1 -a "$(jqr .error.error_type)" = Runtime.ResponseTooLarge

http GET "/v1/functions/$FUNCTION_ID/dead-letters" "$TOKEN"
printf '%s\n' "$HTTP_BODY" >"$EVIDENCE/dead-letters-before-redrive.json"
reasons="$(jqr '[.items[] | .reason] | sort | join(",")')"
check deadletter.listed_by_reason "$reasons" test "$reasons" = "attempts_exhausted,non_retryable"
http GET "/v1/functions/$FUNCTION_ID/dead-letters" "$TOKEN_B"
check tenant.dead_letters_not_listed_for_other_tenant "items=$(jqr '.items | length')" test "$(jqr '.items | length')" = 0
http GET "/v1/dead-letters/$EXHAUST_DLQ" "$TOKEN_B"
check tenant.dead_letter_404_for_other_tenant "code=$HTTP_CODE" test "$HTTP_CODE" = 404

poison_seen=0
for _ in $(seq 1 60); do
  if grep -q 'poison event dead-lettered' "$GATEWAY_LOG"; then poison_seen=1; break; fi
  sleep 0.25
done
check poison.dead_lettered_and_terminated "log=$poison_seen" test "$poison_seen" = 1

# ---------------------------------------------------------------------------
# 3. SIGKILL after the terminal commit, before the ACK
# ---------------------------------------------------------------------------

stop_gateway
start_gateway "dispatch.after_commit=kill"
ACKLOST="$(accept acklost-1)"
check crash.after_commit.killed "" wait_gateway_dead 30
check crash.after_commit.side_effect_applied_once "applied=$(executions acklost-1 applied)" \
  test "$(executions acklost-1 applied)" = 1
start_gateway ""
redelivered=0
for _ in $(seq 1 $((ACK_WAIT * 8))); do
  if grep "delivery handled" "$GATEWAY_LOG" | grep "$ACKLOST" | grep -q 'Skipped(\\"terminal\\")'; then
    redelivered=1
    break
  fi
  sleep 0.25
done
check crash.after_commit.redelivery_recognised_as_done "redelivery acked without running: $redelivered" test "$redelivered" = 1
status="$(wait_terminal "$ACKLOST" 30)"
http GET "/v1/invocations/$ACKLOST" "$TOKEN"
check crash.after_commit.not_run_again "status=$status attempts=$(jqr '.attempts | length') executions=$(executions acklost-1)" \
  test "$status" = succeeded -a "$(jqr '.attempts | length')" = 1 -a "$(executions acklost-1)" = 1

# ---------------------------------------------------------------------------
# 4. SIGKILL after the side effect, before the commit
# ---------------------------------------------------------------------------

stop_gateway
start_gateway "dispatch.before_commit=kill"
SIDE="$(accept side-1)"
check crash.before_commit.killed "" wait_gateway_dead 30
check crash.before_commit.side_effect_happened "applied=$(executions side-1 applied)" test "$(executions side-1 applied)" = 1
start_gateway ""
status="$(wait_terminal "$SIDE" 60)"
http GET "/v1/invocations/$SIDE" "$TOKEN"
check crash.before_commit.converged "status=$status attempts=$(jqr '.attempts | length') output=$(printf "%s" "$HTTP_BODY" | jq -c .output)" \
  test "$status" = succeeded
check crash.before_commit.executed_again_harmlessly "executions=$(executions side-1) applied=$(executions side-1 applied) skipped=$(executions side-1 skipped)" \
  test "$(executions side-1)" = 2 -a "$(executions side-1 applied)" = 1 -a "$(executions side-1 skipped)" = 1
check crash.before_commit.effect_exactly_once "effect=$(effects side-1)" test "$(effects side-1)" = 1

# ---------------------------------------------------------------------------
# 5. redrive
# ---------------------------------------------------------------------------

printf '{"reason":"downstream fixed (dispatch e2e)"}' >"$WORK_DIR/redrive.json"
http POST "/v1/dead-letters/$EXHAUST_DLQ:redrive" "$TOKEN" "$WORK_DIR/redrive.json"
check redrive.without_role_403 "code=$HTTP_CODE" test "$HTTP_CODE" = 403
http POST "/v1/dead-letters/$EXHAUST_DLQ:redrive" "$TOKEN_B" "$WORK_DIR/redrive.json"
check redrive.other_tenant_404 "code=$HTTP_CODE" test "$HTTP_CODE" = 404
http POST "/v1/dead-letters/$EXHAUST_DLQ:redrive" "$TOKEN_REDRIVE" "$WORK_DIR/redrive.json"
printf '%s\n' "$HTTP_BODY" >"$EVIDENCE/redrive.json"
check redrive.accepted_202 "code=$HTTP_CODE" test "$HTTP_CODE" = 202
REDRIVEN="$(jqr .invocation.invocation_id)"
check redrive.audit_record "by=$(jqr .redrive.requested_by) source=$(jqr .redrive.source_invocation_id) revision=$(jqr .redrive.revision_id)" \
  test "$(jqr .redrive.requested_by)" = dispatch-e2e-oncall -a "$(jqr .redrive.source_invocation_id)" = "$EXHAUST" -a "$(jqr .redrive.revision_id)" = "$REVISION_ID"
printf '%s %s\n' exhaust-1-redrive "$REDRIVEN" >>"$ACCEPTED"
status="$(wait_terminal "$REDRIVEN" 60)"
http GET "/v1/invocations/$REDRIVEN" "$TOKEN"
check redrive.new_invocation_succeeded "status=$status attempts=$(jqr '.attempts | length') from=$(jqr .dispatch.redriven_from.dead_letter_id)" \
  test "$status" = succeeded -a "$(jqr .dispatch.redriven_from.dead_letter_id)" = "$EXHAUST_DLQ"
check redrive.input_reused "digest=$(jqr .input_digest)" test "$(jqr .revision_id)" = "$REVISION_ID"
http GET "/v1/dead-letters/$EXHAUST_DLQ" "$TOKEN"
check redrive.entry_marked_redriven "status=$(jqr .status) redrives=$(jqr '.redrives | length')" \
  test "$(jqr .status)" = redriven -a "$(jqr '.redrives | length')" = 1
http POST "/v1/dead-letters/$EXHAUST_DLQ:redrive" "$TOKEN_REDRIVE" "$WORK_DIR/redrive.json"
check redrive.second_redrive_409 "code=$HTTP_CODE" test "$HTTP_CODE" = 409
check redrive.side_effect_once "applied=$(executions exhaust-1 applied) effect=$(effects exhaust-1)" \
  test "$(executions exhaust-1 applied)" = 1 -a "$(effects exhaust-1)" = 1

# ---------------------------------------------------------------------------
# 6. convergence, from the ledger itself
# ---------------------------------------------------------------------------

not_terminal=0
while read -r _order id; do
  case "$(wait_terminal "$id" 20)" in
    succeeded | failed | cancelled | outcome_unknown) ;;
    *) not_terminal=$((not_terminal + 1)) ;;
  esac
done <"$ACCEPTED"
check converge.every_invocation_terminal "not_terminal=$not_terminal" test "$not_terminal" = 0
if provider_is_fc; then
  # Every attempt that reached an environment ran in a jailed Firecracker microVM with a guest boot id.
  : >"$EVIDENCE/attempt-environments.txt"
  while read -r _order id; do
    http GET "/v1/invocations/$id" "$TOKEN"
    printf '%s' "$HTTP_BODY" | jq -r --arg id "$id" '.attempts[] | select(.environment_id != null) |
      [$id, .id, .status, .start_kind, .environment_id, (.boot_evidence.details.provider // "-"),
       (.boot_evidence.details.jailed // "-"), (.boot_evidence.guest_boot_id // "-")] | @tsv' \
      >>"$EVIDENCE/attempt-environments.txt"
  done <"$ACCEPTED"
  not_vm="$(awk -F'\t' '$6 != "firecracker" || $7 != "true"' "$EVIDENCE/attempt-environments.txt" | wc -l | tr -d ' ')"
  check microvm.every_attempt_in_a_jailed_firecracker_vm \
    "attempts=$(wc -l <"$EVIDENCE/attempt-environments.txt" | tr -d ' ') not_microvm=$not_vm warm=$(awk -F'\t' '$4 == "warm"' "$EVIDENCE/attempt-environments.txt" | wc -l | tr -d ' ')" \
    test "$not_vm" = 0 -a "$(wc -l <"$EVIDENCE/attempt-environments.txt" | tr -d ' ')" -gt 0
fi
stop_gateway

python3 - "$WORK_DIR/data/state.db" "$EVIDENCE/ledger.txt" <<'PY'
import json, sqlite3, sys
db = sqlite3.connect(sys.argv[1])
out = open(sys.argv[2], "w")
def q(sql):
    return db.execute(sql).fetchall()
dl_per_inv = q("SELECT invocation_id, COUNT(*) FROM dead_letters WHERE invocation_id IS NOT NULL GROUP BY invocation_id HAVING COUNT(*) > 1")
reasons = dict(q("SELECT reason, COUNT(*) FROM dead_letters GROUP BY reason"))
non_terminal_async = q("SELECT COUNT(*) FROM invocations v JOIN invocation_inputs i ON i.invocation_id = v.id WHERE v.terminal = 0")[0][0]
succeeded_attempts = q("SELECT invocation_id, COUNT(*) FROM attempts WHERE status = 'succeeded' GROUP BY invocation_id HAVING COUNT(*) > 1")
states = dict(q("SELECT state, COUNT(*) FROM async_dispatch GROUP BY state"))
redrives = q("SELECT COUNT(*) FROM redrives")[0][0]
print(f"dead_letters_by_reason={json.dumps(reasons, sort_keys=True)}", file=out)
print(f"invocations_with_more_than_one_dead_letter={len(dl_per_inv)}", file=out)
print(f"non_terminal_async_invocations={non_terminal_async}", file=out)
print(f"dispatch_states={json.dumps(states, sort_keys=True)}", file=out)
print(f"redrives={redrives}", file=out)
print(f"invocations_with_several_succeeded_attempts={len(succeeded_attempts)}", file=out)
PY
cat "$EVIDENCE/ledger.txt" >&2
check converge.one_dead_letter_per_invocation "$(grep '^invocations_with_more' "$EVIDENCE/ledger.txt")" \
  grep -q '^invocations_with_more_than_one_dead_letter=0$' "$EVIDENCE/ledger.txt"
check converge.dead_letter_reasons "$(grep '^dead_letters_by_reason' "$EVIDENCE/ledger.txt")" \
  grep -q '^dead_letters_by_reason={"attempts_exhausted": 1, "non_retryable": 1, "poison": 1}$' "$EVIDENCE/ledger.txt"
check converge.no_async_invocation_left_open "$(grep '^non_terminal' "$EVIDENCE/ledger.txt")" \
  grep -q '^non_terminal_async_invocations=0$' "$EVIDENCE/ledger.txt"
check converge.redrive_audited_once "$(grep '^redrives' "$EVIDENCE/ledger.txt")" grep -q '^redrives=1$' "$EVIDENCE/ledger.txt"
effects_count="$(find "$EFFECTS/effects" -name '*.json' | wc -l | tr -d ' ')"
applied_lines="$(grep -c ' applied$' "$EFFECTS/executions.log" || true)"
# flaky-1, ok-1..3, acklost-1, side-1, exhaust-1 (after the redrive): 7 effects, each applied once.
check converge.each_side_effect_exactly_once "effects=$effects_count applied_executions=$applied_lines" \
  test "$effects_count" = 7 -a "$applied_lines" = 7

{
  echo "nats_server_version=$NATS_SERVER_VERSION"
  echo "platform=$(queue_platform)"
  echo "provider=$PROVIDER"
  echo "ack_wait_seconds=$ACK_WAIT"
  echo "stamp=$STAMP"
  echo "host=$(uname -srm)"
  echo "git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "${TSLS_COMMIT:-unknown}")"
  echo "accepted=$(wc -l <"$ACCEPTED" | tr -d ' ')"
  echo "executions=$(wc -l <"$EFFECTS/executions.log" | tr -d ' ')"
} >"$EVIDENCE/environment.txt"

if [ "$FAILED" = 0 ]; then
  log "PASS (evidence: $EVIDENCE)"
else
  log "FAIL (evidence: $EVIDENCE)"
fi
exit "$FAILED"

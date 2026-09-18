#!/usr/bin/env bash
# PLT-4635: scale to zero, re-activation, drain on alias switch and on deletion,
# through a real gateway (docs/adr/0009-scale-to-zero-and-drain.md).
#
#   scripts/e2e/zero-scale.sh                                   # process provider, generated config
#   TSLS_GATEWAY_CONFIG=path/to/gateway.toml TSLS_API_URL=http://127.0.0.1:8080 \
#     TSLS_TOKEN_A=... TSLS_GUEST_DIR=target/aarch64-unknown-linux-musl/release \
#     TSLS_PROVIDER=firecracker scripts/e2e/zero-scale.sh       # any provider, your config
#
# Scenario (example-cpu-burn, payload {"seconds": N}):
#   1. zero        the fresh revision has no environment
#   2. burst       7 concurrent invocations, revision max_concurrency 3: all 200, never more
#                  than 3 environments provisioned at once (coalesced activation)
#   3. idle -> 0   the revision's environments reach 0 (idle TTL 2 s and cooldown 1 s are set on
#                  the revision; with a warm pool the idle ones are scaled down, without one they
#                  ended with their invocations) and no runtime process is left for it
#   4. re-access   the next invocation starts cold and succeeds
#   5. switch      prod moves to a new revision while a 4 s handler runs: new invocations go to
#                  the new revision (higher alias_generation); the running one completes on the
#                  old revision; the old revision drains to 0
#   6. drain       (only when [scaling] drain_timeout_seconds <= 20) a handler still running on a
#                  drained revision past the drain timeout ends 504 Host.DrainTimeout
#   7. delete      the function is deleted while a 3 s handler runs: new invocations and a retry
#                  with the running invocation's Idempotency-Key answer 409 Host.FunctionDeleted,
#                  the running one completes 200, the function reaches deletion_state = deleted
#   8. host        GET /v1/capacity states that zero environments is not zero host cost
#
# Environment (all optional):
#   TSLS_PROVIDER         label for the evidence directory and the orphan check (default process)
#   TSLS_GATEWAY_CONFIG   use this config as-is instead of generating one; then also set
#                         TSLS_API_URL (its listen address) and TSLS_TOKEN_A (a deploy+invoke token).
#                         Recommended for a warm run: [pool] enabled = true, [scaling]
#                         reconcile_interval_ms = 500, drain_timeout_seconds = 5, allow_short_drain = true.
#   TSLS_GUEST_DIR        directory with example-cpu-burn (default target/debug)
#   TSLS_ARCH             aarch64 | x86_64 (default: host)
#   TSLS_SKIP_BUILD=1     do not run cargo build
#   TSLS_EVIDENCE_DIR     evidence root (default docs/evidence)
#
# Exit 0 only when every check passed. Zero environments is not zero host cost: the gateway,
# its store and the node keep running; this script never claims otherwise.
#
# Predicates and the EXIT-trap cleanup are called indirectly (wait_until, rc_of, trap),
# which shellcheck cannot follow (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_tools curl jq cargo python3 || e2e_die "missing tools"

PROVIDER="${TSLS_PROVIDER:-process}"
TENANT_A_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-zero-scale-$PROVIDER"
EVIDENCE_DIR="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}/$RUN_ID"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-zero.XXXXXX")"
mkdir -p "$EVIDENCE_DIR"
GATEWAY_LOG="$WORK_DIR/gateway.log"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/debug}"
case "$(uname -m)" in
  x86_64 | amd64) HOST_ARCH=x86_64 ;;
  *) HOST_ARCH=aarch64 ;;
esac
ARCH="${TSLS_ARCH:-$HOST_ARCH}"
FAILED=0
POLL_PID=""
BG_PIDS=""

check() { # check NAME RC DETAIL
  if [ "$2" -eq 0 ]; then
    printf 'PASS  %s  %s\n' "$1" "$3" | tee -a "$EVIDENCE_DIR/summary.txt"
  else
    printf 'FAIL  %s  %s\n' "$1" "$3" | tee -a "$EVIDENCE_DIR/summary.txt"
    FAILED=1
  fi
}
rc_of() { if "$@"; then echo 0; else echo 1; fi; }

if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  (cd "$REPO_ROOT" && cargo build -q -p tachyon-serverless-gateway -p tachyon-serverless-cli \
    -p tachyon-serverless-runtime-bridge -p example-cpu-burn)
fi
[ -x "$GUEST_DIR/example-cpu-burn" ] || e2e_die "missing $GUEST_DIR/example-cpu-burn"

if [ -n "${TSLS_GATEWAY_CONFIG:-}" ]; then
  CONFIG="$TSLS_GATEWAY_CONFIG"
  API_URL="${TSLS_API_URL:?TSLS_API_URL is required with TSLS_GATEWAY_CONFIG}"
  TOKEN_A="${TSLS_TOKEN_A:?TSLS_TOKEN_A is required with TSLS_GATEWAY_CONFIG}"
else
  PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
  API_URL="http://127.0.0.1:$PORT"
  TOKEN_A="zero-scale-token-a"
  CONFIG="$WORK_DIR/gateway.toml"
  cat > "$CONFIG" <<EOF
listen = "127.0.0.1:$PORT"
profile = "dev"
data_dir = "$WORK_DIR/data"

[provider]
kind = "process"

[provider.process]
bridge_binary = "$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
workdir = "$WORK_DIR/data/process"

[capacity]
max_concurrency = 8
max_queue = 16
queue_timeout_seconds = 30

[scaling]
reconcile_interval_ms = 500
scale_down_cooldown_seconds = 1
drain_timeout_seconds = 5
allow_short_drain = true   # the drain-timeout step needs a short timeout on purpose

[[identity.tokens]]
token = "$TOKEN_A"
tenant_id = "$TENANT_A_ID"
subject = "zero-a"
roles = ["deploy", "invoke"]
EOF
fi
cp "$CONFIG" "$EVIDENCE_DIR/gateway.toml"

"$GATEWAY_BIN" --config "$CONFIG" >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!
cleanup() {
  if [ -n "$POLL_PID" ]; then kill "$POLL_PID" 2>/dev/null || true; fi
  for p in $BG_PIDS; do kill "$p" 2>/dev/null || true; done
  stop_process "$GATEWAY_PID" 15
  cp "$GATEWAY_LOG" "$EVIDENCE_DIR/gateway.log" 2>/dev/null || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT
wait_for_http "$API_URL/readyz" 90 || { tail -n 40 "$GATEWAY_LOG" >&2; exit 1; }

export TSLS_API_URL="$API_URL"
export TSLS_TOKEN="$TOKEN_A"
auth=(-H "authorization: Bearer $TOKEN_A")

capacity() { curl -sS "${auth[@]}" "$API_URL/v1/capacity"; }
scaling_value() { capacity | jq -r ".scaling.$1"; }
WARM_POOL="$(scaling_value warm_pool)"
DRAIN_TIMEOUT="$(scaling_value drain_timeout_seconds)"
RECONCILE_MS="$(scaling_value reconcile_interval_ms)"
echo "provider=$PROVIDER warm_pool=$WARM_POOL drain_timeout=${DRAIN_TIMEOUT}s reconcile=${RECONCILE_MS}ms" \
  | tee -a "$EVIDENCE_DIR/summary.txt"

# rev_json REVISION_ID -> the revision's capacity entry ({} when absent: nothing provisioned).
rev_json() { capacity | jq -c --arg r "$1" '(.revisions[] | select(.revision_id == $r)) // {}'; }
# provisioned REVISION_ID -> the environments that exist: starting + busy + parking + idle + draining.
# `promised` is NOT one of them: it counts an invocation that was granted a pooled environment which
# is still counted in `idle` until it is taken (docs/openapi.json `EnvironmentCounts.promised`, and
# `ResState::holds_resources` in crates/application/src/services/admission/state.rs: "a promise does
# not hold node resources: the idle environment it points at already does"). Adding it counted one
# environment twice and made the burst look like it had passed the revision's cap.
provisioned() {
  rev_json "$1" | jq '(.environments // {}) | [.starting, .busy, .parking, .idle, .draining] | map(. // 0) | add'
}
busy() { rev_json "$1" | jq '.environments.busy // 0'; }
# wait_until SECONDS CMD... -> 0 once CMD succeeds
wait_until() {
  local limit="$1" deadline
  shift
  deadline=$(( $(date +%s) + limit ))
  while ! "$@"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then return 1; fi
    sleep 0.2
  done
}
is_zero() { [ "$(provisioned "$1")" = 0 ]; }
is_busy() { [ "$(busy "$1")" -ge 1 ]; }

deploy() { # deploy FUNCTION_ID EXTRA_ARGS... -> revision id
  local fn="$1"
  shift
  "$TSLS_BIN" functions deploy --function "$fn" --binary "$GUEST_DIR/example-cpu-burn" \
    --arch "$ARCH" --idle-ttl-seconds 2 --scale-down-cooldown-seconds 1 --json "$@" \
    | jq -r '.id'
}
# invoke FUNCTION_ID PAYLOAD OUT_PREFIX [IDEMPOTENCY_KEY] -> writes OUT.status/.body/.headers
invoke() {
  local key_header=()
  if [ -n "${4:-}" ]; then key_header=(-H "idempotency-key: $4"); fi
  curl -sS -o "$3.body" -D "$3.headers" -w '%{http_code}\n' -X POST "${auth[@]}" \
    -H 'content-type: application/json' ${key_header[@]+"${key_header[@]}"} \
    --data "$2" "$API_URL/v1/functions/$1/invoke" > "$3.status" || echo 000 > "$3.status"
}
invocation_of() { # invocation_of OUT_PREFIX -> GET /v1/invocations/<id> JSON
  local id
  id="$(tr -d '\r' < "$1.headers" | awk -F': ' 'tolower($1) == "x-tachyon-invocation-id" {print $2}')"
  curl -sS "${auth[@]}" "$API_URL/v1/invocations/$id"
}
poll_capacity() {
  while :; do
    capacity | jq -c '{t: now, environments, in_flight, queue: .queue.length, revisions: [.revisions[] | {revision_id, route_state, environments, last_scale_event}]}' \
      >> "$EVIDENCE_DIR/capacity.jsonl" 2>/dev/null || true
    sleep 0.1
  done
}
poll_capacity & POLL_PID=$!

FN="$("$TSLS_BIN" functions create --name zero-scale --description plt-4635 --json | jq -r .id)"
REV1="$(deploy "$FN" --max-concurrency 3 --timeout-seconds 60)"

# 1. zero ---------------------------------------------------------------------------------
check "1-zero-before-traffic" "$(rc_of is_zero "$REV1")" "provisioned=$(provisioned "$REV1")"

# 2. burst --------------------------------------------------------------------------------
mkdir -p "$WORK_DIR/burst"
BURST_PIDS=""
for i in 1 2 3 4 5 6 7; do
  invoke "$FN" '{"seconds":0.4}' "$WORK_DIR/burst/$i" &
  BURST_PIDS="$BURST_PIDS $!"
done
max_prov=0
while :; do
  running=0
  for p in $BURST_PIDS; do if kill -0 "$p" 2>/dev/null; then running=1; fi; done
  n="$(provisioned "$REV1")"
  if [ "$n" -gt "$max_prov" ]; then max_prov="$n"; fi
  [ "$running" = 1 ] || break
  sleep 0.05
done
for p in $BURST_PIDS; do wait "$p" 2>/dev/null || true; done
ok="$(cat "$WORK_DIR"/burst/*.status | grep -c '^200$' || true)"
check "2-burst-served" "$(rc_of test "$ok" = 7)" "200s=$ok/7"
check "2-burst-coalesced" "$(rc_of test "$max_prov" -le 3)" "max provisioned=$max_prov (revision max_concurrency 3)"

# 3. idle -> 0 ----------------------------------------------------------------------------
if wait_until 30 is_zero "$REV1"; then zero_rc=0; else zero_rc=1; fi
last="$(rev_json "$REV1" | jq -c '.last_scale_event // null')"
check "3-idle-to-zero" "$zero_rc" "provisioned=$(provisioned "$REV1") warm_pool=$WARM_POOL last_scale_event=$last"
if [ "$WARM_POOL" = "true" ]; then
  kind="$(rev_json "$REV1" | jq -r '.last_scale_event.kind // "none"')"
  check "3-scaled-down-by-sweeper" "$(rc_of test "$kind" = scale_to_zero -o "$kind" = scale_down)" "last kind=$kind"
else
  echo "NOTE  3  warm pool off: environments ended with their invocations (destroy-after-invoke); nothing idle to scale down" \
    | tee -a "$EVIDENCE_DIR/summary.txt"
fi
if [ "$PROVIDER" = process ]; then
  if TSLS_BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge" \
    "$SCRIPT_DIR/orphan-check.sh" process >"$WORK_DIR/orphans.txt" 2>&1; then orc=0; else orc=1; fi
  check "3-no-runtime-process-left" "$orc" "$(tr '\n' ' ' < "$WORK_DIR/orphans.txt" | cut -c1-200)"
fi

# 4. re-access ----------------------------------------------------------------------------
invoke "$FN" '{"seconds":0.1}' "$WORK_DIR/reaccess"
start_kind="$(invocation_of "$WORK_DIR/reaccess" | jq -r '.attempts[0].start_kind')"
check "4-reaccess-cold-start" "$(rc_of test "$(cat "$WORK_DIR/reaccess.status")" = 200 -a "$start_kind" = cold)" \
  "status=$(cat "$WORK_DIR/reaccess.status") start_kind=$start_kind"
wait_until 30 is_zero "$REV1" || true

# 5. alias switch during a long handler ---------------------------------------------------
invoke "$FN" '{"seconds":4}' "$WORK_DIR/long" &
LONG_PID=$!
BG_PIDS="$BG_PIDS $LONG_PID"
wait_until 60 is_busy "$REV1" || true
REV2="$(deploy "$FN" --max-concurrency 3 --timeout-seconds 60)"
invoke "$FN" '{"seconds":0.1}' "$WORK_DIR/after-switch"
after="$(invocation_of "$WORK_DIR/after-switch")"
wait "$LONG_PID" 2>/dev/null || true
long="$(invocation_of "$WORK_DIR/long")"
printf '%s\n%s\n' "$long" "$after" > "$EVIDENCE_DIR/switch-invocations.jsonl"
check "5-new-admission-on-new-revision" \
  "$(rc_of test "$(echo "$after" | jq -r .revision_id)" = "$REV2")" \
  "after: revision=$(echo "$after" | jq -r .revision_id) alias_generation=$(echo "$after" | jq -r .alias_generation)"
check "5-running-invocation-not-switched" \
  "$(rc_of test "$(cat "$WORK_DIR/long.status")" = 200 -a "$(echo "$long" | jq -r .revision_id)" = "$REV1")" \
  "long: status=$(cat "$WORK_DIR/long.status") revision=$(echo "$long" | jq -r .revision_id) alias_generation=$(echo "$long" | jq -r .alias_generation)"
check "5-route-generation-at-admission" \
  "$(rc_of test "$(echo "$long" | jq -r .alias_generation)" -lt "$(echo "$after" | jq -r .alias_generation)")" \
  "long=$(echo "$long" | jq -r .alias_generation) < after=$(echo "$after" | jq -r .alias_generation)"
if wait_until 30 is_zero "$REV1"; then d_rc=0; else d_rc=1; fi
check "5-old-revision-drained" "$d_rc" "rev1 provisioned=$(provisioned "$REV1") route_state=$(rev_json "$REV1" | jq -r '.route_state // "gone"')"

# 6. drain timeout ------------------------------------------------------------------------
if [ "$DRAIN_TIMEOUT" -le 20 ]; then
  invoke "$FN" '{"seconds":40}' "$WORK_DIR/stuck" &
  STUCK_PID=$!
  BG_PIDS="$BG_PIDS $STUCK_PID"
  wait_until 60 is_busy "$REV2" || true
  started="$(date +%s)"
  deploy "$FN" --max-concurrency 3 --timeout-seconds 60 >/dev/null
  wait "$STUCK_PID" 2>/dev/null || true
  took=$(( $(date +%s) - started ))
  etype="$(jq -r '.error.error_type // empty' "$WORK_DIR/stuck.body")"
  check "6-drain-timeout" \
    "$(rc_of test "$(cat "$WORK_DIR/stuck.status")" = 504 -a "$etype" = Host.DrainTimeout -a "$took" -lt 35)" \
    "status=$(cat "$WORK_DIR/stuck.status") error_type=$etype after ${took}s (drain timeout ${DRAIN_TIMEOUT}s)"
else
  echo "SKIP  6-drain-timeout  drain_timeout_seconds=$DRAIN_TIMEOUT > 20" | tee -a "$EVIDENCE_DIR/summary.txt"
fi

# 7. delete during an in-flight invocation, then retry ------------------------------------
invoke "$FN" '{"seconds":3}' "$WORK_DIR/inflight" "zero-scale-inflight" &
INFLIGHT_PID=$!
BG_PIDS="$BG_PIDS $INFLIGHT_PID"
CURRENT_REV="$(curl -sS "${auth[@]}" "$API_URL/v1/functions/$FN/aliases/prod" | jq -r .revision_id)"
wait_until 60 is_busy "$CURRENT_REV" || true
"$TSLS_BIN" functions delete "$FN" --json > "$EVIDENCE_DIR/delete.json"
check "7-deleting" "$(rc_of test "$(jq -r .deletion_state "$EVIDENCE_DIR/delete.json")" = deleting)" \
  "deletion_state=$(jq -r .deletion_state "$EVIDENCE_DIR/delete.json")"
invoke "$FN" '{"seconds":0.1}' "$WORK_DIR/new-after-delete"
check "7-new-invocation-refused" \
  "$(rc_of test "$(cat "$WORK_DIR/new-after-delete.status")" = 409 -a "$(jq -r .error.error_type "$WORK_DIR/new-after-delete.body")" = Host.FunctionDeleted)" \
  "status=$(cat "$WORK_DIR/new-after-delete.status") $(jq -c .error "$WORK_DIR/new-after-delete.body")"
wait "$INFLIGHT_PID" 2>/dev/null || true
check "7-in-flight-completed" "$(rc_of test "$(cat "$WORK_DIR/inflight.status")" = 200)" \
  "status=$(cat "$WORK_DIR/inflight.status") body=$(head -c 120 "$WORK_DIR/inflight.body")"
invoke "$FN" '{"seconds":3}' "$WORK_DIR/retry" "zero-scale-inflight"
check "7-retry-after-delete-refused" \
  "$(rc_of test "$(cat "$WORK_DIR/retry.status")" = 409 -a "$(jq -r .error.error_type "$WORK_DIR/retry.body")" = Host.FunctionDeleted)" \
  "status=$(cat "$WORK_DIR/retry.status") $(jq -c .error "$WORK_DIR/retry.body")"
deleted() { [ "$("$TSLS_BIN" functions get "$FN" --json | jq -r .deletion_state)" = deleted ]; }
if wait_until 30 deleted; then del_rc=0; else del_rc=1; fi
"$TSLS_BIN" functions get "$FN" --json > "$EVIDENCE_DIR/function-final.json" || true
check "7-deletion-drained" "$del_rc" "$(jq -c '{deletion_state, deleted_at, drained_at}' "$EVIDENCE_DIR/function-final.json")"
if wait_until 30 is_zero "$CURRENT_REV"; then z_rc=0; else z_rc=1; fi
check "7-no-environment-left" "$z_rc" "provisioned=$(provisioned "$CURRENT_REV")"

# 8. host is not zero ---------------------------------------------------------------------
capacity | jq . > "$EVIDENCE_DIR/capacity-final.json"
at_zero="$(jq -r .scaling.at_zero "$EVIDENCE_DIR/capacity-final.json")"
check "8-zero-environments-is-not-zero-host-cost" "$(rc_of test -n "$at_zero")" "$at_zero; hosts=$(jq .node.hosts "$EVIDENCE_DIR/capacity-final.json")"
if [ "$PROVIDER" = process ]; then
  if TSLS_BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge" \
    "$SCRIPT_DIR/orphan-check.sh" process >"$WORK_DIR/orphans-final.txt" 2>&1; then orc=0; else orc=1; fi
  check "8-no-orphans" "$orc" "$(tr '\n' ' ' < "$WORK_DIR/orphans-final.txt" | cut -c1-200)"
fi

kill "$POLL_PID" 2>/dev/null || true
wait "$POLL_PID" 2>/dev/null || true
POLL_PID=""
echo "evidence: $EVIDENCE_DIR"
exit "$FAILED"

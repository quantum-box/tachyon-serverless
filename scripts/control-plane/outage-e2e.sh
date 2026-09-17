#!/usr/bin/env bash
# PLT-4636: control-plane / data-plane split end to end, with the process provider.
#
#   scripts/control-plane/outage-e2e.sh
#
# Two gateway processes on one data_dir (one cell):
#   management  role = combined, serves the management API and GET /v1/internal/config
#   data plane  role = data_plane, invoke only, pulls its configuration from management
#
# Flow: build -> start management -> deploy hello (v1) and cpu-burn through the CLI -> start the
# data plane -> invoke through the data plane -> management API on the data plane is 503 ->
# deploy v2, the data plane converges -> start a long invocation -> stop management -> invokes
# continue from the cache -> the long invocation completes -> at the config TTL new invocations
# are refused (Host.ConfigExpired) while /readyz says existing executions continue -> at the auth
# lease the credential is refused (Host.AuthLeaseExpired) -> restart management without tenant
# B's token -> the data plane reconnects, converges, tenant B is revoked, tenant A invokes again
# -> both stop cleanly, no orphans, no secret in logs. Evidence under
# docs/evidence/<UTC>-split-process/.
#
# It lives outside scripts/e2e/ on purpose: that directory is the Firecracker E2E and changes to it
# require the KVM gate (scripts/ci/classify-changes.sh); this flow is provider independent and runs
# on the process provider only.
#
# Environment (all optional):
#   TSLS_SKIP_BUILD=1     do not run cargo build
#   TSLS_EVIDENCE_DIR     evidence root (default docs/evidence)
#   TSLS_CONFIG_TTL       data plane config_ttl_seconds (default 8)
#   TSLS_AUTH_LEASE       data plane auth_lease_seconds (default 14)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$REPO_ROOT/scripts/e2e/lib.sh"

require_tools curl jq cargo python3 || e2e_die "missing tools"

TSLS_SKIP_BUILD="${TSLS_SKIP_BUILD:-0}"
CONFIG_TTL="${TSLS_CONFIG_TTL:-8}"
AUTH_LEASE="${TSLS_AUTH_LEASE:-14}"
REFRESH_MS=500
TOKEN_A="split-token-tenant-a"
TOKEN_B="split-token-tenant-b"
TENANT_A="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TENANT_B="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
INTERNAL_TOKEN="split-internal-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
SECRET_VALUE="split-s3cr3t-$(python3 -c 'import secrets; print(secrets.token_hex(8))')"

case "$(uname -m)" in
  x86_64|amd64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *) e2e_die "unsupported host architecture $(uname -m)" ;;
esac

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-split-process"
EVIDENCE_ROOT="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}"
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-split.XXXXXX")"
export E2E_STATE_DIR="$WORK_DIR/state"
export STEP_LOG_DIR="$EVIDENCE_DIR/steps"
mkdir -p "$EVIDENCE_DIR" "$STEP_LOG_DIR" "$E2E_STATE_DIR" "$WORK_DIR/data"

TSLS_BIN="$REPO_ROOT/target/debug/tsls"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
GUEST_DIR="$REPO_ROOT/target/debug"
MGMT_LOG="$EVIDENCE_DIR/management.log"
DP_LOG="$EVIDENCE_DIR/data-plane.log"

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
MGMT_PORT="$(free_port)"
DP_PORT="$(free_port)"
MGMT_URL="http://127.0.0.1:$MGMT_PORT"
DP_URL="http://127.0.0.1:$DP_PORT"

tsls() { "$TSLS_BIN" "$@"; }
export TSLS_API_URL="$MGMT_URL"
export TSLS_TOKEN="$TOKEN_A"

MGMT_PID=""
DP_PID=""
LONG_PID=""
BG_PIDS=""

cleanup() {
  local rc=$?
  set +e
  for p in $BG_PIDS; do kill "$p" 2>/dev/null || true; done
  if [ -n "$DP_PID" ] && kill -0 "$DP_PID" 2>/dev/null; then stop_process "$DP_PID" 10; fi
  if [ -n "$MGMT_PID" ] && kill -0 "$MGMT_PID" 2>/dev/null; then stop_process "$MGMT_PID" 10; fi
  rm -rf "$WORK_DIR"
  exit $rc
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# configs
# ---------------------------------------------------------------------------

common_config() {
  local listen="$1" instance="$2"
  echo "listen = \"$listen\""
  echo 'profile = "dev"'
  echo "data_dir = \"$WORK_DIR/data\""
  echo
  echo '[provider]'
  echo 'kind = "process"'
  echo
  echo '[provider.process]'
  echo "bridge_binary = \"$BRIDGE_BIN\""
  echo "workdir = \"$WORK_DIR/data/process-$instance\""
  echo
  echo '[dispatcher]'
  echo "instance = \"$instance\""
  echo
  echo '[[secrets.bindings]]'
  echo "tenant_id = \"$TENANT_A\""
  echo 'binding_ref = "demo-secret"'
  echo "value = \"$SECRET_VALUE\""
  echo
}

# write_management_config OUT WITH_TOKEN_B(0|1)
write_management_config() {
  local out="$1" with_b="$2"
  {
    common_config "127.0.0.1:$MGMT_PORT" management
    echo '[control_plane]'
    echo "internal_token = \"$INTERNAL_TOKEN\""
    echo
    echo '[[identity.tokens]]'
    echo "token = \"$TOKEN_A\""
    echo "tenant_id = \"$TENANT_A\""
    echo 'subject = "split-a"'
    echo 'roles = ["deploy", "invoke"]'
    if [ "$with_b" = "1" ]; then
      echo
      echo '[[identity.tokens]]'
      echo "token = \"$TOKEN_B\""
      echo "tenant_id = \"$TENANT_B\""
      echo 'subject = "split-b"'
      echo 'roles = ["deploy", "invoke"]'
    fi
  } > "$out"
}

write_data_plane_config() {
  local out="$1"
  {
    common_config "127.0.0.1:$DP_PORT" data-plane
    echo '[control_plane]'
    echo 'role = "data_plane"'
    echo "url = \"$MGMT_URL\""
    echo "internal_token = \"$INTERNAL_TOKEN\""
    echo "refresh_interval_ms = $REFRESH_MS"
    echo "config_ttl_seconds = $CONFIG_TTL"
    echo "auth_lease_seconds = $AUTH_LEASE"
    echo 'backoff_initial_ms = 250'
    echo 'backoff_max_ms = 1000'
    echo 'fetch_timeout_ms = 1000'
    echo
    echo '[control_plane_outage]'
    echo 'allow_cold_start = true'
  } > "$out"
}

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

# http METHOD URL TOKEN [BODY] -> HTTP_CODE, HTTP_BODY
HTTP_CODE=""
HTTP_BODY=""
http() {
  local method="$1" url="$2" token="$3" body="${4:-}" out
  out="$WORK_DIR/http.$$.out"
  if [ -n "$body" ]; then
    HTTP_CODE="$(curl -s -o "$out" -w '%{http_code}' --max-time 60 -X "$method" \
      -H "authorization: Bearer $token" -H 'content-type: application/json' --data "$body" "$url" || true)"
  else
    HTTP_CODE="$(curl -s -o "$out" -w '%{http_code}' --max-time 60 -X "$method" \
      -H "authorization: Bearer $token" "$url" || true)"
  fi
  HTTP_BODY="$(cat "$out" 2>/dev/null || true)"
  rm -f "$out"
}

dp_invoke() {
  local token="$1" payload="$2" fn
  fn="$(state_get fn.hello)"
  http POST "$DP_URL/v1/functions/$fn/invoke" "$token" "$payload"
}

readyz() {
  curl -s --max-time 5 "$1/readyz" || true
}

start_management() {
  local config="$1"
  LOG_FORMAT=json "$GATEWAY_BIN" --config "$config" >>"$MGMT_LOG" 2>&1 &
  MGMT_PID=$!
  state_set mgmt.pid "$MGMT_PID"
}

start_data_plane() {
  LOG_FORMAT=json "$GATEWAY_BIN" --config "$WORK_DIR/data-plane.toml" >>"$DP_LOG" 2>&1 &
  DP_PID=$!
}

wait_ready() {
  local url="$1" pid="$2" i
  for i in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || { echo "gateway $pid exited during startup" >&2; return 1; }
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url/readyz" || true)" = "200" ]; then
      return 0
    fi
    sleep 0.25
  done
  echo "$url/readyz never became 200: $(readyz "$url")" >&2
  return 1
}

# ---------------------------------------------------------------------------
# steps
# ---------------------------------------------------------------------------

build_all() {
  cd "$REPO_ROOT"
  if [ "$TSLS_SKIP_BUILD" != "1" ]; then
    cargo build -p tachyon-serverless-gateway -p tachyon-serverless-cli \
      -p tachyon-serverless-runtime-bridge -p example-hello -p example-cpu-burn
  fi
  for b in "$TSLS_BIN" "$GATEWAY_BIN" "$BRIDGE_BIN" "$GUEST_DIR/example-hello" "$GUEST_DIR/example-cpu-burn"; do
    [ -x "$b" ] || { echo "missing binary: $b" >&2; return 1; }
  done
}

management_up() {
  wait_ready "$MGMT_URL" "$MGMT_PID"
  readyz "$MGMT_URL" | jq -e '.control_plane.role == "combined"' >/dev/null
}

deploy_functions() {
  local hello burn rev
  hello="$(tsls functions create --name hello --description split --json | jq -r .id)"
  burn="$(tsls functions create --name cpu-burn --description split --json | jq -r .id)"
  state_set fn.hello "$hello"
  state_set fn.cpu-burn "$burn"
  rev="$(tsls functions deploy --function hello --binary "$GUEST_DIR/example-hello" --arch "$ARCH" \
    --env GREETING=v1 --secret DEMO_SECRET=demo-secret --description v1 --json | jq -r .id)"
  state_set rev.hello.v1 "$rev"
  tsls functions deploy --function cpu-burn --binary "$GUEST_DIR/example-cpu-burn" --arch "$ARCH" \
    --timeout-seconds 60 --description v1 --json | jq -r .id >/dev/null
  # the internal endpoint refuses tenant tokens
  http GET "$MGMT_URL/v1/internal/config" "$TOKEN_A"
  assert_eq 401 "$HTTP_CODE" "internal config with a tenant token"
  http GET "$MGMT_URL/v1/internal/config?since=0" "$INTERNAL_TOKEN"
  assert_eq 200 "$HTTP_CODE" "internal config with the internal credential"
  printf '%s' "$HTTP_BODY" | jq '{generation, since, config_ttl_seconds, auth_lease_seconds,
    entries: [.entries[] | {kind: .key.kind, generation, tombstone: (.value == null)}]}' \
    > "$EVIDENCE_DIR/delivery-initial.json"
  case "$HTTP_BODY" in
    *"$TOKEN_A"*|*"$SECRET_VALUE"*) echo "the delivery carries a token or a secret value" >&2; return 1 ;;
  esac
}

data_plane_up() {
  wait_ready "$DP_URL" "$DP_PID"
  readyz "$DP_URL" | tee "$EVIDENCE_DIR/readyz-data-plane-connected.json" | jq -e '
    .control_plane.role == "data_plane" and .control_plane.control_plane_reachable == true' >/dev/null
}

invoke_through_data_plane() {
  dp_invoke "$TOKEN_A" '{"name":"split"}'
  assert_eq 200 "$HTTP_CODE" "invoke on the data plane" || { echo "$HTTP_BODY" >&2; return 1; }
  assert_json "$HTTP_BODY" '.message' "hello, split"
  assert_json "$HTTP_BODY" '.greeting' "v1"
  assert_json "$HTTP_BODY" '.secret_present' "true"
  # tenant B is authenticated by the delivered grants and sees nothing of A
  dp_invoke "$TOKEN_B" '{}'
  assert_eq 404 "$HTTP_CODE" "tenant B on tenant A's function"
}

management_api_is_503_on_the_data_plane() {
  http GET "$DP_URL/v1/functions" "$TOKEN_A"
  assert_eq 503 "$HTTP_CODE" "GET /v1/functions on the data plane"
  assert_json "$HTTP_BODY" '.error.code' "control_plane_unavailable"
  assert_json "$HTTP_BODY" '.error.error_type' "Host.ControlPlaneUnavailable"
  http GET "$DP_URL/v1/internal/config" "$INTERNAL_TOKEN"
  assert_eq 404 "$HTTP_CODE" "a data plane does not publish"
}

data_plane_converges_on_a_new_revision() {
  local deadline start
  tsls functions deploy --function hello --binary "$GUEST_DIR/example-hello" --arch "$ARCH" \
    --env GREETING=v2 --secret DEMO_SECRET=demo-secret --description v2 --json | jq -r .id >/dev/null
  start="$(now_ms)"
  deadline=$(( $(date +%s) + 10 ))
  while :; do
    dp_invoke "$TOKEN_A" '{"name":"v2"}'
    if [ "$HTTP_CODE" = "200" ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .greeting)" = "v2" ]; then
      break
    fi
    [ "$(date +%s)" -lt "$deadline" ] || { echo "data plane never served v2: $HTTP_CODE $HTTP_BODY" >&2; return 1; }
    sleep 0.2
  done
  e2e_log "data plane served v2 $(( $(now_ms) - start )) ms after the deploy returned"
}

# A long invocation that is running when the control plane goes away. Started from main (not a
# step subshell) so that the parent can wait for it.
start_long_invocation() {
  local fn
  fn="$(state_get fn.cpu-burn)"
  : > "$WORK_DIR/long.code"
  (
    curl -s -o "$WORK_DIR/long.out" -w '%{http_code}' --max-time 120 -X POST \
      -H "authorization: Bearer $TOKEN_A" -H 'content-type: application/json' \
      --data "{\"seconds\": $(( CONFIG_TTL + 3 ))}" "$DP_URL/v1/functions/$fn/invoke" \
      > "$WORK_DIR/long.code.tmp"
    mv "$WORK_DIR/long.code.tmp" "$WORK_DIR/long.code"
  ) &
  LONG_PID=$!
  BG_PIDS="$BG_PIDS $LONG_PID"
}

long_invocation_running() {
  local fn deadline
  fn="$(state_get fn.cpu-burn)"
  deadline=$(( $(date +%s) + 10 ))
  while :; do
    http GET "$MGMT_URL/v1/functions/$fn/invocations?limit=5" "$TOKEN_A"
    if [ "$(printf '%s' "$HTTP_BODY" | jq -r '[.items[] | select(.status == "running")] | length')" = "1" ]; then
      return 0
    fi
    [ "$(date +%s)" -lt "$deadline" ] || { echo "the long invocation never started: $HTTP_BODY" >&2; return 1; }
    sleep 0.2
  done
}

continue_within_ttl() {
  local i
  dp_invoke "$TOKEN_A" '{"name":"outage"}'
  assert_eq 200 "$HTTP_CODE" "invoke right after the control plane stopped" || { echo "$HTTP_BODY" >&2; return 1; }
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if readyz "$DP_URL" | jq -e '.control_plane.control_plane_reachable == false' >/dev/null; then
      break
    fi
    sleep 0.25
  done
  readyz "$DP_URL" | tee "$EVIDENCE_DIR/readyz-control-plane-down.json" | jq -e '
    .ready == true and .control_plane.control_plane_reachable == false
    and .control_plane.new_invocations == "accepted"
    and .control_plane.existing_executions == "continue"' >/dev/null
  dp_invoke "$TOKEN_A" '{"name":"outage-2"}'
  assert_eq 200 "$HTTP_CODE" "invoke during the outage, within the TTL"
}

refused_at_config_ttl() {
  local deadline stopped_ms waited
  stopped_ms="$(state_get mgmt.stopped_ms)"
  deadline=$(( $(date +%s) + CONFIG_TTL + 10 ))
  while :; do
    dp_invoke "$TOKEN_A" '{"name":"late"}'
    [ "$HTTP_CODE" = "200" ] || break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "still accepted past the config TTL" >&2; return 1; }
    sleep 0.25
  done
  waited=$(( $(now_ms) - stopped_ms ))
  e2e_log "first refusal ${waited} ms after the control plane stopped (config TTL ${CONFIG_TTL}s)"
  printf '%s\n' "$HTTP_BODY" > "$EVIDENCE_DIR/refusal-config-expired.json"
  assert_eq 503 "$HTTP_CODE" "status at the config TTL"
  assert_json "$HTTP_BODY" '.error.code' "config_unavailable"
  assert_json "$HTTP_BODY" '.error.error_type' "Host.ConfigExpired"
  # never before the TTL minus one refresh interval, never long after it
  [ "$waited" -ge $(( CONFIG_TTL * 1000 - REFRESH_MS - 1000 )) ] || { echo "refused too early ($waited ms)" >&2; return 1; }
  [ "$waited" -le $(( CONFIG_TTL * 1000 + 3000 )) ] || { echo "refused too late ($waited ms)" >&2; return 1; }
  readyz "$DP_URL" | tee "$EVIDENCE_DIR/readyz-config-expired.json" | jq -e '
    .ready == false and .control_plane.new_invocations == "refused"
    and .control_plane.refusal == "Host.ConfigExpired"
    and .control_plane.existing_executions == "continue"' >/dev/null
}

long_invocation_completed() {
  local deadline
  deadline=$(( $(date +%s) + 30 ))
  while [ ! -s "$WORK_DIR/long.code" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo "the long invocation never returned" >&2; return 1; }
    sleep 0.25
  done
  assert_eq 200 "$(cat "$WORK_DIR/long.code")" "the invocation running across the outage" || { cat "$WORK_DIR/long.out" >&2; return 1; }
  jq -e '.burned_seconds > 0' "$WORK_DIR/long.out" >/dev/null
}

refused_at_auth_lease() {
  local deadline
  deadline=$(( $(date +%s) + AUTH_LEASE + 10 ))
  while :; do
    dp_invoke "$TOKEN_A" '{"name":"later"}'
    [ "$(printf '%s' "$HTTP_BODY" | jq -r '.error.error_type // empty')" = "Host.AuthLeaseExpired" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "the auth lease never expired: $HTTP_CODE $HTTP_BODY" >&2; return 1; }
    sleep 0.25
  done
  printf '%s\n' "$HTTP_BODY" > "$EVIDENCE_DIR/refusal-auth-lease-expired.json"
  assert_eq 503 "$HTTP_CODE" "status at the auth lease"
  http GET "$DP_URL/v1/invocations/inv_01hzzzzzzzzzzzzzzzzzzzzzzz" "$TOKEN_B"
  assert_json "$HTTP_BODY" '.error.error_type' "Host.AuthLeaseExpired"
}

management_restarts_without_token_b() {
  wait_ready "$MGMT_URL" "$MGMT_PID"
  http GET "$MGMT_URL/v1/functions" "$TOKEN_B"
  assert_eq 401 "$HTTP_CODE" "tenant B on the restarted management gateway"
}

data_plane_recovers() {
  local deadline start
  start="$(now_ms)"
  deadline=$(( $(date +%s) + 15 ))
  while :; do
    dp_invoke "$TOKEN_A" '{"name":"back"}'
    [ "$HTTP_CODE" = "200" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || { echo "the data plane did not recover: $HTTP_CODE $HTTP_BODY" >&2; return 1; }
    sleep 0.25
  done
  e2e_log "data plane accepted again $(( $(now_ms) - start )) ms after management was ready"
  assert_json "$HTTP_BODY" '.greeting' "v2"
  readyz "$DP_URL" | tee "$EVIDENCE_DIR/readyz-reconnected.json" | jq -e '
    .ready == true and .control_plane.control_plane_reachable == true
    and .control_plane.cache.reconnects >= 1 and .dispatcher.fenced == false' >/dev/null
  # tenant B's token was removed from the control plane: revoked on the data plane
  dp_invoke "$TOKEN_B" '{}'
  assert_eq 401 "$HTTP_CODE" "tenant B after revocation"
  grep -q 'control plane reachable again' "$DP_LOG"
}


no_secret_in_logs() {
  if grep -rqF -- "$SECRET_VALUE" "$MGMT_LOG" "$DP_LOG" "$EVIDENCE_DIR"; then
    echo "the secret value leaked into logs or evidence" >&2
    return 1
  fi
  if grep -rqF -- "$INTERNAL_TOKEN" "$MGMT_LOG" "$DP_LOG" "$EVIDENCE_DIR"; then
    echo "the internal credential leaked into logs or evidence" >&2
    return 1
  fi
}

main() {
  local dp_rc mgmt_rc mgmt_first_rc
  e2e_log "run $RUN_ID: management $MGMT_URL, data plane $DP_URL (process provider, no isolation)"
  step "build" build_all

  write_management_config "$WORK_DIR/management.toml" 1
  start_management "$WORK_DIR/management.toml"
  step "management gateway up (combined)" management_up
  step "deploy hello v1 and cpu-burn via management; internal endpoint auth" deploy_functions

  write_data_plane_config "$WORK_DIR/data-plane.toml"
  start_data_plane
  step "data plane gateway up, first delivery" data_plane_up
  step "invoke through the data plane (tenant A 200, tenant B 404)" invoke_through_data_plane
  step "management API on the data plane -> 503 control_plane_unavailable" management_api_is_503_on_the_data_plane
  step "deploy v2 via management; data plane converges" data_plane_converges_on_a_new_revision

  start_long_invocation
  step "a long invocation is running on the data plane" long_invocation_running
  stop_process "$MGMT_PID" 15
  mgmt_first_rc=$STOP_RC
  MGMT_PID=""
  state_set mgmt.stopped_ms "$(now_ms)"
  e2e_log "management stopped (exit $mgmt_first_rc)"
  step "management stops on SIGTERM with exit 0" assert_eq 0 "$mgmt_first_rc" "management exit status"
  step "control plane down: invokes continue within the TTL" continue_within_ttl
  step "config TTL passed: new invocations 503 Host.ConfigExpired; existing continue" refused_at_config_ttl
  step "the invocation running across the outage completed" long_invocation_completed
  step "auth lease passed: 503 Host.AuthLeaseExpired" refused_at_auth_lease

  write_management_config "$WORK_DIR/management.toml" 0
  start_management "$WORK_DIR/management.toml"
  step "management restarts without tenant B's token" management_restarts_without_token_b
  step "data plane reconnects, converges, B revoked, A invokes" data_plane_recovers

  stop_process "$DP_PID" 15
  dp_rc=$STOP_RC
  DP_PID=""
  stop_process "$MGMT_PID" 15
  mgmt_rc=$STOP_RC
  MGMT_PID=""
  step "data plane stops on SIGTERM with exit 0" assert_eq 0 "$dp_rc" "data plane exit status"
  step "restarted management stops on SIGTERM with exit 0" assert_eq 0 "$mgmt_rc" "management exit status"
  step "no secret or internal credential in logs and evidence" no_secret_in_logs
  step "no orphan bridge processes" env TSLS_BRIDGE_BIN="$BRIDGE_BIN" "$REPO_ROOT/scripts/e2e/orphan-check.sh" process

  steps_write_summary "$EVIDENCE_DIR/summary.json" \
    "$(jq -n --arg run "$RUN_ID" --arg host "$(uname -srm)" \
        --argjson ttl "$CONFIG_TTL" --argjson lease "$AUTH_LEASE" --argjson refresh "$REFRESH_MS" \
        '{run_id: $run, provider: "process", host: $host, config_ttl_seconds: $ttl,
          auth_lease_seconds: $lease, refresh_interval_ms: $refresh}')"
  e2e_log "summary: $EVIDENCE_DIR/summary.json"
  steps_print_table
}

main "$@"

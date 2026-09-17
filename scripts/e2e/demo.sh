#!/usr/bin/env bash
# PLT-4630: end-to-end demo of the Tachyon Serverless prototype.
#
#   scripts/e2e/demo.sh                      # process provider (macOS / Linux, no isolation)
#   TSLS_PROVIDER=firecracker scripts/e2e/demo.sh   # Linux/KVM, see docs/kvm.md
#
# Flow: build -> start gateway -> provider table -> create/deploy hello, http-axum,
# cpu-burn -> invoke assertions (success, user_error, crash, init_error, http
# passthrough, timeout, orphan check, logs, boot evidence, rollback, cross-tenant,
# cancel) -> evidence under docs/evidence/<UTC>-<provider>/ -> stop gateway ->
# PASS/FAIL table. Exit 0 only when every step passed.
#
# Environment (all optional):
#   TSLS_PROVIDER        process (default) | firecracker
#   TSLS_SKIP_BUILD=1    do not run cargo build
#   TSLS_API_URL         gateway URL when using a repo config (default http://127.0.0.1:8080)
#   TSLS_TOKEN_A/B       tenant A / B tokens (default dev-token-tenant-a / dev-token-tenant-b)
#   TSLS_GATEWAY_CONFIG  explicit gateway config path
#   TSLS_GENERATE_CONFIG=1  always generate a config (ignores config/gateway.*.toml)
#   TSLS_EVIDENCE_DIR    evidence root (default docs/evidence)
#   TSLS_FC_BINARY / TSLS_FC_KERNEL / TSLS_FC_ROOTFS / TSLS_FC_RUN_DIR  firecracker paths for a generated config
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_tools curl jq cargo pgrep || e2e_die "missing tools"

TSLS_PROVIDER="${TSLS_PROVIDER:-process}"
TSLS_SKIP_BUILD="${TSLS_SKIP_BUILD:-0}"
TSLS_TOKEN_A="${TSLS_TOKEN_A:-dev-token-tenant-a}"
TSLS_TOKEN_B="${TSLS_TOKEN_B:-dev-token-tenant-b}"
TENANT_A_ID="${TSLS_TENANT_A_ID:-tn_01hzzzzzzzzzzzzzzzzzzzzzza}"
TENANT_B_ID="${TSLS_TENANT_B_ID:-tn_01hzzzzzzzzzzzzzzzzzzzzzzb}"
DEMO_SECRET_REF="${TSLS_DEMO_SECRET_REF:-demo-secret}"
TSLS_GENERATE_CONFIG="${TSLS_GENERATE_CONFIG:-0}"

case "$TSLS_PROVIDER" in
  process|firecracker) ;;
  *) e2e_die "TSLS_PROVIDER must be process or firecracker (got $TSLS_PROVIDER)" ;;
esac

HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
  x86_64|amd64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *) e2e_die "unsupported host architecture $HOST_ARCH" ;;
esac

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$TSLS_PROVIDER"
EVIDENCE_ROOT="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}"
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-e2e.XXXXXX")"
export E2E_STATE_DIR="$WORK_DIR/state"
export STEP_LOG_DIR="$EVIDENCE_DIR/steps"
mkdir -p "$EVIDENCE_DIR" "$STEP_LOG_DIR" "$E2E_STATE_DIR"
GATEWAY_LOG="$EVIDENCE_DIR/gateway.log"

TSLS_BIN="${TSLS_BIN:-$REPO_ROOT/target/debug/tsls}"
GATEWAY_BIN="${TSLS_GATEWAY_BIN:-$REPO_ROOT/target/debug/tachyon-serverless-gateway}"
GATEWAY_CONFIG_FLAG="${TSLS_GATEWAY_CONFIG_FLAG:---config}"
if [ "$TSLS_PROVIDER" = "firecracker" ]; then
  GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/$ARCH-unknown-linux-musl/release}"
  BRIDGE_BIN="$GUEST_DIR/tachyon-serverless-runtime-bridge"
else
  GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/debug}"
  BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
fi
# Wall-clock budget for the timeout step: handler timeout (2s) + grace (1s)
# + environment boot. microVM boot on nested/slow hosts can take >10s.
if [ "$TSLS_PROVIDER" = "firecracker" ]; then
  TIMEOUT_WALL_BUDGET_MS="${TSLS_TIMEOUT_WALL_MS:-90000}"
else
  TIMEOUT_WALL_BUDGET_MS="${TSLS_TIMEOUT_WALL_MS:-10000}"
fi

tsls() { "$TSLS_BIN" "$@"; }
export -f tsls 2>/dev/null || true
export TSLS_BIN

GATEWAY_PID=""
BG_PIDS=""

cleanup() {
  local rc=$?
  set +e
  for p in $BG_PIDS; do
    kill "$p" 2>/dev/null || true
  done
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    e2e_log "cleanup: stopping gateway $GATEWAY_PID"
    stop_process "$GATEWAY_PID" 10
  fi
  rm -rf "$WORK_DIR"
  exit $rc
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# 1. build
# ---------------------------------------------------------------------------

build_all() {
  cd "$REPO_ROOT"
  if [ "$TSLS_SKIP_BUILD" = "1" ]; then
    e2e_log "TSLS_SKIP_BUILD=1: skipping cargo build"
    return 0
  fi
  cargo build -p tachyon-serverless-gateway -p tachyon-serverless-cli
  if [ "$TSLS_PROVIDER" = "firecracker" ]; then
    cargo build --release --target "$ARCH-unknown-linux-musl" \
      -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn
  else
    cargo build -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn
  fi
  for b in "$TSLS_BIN" "$GATEWAY_BIN" "$BRIDGE_BIN" "$GUEST_DIR/example-hello" "$GUEST_DIR/example-http-axum" "$GUEST_DIR/example-cpu-burn"; do
    [ -x "$b" ] || { echo "missing binary: $b" >&2; return 1; }
  done
}

# ---------------------------------------------------------------------------
# 2. gateway config + start
# ---------------------------------------------------------------------------

generate_config() {
  local out="$1" listen="$2"
  local data_dir="$WORK_DIR/data"
  mkdir -p "$data_dir"
  {
    echo "listen = \"$listen\""
    echo 'profile = "dev"'
    echo "data_dir = \"$data_dir\""
    echo
    echo '[provider]'
    echo "kind = \"$TSLS_PROVIDER\""
    echo
    if [ "$TSLS_PROVIDER" = "firecracker" ]; then
      mkdir -p "${TSLS_FC_RUN_DIR:-$REPO_ROOT/.kvm/run}"
      echo '[provider.firecracker]'
      echo "firecracker_binary = \"${TSLS_FC_BINARY:-$REPO_ROOT/.kvm/bin/firecracker}\""
      echo "kernel = \"${TSLS_FC_KERNEL:-$REPO_ROOT/.kvm/vmlinux}\""
      echo "rootfs = \"${TSLS_FC_ROOTFS:-$REPO_ROOT/.kvm/rootfs.ext4}\""
      echo "workdir = \"${TSLS_FC_RUN_DIR:-$REPO_ROOT/.kvm/run}\""
      echo 'vsock_port = 5000'
    else
      mkdir -p "$data_dir/process"
      echo '[provider.process]'
      echo "bridge_binary = \"$BRIDGE_BIN\""
      echo "workdir = \"$data_dir/process\""
    fi
    echo
    echo '[capacity]'
    echo 'max_concurrency = 8'
    echo 'max_queue = 32'
    echo 'queue_timeout_seconds = 10'
    echo
    echo '[[identity.tokens]]'
    echo "token = \"$TSLS_TOKEN_A\""
    echo "tenant_id = \"$TENANT_A_ID\""
    echo 'subject = "demo-a"'
    echo 'roles = ["deploy", "invoke"]'
    echo
    echo '[[identity.tokens]]'
    echo "token = \"$TSLS_TOKEN_B\""
    echo "tenant_id = \"$TENANT_B_ID\""
    echo 'subject = "demo-b"'
    echo 'roles = ["deploy", "invoke"]'
    echo
    echo '[[secrets.bindings]]'
    echo "tenant_id = \"$TENANT_A_ID\""
    echo "binding_ref = \"$DEMO_SECRET_REF\""
    echo 'value = "s3cr3t-demo"'
  } > "$out"
}

select_config() {
  local repo_cfg
  if [ -n "${TSLS_GATEWAY_CONFIG:-}" ]; then
    CONFIG_PATH="$TSLS_GATEWAY_CONFIG"
    API_URL="${TSLS_API_URL:-http://127.0.0.1:8080}"
    e2e_log "using explicit config $CONFIG_PATH (api $API_URL)"
    return 0
  fi
  if [ "$TSLS_PROVIDER" = "firecracker" ]; then
    repo_cfg="$REPO_ROOT/config/gateway.firecracker.toml"
  else
    repo_cfg="$REPO_ROOT/config/gateway.dev.toml"
  fi
  if [ "$TSLS_GENERATE_CONFIG" != "1" ] && [ -f "$repo_cfg" ]; then
    CONFIG_PATH="$repo_cfg"
    API_URL="${TSLS_API_URL:-http://127.0.0.1:8080}"
    e2e_log "using repo config $CONFIG_PATH (api $API_URL); tokens must match TSLS_TOKEN_A/B"
    return 0
  fi
  local port
  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])' 2>/dev/null || echo 18080)"
  CONFIG_PATH="$WORK_DIR/gateway.toml"
  API_URL="http://127.0.0.1:$port"
  generate_config "$CONFIG_PATH" "127.0.0.1:$port"
  e2e_log "generated config $CONFIG_PATH (api $API_URL)"
}

start_gateway() {
  cd "$REPO_ROOT"
  LOG_FORMAT=json TACHYON_GATEWAY_CONFIG="$CONFIG_PATH" \
    "$GATEWAY_BIN" "$GATEWAY_CONFIG_FLAG" "$CONFIG_PATH" >"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  e2e_log "gateway pid $GATEWAY_PID, log $GATEWAY_LOG"
  export TSLS_API_URL="$API_URL"
  export TSLS_TOKEN="$TSLS_TOKEN_A"
}

wait_gateway() {
  # Fail fast when our gateway died during startup (e.g. the port is taken by
  # another local server): otherwise every later step would talk to whatever
  # process answers on that port.
  local i
  for i in $(seq 1 60); do
    if ! kill -0 "$GATEWAY_PID" 2>/dev/null; then
      echo "gateway process $GATEWAY_PID exited during startup (port in use or invalid config); last log lines:" >&2
      tail -n 30 "$GATEWAY_LOG" >&2
      return 1
    fi
    curl -fsS -m 2 -o /dev/null "$API_URL/healthz" 2>/dev/null && break
    sleep 0.5
  done
  wait_for_http "$API_URL/healthz" 30 || { tail -n 30 "$GATEWAY_LOG" >&2; return 1; }
  wait_for_http "$API_URL/readyz" 60 || { tail -n 30 "$GATEWAY_LOG" >&2; return 1; }
  tsls health
}

check_tokens() {
  run_capture tsls functions list --json
  if [ "$RUN_RC" -ne 0 ]; then
    echo "tenant A token rejected (exit $RUN_RC): $RUN_ERR" >&2
    echo "the gateway config must define token TSLS_TOKEN_A=$TSLS_TOKEN_A (tenant A, roles deploy+invoke)" >&2
    return 1
  fi
  TSLS_TOKEN="$TSLS_TOKEN_B" run_capture tsls functions list --json
  if [ "$RUN_RC" -ne 0 ]; then
    echo "tenant B token rejected (exit $RUN_RC): $RUN_ERR" >&2
    echo "the gateway config must define token TSLS_TOKEN_B=$TSLS_TOKEN_B for a second tenant" >&2
    return 1
  fi
}

# ---------------------------------------------------------------------------
# 3. provider
# ---------------------------------------------------------------------------

show_provider() {
  tsls provider
  tsls provider --json > "$EVIDENCE_DIR/provider.json"
  assert_json "$(cat "$EVIDENCE_DIR/provider.json")" '.kind' "$TSLS_PROVIDER"
  if [ "$TSLS_PROVIDER" = "firecracker" ]; then
    assert_json "$(cat "$EVIDENCE_DIR/provider.json")" '.isolation' "micro_vm"
    assert_json "$(cat "$EVIDENCE_DIR/provider.json")" '.dev_only' "false"
  else
    assert_json "$(cat "$EVIDENCE_DIR/provider.json")" '.dev_only' "true"
  fi
}

# ---------------------------------------------------------------------------
# 4. functions
# ---------------------------------------------------------------------------

ensure_function() {
  local name="$1" id
  run_capture tsls functions get "$name" --json
  if [ "$RUN_RC" -eq 0 ]; then
    id="$(printf '%s' "$RUN_OUT" | jq -r .id)"
    e2e_log "function $name exists: $id"
  elif [ "$RUN_RC" -eq 2 ]; then
    id="$(tsls functions create --name "$name" --description "e2e demo" --json | jq -r .id)"
    e2e_log "function $name created: $id"
  else
    echo "unexpected exit $RUN_RC from functions get $name: $RUN_ERR" >&2
    return 1
  fi
  state_set "fn.$name" "$id"
}

create_functions() {
  ensure_function hello
  ensure_function http-axum
  ensure_function cpu-burn
}

# deploy_fn NAME BINARY [EXTRA ARGS...] -> prints revision id
deploy_fn() {
  local name="$1" binary="$2"
  shift 2
  tsls functions deploy --function "$name" --binary "$binary" --arch "$ARCH" --json "$@" | jq -r .id
}

deploy_hello() {
  local rev
  rev="$(deploy_fn hello "$GUEST_DIR/example-hello" --env GREETING=v1 --secret "DEMO_SECRET=$DEMO_SECRET_REF" --description v1)"
  [ -n "$rev" ] || return 1
  state_set rev.hello.v1 "$rev"
}

deploy_http_axum() {
  local rev
  rev="$(deploy_fn http-axum "$GUEST_DIR/example-http-axum" --description v1)"
  [ -n "$rev" ] || return 1
  state_set rev.http-axum "$rev"
}

deploy_cpu_burn() {
  local rev
  rev="$(deploy_fn cpu-burn "$GUEST_DIR/example-cpu-burn" --description v1)"
  [ -n "$rev" ] || return 1
  state_set rev.cpu-burn "$rev"
}

# ---------------------------------------------------------------------------
# 5. invoke assertions
# ---------------------------------------------------------------------------

invoke_hello_ok() {
  invoke_capture hello '{"name":"demo"}'
  printf '%s\n' "$INVOKE_ERR" >&2
  assert_eq 0 "$INVOKE_RC" "exit code" || { printf '%s\n' "$INVOKE_OUT"; return 1; }
  assert_json "$INVOKE_OUT" '.message' "hello, demo"
  assert_json "$INVOKE_OUT" '.secret_present' "true"
  assert_json "$INVOKE_OUT" '.greeting' "v1"
  [ -n "$INVOKE_ID" ] || { echo "no invocation id captured" >&2; return 1; }
  state_set inv.hello.ok "$INVOKE_ID"
}

invoke_hello_user_error() {
  invoke_capture hello '{"fail":true}'
  printf '%s\n' "$INVOKE_ERR" >&2
  assert_eq 3 "$INVOKE_RC" "exit code"
  assert_json "$INVOKE_OUT" '.error.code' "user_error"
}

invoke_hello_crash() {
  invoke_capture hello '{"panic":true}'
  printf '%s\n' "$INVOKE_ERR" >&2
  assert_eq 3 "$INVOKE_RC" "exit code"
  assert_json "$INVOKE_OUT" '.error.code' "crash"
}

invoke_hello_init_error() {
  local rev
  rev="$(deploy_fn hello "$GUEST_DIR/example-hello" --env HELLO_FAIL_INIT=1 --no-publish --description init-fail)"
  [ -n "$rev" ] || return 1
  invoke_capture hello '{"name":"init"}' --revision-id "$rev"
  printf '%s\n' "$INVOKE_ERR" >&2
  assert_eq 3 "$INVOKE_RC" "exit code"
  assert_json "$INVOKE_OUT" '.error.code' "init_error"
}

http_get_root() {
  local out
  out="$(tsls functions http http-axum --method GET --path / --json)"
  assert_json "$out" '.status' "200"
  assert_json "$out" '.body' "ok"
}

http_post_echo() {
  local out
  out="$(tsls functions http http-axum --method POST --path /echo --data 'hello-echo-body' --header 'content-type: text/plain' --json)"
  assert_json "$out" '.status' "200"
  assert_json "$out" '.body' "hello-echo-body"
}

http_status_404() {
  local out
  out="$(tsls functions http http-axum --method GET --path /status/404 --json)"
  assert_json "$out" '.status' "404"
}

http_headers() {
  local out body
  out="$(tsls functions http http-axum --method GET --path /headers --header 'x-demo-header: demo-value' --json)"
  assert_json "$out" '.status' "200"
  body="$(printf '%s' "$out" | jq -r .body)"
  assert_contains "$body" "demo-value" "custom request header echoed"
}

invoke_cpu_burn_ok() {
  invoke_capture cpu-burn '{"seconds":1}'
  printf '%s\n' "$INVOKE_ERR" >&2
  assert_eq 0 "$INVOKE_RC" "exit code" || { printf '%s\n' "$INVOKE_OUT"; return 1; }
}

invoke_cpu_burn_timeout() {
  local rev
  rev="$(deploy_fn cpu-burn "$GUEST_DIR/example-cpu-burn" --timeout-seconds 2 --no-publish --description timeout-2s)"
  [ -n "$rev" ] || return 1
  state_set rev.cpu-burn.timeout "$rev"
  invoke_capture cpu-burn '{"seconds":30,"ignore_sigterm":true}' --revision-id "$rev"
  printf '%s\n' "$INVOKE_ERR" >&2
  e2e_log "timeout invoke took ${INVOKE_MS} ms"
  assert_eq 4 "$INVOKE_RC" "exit code"
  assert_json "$INVOKE_OUT" '.error.code' "timeout"
  # Wall time includes environment boot (seconds on a microVM), so the budget
  # is provider dependent; the host-enforced deadline is checked below from
  # the invocation record itself.
  assert_lt "$INVOKE_MS" "$TIMEOUT_WALL_BUDGET_MS" "wall time < ${TIMEOUT_WALL_BUDGET_MS} ms"
  [ -n "$INVOKE_ID" ] || { echo "no invocation id" >&2; return 1; }
  state_set inv.cpu-burn.timeout "$INVOKE_ID"
  local detail exec_ms
  detail="$(tsls functions invocation "$INVOKE_ID" --json)"
  assert_json "$detail" '.status' "failed"
  assert_json "$detail" '.error.class' "timeout"
  # started_at -> finished_at must stay within timeout (2s) + cancel grace (1s) + slack.
  exec_ms="$(jq -r '
    def ms: sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601 * 1000;
    if .started_at and .finished_at then ((.finished_at | ms) - (.started_at | ms)) else -1 end' <<<"$detail")"
  e2e_log "host-enforced execution window: ${exec_ms} ms (timeout 2000 ms + grace 1000 ms)"
  [ "$exec_ms" -ge 0 ] || { echo "invocation has no started_at/finished_at" >&2; return 1; }
  assert_lt "$exec_ms" 8000 "started_at..finished_at < 8s"
  return 0
}

orphan_check() {
  # Give the host a moment to reap the killed environment.
  sleep 2
  "$SCRIPT_DIR/orphan-check.sh" "$TSLS_PROVIDER" "${TSLS_FC_RUN_DIR:-$REPO_ROOT/.kvm/run}"
}

logs_contain_handler_lines() {
  local id out
  id="$(state_get inv.hello.ok)"
  [ -n "$id" ] || { echo "no invocation id from invoke_hello_ok" >&2; return 1; }
  out="$(tsls functions logs --invocation "$id")"
  printf '%s\n' "$out"
  assert_contains "$out" "/handler]" "handler phase lines present"
  # Kept for the restart step (docs/adr/0018).
  printf '%s\n' "$out" > "$WORK_DIR/logs-before-restart.txt"
}

logs_survive_gateway_restart() {
  # docs/adr/0018: invocation logs live in <data_dir>/logs/logs.db, so a new
  # gateway process on the same data_dir serves exactly the lines the stopped
  # one served. Runs after the first gateway stopped; starts and stops its
  # own gateway (the step runs in a subshell).
  local id before after pid restart_log
  id="$(state_get inv.hello.ok)"
  [ -n "$id" ] || { echo "no invocation id from invoke_hello_ok" >&2; return 1; }
  [ -s "$WORK_DIR/logs-before-restart.txt" ] || { echo "no logs captured before the restart" >&2; return 1; }
  before="$(cat "$WORK_DIR/logs-before-restart.txt")"
  restart_log="$EVIDENCE_DIR/gateway-restart.log"
  cd "$REPO_ROOT"
  LOG_FORMAT=json TACHYON_GATEWAY_CONFIG="$CONFIG_PATH" \
    "$GATEWAY_BIN" "$GATEWAY_CONFIG_FLAG" "$CONFIG_PATH" >"$restart_log" 2>&1 &
  pid=$!
  # shellcheck disable=SC2064 # expand pid now: the trap runs in this subshell
  trap "stop_process $pid 15" EXIT
  GATEWAY_PID="$pid"
  GATEWAY_LOG="$restart_log"
  wait_gateway
  after="$(tsls functions logs --invocation "$id")"
  printf '%s\n' "$after"
  assert_contains "$after" "/handler]" "handler phase lines served after the restart"
  if [ "$before" != "$after" ]; then
    echo "logs differ after the restart" >&2
    diff <(printf '%s\n' "$before") <(printf '%s\n' "$after") >&2 || true
    return 1
  fi
  curl -fsS "$API_URL/readyz" | jq -e '.logs.durable == true and .logs.healthy == true' >/dev/null \
    || { echo "/readyz does not report a healthy durable log store" >&2; return 1; }
  stop_process "$pid" 15
  trap - EXIT
  assert_eq 0 "$STOP_RC" "restarted gateway exit status"
}

history_has_boot_evidence() {
  local id out
  id="$(state_get inv.hello.ok)"
  [ -n "$id" ] || { echo "no invocation id from invoke_hello_ok" >&2; return 1; }
  tsls functions invocation "$id"
  out="$(tsls functions invocation "$id" --json)"
  assert_json "$out" '.status' "succeeded"
  assert_json "$out" '.attempts | length > 0' "true"
  assert_json "$out" '.attempts[0].boot_evidence.host_pid != null' "true"
  if [ "$TSLS_PROVIDER" = "firecracker" ]; then
    assert_json "$out" '.attempts[0].boot_evidence.guest_boot_id != null' "true"
  fi
  assert_json "$out" '.attempts[0].timings.handler_ms != null' "true"
}

rollback_flow() {
  local rev
  rev="$(deploy_fn hello "$GUEST_DIR/example-hello" --env GREETING=v2 --secret "DEMO_SECRET=$DEMO_SECRET_REF" --description v2)"
  [ -n "$rev" ] || return 1
  invoke_capture hello '{"name":"v2"}'
  assert_eq 0 "$INVOKE_RC" "exit code" || { printf '%s\n' "$INVOKE_ERR"; return 1; }
  assert_json "$INVOKE_OUT" '.greeting' "v2"
  tsls functions rollback hello
  tsls functions aliases hello
  invoke_capture hello '{"name":"v1-again"}'
  assert_eq 0 "$INVOKE_RC" "exit code" || { printf '%s\n' "$INVOKE_ERR"; return 1; }
  assert_json "$INVOKE_OUT" '.greeting' "v1"
}

cross_tenant_is_404() {
  local id
  id="$(state_get fn.hello)"
  TSLS_TOKEN="$TSLS_TOKEN_B" run_capture tsls functions get "$id" --json
  printf '%s\n' "$RUN_ERR" >&2
  assert_eq 2 "$RUN_RC" "get exit code"
  assert_json "$RUN_OUT" '.error.code' "not_found"
  TSLS_TOKEN="$TSLS_TOKEN_B" run_capture tsls functions invoke "$id" --payload '{}' --json
  printf '%s\n' "$RUN_ERR" >&2
  assert_eq 2 "$RUN_RC" "invoke exit code"
  assert_json "$RUN_OUT" '.error.code' "not_found"
}

cancel_flow() {
  local id="" deadline out
  tsls functions invoke cpu-burn --payload '{"seconds":20}' --json \
    >"$WORK_DIR/cancel.out" 2>"$WORK_DIR/cancel.err" &
  local bg=$!
  BG_PIDS="$BG_PIDS $bg"
  deadline=$(( $(date +%s) + 20 ))
  while [ -z "$id" ]; do
    id="$(tsls functions invocations cpu-burn --limit 20 --json \
      | jq -r '(.items // .)[] | select(.status == "running" or .status == "queued" or .status == "accepted") | .id' \
      | head -n 1)"
    if [ -z "$id" ] && [ "$(date +%s)" -ge "$deadline" ]; then
      echo "no in-flight cpu-burn invocation found to cancel" >&2
      cat "$WORK_DIR/cancel.err" >&2 || true
      return 1
    fi
    [ -n "$id" ] || sleep 0.5
  done
  e2e_log "cancelling $id"
  tsls functions cancel "$id"
  set +e
  wait "$bg"
  local rc=$?
  set -e
  e2e_log "background invoke exited with $rc"
  cat "$WORK_DIR/cancel.err" >&2 || true
  out="$(tsls functions invocation "$id" --json)"
  assert_json "$out" '.status' "cancelled"
  state_set inv.cancelled "$id"
}

# ---------------------------------------------------------------------------
# 6. evidence
# ---------------------------------------------------------------------------

collect_evidence() {
  local ids="" name id
  for name in hello http-axum cpu-burn; do
    ids="$ids $(tsls functions invocations "$name" --limit 100 --json | jq -r '(.items // .)[] | .id')"
  done
  : > "$WORK_DIR/invocations.ndjson"
  for id in $ids; do
    tsls functions invocation "$id" --json >> "$WORK_DIR/invocations.ndjson"
  done
  jq -s '.' "$WORK_DIR/invocations.ndjson" > "$EVIDENCE_DIR/invocations.json"
  e2e_log "evidence: $(jq 'length' "$EVIDENCE_DIR/invocations.json") invocations written"
  for name in hello http-axum cpu-burn; do
    tsls functions revisions "$name" --json > "$EVIDENCE_DIR/revisions-$name.json" || true
    tsls functions aliases "$name" --json > "$EVIDENCE_DIR/aliases-$name.json" || true
  done
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

secrets_not_leaked() {
  # T03 / PLT-4623: resolved secret values must never reach the gateway log,
  # the evidence directory or the persisted ledger. The values themselves are
  # never printed.
  local values data_dir target v hits=0 checked=0
  values="$(sed -n 's/^value *= *"\(.*\)"$/\1/p' "$CONFIG_PATH")"
  if [ -z "$values" ]; then
    echo "no secret bindings in $CONFIG_PATH; nothing to check" >&2
    return 0
  fi
  data_dir="$(sed -n 's/^data_dir *= *"\(.*\)"$/\1/p' "$CONFIG_PATH" | head -n1)"
  data_dir="${data_dir:-./data}"
  case "$data_dir" in /*) ;; *) data_dir="$REPO_ROOT/${data_dir#./}" ;; esac
  while IFS= read -r v; do
    [ -n "$v" ] || continue
    # The ledger is state.db plus its write-ahead log (row bodies are plain
    # JSON text inside the pages); state.json only exists before its import.
    # Invocation logs are logs/logs.db plus its WAL (docs/adr/0018): the demo
    # functions never print their secret, so the host must not have either.
    for target in "$GATEWAY_LOG" "$EVIDENCE_DIR" "$data_dir/state.db" "$data_dir/state.db-wal" "$data_dir/state.json" \
      "$data_dir/logs/logs.db" "$data_dir/logs/logs.db-wal"; do
      [ -e "$target" ] || continue
      checked=$((checked + 1))
      if grep -arqF -- "$v" "$target"; then
        echo "a configured secret value was found in $target" >&2
        hits=$((hits + 1))
      fi
    done
  done <<<"$values"
  e2e_log "secret scan: $checked locations checked, $hits leaks"
  [ "$hits" -eq 0 ]
}

main() {
  e2e_log "run $RUN_ID (arch $ARCH, evidence $EVIDENCE_DIR)"
  if [ "$TSLS_PROVIDER" = "process" ]; then
    e2e_log "!! process provider: NO isolation; functions run as plain host processes (dev only)"
  fi

  step "build" build_all
  select_config
  start_gateway
  step "gateway healthz/readyz" wait_gateway
  step "tokens for tenant A and B" check_tokens
  step "provider capability table" show_provider
  step "create functions (idempotent)" create_functions
  step "deploy hello (GREETING=v1, DEMO_SECRET)" deploy_hello
  step "deploy http-axum" deploy_http_axum
  step "deploy cpu-burn" deploy_cpu_burn
  step "invoke hello ok (message, secret_present, greeting)" invoke_hello_ok
  step "invoke hello fail -> exit 3 user_error" invoke_hello_user_error
  step "invoke hello panic -> exit 3 crash" invoke_hello_crash
  step "init failure revision -> exit 3 init_error" invoke_hello_init_error
  step "http GET / -> 200 ok" http_get_root
  step "http POST /echo -> body echoed" http_post_echo
  step "http GET /status/404 -> 404 passthrough" http_status_404
  step "http GET /headers -> custom header visible" http_headers
  step "invoke cpu-burn 1s ok" invoke_cpu_burn_ok
  step "cpu-burn timeout 2s -> exit 4 timeout, wall < 10s" invoke_cpu_burn_timeout
  step "no orphan bridge / VMM processes" orphan_check
  step "logs contain handler lines" logs_contain_handler_lines
  step "history shows attempts with boot evidence" history_has_boot_evidence
  step "rollback: v2 -> rollback -> v1" rollback_flow
  step "cross-tenant get/invoke -> 404" cross_tenant_is_404
  step "cancel running invocation" cancel_flow
  step "collect evidence" collect_evidence

  local gw_rc
  stop_process "$GATEWAY_PID" 15
  gw_rc=$STOP_RC
  GATEWAY_PID=""
  e2e_log "gateway exit status $gw_rc"
  step "gateway stops on SIGTERM with exit 0" assert_eq 0 "$gw_rc" "gateway exit status"
  step "invocation logs survive a gateway restart" logs_survive_gateway_restart
  step "secret values absent from gateway log, evidence, state and logs.db" secrets_not_leaked
  step "no orphans after gateway shutdown" "$SCRIPT_DIR/orphan-check.sh" "$TSLS_PROVIDER" "${TSLS_FC_RUN_DIR:-$REPO_ROOT/.kvm/run}"

  steps_write_summary "$EVIDENCE_DIR/summary.json" \
    "$(jq -n --arg run "$RUN_ID" --arg provider "$TSLS_PROVIDER" --arg arch "$ARCH" \
        --arg host "$(uname -srm)" --arg config "$CONFIG_PATH" \
        '{run_id: $run, provider: $provider, architecture: $arch, host: $host, gateway_config: $config}')"
  e2e_log "summary: $EVIDENCE_DIR/summary.json"
  e2e_log "gateway log: $GATEWAY_LOG"
  steps_print_table
}

main "$@"

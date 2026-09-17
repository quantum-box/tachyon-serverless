#!/usr/bin/env bash
# scripts/console/e2e.sh - Playwright E2E of the Functions console against a real gateway (PLT-4644).
#
# Builds the gateway, tsls, the bridge and the example functions, builds the console
# (apps/console, static export), starts ONE gateway (process provider, scratch data_dir, dev-only
# SQLite queue so asynchronous invoke / dead letters / redrive work) that serves the console under
# /console/, seeds two tenants through the public API, and runs apps/console/e2e/*.spec.ts in
# headless Chromium:
#   sign-in, function list / detail navigation, deploy state (ready + failed revision), test
#   invoke success / error / async, OutcomeUnknown (mocked API response, see docs/console.md),
#   rollback / cancel / redrive confirmations and toasts, permission denied (operator token),
#   cross-tenant URLs (not found), provisional banner, budget unavailable, loading / empty /
#   error states (mocked), and no demo secret value or token in the DOM.
#
# Usage:
#   scripts/console/e2e.sh [--evidence DIR] [-- PLAYWRIGHT ARGS...]
#
# Environment:
#   TSLS_SKIP_BUILD=1          skip cargo build
#   TSLS_SKIP_CONSOLE_BUILD=1  skip pnpm install / build (apps/console/out must exist)
# Requires: cargo, curl, jq, python3, node + pnpm (or mise, which reads apps/console/mise.toml).
# Exit 0 only when every Playwright test passed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONSOLE_DIR="$REPO_ROOT/apps/console"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --evidence) EVIDENCE="$2"; shift 2 ;;
    -h | --help) sed -n '2,23p' "$0"; exit 0 ;;
    --) shift; break ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
# Anything after `--` goes to `playwright test` (e.g. `-- --grep rollback`).
EVIDENCE="${EVIDENCE:-$REPO_ROOT/target/console-e2e/console-$STAMP}"
mkdir -p "$EVIDENCE"
EVIDENCE="$(cd "$EVIDENCE" && pwd)"

for tool in cargo curl jq python3; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

# pnpm from PATH, or through mise with the console's pinned node / pnpm.
pnpm_console() {
  if command -v pnpm >/dev/null && command -v node >/dev/null && node -v >/dev/null 2>&1 && pnpm -v >/dev/null 2>&1; then
    (cd "$CONSOLE_DIR" && pnpm "$@")
  elif command -v mise >/dev/null; then
    (cd "$CONSOLE_DIR" && mise exec -- pnpm "$@")
  else
    echo "node + pnpm (or mise) are required for the console" >&2
    return 1
  fi
}

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-console-e2e.XXXXXX")"
GATEWAY_PID=""
log() { printf '[console-e2e] %s\n' "$*" >&2; }

# shellcheck disable=SC2317 # invoked by the EXIT trap
cleanup() {
  set +e
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill -TERM "$GATEWAY_PID" 2>/dev/null
    wait "$GATEWAY_PID" 2>/dev/null
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

case "$(uname -m)" in
  x86_64 | amd64) ARCH="x86_64" ;;
  aarch64 | arm64) ARCH="aarch64" ;;
  *) echo "unsupported host architecture $(uname -m)" >&2; exit 1 ;;
esac

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
HELLO_BIN="$REPO_ROOT/target/debug/example-hello"
CPU_BIN="$REPO_ROOT/target/debug/example-cpu-burn"

# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
cd "$REPO_ROOT"
if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  log "building the gateway, tsls, the bridge and the examples"
  cargo build -q -p tachyon-serverless-gateway -p tachyon-serverless-cli \
    -p tachyon-serverless-runtime-bridge -p example-hello -p example-cpu-burn
fi
for b in "$GATEWAY_BIN" "$TSLS_BIN" "$BRIDGE_BIN" "$HELLO_BIN" "$CPU_BIN"; do
  [ -x "$b" ] || { echo "missing binary: $b" >&2; exit 1; }
done
if [ "${TSLS_SKIP_CONSOLE_BUILD:-0}" != "1" ]; then
  log "installing and building the console"
  pnpm_console install --frozen-lockfile
  NEXT_TELEMETRY_DISABLED=1 pnpm_console build
fi
[ -f "$CONSOLE_DIR/out/index.html" ] || { echo "console not built: $CONSOLE_DIR/out/index.html" >&2; exit 1; }

# ---------------------------------------------------------------------------
# gateway
# ---------------------------------------------------------------------------
PORT="$(free_port)"
API="http://127.0.0.1:$PORT"
TENANT_A="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TENANT_B="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
# Throwaway credentials of this run only (never used anywhere else).
TOKEN_A="console-e2e-token-a-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
TOKEN_A_OPERATOR="console-e2e-token-a-operator-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
TOKEN_B="console-e2e-token-b-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
# The demo secret value the DOM must never contain.
SECRET_VALUE="console-e2e-secret-value-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"

mkdir -p "$WORK_DIR/data/process"
CONFIG="$WORK_DIR/gateway.toml"
cat >"$CONFIG" <<EOF
listen = "127.0.0.1:$PORT"
profile = "dev"
data_dir = "$WORK_DIR/data"

[provider]
kind = "process"

[provider.process]
bridge_binary = "$BRIDGE_BIN"
workdir = "$WORK_DIR/data/process"

[invoke]
cancel_grace_ms = 500

[dispatcher]
instance = "console-e2e"

[[identity.tokens]]
token = "$TOKEN_A"
tenant_id = "$TENANT_A"
subject = "console-e2e-a"
roles = ["deploy", "invoke", "redrive"]

[[identity.tokens]]
token = "$TOKEN_A_OPERATOR"
tenant_id = "$TENANT_A"
subject = "console-e2e-a-operator"
roles = ["operator"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B"
subject = "console-e2e-b"
roles = ["deploy", "invoke"]

[[secrets.bindings]]
tenant_id = "$TENANT_A"
binding_ref = "demo-secret"
value = "$SECRET_VALUE"

[queue]
backend = "sqlite"

[async_dispatch]
workers = 2
fetch_wait_ms = 200
ack_wait_seconds = 5
claim_ttl_seconds = 4
max_attempts = 2
backoff_initial_ms = 200
backoff_max_ms = 500
backoff_floor_ms = 100
retry_budget = 0
reaper_interval_seconds = 1

[usage]
collect_interval_ms = 300

# PLT-4643: both tenants need a budget entry (a tenant without one is refused, fail closed).
# Large hard limits never refuse; tenant A's tiny soft limit makes the alerts fire.
[budget]
enabled = true
file = "$WORK_DIR/budgets.toml"

[console]
enabled = true
dir = "$CONSOLE_DIR/out"
EOF
cat >"$WORK_DIR/budgets.toml" <<EOF
[[tenants]]
tenant_id = "$TENANT_A"
hard_limit_micros = 1000000000000
soft_limit_micros = 1
alert_thresholds_percent = [50, 100]

[[tenants]]
tenant_id = "$TENANT_B"
hard_limit_micros = 1000000000000
EOF

GATEWAY_LOG="$EVIDENCE/gateway.log"
log "starting the gateway on $API"
LOG_FORMAT=json "$GATEWAY_BIN" --config "$CONFIG" >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!
ready=0
for _ in $(seq 1 240); do
  kill -0 "$GATEWAY_PID" 2>/dev/null || { echo "gateway exited during startup" >&2; tail -n 30 "$GATEWAY_LOG" >&2; exit 1; }
  if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$API/readyz" || true)" = "200" ]; then
    ready=1
    break
  fi
  sleep 0.25
done
[ "$ready" = 1 ] || { echo "gateway not ready" >&2; tail -n 30 "$GATEWAY_LOG" >&2; exit 1; }

# ---------------------------------------------------------------------------
# seed through the public API
# ---------------------------------------------------------------------------
api() { # api TOKEN METHOD PATH [JSON]
  local token="$1" method="$2" path="$3" body="${4:-}"
  if [ -n "$body" ]; then
    curl -sS --max-time 60 -X "$method" -H "authorization: Bearer $token" -H 'content-type: application/json' \
      --data "$body" "$API$path"
  else
    curl -sS --max-time 60 -X "$method" -H "authorization: Bearer $token" "$API$path"
  fi
}
tsls_as() { # tsls_as TOKEN ARGS...
  local token="$1"
  shift
  TSLS_API_URL="$API" TSLS_TOKEN="$token" "$TSLS_BIN" "$@"
}

log "seeding tenant A"
HELLO_ID="$(tsls_as "$TOKEN_A" functions create --name hello --description 'console e2e: greeting function' --json | jq -r .id)"
CPU_ID="$(tsls_as "$TOKEN_A" functions create --name cpu-burn --description 'console e2e: long runner for cancel' --json | jq -r .id)"
REV_V1="$(tsls_as "$TOKEN_A" functions deploy --function hello --binary "$HELLO_BIN" --env GREETING=v1 \
  --secret DEMO_SECRET=demo-secret --description v1 --json | jq -r .id)"
REV_V2="$(tsls_as "$TOKEN_A" functions deploy --function hello --binary "$HELLO_BIN" --env GREETING=v2 \
  --secret DEMO_SECRET=demo-secret --description v2 --json | jq -r .id)"
# A revision the provider cannot run: an OCI image reference is accepted and then fails
# validation, so the deploy-state view has a `failed` revision with its reason.
REV_FAILED="$(api "$TOKEN_A" POST "/v1/functions/$HELLO_ID/revisions" \
  "{\"artifact\":{\"kind\":\"oci_image\",\"reference\":\"example.invalid/hello@sha256:$(printf '0%.0s' $(seq 1 64))\"},\"architecture\":\"$ARCH\",\"description\":\"oci (unsupported)\",\"publish_to_prod\":false}" | jq -r .id)"
for _ in $(seq 1 120); do
  status="$(api "$TOKEN_A" GET "/v1/functions/$HELLO_ID/revisions/$REV_FAILED" | jq -r .status)"
  [ "$status" = failed ] || [ "$status" = ready ] && break
  sleep 0.25
done
tsls_as "$TOKEN_A" functions deploy --function cpu-burn --binary "$CPU_BIN" --timeout-seconds 60 --description v1 --json >/dev/null

SEED_OK_ID="$(curl -sS -o /dev/null -D - --max-time 60 -X POST -H "authorization: Bearer $TOKEN_A" \
  -H 'content-type: application/json' --data '{"name":"seed"}' "$API/v1/functions/$HELLO_ID/invoke?alias=prod" \
  | tr -d '\r' | awk -F': ' 'tolower($1)=="x-tachyon-invocation-id"{print $2}')"

# One asynchronous invocation that fails every attempt and ends in a dead letter.
DLQ_INV="$(api "$TOKEN_A" POST "/v1/functions/$HELLO_ID/invokeAsync?alias=prod" '{"fail":true}' | jq -r .invocation_id)"
DEAD_LETTER_ID=""
for _ in $(seq 1 240); do
  DEAD_LETTER_ID="$(api "$TOKEN_A" GET "/v1/invocations/$DLQ_INV" | jq -r '.dispatch.dead_letter_id // empty')"
  [ -n "$DEAD_LETTER_ID" ] && break
  sleep 0.25
done
[ -n "$DEAD_LETTER_ID" ] || { echo "the failing asynchronous invocation did not reach a dead letter" >&2; exit 1; }

log "seeding tenant B"
B_FN_ID="$(tsls_as "$TOKEN_B" functions create --name tenant-b-private --description 'belongs to tenant B' --json | jq -r .id)"
tsls_as "$TOKEN_B" functions deploy --function tenant-b-private --binary "$HELLO_BIN" --description b1 --json >/dev/null
B_INV_ID="$(curl -sS -o /dev/null -D - --max-time 60 -X POST -H "authorization: Bearer $TOKEN_B" \
  -H 'content-type: application/json' --data '{"name":"b"}' "$API/v1/functions/$B_FN_ID/invoke?alias=prod" \
  | tr -d '\r' | awk -F': ' 'tolower($1)=="x-tachyon-invocation-id"{print $2}')"

SEED="$WORK_DIR/seed.json"
jq -n \
  --arg baseUrl "$API" --arg tokenA "$TOKEN_A" --arg tokenOperator "$TOKEN_A_OPERATOR" --arg tokenB "$TOKEN_B" \
  --arg tenantA "$TENANT_A" --arg tenantB "$TENANT_B" --arg secretValue "$SECRET_VALUE" \
  --arg helloId "$HELLO_ID" --arg cpuId "$CPU_ID" --arg revV1 "$REV_V1" --arg revV2 "$REV_V2" \
  --arg revFailed "$REV_FAILED" --arg seedOk "$SEED_OK_ID" --arg dlqInvocation "$DLQ_INV" \
  --arg deadLetterId "$DEAD_LETTER_ID" --arg bFunctionId "$B_FN_ID" --arg bInvocationId "$B_INV_ID" \
  '{baseUrl:$baseUrl, tokens:{a:$tokenA, operator:$tokenOperator, b:$tokenB}, tenants:{a:$tenantA, b:$tenantB},
    secretValue:$secretValue, ids:{hello:$helloId, cpuBurn:$cpuId, revV1:$revV1, revV2:$revV2, revFailed:$revFailed,
    seedInvocation:$seedOk, dlqInvocation:$dlqInvocation, deadLetter:$deadLetterId, bFunction:$bFunctionId,
    bInvocation:$bInvocationId}}' >"$SEED"
# Evidence gets the ids, never the tokens or the secret value.
jq '{baseUrl, tenants, ids}' "$SEED" >"$EVIDENCE/seed-ids.json"

# ---------------------------------------------------------------------------
# Playwright
# ---------------------------------------------------------------------------
log "running Playwright (chromium, headless)"
mkdir -p "$EVIDENCE/screenshots"
rc=0
CONSOLE_E2E_SEED="$SEED" CONSOLE_E2E_EVIDENCE="$EVIDENCE" \
  pnpm_console exec playwright test "$@" 2>&1 | tee "$EVIDENCE/playwright.txt" || rc=$?
# `tee` hides the exit status without pipefail's help on some shells; re-check the JSON report.
if [ -f "$EVIDENCE/results.json" ]; then
  unexpected="$(jq '.stats.unexpected + .stats.flaky' "$EVIDENCE/results.json")"
  [ "$unexpected" = 0 ] || rc=1
else
  rc=1
fi

# The run's secret value and tokens must not appear in anything kept as evidence.
if grep -rIl -e "$SECRET_VALUE" -e "$TOKEN_A" -e "$TOKEN_B" -e "$TOKEN_A_OPERATOR" "$EVIDENCE" >/dev/null 2>&1; then
  echo "a token or the secret value leaked into the evidence directory" >&2
  grep -rIl -e "$SECRET_VALUE" -e "$TOKEN_A" -e "$TOKEN_B" -e "$TOKEN_A_OPERATOR" "$EVIDENCE" >&2 || true
  rc=1
fi

{
  echo "run: $STAMP"
  echo "host: $(uname -srm)"
  echo "gateway: process provider, [queue] backend = sqlite, [budget] enabled (file), [console] enabled"
  echo "playwright: $(pnpm_console exec playwright --version 2>/dev/null || echo unknown)"
  echo "node: $(cd "$CONSOLE_DIR" && (node -v 2>/dev/null || mise exec -- node -v))"
  if [ -f "$EVIDENCE/results.json" ]; then
    jq -r '"tests: expected=\(.stats.expected) unexpected=\(.stats.unexpected) flaky=\(.stats.flaky) skipped=\(.stats.skipped)"' "$EVIDENCE/results.json"
  fi
  echo "exit: $rc"
} | tee "$EVIDENCE/summary.txt"
exit "$rc"

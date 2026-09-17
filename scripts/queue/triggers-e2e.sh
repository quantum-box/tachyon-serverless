#!/usr/bin/env bash
# scripts/queue/triggers-e2e.sh - cron and signed webhook triggers end to end (PLT-4641).
#
# Starts a gateway (process provider, durable ledger, embedded SQLite queue, [triggers] with a
# sealing key) on a free port and a scratch data_dir, deploys example-hello, and checks with the
# real binaries (`tsls triggers ...`, curl, openssl):
#   1. cron:     a 6-field `*/2 * * * * *` trigger fires; every fire is an async invocation with
#                the idempotency key cron:{trigger}:{scheduled_at}; no scheduled time twice
#   2. restart:  the gateway is SIGKILLed and restarted on the same data_dir after a downtime,
#                then stopped with SIGTERM and restarted at once; fires resume, no scheduled time
#                is recorded or invoked twice, and (policy `skip`) nothing inside the downtime runs
#   3. disable:  after `tsls triggers update --disable` no new fire and no new invocation
#   4. webhook:  HMAC computed with openssl (and cross-checked with `tsls triggers webhook-sign`):
#                valid 202; the same event id again -> 202 with the same invocation; a bad
#                signature, an expired timestamp (401) and an oversized body (413) leave no
#                invocation and no fire row; a missing event id is 400; disabled 410; deleted 404
#   5. dispatch: every cron and webhook fire is executed by the asynchronous dispatcher (PLT-4640)
#                and reaches a terminal state, and a webhook fire whose handler always fails
#                (examples/idempotent-async without a top-level order_id) is retried and ends in
#                one `attempts_exhausted` dead letter through the common path; the runs are metered
#                (`AttemptSettled`, PLT-4642)
# and writes key=value results plus logs to the evidence directory.
#
# Fires are accepted like invokeAsync (the shared acceptance path) and run by the same dispatcher;
# the trigger service has no retry code of its own.
#
# Usage:
#   scripts/queue/triggers-e2e.sh [--evidence DIR]
#
# Environment: TSLS_SKIP_BUILD=1 skips cargo build. TSLS_PROVIDER=process (default) | firecracker
# (scripts/kvm/provider-lib.sh; run as root, e.g. `sudo -n env PATH="$PATH" HOME="$HOME"
# TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 scripts/queue/triggers-e2e.sh`): every fire runs in a
# jailed microVM with the warm pool on, and each attempt's environment is recorded and checked.
# Exit 0 only when every check passed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/kvm/provider-lib.sh
. "$REPO_ROOT/scripts/kvm/provider-lib.sh"
provider_init "$REPO_ROOT"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --evidence) EVIDENCE="$2"; shift 2 ;;
    -h | --help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

EVIDENCE="${EVIDENCE:-$REPO_ROOT/target/queue/triggers-e2e-$STAMP}"
mkdir -p "$EVIDENCE"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-triggers.XXXXXX")"
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

GATEWAY_PORT="$(free_port)"
API="http://127.0.0.1:$GATEWAY_PORT"
TOKEN="triggers-e2e-token-a"
TENANT="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS="$REPO_ROOT/target/debug/tsls"
BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
HELLO_BIN="$GUEST_DIR/example-hello"
GATEWAY_LOG="$EVIDENCE/gateway.log"
RESULTS="$EVIDENCE/results.txt"
DB="$WORK_DIR/data/state.db"
: >"$RESULTS"
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
  if provider_is_fc; then provider_leftovers >"$EVIDENCE/leftovers-after.txt" 2>&1; fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

log() { printf '[triggers-e2e] %s\n' "$*" >&2; }
record() { # name ok|FAIL detail
  printf '%-44s %s %s\n' "$1" "$2" "${3:-}" | tee -a "$RESULTS"
  if [ "$2" != ok ]; then FAILED=1; fi
}
check() { # name condition-exit-status detail
  if [ "$2" = 0 ]; then record "$1" ok "${3:-}"; else record "$1" FAIL "${3:-}"; fi
}

for tool in curl jq openssl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# build and configuration
# ---------------------------------------------------------------------------

cd "$REPO_ROOT"
if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  log "building the gateway, tsls, the bridge and example-hello"
  cargo build -q -p tachyon-serverless-gateway -p tachyon-serverless-cli \
    -p tachyon-serverless-runtime-bridge -p example-hello -p example-idempotent-async
  if provider_is_fc; then
    cargo build -q --release --target "$(uname -m)-unknown-linux-musl" -p example-hello -p example-idempotent-async
  fi
fi
FAILING_BIN="$GUEST_DIR/example-idempotent-async"
for b in "$GATEWAY_BIN" "$TSLS" "$BRIDGE_BIN" "$HELLO_BIN" "$FAILING_BIN"; do
  [ -x "$b" ] || { echo "missing binary: $b" >&2; exit 1; }
done

mkdir -p "$WORK_DIR/data"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$WORK_DIR/triggers.key"
umask 022
POOL_TOML=""
if provider_is_fc; then
  # A cold microVM boot takes seconds on a nested host; the pool keeps the 2 s cron warm.
  POOL_TOML=$'[pool]\nenabled = true\nmax_idle_per_revision = 2\nidle_ttl_seconds = 300'
fi
CONFIG="$WORK_DIR/gateway.toml"
cat >"$CONFIG" <<EOF
listen = "127.0.0.1:$GATEWAY_PORT"
profile = "dev"
data_dir = "$WORK_DIR/data"

$(provider_toml "$WORK_DIR/data" "$BRIDGE_BIN")

$POOL_TOML

[dispatcher]
instance = "triggers-e2e"
lease_ttl_seconds = 6
heartbeat_interval_seconds = 2

[[identity.tokens]]
token = "$TOKEN"
tenant_id = "$TENANT"
subject = "triggers-e2e"
roles = ["deploy", "invoke"]

[queue]
backend = "sqlite"

[triggers]
scheduler_interval_ms = 200
grace_seconds = 1
max_catchup_seconds = 600
fire_retention_seconds = 3600
webhook_max_body_bytes = 4096
secret_key_file = "$WORK_DIR/triggers.key"

# PLT-4640: fires are executed by the asynchronous dispatcher; short retries for the failing fire.
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
EOF

start_gateway() {
  printf '\n===== gateway start =====\n' >>"$GATEWAY_LOG"
  LOG_FORMAT=json "$GATEWAY_BIN" --config "$CONFIG" >>"$GATEWAY_LOG" 2>&1 &
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

stop_gateway() { # stop_gateway TERM|KILL
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill "-$1" "$GATEWAY_PID"
    wait "$GATEWAY_PID" 2>/dev/null || true
  fi
  GATEWAY_PID=""
}

tsls() { "$TSLS" --api-url "$API" --token "$TOKEN" "$@"; }

HTTP_CODE=""
HTTP_BODY=""
# api METHOD PATH [BODY_FILE]
api() {
  local out="$WORK_DIR/http.out"
  : >"$out"
  local args=(-s -o "$out" -w '%{http_code}' --max-time 30 -X "$1" -H "authorization: Bearer $TOKEN")
  if [ -n "${3:-}" ]; then args+=(-H 'content-type: application/json' --data-binary "@$3"); fi
  HTTP_CODE="$(curl "${args[@]}" "$API$2" || true)"
  HTTP_BODY="$(cat "$out")"
}

# sql QUERY -> rows as `a|b|c` (python3's sqlite3: no sqlite3 CLI needed)
sql() {
  python3 - "$DB" "$1" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=10)
for row in con.execute(sys.argv[2]):
    print("|".join("" if v is None else str(v) for v in row))
PY
}

# ---------------------------------------------------------------------------
# deploy
# ---------------------------------------------------------------------------

start_gateway
printf '{"name":"trigger-hello","description":"PLT-4641 triggers e2e"}' >"$WORK_DIR/fn.json"
api POST /v1/functions "$WORK_DIR/fn.json"
FUNCTION_ID="$(printf '%s' "$HTTP_BODY" | jq -r .id)"
DIGEST="$(curl -s --max-time 30 -X POST -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/octet-stream' --data-binary "@$HELLO_BIN" "$API/v1/artifacts" | jq -r .digest)"
case "$(uname -m)" in arm64 | aarch64) ARCH=aarch64 ;; *) ARCH=x86_64 ;; esac
printf '{"artifact":{"kind":"binary","digest":"%s"},"architecture":"%s","publish_to_prod":true}' \
  "$DIGEST" "$ARCH" >"$WORK_DIR/rev.json"
api POST "/v1/functions/$FUNCTION_ID/revisions" "$WORK_DIR/rev.json"
REVISION_ID="$(printf '%s' "$HTTP_BODY" | jq -r .id)"
for _ in $(seq 1 100); do
  api GET "/v1/functions/$FUNCTION_ID/revisions/$REVISION_ID"
  [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = ready ] && break
  sleep 0.1
done
if [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = ready ]; then rc=0; else rc=1; fi
check deploy.revision_ready "$rc" "$REVISION_ID"

# ---------------------------------------------------------------------------
# 1. cron
# ---------------------------------------------------------------------------

tsls --json triggers create "$FUNCTION_ID" --name every-2s --kind cron \
  --schedule '*/2 * * * * *' --timezone Asia/Tokyo --payload '{"job":"e2e"}' \
  --missed-run skip >"$WORK_DIR/cron.json"
CRON_ID="$(jq -r .id "$WORK_DIR/cron.json")"
if [ "$(jq -r .cron.next_fire_at "$WORK_DIR/cron.json")" != null ]; then rc=0; else rc=1; fi
check cron.created_via_tsls "$rc" "$CRON_ID next=$(jq -r .cron.next_fire_at "$WORK_DIR/cron.json")"

sleep 9
fires_accepted() { sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$CRON_ID' AND outcome = 'accepted'"; }
distinct_times() { sql "SELECT COUNT(DISTINCT scheduled_at) FROM trigger_fires WHERE trigger_id = '$CRON_ID'"; }
cron_invocations() { sql "SELECT COUNT(*) FROM invocations WHERE body LIKE '%\"idempotency_key\":\"cron:$CRON_ID:%'"; }
async_cron_invocations() {
  sql "SELECT COUNT(*) FROM trigger_fires f JOIN invocations i ON i.id = f.invocation_id WHERE f.trigger_id = '$CRON_ID' AND i.body LIKE '%\"mode\":\"async\"%' AND i.revision_id = '$REVISION_ID'"
}
n1="$(fires_accepted)"
if [ "$n1" -ge 3 ]; then rc=0; else rc=1; fi
check cron.fired "$rc" "fires=$n1 after 9s"
if [ "$(async_cron_invocations)" = "$n1" ]; then rc=0; else rc=1; fi
check cron.each_fire_is_an_async_invocation_of_the_pinned_revision "$rc" "invocations=$(async_cron_invocations)"
if [ "$(cron_invocations)" = "$n1" ] && [ "$(distinct_times)" = "$n1" ]; then rc=0; else rc=1; fi
check cron.idempotency_key_per_scheduled_time "$rc" "keys=$(cron_invocations) distinct_times=$(distinct_times)"
if tsls triggers fires "$FUNCTION_ID" "$CRON_ID" --limit 5 >"$EVIDENCE/cron-fires-before-restart.txt"; then rc=0; else rc=1; fi
check cron.fires_listed_via_tsls "$rc" "$(wc -l <"$EVIDENCE/cron-fires-before-restart.txt" | tr -d ' ') lines"

# ---------------------------------------------------------------------------
# 2. restart: SIGKILL + downtime, then SIGTERM + immediate restart
# ---------------------------------------------------------------------------

stop_gateway KILL
killed_at="$(date -u +%s)"
sleep 6
start_gateway
restarted_at="$(date -u +%s)"
sleep 6
stop_gateway TERM
start_gateway
sleep 5
n2="$(fires_accepted)"
if [ "$n2" -gt "$n1" ]; then rc=0; else rc=1; fi
check restart.fires_resume "$rc" "before=$n1 after=$n2"
if [ "$(distinct_times)" = "$n2" ] && [ "$(cron_invocations)" = "$n2" ]; then rc=0; else rc=1; fi
check restart.no_scheduled_time_twice "$rc" "fires=$n2 distinct_times=$(distinct_times) invocations=$(cron_invocations)"
dup_keys="$(sql "SELECT COUNT(*) FROM (SELECT fire_key FROM trigger_fires GROUP BY trigger_id, fire_key HAVING COUNT(*) > 1)")"
if [ "$dup_keys" = 0 ]; then rc=0; else rc=1; fi
check restart.unique_fire_rows "$rc" "duplicate keys=$dup_keys"
# skip policy: times strictly inside the downtime (with 2 s of margin each side) never ran
inside="$(python3 - "$DB" "$CRON_ID" "$killed_at" "$restarted_at" <<'PY'
import sqlite3, sys, datetime
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
lo, hi = int(sys.argv[3]) + 2, int(sys.argv[4]) - 2
n = 0
for (s,) in con.execute("SELECT scheduled_at FROM trigger_fires WHERE trigger_id = ?", (sys.argv[2],)):
    t = int(datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp())
    if lo < t < hi:
        n += 1
print(n)
PY
)"
if [ "$inside" = 0 ]; then rc=0; else rc=1; fi
check restart.skip_policy_skips_the_downtime "$rc" "fired_inside_downtime=$inside window=$killed_at..$restarted_at"
skipped="$(grep -c 'late scheduled times skipped' "$GATEWAY_LOG" || true)"
record restart.skipped_logged ok "log_lines=$skipped"
owners="$(sql "SELECT owner_id FROM trigger_scheduler")"
record restart.scheduler_lease_owner ok "$owners"

# ---------------------------------------------------------------------------
# 3. disable
# ---------------------------------------------------------------------------

tsls --json triggers update "$FUNCTION_ID" "$CRON_ID" --disable >"$WORK_DIR/disabled.json"
if [ "$(jq -r .status "$WORK_DIR/disabled.json")" = disabled ]; then rc=0; else rc=1; fi
check disable.status "$rc" "$(jq -r '.status + " next_fire_at=" + (.cron.next_fire_at // "null")' "$WORK_DIR/disabled.json")"
sleep 1
n3="$(fires_accepted)"
i3="$(sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$FUNCTION_ID'")"
sleep 5
n4="$(fires_accepted)"
i4="$(sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$FUNCTION_ID'")"
if [ "$n3" = "$n4" ] && [ "$i3" = "$i4" ]; then rc=0; else rc=1; fi
check disable.no_new_fires "$rc" "fires $n3 -> $n4, invocations $i3 -> $i4 over 5s"

# ---------------------------------------------------------------------------
# 4. webhook
# ---------------------------------------------------------------------------

tsls --json triggers create "$FUNCTION_ID" --name orders --kind webhook --max-body-bytes 1024 \
  >"$WORK_DIR/hook.json"
HOOK_ID="$(jq -r .id "$WORK_DIR/hook.json")"
SECRET="$(jq -r .secret "$WORK_DIR/hook.json")"
if case "$SECRET" in whsec_*) true ;; *) false ;; esac; then rc=0; else rc=1; fi
check webhook.secret_shown_once_on_create "$rc" "fingerprint=$(jq -r .webhook.secret_fingerprint "$WORK_DIR/hook.json")"
tsls --json triggers get "$FUNCTION_ID" "$HOOK_ID" >"$WORK_DIR/hook-get.json"
if grep -q "${SECRET#whsec_}" "$WORK_DIR/hook-get.json"; then rc=1; else rc=0; fi
check webhook.secret_never_returned_again "$rc" "get has secret field: $(jq 'has("secret")' "$WORK_DIR/hook-get.json")"

sign() { # sign TIMESTAMP BODY_FILE -> v1=<hex>
  { printf '%s.' "$1"; cat "$2"; } | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= *//;s/^/v1=/'
}
# deliver TIMESTAMP SIGNATURE EVENT_ID BODY_FILE
deliver() {
  local out="$WORK_DIR/hook.out" args
  : >"$out"
  args=(-s -o "$out" -w '%{http_code}' --max-time 30 -X POST -H 'content-type: application/json'
    -H "x-tachyon-webhook-timestamp: $1" -H "x-tachyon-webhook-signature: $2")
  if [ -n "$3" ]; then args+=(-H "x-tachyon-webhook-id: $3"); fi
  args+=(--data-binary "@$4")
  HTTP_CODE="$(curl "${args[@]}" "$API/v1/hooks/$HOOK_ID" || true)"
  HTTP_BODY="$(cat "$out")"
}
hook_rows() {
  printf '%s/%s' "$(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$HOOK_ID'")" \
    "$(sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$FUNCTION_ID'")"
}

printf '{"order":42,"items":["a","b"]}' >"$WORK_DIR/body.json"
now="$(date -u +%s)"
sig="$(sign "$now" "$WORK_DIR/body.json")"
SECRET_ENV_VALUE="$SECRET" "$TSLS" --json triggers webhook-sign --secret-env SECRET_ENV_VALUE \
  --timestamp "$now" --body-file "$WORK_DIR/body.json" >"$WORK_DIR/tsls-sign.json"
if [ "$(jq -r .signature "$WORK_DIR/tsls-sign.json")" = "$sig" ]; then rc=0; else rc=1; fi
check webhook.openssl_and_tsls_signatures_agree "$rc" ""

before="$(hook_rows)"
deliver "$now" "v1=$(printf '0%.0s' $(seq 1 64))" evt-1 "$WORK_DIR/body.json"
bad_code="$HTTP_CODE"
deliver "$((now - 3600))" "$(sign "$((now - 3600))" "$WORK_DIR/body.json")" evt-1 "$WORK_DIR/body.json"
old_code="$HTTP_CODE"
python3 -c 'print("{\"blob\":\"" + "x" * 2000 + "\"}")' >"$WORK_DIR/big.json"
deliver "$now" "$(sign "$now" "$WORK_DIR/big.json")" evt-1 "$WORK_DIR/big.json"
big_code="$HTTP_CODE"
deliver "$now" "$sig" "" "$WORK_DIR/body.json"
noid_code="$HTTP_CODE"
after="$(hook_rows)"
if [ "$bad_code" = 401 ]; then rc=0; else rc=1; fi
check webhook.invalid_signature_401 "$rc" "code=$bad_code"
if [ "$old_code" = 401 ]; then rc=0; else rc=1; fi
check webhook.expired_timestamp_401 "$rc" "code=$old_code"
if [ "$big_code" = 413 ]; then rc=0; else rc=1; fi
check webhook.oversized_body_413 "$rc" "code=$big_code"
if [ "$noid_code" = 400 ]; then rc=0; else rc=1; fi
check webhook.missing_event_id_400 "$rc" "code=$noid_code"
if [ "$before" = "$after" ]; then rc=0; else rc=1; fi
check webhook.refusals_store_nothing "$rc" "fires/invocations before=$before after=$after"

deliver "$now" "$sig" evt-1 "$WORK_DIR/body.json"
first_code="$HTTP_CODE"
first_inv="$(printf '%s' "$HTTP_BODY" | jq -r .invocation_id)"
if [ "$first_code" = 202 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .replayed)" = false ]; then rc=0; else rc=1; fi
check webhook.valid_202 "$rc" "code=$first_code invocation=$first_inv"
later="$((now + 2))"
deliver "$later" "$(sign "$later" "$WORK_DIR/body.json")" evt-1 "$WORK_DIR/body.json"
if [ "$HTTP_CODE" = 202 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .invocation_id)" = "$first_inv" ] \
  && [ "$(printf '%s' "$HTTP_BODY" | jq -r .replayed)" = true ]; then rc=0; else rc=1; fi
check webhook.duplicate_event_same_invocation "$rc" "code=$HTTP_CODE $(printf '%s' "$HTTP_BODY" | jq -c '{invocation_id, replayed}')"
deliver "$now" "$sig" evt-forged "$WORK_DIR/body.json"
if [ "$HTTP_CODE" = 202 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .invocation_id)" = "$first_inv" ]; then rc=0; else rc=1; fi
check webhook.replayed_signature_new_event_id_same_invocation "$rc" "code=$HTTP_CODE"
if [ "$(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$HOOK_ID'")" = 1 ]; then rc=0; else rc=1; fi
check webhook.one_fire_row "$rc" "rows=$(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$HOOK_ID'")"
api GET "/v1/invocations/$first_inv"
if [ "$HTTP_CODE" = 200 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .mode)" = async ]; then rc=0; else rc=1; fi
check webhook.invocation_readable_async "$rc" "$(printf '%s' "$HTTP_BODY" | jq -c '{status, mode, revision_id}')"

tsls triggers update "$FUNCTION_ID" "$HOOK_ID" --disable >/dev/null
deliver "$now" "$(sign "$now" "$WORK_DIR/body.json")" evt-2 "$WORK_DIR/body.json"
if [ "$HTTP_CODE" = 410 ]; then rc=0; else rc=1; fi
check webhook.disabled_410 "$rc" "code=$HTTP_CODE"
tsls triggers delete "$FUNCTION_ID" "$HOOK_ID" >/dev/null
deliver "$now" "$(sign "$now" "$WORK_DIR/body.json")" evt-3 "$WORK_DIR/body.json"
if [ "$HTTP_CODE" = 404 ]; then rc=0; else rc=1; fi
check webhook.deleted_404 "$rc" "code=$HTTP_CODE"
if [ "$(sql "SELECT COUNT(*) FROM triggers WHERE id = '$HOOK_ID' AND secret_sealed IS NULL")" = 1 ]; then rc=0; else rc=1; fi
check webhook.deleted_secret_erased "$rc" ""

# ---------------------------------------------------------------------------
# 5. dispatch (PLT-4640): fires run through the common asynchronous path
# ---------------------------------------------------------------------------

# wait_all_terminal FUNCTION_ID SECONDS -> 0 when no invocation of the function is non-terminal
wait_all_terminal() {
  local deadline=$((SECONDS + $2)) open
  while :; do
    open="$(sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$1' AND terminal = 0")"
    [ "$open" = 0 ] && return 0
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}
if wait_all_terminal "$FUNCTION_ID" 90; then rc=0; else rc=1; fi
total="$(sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$FUNCTION_ID'")"
succeeded="$(sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$FUNCTION_ID' AND status = 'succeeded'")"
check dispatch.every_fire_terminal "$rc" "invocations=$total succeeded=$succeeded"
cron_done="$(sql "SELECT COUNT(*) FROM trigger_fires f JOIN invocations i ON i.id = f.invocation_id WHERE f.trigger_id = '$CRON_ID' AND i.status = 'succeeded'")"
if [ "$cron_done" -ge 1 ] && [ "$cron_done" = "$(fires_accepted)" ]; then rc=0; else rc=1; fi
check dispatch.cron_fires_succeeded "$rc" "succeeded=$cron_done of $(fires_accepted)"
api GET "/v1/invocations/$first_inv"
if [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = succeeded ] \
  && [ "$(printf '%s' "$HTTP_BODY" | jq -r .output.message)" = "hello, world" ]; then rc=0; else rc=1; fi
check dispatch.webhook_fire_succeeded "$rc" "$(printf '%s' "$HTTP_BODY" | jq -c '{status, dispatch: .dispatch.state, attempts: (.attempts | length)}')"

printf '{"name":"trigger-failing","description":"PLT-4640 failing trigger"}' >"$WORK_DIR/fn2.json"
api POST /v1/functions "$WORK_DIR/fn2.json"
FAILING_ID="$(printf '%s' "$HTTP_BODY" | jq -r .id)"
DIGEST2="$(curl -s --max-time 30 -X POST -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/octet-stream' --data-binary "@$FAILING_BIN" "$API/v1/artifacts" | jq -r .digest)"
printf '{"artifact":{"kind":"binary","digest":"%s"},"architecture":"%s","env_vars":[["IDEMPOTENT_ASYNC_DIR","%s"]],"publish_to_prod":true}' \
  "$DIGEST2" "$ARCH" "$WORK_DIR/effects" >"$WORK_DIR/rev2.json"
api POST "/v1/functions/$FAILING_ID/revisions" "$WORK_DIR/rev2.json"
REVISION2_ID="$(printf '%s' "$HTTP_BODY" | jq -r .id)"
for _ in $(seq 1 100); do
  api GET "/v1/functions/$FAILING_ID/revisions/$REVISION2_ID"
  [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = ready ] && break
  sleep 0.1
done
tsls --json triggers create "$FAILING_ID" --name always-fails --kind webhook >"$WORK_DIR/hook2.json"
HOOK_ID="$(jq -r .id "$WORK_DIR/hook2.json")"
SECRET="$(jq -r .secret "$WORK_DIR/hook2.json")"
# The handler reads `order_id` at the top level; a webhook wraps the body, so every run fails
# with the retryable handler error Order.InvalidKey.
printf '{"order_id":"o-1"}' >"$WORK_DIR/fail.json"
now="$(date -u +%s)"
deliver "$now" "$(sign "$now" "$WORK_DIR/fail.json")" evt-fail "$WORK_DIR/fail.json"
FAIL_INV="$(printf '%s' "$HTTP_BODY" | jq -r .invocation_id)"
if [ "$HTTP_CODE" = 202 ]; then rc=0; else rc=1; fi
check dispatch.failing_webhook_accepted "$rc" "code=$HTTP_CODE invocation=$FAIL_INV"
if wait_all_terminal "$FAILING_ID" 60; then rc=0; else rc=1; fi
api GET "/v1/invocations/$FAIL_INV"
fail_view="$(printf '%s' "$HTTP_BODY" | jq -c '{status, error: .error.error_type, attempts: (.attempts | length), dispatch: .dispatch.state, dead_letter: .dispatch.dead_letter_id}')"
if [ "$rc" = 0 ] && [ "$(printf '%s' "$HTTP_BODY" | jq -r .status)" = failed ] \
  && [ "$(printf '%s' "$HTTP_BODY" | jq -r '.attempts | length')" = 2 ] \
  && [ "$(printf '%s' "$HTTP_BODY" | jq -r .error.error_type)" = Order.InvalidKey ]; then rc=0; else rc=1; fi
check dispatch.failing_fire_retried_then_failed "$rc" "$fail_view"
api GET "/v1/functions/$FAILING_ID/dead-letters"
printf '%s\n' "$HTTP_BODY" >"$EVIDENCE/dead-letters.json"
if [ "$(printf '%s' "$HTTP_BODY" | jq -r '[.items[] | select(.invocation_id == "'"$FAIL_INV"'" and .reason == "attempts_exhausted")] | length')" = 1 ] \
  && [ "$(printf '%s' "$HTTP_BODY" | jq -r '.items | length')" = 1 ]; then rc=0; else rc=1; fi
check dispatch.failing_fire_one_dead_letter "$rc" "$(printf '%s' "$HTTP_BODY" | jq -c '[.items[] | {reason, attempts}]')"
# Metering (PLT-4642): every run of a fire emits AttemptSettled, first then retry.
metered=""
for _ in $(seq 1 40); do
  api GET "/v1/usage?group_by=function"
  metered="$(printf '%s' "$HTTP_BODY" | jq -r '.totals.usage.attempts // 0')"
  [ "$metered" -ge $((total + 2)) ] 2>/dev/null && break
  sleep 0.5
done
if [ "${metered:-0}" -ge $((total + 2)) ] 2>/dev/null; then rc=0; else rc=1; fi
check dispatch.fire_runs_metered "$rc" "usage attempts=$metered (runs >= $((total + 2)))"

if provider_is_fc; then
  # Every attempt of every fire that reached an environment ran in a jailed Firecracker microVM.
  : >"$EVIDENCE/attempt-environments.txt"
  for inv in $(sql "SELECT id FROM invocations WHERE function_id IN ('$FUNCTION_ID', '$FAILING_ID') ORDER BY accepted_at"); do
    api GET "/v1/invocations/$inv"
    printf '%s' "$HTTP_BODY" | jq -r --arg id "$inv" '.attempts[] | select(.environment_id != null) |
      [$id, .id, .status, .start_kind, .environment_id, (.boot_evidence.details.provider // "-"),
       (.boot_evidence.details.jailed // "-"), (.boot_evidence.guest_boot_id // "-")] | @tsv' \
      >>"$EVIDENCE/attempt-environments.txt"
  done
  n_att="$(wc -l <"$EVIDENCE/attempt-environments.txt" | tr -d ' ')"
  not_vm="$(awk -F'\t' '$6 != "firecracker" || $7 != "true" || $8 == "-"' "$EVIDENCE/attempt-environments.txt" | wc -l | tr -d ' ')"
  if [ "$not_vm" = 0 ] && [ "$n_att" -gt 0 ]; then rc=0; else rc=1; fi
  check microvm.every_fire_attempt_in_a_jailed_firecracker_vm "$rc" \
    "attempts=$n_att not_microvm=$not_vm cold=$(awk -F'\t' '$4 == "cold"' "$EVIDENCE/attempt-environments.txt" | wc -l | tr -d ' ') warm=$(awk -F'\t' '$4 == "warm"' "$EVIDENCE/attempt-environments.txt" | wc -l | tr -d ' ') environments=$(cut -f5 "$EVIDENCE/attempt-environments.txt" | sort -u | wc -l | tr -d ' ')"
fi

# ---------------------------------------------------------------------------
# evidence
# ---------------------------------------------------------------------------

# Read while the gateway still holds the WAL open (a read-only connection cannot recreate -shm).
sql "SELECT trigger_id, fire_key, outcome, invocation_id FROM trigger_fires ORDER BY trigger_id, fire_key" \
  >"$EVIDENCE/trigger_fires.txt"
stop_gateway TERM
# The secret must not end up in the evidence.
if grep -rq "${SECRET#whsec_}" "$EVIDENCE"; then
  record evidence.no_secret FAIL "the webhook secret appears in the evidence"
else
  record evidence.no_secret ok ""
fi
{
  echo "stamp=$STAMP"
  echo "host=$(uname -srm)"
  echo "git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "${TSLS_COMMIT:-unknown}")"
  echo "queue=sqlite"
  echo "provider=$PROVIDER"
  echo "cron_fires=$n2"
  echo "downtime_seconds=$((restarted_at - killed_at))"
} >"$EVIDENCE/environment.txt"

if [ "$FAILED" = 0 ]; then
  log "PASS (evidence: $EVIDENCE)"
else
  log "FAIL (evidence: $EVIDENCE)"
fi
exit "$FAILED"

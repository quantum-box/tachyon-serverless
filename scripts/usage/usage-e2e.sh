#!/usr/bin/env bash
# PLT-4642: host-measured usage → bounded durable journal → collector (at least once) →
# de-duplicating ledger → provisional rating, through a real gateway with the process
# provider (docs/adr/0012-usage-ledger-and-rating.md).
#
#   scripts/usage/usage-e2e.sh
#
# Scenario (example-cpu-burn, payload {"seconds": N}, revision timeout 2 s):
#   A. gateway #1, collector effectively off ([usage] collect_interval_ms = 3600000)
#      1. three known-duration invocations of 1.0 s                     -> 200
#      2. one invocation of 5 s                                          -> 504 timeout
#      3. one invocation with an Idempotency-Key, sent twice             -> 200, replayed
#      4. the journal holds every event, the ledger none; /readyz shows it
#      5. kill -9 gateway #1 (nothing was collected)
#   B. gateway #2 with TSLS_USAGE_CRASH_POINT=collector.after_ledger_commit and
#      collect_batch = 4: the first collection commits a batch to the ledger and the
#      process SIGKILLs itself before moving the journal cursor
#      6. the gateway died of SIGKILL; the ledger has exactly one batch; the cursor did not move
#   C. gateway #3, collector on
#      7. the journal drains to 0 pending; the ledger counts the re-delivered batch as
#         duplicates and holds every event exactly once
#      8. GET /v1/usage (tenant A): 5 invocations, 5 attempts, 0 retries, 1 timeout;
#         host-measured handler time within tolerance of the known durations; billable time
#         and provisional charges follow the price table; provisional / not an invoice
#      9. tenant B sees nothing, also when naming A's function
#     10. `tsls usage` prints the provisional report
#
# Firecracker (cgroup CPU usec as provider_reported) is NOT exercised here.
#
# Environment (all optional):
#   TSLS_SKIP_BUILD=1     do not run cargo build
#   TSLS_EVIDENCE_DIR     evidence root (default docs/evidence)
#
# Exit 0 only when every check passed.
#
# The EXIT-trap cleanup and predicates are called indirectly (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$REPO_ROOT/scripts/e2e/lib.sh"

require_tools curl jq cargo python3 || e2e_die "missing tools"

TENANT_A_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TENANT_B_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
RUN_ID="usage-$(date -u +%Y%m%dT%H%M%SZ)-process"
EVIDENCE_DIR="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}/$RUN_ID"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-usage.XXXXXX")"
DATA_DIR="$WORK_DIR/data"
mkdir -p "$EVIDENCE_DIR"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
GUEST="$REPO_ROOT/target/debug/example-cpu-burn"
case "$(uname -m)" in
  x86_64 | amd64) ARCH=x86_64 ;;
  *) ARCH=aarch64 ;;
esac
FAILED=0
GATEWAY_PID=""
TOKEN_A="usage-token-a"
TOKEN_B="usage-token-b"
# Scheduling noise allowed on top of a known duration, per invocation (process provider:
# bridge, user process start and the SDK's round trip are inside the handler window).
TOLERANCE_MS=700

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
[ -x "$GUEST" ] || e2e_die "missing $GUEST"

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
API_URL="http://127.0.0.1:$PORT"

write_config() { # write_config PATH COLLECT_INTERVAL_MS
  cat > "$1" <<EOF
listen = "127.0.0.1:$PORT"
profile = "dev"
data_dir = "$DATA_DIR"

[provider]
kind = "process"

[provider.process]
bridge_binary = "$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
workdir = "$DATA_DIR/process"

[usage]
collect_interval_ms = $2
collect_batch = 4

[[identity.tokens]]
token = "$TOKEN_A"
tenant_id = "$TENANT_A_ID"
subject = "usage-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B_ID"
subject = "usage-b"
roles = ["deploy", "invoke"]
EOF
}

start_gateway() { # start_gateway CONFIG LOG [ENV=VALUE]
  local config="$1" log="$2"
  shift 2
  env "$@" "$GATEWAY_BIN" --config "$config" >"$log" 2>&1 &
  GATEWAY_PID=$!
}

cleanup() {
  if [ -n "$GATEWAY_PID" ]; then stop_process "$GATEWAY_PID" 15; fi
  for f in "$WORK_DIR"/gateway-*.log; do
    if [ -f "$f" ]; then cp "$f" "$EVIDENCE_DIR/" 2>/dev/null || true; fi
  done
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

sql() { # sql DB QUERY -> rows as JSON
  python3 - "$1" "$2" <<'PY'
import json, sqlite3, sys
con = sqlite3.connect(sys.argv[1])
print(json.dumps(con.execute(sys.argv[2]).fetchall()))
PY
}
JOURNAL_DB="$DATA_DIR/usage/journal.db"
LEDGER_DB="$DATA_DIR/usage/ledger.db"

auth_a=(-H "authorization: Bearer $TOKEN_A")
auth_b=(-H "authorization: Bearer $TOKEN_B")
invoke() { # invoke FUNCTION_ID PAYLOAD OUT_PREFIX [IDEMPOTENCY_KEY]
  local key_header=()
  if [ -n "${4:-}" ]; then key_header=(-H "idempotency-key: $4"); fi
  curl -sS -o "$3.body" -D "$3.headers" -w '%{http_code}\n' -X POST "${auth_a[@]}" \
    -H 'content-type: application/json' ${key_header[@]+"${key_header[@]}"} \
    --data "$2" "$API_URL/v1/functions/$1/invoke" > "$3.status" || echo 000 > "$3.status"
}
readyz() { curl -sS "$API_URL/readyz"; }

# ---------------------------------------------------------------------------------------------
# A. gateway #1: meter, do not collect, kill -9
# ---------------------------------------------------------------------------------------------
write_config "$WORK_DIR/gateway-1.toml" 3600000
cp "$WORK_DIR/gateway-1.toml" "$EVIDENCE_DIR/gateway-1.toml"
start_gateway "$WORK_DIR/gateway-1.toml" "$WORK_DIR/gateway-1.log"
wait_for_http "$API_URL/readyz" 90 || { tail -n 40 "$WORK_DIR/gateway-1.log" >&2; exit 1; }
export TSLS_API_URL="$API_URL"
export TSLS_TOKEN="$TOKEN_A"

FN="$("$TSLS_BIN" functions create --name usage-e2e --description plt-4642 --json | jq -r .id)"
"$TSLS_BIN" functions deploy --function "$FN" --binary "$GUEST" --arch "$ARCH" \
  --timeout-seconds 2 --json > "$EVIDENCE_DIR/deploy.json"
echo "function=$FN revision=$(jq -r .id "$EVIDENCE_DIR/deploy.json")" | tee -a "$EVIDENCE_DIR/summary.txt"

mkdir -p "$WORK_DIR/inv"
ok=0
for i in 1 2 3; do
  invoke "$FN" '{"seconds":1.0}' "$WORK_DIR/inv/known-$i"
  if [ "$(cat "$WORK_DIR/inv/known-$i.status")" = 200 ]; then ok=$((ok + 1)); fi
done
check "1-known-duration-invocations" "$(rc_of test "$ok" = 3)" "200s=$ok/3 (payload seconds=1.0)"

invoke "$FN" '{"seconds":5}' "$WORK_DIR/inv/timeout"
t_status="$(cat "$WORK_DIR/inv/timeout.status")"
t_code="$(jq -r '.error.code // empty' "$WORK_DIR/inv/timeout.body")"
check "2-timeout" "$(rc_of test "$t_status" = 504 -a "$t_code" = timeout)" "status=$t_status code=$t_code"

invoke "$FN" '{"seconds":0.2}' "$WORK_DIR/inv/idem-1" usage-e2e-key
invoke "$FN" '{"seconds":0.2}' "$WORK_DIR/inv/idem-2" usage-e2e-key
id1="$(tr -d '\r' < "$WORK_DIR/inv/idem-1.headers" | awk -F': ' 'tolower($1) == "x-tachyon-invocation-id" {print $2}')"
id2="$(tr -d '\r' < "$WORK_DIR/inv/idem-2.headers" | awk -F': ' 'tolower($1) == "x-tachyon-invocation-id" {print $2}')"
check "3-duplicate-request-replayed" \
  "$(rc_of test "$(cat "$WORK_DIR/inv/idem-2.status")" = 200 -a -n "$id1" -a "$id1" = "$id2")" \
  "same invocation id for both sends: $id1 / $id2"

readyz > "$EVIDENCE_DIR/readyz-1-before-kill.json"
pending="$(jq -r '.usage.journal.pending_events' "$EVIDENCE_DIR/readyz-1-before-kill.json")"
ledger_before="$(jq -r '.usage.ledger.events' "$EVIDENCE_DIR/readyz-1-before-kill.json")"
settled_in_journal="$(sql "$JOURNAL_DB" "select count(*) from function_usage_journal where body like '%\"event_type\":\"attempt_settled\"%'" | jq '.[0][0]')"
check "4-journal-holds-events-ledger-empty" \
  "$(rc_of test "$pending" -gt 0 -a "$ledger_before" = 0 -a "$settled_in_journal" = 5)" \
  "journal pending=$pending (attempt_settled=$settled_in_journal) ledger events=$ledger_before"

kill -KILL "$GATEWAY_PID"
set +e
wait "$GATEWAY_PID" 2>/dev/null
rc=$?
set -e
GATEWAY_PID=""
check "5-kill-9-gateway-1" "$(rc_of test "$rc" = 137)" "exit=$rc (nothing was collected)"

# ---------------------------------------------------------------------------------------------
# B. gateway #2: the collector commits one batch to the ledger and dies before the cursor
# ---------------------------------------------------------------------------------------------
write_config "$WORK_DIR/gateway-2.toml" 300
start_gateway "$WORK_DIR/gateway-2.toml" "$WORK_DIR/gateway-2.log" \
  TSLS_USAGE_CRASH_POINT=collector.after_ledger_commit
set +e
wait "$GATEWAY_PID" 2>/dev/null
rc=$?
set -e
GATEWAY_PID=""
ledger_after_crash="$(sql "$LEDGER_DB" 'select count(*) from function_usage_events' | jq '.[0][0]')"
cursor_after_crash="$(sql "$JOURNAL_DB" 'select cursor_seq from function_usage_journal_state' | jq '.[0][0]')"
crash_line="$(grep -c 'usage crash point: SIGKILL after the ledger commit' "$WORK_DIR/gateway-2.log" || true)"
check "6-collector-killed-between-ledger-commit-and-cursor" \
  "$(rc_of test "$rc" = 137 -a "$ledger_after_crash" = 4 -a "$cursor_after_crash" = 0 -a "$crash_line" -ge 1)" \
  "exit=$rc ledger events=$ledger_after_crash journal cursor=$cursor_after_crash"

# ---------------------------------------------------------------------------------------------
# C. gateway #3: recover and verify
# ---------------------------------------------------------------------------------------------
write_config "$WORK_DIR/gateway-3.toml" 300
start_gateway "$WORK_DIR/gateway-3.toml" "$WORK_DIR/gateway-3.log"
wait_for_http "$API_URL/readyz" 90 || { tail -n 40 "$WORK_DIR/gateway-3.log" >&2; exit 1; }
drained() { [ "$(readyz | jq -r '.usage.journal.pending_events')" = 0 ]; }
deadline=$(( $(date +%s) + 30 ))
while ! drained; do
  if [ "$(date +%s)" -ge "$deadline" ]; then break; fi
  sleep 0.3
done
readyz > "$EVIDENCE_DIR/readyz-3-after-recovery.json"
total_events="$(jq -r '.usage.ledger.events' "$EVIDENCE_DIR/readyz-3-after-recovery.json")"
dups="$(jq -r '.usage.ledger.duplicates_ignored' "$EVIDENCE_DIR/readyz-3-after-recovery.json")"
distinct="$(sql "$LEDGER_DB" 'select count(distinct event_id) from function_usage_events' | jq '.[0][0]')"
check "7-journal-drained-no-double-count" \
  "$(rc_of test "$(jq -r '.usage.journal.pending_events' "$EVIDENCE_DIR/readyz-3-after-recovery.json")" = 0 \
    -a "$total_events" = "$pending" -a "$distinct" = "$pending" -a "$dups" -ge 4)" \
  "ledger events=$total_events (journal had $pending) distinct=$distinct duplicates_ignored=$dups"

curl -sS "${auth_a[@]}" "$API_URL/v1/usage?group_by=function" | jq . > "$EVIDENCE_DIR/usage-report-tenant-a.json"
R="$EVIDENCE_DIR/usage-report-tenant-a.json"
inv="$(jq '.totals.usage.invocations' "$R")"
att="$(jq '.totals.usage.attempts' "$R")"
ret="$(jq '.totals.usage.retries' "$R")"
tmo="$(jq '.totals.usage.outcomes.timeout' "$R")"
check "8a-counts" "$(rc_of test "$inv" = 5 -a "$att" = 5 -a "$ret" = 0 -a "$tmo" = 1)" \
  "invocations=$inv attempts=$att retries=$ret timeouts=$tmo"
handler="$(jq '.totals.usage.segments_ms.handler_ms' "$R")"
# 3 x 1000 ms + the 2 s timeout + 200 ms, each within tolerance.
low=5200
high=$(( low + 5 * TOLERANCE_MS ))
check "8b-host-measured-handler-time" "$(rc_of test "$handler" -ge "$low" -a "$handler" -le "$high")" \
  "handler_ms=$handler expected [$low, $high]"
per_known="$(sql "$LEDGER_DB" "select body from function_usage_events where event_type = 'attempt_settled'" \
  | jq -c '[.[][0] | fromjson | {outcome, handler: .segments.handler_ms, kind: .attempt_kind, guest: .guest_reported.guest_handler_ms}]')"
echo "$per_known" | jq . > "$EVIDENCE_DIR/attempt-settled.json"
measured="$(echo "$per_known" | jq '[.[] | select(.handler.measurement != "host_measured")] | length')"
check "8c-every-handler-segment-host-measured" "$(rc_of test "$measured" = 0)" "non-host handler segments=$measured"
# Each known-duration sample on its own: 1.0 s x3, the 2 s timeout, 0.2 s.
in_window() { # in_window OUTCOME LOW COUNT
  [ "$(echo "$per_known" | jq --arg o "$1" --argjson lo "$2" --argjson hi "$(( $2 + TOLERANCE_MS ))" \
    '[.[] | select(.outcome == $o and .handler.value >= $lo and .handler.value <= $hi)] | length')" = "$3" ]
}
samples="$(echo "$per_known" | jq -c '[.[] | {outcome, ms: .handler.value, guest}]')"
check "8c-known-duration-samples" \
  "$(rc_of eval 'in_window succeeded 1000 3 && in_window timeout 2000 1 && in_window succeeded 200 1')" \
  "tolerance ${TOLERANCE_MS} ms: $samples"
billable="$(jq '.totals.usage.billable_ms' "$R")"
init="$(jq '.totals.usage.segments_ms.user_init_ms' "$R")"
vcpu_q="$(jq '.totals.usage.vcpu_milli_ms' "$R")"
check "8d-billable-is-init-plus-handler" "$(rc_of test "$billable" = $((init + handler)))" \
  "billable_ms=$billable user_init_ms=$init handler_ms=$handler vcpu_milli_ms=$vcpu_q"
expected_vcpu="$(jq -r '.price_table.unit_prices_micros.vcpu_second as $p | (.lines | map(.usage.vcpu_milli_ms * $p / 1000000 | . + 0.5 | floor) | add)' "$R")"
charged_vcpu="$(jq '.totals.provisional_charges_micros.vcpu' "$R")"
check "8e-provisional-charge-follows-price-table" "$(rc_of test "$expected_vcpu" = "$charged_vcpu")" \
  "vcpu charge=$charged_vcpu micros (expected $expected_vcpu, table $(jq -r .price_table.version "$R"))"
flags="$(jq -c '{provisional, not_an_invoice, billing_enabled}' "$R")"
check "8f-provisional-not-an-invoice" \
  "$(rc_of test "$flags" = '{"provisional":true,"not_an_invoice":true,"billing_enabled":false}')" "$flags"

curl -sS "${auth_b[@]}" "$API_URL/v1/usage?function_id=$FN" | jq . > "$EVIDENCE_DIR/usage-report-tenant-b.json"
b_att="$(jq '.totals.usage.attempts' "$EVIDENCE_DIR/usage-report-tenant-b.json")"
b_lines="$(jq '.lines | length' "$EVIDENCE_DIR/usage-report-tenant-b.json")"
check "9-tenant-b-sees-nothing" "$(rc_of test "$b_att" = 0 -a "$b_lines" = 0)" "attempts=$b_att lines=$b_lines"

if "$TSLS_BIN" usage --group-by function > "$EVIDENCE_DIR/tsls-usage.txt" 2>&1; then urc=0; else urc=1; fi
check "10-tsls-usage" "$(rc_of test "$urc" = 0 -a "$(grep -c '^PROVISIONAL' "$EVIDENCE_DIR/tsls-usage.txt")" = 1)" \
  "$(head -n 1 "$EVIDENCE_DIR/tsls-usage.txt")"

stop_process "$GATEWAY_PID" 15
GATEWAY_PID=""

echo "evidence: $EVIDENCE_DIR" | tee -a "$EVIDENCE_DIR/summary.txt"
if [ "$FAILED" -ne 0 ]; then
  echo "usage e2e: FAILED" | tee -a "$EVIDENCE_DIR/summary.txt"
  exit 1
fi
echo "usage e2e: all checks passed" | tee -a "$EVIDENCE_DIR/summary.txt"

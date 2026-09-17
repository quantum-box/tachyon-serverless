#!/usr/bin/env bash
# PLT-4643: budget reservation, hard limit, alerts, settlement against the usage ledger and
# fail-closed admission, through a real gateway with the process provider
# (docs/adr/0016-budget-reservation-and-admission.md).
#
#   scripts/usage/budget-e2e.sh
#
# Scenario (example-cpu-burn, payload {"seconds": N}, revision timeout 2 s, one gateway,
# collector every 300 ms, budgets from a file the control plane re-reads):
#    1. probe: one invocation under a large limit; its reservation (the maximum charge) and
#       its settlement are read back
#    2. the hard limit is set to settled + 3 maximum charges + half of one
#    3. eight parallel 1.0 s invocations: exactly 3 run (200), 5 are refused 429
#       budget_exhausted / reason budget / Host.BudgetExhausted; while they run the reserved
#       amount is 3 maximum charges and never above the limit
#    4. after collection every reservation is settled: reserved 0, no hold, settled equals the
#       rated AttemptSettled events of the ledger (GET /v1/usage) within per-run rounding,
#       remaining = limit - settled; the budget store totals equal the sum of its rows
#    5. the excess came back: a new invocation is admitted
#    6. soft limit alerts fired (alert only), never a refusal by themselves
#    7. collector stopped (dev failpoint file usage/collector.pause): after
#       max_unsettled_age_seconds new invocations are refused 503 Host.BudgetUnknown and
#       /readyz is 503 with budget.collector_stalled; an invocation already running finishes
#    8. collector resumed: settlement catches up, admission resumes
#    9. limit lowered to the committed amount: refused; raised: admitted again
#   10. tenant B has no budget entry: 503 Host.BudgetUnknown (fail closed); its GET /v1/budget
#       shows nothing of tenant A
#   11. GET /metrics budget families; `tsls budget` prints the provisional header
#
# Environment (all optional):
#   TSLS_SKIP_BUILD=1     do not run cargo build
#   TSLS_EVIDENCE_DIR     evidence root (default docs/evidence)
#   TSLS_PROVIDER         process (default) | firecracker (scripts/kvm/provider-lib.sh: jailed
#                         microVMs; run as root, e.g. `sudo -n env PATH="$PATH" HOME="$HOME"
#                         TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 scripts/usage/budget-e2e.sh`;
#                         additionally checks that no VMM, jail, cgroup or tap is left)
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

# shellcheck source=scripts/kvm/provider-lib.sh
. "$REPO_ROOT/scripts/kvm/provider-lib.sh"

require_tools curl jq cargo python3 || e2e_die "missing tools"
provider_init "$REPO_ROOT" || e2e_die "provider"

TENANT_A_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TENANT_B_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
RUN_ID="budget-$(date -u +%Y%m%dT%H%M%SZ)-$PROVIDER"
EVIDENCE_DIR="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}/$RUN_ID"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-budget.XXXXXX")"
DATA_DIR="$WORK_DIR/data"
BUDGETS="$WORK_DIR/budgets.toml"
mkdir -p "$EVIDENCE_DIR" "$DATA_DIR/usage"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
GUEST="$GUEST_DIR/example-cpu-burn"
case "$(uname -m)" in
  x86_64 | amd64) ARCH=x86_64 ;;
  *) ARCH=aarch64 ;;
esac
FAILED=0
GATEWAY_PID=""
TOKEN_A="budget-token-a"
TOKEN_B="budget-token-b"
METRICS_TOKEN="budget-metrics-operator-token"
STALL_SECONDS=3
EDIT=0

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
  if provider_is_fc; then
    (cd "$REPO_ROOT" && cargo build -q --release --target "$(uname -m)-unknown-linux-musl" -p example-cpu-burn)
  fi
fi
[ -x "$GUEST" ] || e2e_die "missing $GUEST"

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
API_URL="http://127.0.0.1:$PORT"

set_budgets() { # set_budgets HARD_LIMIT_MICROS [SOFT_LIMIT_MICROS]
  EDIT=$((EDIT + 1))
  {
    # The edit number changes the file's size as well as its modification time.
    printf '# edit %s %s\n' "$EDIT" "$(printf '%*s' "$EDIT" '' | tr ' ' '#')"
    printf '[[tenants]]\ntenant_id = "%s"\nhard_limit_micros = %s\n' "$TENANT_A_ID" "$1"
    if [ -n "${2:-}" ]; then
      printf 'soft_limit_micros = %s\nalert_thresholds_percent = [50, 100]\n' "$2"
    fi
  } > "$BUDGETS"
  cp "$BUDGETS" "$EVIDENCE_DIR/budgets-edit-$EDIT.toml"
}

cat > "$WORK_DIR/gateway.toml" <<EOF
listen = "127.0.0.1:$PORT"
profile = "dev"
data_dir = "$DATA_DIR"

$(provider_toml "$DATA_DIR")

[usage]
collect_interval_ms = 300

[budget]
enabled = true
file = "$BUDGETS"
max_unsettled_age_seconds = $STALL_SECONDS
expiry_grace_seconds = 5

[metrics]
bearer_token = "$METRICS_TOKEN"

[[identity.tokens]]
token = "$TOKEN_A"
tenant_id = "$TENANT_A_ID"
subject = "budget-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B_ID"
subject = "budget-b"
roles = ["deploy", "invoke"]
EOF
cp "$WORK_DIR/gateway.toml" "$EVIDENCE_DIR/gateway.toml"
set_budgets 1000000000000

cleanup() {
  if [ -n "$GATEWAY_PID" ]; then stop_process "$GATEWAY_PID" 15; fi
  if [ -f "$WORK_DIR/gateway.log" ]; then cp "$WORK_DIR/gateway.log" "$EVIDENCE_DIR/" 2>/dev/null || true; fi
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
BUDGET_DB="$DATA_DIR/usage/budget.db"

auth_a=(-H "authorization: Bearer $TOKEN_A")
auth_b=(-H "authorization: Bearer $TOKEN_B")
invoke() { # invoke TOKEN FUNCTION_ID PAYLOAD OUT_PREFIX
  curl -sS -o "$4.body" -w '%{http_code}\n' -X POST -H "authorization: Bearer $1" \
    -H 'content-type: application/json' --data "$3" "$API_URL/v1/functions/$2/invoke" \
    > "$4.status" || echo 000 > "$4.status"
}
budget_a() { curl -sS "${auth_a[@]}" "$API_URL/v1/budget"; }
readyz() { curl -sS "$API_URL/readyz"; }
wait_until() { # wait_until SECONDS COMMAND...
  local deadline=$(( $(date +%s) + $1 ))
  shift
  while ! "$@"; do
    if [ "$(date +%s)" -ge "$deadline" ]; then return 1; fi
    sleep 0.2
  done
}
settled_all() { [ "$(budget_a | jq -r '.tenant.reserved_micros')" = 0 ]; }

"$GATEWAY_BIN" --config "$WORK_DIR/gateway.toml" >"$WORK_DIR/gateway.log" 2>&1 &
GATEWAY_PID=$!
wait_for_http "$API_URL/readyz" 90 || { tail -n 40 "$WORK_DIR/gateway.log" >&2; exit 1; }
export TSLS_API_URL="$API_URL"
export TSLS_TOKEN="$TOKEN_A"

FN="$("$TSLS_BIN" functions create --name budget-e2e --description plt-4643 --json | jq -r .id)"
"$TSLS_BIN" functions deploy --function "$FN" --binary "$GUEST" --arch "$ARCH" \
  --timeout-seconds 2 --json > "$EVIDENCE_DIR/deploy.json"
echo "function=$FN revision=$(jq -r .id "$EVIDENCE_DIR/deploy.json")" | tee -a "$EVIDENCE_DIR/summary.txt"
mkdir -p "$WORK_DIR/inv"

# 1. probe ------------------------------------------------------------------------------------
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/probe"
wait_until 20 settled_all || true
MAX="$(sql "$BUDGET_DB" 'select reserved_micros from function_budget_reservations limit 1' | jq '.[0][0]')"
SETTLED1="$(budget_a | jq '.tenant.settled_micros')"
check "1-probe-reserved-and-settled" \
  "$(rc_of test "$(cat "$WORK_DIR/inv/probe.status")" = 200 -a "$MAX" -gt 0 -a "$SETTLED1" -gt 0 -a "$SETTLED1" -lt "$MAX")" \
  "status=$(cat "$WORK_DIR/inv/probe.status") max_charge=$MAX settled=$SETTLED1 micros"

# 2. hard limit: 3 maximum charges and a half above what is settled -------------------------
LIMIT=$(( SETTLED1 + 3 * MAX + MAX / 2 ))
set_budgets "$LIMIT" "$SETTLED1"
budget_a > "$EVIDENCE_DIR/budget-2-after-limit.json"
check "2-limit-delivered" \
  "$(rc_of test "$(jq -r '.tenant.hard_limit_micros' "$EVIDENCE_DIR/budget-2-after-limit.json")" = "$LIMIT")" \
  "hard_limit=$LIMIT generation=$(jq -r '.config_generation' "$EVIDENCE_DIR/budget-2-after-limit.json")"

# 3. parallel invocations ---------------------------------------------------------------------
pids=()
for i in 1 2 3 4 5 6 7 8; do
  invoke "$TOKEN_A" "$FN" '{"seconds":1.0}' "$WORK_DIR/inv/par-$i" &
  pids+=($!)
done
sleep 0.5
budget_a > "$EVIDENCE_DIR/budget-3-during-burst.json"
for p in "${pids[@]}"; do wait "$p" || true; done
ok=0
refused=0
bad=0
for i in 1 2 3 4 5 6 7 8; do
  s="$(cat "$WORK_DIR/inv/par-$i.status")"
  if [ "$s" = 200 ]; then
    ok=$((ok + 1))
  elif [ "$s" = 429 ] \
    && [ "$(jq -r '.error.reason' "$WORK_DIR/inv/par-$i.body")" = budget ] \
    && [ "$(jq -r '.error.code' "$WORK_DIR/inv/par-$i.body")" = budget_exhausted ] \
    && [ "$(jq -r '.error.error_type' "$WORK_DIR/inv/par-$i.body")" = Host.BudgetExhausted ]; then
    refused=$((refused + 1))
    cp "$WORK_DIR/inv/par-$i.body" "$EVIDENCE_DIR/refusal-budget-exhausted.json"
  else
    bad=$((bad + 1))
  fi
done
check "3a-parallel-refused-by-budget" "$(rc_of test "$ok" = 3 -a "$refused" = 5 -a "$bad" = 0)" \
  "200=$ok 429-budget=$refused other=$bad"
during_reserved="$(jq '.tenant.reserved_micros' "$EVIDENCE_DIR/budget-3-during-burst.json")"
during_committed="$(jq '.tenant.committed_micros' "$EVIDENCE_DIR/budget-3-during-burst.json")"
check "3b-reserved-never-above-limit" \
  "$(rc_of test "$during_reserved" = $((3 * MAX)) -a "$during_committed" -le "$LIMIT")" \
  "reserved=$during_reserved (3 x $MAX) committed=$during_committed limit=$LIMIT"

# 4. settlement against the ledger ------------------------------------------------------------
wait_until 20 settled_all || true
budget_a > "$EVIDENCE_DIR/budget-4-settled.json"
B4="$EVIDENCE_DIR/budget-4-settled.json"
settled="$(jq '.tenant.settled_micros' "$B4")"
curl -sS "${auth_a[@]}" "$API_URL/v1/usage?group_by=none" | jq . > "$EVIDENCE_DIR/usage-4.json"
ledger_total="$(jq '.totals.provisional_charges_micros.total' "$EVIDENCE_DIR/usage-4.json")"
runs="$(jq '.tenant.settlements' "$B4")"
diff=$(( settled > ledger_total ? settled - ledger_total : ledger_total - settled ))
check "4a-excess-returned-all-settled" \
  "$(rc_of test "$(jq '.tenant.reserved_micros' "$B4")" = 0 -a "$(jq '.tenant.unmetered_hold_micros' "$B4")" = 0 -a "$runs" = 4)" \
  "reserved=0 hold=0 settlements=$runs settled=$settled (4 runs reserved $((4 * MAX)))"
check "4b-settled-matches-ledger-rating" "$(rc_of test "$diff" -le $((runs * 2)))" \
  "budget settled=$settled usage ledger total=$ledger_total |diff|=$diff (per-run rounding bound $((runs * 2)))"
check "4c-remaining" "$(rc_of test "$(jq '.tenant.remaining_micros' "$B4")" = $((LIMIT - settled)))" \
  "remaining=$(jq '.tenant.remaining_micros' "$B4") = $LIMIT - $settled"
rows="$(sql "$BUDGET_DB" "select coalesce(sum(settled_micros),0), coalesce(sum(held_micros),0), sum(case when state='reserved' then reserved_micros else 0 end) from function_budget_reservations where tenant_id='$TENANT_A_ID'" | jq -c '.[0]')"
tot="$(sql "$BUDGET_DB" "select settled_micros, held_micros, reserved_micros from function_budget_totals where tenant_id='$TENANT_A_ID' and scope=''" | jq -c '.[0]')"
states="$(sql "$BUDGET_DB" "select state, count(*) from function_budget_reservations group by state" | jq -c .)"
check "4d-store-totals-equal-rows" "$(rc_of test "$rows" = "$tot")" "rows=$rows totals=$tot states=$states"

# 5. the excess admits new work ---------------------------------------------------------------
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/after"
check "5-admitted-after-settlement" "$(rc_of test "$(cat "$WORK_DIR/inv/after.status")" = 200)" \
  "status=$(cat "$WORK_DIR/inv/after.status")"

# 6. alerts -----------------------------------------------------------------------------------
wait_until 20 settled_all || true
alerts="$(budget_a | jq -c '[.tenant.alerts_fired[].threshold_percent]')"
check "6-soft-limit-alerts-only" "$(rc_of test "$alerts" = '[50,100]')" \
  "alerts_fired=$alerts (soft limit $SETTLED1 = the probe, crossed by the next settlement; no refusal from it)"

# 7. collector stopped -> fail closed ---------------------------------------------------------
touch "$DATA_DIR/usage/collector.pause"
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/unsettled"
invoke "$TOKEN_A" "$FN" "{\"seconds\":1.0}" "$WORK_DIR/inv/long" &
long_pid=$!
sleep $((STALL_SECONDS + 1))
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/stalled"
cp "$WORK_DIR/inv/stalled.body" "$EVIDENCE_DIR/refusal-budget-unknown.json"
readyz > "$EVIDENCE_DIR/readyz-7-stalled.json"
ready_code="$(curl -s -o /dev/null -w '%{http_code}' "$API_URL/readyz")"
wait "$long_pid" || true
check "7a-stalled-collector-refuses" \
  "$(rc_of test "$(cat "$WORK_DIR/inv/stalled.status")" = 503 -a "$(jq -r '.error.error_type' "$WORK_DIR/inv/stalled.body")" = Host.BudgetUnknown -a "$(jq -r '.error.reason' "$WORK_DIR/inv/stalled.body")" = budget)" \
  "status=$(cat "$WORK_DIR/inv/stalled.status") $(jq -c '.error | {code, reason, error_type}' "$WORK_DIR/inv/stalled.body")"
check "7b-readyz-reports-stall" \
  "$(rc_of test "$ready_code" = 503 -a "$(jq -r '.budget.collector_stalled' "$EVIDENCE_DIR/readyz-7-stalled.json")" = true)" \
  "readyz=$ready_code oldest_unsettled_age=$(jq -r '.budget.oldest_unsettled_age_seconds' "$EVIDENCE_DIR/readyz-7-stalled.json")s"
check "7c-running-work-finished" "$(rc_of test "$(cat "$WORK_DIR/inv/long.status")" = 200)" \
  "started before the stall threshold: status=$(cat "$WORK_DIR/inv/long.status")"

# 8. collector resumed ------------------------------------------------------------------------
rm -f "$DATA_DIR/usage/collector.pause"
accepting() { [ "$(readyz | jq -r '.budget.accepting')" = true ]; }
wait_until 20 accepting || true
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/resumed"
check "8-collector-resumed-admission-resumes" "$(rc_of test "$(cat "$WORK_DIR/inv/resumed.status")" = 200)" \
  "status=$(cat "$WORK_DIR/inv/resumed.status")"

# 9. lower to the committed amount, then raise --------------------------------------------------
wait_until 20 settled_all || true
committed="$(budget_a | jq '.tenant.committed_micros')"
set_budgets "$committed"
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/lowered"
set_budgets "$(( committed + 10 * MAX ))"
invoke "$TOKEN_A" "$FN" '{"seconds":0.1}' "$WORK_DIR/inv/raised"
budget_a > "$EVIDENCE_DIR/budget-9-raised.json"
check "9-lower-refuses-raise-resumes" \
  "$(rc_of test "$(cat "$WORK_DIR/inv/lowered.status")" = 429 -a "$(cat "$WORK_DIR/inv/raised.status")" = 200)" \
  "at committed=$committed: $(cat "$WORK_DIR/inv/lowered.status"); raised to $(( committed + 10 * MAX )): $(cat "$WORK_DIR/inv/raised.status") generation=$(jq -r .config_generation "$EVIDENCE_DIR/budget-9-raised.json")"

# 10. tenant B: no budget entry -> fail closed, isolated report ---------------------------------
FNB="$(TSLS_TOKEN="$TOKEN_B" "$TSLS_BIN" functions create --name budget-e2e-b --description plt-4643 --json | jq -r .id)"
TSLS_TOKEN="$TOKEN_B" "$TSLS_BIN" functions deploy --function "$FNB" --binary "$GUEST" --arch "$ARCH" \
  --timeout-seconds 2 --json > /dev/null
invoke "$TOKEN_B" "$FNB" '{"seconds":0.1}' "$WORK_DIR/inv/tenant-b"
curl -sS "${auth_b[@]}" "$API_URL/v1/budget" | jq . > "$EVIDENCE_DIR/budget-tenant-b.json"
check "10-no-budget-fails-closed-and-isolated" \
  "$(rc_of test "$(cat "$WORK_DIR/inv/tenant-b.status")" = 503 -a "$(jq -r '.error.error_type' "$WORK_DIR/inv/tenant-b.body")" = Host.BudgetUnknown -a "$(jq -r .config_state "$EVIDENCE_DIR/budget-tenant-b.json")" = not_delivered -a "$(grep -c "$TENANT_A_ID\|$FN" "$EVIDENCE_DIR/budget-tenant-b.json" || true)" = 0)" \
  "status=$(cat "$WORK_DIR/inv/tenant-b.status") error_type=$(jq -r '.error.error_type' "$WORK_DIR/inv/tenant-b.body") config_state=$(jq -r .config_state "$EVIDENCE_DIR/budget-tenant-b.json")"

# 11. metrics and CLI ------------------------------------------------------------------------------
curl -sS -H "authorization: Bearer $METRICS_TOKEN" "$API_URL/metrics" | grep '^tsls_budget_' > "$EVIDENCE_DIR/metrics-budget.txt" || true
exhausted_metric="$(awk '/^tsls_budget_refusals_total\{reason="budget_exhausted",cause="tenant_hard_limit"\}/ {print $2}' "$EVIDENCE_DIR/metrics-budget.txt")"
unknown_metric="$(awk '/^tsls_budget_refusals_total\{reason="budget_unknown",cause="collector_stalled"\}/ {print $2}' "$EVIDENCE_DIR/metrics-budget.txt")"
check "11a-metrics" "$(rc_of test "${exhausted_metric:-0}" -ge 6 -a "${unknown_metric:-0}" -ge 1)" \
  "refusals budget_exhausted/tenant_hard_limit=$exhausted_metric budget_unknown/collector_stalled=$unknown_metric"
if "$TSLS_BIN" budget > "$EVIDENCE_DIR/tsls-budget.txt" 2>&1; then brc=0; else brc=1; fi
check "11b-tsls-budget" "$(rc_of test "$brc" = 0 -a "$(grep -c '^PROVISIONAL' "$EVIDENCE_DIR/tsls-budget.txt")" = 1)" \
  "$(head -n 1 "$EVIDENCE_DIR/tsls-budget.txt")"
budget_a > "$EVIDENCE_DIR/budget-final.json"

stop_process "$GATEWAY_PID" 15
GATEWAY_PID=""
if provider_is_fc; then
  provider_leftovers > "$EVIDENCE_DIR/leftovers-after.txt" 2>&1
  check "12-no-vmm-jail-cgroup-tap-left" "$(rc_of test ! -s "$EVIDENCE_DIR/leftovers-after.txt")" \
    "$(wc -l < "$EVIDENCE_DIR/leftovers-after.txt" | tr -d ' ') leftovers"
fi

echo "evidence: $EVIDENCE_DIR" | tee -a "$EVIDENCE_DIR/summary.txt"
if [ "$FAILED" -ne 0 ]; then
  echo "budget e2e: FAILED" | tee -a "$EVIDENCE_DIR/summary.txt"
  exit 1
fi
echo "budget e2e: all checks passed" | tee -a "$EVIDENCE_DIR/summary.txt"

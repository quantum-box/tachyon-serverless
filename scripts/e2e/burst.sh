#!/usr/bin/env bash
# PLT-4634: bounded burst through a real gateway (process provider, local only).
#
#   scripts/e2e/burst.sh
#
# Starts a throwaway gateway on a free port with a scratch data_dir and a node
# whose memory fits three environments including the per-environment VMM and
# bridge overhead ((256 + 24) MiB each, node 900 MiB), deploys cpu-burn, and:
#
#   1. fires 7 concurrent invocations (0.4 s each): 3 run, 4 wait; all 200, and
#      GET /v1/capacity never shows more than 3 in flight or 900 MiB reserved;
#   2. fires 14 concurrent invocations with max_queue = 4: at most 3 + 4 are
#      served, the rest answer 429 with reason `queue_full`;
#   3. tenant B requires region `jp` on a node labelled `us`: 503 `placement`,
#      also when the node is idle (never relaxed);
#   4. GET /v1/capacity for tenant A shows the host (1 node, no scale-out)
#      separately from its environments, and never lists tenant B.
#
# Environment (optional): TSLS_SKIP_BUILD=1, TSLS_EVIDENCE_DIR (default docs/evidence).
# Exit 0 only when every check passed. Not a load test: numbers are small on purpose.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_tools curl jq cargo python3 || e2e_die "missing tools"

TENANT_A_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TENANT_B_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
TOKEN_A="burst-token-a"
TOKEN_B="burst-token-b"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-burst-process"
EVIDENCE_DIR="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}/$RUN_ID"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-burst.XXXXXX")"
mkdir -p "$EVIDENCE_DIR"
GATEWAY_LOG="$WORK_DIR/gateway.log"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
FAILED=0

check() { # check NAME CONDITION_RC DETAIL
  if [ "$2" -eq 0 ]; then
    printf 'PASS  %s  %s\n' "$1" "$3" | tee -a "$EVIDENCE_DIR/summary.txt"
  else
    printf 'FAIL  %s  %s\n' "$1" "$3" | tee -a "$EVIDENCE_DIR/summary.txt"
    FAILED=1
  fi
}

if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  (cd "$REPO_ROOT" && cargo build -q -p tachyon-serverless-gateway -p tachyon-serverless-cli \
    -p tachyon-serverless-runtime-bridge -p example-cpu-burn)
fi

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
API_URL="http://127.0.0.1:$PORT"
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
max_queue = 4
queue_timeout_seconds = 20

[capacity.node]
name = "burst-local"
region = "us"
memory_mib = 900

[[capacity.tenants]]
tenant_id = "$TENANT_B_ID"
required_region = "jp"

[[identity.tokens]]
token = "$TOKEN_A"
tenant_id = "$TENANT_A_ID"
subject = "burst-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B_ID"
subject = "burst-b"
roles = ["deploy", "invoke"]
EOF
cp "$CONFIG" "$EVIDENCE_DIR/gateway.toml"

"$GATEWAY_BIN" --config "$CONFIG" >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!
cleanup() {
  [ -n "${POLL_PID:-}" ] && kill "$POLL_PID" 2>/dev/null || true
  stop_process "$GATEWAY_PID" 10
  cp "$GATEWAY_LOG" "$EVIDENCE_DIR/gateway.log" 2>/dev/null || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT
wait_for_http "$API_URL/readyz" 60 || { tail -n 30 "$GATEWAY_LOG" >&2; exit 1; }

export TSLS_API_URL="$API_URL"
deploy() { # deploy TOKEN NAME -> function id
  local id
  id="$(TSLS_TOKEN="$1" "$TSLS_BIN" functions create --name "$2" --description burst --json | jq -r .id)"
  TSLS_TOKEN="$1" "$TSLS_BIN" functions deploy --function "$id" \
    --binary "$REPO_ROOT/target/debug/example-cpu-burn" --max-concurrency 8 --json >/dev/null
  echo "$id"
}
FN_A="$(deploy "$TOKEN_A" burst-a)"
FN_B="$(deploy "$TOKEN_B" burst-b)"

capacity() { curl -sS -H "authorization: Bearer $TOKEN_A" "$API_URL/v1/capacity"; }
poll_capacity() {
  while :; do
    capacity | jq -c '{in_flight, reserved_mib: .reserved.memory_mib, queue: .queue.length, env: .environments}' \
      >> "$1" 2>/dev/null || true
    sleep 0.05
  done
}
burst() { # burst N OUTDIR
  local i
  mkdir -p "$2"
  for i in $(seq 1 "$1"); do
    curl -sS -o "$2/$i.body" -w '%{http_code}\n' -X POST \
      -H "authorization: Bearer $TOKEN_A" -H 'content-type: application/json' \
      --data '{"seconds":0.4}' "$API_URL/v1/functions/$FN_A/invoke" > "$2/$i.status" &
  done
  local j
  for j in $(jobs -p); do
    [ "$j" = "$GATEWAY_PID" ] && continue
    [ "$j" = "${POLL_PID:-x}" ] && continue
    wait "$j" 2>/dev/null || true
  done
}

# 1. burst within the queue: everyone is served, nothing overshoots.
poll_capacity "$EVIDENCE_DIR/capacity-burst1.jsonl" & POLL_PID=$!
burst 7 "$WORK_DIR/b1"
kill "$POLL_PID" 2>/dev/null || true; wait "$POLL_PID" 2>/dev/null || true; POLL_PID=""
ok1="$(cat "$WORK_DIR"/b1/*.status | grep -c '^200$' || true)"
check "burst-7-all-served" "$([ "$ok1" = 7 ]; echo $?)" "200s=$ok1/7"
max_inflight="$(jq -s 'map(.in_flight) | max' "$EVIDENCE_DIR/capacity-burst1.jsonl")"
max_reserved="$(jq -s 'map(.reserved_mib) | max' "$EVIDENCE_DIR/capacity-burst1.jsonl")"
max_queue="$(jq -s 'map(.queue) | max' "$EVIDENCE_DIR/capacity-burst1.jsonl")"
check "no-overshoot-in-flight" "$([ "$max_inflight" -le 3 ] && [ "$max_inflight" -ge 2 ]; echo $?)" \
  "max in_flight=$max_inflight (node fits 3 incl. overhead)"
check "no-overshoot-memory" "$([ "$max_reserved" -le 900 ]; echo $?)" "max reserved=${max_reserved} MiB of 900"
check "queued-while-full" "$([ "$max_queue" -ge 1 ]; echo $?)" "max queue length=$max_queue"

# 2. burst beyond the queue: bounded, explicit queue_full.
burst 14 "$WORK_DIR/b2"
ok2="$(cat "$WORK_DIR"/b2/*.status | grep -c '^200$' || true)"
full2="$(cat "$WORK_DIR"/b2/*.status | grep -c '^429$' || true)"
reasons="$(for f in "$WORK_DIR"/b2/*.body; do jq -r '.error.reason // empty' "$f" 2>/dev/null; done | sort | uniq -c | tr '\n' ';')"
check "burst-14-bounded" "$([ $((ok2 + full2)) -eq 14 ] && [ "$ok2" -le 10 ] && [ "$full2" -ge 4 ]; echo $?)" \
  "200s=$ok2 429s=$full2 reasons=[$reasons]"
check "queue-full-reason" "$(echo "$reasons" | grep -q 'queue_full'; echo $?)" "$reasons"

# 3. jp-only is refused on a `us` node, even while idle.
code="$(curl -sS -o "$WORK_DIR/b.body" -w '%{http_code}' -X POST -H "authorization: Bearer $TOKEN_B" \
  -H 'content-type: application/json' --data '{"seconds":0}' "$API_URL/v1/functions/$FN_B/invoke")"
reason="$(jq -r .error.reason "$WORK_DIR/b.body")"
cp "$WORK_DIR/b.body" "$EVIDENCE_DIR/placement-error.json"
check "jp-only-placement" "$([ "$code" = 503 ] && [ "$reason" = placement ]; echo $?)" "status=$code reason=$reason"

# 4. host vs environments, tenant scoped.
capacity | jq . > "$EVIDENCE_DIR/capacity-final.json"
if jq -e '.node.hosts == 1 and .node.host_scale_out == "not_supported" and .node.capacity.memory_mib == 900
       and .node.per_environment_overhead.memory_mib == 24 and .in_flight == 0 and .reserved.memory_mib == 0' \
  "$EVIDENCE_DIR/capacity-final.json" >/dev/null; then report_rc=0; else report_rc=1; fi
check "capacity-report" "$report_rc" "$(jq -c '{node: .node.name, hosts: .node.hosts, rejections}' "$EVIDENCE_DIR/capacity-final.json")"
leak="$(grep -c "$TENANT_B_ID" "$EVIDENCE_DIR/capacity-final.json" || true)"
check "capacity-tenant-scoped" "$([ "$leak" = 0 ]; echo $?)" "tenant B mentions=$leak"

echo "evidence: $EVIDENCE_DIR"
exit "$FAILED"

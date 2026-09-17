#!/usr/bin/env bash
# PLT-4634: bounded burst through a real gateway.
#
#   scripts/e2e/burst.sh                                        # process provider
#   sudo -E env PATH="$PATH" TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 \
#     scripts/e2e/burst.sh                                      # Firecracker (Linux/KVM, root)
#
# Starts a throwaway gateway on a free port with a scratch data_dir and a node
# whose memory fits three environments including the per-environment VMM and
# bridge overhead (process: (256 + 24) MiB each, node 900 MiB; firecracker:
# (256 + 64) MiB each, node 1020 MiB), deploys cpu-burn, and:
#
#   1. fires 7 concurrent invocations (0.4 s each): 3 run, 4 wait; all 200, and
#      GET /v1/capacity never shows more than 3 in flight or the node's memory reserved;
#   2. fires 14 concurrent invocations with max_queue = 4: at most 3 + 4 are
#      served, the rest answer 429 with reason `queue_full`;
#   3. tenant B requires region `jp` on a node labelled `us`: 503 `placement`,
#      also when the node is idle (never relaxed);
#   4. GET /v1/capacity for tenant A shows the host (1 node, no scale-out)
#      separately from its environments, and never lists tenant B.
#
# Environment (optional): TSLS_SKIP_BUILD=1, TSLS_EVIDENCE_DIR (default docs/evidence),
#   TSLS_PROVIDER         process (default) | firecracker
#   TSLS_PROVIDER_CONFIG  firecracker only: gateway config whose `profile` and
#                         `[provider]` / `[provider.*]` sections are copied into the
#                         generated config (default config/gateway.firecracker.toml,
#                         i.e. jailer and cgroup required, so the script must run as
#                         root; relative paths resolve against the repository root)
#   TSLS_GUEST_DIR        directory with example-cpu-burn (default target/debug, or
#                         target/<arch>-unknown-linux-musl/release for firecracker)
#   TSLS_ARCH             aarch64 | x86_64 (default: host)
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
PROVIDER="${TSLS_PROVIDER:-process}"
case "$PROVIDER" in
  process | firecracker) ;;
  *) e2e_die "TSLS_PROVIDER must be process or firecracker (got $PROVIDER)" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) HOST_ARCH=x86_64 ;;
  *) HOST_ARCH=aarch64 ;;
esac
ARCH="${TSLS_ARCH:-$HOST_ARCH}"
if [ "$PROVIDER" = firecracker ]; then
  GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/$ARCH-unknown-linux-musl/release}"
  PROVIDER_CONFIG="${TSLS_PROVIDER_CONFIG:-$REPO_ROOT/config/gateway.firecracker.toml}"
  [ -f "$PROVIDER_CONFIG" ] || e2e_die "provider config not found: $PROVIDER_CONFIG"
  # The VMM's cgroup is memory.max = guest memory + 64 MiB (config/gateway.firecracker.toml):
  # reserve exactly that, so three 256 MiB environments fit in 1020 MiB and a fourth does not.
  OVERHEAD_MIB=64
  NODE_OVERHEAD=$'vmm_overhead_memory_mib = 64\nbridge_overhead_memory_mib = 0'
  # A microVM cold start takes seconds on a nested host: queued requests wait for two rounds.
  QUEUE_TIMEOUT=90
else
  GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/debug}"
  OVERHEAD_MIB=24
  NODE_OVERHEAD=""
  QUEUE_TIMEOUT=20
fi
NODE_MEMORY_MIB=$(( 3 * (256 + OVERHEAD_MIB) + 60 ))
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-burst-$PROVIDER"
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
[ -x "$GUEST_DIR/example-cpu-burn" ] || e2e_die "missing $GUEST_DIR/example-cpu-burn"

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
API_URL="http://127.0.0.1:$PORT"
CONFIG="$WORK_DIR/gateway.toml"
if [ "$PROVIDER" = firecracker ]; then
  PROFILE="$(sed -n 's/^profile *= *"\([a-z]*\)".*/\1/p' "$PROVIDER_CONFIG" | head -n1)"
  # [provider] and every [provider.*] table, nothing else.
  PROVIDER_TOML="$(awk '/^\[/ { keep = ($0 ~ /^\[provider(\.[a-z_.]+)?\]/) } keep' "$PROVIDER_CONFIG")"
else
  PROFILE=dev
  PROVIDER_TOML="[provider]
kind = \"process\"

[provider.process]
bridge_binary = \"$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge\"
workdir = \"$WORK_DIR/data/process\""
fi
cat > "$CONFIG" <<EOF
listen = "127.0.0.1:$PORT"
profile = "${PROFILE:-dev}"
data_dir = "$WORK_DIR/data"

$PROVIDER_TOML

[capacity]
max_concurrency = 8
max_queue = 4
queue_timeout_seconds = $QUEUE_TIMEOUT

[capacity.node]
name = "burst-local"
region = "us"
memory_mib = $NODE_MEMORY_MIB
$NODE_OVERHEAD

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

# Relative provider paths (.kvm/...) resolve against the gateway's working directory.
(cd "$REPO_ROOT" && exec "$GATEWAY_BIN" --config "$CONFIG") >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!
# cleanup runs from the EXIT trap, which shellcheck cannot follow (SC2317).
# shellcheck disable=SC2317
cleanup() {
  if [ -n "${POLL_PID:-}" ]; then kill "$POLL_PID" 2>/dev/null || true; fi
  stop_process "$GATEWAY_PID" 10
  cp "$GATEWAY_LOG" "$EVIDENCE_DIR/gateway.log" 2>/dev/null || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT
wait_for_http "$API_URL/readyz" 90 || { tail -n 30 "$GATEWAY_LOG" >&2; exit 1; }

export TSLS_API_URL="$API_URL"
deploy() { # deploy TOKEN NAME -> function id
  local id
  id="$(TSLS_TOKEN="$1" "$TSLS_BIN" functions create --name "$2" --description burst --json | jq -r .id)"
  TSLS_TOKEN="$1" "$TSLS_BIN" functions deploy --function "$id" \
    --binary "$GUEST_DIR/example-cpu-burn" --arch "$ARCH" --max-concurrency 8 --json >/dev/null
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
check "burst-7-all-served" "$(if [ "$ok1" = 7 ]; then echo 0; else echo 1; fi)" "200s=$ok1/7"
max_inflight="$(jq -s 'map(.in_flight) | max' "$EVIDENCE_DIR/capacity-burst1.jsonl")"
max_reserved="$(jq -s 'map(.reserved_mib) | max' "$EVIDENCE_DIR/capacity-burst1.jsonl")"
max_queue="$(jq -s 'map(.queue) | max' "$EVIDENCE_DIR/capacity-burst1.jsonl")"
check "no-overshoot-in-flight" "$(if [ "$max_inflight" -le 3 ] && [ "$max_inflight" -ge 2 ]; then echo 0; else echo 1; fi)" \
  "max in_flight=$max_inflight (node fits 3 incl. overhead)"
check "no-overshoot-memory" "$(if [ "$max_reserved" -le "$NODE_MEMORY_MIB" ]; then echo 0; else echo 1; fi)" \
  "max reserved=${max_reserved} MiB of $NODE_MEMORY_MIB"
check "queued-while-full" "$(if [ "$max_queue" -ge 1 ]; then echo 0; else echo 1; fi)" "max queue length=$max_queue"

# 2. burst beyond the queue: bounded, explicit queue_full.
burst 14 "$WORK_DIR/b2"
ok2="$(cat "$WORK_DIR"/b2/*.status | grep -c '^200$' || true)"
full2="$(cat "$WORK_DIR"/b2/*.status | grep -c '^429$' || true)"
reasons="$(for f in "$WORK_DIR"/b2/*.body; do jq -r '.error.reason // empty' "$f" 2>/dev/null; done | sort | uniq -c | tr '\n' ';')"
check "burst-14-bounded" "$(if [ $((ok2 + full2)) -eq 14 ] && [ "$ok2" -le 10 ] && [ "$full2" -ge 4 ]; then echo 0; else echo 1; fi)" \
  "200s=$ok2 429s=$full2 reasons=[$reasons]"
check "queue-full-reason" "$(echo "$reasons" | grep -q 'queue_full'; echo $?)" "$reasons"

# 3. jp-only is refused on a `us` node, even while idle.
code="$(curl -sS -o "$WORK_DIR/b.body" -w '%{http_code}' -X POST -H "authorization: Bearer $TOKEN_B" \
  -H 'content-type: application/json' --data '{"seconds":0}' "$API_URL/v1/functions/$FN_B/invoke")"
reason="$(jq -r .error.reason "$WORK_DIR/b.body")"
cp "$WORK_DIR/b.body" "$EVIDENCE_DIR/placement-error.json"
check "jp-only-placement" "$(if [ "$code" = 503 ] && [ "$reason" = placement ]; then echo 0; else echo 1; fi)" "status=$code reason=$reason"

# 4. host vs environments, tenant scoped.
capacity | jq . > "$EVIDENCE_DIR/capacity-final.json"
if jq -e --argjson mem "$NODE_MEMORY_MIB" --argjson ovh "$OVERHEAD_MIB" '.node.hosts == 1 and .node.host_scale_out == "not_supported" and .node.capacity.memory_mib == $mem
       and .node.per_environment_overhead.memory_mib == $ovh and .in_flight == 0 and .reserved.memory_mib == 0' \
  "$EVIDENCE_DIR/capacity-final.json" >/dev/null; then report_rc=0; else report_rc=1; fi
check "capacity-report" "$report_rc" "$(jq -c '{node: .node.name, hosts: .node.hosts, rejections}' "$EVIDENCE_DIR/capacity-final.json")"
leak="$(grep -c "$TENANT_B_ID" "$EVIDENCE_DIR/capacity-final.json" || true)"
check "capacity-tenant-scoped" "$(if [ "$leak" = 0 ]; then echo 0; else echo 1; fi)" "tenant B mentions=$leak"

# 5. firecracker: the gateway stops cleanly and leaves no VMM, socket or drive behind.
if [ "$PROVIDER" = firecracker ]; then
  stop_process "$GATEWAY_PID" 30
  check "gateway-stopped" "$STOP_RC" "exit status $STOP_RC"
  if "$SCRIPT_DIR/orphan-check.sh" firecracker "$REPO_ROOT/.kvm/run" >"$EVIDENCE_DIR/orphan-check.txt" 2>&1; then
    orc=0
  else
    orc=1
  fi
  check "no-orphans" "$orc" "$(tr '\n' ' ' < "$EVIDENCE_DIR/orphan-check.txt" | cut -c1-200)"
fi

echo "evidence: $EVIDENCE_DIR"
exit "$FAILED"

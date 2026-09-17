#!/usr/bin/env bash
# PLT-4637: reproducible, bounded load scenarios against a LOCAL gateway, with metrics
# sampling, detectors and a timeline graph (docs/metrics.md §6).
#
#   scripts/load/scenarios.sh                      # every scenario, process provider
#   scripts/load/scenarios.sh burst restart        # some of them
#
# Scenarios:
#   single        three single invocations with pauses: 0 -> 1 -> 0 each time
#   burst         ramp -> burst over the revision cap -> decrease -> idle until 0
#   idle-to-zero  a burst, idle until 0, stay idle (idle CPU detector), re-access
#   mixed         tenant A long handlers and tenant B short ones at the same time
#                 (starvation detector); node 4, tenant quota 3
#   restart       load, idle, gateway restart while idle, load again, idle until 0
#   lifecycle     0 -> ramp -> cap -> decrease -> 0 -> restart -> activity -> 0 in one run
#                 (the acceptance graph)
#
# Every scenario starts its own throwaway gateway on a free 127.0.0.1 port with a scratch
# data_dir, records commit, config, seed and limits in run.json, samples GET /metrics and
# GET /v1/capacity every TSLS_LOAD_SAMPLE_MS into samples.jsonl, and writes summary.json,
# timeline.svg and timeline.txt (apps/load, `tsls-load report`).
#
# Declared limits (enforced by tsls-load, recorded in run.json and summary.json; the
# tool's compiled ceilings are 64 / 2000 / 1800 s):
#   TSLS_LOAD_MAX_CONCURRENCY      default 12   invocations in flight at once
#   TSLS_LOAD_MAX_REQUESTS         default 150  invocations per scenario
#   TSLS_LOAD_MAX_DURATION_SECONDS default 240  wall time per scenario
# Load is only sent to 127.0.0.1 (or TSLS_LOAD_LAB_HOST, which must be named explicitly);
# tsls-load refuses anything else, including a TSLS_API_URL that points elsewhere.
#
# Other environment (optional):
#   TSLS_LOAD_SEED        jitter seed (default 20260917)
#   TSLS_LOAD_SAMPLE_MS   sampling interval (default 250)
#   TSLS_PROVIDER         label for the evidence directory (default process)
#   TSLS_GATEWAY_CONFIG   use this gateway config instead of generating one (any provider, e.g.
#                         Firecracker); also set TSLS_API_URL, TSLS_TOKEN_A, TSLS_TOKEN_B,
#                         TSLS_METRICS_TOKEN (its [metrics] bearer_token) and TSLS_GUEST_DIR.
#   TSLS_GUEST_DIR        directory with example-cpu-burn (default target/debug)
#   TSLS_EVIDENCE_DIR     evidence root (default docs/evidence)
#   TSLS_SKIP_BUILD=1     do not run cargo build
#
# Numbers in the results are observations of one run on one machine, never an SLA.
# Exit 0 only when every scenario's checks passed and no detector fired.
#
# EXIT-trap cleanup and helpers are called indirectly (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$REPO_ROOT/scripts/e2e/lib.sh"

require_tools curl jq cargo python3 git || e2e_die "missing tools"

PROVIDER="${TSLS_PROVIDER:-process}"
SEED="${TSLS_LOAD_SEED:-20260917}"
MAX_CONCURRENCY="${TSLS_LOAD_MAX_CONCURRENCY:-12}"
MAX_REQUESTS="${TSLS_LOAD_MAX_REQUESTS:-150}"
MAX_DURATION="${TSLS_LOAD_MAX_DURATION_SECONDS:-240}"
SAMPLE_MS="${TSLS_LOAD_SAMPLE_MS:-250}"
EVIDENCE_ROOT="${TSLS_EVIDENCE_DIR:-$REPO_ROOT/docs/evidence}"
GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/debug}"
GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
LOAD_BIN="$REPO_ROOT/target/debug/tsls-load"
TENANT_A_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TENANT_B_ID="tn_01hzzzzzzzzzzzzzzzzzzzzzzb"
case "$(uname -m)" in
  x86_64 | amd64) ARCH=x86_64 ;;
  *) ARCH=aarch64 ;;
esac
LAB_ARGS=()
if [ -n "${TSLS_LOAD_LAB_HOST:-}" ]; then LAB_ARGS=(--lab-host "$TSLS_LOAD_LAB_HOST"); fi
LIMIT_ARGS=(--max-concurrency "$MAX_CONCURRENCY" --max-requests "$MAX_REQUESTS"
  --max-duration-seconds "$MAX_DURATION")

if [ "$#" -gt 0 ]; then
  SCENARIOS=("$@")
else
  SCENARIOS=(single burst idle-to-zero mixed restart lifecycle)
fi

if [ "${TSLS_SKIP_BUILD:-0}" != "1" ]; then
  (cd "$REPO_ROOT" && cargo build -q -p tachyon-serverless-gateway -p tachyon-serverless-cli \
    -p tachyon-serverless-runtime-bridge -p example-cpu-burn -p tachyon-serverless-load)
fi
[ -x "$GUEST_DIR/example-cpu-burn" ] || e2e_die "missing $GUEST_DIR/example-cpu-burn"

GATEWAY_PID=""
SAMPLER_PID=""
WORK_DIR=""
cleanup() {
  if [ -n "$SAMPLER_PID" ]; then kill "$SAMPLER_PID" 2>/dev/null || true; fi
  if [ -n "$GATEWAY_PID" ]; then stop_process "$GATEWAY_PID" 15; fi
  if [ -n "$WORK_DIR" ]; then rm -rf "$WORK_DIR"; fi
}
trap cleanup EXIT

# start_gateway -> GATEWAY_PID (appends to $WORK_DIR/gateway.log)
start_gateway() {
  "$GATEWAY_BIN" --config "$CONFIG" >>"$WORK_DIR/gateway.log" 2>&1 &
  GATEWAY_PID=$!
  wait_for_http "$API_URL/readyz" 90 || { tail -n 40 "$WORK_DIR/gateway.log" >&2; return 1; }
}

stop_gateway() {
  stop_process "$GATEWAY_PID" 15
  GATEWAY_PID=""
}

# setup_scenario NAME -> WORK_DIR, OUT, CONFIG, API_URL, tokens; gateway running
setup_scenario() {
  local name="$1"
  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-load.XXXXXX")"
  OUT="$EVIDENCE_ROOT/load-$name-$(date -u +%Y%m%dT%H%M%SZ)-$PROVIDER"
  mkdir -p "$OUT"
  if [ -n "${TSLS_GATEWAY_CONFIG:-}" ]; then
    CONFIG="$TSLS_GATEWAY_CONFIG"
    API_URL="${TSLS_API_URL:?TSLS_API_URL is required with TSLS_GATEWAY_CONFIG}"
    TOKEN_A="${TSLS_TOKEN_A:?}"
    TOKEN_B="${TSLS_TOKEN_B:?}"
    METRICS_TOKEN="${TSLS_METRICS_TOKEN:?}"
  else
    local port
    port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
    API_URL="http://127.0.0.1:$port"
    TOKEN_A="load-token-tenant-a"
    TOKEN_B="load-token-tenant-b"
    METRICS_TOKEN="load-metrics-operator-token"
    CONFIG="$WORK_DIR/gateway.toml"
    cat > "$CONFIG" <<EOF
listen = "127.0.0.1:$port"
profile = "dev"
data_dir = "$WORK_DIR/data"

[provider]
kind = "process"

[provider.process]
bridge_binary = "$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
workdir = "$WORK_DIR/data/process"

[capacity]
max_concurrency = 4
max_queue = 64
queue_timeout_seconds = 60

[capacity.tenant_defaults]
max_concurrency = 3

[capacity.start_rate]
per_second = 50
burst = 50

[scaling]
reconcile_interval_ms = 500
scale_down_cooldown_seconds = 1

[metrics]
bearer_token = "$METRICS_TOKEN"

[[identity.tokens]]
token = "$TOKEN_A"
tenant_id = "$TENANT_A_ID"
subject = "load-a"
roles = ["deploy", "invoke"]

[[identity.tokens]]
token = "$TOKEN_B"
tenant_id = "$TENANT_B_ID"
subject = "load-b"
roles = ["deploy", "invoke"]
EOF
  fi
  # Refuse a non-local target before anything is started or deployed.
  "$LOAD_BIN" check-target --base-url "$API_URL" ${LAB_ARGS[@]+"${LAB_ARGS[@]}"} >/dev/null
  sed -E 's/^(token|bearer_token|internal_token|password) = .*/\1 = "<redacted>"/' "$CONFIG" \
    > "$OUT/gateway.toml"
  : > "$WORK_DIR/gateway.log"
  start_gateway
  FN_A="$(deploy_fn "$TOKEN_A" "load-$name-a")"
  FN_B="$(deploy_fn "$TOKEN_B" "load-$name-b")"
}

# deploy_fn TOKEN NAME -> function id (revision: max_concurrency 3, idle TTL 2 s, cooldown 1 s)
deploy_fn() {
  local fn
  fn="$(TSLS_API_URL="$API_URL" TSLS_TOKEN="$1" "$TSLS_BIN" functions create --name "$2" \
    --description plt-4637 --json | jq -r .id)"
  TSLS_API_URL="$API_URL" TSLS_TOKEN="$1" "$TSLS_BIN" functions deploy --function "$fn" \
    --binary "$GUEST_DIR/example-cpu-burn" --arch "$ARCH" --max-concurrency 3 \
    --timeout-seconds 60 --idle-ttl-seconds 2 --scale-down-cooldown-seconds 1 --json >/dev/null
  printf '%s' "$fn"
}

start_sampler() {
  "$LOAD_BIN" sample --base-url "$API_URL" ${LAB_ARGS[@]+"${LAB_ARGS[@]}"} --out "$OUT" \
    --metrics-token "$METRICS_TOKEN" --token-a "$TOKEN_A" --interval-ms "$SAMPLE_MS" \
    --max-duration-seconds "$MAX_DURATION" 2>>"$WORK_DIR/sampler.log" &
  SAMPLER_PID=$!
  sleep 1
}

stop_sampler() {
  touch "$OUT/stop"
  wait "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
  rm -f "$OUT/stop"
}

# load PHASE_SPEC... (every phase is `--phase` for tsls-load)
load() {
  local args=() p
  for p in "$@"; do args+=(--phase "$p"); done
  "$LOAD_BIN" load --base-url "$API_URL" ${LAB_ARGS[@]+"${LAB_ARGS[@]}"} --out "$OUT" \
    "${LIMIT_ARGS[@]}" --seed "$SEED" --token-a "$TOKEN_A" --function-a "$FN_A" \
    --token-b "$TOKEN_B" --function-b "$FN_B" "${args[@]}"
}

restart_gateway() {
  echo "restarting the gateway while idle" >&2
  stop_gateway
  sleep 2
  start_gateway
}

write_run_json() { # write_run_json NAME PHASES_DESCRIPTION
  local commit dirty
  # A tree copied to a KVM host is not a git checkout: TSLS_COMMIT / TSLS_DIRTY_FILES name it.
  commit="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "${TSLS_COMMIT:-unknown}")"
  dirty="$(git -C "$REPO_ROOT" status --porcelain --untracked-files=no 2>/dev/null | wc -l | tr -d ' ')"
  if ! git -C "$REPO_ROOT" rev-parse HEAD >/dev/null 2>&1; then dirty="${TSLS_DIRTY_FILES:-0}"; fi
  jq -n --arg scenario "$1" --arg plan "$2" --arg commit "$commit" --argjson dirty_files "$dirty" \
    --arg provider "$PROVIDER" --argjson seed "$SEED" --argjson sample_ms "$SAMPLE_MS" \
    --argjson max_concurrency "$MAX_CONCURRENCY" --argjson max_requests "$MAX_REQUESTS" \
    --argjson max_duration_seconds "$MAX_DURATION" \
    --arg config_sha256 "$(shasum -a 256 "$OUT/gateway.toml" | cut -d' ' -f1)" \
    --arg host "$(uname -srm)" --arg rustc "$(rustc --version 2>/dev/null || echo unknown)" \
    --arg target "$API_URL" --arg started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{scenario: $scenario, plan: $plan, commit: $commit, dirty_tracked_files: $dirty_files,
      provider: $provider, seed: $seed, sample_interval_ms: $sample_ms,
      limits: {max_concurrency: $max_concurrency, max_requests: $max_requests,
               max_duration_seconds: $max_duration_seconds},
      gateway_config: "gateway.toml", gateway_config_sha256: $config_sha256,
      host: $host, rustc: $rustc, target: $target, started_at: $started_at,
      revision: {max_concurrency: 3, idle_ttl_seconds: 2, scale_down_cooldown_seconds: 1},
      note: "observations of one run on one machine; not an SLA"}' > "$OUT/run.json"
}

report() { # report TITLE CAP EXPECT
  local reuse
  reuse="$(tail -n 1 "$OUT/samples.jsonl" | jq -r '.capacity.reuse.mode // "unknown"')"
  "$LOAD_BIN" report --out "$OUT" "${LIMIT_ARGS[@]}" --title "$1" --cap "$2" \
    --expect "$3,$reuse" > "$OUT/report.txt" 2>&1
}

finish_scenario() {
  local rc=$1
  stop_sampler
  if [ -n "$GATEWAY_PID" ]; then stop_gateway; fi
  cp "$WORK_DIR/gateway.log" "$OUT/gateway.log" 2>/dev/null || true
  rm -rf "$WORK_DIR"
  WORK_DIR=""
  if [ "$rc" -eq 0 ]; then
    printf 'PASS  %s  %s\n' "$SCENARIO" "$OUT" | tee -a "$SUMMARY"
  else
    printf 'FAIL  %s  %s (see report.txt)\n' "$SCENARIO" "$OUT" | tee -a "$SUMMARY"
    FAILED=1
  fi
}

IDLE="idle_until_zero_ms=30000"
FAILED=0
SUMMARY="$(mktemp "${TMPDIR:-/tmp}/tsls-load-summary.XXXXXX")"
for SCENARIO in "${SCENARIOS[@]}"; do
  e2e_log "scenario: $SCENARIO"
  setup_scenario "$SCENARIO"
  start_sampler
  rc=0
  case "$SCENARIO" in
    single)
      write_run_json single "three single 0.2 s invocations 2 s apart, idle until zero"
      load "name=single-1,concurrency=1,requests=1,handler_ms=200" "name=gap-1,pause_ms=2000" \
        "name=single-2,concurrency=1,requests=1,handler_ms=200" "name=gap-2,pause_ms=2000" \
        "name=single-3,concurrency=1,requests=1,handler_ms=200" "name=idle,$IDLE" || rc=1
      sleep 1
      report "single: 0 -> 1 -> 0 (x3)" 1 "zero_before,rose,zero_after,all_succeeded,no_findings" || rc=1
      ;;
    burst)
      write_run_json burst "ramp 1 -> 2 -> burst 8 workers over a revision cap of 3 -> 1 -> idle until zero"
      load "name=ramp-1,concurrency=1,requests=3,handler_ms=300,jitter_ms=100" \
        "name=ramp-2,concurrency=2,requests=6,handler_ms=300,jitter_ms=100" \
        "name=burst,concurrency=8,requests=24,handler_ms=500,jitter_ms=100" \
        "name=down,concurrency=1,requests=3,handler_ms=300,jitter_ms=100" "name=idle,$IDLE" || rc=1
      sleep 1
      report "burst: 0 -> up -> cap (3) -> down -> 0" 3 \
        "zero_before,rose,cap,queue,decreased,zero_after,all_succeeded,no_findings" || rc=1
      ;;
    idle-to-zero)
      write_run_json idle-to-zero "burst of 8 on 4 workers, idle until zero, 5 s idle, one re-access"
      load "name=burst,concurrency=4,requests=8,handler_ms=300,jitter_ms=100" "name=idle,$IDLE" \
        "name=stay-idle,pause_ms=5000" "name=reaccess,concurrency=1,requests=1,handler_ms=100" \
        "name=idle-again,$IDLE" || rc=1
      sleep 1
      report "idle-to-zero: burst -> 0 -> idle -> re-access -> 0" 3 \
        "zero_before,rose,zero_after,all_succeeded,no_findings" || rc=1
      ;;
    mixed)
      write_run_json mixed "tenant A 6 workers x 2 s handlers and tenant B 2 workers x 0.1 s handlers at once (node 4, tenant quota 3)"
      load "name=a-long,tenant=a,concurrency=6,requests=12,handler_ms=2000,jitter_ms=200,wave=1" \
        "name=b-short,tenant=b,concurrency=2,requests=16,handler_ms=100,jitter_ms=300,wave=1" \
        "name=idle,$IDLE" || rc=1
      sleep 1
      report "mixed: long tenant A and short tenant B at once" 4 \
        "rose,cap,queue,zero_after,two_tenants_served,all_succeeded,no_findings" || rc=1
      ;;
    restart)
      write_run_json restart "load, idle until zero, gateway restart while idle, load, idle until zero"
      load "name=before,concurrency=2,requests=6,handler_ms=300,jitter_ms=100" "name=idle,$IDLE" || rc=1
      restart_gateway || rc=1
      load "name=after,concurrency=2,requests=4,handler_ms=300,jitter_ms=100" "name=idle-after,$IDLE" || rc=1
      sleep 1
      report "restart: load -> 0 -> gateway restart while idle -> load -> 0" 2 \
        "zero_before,rose,zero_after,restart,active_after_restart,zero_after_restart,all_succeeded,no_findings" || rc=1
      ;;
    lifecycle)
      write_run_json lifecycle "0 -> ramp 1,2 -> burst 8 workers over a cap of 3 -> down 1 -> idle until zero -> gateway restart -> 2 workers -> idle until zero"
      load "name=ramp-1,concurrency=1,requests=3,handler_ms=400,jitter_ms=100" \
        "name=ramp-2,concurrency=2,requests=6,handler_ms=400,jitter_ms=100" \
        "name=cap,concurrency=8,requests=24,handler_ms=600,jitter_ms=100" \
        "name=down,concurrency=1,requests=3,handler_ms=400,jitter_ms=100" \
        "name=idle,$IDLE" "name=stay-idle,pause_ms=3000" || rc=1
      restart_gateway || rc=1
      load "name=after-restart,concurrency=2,requests=6,handler_ms=400,jitter_ms=100" \
        "name=idle-after,$IDLE" || rc=1
      sleep 1
      report "lifecycle: 0 -> up -> cap (3) -> down -> 0 -> restart -> up -> 0" 3 \
        "zero_before,rose,cap,queue,decreased,zero_after,restart,active_after_restart,zero_after_restart,all_succeeded,no_findings" || rc=1
      ;;
    *)
      e2e_die "unknown scenario $SCENARIO"
      ;;
  esac
  finish_scenario "$rc"
done
cat "$SUMMARY"
rm -f "$SUMMARY"
exit "$FAILED"

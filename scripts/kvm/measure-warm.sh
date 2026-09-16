#!/usr/bin/env bash
# scripts/kvm/measure-warm.sh - measure PLT-4633 (idle quiesce / resume) on a real
# Firecracker guest: what a warm start costs, what a paused microVM costs the host,
# and whether a warm start ever happened at all.
#
# Flow: build (host tools + the guest musl example) -> write a gateway config with
# environment reuse switched on -> start that gateway -> deploy examples/hello ->
# invoke it N times -> record for every invocation the start kind, the boot (cold) or
# resume (warm) time, the handler time and the total -> sample the host-side resource
# usage of the paused VMM process while its environment sits in the pool -> stop the
# gateway -> print a cold vs warm table -> write evidence under docs/evidence/warm-<UTC>/.
#
# Usage:
#   scripts/kvm/measure-warm.sh            # Linux/KVM, see docs/kvm.md section 3.7
#
# Environment (all optional):
#   TSLS_SKIP_BUILD=1     do not run cargo build
#   TSLS_GATEWAY_CONFIG   base gateway config (default config/gateway.firecracker.toml).
#                         It must not already contain a [pool] section; this script
#                         appends one and records the result as evidence.
#   TSLS_API_URL          URL that config listens on (default http://127.0.0.1:8080)
#   TSLS_TOKEN            tenant token of that config (default dev-token-tenant-a)
#   EVIDENCE_ROOT         where to save results (default docs/evidence)
#   WARM_FUNCTION         function name (default warm-probe)
#   WARM_INVOCATIONS      how many invocations to take (default 6, minimum 2)
#   WARM_MEMORY_MIB / WARM_CPU_MILLIS / WARM_TIMEOUT_SECONDS   revision resources
#                         (default 256 / 500 / 30)
#   WARM_IDLE_TTL_SECONDS idle TTL of the pool for this run (default 300; long enough
#                         that the sweeper cannot reap the environment mid-measurement)
#   PAUSED_SAMPLE_SECONDS gap between the two samples of the paused VMM (default 3)
#
# Output: docs/evidence/warm-<UTC>/{summary.txt,summary.json,comparison.json,
#   attempts.jsonl,invocations/NN.json,paused-vmm.json,provider.json,gateway.toml,
#   gateway.log,steps/} plus a cold vs warm table on stdout.
#
# Exit codes:
#   0  the measurement ran and at least one invocation was served warm
#   1  reuse was on but nothing was ever served warm - the thing being measured did
#      not happen, which is a loud failure on purpose
#   2  the measurement could not be taken (build, gateway, deploy or invoke failure)
#
# This script never changes the provider's Capabilities. The Firecracker provider
# reports idle_quiesce / idle_resume as "unverified", and reuse only runs here because
# the generated config sets [pool] allow_unverified_idle. The evidence records that
# the run was a measurement, not a verified warm configuration: promoting the
# capability to Supported is a separate, reviewed decision that cites this evidence
# (docs/architecture.md section 4, docs/adr/0001).
#
# The step functions run through `step` (scripts/e2e/lib.sh) and `cleanup` through a trap,
# neither of which shellcheck can follow (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../e2e/lib.sh
. "$SCRIPT_DIR/../e2e/lib.sh"
cd "$REPO_ROOT"

require_tools curl jq cargo || e2e_die "missing tools"

TSLS_SKIP_BUILD="${TSLS_SKIP_BUILD:-0}"
BASE_CONFIG="${TSLS_GATEWAY_CONFIG:-$REPO_ROOT/config/gateway.firecracker.toml}"
API_URL="${TSLS_API_URL:-http://127.0.0.1:8080}"
TOKEN="${TSLS_TOKEN:-dev-token-tenant-a}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$REPO_ROOT/docs/evidence}"
FUNCTION_NAME="${WARM_FUNCTION:-warm-probe}"
INVOCATIONS="${WARM_INVOCATIONS:-6}"
WARM_MEMORY_MIB="${WARM_MEMORY_MIB:-256}"
WARM_CPU_MILLIS="${WARM_CPU_MILLIS:-500}"
WARM_TIMEOUT_SECONDS="${WARM_TIMEOUT_SECONDS:-30}"
IDLE_TTL_SECONDS="${WARM_IDLE_TTL_SECONDS:-300}"
PAUSED_SAMPLE_SECONDS="${PAUSED_SAMPLE_SECONDS:-3}"

[ -f "$BASE_CONFIG" ] || e2e_die "gateway config not found: $BASE_CONFIG"
[ "$INVOCATIONS" -ge 2 ] || e2e_die "WARM_INVOCATIONS must be at least 2 (one cold, one warm)"

HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
  x86_64|amd64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *) e2e_die "unsupported host architecture $HOST_ARCH" ;;
esac

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$EVIDENCE_ROOT/warm-$STAMP"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-warm.XXXXXX")"
export E2E_STATE_DIR="$WORK_DIR/state"
export STEP_LOG_DIR="$EVIDENCE_DIR/steps"
mkdir -p "$EVIDENCE_DIR" "$STEP_LOG_DIR" "$E2E_STATE_DIR" "$EVIDENCE_DIR/invocations"
GATEWAY_LOG="$EVIDENCE_DIR/gateway.log"
SUMMARY_TXT="$EVIDENCE_DIR/summary.txt"
CONFIG_PATH="$EVIDENCE_DIR/gateway.toml"
ATTEMPTS="$EVIDENCE_DIR/attempts.jsonl"
COMPARISON="$EVIDENCE_DIR/comparison.json"
: > "$ATTEMPTS"

TSLS_BIN="${TSLS_BIN:-$REPO_ROOT/target/debug/tsls}"
GATEWAY_BIN="${TSLS_GATEWAY_BIN:-$REPO_ROOT/target/debug/tachyon-serverless-gateway}"
GATEWAY_CONFIG_FLAG="${TSLS_GATEWAY_CONFIG_FLAG:---config}"
GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/$ARCH-unknown-linux-musl/release}"
FUNCTION_BIN="$GUEST_DIR/example-hello"
# The provider's run directory, used to find the paused VMM and for the orphan note.
FC_RUN_DIR="$(sed -n 's/^workdir *= *"\(.*\)"$/\1/p' "$BASE_CONFIG" | head -n1)"
FC_RUN_DIR="${FC_RUN_DIR:-.kvm/run}"
case "$FC_RUN_DIR" in /*) ;; *) FC_RUN_DIR="$REPO_ROOT/${FC_RUN_DIR#./}" ;; esac

tsls() { "$TSLS_BIN" "$@"; }
export TSLS_BIN

GATEWAY_PID=""
REUSE_ENABLED="unknown"
REUSE_VERIFIED="unknown"
REUSE_REASON="not read"
WARM_COUNT=0
COLD_COUNT=0
ORPHAN_NOTE="not checked"
FINDINGS=""

cleanup() {
  local rc=$?
  set +e
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    e2e_log "cleanup: stopping gateway $GATEWAY_PID"
    stop_process "$GATEWAY_PID" 15
  fi
  rm -rf "$WORK_DIR"
  exit "$rc"
}
trap cleanup EXIT INT TERM

note_finding() { # note_finding TEXT
  e2e_warn "finding: $1"
  FINDINGS="$FINDINGS$1"$'\n'
}

# ---------------------------------------------------------------------------
# 1. build, config and gateway
# ---------------------------------------------------------------------------

build_all() {
  if [ "$TSLS_SKIP_BUILD" = "1" ]; then
    e2e_log "TSLS_SKIP_BUILD=1: skipping cargo build"
  else
    cargo build -p tachyon-serverless-gateway -p tachyon-serverless-cli
    cargo build --release --target "$ARCH-unknown-linux-musl" \
      -p tachyon-serverless-runtime-bridge -p example-hello
  fi
  local b
  for b in "$TSLS_BIN" "$GATEWAY_BIN" "$FUNCTION_BIN"; do
    [ -x "$b" ] || { echo "missing binary: $b (run scripts/kvm/bootstrap.sh first)" >&2; return 1; }
  done
}

# A copy of the base config with environment reuse switched on. Written into the
# evidence directory so the run records exactly what it measured.
write_config() {
  if grep -qE '^[[:space:]]*\[pool\]' "$BASE_CONFIG"; then
    echo "the base config $BASE_CONFIG already has a [pool] section." >&2
    echo "Point TSLS_GATEWAY_CONFIG at a config without one, or run the gateway yourself." >&2
    return 1
  fi
  cp "$BASE_CONFIG" "$CONFIG_PATH"
  cat >> "$CONFIG_PATH" <<EOF

# Added by scripts/kvm/measure-warm.sh (PLT-4633).
#
# allow_unverified_idle accepts the provider's "unverified" idle capability so that
# this measurement can be taken at all. It does not make the configuration verified:
# GET /v1/provider reports reuse.verified = false and the gateway warns at startup.
[pool]
enabled = true
allow_unverified_idle = true
max_idle_per_revision = 1
idle_ttl_seconds = $IDLE_TTL_SECONDS
max_total_idle = 4
EOF
  e2e_log "gateway config with reuse on: $CONFIG_PATH"
}

start_gateway() {
  LOG_FORMAT=json TACHYON_GATEWAY_CONFIG="$CONFIG_PATH" \
    "$GATEWAY_BIN" "$GATEWAY_CONFIG_FLAG" "$CONFIG_PATH" >"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  e2e_log "gateway pid $GATEWAY_PID, config $CONFIG_PATH, log $GATEWAY_LOG"
  export TSLS_API_URL="$API_URL"
  export TSLS_TOKEN="$TOKEN"
}

wait_gateway() {
  local i
  for i in $(seq 1 60); do
    if ! kill -0 "$GATEWAY_PID" 2>/dev/null; then
      echo "gateway process $GATEWAY_PID exited during startup (port in use or invalid config)" >&2
      tail -n 30 "$GATEWAY_LOG" >&2
      return 1
    fi
    curl -fsS -m 2 -o /dev/null "$API_URL/healthz" 2>/dev/null && break
    sleep 0.5
  done
  wait_for_http "$API_URL/healthz" 30 || { tail -n 30 "$GATEWAY_LOG" >&2; return 1; }
  wait_for_http "$API_URL/readyz" 60 || {
    echo "gateway is not ready: run scripts/kvm/preflight.sh and scripts/kvm/bootstrap.sh" >&2
    tail -n 30 "$GATEWAY_LOG" >&2
    return 1
  }
  tsls health
}

# The provider view is the record of what was measured and under which gate.
check_provider() {
  local provider
  tsls provider --json > "$EVIDENCE_DIR/provider.json"
  provider="$(cat "$EVIDENCE_DIR/provider.json")"
  assert_json "$provider" '.kind' "firecracker" || {
    echo "this measurement is only meaningful on the firecracker provider" >&2
    return 1
  }
  assert_json "$provider" '.isolation' "micro_vm"
  REUSE_ENABLED="$(printf '%s' "$provider" | jq -r '.reuse.enabled')"
  REUSE_VERIFIED="$(printf '%s' "$provider" | jq -r '.reuse.verified')"
  REUSE_REASON="$(printf '%s' "$provider" | jq -r '.reuse.reason')"
  e2e_log "reuse: enabled=$REUSE_ENABLED verified=$REUSE_VERIFIED ($REUSE_REASON)"
  printf '%s\n' "  idle_quiesce=$(printf '%s' "$provider" | jq -r '.capabilities.idle_quiesce.status')"
  printf '%s\n' "  idle_resume=$(printf '%s' "$provider" | jq -r '.capabilities.idle_resume.status')"
  if [ "$REUSE_ENABLED" != "true" ]; then
    echo "environment reuse is off, so there is nothing to measure: $REUSE_REASON" >&2
    return 1
  fi
  if [ "$REUSE_VERIFIED" = "true" ]; then
    e2e_log "note: this provider already reports both idle capabilities as supported"
  fi
}

# ---------------------------------------------------------------------------
# 2. function and revision
# ---------------------------------------------------------------------------

ensure_function() {
  local id
  run_capture tsls functions get "$FUNCTION_NAME" --json
  if [ "$RUN_RC" -eq 0 ]; then
    id="$(printf '%s' "$RUN_OUT" | jq -r .id)"
    e2e_log "function $FUNCTION_NAME exists: $id"
  elif [ "$RUN_RC" -eq 2 ]; then
    id="$(tsls functions create --name "$FUNCTION_NAME" \
      --description "PLT-4633 warm reuse measurement" --json | jq -r .id)"
    e2e_log "function $FUNCTION_NAME created: $id"
  else
    echo "unexpected exit $RUN_RC from functions get $FUNCTION_NAME: $RUN_ERR" >&2
    return 1
  fi
  state_set "fn" "$id"
}

deploy_revision() {
  local rev
  rev="$(tsls functions deploy --function "$FUNCTION_NAME" --binary "$FUNCTION_BIN" \
    --arch "$ARCH" --memory-mib "$WARM_MEMORY_MIB" --cpu-millis "$WARM_CPU_MILLIS" \
    --timeout-seconds "$WARM_TIMEOUT_SECONDS" --description "warm reuse probe" --json | jq -r .id)"
  if [ -z "$rev" ] || [ "$rev" = "null" ]; then
    echo "deploy did not return a revision id" >&2
    return 1
  fi
  state_set "rev.warm" "$rev"
  tsls functions revision "$FUNCTION_NAME" "$rev" --json > "$EVIDENCE_DIR/revision.json"
  e2e_log "revision $rev (${WARM_MEMORY_MIB} MiB, ${WARM_CPU_MILLIS} m)"
}

# ---------------------------------------------------------------------------
# 3. invocations
# ---------------------------------------------------------------------------

# invoke_once N -- invoke, record the attempt row, print one line.
invoke_once() {
  local n="$1" id file payload row
  payload="$(jq -nc --argjson n "$n" '{name: "warm", n: $n}')"
  invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.warm)"
  if [ "$INVOKE_RC" -ne 0 ]; then
    printf '%s\n' "$INVOKE_ERR" >&2
    echo "invocation $n failed with exit $INVOKE_RC" >&2
    return 1
  fi
  id="$INVOKE_ID"
  if [ -z "$id" ]; then
    echo "invocation $n returned no invocation id" >&2
    return 1
  fi
  file="$EVIDENCE_DIR/invocations/$(printf '%02d' "$n").json"
  tsls functions invocation "$id" --json > "$file"
  row="$(jq -c --argjson n "$n" --argjson client_ms "$INVOKE_MS" '
    (.attempts[-1] // {}) as $a |
    {n: $n, invocation: .id, status: .status, attempts: (.attempts | length),
     start_kind: ($a.start_kind // "unknown"),
     environment_id: ($a.environment_id // null),
     epoch: ($a.epoch // null),
     boot_ms: ($a.timings.environment_boot_ms // null),
     init_ms: ($a.timings.runtime_init_ms // null),
     resume_ms: ($a.timings.resume_ms // null),
     readiness_ms: ($a.timings.readiness_ms // null),
     handler_ms: ($a.timings.handler_ms // null),
     total_ms: ($a.timings.total_ms // null),
     client_ms: $client_ms}' "$file")"
  printf '%s\n' "$row" >> "$ATTEMPTS"
  printf '%s\n' "$row" | jq -r '"  #\(.n) \(.start_kind) env=\(.environment_id // "-") boot=\(.boot_ms // "-") resume=\(.resume_ms // "-") readiness=\(.readiness_ms // "-") handler=\(.handler_ms // "-") total=\(.total_ms // "-") client=\(.client_ms)"'
  if [ "$n" -eq 1 ]; then
    state_set "warm.env" "$(printf '%s' "$row" | jq -r '.environment_id // empty')"
  fi
}

run_invocations() {
  local n
  for n in $(seq 1 "$INVOKE_FIRST_BATCH"); do
    invoke_once "$n" || return 1
  done
}

run_remaining_invocations() {
  local n
  for n in $(seq $(( INVOKE_FIRST_BATCH + 1 )) "$INVOCATIONS"); do
    invoke_once "$n" || return 1
  done
}

# ---------------------------------------------------------------------------
# 4. the paused VMM on the host
# ---------------------------------------------------------------------------

# One sample of a pid: RSS / VSZ / state / accumulated CPU ticks.
sample_pid() { # sample_pid PID
  local pid="$1" rss vsz state vmrss vmsize threads utime stime
  rss="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')"
  vsz="$(ps -o vsz= -p "$pid" 2>/dev/null | tr -d ' ')"
  state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ')"
  if [ -r "/proc/$pid/status" ]; then
    vmrss="$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status")"
    vmsize="$(awk '/^VmSize:/ {print $2}' "/proc/$pid/status")"
    threads="$(awk '/^Threads:/ {print $2}' "/proc/$pid/status")"
  fi
  if [ -r "/proc/$pid/stat" ]; then
    utime="$(awk '{print $14}' "/proc/$pid/stat")"
    stime="$(awk '{print $15}' "/proc/$pid/stat")"
  fi
  jq -n --arg rss "${rss:-}" --arg vsz "${vsz:-}" --arg state "${state:-}" \
    --arg vmrss "${vmrss:-}" --arg vmsize "${vmsize:-}" --arg threads "${threads:-}" \
    --arg utime "${utime:-}" --arg stime "${stime:-}" \
    --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '
    def num: if . == "" then null else tonumber end;
    {at: $at, ps_rss_kib: ($rss | num), ps_vsz_kib: ($vsz | num), ps_state: $state,
     vm_rss_kib: ($vmrss | num), vm_size_kib: ($vmsize | num),
     threads: ($threads | num),
     cpu_ticks: {utime: ($utime | num), stime: ($stime | num)}}'
}

# While the environment sits in the pool its microVM is paused. Two samples a few
# seconds apart show both what it holds (RSS) and what it burns (CPU ticks).
capture_paused_vmm() {
  local env_id dir pid_file pid first second out
  out="$EVIDENCE_DIR/paused-vmm.json"
  env_id="$(state_get warm.env)"
  if [ -z "$env_id" ]; then
    jq -n '{sampled: false, reason: "no environment id was recorded for the first invocation"}' > "$out"
    note_finding "no environment id for the first invocation; the paused VMM was not sampled"
    return 0
  fi
  dir="$FC_RUN_DIR/$env_id"
  pid_file="$dir/fc.pid"
  if [ ! -r "$pid_file" ]; then
    jq -n --arg env "$env_id" --arg dir "$dir" \
      '{sampled: false, environment_id: $env, env_dir: $dir, reason: "no pid file while the environment was pooled"}' > "$out"
    note_finding "the pooled environment $env_id has no pid file at $pid_file"
    return 0
  fi
  pid="$(tr -d '[:space:]' < "$pid_file")"
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
    jq -n --arg env "$env_id" --arg pid "${pid:-}" \
      '{sampled: false, environment_id: $env, pid: $pid, reason: "no live VMM process while the environment was pooled"}' > "$out"
    note_finding "the pooled environment $env_id has no live VMM process (pid ${pid:-unknown})"
    return 0
  fi
  first="$(sample_pid "$pid")"
  sleep "$PAUSED_SAMPLE_SECONDS"
  second="$(sample_pid "$pid")"
  jq -n --arg env "$env_id" --argjson pid "$pid" --arg dir "$dir" \
    --argjson gap "$PAUSED_SAMPLE_SECONDS" \
    --argjson first "$first" --argjson second "$second" '
    {sampled: true, environment_id: $env, pid: $pid, env_dir: $dir,
     note: "sampled while the environment was idle in the pool, i.e. after idle_quiesce returned",
     gap_seconds: $gap, samples: [$first, $second],
     cpu_ticks_during_gap:
       (if ($first.cpu_ticks.utime != null and $second.cpu_ticks.utime != null)
        then (($second.cpu_ticks.utime + $second.cpu_ticks.stime)
              - ($first.cpu_ticks.utime + $first.cpu_ticks.stime))
        else null end)}' > "$out"
  jq -r '"  paused vmm pid \(.pid): rss=\(.samples[0].vm_rss_kib // .samples[0].ps_rss_kib // "-") kiB, state=\(.samples[0].ps_state // "-"), cpu ticks over \(.gap_seconds)s = \(.cpu_ticks_during_gap // "-")"' "$out"
}

orphan_note() {
  local out rc
  if [ ! -x "$REPO_ROOT/scripts/e2e/orphan-check.sh" ] || ! command -v pgrep >/dev/null 2>&1; then
    ORPHAN_NOTE="skipped (orphan-check.sh or pgrep unavailable)"
    return 0
  fi
  set +e
  out="$("$REPO_ROOT/scripts/e2e/orphan-check.sh" firecracker "$FC_RUN_DIR" 2>&1)"
  rc=$?
  set -e
  printf '%s\n' "$out" > "$EVIDENCE_DIR/orphan-check.txt"
  if [ "$rc" -eq 0 ]; then
    ORPHAN_NOTE="clean"
  else
    ORPHAN_NOTE="leftovers found (see orphan-check.txt)"
    note_finding "cleanup: $ORPHAN_NOTE"
  fi
}

# ---------------------------------------------------------------------------
# 5. comparison and reporting
# ---------------------------------------------------------------------------

json_file_ok() { [ -s "$1" ] && jq -e . "$1" >/dev/null 2>&1; }

compare() {
  jq -s '
    def med: map(select(. != null)) | sort
      | if length == 0 then null
        elif length % 2 == 1 then .[(length / 2) | floor]
        else ((.[length / 2 - 1] + .[length / 2]) / 2) end;
    { cold: [.[] | select(.start_kind == "cold")],
      warm: [.[] | select(.start_kind == "warm")],
      other: [.[] | select(.start_kind != "cold" and .start_kind != "warm")] }
    | { invocations: ((.cold | length) + (.warm | length) + (.other | length)),
        cold_count: (.cold | length),
        warm_count: (.warm | length),
        other_count: (.other | length),
        environments: ([.cold[], .warm[]] | map(.environment_id) | unique | length),
        cold: {boot_ms: ([.cold[].boot_ms] | med),
               init_ms: ([.cold[].init_ms] | med),
               handler_ms: ([.cold[].handler_ms] | med),
               total_ms: ([.cold[].total_ms] | med),
               client_ms: ([.cold[].client_ms] | med)},
        warm: {resume_ms: ([.warm[].resume_ms] | med),
               readiness_ms: ([.warm[].readiness_ms] | med),
               handler_ms: ([.warm[].handler_ms] | med),
               total_ms: ([.warm[].total_ms] | med),
               client_ms: ([.warm[].client_ms] | med)} }
    | .saved_ms = (if (.cold.total_ms != null and .warm.total_ms != null)
                   then (.cold.total_ms - .warm.total_ms) else null end)
  ' "$ATTEMPTS" > "$COMPARISON"
  COLD_COUNT="$(jq -r '.cold_count' "$COMPARISON")"
  WARM_COUNT="$(jq -r '.warm_count' "$COMPARISON")"
}

write_summary_txt() {
  {
    echo "tachyon-serverless warm reuse measurement $STAMP"
    echo "host              $(uname -srm) ($ARCH)"
    echo "gateway config    $CONFIG_PATH (derived from $BASE_CONFIG)"
    echo "provider          $(jq -r '"\(.kind) / \(.isolation) / dev_only=\(.dev_only)"' "$EVIDENCE_DIR/provider.json" 2>/dev/null || echo unknown)"
    echo "idle capability   quiesce=$(jq -r '.capabilities.idle_quiesce.status' "$EVIDENCE_DIR/provider.json" 2>/dev/null || echo '-') resume=$(jq -r '.capabilities.idle_resume.status' "$EVIDENCE_DIR/provider.json" 2>/dev/null || echo '-')"
    echo "reuse             enabled=$REUSE_ENABLED verified=$REUSE_VERIFIED"
    echo "reuse reason      $REUSE_REASON"
    echo "function          $FUNCTION_NAME ($(state_get fn))"
    echo "revision          $(state_get rev.warm) (${WARM_MEMORY_MIB} MiB, ${WARM_CPU_MILLIS} m)"
    echo "invocations       $INVOCATIONS"
    echo "orphans           $ORPHAN_NOTE"
    echo
    echo "== per invocation =="
    if [ -s "$ATTEMPTS" ]; then
      jq -r '"  #\(.n)\t\(.start_kind)\tenv=\(.environment_id // "-")\tepoch=\(.epoch // "-")\tboot=\(.boot_ms // "-")\tresume=\(.resume_ms // "-")\treadiness=\(.readiness_ms // "-")\thandler=\(.handler_ms // "-")\ttotal=\(.total_ms // "-")\tclient=\(.client_ms)"' "$ATTEMPTS"
    else
      echo "  no invocation was recorded"
    fi
    echo
    echo "== cold vs warm (median ms) =="
    if json_file_ok "$COMPARISON"; then
      jq -r '
        "  cold  n=\(.cold_count)\tboot=\(.cold.boot_ms // "-")\tinit=\(.cold.init_ms // "-")\thandler=\(.cold.handler_ms // "-")\ttotal=\(.cold.total_ms // "-")\tclient=\(.cold.client_ms // "-")",
        "  warm  n=\(.warm_count)\tresume=\(.warm.resume_ms // "-")\treadiness=\(.warm.readiness_ms // "-")\thandler=\(.warm.handler_ms // "-")\ttotal=\(.warm.total_ms // "-")\tclient=\(.warm.client_ms // "-")",
        "  environments used: \(.environments)\ttotal_ms difference (cold - warm): \(.saved_ms // "-")"' "$COMPARISON"
    else
      echo "  no comparison"
    fi
    echo
    echo "== paused VMM on the host =="
    if json_file_ok "$EVIDENCE_DIR/paused-vmm.json"; then
      jq -r 'if .sampled then
          "  pid \(.pid) of environment \(.environment_id)",
          "  rss=\(.samples[0].vm_rss_kib // .samples[0].ps_rss_kib // "-") kiB  vsz=\(.samples[0].vm_size_kib // .samples[0].ps_vsz_kib // "-") kiB  threads=\(.samples[0].threads // "-")  state=\(.samples[0].ps_state // "-")",
          "  cpu ticks used over \(.gap_seconds)s while paused: \(.cpu_ticks_during_gap // "-")"
        else "  not sampled: \(.reason)" end' "$EVIDENCE_DIR/paused-vmm.json"
    else
      echo "  no sample"
    fi
    echo
    echo "== findings =="
    if [ -n "$FINDINGS" ]; then printf '%s' "$FINDINGS" | sed 's/^/  - /'; else echo "  none"; fi
    echo
    echo "== how to read this =="
    echo "  The numbers are host-observed (AttemptTimings). A warm attempt reports"
    echo "  resume and readiness instead of boot and init, so cold total vs warm total"
    echo "  is the comparison; boot is what warm avoids."
    if [ "$REUSE_VERIFIED" != "true" ]; then
      echo "  reuse.verified is false: this run used [pool] allow_unverified_idle to take"
      echo "  the measurement. It is NOT evidence that the configuration is a verified"
      echo "  warm setup. Promoting idle_quiesce / idle_resume to Supported is a separate,"
      echo "  reviewed change that cites this directory (docs/adr/0001)."
    fi
  } > "$SUMMARY_TXT"
}

print_table() {
  echo
  printf '%-6s %-6s %-10s %-10s %-10s %-10s %-10s\n' "START" "N" "BOOT" "RESUME" "HANDLER" "TOTAL" "CLIENT"
  printf '%-6s %-6s %-10s %-10s %-10s %-10s %-10s\n' "-----" "-----" "---------" "---------" "---------" "---------" "---------"
  if json_file_ok "$COMPARISON"; then
    jq -r '
      "cold  \t\(.cold_count)\t\(.cold.boot_ms // "-")\t-\t\(.cold.handler_ms // "-")\t\(.cold.total_ms // "-")\t\(.cold.client_ms // "-")",
      "warm  \t\(.warm_count)\t-\t\(.warm.resume_ms // "-")\t\(.warm.handler_ms // "-")\t\(.warm.total_ms // "-")\t\(.warm.client_ms // "-")"' \
      "$COMPARISON" |
      while IFS=$'\t' read -r a b c d e f g; do
        printf '%-6s %-6s %-10s %-10s %-10s %-10s %-10s\n' "$a" "$b" "$c" "$d" "$e" "$f" "$g"
      done
  fi
  echo
  echo "evidence: $EVIDENCE_DIR"
  echo "summary:  $SUMMARY_TXT"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

# The first invocation boots and pools an environment; the paused VMM is sampled
# after it, and the rest of the invocations then run against the pool.
INVOKE_FIRST_BATCH=1

main() {
  e2e_log "warm reuse measurement $STAMP (arch $ARCH, evidence $EVIDENCE_DIR)"

  step "build gateway, cli and the guest function" build_all
  step "write a gateway config with reuse on" write_config
  start_gateway
  step "gateway healthz/readyz" wait_gateway
  step "provider is firecracker and reuse is on" check_provider
  step "function $FUNCTION_NAME (idempotent)" ensure_function
  step "deploy revision (${WARM_MEMORY_MIB} MiB)" deploy_revision
  step "invocation 1 of $INVOCATIONS (cold)" run_invocations
  step "sample the paused VMM on the host" capture_paused_vmm
  step "invocations 2..$INVOCATIONS (expected warm)" run_remaining_invocations

  local gw_rc=0
  if [ -n "$GATEWAY_PID" ]; then
    stop_process "$GATEWAY_PID" 20
    gw_rc=$STOP_RC
    GATEWAY_PID=""
    e2e_log "gateway exit status $gw_rc"
  fi
  orphan_note

  compare
  if [ "$WARM_COUNT" -eq 0 ]; then
    note_finding "no invocation was served warm, so nothing about warm reuse was measured"
  fi
  write_summary_txt

  steps_write_summary "$EVIDENCE_DIR/summary.json" \
    "$(jq -n --arg run "warm-$STAMP" --arg arch "$ARCH" --arg host "$(uname -srm)" \
        --arg config "$CONFIG_PATH" --arg base_config "$BASE_CONFIG" \
        --arg function "$FUNCTION_NAME" --arg revision "$(state_get rev.warm)" \
        --arg reuse_enabled "$REUSE_ENABLED" --arg reuse_verified "$REUSE_VERIFIED" \
        --arg reuse_reason "$REUSE_REASON" --arg orphans "$ORPHAN_NOTE" \
        --arg findings "$FINDINGS" \
        --argjson invocations "$INVOCATIONS" \
        --argjson memory_mib "$WARM_MEMORY_MIB" --argjson cpu_millis "$WARM_CPU_MILLIS" \
        --argjson comparison "$(cat "$COMPARISON")" \
        --argjson paused "$(cat "$EVIDENCE_DIR/paused-vmm.json")" \
        '{run_id: $run, architecture: $arch, host: $host, gateway_config: $config,
          base_config: $base_config, function: $function, revision: $revision,
          requested: {invocations: $invocations, memory_mib: $memory_mib, cpu_millis: $cpu_millis},
          reuse: {enabled: $reuse_enabled, verified: $reuse_verified, reason: $reuse_reason,
                  measurement_only: ($reuse_verified != "true")},
          comparison: $comparison, paused_vmm: $paused, orphans: $orphans,
          findings: ($findings | split("\n") | map(select(length > 0)))}')"

  steps_print_table || true
  print_table

  local failed
  failed="$(steps_failed_count)"
  if [ "$failed" -ne 0 ]; then
    echo "INCOMPLETE: the measurement could not be taken ($failed step(s) failed)" >&2
    exit 2
  fi
  if [ "$WARM_COUNT" -eq 0 ]; then
    echo "FAIL: $COLD_COUNT invocation(s) ran and NONE of them was warm." >&2
    echo "Nothing about idle quiesce / resume was measured. Check gateway.log for" >&2
    echo "'quiescing the environment failed' or 'resuming a pooled environment failed'," >&2
    echo "and $EVIDENCE_DIR/attempts.jsonl for the start kinds." >&2
    exit 1
  fi
  echo "OK: $WARM_COUNT of $INVOCATIONS invocation(s) were warm (cold: $COLD_COUNT)"
  if [ "$REUSE_VERIFIED" != "true" ]; then
    echo "NOTE: reuse.verified = false. This is a measurement run, not a verified warm setup."
  fi
  exit 0
}

main "$@"

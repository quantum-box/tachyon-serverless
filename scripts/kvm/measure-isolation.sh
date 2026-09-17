#!/usr/bin/env bash
# scripts/kvm/measure-isolation.sh - measure ADR-0001 M8 (egress none), M9 (resource limits) and
# the PLT-4622 ephemeral storage limit (DISK) on a real Firecracker guest, through the gateway and
# the `tsls` CLI.
#
# Flow: build (host tools + the guest musl probe) -> start a gateway with
# config/gateway.firecracker.toml -> deploy examples/isolation-probe -> run the egress probe
# and assert that every target failed to connect -> print the guest interface list -> run the
# resource probe and compare the vCPU / memory the guest sees with what the revision asked for
# -> deploy a revision with a small memory limit, run the allocation probe past that limit and
# record how the platform classified the invocation -> deploy a revision with a small
# ephemeral_storage_mib, fill /tmp from the guest until the write fails while sampling the host's
# free space and the provider workdir, and check that the write stopped at the cap, that / and
# /function refused writes and that the host lost no more than the environment's budget -> stop
# the gateway -> PASS/FAIL table.
#
# Usage:
#   scripts/kvm/measure-isolation.sh            # Linux/KVM, see docs/kvm.md section 3.6
#
# Environment (all optional):
#   TSLS_SKIP_BUILD=1     do not run cargo build
#   TSLS_GATEWAY_CONFIG   gateway config (default config/gateway.firecracker.toml)
#   TSLS_API_URL          URL that config listens on (default http://127.0.0.1:8080)
#   TSLS_TOKEN            tenant token of that config (default dev-token-tenant-a)
#   EVIDENCE_ROOT         where to save results (default docs/evidence)
#   PROBE_FUNCTION        function name (default isolation-probe)
#   PROBE_MEMORY_MIB / PROBE_CPU_MILLIS   baseline revision resources (default 256 / 500)
#   PROBE_TIMEOUT_SECONDS handler timeout of the probe revisions (default 60)
#   ALLOC_MEMORY_MIB      memory of the allocation revision (default 128)
#   ALLOC_MIB             MiB the guest tries to allocate (default 4 x ALLOC_MEMORY_MIB)
#   MEM_TOLERANCE_PCT     lowest MemTotal accepted, in percent of the requested memory
#                         (default 70; a guest kernel always reserves some of it)
#   CONNECT_TIMEOUT_MS / DNS_TIMEOUT_MS   per-target probe timeouts (default 2000 / 5000)
#   DISK_STORAGE_MIB      ephemeral_storage_mib of the disk revision (default 64)
#   DISK_FILL_MIB         MiB the guest tries to write (default 4 x DISK_STORAGE_MIB)
#   DISK_TOLERANCE_PCT    lowest accepted write, in percent of DISK_STORAGE_MIB (default 80;
#                         ext4 metadata takes a few percent of a small file system)
#   DISK_HOST_SLACK_MIB   host free space the run may lose beyond the environment's scratch drive
#                         (default 64: function drive, capped logs, gateway data)
#
# Output: docs/evidence/isolation-<UTC>/{egress.json,resources.json,alloc-invoke.json,
#   alloc-invocation.json,alloc-logs.txt,revision-baseline.json,revision-alloc.json,
#   disk.json,disk-invocation.json,disk-logs.txt,disk-host-samples.txt,disk-host.json,
#   revision-disk.json,provider.json,gateway.log,steps/,summary.json,summary.txt} plus a
#   PASS/FAIL table for M8, M9 and DISK on stdout.
#
# Exit codes:
#   0  the measurement ran, M8 and DISK passed (M9 findings are reported, not fatal)
#   1  a probe reached the network: the security-relevant failure (M8 FAIL)
#   2  the measurement could not be taken (build, gateway, deploy or probe failure)
#   3  the guest wrote past its ephemeral storage cap, wrote to a read-only drive, or the host
#      lost more free space than the environment's budget (DISK FAIL)
#
# Idempotent: the function is reused when it already exists, every run writes a new evidence
# directory, and the gateway started here is always stopped again. This script never changes
# the provider's Capabilities; flipping `egress_none` is a separate, reviewed decision.
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
CONFIG_PATH="${TSLS_GATEWAY_CONFIG:-$REPO_ROOT/config/gateway.firecracker.toml}"
API_URL="${TSLS_API_URL:-http://127.0.0.1:8080}"
TOKEN="${TSLS_TOKEN:-dev-token-tenant-a}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$REPO_ROOT/docs/evidence}"
FUNCTION_NAME="${PROBE_FUNCTION:-isolation-probe}"
PROBE_MEMORY_MIB="${PROBE_MEMORY_MIB:-256}"
PROBE_CPU_MILLIS="${PROBE_CPU_MILLIS:-500}"
PROBE_TIMEOUT_SECONDS="${PROBE_TIMEOUT_SECONDS:-60}"
ALLOC_MEMORY_MIB="${ALLOC_MEMORY_MIB:-128}"
ALLOC_MIB="${ALLOC_MIB:-$(( ALLOC_MEMORY_MIB * 4 ))}"
MEM_TOLERANCE_PCT="${MEM_TOLERANCE_PCT:-70}"
CONNECT_TIMEOUT_MS="${CONNECT_TIMEOUT_MS:-2000}"
DNS_TIMEOUT_MS="${DNS_TIMEOUT_MS:-5000}"
DISK_STORAGE_MIB="${DISK_STORAGE_MIB:-64}"
DISK_FILL_MIB="${DISK_FILL_MIB:-$(( DISK_STORAGE_MIB * 4 ))}"
DISK_TOLERANCE_PCT="${DISK_TOLERANCE_PCT:-80}"
DISK_HOST_SLACK_MIB="${DISK_HOST_SLACK_MIB:-64}"

[ -f "$CONFIG_PATH" ] || e2e_die "gateway config not found: $CONFIG_PATH"

HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
  x86_64|amd64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *) e2e_die "unsupported host architecture $HOST_ARCH" ;;
esac

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$EVIDENCE_ROOT/isolation-$STAMP"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-isolation.XXXXXX")"
export E2E_STATE_DIR="$WORK_DIR/state"
export STEP_LOG_DIR="$EVIDENCE_DIR/steps"
mkdir -p "$EVIDENCE_DIR" "$STEP_LOG_DIR" "$E2E_STATE_DIR"
GATEWAY_LOG="$EVIDENCE_DIR/gateway.log"
SUMMARY_TXT="$EVIDENCE_DIR/summary.txt"

TSLS_BIN="${TSLS_BIN:-$REPO_ROOT/target/debug/tsls}"
GATEWAY_BIN="${TSLS_GATEWAY_BIN:-$REPO_ROOT/target/debug/tachyon-serverless-gateway}"
GATEWAY_CONFIG_FLAG="${TSLS_GATEWAY_CONFIG_FLAG:---config}"
GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/$ARCH-unknown-linux-musl/release}"
PROBE_BIN="$GUEST_DIR/example-isolation-probe"
# The provider's run directory, used for the orphan note after the gateway stops.
FC_RUN_DIR="$(sed -n 's/^workdir *= *"\(.*\)"$/\1/p' "$CONFIG_PATH" | head -n1)"
FC_RUN_DIR="${FC_RUN_DIR:-.kvm/run}"
case "$FC_RUN_DIR" in /*) ;; *) FC_RUN_DIR="$REPO_ROOT/${FC_RUN_DIR#./}" ;; esac

tsls() { "$TSLS_BIN" "$@"; }
export TSLS_BIN

GATEWAY_PID=""
M8_STATUS="UNKNOWN"
M8_DETAIL="the egress probe did not produce a report"
M9_STATUS="UNKNOWN"
M9_DETAIL="the resource probe did not produce a report"
DISK_STATUS="UNKNOWN"
DISK_DETAIL="the disk probe did not produce a report"
ORPHAN_NOTE="not checked"
FINDINGS=""

cleanup() {
  local rc=$?
  set +e
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    e2e_log "cleanup: stopping gateway $GATEWAY_PID"
    stop_process "$GATEWAY_PID" 10
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
# 1. build and gateway
# ---------------------------------------------------------------------------

build_all() {
  if [ "$TSLS_SKIP_BUILD" = "1" ]; then
    e2e_log "TSLS_SKIP_BUILD=1: skipping cargo build"
  else
    cargo build -p tachyon-serverless-gateway -p tachyon-serverless-cli
    cargo build --release --target "$ARCH-unknown-linux-musl" \
      -p tachyon-serverless-runtime-bridge -p example-isolation-probe
  fi
  local b
  for b in "$TSLS_BIN" "$GATEWAY_BIN" "$PROBE_BIN"; do
    [ -x "$b" ] || { echo "missing binary: $b (run scripts/kvm/bootstrap.sh first)" >&2; return 1; }
  done
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
  # readyz stays 503 while preflight fails (missing /dev/kvm, kernel or rootfs).
  wait_for_http "$API_URL/readyz" 60 || {
    echo "gateway is not ready: run scripts/kvm/preflight.sh and scripts/kvm/bootstrap.sh" >&2
    tail -n 30 "$GATEWAY_LOG" >&2
    return 1
  }
  tsls health
}

check_provider() {
  tsls provider --json > "$EVIDENCE_DIR/provider.json"
  local provider
  provider="$(cat "$EVIDENCE_DIR/provider.json")"
  assert_json "$provider" '.kind' "firecracker" || {
    echo "this measurement is only meaningful on the firecracker provider" >&2
    return 1
  }
  assert_json "$provider" '.isolation' "micro_vm"
  assert_json "$provider" '.dev_only' "false"
}

# ---------------------------------------------------------------------------
# 2. function and revisions
# ---------------------------------------------------------------------------

ensure_function() {
  local id
  run_capture tsls functions get "$FUNCTION_NAME" --json
  if [ "$RUN_RC" -eq 0 ]; then
    id="$(printf '%s' "$RUN_OUT" | jq -r .id)"
    e2e_log "function $FUNCTION_NAME exists: $id"
  elif [ "$RUN_RC" -eq 2 ]; then
    id="$(tsls functions create --name "$FUNCTION_NAME" \
      --description "ADR-0001 M8/M9 isolation probe" --json | jq -r .id)"
    e2e_log "function $FUNCTION_NAME created: $id"
  else
    echo "unexpected exit $RUN_RC from functions get $FUNCTION_NAME: $RUN_ERR" >&2
    return 1
  fi
  state_set "fn" "$id"
}

# deploy_revision KEY MEMORY_MIB DESCRIPTION [EXTRA ARGS...]
deploy_revision() {
  local key="$1" memory="$2" description="$3" rev
  shift 3
  rev="$(tsls functions deploy --function "$FUNCTION_NAME" --binary "$PROBE_BIN" --arch "$ARCH" \
    --memory-mib "$memory" --cpu-millis "$PROBE_CPU_MILLIS" \
    --timeout-seconds "$PROBE_TIMEOUT_SECONDS" --description "$description" --json "$@" | jq -r .id)"
  if [ -z "$rev" ] || [ "$rev" = "null" ]; then
    echo "deploy did not return a revision id" >&2
    return 1
  fi
  state_set "rev.$key" "$rev"
  tsls functions revision "$FUNCTION_NAME" "$rev" --json > "$EVIDENCE_DIR/revision-$key.json"
  e2e_log "revision $key: $rev (${memory} MiB, ${PROBE_CPU_MILLIS} m)"
}

deploy_baseline() {
  deploy_revision baseline "$PROBE_MEMORY_MIB" "isolation probe baseline"
}

deploy_disk_revision() {
  deploy_revision disk "$PROBE_MEMORY_MIB" \
    "isolation probe disk fill ${DISK_FILL_MIB} MiB into ${DISK_STORAGE_MIB} MiB of ephemeral storage" \
    --ephemeral-storage-mib "$DISK_STORAGE_MIB" --no-publish
}

deploy_alloc_revision() {
  deploy_revision alloc "$ALLOC_MEMORY_MIB" \
    "isolation probe alloc ${ALLOC_MIB} MiB in a ${ALLOC_MEMORY_MIB} MiB environment" --no-publish
}

# ---------------------------------------------------------------------------
# 3. probes
# ---------------------------------------------------------------------------

run_egress_probe() {
  local payload
  payload="$(jq -nc --argjson connect "$CONNECT_TIMEOUT_MS" --argjson dns "$DNS_TIMEOUT_MS" \
    '{probe: "egress", connect_timeout_ms: $connect, dns_timeout_ms: $dns}')"
  invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.baseline)"
  printf '%s\n' "$INVOKE_ERR" >&2
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/egress.json"
  assert_eq 0 "$INVOKE_RC" "egress probe exit code" || return 1
  jq -e '.egress.targets | length > 0' "$EVIDENCE_DIR/egress.json" >/dev/null || {
    echo "the egress probe attempted no target" >&2
    return 1
  }
  jq -r '.egress.targets[] | "  \(.target) connected=\(.connected) error=\(.error_kind // "-") \(.elapsed_ms) ms"' \
    "$EVIDENCE_DIR/egress.json"
}

run_resource_probe() {
  invoke_capture "$FUNCTION_NAME" '{"probe":"resources"}' --revision-id "$(state_get rev.baseline)"
  printf '%s\n' "$INVOKE_ERR" >&2
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/resources.json"
  assert_eq 0 "$INVOKE_RC" "resource probe exit code" || return 1
  jq -e '.resources.mem_total_mib != null' "$EVIDENCE_DIR/resources.json" >/dev/null || {
    echo "the guest did not report MemTotal (is /proc mounted?)" >&2
    return 1
  }
}

# Drive the allocation past the revision's memory and record the classification.
# The invocation is expected to fail; only a missing record fails this step.
run_alloc_probe() {
  local payload id
  payload="$(jq -nc --argjson mib "$ALLOC_MIB" '{probe: "resources", alloc_mib: $mib}')"
  invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.alloc)"
  printf '%s\n' "$INVOKE_ERR" > "$EVIDENCE_DIR/alloc-invoke.stderr.txt"
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/alloc-invoke.json"
  state_set alloc.exit "$INVOKE_RC"
  id="$INVOKE_ID"
  if [ -z "$id" ]; then
    echo "no invocation id for the allocation probe (CLI exit $INVOKE_RC)" >&2
    cat "$EVIDENCE_DIR/alloc-invoke.stderr.txt" >&2
    return 1
  fi
  state_set alloc.invocation "$id"
  tsls functions invocation "$id" --json > "$EVIDENCE_DIR/alloc-invocation.json"
  tsls functions logs --invocation "$id" > "$EVIDENCE_DIR/alloc-logs.txt" 2>/dev/null || true
  e2e_log "allocation probe invocation $id (CLI exit $INVOKE_RC)"
  jq -r '"status=\(.status) class=\(.error.class // "-") type=\(.error.error_type // "-")"' \
    "$EVIDENCE_DIR/alloc-invocation.json"
  grep -c 'alloc touched' "$EVIDENCE_DIR/alloc-logs.txt" >/dev/null 2>&1 &&
    tail -n 3 "$EVIDENCE_DIR/alloc-logs.txt"
  return 0
}

# Bytes available on the file system holding the provider workdir, and bytes allocated under it.
host_avail_bytes() { df -B1 --output=avail "$FC_RUN_DIR" | tail -n 1 | tr -d ' '; }
run_dir_bytes() { du -sB1 "$FC_RUN_DIR" 2>/dev/null | cut -f1; }

# Fill /tmp from the guest while a sampler records the host side every 200 ms.
run_disk_probe() {
  local payload id sampler before_avail samples="$EVIDENCE_DIR/disk-host-samples.txt"
  mkdir -p "$FC_RUN_DIR"
  before_avail="$(host_avail_bytes)"
  : > "$samples"
  (
    while :; do
      printf '%s %s %s\n' "$(now_ms)" "$(host_avail_bytes)" "$(run_dir_bytes)" >> "$samples"
      sleep 0.2
    done
  ) &
  sampler=$!
  payload="$(jq -nc --argjson mib "$DISK_FILL_MIB" '{probe: "disk", fill_mib: $mib}')"
  invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.disk)"
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  printf '%s\n' "$INVOKE_ERR" >&2
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/disk.json"
  id="$INVOKE_ID"
  if [ -n "$id" ]; then
    tsls functions invocation "$id" --json > "$EVIDENCE_DIR/disk-invocation.json" || true
    tsls functions logs --invocation "$id" > "$EVIDENCE_DIR/disk-logs.txt" 2>/dev/null || true
  fi
  # Once the environment is gone its scratch drive must be gone too.
  sleep 1
  jq -n --argjson before "$before_avail" --argjson after "$(host_avail_bytes)" \
    --argjson min_avail "$(awk 'NR==1||$2<m{m=$2} END{print m+0}' "$samples")" \
    --argjson max_run_dir "$(awk '$3>m{m=$3} END{print m+0}' "$samples")" \
    --argjson samples "$(wc -l < "$samples" | tr -d ' ')" \
    --arg run_dir "$FC_RUN_DIR" \
    '{run_dir: $run_dir, samples: $samples, host_avail_before_bytes: $before,
      host_avail_min_bytes: $min_avail, host_avail_after_bytes: $after,
      host_avail_max_drop_bytes: ($before - $min_avail), run_dir_max_bytes: $max_run_dir}' \
    > "$EVIDENCE_DIR/disk-host.json"
  assert_eq 0 "$INVOKE_RC" "disk probe exit code" || return 1
  jq -e '.disk.fill.written_bytes != null' "$EVIDENCE_DIR/disk.json" >/dev/null || {
    echo "the disk probe did not report how much it wrote" >&2
    return 1
  }
  jq -r '.disk | "wrote \(.fill.written_mib) MiB into \(.dir) (\(.mount.device // "-") \(.mount.fs_type // "-")), stopped_by=\(.fill.stopped_by) \(.fill.error_kind // "")"' \
    "$EVIDENCE_DIR/disk.json"
  jq -r '"host avail drop max \(.host_avail_max_drop_bytes) B, run dir max \(.run_dir_max_bytes) B over \(.samples) samples"' \
    "$EVIDENCE_DIR/disk-host.json"
}

# ---------------------------------------------------------------------------
# 4. verdicts
# ---------------------------------------------------------------------------

json_file_ok() { [ -s "$1" ] && jq -e . "$1" >/dev/null 2>&1; }

evaluate_m8() {
  local file="$EVIDENCE_DIR/egress.json" reached connected attempted dns
  if ! json_file_ok "$file"; then
    M8_STATUS="UNKNOWN"
    M8_DETAIL="no egress report (see steps/ and gateway.log)"
    return 0
  fi
  reached="$(jq -r '.egress.reached_network' "$file")"
  attempted="$(jq -r '[.egress.targets[] | select(.attempted)] | length' "$file")"
  connected="$(jq -r '[.egress.targets[] | select(.connected) | .target] | join(", ")' "$file")"
  dns="$(jq -r '.egress.dns_resolved' "$file")"
  if [ "$reached" = "false" ] && [ "$attempted" -gt 0 ]; then
    M8_STATUS="PASS"
    M8_DETAIL="$attempted/$(jq -r '.egress.targets | length' "$file") targets attempted, none connected, dns_resolved=$dns"
  elif [ "$reached" = "true" ]; then
    M8_STATUS="FAIL"
    M8_DETAIL="the guest reached the network (connected: ${connected:-none}, dns_resolved=$dns)"
    note_finding "M8: $M8_DETAIL"
  else
    M8_STATUS="UNKNOWN"
    M8_DETAIL="no target was attempted (reached_network=$reached)"
  fi
  local loopback_only
  loopback_only="$(jq -r '.interfaces.loopback_only' "$file")"
  if [ "$loopback_only" != "true" ]; then
    note_finding "M8: the guest lists interfaces other than loopback: $(jq -r '.interfaces.non_loopback | join(", ")' "$file")"
  fi
  if [ "$(jq -r '.interfaces.routes.default_routes' "$file")" != "0" ]; then
    note_finding "M8: the guest has a default route"
  fi
}

evaluate_m9() {
  local resources="$EVIDENCE_DIR/resources.json" revision="$EVIDENCE_DIR/revision-baseline.json"
  local alloc="$EVIDENCE_DIR/alloc-invocation.json"
  local requested_mem requested_cpu expected_vcpus seen_vcpus seen_cpuinfo seen_mem floor
  local status class error_type ok=1 detail=""
  if ! json_file_ok "$resources" || ! json_file_ok "$revision"; then
    M9_STATUS="UNKNOWN"
    M9_DETAIL="no resource report (see steps/ and gateway.log)"
    return 0
  fi
  requested_mem="$(jq -r '.spec.resources.memory_mib // empty' "$revision")"
  requested_cpu="$(jq -r '.spec.resources.cpu_millis // empty' "$revision")"
  case "$requested_mem/$requested_cpu" in
    */|/*|*[!0-9/]*)
      M9_STATUS="UNKNOWN"
      M9_DETAIL="the revision record carries no resources (see revision-baseline.json)"
      return 0
      ;;
  esac
  expected_vcpus=$(( (requested_cpu + 999) / 1000 ))
  [ "$expected_vcpus" -ge 1 ] || expected_vcpus=1
  seen_vcpus="$(jq -r '.resources.available_parallelism // "null"' "$resources")"
  seen_cpuinfo="$(jq -r '.resources.cpuinfo_processors // "null"' "$resources")"
  seen_mem="$(jq -r '.resources.mem_total_mib // "null"' "$resources")"
  floor=$(( requested_mem * MEM_TOLERANCE_PCT / 100 ))
  detail="vcpu ${seen_vcpus}/${seen_cpuinfo} vs ${expected_vcpus} requested; MemTotal ${seen_mem} MiB vs ${requested_mem} MiB requested"

  if [ "$seen_vcpus" != "$expected_vcpus" ] || [ "$seen_cpuinfo" != "$expected_vcpus" ]; then
    ok=0
    note_finding "M9: the guest sees ${seen_vcpus} vCPU (cpuinfo ${seen_cpuinfo}), the revision asked for ${expected_vcpus} (${requested_cpu} m)"
  fi
  if [ "$seen_mem" = "null" ]; then
    ok=0
    note_finding "M9: the guest did not report MemTotal"
  elif [ "$seen_mem" -gt "$requested_mem" ] || [ "$seen_mem" -lt "$floor" ]; then
    ok=0
    note_finding "M9: the guest sees ${seen_mem} MiB, outside ${floor}..${requested_mem} MiB (MEM_TOLERANCE_PCT=$MEM_TOLERANCE_PCT)"
  fi

  if json_file_ok "$alloc"; then
    status="$(jq -r '.status' "$alloc")"
    class="$(jq -r '.error.class // "-"' "$alloc")"
    error_type="$(jq -r '.error.error_type // "-"' "$alloc")"
    detail="$detail; alloc ${ALLOC_MIB} MiB in ${ALLOC_MEMORY_MIB} MiB -> ${status}/${class}/${error_type}"
    if [ "$status" = "succeeded" ]; then
      ok=0
      note_finding "M9: allocating ${ALLOC_MIB} MiB succeeded in a ${ALLOC_MEMORY_MIB} MiB environment (the limit did not bind)"
    elif [ "$class" != "crash" ]; then
      ok=0
      note_finding "M9: the over-allocation was classified as ${class} (${error_type}); ADR-0001 M9 expects crash"
    fi
  else
    ok=0
    detail="$detail; no allocation record"
    note_finding "M9: the allocation probe produced no invocation record"
  fi

  if [ "$ok" -eq 1 ]; then M9_STATUS="PASS"; else M9_STATUS="FAIL"; fi
  M9_DETAIL="$detail"
}

evaluate_disk() {
  local file="$EVIDENCE_DIR/disk.json" host="$EVIDENCE_DIR/disk-host.json"
  local written stopped device fstype cap_bytes floor_bytes drop allowed ro_ok ok=1 detail
  if ! json_file_ok "$file" || ! jq -e '.disk' "$file" >/dev/null 2>&1; then
    DISK_STATUS="UNKNOWN"
    DISK_DETAIL="no disk report (see steps/ and gateway.log)"
    return 0
  fi
  written="$(jq -r '.disk.fill.written_bytes' "$file")"
  stopped="$(jq -r '.disk.fill.stopped_by' "$file")"
  device="$(jq -r '.disk.mount.device // "-"' "$file")"
  fstype="$(jq -r '.disk.mount.fs_type // "-"' "$file")"
  cap_bytes=$(( DISK_STORAGE_MIB * 1024 * 1024 ))
  floor_bytes=$(( cap_bytes * DISK_TOLERANCE_PCT / 100 ))
  detail="wrote $(( written / 1024 / 1024 )) MiB of ${DISK_FILL_MIB} MiB into ${DISK_STORAGE_MIB} MiB (${device} ${fstype}), stopped_by=${stopped}"

  if [ "$written" -gt "$cap_bytes" ]; then
    ok=0
    note_finding "DISK: the guest wrote ${written} bytes, more than the ${cap_bytes} byte cap"
  fi
  if [ "$stopped" != "enospc" ]; then
    ok=0
    note_finding "DISK: the fill stopped by ${stopped}, expected enospc at the cap"
  elif [ "$written" -lt "$floor_bytes" ]; then
    ok=0
    note_finding "DISK: ENOSPC after ${written} bytes, below ${DISK_TOLERANCE_PCT}% of the cap"
  fi
  ro_ok="$(jq -r '[.disk.read_only_checks[] | select(.refused and .read_only_fs)] | length' "$file")"
  if [ "$ro_ok" != "$(jq -r '.disk.read_only_checks | length' "$file")" ]; then
    ok=0
    note_finding "DISK: a read-only path accepted a write or failed with something other than EROFS: $(jq -c '.disk.read_only_checks' "$file")"
  fi
  detail="$detail; read-only refused ${ro_ok}/$(jq -r '.disk.read_only_checks | length' "$file")"
  if json_file_ok "$host"; then
    drop="$(jq -r '.host_avail_max_drop_bytes' "$host")"
    allowed=$(( cap_bytes + DISK_HOST_SLACK_MIB * 1024 * 1024 ))
    detail="$detail; host avail drop max $(( drop / 1024 / 1024 )) MiB (allowed $(( allowed / 1024 / 1024 )) MiB)"
    if [ "$drop" -gt "$allowed" ]; then
      ok=0
      note_finding "DISK: the host lost ${drop} bytes of free space during the fill, more than ${allowed}"
    fi
  else
    ok=0
    note_finding "DISK: no host-side samples"
  fi
  if [ "$ok" -eq 1 ]; then DISK_STATUS="PASS"; else DISK_STATUS="FAIL"; fi
  DISK_DETAIL="$detail"
}

orphan_note() {
  local out
  if [ ! -x "$REPO_ROOT/scripts/e2e/orphan-check.sh" ] || ! command -v pgrep >/dev/null 2>&1; then
    ORPHAN_NOTE="skipped (orphan-check.sh or pgrep unavailable)"
    return 0
  fi
  set +e
  out="$("$REPO_ROOT/scripts/e2e/orphan-check.sh" firecracker "$FC_RUN_DIR" 2>&1)"
  local rc=$?
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
# 5. reporting
# ---------------------------------------------------------------------------

write_summary_txt() {
  {
    echo "tachyon-serverless isolation measurement $STAMP"
    echo "host              $(uname -srm) ($ARCH)"
    echo "gateway config    $CONFIG_PATH"
    echo "provider          $(jq -r '"\(.kind) / \(.isolation) / dev_only=\(.dev_only)"' "$EVIDENCE_DIR/provider.json" 2>/dev/null || echo unknown)"
    echo "function          $FUNCTION_NAME ($(state_get fn))"
    echo "baseline revision $(state_get rev.baseline) (${PROBE_MEMORY_MIB} MiB, ${PROBE_CPU_MILLIS} m)"
    echo "alloc revision    $(state_get rev.alloc) (${ALLOC_MEMORY_MIB} MiB, alloc ${ALLOC_MIB} MiB)"
    echo "orphans           $ORPHAN_NOTE"
    echo
    echo "== M8 egress (expected: every target fails) =="
    if json_file_ok "$EVIDENCE_DIR/egress.json"; then
      jq -r '.egress.targets[] | "  \(.target)\tconnected=\(.connected)\terror=\(.error_kind // "-")\t\(.elapsed_ms) ms"' \
        "$EVIDENCE_DIR/egress.json"
      jq -r '"  dns \(.egress.dns.name)\tresolved=\(.egress.dns.resolved)\terror=\(.egress.dns.error_kind // "-")\t\(.egress.dns.elapsed_ms) ms"' \
        "$EVIDENCE_DIR/egress.json"
      jq -r '"  interfaces: \(.interfaces.names | join(", ")) (loopback_only=\(.interfaces.loopback_only), default routes \(.interfaces.routes.default_routes))"' \
        "$EVIDENCE_DIR/egress.json"
      jq -r '"  guest boot_id: \(.guest.boot_id // "-")"' "$EVIDENCE_DIR/egress.json"
    else
      echo "  no egress report"
    fi
    echo
    echo "== M9 resources (expected: the guest sees what the revision asked for) =="
    if json_file_ok "$EVIDENCE_DIR/resources.json"; then
      jq -r '"  vcpus: available_parallelism=\(.resources.available_parallelism // "-") cpuinfo=\(.resources.cpuinfo_processors // "-")"' \
        "$EVIDENCE_DIR/resources.json"
      jq -r '"  memory: MemTotal=\(.resources.mem_total_mib // "-") MiB (\(.resources.mem_total_kib // "-") kB)"' \
        "$EVIDENCE_DIR/resources.json"
      jq -r '"  cgroup: available=\(.resources.cgroup.available) memory.max=\(.resources.cgroup.memory_max // "-") cpu.max=\(.resources.cgroup.cpu_max // "-")"' \
        "$EVIDENCE_DIR/resources.json"
    else
      echo "  no resource report"
    fi
    if json_file_ok "$EVIDENCE_DIR/alloc-invocation.json"; then
      jq -r '"  alloc invocation \(.id): status=\(.status) class=\(.error.class // "-") type=\(.error.error_type // "-")"' \
        "$EVIDENCE_DIR/alloc-invocation.json"
    fi
    if [ -s "$EVIDENCE_DIR/alloc-logs.txt" ]; then
      echo "  last guest progress lines:"
      grep 'alloc touched' "$EVIDENCE_DIR/alloc-logs.txt" | tail -n 2 | sed 's/^/    /' || true
    fi
    echo
    echo "== DISK ephemeral storage (expected: ENOSPC at ${DISK_STORAGE_MIB} MiB, host unaffected) =="
    if json_file_ok "$EVIDENCE_DIR/disk.json" && jq -e .disk "$EVIDENCE_DIR/disk.json" >/dev/null 2>&1; then
      jq -r '.disk | "  fill: \(.path) wrote \(.fill.written_bytes) B (\(.fill.written_mib) MiB) stopped_by=\(.fill.stopped_by) error=\(.fill.error // "-")"' \
        "$EVIDENCE_DIR/disk.json"
      jq -r '.disk | "  mount: \(.mount.device // "-") on \(.mount.mount_point // "-") \(.mount.fs_type // "-") \(.mount.options // "-")"' \
        "$EVIDENCE_DIR/disk.json"
      jq -r '.disk | "  statvfs before: total \(.fs_before.total_bytes // "-") B avail \(.fs_before.avail_bytes // "-") B; after fill avail \(.fs_after_fill.avail_bytes // "-") B; after cleanup avail \(.fs_after_cleanup.avail_bytes // "-") B"' \
        "$EVIDENCE_DIR/disk.json"
      jq -r '.disk.read_only_checks[] | "  write into \(.dir): refused=\(.refused) \(.error // "")"' \
        "$EVIDENCE_DIR/disk.json"
    else
      echo "  no disk report"
    fi
    if json_file_ok "$EVIDENCE_DIR/disk-host.json"; then
      jq -r '"  host: avail before \(.host_avail_before_bytes) B, min \(.host_avail_min_bytes) B, after \(.host_avail_after_bytes) B; run dir max \(.run_dir_max_bytes) B (\(.samples) samples)"' \
        "$EVIDENCE_DIR/disk-host.json"
    fi
    echo
    echo "== findings =="
    if [ -n "$FINDINGS" ]; then printf '%s' "$FINDINGS" | sed 's/^/  - /'; else echo "  none"; fi
    echo
    echo "== verdict =="
    printf '  %-4s %-8s %s\n' "M8" "$M8_STATUS" "$M8_DETAIL"
    printf '  %-4s %-8s %s\n' "M9" "$M9_STATUS" "$M9_DETAIL"
    printf '  %-4s %-8s %s\n' "DISK" "$DISK_STATUS" "$DISK_DETAIL"
  } > "$SUMMARY_TXT"
}

print_table() {
  echo
  printf '%-4s %-8s %s\n' "CHK" "RESULT" "DETAIL"
  printf '%-4s %-8s %s\n' "---" "------" "----------------------------------------"
  printf '%-4s %-8s %s\n' "M8" "$M8_STATUS" "$M8_DETAIL"
  printf '%-4s %-8s %s\n' "M9" "$M9_STATUS" "$M9_DETAIL"
  printf '%-4s %-8s %s\n' "DISK" "$DISK_STATUS" "$DISK_DETAIL"
  echo
  echo "evidence: $EVIDENCE_DIR"
  echo "summary:  $SUMMARY_TXT"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

main() {
  e2e_log "isolation measurement $STAMP (arch $ARCH, evidence $EVIDENCE_DIR)"

  step "build gateway, cli and the guest probe" build_all
  start_gateway
  step "gateway healthz/readyz" wait_gateway
  step "provider is firecracker / micro_vm" check_provider
  step "function $FUNCTION_NAME (idempotent)" ensure_function
  step "deploy baseline revision (${PROBE_MEMORY_MIB} MiB)" deploy_baseline
  step "M8: egress probe" run_egress_probe
  step "M9: resource probe" run_resource_probe
  step "deploy allocation revision (${ALLOC_MEMORY_MIB} MiB)" deploy_alloc_revision
  step "M9: allocate ${ALLOC_MIB} MiB past the limit" run_alloc_probe
  step "deploy disk revision (${DISK_STORAGE_MIB} MiB ephemeral storage)" deploy_disk_revision
  step "DISK: fill /tmp with ${DISK_FILL_MIB} MiB" run_disk_probe

  local gw_rc=0
  if [ -n "$GATEWAY_PID" ]; then
    stop_process "$GATEWAY_PID" 15
    gw_rc=$STOP_RC
    GATEWAY_PID=""
    e2e_log "gateway exit status $gw_rc"
  fi
  orphan_note

  evaluate_m8
  evaluate_m9
  evaluate_disk
  write_summary_txt

  steps_write_summary "$EVIDENCE_DIR/summary.json" \
    "$(jq -n --arg run "isolation-$STAMP" --arg arch "$ARCH" --arg host "$(uname -srm)" \
        --arg config "$CONFIG_PATH" --arg function "$FUNCTION_NAME" \
        --arg baseline "$(state_get rev.baseline)" --arg alloc_rev "$(state_get rev.alloc)" \
        --arg m8 "$M8_STATUS" --arg m8_detail "$M8_DETAIL" \
        --arg m9 "$M9_STATUS" --arg m9_detail "$M9_DETAIL" \
        --arg disk "$DISK_STATUS" --arg disk_detail "$DISK_DETAIL" \
        --arg disk_rev "$(state_get rev.disk)" \
        --argjson disk_storage_mib "$DISK_STORAGE_MIB" --argjson disk_fill_mib "$DISK_FILL_MIB" \
        --arg orphans "$ORPHAN_NOTE" --arg findings "$FINDINGS" \
        --argjson memory_mib "$PROBE_MEMORY_MIB" --argjson cpu_millis "$PROBE_CPU_MILLIS" \
        --argjson alloc_memory_mib "$ALLOC_MEMORY_MIB" --argjson alloc_mib "$ALLOC_MIB" \
        '{run_id: $run, architecture: $arch, host: $host, gateway_config: $config,
          function: $function, baseline_revision: $baseline, alloc_revision: $alloc_rev,
          disk_revision: $disk_rev,
          requested: {memory_mib: $memory_mib, cpu_millis: $cpu_millis,
                      alloc_memory_mib: $alloc_memory_mib, alloc_mib: $alloc_mib,
                      disk_storage_mib: $disk_storage_mib, disk_fill_mib: $disk_fill_mib},
          measurements: {M8: {status: $m8, detail: $m8_detail},
                         M9: {status: $m9, detail: $m9_detail},
                         DISK: {status: $disk, detail: $disk_detail}},
          orphans: $orphans,
          findings: ($findings | split("\n") | map(select(length > 0)))}')"

  steps_print_table || true
  print_table

  local failed
  failed="$(steps_failed_count)"
  if [ "$M8_STATUS" = "FAIL" ]; then
    echo "FAIL: a probe reached the network from the guest" >&2
    exit 1
  fi
  if [ "$DISK_STATUS" = "FAIL" ]; then
    echo "FAIL: the ephemeral storage limit did not hold (see findings)" >&2
    exit 3
  fi
  if [ "$failed" -ne 0 ] || [ "$M8_STATUS" = "UNKNOWN" ] || [ "$DISK_STATUS" = "UNKNOWN" ]; then
    echo "INCOMPLETE: the measurement could not be taken ($failed step(s) failed)" >&2
    exit 2
  fi
  echo "M8 PASS, DISK PASS (M9 $M9_STATUS; M9 findings are reported, not fatal)"
  exit 0
}

main "$@"

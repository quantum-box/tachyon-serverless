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
#   NET_MEASURE           1 (default) also measures the egress profiles `restricted` and
#                         `public-web` (PLT-4622, NET): needs passwordless sudo (or root), nftables
#                         and iproute2. The gateway then runs as root, net.ipv4.ip_forward is set
#                         to 1 for the run and restored afterwards. 0 keeps the old unprivileged run.
#   NET_TOKEN_B           token of the second tenant for the cross-tenant check
#                         (default dev-token-tenant-b)
#   NET_LISTEN_MS         how long tenant B's guest listens for tenant A (default 25000)
#   NET_REDIRECT_URL      an http:// URL that answers 3xx towards the metadata address (default
#                         httpbin.org redirect-to; unreachable = inconclusive, not a failure)
#   HOST_MEASURE          1 (default) samples every VMM from the host while the run lasts
#                         (scripts/kvm/host-watch.sh, as root) and checks HOST: each VMM ran in
#                         its own cgroup with cpu.max / memory.max set, and, when the config enables
#                         the jailer, as JAILER_UID without capabilities, chrooted, in new PID and
#                         mount namespaces with seccomp on its vCPU / API threads; afterwards no
#                         cgroup, jail or VMM process is left. Needs passwordless sudo.
#   CGROUP_PARENT         cgroup directory of the environments (default /sys/fs/cgroup/tachyon)
#   JAIL_CHROOT_BASE      chroot base of the jailer (default /srv/jailer)
#   JAILER_UID            uid the jailed VMMs must run as (default 64000)
#   NOISY_MEASURE         1 (default) runs NOISY (PLT-4622 noisy neighbour, needs HOST_MEASURE=1):
#                         tenant B (NOISY_B_CPU_MILLIS, default 1000) runs a fixed workload
#                         (NOISY_B_ITERATIONS mixer iterations, NOISY_B_FILES x NOISY_B_FILE_KIB fsynced
#                         files) NOISY_RUNS times alone, then NOISY_RUNS times while tenant A
#                         (NOISY_A_CPU_MILLIS, default 500, memory ALLOC_MEMORY_MIB, ephemeral storage
#                         DISK_STORAGE_MIB) runs at once: a CPU burn with NOISY_A_THREADS threads, a
#                         disk fill followed by fdatasync rewrites, and repeated allocations past
#                         its memory; then NOISY_AFTER_RUNS more times alone. PASS needs: A's CPU
#                         usage from its cgroup's cpu.stat stays within NOISY_QUOTA_TOLERANCE (1.10)
#                         times its quota in every 5 s window, A actually used >= 80% of its quota,
#                         every B invocation succeeded, and B's median CPU part and median fsync'd
#                         write slowed down by no more than NOISY_CPU_SLOWDOWN_MAX (1.30) and
#                         NOISY_IO_SLOWDOWN_MAX (3.00). The bounds were fixed before the first run
#                         on the 4-vCPU nested host (docs/kvm.md section 3.6); a miss is reported as
#                         FAIL, not tuned away.
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
#   4  a guest with egress restricted / public-web reached a destination its profile denies,
#      another tenant's guest, the node or the management network, user code could start
#      before the policy was verified, or taps / nftables state outlived the run (NET FAIL)
#   5  tenant A exceeded its CPU quota, tenant B did not complete, or B slowed down past the
#      documented bounds while A ran (NOISY FAIL)
#   6  a VMM ran outside its cgroup or without its limits, a jailed VMM was not confined, or a
#      cgroup / jail / VMM process outlived the run (HOST FAIL)
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
NET_MEASURE="${NET_MEASURE:-1}"
NET_TOKEN_B="${NET_TOKEN_B:-dev-token-tenant-b}"
NET_LISTEN_MS="${NET_LISTEN_MS:-25000}"
NET_REDIRECT_URL="${NET_REDIRECT_URL:-http://httpbin.org/redirect-to?url=http%3A%2F%2F169.254.169.254%2Flatest%2Fmeta-data%2F}"
NET_TABLE="tachyon_egress"
HOST_MEASURE="${HOST_MEASURE:-1}"
CGROUP_PARENT="${CGROUP_PARENT:-/sys/fs/cgroup/tachyon}"
JAIL_CHROOT_BASE="${JAIL_CHROOT_BASE:-/srv/jailer}"
JAILER_UID="${JAILER_UID:-64000}"
NOISY_MEASURE="${NOISY_MEASURE:-1}"
NOISY_RUNS="${NOISY_RUNS:-5}"
NOISY_AFTER_RUNS="${NOISY_AFTER_RUNS:-3}"
NOISY_A_CPU_MILLIS="${NOISY_A_CPU_MILLIS:-500}"
NOISY_A_THREADS="${NOISY_A_THREADS:-4}"
NOISY_A_BURN_MS="${NOISY_A_BURN_MS:-90000}"
NOISY_A_WARMUP_S="${NOISY_A_WARMUP_S:-15}"
NOISY_B_CPU_MILLIS="${NOISY_B_CPU_MILLIS:-1000}"
NOISY_B_ITERATIONS="${NOISY_B_ITERATIONS:-600000000}"
NOISY_B_FILES="${NOISY_B_FILES:-32}"
NOISY_B_FILE_KIB="${NOISY_B_FILE_KIB:-64}"
NOISY_QUOTA_TOLERANCE="${NOISY_QUOTA_TOLERANCE:-1.10}"
NOISY_CPU_SLOWDOWN_MAX="${NOISY_CPU_SLOWDOWN_MAX:-1.30}"
NOISY_IO_SLOWDOWN_MAX="${NOISY_IO_SLOWDOWN_MAX:-3.00}"
[ "$HOST_MEASURE" = "1" ] || NOISY_MEASURE=0
# The gateway (the provider's nft / ip calls, the jailer, cgroups) runs as root for NET and HOST.
SUDO=""
if { [ "$NET_MEASURE" = "1" ] || [ "$HOST_MEASURE" = "1" ]; } && [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo -n"
fi

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
DATA_DIR="$(sed -n 's/^data_dir *= *"\(.*\)"$/\1/p' "$CONFIG_PATH" | head -n1)"
DATA_DIR="${DATA_DIR:-./data}"
case "$DATA_DIR" in /*) ;; *) DATA_DIR="$REPO_ROOT/${DATA_DIR#./}" ;; esac

tsls() { "$TSLS_BIN" "$@"; }
export TSLS_BIN

GATEWAY_PID=""
M8_STATUS="UNKNOWN"
M8_DETAIL="the egress probe did not produce a report"
M9_STATUS="UNKNOWN"
M9_DETAIL="the resource probe did not produce a report"
DISK_STATUS="UNKNOWN"
DISK_DETAIL="the disk probe did not produce a report"
NET_STATUS="SKIPPED"
NET_DETAIL="NET_MEASURE=0"
HOST_STATUS="SKIPPED"
HOST_DETAIL="HOST_MEASURE=0"
NOISY_STATUS="SKIPPED"
NOISY_DETAIL="NOISY_MEASURE=0"
ORPHAN_NOTE="not checked"
FINDINGS=""
ORIG_IP_FORWARD=""
WATCH_PID=""
WATCH_STOP="$WORK_DIR/host-watch.stop"

gateway_alive() { [ -n "$GATEWAY_PID" ] && $SUDO kill -0 "$GATEWAY_PID" 2>/dev/null; }

# stop_gateway GRACE_SECS: SIGTERM (relayed by sudo), then SIGKILL; reap the child.
stop_gateway() {
  local grace="$1" waited=0
  [ -n "$GATEWAY_PID" ] || return 0
  if gateway_alive; then
    $SUDO kill -TERM "$GATEWAY_PID" 2>/dev/null || true
    while gateway_alive && [ "$waited" -lt $(( grace * 4 )) ]; do
      sleep 0.25
      waited=$(( waited + 1 ))
    done
    if gateway_alive; then
      e2e_warn "gateway $GATEWAY_PID did not exit after SIGTERM within ${grace}s; sending SIGKILL"
      $SUDO pkill -KILL -P "$GATEWAY_PID" 2>/dev/null || true
      $SUDO kill -KILL "$GATEWAY_PID" 2>/dev/null || true
    fi
  fi
  set +e
  wait "$GATEWAY_PID" 2>/dev/null
  STOP_RC=$?
  set -e
  GATEWAY_PID=""
  if [ -n "$SUDO" ]; then
    # Files the root gateway created stay usable for the next unprivileged run.
    $SUDO chown -R "$(id -u):$(id -g)" "$FC_RUN_DIR" "$DATA_DIR" 2>/dev/null || true
  fi
}

start_host_watch() {
  [ "$HOST_MEASURE" = "1" ] || return 0
  rm -f "$WATCH_STOP"
  $SUDO "$SCRIPT_DIR/host-watch.sh" "$CGROUP_PARENT" "$EVIDENCE_DIR" "$WATCH_STOP" 0.5 \
    > "$EVIDENCE_DIR/host-watch.stderr.txt" 2>&1 &
  WATCH_PID=$!
  e2e_log "host watcher pid $WATCH_PID (cgroups under $CGROUP_PARENT)"
}

stop_host_watch() {
  [ -n "$WATCH_PID" ] || return 0
  touch "$WATCH_STOP"
  wait "$WATCH_PID" 2>/dev/null || true
  WATCH_PID=""
  $SUDO chown "$(id -u):$(id -g)" "$EVIDENCE_DIR/cgroup-samples.tsv" "$EVIDENCE_DIR/vmm-isolation.jsonl" 2>/dev/null || true
}

cleanup() {
  local rc=$?
  set +e
  if gateway_alive; then
    e2e_log "cleanup: stopping gateway $GATEWAY_PID"
    stop_gateway 10
  fi
  stop_host_watch
  if [ -n "$ORIG_IP_FORWARD" ]; then
    $SUDO sysctl -q -w "net.ipv4.ip_forward=$ORIG_IP_FORWARD" >/dev/null 2>&1
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
  if [ "$NET_MEASURE" = "1" ]; then
    $SUDO true || e2e_die "NET_MEASURE=1 needs passwordless sudo (or run with NET_MEASURE=0)"
    ORIG_IP_FORWARD="$(sysctl -n net.ipv4.ip_forward)"
    $SUDO sysctl -q -w net.ipv4.ip_forward=1
  fi
  start_host_watch
  $SUDO env LOG_FORMAT=json TACHYON_GATEWAY_CONFIG="$CONFIG_PATH" \
    "$GATEWAY_BIN" "$GATEWAY_CONFIG_FLAG" "$CONFIG_PATH" >"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  e2e_log "gateway pid $GATEWAY_PID, config $CONFIG_PATH, log $GATEWAY_LOG"
  export TSLS_API_URL="$API_URL"
  export TSLS_TOKEN="$TOKEN"
}

wait_gateway() {
  local i
  for i in $(seq 1 60); do
    if ! gateway_alive; then
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
# 3b. NET: egress restricted / public-web (PLT-4622)
# ---------------------------------------------------------------------------

# Host addresses a guest must never reach, and a control that they are really listening.
net_host_facts() {
  local route
  route="$(ip -4 route show default | head -n 1)"
  HOST_GW="$(printf '%s' "$route" | awk '{print $3}')"
  HOST_IF="$(printf '%s' "$route" | awk '{print $5}')"
  HOST_IP="$(ip -4 -o addr show dev "$HOST_IF" | awk '{print $4}' | cut -d/ -f1 | head -n 1)"
  if [ -z "$HOST_GW" ] || [ -z "$HOST_IP" ]; then
    echo "cannot find the host address / default gateway" >&2
    return 1
  fi
  state_set net.host_gw "$HOST_GW"
  state_set net.host_ip "$HOST_IP"
  local ssh_control="closed"
  timeout 2 bash -c "</dev/tcp/$HOST_IP/22" 2>/dev/null && ssh_control="open"
  {
    echo "host_if=$HOST_IF host_ip=$HOST_IP default_gw=$HOST_GW"
    echo "control: $HOST_IP:22 from the host itself is $ssh_control (a guest must not reach it)"
    echo "net.ipv4.ip_forward=$(sysctl -n net.ipv4.ip_forward) (was $ORIG_IP_FORWARD)"
    echo "nft $($SUDO nft --version 2>/dev/null)"
    echo "--- taps before ---"
    ip -br link show | grep '^tsls' || echo "(none)"
    echo "--- table inet $NET_TABLE before ---"
    $SUDO nft list table inet "$NET_TABLE" 2>&1 || true
  } > "$EVIDENCE_DIR/net-host.txt"
  cat "$EVIDENCE_DIR/net-host.txt"
  jq -r '.capabilities | "capabilities: restricted=\(.egress_restricted.status) public_web=\(.egress_public_web.status)"' \
    "$EVIDENCE_DIR/provider.json"
}

ensure_function_b() {
  local id
  run_capture env TSLS_TOKEN="$NET_TOKEN_B" "$TSLS_BIN" functions get "$FUNCTION_NAME" --json
  if [ "$RUN_RC" -eq 0 ]; then
    id="$(printf '%s' "$RUN_OUT" | jq -r .id)"
  else
    id="$(TSLS_TOKEN="$NET_TOKEN_B" tsls functions create --name "$FUNCTION_NAME" \
      --description "PLT-4622 cross-tenant listener" --json | jq -r .id)"
  fi
  state_set "fn.b" "$id"
  e2e_log "tenant B function $FUNCTION_NAME: $id"
}

deploy_net_revisions() {
  deploy_revision publicweb "$PROBE_MEMORY_MIB" "isolation probe egress public-web" \
    --egress public-web --no-publish
  deploy_revision restricted "$PROBE_MEMORY_MIB" "isolation probe egress restricted to 1.1.1.1:443" \
    --egress restricted --egress-allow 1.1.1.1/32:443 --no-publish
  local rev
  rev="$(TSLS_TOKEN="$NET_TOKEN_B" tsls functions deploy --function "$FUNCTION_NAME" --binary "$PROBE_BIN" \
    --arch "$ARCH" --memory-mib "$PROBE_MEMORY_MIB" --cpu-millis "$PROBE_CPU_MILLIS" \
    --timeout-seconds "$PROBE_TIMEOUT_SECONDS" --egress public-web --no-publish \
    --description "tenant B listener (egress public-web)" --json | jq -r .id)"
  if [ -z "$rev" ] || [ "$rev" = "null" ]; then
    echo "tenant B deploy returned no revision" >&2
    return 1
  fi
  state_set rev.b "$rev"
  TSLS_TOKEN="$NET_TOKEN_B" tsls functions revision "$FUNCTION_NAME" "$rev" --json > "$EVIDENCE_DIR/revision-tenant-b.json"
  jq -c '.spec | {egress, egress_allow}' "$EVIDENCE_DIR/revision-publicweb.json" \
    "$EVIDENCE_DIR/revision-restricted.json" "$EVIDENCE_DIR/revision-tenant-b.json"
}

# The node's own tap addresses of the first two leases (172.30.0.1 and .5 with the default pool).
NODE_TAP_TARGETS='"172.30.0.1:22", "172.30.0.5:22"'

write_net_expectations() {
  local gw ip
  gw="$(state_get net.host_gw)"
  ip="$(state_get net.host_ip)"
  cat > "$EVIDENCE_DIR/net-expect-publicweb.json" <<EOF
{
  "connect": {
    "allow": ["1.1.1.1:443", "1.0.0.1:443"],
    "deny": ["169.254.169.254:80", "10.0.2.2:80", "100.64.0.1:80", "192.168.0.1:80",
             "$gw:22", "$gw:53", "$ip:22", "$ip:8080", $NODE_TAP_TARGETS,
             "[2606:4700:4700::1111]:443", "[::ffff:169.254.169.254]:80"]
  },
  "udp_dns": { "allow": ["1.1.1.1:53"], "deny": ["8.8.8.8:53", "$gw:53"] },
  "resolve": { "allow": ["example.com"], "deny_connect": ["169.254.169.254.nip.io"], "deny_resolve": [] },
  "http_redirect": { "deny_follow": true }
}
EOF
  cat > "$EVIDENCE_DIR/net-expect-restricted.json" <<EOF
{
  "connect": {
    "allow": ["1.1.1.1:443"],
    "deny": ["1.0.0.1:443", "1.1.1.1:80", "169.254.169.254:80", "$gw:22", "$ip:22",
             $NODE_TAP_TARGETS, "[2606:4700:4700::1111]:443"]
  },
  "udp_dns": { "allow": [], "deny": ["1.1.1.1:53", "8.8.8.8:53"] },
  "resolve": { "allow": [], "deny_connect": [], "deny_resolve": ["example.com"] },
  "http_redirect": { "deny_follow": false }
}
EOF
}

# net_payload EXPECT_FILE -> the probe payload that exercises every target of the expectation.
net_payload() {
  jq -c --argjson connect "$CONNECT_TIMEOUT_MS" --argjson dns "$DNS_TIMEOUT_MS" --arg url "$NET_REDIRECT_URL" \
    '{probe: "net", connect_timeout_ms: $connect, dns_timeout_ms: $dns, dns_name: "example.com",
      connect: (.connect.allow + .connect.deny),
      udp_dns: (.udp_dns.allow + .udp_dns.deny),
      resolve: (.resolve.allow + .resolve.deny_connect + .resolve.deny_resolve),
      resolve_port: 80}
     + (if .http_redirect.deny_follow then {http_redirect: $url} else {} end)' "$1"
}

# net_checks REPORT EXPECT -> one JSON array of {kind, target, expect, ok, detail}.
net_checks() {
  jq -n --slurpfile r "$1" --slurpfile e "$2" '
    ($r[0].net // {}) as $n | $e[0] as $e |
    def find(arr; key; t): [(arr // [])[] | select(.[key] == t)] | first;
    [
      ($e.connect.allow[] as $t | find($n.connect; "target"; $t) as $c
        | {kind: "connect", target: $t, expect: "allow", inconclusive: ($c == null), ok: ($c.connected == true),
           detail: ($c.error_kind // $c.peer // "missing")}),
      ($e.connect.deny[] as $t | find($n.connect; "target"; $t) as $c
        | {kind: "connect", target: $t, expect: "deny", inconclusive: ($c == null), ok: ($c != null and $c.connected == false),
           detail: ($c.error_kind // $c.peer // "missing")}),
      ($e.udp_dns.allow[] as $t | find($n.udp_dns; "server"; $t) as $c
        | {kind: "udp_dns", target: $t, expect: "allow", inconclusive: ($c == null), ok: ($c.answered == true),
           detail: (($c.addresses // []) | join(",")) }),
      ($e.udp_dns.deny[] as $t | find($n.udp_dns; "server"; $t) as $c
        | {kind: "udp_dns", target: $t, expect: "deny", inconclusive: ($c == null), ok: ($c != null and $c.answered == false),
           detail: ($c.error_kind // "answered")}),
      ($e.resolve.allow[] as $t | find($n.resolve; "name"; $t) as $c
        | {kind: "resolve+connect", target: $t, expect: "allow", inconclusive: ($c == null), ok: ($c.connected == true),
           detail: (($c.addresses // []) | join(","))}),
      ($e.resolve.deny_connect[] as $t | find($n.resolve; "name"; $t) as $c
        | {kind: "resolve+connect", target: $t, expect: "deny_connect", inconclusive: ($c == null),
           ok: ($c != null and $c.connected == false),
           detail: "resolved=\($c.resolved) to \(($c.addresses // []) | join(","))"}),
      ($e.resolve.deny_resolve[] as $t | find($n.resolve; "name"; $t) as $c
        | {kind: "resolve", target: $t, expect: "deny_resolve", inconclusive: ($c == null),
           ok: ($c != null and $c.resolved == false), detail: ($c.error_kind // "resolved")}),
      (if $e.http_redirect.deny_follow then
         ($n.http_redirect // {}) as $h
         | {kind: "http_redirect", target: ($h.url // "missing"), expect: "deny_follow",
            ok: ($h.follow_connected != true), inconclusive: ($h.fetched != true),
            detail: "fetched=\($h.fetched) status=\($h.status) location=\($h.location)"}
       else empty end)
    ]'
}

# run_net_probe KEY REVISION_KEY: invoke, record, evaluate against net-expect-KEY.json.
run_net_probe() {
  local key="$1" rev_key="$2" expect="$EVIDENCE_DIR/net-expect-$1.json" payload
  payload="$(net_payload "$expect")"
  invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get "rev.$rev_key")"
  printf '%s\n' "$INVOKE_ERR" >&2
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/net-$key.json"
  if [ -n "$INVOKE_ID" ]; then
    tsls functions invocation "$INVOKE_ID" --json > "$EVIDENCE_DIR/net-$key-invocation.json" || true
  fi
  assert_eq 0 "$INVOKE_RC" "net probe ($key) exit code" || return 1
  net_checks "$EVIDENCE_DIR/net-$key.json" "$expect" > "$EVIDENCE_DIR/net-checks-$key.json"
  jq -r '.[] | "  \(if .ok then "ok  " else "BAD " end) \(.kind) \(.target) expect=\(.expect) (\(.detail))"' \
    "$EVIDENCE_DIR/net-checks-$key.json"
  jq -r '.attempts[-1].boot_evidence.details // {} | "tap=\(.egress_tap) guest_ip=\(.guest_ip) rules=\(.egress_policy_rules) verified_ms=\(.egress_policy_verified_ms)"' \
    "$EVIDENCE_DIR/net-$key-invocation.json" 2>/dev/null || true
}

run_net_publicweb() { run_net_probe publicweb publicweb; }
run_net_restricted() { run_net_probe restricted restricted; }

# Leases (net.json) currently present under the provider workdir, one JSON object per line.
current_leases() {
  $SUDO find "$FC_RUN_DIR" -mindepth 2 -maxdepth 2 -name net.json -exec cat {} + 2>/dev/null |
    jq -c '.' 2>/dev/null || true
}

# Two tenants at once: B listens, A (public-web) tries B's guest and B's host tap.
run_net_cross_tenant() {
  local _ b_lease b_env a_pid b_pid payload during="$EVIDENCE_DIR/net-cross-during.txt"
  # The environments of the previous probes are torn down after their response; start clean so
  # the first lease that appears is tenant B's.
  for _ in $(seq 1 120); do
    [ -z "$(current_leases)" ] && break
    sleep 0.25
  done
  [ -z "$(current_leases)" ] || { echo "leases of earlier environments are still present" >&2; return 1; }
  (
    run_capture env TSLS_TOKEN="$NET_TOKEN_B" "$TSLS_BIN" functions invoke "$FUNCTION_NAME" \
      --payload "{\"probe\":\"listen\",\"port\":8080,\"duration_ms\":$NET_LISTEN_MS}" --json \
      --revision-id "$(state_get rev.b)"
    printf '%s\n' "$RUN_OUT" > "$EVIDENCE_DIR/net-cross-b.json"
    printf '%s\n' "$RUN_ERR" > "$EVIDENCE_DIR/net-cross-b.stderr.txt"
  ) &
  b_pid=$!
  b_lease=""
  for _ in $(seq 1 120); do
    b_lease="$(current_leases | head -n 1)"
    [ -n "$b_lease" ] && break
    sleep 0.25
  done
  [ -n "$b_lease" ] || { echo "tenant B's environment never got a lease" >&2; wait "$b_pid"; return 1; }
  b_env="$(printf '%s' "$b_lease" | jq -r .env_id)"
  state_set net.b_guest_ip "$(printf '%s' "$b_lease" | jq -r .guest_ip)"
  state_set net.b_host_ip "$(printf '%s' "$b_lease" | jq -r .host_ip)"
  e2e_log "tenant B env $b_env: guest $(state_get net.b_guest_ip), tap host $(state_get net.b_host_ip)"
  sleep 3 # boot + handler start: B must be listening before A connects
  payload="$(jq -nc --arg g "$(state_get net.b_guest_ip)" --arg h "$(state_get net.b_host_ip)" \
    --argjson connect "$CONNECT_TIMEOUT_MS" \
    '{probe: "net", connect_timeout_ms: $connect,
      connect: ["\($g):8080", "\($g):22", "\($h):22", "\($h):8080", "1.1.1.1:443"]}')"
  jq -n --arg g "$(state_get net.b_guest_ip)" --arg h "$(state_get net.b_host_ip)" \
    '{connect: {allow: ["1.1.1.1:443"], deny: ["\($g):8080", "\($g):22", "\($h):22", "\($h):8080"]},
      udp_dns: {allow: [], deny: []}, resolve: {allow: [], deny_connect: [], deny_resolve: []},
      http_redirect: {deny_follow: false}}' > "$EVIDENCE_DIR/net-expect-cross.json"
  (
    invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.publicweb)"
    printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/net-cross-a.json"
    printf '%s\n' "$INVOKE_ERR" > "$EVIDENCE_DIR/net-cross-a.stderr.txt"
  ) &
  a_pid=$!
  for _ in $(seq 1 120); do
    [ "$(current_leases | wc -l | tr -d ' ')" -ge 2 ] && break
    sleep 0.25
  done
  {
    echo "== leases while both environments run =="
    current_leases
    echo "== taps =="
    ip -br link show | grep '^tsls' || echo "(none)"
    echo "== table inet $NET_TABLE =="
    $SUDO nft list table inet "$NET_TABLE" 2>&1 || true
  } > "$during"
  wait "$a_pid" || true
  wait "$b_pid" || true
  net_checks "$EVIDENCE_DIR/net-cross-a.json" "$EVIDENCE_DIR/net-expect-cross.json" \
    > "$EVIDENCE_DIR/net-checks-cross.json"
  jq -r '.[] | "  \(if .ok then "ok  " else "BAD " end) A -> \(.target) expect=\(.expect) (\(.detail))"' \
    "$EVIDENCE_DIR/net-checks-cross.json"
  jq -r '"  B listening=\(.listen.listening) accepted=\(.listen.accepted) peers=\(.listen.peers)"' \
    "$EVIDENCE_DIR/net-cross-b.json"
  [ "$(grep -c '^table inet' "$during")" -ge 1 ] || { echo "no nft table while two environments ran" >&2; return 1; }
  jq -e '.listen.listening == true' "$EVIDENCE_DIR/net-cross-b.json" >/dev/null
}

# After the gateway stopped: no tap, no table, no lease may be left.
net_cleanup_check() {
  local taps table leases
  taps="$(ip -br link show | grep '^tsls' || true)"
  if $SUDO nft list table inet "$NET_TABLE" >/dev/null 2>&1; then table="present"; else table="absent"; fi
  leases="$(current_leases | wc -l | tr -d ' ')"
  {
    echo "taps after the run: ${taps:-(none)}"
    echo "table inet $NET_TABLE after the run: $table"
    echo "net.json leases left under $FC_RUN_DIR: $leases"
    echo "--- nft list tables ---"
    $SUDO nft list tables 2>&1
  } > "$EVIDENCE_DIR/net-cleanup.txt"
  cat "$EVIDENCE_DIR/net-cleanup.txt"
  [ -z "$taps" ] && [ "$table" = "absent" ] && [ "$leases" = "0" ]
}

# ---------------------------------------------------------------------------
# 3c. NOISY: two tenants on one host (PLT-4622)
# ---------------------------------------------------------------------------

# invoke_as TOKEN FUNCTION PAYLOAD ARGS...: invoke_capture with another tenant's token.
invoke_as() {
  local token="$1" saved="$TSLS_TOKEN"
  shift
  export TSLS_TOKEN="$token"
  invoke_capture "$@"
  export TSLS_TOKEN="$saved"
}

deploy_noisy_revisions() {
  ensure_function_b
  local timeout=$(( NOISY_A_BURN_MS / 1000 + 90 )) rev
  for spec in "cpu:cpu burn" "io:disk fill and fdatasync rewrites" "oom:allocation past memory"; do
    rev="$(tsls functions deploy --function "$FUNCTION_NAME" --binary "$PROBE_BIN" --arch "$ARCH" \
      --memory-mib "$ALLOC_MEMORY_MIB" --cpu-millis "$NOISY_A_CPU_MILLIS" \
      --ephemeral-storage-mib "$DISK_STORAGE_MIB" --timeout-seconds "$timeout" --no-publish \
      --description "noisy tenant A: ${spec#*:}" --json | jq -r .id)"
    if [ -z "$rev" ] || [ "$rev" = "null" ]; then echo "noisy A deploy (${spec%%:*}) returned no revision" >&2; return 1; fi
    state_set "rev.noisy_a_${spec%%:*}" "$rev"
    tsls functions revision "$FUNCTION_NAME" "$rev" --json > "$EVIDENCE_DIR/revision-noisy-a-${spec%%:*}.json"
  done
  rev="$(TSLS_TOKEN="$NET_TOKEN_B" tsls functions deploy --function "$FUNCTION_NAME" --binary "$PROBE_BIN" \
    --arch "$ARCH" --memory-mib "$PROBE_MEMORY_MIB" --cpu-millis "$NOISY_B_CPU_MILLIS" \
    --timeout-seconds "$PROBE_TIMEOUT_SECONDS" --no-publish \
    --description "noisy tenant B: fixed workload" --json | jq -r .id)"
  if [ -z "$rev" ] || [ "$rev" = "null" ]; then echo "noisy B deploy returned no revision" >&2; return 1; fi
  state_set rev.noisy_b "$rev"
  TSLS_TOKEN="$NET_TOKEN_B" tsls functions revision "$FUNCTION_NAME" "$rev" --json > "$EVIDENCE_DIR/revision-noisy-b.json"
}

# noisy_b_run PHASE N: one invocation of B's workload, one JSON line in noisy-b.jsonl.
noisy_b_run() {
  local phase="$1" n="$2" payload record="{}"
  payload="$(jq -nc --argjson i "$NOISY_B_ITERATIONS" --argjson f "$NOISY_B_FILES" --argjson k "$NOISY_B_FILE_KIB" \
    '{probe: "work", cpu_iterations: $i, files: $f, file_kib: $k}')"
  invoke_as "$NET_TOKEN_B" "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.noisy_b)"
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/noisy-b-$phase-$n.json"
  if [ -n "$INVOKE_ID" ]; then
    TSLS_TOKEN="$NET_TOKEN_B" tsls functions invocation "$INVOKE_ID" --json > "$EVIDENCE_DIR/noisy-b-$phase-$n-invocation.json" 2>/dev/null || true
    record="$(cat "$EVIDENCE_DIR/noisy-b-$phase-$n-invocation.json" 2>/dev/null || echo '{}')"
  fi
  jq -nc --arg phase "$phase" --argjson n "$n" --argjson rc "$INVOKE_RC" --arg id "$INVOKE_ID" \
    --argjson client_ms "$INVOKE_MS" --argjson record "$record" \
    --slurpfile out <(printf '%s' "${INVOKE_OUT:-null}" | jq -c . 2>/dev/null || echo null) \
    '($out[0] // {}) as $o | {phase: $phase, run: $n, rc: $rc, invocation: $id, client_ms: $client_ms,
      status: ($record.status // null), env: ($record.attempts[-1].environment_id // null),
      handler_ms: ($record.attempts[-1].timings.handler_ms // null),
      boot_ms: ($record.attempts[-1].timings.environment_boot_ms // null),
      ok: ($o.work.ok // false), cpu_ms: ($o.work.cpu.ms // null),
      write_p50_ms: ($o.work.writes.latency.p50_ms // null), write_ms: ($o.work.writes.ms // null),
      total_ms: ($o.work.total_ms // null)}' >> "$EVIDENCE_DIR/noisy-b.jsonl"
  tail -n 1 "$EVIDENCE_DIR/noisy-b.jsonl" | jq -r '"  B \(.phase) #\(.run): status=\(.status) cpu \(.cpu_ms) ms, write p50 \(.write_p50_ms) ms, handler \(.handler_ms) ms"'
}

noisy_baseline() {
  : > "$EVIDENCE_DIR/noisy-b.jsonl"
  local i
  for i in $(seq 1 "$NOISY_RUNS"); do noisy_b_run alone "$i"; done
}

# noisy_a_invoke KIND PAYLOAD: tenant A's invocation in the background; files noisy-a-KIND*.
noisy_a_invoke() {
  local kind="$1" payload="$2"
  invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get "rev.noisy_a_$kind")"
  printf '%s\n' "$INVOKE_OUT" > "$EVIDENCE_DIR/noisy-a-$kind.json"
  printf '%s\n' "$INVOKE_ERR" > "$EVIDENCE_DIR/noisy-a-$kind.stderr.txt"
  [ -z "$INVOKE_ID" ] || tsls functions invocation "$INVOKE_ID" --json > "$EVIDENCE_DIR/noisy-a-$kind-invocation.json" 2>/dev/null || true
}

noisy_contended() {
  local cpu_pid io_pid oom_pid i done_file="$WORK_DIR/noisy-a-cpu.done"
  rm -f "$done_file"
  ( noisy_a_invoke cpu "$(jq -nc --argjson ms "$NOISY_A_BURN_MS" --argjson t "$NOISY_A_THREADS" \
      '{probe: "cpu", burn_ms: $ms, threads: $t}')"; touch "$done_file" ) &
  cpu_pid=$!
  ( noisy_a_invoke io "$(jq -nc --argjson ms "$NOISY_A_BURN_MS" '{probe: "io", duration_ms: $ms, block_kib: 1024, fill_first: true}')" ) &
  io_pid=$!
  (
    : > "$EVIDENCE_DIR/noisy-a-oom.jsonl"
    local n=0 payload
    payload="$(jq -nc --argjson mib "$ALLOC_MIB" '{probe: "resources", alloc_mib: $mib}')"
    while [ ! -e "$done_file" ] && [ "$n" -lt 40 ]; do
      n=$(( n + 1 ))
      invoke_capture "$FUNCTION_NAME" "$payload" --revision-id "$(state_get rev.noisy_a_oom)"
      local rec='{}'
      [ -z "$INVOKE_ID" ] || rec="$(tsls functions invocation "$INVOKE_ID" --json 2>/dev/null || echo '{}')"
      printf '%s' "$rec" | jq -c --argjson n "$n" --argjson rc "$INVOKE_RC" \
        '{run: $n, rc: $rc, invocation: .id, status: .status, class: .error.class, error_type: .error.error_type,
          env: .attempts[-1].environment_id}' >> "$EVIDENCE_DIR/noisy-a-oom.jsonl" 2>/dev/null || true
    done
  ) &
  oom_pid=$!
  e2e_log "tenant A started (cpu $cpu_pid, io $io_pid, oom loop $oom_pid); warming up ${NOISY_A_WARMUP_S}s"
  state_set noisy.a_started_ms "$(now_ms)"
  sleep "$NOISY_A_WARMUP_S"
  state_set noisy.b_contended_start_ms "$(now_ms)"
  for i in $(seq 1 "$NOISY_RUNS"); do noisy_b_run contended "$i"; done
  state_set noisy.b_contended_end_ms "$(now_ms)"
  [ -e "$done_file" ] && note_finding "NOISY: tenant A's CPU burn ended before tenant B's contended runs did (raise NOISY_A_BURN_MS)"
  wait "$cpu_pid" "$io_pid" "$oom_pid" 2>/dev/null || true
  for i in $(seq 1 "$NOISY_AFTER_RUNS"); do noisy_b_run after "$i"; done
}

# ---------------------------------------------------------------------------
# 3d. HOST: cgroup placement and jail confinement of every VMM (PLT-4622)
# ---------------------------------------------------------------------------

# After the gateway stopped: no environment cgroup, jail or VMM process may be left.
host_cleanup_check() {
  local cgroups jails procs
  cgroups="$(find "$CGROUP_PARENT" -mindepth 1 -maxdepth 1 -type d 2>/dev/null || true)"
  jails="$($SUDO find "$JAIL_CHROOT_BASE" -mindepth 1 -maxdepth 2 2>/dev/null || true)"
  procs="$(pgrep -a -f '(^|/)(firecracker|jailer)( |$)' 2>/dev/null | grep -v -e host-watch -e pgrep || true)"
  {
    echo "cgroup parent $CGROUP_PARENT: $([ -d "$CGROUP_PARENT" ] && echo present || echo absent)"
    echo "environment cgroups left: ${cgroups:-(none)}"
    echo "jails left under $JAIL_CHROOT_BASE: ${jails:-(none)}"
    echo "firecracker / jailer processes left: ${procs:-(none)}"
  } > "$EVIDENCE_DIR/host-cleanup.txt"
  cat "$EVIDENCE_DIR/host-cleanup.txt"
  [ -z "$cgroups" ] && [ -z "$jails" ] && [ -z "$procs" ] && [ ! -d "$CGROUP_PARENT" ]
}

# ---------------------------------------------------------------------------
# 4. verdicts
# ---------------------------------------------------------------------------

json_file_ok() { [ -s "$1" ] && jq -e . "$1" >/dev/null 2>&1; }
json_or_null() { if json_file_ok "$1"; then jq -c . "$1"; else echo null; fi; }

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

# Initial egress race: for every environment with a policed NIC, the policy was installed and read
# back before InstanceStart (gateway.log is JSON lines; RFC 3339 timestamps compare as strings).
net_race_record() {
  jq -R 'fromjson? // empty' "$GATEWAY_LOG" | jq -s '
    [ .[] | select(.message == "egress policy installed and verified" or .message == "InstanceStart accepted"
                   or .message == "egress policy counters at teardown") ]
    | group_by(.env_id)
    | map({env_id: .[0].env_id,
           egress: (map(select(.egress)) | first | .egress),
           tap: (map(select(.tap)) | first | .tap),
           policy_verified_at: (map(select(.message == "egress policy installed and verified")) | first | .timestamp),
           policy_verify_ms: (map(select(.message == "egress policy installed and verified")) | first | .ms),
           instance_start_at: (map(select(.message == "InstanceStart accepted")) | first | .timestamp),
           torn_down: (map(select(.message == "egress policy counters at teardown")) | length > 0)})
    | map(select(.policy_verified_at != null))
    | map(. + {started: (.instance_start_at != null),
               policy_before_start: (.instance_start_at == null or .policy_verified_at < .instance_start_at)})' \
    > "$EVIDENCE_DIR/net-race.json"
  jq -R 'fromjson? // empty' "$GATEWAY_LOG" |
    jq -r 'select(.message == "egress policy counters at teardown") | "== \(.env_id) ==\n\(.counters)"' \
    > "$EVIDENCE_DIR/net-counters.txt"
}

evaluate_net() {
  [ "$NET_MEASURE" = "1" ] || return 0
  local f deny_bad allow_bad inconclusive missing accepted envs race_bad detail="" ok=1 unknown=0
  for f in publicweb restricted cross; do
    if ! json_file_ok "$EVIDENCE_DIR/net-checks-$f.json" || [ "$(jq length "$EVIDENCE_DIR/net-checks-$f.json")" = "0" ]; then
      unknown=1
      note_finding "NET: no checks for $f (see steps/)"
      continue
    fi
    deny_bad="$(jq -r '[.[] | select(.expect != "allow" and (.ok | not) and (.inconclusive | not)) | "\(.kind) \(.target)"] | join("; ")' "$EVIDENCE_DIR/net-checks-$f.json")"
    allow_bad="$(jq -r '[.[] | select(.expect == "allow" and (.ok | not) and (.inconclusive | not)) | "\(.kind) \(.target)"] | join("; ")' "$EVIDENCE_DIR/net-checks-$f.json")"
    inconclusive="$(jq -r '[.[] | select(.inconclusive == true and .kind == "http_redirect") | .target] | join("; ")' "$EVIDENCE_DIR/net-checks-$f.json")"
    missing="$(jq -r '[.[] | select(.inconclusive == true and .kind != "http_redirect") | "\(.kind) \(.target)"] | join("; ")' "$EVIDENCE_DIR/net-checks-$f.json")"
    if [ -n "$missing" ]; then
      unknown=1
      note_finding "NET: $f has no result for: $missing (the probe did not run; see steps/)"
    fi
    detail="$detail$f: $(jq -r '[.[] | select(.expect != "allow")] | "\([.[] | select(.ok)] | length)/\(length) denied"' "$EVIDENCE_DIR/net-checks-$f.json"), $(jq -r '[.[] | select(.expect == "allow")] | "\([.[] | select(.ok)] | length)/\(length) allowed"' "$EVIDENCE_DIR/net-checks-$f.json"); "
    if [ -n "$deny_bad" ]; then
      ok=0
      note_finding "NET: $f reached what its profile denies: $deny_bad"
    fi
    if [ -n "$allow_bad" ]; then
      unknown=1
      note_finding "NET: $f could not reach what its profile allows (policy or host connectivity): $allow_bad"
    fi
    [ -z "$inconclusive" ] || note_finding "NET: $f redirect check inconclusive (the redirector was not reachable): $inconclusive"
  done
  if json_file_ok "$EVIDENCE_DIR/net-cross-b.json"; then
    accepted="$(jq -r '.listen.accepted // "null"' "$EVIDENCE_DIR/net-cross-b.json")"
    detail="${detail}tenant B accepted $accepted connection(s); "
    if [ "$accepted" != "0" ]; then
      ok=0
      note_finding "NET: tenant B's guest accepted $accepted connection(s) during tenant A's probe: $(jq -c '.listen.peers' "$EVIDENCE_DIR/net-cross-b.json")"
    fi
  else
    unknown=1
    note_finding "NET: no tenant B listener report"
  fi
  net_race_record
  envs="$(jq '[.[] | select(.started)] | length' "$EVIDENCE_DIR/net-race.json")"
  race_bad="$(jq -r '[.[] | select(.policy_before_start | not) | .env_id] | join(", ")' "$EVIDENCE_DIR/net-race.json")"
  detail="${detail}policy verified before InstanceStart in $(jq '[.[] | select(.started and .policy_before_start)] | length' "$EVIDENCE_DIR/net-race.json")/$envs policed boots; "
  if [ "$envs" -lt 4 ]; then
    unknown=1
    note_finding "NET: only $envs policed boots started (expected 4: public-web, restricted, cross A and B)"
  fi
  if [ -n "$race_bad" ]; then
    ok=0
    note_finding "NET: InstanceStart was not preceded by a verified policy for $race_bad"
  fi
  if [ "$(state_get net.cleanup)" = "clean" ]; then
    detail="${detail}taps / table / leases gone after the run"
  else
    ok=0
    detail="${detail}leftovers after the run (net-cleanup.txt)"
    note_finding "NET: taps, nftables state or leases outlived the run (net-cleanup.txt)"
  fi
  if [ "$ok" -eq 0 ]; then
    NET_STATUS="FAIL"
  elif [ "$unknown" -eq 1 ]; then
    NET_STATUS="UNKNOWN"
  else
    NET_STATUS="PASS"
  fi
  NET_DETAIL="$detail"
}

evaluate_host() {
  [ "$HOST_MEASURE" = "1" ] || return 0
  local vmms="$EVIDENCE_DIR/vmm-isolation.jsonl" spawned="$EVIDENCE_DIR/host-spawned.json" checks="$EVIDENCE_DIR/host-checks.json"
  local total seen bad missing jailed ok=1 unknown=0 detail
  jq -R 'fromjson? // empty' "$GATEWAY_LOG" |
    jq -s '[.[] | select(.message == "firecracker spawned") | {env_id, pid, jailed, cgroup}]' > "$spawned"
  if [ ! -s "$vmms" ]; then
    HOST_STATUS="UNKNOWN"
    HOST_DETAIL="the host watcher recorded no VMM (host-watch.stderr.txt)"
    return 0
  fi
  jq -s --slurpfile spawned "$spawned" --argjson uid "$JAILER_UID" --arg parent "/${CGROUP_PARENT#/sys/fs/cgroup/}" '
    (map({key: .env, value: .}) | from_entries) as $by_env |
    [ $spawned[0][] | . as $s | ($by_env[$s.env_id] // null) as $v |
      {env: $s.env_id, jailed: ($s.jailed == true), observed: ($v != null),
       in_cgroup: ($v != null and $v.cgroup == "0::\($parent)/\($s.env_id)"
                  and ($v.cgroup_procs | split(" ") | index($v.pid | tostring)) != null),
       limits_set: ($v != null and $v.cpu_max != "" and ($v.cpu_max | startswith("max") | not)
                    and $v.memory_max != "max" and $v.pids_max != "max"),
       unprivileged: ($v != null and $v.uid == $uid and $v.gid != 0 and $v.cap_eff == "0000000000000000"),
       confined: ($v != null and $v.chroot and $v.new_pid_ns and $v.new_mnt_ns),
       seccomp: ($v != null and ([$v.threads[] | select(.comm | test("^fc_(vcpu|api)")) | .seccomp] | length > 0
                 and all(. == 2))),
       cpu_max: ($v.cpu_max // null), memory_max: ($v.memory_max // null), uid: ($v.uid // null),
       threads: ($v.threads // null)} ]' "$vmms" > "$checks"
  total="$(jq length "$checks")"
  seen="$(jq '[.[] | select(.observed)] | length' "$checks")"
  jailed="$(jq '[.[] | select(.jailed)] | length' "$checks")"
  missing="$(jq -r '[.[] | select(.observed | not) | .env] | join(", ")' "$checks")"
  bad="$(jq -r '[.[] | select(.observed) | select((.in_cgroup and .limits_set) | not) | .env] | join(", ")' "$checks")"
  detail="$seen/$total VMMs observed; in own cgroup with cpu.max/memory.max/pids.max: $(jq '[.[] | select(.in_cgroup and .limits_set)] | length' "$checks")/$seen"
  if [ -n "$bad" ]; then
    ok=0
    note_finding "HOST: VMMs outside their cgroup or without limits: $bad"
  fi
  if [ -n "$missing" ]; then
    unknown=1
    note_finding "HOST: the watcher did not observe the VMM of $missing (it may have exited within a sample)"
  fi
  if [ "$jailed" -gt 0 ]; then
    bad="$(jq -r '[.[] | select(.observed and .jailed) | select((.unprivileged and .confined and .seccomp) | not) | .env] | join(", ")' "$checks")"
    detail="$detail; jailed (uid $JAILER_UID, CapEff 0, chroot, new PID+mount ns, seccomp 2 on vCPU/API threads): $(jq '[.[] | select(.observed and .jailed and .unprivileged and .confined and .seccomp)] | length' "$checks")/$(jq '[.[] | select(.observed and .jailed)] | length' "$checks")"
    if [ -n "$bad" ]; then
      ok=0
      note_finding "HOST: jailed VMMs not confined as configured: $bad (host-checks.json)"
    fi
  else
    detail="$detail; jailer not enabled in $CONFIG_PATH"
    note_finding "HOST: the VMMs ran without the jailer"
  fi
  if [ "$(state_get host.cleanup)" = "clean" ]; then
    detail="$detail; no cgroup / jail / VMM left"
  else
    ok=0
    detail="$detail; leftovers (host-cleanup.txt)"
    note_finding "HOST: cgroups, jails or VMM processes outlived the run (host-cleanup.txt)"
  fi
  if [ "$ok" -eq 0 ]; then HOST_STATUS="FAIL"; elif [ "$unknown" -eq 1 ]; then HOST_STATUS="UNKNOWN"; else HOST_STATUS="PASS"; fi
  HOST_DETAIL="$detail"
}

# cgroup CPU usage of one environment from cgroup-samples.tsv: mean over its life after a warm-up
# and the highest rate over any window of at least 5 s, in cores.
cgroup_cpu_rates() { # ENV SKIP_MS
  awk -F '\t' -v env="$1" -v skip="$2" '
    NR > 1 && $2 == env && $3 != "" { n++; t[n] = $1; u[n] = $3 }
    END {
      if (n < 2) { print "{}"; exit }
      start = 0
      for (i = 1; i <= n; i++) if (t[i] >= t[1] + skip) { start = i; break }
      mean = "null"
      if (start > 0 && n > start && t[n] > t[start]) mean = (u[n] - u[start]) / ((t[n] - t[start]) * 1000)
      max = 0; windows = 0
      for (i = 1; i <= n; i++) {
        for (j = i + 1; j <= n; j++) if (t[j] - t[i] >= 5000) break
        if (j <= n) { r = (u[j] - u[i]) / ((t[j] - t[i]) * 1000); windows++; if (r > max) max = r }
      }
      printf "{\"samples\": %d, \"span_ms\": %d, \"mean_cores_after_warmup\": %s, \"max_cores_5s\": %.4f, \"windows_5s\": %d, \"usage_usec\": %d}\n", n, t[n] - t[1], mean, max, windows, u[n]
    }' "$EVIDENCE_DIR/cgroup-samples.tsv"
}

evaluate_noisy() {
  [ "$NOISY_MEASURE" = "1" ] || return 0
  local b="$EVIDENCE_DIR/noisy-b.jsonl" out="$EVIDENCE_DIR/noisy.json" a_env quota rates ok=1 unknown=0 detail
  if [ ! -s "$b" ] || ! json_file_ok "$EVIDENCE_DIR/noisy-a-cpu-invocation.json"; then
    NOISY_STATUS="UNKNOWN"
    NOISY_DETAIL="no tenant B record or no tenant A CPU invocation (see steps/)"
    return 0
  fi
  a_env="$(jq -r '.attempts[-1].environment_id // empty' "$EVIDENCE_DIR/noisy-a-cpu-invocation.json")"
  quota="$(awk -v m="$NOISY_A_CPU_MILLIS" 'BEGIN { printf "%.4f", m / 1000 }')"
  rates="$(cgroup_cpu_rates "$a_env" 10000)"
  jq -s --argjson rates "$rates" --arg a_env "$a_env" --argjson quota "$quota" \
    --argjson tol "$NOISY_QUOTA_TOLERANCE" --argjson cpu_max "$NOISY_CPU_SLOWDOWN_MAX" \
    --argjson io_max "$NOISY_IO_SLOWDOWN_MAX" \
    --slurpfile a_cpu <(json_or_null "$EVIDENCE_DIR/noisy-a-cpu.json") \
    --slurpfile a_io <(json_or_null "$EVIDENCE_DIR/noisy-a-io.json") \
    --slurpfile a_oom <(jq -s . "$EVIDENCE_DIR/noisy-a-oom.jsonl" 2>/dev/null || echo '[]') \
    --slurpfile teardown <(jq -R 'fromjson? // empty' "$GATEWAY_LOG" | jq -s '[.[] | select(.message == "cgroup stats at teardown") | {env_id, stats: (.stats | fromjson? // .stats)}]') '
    def median: sort | if length == 0 then null elif length % 2 == 1 then .[length / 2 | floor]
                       else (.[length / 2 - 1] + .[length / 2]) / 2 end;
    def phase(p): [.[] | select(.phase == p)];
    def ratio(a; b): if a == null or b == null or b == 0 then null else (a / b * 1000 | round) / 1000 end;
    . as $all |
    (phase("alone") | map(.cpu_ms) | median) as $cpu_alone |
    (phase("contended") | map(.cpu_ms) | median) as $cpu_cont |
    (phase("alone") | map(.write_p50_ms) | median) as $io_alone |
    (phase("contended") | map(.write_p50_ms) | median) as $io_cont |
    (phase("alone") | map(.handler_ms) | median) as $h_alone |
    (phase("contended") | map(.handler_ms) | median) as $h_cont |
    ($teardown[0] | map({key: .env_id, value: .stats}) | from_entries) as $td |
    {
      bounds: {quota_tolerance: $tol, cpu_slowdown_max: $cpu_max, io_slowdown_max: $io_max},
      tenant_a: {
        cpu: {env: $a_env, quota_cores: $quota, cgroup: $rates,
              guest: ($a_cpu[0].cpu // null | if . then {threads, elapsed_ms, iterations_per_sec, proc_stat_delta} else null end),
              teardown: ($td[$a_env] // null),
              within_quota: ($rates.max_cores_5s != null and $rates.max_cores_5s <= $quota * $tol),
              saturated: ($rates.mean_cores_after_warmup != null and $rates.mean_cores_after_warmup >= $quota * 0.8)},
        io: ($a_io[0].io // null | if . then {fill, rewrite: (.rewrite | {ops, errors, mib_per_sec, latency})} else null end),
        oom: {runs: ($a_oom[0] | length), classes: ($a_oom[0] | group_by(.class) | map({class: .[0].class, count: length})),
              host_oom_kills: ([$a_oom[0][] | .env as $e | ($td[$e]["memory.events"].oom_kill // 0)] | add // 0)}
      },
      tenant_b: {
        runs: ($all | length), succeeded: ([$all[] | select(.status == "succeeded" and .ok)] | length),
        alone: {cpu_ms_median: $cpu_alone, write_p50_ms_median: $io_alone, handler_ms_median: $h_alone},
        contended: {cpu_ms_median: $cpu_cont, write_p50_ms_median: $io_cont, handler_ms_median: $h_cont},
        after: {cpu_ms_median: (phase("after") | map(.cpu_ms) | median),
                write_p50_ms_median: (phase("after") | map(.write_p50_ms) | median),
                handler_ms_median: (phase("after") | map(.handler_ms) | median)},
        slowdown: {cpu: ratio($cpu_cont; $cpu_alone), write: ratio($io_cont; $io_alone), handler: ratio($h_cont; $h_alone)}
      }
    }' "$b" > "$out"
  detail="$(jq -r '"A cpu \(.tenant_a.cpu.cgroup.max_cores_5s) cores max/5s, \(.tenant_a.cpu.cgroup.mean_cores_after_warmup) mean (quota \(.tenant_a.cpu.quota_cores), ≤ ×\(.bounds.quota_tolerance)); A io fill \(.tenant_a.io.fill.stopped_by // "-") at \(.tenant_a.io.fill.written_mib // "-") MiB; A oom runs \(.tenant_a.oom.runs) \(.tenant_a.oom.classes | map("\(.class)=\(.count)") | join(",")), host oom_kill \(.tenant_a.oom.host_oom_kills); B \(.tenant_b.succeeded)/\(.tenant_b.runs) ok, slowdown cpu ×\(.tenant_b.slowdown.cpu) (≤ ×\(.bounds.cpu_slowdown_max)), write ×\(.tenant_b.slowdown.write) (≤ ×\(.bounds.io_slowdown_max)), handler ×\(.tenant_b.slowdown.handler)"' "$out")"
  if [ "$(jq -r '.tenant_a.cpu.within_quota' "$out")" != "true" ]; then
    ok=0
    note_finding "NOISY: tenant A used more CPU than its quota: $(jq -c '.tenant_a.cpu.cgroup' "$out")"
  fi
  if [ "$(jq -r '.tenant_a.cpu.saturated' "$out")" != "true" ]; then
    unknown=1
    note_finding "NOISY: tenant A did not use its quota (the burn did not saturate); the quota check is inconclusive"
  fi
  if [ "$(jq -r '.tenant_b.succeeded == .tenant_b.runs' "$out")" != "true" ]; then
    ok=0
    note_finding "NOISY: tenant B had failed invocations: $(jq -c '[.[] | select(.status != "succeeded" or (.ok | not)) | {phase, run, status, rc}]' -s "$b")"
  fi
  if [ "$(jq -r --argjson m "$NOISY_CPU_SLOWDOWN_MAX" '.tenant_b.slowdown.cpu != null and .tenant_b.slowdown.cpu <= $m' "$out")" != "true" ]; then
    ok=0
    note_finding "NOISY: tenant B's CPU work slowed down by ×$(jq -r '.tenant_b.slowdown.cpu' "$out") (bound ×$NOISY_CPU_SLOWDOWN_MAX)"
  fi
  if [ "$(jq -r --argjson m "$NOISY_IO_SLOWDOWN_MAX" '.tenant_b.slowdown.write != null and .tenant_b.slowdown.write <= $m' "$out")" != "true" ]; then
    ok=0
    note_finding "NOISY: tenant B's fsync'd writes slowed down by ×$(jq -r '.tenant_b.slowdown.write' "$out") (bound ×$NOISY_IO_SLOWDOWN_MAX; no io.max / drive rate limiter)"
  fi
  if [ "$(jq -r '.tenant_a.io.fill.stopped_by // "-"' "$out")" != "enospc" ]; then
    note_finding "NOISY: tenant A's disk fill did not stop with ENOSPC: $(jq -c '.tenant_a.io.fill' "$out")"
  fi
  if [ "$ok" -eq 0 ]; then NOISY_STATUS="FAIL"; elif [ "$unknown" -eq 1 ]; then NOISY_STATUS="UNKNOWN"; else NOISY_STATUS="PASS"; fi
  NOISY_DETAIL="$detail"
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
    echo "== NET egress restricted / public-web (PLT-4622) =="
    local f
    for f in publicweb restricted cross; do
      if json_file_ok "$EVIDENCE_DIR/net-checks-$f.json"; then
        echo "  [$f]"
        jq -r '.[] | "    \(if .ok then "ok " else "BAD" end) \(.kind) \(.target) expect=\(.expect) (\(.detail))"' \
          "$EVIDENCE_DIR/net-checks-$f.json"
      fi
    done
    if json_file_ok "$EVIDENCE_DIR/net-cross-b.json"; then
      jq -r '"  [cross] tenant B listening=\(.listen.listening) accepted=\(.listen.accepted)"' "$EVIDENCE_DIR/net-cross-b.json"
    fi
    if json_file_ok "$EVIDENCE_DIR/net-race.json"; then
      jq -r '.[] | "  [race] \(.env_id) \(.egress) policy \(.policy_verified_at) < start \(.instance_start_at): \(.policy_before_start)"' \
        "$EVIDENCE_DIR/net-race.json"
    fi
    [ ! -s "$EVIDENCE_DIR/net-cleanup.txt" ] || sed -n '1,3s/^/  [cleanup] /p' "$EVIDENCE_DIR/net-cleanup.txt"
    echo
    echo "== HOST cgroup placement and jail confinement (PLT-4622) =="
    if json_file_ok "$EVIDENCE_DIR/host-checks.json"; then
      jq -r '.[] | "  \(.env) observed=\(.observed) cgroup=\(.in_cgroup) limits=\(.limits_set) cpu.max=\(.cpu_max) memory.max=\(.memory_max) jailed=\(.jailed) uid=\(.uid) confined=\(.confined) seccomp=\(.seccomp)"' \
        "$EVIDENCE_DIR/host-checks.json"
    fi
    [ ! -s "$EVIDENCE_DIR/host-cleanup.txt" ] || sed 's/^/  [cleanup] /' "$EVIDENCE_DIR/host-cleanup.txt"
    echo
    echo "== NOISY two tenants on one host (PLT-4622) =="
    if json_file_ok "$EVIDENCE_DIR/noisy.json"; then
      jq -r '"  A cpu: quota \(.tenant_a.cpu.quota_cores) cores, cgroup max over 5 s \(.tenant_a.cpu.cgroup.max_cores_5s), mean after warm-up \(.tenant_a.cpu.cgroup.mean_cores_after_warmup), guest steal \(.tenant_a.cpu.guest.proc_stat_delta.steal_pct // "-")%",
             "  A io: fill \(.tenant_a.io.fill // {} | "\(.stopped_by) at \(.written_mib) MiB"), rewrites \(.tenant_a.io.rewrite.ops // "-") at \(.tenant_a.io.rewrite.mib_per_sec // "-") MiB/s",
             "  A oom: \(.tenant_a.oom.runs) runs \(.tenant_a.oom.classes | map("\(.class)=\(.count)") | join(" ")), host oom_kill \(.tenant_a.oom.host_oom_kills)",
             "  B: \(.tenant_b.succeeded)/\(.tenant_b.runs) succeeded",
             "  B median alone:     cpu \(.tenant_b.alone.cpu_ms_median) ms, write p50 \(.tenant_b.alone.write_p50_ms_median) ms, handler \(.tenant_b.alone.handler_ms_median) ms",
             "  B median contended: cpu \(.tenant_b.contended.cpu_ms_median) ms, write p50 \(.tenant_b.contended.write_p50_ms_median) ms, handler \(.tenant_b.contended.handler_ms_median) ms",
             "  B median after:     cpu \(.tenant_b.after.cpu_ms_median) ms, write p50 \(.tenant_b.after.write_p50_ms_median) ms, handler \(.tenant_b.after.handler_ms_median) ms",
             "  B slowdown: cpu ×\(.tenant_b.slowdown.cpu) (≤ ×\(.bounds.cpu_slowdown_max)), write ×\(.tenant_b.slowdown.write) (≤ ×\(.bounds.io_slowdown_max)), handler ×\(.tenant_b.slowdown.handler)"' \
        "$EVIDENCE_DIR/noisy.json"
    fi
    echo
    echo "== findings =="
    if [ -n "$FINDINGS" ]; then printf '%s' "$FINDINGS" | sed 's/^/  - /'; else echo "  none"; fi
    echo
    echo "== verdict =="
    printf '  %-5s %-8s %s\n' "M8" "$M8_STATUS" "$M8_DETAIL"
    printf '  %-5s %-8s %s\n' "M9" "$M9_STATUS" "$M9_DETAIL"
    printf '  %-5s %-8s %s\n' "DISK" "$DISK_STATUS" "$DISK_DETAIL"
    printf '  %-5s %-8s %s\n' "NET" "$NET_STATUS" "$NET_DETAIL"
    printf '  %-5s %-8s %s\n' "HOST" "$HOST_STATUS" "$HOST_DETAIL"
    printf '  %-5s %-8s %s\n' "NOISY" "$NOISY_STATUS" "$NOISY_DETAIL"
  } > "$SUMMARY_TXT"
}

print_table() {
  echo
  printf '%-5s %-8s %s\n' "CHK" "RESULT" "DETAIL"
  printf '%-5s %-8s %s\n' "---" "------" "----------------------------------------"
  printf '%-5s %-8s %s\n' "M8" "$M8_STATUS" "$M8_DETAIL"
  printf '%-5s %-8s %s\n' "M9" "$M9_STATUS" "$M9_DETAIL"
  printf '%-5s %-8s %s\n' "DISK" "$DISK_STATUS" "$DISK_DETAIL"
  printf '%-5s %-8s %s\n' "NET" "$NET_STATUS" "$NET_DETAIL"
  printf '%-5s %-8s %s\n' "HOST" "$HOST_STATUS" "$HOST_DETAIL"
  printf '%-5s %-8s %s\n' "NOISY" "$NOISY_STATUS" "$NOISY_DETAIL"
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
  if [ "$NET_MEASURE" = "1" ]; then
    step "NET: host addresses and capabilities" net_host_facts
    step "NET: tenant B function $FUNCTION_NAME" ensure_function_b
    step "NET: deploy public-web, restricted and tenant B revisions" deploy_net_revisions
    step "NET: expectations" write_net_expectations
    step "NET: public-web probe" run_net_publicweb
    step "NET: restricted probe" run_net_restricted
    step "NET: two tenants at once (A -> B)" run_net_cross_tenant
  fi
  if [ "$NOISY_MEASURE" = "1" ]; then
    step "NOISY: deploy tenant A (cpu / io / oom) and tenant B revisions" deploy_noisy_revisions
    step "NOISY: tenant B alone (${NOISY_RUNS} runs)" noisy_baseline
    step "NOISY: tenant B next to tenant A (${NOISY_RUNS} runs, then ${NOISY_AFTER_RUNS} after)" noisy_contended
  fi

  local gw_rc=0
  if [ -n "$GATEWAY_PID" ]; then
    stop_gateway 15
    gw_rc=$STOP_RC
    e2e_log "gateway exit status $gw_rc"
  fi
  stop_host_watch
  if [ "$NET_MEASURE" = "1" ]; then
    if net_cleanup_check; then state_set net.cleanup clean; else state_set net.cleanup dirty; fi
  fi
  if [ "$HOST_MEASURE" = "1" ]; then
    if host_cleanup_check; then state_set host.cleanup clean; else state_set host.cleanup dirty; fi
  fi
  orphan_note

  evaluate_m8
  evaluate_m9
  evaluate_disk
  evaluate_net
  evaluate_host
  evaluate_noisy
  write_summary_txt

  steps_write_summary "$EVIDENCE_DIR/summary.json" \
    "$(jq -n --arg run "isolation-$STAMP" --arg arch "$ARCH" --arg host "$(uname -srm)" \
        --arg config "$CONFIG_PATH" --arg function "$FUNCTION_NAME" \
        --arg baseline "$(state_get rev.baseline)" --arg alloc_rev "$(state_get rev.alloc)" \
        --arg m8 "$M8_STATUS" --arg m8_detail "$M8_DETAIL" \
        --arg m9 "$M9_STATUS" --arg m9_detail "$M9_DETAIL" \
        --arg disk "$DISK_STATUS" --arg disk_detail "$DISK_DETAIL" \
        --arg disk_rev "$(state_get rev.disk)" \
        --arg net "$NET_STATUS" --arg net_detail "$NET_DETAIL" \
        --arg host_status "$HOST_STATUS" --arg host_detail "$HOST_DETAIL" \
        --arg noisy "$NOISY_STATUS" --arg noisy_detail "$NOISY_DETAIL" \
        --arg net_publicweb_rev "$(state_get rev.publicweb)" --arg net_restricted_rev "$(state_get rev.restricted)" \
        --arg net_tenant_b_rev "$(state_get rev.b)" \
        --argjson disk_storage_mib "$DISK_STORAGE_MIB" --argjson disk_fill_mib "$DISK_FILL_MIB" \
        --arg orphans "$ORPHAN_NOTE" --arg findings "$FINDINGS" \
        --argjson memory_mib "$PROBE_MEMORY_MIB" --argjson cpu_millis "$PROBE_CPU_MILLIS" \
        --argjson alloc_memory_mib "$ALLOC_MEMORY_MIB" --argjson alloc_mib "$ALLOC_MIB" \
        '{run_id: $run, architecture: $arch, host: $host, gateway_config: $config,
          function: $function, baseline_revision: $baseline, alloc_revision: $alloc_rev,
          disk_revision: $disk_rev,
          net_revisions: {public_web: $net_publicweb_rev, restricted: $net_restricted_rev,
                          tenant_b_public_web: $net_tenant_b_rev},
          requested: {memory_mib: $memory_mib, cpu_millis: $cpu_millis,
                      alloc_memory_mib: $alloc_memory_mib, alloc_mib: $alloc_mib,
                      disk_storage_mib: $disk_storage_mib, disk_fill_mib: $disk_fill_mib},
          measurements: {M8: {status: $m8, detail: $m8_detail},
                         M9: {status: $m9, detail: $m9_detail},
                         DISK: {status: $disk, detail: $disk_detail},
                         NET: {status: $net, detail: $net_detail},
                         HOST: {status: $host_status, detail: $host_detail},
                         NOISY: {status: $noisy, detail: $noisy_detail}},
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
  if [ "$NET_STATUS" = "FAIL" ]; then
    echo "FAIL: an egress profile did not hold (see findings)" >&2
    exit 4
  fi
  if [ "$NOISY_STATUS" = "FAIL" ]; then
    echo "FAIL: the noisy-neighbour measurement did not meet its bounds (see findings)" >&2
    exit 5
  fi
  if [ "$HOST_STATUS" = "FAIL" ]; then
    echo "FAIL: a VMM was not placed or confined as configured (see findings)" >&2
    exit 6
  fi
  if [ "$failed" -ne 0 ] || [ "$M8_STATUS" = "UNKNOWN" ] || [ "$DISK_STATUS" = "UNKNOWN" ] \
    || [ "$NET_STATUS" = "UNKNOWN" ] || [ "$HOST_STATUS" = "UNKNOWN" ] || [ "$NOISY_STATUS" = "UNKNOWN" ]; then
    echo "INCOMPLETE: the measurement could not be taken ($failed step(s) failed)" >&2
    exit 2
  fi
  echo "M8 PASS, DISK PASS, NET $NET_STATUS, HOST $HOST_STATUS, NOISY $NOISY_STATUS (M9 $M9_STATUS; M9 findings are reported, not fatal)"
  exit 0
}

main "$@"

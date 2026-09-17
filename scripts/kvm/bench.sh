#!/usr/bin/env bash
# scripts/kvm/bench.sh - first response, concurrency and resource cost of the Rust samples on
# Firecracker (PLT-4647). Runs on a Linux/KVM host with passwordless sudo; see docs/benchmark.md.
#
# For each sample (examples/hello, examples/http-axum, examples/cpu-burn with a short burn):
#   a) fresh host   BENCH_FRESH_TRIALS times: stop the gateway, delete its data_dir and workdir,
#                   drop the page cache, start the gateway, create + deploy the function, invoke
#                   once. That first invoke is the "image cache miss" row (nothing of this
#                   function, the kernel, the rootfs, the VMM binaries or the gateway is in the
#                   page cache; the artifact was just uploaded).
#   b) cold         BENCH_COLD_N sequential invokes on the same gateway with the pool off
#                   ("cache hit": the page cache holds kernel / rootfs / artifact; the function
#                   drive and the scratch drive are still built for every environment - there is
#                   no drive cache in this implementation).
#   c) warm         restart the gateway with [pool] enabled (profile stays production), one priming
#                   invoke, then BENCH_WARM_N sequential invokes BENCH_WARM_GAP_MS apart.
#   d) sweep        BENCH_SWEEP_LEVELS concurrent clients (default 1 2 4 8), BENCH_SWEEP_REQUESTS
#                   requests in total per level, once with the pool off and once with it on.
#   e) resources    with environments paused in the pool: VMM RSS, cgroup memory.current / peak,
#                   CPU ticks and cgroup CPU over BENCH_IDLE_SECONDS of idleness, host disk bytes
#                   per environment, gateway RSS with and without pooled environments; plus the
#                   cgroup teardown stats the provider logs for every environment.
# Every request is one JSON line in attempts.jsonl, failures included. scripts/kvm/bench-report.sh
# computes the tables (summary.json, summary.md) from the raw files and can be rerun offline.
#
# Environment (all optional):
#   BENCH_SAMPLES            samples to run (default "hello http-axum cpu-burn")
#   BENCH_FRESH_TRIALS       fresh-host trials per sample (default 5)
#   BENCH_COLD_N / BENCH_WARM_N   sequential invokes (default 20 / 20)
#   BENCH_WARM_GAP_MS        pause between warm invokes so the previous environment is back in the
#                            pool (default 250)
#   BENCH_SWEEP_LEVELS       concurrent clients (default "1 2 4 8")
#   BENCH_SWEEP_REQUESTS     requests per level, split across the clients (default 24)
#   BENCH_SWEEP_MODES        "cold warm" (default)
#   BENCH_IDLE_SECONDS       idle window for the paused-environment cost (default 60)
#   BENCH_MEMORY_MIB / BENCH_CPU_MILLIS / BENCH_TIMEOUT_SECONDS   revision resources (256 / 500 / 30)
#   BENCH_REVISION_MAX_CONCURRENCY   revision max_concurrency (default: the product default, 4)
#   BENCH_CPU_BURN_SECONDS   cpu-burn payload seconds (default 0.2)
#   BENCH_REQUEST_TIMEOUT_S  client timeout per request (default 90)
#   BENCH_MAX_DISK_PCT       refuse to start (and stop between samples) above this root fs use (60)
#   BENCH_COMMIT             commit to record when the tree is not a git checkout
#   BENCH_DIRTY              whether that tree had uncommitted changes (recorded as given)
#   BENCH_HOST_NOTE          free text describing the physical host (a VM cannot see it)
#   BENCH_NESTED             true / false, overrides the nested virtualization detection
#   BENCH_MAX_CALIBRATION_MS refuse to start when the fixed CPU loop (calibration.jsonl) is slower
#                            than this; unset = record only
#   TSLS_SKIP_BUILD=1        do not run cargo build
#   TSLS_GATEWAY_CONFIG      base config (default config/gateway.firecracker.toml, must require the
#                            jailer and the host cgroup)
#   EVIDENCE_ROOT            default docs/evidence
#
# Exit: 0 measured (failed requests are data, not an exit status), 2 the benchmark could not run
# (preflight, build, gateway, deploy), 3 something outlived the run (cleanup.txt).
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

require_tools curl jq cargo sudo pgrep || e2e_die "missing tools"

TSLS_SKIP_BUILD="${TSLS_SKIP_BUILD:-0}"
BASE_CONFIG="${TSLS_GATEWAY_CONFIG:-$REPO_ROOT/config/gateway.firecracker.toml}"
API_URL="${TSLS_API_URL:-http://127.0.0.1:8080}"
TOKEN="${TSLS_TOKEN:-dev-token-tenant-a}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$REPO_ROOT/docs/evidence}"
SAMPLES="${BENCH_SAMPLES:-hello http-axum cpu-burn}"
FRESH_TRIALS="${BENCH_FRESH_TRIALS:-5}"
COLD_N="${BENCH_COLD_N:-20}"
WARM_N="${BENCH_WARM_N:-20}"
WARM_GAP_MS="${BENCH_WARM_GAP_MS:-250}"
SWEEP_LEVELS="${BENCH_SWEEP_LEVELS:-1 2 4 8}"
SWEEP_REQUESTS="${BENCH_SWEEP_REQUESTS:-24}"
SWEEP_MODES="${BENCH_SWEEP_MODES:-cold warm}"
IDLE_SECONDS="${BENCH_IDLE_SECONDS:-60}"
MEMORY_MIB="${BENCH_MEMORY_MIB:-256}"
CPU_MILLIS="${BENCH_CPU_MILLIS:-500}"
TIMEOUT_SECONDS="${BENCH_TIMEOUT_SECONDS:-30}"
REV_MAX_CONCURRENCY="${BENCH_REVISION_MAX_CONCURRENCY:-}"
CPU_BURN_SECONDS="${BENCH_CPU_BURN_SECONDS:-0.2}"
REQUEST_TIMEOUT_S="${BENCH_REQUEST_TIMEOUT_S:-90}"
MAX_DISK_PCT="${BENCH_MAX_DISK_PCT:-60}"
CGROUP_PARENT="${CGROUP_PARENT:-/sys/fs/cgroup/tachyon}"
JAIL_BASE="${JAIL_CHROOT_BASE:-/srv/jailer}"
SUDO="sudo -n"
[ "$(id -u)" -ne 0 ] || SUDO=""

[ -f "$BASE_CONFIG" ] || e2e_die "gateway config not found: $BASE_CONFIG"
if [ "$COLD_N" -lt 1 ] || [ "$WARM_N" -lt 1 ] || [ "$FRESH_TRIALS" -lt 1 ]; then e2e_die "trial counts must be >= 1"; fi

case "$(uname -m)" in
  x86_64|amd64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *) e2e_die "unsupported host architecture $(uname -m)" ;;
esac

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$EVIDENCE_ROOT/bench-$STAMP"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tsls-bench.XXXXXX")"
export E2E_STATE_DIR="$WORK_DIR/state"
export STEP_LOG_DIR="$EVIDENCE_DIR/steps"
mkdir -p "$EVIDENCE_DIR" "$STEP_LOG_DIR" "$E2E_STATE_DIR" "$EVIDENCE_DIR/gateway-logs"
ATTEMPTS="$EVIDENCE_DIR/attempts.jsonl"
INVOCATIONS="$EVIDENCE_DIR/invocations.jsonl"
GATEWAY_RUNS="$EVIDENCE_DIR/gateway-runs.jsonl"
DEPLOYS="$EVIDENCE_DIR/deploys.jsonl"
SWEEPS="$EVIDENCE_DIR/sweeps.jsonl"
RESOURCES="$EVIDENCE_DIR/resources.jsonl"
: > "$ATTEMPTS"; : > "$INVOCATIONS"; : > "$GATEWAY_RUNS"; : > "$DEPLOYS"; : > "$SWEEPS"; : > "$RESOURCES"
CALIBRATION="$EVIDENCE_DIR/calibration.jsonl"
: > "$CALIBRATION"

BUILD_PROFILE="release"
TSLS_BIN="${TSLS_BIN:-$REPO_ROOT/target/$BUILD_PROFILE/tsls}"
GATEWAY_BIN="${TSLS_GATEWAY_BIN:-$REPO_ROOT/target/$BUILD_PROFILE/tachyon-serverless-gateway}"
GUEST_DIR="${TSLS_GUEST_DIR:-$REPO_ROOT/target/$ARCH-unknown-linux-musl/release}"
DATA_DIR="$REPO_ROOT/data/bench"
FC_RUN_DIR="$(sed -n 's/^workdir *= *"\([^"]*\)".*/\1/p' "$BASE_CONFIG" | head -n1)"
FC_RUN_DIR="${FC_RUN_DIR:-.kvm/run}"
case "$FC_RUN_DIR" in /*) ;; *) FC_RUN_DIR="$REPO_ROOT/${FC_RUN_DIR#./}" ;; esac
COLD_CONFIG="$EVIDENCE_DIR/gateway-cold.toml"
WARM_CONFIG="$EVIDENCE_DIR/gateway-warm.toml"

tsls() { "$TSLS_BIN" "$@"; }
export TSLS_BIN TSLS_API_URL="$API_URL" TSLS_TOKEN="$TOKEN"

GATEWAY_PID=""
GATEWAY_REAL_PID=""
GATEWAY_LOG=""
GATEWAY_SEQ=0

gateway_alive() { [ -n "$GATEWAY_PID" ] && $SUDO kill -0 "$GATEWAY_PID" 2>/dev/null; }

stop_gateway() { # stop_gateway GRACE_SECS
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
  wait "$GATEWAY_PID" 2>/dev/null || true
  GATEWAY_PID=""
  GATEWAY_REAL_PID=""
  $SUDO chown -R "$(id -u):$(id -g)" "$FC_RUN_DIR" "$DATA_DIR" "$EVIDENCE_DIR" 2>/dev/null || true
}

cleanup() {
  local rc=$?
  set +e
  if gateway_alive; then
    e2e_log "cleanup: stopping gateway $GATEWAY_PID"
    stop_gateway 20
  fi
  rm -rf "$WORK_DIR"
  exit "$rc"
}
trap cleanup EXIT INT TERM

# calibrate PHASE: wall time of a fixed single-threaded CPU loop, three times. A VM cannot see the
# load of the machine it runs on (Apple's hypervisor reports no steal time), so this is the in-guest
# signal that the physical host was busy: compare the rows of one run, and of runs on one host.
calibrate() {
  local phase="$1" i t0 runs="" worst
  for i in 1 2 3; do
    t0="$(now_ms)"
    awk 'BEGIN { s = 0; for (i = 0; i < 3000000; i++) s += i }'
    runs="$runs $(( $(now_ms) - t0 ))"
  done
  jq -nc --arg phase "$phase" --arg runs "$runs" --arg load "$(cut -d' ' -f1-3 /proc/loadavg)" \
    '{phase: $phase, awk_loop_ms: ($runs | split(" ") | map(select(length > 0) | tonumber)), vm_loadavg: $load}' >> "$CALIBRATION"
  worst="$(printf '%s' "$runs" | tr ' ' '\n' | sort -n | tail -n1)"
  e2e_log "calibration ($phase): awk loop ms =$runs"
  if [ -n "${BENCH_MAX_CALIBRATION_MS:-}" ] && [ "$worst" -gt "$BENCH_MAX_CALIBRATION_MS" ]; then
    e2e_warn "calibration $worst ms > BENCH_MAX_CALIBRATION_MS=$BENCH_MAX_CALIBRATION_MS: the host looks busy"
    return 1
  fi
}

root_fs_pct() { df -P "$REPO_ROOT" | awk 'NR == 2 { gsub("%", "", $5); print $5 }'; }

# ---------------------------------------------------------------------------
# preflight, build, configs, metadata
# ---------------------------------------------------------------------------

preflight() {
  $SUDO true || { echo "passwordless sudo is required (jailer + host cgroup need a root gateway)" >&2; return 1; }
  if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then echo "/dev/kvm is not rw" >&2; return 1; fi
  grep -qE '^[[:space:]]*mode[[:space:]]*=[[:space:]]*"required"' "$BASE_CONFIG" ||
    { echo "$BASE_CONFIG does not require the host cgroup" >&2; return 1; }
  awk '/^\[provider.firecracker.jailer\]/ { j = 1; next } /^\[/ { j = 0 } j && /^[[:space:]]*enabled[[:space:]]*=[[:space:]]*true/ { ok = 1 } END { exit !ok }' "$BASE_CONFIG" ||
    { echo "$BASE_CONFIG does not enable the jailer" >&2; return 1; }
  if grep -qE '^[[:space:]]*\[pool\]' "$BASE_CONFIG"; then
    echo "$BASE_CONFIG already has a [pool] section; the benchmark writes its own" >&2
    return 1
  fi
  local others
  others="$(pgrep -a -f '(^|/)(firecracker|jailer|tachyon-serverless-gateway)( |$)' 2>/dev/null | grep -v -e pgrep -e bench.sh || true)"
  if [ -n "$others" ]; then
    echo "other gateway / firecracker / jailer processes are running; refusing to share the host:" >&2
    echo "$others" >&2
    return 1
  fi
  if curl -s -o /dev/null -m 2 "$API_URL/healthz"; then
    echo "something already answers on $API_URL" >&2
    return 1
  fi
  local pct
  pct="$(root_fs_pct)"
  e2e_log "file system use ${pct}% (limit ${MAX_DISK_PCT}%)"
  [ "$pct" -lt "$MAX_DISK_PCT" ] || { echo "file system use ${pct}% >= ${MAX_DISK_PCT}%" >&2; return 1; }
}

build_all() {
  if [ "$TSLS_SKIP_BUILD" = "1" ]; then
    e2e_log "TSLS_SKIP_BUILD=1: skipping cargo build"
  else
    cargo build --release -p tachyon-serverless-gateway -p tachyon-serverless-cli
    cargo build --release --target "$ARCH-unknown-linux-musl" \
      -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn
  fi
  local b
  for b in "$TSLS_BIN" "$GATEWAY_BIN" "$GUEST_DIR/example-hello" "$GUEST_DIR/example-http-axum" "$GUEST_DIR/example-cpu-burn"; do
    [ -x "$b" ] || { echo "missing binary: $b (run scripts/kvm/bootstrap.sh first)" >&2; return 1; }
  done
  check_rootfs_bridge
}

# Same check as scripts/kvm/measure-warm.sh: a rootfs built before a protocol change fails every
# boot, which in a benchmark reads as a 100% failure rate of the provider.
check_rootfs_bridge() {
  local rootfs bridge dumped debugfs_bin
  rootfs="$(sed -n 's/^[[:space:]]*rootfs[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$BASE_CONFIG" | head -n1)"
  case "$rootfs" in /*) ;; *) rootfs="$REPO_ROOT/${rootfs#./}" ;; esac
  bridge="$GUEST_DIR/tachyon-serverless-runtime-bridge"
  debugfs_bin="$(command -v debugfs || echo /usr/sbin/debugfs)"
  [ -x "$debugfs_bin" ] || { echo "debugfs (e2fsprogs) is required to check the rootfs bridge" >&2; return 1; }
  dumped="$WORK_DIR/rootfs-tachyon-init"
  "$debugfs_bin" -R "dump /sbin/tachyon-init $dumped" "$rootfs" >/dev/null 2>&1
  if [ "$(sha256sum "$dumped" | awk '{print $1}')" != "$(sha256sum "$bridge" | awk '{print $1}')" ]; then
    echo "the bridge inside $rootfs is not the one just built: run scripts/kvm/build-rootfs.sh" >&2
    return 1
  fi
  e2e_log "guest bridge in $(basename "$rootfs") matches the build"
}

write_configs() {
  # Only data_dir changes; the provider section (jailer, cgroup required, workdir) is the base.
  sed -e "s|^data_dir *=.*|data_dir = \"$DATA_DIR\"   # scripts/kvm/bench.sh|" "$BASE_CONFIG" > "$COLD_CONFIG"
  cp "$COLD_CONFIG" "$WARM_CONFIG"
  cat >> "$WARM_CONFIG" <<EOF

# Added by scripts/kvm/bench.sh (PLT-4647). profile stays production: the Firecracker provider
# reports idle_quiesce / idle_resume as supported, so no measurement switch is needed.
[pool]
enabled = true
max_idle_per_revision = 8
idle_ttl_seconds = 900
max_total_idle = 24
EOF
}

write_metadata() {
  local commit dirty
  commit="${BENCH_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)}"
  dirty="${BENCH_DIRTY:-unknown}"
  GITHUB_SHA="$commit" "$REPO_ROOT/scripts/ci/kvm-profile.sh" "$EVIDENCE_DIR/profile" >/dev/null
  local lscpu_json virt nested
  lscpu_json="$(lscpu -J 2>/dev/null || echo 'null')"
  # kvm-profile.sh looks for the x86 `hypervisor` cpu flag, which aarch64 guests do not show.
  # systemd-detect-virt names the hypervisor this host runs under ("none" on bare metal).
  virt="$(systemd-detect-virt 2>/dev/null || true)"
  nested="${BENCH_NESTED:-}"
  if [ -z "$nested" ]; then
    case "$virt" in
      none) nested=false ;;
      "") nested="$(jq -r '.host.nested_virtualization' "$EVIDENCE_DIR/profile/profile.json")" ;;
      *) nested=true ;;
    esac
  fi
  jq -n \
    --arg stamp "$STAMP" --arg commit "$commit" --arg dirty "$dirty" \
    --argjson profile "$(cat "$EVIDENCE_DIR/profile/profile.json")" \
    --argjson lscpu "$lscpu_json" --arg virt "$virt" --arg nested "$nested" \
    --arg host_note "${BENCH_HOST_NOTE:-}" \
    --arg cpu_vendor "$(lscpu 2>/dev/null | awk -F: '/^Vendor ID/ { gsub(/^ +/, "", $2); print $2 }')" \
    --arg jailer "$("$REPO_ROOT/.kvm/bin/jailer" --version 2>/dev/null | head -n1)" \
    --arg cold_sha "$(sha256sum "$COLD_CONFIG" | awk '{print $1}')" \
    --arg warm_sha "$(sha256sum "$WARM_CONFIG" | awk '{print $1}')" \
    --arg base "$BASE_CONFIG" \
    --arg bridge_sha "$(sha256sum "$GUEST_DIR/tachyon-serverless-runtime-bridge" | awk '{print $1}')" \
    --argjson bridge_bytes "$(stat -c %s "$GUEST_DIR/tachyon-serverless-runtime-bridge")" \
    --argjson rootfs_bytes "$(stat -c %s "$REPO_ROOT/.kvm/rootfs.ext4")" \
    --argjson kernel_bytes "$(stat -c %s "$REPO_ROOT/.kvm/vmlinux")" \
    --arg hello_sha "$(sha256sum "$GUEST_DIR/example-hello" | awk '{print $1}')" \
    --argjson hello_bytes "$(stat -c %s "$GUEST_DIR/example-hello")" \
    --arg axum_sha "$(sha256sum "$GUEST_DIR/example-http-axum" | awk '{print $1}')" \
    --argjson axum_bytes "$(stat -c %s "$GUEST_DIR/example-http-axum")" \
    --arg burn_sha "$(sha256sum "$GUEST_DIR/example-cpu-burn" | awk '{print $1}')" \
    --argjson burn_bytes "$(stat -c %s "$GUEST_DIR/example-cpu-burn")" \
    --arg samples "$SAMPLES" --argjson fresh "$FRESH_TRIALS" --argjson cold "$COLD_N" --argjson warm "$WARM_N" \
    --argjson gap "$WARM_GAP_MS" --arg levels "$SWEEP_LEVELS" --argjson sweep_req "$SWEEP_REQUESTS" \
    --arg modes "$SWEEP_MODES" --argjson idle "$IDLE_SECONDS" \
    --argjson mem "$MEMORY_MIB" --argjson cpu "$CPU_MILLIS" --argjson timeout "$TIMEOUT_SECONDS" \
    --arg rev_conc "$REV_MAX_CONCURRENCY" --arg burn "$CPU_BURN_SECONDS" \
    --argjson req_timeout "$REQUEST_TIMEOUT_S" \
    --arg gateway_build "$BUILD_PROFILE" --arg rustc "$(rustc --version 2>/dev/null || true)" \
    --arg sudo_user "$(id -un)" '
    {schema: "tachyon-serverless/bench/v1", run: ("bench-" + $stamp), commit: $commit, worktree_dirty: $dirty,
     host: ($profile.host + {lscpu: $lscpu, cpu_vendor: $cpu_vendor, virtualization: $virt,
             nested_virtualization: $nested,
             nested_detection: "BENCH_NESTED, else systemd-detect-virt != none, else the x86 hypervisor cpu flag",
             physical_host: (if $host_note == "" then null else $host_note end)}),
     firecracker: $profile.firecracker, jailer_version: $jailer,
     guest_kernel: ($profile.guest_kernel + {bytes: $kernel_bytes}),
     rootfs: ($profile.rootfs + {bytes: $rootfs_bytes}),
     runtime_bridge: {sha256: $bridge_sha, bytes: $bridge_bytes},
     artifacts: {"hello": {sha256: $hello_sha, bytes: $hello_bytes},
                 "http-axum": {sha256: $axum_sha, bytes: $axum_bytes},
                 "cpu-burn": {sha256: $burn_sha, bytes: $burn_bytes}},
     gateway: {build_profile: $gateway_build, rustc: $rustc, base_config: $base,
               cold_config: {file: "gateway-cold.toml", sha256: $cold_sha},
               warm_config: {file: "gateway-warm.toml", sha256: $warm_sha},
               runs_as: "root via sudo (jailer + host cgroup required)", invoked_by: $sudo_user},
     profile: {isolation: "firecracker microVM, jailer enabled, host cgroup v2 mode = required, egress none",
               memory_mib: $mem, cpu_millis: $cpu, timeout_seconds: $timeout,
               revision_max_concurrency: (if $rev_conc == "" then "product default (4)" else ($rev_conc | tonumber) end)},
     load: {samples: ($samples | split(" ") | map(select(length > 0))),
            fresh_host_trials_per_sample: $fresh, cold_n: $cold, warm_n: $warm, warm_gap_ms: $gap,
            sweep_levels: ($levels | split(" ") | map(select(length > 0) | tonumber)),
            sweep_requests_per_level: $sweep_req, sweep_modes: ($modes | split(" ") | map(select(length > 0))),
            idle_seconds: $idle, request_timeout_s: $req_timeout,
            payloads: {"hello": "POST :invoke {\"name\":\"bench\"}",
                       "http-axum": "GET /http/ (router answers ok)",
                       "cpu-burn": ("POST :invoke {\"seconds\":" + $burn + "}")},
            seeds: "none: payloads are fixed and requests are issued in a fixed order; nothing is randomised",
            client: "curl on the same host over loopback, one request per process, no keep-alive"}}' \
    > "$EVIDENCE_DIR/metadata.json"
  cp "$REPO_ROOT/.kvm/manifest.json" "$EVIDENCE_DIR/kvm-manifest.json" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# gateway lifecycle
# ---------------------------------------------------------------------------

# start_gateway CONFIG KIND LABEL -> appends one row to gateway-runs.jsonl
start_gateway() {
  local config="$1" kind="$2" label="$3" started ready i
  GATEWAY_SEQ=$(( GATEWAY_SEQ + 1 ))
  GATEWAY_LOG="$EVIDENCE_DIR/gateway-logs/$(printf '%02d' "$GATEWAY_SEQ")-$label.log"
  started="$(now_ms)"
  $SUDO env LOG_FORMAT=json TACHYON_GATEWAY_CONFIG="$config" \
    "$GATEWAY_BIN" --config "$config" >"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  for i in $(seq 1 240); do
    if ! gateway_alive; then
      echo "gateway exited during startup" >&2
      tail -n 30 "$GATEWAY_LOG" >&2
      return 1
    fi
    [ "$(curl -s -o /dev/null -w '%{http_code}' -m 2 "$API_URL/readyz" 2>/dev/null || true)" = "200" ] && break
    sleep 0.25
  done
  ready="$(now_ms)"
  [ "$(curl -s -o /dev/null -w '%{http_code}' -m 2 "$API_URL/readyz")" = "200" ] ||
    { echo "gateway not ready after 60 s" >&2; tail -n 30 "$GATEWAY_LOG" >&2; return 1; }
  GATEWAY_REAL_PID="$(pgrep -f "^$GATEWAY_BIN --config $config" | head -n1 || true)"
  jq -nc --argjson seq "$GATEWAY_SEQ" --arg kind "$kind" --arg label "$label" \
    --arg log "gateway-logs/$(basename "$GATEWAY_LOG")" --argjson started "$started" \
    --argjson ready_ms "$(( ready - started ))" --arg pid "${GATEWAY_REAL_PID:-}" \
    '{seq: $seq, kind: $kind, label: $label, log: $log, started_ms: $started, start_to_ready_ms: $ready_ms,
      pid: (if $pid == "" then null else ($pid | tonumber) end)}' >> "$GATEWAY_RUNS"
  e2e_log "gateway $kind ($label) ready in $(( ready - started )) ms, pid ${GATEWAY_REAL_PID:-?}"
}

# The host after a reboot as far as this service is concerned: no ledger, no artifacts, no
# environments, and nothing of the kernel / rootfs / binaries in the page cache.
fresh_host_reset() {
  $SUDO rm -rf "$DATA_DIR" "$FC_RUN_DIR"
  mkdir -p "$DATA_DIR"
  sync
  $SUDO sh -c 'echo 3 > /proc/sys/vm/drop_caches'
}

# ---------------------------------------------------------------------------
# functions and requests
# ---------------------------------------------------------------------------

bin_of() {
  case "$1" in
    hello) echo "$GUEST_DIR/example-hello" ;;
    http-axum) echo "$GUEST_DIR/example-http-axum" ;;
    cpu-burn) echo "$GUEST_DIR/example-cpu-burn" ;;
    *) return 1 ;;
  esac
}

# deploy_sample SAMPLE TRIAL -> state fn.SAMPLE / rev.SAMPLE, one row in deploys.jsonl
deploy_sample() {
  local sample="$1" trial="$2" name t0 t1 t2 fn rev extra=()
  name="bench-$sample"
  [ -z "$REV_MAX_CONCURRENCY" ] || extra=(--max-concurrency "$REV_MAX_CONCURRENCY")
  t0="$(now_ms)"
  fn="$(tsls functions create --name "$name" --description "PLT-4647 benchmark" --json | jq -r .id)"
  t1="$(now_ms)"
  rev="$(tsls functions deploy --function "$name" --binary "$(bin_of "$sample")" --arch "$ARCH" \
    --memory-mib "$MEMORY_MIB" --cpu-millis "$CPU_MILLIS" --timeout-seconds "$TIMEOUT_SECONDS" \
    "${extra[@]+"${extra[@]}"}" --description "bench" --json | jq -r .id)"
  t2="$(now_ms)"
  if [ -z "$fn" ] || [ "$fn" = null ] || [ -z "$rev" ] || [ "$rev" = null ]; then
    echo "create / deploy of $sample failed" >&2
    return 1
  fi
  state_set "fn.$sample" "$fn"
  state_set "rev.$sample" "$rev"
  tsls functions revision "$name" "$rev" --json > "$EVIDENCE_DIR/revision-$sample.json"
  jq -nc --arg sample "$sample" --argjson trial "$trial" --arg fn "$fn" --arg rev "$rev" \
    --argjson create_ms "$(( t1 - t0 ))" --argjson deploy_ms "$(( t2 - t1 ))" \
    '{sample: $sample, trial: $trial, function_id: $fn, revision_id: $rev,
      create_client_ms: $create_ms, deploy_client_ms: $deploy_ms,
      note: "deploy = CLI process + artifact upload + revision ready; measured with the tsls CLI"}' >> "$DEPLOYS"
}

# request SAMPLE FN REV OUT_FILE SCENARIO TRIAL CONCURRENCY CLIENT SEQ
# One HTTP request with curl, one JSON line appended to OUT_FILE. Never fails.
request() {
  local sample="$1" fn="$2" rev="$3" out="$4" scenario="$5" trial="$6" conc="$7" client="$8" seq="$9"
  local body hdr metrics rc started data inv code ecode etype
  body="$(mktemp "$WORK_DIR/body.XXXXXX")"
  hdr="$(mktemp "$WORK_DIR/hdr.XXXXXX")"
  started="$(now_ms)"
  set +e
  case "$sample" in
    http-axum)
      metrics="$(curl -sS -o "$body" -D "$hdr" --max-time "$REQUEST_TIMEOUT_S" \
        -w '%{http_code} %{time_total} %{time_starttransfer} %{size_upload} %{size_download} %{size_header} %{size_request}' \
        -H "authorization: Bearer $TOKEN" -H "x-tachyon-revision-id: $rev" \
        "$API_URL/v1/functions/$fn/http/" 2>"$body.err")"
      ;;
    *)
      if [ "$sample" = "cpu-burn" ]; then data="{\"seconds\":$CPU_BURN_SECONDS}"; else data='{"name":"bench"}'; fi
      metrics="$(curl -sS -o "$body" -D "$hdr" --max-time "$REQUEST_TIMEOUT_S" \
        -w '%{http_code} %{time_total} %{time_starttransfer} %{size_upload} %{size_download} %{size_header} %{size_request}' \
        -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' --data "$data" \
        "$API_URL/v1/functions/$fn/invoke?revision_id=$rev" 2>"$body.err")"
      ;;
  esac
  rc=$?
  set -e
  inv="$(tr -d '\r' < "$hdr" | awk 'tolower($1) == "x-tachyon-invocation-id:" { print $2 }' | tail -n1)"
  code="$(printf '%s' "$metrics" | awk '{ print $1 }')"
  ecode=""; etype=""
  if [ "${code:-000}" != "200" ]; then
    ecode="$(jq -r '.error.code // empty' "$body" 2>/dev/null || true)"
    etype="$(jq -r '.error.error_type // empty' "$body" 2>/dev/null || true)"
    [ -n "$inv" ] || inv="$(jq -r '.error.invocation_id // empty' "$body" 2>/dev/null || true)"
  fi
  jq -nc --arg sample "$sample" --arg scenario "$scenario" --argjson trial "$trial" \
    --argjson conc "$conc" --argjson client "$client" --argjson seq "$seq" --argjson started "$started" \
    --arg fn "$fn" --arg rev "$rev" --arg metrics "${metrics:-}" --argjson rc "$rc" \
    --arg inv "$inv" --arg ecode "$ecode" --arg etype "$etype" --arg cerr "$(head -c 300 "$body.err")" '
    ($metrics | split(" ")) as $m |
    {sample: $sample, scenario: $scenario, trial: $trial, concurrency: $conc, client: $client, seq: $seq,
     started_ms: $started, function_id: $fn, revision_id: $rev, curl_exit: $rc,
     curl_error: (if $cerr == "" then null else $cerr end),
     http_code: (($m[0] // "0") | tonumber),
     client_ms: (if ($m | length) > 1 then (($m[1] | tonumber) * 1000 | round) else null end),
     client_ttfb_ms: (if ($m | length) > 2 then (($m[2] | tonumber) * 1000 | round) else null end),
     bytes_up_body: (($m[3] // "0") | tonumber), bytes_down_body: (($m[4] // "0") | tonumber),
     bytes_down_headers: (($m[5] // "0") | tonumber), bytes_up_request_headers: (($m[6] // "0") | tonumber),
     invocation_id: (if $inv == "" then null else $inv end),
     error_code: (if $ecode == "" then null else $ecode end),
     error_type: (if $etype == "" then null else $etype end)}' >> "$out"
  rm -f "$body" "$body.err" "$hdr"
}

# enrich RAW_FILE: fetch every invocation of RAW_FILE, append it to invocations.jsonl and the
# merged row to attempts.jsonl. Done after the timed part so the fetch never overlaps a request.
enrich() {
  local raw="$1" row inv detail
  while IFS= read -r row; do
    inv="$(printf '%s' "$row" | jq -r '.invocation_id // empty')"
    detail='null'
    if [ -n "$inv" ]; then
      detail="$(curl -sS -m 10 -H "authorization: Bearer $TOKEN" "$API_URL/v1/invocations/$inv" 2>/dev/null || true)"
      if printf '%s' "$detail" | jq -e .id >/dev/null 2>&1; then
        printf '%s\n' "$detail" | jq -c . >> "$INVOCATIONS"
      else
        detail='null'
      fi
    fi
    printf '%s' "$row" | jq -c --argjson d "$detail" '
      . + (if $d == null then {status: null, attempts: 0, start_kind: null, first_start_kind: null, timings: null, env: null}
           else
             ($d.attempts // []) as $as | ($as[-1] // {}) as $a | ($a.boot_evidence.details // {}) as $x |
             {status: $d.status, invocation_error: ($d.error // null), attempts: ($as | length),
              start_kind: ($a.start_kind // null), first_start_kind: ($as[0].start_kind // null),
              environment_id: ($a.environment_id // null), epoch: ($a.epoch // null),
              timings: ($a.timings // null),
              attempt_timings: [$as[] | {start_kind, status, timings}],
              env: {host_pid: ($a.boot_evidence.host_pid // null), provider_boot_ms: ($x.boot_ms // null),
                    function_drive_bytes: ($x.function_drive_bytes // null),
                    scratch_drive_bytes: ($x.scratch_drive_bytes // null),
                    scratch_drive_ms: ($x.scratch_drive_ms // null),
                    host_disk_budget_bytes: ($x.host_disk_budget_bytes // null),
                    vcpus: ($x.vcpus // null), mem_mib: ($x.mem_mib // null),
                    cgroup_memory_max_bytes: ($x.cgroup_memory_max_bytes // null),
                    cgroup_cpu_max: ($x.cgroup_cpu_max // null), jailed: ($x.jailed // null),
                    network_interfaces: ($x.network_interfaces // null)}}
           end)' >> "$ATTEMPTS"
  done < "$raw"
}

sequential() { # sequential SAMPLE SCENARIO N GAP_MS
  local sample="$1" scenario="$2" n="$3" gap="$4" fn rev raw i
  fn="$(state_get "fn.$sample")"; rev="$(state_get "rev.$sample")"
  raw="$WORK_DIR/raw-$sample-$scenario.jsonl"
  : > "$raw"
  for i in $(seq 1 "$n"); do
    request "$sample" "$fn" "$rev" "$raw" "$scenario" 0 1 1 "$i"
    [ "$gap" -eq 0 ] || sleep "$(awk -v g="$gap" 'BEGIN { printf "%.3f", g / 1000 }')"
  done
  enrich "$raw"
  jq -r --arg s "$sample" --arg sc "$scenario" 'select(.sample == $s and .scenario == $sc) |
    "  \(.seq) http=\(.http_code) \(.start_kind // "-") client=\(.client_ms) total=\(.timings.total_ms // "-") boot=\(.timings.environment_boot_ms // "-") handler=\(.timings.handler_ms // "-")"' "$ATTEMPTS" | tail -n "$n"
}

# sweep SAMPLE MODE: for every level, LEVEL clients share SWEEP_REQUESTS requests.
sweep() {
  local sample="$1" mode="$2" fn rev level per extra c raw t0 t1 pids
  fn="$(state_get "fn.$sample")"; rev="$(state_get "rev.$sample")"
  for level in $SWEEP_LEVELS; do
    raw="$WORK_DIR/raw-$sample-sweep-$mode-$level.jsonl"
    : > "$raw"
    per=$(( SWEEP_REQUESTS / level ))
    extra=$(( SWEEP_REQUESTS - per * level ))
    pids=()
    t0="$(now_ms)"
    for c in $(seq 1 "$level"); do
      (
        local_n=$per
        [ "$c" -gt "$extra" ] || local_n=$(( per + 1 ))
        for j in $(seq 1 "$local_n"); do
          request "$sample" "$fn" "$rev" "$raw.$c" "sweep-$mode" 0 "$level" "$c" "$j"
        done
      ) &
      pids+=("$!")
    done
    for c in "${pids[@]}"; do wait "$c" || true; done
    t1="$(now_ms)"
    cat "$raw".* > "$raw" 2>/dev/null || true
    rm -f "$raw".*
    enrich "$raw"
    jq -nc --arg sample "$sample" --arg mode "$mode" --argjson level "$level" \
      --argjson requests "$SWEEP_REQUESTS" --argjson wall "$(( t1 - t0 ))" \
      '{sample: $sample, mode: $mode, concurrency: $level, requests: $requests, wall_ms: $wall}' >> "$SWEEPS"
    e2e_log "sweep $sample $mode c=$level: $(wc -l < "$raw") requests in $(( t1 - t0 )) ms"
    # Let quiesces and teardowns of the level finish before the next one starts.
    sleep 3
  done
}

# sample_resources SAMPLE PHASE -> one line in resources.jsonl
sample_resources() {
  local sample="$1" phase="$2" snap
  snap="$($SUDO "$SCRIPT_DIR/bench-sample.sh" "$CGROUP_PARENT" "$FC_RUN_DIR" "$JAIL_BASE" "${GATEWAY_REAL_PID:-}")"
  printf '%s' "$snap" | jq -c --arg sample "$sample" --arg phase "$phase" '{sample: $sample, phase: $phase} + .' >> "$RESOURCES"
  printf '%s' "$snap" | jq -r --arg p "$phase" '"  [\($p)] environments=\(.environments | length) gateway_rss_kib=\(.gateway.vm_rss_kib // "-") host_available_kib=\(.host.mem_available_kib)"'
}

idle_cost() { # idle_cost SAMPLE
  sample_resources "$1" "idle-start"
  sleep "$IDLE_SECONDS"
  sample_resources "$1" "idle-end"
}

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# cleanup proof
# ---------------------------------------------------------------------------

prove_clean() {
  local procs cgroups jails taps table runs out="$EVIDENCE_DIR/cleanup.txt"
  procs="$(pgrep -a -f '(^|/)(firecracker|jailer|tachyon-serverless-gateway)( |$)' 2>/dev/null | grep -v -e pgrep -e bench.sh || true)"
  cgroups="$(find "$CGROUP_PARENT" -mindepth 1 -maxdepth 1 -type d 2>/dev/null || true)"
  jails="$($SUDO find "$JAIL_BASE" -mindepth 1 -maxdepth 2 2>/dev/null || true)"
  taps="$(ip -br link show 2>/dev/null | awk '$1 ~ /^tsls/ { print $1 }' || true)"
  table="$($SUDO nft list table inet tachyon_egress 2>/dev/null | head -n 3 || true)"
  runs="$(find "$FC_RUN_DIR" -mindepth 1 -maxdepth 1 ! -name _archive 2>/dev/null || true)"
  {
    echo "checked at $(date -u +%Y-%m-%dT%H:%M:%SZ) after the last gateway stopped"
    echo "gateway / firecracker / jailer processes: ${procs:-(none)}"
    echo "environment cgroups under $CGROUP_PARENT: ${cgroups:-(none)}"
    echo "cgroup parent present: $([ -d "$CGROUP_PARENT" ] && echo yes || echo no)"
    echo "jails under $JAIL_BASE: ${jails:-(none)}"
    echo "tsls* taps: ${taps:-(none)}"
    echo "nft table inet tachyon_egress: ${table:-(none)}"
    echo "environment dirs under $FC_RUN_DIR: ${runs:-(none)}"
    echo "loop devices: $(losetup -a 2>/dev/null | wc -l)"
    echo "root file system use: $(root_fs_pct)%"
  } > "$out"
  cat "$out"
  [ -z "$procs" ] && [ -z "$cgroups" ] && [ -z "$jails" ] && [ -z "$taps" ] && [ -z "$table" ] && [ -z "$runs" ]
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

# step() runs its function in a subshell, so everything that starts a gateway runs in the parent
# and only the measurement bodies go through step. start_gateway is wrapped here.
main() {
  e2e_log "benchmark $STAMP (arch $ARCH, evidence $EVIDENCE_DIR)"
  step "preflight (sudo, kvm, jailer + cgroup required, host not shared, disk)" preflight
  if [ "$(steps_failed_count)" -ne 0 ]; then
    steps_print_table || true
    exit 2
  fi
  step "build (release gateway / cli, musl guests)" build_all
  write_configs
  step "metadata" write_metadata
  step "calibration before the run (BENCH_MAX_CALIBRATION_MS)" calibrate start
  if [ "$(steps_failed_count)" -ne 0 ]; then
    steps_print_table || true
    exit 2
  fi

  local sample t raw
  for sample in $SAMPLES; do
    bin_of "$sample" >/dev/null || e2e_die "unknown sample $sample"
    calibrate "before-$sample" || true
    for t in $(seq 1 "$FRESH_TRIALS"); do
      e2e_log "$sample: fresh host trial $t"
      stop_gateway 20
      fresh_host_reset
      start_gateway "$COLD_CONFIG" cold "$sample-fresh-$t" || { steps_print_table || true; exit 2; }
      sample_resources "$sample" "gateway-empty" >/dev/null || true
      step "$sample: create + deploy (fresh host $t)" deploy_sample "$sample" "$t"
      raw="$WORK_DIR/raw-$sample-fresh-$t.jsonl"
      : > "$raw"
      request "$sample" "$(state_get "fn.$sample")" "$(state_get "rev.$sample")" "$raw" fresh-miss "$t" 1 1 1
      enrich "$raw"
      jq -r 'select(.scenario == "fresh-miss") | "  fresh trial \(.trial) http=\(.http_code) client=\(.client_ms) boot=\(.timings.environment_boot_ms // "-")"' "$ATTEMPTS" | tail -n1
    done
    step "$sample: cold x$COLD_N (cache hit, pool off)" sequential "$sample" cold "$COLD_N" 0
    case " $SWEEP_MODES " in *" cold "*) step "$sample: concurrency sweep, pool off" sweep "$sample" cold ;; esac
    stop_gateway 30

    start_gateway "$WARM_CONFIG" warm "$sample-warm" || { steps_print_table || true; exit 2; }
    sample_resources "$sample" "warm-gateway-empty" >/dev/null || true
    step "$sample: warm prime" sequential "$sample" warm-prime 1 "$WARM_GAP_MS"
    step "$sample: warm x$WARM_N (pool on)" sequential "$sample" warm "$WARM_N" "$WARM_GAP_MS"
    step "$sample: paused environment cost over ${IDLE_SECONDS}s" idle_cost "$sample"
    case " $SWEEP_MODES " in *" warm "*) step "$sample: concurrency sweep, pool on" sweep "$sample" warm ;; esac
    sleep 2
    step "$sample: pooled environments after the sweep" sample_resources "$sample" "warm-after-sweep"
    stop_gateway 30

    local pct
    pct="$(root_fs_pct)"
    e2e_log "file system use after $sample: ${pct}%"
    if [ "$pct" -ge "$MAX_DISK_PCT" ]; then
      e2e_warn "file system use ${pct}% >= ${MAX_DISK_PCT}%: stopping before the next sample"
      _step_record "disk budget" "FAIL" 0 "file system use ${pct}%"
      break
    fi
  done

  stop_gateway 30
  calibrate end || true
  local clean=0
  step "cleanup proof (no gateway, VMM, jail, cgroup, tap, env dir)" prove_clean || true
  [ "${STEP_STATUS[${#STEP_STATUS[@]}-1]}" = "PASS" ] || clean=1
  $SUDO rm -rf "$DATA_DIR" "$FC_RUN_DIR"
  $SUDO chown -R "$(id -u):$(id -g)" "$EVIDENCE_DIR" 2>/dev/null || true

  steps_write_summary "$EVIDENCE_DIR/steps.json" "$(jq -nc --arg run "bench-$STAMP" '{run: $run}')"
  "$SCRIPT_DIR/bench-report.sh" "$EVIDENCE_DIR" || e2e_warn "bench-report.sh failed; the raw files are intact"
  steps_print_table || true
  echo "evidence: $EVIDENCE_DIR"
  if [ "$clean" -ne 0 ]; then
    echo "LEFTOVERS: see $EVIDENCE_DIR/cleanup.txt" >&2
    exit 3
  fi
  [ "$(steps_failed_count)" -eq 0 ] || exit 2
  exit 0
}

main "$@"

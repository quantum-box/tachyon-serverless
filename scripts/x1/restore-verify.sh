#!/usr/bin/env bash
# scripts/x1/restore-verify.sh - X1 (PLT-4654, experimental): does a restored Rust function resume
# correctly, and is it worth it? Identity / RNG / clock / connections / disk / auth checks on clones
# of examples/restore-verify, plus first-response, memory and storage against cold and warm starts,
# all through the gateway on real KVM. Judged separately from P0-P4.
#
# Flow (checks go to checks.tsv; every request is one row of attempts.jsonl, failures included):
#   build    gateway + cli with `experimental-restore`, the bridge with it, examples/restore-verify
#            (musl), rootfs at .kvm/x1/rootfs-restore.ext4 (as the calling user)
#   run      as root (jailer + cgroup required), gateway release build, profile dev (the X1
#            measurement profile: [snapshots] allow_unverified), a background sampler of every
#            environment cgroup and firecracker process (resources.jsonl)
#   phase A  gateway without [pool]
#     cold-hit (N)          restore policy none, page cache warm
#     cold-miss (N)         same, `echo 3 > drop_caches` before each request
#     require-no-snapshot   refused, nothing boots
#     snapshot S1           + plaintext / sealed sizes and sha256
#     restored-first        the first restore after S1 was created
#     restored-hit (N)      sequential restores (also the vsock/agent reconnect series)
#     restored-miss (N)     drop_caches before each restore
#     restored-concurrent   ROUNDS x CONC clones at once, payload verify=true (full checksum)
#     snapshot-grep         per-clone values absent from S1's plaintext files, bootstrap marker
#                           present (positive control), S1 files unchanged by the clones
#     restored-plain-miss(N) S1's plaintext cache removed first: decrypt from the sealed store
#     first/second (CYCLES) revoke the active snapshot, create a new one, restore twice
#     revoked               require after revoking the only snapshot -> refused
#     corrupt-*             flipped byte in plaintext (require refused + quarantined, prefer cold
#                           fallback) and in the sealed store with the plaintext removed
#     revision-stale        new revision refuses the old snapshot
#   phase B  gateway with [pool]: warm-prime + warm (N, 250 ms apart) on the cold function
#   cleanup  gateway stopped, snapshot files removed, no VMM / jail / cgroup / env dir left
#   report   summary.json + summary.md (nearest-rank p50/p95/p99 over all attempts)
#
# Usage:   scripts/x1/restore-verify.sh [out_dir]    (default docs/evidence/x1-restore-verify-<UTC>)
# Env:     TSLS_SKIP_BUILD=1, RV_PORT (18094), RV_COLD_N (20), RV_COLD_MISS_N (10), RV_WARM_N (20),
#          RV_RESTORE_N (30), RV_MISS_N (20), RV_PLAIN_MISS_N (5), RV_CONC (4), RV_CONC_ROUNDS (2),
#          RV_CYCLES (5), RV_DATASET_MIB (64), RV_PASSES (1), RV_MEMORY_MIB (256), RV_CPU_MILLIS
#          (1000), RV_CLOCK_TOLERANCE_MS (1000), RV_COMMIT, KEEP_WORK=1
# Needs:   Linux + /dev/kvm, passwordless sudo, .kvm/bin/{firecracker,jailer}, .kvm/vmlinux,
#          cargo with <arch>-unknown-linux-musl, curl, jq, perl.
# Exit:    0 all checks passed, 1 a check failed, 2 prerequisites / setup failed, 3 leftovers.
#
# Functions run through a trap and background jobs, which shellcheck cannot follow (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"
X1_TARGET="$REPO_ROOT/target/x1-restore"
GATEWAY_BIN="$X1_TARGET/release/tachyon-serverless-gateway"
TSLS_BIN="$X1_TARGET/release/tsls"
GUEST_DIR="$X1_TARGET/$MUSL_TARGET/release"
SAMPLE_BIN="$GUEST_DIR/example-restore-verify"
ROOTFS="$REPO_ROOT/.kvm/x1/rootfs-restore.ext4"
BASE_CONFIG="${TSLS_GATEWAY_CONFIG:-$REPO_ROOT/config/gateway.firecracker.toml}"
PORT="${RV_PORT:-18094}"
API="http://127.0.0.1:$PORT"
TOKEN="${TSLS_TOKEN:-dev-token-tenant-a}"
STAMP="${RV_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${1:-$REPO_ROOT/docs/evidence/x1-restore-verify-$STAMP}"
WORK="$REPO_ROOT/.kvm/x1/restore-verify-$STAMP"

COLD_N="${RV_COLD_N:-20}"
COLD_MISS_N="${RV_COLD_MISS_N:-10}"
WARM_N="${RV_WARM_N:-20}"
RESTORE_N="${RV_RESTORE_N:-30}"
MISS_N="${RV_MISS_N:-20}"
PLAIN_MISS_N="${RV_PLAIN_MISS_N:-5}"
CONC="${RV_CONC:-4}"
CONC_ROUNDS="${RV_CONC_ROUNDS:-2}"
CYCLES="${RV_CYCLES:-5}"
DATASET_MIB="${RV_DATASET_MIB:-64}"
PASSES="${RV_PASSES:-1}"
MEMORY_MIB="${RV_MEMORY_MIB:-256}"
CPU_MILLIS="${RV_CPU_MILLIS:-1000}"
CLOCK_TOL_MS="${RV_CLOCK_TOLERANCE_MS:-1000}"

log() { printf '[x1-restore-verify] %s\n' "$*" >&2; }
now_ms() { echo $(($(date +%s%N) / 1000000)); }

# --- phase 1: build as the calling user, then re-run as root ------------------------------------
if [ "${1:-}" != "--as-root" ] && [ "$(id -u)" != 0 ]; then
  for tool in cargo curl jq perl sudo; do
    command -v "$tool" >/dev/null 2>&1 || { log "missing $tool"; exit 2; }
  done
  if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then log "/dev/kvm is not readable and writable"; exit 2; fi
  for f in .kvm/bin/firecracker .kvm/bin/jailer .kvm/vmlinux; do
    [ -f "$f" ] || { log "missing $f (scripts/kvm/bootstrap.sh)"; exit 2; }
  done
  if [ "${TSLS_SKIP_BUILD:-0}" != 1 ]; then
    log "building gateway + cli (experimental-restore), bridge (experimental-restore), restore-verify"
    CARGO_TARGET_DIR="$X1_TARGET" cargo build --release -p tachyon-serverless-gateway \
      --features tachyon-serverless-gateway/experimental-restore -p tachyon-serverless-cli >&2
    # ring (rustls / rcgen in the sample) compiles C: without a musl cross gcc, the host cc of the
    # same architecture is enough for a static musl binary.
    musl_cc_var="CC_${MUSL_TARGET//-/_}"
    if ! command -v "${ARCH}-linux-musl-gcc" >/dev/null 2>&1 && [ -z "${!musl_cc_var:-}" ]; then
      export "$musl_cc_var=cc"
    fi
    CARGO_TARGET_DIR="$X1_TARGET" cargo build --release --target "$MUSL_TARGET" \
      -p tachyon-serverless-runtime-bridge --features tachyon-serverless-runtime-bridge/experimental-restore \
      -p example-restore-verify >&2
    mkdir -p "$(dirname "$ROOTFS")"
    BRIDGE_BIN="$GUEST_DIR/tachyon-serverless-runtime-bridge" ROOTFS="$ROOTFS" scripts/kvm/build-rootfs.sh >&2
  fi
  for b in "$GATEWAY_BIN" "$TSLS_BIN" "$SAMPLE_BIN" "$ROOTFS"; do
    [ -f "$b" ] || { log "missing $b"; exit 2; }
  done
  mkdir -p "$OUT"
  rc=0
  sudo -n env PATH="$PATH" HOME="$HOME" RV_STAMP="$STAMP" RV_PORT="$PORT" RV_COMMIT="${RV_COMMIT:-}" \
    RV_COLD_N="$COLD_N" RV_COLD_MISS_N="$COLD_MISS_N" RV_WARM_N="$WARM_N" RV_RESTORE_N="$RESTORE_N" \
    RV_MISS_N="$MISS_N" RV_PLAIN_MISS_N="$PLAIN_MISS_N" RV_CONC="$CONC" RV_CONC_ROUNDS="$CONC_ROUNDS" \
    RV_CYCLES="$CYCLES" RV_DATASET_MIB="$DATASET_MIB" RV_PASSES="$PASSES" RV_MEMORY_MIB="$MEMORY_MIB" \
    RV_CPU_MILLIS="$CPU_MILLIS" RV_CLOCK_TOLERANCE_MS="$CLOCK_TOL_MS" RV_HOST_NOTE="${RV_HOST_NOTE:-}" \
    KEEP_WORK="${KEEP_WORK:-0}" TSLS_GATEWAY_CONFIG="$BASE_CONFIG" bash "$0" --as-root "$OUT" || rc=$?
  sudo -n chown -R "$(id -u):$(id -g)" "$OUT" 2>/dev/null || true
  exit "$rc"
fi
[ "${1:-}" = "--as-root" ] && shift
OUT="${1:-$OUT}"

# --- phase 2: as root ---------------------------------------------------------------------------
mkdir -p "$OUT" "$WORK/data" "$WORK/raw"
CHECKS="$OUT/checks.tsv"
ATTEMPTS="$OUT/attempts.jsonl"
INVOCATIONS="$OUT/invocations.jsonl"
RESOURCES="$OUT/resources.jsonl"
SNAPSHOTS="$OUT/snapshots.jsonl"
CALIBRATION="$OUT/calibration.jsonl"
printf 'check\tresult\tdetail\n' >"$CHECKS"
: >"$ATTEMPTS"; : >"$INVOCATIONS"; : >"$RESOURCES"; : >"$SNAPSHOTS"; : >"$CALIBRATION"
FAILS=0
check() { # name PASS|FAIL|INFO detail
  printf '%s\t%s\t%s\n' "$1" "$2" "$(printf '%s' "$3" | tr '\t\n' '  ')" >>"$CHECKS"
  log "$1: $2 $(printf '%s' "$3" | head -c 400)"
  if [ "$2" = FAIL ]; then FAILS=$((FAILS + 1)); fi
  return 0
}

FC_RUN_DIR="$(sed -n 's/^workdir *= *"\([^"]*\)".*/\1/p' "$BASE_CONFIG" | head -n1)"
case "$FC_RUN_DIR" in /*) ;; *) FC_RUN_DIR="$REPO_ROOT/${FC_RUN_DIR#./}" ;; esac
CHROOT_BASE="$(sed -n 's/^chroot_base *= *"\([^"]*\)".*/\1/p' "$BASE_CONFIG" | head -n1)"
CHROOT_BASE="${CHROOT_BASE:-/srv/jailer}"
CG_PARENT=/sys/fs/cgroup/tachyon
SNAP_ROOT="$FC_RUN_DIR/_snapshots"
GATEWAY_PID=""
SAMPLER_PID=""

stop_gateway() {
  if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
    kill -TERM "$GATEWAY_PID"
    for _ in $(seq 1 80); do kill -0 "$GATEWAY_PID" 2>/dev/null || break; sleep 0.25; done
    kill -KILL "$GATEWAY_PID" 2>/dev/null || true
  fi
  GATEWAY_PID=""
}

cleanup() {
  local rc=$?
  set +e
  [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null
  stop_gateway
  pkill -KILL -x firecracker 2>/dev/null
  rm -rf "$SNAP_ROOT"
  if [ "${KEEP_WORK:-0}" != 1 ]; then rm -rf "$WORK"; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

calibrate() { # phase
  local runs="" t0
  for _ in 1 2 3; do
    t0="$(now_ms)"
    awk 'BEGIN { s = 0; for (i = 0; i < 3000000; i++) s += i }'
    runs="$runs $(($(now_ms) - t0))"
  done
  jq -nc --arg phase "$1" --arg runs "$runs" --arg load "$(cut -d' ' -f1-3 /proc/loadavg)" \
    '{phase: $phase, awk_loop_ms: ($runs | split(" ") | map(select(length > 0) | tonumber)), vm_loadavg: $load}' >>"$CALIBRATION"
}

# --- config + gateway ---------------------------------------------------------------------------
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$WORK/snap.key"
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$WORK/snap.sign"
chmod 600 "$WORK/snap.key" "$WORK/snap.sign"
COLD_CONFIG="$OUT/gateway-cold.toml"
WARM_CONFIG="$OUT/gateway-warm.toml"
sed -e "s|^listen *=.*|listen = \"127.0.0.1:$PORT\"|" \
  -e 's|^profile *=.*|profile = "dev"                 # X1 measurement run (scripts/x1/restore-verify.sh)|' \
  -e "s|^data_dir *=.*|data_dir = \"$WORK/data\"|" \
  -e "s|^rootfs *=.*|rootfs = \"$ROOTFS\"|" \
  "$BASE_CONFIG" >"$COLD_CONFIG"
cat >>"$COLD_CONFIG" <<EOF

# Added by scripts/x1/restore-verify.sh (PLT-4654, experimental). allow_unverified accepts the
# provider's Unverified snapshot capabilities for this measurement run only. Key files are
# generated per run under the work directory and removed afterwards.
[snapshots]
enabled = true
allow_unverified = true
ttl_seconds = 7200
key_file = "$WORK/snap.key"
signing_key_file = "$WORK/snap.sign"
EOF
cp "$COLD_CONFIG" "$WARM_CONFIG"
cat >>"$WARM_CONFIG" <<EOF

# Phase B (warm): same as bench.sh (PLT-4647).
[pool]
enabled = true
max_idle_per_revision = 8
idle_ttl_seconds = 900
max_total_idle = 24
EOF

start_gateway() { # config label
  LOG_FORMAT=json "$GATEWAY_BIN" --config "$1" >>"$OUT/gateway-$2.log" 2>&1 &
  GATEWAY_PID=$!
  for _ in $(seq 1 240); do
    curl -fsS -m 2 -o /dev/null "$API/readyz" 2>/dev/null && return 0
    kill -0 "$GATEWAY_PID" 2>/dev/null || { log "gateway exited"; tail -n 30 "$OUT/gateway-$2.log" >&2; exit 2; }
    sleep 0.5
  done
  log "gateway not ready"; exit 2
}

api() { # method path [json] -> body on stdout; HTTP code in $WORK/.code
  if [ -n "${3:-}" ]; then
    curl -sS -o "$WORK/.body" -w '%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer $TOKEN" \
      -H 'content-type: application/json' -d "$3" >"$WORK/.code"
  else
    curl -sS -o "$WORK/.body" -w '%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer $TOKEN" >"$WORK/.code"
  fi
  cat "$WORK/.body"
}

# --- profile ------------------------------------------------------------------------------------
COMMIT="${RV_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)}"
GITHUB_SHA="$COMMIT" "$REPO_ROOT/scripts/ci/kvm-profile.sh" "$OUT/profile" >/dev/null 2>&1 || true
{
  echo "commit $COMMIT"
  echo "host_note ${RV_HOST_NOTE:-}"
  echo "virt $(systemd-detect-virt 2>/dev/null || echo unknown)"
  echo "host_uname $(uname -srm)"
  echo "host_cpu $(grep -m1 -i -E 'model name|CPU part' /proc/cpuinfo | sed 's/.*: //')"
  echo "host_vcpus $(nproc)"
  echo "host_mem_kib $(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  echo "firecracker $(.kvm/bin/firecracker --version 2>/dev/null | head -n1)"
  for f in .kvm/bin/firecracker .kvm/bin/jailer .kvm/vmlinux "$ROOTFS" "$GATEWAY_BIN" "$TSLS_BIN" \
    "$GUEST_DIR/tachyon-serverless-runtime-bridge" "$SAMPLE_BIN" "$COLD_CONFIG" "$WARM_CONFIG"; do
    echo "sha256 $(sha256sum "$f" | awk '{print $1}') $(stat -c %s "$f") ${f#"$REPO_ROOT"/}"
  done
  echo "params cold_n=$COLD_N cold_miss_n=$COLD_MISS_N warm_n=$WARM_N restore_n=$RESTORE_N miss_n=$MISS_N plain_miss_n=$PLAIN_MISS_N conc=$CONC conc_rounds=$CONC_ROUNDS cycles=$CYCLES dataset_mib=$DATASET_MIB passes=$PASSES memory_mib=$MEMORY_MIB cpu_millis=$CPU_MILLIS clock_tolerance_ms=$CLOCK_TOL_MS"
} >"$OUT/versions.txt"
calibrate start

# --- resource sampler: every 0.5 s, one line per environment ------------------------------------
sampler() {
  set +e
  while :; do
    local t
    t="$(now_ms)"
    for dir in "$CG_PARENT"/*/; do
      [ -d "$dir" ] || continue
      dir="${dir%/}"
      local env="${dir##*/}" cur peak pid rss="null" pss="null" sc="null" pd="null" maps=false
      cur="$(cat "$dir/memory.current" 2>/dev/null || echo null)"
      peak="$(cat "$dir/memory.peak" 2>/dev/null || echo null)"
      pid="$(head -n1 "$dir/cgroup.procs" 2>/dev/null || true)"
      if [ -n "$pid" ] && [ -r "/proc/$pid/smaps_rollup" ]; then
        read -r rss pss sc pd < <(awk '/^Rss:/ {r=$2} /^Pss:/ {p=$2} /^Shared_Clean:/ {s=$2} /^Private_Dirty:/ {d=$2}
          END {print (r==""?"null":r), (p==""?"null":p), (s==""?"null":s), (d==""?"null":d)}' "/proc/$pid/smaps_rollup" 2>/dev/null)
        grep -q snapshot.mem "/proc/$pid/maps" 2>/dev/null && maps=true
      fi
      printf '{"t":%s,"env":"%s","memory_current":%s,"memory_peak":%s,"rss_kib":%s,"pss_kib":%s,"shared_clean_kib":%s,"private_dirty_kib":%s,"maps_snapshot_mem":%s}\n' \
        "$t" "$env" "${cur:-null}" "${peak:-null}" "${rss:-null}" "${pss:-null}" "${sc:-null}" "${pd:-null}" "$maps"
    done
    sleep 0.5
  done
}
sampler >>"$RESOURCES" 2>/dev/null &
SAMPLER_PID=$!

# --- requests -----------------------------------------------------------------------------------
SEQ=0
# request scenario function_id payload label -> one raw row in $WORK/raw/<label>.json. Never fails.
request() {
  local scenario="$1" fid="$2" payload="$3" label="$4" started metrics rc=0 inv code
  started="$(now_ms)"
  metrics="$(curl -sS -o "$WORK/raw/$label.body" -D "$WORK/raw/$label.hdr" --max-time 90 \
    -w '%{http_code} %{time_total} %{time_starttransfer}' -X POST "$API/v1/functions/$fid/invoke" \
    -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d "$payload" 2>"$WORK/raw/$label.err")" || rc=$?
  inv="$(tr -d '\r' <"$WORK/raw/$label.hdr" 2>/dev/null | awk 'tolower($1) == "x-tachyon-invocation-id:" {print $2}' | tail -n1)"
  code="$(printf '%s' "$metrics" | awk '{print $1}')"
  [ -n "$inv" ] || inv="$(jq -r '.error.invocation_id // empty' "$WORK/raw/$label.body" 2>/dev/null || true)"
  jq -nc --arg scenario "$scenario" --arg label "$label" --argjson started "$started" --argjson ended "$(now_ms)" \
    --arg metrics "${metrics:-}" --argjson rc "$rc" --arg inv "$inv" --arg fid "$fid" \
    --arg cerr "$(head -c 300 "$WORK/raw/$label.err" 2>/dev/null)" \
    --slurpfile body <(jq -c . "$WORK/raw/$label.body" 2>/dev/null || echo null) '
    ($metrics | split(" ")) as $m |
    {scenario: $scenario, label: $label, function_id: $fid, started_ms: $started, ended_ms: $ended,
     curl_exit: $rc, curl_error: (if $cerr == "" then null else $cerr end),
     http_code: (($m[0] // "0") | tonumber? // 0),
     client_ms: (if ($m | length) > 1 then (($m[1] | tonumber) * 1000 | round) else null end),
     client_ttfb_ms: (if ($m | length) > 2 then (($m[2] | tonumber) * 1000 | round) else null end),
     invocation_id: (if $inv == "" then null else $inv end), body: ($body[0] // null)}' >"$WORK/raw/$label.json"
  rm -f "$WORK/raw/$label.body" "$WORK/raw/$label.hdr" "$WORK/raw/$label.err"
}

# enrich label... -> attempts.jsonl rows (invocation detail merged). Run after the timed requests.
enrich() {
  local label row inv detail
  for label in "$@"; do
    row="$WORK/raw/$label.json"
    [ -f "$row" ] || continue
    inv="$(jq -r '.invocation_id // empty' "$row")"
    detail=null
    if [ -n "$inv" ]; then
      detail="$(curl -sS -m 10 -H "authorization: Bearer $TOKEN" "$API/v1/invocations/$inv" 2>/dev/null || echo null)"
      if printf '%s' "$detail" | jq -e .id >/dev/null 2>&1; then
        printf '%s\n' "$detail" | jq -c . >>"$INVOCATIONS"
      else
        detail=null
      fi
    fi
    jq -c --argjson d "$detail" '
      (if $d == null then {} else $d end) as $d |
      ($d.attempts // []) as $as | ($as[-1] // {}) as $a | ($a.boot_evidence.details // {}) as $x |
      . + {status: ($d.status // null), invocation_error: ($d.error // null), attempts: ($as | length),
           start_kind: ($a.start_kind // null), environment_id: ($a.environment_id // null),
           dispatched_at: ($a.dispatched_at // null), timings: ($a.timings // null),
           restore: ($x | with_entries(select(.key | test("^restore_|^snapshot_|doorbell|reconnect|scratch_drive|^boot_ms$|^resume_ms$")))),
           output: .body} | del(.body)' "$row" >>"$ATTEMPTS"
  done
}

deploy() { # name policy -> function id
  local fn="$1" policy="$2" extra=()
  if [ "$policy" != none ]; then extra=(--restore-policy "$policy" --synthetic-init-sample); fi
  "$TSLS_BIN" functions get "$fn" --json >/dev/null 2>&1 ||
    "$TSLS_BIN" functions create --name "$fn" --description "X1 restore verify" --json >/dev/null
  "$TSLS_BIN" functions deploy --function "$fn" --binary "$SAMPLE_BIN" --arch "$ARCH" \
    --memory-mib "$MEMORY_MIB" --cpu-millis "$CPU_MILLIS" --ephemeral-storage-mib 64 \
    --timeout-seconds 30 --init-timeout-seconds 30 --max-concurrency "$CONC" \
    --env "RESTORE_VERIFY_DATASET_MIB=$DATASET_MIB" --env "RESTORE_VERIFY_PRECOMPUTE_PASSES=$PASSES" \
    --description "restore=$policy" "${extra[@]}" --json >"$OUT/revision-$fn-$(now_ms).json"
  "$TSLS_BIN" functions get "$fn" --json | jq -r .id
}

drop_caches() { sync; echo 3 >/proc/sys/vm/drop_caches; }

run_seq() { # scenario function_id n payload [drop]
  local s="$1" fid="$2" n="$3" payload="$4" drop="${5:-}" i labels=()
  for i in $(seq 1 "$n"); do
    SEQ=$((SEQ + 1))
    if [ "$drop" = drop ]; then drop_caches; fi
    request "$s" "$fid" "$payload" "$s-$i"
    labels+=("$s-$i")
  done
  enrich "${labels[@]}"
}

snapshot_create() { # function_id label -> snapshot id (prints; empty after two failed attempts)
  # A failed creation (e.g. the source missing the checkpoint deadline on a busy host) is kept in
  # snapshots.jsonl with its HTTP code and retried once; the report counts the failures.
  local t0 code try id
  for try in 1 2; do
    t0="$(now_ms)"
    api POST "/v1/functions/$1/snapshots" '{}' >"$WORK/snap.json"
    code="$(cat "$WORK/.code")"
    jq -c --arg label "$2" --argjson try "$try" --argjson client_ms "$(($(now_ms) - t0))" --argjson code "${code:-0}" \
      '{label: $label, try: $try, http_code: $code, client_ms: $client_ms, snapshot: .}' "$WORK/snap.json" >>"$SNAPSHOTS" 2>/dev/null ||
      jq -nc --arg label "$2" --argjson try "$try" '{label: $label, try: $try, http_code: 0, snapshot: null}' >>"$SNAPSHOTS"
    id="$(jq -r '.id // empty' "$WORK/snap.json" 2>/dev/null || true)"
    if [ -n "$id" ]; then echo "$id"; return 0; fi
    log "snapshot $2 attempt $try failed: $(head -c 300 "$WORK/snap.json")"
  done
}

snapshot_storage() { # snapshot id label -> storage row in snapshots.jsonl
  local id="$1" sealed plain_app plain_alloc
  sealed="$(du -sb "$WORK/data/snapshots/$id" 2>/dev/null | awk '{print $1}')"
  plain_app="$(du -sb --apparent-size "$SNAP_ROOT/$id" 2>/dev/null | awk '{print $1}')"
  plain_alloc="$(du -sB1 "$SNAP_ROOT/$id" 2>/dev/null | awk '{print $1}')"
  jq -nc --arg id "$id" --arg label "$2" --arg sealed "${sealed:-}" --arg pa "${plain_app:-}" --arg pl "${plain_alloc:-}" \
    --arg files "$(cd "$SNAP_ROOT/$id" 2>/dev/null && stat -c '%n %s %b %B' -- * 2>/dev/null | tr '\n' ';')" \
    --arg sealed_files "$(cd "$WORK/data/snapshots/$id" 2>/dev/null && stat -c '%n %s' -- * 2>/dev/null | tr '\n' ';')" \
    '{label: $label, storage: {snapshot_id: $id, sealed_bytes: ($sealed | tonumber? // null),
      plaintext_apparent_bytes: ($pa | tonumber? // null), plaintext_allocated_bytes: ($pl | tonumber? // null),
      plaintext_files: $files, sealed_files: $sealed_files}}' >>"$SNAPSHOTS"
}

flip_byte() { # file offset: flip the lowest bit. Never aborts the run: the check that follows fails.
  perl -e 'open(my $f, "+<", $ARGV[0]) or die "$ARGV[0]: $!"; binmode $f; seek($f, $ARGV[1], 0);
    read($f, my $b, 1) == 1 or die "short file"; seek($f, $ARGV[1], 0); print $f chr(ord($b) ^ 1); close $f' "$1" "$2" ||
    log "could not flip a byte of $1"
}

last_attempt() { jq -c --arg l "$1" 'select(.label == $l)' "$ATTEMPTS" | tail -n1; }

refused() { # label code -> PASS when refused with that restore code and nothing booted cold
  local row
  row="$(last_attempt "$1")"
  printf '%s' "$row" | jq -e --arg c "$2" '.http_code != 200 and (.start_kind == null or .start_kind != "cold")
    and ((.output // {} | tostring) + (.invocation_error // {} | tostring) | contains("RestoreRequiredUnavailable") and contains($c))' >/dev/null
}

# ================================================================================================
# phase A: no pool
# ================================================================================================
start_gateway "$COLD_CONFIG" a
export TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN"
api GET /v1/provider >"$OUT/provider.json"
check provider "$(jq -e '.capabilities.snapshot_clone.status != "unsupported" and .capabilities.snapshot_create.status != "unsupported"' "$OUT/provider.json" >/dev/null && echo PASS || echo FAIL)" \
  "snapshot_create=$(jq -r '.capabilities.snapshot_create.status' "$OUT/provider.json") snapshot_clone=$(jq -r '.capabilities.snapshot_clone.status' "$OUT/provider.json")"

COLD_FN="$(deploy rv-cold none)"
REQ_FN="$(deploy rv-restore require)"
LOOKUP='{"verify":false}'
VERIFY='{"verify":true}'

log "cold-check + cold-hit x$COLD_N"
request cold-check "$COLD_FN" "$VERIFY" cold-check-1
enrich cold-check-1
run_seq cold-hit "$COLD_FN" "$COLD_N" "$LOOKUP"
calibrate after-cold-hit
log "cold-miss x$COLD_MISS_N (drop_caches before each)"
run_seq cold-miss "$COLD_FN" "$COLD_MISS_N" "$LOOKUP" drop

request require-no-snapshot "$REQ_FN" "$LOOKUP" require-no-snapshot
enrich require-no-snapshot
if refused require-no-snapshot no_snapshot; then
  check require-no-snapshot PASS "Host.RestoreRequiredUnavailable (no_snapshot), nothing booted"
else
  check require-no-snapshot FAIL "$(last_attempt require-no-snapshot | head -c 400)"
fi

log "snapshot S1"
S1="$(snapshot_create "$REQ_FN" S1)"
if [ -z "$S1" ]; then check snapshot-S1 FAIL "$(head -c 400 "$WORK/snap.json")"; exit 1; fi
check snapshot-S1 PASS "$S1 $(jq -c .timings "$WORK/snap.json")"
snapshot_storage "$S1" S1
S1_SOURCE_ENV="$(jq -r .source_environment_id "$WORK/snap.json")"
S1_CREATED_MS="$(($(date -d "$(jq -r .created_at "$WORK/snap.json")" +%s%N) / 1000000))"
S1_BOOT_ID="$(jq -r '.manifest | fromjson | .source_boot_id // empty' "$WORK/data/snapshots/$S1/manifest.json" 2>/dev/null || true)"
(cd "$SNAP_ROOT/$S1" && sha256sum -- *) >"$WORK/s1-before.sha256" 2>/dev/null || true

log "restored-first + restored-hit x$RESTORE_N"
request restored-first "$REQ_FN" "$LOOKUP" restored-first-S1
enrich restored-first-S1
run_seq restored-hit "$REQ_FN" "$RESTORE_N" "$LOOKUP"
calibrate after-restored-hit
log "restored-miss x$MISS_N (drop_caches before each)"
run_seq restored-miss "$REQ_FN" "$MISS_N" "$LOOKUP" drop

log "restored-concurrent: $CONC_ROUNDS rounds x $CONC"
for r in $(seq 1 "$CONC_ROUNDS"); do
  labels=()
  pids=()
  for c in $(seq 1 "$CONC"); do
    request restored-concurrent "$REQ_FN" "$VERIFY" "restored-concurrent-$r-$c" &
    pids+=("$!")
    labels+=("restored-concurrent-$r-$c")
  done
  # Only the requests: a bare `wait` would also wait for the resource sampler.
  wait "${pids[@]}" || true
  enrich "${labels[@]}"
done

# --- checks over every restored attempt of S1 ---------------------------------------------------
RESTORED_S1="$WORK/restored-s1.jsonl"
jq -c --arg s "$S1" 'select((.scenario | startswith("restored-")) and .restore.snapshot_id == $s)' "$ATTEMPTS" >"$RESTORED_S1"
COLD_ROWS="$WORK/cold.jsonl"
jq -c 'select(.scenario | startswith("cold-"))' "$ATTEMPTS" >"$COLD_ROWS"
n_restored="$(wc -l <"$RESTORED_S1" | tr -d ' ')"
n_restored_attempted="$(jq -c 'select(.scenario | test("^restored-(first|hit|miss|concurrent)$"))' "$ATTEMPTS" | wc -l | tr -d ' ')"
COLD_SUM="$(jq -r 'select(.http_code == 200) | .output.dataset.checksum' "$COLD_ROWS" | sort -u | tr '\n' ' ')"

if [ "$n_restored" = "$n_restored_attempted" ] && jq -se 'all(.http_code == 200 and .start_kind == "restored" and .output.restored == true)' "$RESTORED_S1" >/dev/null; then
  check restored-all "PASS" "$n_restored/$n_restored_attempted attempts HTTP 200, start_kind=restored, guest restored=true"
else
  check restored-all FAIL "$n_restored of $n_restored_attempted attempts restored from $S1: $(jq -c 'select(.scenario | test("^restored-")) | select(.start_kind != "restored" or .http_code != 200) | {label, http_code, start_kind, invocation_error}' "$ATTEMPTS" | head -n 5 | tr '\n' ' ')"
fi

if jq -se --arg cs "$COLD_SUM" '
    (map(.output.dataset.checksum) | unique) as $u |
    ($u | length) == 1 and ($cs | split(" ") | map(select(length > 0))) == $u and
    all(.output.dataset.lookup.ok == true and .output.dataset.spot_ok == true) and
    all(.output.dataset.full == null or .output.dataset.full.ok == true)' "$RESTORED_S1" >/dev/null &&
  jq -se 'all(.output.dataset.full == null or .output.dataset.full.ok == true) and all(.output.dataset.lookup.ok and .output.dataset.spot_ok)' "$COLD_ROWS" >/dev/null; then
  check fixed-data PASS "checksum $(jq -r .output.dataset.checksum "$RESTORED_S1" | head -n1) in all $n_restored clones = cold ($COLD_SUM); lookup + 64 spot checks ok; full recompute ok in $(jq -s 'map(select(.output.dataset.full != null)) | length' "$RESTORED_S1") clones"
else
  check fixed-data FAIL "restored checksums $(jq -r .output.dataset.checksum "$RESTORED_S1" | sort | uniq -c | tr '\n' ' ') cold $COLD_SUM"
fi

if jq -se --arg src "$S1_SOURCE_ENV" --argjson created "$S1_CREATED_MS" '
    all(.output.dataset.bootstrap_env == $src and .environment_id != $src and
        .output.dataset.bootstrap_wall_ms < $created and
        .output.dataset.bootstrap_runs_in_process == 1 and
        (.output.scratch.bootstrap_log_lines | length) == 1 and
        (.output.scratch.bootstrap_log_lines[0] | contains("env=" + $src)))' "$RESTORED_S1" >/dev/null &&
  jq -se 'all(select(.http_code == 200) | .output.dataset.bootstrap_env == .environment_id and (.output.scratch.bootstrap_log_lines | length) == 1)' "$COLD_ROWS" >/dev/null; then
  check bootstrap-not-rerun PASS "every clone: bootstrap_env = source $S1_SOURCE_ENV, 1 bootstrap run in the process, bootstrap.log has only the source line; cold starts ran their own bootstrap ($(jq -s 'map(.output.dataset.bootstrap_ms) | sort | .[length/2|floor]' "$COLD_ROWS") ms p50)"
else
  check bootstrap-not-rerun FAIL "$(jq -c '{label, env: .environment_id, b: .output.dataset.bootstrap_env, runs: .output.dataset.bootstrap_runs_in_process, lines: .output.scratch.bootstrap_log_lines}' "$RESTORED_S1" | head -n 3 | tr '\n' ' ')"
fi

dups() { jq -r "$1" "$RESTORED_S1" | sort | uniq -d | wc -l | tr -d ' '; }
distinct() { jq -r "$1" "$RESTORED_S1" | sort -u | grep -c . || true; }
d_detail=""
d_ok=1
for f in .environment_id .output.instance_id .output.ctx_instance_id .output.token .output.rng.first .output.db.session_id .output.db.server_nonce .output.tls.cert_sha256 .output.tls.client_exporter; do
  nd="$(distinct "$f")"
  d_detail="$d_detail ${f#.output.}=$nd"
  if [ "$(dups "$f")" != 0 ] || [ "$nd" != "$n_restored" ]; then d_ok=0; fi
done
gens="$(jq -r .output.generation "$RESTORED_S1" | sort -n | uniq -d | wc -l | tr -d ' ')"
bootids="$(jq -r .output.guest_boot_id "$RESTORED_S1" | sort -u | tr '\n' ' ')"
[ "$gens" = 0 ] || d_ok=0
if [ "$d_ok" = 1 ] && [ "$n_restored" -ge 4 ]; then
  check identity-diverges PASS "$n_restored clones, distinct:$d_detail; generations unique; guest boot id shared by all = source ($bootids, manifest $S1_BOOT_ID)"
else
  check identity-diverges FAIL "n=$n_restored distinct:$d_detail duplicate generations=$gens"
fi

if jq -se --argjson tol "$CLOCK_TOL_MS" '
    all(.output.clock as $c |
      ($c.wall_now_ms >= (.started_ms - $tol)) and ($c.wall_now_ms <= (.ended_ms + $tol)) and
      ($c.after_restore_timer.mono_ms >= 50) and ($c.after_restore_timer.mono_ms < 50 + $tol) and
      (($c.after_restore_timer.wall_ms - $c.after_restore_timer.mono_ms) | fabs) <= 100 and
      ($c.handler_timer.mono_ms >= 50) and ($c.handler_timer.mono_ms < 50 + $tol) and
      (($c.handler_timer.wall_ms - $c.handler_timer.mono_ms) | fabs) <= 100 and
      (($c.wall_since_after_restore_ms - $c.mono_since_after_restore_ms) | fabs) <= 100)' "$RESTORED_S1" >/dev/null; then
  check clock-timer PASS "$(jq -s '{wall_vs_client_window_ms_max: (map(if .output.clock.wall_now_ms > .ended_ms then .output.clock.wall_now_ms - .ended_ms elif .output.clock.wall_now_ms < .started_ms then .started_ms - .output.clock.wall_now_ms else 0 end) | max),
    timer50_mono_ms: (map(.output.clock.handler_timer.mono_ms) | [min, max]), timer_wall_minus_mono_ms_max: (map((.output.clock.handler_timer.wall_ms - .output.clock.handler_timer.mono_ms) | fabs) | max),
    wall_minus_mono_since_after_restore_ms: (map(.output.clock.wall_since_after_restore_ms - .output.clock.mono_since_after_restore_ms) | [min, max]),
    guest_uptime_at_after_restore_ms: (map(.output.clock.guest_uptime_at_after_restore_ms) | [min, max])}' "$RESTORED_S1" | tr -d '\n ')"
else
  check clock-timer FAIL "$(jq -c '{label, started_ms, ended_ms, clock: .output.clock}' "$RESTORED_S1" | head -n 3 | tr '\n' ' ')"
fi

if jq -se 'all(.output.db.ok == true and .output.db.token_sha256_seen_by_server == .output.token_sha256 and .output.db.handshakes == 1
      and .output.tls.exporters_match == true and .output.tls.ping.ok == true)' "$RESTORED_S1" >/dev/null; then
  check connections PASS "loopback DB: session authenticated with the clone's own token in every clone (1 handshake, no reconnect needed); TLS: cert generated after restore, client/server exporters match, PING/PONG ok; $(jq -s '{db_connect_ms: (map(.output.db.connect_ms) | [min, max]), tls_handshake_ms: (map(.output.tls.handshake_ms) | [min, max]), tls: (map(.output.tls.protocol + " " + .output.tls.cipher) | unique)}' "$RESTORED_S1" | tr -d '\n')"
else
  check connections FAIL "$(jq -c '{label, db: .output.db, tls: .output.tls}' "$RESTORED_S1" | head -n 2 | tr '\n' ' ')"
fi

if jq -se 'all(.output.scratch.own_present == true and (.output.scratch.foreign_instance_files | length) == 0
      and (.output.scratch.foreign_before_own_write | length) == 0
      and ([.output.scratch.entries[].name] | sort) == (["rv-bootstrap.log", .output.scratch.own_file] | sort))' "$RESTORED_S1" >/dev/null; then
  check scratch-isolation PASS "each of $n_restored clones sees exactly rv-bootstrap.log (source) + its own rv-instance-<id>; no other clone's file before or after its write"
else
  check scratch-isolation FAIL "$(jq -c '{label, scratch: .output.scratch | {own_file, foreign_instance_files, foreign_before_own_write, names: [.entries[].name]}}' "$RESTORED_S1" | head -n 3 | tr '\n' ' ')"
fi

# --- per-clone values must not be in the snapshot -----------------------------------------------
jq -r '.output | .instance_id, .ctx_instance_id, .token, .db.session_id, .db.server_nonce, .tls.client_exporter' "$RESTORED_S1" |
  grep -v '^null$' | sort -u >"$WORK/per-clone-values.txt"
MARKER="$(jq -r .output.dataset.bootstrap_marker "$RESTORED_S1" | sort -u | head -n1)"
(cd "$SNAP_ROOT/$S1" && sha256sum -- *) >"$WORK/s1-after.sha256" 2>/dev/null || true
{
  echo "# S1 = $S1; per-clone values searched: $(wc -l <"$WORK/per-clone-values.txt") (instance ids, tokens, DB session ids, server nonces, TLS exporters)"
  for f in "$SNAP_ROOT/$S1"/* "$WORK/data/snapshots/$S1"/*; do
    hits="$({ grep -a -o -F -f "$WORK/per-clone-values.txt" "$f" 2>/dev/null || true; } | sort -u | wc -l | tr -d ' ')"
    marker="$(grep -a -c -F "$MARKER" "$f" 2>/dev/null || true)"
    echo "${f#"$WORK"/} per_clone_hits=$hits bootstrap_marker_lines=${marker:-0}"
  done
  echo "# plaintext sha256 before the first restore"; cat "$WORK/s1-before.sha256"
  echo "# plaintext sha256 after all clones of S1"; cat "$WORK/s1-after.sha256"
} >"$OUT/snapshot-grep.txt"
grep_hits="$(awk '/per_clone_hits=/ {split($2, a, "="); s += a[2]} END {print s + 0}' "$OUT/snapshot-grep.txt")"
marker_mem="$(awk '$1 ~ /_snapshots\/.*\/memory$/ {sub(/.*bootstrap_marker_lines=/, ""); print; exit}' "$OUT/snapshot-grep.txt")"
marker_sealed="$(awk '$1 ~ /data\/snapshots\/.*\/memory\.sealed$/ {sub(/.*bootstrap_marker_lines=/, ""); print; exit}' "$OUT/snapshot-grep.txt")"
if [ "$grep_hits" = 0 ] && [ "${marker_mem:-0}" -ge 1 ] && [ "${marker_sealed:-missing}" = 0 ] &&
  cmp -s "$WORK/s1-before.sha256" "$WORK/s1-after.sha256" && [ -s "$WORK/s1-before.sha256" ]; then
  check auth-not-in-snapshot PASS "0 of $(wc -l <"$WORK/per-clone-values.txt") per-clone values in S1's plaintext or sealed files; positive control: bootstrap marker found in plaintext memory ($marker_mem) and not in memory.sealed; S1 plaintext sha256 unchanged by $n_restored clones"
else
  check auth-not-in-snapshot FAIL "hits=$grep_hits marker_mem=$marker_mem marker_sealed=$marker_sealed unchanged=$(cmp -s "$WORK/s1-before.sha256" "$WORK/s1-after.sha256" && echo yes || echo no)"
fi

# --- reconnect series ---------------------------------------------------------------------------
if jq -se 'map(select(.scenario == "restored-hit")) | length > 0 and all(.http_code == 200 and .start_kind == "restored" and .restore.restore_reconnects >= 1)' "$ATTEMPTS" >/dev/null; then
  check reconnect-series PASS "$(jq -s 'map(select(.scenario == "restored-hit")) | {n: length, reconnects: (map(.restore.restore_reconnects) | unique), doorbell_attempts: (map(.restore.doorbell_attempts) | unique), reconnect_ms_min_max: (map(.restore.restore_reconnect_ms) | [min, max]), failures: (map(select(.http_code != 200)) | length)}' "$ATTEMPTS" | tr -d '\n ')"
else
  check reconnect-series FAIL "$(jq -c 'select(.scenario == "restored-hit" and (.http_code != 200 or .start_kind != "restored")) | {label, http_code, start_kind, invocation_error}' "$ATTEMPTS" | head -n 3 | tr '\n' ' ')"
fi

# --- plaintext cache miss: decrypt from the sealed store -----------------------------------------
log "restored-plain-miss x$PLAIN_MISS_N"
labels=()
for i in $(seq 1 "$PLAIN_MISS_N"); do
  rm -f "$SNAP_ROOT/$S1"/*
  drop_caches
  request restored-plain-miss "$REQ_FN" "$LOOKUP" "restored-plain-miss-$i"
  labels+=("restored-plain-miss-$i")
done
enrich "${labels[@]}"
if jq -se 'map(select(.scenario == "restored-plain-miss")) | length > 0 and all(.http_code == 200 and .start_kind == "restored")' "$ATTEMPTS" >/dev/null; then
  check plaintext-cache-miss PASS "restore with S1's plaintext removed: decrypted from the sealed store and restored ($(jq -s 'map(select(.scenario == "restored-plain-miss") | .restore.restore_verify_ms)' "$ATTEMPTS" | tr -d '\n ') ms verify+decrypt)"
else
  check plaintext-cache-miss FAIL "$(jq -c 'select(.scenario == "restored-plain-miss") | {label, http_code, start_kind, err: (.invocation_error.message // .output.error.message // null)}' "$ATTEMPTS" | head -n 3 | tr '\n' ' ')"
fi

# --- first restore after creation vs the second, with revoke ------------------------------------
log "first/second cycles x$CYCLES"
CUR="$S1"
for k in $(seq 1 "$CYCLES"); do
  api POST "/v1/functions/$REQ_FN/snapshots/$CUR/revoke" '{"reason":"restore-verify cycle"}' >"$WORK/revoke.json"
  if [ "$k" = 1 ]; then
    request revoked "$REQ_FN" "$LOOKUP" revoked-1
    enrich revoked-1
    if [ "$(jq -r .state "$WORK/revoke.json")" = revoked ] && refused revoked-1 revoked; then
      check revoked PASS "POST .../revoke -> state revoked; require refused ($(last_attempt revoked-1 | jq -r '.output.error.message // .invocation_error.message' | head -c 200))"
    else
      check revoked FAIL "revoke=$(head -c 200 "$WORK/revoke.json") $(last_attempt revoked-1 | head -c 300)"
    fi
  fi
  CUR="$(snapshot_create "$REQ_FN" "C$k")"
  [ -n "$CUR" ] || { check "cycle-$k" FAIL "snapshot not created: $(head -c 300 "$WORK/snap.json")"; break; }
  snapshot_storage "$CUR" "C$k"
  request restored-first "$REQ_FN" "$LOOKUP" "restored-first-C$k"
  request restored-second "$REQ_FN" "$LOOKUP" "restored-second-C$k"
  enrich "restored-first-C$k" "restored-second-C$k"
done

# --- corrupt plaintext, require -----------------------------------------------------------------
flip_byte "$SNAP_ROOT/$CUR/memory" 4096
request corrupt-require "$REQ_FN" "$LOOKUP" corrupt-require
enrich corrupt-require
state="$(api GET "/v1/functions/$REQ_FN/snapshots" | jq -r --arg s "$CUR" '.items[] | select(.id == $s) | .state')"
if refused corrupt-require artifact_corrupted && [ "$state" = quarantined ]; then
  check corrupt-require PASS "flipped bit in plaintext memory: refused artifact_corrupted, $CUR quarantined"
else
  check corrupt-require FAIL "state=$state $(last_attempt corrupt-require | head -c 400)"
fi

# --- corrupt sealed store with the plaintext cache removed --------------------------------------
SEALED_SNAP="$(snapshot_create "$REQ_FN" sealed-corrupt)"
rm -f "$SNAP_ROOT/$SEALED_SNAP/memory"
sealed_file="$(find "$WORK/data/snapshots/${SEALED_SNAP:-none}" -name 'memory*' -type f 2>/dev/null | head -n1 || true)"
flip_byte "$sealed_file" $((1024 * 1024 + 17))
request corrupt-sealed "$REQ_FN" "$LOOKUP" corrupt-sealed
enrich corrupt-sealed
state="$(api GET "/v1/functions/$REQ_FN/snapshots" | jq -r --arg s "$SEALED_SNAP" '.items[] | select(.id == $s) | .state')"
if refused corrupt-sealed artifact_corrupted && [ "$state" = quarantined ]; then
  check corrupt-sealed PASS "flipped bit in $(basename "$sealed_file") with plaintext removed: refused artifact_corrupted (AES-GCM), quarantined"
else
  check corrupt-sealed FAIL "state=$state $(last_attempt corrupt-sealed | head -c 400)"
fi

# --- prefer: intact restores, corrupt falls back cold -------------------------------------------
PREF_FN="$(deploy rv-prefer prefer)"
PSNAP="$(snapshot_create "$PREF_FN" prefer)"
request prefer-restored "$PREF_FN" "$LOOKUP" prefer-restored
flip_byte "$SNAP_ROOT/$PSNAP/memory" 8192
request corrupt-prefer "$PREF_FN" "$LOOKUP" corrupt-prefer
enrich prefer-restored corrupt-prefer
if last_attempt prefer-restored | jq -e '.http_code == 200 and .start_kind == "restored"' >/dev/null &&
  last_attempt corrupt-prefer | jq -e '.http_code == 200 and .start_kind == "cold" and .output.restored == false' >/dev/null; then
  fb="$(last_attempt corrupt-prefer | jq -r '.restore.restore_fallback // empty')"
  if [ "$fb" = artifact_corrupted ]; then
    check corrupt-prefer PASS "intact prefer restored; after a flipped bit: HTTP 200 start_kind=cold, guest restored=false, restore_fallback=artifact_corrupted"
  else
    check corrupt-prefer FAIL "restore_fallback=$fb"
  fi
else
  check corrupt-prefer FAIL "$(last_attempt prefer-restored | jq -c '{http_code, start_kind}') $(last_attempt corrupt-prefer | jq -c '{http_code, start_kind, r: .output.restored}')"
fi

# --- revision change ----------------------------------------------------------------------------
NEW_SNAP="$(snapshot_create "$REQ_FN" pre-revision)"
deploy rv-restore require >/dev/null
request revision-stale "$REQ_FN" "$LOOKUP" revision-stale
enrich revision-stale
if [ -n "$NEW_SNAP" ] && refused revision-stale revision_mismatch; then
  check revision-stale PASS "active snapshot $NEW_SNAP of the previous revision refused after deploy (revision_mismatch)"
else
  check revision-stale FAIL "$(last_attempt revision-stale | head -c 400)"
fi

bash "$REPO_ROOT/scripts/kvm/bench-sample.sh" "$CG_PARENT" "$FC_RUN_DIR" "$CHROOT_BASE" "$GATEWAY_PID" 2>/dev/null |
  jq -c '{label: "phase-a-end"} + .' >>"$OUT/host-samples.jsonl" || true
stop_gateway

# ================================================================================================
# phase B: pool on (warm)
# ================================================================================================
log "warm: prime + x$WARM_N (250 ms apart)"
start_gateway "$WARM_CONFIG" b
request warm-prime "$COLD_FN" "$LOOKUP" warm-prime-1
enrich warm-prime-1
labels=()
for i in $(seq 1 "$WARM_N"); do
  sleep 0.25
  request warm "$COLD_FN" "$LOOKUP" "warm-$i"
  labels+=("warm-$i")
done
enrich "${labels[@]}"
bash "$REPO_ROOT/scripts/kvm/bench-sample.sh" "$CG_PARENT" "$FC_RUN_DIR" "$CHROOT_BASE" "$GATEWAY_PID" 2>/dev/null |
  jq -c '{label: "warm-pool"} + .' >>"$OUT/host-samples.jsonl" || true
if jq -se 'map(select(.scenario == "warm")) | all(.http_code == 200 and .output.dataset.lookup.ok and .output.dataset.spot_ok)' "$ATTEMPTS" >/dev/null; then
  check warm PASS "$(jq -s 'map(select(.scenario == "warm")) | {n: length, start_kinds: (group_by(.start_kind) | map({(.[0].start_kind // "none"): length}) | add)}' "$ATTEMPTS" | tr -d '\n ')"
else
  check warm FAIL "$(jq -c 'select(.scenario == "warm" and .http_code != 200) | {label, http_code}' "$ATTEMPTS" | head -n 3 | tr '\n' ' ')"
fi
calibrate end
stop_gateway

# --- teardown stats from the gateway logs -------------------------------------------------------
cat "$OUT"/gateway-*.log | jq -c 'select(.message == "cgroup stats at teardown") |
  {env: .env_id, stats: (.stats | fromjson? // null)}' >"$OUT/teardown-stats.jsonl" 2>/dev/null || true

# --- cleanup + leftovers ------------------------------------------------------------------------
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""
rm -rf "$SNAP_ROOT" "$WORK/data/snapshots"
{
  echo "# leftovers after the run (gateways stopped, snapshots removed)"
  echo "gateway: $(pgrep -af tachyon-serverless-gateway || echo none)"
  echo "firecracker: $(pgrep -a firecracker || echo none)"
  echo "jailer: $(pgrep -a jailer || echo none)"
  echo "jails: $(find "$CHROOT_BASE/firecracker" -mindepth 1 -maxdepth 1 2>/dev/null | tr '\n' ' ' || true)"
  echo "cgroups: $(find "$CG_PARENT" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | tr '\n' ' ' || true)"
  echo "env dirs: $(find "$FC_RUN_DIR" -mindepth 1 -maxdepth 1 -name 'env_*' 2>/dev/null | tr '\n' ' ' || true)"
  echo "snapshot dirs: $(find "$SNAP_ROOT" -mindepth 1 -maxdepth 1 2>/dev/null | tr '\n' ' ' || true)"
  echo "listening on $PORT: $(ss -ltn "sport = :$PORT" 2>/dev/null | tail -n +2 || true)"
} >"$OUT/leftovers.txt"
LEFT=0
if pgrep -x firecracker >/dev/null || pgrep -f tachyon-serverless-gateway >/dev/null ||
  [ -n "$(find "$CHROOT_BASE/firecracker" -mindepth 1 -maxdepth 1 2>/dev/null)" ] ||
  [ -n "$(find "$CG_PARENT" -mindepth 1 -maxdepth 1 -type d 2>/dev/null)" ] ||
  [ -n "$(find "$FC_RUN_DIR" -mindepth 1 -maxdepth 1 -name 'env_*' 2>/dev/null)" ]; then
  check cleanup FAIL "see leftovers.txt"
  LEFT=1
else
  check cleanup PASS "no gateway, firecracker, jailer, jail, cgroup, env dir or snapshot left"
fi

# --- report -------------------------------------------------------------------------------------
bash "$REPO_ROOT/scripts/x1/restore-verify-report.sh" "$OUT" >/dev/null || log "report failed"
log "checks: $CHECKS (failures: $FAILS); summary: $OUT/summary.md"
if [ "$FAILS" != 0 ]; then exit 1; fi
if [ "$LEFT" != 0 ]; then exit 3; fi
exit 0

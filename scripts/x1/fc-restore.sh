#!/usr/bin/env bash
# scripts/x1/fc-restore.sh - X1 (PLT-4652, experimental): Firecracker snapshot / restore / clone
# of a microVM that runs the runtime bridge + examples/restore-aware.
#
# Scenarios (each writes <out>/<scenario>/):
#   cold-N      baseline: cold boot through the same lifecycle (host answers `cold`), N=COLD_RUNS
#   source      cold boot to the checkpoint wait point (process blocked in lifecycle `continue`),
#               PATCH /vm Paused, PUT /snapshot/create Full, copy the scratch drive while paused,
#               then resume the source to show SnapshotCreate reset its vsock connection too
#   clone-a/b   two VMMs load the same snapshot at the same time, each in its own directory with
#               its own scratch copy and vsock UDS (drive / vsock paths in the snapshot are
#               relative, so they resolve per VMM cwd); host answers `restored` with a distinct
#               instance id delivered after the restore
#   restore-N   sequential restores for timings: restore-1 without the doorbell (the guest notices
#               the vsock reset only at its next write, the 5 s bridge heartbeat), the others ring
#               the guest doorbell (host connects to guest vsock port 5001 right after the load);
#               the last one also uses vsock_override
#   neg-missing-drive  load without the scratch drive file at the recorded path (expected error)
#   bridge-*    the product bridge (no pump) snapshotted after Ready and restored: records what
#               the unmodified bridge does after a vsock transport reset
#
# A restore is only reported when the restored guest's first frame is `x1_reconnect` carrying
# the source's guest_boot_id. A `hello` on a restored VMM means the guest booted and x1-host
# exits 3 (never counted as a restore).
#
# Usage:   scripts/x1/fc-restore.sh [out_dir]
# Env:     COLD_RUNS (3), RESTORE_RUNS (3), MEM_MIB (256), KEEP_WORK (0)
# Needs:   Linux + /dev/kvm (rw), .kvm/bin/firecracker, .kvm/vmlinux (scripts/kvm/bootstrap.sh),
#          cargo with the <arch>-unknown-linux-musl target, curl, jq, debugfs, mkfs.ext4.
# Output:  summary.tsv, summary.json, per-scenario host.jsonl / console.log / fc.log / api.log.
# Exit:    0 when every expected outcome was observed, 1 otherwise, 2 on missing prerequisites.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${1:-$REPO_ROOT/docs/evidence/x1-restore-$STAMP/fc}"
COLD_RUNS="${COLD_RUNS:-3}"
RESTORE_RUNS="${RESTORE_RUNS:-3}"
MEM_MIB="${MEM_MIB:-256}"
KEEP_WORK="${KEEP_WORK:-0}"
FC="$REPO_ROOT/.kvm/bin/firecracker"
KERNEL="$REPO_ROOT/.kvm/vmlinux"
WORK="$REPO_ROOT/.kvm/x1/fc-$STAMP"
BOOT_ARGS_BASE="console=ttyS0 reboot=k panic=1 pci=off"
if [ "$ARCH" = "aarch64" ]; then BOOT_ARGS_BASE="$BOOT_ARGS_BASE keep_bootcon"; fi
BOOT_ARGS="$BOOT_ARGS_BASE init=/sbin/tachyon-init tachyon.env_id=env_x1restore tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb tachyon.scratch_dev=/dev/vdc"

log() { printf '[x1-fc] %s\n' "$*" >&2; }
# uutils date (Ubuntu 26.04) ignores the width in %3N; derive ms from %N.
now_ms() { echo $(($(date +%s%N) / 1000000)); }

for tool in curl jq debugfs mkfs.ext4 cargo; do
  command -v "$tool" >/dev/null 2>&1 || { log "missing $tool"; exit 2; }
done
for f in "$FC" "$KERNEL"; do
  [ -f "$f" ] || { log "missing $f (run scripts/kvm/bootstrap.sh)"; exit 2; }
done
if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then
  log "/dev/kvm is not readable and writable"
  exit 2
fi

mkdir -p "$OUT" "$WORK/base"
PIDS=()
cleanup() {
  local p
  for p in "${PIDS[@]:-}"; do
    if [ -n "$p" ]; then kill -9 "$p" 2>/dev/null || true; fi
  done
  if [ "$KEEP_WORK" != "1" ]; then rm -rf "$WORK"; fi
}
trap cleanup EXIT

# --- build -------------------------------------------------------------------------------
log "building guest binaries ($MUSL_TARGET) and x1-host"
cargo build --release --target "$MUSL_TARGET" \
  -p tachyon-serverless-runtime-bridge -p example-restore-aware \
  -p tachyon-serverless-x1-restore --bin x1-guest-init >&2
cargo build --release -p tachyon-serverless-x1-restore --bin x1-host >&2
GUEST_BIN="$REPO_ROOT/target/$MUSL_TARGET/release"
X1_HOST="$REPO_ROOT/target/release/x1-host"

# --- shared read-only images ---------------------------------------------------------------
BRIDGE_BIN="$GUEST_BIN/x1-guest-init" ROOTFS="$WORK/base/rootfs-x1.ext4" \
  scripts/kvm/build-rootfs.sh >&2
BRIDGE_BIN="$GUEST_BIN/tachyon-serverless-runtime-bridge" ROOTFS="$WORK/base/rootfs-bridge.ext4" \
  scripts/kvm/build-rootfs.sh >&2
mkdir -p "$WORK/base/stage"
install -m 0755 "$GUEST_BIN/example-restore-aware" "$WORK/base/stage/app"
truncate -s 32M "$WORK/base/function.ext4"
mkfs.ext4 -q -F -d "$WORK/base/stage" "$WORK/base/function.ext4" 32M
rm -rf "$WORK/base/stage"
fallocate -l 64M "$WORK/base/scratch-empty.ext4"
mkfs.ext4 -q -F -b 4096 -m 0 -O ^has_journal -E nodiscard,lazy_itable_init=0 \
  -L tachyon-scratch "$WORK/base/scratch-empty.ext4" 64M
chmod a-w "$WORK/base/rootfs-x1.ext4" "$WORK/base/rootfs-bridge.ext4" "$WORK/base/function.ext4"

{
  echo "firecracker $("$FC" --version 2>/dev/null | head -n1)"
  echo "firecracker_sha256 $(sha256sum "$FC" | awk '{print $1}')"
  echo "kernel_sha256 $(sha256sum "$KERNEL" | awk '{print $1}')"
  echo "kernel_file $(file -b "$KERNEL")"
  for f in rootfs-x1.ext4 rootfs-bridge.ext4 function.ext4 scratch-empty.ext4; do
    echo "$f $(sha256sum "$WORK/base/$f" | awk '{print $1}')"
  done
  for b in x1-guest-init tachyon-serverless-runtime-bridge example-restore-aware; do
    echo "$b $(sha256sum "$GUEST_BIN/$b" | awk '{print $1}')"
  done
  echo "host_uname $(uname -srm)"
  echo "host_cpu $(grep -m1 -i -E 'model name|CPU part' /proc/cpuinfo | sed 's/.*: //')"
  echo "commit $(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "boot_args $BOOT_ARGS"
  echo "mem_mib $MEM_MIB vcpus 1"
} >"$OUT/versions.txt"

# --- helpers -------------------------------------------------------------------------------
# api <dir> <method> <path> [json] -> prints "<http_code> <ms>" and appends the exchange to api.log
api() {
  local dir="$1" method="$2" path="$3" body="${4:-}" start code end
  start="$(now_ms)"
  if [ -n "$body" ]; then
    code="$(curl -sS -o "$dir/.api-body" -w '%{http_code}' --unix-socket "$dir/api.sock" \
      -X "$method" "http://localhost$path" -H 'Content-Type: application/json' -d "$body" || echo 000)"
  else
    code="$(curl -sS -o "$dir/.api-body" -w '%{http_code}' --unix-socket "$dir/api.sock" \
      -X "$method" "http://localhost$path" || echo 000)"
  fi
  end="$(now_ms)"
  {
    printf '%s %s %s -> %s (%s ms)\n' "$(date -u +%H:%M:%S.%3N)" "$method" "$path" "$code" "$((end - start))"
    [ -n "$body" ] && printf '  request: %s\n' "$body"
    [ -s "$dir/.api-body" ] && printf '  response: %s\n' "$(cat "$dir/.api-body")"
    true
  } >>"$dir/api.log"
  rm -f "$dir/.api-body"
  printf '%s %s\n' "$code" "$((end - start))"
}

api_ok() { # dir method path [json]
  local r
  r="$(api "$@")"
  case "${r%% *}" in
    2??) return 0 ;;
    *) log "API $2 $3 failed: $r (see $1/api.log)"; return 1 ;;
  esac
}

# vm_dir <name> <rootfs> [scratch source] -> creates $WORK/<name> with relative drive names
vm_dir() {
  local rootfs="$2" scratch="${3:-$WORK/base/scratch-empty.ext4}" d="$WORK/$1"
  mkdir -p "$d"
  ln -sf "$rootfs" "$d/rootfs.ext4"
  ln -sf "$WORK/base/function.ext4" "$d/function.ext4"
  cp --sparse=always "$scratch" "$d/scratch.ext4"
  printf '%s\n' "$d"
}

start_fc() { # dir -> sets FC_PID
  local d="$1"
  : >"$d/fc.log"
  (cd "$d" && exec "$FC" --api-sock api.sock --log-path fc.log --level Info --show-level \
    >console.log 2>&1) &
  FC_PID=$!
  PIDS+=("$FC_PID")
  for _ in $(seq 1 100); do
    [ -S "$d/api.sock" ] && return 0
    sleep 0.02
  done
  log "firecracker API socket did not appear in $d"
  return 1
}

start_host() { # dir mode [args...] -> sets HOST_PID
  local d="$1" mode="$2"
  shift 2
  "$X1_HOST" --listen "$d/v.sock_5000" --mode "$mode" "$@" >"$d/host.jsonl" 2>"$d/host.stderr" &
  HOST_PID=$!
  PIDS+=("$HOST_PID")
  for _ in $(seq 1 100); do
    [ -S "$d/v.sock_5000" ] && return 0
    sleep 0.02
  done
  log "x1-host did not listen in $d"
  return 1
}

configure_and_start() { # dir
  local d="$1"
  api_ok "$d" PUT /machine-config "{\"vcpu_count\":1,\"mem_size_mib\":$MEM_MIB}"
  api_ok "$d" PUT /boot-source "{\"kernel_image_path\":\"$KERNEL\",\"boot_args\":\"$BOOT_ARGS\"}"
  api_ok "$d" PUT /drives/rootfs '{"drive_id":"rootfs","path_on_host":"rootfs.ext4","is_root_device":true,"is_read_only":true}'
  api_ok "$d" PUT /drives/function '{"drive_id":"function","path_on_host":"function.ext4","is_root_device":false,"is_read_only":true}'
  api_ok "$d" PUT /drives/scratch '{"drive_id":"scratch","path_on_host":"scratch.ext4","is_root_device":false,"is_read_only":false}'
  api_ok "$d" PUT /vsock '{"guest_cid":3,"uds_path":"v.sock"}'
  api_ok "$d" PUT /actions '{"action_type":"InstanceStart"}'
}

wait_event() { # dir event|type timeout_s -> 0 when host.jsonl has an event (or frame type) of that name
  local d="$1" ev="$2" t="$3"
  for _ in $(seq 1 $((t * 20))); do
    grep -q -E "\"(event|type)\":\"$ev\"" "$d/host.jsonl" 2>/dev/null && return 0
    sleep 0.05
  done
  return 1
}

event_ms() { # dir event [n-th, 1-based] -> host_wall_ms of that event
  jq -r --arg e "$2" 'select(.event == $e) | .host_wall_ms' "$1/host.jsonl" | sed -n "${3:-1}p"
}

wait_pid() { # pid timeout_s -> 0 if exited
  local p="$1" t="$2"
  for _ in $(seq 1 $((t * 20))); do
    kill -0 "$p" 2>/dev/null || return 0
    sleep 0.05
  done
  return 1
}

stop_pid() { # pid
  kill -9 "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

# host exit code via its "exit" / "error" event
host_code() {
  local c
  c="$(jq -r 'select(.event == "exit") | .code' "$1/host.jsonl" | tail -n1)"
  if [ -n "$c" ]; then echo "$c"; elif grep -q '"event":"error"' "$1/host.jsonl"; then echo 1; else echo none; fi
}

collect() { # scenario dir
  local s="$1" d="$2" dst="$OUT/$1"
  mkdir -p "$dst"
  for f in host.jsonl host.stderr console.log fc.log api.log; do
    [ -f "$d/$f" ] && cp "$d/$f" "$dst/$f"
  done
  return 0
}

RESULTS="$OUT/summary.tsv"
printf 'scenario\tresult\tmetric_ms\tdetail\n' >"$RESULTS"
FAILS=0
record() { # scenario result metric detail ; result in PASS/FAIL/INFO
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >>"$RESULTS"
  log "$1: $2 $3 $4"
  if [ "$2" = FAIL ]; then FAILS=$((FAILS + 1)); fi
  return 0
}

# --- 1. cold baseline ----------------------------------------------------------------------
for n in $(seq 1 "$COLD_RUNS"); do
  s="cold-$n"
  d="$(vm_dir "$s" "$WORK/base/rootfs-x1.ext4")"
  start_host "$d" cold --instance-id unused --invokes 1 --deadline-secs 90
  start_fc "$d"
  t0="$(now_ms)"
  configure_and_start "$d"
  if wait_event "$d" exit 90 && [ "$(host_code "$d")" = 0 ]; then
    ready="$(jq -r 'select(.event == "guest_frame" and .type == "ready") | .host_wall_ms' "$d/host.jsonl" | head -n1)"
    record "$s" PASS "$((ready - t0))" "InstanceStart request -> Ready (cold boot incl. bootstrap)"
  else
    record "$s" FAIL "" "cold run did not finish (host code $(host_code "$d"))"
  fi
  wait_pid "$FC_PID" 10 || stop_pid "$FC_PID"
  stop_pid "$HOST_PID"
  collect "$s" "$d"
  rm -rf "$d"
done

# --- 2. source: boot to the checkpoint wait point and snapshot -----------------------------
SNAP="$WORK/snap"
mkdir -p "$SNAP"
d="$(vm_dir source "$WORK/base/rootfs-x1.ext4")"
start_host "$d" source --deadline-secs 600
start_fc "$d"
SRC_FC="$FC_PID"
SRC_HOST="$HOST_PID"
t0="$(now_ms)"
configure_and_start "$d"
if ! wait_event "$d" checkpoint_wait 90; then
  record source FAIL "" "source never reached the checkpoint wait point"
  collect source "$d"
  exit 1
fi
t_wait="$(event_ms "$d" checkpoint_wait)"
record source-boot-to-checkpoint INFO "$((t_wait - t0))" "InstanceStart request -> x1_waiting"
sleep 1
api_ok "$d" PATCH /vm '{"state":"Paused"}'
r="$(api "$d" PUT /snapshot/create '{"snapshot_type":"Full","snapshot_path":"../snap/vmstate","mem_file_path":"../snap/mem"}')"
case "${r%% *}" in
  2??) record snapshot-create PASS "${r##* }" "PUT /snapshot/create Full (paused, ${MEM_MIB} MiB)" ;;
  *) record snapshot-create FAIL "${r##* }" "HTTP ${r%% *}: $(tail -n1 "$d/api.log")"; collect source "$d"; exit 1 ;;
esac
# Disk captured at the same pause point: the guest cannot write while paused.
t_copy="$(now_ms)"
cp --sparse=always "$d/scratch.ext4" "$SNAP/scratch.ext4"
record snapshot-disk-copy INFO "$(($(now_ms) - t_copy))" "scratch drive copied while paused (64 MiB sparse)"
sync
SNAP_SHA_BEFORE="$(sha256sum "$SNAP/vmstate" "$SNAP/mem" "$SNAP/scratch.ext4")"
{
  ls -ls "$SNAP"
  sha256sum "$SNAP/vmstate" "$SNAP/mem" "$SNAP/scratch.ext4"
} >"$OUT/snapshot-files.txt"
du -k --apparent-size "$SNAP/mem" >>"$OUT/snapshot-files.txt"
# Resume the source: SnapshotCreate reset its vsock connection as well (upstream doc).
api_ok "$d" PATCH /vm '{"state":"Resumed"}'
if wait_event "$d" x1_reconnect 15; then
  record source-resume-vsock-reset PASS "" "resumed source lost its vsock connection and reconnected (x1_reconnect)"
else
  record source-resume-vsock-reset FAIL "" "no x1_reconnect from the resumed source"
fi
SRC_BOOT_ID="$(jq -r 'select(.event == "guest_frame" and .type == "hello") | .guest_boot_id' "$d/host.jsonl" | head -n1)"
stop_pid "$SRC_FC"
stop_pid "$SRC_HOST"
collect source "$d"
rm -rf "$d"
log "source guest_boot_id=$SRC_BOOT_ID; waiting 5 s so the snapshot's wall clock is visibly stale"
sleep 5

scratch_marker() { # image -> content of /x1-instance, or the debugfs error without its banner
  debugfs -R 'cat /x1-instance' "$1" 2>&1 | grep -v '^debugfs [0-9]' | tr -d '\n'
}

# restored-copy checks: identity after restore, boot id, marker on its own scratch copy
check_clone() { # scenario dir instance_id t_load_start
  local s="$1" d="$2" iid="$3" t0="$4" code boot ready resp marker
  code="$(host_code "$d")"
  if [ "$code" = 3 ]; then
    record "$s" FAIL "" "guest cold-booted on a restore VMM (hello instead of x1_reconnect): NOT a restore"
    return 0
  fi
  if [ "$code" != 0 ]; then
    record "$s" FAIL "" "host code $code: $(jq -r 'select(.event == "error") | .message' "$d/host.jsonl" | tail -n1)"
    return 0
  fi
  boot="$(jq -r 'select(.event == "guest_control" and .type == "x1_reconnect") | .guest_boot_id' "$d/host.jsonl" | head -n1)"
  ready="$(jq -r 'select(.event == "guest_frame" and .type == "ready") | .host_wall_ms' "$d/host.jsonl" | head -n1)"
  resp="$(jq -c 'select(.event == "guest_frame" and .type == "response") | .payload' "$d/host.jsonl" | head -n1)"
  if [ "$boot" != "$SRC_BOOT_ID" ]; then
    record "$s" FAIL "" "guest_boot_id $boot differs from the source $SRC_BOOT_ID"
    return 0
  fi
  if ! printf '%s' "$resp" | jq -e --arg i "$iid" '.restored == true and (.instance_id | startswith($i))' >/dev/null; then
    record "$s" FAIL "" "response does not carry restored=true and instance $iid: $resp"
    return 0
  fi
  marker="$(scratch_marker "$d/scratch.ext4")"
  if [ "$marker" != "$iid" ]; then
    record "$s" FAIL "" "scratch marker '$marker' != '$iid'"
    return 0
  fi
  local reconnect lost
  reconnect="$(jq -r 'select(.event == "guest_control" and .type == "x1_reconnect") | .host_wall_ms' "$d/host.jsonl" | head -n1)"
  lost="$(jq -r 'select(.event == "guest_control" and .type == "x1_reconnect") | .lost' "$d/host.jsonl" | head -n1)"
  record "$s" PASS "$((ready - t0))" "load request -> Ready (load -> x1_reconnect $((reconnect - t0)) ms, detected by: $lost); boot_id same as source; restored=true; own scratch marker"
  printf '%s\t%s\n' "$s" "$resp" >>"$OUT/responses.tsv"
  jq -c --arg s "$s" 'select(.event == "guest_control" and .type == "x1_reconnect") | {scenario: $s, host_wall_ms, clocks, urandom_hex, lost}' \
    "$d/host.jsonl" >>"$OUT/restore-clocks.jsonl"
}

load_body() { # [vsock override path]
  local extra=""
  if [ -n "${1:-}" ]; then extra=",\"vsock_override\":{\"uds_path\":\"$1\"}"; fi
  printf '{"snapshot_path":"../snap/vmstate","mem_backend":{"backend_type":"File","backend_path":"../snap/mem"},"resume_vm":true%s}' "$extra"
}

# --- 3. two concurrent clones --------------------------------------------------------------
declare -A CL_FC CL_HOST CL_T0
for c in a b; do
  d="$(vm_dir "clone-$c" "$WORK/base/rootfs-x1.ext4" "$SNAP/scratch.ext4")"
  start_host "$d" clone --instance-id "clone-$c" --invokes 2 --linger-ms 3000 --deadline-secs 60 \
    --doorbell-uds "$d/v.sock"
  CL_HOST[$c]="$HOST_PID"
  start_fc "$d"
  CL_FC[$c]="$FC_PID"
done
for c in a b; do
  d="$WORK/clone-$c"
  CL_T0[$c]="$(now_ms)"
  r="$(api "$d" PUT /snapshot/load "$(load_body)")"
  record "clone-$c-load-call" INFO "${r##* }" "PUT /snapshot/load HTTP ${r%% *}"
done
sleep 1.5
{
  echo "# both clone VMMs alive at the same time, both mapping the one snapshot memory file"
  for c in a b; do
    p="${CL_FC[$c]}"
    echo "clone-$c firecracker pid $p alive=$(kill -0 "$p" 2>/dev/null && echo yes || echo no)"
    grep -F "$SNAP/mem" "/proc/$p/maps" 2>/dev/null | head -n 3 || true
    awk '/^(Rss|Pss|Shared_Clean|Private_Dirty):/ {print "  " $0}' "/proc/$p/smaps_rollup" 2>/dev/null || true
  done
} >"$OUT/clones-concurrent.txt"
for c in a b; do
  d="$WORK/clone-$c"
  wait_event "$d" exit 60 || true
  check_clone "clone-$c" "$d" "clone-$c" "${CL_T0[$c]}"
  wait_pid "${CL_FC[$c]}" 10 || stop_pid "${CL_FC[$c]}"
  stop_pid "${CL_HOST[$c]}"
done
{
  echo "# scratch drive marker per copy (debugfs cat /x1-instance)"
  echo "snapshot copy: $(scratch_marker "$SNAP/scratch.ext4")"
  for c in a b; do
    echo "clone-$c: $(scratch_marker "$WORK/clone-$c/scratch.ext4")"
  done
  echo "# snapshot files after the clones ran (must equal snapshot-files.txt)"
  sha256sum "$SNAP/vmstate" "$SNAP/mem" "$SNAP/scratch.ext4"
} >"$OUT/clones-disk.txt"
SNAP_SHA_AFTER="$(sha256sum "$SNAP/vmstate" "$SNAP/mem" "$SNAP/scratch.ext4")"
if [ "$SNAP_SHA_AFTER" = "$SNAP_SHA_BEFORE" ] &&
  grep -q '^snapshot copy: .*File not found' "$OUT/clones-disk.txt" &&
  grep -qx 'clone-a: clone-a' "$OUT/clones-disk.txt" &&
  grep -qx 'clone-b: clone-b' "$OUT/clones-disk.txt"; then
  record clones-disk-isolation PASS "" "each clone wrote only its own scratch copy; snapshot files unchanged"
else
  record clones-disk-isolation FAIL "" "see clones-disk.txt"
fi
for c in a b; do collect "clone-$c" "$WORK/clone-$c"; rm -rf "${WORK:?}/clone-$c"; done

# --- 4. sequential restores (the last one with vsock_override) ------------------------------
for n in $(seq 1 "$RESTORE_RUNS"); do
  s="restore-$n"
  d="$(vm_dir "$s" "$WORK/base/rootfs-x1.ext4" "$SNAP/scratch.ext4")"
  override=""
  uds="$d/v.sock"
  if [ "$n" = "$RESTORE_RUNS" ] && [ "$n" -gt 1 ]; then
    override="$d/override.sock"
    uds="$override"
  fi
  doorbell=(--doorbell-uds "$uds")
  if [ "$n" = 1 ]; then doorbell=(); fi
  "$X1_HOST" --listen "${uds}_5000" --mode clone --instance-id "$s" --invokes 1 --deadline-secs 60 \
    "${doorbell[@]}" >"$d/host.jsonl" 2>"$d/host.stderr" &
  HOST_PID=$!
  PIDS+=("$HOST_PID")
  wait_event "$d" listening 5
  start_fc "$d"
  t0="$(now_ms)"
  r="$(api "$d" PUT /snapshot/load "$(load_body "$override")")"
  record "$s-load-call" INFO "${r##* }" "PUT /snapshot/load HTTP ${r%% *}${override:+ (vsock_override)}${doorbell[*]:+ (doorbell)}"
  wait_event "$d" exit 60 || true
  check_clone "$s" "$d" "$s" "$t0"
  wait_pid "$FC_PID" 10 || stop_pid "$FC_PID"
  stop_pid "$HOST_PID"
  collect "$s" "$d"
  rm -rf "$d"
done

# --- 5. negative: the scratch drive is not at the recorded relative path -------------------
s="neg-missing-drive"
d="$(vm_dir "$s" "$WORK/base/rootfs-x1.ext4" "$SNAP/scratch.ext4")"
rm -f "$d/scratch.ext4"
start_fc "$d"
r="$(api "$d" PUT /snapshot/load "$(load_body)")"
wait_pid "$FC_PID" 5 || stop_pid "$FC_PID"
case "${r%% *}" in
  2??) record "$s" FAIL "" "load succeeded without the scratch drive" ;;
  *) record "$s" PASS "" "HTTP ${r%% *}: $(grep -m1 'response:' "$d/api.log" | sed 's/^ *response: //')" ;;
esac
collect "$s" "$d"
rm -rf "$d"

# --- 6. the product bridge (no pump) --------------------------------------------------------
BSNAP="$WORK/bsnap"
mkdir -p "$BSNAP"
d="$(vm_dir bridge-source "$WORK/base/rootfs-bridge.ext4")"
start_host "$d" bridge --invokes 1 --deadline-secs 120
start_fc "$d"
B_FC="$FC_PID"
B_HOST="$HOST_PID"
configure_and_start "$d"
if wait_event "$d" bridge_idle 90; then
  api_ok "$d" PATCH /vm '{"state":"Paused"}'
  r="$(api "$d" PUT /snapshot/create '{"snapshot_type":"Full","snapshot_path":"../bsnap/vmstate","mem_file_path":"../bsnap/mem"}')"
  cp --sparse=always "$d/scratch.ext4" "$BSNAP/scratch.ext4"
  record bridge-snapshot-create INFO "${r##* }" "product bridge after Ready: HTTP ${r%% *}"
else
  record bridge-source FAIL "" "product bridge did not become idle"
fi
stop_pid "$B_FC"
stop_pid "$B_HOST"
collect bridge-source "$d"
rm -rf "$d"
if [ -f "$BSNAP/vmstate" ]; then
  s="bridge-restore"
  d="$(vm_dir "$s" "$WORK/base/rootfs-bridge.ext4" "$BSNAP/scratch.ext4")"
  start_host "$d" listen --deadline-secs 20
  start_fc "$d"
  t0="$(now_ms)"
  r="$(api "$d" PUT /snapshot/load '{"snapshot_path":"../bsnap/vmstate","mem_backend":{"backend_type":"File","backend_path":"../bsnap/mem"},"resume_vm":true}')"
  if wait_pid "$FC_PID" 20; then
    wait "$FC_PID" 2>/dev/null && fc_rc=0 || fc_rc=$?
    exited="firecracker exited after $(($(now_ms) - t0)) ms (rc $fc_rc)"
  else
    exited="firecracker still running after 20 s"
    stop_pid "$FC_PID"
  fi
  wait_pid "$HOST_PID" 25 || stop_pid "$HOST_PID"
  conns="$(grep -c '"event":"accepted"' "$d/host.jsonl" || true)"
  bridge_err="$(grep -m1 -E 'transport|connection|exit' "$d/console.log" | tr -d '\r' | cut -c1-200 || true)"
  if [ "${r%% *}" = 204 ] && [ "$conns" = 0 ]; then
    record "$s" PASS "" "load HTTP 204; host connections after restore: 0; $exited; console: ${bridge_err:-<none>}"
  else
    record "$s" INFO "" "load HTTP ${r%% *}; host connections: $conns; $exited; console: ${bridge_err:-<none>}"
  fi
  collect "$s" "$d"
  rm -rf "$d"
fi

# --- summary --------------------------------------------------------------------------------
jq -R -s 'split("\n") | map(select(length > 0) | split("\t")) | .[1:] |
  map({scenario: .[0], result: .[1], metric_ms: (.[2] | tonumber? // null), detail: .[3]})' \
  "$RESULTS" >"$OUT/summary.json"
{
  echo "# leftovers after the run"
  pgrep -a firecracker || echo "firecracker processes: none"
  pgrep -a x1-host || echo "x1-host processes: none"
} >"$OUT/leftovers.txt"
log "results: $RESULTS (failures: $FAILS)"
[ "$FAILS" = 0 ]

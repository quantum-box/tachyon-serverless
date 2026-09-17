#!/usr/bin/env bash
# scripts/x1/ch-restore.sh - X1 (PLT-4652, experimental): Cloud Hypervisor snapshot / restore /
# clone of the same guest (x1-guest-init + examples/restore-aware) that scripts/x1/fc-restore.sh
# uses, with a pinned static Cloud Hypervisor release.
#
# Steps (each writes <out>/<step>/):
#   fetch     download cloud-hypervisor-static-<arch> and ch-remote-static-<arch> for CH_VERSION and
#             verify them against CH_SHA256 / CH_REMOTE_SHA256 (the GitHub release asset digests)
#   cold-N    boot the Firecracker CI kernel (arm64 Image) directly (no firmware), host answers
#             `cold`; N=COLD_RUNS
#   source    boot to the checkpoint wait point, `ch-remote pause`, `ch-remote snapshot
#             file://<dir>`, copy the scratch drive while paused
#   clone-a/b two fresh cloud-hypervisor processes `--restore source_url=file://<dir>` in their own
#             directories (disk / vsock paths in config.json are relative), `ch-remote resume`,
#             doorbell, restored identity, own scratch marker
#
# The same rule as the Firecracker script applies: only a guest whose first frame on the restored
# VMM is `x1_reconnect` with the source's guest_boot_id counts as a restore.
#
# Usage:   scripts/x1/ch-restore.sh [out_dir]
# Env:     CH_VERSION (v53.0), CH_SHA256 / CH_REMOTE_SHA256 (pinned for v53.0 aarch64),
#          COLD_RUNS (2), MEM_MIB (256), KEEP_WORK (0),
#          CH_CONSOLE (tty: guest console on hvc0 into console.log; off: no guest console device)
# Needs:   Linux aarch64 + /dev/kvm (rw), .kvm/vmlinux, cargo (musl target), curl, jq, debugfs.
# Exit:    0 when every step behaved as recorded in summary.tsv without FAIL, 1 otherwise,
#          2 on missing prerequisites or a checksum mismatch.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${1:-$REPO_ROOT/docs/evidence/x1-restore-$STAMP/ch}"
CH_VERSION="${CH_VERSION:-v53.0}"
# GitHub release asset digests for v53.0 (gh release view v53.0 --json assets).
CH_SHA256="${CH_SHA256:-f192b510eea1c710cbc439d716bb0573c223fc463dbe3e6523788a2b7ef62850}"
CH_REMOTE_SHA256="${CH_REMOTE_SHA256:-ade26617f74264467e1381f146fd1face6b8b0fb13c5ec84f4acedd72f972596}"
COLD_RUNS="${COLD_RUNS:-2}"
MEM_MIB="${MEM_MIB:-256}"
KEEP_WORK="${KEEP_WORK:-0}"
KERNEL="$REPO_ROOT/.kvm/vmlinux"
CH_DIR="$REPO_ROOT/.kvm/ch/$CH_VERSION"
WORK="$REPO_ROOT/.kvm/x1/ch-$STAMP"
# virtio console (the Firecracker CI kernel has no PL011 driver); no `pci=off` (Cloud Hypervisor exposes virtio devices over PCI); `root=` is
# explicit because, unlike Firecracker, Cloud Hypervisor does not add it for the root drive;
# `loglevel=1` because with kernel messages on hvc0 the guest stalled right after the first
# virtio-blk probe on this host (console.log ends at "[vda] 131072 512-byte logical blocks").
# Disks declare image_type=raw: an auto-detected raw image gets sector 0 write-protected and
# mounting the scratch ext4 read-write then fails with EIO (block 0 holds the superblock).
CH_CONSOLE="${CH_CONSOLE:-tty}"
CONSOLE_ARG="console=hvc0 loglevel=1"
if [ "$CH_CONSOLE" = off ]; then CONSOLE_ARG="loglevel=1"; fi
CMDLINE="$CONSOLE_ARG root=/dev/vda ro reboot=k panic=1 keep_bootcon init=/sbin/tachyon-init tachyon.env_id=env_x1restore tachyon.vsock_port=5000 tachyon.function_dev=/dev/vdb tachyon.scratch_dev=/dev/vdc"

log() { printf '[x1-ch] %s\n' "$*" >&2; }
now_ms() { echo $(($(date +%s%N) / 1000000)); }

if [ "$ARCH" != "aarch64" ]; then
  log "pinned digests are for aarch64; set CH_SHA256 / CH_REMOTE_SHA256 for $ARCH"
  exit 2
fi
for tool in curl jq debugfs mkfs.ext4 cargo sha256sum; do
  command -v "$tool" >/dev/null 2>&1 || { log "missing $tool"; exit 2; }
done
[ -f "$KERNEL" ] || { log "missing $KERNEL (run scripts/kvm/bootstrap.sh)"; exit 2; }
if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then
  log "/dev/kvm is not readable and writable"
  exit 2
fi

mkdir -p "$OUT" "$WORK/base" "$CH_DIR"
PIDS=()
cleanup() {
  local p
  for p in "${PIDS[@]:-}"; do
    if [ -n "$p" ]; then kill -9 "$p" 2>/dev/null || true; fi
  done
  if [ "$KEEP_WORK" != "1" ]; then rm -rf "$WORK"; fi
}
trap cleanup EXIT

RESULTS="$OUT/summary.tsv"
printf 'scenario\tresult\tmetric_ms\tdetail\n' >"$RESULTS"
FAILS=0
record() { # scenario PASS|FAIL|INFO|UNSUPPORTED metric detail
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >>"$RESULTS"
  log "$1: $2 $3 $4"
  if [ "$2" = FAIL ]; then FAILS=$((FAILS + 1)); fi
  return 0
}

# --- fetch --------------------------------------------------------------------------------
fetch() { # asset expected_sha dest
  local url="https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/$CH_VERSION/$1"
  if [ ! -f "$3" ]; then
    curl -sSfL -o "$3.part" "$url"
    mv "$3.part" "$3"
  fi
  local got
  got="$(sha256sum "$3" | awk '{print $1}')"
  if [ "$got" != "$2" ]; then
    log "checksum mismatch for $1: got $got want $2"
    rm -f "$3"
    exit 2
  fi
  chmod 0755 "$3"
  printf '%s %s %s\n' "$1" "$got" "$url" >>"$OUT/versions.txt"
}
: >"$OUT/versions.txt"
fetch "cloud-hypervisor-static-$ARCH" "$CH_SHA256" "$CH_DIR/cloud-hypervisor"
fetch "ch-remote-static-$ARCH" "$CH_REMOTE_SHA256" "$CH_DIR/ch-remote"
CH="$CH_DIR/cloud-hypervisor"
CHR="$CH_DIR/ch-remote"
{
  echo "cloud_hypervisor_version $("$CH" --version 2>&1 | head -n1)"
  echo "ch_remote_version $("$CHR" --version 2>&1 | head -n1)"
  echo "kernel_sha256 $(sha256sum "$KERNEL" | awk '{print $1}')"
  echo "kernel_file $(file -b "$KERNEL")"
  echo "host_uname $(uname -srm)"
  echo "commit $(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "cmdline $CMDLINE"
  echo "mem_mib $MEM_MIB vcpus 1"
} >>"$OUT/versions.txt"

# --- build and images (same guest as fc-restore.sh) ----------------------------------------
cargo build --release --target "$MUSL_TARGET" \
  -p example-restore-aware -p tachyon-serverless-x1-restore --bin x1-guest-init >&2
cargo build --release -p tachyon-serverless-x1-restore --bin x1-host >&2
GUEST_BIN="$REPO_ROOT/target/$MUSL_TARGET/release"
X1_HOST="$REPO_ROOT/target/release/x1-host"
BRIDGE_BIN="$GUEST_BIN/x1-guest-init" ROOTFS="$WORK/base/rootfs-x1.ext4" scripts/kvm/build-rootfs.sh >&2
mkdir -p "$WORK/base/stage"
install -m 0755 "$GUEST_BIN/example-restore-aware" "$WORK/base/stage/app"
truncate -s 32M "$WORK/base/function.ext4"
mkfs.ext4 -q -F -d "$WORK/base/stage" "$WORK/base/function.ext4" 32M
rm -rf "$WORK/base/stage"
fallocate -l 64M "$WORK/base/scratch-empty.ext4"
mkfs.ext4 -q -F -b 4096 -m 0 -O ^has_journal -E nodiscard,lazy_itable_init=0 \
  -L tachyon-scratch "$WORK/base/scratch-empty.ext4" 64M
chmod a-w "$WORK/base/rootfs-x1.ext4" "$WORK/base/function.ext4"
for f in rootfs-x1.ext4 function.ext4 scratch-empty.ext4; do
  echo "$f $(sha256sum "$WORK/base/$f" | awk '{print $1}')" >>"$OUT/versions.txt"
done

# --- helpers ------------------------------------------------------------------------------
vm_dir() { # name [scratch source]
  local d="$WORK/$1"
  mkdir -p "$d"
  ln -sf "$WORK/base/rootfs-x1.ext4" "$d/rootfs.ext4"
  ln -sf "$WORK/base/function.ext4" "$d/function.ext4"
  cp --sparse=always "${2:-$WORK/base/scratch-empty.ext4}" "$d/scratch.ext4"
  printf '%s\n' "$d"
}

start_host() { # dir mode [args...] -> HOST_PID
  local d="$1" mode="$2"
  shift 2
  "$X1_HOST" --listen "$d/v.sock_5000" --mode "$mode" "$@" >"$d/host.jsonl" 2>"$d/host.stderr" &
  HOST_PID=$!
  PIDS+=("$HOST_PID")
  for _ in $(seq 1 100); do
    [ -S "$d/v.sock_5000" ] && return 0
    sleep 0.02
  done
  return 1
}

chr() { # dir args... -> logs to chr.log, returns ch-remote's status
  local d="$1" rc=0 start
  shift
  start="$(now_ms)"
  (cd "$d" && "$CHR" --api-socket api.sock "$@") >>"$d/chr.log" 2>&1 || rc=$?
  printf '%s ch-remote %s -> rc %s (%s ms)\n' "$(date -u +%H:%M:%S)" "$*" "$rc" "$(($(now_ms) - start))" >>"$d/chr.log"
  LAST_MS=$(($(now_ms) - start))
  return "$rc"
}

boot_ch() { # dir -> CH_PID (cold boot with the full config on the command line)
  local d="$1"
  (cd "$d" && exec "$CH" --api-socket path=api.sock \
    --kernel "$KERNEL" --cmdline "$CMDLINE" \
    --cpus boot=1 --memory "size=${MEM_MIB}M" \
    --disk path=rootfs.ext4,readonly=on,image_type=raw path=function.ext4,readonly=on,image_type=raw \
    path=scratch.ext4,image_type=raw \
    --vsock cid=3,socket=v.sock \
    --serial off --console "$CH_CONSOLE" -v >console.log 2>&1) &
  CH_PID=$!
  PIDS+=("$CH_PID")
}

wait_event() { # dir event|type timeout_s
  for _ in $(seq 1 $(($3 * 20))); do
    grep -q -E "\"(event|type)\":\"$2\"" "$1/host.jsonl" 2>/dev/null && return 0
    sleep 0.05
  done
  return 1
}

wait_pid() { # pid timeout_s
  for _ in $(seq 1 $(($2 * 20))); do
    kill -0 "$1" 2>/dev/null || return 0
    sleep 0.05
  done
  return 1
}

stop_pid() {
  kill -9 "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

host_code() {
  local c
  c="$(jq -r 'select(.event == "exit") | .code' "$1/host.jsonl" | tail -n1)"
  if [ -n "$c" ]; then echo "$c"; elif grep -q '"event":"error"' "$1/host.jsonl"; then echo 1; else echo none; fi
}

collect() { # scenario dir
  mkdir -p "$OUT/$1"
  for f in host.jsonl host.stderr console.log chr.log; do
    if [ -f "$2/$f" ]; then cp "$2/$f" "$OUT/$1/$f"; fi
  done
  return 0
}

console_hint() { # dir -> last meaningful console line
  grep -v '^\s*$' "$1/console.log" 2>/dev/null | tail -n 3 | tr -d '\r' | tr '\n' ' ' | cut -c1-400
}

scratch_marker() {
  debugfs -R 'cat /x1-instance' "$1" 2>&1 | grep -v '^debugfs [0-9]' | tr -d '\n'
}

# --- cold ---------------------------------------------------------------------------------
COLD_OK=0
for n in $(seq 1 "$COLD_RUNS"); do
  s="cold-$n"
  d="$(vm_dir "$s")"
  start_host "$d" cold --invokes 1 --deadline-secs 60
  t0="$(now_ms)"
  boot_ch "$d"
  if wait_event "$d" exit 60 && [ "$(host_code "$d")" = 0 ]; then
    ready="$(jq -r 'select(.event == "guest_frame" and .type == "ready") | .host_wall_ms' "$d/host.jsonl" | head -n1)"
    record "$s" PASS "$((ready - t0))" "process start -> Ready (cold boot incl. bootstrap)"
    COLD_OK=1
  else
    alive="exited"
    if kill -0 "$CH_PID" 2>/dev/null; then alive="still running"; fi
    record "$s" FAIL "" "no Ready within 60 s (cloud-hypervisor $alive; host code $(host_code "$d")); console: $(console_hint "$d")"
  fi
  wait_pid "$CH_PID" 10 || stop_pid "$CH_PID"
  stop_pid "$HOST_PID"
  collect "$s" "$d"
  rm -rf "$d"
  if [ "$COLD_OK" = 0 ]; then break; fi
done
if [ "$COLD_OK" = 0 ]; then
  # VMM mechanics only: the guest never reached the handshake, so nothing below can show that
  # a guest resumes; it only records whether pause / snapshot / restore / resume are accepted.
  d="$(vm_dir mechanics-source)"
  start_host "$d" listen --deadline-secs 30
  boot_ch "$d"
  M_CH="$CH_PID"
  sleep 3
  MSNAP="$WORK/msnap"
  mkdir -p "$MSNAP"
  if chr "$d" pause && chr "$d" snapshot "file://$MSNAP"; then
    record mechanics-snapshot INFO "$LAST_MS" "ch-remote pause + snapshot accepted on a guest that never reached the handshake (files: $(find "$MSNAP" -type f -printf '%f ' ))"
    jq '{disks: [.disks[]? | {path, readonly}], vsock}' "$MSNAP/config.json" >"$OUT/mechanics-config.json" 2>/dev/null || true
  else
    record mechanics-snapshot INFO "" "pause/snapshot refused: $(tail -n3 "$d/chr.log" | tr '\n' ' ')"
  fi
  stop_pid "$M_CH"
  stop_pid "$HOST_PID"
  collect mechanics-source "$d"
  if [ -f "$MSNAP/config.json" ]; then
    d="$(vm_dir mechanics-restore)"
    start_host "$d" listen --deadline-secs 15
    t0="$(now_ms)"
    (cd "$d" && exec "$CH" --api-socket path=api.sock --restore "source_url=file://$MSNAP" -v \
      >console.log 2>&1) &
    M_CH=$!
    PIDS+=("$M_CH")
    for _ in $(seq 1 100); do
      [ -S "$d/api.sock" ] && break
      sleep 0.05
    done
    resumed=no
    for _ in $(seq 1 100); do
      if chr "$d" resume; then resumed=yes; break; fi
      kill -0 "$M_CH" 2>/dev/null || break
      sleep 0.05
    done
    sleep 3
    state="$(cd "$d" && "$CHR" --api-socket api.sock info 2>/dev/null | jq -r '.state' 2>/dev/null || echo unknown)"
    record mechanics-restore INFO "$(($(now_ms) - t0))" "fresh process --restore: resume accepted=$resumed, VMM state after 3 s=$state, host connections=$(grep -c '"event":"accepted"' "$d/host.jsonl" || true); guest liveness NOT verified"
    stop_pid "$M_CH"
    stop_pid "$HOST_PID"
    collect mechanics-restore "$d"
  fi
  record snapshot-restore-of-running-guest FAIL "" "not measurable: the guest does not reach the bridge handshake on this host with $CH_VERSION"
  jq -R -s 'split("\n") | map(select(length > 0) | split("\t")) | .[1:] | map({scenario: .[0], result: .[1], metric_ms: (.[2] | tonumber? // null), detail: .[3]})' "$RESULTS" >"$OUT/summary.json"
  exit 1
fi

# --- source + snapshot ----------------------------------------------------------------------
SNAP="$WORK/snap"
mkdir -p "$SNAP"
d="$(vm_dir source)"
start_host "$d" source --deadline-secs 600
SRC_HOST="$HOST_PID"
t0="$(now_ms)"
boot_ch "$d"
SRC_CH="$CH_PID"
if ! wait_event "$d" checkpoint_wait 60; then
  record source FAIL "" "no checkpoint wait; console: $(console_hint "$d")"
  collect source "$d"
  exit 1
fi
record source-boot-to-checkpoint INFO "$(($(jq -r 'select(.event == "checkpoint_wait") | .host_wall_ms' "$d/host.jsonl") - t0))" "process start -> x1_waiting"
SRC_BOOT_ID="$(jq -r 'select(.event == "guest_frame" and .type == "hello") | .guest_boot_id' "$d/host.jsonl" | head -n1)"
sleep 1
if chr "$d" pause; then record ch-pause INFO "$LAST_MS" "ch-remote pause"; else record ch-pause FAIL "" "$(tail -n2 "$d/chr.log" | tr '\n' ' ')"; fi
if chr "$d" snapshot "file://$SNAP"; then
  record snapshot-create PASS "$LAST_MS" "ch-remote snapshot file:// (paused, ${MEM_MIB} MiB)"
else
  record snapshot-create FAIL "$LAST_MS" "$(tail -n3 "$d/chr.log" | tr '\n' ' ')"
  collect source "$d"
  exit 1
fi
cp --sparse=always "$d/scratch.ext4" "$SNAP/scratch.ext4"
{
  ls -ls "$SNAP"
  sha256sum "$SNAP"/*
  echo "# config.json (paths as recorded)"
  jq '{disks: [.disks[]? | {path, readonly}], vsock, payload: .payload, memory: .memory}' "$SNAP/config.json" 2>/dev/null || cat "$SNAP/config.json"
} >"$OUT/snapshot-files.txt"
SNAP_SHA_BEFORE="$(sha256sum "$SNAP/config.json" "$SNAP/state.json" "$SNAP/memory-ranges" "$SNAP/scratch.ext4" 2>/dev/null || true)"
stop_pid "$SRC_CH"
stop_pid "$SRC_HOST"
collect source "$d"
rm -rf "$d"
sleep 5

# --- clones --------------------------------------------------------------------------------
declare -A CL_CH CL_HOST CL_T0
for c in a b; do
  d="$(vm_dir "clone-$c" "$SNAP/scratch.ext4")"
  start_host "$d" clone --instance-id "clone-$c" --invokes 2 --linger-ms 3000 --deadline-secs 60 \
    --doorbell-uds "$d/v.sock"
  CL_HOST[$c]="$HOST_PID"
  CL_T0[$c]="$(now_ms)"
  (cd "$d" && exec "$CH" --api-socket path=api.sock --restore "source_url=file://$SNAP" -v \
    >console.log 2>&1) &
  CL_CH[$c]=$!
  PIDS+=("${CL_CH[$c]}")
done
for c in a b; do
  d="$WORK/clone-$c"
  for _ in $(seq 1 100); do
    [ -S "$d/api.sock" ] && break
    sleep 0.05
  done
  # the restored VM stays paused until resumed; wait for the restore to finish
  resumed=0
  for _ in $(seq 1 100); do
    if chr "$d" resume; then resumed=1; break; fi
    kill -0 "${CL_CH[$c]}" 2>/dev/null || break
    sleep 0.05
  done
  record "clone-$c-resume" INFO "$(($(now_ms) - CL_T0[$c]))" "process start (--restore) -> ch-remote resume ok=$resumed"
done
sleep 1.5
{
  for c in a b; do
    p="${CL_CH[$c]}"
    echo "clone-$c cloud-hypervisor pid $p alive=$(kill -0 "$p" 2>/dev/null && echo yes || echo no)"
    grep -F "$SNAP" "/proc/$p/maps" 2>/dev/null | head -n 3 || true
    awk '/^(Rss|Pss|Shared_Clean|Private_Dirty|Anonymous):/ {print "  " $0}' "/proc/$p/smaps_rollup" 2>/dev/null || true
  done
} >"$OUT/clones-concurrent.txt"
for c in a b; do
  d="$WORK/clone-$c"
  wait_event "$d" exit 60 || true
  code="$(host_code "$d")"
  s="clone-$c"
  if [ "$code" = 3 ]; then
    record "$s" FAIL "" "guest cold-booted on the restore VMM: NOT a restore"
  elif [ "$code" != 0 ]; then
    record "$s" FAIL "" "host code $code: $(jq -r 'select(.event == "error") | .message' "$d/host.jsonl" | tail -n1); console: $(console_hint "$d")"
  else
    boot="$(jq -r 'select(.event == "guest_control" and .type == "x1_reconnect") | .guest_boot_id' "$d/host.jsonl" | head -n1)"
    ready="$(jq -r 'select(.event == "guest_frame" and .type == "ready") | .host_wall_ms' "$d/host.jsonl" | head -n1)"
    reconnect="$(jq -r 'select(.event == "guest_control" and .type == "x1_reconnect") | .host_wall_ms' "$d/host.jsonl" | head -n1)"
    lost="$(jq -r 'select(.event == "guest_control" and .type == "x1_reconnect") | .lost' "$d/host.jsonl" | head -n1)"
    resp="$(jq -c 'select(.event == "guest_frame" and .type == "response") | .payload' "$d/host.jsonl" | head -n1)"
    marker="$(scratch_marker "$d/scratch.ext4")"
    if [ "$boot" = "$SRC_BOOT_ID" ] && [ "$marker" = "$s" ] &&
      printf '%s' "$resp" | jq -e --arg i "$s" '.restored == true and (.instance_id | startswith($i))' >/dev/null; then
      record "$s" PASS "$((ready - CL_T0[$c]))" "process start (--restore) -> Ready (-> x1_reconnect $((reconnect - CL_T0[$c])) ms, detected by: $lost); boot_id same as source; own scratch marker"
      printf '%s\t%s\n' "$s" "$resp" >>"$OUT/responses.tsv"
      jq -c --arg s "$s" 'select(.event == "guest_control" and .type == "x1_reconnect") | {scenario: $s, host_wall_ms, clocks, urandom_hex, lost}' \
        "$d/host.jsonl" >>"$OUT/restore-clocks.jsonl"
    else
      record "$s" FAIL "" "boot $boot vs $SRC_BOOT_ID, marker '$marker', response $resp"
    fi
  fi
  wait_pid "${CL_CH[$c]}" 10 || stop_pid "${CL_CH[$c]}"
  stop_pid "${CL_HOST[$c]}"
done
{
  echo "snapshot copy: $(scratch_marker "$SNAP/scratch.ext4")"
  for c in a b; do echo "clone-$c: $(scratch_marker "$WORK/clone-$c/scratch.ext4")"; done
  sha256sum "$SNAP/config.json" "$SNAP/state.json" "$SNAP/memory-ranges" "$SNAP/scratch.ext4"
} >"$OUT/clones-disk.txt"
SNAP_SHA_AFTER="$(sha256sum "$SNAP/config.json" "$SNAP/state.json" "$SNAP/memory-ranges" "$SNAP/scratch.ext4" 2>/dev/null || true)"
if [ "$SNAP_SHA_AFTER" = "$SNAP_SHA_BEFORE" ] && grep -qx 'clone-a: clone-a' "$OUT/clones-disk.txt" &&
  grep -qx 'clone-b: clone-b' "$OUT/clones-disk.txt"; then
  record clones-disk-isolation PASS "" "each clone wrote only its own scratch copy; snapshot files unchanged"
else
  record clones-disk-isolation FAIL "" "see clones-disk.txt"
fi
for c in a b; do collect "clone-$c" "$WORK/clone-$c"; done

jq -R -s 'split("\n") | map(select(length > 0) | split("\t")) | .[1:] | map({scenario: .[0], result: .[1], metric_ms: (.[2] | tonumber? // null), detail: .[3]})' "$RESULTS" >"$OUT/summary.json"
{
  pgrep -a cloud-hypervisor || echo "cloud-hypervisor processes: none"
  pgrep -a x1-host || echo "x1-host processes: none"
} >"$OUT/leftovers.txt"
log "results: $RESULTS (failures: $FAILS)"
[ "$FAILS" = 0 ]

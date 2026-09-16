#!/usr/bin/env bash
# scripts/kvm/teardown.sh - stop every Firecracker started from this repository's .kvm/run,
# remove the run directory and audit for orphans (processes, sockets, loop devices, tap devices).
#
# Usage:
#   scripts/kvm/teardown.sh           # kill + clean + audit; exit 1 when orphans remain
#   scripts/kvm/teardown.sh --purge   # additionally delete .kvm entirely (binaries, kernel, rootfs)
#
# Only processes whose command line references <repo>/.kvm/run are touched. Root is not required
# for processes started by this user. Idempotent.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KVM_DIR="$REPO_ROOT/.kvm"
RUN_DIR="$KVM_DIR/run"
PURGE=0
for arg in "$@"; do
  case "$arg" in
    --purge) PURGE=1 ;;
    -h | --help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

fc_pids() { pgrep -f -- "firecracker.*--api-sock ${RUN_DIR}/" 2>/dev/null || true; }

# --- 1. kill our Firecracker processes (whole process groups) ------------------------
PIDS="$(fc_pids)"
if [ -n "$PIDS" ]; then
  for pid in $PIDS; do
    echo "[teardown] SIGKILL firecracker pid $pid (process group)"
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
  done
  sleep 1
else
  echo "[teardown] no firecracker processes under $RUN_DIR"
fi

# --- 2. remove run directory -----------------------------------------------------------
if [ -d "$RUN_DIR" ]; then
  echo "[teardown] removing $RUN_DIR"
  rm -rf "$RUN_DIR"
fi

# --- 3. orphan audit -----------------------------------------------------------------
ORPHANS=0
echo
echo "orphan audit"

LEFT_PIDS="$(fc_pids)"
if [ -n "$LEFT_PIDS" ]; then
  echo "  FAIL processes still alive:"
  for pid in $LEFT_PIDS; do ps -o pid=,ppid=,etime=,args= -p "$pid" | sed 's/^/    /'; done
  ORPHANS=1
else
  echo "  ok   no firecracker process references $RUN_DIR"
fi

if [ -d "$KVM_DIR" ]; then
  SOCKS="$(find "$KVM_DIR" \( -name '*.sock' -o -name 'v.sock_*' -o -name 'v.sock' \) 2>/dev/null || true)"
  if [ -n "$SOCKS" ]; then
    echo "  FAIL leftover sockets under .kvm:"
    printf '    %s\n' "$SOCKS"
    ORPHANS=1
  else
    echo "  ok   no *.sock / v.sock_* under .kvm"
  fi
fi

if command -v losetup >/dev/null 2>&1; then
  LOOPS="$(losetup -a 2>/dev/null | grep -F "$KVM_DIR" || true)"
  if [ -n "$LOOPS" ]; then
    echo "  FAIL loop devices attached to .kvm images (none expected):"
    printf '    %s\n' "$LOOPS"
    ORPHANS=1
  else
    echo "  ok   no loop devices attached to .kvm images"
  fi
else
  echo "  skip losetup not available"
fi

if command -v ip >/dev/null 2>&1; then
  TAPS="$(ip -o link show 2>/dev/null | awk -F': ' '{print $2}' | grep -E '^tsls' || true)"
  if [ -n "$TAPS" ]; then
    echo "  FAIL tap devices named tsls* exist (P1 never creates them):"
    printf '    %s\n' "$TAPS"
    ORPHANS=1
  else
    echo "  ok   no tsls* tap devices"
  fi
else
  echo "  skip ip(8) not available"
fi

# --- 4. optional purge ----------------------------------------------------------------
if [ "$PURGE" -eq 1 ]; then
  echo
  echo "[teardown] --purge: removing $KVM_DIR"
  rm -rf "$KVM_DIR"
fi

echo
if [ "$ORPHANS" -eq 0 ]; then
  echo "teardown complete: no orphans"
  exit 0
fi
echo "teardown finished with orphans (see above)"
exit 1

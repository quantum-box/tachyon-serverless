#!/usr/bin/env bash
# scripts/kvm/smoke.sh - boot real microVMs with fc-smoke (no gateway) and save the evidence.
#
#   1. example-hello with payload {"name":"kvm"}: boot -> Hello -> Ready -> Invoke -> Response
#      -> Shutdown -> terminate, no leftovers.
#   2. example-cpu-burn with --timeout-demo: the host enforces a short deadline
#      (Cancel -> SIGKILL of the Firecracker process group) and proves the VM is gone.
#
# Usage:
#   scripts/kvm/smoke.sh
# Environment:
#   CPU_BURN_PAYLOAD   payload for the timeout demo (default {"seconds":60})
#   TIMEOUT_SECONDS    boot/handshake budget (default 30)
#   DEADLINE_SECONDS   deadline used by the timeout demo (default 3)
#   EVIDENCE_ROOT      where to save results (default docs/evidence)
# Output: docs/evidence/kvm-<UTC>/{hello,timeout}.json, *.stderr.txt, *-console.txt, *-fc.txt,
#         summary.txt. Exit 0 when both scenarios PASS.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"
KVM_DIR="$REPO_ROOT/.kvm"
WORKDIR="$KVM_DIR/run"
GUEST_BIN_DIR="$REPO_ROOT/target/$MUSL_TARGET/release"
CPU_BURN_PAYLOAD="${CPU_BURN_PAYLOAD:-}"
if [ -z "$CPU_BURN_PAYLOAD" ]; then CPU_BURN_PAYLOAD='{"seconds":60}'; fi
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"
DEADLINE_SECONDS="${DEADLINE_SECONDS:-3}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$REPO_ROOT/docs/evidence}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$EVIDENCE_ROOT/kvm-$STAMP"

for f in "$KVM_DIR/bin/firecracker" "$KVM_DIR/vmlinux" "$KVM_DIR/rootfs.ext4" \
  "$GUEST_BIN_DIR/example-hello" "$GUEST_BIN_DIR/example-cpu-burn"; do
  if [ ! -f "$f" ]; then
    echo "missing $f (run scripts/kvm/bootstrap.sh first)" >&2
    exit 1
  fi
done

echo "[smoke] building fc-smoke"
cargo build --release -p tachyon-serverless-provider-firecracker --bin fc-smoke
FC_SMOKE="$REPO_ROOT/target/release/fc-smoke"
mkdir -p "$EVIDENCE_DIR" "$WORKDIR"

json_get() { # file key -> first string value
  sed -n "s/^ *\"$2\": *\"\([^\"]*\)\".*/\1/p" "$1" | head -n1
}
json_get_raw() { # file key -> first raw scalar (number/bool/null)
  sed -n "s/^ *\"$2\": *\([^,\"]*\),*\$/\1/p" "$1" | head -n1
}

copy_archived_logs() { # name json
  local env_id
  env_id="$(json_get "$2" environment_id)"
  if [ -n "$env_id" ] && [ -d "$WORKDIR/_archive/$env_id" ]; then
    [ -f "$WORKDIR/_archive/$env_id/console.log" ] && cp "$WORKDIR/_archive/$env_id/console.log" "$EVIDENCE_DIR/$1-console.txt"
    [ -f "$WORKDIR/_archive/$env_id/fc.log" ] && cp "$WORKDIR/_archive/$env_id/fc.log" "$EVIDENCE_DIR/$1-fc.txt"
  fi
  return 0
}

run_case() { # name binary payload extra-args...
  local name="$1" binary="$2" payload="$3"
  shift 3
  local json="$EVIDENCE_DIR/$name.json"
  echo "[smoke] case $name: $binary payload=$payload $*"
  set +e
  "$FC_SMOKE" \
    --firecracker "$KVM_DIR/bin/firecracker" \
    --kernel "$KVM_DIR/vmlinux" \
    --rootfs "$KVM_DIR/rootfs.ext4" \
    --workdir "$WORKDIR" \
    --binary "$binary" \
    --payload "$payload" \
    --timeout-seconds "$TIMEOUT_SECONDS" \
    "$@" >"$json" 2>"$EVIDENCE_DIR/$name.stderr.txt"
  local rc=$?
  set -e
  copy_archived_logs "$name" "$json"
  if [ "$rc" -eq 0 ]; then
    echo "[smoke] $name PASS"
    RESULTS+=("PASS $name")
  else
    echo "[smoke] $name FAIL (exit $rc): $(json_get "$json" error)"
    tail -n 20 "$EVIDENCE_DIR/$name.stderr.txt" || true
    RESULTS+=("FAIL $name (exit $rc)")
    FAILED=1
  fi
  {
    echo "== $name =="
    echo "environment_id      $(json_get "$json" environment_id)"
    echo "guest_boot_id       $(json_get "$json" guest_boot_id)"
    echo "host_pid            $(json_get_raw "$json" host_pid)"
    echo "firecracker_version $(json_get "$json" firecracker_version)"
    echo "kernel_sha256       $(json_get "$json" kernel_sha256)"
    echo "rootfs_sha256       $(json_get "$json" rootfs_sha256)"
    echo "outcome             $(json_get "$json" outcome)"
    echo "boot_ms             $(json_get_raw "$json" boot_ms)"
    echo "init_ms             $(json_get_raw "$json" init_ms)"
    echo "handler_ms          $(json_get_raw "$json" handler_ms)"
    echo "total_ms            $(json_get_raw "$json" total_ms)"
    echo
  } >>"$EVIDENCE_DIR/summary.txt"
}

RESULTS=()
FAILED=0
{
  echo "tachyon-serverless KVM smoke $STAMP"
  echo "host $(uname -srm)  firecracker $("$KVM_DIR/bin/firecracker" --version | head -n1)"
  echo "kernel $(sha256sum "$KVM_DIR/vmlinux" | awk '{print $1}')  rootfs $(sha256sum "$KVM_DIR/rootfs.ext4" | awk '{print $1}')"
  echo
} >"$EVIDENCE_DIR/summary.txt"

run_case hello "$GUEST_BIN_DIR/example-hello" '{"name":"kvm"}'
run_case timeout "$GUEST_BIN_DIR/example-cpu-burn" "$CPU_BURN_PAYLOAD" --timeout-demo --deadline-seconds "$DEADLINE_SECONDS"

echo
echo "evidence: $EVIDENCE_DIR"
printf '  %s\n' "${RESULTS[@]}"
if [ "$FAILED" -eq 0 ]; then
  echo "PASS"
  exit 0
fi
echo "FAIL"
exit 1

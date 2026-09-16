#!/usr/bin/env bash
# scripts/kvm/preflight.sh - check that this Linux host can run the Firecracker provider.
#
# Usage:
#   scripts/kvm/preflight.sh          # prints a table; exit 0 when READY, 1 when NOT READY
#
# Checks (read-only, root not required):
#   OS == Linux, arch in {x86_64, aarch64}, /dev/kvm exists and is rw for this uid,
#   CPU virtualization (vmx/svm on x86_64), kernel >= 4.14,
#   tools: curl sha256sum tar mkfs.ext4 (e2fsprogs >= 1.43 for -d) cargo rustup gcc/cc,
#   Rust musl target installed, Unix socket path length under .kvm/run <= 107 bytes.
# All paths are relative to the repository root.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KVM_DIR="$REPO_ROOT/.kvm"

FAILED=0
row() { # status name detail
  printf '%-4s %-22s %s\n' "$1" "$2" "$3"
  if [ "$1" = "FAIL" ]; then FAILED=1; fi
}
have() { command -v "$1" >/dev/null 2>&1; }
# version_ge A B : true when A >= B (dotted versions)
version_ge() { [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]; }

OS="$(uname -s)"
ARCH="$(uname -m)"

echo "tachyon-serverless KVM preflight ($REPO_ROOT)"
echo

# --- OS / arch -------------------------------------------------------------
if [ "$OS" = "Linux" ]; then
  row ok os "$OS"
else
  row FAIL os "$OS (Linux required; on macOS use the Lima VM described in docs/kvm.md)"
fi
case "$ARCH" in
  x86_64 | aarch64) row ok arch "$ARCH" ;;
  *) row FAIL arch "$ARCH (x86_64 or aarch64 required)" ;;
esac

# --- KVM -------------------------------------------------------------------
if [ -e /dev/kvm ]; then
  if [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
    row ok kvm "/dev/kvm is rw for $(id -un)"
  else
    row FAIL kvm "/dev/kvm exists but is not rw for $(id -un): sudo usermod -aG kvm $(id -un) (then re-login) or sudo chmod 666 /dev/kvm"
  fi
else
  row FAIL kvm "/dev/kvm missing (enable KVM / nested virtualization on this host)"
fi

if [ "$ARCH" = "x86_64" ]; then
  if grep -Eq '^flags.*\b(vmx|svm)\b' /proc/cpuinfo 2>/dev/null; then
    row ok cpu_virt "vmx/svm flag present"
  else
    row FAIL cpu_virt "no vmx/svm flag in /proc/cpuinfo (nested virtualization disabled?)"
  fi
elif [ "$ARCH" = "aarch64" ]; then
  if [ -e /dev/kvm ]; then
    row ok cpu_virt "aarch64: no cpuinfo flag; /dev/kvm present"
  else
    row FAIL cpu_virt "aarch64: /dev/kvm missing"
  fi
fi

KREL="$(uname -r)"
KVER="${KREL%%-*}"
if version_ge "$KVER" "4.14"; then
  row ok kernel "$KREL"
else
  row FAIL kernel "$KREL (Firecracker requires >= 4.14)"
fi

# --- tools -----------------------------------------------------------------
for t in curl sha256sum tar cargo rustup; do
  if have "$t"; then
    row ok "tool:$t" "$(command -v "$t")"
  else
    row FAIL "tool:$t" "not found in PATH"
  fi
done

if have gcc; then
  row ok "tool:gcc" "$(command -v gcc)"
elif have cc; then
  row ok "tool:gcc" "$(command -v cc) (cc)"
else
  row FAIL "tool:gcc" "gcc/cc not found (needed as linker driver for the musl target)"
fi

if have mkfs.ext4; then
  MKE2FS_VER="$(mkfs.ext4 -V 2>&1 | head -n1 | awk '{print $2}')"
  if [ -n "$MKE2FS_VER" ] && version_ge "$MKE2FS_VER" "1.43"; then
    row ok "tool:mkfs.ext4" "$(command -v mkfs.ext4) (e2fsprogs $MKE2FS_VER)"
  else
    row FAIL "tool:mkfs.ext4" "e2fsprogs ${MKE2FS_VER:-?} too old; >= 1.43 required for mkfs.ext4 -d"
  fi
else
  row FAIL "tool:mkfs.ext4" "not found (apt install e2fsprogs)"
fi

# --- Rust musl target ------------------------------------------------------
MUSL_TARGET="${ARCH}-unknown-linux-musl"
if have rustup && rustup target list --installed 2>/dev/null | grep -qx "$MUSL_TARGET"; then
  row ok musl_target "$MUSL_TARGET installed"
else
  row FAIL musl_target "$MUSL_TARGET not installed: rustup target add $MUSL_TARGET (bootstrap.sh does this)"
fi

# --- socket path length ----------------------------------------------------
SAMPLE="$KVM_DIR/run/env_00000000000000000000000000/v.sock_5000"
if [ "${#SAMPLE}" -le 107 ]; then
  row ok socket_path "${#SAMPLE} bytes (max 107): $SAMPLE"
else
  row FAIL socket_path "${#SAMPLE} bytes > 107 (sun_path limit); clone the repo into a shorter path"
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "READY: this host can run scripts/kvm/bootstrap.sh and smoke.sh"
  exit 0
else
  echo "NOT READY: fix the FAIL rows above (see docs/kvm.md)"
  exit 1
fi

#!/usr/bin/env bash
# scripts/kvm/build-rootfs.sh - build the read-only base rootfs for Firecracker guests.
#
# Layout (docs/protocol.md section C):
#   /sbin/tachyon-init   runtime bridge, static musl binary, 0755 (PID 1 in the guest)
#   /proc /sys /dev /tmp /function   empty mount points
#
# Usage:
#   scripts/kvm/build-rootfs.sh
# Environment:
#   BRIDGE_BIN    bridge binary (default target/<arch>-unknown-linux-musl/release/tachyon-serverless-runtime-bridge)
#   ROOTFS        output image (default .kvm/rootfs.ext4)
#   ROOTFS_SIZE   image size for mkfs.ext4 (default 64M)
# Requires e2fsprogs >= 1.43 (mkfs.ext4 -d). Root is not required.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"
BRIDGE_BIN="${BRIDGE_BIN:-$REPO_ROOT/target/$MUSL_TARGET/release/tachyon-serverless-runtime-bridge}"
ROOTFS="${ROOTFS:-$REPO_ROOT/.kvm/rootfs.ext4}"
ROOTFS_SIZE="${ROOTFS_SIZE:-64M}"

if [ ! -f "$BRIDGE_BIN" ]; then
  echo "bridge binary not found: $BRIDGE_BIN (run scripts/kvm/bootstrap.sh or set BRIDGE_BIN)" >&2
  exit 1
fi
if ! command -v mkfs.ext4 >/dev/null 2>&1; then
  echo "mkfs.ext4 not found (apt install e2fsprogs)" >&2
  exit 1
fi
mkdir -p "$(dirname "$ROOTFS")"

STAGING="$(mktemp -d "$(dirname "$ROOTFS")/rootfs-staging.XXXXXX")"
cleanup() { rm -rf "$STAGING"; }
trap cleanup EXIT

mkdir -p "$STAGING/sbin" "$STAGING/proc" "$STAGING/sys" "$STAGING/dev" "$STAGING/tmp" "$STAGING/function"
install -m 0755 "$BRIDGE_BIN" "$STAGING/sbin/tachyon-init"

rm -f "$ROOTFS"
# Pre-size the image (sparse) and pass the size explicitly: works with every mke2fs >= 1.43.
truncate -s "$ROOTFS_SIZE" "$ROOTFS"
mkfs.ext4 -q -F -d "$STAGING" "$ROOTFS" "$ROOTFS_SIZE"

echo "rootfs: $ROOTFS ($ROOTFS_SIZE)"
echo "  /sbin/tachyon-init <- $BRIDGE_BIN"
printf '  sha256 %s\n' "$(sha256sum "$ROOTFS" | awk '{print $1}')"

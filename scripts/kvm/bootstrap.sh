#!/usr/bin/env bash
# scripts/kvm/bootstrap.sh - download pinned Firecracker + CI guest kernel, build the guest
# binaries for the musl target and build the base rootfs. Idempotent: re-running verifies
# checksums recorded in .kvm/manifest.json instead of downloading again.
#
# Usage:
#   scripts/kvm/bootstrap.sh
#
# Environment overrides:
#   FIRECRACKER_VERSION  release tag (default v1.17.0)
#   CI_VERSION           S3 prefix under firecracker-ci/ holding guest kernels.
#                        "auto" (default) = newest date-based prefix, the method used by the
#                        v1.17 getting-started guide. Set e.g. "v1.15" to pin an older layout.
#   GUEST_KERNEL_SERIES  restrict the kernel to a series, e.g. "6.1" (default: newest vmlinux-X.Y.Z)
#   BRIDGE_BIN           runtime bridge binary for the rootfs (default: musl release build)
#
# Artifacts (all under .kvm/, gitignored):
#   .kvm/bin/firecracker   .kvm/vmlinux   .kvm/rootfs.ext4   .kvm/manifest.json   .kvm/dl/
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

FIRECRACKER_VERSION="${FIRECRACKER_VERSION:-v1.17.0}"
CI_VERSION="${CI_VERSION:-auto}"
GUEST_KERNEL_SERIES="${GUEST_KERNEL_SERIES:-}"
ARCH="$(uname -m)"
MUSL_TARGET="${ARCH}-unknown-linux-musl"

KVM_DIR="$REPO_ROOT/.kvm"
BIN_DIR="$KVM_DIR/bin"
DL_DIR="$KVM_DIR/dl"
KERNEL="$KVM_DIR/vmlinux"
ROOTFS="$KVM_DIR/rootfs.ext4"
MANIFEST="$KVM_DIR/manifest.json"
FC_BIN="$BIN_DIR/firecracker"
S3="https://s3.amazonaws.com/spec.ccfc.min"
RELEASE_URL="https://github.com/firecracker-microvm/firecracker/releases/download/${FIRECRACKER_VERSION}"

case "$ARCH" in
  x86_64 | aarch64) ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
if [ "$(uname -s)" != "Linux" ]; then
  echo "bootstrap.sh must run on Linux (see docs/kvm.md for the Lima VM)" >&2
  exit 1
fi
mkdir -p "$BIN_DIR" "$DL_DIR"

sha256_of() { sha256sum "$1" | awk '{print $1}'; }
manifest_get() { # key -> value (empty when absent)
  [ -f "$MANIFEST" ] || return 0
  sed -n "s/.*\"$1\": *\"\([^\"]*\)\".*/\1/p" "$MANIFEST" | head -n1
}
# list S3 <Key> or <Prefix> values for a query string (the XML comes back on one line; no PCRE needed)
s3_list() { curl -fsSL "${S3}?list-type=2&${1}" | grep -o "<${2}>[^<]*" | sed "s/^<${2}>//"; }

# --- 1. Firecracker binary ------------------------------------------------------------
fetch_firecracker() {
  if [ -x "$FC_BIN" ] && "$FC_BIN" --version 2>/dev/null | head -n1 | grep -q "Firecracker ${FIRECRACKER_VERSION}\$"; then
    echo "[firecracker] $FC_BIN is already ${FIRECRACKER_VERSION}"
    return
  fi
  local tgz="firecracker-${FIRECRACKER_VERSION}-${ARCH}.tgz"
  echo "[firecracker] downloading ${RELEASE_URL}/${tgz}"
  curl -fsSL -o "$DL_DIR/$tgz" "${RELEASE_URL}/${tgz}"
  curl -fsSL -o "$DL_DIR/$tgz.sha256.txt" "${RELEASE_URL}/${tgz}.sha256.txt"
  local expected actual
  expected="$(awk '{print $1}' "$DL_DIR/$tgz.sha256.txt")"
  actual="$(sha256_of "$DL_DIR/$tgz")"
  if [ "$expected" != "$actual" ]; then
    echo "[firecracker] checksum mismatch: expected $expected got $actual" >&2
    exit 1
  fi
  echo "[firecracker] sha256 verified ($actual)"
  rm -rf "$DL_DIR/release-${FIRECRACKER_VERSION}-${ARCH}"
  tar -xzf "$DL_DIR/$tgz" -C "$DL_DIR"
  install -m 0755 "$DL_DIR/release-${FIRECRACKER_VERSION}-${ARCH}/firecracker-${FIRECRACKER_VERSION}-${ARCH}" "$FC_BIN"
  FC_TGZ_SHA256="$actual"
  "$FC_BIN" --version | head -n1
}

# --- 2. Guest kernel ------------------------------------------------------------------
resolve_ci_prefix() {
  if [ "$CI_VERSION" != "auto" ]; then
    echo "firecracker-ci/${CI_VERSION}/"
    return
  fi
  # Official v1.17 quickstart: newest date-based prefix firecracker-ci/YYYYMMDD-<sha>-<n>/
  s3_list "prefix=firecracker-ci/&delimiter=/" Prefix | grep -E '^firecracker-ci/[0-9]{8}-[^/]+/$' | sort | tail -n1
}

fetch_kernel() {
  local recorded
  recorded="$(manifest_get kernel_sha256)"
  if [ -f "$KERNEL" ] && [ -n "$recorded" ]; then
    if [ "$(sha256_of "$KERNEL")" = "$recorded" ]; then
      echo "[kernel] $KERNEL verified against manifest ($recorded)"
      KERNEL_KEY="$(manifest_get kernel_key)"
      KERNEL_URL="$(manifest_get kernel_url)"
      KERNEL_SHA256="$recorded"
      return
    fi
    echo "[kernel] checksum differs from manifest; downloading again"
  fi
  local prefix keys key
  prefix="$(resolve_ci_prefix)"
  if [ -z "$prefix" ]; then
    echo "[kernel] cannot resolve a CI prefix on $S3" >&2
    exit 1
  fi
  echo "[kernel] CI prefix: $prefix"
  keys="$(s3_list "prefix=${prefix}${ARCH}/vmlinux-" Key | grep -E "/vmlinux-[0-9]+\.[0-9]+\.[0-9]+$" || true)"
  if [ -n "$GUEST_KERNEL_SERIES" ]; then
    keys="$(printf '%s\n' "$keys" | grep -E "/vmlinux-${GUEST_KERNEL_SERIES//./\\.}\.[0-9]+$" || true)"
  fi
  key="$(printf '%s\n' "$keys" | sort -V | tail -n1)"
  if [ -z "$key" ]; then
    echo "[kernel] no vmlinux-X.Y.Z found under ${prefix}${ARCH}/ (series='${GUEST_KERNEL_SERIES}')" >&2
    exit 1
  fi
  echo "[kernel] downloading ${S3}/${key}"
  curl -fsSL -o "$KERNEL.tmp" "${S3}/${key}"
  mv "$KERNEL.tmp" "$KERNEL"
  KERNEL_KEY="$key"
  KERNEL_URL="${S3}/${key}"
  KERNEL_SHA256="$(sha256_of "$KERNEL")"
  echo "[kernel] sha256 $KERNEL_SHA256"
}

write_manifest() {
  local fc_sha fc_tgz_sha
  fc_sha="$(sha256_of "$FC_BIN")"
  # Read the previous value before the redirection below truncates the file.
  fc_tgz_sha="${FC_TGZ_SHA256:-$(manifest_get firecracker_tgz_sha256)}"
  cat >"$MANIFEST" <<EOF
{
  "arch": "${ARCH}",
  "firecracker_version": "${FIRECRACKER_VERSION}",
  "firecracker_url": "${RELEASE_URL}/firecracker-${FIRECRACKER_VERSION}-${ARCH}.tgz",
  "firecracker_tgz_sha256": "${fc_tgz_sha}",
  "firecracker_binary_sha256": "${fc_sha}",
  "ci_version": "${CI_VERSION}",
  "kernel_key": "${KERNEL_KEY}",
  "kernel_url": "${KERNEL_URL}",
  "kernel_sha256": "${KERNEL_SHA256}",
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
  echo "[manifest] wrote $MANIFEST"
}

# --- 3. Guest binaries (static musl) -------------------------------------------------
build_guest() {
  echo "[rust] rustup target add $MUSL_TARGET"
  rustup target add "$MUSL_TARGET"
  # musl targets link statically by default (static-pie). If a toolchain produces a
  # dynamically linked binary, export RUSTFLAGS="-C target-feature=+crt-static" and re-run.
  echo "[rust] cargo build --release --target $MUSL_TARGET (bridge + examples)"
  cargo build --release --target "$MUSL_TARGET" \
    -p tachyon-serverless-runtime-bridge \
    -p example-hello -p example-http-axum -p example-cpu-burn
  local out="$REPO_ROOT/target/$MUSL_TARGET/release"
  for bin in tachyon-serverless-runtime-bridge example-hello example-http-axum example-cpu-burn; do
    if [ ! -f "$out/$bin" ]; then
      echo "[rust] expected binary missing: $out/$bin" >&2
      exit 1
    fi
    if command -v readelf >/dev/null 2>&1 && readelf -l "$out/$bin" | grep -q INTERP; then
      echo "[rust] $bin is dynamically linked (PT_INTERP); rebuild with RUSTFLAGS='-C target-feature=+crt-static'" >&2
      exit 1
    fi
  done
  echo "[rust] cargo build --release --bin fc-smoke (host)"
  cargo build --release -p tachyon-serverless-provider-firecracker --bin fc-smoke
}

# --- 4. Base rootfs --------------------------------------------------------------------
build_rootfs() {
  BRIDGE_BIN="${BRIDGE_BIN:-$REPO_ROOT/target/$MUSL_TARGET/release/tachyon-serverless-runtime-bridge}" \
    "$REPO_ROOT/scripts/kvm/build-rootfs.sh"
}

summary() {
  echo
  echo "bootstrap complete ($ARCH, Firecracker $FIRECRACKER_VERSION)"
  printf '  %-64s  %s\n' "$(sha256_of "$FC_BIN")" ".kvm/bin/firecracker"
  printf '  %-64s  %s\n' "$KERNEL_SHA256" ".kvm/vmlinux ($KERNEL_KEY)"
  printf '  %-64s  %s\n' "$(sha256_of "$ROOTFS")" ".kvm/rootfs.ext4"
  local out="$REPO_ROOT/target/$MUSL_TARGET/release"
  for bin in tachyon-serverless-runtime-bridge example-hello example-http-axum example-cpu-burn; do
    printf '  %-64s  %s\n' "$(sha256_of "$out/$bin")" "target/$MUSL_TARGET/release/$bin"
  done
  echo "next: scripts/kvm/smoke.sh"
}

fetch_firecracker
fetch_kernel
write_manifest
build_guest
build_rootfs
summary

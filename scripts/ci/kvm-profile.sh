#!/usr/bin/env bash
# scripts/ci/kvm-profile.sh - record the runtime profile of a KVM integration run (PLT-4645).
#
# Writes <out>/profile.json and <out>/profile.txt describing exactly what was tested, so that
# evidence can be compared across runs and a runtime profile change (Firecracker, guest kernel,
# rootfs, host kernel) is visible: commit, ref, run, Firecracker version and binary sha256, guest
# kernel sha256 / source, rootfs sha256, host uname / CPU / memory, KVM and nested virtualization
# state, Rust toolchain.
#
# Usage: scripts/ci/kvm-profile.sh <out-dir>
# Environment (read when present): GITHUB_SHA GITHUB_REF GITHUB_RUN_ID GITHUB_RUN_ATTEMPT
#   GITHUB_EVENT_NAME GITHUB_REPOSITORY RUNNER_NAME
# Missing values are recorded as null / "unknown"; this script never fails the job on its own
# except for bad usage (exit 2).
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 <out-dir>" >&2; exit 2; }
OUT="$1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KVM_DIR="$REPO_ROOT/.kvm"
mkdir -p "$OUT"

sha256_of() {
  if [ -f "$1" ]; then
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
  else
    echo ""
  fi
}
manifest_get() {
  [ -f "$KVM_DIR/manifest.json" ] || return 0
  sed -n "s/.*\"$1\": *\"\([^\"]*\)\".*/\1/p" "$KVM_DIR/manifest.json" | head -n1
}
# JSON string or null
js() {
  if [ -z "$1" ]; then printf 'null'; else printf '"%s"' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr '\n\t' '  ')"; fi
}

commit="${GITHUB_SHA:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)}"
dirty="unknown"
if git -C "$REPO_ROOT" rev-parse HEAD >/dev/null 2>&1; then
  if [ -z "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=no 2>/dev/null)" ]; then dirty=false; else dirty=true; fi
fi
fc_version=""
[ -x "$KVM_DIR/bin/firecracker" ] && fc_version="$("$KVM_DIR/bin/firecracker" --version 2>/dev/null | head -n1 || true)"
kvm_state="missing"
if [ -e /dev/kvm ]; then
  if [ -r /dev/kvm ] && [ -w /dev/kvm ]; then kvm_state="rw"; else kvm_state="present-not-rw"; fi
fi
# Nested virtualization: a hypervisor flag in /proc/cpuinfo means this host is itself a guest.
nested="unknown"
if [ -r /proc/cpuinfo ]; then
  if grep -qw hypervisor /proc/cpuinfo; then nested=true; else nested=false; fi
fi
cpu_model="$( (grep -m1 -E '^(model name|Model|CPU part)' /proc/cpuinfo 2>/dev/null || true) | sed 's/^[^:]*: *//')"
mem_total_kib="$( (grep -m1 MemTotal /proc/meminfo 2>/dev/null || true) | awk '{print $2}')"
nproc_n="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
rustc_v="$(rustc --version 2>/dev/null || true)"

cat >"$OUT/profile.json" <<EOF
{
  "schema": "tachyon-serverless/kvm-profile/v1",
  "recorded_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "commit": $(js "$commit"),
  "worktree_dirty": $(js "$dirty"),
  "ref": $(js "${GITHUB_REF:-}"),
  "event": $(js "${GITHUB_EVENT_NAME:-}"),
  "repository": $(js "${GITHUB_REPOSITORY:-}"),
  "run_id": $(js "${GITHUB_RUN_ID:-}"),
  "run_attempt": $(js "${GITHUB_RUN_ATTEMPT:-}"),
  "runner_name": $(js "${RUNNER_NAME:-}"),
  "firecracker": {
    "version": $(js "$fc_version"),
    "manifest_version": $(js "$(manifest_get firecracker_version)"),
    "binary_sha256": $(js "$(sha256_of "$KVM_DIR/bin/firecracker")"),
    "tgz_sha256": $(js "$(manifest_get firecracker_tgz_sha256)")
  },
  "guest_kernel": {
    "sha256": $(js "$(sha256_of "$KVM_DIR/vmlinux")"),
    "manifest_sha256": $(js "$(manifest_get kernel_sha256)"),
    "key": $(js "$(manifest_get kernel_key)"),
    "url": $(js "$(manifest_get kernel_url)"),
    "ci_version": $(js "$(manifest_get ci_version)")
  },
  "rootfs": {
    "sha256": $(js "$(sha256_of "$KVM_DIR/rootfs.ext4")")
  },
  "gateway_config_sha256": $(js "$(sha256_of "$REPO_ROOT/config/gateway.firecracker.toml")"),
  "host": {
    "uname": $(js "$(uname -a)"),
    "arch": $(js "$(uname -m)"),
    "kernel_release": $(js "$(uname -r)"),
    "cpu_model": $(js "$cpu_model"),
    "cpus": $(js "$nproc_n"),
    "mem_total_kib": $(js "$mem_total_kib"),
    "kvm": $(js "$kvm_state"),
    "nested_virtualization": $(js "$nested")
  },
  "rustc": $(js "$rustc_v")
}
EOF

{
  echo "commit           $commit (dirty=$dirty)"
  echo "ref / event      ${GITHUB_REF:-} / ${GITHUB_EVENT_NAME:-}"
  echo "run              ${GITHUB_RUN_ID:-} attempt ${GITHUB_RUN_ATTEMPT:-}"
  echo "firecracker      ${fc_version:-unknown}"
  echo "guest kernel     $(sha256_of "$KVM_DIR/vmlinux") $(manifest_get kernel_key)"
  echo "rootfs           $(sha256_of "$KVM_DIR/rootfs.ext4")"
  echo "host             $(uname -a)"
  echo "kvm / nested     $kvm_state / $nested"
} >"$OUT/profile.txt"
cat "$OUT/profile.txt"

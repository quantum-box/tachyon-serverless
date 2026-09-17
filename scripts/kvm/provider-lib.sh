#!/usr/bin/env bash
# scripts/kvm/provider-lib.sh - the provider switch shared by the harnesses that generate their own
# gateway configuration (scripts/queue/*, scripts/usage/*, scripts/chaos/*). Sourced, never run.
#
#   TSLS_PROVIDER=process      (default) dev-only process provider, target/debug guest binaries
#   TSLS_PROVIDER=firecracker  Firecracker microVMs on Linux/KVM: the [provider] and [provider.*]
#                              tables of TSLS_PROVIDER_CONFIG (default config/gateway.firecracker.toml,
#                              i.e. jailer + host cgroup required, so the gateway must run as root)
#                              with the .kvm/ paths made absolute and `workdir` moved into the
#                              harness's scratch directory; guest binaries from
#                              target/<arch>-unknown-linux-musl/release (TSLS_GUEST_DIR overrides).
#                              Build them and the rootfs first (scripts/kvm/bootstrap.sh).
#
# After `provider_init REPO_ROOT`:
#   PROVIDER            process | firecracker
#   GUEST_DIR           directory holding example-* guest binaries
#   provider_toml WORKDIR   prints the provider tables for a config whose scratch dir is WORKDIR
#   provider_is_fc      exit 0 for firecracker
#   provider_leftovers  (firecracker) prints firecracker/jailer processes, tachyon cgroups, jails,
#                       tsls* taps and the egress table that exist right now (empty = clean)
#
# The variables are read by the sourcing scripts (SC2034).
# shellcheck disable=SC2034

provider_init() {
  PROVIDER_REPO_ROOT="$1"
  PROVIDER="${TSLS_PROVIDER:-process}"
  local arch
  case "$(uname -m)" in x86_64 | amd64) arch=x86_64 ;; *) arch=aarch64 ;; esac
  case "$PROVIDER" in
    process)
      GUEST_DIR="${TSLS_GUEST_DIR:-$PROVIDER_REPO_ROOT/target/debug}"
      ;;
    firecracker)
      GUEST_DIR="${TSLS_GUEST_DIR:-$PROVIDER_REPO_ROOT/target/$arch-unknown-linux-musl/release}"
      PROVIDER_CONFIG="${TSLS_PROVIDER_CONFIG:-$PROVIDER_REPO_ROOT/config/gateway.firecracker.toml}"
      [ -f "$PROVIDER_CONFIG" ] || { echo "provider config not found: $PROVIDER_CONFIG" >&2; return 1; }
      [ "$(uname -s)" = Linux ] || { echo "TSLS_PROVIDER=firecracker needs Linux/KVM" >&2; return 1; }
      ;;
    *)
      echo "TSLS_PROVIDER must be process or firecracker (got $PROVIDER)" >&2
      return 1
      ;;
  esac
}

provider_is_fc() { [ "$PROVIDER" = firecracker ]; }

# provider_toml WORKDIR [BRIDGE_BIN]
provider_toml() {
  local workdir="$1" bridge="${2:-$PROVIDER_REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge}"
  if provider_is_fc; then
    awk '/^\[/ { keep = ($0 ~ /^\[provider(\.[a-z_.]+)?\]/) } keep' "$PROVIDER_CONFIG" \
      | sed -e "s#\"\\.kvm/#\"$PROVIDER_REPO_ROOT/.kvm/#g" \
        -e "s#^workdir = \"[^\"]*\"#workdir = \"$workdir/fc\"#"
  else
    cat <<EOF
[provider]
kind = "process"

[provider.process]
bridge_binary = "$bridge"
workdir = "$workdir/process"
EOF
  fi
}

provider_leftovers() {
  provider_is_fc || return 0
  # shellcheck disable=SC2009 # the full command line is needed
  ps -eo pid=,args= | grep -E '(^|/)(firecracker|jailer)( |$)' | grep -v -e grep -e 'ps -eo' || true
  # A missing directory is "nothing left" (find fails, and callers run with errexit + pipefail).
  { find /sys/fs/cgroup/tachyon -mindepth 1 -maxdepth 1 -type d 2>/dev/null || true; } | sed 's/^/cgroup /'
  { find /srv/jailer/firecracker -mindepth 1 -maxdepth 1 2>/dev/null || true; } | sed 's/^/jail /'
  { ip -br link 2>/dev/null || true; } | awk '$1 ~ /^tsls/ {print "tap " $1}'
  { nft list tables 2>/dev/null || true; } | { grep tachyon_egress || true; }
  return 0
}

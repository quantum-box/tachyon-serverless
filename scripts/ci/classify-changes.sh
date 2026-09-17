#!/usr/bin/env bash
# scripts/ci/classify-changes.sh - decide which CI gates a change needs (PLT-4645).
#
# Reads changed paths (one per line) and prints `key=value` lines:
#
#   docs_only=true|false     every path is documentation -> heavy Rust jobs may be skipped
#   kvm_required=true|false  a path touches the runtime / network / kernel / billing surface
#                            -> the KVM integration gate must pass before merge
#   contract=true|false      a path touches the API or wire contract (informational)
#   kvm_paths=<a,b,...>      the paths that made KVM required (first 20)
#   changed_count=<n>
#
# Usage:
#   scripts/ci/classify-changes.sh --base <sha> --head <sha>   # git diff --name-only base...head
#   scripts/ci/classify-changes.sh --all                       # unknown range: assume everything
#   printf '%s\n' path1 path2 | scripts/ci/classify-changes.sh --stdin
#
# When GITHUB_OUTPUT is set, the key=value lines are appended to it as well.
#
# The rules are deliberately path-based and conservative: an unknown range (first push of a
# branch, force-push, shallow history) classifies as "everything changed", and a path that is
# not recognised as documentation is never docs-only. The same rules are documented in
# docs/ci.md; scripts/ci/selftest.sh pins them.
set -euo pipefail

MODE=""
BASE=""
HEAD=""
while [ $# -gt 0 ]; do
  case "$1" in
    --base) BASE="$2"; MODE=git; shift 2 ;;
    --head) HEAD="$2"; shift 2 ;;
    --all) MODE=all; shift ;;
    --stdin) MODE=stdin; shift ;;
    -h | --help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$MODE" ] || { echo "usage: $0 --base SHA --head SHA | --all | --stdin" >&2; exit 2; }

# is_docs PATH -> documentation that cannot change behaviour or a contract.
is_docs() {
  case "$1" in
    docs/openapi.json) return 1 ;; # the OpenAPI snapshot is a contract, not prose
    docs/*) return 0 ;;
    *.md) return 0 ;; # including .github/pull_request_template.md
    LICENSE | LICENSE.*) return 0 ;;
    .github/ISSUE_TEMPLATE/*) return 0 ;;
  esac
  return 1
}

# is_kvm PATH -> runtime / network / kernel / billing surface that only a real microVM verifies.
is_kvm() {
  case "$1" in
    *.md) return 1 ;; # prose next to the code
    crates/providers/firecracker/*) return 0 ;;   # VMM driver, egress gate, drives, boot args
    crates/runtime-bridge/*) return 0 ;;          # guest init / bridge (runs as /sbin/tachyon-init)
    crates/protocol/*) return 0 ;;                # host <-> guest wire + Runtime API
    crates/provider-port/*) return 0 ;;           # ExecutionProvider / capabilities / usage port
    crates/application/src/bridge_session.rs) return 0 ;; # host half of the wire protocol
    crates/application/src/services/pool.rs) return 0 ;;  # quiesce / resume of real VMs
    crates/domain/src/egress.rs) return 0 ;;              # egress allowlist -> host nftables / tap policy
    # Not KVM: services/dispatcher.rs, repository/slot.rs (PLT-4631 lease / fencing): control-plane
    # CAS logic verified deterministically by the security regression group (lease_epoch).
    # future billing / usage / metering code anywhere under crates/ (`*` also matches `/` here)
    crates/*billing* | crates/*usage* | crates/*metering*) return 0 ;;
    scripts/kvm/*) return 0 ;;                    # bootstrap / smoke / measurements / teardown
    scripts/e2e/*) return 0 ;;                    # the E2E that runs against firecracker
    scripts/ci/kvm-*) return 0 ;;                 # KVM job helpers (profile, gate)
    config/gateway.firecracker.toml) return 0 ;;  # runtime profile of the firecracker gateway
    examples/hello/* | examples/cpu-burn/* | examples/isolation-probe/*) return 0 ;; # guest probes
    .github/workflows/kvm-integration.yml) return 0 ;;
    rust-toolchain.toml) return 0 ;;              # guest binaries are built with it
  esac
  return 1
}

is_contract() {
  case "$1" in
    crates/protocol/* | crates/api-types/* | apps/gateway/* | docs/openapi.json) return 0 ;;
    scripts/ci/security-regression.list) return 0 ;;
  esac
  return 1
}

emit() {
  echo "$1"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then echo "$1" >>"$GITHUB_OUTPUT"; fi
}

if [ "$MODE" = all ]; then
  emit "docs_only=false"
  emit "kvm_required=true"
  emit "contract=true"
  emit "kvm_paths=<unknown range: every path assumed changed>"
  emit "changed_count=-1"
  exit 0
fi

if [ "$MODE" = git ]; then
  [ -n "$HEAD" ] || { echo "--head is required with --base" >&2; exit 2; }
  if ! CHANGED="$(git diff --name-only "$BASE...$HEAD" 2>/dev/null)"; then
    echo "classify-changes: cannot diff $BASE...$HEAD; classifying as everything changed" >&2
    exec "$0" --all
  fi
else
  CHANGED="$(cat)"
fi

count=0
docs_only=true
kvm=false
contract=false
kvm_paths=""
kvm_n=0
while IFS= read -r path; do
  [ -n "$path" ] || continue
  count=$((count + 1))
  is_docs "$path" || docs_only=false
  if is_kvm "$path"; then
    kvm=true
    kvm_n=$((kvm_n + 1))
    if [ "$kvm_n" -le 20 ]; then kvm_paths="${kvm_paths:+$kvm_paths,}$path"; fi
  fi
  if is_contract "$path"; then contract=true; fi
done <<EOF
$CHANGED
EOF

# An empty change set (e.g. an empty merge) proves nothing: run the normal gates.
if [ "$count" -eq 0 ]; then docs_only=false; fi

emit "docs_only=$docs_only"
emit "kvm_required=$kvm"
emit "contract=$contract"
emit "kvm_paths=$kvm_paths"
emit "changed_count=$count"

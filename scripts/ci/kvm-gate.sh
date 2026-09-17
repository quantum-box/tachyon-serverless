#!/usr/bin/env bash
# scripts/ci/kvm-gate.sh - decide the "kvm-gate" status (PLT-4645).
#
# The KVM integration job runs on a self-hosted runner that may not exist, may not be allowed to
# run this event, or may not have been asked to (missing `kvm` label). This script turns that
# into an explicit status so that "KVM did not run" is never shown as a pass:
#
#   kvm not required                         -> exit 0, "not required"
#   required, job result success             -> exit 0, "passed"
#   required, job skipped (untrusted / no label / fork) -> exit 1 with what to do
#   required, job failure / cancelled / other -> exit 1
#
# A job that is still queued because no runner with [self-hosted, linux, kvm] is online never
# reaches this script: the workflow's kvm-gate job `needs` it, so the check stays pending.
#
# Usage: scripts/ci/kvm-gate.sh <kvm_required:true|false> <kvm_job_result> <event> <trusted:true|false> <labeled:true|false>
#   kvm_job_result: success | failure | cancelled | skipped | "" (as in needs.<job>.result)
# Writes a markdown summary to GITHUB_STEP_SUMMARY when set.
set -euo pipefail

if [ $# -ne 5 ]; then
  echo "usage: $0 <kvm_required> <kvm_job_result> <event> <trusted> <labeled>" >&2
  exit 2
fi
REQUIRED="$1"
RESULT="${2:-skipped}"
EVENT="$3"
TRUSTED="$4"
LABELED="$5"

summary() {
  echo "$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf '### kvm-gate\n\n%s\n' "$1" >>"$GITHUB_STEP_SUMMARY"
  fi
}

if [ "$REQUIRED" != true ]; then
  summary "kvm-gate: NOT REQUIRED - no runtime/network/kernel/billing path changed (KVM integration did not run and is not claimed as passed)."
  exit 0
fi

case "$RESULT" in
  success)
    summary "kvm-gate: PASSED - KVM integration ran on a self-hosted KVM runner and passed (evidence artifact attached to the run)."
    exit 0
    ;;
  skipped)
    if [ "$TRUSTED" != true ]; then
      summary "kvm-gate: FAILED - KVM integration is REQUIRED but was NOT RUN: the change comes from a fork or an untrusted event, which is never scheduled on the KVM runner. A maintainer must re-create the change on a branch of this repository (after review) and label that PR \`kvm\`."
    elif [ "$EVENT" = pull_request ] && [ "$LABELED" != true ]; then
      summary "kvm-gate: FAILED - KVM integration is REQUIRED but was NOT RUN: a maintainer must review the diff and add the \`kvm\` label to this pull request (docs/ci.md)."
    else
      summary "kvm-gate: FAILED - KVM integration is REQUIRED but was NOT RUN (event=$EVENT). Dispatch .github/workflows/kvm-integration.yml manually (docs/ci.md)."
    fi
    exit 1
    ;;
  *)
    summary "kvm-gate: FAILED - KVM integration is REQUIRED and finished with '$RESULT' (see the kvm job log and its evidence artifact)."
    exit 1
    ;;
esac

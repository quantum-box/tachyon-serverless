#!/usr/bin/env bash
# Verify that no execution environment survived the demo.
#
#   scripts/e2e/orphan-check.sh [process|firecracker] [FC_RUN_DIR]
#
# process:     no runtime bridge process (argv[0] ending in
#              `tachyon-serverless-runtime-bridge`) and no leftover user process
#              (example binaries or an executable under an `artifacts/` store)
#              may be alive.
# firecracker: additionally no `firecracker` process whose command line refers to
#              FC_RUN_DIR (default `.kvm/run`), and no files left under FC_RUN_DIR.
#
# Environment:
#   TSLS_BRIDGE_BIN            when set, only a bridge with exactly this argv[0]
#                              counts (scopes the check to one build / run).
#   TSLS_ORPHAN_USER_PATTERN   extended regex for leftover user processes.
#
# Patterns are anchored to argv[0] so that `cargo test -p tachyon-serverless-runtime-bridge`
# or a shell whose command line mentions the name are not reported.
#
# Exit 0 when clean, 1 when leftovers were found (listed on stderr), 2 on bad usage.
set -euo pipefail

PROVIDER="${1:-${TSLS_PROVIDER:-process}}"
RUN_DIR="${2:-${TSLS_FC_RUN_DIR:-.kvm/run}}"
USER_PATTERN="${TSLS_ORPHAN_USER_PATTERN:-^([^ ]*/)?(example-hello|example-http-axum|example-cpu-burn)( |$)|^[^ ]*/artifacts/[^ ]*( |$)}"
rc=0

escape_regex() {
  printf '%s' "$1" | sed 's/[][\.*^$/+?(){}|]/\\&/g'
}

if [ -n "${TSLS_BRIDGE_BIN:-}" ]; then
  BRIDGE_PATTERN="^$(escape_regex "$TSLS_BRIDGE_BIN")( |$)"
else
  BRIDGE_PATTERN='^([^ ]*/)?tachyon-serverless-runtime-bridge( |$)'
fi

# list_matching REGEX -> `pid ppid etime command` lines of matching processes (excluding us).
list_matching() {
  local pattern="$1" pids
  pids="$(pgrep -f -- "$pattern" 2>/dev/null || true)"
  pids="$(printf '%s\n' "$pids" | grep -v -x -e "$$" -e "$PPID" -e '' || true)"
  [ -n "$pids" ] || return 0
  # shellcheck disable=SC2086
  ps -o pid=,ppid=,etime=,command= -p "$(printf '%s' "$pids" | tr '\n' ',' | sed 's/,$//')" 2>/dev/null || true
}

report() {
  local title="$1" lines="$2"
  [ -n "$lines" ] || return 0
  echo "orphan-check: $title:" >&2
  printf '%s\n' "$lines" >&2
  rc=1
}

case "$PROVIDER" in
  process|firecracker) ;;
  *)
    echo "orphan-check: unknown provider '$PROVIDER' (expected process|firecracker)" >&2
    exit 2
    ;;
esac

report "leftover runtime bridge processes" "$(list_matching "$BRIDGE_PATTERN")"
report "leftover user processes" "$(list_matching "$USER_PATTERN")"

if [ "$PROVIDER" = "firecracker" ]; then
  report "leftover firecracker processes under $RUN_DIR" \
    "$(list_matching '^([^ ]*/)?firecracker( |$)' | grep -F -- "$RUN_DIR" || true)"
  if [ -d "$RUN_DIR" ]; then
    report "leftover files under $RUN_DIR (sockets/drives/workdirs must be removed on terminate)" \
      "$(find "$RUN_DIR" -mindepth 1 -maxdepth 1 -not -name _archive 2>/dev/null || true)"
  fi
fi

if [ "$rc" -eq 0 ]; then
  echo "orphan-check: clean ($PROVIDER)"
fi
exit "$rc"

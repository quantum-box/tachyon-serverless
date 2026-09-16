#!/usr/bin/env bash
# Self-test of the e2e helpers (lib.sh, orphan-check.sh) with a stub `tsls`.
# Runs anywhere bash + jq + curl exist; no gateway or cargo build needed.
#
#   scripts/e2e/selftest.sh
#
# Check functions are invoked indirectly through `check`, which shellcheck
# cannot follow (SC2317).
# shellcheck disable=SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/e2e/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_tools jq curl pgrep || e2e_die "missing tools"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/tsls-selftest.XXXXXX")"
export E2E_STATE_DIR="$WORK/state"
export STEP_LOG_DIR="$WORK/steps"
mkdir -p "$STEP_LOG_DIR"
BG=""
cleanup() {
  for p in $BG; do kill "$p" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

FAILURES=0
check() {
  # check NAME CMD... : runs CMD (in the current shell, errexit off) and counts failures.
  local name="$1"
  shift
  set +e
  "$@"
  local rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    e2e_log "ok   $name"
  else
    e2e_log "FAIL $name (exit $rc)"
    FAILURES=$(( FAILURES + 1 ))
  fi
}

# ---------------------------------------------------------------------------
# stub tsls: emulates the subset of commands the helpers rely on
# ---------------------------------------------------------------------------
STUB_DIR="$WORK/bin"
mkdir -p "$STUB_DIR"
cat > "$STUB_DIR/tsls" <<'EOF'
#!/usr/bin/env bash
# Stub tsls used by scripts/e2e/selftest.sh.
set -u
cmd="${1:-} ${2:-}"
payload="{}"
args=("$@")
i=0
while [ "$i" -lt "${#args[@]}" ]; do
  if [ "${args[$i]}" = "--payload" ]; then payload="${args[$((i+1))]}"; fi
  i=$((i+1))
done
case "$cmd" in
  "functions invoke")
    case "$payload" in
      *fail*)
        echo '{"error":{"code":"user_error","message":"boom","invocation_id":"inv_01hstubfail00000000000000","error_type":"Handler.Error"}}'
        echo 'error: user_error (HTTP 502): boom [type=Handler.Error] [invocation=inv_01hstubfail00000000000000]' >&2
        exit 3 ;;
      *)
        echo '{"message":"hello, demo","greeting":"v1","secret_present":true}'
        echo 'invocation inv_01hstubok0000000000000000 trace=t-1' >&2
        exit 0 ;;
    esac ;;
  "functions get")
    if [ "${3:-}" = "hello" ]; then echo '{"id":"fn_01hstub00000000000000000000","name":"hello"}'; exit 0; fi
    echo '{"error":{"code":"not_found","message":"no"}}'; echo 'error: not_found (HTTP 404): no' >&2; exit 2 ;;
  "provider ")
    echo '{"kind":"process","dev_only":true,"isolation":"process","capabilities":{},"preflight":{"ok":true,"checks":[]}}' ;;
  *)
    echo "stub tsls: unsupported: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$STUB_DIR/tsls"
export PATH="$STUB_DIR:$PATH"

# ---------------------------------------------------------------------------
# syntax of every script
# ---------------------------------------------------------------------------
syntax_ok() {
  local f
  for f in "$SCRIPT_DIR"/*.sh; do
    bash -n "$f" || return 1
  done
}
check "bash -n on scripts/e2e/*.sh" syntax_ok

# ---------------------------------------------------------------------------
# assertions
# ---------------------------------------------------------------------------
check "assert_eq passes on equal" assert_eq a a
assert_eq_fails() { ! assert_eq a b 2>/dev/null; }
check "assert_eq fails on different" assert_eq_fails
check "assert_ne" assert_ne a b
check "assert_contains" assert_contains "hello world" "lo wo"
assert_contains_fails() { ! assert_contains "hello" "xyz" 2>/dev/null; }
check "assert_contains fails when missing" assert_contains_fails
check "assert_lt" assert_lt 5 10
assert_lt_fails() { ! assert_lt 10 5 2>/dev/null; }
check "assert_lt fails" assert_lt_fails
check "assert_json" assert_json '{"a":{"b":"x"}}' '.a.b' "x"
assert_json_bad() { ! assert_json 'not json' '.a' "x" 2>/dev/null; }
check "assert_json rejects invalid JSON" assert_json_bad

# ---------------------------------------------------------------------------
# run_capture / invoke_capture with the stub
# ---------------------------------------------------------------------------
capture_ok() {
  run_capture tsls functions get hello --json
  assert_eq 0 "$RUN_RC" && assert_json "$RUN_OUT" '.name' hello
}
check "run_capture success" capture_ok

capture_err() {
  run_capture tsls functions get nope --json
  assert_eq 2 "$RUN_RC" && assert_json "$RUN_OUT" '.error.code' not_found && assert_contains "$RUN_ERR" "not_found"
}
check "run_capture failure keeps stdout/stderr/rc" capture_err

invoke_ok() {
  invoke_capture hello '{"name":"demo"}'
  assert_eq 0 "$INVOKE_RC" \
    && assert_json "$INVOKE_OUT" '.message' "hello, demo" \
    && assert_eq "inv_01hstubok0000000000000000" "$INVOKE_ID" "id from stderr" \
    && [ "$INVOKE_MS" -ge 0 ]
}
check "invoke_capture parses invocation id on success" invoke_ok

invoke_fail() {
  invoke_capture hello '{"fail":true}'
  assert_eq 3 "$INVOKE_RC" \
    && assert_json "$INVOKE_OUT" '.error.code' user_error \
    && assert_eq "inv_01hstubfail00000000000000" "$INVOKE_ID" "id from error"
}
check "invoke_capture parses invocation id on failure" invoke_fail

# ---------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------
state_roundtrip() {
  state_set fn.hello fn_x
  ( assert_eq fn_x "$(state_get fn.hello)" ) && assert_eq "dflt" "$(state_get missing dflt)"
}
check "state_set/state_get across subshells" state_roundtrip

# ---------------------------------------------------------------------------
# step accounting + summary
# ---------------------------------------------------------------------------
steps_flow() {
  steps_reset
  step "passing step" true
  step "failing step" false
  step "state visible in step" assert_eq fn_x "$(state_get fn.hello)"
  step_skip "skipped step" "not applicable"
  assert_eq 4 "${#STEP_NAMES[@]}" "recorded steps" || return 1
  assert_eq PASS "${STEP_STATUS[0]}" || return 1
  assert_eq FAIL "${STEP_STATUS[1]}" || return 1
  assert_eq PASS "${STEP_STATUS[2]}" || return 1
  assert_eq SKIP "${STEP_STATUS[3]}" || return 1
  assert_eq 1 "$(steps_failed_count)" || return 1
  [ -f "$STEP_LOG_DIR/01-passing_step.log" ] || { echo "missing step log" >&2; return 1; }
  steps_write_summary "$WORK/summary.json" '{"provider":"stub"}'
  assert_json "$(cat "$WORK/summary.json")" '.failed' 1 || return 1
  assert_json "$(cat "$WORK/summary.json")" '.passed' 2 || return 1
  assert_json "$(cat "$WORK/summary.json")" '.skipped' 1 || return 1
  assert_json "$(cat "$WORK/summary.json")" '.ok' false || return 1
  assert_json "$(cat "$WORK/summary.json")" '.provider' stub || return 1
  assert_json "$(cat "$WORK/summary.json")" '.steps[1].name' "failing step" || return 1
  ! steps_print_table 2>/dev/null || { echo "table should report failure" >&2; return 1; }
  steps_reset
  step "only pass" true
  steps_print_table 2>/dev/null
}
check "step accounting, summary.json and table" steps_flow

# ---------------------------------------------------------------------------
# wait_for_http / stop_process
# ---------------------------------------------------------------------------
closed_port_times_out() {
  ! wait_for_http "http://127.0.0.1:1/healthz" 1 2>/dev/null
}
check "wait_for_http times out on a closed port" closed_port_times_out

if command -v python3 >/dev/null 2>&1; then
  http_server_ok() {
    local port
    port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
    ( cd "$WORK" && exec python3 -m http.server "$port" --bind 127.0.0.1 >/dev/null 2>&1 ) &
    local pid=$!
    BG="$BG $pid"
    wait_for_http "http://127.0.0.1:$port/" 10 200 || return 1
    wait_for_http "http://127.0.0.1:$port/missing" 5 404 || return 1
    stop_process "$pid" 5
    # python exits 143 on SIGTERM (or 0 if it handled it); both mean "stopped".
    [ "$STOP_RC" = "143" ] || [ "$STOP_RC" = "0" ] || { echo "unexpected exit $STOP_RC" >&2; return 1; }
    ! kill -0 "$pid" 2>/dev/null
  }
  check "wait_for_http against a local server, stop_process" http_server_ok
else
  e2e_log "skip: python3 not available for the HTTP server check"
fi

# ---------------------------------------------------------------------------
# orphan-check.sh: clean, then with a fake bridge process, then clean again
# ---------------------------------------------------------------------------
# Scope the check to a fake binary path so concurrent test runs on the same
# machine (which legitimately spawn real bridges) do not interfere.
export TSLS_BRIDGE_BIN="$WORK/fake/tachyon-serverless-runtime-bridge"
orphan_clean() { "$SCRIPT_DIR/orphan-check.sh" process; }
check "orphan-check.sh is clean" orphan_clean

orphan_detects_fake_bridge() {
  # Give a sleeping process the bridge's path via argv[0].
  bash -c 'exec -a "$0" sleep 30' "$TSLS_BRIDGE_BIN" &
  local pid=$!
  BG="$BG $pid"
  sleep 0.5
  local rc=0
  "$SCRIPT_DIR/orphan-check.sh" process 2>/dev/null || rc=$?
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  assert_eq 1 "$rc" "orphan-check must fail while the fake bridge is alive" || return 1
  "$SCRIPT_DIR/orphan-check.sh" process
}
check "orphan-check.sh detects a leftover bridge" orphan_detects_fake_bridge

unknown_provider_rejected() {
  local rc=0
  "$SCRIPT_DIR/orphan-check.sh" bogus 2>/dev/null || rc=$?
  assert_eq 2 "$rc"
}
check "orphan-check.sh rejects unknown provider" unknown_provider_rejected

# ---------------------------------------------------------------------------
if [ "$FAILURES" -eq 0 ]; then
  e2e_log "selftest: all checks passed"
  exit 0
fi
e2e_log "selftest: $FAILURES check(s) failed"
exit 1

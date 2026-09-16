#!/usr/bin/env bash
# Shared helpers for the e2e scripts. Sourced, never executed.
#
# Compatible with bash 3.2 (macOS default): no associative arrays, no mapfile.
# Requires: curl, jq. Optional: perl (millisecond timestamps).
#
# Result variables (RUN_*, INVOKE_*, STOP_RC, STEP_*) are read by the sourcing
# script, hence the file-wide SC2034 exemption.
# shellcheck disable=SC2034

# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------

e2e_log() { printf '[e2e] %s\n' "$*" >&2; }
e2e_warn() { printf '[e2e] WARN: %s\n' "$*" >&2; }
e2e_die() { printf '[e2e] FATAL: %s\n' "$*" >&2; exit 1; }

# Milliseconds since the epoch (perl when available, else seconds * 1000).
now_ms() {
  if command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
  else
    echo $(( $(date +%s) * 1000 ))
  fi
}

require_tools() {
  local missing=0 t
  for t in "$@"; do
    if ! command -v "$t" >/dev/null 2>&1; then
      e2e_warn "required tool not found: $t"
      missing=1
    fi
  done
  return $missing
}

# ---------------------------------------------------------------------------
# assertions (return 1 on failure; never exit)
# ---------------------------------------------------------------------------

assert_eq() {
  local expected="$1" actual="$2" msg="${3:-}"
  if [ "$expected" = "$actual" ]; then
    return 0
  fi
  printf 'ASSERT_EQ failed%s: expected [%s] got [%s]\n' "${msg:+ ($msg)}" "$expected" "$actual" >&2
  return 1
}

assert_ne() {
  local unexpected="$1" actual="$2" msg="${3:-}"
  if [ "$unexpected" != "$actual" ]; then
    return 0
  fi
  printf 'ASSERT_NE failed%s: value [%s]\n' "${msg:+ ($msg)}" "$actual" >&2
  return 1
}

assert_contains() {
  local haystack="$1" needle="$2" msg="${3:-}"
  case "$haystack" in
    *"$needle"*) return 0 ;;
  esac
  printf 'ASSERT_CONTAINS failed%s: [%s] not found in:\n%s\n' "${msg:+ ($msg)}" "$needle" "$haystack" >&2
  return 1
}

assert_lt() {
  local actual="$1" limit="$2" msg="${3:-}"
  if [ "$actual" -lt "$limit" ]; then
    return 0
  fi
  printf 'ASSERT_LT failed%s: %s is not < %s\n' "${msg:+ ($msg)}" "$actual" "$limit" >&2
  return 1
}

# assert_json JSON JQ_FILTER EXPECTED  -- compares `jq -r FILTER` output.
assert_json() {
  local json="$1" filter="$2" expected="$3" actual
  actual="$(printf '%s' "$json" | jq -r "$filter" 2>/dev/null)" || {
    printf 'ASSERT_JSON failed: invalid JSON for filter %s:\n%s\n' "$filter" "$json" >&2
    return 1
  }
  assert_eq "$expected" "$actual" "jq $filter"
}

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

# wait_for_http URL TIMEOUT_SECS [EXPECTED_STATUS=200]
wait_for_http() {
  local url="$1" timeout="$2" expected="${3:-200}" deadline code
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url" 2>/dev/null || true)"
    if [ "$code" = "$expected" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      e2e_warn "wait_for_http: $url did not return $expected within ${timeout}s (last: ${code:-none})"
      return 1
    fi
    sleep 0.25
  done
}

# ---------------------------------------------------------------------------
# command capture: run_capture CMD... -> RUN_RC, RUN_OUT, RUN_ERR
# ---------------------------------------------------------------------------

RUN_RC=0
RUN_OUT=""
RUN_ERR=""

run_capture() {
  local errfile
  errfile="$(mktemp "${TMPDIR:-/tmp}/e2e-err.XXXXXX")"
  set +e
  RUN_OUT="$("$@" 2>"$errfile")"
  RUN_RC=$?
  set -e
  RUN_ERR="$(cat "$errfile")"
  rm -f "$errfile"
  return 0
}

# invoke_capture FN PAYLOAD [EXTRA ARGS...] -> INVOKE_RC, INVOKE_OUT, INVOKE_ERR, INVOKE_ID, INVOKE_MS
# Uses the `tsls` function/command available in the caller's environment.
INVOKE_RC=0
INVOKE_OUT=""
INVOKE_ERR=""
INVOKE_ID=""
INVOKE_MS=0

invoke_capture() {
  local fn="$1" payload="$2" start
  shift 2
  start="$(now_ms)"
  run_capture tsls functions invoke "$fn" --payload "$payload" --json "$@"
  INVOKE_MS=$(( $(now_ms) - start ))
  INVOKE_RC=$RUN_RC
  INVOKE_OUT=$RUN_OUT
  INVOKE_ERR=$RUN_ERR
  # The CLI prints `invocation inv_... [trace=...]` on stderr; failures carry
  # `[invocation=inv_...]` in the error line and `invocation_id` in the JSON body.
  INVOKE_ID="$(printf '%s\n' "$INVOKE_ERR" | sed -n 's/.*invocation[ =]\(inv_[0-9a-z]*\).*/\1/p' | head -n 1)"
  if [ -z "$INVOKE_ID" ] && [ -n "$INVOKE_OUT" ]; then
    INVOKE_ID="$(printf '%s' "$INVOKE_OUT" | jq -r '.error.invocation_id // empty' 2>/dev/null || true)"
  fi
  return 0
}

# ---------------------------------------------------------------------------
# step accounting
# ---------------------------------------------------------------------------

STEP_NAMES=()
STEP_STATUS=()
STEP_MS=()
STEP_MSG=()
STEP_LOG_DIR="${STEP_LOG_DIR:-}"

steps_reset() {
  STEP_NAMES=()
  STEP_STATUS=()
  STEP_MS=()
  STEP_MSG=()
}

_step_record() {
  STEP_NAMES+=("$1")
  STEP_STATUS+=("$2")
  STEP_MS+=("$3")
  STEP_MSG+=("$4")
}

# step NAME CMD...  -- runs CMD in a subshell with errexit, records PASS/FAIL
# and duration, never aborts the caller. Output goes to $STEP_LOG_DIR/<n>-NAME.log
# when STEP_LOG_DIR is set, otherwise to stderr.
step() {
  local name="$1" start rc logfile slug idx
  shift
  start="$(now_ms)"
  idx=$(( ${#STEP_NAMES[@]} + 1 ))
  slug="$(printf '%s' "$name" | tr -c 'A-Za-z0-9_-' '_')"
  e2e_log "step $idx: $name"
  set +e
  if [ -n "$STEP_LOG_DIR" ]; then
    logfile="$STEP_LOG_DIR/$(printf '%02d' "$idx")-$slug.log"
    ( set -e; "$@" ) >"$logfile" 2>&1
    rc=$?
  else
    logfile=""
    ( set -e; "$@" )
    rc=$?
  fi
  set -e
  local ms=$(( $(now_ms) - start ))
  if [ "$rc" -eq 0 ]; then
    _step_record "$name" "PASS" "$ms" ""
    e2e_log "  PASS (${ms} ms)"
  else
    local msg="exit $rc"
    if [ -n "$logfile" ]; then
      msg="exit $rc; see $logfile"
      e2e_log "  FAIL (${ms} ms): $msg"
      tail -n 15 "$logfile" | sed 's/^/    | /' >&2
    else
      e2e_log "  FAIL (${ms} ms): $msg"
    fi
    _step_record "$name" "FAIL" "$ms" "$msg"
  fi
  return 0
}

# step_skip NAME REASON
step_skip() {
  _step_record "$1" "SKIP" 0 "$2"
  e2e_log "step: $1 SKIPPED ($2)"
}

steps_failed_count() {
  local n=0 s
  for s in "${STEP_STATUS[@]+"${STEP_STATUS[@]}"}"; do
    [ "$s" = "FAIL" ] && n=$(( n + 1 ))
  done
  echo "$n"
}

# steps_print_table -- prints the PASS/FAIL table; returns 1 if any step failed.
steps_print_table() {
  local i n
  n=${#STEP_NAMES[@]}
  printf '\n%-4s %-6s %-9s %s\n' "#" "STATUS" "MS" "STEP" >&2
  printf '%-4s %-6s %-9s %s\n' "----" "------" "---------" "----------------------------------------" >&2
  i=0
  local msg
  while [ "$i" -lt "$n" ]; do
    msg="${STEP_MSG[$i]}"
    [ -z "$msg" ] || msg="  ($msg)"
    printf '%-4s %-6s %-9s %s%s\n' "$(( i + 1 ))" "${STEP_STATUS[$i]}" "${STEP_MS[$i]}" "${STEP_NAMES[$i]}" "$msg" >&2
    i=$(( i + 1 ))
  done
  local failed
  failed="$(steps_failed_count)"
  printf '\n%s steps, %s failed\n' "$n" "$failed" >&2
  [ "$failed" -eq 0 ]
}

# steps_write_summary OUTFILE [EXTRA_JSON_OBJECT]
steps_write_summary() {
  local out="$1" extra="${2:-}" i n rows
  [ -n "$extra" ] || extra='{}'
  n=${#STEP_NAMES[@]}
  rows="[]"
  i=0
  while [ "$i" -lt "$n" ]; do
    rows="$(printf '%s' "$rows" | jq -c \
      --arg name "${STEP_NAMES[$i]}" --arg status "${STEP_STATUS[$i]}" \
      --argjson ms "${STEP_MS[$i]}" --arg msg "${STEP_MSG[$i]}" \
      '. + [{name: $name, status: $status, duration_ms: $ms, message: $msg}]')"
    i=$(( i + 1 ))
  done
  jq -n --argjson steps "$rows" --argjson extra "$extra" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '$extra + {generated_at: $generated_at, steps: $steps,
      passed: ([$steps[] | select(.status == "PASS")] | length),
      failed: ([$steps[] | select(.status == "FAIL")] | length),
      skipped: ([$steps[] | select(.status == "SKIP")] | length),
      ok: (([$steps[] | select(.status == "FAIL")] | length) == 0)}' > "$out"
}

# ---------------------------------------------------------------------------
# key/value state shared between step subshells (files under $E2E_STATE_DIR)
# ---------------------------------------------------------------------------

E2E_STATE_DIR="${E2E_STATE_DIR:-}"

state_set() {
  [ -n "$E2E_STATE_DIR" ] || e2e_die "E2E_STATE_DIR is not set"
  mkdir -p "$E2E_STATE_DIR"
  printf '%s' "$2" > "$E2E_STATE_DIR/$1"
}

state_get() {
  [ -n "$E2E_STATE_DIR" ] || e2e_die "E2E_STATE_DIR is not set"
  if [ -f "$E2E_STATE_DIR/$1" ]; then
    cat "$E2E_STATE_DIR/$1"
  else
    printf '%s' "${2:-}"
  fi
}

# ---------------------------------------------------------------------------
# process helpers
# ---------------------------------------------------------------------------

# stop_process PID [GRACE_SECS=10] -> sets STOP_RC to the exit status; SIGTERM then SIGKILL.
# Must be called from the shell that started PID (not from a command substitution,
# where `wait` cannot see the parent's children).
STOP_RC=0
stop_process() {
  local pid="$1" grace="${2:-10}" waited=0
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt $(( grace * 4 )) ]; do
      sleep 0.25
      waited=$(( waited + 1 ))
    done
    if kill -0 "$pid" 2>/dev/null; then
      e2e_warn "pid $pid did not exit after SIGTERM within ${grace}s; sending SIGKILL"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  set +e
  wait "$pid" 2>/dev/null
  STOP_RC=$?
  set -e
  return 0
}

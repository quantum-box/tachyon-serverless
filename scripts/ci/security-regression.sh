#!/usr/bin/env bash
# scripts/ci/security-regression.sh - run the named "security regression" test group (PLT-4645).
#
# The group is the explicit list in scripts/ci/security-regression.list: tenant authorization,
# environment reuse key, lease / epoch fencing, deadline enforcement, egress gate ordering and
# resource limit validation. Each listed test is run with `--exact`; the script then checks that
# every listed test was reported `ok`. A test that is missing from the output (renamed, removed,
# moved to another module, #[ignore]d) is a FAILURE, not a skip, so coverage cannot drop silently.
#
# Usage:
#   scripts/ci/security-regression.sh                       # the whole group
#   scripts/ci/security-regression.sh --category deadline   # one category (repeatable)
#   scripts/ci/security-regression.sh --list FILE           # another list (used by prove-gates.sh)
#
# Environment:
#   CARGO                cargo binary (default cargo)
#   GITHUB_STEP_SUMMARY  when set, a markdown table is appended to it
#
# Exit codes: 0 every listed test ran and passed, 1 a listed test failed or did not run,
#             2 bad usage or a malformed list.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LIST="$SCRIPT_DIR/security-regression.list"
CARGO="${CARGO:-cargo}"
CATEGORIES=""
KNOWN_CATEGORIES="tenant_authz reuse_key lease_epoch deadline egress_gate resource_limits"

while [ $# -gt 0 ]; do
  case "$1" in
    --category) CATEGORIES="$CATEGORIES $2"; shift 2 ;;
    --list) LIST="$2"; shift 2 ;;
    -h | --help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[ -f "$LIST" ] || { echo "security-regression: list not found: $LIST" >&2; exit 2; }
cd "$REPO_ROOT"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/security-regression.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

wanted_category() {
  [ -z "$CATEGORIES" ] && return 0
  case " $CATEGORIES " in *" $1 "*) return 0 ;; esac
  return 1
}

# --- 1. parse and validate the list ---------------------------------------------------
: >"$WORK/entries"
lineno=0
while IFS= read -r line || [ -n "$line" ]; do
  lineno=$((lineno + 1))
  case "$line" in '' | '#'*) continue ;; esac
  # shellcheck disable=SC2086
  set -- $line
  if [ $# -ne 4 ]; then
    echo "security-regression: $LIST:$lineno: expected 4 fields, got $#: $line" >&2
    exit 2
  fi
  case " $KNOWN_CATEGORIES " in
    *" $1 "*) ;;
    *) echo "security-regression: $LIST:$lineno: unknown category '$1'" >&2; exit 2 ;;
  esac
  case "$3" in
    lib | test:?*) ;;
    *) echo "security-regression: $LIST:$lineno: target must be 'lib' or 'test:<name>', got '$3'" >&2; exit 2 ;;
  esac
  wanted_category "$1" || continue
  printf '%s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$WORK/entries"
done <"$LIST"

for c in $CATEGORIES; do
  case " $KNOWN_CATEGORIES " in
    *" $c "*) ;;
    *) echo "security-regression: unknown category '$c' (known: $KNOWN_CATEGORIES)" >&2; exit 2 ;;
  esac
done
for c in ${CATEGORIES:-$KNOWN_CATEGORIES}; do
  if ! awk -v c="$c" '$1 == c { found = 1 } END { exit !found }' "$WORK/entries"; then
    echo "security-regression: category '$c' has no tests in $LIST" >&2
    exit 1
  fi
done

total="$(wc -l <"$WORK/entries" | tr -d ' ')"
echo "security-regression: $total listed tests from ${LIST#"$REPO_ROOT"/}"

# --- 2. run one cargo invocation per (package, target) ---------------------------------
awk '{ print $2, $3 }' "$WORK/entries" | sort -u >"$WORK/groups"
group=0
while read -r pkg target; do
  group=$((group + 1))
  names="$(awk -v p="$pkg" -v t="$target" '$2 == p && $3 == t { print $4 }' "$WORK/entries")"
  case "$target" in
    lib) target_args="--lib" ;;
    test:*) target_args="--test ${target#test:}" ;;
  esac
  echo
  echo "::group::cargo test -p $pkg $target_args ($(printf '%s\n' "$names" | wc -l | tr -d ' ') tests)"
  # shellcheck disable=SC2086
  if "$CARGO" test -p "$pkg" $target_args -- --exact $names >"$WORK/out.$group" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  cat "$WORK/out.$group"
  echo "::endgroup::"
  for name in $names; do
    if grep -Fqx "test $name ... ok" "$WORK/out.$group"; then
      status=ok
    elif grep -Fq "test $name ... FAILED" "$WORK/out.$group"; then
      status=FAILED
    elif grep -Fq "test $name ... ignored" "$WORK/out.$group"; then
      status=IGNORED
    elif [ "$rc" -ne 0 ] && ! grep -q '^running [0-9]* test' "$WORK/out.$group"; then
      status=BUILD_FAILED
    else
      status=MISSING
    fi
    printf '%s %s %s %s\n' "$pkg" "$target" "$name" "$status" >>"$WORK/results"
  done
done <"$WORK/groups"

# --- 3. report --------------------------------------------------------------------------
echo
echo "security regression group"
printf '%-8s %-16s %s\n' STATUS CATEGORY TEST
bad=0
while read -r category pkg target name; do
  status="$(awk -v p="$pkg" -v t="$target" -v n="$name" '$1 == p && $2 == t && $3 == n { print $4 }' "$WORK/results")"
  printf '%-8s %-16s %s %s %s\n' "$status" "$category" "$pkg" "$target" "$name"
  [ "$status" = ok ] || bad=$((bad + 1))
done <"$WORK/entries"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### Security regression group"
    echo
    echo "| status | category | package | target | test |"
    echo "|---|---|---|---|---|"
    while read -r category pkg target name; do
      status="$(awk -v p="$pkg" -v t="$target" -v n="$name" '$1 == p && $2 == t && $3 == n { print $4 }' "$WORK/results")"
      echo "| $status | $category | \`$pkg\` | $target | \`$name\` |"
    done <"$WORK/entries"
  } >>"$GITHUB_STEP_SUMMARY"
fi

echo
if [ "$bad" -ne 0 ]; then
  echo "security-regression: FAIL - $bad of $total listed tests did not pass." >&2
  echo "  MISSING means the test no longer exists under that exact path (renamed, moved or removed)." >&2
  echo "  Fix the regression, or update scripts/ci/security-regression.list in the same change." >&2
  exit 1
fi
echo "security-regression: PASS - all $total listed tests ran and passed"

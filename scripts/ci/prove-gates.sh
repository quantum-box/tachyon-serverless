#!/usr/bin/env bash
# scripts/ci/prove-gates.sh - prove that the CI gates fail on known-bad changes (PLT-4645).
#
# Copies the working tree (without .git, target/, .kvm/, data/, docs/evidence/) to a temporary
# directory, checks that every gate passes on the unmodified copy (baseline), then applies one
# known-bad change at a time, runs the gate that must catch it, requires a non-zero exit and
# restores the file. Nothing is committed, no branch is created and the repository itself is
# never modified; only the evidence directory is written.
#
# Usage:
#   scripts/ci/prove-gates.sh                 # all cases, evidence under docs/evidence/ci-gates-<UTC>/
#   scripts/ci/prove-gates.sh --only NAME...  # selected cases (baseline always runs)
#   scripts/ci/prove-gates.sh --list          # print the case names
#
# Environment:
#   EVIDENCE_ROOT        evidence root (default docs/evidence)
#   PROVE_TARGET_DIR     cargo target dir shared by all cases (default target/prove-gates)
#   KEEP_COPY=1          keep the temporary copy for inspection
#
# Exit codes: 0 baseline passed and every case was caught by its gate, 1 otherwise, 2 bad usage.
#
# Case descriptions and code snippets are literal on purpose (SC2016); `cleanup` runs from a
# trap (SC2317).
# shellcheck disable=SC2016,SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$REPO_ROOT/docs/evidence}"
EVIDENCE_DIR="$EVIDENCE_ROOT/ci-gates-$STAMP"
export CARGO_TARGET_DIR="${PROVE_TARGET_DIR:-$REPO_ROOT/target/prove-gates}"
export CARGO_TERM_COLOR=never

# Cases: add_case NAME GATE FILE FROM TO WHAT
#   GATE: security:<category> | golden | openapi | kvm-gate | kvm-gate-cancelled
#   FROM must occur exactly once in FILE; `\n` in FROM / TO means a newline. FILE `-` = no edit.
C_NAME=()
C_GATE=()
C_FILE=()
C_FROM=()
C_TO=()
C_WHAT=()
add_case() {
  C_NAME+=("$1")
  C_GATE+=("$2")
  C_FILE+=("$3")
  C_FROM+=("$4")
  C_TO+=("$5")
  C_WHAT+=("$6")
}
add_case tenant-check-removed security:tenant_authz crates/application/src/authz.rs \
  '    if &principal.tenant_id == owner {' \
  '    if true || &principal.tenant_id == owner {' \
  "ensure_tenant lets a principal reach another tenant's resource"
add_case reuse-key-ignored security:reuse_key crates/application/src/repository/memory.rs \
  '                && &e.reuse_key == key\n' \
  '                && (&e.reuse_key == key || true)\n' \
  'an idle environment is claimed without comparing its reuse key'
add_case epoch-fencing-removed security:lease_epoch crates/domain/src/environment.rs \
  '&self.attempt_id == attempt_id && self.epoch == epoch' \
  '&self.attempt_id == attempt_id && (self.epoch == epoch || true)' \
  'a lease accepts a completion carrying a stale epoch'
add_case elapsed-client-deadline-accepted security:deadline crates/domain/src/invocation.rs \
  '        if deadlines.client_deadline < now {' \
  '        if false && deadlines.client_deadline < now {' \
  'an invocation whose client deadline already elapsed is accepted'
add_case egress-gate-result-ignored security:egress_gate crates/providers/firecracker/src/provider.rs \
  '        check_vm_config(&vm_config, expected_nic.as_ref())\n            .map_err(|e| Self::boot_error(paths, e))?;' \
  '        let _ = check_vm_config(&vm_config, expected_nic.as_ref());' \
  'the pre-boot egress check is not enforced, so a VM with a NIC reaches InstanceStart'
add_case memory-lower-bound-removed security:resource_limits crates/domain/src/revision.rs \
  'if r.memory_mib < limits.min_memory_mib || r.memory_mib > limits.max_memory_mib {' \
  'if r.memory_mib > limits.max_memory_mib {' \
  'a revision below the minimum memory passes validation'
add_case security-test-renamed security:lease_epoch crates/domain/src/environment.rs \
  '    fn lease_fencing() {' \
  '    fn lease_fencing_v2() {' \
  'a listed security test is renamed without updating the list (coverage must not drop silently)'
add_case wire-field-renamed golden crates/protocol/src/wire.rs \
  '        remaining_ms: u64,\n        trace_id: String,' \
  '        #[serde(rename = "remaining")]\n        remaining_ms: u64,\n        trace_id: String,' \
  'HostMessage::Invoke.remaining_ms is renamed on the wire'
add_case wire-tag-renamed golden crates/protocol/src/wire.rs \
  '    Pong {\n        nonce: u64,' \
  '    #[serde(rename = "pong_v2")]\n    Pong {\n        nonce: u64,' \
  'the `pong` frame type tag changes'
add_case runtime-api-header-renamed golden crates/protocol/src/runtime_api.rs \
  'pub const DEADLINE_MS: &str = "tachyon-deadline-ms";' \
  'pub const DEADLINE_MS: &str = "tachyon-deadline";' \
  'a Runtime API header name changes'
add_case wire-fixture-corrupted golden crates/protocol/tests/golden/wire/host_ping.json \
  '  "nonce": 7,' \
  '  "nonce": 8,' \
  'a committed golden fixture is edited by hand'
add_case openapi-route-changed openapi apps/gateway/src/handlers.rs \
  '#[utoipa::path(get, path = "/healthz", tag = "meta"' \
  '#[utoipa::path(get, path = "/health", tag = "meta"' \
  'a documented route changes without updating docs/openapi.json'
add_case openapi-schema-field-renamed openapi crates/api-types/src/lib.rs \
  '    pub idle_resume: String,\n}' \
  '    #[serde(rename = "idleResume")]\n    pub idle_resume: String,\n}' \
  'an API schema field is renamed on the wire without updating docs/openapi.json'
add_case openapi-snapshot-corrupted openapi docs/openapi.json \
  '"title": "Tachyon Serverless Gateway"' \
  '"title": "Tachyon Serverless Gateway (edited)"' \
  'the committed OpenAPI snapshot is edited by hand'
add_case kvm-required-not-run kvm-gate - - - \
  'a firecracker path changed and the kvm job was skipped (no `kvm` label): kvm-gate must fail, not pass'
add_case kvm-runner-cancelled kvm-gate-cancelled - - - \
  'a scripts/kvm path changed and the kvm job was cancelled (e.g. no runner within 24 h): kvm-gate must fail'

ONLY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --only)
      shift
      while [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; do ONLY="$ONLY $1"; shift; done
      ;;
    --list) printf '%s\n' "${C_NAME[@]}"; exit 0 ;;
    -h | --help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

for tool in rsync perl cargo diff; do
  command -v "$tool" >/dev/null 2>&1 || { echo "prove-gates: missing $tool" >&2; exit 2; }
done

COPY="$(mktemp -d "${TMPDIR:-/tmp}/prove-gates.XXXXXX")"
cleanup() { if [ "${KEEP_COPY:-0}" = 1 ]; then echo "kept copy: $COPY"; else rm -rf "$COPY"; fi; }
trap cleanup EXIT

mkdir -p "$EVIDENCE_DIR/cases"
# No `-t`: the copy gets fresh mtimes. The cargo target dir is shared between runs, and cargo
# decides freshness by mtime, so preserved (older) mtimes would let it reuse test binaries built
# from an earlier run's mutated sources or with an earlier copy's CARGO_MANIFEST_DIR.
rsync -rlp --exclude .git --exclude target --exclude .kvm --exclude /data --exclude docs/evidence \
  --exclude .claude "$REPO_ROOT/" "$COPY/"

# run_gate GATE LOG -> exit code of the gate command, output in LOG
run_gate() {
  local gate="$1" log="$2" rc=0
  (
    cd "$COPY"
    case "$gate" in
      security:*) scripts/ci/security-regression.sh --category "${gate#security:}" ;;
      security) scripts/ci/security-regression.sh ;;
      golden) cargo test -p tachyon-serverless-protocol --test golden ;;
      openapi) cargo test -p tachyon-serverless-gateway --test openapi_snapshot ;;
      kvm-gate)
        # what kvm-integration.yml computes for a same-repo PR touching a firecracker path
        required="$(printf '%s\n' crates/providers/firecracker/src/provider.rs | scripts/ci/classify-changes.sh --stdin | sed -n 's/^kvm_required=//p')"
        echo "classify: kvm_required=$required"
        scripts/ci/kvm-gate.sh "$required" skipped pull_request true false
        ;;
      kvm-gate-cancelled)
        required="$(printf '%s\n' scripts/kvm/smoke.sh | scripts/ci/classify-changes.sh --stdin | sed -n 's/^kvm_required=//p')"
        echo "classify: kvm_required=$required"
        scripts/ci/kvm-gate.sh "$required" cancelled push true false
        ;;
      *) echo "unknown gate $gate" >&2; exit 2 ;;
    esac
  ) >"$log" 2>&1 || rc=$?
  return "$rc"
}

# mutate FILE FROM TO -> 0 when exactly one occurrence was replaced
mutate() {
  local file="$COPY/$1"
  [ -f "$file" ] || { echo "no such file: $1" >&2; return 1; }
  FROM="$2" TO="$3" perl -0 -i -e '
    my $from = $ENV{FROM}; my $to = $ENV{TO};
    $from =~ s/\\n/\n/g; $to =~ s/\\n/\n/g;
    local $/; my $s = <>; my $n = () = $s =~ /\Q$from\E/g;
    die "expected exactly one occurrence, found $n\n" unless $n == 1;
    $s =~ s/\Q$from\E/$to/; print $s;
  ' "$file"
}

commit="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
dirty="$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal 2>/dev/null | grep -cv 'docs/evidence/' || true)"
{
  echo "prove-gates run $STAMP"
  echo "commit      $commit (changed or untracked paths outside docs/evidence at run time: $dirty)"
  echo "host        $(uname -srm)"
  echo "rustc       $(rustc --version 2>/dev/null || echo unknown)"
  echo "copy        rsync of the working tree without .git/target/.kvm/data/docs/evidence"
  echo
} | tee "$EVIDENCE_DIR/summary.txt"

overall=0
json_rows=""
add_row() { # name gate expected rc verdict description
  local row
  row="$(printf '{"case":"%s","gate":"%s","expected":"%s","exit_code":%s,"verdict":"%s","what":"%s"}' \
    "$1" "$2" "$3" "$4" "$5" "$(printf '%s' "$6" | sed 's/\\/\\\\/g; s/"/\\"/g')")"
  json_rows="${json_rows:+$json_rows,
}  $row"
}
line() { printf '%-34s %-24s %s\n' "$1" "$2" "$3" | tee -a "$EVIDENCE_DIR/summary.txt"; }

# --- baseline: every gate passes on the unmodified copy ------------------------------------
for gate in security golden openapi; do
  log="$EVIDENCE_DIR/cases/00-baseline-$gate.log"
  rc=0
  run_gate "$gate" "$log" || rc=$?
  if [ "$rc" -eq 0 ]; then
    line baseline "$gate" "PASS (exit 0, as expected)"
    add_row baseline "$gate" pass 0 ok "unmodified tree"
  else
    line baseline "$gate" "UNEXPECTED FAILURE (exit $rc) - see cases/00-baseline-$gate.log"
    add_row baseline "$gate" pass "$rc" BROKEN "unmodified tree"
    overall=1
  fi
done

# --- known-bad changes ----------------------------------------------------------------------
for idx in "${!C_NAME[@]}"; do
  name="${C_NAME[$idx]}"
  gate="${C_GATE[$idx]}"
  file="${C_FILE[$idx]}"
  what="${C_WHAT[$idx]}"
  if [ -n "$ONLY" ]; then
    case " $ONLY " in *" $name "*) ;; *) continue ;; esac
  fi
  log="$EVIDENCE_DIR/cases/$(printf '%02d' "$((idx + 1))")-$name.log"
  {
    echo "# case: $name"
    echo "# what: $what"
    echo "# gate: $gate"
  } >"$log"
  if [ "$file" != - ]; then
    cp "$COPY/$file" "$COPY/$file.orig"
    if ! mutate "$file" "${C_FROM[$idx]}" "${C_TO[$idx]}" 2>"$log.err"; then
      line "$name" "$gate" "MUTATION NOT APPLIED ($(cat "$log.err"))"
      rm -f "$log.err"
      mv "$COPY/$file.orig" "$COPY/$file"
      add_row "$name" "$gate" fail -1 NOT_APPLIED "$what"
      overall=1
      continue
    fi
    rm -f "$log.err"
    {
      echo "# known-bad change (applied to a temporary copy only):"
      (cd "$COPY" && diff -u "$file.orig" "$file") || true
    } >>"$log"
  fi
  echo "# ---- gate output ----" >>"$log"
  rc=0
  run_gate "$gate" "$log.out" || rc=$?
  cat "$log.out" >>"$log"
  rm -f "$log.out"
  echo "# gate exit code: $rc" >>"$log"
  # restore and bump the mtime so cargo rebuilds the restored source for the next case
  if [ "$file" != - ]; then mv "$COPY/$file.orig" "$COPY/$file" && touch "$COPY/$file"; fi
  # A compile error is not proof that the gate detects the regression: require the gate's own
  # failure signal (a failed / missing listed test, or a failed snapshot / golden test).
  if [ "$rc" -ne 0 ] && grep -Eq 'could not compile|BUILD_FAILED' "$log"; then
    line "$name" "$gate" "INCONCLUSIVE (build error, gate exit $rc) - fix the case"
    add_row "$name" "$gate" fail "$rc" BUILD_ERROR "$what"
    overall=1
  elif [ "$rc" -ne 0 ]; then
    line "$name" "$gate" "CAUGHT (gate exit $rc)"
    add_row "$name" "$gate" fail "$rc" caught "$what"
  else
    line "$name" "$gate" "NOT CAUGHT (gate exit 0) - see $(basename "$log")"
    add_row "$name" "$gate" fail 0 MISSED "$what"
    overall=1
  fi
done

result=fail
[ "$overall" -eq 0 ] && result=pass
printf '{\n "stamp": "%s",\n "commit": "%s",\n "host": "%s",\n "result": "%s",\n "cases": [\n%s\n ]\n}\n' \
  "$STAMP" "$commit" "$(uname -srm)" "$result" "$json_rows" >"$EVIDENCE_DIR/summary.json"

echo | tee -a "$EVIDENCE_DIR/summary.txt"
if [ "$overall" -eq 0 ]; then
  echo "prove-gates: PASS - baseline green and every known-bad change was caught by its gate" | tee -a "$EVIDENCE_DIR/summary.txt"
else
  echo "prove-gates: FAIL - see $EVIDENCE_DIR" | tee -a "$EVIDENCE_DIR/summary.txt"
fi
echo "evidence: ${EVIDENCE_DIR#"$REPO_ROOT"/}"
exit "$overall"

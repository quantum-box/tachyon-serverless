#!/usr/bin/env bash
# scripts/ci/selftest.sh - pin the behaviour of the CI helper scripts (PLT-4645).
#
#   - classify-changes.sh: docs-only changes skip heavy jobs; runtime / network / kernel /
#     billing paths require the KVM gate; mixed changes are never docs-only.
#   - kvm-gate.sh: "KVM required but did not run" is a failure, never a pass.
#   - security-regression.sh: a malformed list or an unknown category is rejected before any
#     cargo invocation.
#   - kvm-profile.sh: produces valid JSON with the commit.
#
# Needs bash, jq. No cargo build, no network. Exit 0 when every case passes.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/ci-selftest.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1" >&2; fails=$((fails + 1)); }

# classify <expected docs_only> <expected kvm_required> <name> <paths...>
classify() {
  local want_docs="$1" want_kvm="$2" name="$3" out
  shift 3
  out="$(printf '%s\n' "$@" | GITHUB_OUTPUT="" "$DIR/classify-changes.sh" --stdin)"
  if printf '%s\n' "$out" | grep -qx "docs_only=$want_docs" &&
    printf '%s\n' "$out" | grep -qx "kvm_required=$want_kvm"; then
    pass "classify: $name"
  else
    fail "classify: $name (want docs_only=$want_docs kvm_required=$want_kvm, got: $(printf '%s' "$out" | tr '\n' ' '))"
  fi
}

classify true false "docs only" docs/ci.md docs/evidence/x/summary.json README.md
classify true false "issue template" .github/ISSUE_TEMPLATE/bug_report.yml
classify false false "openapi snapshot is a contract, not docs" docs/openapi.json
classify false false "gateway code" apps/gateway/src/handlers.rs
classify false false "application lease code (unit / property gates only)" crates/application/src/services/invoke.rs
classify false false "ci workflow" .github/workflows/ci.yml
classify false true "firecracker provider" crates/providers/firecracker/src/egress_gate.rs
classify false true "firecracker network policy" crates/providers/firecracker/src/network.rs
classify false true "domain egress allowlist" crates/domain/src/egress.rs
classify false false "dispatcher lease (security group, not KVM)" crates/application/src/services/dispatcher.rs
classify false false "slot store (security group, not KVM)" crates/application/src/repository/slot.rs
classify false true "runtime bridge" crates/runtime-bridge/src/session.rs
classify false true "protocol" crates/protocol/src/wire.rs
classify false true "kvm scripts" scripts/kvm/smoke.sh
classify false true "firecracker runtime profile" config/gateway.firecracker.toml
classify false true "future billing crate" crates/billing/src/lib.rs
classify false true "usage metering" crates/application/src/services/usage.rs
classify false true "kvm workflow" .github/workflows/kvm-integration.yml
classify false true "mixed docs + firecracker" docs/kvm.md crates/providers/firecracker/src/vmm.rs
classify true false "markdown next to firecracker code" crates/providers/firecracker/README.md
classify false false "empty change set is not docs-only" ""

out="$(GITHUB_OUTPUT="" "$DIR/classify-changes.sh" --all)"
if printf '%s\n' "$out" | grep -qx "kvm_required=true" && printf '%s\n' "$out" | grep -qx "docs_only=false"; then
  pass "classify: unknown range requires everything"
else
  fail "classify: unknown range requires everything"
fi

# gate <expected exit> <name> <args...>
gate() {
  local want="$1" name="$2" rc=0
  shift 2
  GITHUB_STEP_SUMMARY="" "$DIR/kvm-gate.sh" "$@" >"$TMP/gate.out" 2>&1 || rc=$?
  if [ "$rc" -eq "$want" ]; then pass "kvm-gate: $name"; else fail "kvm-gate: $name (exit $rc, want $want): $(cat "$TMP/gate.out")"; fi
}
gate 0 "not required, job skipped" false skipped pull_request true false
gate 0 "required and passed" true success push true false
gate 1 "required, not labeled" true skipped pull_request true false
gate 1 "required, fork PR" true skipped pull_request false false
gate 1 "required, labeled but still skipped" true skipped pull_request true true
gate 1 "required, failed" true failure push true false
gate 1 "required, cancelled (e.g. queued without a runner until timeout)" true cancelled push true false
gate 1 "required, empty result" true "" workflow_dispatch true false
gate 2 "bad usage" true

GITHUB_STEP_SUMMARY="" "$DIR/kvm-gate.sh" true skipped pull_request true false >"$TMP/gate.out" 2>&1 || true
if grep -q "REQUIRED but was NOT RUN" "$TMP/gate.out"; then
  pass "kvm-gate: message says NOT RUN"
else
  fail "kvm-gate: message says NOT RUN"
fi

# security-regression.sh list validation (no cargo is invoked on these)
printf 'tenant_authz pkg lib\n' >"$TMP/bad-fields.list"
printf 'nonsense pkg lib a::b\n' >"$TMP/bad-cat.list"
printf 'tenant_authz pkg bin:x a::b\n' >"$TMP/bad-target.list"
for case_ in bad-fields bad-cat bad-target; do
  rc=0
  CARGO=false "$DIR/security-regression.sh" --list "$TMP/$case_.list" >/dev/null 2>&1 || rc=$?
  if [ "$rc" -eq 2 ]; then pass "security-regression: rejects $case_"; else fail "security-regression: rejects $case_ (exit $rc)"; fi
done
# a list missing a whole category fails (exit 1) before running cargo
printf 'tenant_authz pkg lib a::b\n' >"$TMP/one-cat.list"
rc=0
CARGO=false "$DIR/security-regression.sh" --list "$TMP/one-cat.list" >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 1 ]; then pass "security-regression: every category must have tests"; else fail "security-regression: every category must have tests (exit $rc)"; fi
# a test that does not appear in cargo's output is MISSING, i.e. a failure
cat >"$TMP/fake-cargo" <<'EOF'
#!/usr/bin/env bash
echo "running 1 test"
echo "test present::test ... ok"
echo "test result: ok. 1 passed; 0 failed"
EOF
chmod +x "$TMP/fake-cargo"
printf 'deadline pkg lib present::test\ndeadline pkg lib renamed::test\n' >"$TMP/missing.list"
rc=0
CARGO="$TMP/fake-cargo" "$DIR/security-regression.sh" --list "$TMP/missing.list" --category deadline >"$TMP/missing.out" 2>&1 || rc=$?
if [ "$rc" -eq 1 ] && grep -q "^MISSING .*renamed::test" "$TMP/missing.out"; then
  pass "security-regression: a renamed test is reported MISSING and fails"
else
  fail "security-regression: a renamed test is reported MISSING and fails (exit $rc)"
fi
printf 'deadline pkg lib present::test\n' >"$TMP/present.list"
rc=0
CARGO="$TMP/fake-cargo" "$DIR/security-regression.sh" --list "$TMP/present.list" --category deadline >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 0 ]; then pass "security-regression: a present passing test passes"; else fail "security-regression: a present passing test passes (exit $rc)"; fi

# kvm-profile.sh
if "$DIR/kvm-profile.sh" "$TMP/profile" >/dev/null 2>&1 && jq -e '.schema == "tachyon-serverless/kvm-profile/v1" and (.host.uname | length > 0)' "$TMP/profile/profile.json" >/dev/null; then
  pass "kvm-profile: valid JSON"
else
  fail "kvm-profile: valid JSON"
fi

echo
if [ "$fails" -ne 0 ]; then
  echo "ci selftest: $fails failure(s)" >&2
  exit 1
fi
echo "ci selftest: all cases passed"

#!/usr/bin/env bash
# scripts/chaos/matrix.sh - the PLT-4646 failure matrix: controller / DB / queue / object store /
# usage journal / worker failures on ONE host, with the process provider (default) or Firecracker
# microVMs (TSLS_PROVIDER=firecracker, as root; see scripts/chaos/lib.sh for what changes).
#
# Every scenario (scripts/chaos/scenarios.sh) runs in its own subshell with its own scratch
# data_dir, its own pinned nats-server (scripts/queue/up.sh on free ports) and its own gateway
# processes built with the test-only `failpoints` feature:
#   setup -> steady workload (sync invokes, async invokes with inline and object inputs, a cron
#   trigger, a signed webhook) -> fault injection -> recovery -> convergence checks that read the
#   ledger (state.db), the usage ledger, JetStream (tachyon-queue-probe), the file system and the
#   process table -> one JSON result (fault, injected_at, restored_at, recovered_at, outage_ms,
#   recovery_ms, checks[], observations, pass|fail).
#
# Output (default docs/evidence/chaos-<UTC>/):
#   results.jsonl      one line per scenario attempt (machine readable)
#   summary.md         scenario | fault | recovery_ms | result | attempts
#   profile.json       commit, OS, rustc, nats-server, config digests, seeds, options
#   scenarios/<id>/attempt-<n>/  checks.jsonl, observations.jsonl, result.json, gateway logs,
#                      redacted gateway configuration, executions.log, accepted.txt
#
# A scenario that fails is re-run up to --retries times (default 2). Every attempt is kept in
# results.jsonl and counted in summary.md: a scenario that passed only on a retry is FLAKY, never
# reported as a plain pass.
#
# Single host only: nothing here is evidence of multi-host HA, disk corruption, power loss or
# network partitions between hosts (docs/failure-matrix.md).
#
# Usage:
#   scripts/chaos/matrix.sh [--only ID[,ID...]] [--retries N] [--evidence DIR] [--list]
#
# Environment: TSLS_SKIP_BUILD=1 skips cargo build; CHAOS_KEEP_WORK=1 keeps scratch directories;
# CHAOS_TMP overrides the scratch root (default $TMPDIR); TSLS_PROVIDER=process|firecracker.
# Exit 0 only when every scenario passed on its first attempt or on a retry (flaky ones are
# reported as such).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/chaos/lib.sh
. "$SCRIPT_DIR/lib.sh"
# shellcheck source=scripts/chaos/scenarios.sh
. "$SCRIPT_DIR/scenarios.sh"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ONLY=""
RETRIES=2
RUN_DIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --retries) RETRIES="$2"; shift 2 ;;
    --evidence) RUN_DIR="$2"; shift 2 ;;
    --list)
      for s in $SCENARIOS; do printf '%-28s %s\n' "$s" "$(scenario_fault "$s")"; done
      exit 0
      ;;
    -h | --help) sed -n '2,33p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
RUN_DIR="${RUN_DIR:-$REPO_ROOT/docs/evidence/chaos-$STAMP}"
mkdir -p "$RUN_DIR/scenarios"
RUN_DIR="$(cd "$RUN_DIR" && pwd)"
export RUN_DIR

for t in curl jq python3 perl openssl cargo; do
  command -v "$t" >/dev/null 2>&1 || { echo "missing tool: $t" >&2; exit 1; }
done

# The process provider needs no privileges, and root would make `object_store_unavailable` pass
# for the wrong reason: root ignores the `chmod 000` of the object root, so the store stays
# readable and the scenario's refusals never happen. Firecracker is the case that needs root
# (jailer + cgroup).
if ! provider_is_fc && [ "$(id -u)" = 0 ] && [ "${CHAOS_ALLOW_ROOT:-0}" != 1 ]; then
  echo "refusing to run the process provider as root: chmod-based faults cannot fail closed" >&2
  echo "run it as an ordinary user, or set CHAOS_ALLOW_ROOT=1 to override" >&2
  exit 2
fi

cd "$REPO_ROOT"
if [ "${TSLS_SKIP_BUILD:-0}" != 1 ]; then
  chaos_log "building (gateway with the test-only failpoints feature)"
  cargo build -q -p tachyon-serverless-gateway --features failpoints
  cargo build -q -p tachyon-serverless-queue-nats --bin tachyon-queue-probe
  cargo build -q -p tachyon-serverless-cli -p tachyon-serverless-runtime-bridge \
    -p example-hello -p example-idempotent-async -p example-cpu-burn
  if provider_is_fc; then
    cargo build -q --release --target "$(uname -m)-unknown-linux-musl" \
      -p example-hello -p example-idempotent-async -p example-cpu-burn -p example-isolation-probe
  fi
fi
for b in "$GATEWAY_BIN" "$TSLS_BIN" "$PROBE_BIN" "$BRIDGE_BIN" "$HELLO_BIN" "$ASYNC_BIN" "$BURN_BIN"; do
  [ -x "$b" ] || { echo "missing binary: $b" >&2; exit 1; }
done

selected=""
for s in $SCENARIOS; do
  if [ -z "$ONLY" ] || printf ',%s,' "$ONLY" | grep -q ",$s,"; then selected="$selected $s"; fi
done
[ -n "$selected" ] || { echo "no scenario matches --only $ONLY" >&2; exit 2; }

# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------

# shellcheck source=deploy/nats/versions.env
. "$REPO_ROOT/deploy/nats/versions.env"
nats_bin="$(QUEUE_BIN_DIR="$REPO_ROOT/target/queue/bin" bash -c '. scripts/queue/lib.sh; queue_binary' 2>/dev/null || true)"
digest_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@" | awk '{print $1}'; else shasum -a 256 "$@" | awk '{print $1}'; fi; }
jq -n \
  --arg stamp "$STAMP" \
  --arg commit "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "${TSLS_COMMIT:-unknown}")" \
  --arg dirty "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null | wc -l | tr -d ' ')" \
  --arg os "$(uname -srm)" \
  --arg os_version "$(sw_vers -productVersion 2>/dev/null || sed -n 's/^PRETTY_NAME=//p' /etc/os-release 2>/dev/null | tr -d '"')" \
  --arg cpus "$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo unknown)" \
  --arg rustc "$(rustc --version 2>/dev/null || echo unknown)" \
  --arg nats "$NATS_SERVER_VERSION ($("${nats_bin:-false}" --version 2>/dev/null || echo 'not downloaded yet'))" \
  --arg bash "$BASH_VERSION" \
  --arg sqlite "$(python3 -c 'import sqlite3; print(sqlite3.sqlite_version)')" \
  --arg gateway_sha256 "$(digest_of "$GATEWAY_BIN")" \
  --arg lib_sha256 "$(digest_of "$SCRIPT_DIR/lib.sh")" \
  --arg scenarios_sha256 "$(digest_of "$SCRIPT_DIR/scenarios.sh")" \
  --arg selected "${selected# }" --argjson retries "$RETRIES" \
  --arg provider "$(if provider_is_fc; then echo "firecracker (jailed microVMs, host cgroup required, warm pool ${CH_POOL:-true})"; else echo "process (dev-only, no isolation)"; fi)" \
  --arg seed "${CHAOS_SEED:-none: scenarios use fixed timings; secrets and keys are random per scenario}" \
  '{stamp: $stamp, commit: $commit, uncommitted_files: ($dirty | tonumber), os: $os, os_version: $os_version,
    cpus: $cpus, rustc: $rustc, nats_server: $nats, bash: $bash, python_sqlite: $sqlite,
    provider: $provider, build: "debug gateway with --features failpoints",
    gateway_sha256: $gateway_sha256, harness_sha256: {lib: $lib_sha256, scenarios: $scenarios_sha256},
    config_digest_note: "each attempt stores its gateway configuration (secret redacted) next to its result; results carry their sha256",
    scenarios: ($selected | split(" ")), retries: $retries, seeds: $seed,
    scope: "single host; not evidence of multi-host HA"}' >"$RUN_DIR/profile.json"

# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

: >"$RUN_DIR/results.jsonl"
overall=0
for s in $selected; do
  attempt=1
  while :; do
    chaos_log "=== $s attempt $attempt ==="
    set +e
    (
      set -euo pipefail
      export ATTEMPT="$attempt"
      "scenario_$s"
      SC_COMPLETE=1
    ) 2>&1 | tee "$RUN_DIR/scenarios/$s.attempt-$attempt.log" >&2
    set -e
    result_file="$RUN_DIR/scenarios/$s/attempt-$attempt/result.json"
    if [ ! -s "$result_file" ]; then
      mkdir -p "$(dirname "$result_file")"
      jq -nc --arg s "$s" --arg f "$(scenario_fault "$s")" --argjson a "$attempt" \
        '{scenario: $s, fault: $f, attempt: $a, result: "fail", checks: [{name: "harness.result_written", ok: false, detail: "the scenario aborted before writing its result"}]}' >"$result_file"
    fi
    # Config digests of this attempt.
    cfg_digests="$(for c in "$RUN_DIR/scenarios/$s/attempt-$attempt"/gw-*.toml; do if [ -f "$c" ]; then printf '%s=%s\n' "$(basename "$c")" "$(digest_of "$c")"; fi; done | jq -Rn '[inputs | split("=") | {(.[0]): .[1]}] | add // {}')"
    jq -c --argjson d "$cfg_digests" '. + {config_sha256: $d}' "$result_file" >>"$RUN_DIR/results.jsonl"
    r="$(jq -r .result "$result_file")"
    if [ "$r" = pass ] || [ "$attempt" -gt "$RETRIES" ]; then break; fi
    attempt=$((attempt + 1))
  done
  [ "$r" = pass ] || overall=1
done

# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------

{
  echo "# PLT-4646 failure matrix — $STAMP"
  echo
  echo "commit \`$(jq -r .commit "$RUN_DIR/profile.json")\`, $(jq -r .os "$RUN_DIR/profile.json"), $(jq -r .rustc "$RUN_DIR/profile.json"), nats-server $(jq -r .nats_server "$RUN_DIR/profile.json"), provider $(jq -r .provider "$RUN_DIR/profile.json"), debug gateway with failpoints."
  echo
  echo "Single host. These results are not a multi-host HA guarantee."
  echo
  echo "| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |"
  echo "|---|---|---|---|---|---|---|"
  for s in $selected; do
    jq -rs --arg s "$s" '
      [.[] | select(.scenario == $s)] as $a
      | ($a | last) as $l
      | ($a | length) as $n
      | (if $l.result != "pass" then "FAIL" elif $n > 1 then "FLAKY (pass on attempt \($n))" else "pass" end) as $res
      | "| \($s) | \($l.fault) | \($l.outage_ms // "-") | \($l.recovery_ms // "-") | \($res) | \([$a[] | .result] | join(",")) | \([$l.checks[]? | select(.ok | not) | .name] | join(", ")) |"' \
      "$RUN_DIR/results.jsonl"
  done
} >"$RUN_DIR/summary.md"
cat "$RUN_DIR/summary.md" >&2
chaos_log "evidence: $RUN_DIR"
exit "$overall"

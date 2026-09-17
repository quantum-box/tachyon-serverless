#!/usr/bin/env bash
# scripts/db/tidb-verify.sh - run the TiDB half of PLT-4618 against a real TiDB (docs/adr/0003).
#
# Starts a single-node TiDB cluster (pd-server, tikv-server, tidb-server of a pinned version, the
# binaries tiup distributes) with every listener on 127.0.0.1, then runs with TSLS_TIDB_URL
# pointing at it:
#   - repository::tidb::migrations      the TiDB migrations mirror the SQLite ones, additive only
#   - repository::tidb::tests           migrations on an empty / older database, a failing
#                                       migration, a newer schema, concurrent migrators, races
#                                       across separate connection pools, the index review
#                                       (EXPLAIN ANALYZE on a seeded dataset)
#   - repository::contract_tests::tidb  the repository contract suite on TiDB
#   - repository::object_contract_tests::tidb
# and writes to the evidence directory: versions.txt, listeners.txt (every listening socket of
# the cluster processes; the script fails if one is not on loopback), tests.log, results.tsv,
# explain-tidb.md, explain-sqlite.md, summary.txt and processes-after-stop.txt. The cluster is
# stopped and its data directory removed on exit, and the script fails if a process survives.
#
# Why not `tiup playground`: v1.17.1 binds its own command server to *:9527 and tidb-server's
# status port to *:10080 with no flag to change the former, so the cluster would be reachable
# beyond loopback. The components are started directly instead, with the same binaries.
#
# It lives in scripts/db/ (not scripts/e2e/) because it touches no execution provider.
#
# Usage:
#   scripts/db/tidb-verify.sh [--evidence DIR] [--version vX.Y.Z]
#
# Binaries: TSLS_TIDB_BINDIR (a directory with pd-server, tikv-server, tidb-server), else
# ~/.tiup/components/{pd,tikv,tidb}/<version>/ from `tiup install pd:<v> tikv:<v> tidb:<v>`
# (curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh).
# Needs cargo, lsof, python3. Uses the mysql client for versions and readiness when installed.
# Environment: TSLS_TIDB_VERSION (default v8.5.8), TSLS_TIDB_TEST_THREADS (default 4).
# Exit 0 only when every test passed and nothing of the cluster is left running.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
VERSION="${TSLS_TIDB_VERSION:-v8.5.8}"
THREADS="${TSLS_TIDB_TEST_THREADS:-4}"
EVIDENCE=""
# The number of TiDB tests the suite has; fewer results means something did not run.
MIN_TESTS=45

while [ $# -gt 0 ]; do
  case "$1" in
    --evidence) EVIDENCE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h | --help) sed -n '2,35p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

EVIDENCE="${EVIDENCE:-$REPO_ROOT/docs/evidence/tidb-$STAMP}"
mkdir -p "$EVIDENCE"
EVIDENCE="$(cd "$EVIDENCE" && pwd)"

log() { printf '[tidb-verify %s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

command -v lsof >/dev/null || { echo "lsof not found" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 2; }

binary() {
  local component="$1" name="$2"
  if [ -n "${TSLS_TIDB_BINDIR:-}" ]; then
    echo "$TSLS_TIDB_BINDIR/$name"
  else
    echo "$HOME/.tiup/components/$component/$VERSION/$name"
  fi
}
PD_BIN="$(binary pd pd-server)"
KV_BIN="$(binary tikv tikv-server)"
DB_BIN="$(binary tidb tidb-server)"
for bin in "$PD_BIN" "$KV_BIN" "$DB_BIN"; do
  [ -x "$bin" ] || {
    echo "$bin is not executable: set TSLS_TIDB_BINDIR or run tiup install pd:$VERSION tikv:$VERSION tidb:$VERSION" >&2
    exit 2
  }
done

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
PD_CLIENT="$(free_port)"
PD_PEER="$(free_port)"
KV_PORT="$(free_port)"
KV_STATUS="$(free_port)"
DB_PORT="$(free_port)"
DB_STATUS="$(free_port)"
URL="mysql://root@127.0.0.1:$DB_PORT"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/tsls-tidb.XXXXXX")"
PIDS=()

cluster_pids() { pgrep -f "$WORK" || true; }

cleanup() {
  local status=$?
  local i pid
  # Stop in reverse start order: tidb, tikv, pd.
  for ((i = ${#PIDS[@]} - 1; i >= 0; i--)); do
    pid="${PIDS[$i]}"
    kill -TERM "$pid" 2>/dev/null || continue
    for _ in $(seq 1 60); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -KILL "$pid" 2>/dev/null || true
  done
  local left
  left="$(cluster_pids)"
  if [ -n "$left" ]; then
    log "killing leftover cluster processes: $left"
    # shellcheck disable=SC2086
    kill -KILL $left 2>/dev/null || true
    sleep 2
  fi
  for f in pd tikv tidb; do
    if [ -f "$WORK/$f.log" ]; then tail -n 200 "$WORK/$f.log" >"$EVIDENCE/$f.log.tail" 2>/dev/null || true; fi
  done
  {
    echo "checked_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "started pids: ${PIDS[*]:-}"
    echo "started pids still alive:"
    for pid in ${PIDS[@]+"${PIDS[@]}"}; do
      if kill -0 "$pid" 2>/dev/null; then ps -o pid=,command= -p "$pid"; fi
    done
    echo "processes whose command line names the work directory:"
    pgrep -fl "$WORK" || echo "(none)"
    echo "pd-server / tikv-server / tidb-server processes on this host:"
    pgrep -fl 'pd-server|tikv-server|tidb-server' || echo "(none)"
    echo "listeners on the cluster ports:"
    for port in "$PD_CLIENT" "$PD_PEER" "$KV_PORT" "$KV_STATUS" "$DB_PORT" "$DB_STATUS"; do
      lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
    done
    echo "(end)"
  } >"$EVIDENCE/processes-after-stop.txt"
  if [ -n "$(cluster_pids)" ]; then
    log "FAIL: cluster processes survived: $(cluster_pids)"
    status=1
  else
    rm -rf "$WORK"
  fi
  exit "$status"
}
trap cleanup EXIT

ulimit -n 65536 2>/dev/null || ulimit -n 10240 2>/dev/null || true

log "starting pd-server $VERSION on 127.0.0.1:$PD_CLIENT"
"$PD_BIN" --name=pd --data-dir="$WORK/pd" \
  --client-urls="http://127.0.0.1:$PD_CLIENT" --peer-urls="http://127.0.0.1:$PD_PEER" \
  --log-file="$WORK/pd.log" >"$WORK/pd.out" 2>&1 &
PIDS+=("$!")

# TiKV refuses to start when RLIMIT_NOFILE is below its rocksdb max-open-files; a small
# single-node test cluster does not need the production default.
cat >"$WORK/tikv.toml" <<'TOML'
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[storage]
reserve-space = "0MB"
TOML

log "starting tikv-server on 127.0.0.1:$KV_PORT"
"$KV_BIN" --config="$WORK/tikv.toml" --pd-endpoints="127.0.0.1:$PD_CLIENT" --addr="127.0.0.1:$KV_PORT" \
  --status-addr="127.0.0.1:$KV_STATUS" --data-dir="$WORK/tikv" \
  --log-file="$WORK/tikv.log" >"$WORK/tikv.out" 2>&1 &
PIDS+=("$!")

log "starting tidb-server on 127.0.0.1:$DB_PORT"
"$DB_BIN" --store=tikv --path="127.0.0.1:$PD_CLIENT" -host=127.0.0.1 -P "$DB_PORT" \
  -status "$DB_STATUS" -status-host=127.0.0.1 --log-file="$WORK/tidb.log" \
  >"$WORK/tidb.out" 2>&1 &
PIDS+=("$!")

ready=0
for _ in $(seq 1 300); do
  for pid in "${PIDS[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      log "a cluster process exited early (pid $pid)"
      cat "$WORK"/*.out >&2 || true
      exit 1
    fi
  done
  if ! lsof -nP -iTCP:"$DB_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    sleep 1
    continue
  fi
  if ! command -v mysql >/dev/null || mysql -h 127.0.0.1 -P "$DB_PORT" -u root -e 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[ "$ready" = 1 ] || { log "TiDB did not come up within 300s"; exit 1; }
log "TiDB is accepting connections on 127.0.0.1:$DB_PORT"

# Every listening socket of the cluster processes must be on loopback.
pids_csv="$(IFS=,; echo "${PIDS[*]}")"
lsof -nP -a -p "$pids_csv" -iTCP -sTCP:LISTEN >"$EVIDENCE/listeners.txt" 2>/dev/null || true
if awk 'NR > 1 { print $9 }' "$EVIDENCE/listeners.txt" | grep -vE '^(127\.0\.0\.1|\[::1\]):' | grep -q .; then
  log "FAIL: a cluster process listens beyond loopback (listeners.txt)"
  exit 1
fi

{
  echo "utc: $STAMP"
  echo "commit: $(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "host: $(uname -srm)"
  echo "version requested: $VERSION"
  for bin in "$PD_BIN" "$KV_BIN" "$DB_BIN"; do
    echo "--- $(basename "$bin") -V"
    "$bin" -V 2>&1 | sed 's/^/  /'
    echo "  sha256 $(shasum -a 256 "$bin" | cut -d' ' -f1)"
  done
  echo "rustc: $(rustc --version)"
  echo "mysql crate: $(grep -A1 '^name = "mysql"$' "$REPO_ROOT/Cargo.lock" | sed -n 's/^version = //p' | tr -d '"')"
  if command -v mysql >/dev/null; then
    echo "--- SELECT tidb_version()"
    mysql -h 127.0.0.1 -P "$DB_PORT" -u root -N -e 'SELECT tidb_version()\G' 2>&1 | sed 's/^/  /'
    echo "--- server defaults"
    mysql -h 127.0.0.1 -P "$DB_PORT" -u root -e "SHOW GLOBAL VARIABLES WHERE Variable_name IN \
      ('tidb_txn_mode','transaction_isolation','tidb_enable_clustered_index', \
       'tidb_constraint_check_in_place_pessimistic','innodb_lock_wait_timeout', \
       'tidb_enable_fast_create_table','txn_entry_size_limit','max_allowed_packet')" 2>&1 | sed 's/^/  /'
  fi
} >"$EVIDENCE/versions.txt"

log "running the TiDB tests (threads $THREADS)"
set +e
(
  cd "$REPO_ROOT"
  TSLS_TIDB_URL="$URL" TSLS_TIDB_EVIDENCE_DIR="$EVIDENCE" \
    cargo test -p tachyon-serverless-application --lib -- \
    repository::tidb:: repository::contract_tests::tidb:: repository::object_contract_tests::tidb:: \
    --test-threads "$THREADS" 2>&1
) | tee "$EVIDENCE/tests.log"
test_status=${PIPESTATUS[0]}
set -e

awk '$1 == "test" && $2 ~ /^repository::/ && $3 == "..." { print $2 "\t" $4 }' "$EVIDENCE/tests.log" \
  | sort >"$EVIDENCE/results.tsv"
passed="$(awk -F'\t' '$2 == "ok"' "$EVIDENCE/results.tsv" | wc -l | tr -d ' ')"
failed="$(awk -F'\t' '$2 != "ok"' "$EVIDENCE/results.tsv" | wc -l | tr -d ' ')"
{
  echo "tidb_url=$URL"
  echo "tidb_version_requested=$VERSION"
  echo "tests_passed=$passed"
  echo "tests_not_ok=$failed"
  echo "cargo_test_exit=$test_status"
} >"$EVIDENCE/summary.txt"
cat "$EVIDENCE/summary.txt"

if [ "$test_status" != 0 ] || [ "$failed" != 0 ] || [ "$passed" -lt "$MIN_TESTS" ]; then
  log "FAIL: see $EVIDENCE/tests.log"
  exit 1
fi
log "PASS: evidence in $EVIDENCE"

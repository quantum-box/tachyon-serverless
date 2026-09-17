#!/usr/bin/env bash
# scripts/chaos/lib.sh - helpers for the PLT-4646 failure matrix. Sourced by matrix.sh and
# scenarios.sh, never executed.
#
# One scenario = one subshell with its own scratch directory (data_dir, process-provider workdir,
# object root, usage journal), its own nats-server (scripts/queue/up.sh on free ports) and its own
# gateway processes. Every check reads ground truth itself: state.db / usage ledger through
# python3's sqlite3, JetStream through tachyon-queue-probe, the file system and the process table.
#
# Result variables and helpers are used by the sourcing scripts (SC2034), and many functions are
# only called indirectly (SC2317).
# shellcheck disable=SC2034,SC2317

CHAOS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$CHAOS_LIB_DIR/../.." && pwd)"
# TSLS_PROVIDER=process (default) | firecracker (scripts/kvm/provider-lib.sh). With firecracker the
# whole matrix runs as root (jailer + host cgroup required), CHAOS_TMP must be on the file system of
# the jailer's chroot_base (/srv/jailer; drives are hard-linked into the jail), the warm pool is on
# unless CH_POOL=false, and idempotent-async keeps its records in the external store of
# scripts/queue/effects-netns.sh (egress restricted).
# shellcheck source=scripts/kvm/provider-lib.sh
. "$REPO_ROOT/scripts/kvm/provider-lib.sh"
provider_init "$REPO_ROOT" || exit 2

GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
TSLS_BIN="$REPO_ROOT/target/debug/tsls"
PROBE_BIN="$REPO_ROOT/target/debug/tachyon-queue-probe"
BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
HELLO_BIN="$GUEST_DIR/example-hello"
ASYNC_BIN="$GUEST_DIR/example-idempotent-async"
BURN_BIN="$GUEST_DIR/example-cpu-burn"
ISOLATION_PROBE_BIN="$GUEST_DIR/example-isolation-probe"

TENANT="tn_01hzzzzzzzzzzzzzzzzzzzzzza"
TOKEN="chaos-token-a"
SECRET_REF="chaos-secret"
ACK_WAIT=4
case "$(uname -m)" in arm64 | aarch64) ARCH=aarch64 ;; *) ARCH=x86_64 ;; esac

chaos_log() { printf '[chaos %s] %s\n' "${SC_ID:-matrix}" "$*" >&2; }

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }
iso_of_ms() { # iso_of_ms MS -> RFC 3339 UTC with milliseconds ("" for empty)
  [ -n "${1:-}" ] || { printf ''; return 0; }
  perl -MPOSIX=strftime -e 'my $ms = shift; printf "%s.%03dZ", strftime("%Y-%m-%dT%H:%M:%S", gmtime(int($ms / 1000))), $ms % 1000' "$1"
}
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
rand_hex() { od -An -N"${1:-16}" -tx1 /dev/urandom | tr -d ' \n'; }

# ---------------------------------------------------------------------------
# scenario context and machine-readable result
# ---------------------------------------------------------------------------

# sc_begin ID FAULT: creates SC_DIR (evidence) and WORK (scratch), starts the clock.
sc_begin() {
  SC_ID="$1"
  SC_FAULT="$2"
  SC_DIR="$RUN_DIR/scenarios/$SC_ID/attempt-${ATTEMPT:-1}"
  rm -rf "$SC_DIR"
  mkdir -p "$SC_DIR"
  WORK="$(mktemp -d "${CHAOS_TMP:-${TMPDIR:-/tmp}}/tsls-chaos.XXXXXX")"
  WORK="$(cd "$WORK" && pwd -P)"
  mkdir -p "$WORK/pids" "$WORK/data" "$WORK/effects"
  SC_CHECKS="$SC_DIR/checks.jsonl"
  SC_OBS="$SC_DIR/observations.jsonl"
  : >"$SC_CHECKS"
  : >"$SC_OBS"
  : >"$WORK/accepted.txt"
  : >"$WORK/expect-failed.txt"
  SC_T0="$(now_ms)"
  SC_INJECTED=""
  SC_RESTORED=""
  SC_RECOVERED=""
  SECRET_VALUE="chaos-s3cr3t-$(rand_hex 12)"
  NATS_ENABLED=0
  trap sc_cleanup EXIT
  chaos_log "begin ($SC_FAULT) work=$WORK"
}

mark_injected() { SC_INJECTED="$(now_ms)"; chaos_log "fault injected"; }
mark_restored() { SC_RESTORED="$(now_ms)"; chaos_log "fault removed"; }
mark_recovered() { SC_RECOVERED="$(now_ms)"; chaos_log "converged"; }

# ck NAME RC DETAIL: record one check (RC 0 = pass).
ck() {
  local ok=false
  [ "$2" = 0 ] && ok=true
  jq -nc --arg name "$1" --argjson ok "$ok" --arg detail "${3:-}" \
    '{name: $name, ok: $ok, detail: $detail}' >>"$SC_CHECKS"
  if [ "$ok" = true ]; then chaos_log "  ok   $1 $3"; else chaos_log "  FAIL $1 $3"; fi
}
# ckc NAME DETAIL CMD...: run CMD, record its status.
ckc() {
  local name="$1" detail="$2"
  shift 2
  if "$@"; then ck "$name" 0 "$detail"; else ck "$name" 1 "$detail"; fi
}
# obs KEY JSON_VALUE: a measured fact (counts before/after, codes).
obs() { jq -nc --arg k "$1" --argjson v "$2" '{key: $k, value: $v}' >>"$SC_OBS"; }
obs_s() { jq -nc --arg k "$1" --arg v "$2" '{key: $k, value: $v}' >>"$SC_OBS"; }
rc_of() { if "$@"; then echo 0; else echo 1; fi; }

sc_write_result() {
  local end recovery outage result
  end="$(now_ms)"
  if [ "${SC_COMPLETE:-0}" != 1 ]; then
    ck harness.scenario_completed 1 "the scenario stopped before its last step (see the attempt log)"
  fi
  recovery=null
  outage=null
  if [ -n "$SC_RECOVERED" ] && [ -n "$SC_RESTORED" ]; then recovery=$((SC_RECOVERED - SC_RESTORED)); fi
  if [ -n "$SC_RESTORED" ] && [ -n "$SC_INJECTED" ]; then outage=$((SC_RESTORED - SC_INJECTED)); fi
  result=pass
  if [ ! -s "$SC_CHECKS" ] || grep -q '"ok":false' "$SC_CHECKS"; then result=fail; fi
  if [ -z "$SC_RECOVERED" ] && [ -n "$SC_INJECTED" ]; then result=fail; fi
  jq -nc \
    --arg scenario "$SC_ID" --arg fault "$SC_FAULT" --argjson attempt "${ATTEMPT:-1}" \
    --arg injected_at "$(iso_of_ms "$SC_INJECTED")" --arg restored_at "$(iso_of_ms "$SC_RESTORED")" \
    --arg recovered_at "$(iso_of_ms "$SC_RECOVERED")" \
    --argjson recovery_ms "$recovery" --argjson outage_ms "$outage" \
    --argjson duration_ms $((end - SC_T0)) --arg result "$result" \
    --slurpfile checks "$SC_CHECKS" --slurpfile observations "$SC_OBS" \
    '{scenario: $scenario, fault: $fault, attempt: $attempt,
      injected_at: (if $injected_at == "" then null else $injected_at end),
      restored_at: (if $restored_at == "" then null else $restored_at end),
      recovered_at: (if $recovered_at == "" then null else $recovered_at end),
      outage_ms: $outage_ms, recovery_ms: $recovery_ms, duration_ms: $duration_ms,
      checks: $checks, observations: ($observations | map({(.key): .value}) | add // {}),
      result: $result}' >"$SC_DIR/result.json"
}

sc_cleanup() {
  set +e
  local f pid
  # Nothing may stay stopped or restricted.
  [ -d "$WORK/data/objects" ] && chmod -R u+rwX "$WORK/data/objects" 2>/dev/null
  for f in "$WORK"/pids/*; do
    [ -f "$f" ] || continue
    pid="$(cat "$f")"
    kill -CONT "$pid" 2>/dev/null
    kill -TERM "$pid" 2>/dev/null
  done
  for f in "$WORK"/pids/*; do
    [ -f "$f" ] || continue
    pid="$(cat "$f")"
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
    kill -KILL "$pid" 2>/dev/null
  done
  if [ "$NATS_ENABLED" = 1 ]; then
    [ -n "${QUEUE_STATE_DIR:-}" ] && [ -f "$QUEUE_STATE_DIR/nats-server.pid" ] && kill -CONT "$(cat "$QUEUE_STATE_DIR/nats-server.pid")" 2>/dev/null
    "$REPO_ROOT/scripts/queue/down.sh" >/dev/null 2>&1
    cp "$QUEUE_STATE_DIR/nats-server.log" "$SC_DIR/nats-server.log" 2>/dev/null
  fi
  if provider_is_fc; then
    cp "${EFFECTS_STORE_LOG:-/nonexistent}" "$SC_DIR/effects-store.log" 2>/dev/null
    "$REPO_ROOT/scripts/queue/effects-netns.sh" down >/dev/null 2>&1
    # What the scenario left on the host after its gateways stopped (evidence; checked in
    # cv_clean_after_stop while the scenario still runs).
    provider_leftovers >"$SC_DIR/host-leftovers-at-cleanup.txt" 2>&1
  fi
  # Leftovers of this scenario only (argv names its scratch directory).
  pkill -KILL -f -- "$WORK/" 2>/dev/null
  cp "$WORK"/gateway-*.log "$SC_DIR/" 2>/dev/null
  cp "$WORK"/gw-*.toml "$SC_DIR/" 2>/dev/null
  # The configuration holds the scenario's secret value: redact it in the evidence.
  for f in "$SC_DIR"/gw-*.toml; do
    [ -f "$f" ] && sed -i.bak "s/$SECRET_VALUE/<redacted>/g" "$f" && rm -f "$f.bak"
  done
  cp "$WORK/effects/executions.log" "$SC_DIR/executions.log" 2>/dev/null
  cp "$WORK/accepted.txt" "$SC_DIR/accepted.txt" 2>/dev/null
  sc_write_result
  if [ "${CHAOS_KEEP_WORK:-0}" = 1 ]; then
    chaos_log "kept $WORK"
  else
    rm -rf "$WORK"
  fi
}

# ---------------------------------------------------------------------------
# NATS
# ---------------------------------------------------------------------------

nats_up() {
  export QUEUE_STATE_DIR="$WORK/nats"
  if [ -z "${QUEUE_PORT:-}" ] || [ "${NATS_ENABLED:-0}" = 0 ]; then
    QUEUE_PORT="$(free_port)"
    QUEUE_HTTP_PORT="$(free_port)"
    export QUEUE_PORT QUEUE_HTTP_PORT
  fi
  NATS_ENABLED=1
  eval "$("$REPO_ROOT/scripts/queue/up.sh" 2>>"$WORK/nats-up.log")"
}
nats_pid() { cat "$QUEUE_STATE_DIR/nats-server.pid" 2>/dev/null; }
probe() {
  "$PROBE_BIN" --url "$TACHYON_NATS_URL" --password-file "$TACHYON_NATS_PASSWORD_FILE" \
    --ack-wait-ms $((ACK_WAIT * 1000)) --max-deliver 1000 "$@"
}
# stream_stat KEY -> value from `tachyon-queue-probe stats` (empty when the broker is down)
stream_stat() { probe stats 2>/dev/null | sed -n "s/^$1=//p"; }

# ---------------------------------------------------------------------------
# gateway configuration and processes
# ---------------------------------------------------------------------------

# Knobs (environment of the scenario function): CH_LEASE_TTL, CH_HEARTBEAT, CH_MAX_PENDING,
# CH_JOURNAL_MAX_EVENTS, CH_JOURNAL_HEADROOM, CH_COLLECT_MS, CH_MAX_ATTEMPTS, CH_POOL,
# CH_CAPACITY, CH_MAX_QUEUE, CH_QUEUE (nats|sqlite), CH_ASYNC_CLAIM_TTL, CH_TRIGGERS (1|0),
# CH_COLLECT_BATCH. CH_POOL=true also sets allow_unverified_idle (measurement-only, dev profile).
gw_config() { # gw_config NAME PORT INSTANCE
  local name="$1" port="$2" instance="$3" file="$WORK/gw-$1.toml"
  [ -f "$WORK/objects.key" ] || { (umask 077; rand_hex 32 >"$WORK/objects.key"); }
  [ -f "$WORK/triggers.key" ] || { (umask 077; rand_hex 32 >"$WORK/triggers.key"); }
  {
    cat <<EOF
listen = "127.0.0.1:$port"
profile = "dev"
data_dir = "$WORK/data"

$(provider_toml "$WORK/data" "$BRIDGE_BIN")

[capacity]
max_concurrency = ${CH_CAPACITY:-8}
max_queue = ${CH_MAX_QUEUE:-32}
queue_timeout_seconds = $(provider_is_fc && echo 90 || echo 30)

[pool]
enabled = ${CH_POOL:-$(provider_is_fc && echo true || echo false)}
allow_unverified_idle = ${CH_POOL:-false}
idle_ttl_seconds = 300

[limits]
max_response_bytes = 65536

[invoke]
inline_output_max_bytes = 16384

[dispatcher]
instance = "$instance"
lease_ttl_seconds = ${CH_LEASE_TTL:-6}
heartbeat_interval_seconds = ${CH_HEARTBEAT:-1}
max_clock_skew_ms = 500

[[identity.tokens]]
token = "$TOKEN"
tenant_id = "$TENANT"
subject = "chaos"
roles = ["deploy", "invoke", "redrive"]

[[secrets.bindings]]
tenant_id = "$TENANT"
binding_ref = "$SECRET_REF"
value = "$SECRET_VALUE"

[objects]
backend = "filesystem"
key_file = "$WORK/objects.key"
orphan_grace_seconds = ${CH_ORPHAN_GRACE:-6}
gc_interval_seconds = 2

[invoke_async]
inline_input_max_bytes = 1024
max_pending_events = ${CH_MAX_PENDING:-1000}
claim_ttl_seconds = 3
retry_initial_ms = 200
retry_max_ms = 1000
publish_interval_ms = 100

[async_dispatch]
workers = 2
ack_wait_seconds = $ACK_WAIT
max_deliver = 1000
fetch_wait_ms = 200
claim_ttl_seconds = ${CH_ASYNC_CLAIM_TTL:-4}
admission_wait_ms = 2000
max_attempts = ${CH_MAX_ATTEMPTS:-3}
backoff_initial_ms = 300
backoff_max_ms = 1000
backoff_floor_ms = 100
retry_budget = 0
reaper_interval_seconds = 1
stall_timeout_seconds = 60

[usage]
collect_interval_ms = ${CH_COLLECT_MS:-300}
collect_batch = ${CH_COLLECT_BATCH:-500}
journal_max_events = ${CH_JOURNAL_MAX_EVENTS:-100000}
admission_headroom_events = ${CH_JOURNAL_HEADROOM:-1000}
EOF
    if [ "${CH_TRIGGERS:-1}" = 1 ]; then
      cat <<EOF

[triggers]
scheduler_interval_ms = 200
grace_seconds = 1
max_catchup_seconds = 600
fire_retention_seconds = 3600
webhook_max_body_bytes = 16384
secret_key_file = "$WORK/triggers.key"
EOF
    fi
    if [ "${CH_QUEUE:-nats}" = nats ]; then
      cat <<EOF

[queue]
backend = "nats"

[queue.nats]
url = "$TACHYON_NATS_URL"
user = "$TACHYON_NATS_USER"
password_file = "$TACHYON_NATS_PASSWORD_FILE"
connect_timeout_ms = 2000
request_timeout_ms = 2000
EOF
    else
      printf '\n[queue]\nbackend = "sqlite"\n'
    fi
  } >"$file"
  printf '%s\n' "$port" >"$WORK/port-$name"
}

gw_url() { printf 'http://127.0.0.1:%s' "$(cat "$WORK/port-$1")"; }
gw_pid() { cat "$WORK/pids/gw-$1" 2>/dev/null; }

# gw_start NAME [FAILPOINTS] [ENV=VALUE...]: start and wait for /healthz (not /readyz).
gw_start() {
  local name="$1" fp="${2:-}" url pid i
  shift
  [ $# -gt 0 ] && shift
  url="$(gw_url "$name")"
  printf '\n===== %s start (failpoints: %s) %s =====\n' "$name" "${fp:-none}" "$(date -u +%H:%M:%S)" >>"$WORK/gateway-$name.log"
  env LOG_FORMAT=json TSLS_FAILPOINTS="$fp" "$@" "$GATEWAY_BIN" --config "$WORK/gw-$name.toml" >>"$WORK/gateway-$name.log" 2>&1 &
  pid=$!
  printf '%s\n' "$pid" >"$WORK/pids/gw-$name"
  for i in $(seq 1 240); do
    kill -0 "$pid" 2>/dev/null || { chaos_log "gateway $name exited during startup (poll $i)"; return 1; }
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url/healthz" || true)" = 200 ]; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

# gw_wait_ready NAME SECONDS: /readyz answers 200
gw_wait_ready() {
  local url deadline
  url="$(gw_url "$1")"
  deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url/readyz" || true)" = 200 ] && return 0
    sleep 0.2
  done
  return 1
}

# gw_wait_dead NAME SECONDS -> 0 when the process exited; GW_EXIT is its status
gw_wait_dead() {
  local pid deadline
  pid="$(gw_pid "$1")"
  deadline=$((SECONDS + $2))
  while kill -0 "$pid" 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.1
  done
  set +e
  wait "$pid" 2>/dev/null
  GW_EXIT=$?
  set -e
  rm -f "$WORK/pids/gw-$1"
  return 0
}

gw_signal() { kill "-$2" "$(gw_pid "$1")"; }

gw_stop() { # gw_stop NAME [TERM|KILL]
  local pid
  pid="$(gw_pid "$1")"
  [ -n "$pid" ] || return 0
  kill -CONT "$pid" 2>/dev/null || true
  kill "-${2:-TERM}" "$pid" 2>/dev/null || true
  gw_wait_dead "$1" 60 || { kill -KILL "$pid" 2>/dev/null || true; gw_wait_dead "$1" 10 || true; }
}

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

HTTP_CODE=""
HTTP_BODY=""
HTTP_HEADERS=""
# api GW METHOD PATH [BODY_FILE] [CURL ARGS...]
api() {
  local gw="$1" method="$2" path="$3" body="${4:-}" out hdr
  shift 3
  [ $# -gt 0 ] && shift
  out="$(mktemp "$WORK/http.XXXXXX")"
  hdr="$out.h"
  local args=(-s -o "$out" -D "$hdr" -w '%{http_code}' --max-time "${API_MAX_TIME:-60}" -X "$method" -H "authorization: Bearer $TOKEN")
  if [ -n "$body" ]; then args+=(-H 'content-type: application/json' --data-binary "@$body"); fi
  HTTP_CODE="$(curl "${args[@]}" "$@" "$(gw_url "$gw")$path" 2>/dev/null || true)"
  HTTP_BODY="$(cat "$out" 2>/dev/null || true)"
  HTTP_HEADERS="$(tr -d '\r' <"$hdr" 2>/dev/null || true)"
  rm -f "$out" "$hdr"
}
jqb() { printf '%s' "$HTTP_BODY" | jq -r "$1" 2>/dev/null; }
header() { printf '%s\n' "$HTTP_HEADERS" | awk -F': ' -v k="$1" 'tolower($1) == k {print $2}' | tail -n 1; }

# ---------------------------------------------------------------------------
# ledgers
# ---------------------------------------------------------------------------

# sql QUERY -> rows as `a|b|c` from state.db (read-only)
sql() { sqldb "$WORK/data/state.db" "$1"; }
sqldb() {
  python3 - "$1" "$2" <<'PY'
import sqlite3, sys
# Read-write open (no writes): a read-only open cannot create the -shm file of a WAL database
# whose writer closed cleanly.
con = sqlite3.connect(sys.argv[1], timeout=30)
for row in con.execute(sys.argv[2]):
    print("|".join("" if v is None else str(v) for v in row))
PY
}

inv_status() { sql "SELECT status FROM invocations WHERE id = '$1'"; }
inv_error_type() { sql "SELECT json_extract(body, '\$.status.error.error_type') FROM invocations WHERE id = '$1'"; }

# ---------------------------------------------------------------------------
# workload
# ---------------------------------------------------------------------------

# deploy GW NAME BINARY ENV_VARS_JSON SECRETS_JSON TIMEOUT MAX_CONCURRENCY [EXTRA_JSON] -> prints
# function id. EXTRA_JSON is merged into the revision request (e.g. egress, resources).
deploy() {
  local gw="$1" name="$2" bin="$3" envs="$4" secrets="$5" timeout="$6" conc="$7" extra="${8:-}" fid digest rev init=10
  [ -n "$extra" ] || extra='{}'
  # A cold microVM boot (inside the initialization deadline) takes seconds on a nested host.
  if provider_is_fc; then init=60; fi
  printf '{"name":"%s","description":"PLT-4646 chaos"}' "$name" >"$WORK/fn-$name.json"
  api "$gw" POST /v1/functions "$WORK/fn-$name.json"
  fid="$(jqb .id)"
  digest="$(curl -s --max-time 60 -X POST -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/octet-stream' --data-binary "@$bin" "$(gw_url "$gw")/v1/artifacts" | jq -r .digest)"
  jq -nc --arg d "$digest" --arg a "$ARCH" --argjson env "$envs" --argjson sec "$secrets" \
    --argjson t "$timeout" --argjson c "$conc" --argjson init "$init" --argjson extra "$extra" \
    '{artifact: {kind: "binary", digest: $d}, architecture: $a,
      execution: {timeout_seconds: $t, initialization_timeout_seconds: $init, max_concurrency: $c},
      env_vars: $env, secrets: $sec, publish_to_prod: true} + $extra' >"$WORK/rev-$name.json"
  api "$gw" POST "/v1/functions/$fid/revisions" "$WORK/rev-$name.json"
  rev="$(jqb .id)"
  for _ in $(seq 1 200); do
    api "$gw" GET "/v1/functions/$fid/revisions/$rev"
    [ "$(jqb .status)" = ready ] && break
    sleep 0.1
  done
  [ "$(jqb .status)" = ready ] || { chaos_log "revision of $name not ready: $HTTP_BODY"; return 1; }
  printf '%s\n' "$fid"
}

# wl_setup GW [cron=1] [webhook=1]: deploy the three workload functions and the triggers (cron and
# webhook fire F_HELLO).
# Sets F_ASYNC (idempotent-async), F_HELLO (hello with a secret), F_BURN (cpu-burn), CRON_ID, HOOK_ID.
wl_setup() {
  local gw="$1" cron="${2:-1}" hook="${3:-1}" secrets async_env async_extra='{}'
  secrets="$(jq -nc --arg r "$SECRET_REF" '[{env_name: "DEMO_SECRET", binding_ref: $r}]')"
  async_env="$(jq -nc --arg d "$WORK/effects" '[["IDEMPOTENT_ASYNC_DIR", $d]]')"
  if provider_is_fc; then
    # The idempotency records live outside the microVM (scripts/queue/effects-netns.sh), in the
    # same files ($WORK/effects) the checks read.
    local store_env url allow
    "$REPO_ROOT/scripts/queue/effects-netns.sh" down >/dev/null 2>&1 || true
    store_env="$("$REPO_ROOT/scripts/queue/effects-netns.sh" up "$WORK/effects")" || return 1
    url="$(printf '%s\n' "$store_env" | sed -n 's/^IDEMPOTENT_ASYNC_URL=//p')"
    allow="$(printf '%s\n' "$store_env" | sed -n 's/^EFFECTS_ALLOW=//p')"
    EFFECTS_STORE_LOG="$(printf '%s\n' "$store_env" | sed -n 's/^EFFECTS_STORE_LOG=//p')"
    async_env="$(jq -nc --arg u "$url" '[["IDEMPOTENT_ASYNC_URL", $u]]')"
    async_extra="$(jq -nc --arg c "${allow%:*}" --argjson p "${allow##*:}" '{egress: "restricted", egress_allow: [{cidr: $c, ports: [$p]}]}')"
  fi
  F_ASYNC="$(deploy "$gw" chaos-async "$ASYNC_BIN" "$async_env" "$secrets" 20 4 "$async_extra")"
  F_HELLO="$(deploy "$gw" chaos-hello "$HELLO_BIN" '[["GREETING","chaos"]]' "$secrets" 10 4)"
  F_BURN="$(deploy "$gw" chaos-burn "$BURN_BIN" '[]' "$secrets" 30 4)"
  [ -n "$F_ASYNC" ] && [ -n "$F_HELLO" ] && [ -n "$F_BURN" ] || return 1
  CRON_ID=""
  HOOK_ID=""
  if [ "$cron" = 1 ]; then
    "$TSLS_BIN" --api-url "$(gw_url "$gw")" --token "$TOKEN" --json triggers create "$F_HELLO" \
      --name chaos-cron --kind cron --schedule '*/2 * * * * *' --timezone UTC \
      --payload '{"job":"chaos"}' --missed-run skip >"$WORK/cron.json" 2>>"$WORK/tsls.log" || return 1
    CRON_ID="$(jq -r .id "$WORK/cron.json")"
  fi
  if [ "$hook" = 1 ]; then
    # The fire's event wraps the delivered body, so the webhook targets hello (any input).
    "$TSLS_BIN" --api-url "$(gw_url "$gw")" --token "$TOKEN" --json triggers create "$F_HELLO" \
      --name chaos-hook --kind webhook --max-body-bytes 16384 >"$WORK/hook.json" 2>>"$WORK/tsls.log" || return 1
    HOOK_ID="$(jq -r .id "$WORK/hook.json")"
    (umask 077; jq -r .secret "$WORK/hook.json" >"$WORK/hook.secret")
    rm -f "$WORK/hook.json"
  fi
  return 0
}

# wl_async GW ORDER [BYTES=16] [EXTRA_JSON_FIELDS] -> 202 appends "order id storage" to accepted.txt
wl_async() {
  local gw="$1" order="$2" bytes="${3:-16}" extra="${4:-}" f="$WORK/payload-$2.json"
  [ -n "$extra" ] || extra='{}'
  python3 -c 'import json,sys; d={"order_id": sys.argv[1], "blob": "x" * int(sys.argv[2])}; d.update(json.loads(sys.argv[3] or "{}")); print(json.dumps(d))' \
    "$order" "$bytes" "$extra" >"$f"
  api "$gw" POST "/v1/functions/$F_ASYNC:invokeAsync" "$f" -H "idempotency-key: chaos-$order"
  if [ "$HTTP_CODE" = 202 ]; then
    printf '%s %s %s\n' "$order" "$(jqb .invocation_id)" "$(jqb .input_storage)" >>"$WORK/accepted.txt"
  fi
}

# wl_webhook GW ORDER -> 202 appends "order id webhook"
wl_webhook() {
  local gw="$1" order="$2" f="$WORK/hook-$2.json" ts sig
  printf '{"order_id":"%s"}' "$order" >"$f"
  ts="$(date -u +%s)"
  sig="$({ printf '%s.' "$ts"; cat "$f"; } | openssl dgst -sha256 -hmac "$(cat "$WORK/hook.secret")" | sed 's/^.*= *//;s/^/v1=/')"
  HTTP_CODE="$(curl -s -o "$WORK/hook.out" -w '%{http_code}' --max-time 30 -X POST -H 'content-type: application/json' \
    -H "x-tachyon-webhook-timestamp: $ts" -H "x-tachyon-webhook-signature: $sig" -H "x-tachyon-webhook-id: evt-$order" \
    --data-binary "@$f" "$(gw_url "$gw")/v1/hooks/$HOOK_ID" || true)"
  HTTP_BODY="$(cat "$WORK/hook.out" 2>/dev/null || true)"
  if [ "$HTTP_CODE" = 202 ]; then
    printf '%s %s webhook\n' "$order" "$(jqb .invocation_id)" >>"$WORK/accepted.txt"
  fi
}

# wl_sync GW FUNCTION PAYLOAD -> HTTP_CODE, SYNC_ID
wl_sync() {
  printf '%s' "$3" >"$WORK/sync-payload.json"
  api "$1" POST "/v1/functions/$2/invoke" "$WORK/sync-payload.json"
  SYNC_ID="$(header x-tachyon-invocation-id)"
}

# wl_steady GW ROUNDS PREFIX: sync hello, async inline, async object, webhook (every round).
# Counts land in WL_SYNC_OK / WL_ASYNC_OK / WL_REFUSED.
wl_steady() {
  local gw="$1" rounds="$2" prefix="$3" n
  WL_SYNC_OK=0
  WL_ASYNC_OK=0
  WL_REFUSED=0
  for n in $(seq 1 "$rounds"); do
    wl_sync "$gw" "$F_HELLO" '{"name":"chaos"}'
    if [ "$HTTP_CODE" = 200 ] && [ "$(jqb .secret_present)" = true ]; then WL_SYNC_OK=$((WL_SYNC_OK + 1)); fi
    wl_async "$gw" "$prefix-i$n" 16
    if [ "$HTTP_CODE" = 202 ]; then WL_ASYNC_OK=$((WL_ASYNC_OK + 1)); else WL_REFUSED=$((WL_REFUSED + 1)); fi
    wl_async "$gw" "$prefix-o$n" 4096
    if [ "$HTTP_CODE" = 202 ]; then WL_ASYNC_OK=$((WL_ASYNC_OK + 1)); else WL_REFUSED=$((WL_REFUSED + 1)); fi
    if [ -n "${HOOK_ID:-}" ]; then
      wl_webhook "$gw" "$prefix-w$n"
      if [ "$HTTP_CODE" = 202 ]; then WL_ASYNC_OK=$((WL_ASYNC_OK + 1)); else WL_REFUSED=$((WL_REFUSED + 1)); fi
    fi
  done
}

# ---------------------------------------------------------------------------
# convergence (ground truth)
# ---------------------------------------------------------------------------

accepted_ids() { awk '{print $2}' "$WORK/accepted.txt" | sort -u; }
id_list_sql() { accepted_ids | sed "s/.*/'&'/" | paste -sd, -; }

# open_accepted -> number of accepted invocations not terminal in the ledger (missing rows count)
open_accepted() {
  local ids total present terminal
  ids="$(id_list_sql)"
  [ -n "$ids" ] || { echo 0; return 0; }
  total="$(accepted_ids | wc -l | tr -d ' ')"
  terminal="$(sql "SELECT COUNT(*) FROM invocations WHERE terminal = 1 AND id IN ($ids)")"
  present="$(sql "SELECT COUNT(*) FROM invocations WHERE id IN ($ids)")"
  echo $((total - terminal + 0 * present))
}

wait_accepted_terminal() { # SECONDS
  local deadline=$((SECONDS + $1))
  while [ "$(open_accepted)" != 0 ]; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}

wait_until() { # wait_until SECONDS CMD... (re-runs CMD every 0.25 s until it succeeds)
  local deadline=$((SECONDS + $1))
  shift
  until "$@"; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.25
  done
}
# wait_eq SECONDS EXPECTED CMD...: re-runs CMD until its output equals EXPECTED
wait_eq() {
  local deadline=$((SECONDS + $1)) expected="$2"
  shift 2
  until [ "$({ "$@" || true; } 2>/dev/null)" = "$expected" ]; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.25
  done
}
# wait_ge SECONDS MIN CMD...: re-runs CMD until its numeric output is >= MIN
wait_ge() {
  local deadline=$((SECONDS + $1)) min="$2" v
  shift 2
  while :; do
    v="$({ "$@" || true; } 2>/dev/null)"
    case "$v" in '' | *[!0-9]*) v=-1 ;; esac
    [ "$v" -ge "$min" ] && return 0
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.25
  done
}
nlines() { { "$@" || true; } | sed '/^$/d' | wc -l | tr -d ' '; }
readyz_field() { curl -s --max-time 2 "$(gw_url "$1")/readyz" | jq -r "$2"; }
readyz_code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$(gw_url "$1")/readyz"; }

# cv_async GW [TIMEOUT]: the async invariants over everything in accepted.txt.
cv_async() {
  local gw="$1" timeout="${2:-120}" ids total missing open not_ok bad_dl effects_bad multi_succ order id kind applied status
  ids="$(id_list_sql)"
  total="$(accepted_ids | wc -l | tr -d ' ')"
  [ "$total" -gt 0 ] || { ck cv.accepted_nonempty 1 "nothing accepted"; return 0; }
  missing=$((total - $(sql "SELECT COUNT(*) FROM invocations WHERE id IN ($ids)")))
  ck cv.accepted_never_lost "$([ "$missing" = 0 ] && echo 0 || echo 1)" "accepted=$total missing_rows=$missing"
  wait_accepted_terminal "$timeout" || true
  open="$(open_accepted)"
  ck cv.accepted_all_terminal "$([ "$open" = 0 ] && echo 0 || echo 1)" "accepted=$total not_terminal=$open"
  # Policy: succeeded, unless listed in expect-failed.txt (then failed with exactly one dead letter).
  not_ok=0
  while read -r order id kind; do
    status="$(inv_status "$id")"
    if grep -qx "$order" "$WORK/expect-failed.txt"; then
      [ "$status" = failed ] || [ "$status" = outcome_unknown ] || not_ok=$((not_ok + 1))
    else
      [ "$status" = succeeded ] || { not_ok=$((not_ok + 1)); chaos_log "    $order $id $kind -> $status $(inv_error_type "$id")"; }
    fi
  done <"$WORK/accepted.txt"
  ck cv.outcome_per_policy "$([ "$not_ok" = 0 ] && echo 0 || echo 1)" "unexpected_outcomes=$not_ok expected_failures=$(wc -l <"$WORK/expect-failed.txt" | tr -d ' ')"
  bad_dl="$(sql "SELECT COUNT(*) FROM (SELECT invocation_id FROM dead_letters WHERE invocation_id IS NOT NULL GROUP BY invocation_id HAVING COUNT(*) > 1)")"
  multi_succ="$(sql "SELECT COUNT(*) FROM (SELECT invocation_id FROM attempts WHERE status = 'succeeded' GROUP BY invocation_id HAVING COUNT(*) > 1)")"
  # A run re-executed after a crash before its commit records a second attempt (at least once,
  # ADR-0013): several succeeded attempts are allowed, one outcome (status, dead letter) is not.
  local succ_and_dead
  succ_and_dead="$(sql "SELECT COUNT(*) FROM invocations v JOIN dead_letters d ON d.invocation_id = v.id WHERE v.status = 'succeeded'")"
  ck cv.one_outcome_per_invocation "$([ "$bad_dl" = 0 ] && [ "$succ_and_dead" = 0 ] && echo 0 || echo 1)" \
    "invocations_with_>1_dead_letter=$bad_dl succeeded_but_dead_lettered=$succ_and_dead invocations_with_>1_succeeded_attempt=$multi_succ (re-executions, recorded)"
  obs cv.invocations_with_several_succeeded_attempts "$multi_succ"
  effects_bad=0
  while read -r order id kind; do
    grep -qx "$order" "$WORK/expect-failed.txt" && continue
    [ "$kind" = webhook ] && continue
    applied="$(grep -c "^$order .* applied\$" "$WORK/effects/executions.log" 2>/dev/null || true)"
    if [ "${applied:-0}" != 1 ] || [ ! -f "$WORK/effects/effects/$order.json" ]; then
      effects_bad=$((effects_bad + 1))
      chaos_log "    side effect of $order applied=${applied:-0}"
    fi
  done <"$WORK/accepted.txt"
  ck cv.side_effect_exactly_once "$([ "$effects_bad" = 0 ] && echo 0 || echo 1)" \
    "orders_not_applied_exactly_once=$effects_bad executions=$(wc -l <"$WORK/effects/executions.log" 2>/dev/null | tr -d ' ')"
  local unsent
  unsent="$(sql "SELECT COUNT(*) FROM outbox WHERE sent = 0")"
  ck cv.outbox_drained "$([ "$unsent" = 0 ] && echo 0 || echo 1)" "unsent_outbox_rows=$unsent"
  obs async_accepted "$total"
}

# cv_queue: the JetStream consumer holds nothing unacknowledged.
cv_queue() {
  local pending ack_pending msgs
  wait_eq 30 0 stream_stat ack_pending || true
  pending="$(stream_stat pending)"
  ack_pending="$(stream_stat ack_pending)"
  msgs="$(stream_stat stream_messages)"
  ck cv.jetstream_drained "$([ "$pending" = 0 ] && [ "$ack_pending" = 0 ] && echo 0 || echo 1)" \
    "pending=$pending ack_pending=$ack_pending stream_messages=$msgs redelivered=$(stream_stat redelivered)"
}

# cv_usage GW: the collector drained and the ledger holds each event and each settled attempt once.
cv_usage() {
  local gw="$1" pending events distinct dup_attempts ledger="$WORK/data/usage/ledger.db"
  wait_eq 30 0 readyz_field "$gw" .usage.journal.pending_events || true
  pending="$(curl -s --max-time 2 "$(gw_url "$gw")/readyz" | jq -r .usage.journal.pending_events)"
  events="$(sqldb "$ledger" 'SELECT COUNT(*) FROM function_usage_events')"
  distinct="$(sqldb "$ledger" 'SELECT COUNT(DISTINCT event_id) FROM function_usage_events')"
  dup_attempts="$(sqldb "$ledger" "SELECT COUNT(*) FROM (SELECT attempt_id FROM function_usage_events WHERE event_type = 'attempt_settled' GROUP BY attempt_id HAVING COUNT(*) > 1)")"
  ck cv.usage_counted_once "$([ "$pending" = 0 ] && [ "$events" = "$distinct" ] && [ "$dup_attempts" = 0 ] && echo 0 || echo 1)" \
    "journal_pending=$pending ledger_events=$events distinct=$distinct attempts_settled_twice=$dup_attempts duplicates_ignored=$(curl -s --max-time 2 "$(gw_url "$gw")/readyz" | jq -r .usage.ledger.duplicates_ignored)"
  if provider_is_fc; then
    # Host cost from cgroup accounting. Every EnvironmentStopped either carries the VMM cgroup's
    # usage_usec and memory.peak as provider_reported (> 0) or says `unknown`: a value is never
    # invented. Environments still pooled report when they stop (after this check).
    # Known gaps, recorded as observations and in docs/failure-matrix.md §8 (KVM final batch):
    #   - an environment ended by another path first (a fenced environment the reclaim terminated,
    #     then settled late by its old owner; one abandoned while booting during a shutdown) is
    #     reported with `unknown` host usage (CH_CGROUP_UNKNOWN_OK=1 in those scenarios);
    #   - an environment terminated by the reclaim / startup reconcile of a dead owner gets no
    #     EnvironmentStopped at all: its host cost is not metered (environments_without_stop_event).
    local stopped reported unknown invented terminal_envs envs_with_stop
    stopped="$(sqldb "$ledger" "SELECT COUNT(*) FROM function_usage_events WHERE event_type = 'environment_stopped'")"
    reported="$(sqldb "$ledger" "SELECT COUNT(*) FROM function_usage_events WHERE event_type = 'environment_stopped' AND json_extract(body, '\$.resources.cgroup_cpu_usec.measurement') = 'provider_reported' AND json_extract(body, '\$.resources.cgroup_cpu_usec.value') > 0 AND json_extract(body, '\$.resources.cgroup_memory_peak_bytes.measurement') = 'provider_reported' AND json_extract(body, '\$.resources.cgroup_memory_peak_bytes.value') > 0")"
    unknown="$(sqldb "$ledger" "SELECT COUNT(*) FROM function_usage_events WHERE event_type = 'environment_stopped' AND json_extract(body, '\$.resources.cgroup_cpu_usec.measurement') = 'unknown' AND json_extract(body, '\$.resources.cgroup_cpu_usec.value') IS NULL AND json_extract(body, '\$.resources.cgroup_memory_peak_bytes.measurement') = 'unknown'")"
    invented=$((stopped - reported - unknown))
    terminal_envs="$(sql "SELECT COUNT(*) FROM environments WHERE terminal = 1")"
    envs_with_stop="$(sqldb "$ledger" "SELECT COUNT(DISTINCT json_extract(body, '\$.environment_id')) FROM function_usage_events WHERE event_type = 'environment_stopped'")"
    obs usage.environment_stopped_events "$stopped"
    obs usage.environment_stopped_with_cgroup_usage "$reported"
    obs usage.environment_stopped_with_unknown_host_usage "$unknown"
    obs usage.terminal_environments "$terminal_envs"
    obs usage.environments_without_stop_event "$((terminal_envs - envs_with_stop))"
    if [ "${CH_CGROUP_UNKNOWN_OK:-0}" = 1 ]; then
      ck cv.usage_from_cgroup_accounting_or_unknown "$([ "$reported" -ge 1 ] && [ "$invented" = 0 ] && echo 0 || echo 1)" \
        "environment_stopped=$stopped provider_reported=$reported unknown=$unknown other=$invented; terminal environments=$terminal_envs, without a stop event=$((terminal_envs - envs_with_stop)) (known gaps, docs/failure-matrix.md §8)"
    else
      ck cv.usage_from_cgroup_accounting "$([ "$stopped" -ge 1 ] && [ "$reported" = "$stopped" ] && echo 0 || echo 1)" \
        "environment_stopped=$stopped with provider_reported cgroup cpu usec and memory.peak=$reported unknown=$unknown; terminal environments=$terminal_envs, without a stop event=$((terminal_envs - envs_with_stop))"
    fi
  fi
}

# cron_disable GW: stop new fires so the queue can drain for the checks
cron_disable() {
  [ -n "${CRON_ID:-}" ] || return 0
  "$TSLS_BIN" --api-url "$(gw_url "$1")" --token "$TOKEN" triggers update "$F_HELLO" "$CRON_ID" --disable >/dev/null 2>>"$WORK/tsls.log" || true
}

# cv_ledger_terminal [SECONDS]: every invocation in the ledger (sync, async, cron, webhook) is terminal
cv_ledger_terminal() {
  local open
  wait_eq "${1:-60}" 0 sql 'SELECT COUNT(*) FROM invocations WHERE terminal = 0' || true
  open="$(sql 'SELECT COUNT(*) FROM invocations WHERE terminal = 0')"
  ck cv.ledger_all_terminal "$([ "$open" = 0 ] && echo 0 || echo 1)" \
    "invocations=$(sql 'SELECT COUNT(*) FROM invocations') non_terminal=$open by_status=$(sql "SELECT group_concat(status || ':' || n) FROM (SELECT status, COUNT(*) n FROM invocations GROUP BY status)")"
}

# cv_cron: no scheduled time fired twice
cv_cron() {
  [ -n "${CRON_ID:-}" ] || return 0
  local fires distinct
  fires="$(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$CRON_ID'")"
  distinct="$(sql "SELECT COUNT(DISTINCT scheduled_at) FROM trigger_fires WHERE trigger_id = '$CRON_ID'")"
  ck cv.cron_each_time_once "$([ "$fires" = "$distinct" ] && echo 0 || echo 1)" "fires=$fires distinct_times=$distinct"
}

# scenario_processes -> `pid command` of processes whose argv names this scenario's scratch
# directory, excluding gateways and nats-server (bridges and user processes).
scenario_processes() {
  if provider_is_fc; then
    # A jailed VMM's argv does not name the scratch directory; the matrix is the host's only
    # Firecracker user, so every firecracker / jailer process counts.
    # shellcheck disable=SC2009 # the full command line is needed
    ps -eo pid=,args= | grep -E '^ *[0-9]+ ([^ ]*/)?(firecracker|jailer)( |$)' || true
    return 0
  fi
  # shellcheck disable=SC2009 # the full command line is needed (pgrep -f matches, ps prints)
  ps -axo pid=,command= | grep -F -- "$WORK/" | grep -v -e 'tachyon-serverless-gateway' -e 'nats-server' -e 'grep ' -e 'curl ' -e 'python3' -e 'sqlite3' || true
}
env_dirs() { find "$WORK/data/process" "$WORK/data/fc" -mindepth 1 -maxdepth 1 -type d -name 'env_*' 2>/dev/null | wc -l | tr -d ' '; }

# env_host_leftovers ENV_ID -> what of one environment still exists on the host: processes naming
# it, and with firecracker its jail (chroot), VMM cgroup and env dir (empty = gone). Taps and the
# egress table are host-wide and checked by cv_clean_after_stop.
env_host_leftovers() {
  local env="$1" inst
  inst="env-${env#env_}"
  # shellcheck disable=SC2009 # the full command line is needed
  ps -eo pid=,args= | grep -F -e "$env" -e "$inst" | grep -v -e 'grep ' -e 'python3' -e 'tachyon-serverless-gateway' | sed 's/^/process /' || true
  if provider_is_fc; then
    [ -e "/srv/jailer/firecracker/$inst" ] && echo "jail /srv/jailer/firecracker/$inst"
    [ -e "/sys/fs/cgroup/tachyon/$env" ] && echo "cgroup /sys/fs/cgroup/tachyon/$env"
    [ -e "$WORK/data/fc/$env" ] && echo "env_dir $WORK/data/fc/$env"
  fi
  return 0
}

# vmm_pids ENV_ID -> pids in the environment's VMM cgroup (firecracker)
vmm_pids() { cat "/sys/fs/cgroup/tachyon/$1/cgroup.procs" 2>/dev/null || true; }

# cv_clean_after_stop: every gateway stopped; nothing of this scenario runs; no environment is
# left open in the ledger; no secret value on disk.
cv_clean_after_stop() {
  local procs open_envs hits
  wait_eq 15 0 nlines scenario_processes || true
  procs="$(scenario_processes | wc -l | tr -d ' ')"
  [ "$procs" = 0 ] || scenario_processes >"$SC_DIR/leftover-processes.txt"
  ck cv.no_orphan_processes "$([ "$procs" = 0 ] && echo 0 || echo 1)" "bridge_or_user_processes=$procs env_dirs=$(env_dirs)"
  open_envs="$(sql "SELECT COUNT(*) FROM environments WHERE terminal = 0")"
  ck cv.no_open_environments "$([ "$open_envs" = 0 ] && echo 0 || echo 1)" "non_terminal_environment_rows=$open_envs"
  hits="$(secret_hits)"
  ck cv.no_secret_value_on_disk "$([ "$hits" = 0 ] && echo 0 || echo 1)" "files_containing_the_secret_value=$hits (config excluded; $(secret_scan_scope))"
  if provider_is_fc; then
    local left
    wait_eq 30 0 nlines provider_leftovers || true
    left="$(provider_leftovers | wc -l | tr -d ' ')"
    [ "$left" = 0 ] || provider_leftovers >"$SC_DIR/leftover-host-state.txt"
    ck cv.no_vmm_jail_cgroup_tap_or_table_left "$([ "$left" = 0 ] && echo 0 || echo 1)" \
      "firecracker/jailer processes, /sys/fs/cgroup/tachyon/*, /srv/jailer/firecracker/*, tsls* taps, table inet tachyon_egress: $left"
  fi
}

# secret_scan_scope -> the directories secret_hits reads
secret_scan_scope() {
  if provider_is_fc; then
    echo "$WORK (data_dir, provider workdir with function/scratch drives, console and fc logs, snapshots) /srv/jailer (jails)"
  else
    echo "$WORK"
  fi
}

# secret_hits -> files (config excluded) containing the secret value: the scratch directory, and
# with firecracker the jails (drives and logs hard-linked into chroots).
secret_hits() {
  local dirs=("$WORK")
  if provider_is_fc && [ -d /srv/jailer ]; then dirs+=(/srv/jailer); fi
  { grep -rlaF --exclude='gw-*.toml' -- "$SECRET_VALUE" "${dirs[@]}" 2>/dev/null || true; } | wc -l | tr -d ' '
}

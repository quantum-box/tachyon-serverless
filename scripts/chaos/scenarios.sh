#!/usr/bin/env bash
# scripts/chaos/scenarios.sh - the scenarios of the PLT-4646 failure matrix. Sourced by matrix.sh.
#
# Each `scenario_<id>` runs in its own subshell (errexit on) and ends through sc_cleanup (EXIT
# trap), which writes result.json. Timings (lease 6 s, heartbeat 1 s, skew 0.5 s, async claim 4 s,
# ack wait 4 s, orphan grace 6 s) come from gw_config in lib.sh.
# shellcheck disable=SC2034,SC2317

SCENARIOS="baseline
sync_gateway_kill
async_kill_accept_after_commit
async_kill_after_publish
async_kill_after_claim
async_kill_before_commit
async_kill_after_commit_before_ack
stale_owner_sync_lease
stale_owner_async_claim
db_locked_within_lease
db_locked_past_lease
broker_sigstop
broker_sigkill
object_store_unavailable
usage_journal_full_and_replay
worker_bridge_kill_sync
worker_user_process_kill_sync
worker_bridge_kill_async
control_plane_outage
orphan_recovery_after_crash"
# A guest kernel exists only with firecracker (TSLS_PROVIDER, scripts/kvm/provider-lib.sh).
if provider_is_fc; then
  SCENARIOS="$SCENARIOS
worker_user_process_oom_sync"
fi

scenario_fault() {
  case "$1" in
    baseline) echo "none (steady workload, graceful stop)" ;;
    sync_gateway_kill) echo "SIGKILL gateway with one sync invocation running and one queued behind it" ;;
    async_kill_accept_after_commit) echo "SIGKILL at failpoint accept.after_commit (before the 202)" ;;
    async_kill_after_publish) echo "SIGKILL at failpoint outbox.after_publish (broker ACK, row not marked)" ;;
    async_kill_after_claim) echo "SIGKILL at failpoint dispatch.after_claim (claimed, handler not started)" ;;
    async_kill_before_commit) echo "SIGKILL at failpoint dispatch.before_commit (side effect done, outcome not committed)" ;;
    async_kill_after_commit_before_ack) echo "SIGKILL at failpoint dispatch.after_commit (terminal committed, message not ACKed)" ;;
    stale_owner_sync_lease) echo "SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A" ;;
    stale_owner_async_claim) echo "SIGSTOP gateway A holding an async claim past expiry; B on the same data_dir retries; SIGCONT A" ;;
    db_locked_within_lease) echo "state.db write-locked (BEGIN EXCLUSIVE from another process) for 20 s < dispatcher lease 60 s" ;;
    db_locked_past_lease) echo "state.db write-locked for 12 s > dispatcher lease 6 s + skew" ;;
    broker_sigstop) echo "nats-server SIGSTOP (hung broker) with a small outbox bound, then SIGCONT" ;;
    broker_sigkill) echo "nats-server SIGKILL with messages in flight, then restart on the same store" ;;
    object_store_unavailable) echo "object root unreadable/unwritable (chmod 000) + orphan object from a SIGKILL after the put" ;;
    usage_journal_full_and_replay) echo "usage journal bound reached with the collector stopped; SIGKILL; collector SIGKILL after ledger commit; replay" ;;
    worker_bridge_kill_sync)
      if provider_is_fc; then echo "SIGKILL the Firecracker VMM (vsock to the guest bridge lost) of a running sync invocation"
      else echo "SIGKILL the runtime bridge (worker) of a running sync invocation"; fi ;;
    worker_user_process_kill_sync) echo "SIGKILL the user process of a running sync invocation" ;;
    worker_bridge_kill_async)
      if provider_is_fc; then echo "SIGKILL the Firecracker VMM of a running async invocation"
      else echo "SIGKILL the runtime bridge of a running async invocation"; fi ;;
    worker_user_process_oom_sync) echo "user process allocates 512 MiB in a 128 MiB guest (guest OOM killer; firecracker only)" ;;
    control_plane_outage) echo "control plane (management gateway) stopped past config TTL and auth lease (scripts/control-plane/outage-e2e.sh)" ;;
    orphan_recovery_after_crash) echo "SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object" ;;
    *) echo "?" ;;
  esac
}

# ---------------------------------------------------------------------------
# shared steps
# ---------------------------------------------------------------------------

# common_start [CRON=1] [WEBHOOK=1]: nats + gateway `a` + workload functions and triggers
common_start() {
  nats_up
  gw_config a "$(free_port)" chaos-a
  gw_start a
  gw_wait_ready a 60
  wl_setup a "${1:-1}" "${2:-1}"
}

# common_finish GW [TIMEOUT]: steady-state convergence, then a graceful stop and the clean-up checks
common_finish() {
  local gw="$1"
  cron_disable "$gw"
  cv_async "$gw" "${2:-120}"
  cv_ledger_terminal 60
  [ "$NATS_ENABLED" = 1 ] && cv_queue
  cv_cron
  cv_usage "$gw"
  gw_stop "$gw" TERM
  ck cv.graceful_stop_exit_0 "$([ "$GW_EXIT" = 0 ] && echo 0 || echo 1)" "exit=$GW_EXIT"
  cv_clean_after_stop
}

steady_checks() { # steady_checks ROUNDS
  ck workload.sync_ok "$([ "$WL_SYNC_OK" = "$1" ] && echo 0 || echo 1)" "sync_200_with_secret=$WL_SYNC_OK/$1"
  ck workload.async_accepted "$([ "$WL_REFUSED" = 0 ] && echo 0 || echo 1)" "accepted=$WL_ASYNC_OK refused=$WL_REFUSED"
}

# api_status GW INVOCATION -> "status error_type error_code" from the status URL
api_status() {
  api "$1" GET "/v1/invocations/$2"
  printf '%s %s\n' "$(jqb .status)" "$(jqb '.error.error_type // "-"')"
}

# running_of FUNCTION -> ids of its running invocations
running_of() { sql "SELECT id FROM invocations WHERE function_id = '$1' AND status = 'running' ORDER BY accepted_at"; }
count_of() { sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$1' AND status = '$2'"; }
nonempty() { [ -n "$("$@")" ]; }
env_of_invocation() { sql "SELECT environment_id FROM attempts WHERE invocation_id = '$1' ORDER BY number DESC LIMIT 1"; }
attempts_digest() { sql "SELECT id || ':' || status || ':' || terminal || ':' || length(body) FROM attempts WHERE invocation_id = '$1' ORDER BY number" | paste -sd, -; }
inv_digest() { sql "SELECT status || ':' || terminal || ':' || COALESCE(finished_at, '') || ':' || length(body) FROM invocations WHERE id = '$1'"; }
# executions_of ORDER [applied|skipped|failed] -> lines in executions.log (0 when none)
executions_of() {
  local n
  n="$(grep -c "^$1 .*${2:-}\$" "$WORK/effects/executions.log" 2>/dev/null || true)"
  printf '%s\n' "${n:-0}"
}
# The environment's worker: the runtime bridge (process provider) or the VMM (firecracker; killing
# it breaks the vsock connection to the guest bridge, the microVM equivalent of losing the bridge).
bridge_pid_of_env() {
  if provider_is_fc; then vmm_pids "$1" | head -n 1; else cat "$WORK/data/process/$1/bridge.pid" 2>/dev/null || true; fi
}
# procs_of_env ENV -> one line per thing of the environment still on the host. Process provider: its
# processes. Firecracker: the VMM / jailer processes and also its jail, VMM cgroup and env dir
# (env_host_leftovers), so every "terminated" check below proves the host side is gone too.
procs_of_env() {
  if provider_is_fc; then env_host_leftovers "$1"; return 0; fi
  # shellcheck disable=SC2009 # the full command line is needed
  ps -axo pid=,command= | grep -F -- "$1" | grep -v -e 'grep ' -e 'python3' -e 'tachyon-serverless-gateway' || true
}

# ---------------------------------------------------------------------------
# baseline
# ---------------------------------------------------------------------------

scenario_baseline() {
  sc_begin baseline "$(scenario_fault baseline)"
  common_start
  wl_steady a 3 base
  steady_checks 3
  sleep 3
  common_finish a
}

# ---------------------------------------------------------------------------
# 1. gateway SIGKILL during sync invokes
# ---------------------------------------------------------------------------

scenario_sync_gateway_kill() {
  sc_begin sync_gateway_kill "$(scenario_fault sync_gateway_kill)"
  common_start
  wl_steady a 2 pre
  steady_checks 2
  # One slot: the second invocation waits in the function's admission queue (not dispatched).
  F_SLOW="$(deploy a chaos-slow "$BURN_BIN" '[]' '[]' 30 1)"
  cron_disable a
  printf '{"seconds":20}' >"$WORK/slow1.json"
  printf '{"seconds":1}' >"$WORK/slow2.json"
  local url
  url="$(gw_url a)"
  curl -s -o "$WORK/slow1.out" -w '%{http_code}' --max-time 90 -X POST -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' -H 'idempotency-key: chaos-slow-1' --data-binary "@$WORK/slow1.json" \
    "$url/v1/functions/$F_SLOW/invoke" >"$WORK/slow1.code" 2>/dev/null &
  wait_until 30 nonempty running_of "$F_SLOW"
  local running queued
  running="$(running_of "$F_SLOW" | head -n 1)"
  curl -s -o "$WORK/slow2.out" -w '%{http_code}' --max-time 90 -X POST -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' -H 'idempotency-key: chaos-slow-2' --data-binary "@$WORK/slow2.json" \
    "$url/v1/functions/$F_SLOW/invoke" >"$WORK/slow2.code" 2>/dev/null &
  wait_eq 30 2 sql "SELECT COUNT(*) FROM invocations WHERE function_id = '$F_SLOW'"
  sleep 0.5
  queued="$(sql "SELECT id FROM invocations WHERE function_id = '$F_SLOW' AND id != '$running'")"
  obs_s before.running_status "$(inv_status "$running")"
  obs_s before.queued_status "$(inv_status "$queued")"
  ck setup.one_running_one_not_dispatched "$([ "$(inv_status "$running")" = running ] && [ "$(inv_status "$queued")" != running ] && echo 0 || echo 1)" \
    "running=$running:$(inv_status "$running") queued=$queued:$(inv_status "$queued")"
  local env_id
  env_id="$(env_of_invocation "$running")"
  mark_injected
  gw_signal a KILL
  gw_wait_dead a 10
  wait
  obs before.worker_processes_after_kill "$(procs_of_env "$env_id" | wc -l | tr -d ' ')"
  if provider_is_fc; then
    # The VMM is not a child of the gateway: it survives the SIGKILL and reconcile must end it.
    procs_of_env "$env_id" >"$SC_DIR/env-host-state-after-kill.txt"
    ck fault.vmm_survived_gateway_sigkill "$([ -n "$(vmm_pids "$env_id")" ] && echo 0 || echo 1)" \
      "env=$env_id vmm_pids=$(vmm_pids "$env_id" | paste -sd, -) host_state=$(wc -l <"$SC_DIR/env-host-state-after-kill.txt" | tr -d ' ') lines"
  fi
  ck client.no_answer_from_killed_gateway "$([ "$(cat "$WORK/slow1.code")" = 000 ] && [ "$(cat "$WORK/slow2.code")" = 000 ] && echo 0 || echo 1)" \
    "running_client=$(cat "$WORK/slow1.code") queued_client=$(cat "$WORK/slow2.code") (transport error: the client retries or reads the status URL)"
  mark_restored
  gw_start a
  gw_wait_ready a 60
  wait_eq 30 0 nlines procs_of_env "$env_id" || true
  mark_recovered
  local s1 s2
  s1="$(api_status a "$running")"
  s2="$(api_status a "$queued")"
  ck ledger.dispatched_is_outcome_unknown "$([ "$s1" = "outcome_unknown Host.Restarted" ] && echo 0 || echo 1)" "running -> $s1"
  ck ledger.not_dispatched_is_failed_platform "$([ "$s2" = "failed Host.Restarted" ] && echo 0 || echo 1)" "queued -> $s2"
  api a GET "/v1/invocations/$queued"
  ck ledger.not_dispatched_has_no_attempt_run "$([ "$(jqb '[.attempts[]? | select(.status == "succeeded" or .status == "running")] | length')" = 0 ] && echo 0 || echo 1)" \
    "attempts=$(jqb '[.attempts[]? | .status] | join(",")')"
  ck orphans.worker_of_dead_gateway_terminated "$([ "$(procs_of_env "$env_id" | wc -l | tr -d ' ')" = 0 ] && echo 0 || echo 1)" \
    "env=$env_id processes_left=$(procs_of_env "$env_id" | wc -l | tr -d ' ') reconcile=$(curl -s "$(gw_url a)/readyz" | jq -c '.reconcile | {found, terminated, lost, reclaim}')"
  # The client retries the invocation that never started: a new key runs it.
  wl_sync a "$F_SLOW" '{"seconds":0.2}'
  ck client.retry_of_not_started_succeeds "$([ "$HTTP_CODE" = 200 ] && echo 0 || echo 1)" "code=$HTTP_CODE"
  # The same key answers the recorded outcome instead of running again.
  api a POST "/v1/functions/$F_SLOW/invoke" "$WORK/slow1.json" -H 'idempotency-key: chaos-slow-1'
  ck client.replay_of_unknown_does_not_rerun "$([ "$(count_of "$F_SLOW" running)" = 0 ] && [ "$(header x-tachyon-invocation-id)" = "$running" ] && echo 0 || echo 1)" \
    "code=$HTTP_CODE error=$(jqb '.error.code // empty') invocation=$(header x-tachyon-invocation-id)"
  common_finish a
}

# ---------------------------------------------------------------------------
# 2. gateway SIGKILL at the async failpoints
# ---------------------------------------------------------------------------

# async_failpoint ID FAILPOINT BYTES EXPECT (accept_lost|accepted)
async_failpoint() {
  local id="$1" fp="$2" bytes="$3" mode="$4" order="fp-$1"
  sc_begin "$id" "$(scenario_fault "$id")"
  common_start
  wl_steady a 2 pre
  steady_checks 2
  cron_disable a
  sleep 1
  wait_eq 60 0 sql 'SELECT COUNT(*) FROM invocations WHERE terminal = 0' || true
  gw_stop a TERM
  gw_start a "$fp=kill"
  mark_injected
  wl_async a "$order" "$bytes" '{"sleep_ms":300}'
  local first_code="$HTTP_CODE"
  ckc fault.failpoint_killed_gateway "failpoint=$fp first_answer=$first_code" gw_wait_dead a 30
  ck fault.gateway_died_of_sigkill "$([ "${GW_EXIT:-0}" = 137 ] && echo 0 || echo 1)" "exit=${GW_EXIT:-}"
  local inv=""
  inv="$(awk -v o="$order" '$1 == o {print $2}' "$WORK/accepted.txt" | head -n 1)"
  if [ "$mode" = accept_lost ]; then
    ck fault.no_202_before_crash "$([ "$first_code" != 202 ] && echo 0 || echo 1)" "code=$first_code"
    obs before.rows_for_key "$(sql "SELECT COUNT(*) FROM idempotency WHERE idem_key = 'chaos-$order'" 2>/dev/null || echo 0)"
  else
    ck fault.accepted_before_crash "$([ "$first_code" = 202 ] && [ -n "$inv" ] && echo 0 || echo 1)" "code=$first_code invocation=$inv"
  fi
  if [ -n "$inv" ]; then
    local st applied unacked
    st="$(inv_status "$inv")"
    applied="$(grep -c "^$order .* applied\$" "$WORK/effects/executions.log" 2>/dev/null || true)"
    unacked=$(( $(stream_stat pending) + $(stream_stat ack_pending) ))
    obs_s at_crash.status "$st"
    obs at_crash.side_effects_applied "${applied:-0}"
    obs at_crash.unacked_messages "$unacked"
    # The queue message is never acknowledged before the terminal commit.
    if [ "$(sql "SELECT terminal FROM invocations WHERE id = '$inv'")" != 1 ]; then
      local outbox_unsent
      outbox_unsent="$(sql "SELECT COUNT(*) FROM outbox WHERE sent = 0")"
      ck at_crash.no_ack_before_terminal "$([ "$unacked" -ge 1 ] || [ "$outbox_unsent" -ge 1 ] && echo 0 || echo 1)" \
        "status=$st unacked_messages=$unacked unsent_outbox=$outbox_unsent"
    else
      ck at_crash.terminal_committed_message_still_unacked "$([ "$unacked" -ge 1 ] && echo 0 || echo 1)" "status=$st unacked_messages=$unacked"
    fi
  fi
  mark_restored
  gw_start a
  gw_wait_ready a 60
  if [ "$mode" = accept_lost ]; then
    # The client retries with its Idempotency-Key.
    wl_async a "$order" "$bytes" '{"sleep_ms":300}'
    inv="$(awk -v o="$order" '$1 == o {print $2}' "$WORK/accepted.txt" | head -n 1)"
    ck client.retry_with_key "$([ "$HTTP_CODE" = 202 ] && echo 0 || echo 1)" "code=$HTTP_CODE replayed=$(jqb .replayed)"
    obs_s client.retry_replayed "$(jqb .replayed)"
  fi
  wait_eq 90 1 sql "SELECT terminal FROM invocations WHERE id = '$inv'" || true
  mark_recovered
  api a GET "/v1/invocations/$inv"
  ck converge.invocation_succeeded "$([ "$(jqb .status)" = succeeded ] && echo 0 || echo 1)" \
    "status=$(jqb .status) attempts=$(jqb '.attempts | length') dispatch_attempts=$(jqb .dispatch.attempts) executions=$(grep -c "^$order " "$WORK/effects/executions.log" 2>/dev/null || true)"
  if [ "$fp" = dispatch.after_commit ]; then
    ck converge.not_executed_again "$([ "$(grep -c "^$order " "$WORK/effects/executions.log" || true)" = 1 ] && [ "$(jqb '.attempts | length')" = 1 ] && echo 0 || echo 1)" \
      "executions=$(grep -c "^$order " "$WORK/effects/executions.log" || true) attempts=$(jqb '.attempts | length')"
  fi
  wl_steady a 1 post
  steady_checks 1
  common_finish a
}

scenario_async_kill_accept_after_commit() { async_failpoint async_kill_accept_after_commit accept.after_commit 4096 accept_lost; }
scenario_async_kill_after_publish() { async_failpoint async_kill_after_publish outbox.after_publish 4096 accepted; }
scenario_async_kill_after_claim() { async_failpoint async_kill_after_claim dispatch.after_claim 16 accepted; }
scenario_async_kill_before_commit() { async_failpoint async_kill_before_commit dispatch.before_commit 4096 accepted; }
scenario_async_kill_after_commit_before_ack() { async_failpoint async_kill_after_commit_before_ack dispatch.after_commit 16 accepted; }

# ---------------------------------------------------------------------------
# 3. stale owner: two gateways on one data_dir, A frozen past its leases
# ---------------------------------------------------------------------------

scenario_stale_owner_sync_lease() {
  sc_begin stale_owner_sync_lease "$(scenario_fault stale_owner_sync_lease)"
  common_start
  gw_config b "$(free_port)" chaos-b
  gw_start b
  gw_wait_ready b 60
  wl_steady a 1 pre-a
  wl_steady b 1 pre-b
  cron_disable a
  local a_id url inv env_id before_inv before_att
  a_id="$(curl -s "$(gw_url a)/readyz" | jq -r .dispatcher.id)"
  url="$(gw_url a)"
  printf '{"seconds":15}' >"$WORK/long.json"
  curl -s -o "$WORK/long.out" -w '%{http_code}' --max-time 120 -X POST -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' --data-binary "@$WORK/long.json" \
    "$url/v1/functions/$F_BURN/invoke" >"$WORK/long.code" 2>/dev/null &
  wait_until 30 nonempty running_of "$F_BURN"
  inv="$(running_of "$F_BURN" | head -n 1)"
  env_id="$(env_of_invocation "$inv")"
  ck setup.a_owns_the_running_invocation "$([ "$(sql "SELECT owner_id FROM invocations WHERE id = '$inv'")" = "$a_id" ] && echo 0 || echo 1)" "invocation=$inv owner=$(sql "SELECT owner_id FROM invocations WHERE id = '$inv'") a=$a_id env=$env_id"
  mark_injected
  gw_signal a STOP
  # B reclaims only after A's lease (6 s) + skew (0.5 s): the handler is still running.
  wait_eq 40 outcome_unknown inv_status "$inv" || true
  local detect_ms=$(( $(now_ms) - SC_INJECTED ))
  obs reclaim_detect_ms "$detect_ms"
  local s_after
  s_after="$(api_status b "$inv")"
  ck reclaim.b_settles_as_outcome_unknown "$([ "$s_after" = "outcome_unknown Host.LeaseExpired" ] && echo 0 || echo 1)" "status=$s_after after ${detect_ms} ms"
  # A's lease runs from its last renewal (up to one heartbeat before the SIGSTOP): compare the
  # ledger's own timestamps, not the time since the signal.
  local lease_row margin_ms
  lease_row="$(sql "SELECT lease_expires_at || ' ' || COALESCE(reclaimed_at, '') FROM dispatchers WHERE id = '$a_id'")"
  margin_ms="$(python3 -c 'import sys, datetime as d
def p(s):
    base, _, frac = s.rstrip("Z").split("+")[0].partition(".")
    return d.datetime.strptime(base, "%Y-%m-%dT%H:%M:%S") + d.timedelta(microseconds=int((frac + "000000")[:6]))
e, r = sys.argv[1].split(" ")
print(int((p(r) - p(e)).total_seconds() * 1000))' "$lease_row" 2>/dev/null || echo -1)"
  ck reclaim.not_before_lease_expiry_plus_skew "$([ "$margin_ms" -ge 500 ] && echo 0 || echo 1)" \
    "lease_expires_at+reclaimed_at=[$lease_row] reclaimed ${margin_ms} ms after expiry (skew 500 ms); ${detect_ms} ms after SIGSTOP"
  wait_eq 30 0 nlines procs_of_env "$env_id" || true
  # B settles the fenced environment only after its terminate returned (SIGTERM grace).
  wait_eq 30 1 sql "SELECT terminal FROM environments WHERE id = '$env_id'" || true
  ck reclaim.fenced_environment_terminated "$([ "$(procs_of_env "$env_id" | wc -l | tr -d ' ')" = 0 ] && [ "$(sql "SELECT terminal FROM environments WHERE id = '$env_id'")" = 1 ] && echo 0 || echo 1)" \
    "env=$env_id state=$(sql "SELECT state FROM environments WHERE id = '$env_id'") processes=$(procs_of_env "$env_id" | wc -l | tr -d ' ')"
  before_inv="$(inv_digest "$inv")"
  before_att="$(attempts_digest "$inv")"
  mark_restored
  gw_signal a CONT
  sleep 5
  ck fencing.late_completion_does_not_overwrite "$([ "$(inv_digest "$inv")" = "$before_inv" ] && [ "$(attempts_digest "$inv")" = "$before_att" ] && echo 0 || echo 1)" \
    "invocation $before_inv -> $(inv_digest "$inv"); attempts unchanged=$([ "$(attempts_digest "$inv")" = "$before_att" ] && echo yes || echo no)"
  wait_eq 15 503 readyz_code a || true
  ck fencing.a_is_fenced "$(curl -s "$(gw_url a)/readyz" | jq -e '.dispatcher.fenced == true' >/dev/null && echo 0 || echo 1)" \
    "readyz=$(curl -s -o /dev/null -w '%{http_code}' "$(gw_url a)/readyz") fenced=$(curl -s "$(gw_url a)/readyz" | jq -c .dispatcher.fenced)"
  wl_sync a "$F_HELLO" '{}'
  ck fencing.a_refuses_new_invocations "$([ "$HTTP_CODE" = 503 ] && echo 0 || echo 1)" "code=$HTTP_CODE error_type=$(jqb .error.error_type)"
  wl_sync b "$F_HELLO" '{}'
  ck fencing.b_keeps_serving "$([ "$HTTP_CODE" = 200 ] && echo 0 || echo 1)" "code=$HTTP_CODE"
  wait_until 30 test -s "$WORK/long.code" || true
  obs_s client.a_answer_to_the_stale_invocation "$(cat "$WORK/long.code" 2>/dev/null) $(jq -c '.error | {code, error_type}' "$WORK/long.out" 2>/dev/null || true)"
  # Operations restart the fenced gateway (ADR-0003: no re-registration).
  gw_stop a TERM
  gw_start a
  gw_wait_ready a 60
  mark_recovered
  wl_steady a 1 post-a
  steady_checks 1
  gw_stop b TERM
  common_finish a
}

scenario_stale_owner_async_claim() {
  sc_begin stale_owner_async_claim "$(scenario_fault stale_owner_async_claim)"
  common_start
  gw_config b "$(free_port)" chaos-b
  wl_steady a 1 pre
  cron_disable a
  wait_eq 60 0 sql 'SELECT COUNT(*) FROM invocations WHERE terminal = 0' || true
  local order=stale-1 inv a_id before_inv applied skipped
  a_id="$(curl -s "$(gw_url a)/readyz" | jq -r .dispatcher.id)"
  wl_async a "$order" 16 '{"sleep_ms":9000}'
  inv="$(awk -v o="$order" '$1 == o {print $2}' "$WORK/accepted.txt")"
  wait_eq 30 running sql "SELECT state FROM async_dispatch WHERE invocation_id = '$inv'"
  # The handler is running (attempt dispatched): A's run will finish its side effect while A is
  # frozen, and A will try to settle it late.
  wait_ge 30 1 sql "SELECT COUNT(*) FROM attempts WHERE invocation_id = '$inv' AND status = 'dispatched'"
  obs_s setup.claimed_by "$(sql "SELECT claimed_by FROM async_dispatch WHERE invocation_id = '$inv'")"
  ck setup.a_handler_running "$([ "$(sql "SELECT COUNT(*) FROM attempts WHERE invocation_id = '$inv' AND status = 'dispatched'")" -ge 1 ] && [ "$(sql "SELECT claimed_by FROM async_dispatch WHERE invocation_id = '$inv'")" = "$a_id" ] && echo 0 || echo 1)" \
    "claimed_by=$(sql "SELECT claimed_by FROM async_dispatch WHERE invocation_id = '$inv'") a=$a_id"
  mark_injected
  gw_signal a STOP
  gw_start b
  gw_wait_ready b 60
  wait_eq 90 succeeded inv_status "$inv" || true
  obs takeover_ms $(( $(now_ms) - SC_INJECTED ))
  api b GET "/v1/invocations/$inv"
  ck takeover.b_completed_the_invocation "$([ "$(jqb .status)" = succeeded ] && echo 0 || echo 1)" \
    "status=$(jqb .status) attempts=$(jqb '[.attempts[] | .status] | join(",")') dispatch=$(jqb '.dispatch | {state, attempts, generation} | tojson')"
  wait_ge 20 2 grep -c "^$order " "$WORK/effects/executions.log" || true
  before_inv="$(inv_digest "$inv")"
  local before_att
  before_att="$(attempts_digest "$inv")"
  mark_restored
  gw_signal a CONT
  sleep 6
  ck fencing.late_settle_does_not_overwrite "$([ "$(inv_digest "$inv")" = "$before_inv" ] && [ "$(attempts_digest "$inv")" = "$before_att" ] && echo 0 || echo 1)" \
    "invocation $before_inv -> $(inv_digest "$inv"); attempts $before_att -> $(attempts_digest "$inv")"
  applied="$(grep -c "^$order .* applied\$" "$WORK/effects/executions.log" || true)"
  skipped="$(grep -c "^$order .* skipped\$" "$WORK/effects/executions.log" || true)"
  ck fencing.side_effect_once "$([ "$applied" = 1 ] && echo 0 || echo 1)" "applied=$applied skipped=$skipped"
  ck fencing.one_terminal_no_dead_letter "$([ "$(sql "SELECT COUNT(*) FROM dead_letters WHERE invocation_id = '$inv'")" = 0 ] && [ "$(sql "SELECT COUNT(*) FROM attempts WHERE invocation_id = '$inv' AND status = 'succeeded'")" = 1 ] && echo 0 || echo 1)" \
    "dead_letters=$(sql "SELECT COUNT(*) FROM dead_letters WHERE invocation_id = '$inv'") succeeded_attempts=$(sql "SELECT COUNT(*) FROM attempts WHERE invocation_id = '$inv' AND status = 'succeeded'")"
  obs_s a.refusal_log_lines "$(grep -ciE 'stale|fenced|lost (its|the) claim|claim lost|not the owner' "$WORK/gateway-a.log" || true)"
  wait_eq 15 503 readyz_code a || true
  ck fencing.a_is_fenced "$(curl -s "$(gw_url a)/readyz" | jq -e '.dispatcher.fenced == true' >/dev/null && echo 0 || echo 1)" "a=$a_id"
  gw_stop a TERM
  gw_start a
  gw_wait_ready a 60
  mark_recovered
  wl_steady a 1 post
  steady_checks 1
  gw_stop b TERM
  common_finish a
}

# ---------------------------------------------------------------------------
# 4. DB unavailable: state.db write lock held by another process
# ---------------------------------------------------------------------------

# db_lock SECONDS: hold BEGIN EXCLUSIVE on state.db in the background; LOCK_PID
db_lock() {
  python3 - "$WORK/data/state.db" "$1" "$WORK/lock.ready" <<'PY' &
import sqlite3, sys, time
con = sqlite3.connect(sys.argv[1], timeout=30, isolation_level=None)
con.execute("BEGIN EXCLUSIVE")
open(sys.argv[3], "w").write("locked")
time.sleep(float(sys.argv[2]))
con.execute("ROLLBACK")
PY
  LOCK_PID=$!
  wait_until 30 test -f "$WORK/lock.ready"
}

db_locked() { # db_locked ID SECONDS REQUIRE_503 EXPECT(serve|fenced)
  local id="$1" secs="$2" require_503="$3" expect="$4"
  sc_begin "$id" "$(scenario_fault "$id")"
  common_start
  wl_steady a 2 pre
  steady_checks 2
  sleep 2
  local fns_before invs_before fires_before
  fns_before="$(sql 'SELECT COUNT(*) FROM functions')"
  fires_before="$(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$CRON_ID'")"
  db_lock "$secs"
  mark_injected
  invs_before="$(sql 'SELECT COUNT(*) FROM invocations')"
  # Rounds of parallel requests while the lock is held: a management write, a sync invoke and an
  # async acceptance (each line: kind code error_type key duration_ms).
  local round=0 t0
  : >"$WORK/outage.txt"
  while kill -0 "$LOCK_PID" 2>/dev/null; do
    round=$((round + 1))
    t0="$(now_ms)"
    printf '{"name":"chaos-outage-%s","description":"x"}' "$round" >"$WORK/fn-outage-$round.json"
    (api a POST /v1/functions "$WORK/fn-outage-$round.json"; printf 'mgmt %s %s fn-%s %s\n' "$HTTP_CODE" "$(jqb '.error.error_type // .error.code // "-"')" "$round" $(( $(now_ms) - t0 )) >>"$WORK/outage.txt") &
    (wl_sync a "$F_HELLO" '{}'; printf 'sync %s %s - %s\n' "$HTTP_CODE" "$(jqb '.error.error_type // .error.code // "-"')" $(( $(now_ms) - t0 )) >>"$WORK/outage.txt") &
    (wl_async a "outage-$round" 16; printf 'async %s %s chaos-outage-%s %s\n' "$HTTP_CODE" "$(jqb '.error.error_type // .error.code // "-"')" "$round" $(( $(now_ms) - t0 )) >>"$WORK/outage.txt") &
    wait_eq 90 $((round * 3)) nlines cat "$WORK/outage.txt" || true
  done
  cp "$WORK/outage.txt" "$SC_DIR/outage-requests.txt"
  local bad unavailable created max_ms
  # Allowed: success after the lock, 503 Host.StoreUnavailable, or (past the lease) the 503 of a
  # dispatcher that fenced itself. Anything else (500, a hang past the client timeout) is not.
  bad="$(awk '!(($2 ~ /^20[012]$/) || ($2 == 503 && $3 ~ /StoreUnavailable|Fenced|fenced|provider_unavailable/))' "$WORK/outage.txt" | wc -l | tr -d ' ')"
  unavailable="$(awk '$2 == 503 && $3 == "Host.StoreUnavailable"' "$WORK/outage.txt" | wc -l | tr -d ' ')"
  created="$(awk '$1 == "mgmt" && $2 == 201' "$WORK/outage.txt" | wc -l | tr -d ' ')"
  max_ms="$(awk 'BEGIN {m = 0} $5 > m {m = $5} END {print m}' "$WORK/outage.txt")"
  obs outage.requests "$(wc -l <"$WORK/outage.txt" | tr -d ' ')"
  obs outage.store_unavailable_503 "$unavailable"
  obs outage.slowest_request_ms "$max_ms"
  obs_s outage.answers "$(awk '{print $1 ":" $2 ":" $3}' "$WORK/outage.txt" | sort | uniq -c | awk '{print $2 "x" $1}' | paste -sd' ' -)"
  ck outage.answers_success_or_retryable_503 "$([ "$bad" = 0 ] && echo 0 || echo 1)" \
    "other_answers=$bad store_unavailable=$unavailable slowest_ms=$max_ms (a write waits at most connection wait + busy timeout)"
  if [ "$require_503" = 1 ]; then
    ck outage.store_unavailable_surfaced "$([ "$unavailable" -ge 1 ] && echo 0 || echo 1)" "503 Host.StoreUnavailable answers=$unavailable"
  fi
  obs_s outage.readyz "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$(gw_url a)/readyz")"
  wait "$LOCK_PID" 2>/dev/null || true
  mark_restored
  # Recovery: within the lease the gateway serves again without a restart. Past the lease
  # (+ skew) its heartbeat renewal is refused and it fences itself (ADR-0003 PLT-4631 choice 5:
  # no re-registration): only a restart brings it back. That is recorded, not hidden.
  local deadline=$((SECONDS + 30)) ok=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    wl_sync a "$F_HELLO" '{}'
    if [ "$HTTP_CODE" = 200 ]; then ok=1; break; fi
    sleep 0.5
  done
  obs_s recovery.first_sync_code "$HTTP_CODE $(jqb '.error.error_type // "-"')"
  obs_s recovery.readyz "$(curl -s --max-time 10 "$(gw_url a)/readyz" | jq -c '{ready, dispatcher}')"
  if [ "$expect" = serve ] && [ "$ok" = 1 ] && gw_wait_ready a 30; then
    mark_recovered
    ck recovery.serves_without_restart 0 "readyz 200 and sync 200"
  elif [ "$expect" = serve ]; then
    ck recovery.serves_without_restart 1 "last sync code=$HTTP_CODE $(jqb '.error.error_type // "-"'); readyz=$(curl -s --max-time 10 "$(gw_url a)/readyz" | jq -c '{ready, fenced: .dispatcher.fenced}')"
  else
    ck recovery.fenced_itself_after_losing_its_lease "$([ "$ok" = 0 ] && [ "$(readyz_field a .dispatcher.fenced)" = true ] && echo 0 || echo 1)" \
      "sync=$HTTP_CODE readyz=$(curl -s --max-time 10 "$(gw_url a)/readyz" | jq -c '{ready, fenced: .dispatcher.fenced}') (known limitation: restart required)"
  fi
  if [ -z "$SC_RECOVERED" ]; then
    # Documented behaviour for a lease lost to the outage: restart.
    gw_stop a TERM
    gw_start a
    if gw_wait_ready a 60; then
      mark_recovered
      ck recovery.restart_recovers 0 "fenced gateway restarted"
    else
      ck recovery.restart_recovers 1 "not ready after restart"
    fi
  fi
  local refused_keys refused_rows=0 k
  refused_keys="$(awk '$1 == "async" && $2 != 202 {print $4}' "$WORK/outage.txt")"
  for k in $refused_keys; do
    refused_rows=$((refused_rows + $(sql "SELECT COUNT(*) FROM idempotency WHERE idem_key = '$k'")))
  done
  ck outage.no_partial_rows "$([ "$(sql 'SELECT COUNT(*) FROM functions')" = $((fns_before + created)) ] && [ "$refused_rows" = 0 ] && echo 0 || echo 1)" \
    "functions $fns_before + created $created -> $(sql 'SELECT COUNT(*) FROM functions'); rows for refused async keys=$refused_rows; invocations before the outage=$invs_before (all terminal: cv.ledger_all_terminal)"
  wl_steady a 2 post
  steady_checks 2
  sleep 5
  ck recovery.cron_resumed "$([ "$(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$CRON_ID'")" -gt "$fires_before" ] && echo 0 || echo 1)" \
    "fires $fires_before -> $(sql "SELECT COUNT(*) FROM trigger_fires WHERE trigger_id = '$CRON_ID'")"
  common_finish a
}

scenario_db_locked_within_lease() { CH_LEASE_TTL=60 CH_HEARTBEAT=5 db_locked db_locked_within_lease 20 1 serve; }
scenario_db_locked_past_lease() { db_locked db_locked_past_lease 12 0 fenced; }

# ---------------------------------------------------------------------------
# 5. broker stopped
# ---------------------------------------------------------------------------

scenario_broker_sigstop() {
  CH_MAX_PENDING=12
  sc_begin broker_sigstop "$(scenario_fault broker_sigstop)"
  common_start 1 1
  wl_steady a 2 pre
  steady_checks 2
  mark_injected
  kill -STOP "$(nats_pid)"
  local n accepted=0 refused=0 reason="" sync_ok=0
  for n in $(seq 1 30); do
    wl_async a "out-$n" 16
    case "$HTTP_CODE" in
      202) accepted=$((accepted + 1)) ;;
      503) refused=$((refused + 1)); reason="$(jqb .error.reason)" ;;
    esac
    if [ "$n" = 1 ]; then sleep 4; fi
  done
  for n in 1 2 3; do wl_sync a "$F_HELLO" '{}'; [ "$HTTP_CODE" = 200 ] && sync_ok=$((sync_ok + 1)); done
  obs outage.accepted "$accepted"
  obs outage.refused "$refused"
  obs outage.outbox_unsent "$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')"
  ck outage.accepted_into_outbox_up_to_bound "$([ "$accepted" -ge 1 ] && [ "$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')" -le 12 ] && echo 0 || echo 1)" \
    "accepted=$accepted unsent_outbox=$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0') bound=12"
  ck outage.backlog_admission_refuses "$([ "$refused" -ge 1 ] && [ "$reason" = queue_unavailable ] && echo 0 || echo 1)" "refused=$refused reason=$reason"
  ck outage.sync_path_unaffected "$([ "$sync_ok" = 3 ] && echo 0 || echo 1)" "sync_200=$sync_ok/3"
  mark_restored
  kill -CONT "$(nats_pid)"
  wait_accepted_terminal 120 || true
  wait_eq 30 0 sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0' || true
  mark_recovered
  wl_steady a 1 post
  steady_checks 1
  common_finish a
}

scenario_broker_sigkill() {
  CH_MAX_PENDING=40
  sc_begin broker_sigkill "$(scenario_fault broker_sigkill)"
  common_start
  wl_steady a 1 pre
  steady_checks 1
  local n
  # In flight at the kill: published and delivered, handlers sleeping.
  for n in 1 2 3 4; do wl_async a "inflight-$n" 16 '{"sleep_ms":3000}'; done
  wait_ge 20 1 sql "SELECT COUNT(*) FROM async_dispatch WHERE state = 'running'" || true
  obs before.running_async "$(sql "SELECT COUNT(*) FROM async_dispatch WHERE state = 'running'")"
  mark_injected
  "$REPO_ROOT/scripts/queue/down.sh" --kill >/dev/null 2>&1
  local accepted=0
  for n in 1 2 3 4 5 6; do
    wl_async a "down-$n" 4096
    [ "$HTTP_CODE" = 202 ] && accepted=$((accepted + 1))
    sleep 0.5
  done
  obs outage.accepted "$accepted"
  obs outage.outbox_unsent "$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')"
  ck outage.accepts_into_outbox_while_broker_down "$([ "$accepted" -ge 1 ] && echo 0 || echo 1)" "accepted=$accepted unsent=$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')"
  sleep 5
  mark_restored
  nats_up
  wait_accepted_terminal 150 || true
  wait_eq 30 0 sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0' || true
  mark_recovered
  obs after.redelivered "$(stream_stat redelivered || echo null)"
  wl_steady a 1 post
  steady_checks 1
  common_finish a 150
}

# ---------------------------------------------------------------------------
# 6. object store unavailable, orphan objects
# ---------------------------------------------------------------------------

object_files() { find "$WORK/data/objects" -name '*.data' ! -name '.tmp-*' 2>/dev/null | sed 's#.*/##; s#\.data$##' | sort; }
# unreferenced_objects -> object files no invocation input names
unreferenced_objects() {
  local refs
  refs="$(sql "SELECT object_id FROM invocation_inputs WHERE object_id IS NOT NULL" | sort -u)"
  comm -23 <(object_files) <(printf '%s\n' "$refs" | sed '/^$/d')
}

scenario_object_store_unavailable() {
  CH_MAX_ATTEMPTS=30
  sc_begin object_store_unavailable "$(scenario_fault object_store_unavailable)"
  common_start
  wl_steady a 2 pre
  steady_checks 2
  cron_disable a
  # An orphan object: SIGKILL after the put, before the acceptance transaction.
  gw_stop a TERM
  gw_start a accept.after_object_put=kill
  wl_async a orphan-1 4096
  gw_wait_dead a 30 || true
  gw_start a
  gw_wait_ready a 60
  local orphans_before
  orphans_before="$(unreferenced_objects | wc -l | tr -d ' ')"
  obs before.object_files "$(object_files | wc -l | tr -d ' ')"
  obs before.unreferenced_objects "$orphans_before"
  ck setup.orphan_object_on_disk "$([ "$orphans_before" -ge 1 ] && echo 0 || echo 1)" "unreferenced=$orphans_before"
  # An accepted object-input invocation whose run reads the object during the outage:
  # hold it in the outbox (broker stopped) until the store is unavailable.
  kill -STOP "$(nats_pid)"
  wl_async a held-1 4096
  ck setup.held_invocation_accepted "$([ "$HTTP_CODE" = 202 ] && echo 0 || echo 1)" "code=$HTTP_CODE storage=$(jqb .input_storage)"
  mark_injected
  chmod 000 "$WORK/data/objects"
  kill -CONT "$(nats_pid)"
  wl_async a during-big 4096
  local big_code="$HTTP_CODE" big_reason
  big_reason="$(jqb .error.reason)"
  wl_async a during-small 16
  local small_code="$HTTP_CODE"
  ck outage.object_input_refused_with_reason "$([ "$big_code" = 503 ] && [ "$big_reason" = object_store_unavailable ] && echo 0 || echo 1)" "code=$big_code reason=$big_reason"
  ck outage.inline_input_still_accepted "$([ "$small_code" = 202 ] && echo 0 || echo 1)" "code=$small_code"
  local held
  held="$(awk '$1 == "held-1" {print $2}' "$WORK/accepted.txt")"
  sleep 6
  obs_s outage.held_invocation "$(api_status a "$held") dispatch=$(jqb '.dispatch | {state, attempts, deferrals} | tojson')"
  ck outage.read_failure_is_not_a_success "$([ "$(inv_status "$held")" != succeeded ] && echo 0 || echo 1)" "status=$(inv_status "$held") executions=$(grep -c '^held-1 ' "$WORK/effects/executions.log" || true)"
  ck outage.gateway_alive "$(kill -0 "$(gw_pid a)" 2>/dev/null && echo 0 || echo 1)" "pid=$(gw_pid a)"
  mark_restored
  chmod 700 "$WORK/data/objects"
  wait_accepted_terminal 120 || true
  wait_eq 40 0 nlines unreferenced_objects || true
  mark_recovered
  local unref missing_live
  unref="$(unreferenced_objects | wc -l | tr -d ' ')"
  missing_live="$(comm -13 <(object_files) <(sql "SELECT i.object_id FROM invocation_inputs i JOIN invocations v ON v.id = i.invocation_id WHERE i.object_id IS NOT NULL AND v.terminal = 0" | sort -u | sed '/^$/d') | wc -l | tr -d ' ')"
  ck gc.orphans_collected "$([ "$unref" = 0 ] && echo 0 || echo 1)" "unreferenced $orphans_before -> $unref; files=$(object_files | wc -l | tr -d ' ')"
  ck gc.referenced_objects_kept "$([ "$missing_live" = 0 ] && echo 0 || echo 1)" "open invocations missing their object=$missing_live"
  api a GET "/v1/invocations/$held"
  obs_s after.held_invocation "$(jqb .status) $(jqb '.error.error_type // "-"') attempts=$(jqb '.attempts | length') deferrals=$(jqb .dispatch.deferrals)"
  wl_steady a 1 post
  steady_checks 1
  common_finish a
}

# ---------------------------------------------------------------------------
# 7. usage journal bound, collector stopped, replay
# ---------------------------------------------------------------------------

scenario_usage_journal_full_and_replay() {
  CH_JOURNAL_MAX_EVENTS=60
  CH_JOURNAL_HEADROOM=10
  CH_COLLECT_MS=3600000
  CH_COLLECT_BATCH=25
  sc_begin usage_journal_full_and_replay "$(scenario_fault usage_journal_full_and_replay)"
  common_start 0 0
  local ok=0 refused=0 code etype n invs_before invs_after
  for n in $(seq 1 60); do
    invs_before="$(sql 'SELECT COUNT(*) FROM invocations')"
    wl_sync a "$F_HELLO" '{}'
    code="$HTTP_CODE"
    if [ "$code" = 200 ]; then
      ok=$((ok + 1))
    else
      etype="$(jqb .error.error_type)"
      invs_after="$(sql 'SELECT COUNT(*) FROM invocations')"
      refused=$((refused + 1))
      [ -n "$SC_INJECTED" ] || mark_injected
      [ "$refused" -ge 3 ] && break
    fi
  done
  obs workload.metered_invocations "$ok"
  ck bound.refuses_fail_closed "$([ "$refused" -ge 1 ] && [ "$etype" = Host.UsageJournalFull ] && echo 0 || echo 1)" "ok=$ok refused=$refused last=$code $etype"
  ck bound.refusal_records_nothing "$([ "$invs_before" = "$invs_after" ] && echo 0 || echo 1)" "invocations before=$invs_before after=$invs_after"
  # Async acceptance is durable and does not meter; each run is admitted against the journal and
  # deferred without counting while it is full (ADR-0013). Nothing runs unmetered.
  wl_async a journal-full 16
  local async_code="$HTTP_CODE" async_inv
  async_inv="$(awk '$1 == "journal-full" {print $2}' "$WORK/accepted.txt")"
  ck bound.async_accepted_durably "$([ "$async_code" = 202 ] && echo 0 || echo 1)" "code=$async_code invocation=$async_inv"
  wait_ge 20 1 sql "SELECT deferrals FROM async_dispatch WHERE invocation_id = '$async_inv'" || true
  ck bound.async_run_deferred_not_executed "$([ "$(sql "SELECT deferrals FROM async_dispatch WHERE invocation_id = '$async_inv'")" -ge 1 ] 2>/dev/null && [ "$(executions_of journal-full)" = 0 ] && [ "$(inv_status "$async_inv")" != succeeded ] && echo 0 || echo 1)" \
    "status=$(inv_status "$async_inv") deferrals=$(sql "SELECT deferrals FROM async_dispatch WHERE invocation_id = '$async_inv'") executions=$(executions_of journal-full)"
  local journal="$WORK/data/usage/journal.db" ledger="$WORK/data/usage/ledger.db" pending
  pending="$(sqldb "$journal" 'SELECT pending_events FROM function_usage_journal_state')"
  obs journal.pending_before_crash "$pending"
  ck journal.events_retained_ledger_empty "$([ "$(sqldb "$ledger" 'SELECT COUNT(*) FROM function_usage_events' 2>/dev/null || echo 0)" = 0 ] && [ "$pending" -gt 0 ] && echo 0 || echo 1)" \
    "journal_pending=$pending ledger_events=$(sqldb "$ledger" 'SELECT COUNT(*) FROM function_usage_events' 2>/dev/null || echo 0)"
  gw_signal a KILL
  gw_wait_dead a 10
  # Collector on, killed between the ledger commit and the journal cursor.
  CH_COLLECT_MS=300
  gw_config a "$(cat "$WORK/port-a")" chaos-a
  mark_restored
  env LOG_FORMAT=json TSLS_USAGE_CRASH_POINT=collector.after_ledger_commit "$GATEWAY_BIN" --config "$WORK/gw-a.toml" >>"$WORK/gateway-a.log" 2>&1 &
  local crash_pid=$! crash_rc
  set +e
  wait "$crash_pid"
  crash_rc=$?
  set -e
  ck replay.collector_killed_after_ledger_commit "$([ "$crash_rc" = 137 ] && echo 0 || echo 1)" \
    "exit=$crash_rc ledger_events=$(sqldb "$ledger" 'SELECT COUNT(*) FROM function_usage_events') cursor=$(sqldb "$journal" 'SELECT cursor_seq FROM function_usage_journal_state')"
  gw_start a
  wait_eq 60 0 readyz_field a .usage.journal.pending_events || true
  gw_wait_ready a 30 || true
  mark_recovered
  local r events distinct dups report
  r="$(curl -s "$(gw_url a)/readyz")"
  events="$(sqldb "$ledger" 'SELECT COUNT(*) FROM function_usage_events')"
  distinct="$(sqldb "$ledger" 'SELECT COUNT(DISTINCT event_id) FROM function_usage_events')"
  dups="$(printf '%s' "$r" | jq -r .usage.ledger.duplicates_ignored)"
  # Every event the journal ever held (its cursor after the drain; events appended by the
  # restarted gateways count too) is in the ledger exactly once; the re-delivered batch was
  # recognised as duplicates.
  local collected
  collected="$(sqldb "$journal" 'SELECT cursor_seq FROM function_usage_journal_state')"
  ck replay.ledger_counts_each_event_once "$([ "$events" = "$collected" ] && [ "$distinct" = "$events" ] && [ "$events" -ge "$pending" ] && [ "$dups" -ge 25 ] && echo 0 || echo 1)" \
    "journal_before_crash=$pending journal_collected=$collected ledger_events=$events distinct=$distinct duplicates_ignored=$dups (batch 25)"
  api a GET "/v1/usage?group_by=function"
  report="$HTTP_BODY"
  printf '%s\n' "$report" >"$SC_DIR/usage-report.json"
  local hello_inv hello_att
  hello_inv="$(printf '%s' "$report" | jq --arg f "$F_HELLO" '[.lines[] | select(.function_id == $f) | .usage.invocations] | add // 0')"
  hello_att="$(printf '%s' "$report" | jq --arg f "$F_HELLO" '[.lines[] | select(.function_id == $f) | .usage.attempts] | add // 0')"
  ck replay.usage_matches_known_workload "$([ "$hello_inv" = "$ok" ] && [ "$hello_att" = "$ok" ] && echo 0 || echo 1)" \
    "report invocations=$hello_inv attempts=$hello_att known successful invokes=$ok"
  wl_sync a "$F_HELLO" '{}'
  ck replay.accepting_again "$([ "$HTTP_CODE" = 200 ] && echo 0 || echo 1)" "code=$HTTP_CODE"
  common_finish a
}

# ---------------------------------------------------------------------------
# 8. worker / guest disconnect
# ---------------------------------------------------------------------------

# worker_kill_sync ID TARGET(bridge|user)
worker_kill_sync() {
  local id="$1" target="$2"
  sc_begin "$id" "$(scenario_fault "$id")"
  common_start
  wl_steady a 1 pre
  steady_checks 1
  local url inv env_id bpid upid
  url="$(gw_url a)"
  printf '{"seconds":12}' >"$WORK/long.json"
  curl -s -o "$WORK/long.out" -w '%{http_code}' --max-time 60 -X POST -H "authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' --data-binary "@$WORK/long.json" \
    "$url/v1/functions/$F_BURN/invoke" >"$WORK/long.code" 2>/dev/null &
  local curl_pid=$!
  wait_until 30 nonempty running_of "$F_BURN"
  inv="$(running_of "$F_BURN" | head -n 1)"
  env_id="$(env_of_invocation "$inv")"
  sleep 1
  bpid="$(bridge_pid_of_env "$env_id")"
  upid="$(pgrep -f -- "example-cpu-burn|/artifacts/" 2>/dev/null | while read -r p; do ps -o command= -p "$p" | grep -qF "$WORK/" && echo "$p"; done | head -n 1 || true)"
  [ -n "$upid" ] || upid="$(pgrep -P "$bpid" 2>/dev/null | head -n 1 || true)"
  obs_s setup.pids "bridge=$bpid user=$upid env=$env_id"
  mark_injected
  if [ "$target" = bridge ]; then
    kill -KILL "$bpid"
  else
    kill -KILL "$upid"
  fi
  wait "$curl_pid" 2>/dev/null || true
  mark_restored
  wait_eq 30 1 sql "SELECT terminal FROM invocations WHERE id = '$inv'" || true
  local st code etype
  st="$(api_status a "$inv")"
  code="$(cat "$WORK/long.code")"
  etype="$(jq -r '.error.code // "-"' "$WORK/long.out" 2>/dev/null || echo -)"
  obs_s client.answer "$code $etype $(jq -r '.error.error_type // "-"' "$WORK/long.out" 2>/dev/null || true)"
  obs_s ledger.status "$st"
  case "$target" in
    bridge)
      # The Invoke frame was written: the handler may have run -> OutcomeUnknown (never success/plain failure).
      ck classify.bridge_loss_after_dispatch "$([ "${st%% *}" = outcome_unknown ] && [ "$etype" = outcome_unknown ] && echo 0 || echo 1)" "status=$st client=$code/$etype" ;;
    user)
      ck classify.user_process_crash "$([ "$st" = "failed Runtime.Crash" ] && [ "$etype" = crash ] && echo 0 || echo 1)" "status=$st client=$code/$etype" ;;
  esac
  ck classify.client_not_200 "$([ "$code" != 200 ] && echo 0 || echo 1)" "code=$code"
  wait_eq 30 0 nlines procs_of_env "$env_id" || true
  wait_eq 30 1 sql "SELECT terminal FROM environments WHERE id = '$env_id'" || true
  mark_recovered
  ck cleanup.environment_terminated "$([ "$(procs_of_env "$env_id" | wc -l | tr -d ' ')" = 0 ] && [ "$(sql "SELECT terminal FROM environments WHERE id = '$env_id'")" = 1 ] && echo 0 || echo 1)" \
    "state=$(sql "SELECT state FROM environments WHERE id = '$env_id'") processes=$(procs_of_env "$env_id" | wc -l | tr -d ' ') workdir_left=$([ -d "$WORK/data/process/$env_id" ] && echo yes || echo no)"
  wl_sync a "$F_BURN" '{"seconds":0.2}'
  ck recovery.next_invoke_succeeds "$([ "$HTTP_CODE" = 200 ] && echo 0 || echo 1)" "code=$HTTP_CODE"
  common_finish a
}

scenario_worker_bridge_kill_sync() { worker_kill_sync worker_bridge_kill_sync bridge; }
scenario_worker_user_process_kill_sync() { worker_kill_sync worker_user_process_kill_sync user; }

# The user process runs out of memory inside the guest (firecracker: the guest kernel's OOM killer
# ends it; the host cgroup limit, guest memory + overhead, is not reached). Process provider: the
# allocation is not bounded by a guest, so the scenario is firecracker-only.
scenario_worker_user_process_oom_sync() {
  sc_begin worker_user_process_oom_sync "$(scenario_fault worker_user_process_oom_sync)"
  if ! provider_is_fc; then
    ck harness.requires_firecracker 1 "TSLS_PROVIDER=$PROVIDER: no guest kernel, nothing bounds the allocation"
    SC_COMPLETE=1
    return 0
  fi
  common_start 0 0
  wl_steady a 1 pre
  steady_checks 1
  local f_oom inv env_id st code etype console peak
  f_oom="$(deploy a chaos-oom "$ISOLATION_PROBE_BIN" '[]' '[]' 30 1 '{"resources": {"memory_mib": 128}}')"
  mark_injected
  wl_sync a "$f_oom" '{"probe":"resources","alloc_mib":512}'
  code="$HTTP_CODE"
  etype="$(jqb '.error.code // "-"')"
  inv="$SYNC_ID"
  printf '%s\n' "$HTTP_BODY" >"$SC_DIR/oom-response.json"
  mark_restored
  wait_eq 30 1 sql "SELECT terminal FROM invocations WHERE id = '$inv'" || true
  st="$(api_status a "$inv")"
  env_id="$(env_of_invocation "$inv")"
  obs_s client.answer "$code $etype $(jqb '.error.error_type // "-"')"
  ck classify.guest_oom_is_user_process_crash "$([ "$st" = "failed Runtime.Crash" ] && [ "$etype" = crash ] && echo 0 || echo 1)" \
    "status=$st client=$code/$etype message=$(jqb '.error.message // "-"')"
  api a GET "/v1/invocations/$inv/logs"
  printf '%s\n' "$HTTP_BODY" >"$SC_DIR/oom-logs.json"
  console="$(find "$WORK/data/fc/_archive/$env_id" "$WORK/data/fc/$env_id" -name console.log 2>/dev/null | head -n 1)"
  if [ -n "$console" ]; then cp "$console" "$SC_DIR/oom-console.log"; fi
  ck evidence.guest_kernel_oom_killer "$(grep -qiE 'out of memory|oom-kill|oom_reaper' "$SC_DIR/oom-console.log" 2>/dev/null && echo 0 || echo 1)" \
    "console=$([ -n "$console" ] && echo "${console#"$WORK"/}" || echo missing) oom_lines=$(grep -ciE 'out of memory|oom-kill' "$SC_DIR/oom-console.log" 2>/dev/null || echo 0)"
  wait_eq 30 0 nlines procs_of_env "$env_id" || true
  wait_eq 30 1 sql "SELECT terminal FROM environments WHERE id = '$env_id'" || true
  mark_recovered
  ck cleanup.environment_terminated "$([ "$(procs_of_env "$env_id" | wc -l | tr -d ' ')" = 0 ] && [ "$(sql "SELECT terminal FROM environments WHERE id = '$env_id'")" = 1 ] && echo 0 || echo 1)" \
    "env=$env_id state=$(sql "SELECT state FROM environments WHERE id = '$env_id'") host_leftovers=$(procs_of_env "$env_id" | wc -l | tr -d ' ')"
  # The host cgroup (guest 128 MiB + 64 MiB overhead) held: its peak stays under memory.max.
  wait_ge 30 1 sqldb "$WORK/data/usage/ledger.db" "SELECT COUNT(*) FROM function_usage_events WHERE event_type = 'environment_stopped' AND body LIKE '%$env_id%'" || true
  peak="$(sqldb "$WORK/data/usage/ledger.db" "SELECT json_extract(body, '\$.resources.cgroup_memory_peak_bytes.value') FROM function_usage_events WHERE event_type = 'environment_stopped' AND body LIKE '%$env_id%' LIMIT 1")"
  ck host.vmm_cgroup_peak_below_memory_max "$([ -n "$peak" ] && [ "$peak" -le $(((128 + 64) * 1048576)) ] && echo 0 || echo 1)" \
    "memory.peak=${peak:-unknown} bytes, memory.max=$(((128 + 64) * 1048576))"
  wl_sync a "$f_oom" '{"probe":"resources","alloc_mib":16}'
  ck recovery.next_invoke_of_the_same_function_succeeds "$([ "$HTTP_CODE" = 200 ] && echo 0 || echo 1)" "code=$HTTP_CODE"
  common_finish a
}

scenario_worker_bridge_kill_async() {
  sc_begin worker_bridge_kill_async "$(scenario_fault worker_bridge_kill_async)"
  common_start
  wl_steady a 1 pre
  steady_checks 1
  cron_disable a
  local order=bridge-1 inv env_id bpid
  wl_async a "$order" 16 '{"sleep_ms":6000}'
  inv="$(awk -v o="$order" '$1 == o {print $2}' "$WORK/accepted.txt")"
  wait_ge 30 1 sql "SELECT COUNT(*) FROM attempts WHERE invocation_id = '$inv' AND status = 'dispatched'"
  env_id="$(env_of_invocation "$inv")"
  sleep 1
  bpid="$(bridge_pid_of_env "$env_id")"
  mark_injected
  kill -KILL "$bpid"
  mark_restored
  wait_eq 90 succeeded inv_status "$inv" || true
  wait_eq 30 0 nlines procs_of_env "$env_id" || true
  mark_recovered
  api a GET "/v1/invocations/$inv"
  ck retry.async_run_retried_to_success "$([ "$(jqb .status)" = succeeded ] && [ "$(jqb '.attempts | length')" -ge 2 ] && echo 0 || echo 1)" \
    "status=$(jqb .status) attempts=$(jqb '[.attempts[] | .status + "/" + (.error.error_type // "-")] | join(",")')"
  ck retry.side_effect_once "$([ "$(grep -c "^$order .* applied\$" "$WORK/effects/executions.log" || true)" = 1 ] && echo 0 || echo 1)" \
    "executions=$(grep -c "^$order " "$WORK/effects/executions.log" || true)"
  ck cleanup.killed_environment_gone "$([ "$(procs_of_env "$env_id" | wc -l | tr -d ' ')" = 0 ] && echo 0 || echo 1)" "env=$env_id state=$(sql "SELECT state FROM environments WHERE id = '$env_id'")"
  common_finish a
}

# ---------------------------------------------------------------------------
# 9. control plane outage (existing E2E, asserted through its summary)
# ---------------------------------------------------------------------------

scenario_control_plane_outage() {
  sc_begin control_plane_outage "$(scenario_fault control_plane_outage)"
  local rc=0 summary
  set +e
  TSLS_SKIP_BUILD=1 TSLS_EVIDENCE_DIR="$SC_DIR" "$REPO_ROOT/scripts/control-plane/outage-e2e.sh" >"$SC_DIR/outage-e2e.log" 2>&1
  rc=$?
  set -e
  summary="$(find "$SC_DIR" -name summary.json -path '*split-process*' | head -n 1)"
  ck e2e.exit_0 "$([ "$rc" = 0 ] && echo 0 || echo 1)" "exit=$rc"
  if [ -n "$summary" ]; then
    ck e2e.summary_ok "$(jq -e '.ok == true' "$summary" >/dev/null && echo 0 || echo 1)" \
      "passed=$(jq .passed "$summary") failed=$(jq .failed "$summary") skipped=$(jq .skipped "$summary")"
    local t_stop t_restart t_recovered
    # Times from the step durations (sequential steps, relative to the scenario start).
    t_stop="$(jq '[.steps as $s | range(0; $s | length) | select($s[.].name | startswith("management stops")) | [$s[0:.][] .duration_ms] | add] | first' "$summary")"
    t_restart="$(jq '[.steps as $s | range(0; $s | length) | select($s[.].name | startswith("management restarts")) | [$s[0:.][] .duration_ms] | add] | first' "$summary")"
    t_recovered="$(jq '[.steps as $s | range(0; $s | length) | select($s[.].name | startswith("data plane reconnects")) | [$s[0:(. + 1)][] .duration_ms] | add] | first' "$summary")"
    if [ "$t_stop" != null ] && [ "$t_restart" != null ] && [ "$t_recovered" != null ]; then
      SC_INJECTED=$((SC_T0 + t_stop))
      SC_RESTORED=$((SC_T0 + t_restart))
      SC_RECOVERED=$((SC_T0 + t_recovered))
    fi
    obs_s note "times derived from step durations of outage-e2e summary.json (approximate)"
    jq -c '[.steps[] | select(.status != "PASS") | .name]' "$summary" >"$SC_DIR/failed-steps.json"
  else
    ck e2e.summary_ok 1 "no summary.json"
  fi
}

# ---------------------------------------------------------------------------
# 10. orphan recovery after a crash
# ---------------------------------------------------------------------------

scenario_orphan_recovery_after_crash() {
  CH_MAX_PENDING=100
  # The orphan object must still be there at the crash.
  CH_ORPHAN_GRACE=45
  sc_begin orphan_recovery_after_crash "$(scenario_fault orphan_recovery_after_crash)"
  common_start
  wl_steady a 3 pre
  steady_checks 3
  local url old_id i
  url="$(gw_url a)"
  # An orphan object: SIGKILL after the put, before the acceptance transaction (cron paused so
  # nothing else reaches the failpoint).
  cron_disable a
  gw_stop a TERM
  gw_start a accept.after_object_put=kill
  wl_async a orphan-obj 4096
  gw_wait_dead a 30 || true
  gw_start a
  gw_wait_ready a 60
  "$TSLS_BIN" --api-url "$url" --token "$TOKEN" triggers update "$F_HELLO" "$CRON_ID" --enable >/dev/null 2>>"$WORK/tsls.log"
  sleep 3
  old_id="$(curl -s "$url/readyz" | jq -r .dispatcher.id)"
  # Two busy environments (the process provider cannot pool idle ones: reuse is Unsupported).
  printf '{"seconds":20}' >"$WORK/long.json"
  for i in 1 2; do
    curl -s -o /dev/null --max-time 60 -X POST -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
      --data-binary "@$WORK/long.json" "$url/v1/functions/$F_BURN/invoke" >/dev/null 2>&1 &
  done
  wait_ge 30 2 nlines running_of "$F_BURN" || true
  sleep 1
  # Outbox rows claimed but not published (broker hung).
  kill -STOP "$(nats_pid)"
  local n
  for n in 1 2 3; do wl_async a "held-$n" 4096; done
  sleep 3
  # Before the crash.
  local procs_before envs_before claims_before sched_before objs_before secret_before env_ids
  env_ids="$(sql 'SELECT id FROM environments WHERE terminal = 0')"
  printf '%s\n' "$env_ids" >"$WORK/old-envs.txt"
  procs_before="$(scenario_processes | wc -l | tr -d ' ')"
  envs_before="$(sql 'SELECT COUNT(*) FROM environments WHERE terminal = 0')"
  claims_before="$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')"
  sched_before="$(sql 'SELECT owner_id FROM trigger_scheduler')"
  objs_before="$(object_files | wc -l | tr -d ' ')"
  secret_before="$(secret_hits)"
  obs before.worker_processes "$procs_before"
  obs before.open_environments "$envs_before"
  obs before.unreferenced_objects "$(nlines unreferenced_objects)"
  obs before.unsent_outbox_rows "$claims_before"
  obs_s before.scheduler_owner "$sched_before"
  obs before.object_files "$objs_before"
  obs before.secret_files "$secret_before"
  ck setup.state_to_recover "$([ "$envs_before" -ge 2 ] && [ "$procs_before" -ge 2 ] && [ "$claims_before" -ge 1 ] && [ "$sched_before" = "$old_id" ] && [ "$(nlines unreferenced_objects)" -ge 1 ] && echo 0 || echo 1)" \
    "orphan_objects=$(nlines unreferenced_objects) open_envs=$envs_before worker_processes=$procs_before unsent_outbox=$claims_before scheduler_owner_is_a=$([ "$sched_before" = "$old_id" ] && echo yes || echo no)"
  mark_injected
  gw_signal a KILL
  gw_wait_dead a 10
  kill -CONT "$(nats_pid)"
  obs after_kill.worker_processes "$(scenario_processes | wc -l | tr -d ' ')"
  mark_restored
  gw_start a
  gw_wait_ready a 60
  local new_id
  new_id="$(curl -s "$url/readyz" | jq -r .dispatcher.id)"
  old_env_procs() { local e c=0; while read -r e; do [ -n "$e" ] || continue; c=$((c + $(procs_of_env "$e" | wc -l | tr -d ' '))); done <"$WORK/old-envs.txt"; echo "$c"; }
  old_env_open() { local ids; ids="$(sed "/^$/d; s/.*/'&'/" "$WORK/old-envs.txt" | paste -sd, -)"; [ -n "$ids" ] || { echo 0; return; }; sql "SELECT COUNT(*) FROM environments WHERE terminal = 0 AND id IN ($ids)"; }
  converged() {
    [ "$(old_env_procs)" = 0 ] && [ "$(old_env_open)" = 0 ] \
      && [ "$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')" = 0 ] \
      && [ "$(sql 'SELECT owner_id FROM trigger_scheduler')" = "$new_id" ] \
      && [ "$(unreferenced_objects | wc -l | tr -d ' ')" = 0 ]
  }
  wait_until 90 converged || true
  mark_recovered
  obs after.old_worker_processes "$(old_env_procs)"
  obs after.old_open_environments "$(old_env_open)"
  obs after.unsent_outbox_rows "$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')"
  obs_s after.scheduler_owner "$(sql 'SELECT owner_id FROM trigger_scheduler')"
  obs after.unreferenced_objects "$(unreferenced_objects | wc -l | tr -d ' ')"
  obs after.secret_files "$(secret_hits)"
  ck recover.worker_processes_reclaimed "$([ "$(old_env_procs)" = 0 ] && echo 0 || echo 1)" "before=$procs_before after=$(old_env_procs)"
  ck recover.environments_settled "$([ "$(old_env_open)" = 0 ] && echo 0 || echo 1)" "open before=$envs_before after=$(old_env_open) reconcile=$(curl -s "$url/readyz" | jq -c '.reconcile | {found, adopted, terminated, lost, foreign}')"
  ck recover.outbox_claims_released_and_published "$([ "$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')" = 0 ] && echo 0 || echo 1)" "unsent before=$claims_before after=$(sql 'SELECT COUNT(*) FROM outbox WHERE sent = 0')"
  ck recover.scheduler_lease_taken_over "$([ "$(sql 'SELECT owner_id FROM trigger_scheduler')" = "$new_id" ] && echo 0 || echo 1)" "old=$old_id new=$new_id owner=$(sql 'SELECT owner_id FROM trigger_scheduler')"
  ck recover.orphan_objects_collected "$([ "$(unreferenced_objects | wc -l | tr -d ' ')" = 0 ] && echo 0 || echo 1)" "unreferenced=$(unreferenced_objects | wc -l | tr -d ' ') files=$(object_files | wc -l | tr -d ' ')"
  ck recover.no_secret_value_on_disk "$([ "$(secret_hits)" = 0 ] && [ "$secret_before" = 0 ] && echo 0 || echo 1)" "before=$secret_before after=$(secret_hits)"
  wl_steady a 1 post
  steady_checks 1
  common_finish a 150
}

#!/usr/bin/env bash
# scripts/lab/demo.sh - the P1 / P2 / restart / P3 demos of scripts/lab/lab.sh (PLT-4648, PLT-4649).
# Sourced by lab.sh.
#
# Runs against the lab's already running gateway (lab.sh up) with the lab's generated tokens, so it
# never starts its own gateway and never touches another port or directory. Re-runnable: functions
# are get-or-create, every run adds revisions / triggers with unique names.
#
#   P1  register hello / http-axum / cpu-burn, publish, sync invoke (result, secret binding, user
#       error, timeout, HTTP), logs, boot evidence, v2 -> rollback -> v1, other tenant 404
#   P2  burst against revision max_concurrency, scale to zero, cold re-access, alias switch,
#       placement label (jp node: jp admitted, us refused), metrics snapshot (operator only)
#   restart  a gateway restart (SIGTERM + start) between P2 and P3: the ledger, the rolled back
#       alias, the invocation logs, async work accepted before the restart, the cron schedule and
#       the usage ledger (counted once, no replay) survive it
#   P3  invokeAsync (inline and object-store input), retries -> dead letter -> redrive (role
#       checked), cron trigger, signed webhook (good and bad signature), usage report, budget stop
#       and release, secret values absent from logs / demo outputs / ledger
#
# Results: <lab>/demo/<UTC>-<phase>/results.txt (PASS/FAIL per check) plus the JSON each check read.
# Exit 0 only when every check passed. Provider differences are printed, never hidden.

# HTTP_CODE / HTTP_BODY are set by api() in scripts/lab/lib.sh.
# shellcheck disable=SC2153
DEMO_FAILED=0
DEMO_OUT=""
DEMO_RESULTS=""

check() { # check NAME DETAIL CMD... : run CMD, record PASS/FAIL, continue
  local name="$1" detail="$2"
  shift 2
  if "$@" >/dev/null 2>&1; then
    printf 'PASS  %-58s %s\n' "$name" "$detail" | tee -a "$DEMO_RESULTS"
  else
    printf 'FAIL  %-58s %s\n' "$name" "$detail" | tee -a "$DEMO_RESULTS"
    DEMO_FAILED=1
  fi
}
note() { printf 'NOTE  %s\n' "$*" | tee -a "$DEMO_RESULTS"; }
section() { printf '\n== %s ==\n' "$*"; }
is() { [ "$1" = "$2" ]; }
ge() { [ "${1:-0}" -ge "$2" ] 2>/dev/null; }

# tsls as tenant A (TSLS_TOKEN is exported only for the call; tokens are never on a command line)
ta() { printf '+ tsls %s\n' "$*" >&2; TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN_A" "$TSLS_BIN" "$@"; }
tb() { printf '+ tsls (tenant B) %s\n' "$*" >&2; TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN_B" "$TSLS_BIN" "$@"; }
toncall() { printf '+ tsls (tenant A on-call: invoke+redrive) %s\n' "$*" >&2; TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN_A_ONCALL" "$TSLS_BIN" "$@"; }

RC=0
OUT=""
# capture CMD... -> RC, OUT (stdout); stderr goes to the transcript
capture() {
  set +e
  OUT="$("$@")"
  RC=$?
  set -e
}

ensure_function() { # NAME -> prints id
  local id
  if id="$(ta functions get "$1" --json 2>/dev/null | jq -r .id)" && [ -n "$id" ] && [ "$id" != null ]; then
    printf '%s\n' "$id"
  else
    ta functions create --name "$1" --description "lab demo ($LAB_ID)" --json | jq -r .id
  fi
}

deploy() { # FUNCTION BINARY ARGS... -> prints revision id
  local fn="$1" bin="$2"
  shift 2
  ta functions deploy --function "$fn" --binary "$bin" --arch "$ARCH" --json "$@" | tee -a "$DEMO_OUT/deploys.ndjson" | jq -r .id
}

wait_status() { # INVOCATION_ID SECONDS -> prints the terminal (or last) status
  local deadline=$((SECONDS + $2)) s=""
  while :; do
    s="$(ta functions invocation "$1" --json 2>/dev/null | jq -r .status)"
    case "$s" in succeeded | failed | cancelled | outcome_unknown) break ;; esac
    [ "$SECONDS" -lt "$deadline" ] || break
    sleep 0.5
  done
  printf '%s\n' "$s"
}

# ---------------------------------------------------------------------------
# P1
# ---------------------------------------------------------------------------

demo_p1() {
  section "P1: register -> publish -> sync invoke -> logs -> rollback (provider $PROVIDER)"
  [ "$PROVIDER" = firecracker ] || note "process provider: functions run as host child processes. NOT a microVM, NO isolation."
  local id rev1 rev2 inv detail
  capture ta provider --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/provider.json"
  check p1.provider_kind "kind=$(jq -r .kind <<<"$OUT") isolation=$(jq -r .isolation <<<"$OUT") dev_only=$(jq -r .dev_only <<<"$OUT")" \
    is "$(jq -r .kind <<<"$OUT")" "$PROVIDER"
  if [ "$PROVIDER" = firecracker ]; then
    check p1.provider_is_microvm "isolation=micro_vm dev_only=false" is "$(jq -r '.isolation + "/" + (.dev_only|tostring)' <<<"$OUT")" "micro_vm/false"
  fi

  id="$(ensure_function hello)"
  check p1.function_registered "hello=$id" test -n "$id" -a "$id" != null
  [ -n "$id" ] && [ "$id" != null ] || return 0
  rev1="$(deploy hello "$GUEST_DIR/example-hello" --env GREETING=v1 --secret DEMO_SECRET=demo-secret --description "lab v1")"
  check p1.publish_v1 "revision=$rev1 (prod -> v1)" test -n "$rev1"

  capture ta functions invoke hello --payload '{"name":"lab"}' --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p1-invoke-v1.json"
  check p1.sync_invoke_ok "exit=$RC message=$(jq -r .message <<<"$OUT" 2>/dev/null)" is "$RC/$(jq -r .message <<<"$OUT" 2>/dev/null)" "0/hello, lab"
  check p1.env_and_secret_binding "greeting=$(jq -r .greeting <<<"$OUT" 2>/dev/null) secret_present=$(jq -r .secret_present <<<"$OUT" 2>/dev/null) (the value itself is never returned)" \
    is "$(jq -r '"\(.greeting)/\(.secret_present)"' <<<"$OUT" 2>/dev/null)" "v1/true"
  inv="$(ta functions invocations hello --limit 1 --json | jq -r '(.items // .)[0].id')"
  capture ta functions logs --invocation "$inv"
  printf '%s\n' "$OUT" >"$DEMO_OUT/p1-logs.txt"
  check p1.logs "invocation=$inv lines=$(printf '%s\n' "$OUT" | grep -c . || true)" grep -q 'hello: handling invocation' "$DEMO_OUT/p1-logs.txt"
  detail="$(ta functions invocation "$inv" --json)"
  printf '%s\n' "$detail" >"$DEMO_OUT/p1-invocation.json"
  check p1.boot_evidence "host_pid=$(jq -r '.attempts[0].boot_evidence.host_pid' <<<"$detail") guest_boot_id=$(jq -r '.attempts[0].boot_evidence.guest_boot_id' <<<"$detail") handler_ms=$(jq -r '.attempts[0].timings.handler_ms' <<<"$detail")" \
    is "$(jq -r '.attempts[0].boot_evidence.host_pid != null and .attempts[0].timings.handler_ms != null' <<<"$detail")" true
  if [ "$PROVIDER" = firecracker ]; then
    check p1.guest_boot_id "a microVM reports its own boot id" is "$(jq -r '.attempts[0].boot_evidence.guest_boot_id != null' <<<"$detail")" true
  fi

  capture ta functions invoke hello --payload '{"fail":true}' --json
  check p1.user_error "exit=$RC code=$(jq -r .error.code <<<"$OUT" 2>/dev/null)" is "$RC/$(jq -r .error.code <<<"$OUT" 2>/dev/null)" "3/user_error"

  ensure_function http-axum >/dev/null
  deploy http-axum "$GUEST_DIR/example-http-axum" --description "lab http" >/dev/null
  capture ta functions http http-axum --method GET --path / --json
  check p1.http_adapter "status=$(jq -r .status <<<"$OUT" 2>/dev/null) body=$(jq -r .body <<<"$OUT" 2>/dev/null)" is "$(jq -r '"\(.status)/\(.body)"' <<<"$OUT" 2>/dev/null)" "200/ok"

  ensure_function cpu-burn >/dev/null
  local trev
  trev="$(deploy cpu-burn "$GUEST_DIR/example-cpu-burn" --timeout-seconds 2 --no-publish --description "lab timeout 2s")"
  capture ta functions invoke cpu-burn --payload '{"seconds":30,"ignore_sigterm":true}' --revision-id "$trev" --json
  check p1.timeout_enforced_by_host "exit=$RC code=$(jq -r .error.code <<<"$OUT" 2>/dev/null) (handler ignores SIGTERM; host kills at 2 s + grace)" \
    is "$RC/$(jq -r .error.code <<<"$OUT" 2>/dev/null)" "4/timeout"

  rev2="$(deploy hello "$GUEST_DIR/example-hello" --env GREETING=v2 --secret DEMO_SECRET=demo-secret --description "lab v2")"
  capture ta functions invoke hello --payload '{"name":"lab"}' --json
  check p1.publish_v2 "revision=$rev2 greeting=$(jq -r .greeting <<<"$OUT" 2>/dev/null)" is "$(jq -r .greeting <<<"$OUT" 2>/dev/null)" v2
  ta functions rollback hello >&2 || true
  ta functions aliases hello >&2 || true
  capture ta functions invoke hello --payload '{"name":"lab"}' --json
  check p1.rollback_to_v1 "prod -> previous revision, greeting=$(jq -r .greeting <<<"$OUT" 2>/dev/null)" is "$(jq -r .greeting <<<"$OUT" 2>/dev/null)" v1

  capture tb functions get "$id" --json
  check p1.other_tenant_404 "tenant B get $id: exit=$RC code=$(jq -r .error.code <<<"$OUT" 2>/dev/null)" is "$RC/$(jq -r .error.code <<<"$OUT" 2>/dev/null)" "2/not_found"
  capture tb functions invoke "$id" --payload '{}' --json
  check p1.other_tenant_invoke_404 "tenant B invoke: exit=$RC" is "$RC/$(jq -r .error.code <<<"$OUT" 2>/dev/null)" "2/not_found"

  sleep 1
  capture ta capacity --json
  check p1.no_environment_left "environments after the runs: $(jq -c .environments <<<"$OUT")" \
    is "$(jq -r '[.environments[]] | add' <<<"$OUT")" 0
}

# ---------------------------------------------------------------------------
# P2
# ---------------------------------------------------------------------------

demo_p2() {
  section "P2: burst -> scale to zero -> alias switch -> placement label -> metrics (provider $PROVIDER)"
  local n=7 limit=3 seconds=1 rev i max=0 cur pids="" ok=0 f
  if [ "$PROVIDER" = firecracker ]; then n=5; limit=2; fi
  ensure_function cpu-burn >/dev/null
  rev="$(deploy cpu-burn "$GUEST_DIR/example-cpu-burn" --max-concurrency "$limit" --description "lab burst")"
  check p2.revision_max_concurrency "revision=$rev max_concurrency=$limit" test -n "$rev"
  : >"$DEMO_OUT/p2-capacity-samples.ndjson"
  i=0
  while [ "$i" -lt "$n" ]; do
    (TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN_A" "$TSLS_BIN" functions invoke cpu-burn --payload "{\"seconds\":$seconds}" --json \
      >"$DEMO_OUT/p2-burst-$i.json" 2>/dev/null; echo $? >"$DEMO_OUT/p2-burst-$i.rc") &
    pids="$pids $!"
    i=$((i + 1))
  done
  echo "+ $n concurrent: tsls functions invoke cpu-burn --payload {\"seconds\":$seconds} (revision max_concurrency $limit)"
  local deadline=$((SECONDS + 120))
  while [ "$SECONDS" -lt "$deadline" ]; do
    f="$(TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN_A" "$TSLS_BIN" capacity --json 2>/dev/null || true)"
    printf '%s\n' "$f" | jq -c --arg r "$rev" '{in_flight, queue: .queue.length, rev: [.revisions[] | select(.revision_id == $r) | .environments]}' >>"$DEMO_OUT/p2-capacity-samples.ndjson" 2>/dev/null || true
    cur="$(printf '%s' "$f" | jq -r --arg r "$rev" '[.revisions[] | select(.revision_id == $r) | .environments | .starting + .busy + .promised] | add // 0' 2>/dev/null || echo 0)"
    [ "${cur:-0}" -le "$max" ] || max="$cur"
    [ "$(find "$DEMO_OUT" -name 'p2-burst-*.rc' | wc -l | tr -d ' ')" -lt "$n" ] || break
    sleep 0.2
  done
  # shellcheck disable=SC2086
  wait $pids 2>/dev/null || true
  for f in "$DEMO_OUT"/p2-burst-*.rc; do [ "$(cat "$f")" = 0 ] && ok=$((ok + 1)); done
  check p2.burst_all_served "$ok/$n invocations exit 0 (the rest waited in the queue)" is "$ok" "$n"
  check p2.burst_bounded "max environments of the revision observed=$max (limit $limit; samples in p2-capacity-samples.ndjson)" test "$max" -ge 1 -a "$max" -le "$limit"

  local zero=""
  deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    zero="$(TSLS_API_URL="$API" TSLS_TOKEN="$TOKEN_A" "$TSLS_BIN" capacity --json 2>/dev/null | jq -r --arg r "$rev" '[.revisions[] | select(.revision_id == $r) | .environments | .starting + .busy + .promised + .idle + .parking + .draining] | add // 0')"
    [ "$zero" != 0 ] || break
    sleep 0.5
  done
  capture ta capacity --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p2-capacity-after.json"
  check p2.scale_to_zero "environments of $rev=$zero; $(jq -r .scaling.at_zero <<<"$OUT")" is "$zero" 0
  capture ta functions invoke cpu-burn --payload '{"seconds":0}' --json
  check p2.cold_reaccess "exit=$RC (a new environment is started for the request)" is "$RC" 0

  local v3 gen
  v3="$(deploy hello "$GUEST_DIR/example-hello" --env GREETING=v3 --secret DEMO_SECRET=demo-secret --no-publish --description "lab v3 (not published)")"
  capture ta functions invoke hello --payload '{"name":"lab"}' --json
  local before
  before="$(jq -r .greeting <<<"$OUT" 2>/dev/null)"
  gen="$(ta functions aliases hello --json | jq -r '(.items // .)[] | select(.name == "prod") | .generation')"
  ta functions alias-set hello --alias prod --revision-id "$v3" --expected-generation "$gen" >&2 || true
  capture ta functions invoke hello --payload '{"name":"lab"}' --json
  check p2.alias_switch "prod generation $gen -> v3: greeting before=$before after=$(jq -r .greeting <<<"$OUT" 2>/dev/null)" is "$(jq -r .greeting <<<"$OUT" 2>/dev/null)" v3
  ta functions rollback hello >&2 || true
  capture ta functions invoke hello --payload '{"name":"lab"}' --json
  check p2.alias_rollback "greeting=$(jq -r .greeting <<<"$OUT" 2>/dev/null) (back to $before)" is "$(jq -r .greeting <<<"$OUT" 2>/dev/null)" "$before"

  local jp us
  jp="$(deploy hello "$GUEST_DIR/example-hello" --region jp --no-publish --description "lab region jp")"
  capture ta functions invoke hello --payload '{"name":"jp"}' --revision-id "$jp" --json
  check p2.placement_jp_label_admitted "node label region=jp, revision requires jp: exit=$RC" is "$RC" 0
  us="$(deploy hello "$GUEST_DIR/example-hello" --region us --no-publish --description "lab region us")"
  capture ta functions invoke hello --payload '{"name":"us"}' --revision-id "$us" --json
  check p2.placement_us_refused "exit=$RC reason=$(jq -r .error.reason <<<"$OUT" 2>/dev/null) (never relaxed)" is "$(jq -r .error.reason <<<"$OUT" 2>/dev/null)" placement
  note "region = \"jp\" is a scheduling LABEL written into this lab's config. It is not evidence of where data is stored or processed."

  api "$METRICS_TOKEN" GET /metrics
  printf '%s\n' "$HTTP_BODY" >"$DEMO_OUT/p2-metrics.prom"
  check p2.metrics_snapshot "HTTP $HTTP_CODE, $(grep -c '^tsls_' "$DEMO_OUT/p2-metrics.prom" || true) samples -> p2-metrics.prom" \
    grep -q '^tsls_environment_starts_total' "$DEMO_OUT/p2-metrics.prom"
  printf '%s\n' "$HTTP_BODY" | grep -E '^tsls_(environment_starts_total|admission_grants_total|scale_events_total|node_in_flight|environments\{)' | head -n 20 || true
  api "$TOKEN_A" GET /metrics
  check p2.metrics_operator_only "tenant token -> HTTP $HTTP_CODE" is "$HTTP_CODE" 401
}

# ---------------------------------------------------------------------------
# restart (PLT-4649: the scenario's "idle -> 0 -> restart -> async / cron")
# ---------------------------------------------------------------------------

# Stops the lab's gateway with SIGTERM and starts it again (lab.sh's own stop_gateway /
# start_gateway, so it is the same restart an operator does with `lab.sh down` + `lab.sh up`),
# then checks what has to survive it: the ledger and the alias P1 rolled back, the invocation
# logs, the environments (reclaimed by the startup reconcile), async work accepted before the
# restart, the cron schedule, and the usage ledger (no replay, no double count).
# cron_fires TRIGGER_ID -> how many fires of the trigger were accepted
cron_fires() {
  ta triggers fires hello "$1" --limit 100 --json 2>/dev/null |
    jq -r '[(.items // .)[] | select(.outcome == "accepted")] | length' 2>/dev/null
}

demo_restart() {
  section "restart: accepted async work, cron, ledger, logs and usage survive a gateway restart (provider $PROVIDER)"
  local stamp cbid axid inv n=4 i body
  local greeting_before greeting_after fn_before fn_after schema_before schema_after
  local logs_before logs_after log_offset this_start cron fires_before fires_after
  local cb_before cb_after ax_before ax_after ids="" id s accepted=0 terminal=0 unknown=0 attempts_max=0 a deadline pending
  stamp="$(date -u +%H%M%S)$(random_hex 2)"
  cbid="$(ensure_function cpu-burn)"
  axid="$(ensure_function http-axum)"

  # --- state before the restart -------------------------------------------------------------
  capture ta functions invoke hello --payload '{"name":"restart"}' --json
  greeting_before="$(jq -r .greeting <<<"$OUT" 2>/dev/null)"
  inv="$(ta functions invocations hello --limit 1 --json | jq -r '(.items // .)[0].id')"
  capture ta functions logs --invocation "$inv"
  printf '%s\n' "$OUT" >"$DEMO_OUT/restart-logs-before.txt"
  logs_before="$(grep -c . "$DEMO_OUT/restart-logs-before.txt" || true)"
  fn_before="$(ta functions list --json | jq -r '(.items // .) | length')"
  schema_before="$(sqlite_ro "$DATA_DIR/state.db" 'SELECT MAX(version) FROM schema_version' || true)"
  capture ta usage --group-by function --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/restart-usage-before.json"
  cb_before="$(jq -r --arg f "$cbid" '[.lines[] | select(.function_id == $f) | .usage.invocations] | add // 0' <<<"$OUT" 2>/dev/null)"
  ax_before="$(jq -r --arg f "$axid" '[.lines[] | select(.function_id == $f) | .usage.attempts] | add // 0' <<<"$OUT" 2>/dev/null)"

  # A cron trigger that keeps firing across the restart. Every 5 s, and disabled as soon as the
  # restart has been observed: a cron that fires faster than a cold start builds a backlog of
  # invocations (on firecracker every fire boots a microVM), which would then delay P3.
  cron="$(ta triggers create hello --name "lab-restart-cron-$stamp" --kind cron --schedule '*/5 * * * * *' \
    --timezone Asia/Tokyo --payload '{"name":"restart-cron"}' --missed-run skip --json | jq -r .id)"
  check restart.cron_created "trigger=$cron schedule='*/5 * * * * *' (kept enabled across the restart)" test -n "$cron" -a "$cron" != null
  sleep 6
  fires_before="$(cron_fires "$cron")"

  # async work accepted (durably) just before the restart: cpu-burn, one at a time, 2 s each, so
  # the queue still holds most of it when the gateway goes down.
  deploy cpu-burn "$GUEST_DIR/example-cpu-burn" --max-concurrency 1 --description "lab restart async" >/dev/null
  body="$DEMO_OUT/restart-async-body.json"
  printf '{"seconds":2}' >"$body"
  for i in $(seq 1 "$n"); do
    api "$TOKEN_A" POST "/v1/functions/$cbid:invokeAsync" "$body"
    [ "$HTTP_CODE" = 202 ] || break
    ids="$ids $(jqb .invocation_id)"
    accepted=$((accepted + 1))
  done
  check restart.async_accepted_202 "$accepted/$n accepted with HTTP 202 before the restart (committed before the answer)" \
    is "$accepted" "$n"

  # --- restart -------------------------------------------------------------------------------
  log_offset="$( { wc -c <"$GATEWAY_LOG"; } 2>/dev/null | tr -d ' ' || echo 0)"
  stop_gateway
  check restart.stopped "gateway process gone; GET /healthz -> $(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$API/healthz" || true) (000 = no answer)" \
    test -z "$(gateway_pid || true)"
  start_gateway
  check restart.started_healthy "every component healthy again after the restart (health table)" health_table quiet
  this_start="$DEMO_OUT/restart-gateway-start.log"
  tail -c +"$((log_offset + 1))" "$GATEWAY_LOG" >"$this_start" 2>/dev/null || true

  # the cron keeps its schedule across the restart; disable it as soon as that is visible
  sleep 11
  fires_after="$(cron_fires "$cron")"
  ta triggers update hello "$cron" --disable >&2 || true
  ta triggers delete hello "$cron" >&2 || true
  check restart.cron_fires_after_restart "accepted fires $fires_before -> $fires_after (the scheduler takes its lease again)" \
    test "${fires_after:-0}" -gt "${fires_before:-0}"

  # --- what survived -------------------------------------------------------------------------
  schema_after="$(sqlite_ro "$DATA_DIR/state.db" 'SELECT MAX(version) FROM schema_version' || true)"
  check restart.schema_version_unchanged "state.db schema_version $schema_before -> $schema_after (migrations are not re-applied)" \
    is "$schema_after" "$schema_before"
  fn_after="$(ta functions list --json | jq -r '(.items // .) | length')"
  capture ta functions invoke hello --payload '{"name":"restart"}' --json
  greeting_after="$(jq -r .greeting <<<"$OUT" 2>/dev/null)"
  check restart.cold_invoke_ok "exit=$RC message=$(jq -r .message <<<"$OUT" 2>/dev/null) (a new environment is started after the restart)" is "$RC" 0
  check restart.ledger_survives "functions $fn_before -> $fn_after, prod alias greeting $greeting_before -> $greeting_after (P1's rollback is still in effect)" \
    test "$fn_after" = "$fn_before" -a "$greeting_after" = "$greeting_before"
  capture ta functions logs --invocation "$inv"
  printf '%s\n' "$OUT" >"$DEMO_OUT/restart-logs-after.txt"
  logs_after="$(grep -c . "$DEMO_OUT/restart-logs-after.txt" || true)"
  check restart.logs_survive "invocation=$inv lines $logs_before -> $logs_after (logs.db)" \
    test "$logs_after" = "$logs_before" -a "$logs_before" != 0
  check restart.startup_reconcile "gateway log of this start has the startup reconcile" \
    grep -q 'startup reconcile finished' "$this_start"

  # accepted async work: every invocation reaches a terminal state, none is lost
  for id in $ids; do
    s="$(wait_status "$id" 120)"
    printf '%s %s\n' "$id" "$s" >>"$DEMO_OUT/restart-async-status.txt"
    case "$s" in succeeded | failed | cancelled) terminal=$((terminal + 1)) ;; *) unknown=$((unknown + 1)) ;; esac
    a="$(ta functions invocation "$id" --json | jq -r '.attempts | length' 2>/dev/null || echo 0)"
    [ "${a:-0}" -le "$attempts_max" ] || attempts_max="$a"
  done
  check restart.accepted_async_not_lost "$terminal/$n terminal after the restart, $unknown without an outcome (statuses in restart-async-status.txt)" \
    test "$terminal" = "$n" -a "$unknown" = 0
  check restart.async_attempts_bounded "most attempts on one invocation: $attempts_max (max_attempts 3; a retry after the restart is not a new invocation)" \
    test "$attempts_max" -ge 1 -a "$attempts_max" -le 3

  # usage: the async invocations are counted once each, and nothing is replayed for a function
  # that did not run across the restart
  deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    capture ta usage --group-by function --json
    cb_after="$(jq -r --arg f "$cbid" '[.lines[] | select(.function_id == $f) | .usage.invocations] | add // 0' <<<"$OUT" 2>/dev/null)"
    [ "$((cb_after - cb_before))" -lt "$n" ] || break
    sleep 2
  done
  printf '%s\n' "$OUT" >"$DEMO_OUT/restart-usage-after.json"
  ax_after="$(jq -r --arg f "$axid" '[.lines[] | select(.function_id == $f) | .usage.attempts] | add // 0' <<<"$OUT" 2>/dev/null)"
  check restart.async_counted_once "cpu-burn invocations (first attempts) $cb_before -> $cb_after, expected +$n for the $n async invocations" \
    is "$((cb_after - cb_before))" "$n"
  check restart.no_replay_for_idle_function "http-axum attempts $ax_before -> $ax_after (it did not run across the restart; the journal is not replayed into the ledger twice)" \
    is "$ax_after" "$ax_before"
  # leave no backlog behind: the disabled cron's fires must finish before P3 starts
  deadline=$((SECONDS + 180))
  while [ "$SECONDS" -lt "$deadline" ]; do
    pending="$(ta functions invocations hello --limit 100 --json 2>/dev/null |
      jq -r '[(.items // .)[] | select(.status != "succeeded" and .status != "failed" and .status != "cancelled")] | length' 2>/dev/null)"
    [ "${pending:-0}" != 0 ] || break
    sleep 2
  done
  check restart.cron_backlog_drained "hello invocations still running or queued after the cron was deleted: ${pending:-?}" \
    is "${pending:-1}" 0
  note "the restart is a clean SIGTERM (in-flight synchronous invocations are cancelled). Crash, DB, queue and worker failures are the failure matrix (docs/failure-matrix.md), not this demo."
}

# ---------------------------------------------------------------------------
# P3
# ---------------------------------------------------------------------------

demo_p3() {
  section "P3: invokeAsync -> retries -> dead letter -> redrive -> cron -> webhook -> usage -> budget (provider $PROVIDER)"
  local stamp fn fid inv status body dlq r deadline
  stamp="$(date -u +%H%M%S)$(random_hex 2)"
  if [ "$PROVIDER" = process ]; then
    fn=idempotent-async
    fid="$(ensure_function "$fn")"
    mkdir -p "$DEMO_DIR/effects"
    deploy "$fn" "$GUEST_DIR/example-idempotent-async" --env "IDEMPOTENT_ASYNC_DIR=$DEMO_DIR/effects" --max-concurrency 4 --description "lab async" >/dev/null
  else
    fn=hello
    fid="$(ensure_function "$fn")"
    note "firecracker: examples/idempotent-async needs the host file system, so the async demo uses hello ({\"fail\":true} for the dead letter)"
  fi

  # 1. async, inline input
  body="$DEMO_OUT/p3-async.json"
  if [ "$PROVIDER" = process ]; then printf '{"order_id":"lab-ok-%s"}' "$stamp" >"$body"; else printf '{"name":"async"}' >"$body"; fi
  api "$TOKEN_A" POST "/v1/functions/$fid:invokeAsync" "$body"
  inv="$(jqb .invocation_id)"
  check p3.async_accepted_202 "HTTP $HTTP_CODE invocation=$inv (committed before the 202)" is "$HTTP_CODE" 202
  status="$(wait_status "$inv" 60)"
  check p3.async_succeeded "status=$status (published to NATS JetStream, run by the dispatcher)" is "$status" succeeded

  # 2. async, input above inline_input_max_bytes (64 KiB) -> encrypted object store
  local objs_before objs_after
  objs_before="$(object_files | wc -l | tr -d ' ')"
  python3 - "$DEMO_OUT/p3-async-large.json" "$PROVIDER" "$stamp" <<'PY'
import json, sys
path, provider, stamp = sys.argv[1:4]
doc = {"order_id": f"lab-big-{stamp}"} if provider == "process" else {"name": "big"}
doc["pad"] = "x" * (100 * 1024)
open(path, "w").write(json.dumps(doc))
PY
  api "$TOKEN_A" POST "/v1/functions/$fid:invokeAsync" "$DEMO_OUT/p3-async-large.json"
  inv="$(jqb .invocation_id)"
  status="$(wait_status "$inv" 60)"
  objs_after="$(object_files | wc -l | tr -d ' ')"
  check p3.async_large_input_object_store "HTTP $HTTP_CODE status=$status objects under data/objects: $objs_before -> $objs_after (AES-256-GCM, lab key)" \
    test "$status" = succeeded -a "$objs_after" -gt "$objs_before"
  if [ -n "$(object_files)" ]; then
    # grep: 0 found (plaintext leaked), 1 not found, 2 unreadable (counted as a failure, never as clean)
    r=0
    ${LAB_SUDO:+$LAB_SUDO }grep -rqa 'xxxxxxxxxxxxxxxxxxxxxxxx' "$OBJECTS_ROOT" 2>/dev/null || r=$?
    if [ "$r" = 1 ]; then r=0; else r=1; fi
    check p3.object_store_ciphertext "the 100 KiB plaintext pad is not readable in data/objects" is "$r" 0
  fi

  # 3. retries -> dead letter -> redrive
  if [ "$PROVIDER" = process ]; then printf '{"order_id":"lab-dlq-%s","fail_first":3}' "$stamp" >"$body"; else printf '{"fail":true}' >"$body"; fi
  api "$TOKEN_A" POST "/v1/functions/$fid:invokeAsync" "$body"
  inv="$(jqb .invocation_id)"
  status="$(wait_status "$inv" 90)"
  api "$TOKEN_A" GET "/v1/invocations/$inv"
  printf '%s\n' "$HTTP_BODY" >"$DEMO_OUT/p3-dead-lettered-invocation.json"
  dlq="$(jqb .dispatch.dead_letter_id)"
  check p3.retries_then_dead_letter "status=$status attempts=$(jqb '.attempts | length') (max_attempts 3) dead_letter=$dlq" \
    test "$status" = failed -a "$(jqb '.attempts | length')" = 3 -a -n "$dlq" -a "$dlq" != null
  capture ta dead-letters list "$fn" --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p3-dead-letters.json"
  check p3.dead_letter_listed "reason=$(jq -r --arg d "$dlq" '(.items // .)[] | select(.id == $d) | .reason' <<<"$OUT" 2>/dev/null)" \
    is "$(jq -r --arg d "$dlq" '(.items // .)[] | select(.id == $d) | .reason' <<<"$OUT" 2>/dev/null)" attempts_exhausted
  capture ta dead-letters redrive "$dlq" --reason "lab demo without the role" --json
  check p3.redrive_needs_role "deploy+invoke token: exit=$RC code=$(jq -r .error.code <<<"$OUT" 2>/dev/null)" is "$(jq -r .error.code <<<"$OUT" 2>/dev/null)" forbidden
  capture toncall dead-letters redrive "$dlq" --reason "lab demo: downstream fixed" --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p3-redrive.json"
  inv="$(jq -r .invocation.invocation_id <<<"$OUT" 2>/dev/null)"
  check p3.redrive_accepted "exit=$RC new invocation=$inv requested_by=$(jq -r .redrive.requested_by <<<"$OUT" 2>/dev/null)" \
    is "$RC/$(jq -r .redrive.requested_by <<<"$OUT" 2>/dev/null)" "0/lab-a-oncall"
  status="$(wait_status "$inv" 60)"
  if [ "$PROVIDER" = process ]; then
    check p3.redrive_succeeded "status=$status (4th execution passes fail_first=3; side effect applied once)" \
      test "$status" = succeeded -a -f "$DEMO_DIR/effects/effects/lab-dlq-$stamp.json"
  else
    check p3.redrive_ran "status=$status (hello {\"fail\":true} fails again by design; the redrive itself is what is shown)" \
      test "$status" = failed -o "$status" = succeeded
  fi

  # 4. cron
  local cron fires accepted
  cron="$(ta triggers create hello --name "lab-cron-$stamp" --kind cron --schedule '*/2 * * * * *' \
    --timezone Asia/Tokyo --payload '{"name":"cron"}' --missed-run skip --json | jq -r .id)"
  check p3.cron_created "trigger=$cron schedule='*/2 * * * * *' (6 fields: every 2 s)" test -n "$cron" -a "$cron" != null
  sleep 7
  ta triggers update hello "$cron" --disable >&2 || true
  capture ta triggers fires hello "$cron" --limit 20 --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p3-cron-fires.json"
  accepted="$(jq -r '[(.items // .)[] | select(.outcome == "accepted")] | length' <<<"$OUT" 2>/dev/null)"
  check p3.cron_fired "accepted fires in ~7 s: $accepted (then disabled)" ge "$accepted" 2
  fires="$(jq -r '[(.items // .)[] | .invocation_id // empty] | .[0]' <<<"$OUT" 2>/dev/null)"
  status="$(wait_status "$fires" 60)"
  check p3.cron_fire_ran "first fire invocation=$fires status=$status" is "$status" succeeded
  ta triggers delete hello "$cron" >&2 || true

  # 5. signed webhook
  local hook now sig
  capture ta triggers create hello --name "lab-hook-$stamp" --kind webhook --max-body-bytes 4096 --json
  hook="$(jq -r .id <<<"$OUT" 2>/dev/null)"
  LAB_WEBHOOK_SECRET="$(jq -r .secret <<<"$OUT" 2>/dev/null)"
  export LAB_WEBHOOK_SECRET
  OUT=""
  check p3.webhook_created "trigger=$hook secret shown once (kept in memory only, never written)" test -n "$hook" -a "${LAB_WEBHOOK_SECRET#whsec_}" != "$LAB_WEBHOOK_SECRET"
  printf '{"name":"webhook"}' >"$DEMO_OUT/p3-webhook-body.json"
  now="$(date +%s)"
  sig="$(TSLS_API_URL="$API" "$TSLS_BIN" triggers webhook-sign --secret-env LAB_WEBHOOK_SECRET --timestamp "$now" --body-file "$DEMO_OUT/p3-webhook-body.json" --json | jq -r .signature)"
  echo "+ tsls triggers webhook-sign --secret-env LAB_WEBHOOK_SECRET --timestamp $now --body-file p3-webhook-body.json"
  echo "+ curl -X POST $API/v1/hooks/$hook (x-tachyon-webhook-timestamp, x-tachyon-webhook-signature, x-tachyon-webhook-id)"
  HTTP_CODE="$(curl -s -o "$DEMO_OUT/p3-webhook-response.json" -w '%{http_code}' --max-time 30 -X POST -H 'content-type: application/json' \
    -H "x-tachyon-webhook-timestamp: $now" -H "x-tachyon-webhook-signature: $sig" -H "x-tachyon-webhook-id: lab-evt-$stamp" \
    --data-binary "@$DEMO_OUT/p3-webhook-body.json" "$API/v1/hooks/$hook" || true)"
  check p3.webhook_signed_202 "HTTP $HTTP_CODE" is "$HTTP_CODE" 202
  HTTP_CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 -X POST -H 'content-type: application/json' \
    -H "x-tachyon-webhook-timestamp: $now" -H "x-tachyon-webhook-signature: v1=$(random_hex 32)" -H "x-tachyon-webhook-id: lab-evt-bad-$stamp" \
    --data-binary "@$DEMO_OUT/p3-webhook-body.json" "$API/v1/hooks/$hook" || true)"
  check p3.webhook_bad_signature_401 "HTTP $HTTP_CODE (no invocation)" is "$HTTP_CODE" 401
  unset LAB_WEBHOOK_SECRET
  sleep 1
  capture ta triggers fires hello "$hook" --json
  inv="$(jq -r '[(.items // .)[] | select(.outcome == "accepted") | .invocation_id] | .[0]' <<<"$OUT" 2>/dev/null)"
  status="$(wait_status "$inv" 60)"
  check p3.webhook_fire_ran "invocation=$inv status=$status" is "$status" succeeded
  ta triggers delete hello "$hook" >&2 || true

  # 6. usage (host-measured, provisional, billing disabled)
  sleep 2
  capture ta usage --group-by function --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p3-usage.json"
  check p3.usage_report "lines=$(jq -r '.lines | length' <<<"$OUT" 2>/dev/null) attempts=$(jq -r '[.lines[].usage.attempts] | add' <<<"$OUT" 2>/dev/null) retries=$(jq -r '[.lines[].usage.retries] | add' <<<"$OUT" 2>/dev/null) provisional=$(jq -r .provisional <<<"$OUT" 2>/dev/null) billing_enabled=$(jq -r .billing_enabled <<<"$OUT" 2>/dev/null)" \
    is "$(jq -r '(.lines | length > 0) and ([.lines[].usage.attempts] | add > 0) and .provisional == true and .billing_enabled == false' <<<"$OUT" 2>/dev/null)" true
  ta usage --group-by function >&2 || true

  # 7. budget stop -> release (file re-read by the control plane at every publication)
  local cb
  cb="$(ensure_function cpu-burn)"
  cat >"$BUDGET_FILE" <<EOF
# lab demo $stamp: stop cpu-burn ($cb) of tenant A; everything else keeps the default.
[default_tenant]
hard_limit_micros = 1000000000000

[[tenants]]
tenant_id = "$TENANT_A"
hard_limit_micros = 1000000000000

[[tenants.functions]]
function_id = "$cb"
hard_limit_micros = 1
EOF
  echo "+ wrote $BUDGET_FILE (cpu-burn hard_limit_micros = 1)"
  local code=""
  deadline=$((SECONDS + 45))
  while [ "$SECONDS" -lt "$deadline" ]; do
    capture ta functions invoke cpu-burn --payload '{"seconds":0}' --json
    code="$(jq -r .error.code <<<"$OUT" 2>/dev/null)"
    [ "$code" != budget_exhausted ] || break
    sleep 1
  done
  printf '%s\n' "$OUT" >"$DEMO_OUT/p3-budget-refusal.json"
  check p3.budget_hard_limit_stops "exit=$RC code=$code reason=$(jq -r .error.reason <<<"$OUT" 2>/dev/null)" is "$code" budget_exhausted
  capture ta budget --json
  printf '%s\n' "$OUT" >"$DEMO_OUT/p3-budget.json"
  ta budget >&2 || true
  capture ta functions invoke hello --payload '{"name":"other"}' --json
  check p3.budget_is_per_function "hello still admitted: exit=$RC" is "$RC" 0
  write_budgets_default
  echo "+ restored $BUDGET_FILE (default only)"
  deadline=$((SECONDS + 45))
  while [ "$SECONDS" -lt "$deadline" ]; do
    capture ta functions invoke cpu-burn --payload '{"seconds":0}' --json
    [ "$RC" != 0 ] || break
    sleep 1
  done
  check p3.budget_released "exit=$RC after the limit was removed" is "$RC" 0
  note "budgets and usage are provisional (dev price table); billing is disabled and nothing is charged. Local only: no cloud spend."
}

# ---------------------------------------------------------------------------
# secret scan + main
# ---------------------------------------------------------------------------

# object_files -> files under the object root (root-owned in a privileged firecracker lab)
object_files() { { ${LAB_SUDO:+$LAB_SUDO }find "$OBJECTS_ROOT" -type f 2>/dev/null || true; }; }

demo_secret_scan() {
  section "secret values absent from logs, demo outputs and the ledger"
  local v name hits=0 checked=0 target rc
  for name in TOKEN_A TOKEN_A_ONCALL TOKEN_B METRICS_TOKEN DEMO_SECRET_VALUE; do
    v="$(eval "printf '%s' \"\$$name\"")"
    [ -n "$v" ] || continue
    for target in "$LOG_DIR" "$DEMO_DIR" "$DATA_DIR/state.db" "$DATA_DIR/state.db-wal"; do
      [ -e "$target" ] || continue
      checked=$((checked + 1))
      # 0 found, 1 not found, 2 unreadable: an unreadable file is a hit, never a clean result
      # (a privileged firecracker lab's state.db is root-owned 0600; LAB_SUDO reads it).
      rc=0
      ${LAB_SUDO:+$LAB_SUDO }grep -rqaF -- "$v" "$target" 2>/dev/null || rc=$?
      if [ "$rc" != 1 ]; then
        echo "  $name found in (or could not read) $target (grep exit $rc)"
        hits=$((hits + 1))
      fi
    done
  done
  check secrets.not_leaked "$checked locations x 5 values checked, $hits hits (config/ and secrets/ hold them by design)" is "$hits" 0
}

demo_main() {
  local phase="$1" p
  lab_load_tokens
  DEMO_OUT="$DEMO_DIR/$(lab_stamp)-$phase"
  mkdir -p "$DEMO_OUT"
  DEMO_RESULTS="$DEMO_OUT/results.txt"
  : >"$DEMO_RESULTS"
  echo "demo $phase: lab $LAB_ID provider $PROVIDER api $API outputs $DEMO_OUT"
  case "$phase" in
    all) for p in p1 p2 restart p3; do "demo_$p"; done ;;
    *) "demo_$phase" ;;
  esac
  demo_secret_scan
  echo
  echo "results ($DEMO_RESULTS):"
  cat "$DEMO_RESULTS"
  echo
  printf '%s checks passed, %s failed\n' "$(grep -c '^PASS' "$DEMO_RESULTS" || true)" "$(grep -c '^FAIL' "$DEMO_RESULTS" || true)"
  [ "$DEMO_FAILED" = 0 ] || return 1
}

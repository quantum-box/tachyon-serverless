#!/bin/bash
# PLT-4649 final acceptance: every supplementary run, in order, on this Lima VM.
D="$(cd "$(dirname "$0")" && pwd)"
P=~/plt4649/progress.txt
step() { # step NAME CMD...
  printf '[%s] START %s\n' "$(date -u +%FT%TZ)" "$1" >> "$P"
  shift
  "$@" > /dev/null 2>&1
  printf '[%s] DONE  %s exit=%s\n' "$(date -u +%FT%TZ)" "$STEP" "$?" >> "$P"
}
run() { # run NAME CMD...
  local name="$1"; shift
  printf '[%s] START %s\n' "$(date -u +%FT%TZ)" "$name" >> "$P"
  "$@" > ~/plt4649/runs/"$name".stdout 2>&1
  printf '[%s] DONE  %s exit=%s\n' "$(date -u +%FT%TZ)" "$name" "$?" >> "$P"
}
: > "$P"
run e2e-demo   bash "$D/run-e2e-demo.sh"
run isolation  bash "$D/run-isolation.sh"
run zero-scale bash "$D/run-zero-scale.sh"
run warm       bash "$D/run-warm.sh"
run usage      bash "$D/run-usage.sh"
run chaos-process env PROVIDER=process bash "$D/run-chaos.sh"
run chaos-firecracker env PROVIDER=firecracker ONLY=baseline,sync_gateway_kill,stale_owner_sync_lease,stale_owner_frozen_in_transaction,worker_bridge_kill_sync,worker_bridge_kill_async,worker_user_process_kill_sync,orphan_recovery_after_crash RETRIES=1 bash "$D/run-chaos.sh"
run bench      bash "$D/run-bench.sh"
printf '[%s] ALL DONE\n' "$(date -u +%FT%TZ)" >> "$P"

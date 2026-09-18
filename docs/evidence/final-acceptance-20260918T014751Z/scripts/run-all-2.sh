#!/bin/bash
# PLT-4649 final acceptance, second batch: the runs that needed the full build, plus the
# re-run of zero-scale on the fixed helper.
D="$(cd "$(dirname "$0")" && pwd)"
P=~/plt4649/progress2.txt
run() {
  local name="$1"; shift
  printf '[%s] START %s\n' "$(date -u +%FT%TZ)" "$name" >> "$P"
  "$@" > ~/plt4649/runs/"$name".stdout 2>&1
  printf '[%s] DONE  %s exit=%s\n' "$(date -u +%FT%TZ)" "$name" "$?" >> "$P"
}
: > "$P"
run isolation-2  bash "$D/run-isolation.sh"
run zero-scale-2 bash "$D/run-zero-scale.sh"
run queue        bash "$D/run-queue.sh"
run chaos-process env PROVIDER=process bash "$D/run-chaos.sh"
run chaos-firecracker env PROVIDER=firecracker ONLY=baseline,sync_gateway_kill,stale_owner_sync_lease,stale_owner_frozen_in_transaction,worker_bridge_kill_sync,worker_bridge_kill_async,worker_user_process_kill_sync,orphan_recovery_after_crash RETRIES=1 bash "$D/run-chaos.sh"
run bench        bash "$D/run-bench.sh"
printf '[%s] ALL DONE\n' "$(date -u +%FT%TZ)" >> "$P"

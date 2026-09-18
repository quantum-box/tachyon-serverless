#!/bin/bash
# PLT-4649 final acceptance, third batch: the process-provider failure matrix (it needs the
# debug-built examples) and the firecracker scenario that replaces the host-pid one.
D="$(cd "$(dirname "$0")" && pwd)"
P=~/plt4649/progress3.txt
run() {
  local name="$1"; shift
  printf '[%s] START %s\n' "$(date -u +%FT%TZ)" "$name" >> "$P"
  "$@" > ~/plt4649/runs/"$name".stdout 2>&1
  printf '[%s] DONE  %s exit=%s\n' "$(date -u +%FT%TZ)" "$name" "$?" >> "$P"
}
: > "$P"
run build-debug-examples bash -c 'cd ~/lab && source ~/.cargo/env 2>/dev/null; cargo build -p example-hello -p example-cpu-burn -p example-idempotent-async -p example-http-axum -p example-isolation-probe'
run chaos-firecracker-oom env PROVIDER=firecracker ONLY=worker_user_process_oom_sync RETRIES=1 RUNDIR=chaos-firecracker-oom bash "$D/run-chaos.sh"
run chaos-process env PROVIDER=process bash "$D/run-chaos.sh"
printf '[%s] ALL DONE\n' "$(date -u +%FT%TZ)" >> "$P"

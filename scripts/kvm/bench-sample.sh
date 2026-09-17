#!/usr/bin/env bash
# scripts/kvm/bench-sample.sh - one host-side snapshot of what the live environments cost (PLT-4647).
# Run as root by scripts/kvm/bench.sh (another uid's /proc/<pid>/status, the jail and the env dirs
# need it). Prints one JSON object on stdout and changes nothing.
#
# Usage:
#   scripts/kvm/bench-sample.sh CGROUP_PARENT WORKDIR JAIL_BASE [GATEWAY_PID]
#
# The object carries:
#   - host: MemTotal / MemAvailable / Cached (KiB), CLK_TCK, and the time of the sample;
#   - gateway: pid, VmRSS / VmHWM (KiB), utime + stime ticks (when GATEWAY_PID is given);
#   - environments[]: for every cgroup under CGROUP_PARENT, its memory.current / memory.peak /
#     memory.max, the anon / file / kernel lines of memory.stat, cpu.stat usage_usec, and for each
#     firecracker process in it VmRSS / VmHWM / threads / state / ticks; plus the bytes the
#     environment holds on the host disk: WORKDIR/<env> (function drive, scratch drive, logs) and
#     its jail under JAIL_BASE (hard links to the same inodes are counted once by du).
set -uo pipefail

CG_PARENT="$1"
WORKDIR="$2"
JAIL_BASE="$3"
GATEWAY_PID="${4:-}"

read_or() { cat "$1" 2>/dev/null || printf '%s' "$2"; }
stat_kv() { awk -v k="$2" '$1 == k { print $2 }' "$1" 2>/dev/null; }
status_kib() { awk -v k="$2:" '$1 == k { print $2 }' "/proc/$1/status" 2>/dev/null; }
ticks() { awk '{ print $14 + $15 }' "/proc/$1/stat" 2>/dev/null; }
num_or_null() { if [ -n "$1" ]; then printf '%s' "$1"; else printf 'null'; fi; }

proc_json() { # proc_json PID
  local pid="$1"
  printf '{"pid":%s,"comm":"%s","state":"%s","vm_rss_kib":%s,"vm_hwm_kib":%s,"threads":%s,"cpu_ticks":%s}' \
    "$pid" "$(read_or "/proc/$pid/comm" '' | tr -d '\n"')" \
    "$(awk '/^State:/ { print $2 }' "/proc/$pid/status" 2>/dev/null)" \
    "$(num_or_null "$(status_kib "$pid" VmRSS)")" "$(num_or_null "$(status_kib "$pid" VmHWM)")" \
    "$(num_or_null "$(status_kib "$pid" Threads)")" "$(num_or_null "$(ticks "$pid")")"
}

now_ms() { local ns; ns="$(date +%s%N)"; echo $(( ns / 1000000 )); }

envs="[]"
for dir in "$CG_PARENT"/*/; do
  [ -d "$dir" ] || continue
  dir="${dir%/}"
  env="${dir##*/}"
  procs="[]"
  while read -r pid; do
    [ -n "$pid" ] || continue
    procs="$(printf '%s' "$procs" | jq -c --argjson p "$(proc_json "$pid")" '. + [$p]')"
  done < <(cat "$dir/cgroup.procs" 2>/dev/null)
  env_dir_bytes="$(du -sb "$WORKDIR/$env" 2>/dev/null | awk '{ print $1 }')"
  jail_dirs="$(find "$JAIL_BASE" -mindepth 2 -maxdepth 2 -name "*${env#env_}*" 2>/dev/null | head -n 1)"
  jail_bytes=""
  both_bytes=""
  if [ -n "$jail_dirs" ]; then
    jail_bytes="$(du -sb "$jail_dirs" 2>/dev/null | awk '{ print $1 }')"
    both_bytes="$(du -sbc "$WORKDIR/$env" "$jail_dirs" 2>/dev/null | awk '$2 == "total" { print $1 }')"
  fi
  envs="$(printf '%s' "$envs" | jq -c \
    --arg env "$env" --argjson procs "$procs" \
    --arg mc "$(read_or "$dir/memory.current" '')" --arg mp "$(read_or "$dir/memory.peak" '')" \
    --arg mm "$(read_or "$dir/memory.max" '')" \
    --arg anon "$(stat_kv "$dir/memory.stat" anon)" --arg file "$(stat_kv "$dir/memory.stat" file)" \
    --arg kernel "$(stat_kv "$dir/memory.stat" kernel)" \
    --arg usage "$(stat_kv "$dir/cpu.stat" usage_usec)" \
    --arg throttled "$(stat_kv "$dir/cpu.stat" throttled_usec)" \
    --arg cpu_max "$(read_or "$dir/cpu.max" '' | tr -d '\n')" \
    --arg env_dir_bytes "${env_dir_bytes:-}" --arg jail_dir "${jail_dirs:-}" \
    --arg jail_bytes "${jail_bytes:-}" --arg both_bytes "${both_bytes:-}" '
    def n: rtrimstr("\n") | if . == "" then null elif . == "max" then "max" else tonumber end;
    . + [{env: $env, processes: $procs,
          cgroup: {memory_current_bytes: ($mc | n), memory_peak_bytes: ($mp | n), memory_max: ($mm | n),
                   memory_stat: {anon: ($anon | n), file: ($file | n), kernel: ($kernel | n)},
                   cpu_usage_usec: ($usage | n), cpu_throttled_usec: ($throttled | n), cpu_max: $cpu_max},
          disk: {env_dir_bytes: ($env_dir_bytes | n), jail_dir: (if $jail_dir == "" then null else $jail_dir end),
                 jail_dir_bytes: ($jail_bytes | n), env_and_jail_unique_bytes: ($both_bytes | n)}}]')"
done

gateway="null"
if [ -n "$GATEWAY_PID" ] && [ -r "/proc/$GATEWAY_PID/status" ]; then
  gateway="$(proc_json "$GATEWAY_PID")"
fi

jq -nc --argjson ts "$(now_ms)" --argjson envs "$envs" --argjson gateway "$gateway" \
  --argjson clk "$(getconf CLK_TCK)" \
  --argjson mem_total "$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)" \
  --argjson mem_avail "$(awk '/^MemAvailable:/ { print $2 }' /proc/meminfo)" \
  --argjson cached "$(awk '/^Cached:/ { print $2 }' /proc/meminfo)" \
  '{ts_ms: $ts, host: {mem_total_kib: $mem_total, mem_available_kib: $mem_avail, cached_kib: $cached, clk_tck: $clk},
    gateway: $gateway, environments: $envs}'

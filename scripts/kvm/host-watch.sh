#!/usr/bin/env bash
# scripts/kvm/host-watch.sh - sample the host side of every Firecracker VMM while a measurement
# runs (PLT-4622). Started in the background by scripts/kvm/measure-isolation.sh, as root (reading
# another uid's /proc/<pid>/root and namespaces needs it).
#
# Usage:
#   scripts/kvm/host-watch.sh CGROUP_PARENT OUT_DIR STOP_FILE [INTERVAL_S]
#
# Until STOP_FILE exists, every INTERVAL_S (default 0.5) seconds:
#   - appends one line per environment cgroup under CGROUP_PARENT to OUT_DIR/cgroup-samples.tsv
#     (cpu.stat usage / throttling, memory.current / memory.peak, pids.current, cpu.max,
#     memory.max);
#   - once per environment, as soon as its VMM has a vCPU thread, appends one JSON object to
#     OUT_DIR/vmm-isolation.jsonl describing that process: uid / gid, effective capabilities,
#     PID and mount namespaces against the host's, whether its root is a chroot (the API socket
#     is at its `/` and the host's /etc/passwd is not), /proc/<pid>/cgroup, the cgroup's member
#     list and limits, and the seccomp mode of every thread.
set -uo pipefail

CG_PARENT="$1"
OUT_DIR="$2"
STOP_FILE="$3"
INTERVAL="${4:-0.5}"

SAMPLES="$OUT_DIR/cgroup-samples.tsv"
VMMS="$OUT_DIR/vmm-isolation.jsonl"
HOST_PID_NS="$(readlink /proc/1/ns/pid)"
HOST_MNT_NS="$(readlink /proc/1/ns/mnt)"
SEEN=" "

printf 'ts_ms\tenv\tusage_usec\tthrottled_usec\tnr_throttled\tmemory_current\tmemory_peak\tpids_current\tcpu_max\tmemory_max\n' > "$SAMPLES"
: > "$VMMS"

# Milliseconds since the epoch. `%3N` is not honoured by every date(1) (uutils prints all nine
# digits), so nanoseconds are divided here.
now_ms() { local ns; ns="$(date +%s%N)"; echo $(( ns / 1000000 )); }

stat_field() { awk -v k="$2" '$1 == k { print $2 }' "$1" 2>/dev/null; }
read_or() { cat "$1" 2>/dev/null || printf '%s' "$2"; }

# describe_vmm ENV CGROUP_DIR PID -> one JSON object
describe_vmm() {
  local env="$1" dir="$2" pid="$3" threads t chroot="false"
  threads="$(for t in /proc/"$pid"/task/*; do
      printf '%s\t%s\n' "$(read_or "$t/comm" '?')" "$(awk '/^Seccomp:/ { print $2 }' "$t/status" 2>/dev/null)"
    done | jq -R -s 'split("\n") | map(select(length > 0) | split("\t") | {comm: .[0], seccomp: (.[1] | tonumber? // null)})')"
  if [ -S "/proc/$pid/root/fc.sock" ] && [ ! -e "/proc/$pid/root/etc/passwd" ]; then
    chroot="true"
  fi
  jq -nc --arg env "$env" --argjson pid "$pid" \
    --arg comm "$(read_or "/proc/$pid/comm" '')" \
    --arg uid "$(awk '/^Uid:/ { print $2 }' "/proc/$pid/status")" \
    --arg gid "$(awk '/^Gid:/ { print $2 }' "/proc/$pid/status")" \
    --arg cap_eff "$(awk '/^CapEff:/ { print $2 }' "/proc/$pid/status")" \
    --arg nspid "$(awk '/^NSpid:/ { $1 = ""; sub(/^ /, ""); print }' "/proc/$pid/status")" \
    --arg pid_ns "$(readlink "/proc/$pid/ns/pid")" --arg mnt_ns "$(readlink "/proc/$pid/ns/mnt")" \
    --arg host_pid_ns "$HOST_PID_NS" --arg host_mnt_ns "$HOST_MNT_NS" \
    --argjson chroot "$chroot" \
    --arg cgroup "$(read_or "/proc/$pid/cgroup" '')" \
    --arg procs "$(tr '\n' ' ' < "$dir/cgroup.procs" 2>/dev/null)" \
    --arg cpu_max "$(read_or "$dir/cpu.max" '')" --arg memory_max "$(read_or "$dir/memory.max" '')" \
    --arg pids_max "$(read_or "$dir/pids.max" '')" \
    --arg cmdline "$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)" \
    --argjson threads "$threads" --arg ts "$(now_ms)" \
    '{ts_ms: ($ts | tonumber), env: $env, pid: $pid, comm: $comm, uid: ($uid | tonumber), gid: ($gid | tonumber),
      cap_eff: $cap_eff, nspid: $nspid,
      new_pid_ns: ($pid_ns != $host_pid_ns), new_mnt_ns: ($mnt_ns != $host_mnt_ns), chroot: $chroot,
      cgroup: ($cgroup | rtrimstr("\n")), cgroup_procs: ($procs | rtrimstr(" ")),
      cpu_max: ($cpu_max | rtrimstr("\n")), memory_max: ($memory_max | rtrimstr("\n")),
      pids_max: ($pids_max | rtrimstr("\n")), cmdline: $cmdline, threads: $threads}'
}

while [ ! -e "$STOP_FILE" ]; do
  for dir in "$CG_PARENT"/*/; do
    [ -d "$dir" ] || continue
    dir="${dir%/}"
    env="${dir##*/}"
    # Taken right before this cgroup's counters are read, so rates stay exact.
    ts="$(now_ms)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$ts" "$env" \
      "$(stat_field "$dir/cpu.stat" usage_usec)" "$(stat_field "$dir/cpu.stat" throttled_usec)" \
      "$(stat_field "$dir/cpu.stat" nr_throttled)" "$(read_or "$dir/memory.current" '')" \
      "$(read_or "$dir/memory.peak" '')" "$(read_or "$dir/pids.current" '')" \
      "$(read_or "$dir/cpu.max" '' | tr ' ' '/')" "$(read_or "$dir/memory.max" '')" >> "$SAMPLES"
    case "$SEEN" in *" $env "*) continue ;; esac
    while read -r pid; do
      [ "$(read_or "/proc/$pid/comm" '')" = "firecracker" ] || continue
      # Wait for the vCPU threads: the seccomp filters are installed as the VM starts.
      grep -qs '^fc_vcpu' /proc/"$pid"/task/*/comm || continue
      sleep 0.3
      if describe_vmm "$env" "$dir" "$pid" >> "$VMMS"; then
        SEEN="$SEEN$env "
      fi
    done < <(cat "$dir/cgroup.procs" 2>/dev/null)
  done
  sleep "$INTERVAL"
done

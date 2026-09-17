PLT-4631 (leases / fencing, PR #13) on Firecracker - KVM verification 2026-09-17

Source: origin/main b5a7e54. Host: Lima VM (nested virtualization), Firecracker / jailer v1.17.0.
macOS 1-min load average 17.0 -> 16.2 during the run (other agents' builds).

What was run (run-fencing.sh.txt, summary.txt is its output):
- Two gateways as root on ONE data_dir (state.db) and ONE provider workdir (.kvm/run), each a
  copy of config/gateway.firecracker.toml (production, jailer, cgroup required) with a different
  listen port (A :18485, B :18486, so different dispatcher instances) plus
  [pool] enabled = true, idle_ttl_seconds = 300 and
  [dispatcher] lease_ttl_seconds = 6, heartbeat_interval_seconds = 2, max_clock_skew_ms = 500.
- A: deploy examples/hello, invoke once (cold) -> the environment is parked in A's pool as a
  paused microVM (capacity idle 1; VMM pid 113735 in /sys/fs/cgroup/tachyon/<env>).
- SIGSTOP gateway A (its heartbeat stops; the process and its pid stay alive).
- Start B on the same data_dir: startup reconcile "found=1 ... foreign=1" (A is still live, its
  environment is not touched).
- ~8 s later (lease 6 s + skew 0.5 s, on B's 2 s heartbeat) B logs "reclaimed the work of
  dispatchers that lost their lease ... fenced=1", then the provider terminate of A's
  environment ("cgroup stats at teardown", "environment terminated ... reason=Reconcile
  was_running=true", gateway-b.log line 10) and only then "fenced environment terminated and
  settled" (line 11). The 0.2 s poller (timeline.txt) saw the VMM, its cgroup and its env dir
  alive at 07:06:11.134 and gone at 07:06:11.345; the settle log is 07:06:11.208.
- Invoke through B: 200, start_kind cold on a NEW environment (never A's).
- SIGCONT A (and its sudo parent): A logs "dispatcher lease lost: refusing new invocations",
  /readyz 503, POST invoke 503 provider_unavailable "this gateway lost its dispatcher lease".
- Stop B, stop A (both exit 0); no VMM / jailer / gateway process, cgroup, jail, tap or tachyon
  nft table left (host.txt).
Result: 11/11 PASS (summary.txt).

Not shown on KVM: an in-flight (busy) attempt whose lease expires mid-handler (the fenced
environment here was idle in the pool); a provider terminate that fails and keeps the
environment fenced; clock skew between hosts. Timing is from a single nested host.

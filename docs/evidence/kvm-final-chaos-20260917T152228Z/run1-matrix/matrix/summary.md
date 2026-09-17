# PLT-4646 failure matrix — 20260917T152228Z

commit `unknown`, Linux 7.0.0-31-generic aarch64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider firecracker (jailed microVMs, host cgroup required, warm pool true), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 103 | 8460 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 6542 | 13676 | FAIL | fail,fail | cv.usage_from_cgroup_accounting |
| worker_bridge_kill_sync | SIGKILL the Firecracker VMM (vsock to the guest bridge lost) of a running sync invocation | 131 | 210 | pass | pass |  |
| worker_bridge_kill_async | SIGKILL the Firecracker VMM of a running async invocation | 9 | 6826 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 155 | 7388 | FAIL | fail,fail | cv.usage_from_cgroup_accounting |
| worker_user_process_oom_sync | user process allocates 512 MiB in a 128 MiB guest (guest OOM killer; firecracker only) | 26032 | - | FAIL | fail,fail | harness.scenario_completed |

# PLT-4646 failure matrix — 20260917T154351Z

commit `40a3c4ac148bfc7e8294a8ce7c00769667ecc6b0`, Linux 7.0.0-31-generic aarch64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider firecracker (jailed microVMs, host cgroup required, warm pool true), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 371 | 22845 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 153265 | 11533 | FAIL | fail,fail | reclaim.b_settles_as_outcome_unknown, reclaim.not_before_lease_expiry_plus_skew, reclaim.fenced_environment_terminated, fencing.late_completion_does_not_overwrite, fencing.b_keeps_serving |
| worker_bridge_kill_sync | SIGKILL the Firecracker VMM (vsock to the guest bridge lost) of a running sync invocation | 81 | 71 | pass | pass |  |
| worker_bridge_kill_async | SIGKILL the Firecracker VMM of a running async invocation | 9 | 6489 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 19 | 26366 | pass | pass |  |
| worker_user_process_oom_sync | user process allocates 512 MiB in a 128 MiB guest (guest OOM killer; firecracker only) | 13382 | 67 | pass | pass |  |

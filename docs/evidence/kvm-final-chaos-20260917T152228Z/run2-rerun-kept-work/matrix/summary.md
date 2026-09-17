# PLT-4646 failure matrix — 20260917T153644Z

commit `360dbcb1100976336bf02ccd3a0746a83b219628`, Linux 7.0.0-31-generic aarch64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider firecracker (jailed microVMs, host cgroup required, warm pool true), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 7871 | 13970 | FAIL | fail | cv.usage_from_cgroup_accounting |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 115 | 8671 | FAIL | fail | cv.usage_from_cgroup_accounting |
| worker_user_process_oom_sync | user process allocates 512 MiB in a 128 MiB guest (guest OOM killer; firecracker only) | 27313 | 237 | pass | pass |  |

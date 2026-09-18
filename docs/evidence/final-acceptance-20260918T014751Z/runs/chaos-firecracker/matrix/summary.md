# PLT-4646 failure matrix — 20260918T025322Z

commit `bbd650e13f2a421d7cfde1807ef322cfd166e5d9`, Linux 7.0.0-31-generic aarch64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider firecracker (jailed microVMs, host cgroup required, warm pool true), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 233 | 9568 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 6732 | 14075 | pass | pass |  |
| stale_owner_frozen_in_transaction | gateway A stops itself (SIGSTOP) between BEGIN IMMEDIATE and COMMIT of a state.db write, past both leases; B on the same data_dir; SIGCONT A | 35621 | 12324 | FLAKY (pass on attempt 2) | fail,pass |  |
| worker_bridge_kill_sync | SIGKILL the Firecracker VMM (vsock to the guest bridge lost) of a running sync invocation | 81 | 211 | pass | pass |  |
| worker_user_process_kill_sync | SIGKILL the user process of a running sync invocation | - | - | FAIL | fail,fail | harness.scenario_completed |
| worker_bridge_kill_async | SIGKILL the Firecracker VMM of a running async invocation | 11 | 6772 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 131 | 16361 | pass | pass |  |

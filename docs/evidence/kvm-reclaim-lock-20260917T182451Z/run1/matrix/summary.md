# PLT-4646 failure matrix — 20260917T182412Z

commit `f588b4b19d17021ffa4e213279be8e4263c6a186`, Linux 7.0.0-31-generic aarch64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider firecracker (jailed microVMs, host cgroup required, warm pool true), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 130 | 3880 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 7570 | 8807 | pass | pass |  |
| stale_owner_frozen_in_transaction | gateway A stops itself (SIGSTOP) between BEGIN IMMEDIATE and COMMIT of a state.db write, past both leases; B on the same data_dir; SIGCONT A | 35284 | 5986 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 117 | 26370 | pass | pass |  |

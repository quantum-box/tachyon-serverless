# PLT-4646 failure matrix — 20260917T182735Z

commit `f588b4b19d17021ffa4e213279be8e4263c6a186`, Darwin 25.6.0 arm64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider process (dev-only, no isolation), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 45 | 337 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 9337 | 5750 | pass | pass |  |
| stale_owner_frozen_in_transaction | gateway A stops itself (SIGSTOP) between BEGIN IMMEDIATE and COMMIT of a state.db write, past both leases; B on the same data_dir; SIGCONT A | 35387 | 3107 | pass | pass |  |
| db_locked_within_lease | state.db write-locked (BEGIN EXCLUSIVE from another process) for 20 s < dispatcher lease 60 s | 20240 | 198 | pass | pass |  |
| db_locked_past_lease | state.db write-locked for 12 s > dispatcher lease 6 s + skew | 12091 | 197 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 37 | 38381 | pass | pass |  |

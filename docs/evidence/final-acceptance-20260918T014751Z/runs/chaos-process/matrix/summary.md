# PLT-4646 failure matrix — 20260918T042711Z

commit `af0d9ab02ab936b650e208856344a8200ae07762`, Linux 7.0.0-31-generic aarch64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), provider process (dev-only, no isolation), debug gateway with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 132 | 312 | pass | pass |  |
| async_kill_accept_after_commit | SIGKILL at failpoint accept.after_commit (before the 202) | 68 | 1651 | pass | pass |  |
| async_kill_after_publish | SIGKILL at failpoint outbox.after_publish (broker ACK, row not marked) | 108 | 5639 | pass | pass |  |
| async_kill_after_claim | SIGKILL at failpoint dispatch.after_claim (claimed, handler not started) | 104 | 5572 | pass | pass |  |
| async_kill_before_commit | SIGKILL at failpoint dispatch.before_commit (side effect done, outcome not committed) | 1677 | 5941 | pass | pass |  |
| async_kill_after_commit_before_ack | SIGKILL at failpoint dispatch.after_commit (terminal committed, message not ACKed) | 1781 | 320 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 10305 | 6702 | pass | pass |  |
| stale_owner_frozen_in_transaction | gateway A stops itself (SIGSTOP) between BEGIN IMMEDIATE and COMMIT of a state.db write, past both leases; B on the same data_dir; SIGCONT A | 20364 | 4091 | FLAKY (pass on attempt 2) | fail,pass |  |
| stale_owner_async_claim | SIGSTOP gateway A holding an async claim past expiry; B on the same data_dir retries; SIGCONT A | 33863 | 6540 | pass | pass |  |
| db_locked_within_lease | state.db write-locked (BEGIN EXCLUSIVE from another process) for 20 s < dispatcher lease 60 s | 21432 | 1269 | pass | pass |  |
| db_locked_past_lease | state.db write-locked for 12 s > dispatcher lease 6 s + skew | 13378 | 1282 | pass | pass |  |
| broker_sigstop | nats-server SIGSTOP (hung broker) with a small outbox bound, then SIGCONT | 8289 | 10532 | pass | pass |  |
| broker_sigkill | nats-server SIGKILL with messages in flight, then restart on the same store | 8514 | 19838 | pass | pass |  |
| object_store_unavailable | object root unreadable/unwritable (chmod 000) + orphan object from a SIGKILL after the put | 6136 | 3229 | pass | pass |  |
| usage_journal_full_and_replay | usage journal bound reached with the collector stopped; SIGKILL; collector SIGKILL after ledger commit; replay | 264 | 376 | pass | pass |  |
| worker_bridge_kill_sync | SIGKILL the runtime bridge (worker) of a running sync invocation | 9 | 47 | pass | pass |  |
| worker_user_process_kill_sync | SIGKILL the user process of a running sync invocation | 9 | 43 | pass | pass |  |
| worker_bridge_kill_async | SIGKILL the runtime bridge of a running async invocation | 9 | 7640 | pass | pass |  |
| control_plane_outage | control plane (management gateway) stopped past config TTL and auth lease (scripts/control-plane/outage-e2e.sh) | 13879 | 2394 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 122 | 36038 | pass | pass |  |

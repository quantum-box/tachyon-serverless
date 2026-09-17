# PLT-4646 failure matrix — 20260917T105520Z

commit `d992d95c1113bc34765275ca991de776ef053015`, Darwin 25.6.0 arm64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), process provider, debug build with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 182 | 534 | pass | pass |  |
| async_kill_accept_after_commit | SIGKILL at failpoint accept.after_commit (before the 202) | 521 | 1524 | pass | pass |  |
| async_kill_after_publish | SIGKILL at failpoint outbox.after_publish (broker ACK, row not marked) | 772 | 4561 | pass | pass |  |
| async_kill_after_claim | SIGKILL at failpoint dispatch.after_claim (claimed, handler not started) | 798 | 4809 | pass | pass |  |
| async_kill_before_commit | SIGKILL at failpoint dispatch.before_commit (side effect done, outcome not committed) | 2007 | 4383 | pass | pass |  |
| async_kill_after_commit_before_ack | SIGKILL at failpoint dispatch.after_commit (terminal committed, message not ACKed) | 1681 | 452 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 10916 | 6613 | FLAKY (pass on attempt 2) | fail,pass |  |
| stale_owner_async_claim | SIGSTOP gateway A holding an async claim past expiry; B on the same data_dir retries; SIGCONT A | 23957 | 7576 | pass | pass |  |
| db_locked_within_lease | state.db write-locked (BEGIN EXCLUSIVE from another process) for 20 s < dispatcher lease 60 s | 20555 | 851 | pass | pass |  |
| db_locked_past_lease | state.db write-locked for 12 s > dispatcher lease 6 s + skew | 12199 | 29985 | FAIL | fail,fail,fail | outage.answers_success_or_503_store_unavailable |
| broker_sigstop | nats-server SIGSTOP (hung broker) with a small outbox bound, then SIGCONT | 8738 | 3480 | pass | pass |  |
| broker_sigkill | nats-server SIGKILL with messages in flight, then restart on the same store | 9426 | 10528 | pass | pass |  |
| object_store_unavailable | object root unreadable/unwritable (chmod 000) + orphan object from a SIGKILL after the put | 6622 | 1092 | pass | pass |  |
| usage_journal_full_and_replay | usage journal bound reached with the collector stopped; SIGKILL; collector SIGKILL after ledger commit; replay | 1164 | 590 | pass | pass |  |
| worker_bridge_kill_sync | SIGKILL the runtime bridge (worker) of a running sync invocation | 38 | 493 | pass | pass |  |
| worker_user_process_kill_sync | SIGKILL the user process of a running sync invocation | 28 | 402 | pass | pass |  |
| worker_bridge_kill_async | SIGKILL the runtime bridge of a running async invocation | 16 | 6678 | pass | pass |  |
| control_plane_outage | control plane (management gateway) stopped past config TTL and auth lease (scripts/control-plane/outage-e2e.sh) | 13979 | 2140 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 376 | 32550 | pass | pass |  |

# PLT-4646 failure matrix — 20260917T110929Z

commit `95f4a80c7ea068b5ddb3f3e6b6b1681cd7df2a27`, Darwin 25.6.0 arm64, rustc 1.95.0 (59807616e 2026-04-14), nats-server v2.14.7 (nats-server: v2.14.7), process provider, debug build with failpoints.

Single host. These results are not a multi-host HA guarantee.

| scenario | fault | outage_ms | recovery_ms | result | attempts | failed checks (last attempt) |
|---|---|---|---|---|---|---|
| baseline | none (steady workload, graceful stop) | - | - | pass | pass |  |
| sync_gateway_kill | SIGKILL gateway with one sync invocation running and one queued behind it | 264 | 610 | pass | pass |  |
| async_kill_accept_after_commit | SIGKILL at failpoint accept.after_commit (before the 202) | 406 | 1177 | pass | pass |  |
| async_kill_after_publish | SIGKILL at failpoint outbox.after_publish (broker ACK, row not marked) | 1781 | 4763 | pass | pass |  |
| async_kill_after_claim | SIGKILL at failpoint dispatch.after_claim (claimed, handler not started) | 850 | 4389 | pass | pass |  |
| async_kill_before_commit | SIGKILL at failpoint dispatch.before_commit (side effect done, outcome not committed) | 1653 | 3520 | pass | pass |  |
| async_kill_after_commit_before_ack | SIGKILL at failpoint dispatch.after_commit (terminal committed, message not ACKed) | 1390 | 453 | pass | pass |  |
| stale_owner_sync_lease | SIGSTOP gateway A holding a sync slot lease past expiry; B on the same data_dir reclaims; SIGCONT A | 9337 | 7048 | pass | pass |  |
| stale_owner_async_claim | SIGSTOP gateway A holding an async claim past expiry; B on the same data_dir retries; SIGCONT A | 22835 | 7205 | pass | pass |  |
| db_locked_within_lease | state.db write-locked (BEGIN EXCLUSIVE from another process) for 20 s < dispatcher lease 60 s | 20314 | 400 | pass | pass |  |
| db_locked_past_lease | state.db write-locked for 12 s > dispatcher lease 6 s + skew | 11910 | 30229 | FLAKY (pass on attempt 2) | fail,pass |  |
| broker_sigstop | nats-server SIGSTOP (hung broker) with a small outbox bound, then SIGCONT | 7156 | 3347 | pass | pass |  |
| broker_sigkill | nats-server SIGKILL with messages in flight, then restart on the same store | 8749 | 12224 | pass | pass |  |
| object_store_unavailable | object root unreadable/unwritable (chmod 000) + orphan object from a SIGKILL after the put | 6274 | 1617 | FLAKY (pass on attempt 2) | fail,pass |  |
| usage_journal_full_and_replay | usage journal bound reached with the collector stopped; SIGKILL; collector SIGKILL after ledger commit; replay | 687 | 457 | FLAKY (pass on attempt 3) | fail,fail,pass |  |
| worker_bridge_kill_sync | SIGKILL the runtime bridge (worker) of a running sync invocation | 15 | 221 | pass | pass |  |
| worker_user_process_kill_sync | SIGKILL the user process of a running sync invocation | 16 | 204 | pass | pass |  |
| worker_bridge_kill_async | SIGKILL the runtime bridge of a running async invocation | 7 | 6695 | FLAKY (pass on attempt 2) | fail,pass |  |
| control_plane_outage | control plane (management gateway) stopped past config TTL and auth lease (scripts/control-plane/outage-e2e.sh) | 13286 | 570 | pass | pass |  |
| orphan_recovery_after_crash | SIGKILL gateway holding busy environments (secret-bound workload), unsent outbox rows, the scheduler lease, an orphan object | 77 | 36490 | pass | pass |  |

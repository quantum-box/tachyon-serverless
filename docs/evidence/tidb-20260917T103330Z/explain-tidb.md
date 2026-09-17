# TiDB EXPLAIN ANALYZE (after ANALYZE TABLE)

```
Release Version: v8.5.8
Edition: Community
Git Commit Hash: 8b857efa20363d50a8fa2ea7dd9809a85a61b115
Git Branch: HEAD
UTC Build Time: 2026-08-27 07:18:32
GoVersion: go1.25.12
Race Enabled: false
Check Table Before Drop: false
Store: tikv
```

seeded 10000 invocations, 2000 environments, 10000 leases, 10000 idempotency bindings, 10000 outbox rows, 10000 trigger fires in 8.199869333s

## claim_for_reuse (reuse key) (SlotStore::claim_for_reuse)

```sql
SELECT body FROM environments WHERE state = 'idle' AND tenant_id = 'tnt_00000000000000000000000000' AND revision_id = 'rev_00000000000000000000000000' AND execution_role_version = 1 AND configuration_version = 0 AND resource_profile_digest = 'rp' AND runtime_profile = 'tachyon.runtime.v1' AND network_policy_version = 3 AND secret_binding_generation = 42 AND owner_id <=> 'dsp_00000000000000000000000000' ORDER BY id LIMIT 1 FOR UPDATE
```

```
Projection_9 | 0.18 | 1 | root |  | time:1.7ms, loops:2, RU:2.93, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.body | 7.57 KB | N/A
└─SelectLock_10 | 0.18 | 1 | root |  | time:1.7ms, loops:2 | for update 0 | N/A | N/A
  └─Limit_16 | 0.18 | 1 | root |  | time:1.31ms, loops:2 | offset:0, count:1 | N/A | N/A
    └─IndexLookUp_40 | 0.18 | 1 | root |  | time:1.3ms, loops:1, index_task: {total_time: 459.4µs, fetch_handle: 452.5µs, build: 3.5µs, wait: 3.38µs}, table_task: {total_time: 780µs, num: 1, concurrency: 5}, next: {wait_index: 504.7µs, wait_table_lookup_build: 72.5µs, wait_table_lookup_resp: 710.1µs} |  | 17.2 KB | N/A
      ├─IndexRangeScan_37(Build) | 3.69 | 7 | cop[tikv] | table:environments, index:environments_reuse_key(state, tenant_id, revision_id, execution_role_version, configuration_version, resource_profile_digest, runtime_profile, network_policy_version, secret_binding_generation, id) | time:452.5µs, loops:2, cop_task: {num: 1, max: 430.8µs, proc_keys: 7, tot_proc: 66µs, tot_wait: 52.3µs, copr_cache_hit_ratio: 0.00, build_task_duration: 9µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:403.9µs}}, tikv_task:{time:0s, loops:1}, scan_detail: {total_process_keys: 7, total_process_keys_size: 2849, total_keys: 8, get_snapshot_time: 32.5µs, rocksdb: {key_skipped_count: 7, block: {}}}, time_detail: {total_process_time: 66µs, total_wait_time: 52.3µs, tikv_wall_time: 197.2µs} | range:["idle" "tnt_00000000000000000000000000" "rev_00000000000000000000000000" 1 0 "rp" "tachyon.runtime.v1" 3 42,"idle" "tnt_00000000000000000000000000" "rev_00000000000000000000000000" 1 0 "rp" "tachyon.runtime.v1" 3 42], keep order:true | N/A | N/A
      └─Selection_39(Probe) | 0.18 | 7 | cop[tikv] |  | time:681.9µs, loops:2, cop_task: {num: 1, max: 648.1µs, proc_keys: 7, tot_proc: 220µs, tot_wait: 51.4µs, copr_cache_hit_ratio: 0.00, build_task_duration: 32µs, max_distsql_concurrency: 1, max_extra_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:637.8µs}}, tikv_task:{time:0s, loops:1}, scan_detail: {total_process_keys: 7, total_process_keys_size: 9184, total_keys: 7, get_snapshot_time: 23.4µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 220µs, total_wait_time: 51.4µs, tikv_wall_time: 387µs} | nulleq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.owner_id, "dsp_00000000000000000000000000") | N/A | N/A
        └─TableRowIDScan_38 | 3.69 | 7 | cop[tikv] | table:environments | tikv_task:{time:0s, loops:1} | keep order:false | N/A | N/A
```

expected index: ["environments_reuse_key"] -> used

## list_idle (pool sweep) (SlotStore::list_idle)

```sql
SELECT body FROM environments WHERE state = 'idle' AND owner_id <=> 'dsp_00000000000000000000000000' ORDER BY idle_since, id
```

```
Projection_6 | 20.00 | 100 | root |  | time:4.06ms, loops:2, RU:40.81, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.body | 143.8 KB | N/A
└─Sort_7 | 20.00 | 100 | root |  | time:4.05ms, loops:2 | tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.idle_since, tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.id | 132.3 KB | 0 Bytes
  └─TableReader_11 | 20.00 | 100 | root |  | time:3.75ms, loops:2, cop_task: {num: 1, max: 3.66ms, proc_keys: 2000, tot_proc: 3.19ms, tot_wait: 48.5µs, copr_cache_hit_ratio: 0.00, build_task_duration: 5.75µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:3.65ms}} | data:Selection_10 | 112.5 KB | N/A
    └─Selection_10 | 20.00 | 100 | cop[tikv] |  | tikv_task:{time:4ms, loops:6}, scan_detail: {total_process_keys: 2000, total_process_keys_size: 2574000, total_keys: 2001, get_snapshot_time: 24.2µs, rocksdb: {delete_skipped_count: 1, key_skipped_count: 3999, block: {cache_hit_count: 1}}}, time_detail: {total_process_time: 3.19ms, total_suspend_time: 4.96µs, total_wait_time: 48.5µs, total_kv_read_wall_time: 3ms, tikv_wall_time: 3.34ms} | eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.state, "idle"), nulleq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.environments.owner_id, "dsp_00000000000000000000000000") | N/A | N/A
      └─TableFullScan_9 | 2000.00 | 2000 | cop[tikv] | table:environments | tikv_task:{time:3ms, loops:6} | keep order:false | N/A | N/A
```

expected index: ["environments_state_idle_since", "environments_reuse_key", "environments_owner_terminal"] -> NOT USED (sweep, recorded)

## list invocations by function (InvocationRepository::list_by_function)

```sql
SELECT body FROM invocations WHERE function_id = 'fn_00000000000000000000000007' ORDER BY accepted_at DESC, id DESC LIMIT 50
```

```
Projection_8 | 50.00 | 50 | root |  | time:2.05ms, loops:2, RU:1.74, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.body | 64.7 KB | N/A
└─Projection_31 | 50.00 | 50 | root |  | time:2.05ms, loops:2, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.id, tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.function_id, tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.accepted_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.body | 65.3 KB | N/A
  └─IndexLookUp_30 | 50.00 | 50 | root |  | time:2.04ms, loops:2, index_task: {total_time: 492µs, fetch_handle: 483.8µs, build: 7µs, wait: 1.17µs}, table_task: {total_time: 1.44ms, num: 1, concurrency: 5}, next: {wait_index: 520.3µs, wait_table_lookup_build: 69.4µs, wait_table_lookup_resp: 1.38ms} | limit embedded(offset:0, count:50) | 83.6 KB | N/A
    ├─Limit_29(Build) | 50.00 | 50 | cop[tikv] |  | time:480.2µs, loops:1, cop_task: {num: 1, max: 468.1µs, proc_keys: 50, tot_proc: 138.6µs, tot_wait: 34.2µs, copr_cache_hit_ratio: 0.00, build_task_duration: 4.75µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:450.7µs}}, tikv_task:{time:0s, loops:2}, scan_detail: {total_process_keys: 50, total_process_keys_size: 13350, total_keys: 51, get_snapshot_time: 15.9µs, rocksdb: {key_skipped_count: 51, block: {cache_hit_count: 1, read_count: 1, read_byte: 1.29 KB, read_time: 22.2µs}}}, time_detail: {total_process_time: 138.6µs, total_wait_time: 34.2µs, tikv_wall_time: 286.8µs} | offset:0, count:50 | N/A | N/A
    │ └─IndexRangeScan_27 | 50.00 | 50 | cop[tikv] | table:invocations, index:invocations_function_accepted(function_id, accepted_at, id) | tikv_task:{time:0s, loops:2} | range:["fn_00000000000000000000000007","fn_00000000000000000000000007"], keep order:true, desc | N/A | N/A
    └─TableRowIDScan_28(Probe) | 50.00 | 50 | cop[tikv] | table:invocations | time:1.34ms, loops:2, cop_task: {num: 4, max: 1.26ms, min: 0s, avg: 314.5µs, p95: 1.26ms, max_proc_keys: 15, p95_proc_keys: 15, tot_proc: 2.56ms, tot_wait: 427.6µs, copr_cache_hit_ratio: 0.00, build_task_duration: 35.2µs, max_distsql_concurrency: 1, max_extra_concurrency: 1, store_batch_num: 3}, rpc_info:{Cop:{num_rpc:1, total_time:1.25ms}}, tikv_task:{proc max:1ms, min:0s, avg: 250µs, p80:1ms, p95:1ms, iters:4, tasks:4}, scan_detail: {total_process_keys: 50, total_process_keys_size: 64511, total_keys: 50, get_snapshot_time: 149.1µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 2.56ms, total_wait_time: 427.6µs, total_kv_read_wall_time: 1ms, tikv_wall_time: 986.2µs} | keep order:false | N/A | N/A
```

expected index: ["invocations_function_accepted"] -> used

## idempotency lookup (IdempotencyRepository::{lookup, insert_bound})

```sql
SELECT i.invocation_id, i.input_digest, i.expires_at FROM idempotency i JOIN invocations v ON v.id = i.invocation_id WHERE i.tenant_id = 'tnt_00000000000000000000000002' AND i.function_id = 'fn_00000000000000000000000042' AND i.idem_key = 'key-4242' AND (i.expires_at IS NULL OR i.expires_at > '2026-09-10T01:50:00.000000000Z')
```

```
IndexJoin_14 | 0.00 | 1 | root |  | time:870.6µs, loops:2, RU:0.98, inner:{total:437.1µs, concurrency:5, task:1, construct:9.33µs, fetch:424µs, build:3.04µs}, probe:3.88µs | inner join, inner:IndexReader_13, outer key:tsls_m_01m2qf2hz7hecqedsmcw953pjd.idempotency.invocation_id, inner key:tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.id, equal cond:eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.idempotency.invocation_id, tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.id) | 4.99 KB | N/A
├─Selection_22(Build) | 0.00 | 1 | root |  | time:368.3µs, loops:3 | or(isnull(tsls_m_01m2qf2hz7hecqedsmcw953pjd.idempotency.expires_at), gt(tsls_m_01m2qf2hz7hecqedsmcw953pjd.idempotency.expires_at, "2026-09-10T01:50:00.000000000Z")) | 6.10 KB | N/A
│ └─Point_Get_21 | 1.00 | 1 | root | table:idempotency, clustered index:PRIMARY(tenant_id, function_id, idem_key) | time:345.5µs, loops:4, Get:{num_rpc:1, total_time:299.5µs}, time_detail: {total_process_time: 29µs, total_wait_time: 56.9µs, total_kv_read_wall_time: 88.1µs, tikv_wall_time: 107.9µs}, scan_detail: {total_process_keys: 1, total_process_keys_size: 285, total_keys: 1, get_snapshot_time: 28.5µs, rocksdb: {block: {}}} |  | N/A | N/A
└─IndexReader_13(Probe) | 0.00 | 1 | root |  | time:361.6µs, loops:2, cop_task: {num: 1, max: 339µs, proc_keys: 1, tot_proc: 40.6µs, tot_wait: 34.3µs, copr_cache_hit_ratio: 0.00, build_task_duration: 3.71µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:332.3µs}} | index:IndexRangeScan_12 | 334 Bytes | N/A
  └─IndexRangeScan_12 | 0.00 | 1 | cop[tikv] | table:v, index:PRIMARY(id) | tikv_task:{time:0s, loops:1}, scan_detail: {total_process_keys: 1, total_process_keys_size: 120, total_keys: 2, get_snapshot_time: 17.8µs, rocksdb: {key_skipped_count: 1, block: {}}}, time_detail: {total_process_time: 40.6µs, total_wait_time: 34.3µs, tikv_wall_time: 170.7µs} | range: decided by [eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.id, tsls_m_01m2qf2hz7hecqedsmcw953pjd.idempotency.invocation_id)], keep order:false | N/A | N/A
```

expected index: ["PRIMARY"] -> used

## outbox claim (AsyncInvocationRepository::claim_outbox (sqlite/outbox.rs))

```sql
SELECT event_id FROM outbox WHERE sent = 0 AND next_attempt_at <= '2026-09-10T01:50:00.000000000Z' AND (claimed_by IS NULL OR claim_expires_at <= '2026-09-10T01:50:00.000000000Z') ORDER BY created_at, event_id LIMIT 32
```

```
Projection_8 | 32.00 | 32 | root |  | time:4.81ms, loops:2, RU:193.76, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.event_id | 6.85 KB | N/A
└─TopN_9 | 32.00 | 32 | root |  | time:4.8ms, loops:2 | tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.created_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.event_id, offset:0, count:32 | 9.74 KB | 0 Bytes
  └─TableReader_18 | 32.00 | 122 | root |  | time:4.68ms, loops:4, cop_task: {num: 4, max: 4.67ms, min: 2.2ms, avg: 3.5ms, p95: 4.67ms, max_proc_keys: 3500, p95_proc_keys: 3500, tot_proc: 12.4ms, tot_wait: 270.6µs, copr_cache_hit_ratio: 0.00, build_task_duration: 10.4µs, max_distsql_concurrency: 4}, rpc_info:{Cop:{num_rpc:4, total_time:13.9ms}} | data:TopN_17 | 9.17 KB | N/A
    └─TopN_17 | 32.00 | 122 | cop[tikv] |  | tikv_task:{proc max:5ms, min:2ms, avg: 3.75ms, p80:5ms, p95:5ms, iters:12, tasks:4}, scan_detail: {total_process_keys: 10000, total_process_keys_size: 12303500, total_keys: 10004, get_snapshot_time: 81.8µs, rocksdb: {key_skipped_count: 19996, block: {cache_hit_count: 4}}}, time_detail: {total_process_time: 12.4ms, total_suspend_time: 56.7µs, total_wait_time: 270.6µs, total_kv_read_wall_time: 15ms, tikv_wall_time: 13.1ms} | tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.created_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.event_id, offset:0, count:32 | N/A | N/A
      └─Selection_16 | 280.00 | 251 | cop[tikv] |  | tikv_task:{proc max:5ms, min:2ms, avg: 3.75ms, p80:5ms, p95:5ms, iters:12, tasks:4} | eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.sent, 0), le(tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.next_attempt_at, "2026-09-10T01:50:00.000000000Z"), or(isnull(tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.claimed_by), le(tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.claim_expires_at, "2026-09-10T01:50:00.000000000Z")) | N/A | N/A
        └─TableFullScan_15 | 10000.00 | 10000 | cop[tikv] | table:outbox | tikv_task:{proc max:5ms, min:2ms, avg: 3.75ms, p80:5ms, p95:5ms, iters:12, tasks:4} | keep order:false | N/A | N/A
```

expected index: ["outbox_sent_next", "outbox_sent_created"] -> NOT USED (sweep, recorded)

## trigger fire by key (unique) (TriggerRepository fire dedupe (sqlite/triggers.rs))

```sql
SELECT outcome FROM trigger_fires WHERE trigger_id = 'trg_00000000000000000000000043' AND fire_key = 'event:evt-4243'
```

```
Point_Get_1 | 1.00 | 1 | root | table:trigger_fires, index:PRIMARY(trigger_id, fire_key) | time:646.1µs, loops:2, RU:0.98, Get:{num_rpc:2, total_time:566.9µs}, time_detail: {total_process_time: 66.7µs, total_wait_time: 90.4µs, total_kv_read_wall_time: 166.5µs, tikv_wall_time: 218.8µs}, scan_detail: {total_process_keys: 2, total_process_keys_size: 375, total_keys: 2, get_snapshot_time: 26.3µs, rocksdb: {block: {}}} |  | N/A | N/A
```

expected index: ["PRIMARY"] -> used

## trigger fire by signature (unique) (TriggerRepository webhook replay (sqlite/triggers.rs))

```sql
SELECT outcome FROM trigger_fires WHERE trigger_id = 'trg_00000000000000000000000043' AND signature_digest = 'sha256:sig4243'
```

```
Point_Get_1 | 1.00 | 1 | root | table:trigger_fires, index:trigger_fires_signature(trigger_id, signature_digest) | time:561µs, loops:2, RU:0.98, Get:{num_rpc:2, total_time:489.8µs}, time_detail: {total_process_time: 65.6µs, total_wait_time: 87.7µs, total_kv_read_wall_time: 160µs, tikv_wall_time: 198.1µs}, scan_detail: {total_process_keys: 2, total_process_keys_size: 375, total_keys: 2, get_snapshot_time: 30.8µs, rocksdb: {block: {}}} |  | N/A | N/A
```

expected index: ["trigger_fires_signature"] -> used

## due cron triggers (TriggerRepository::due_cron (sqlite/triggers.rs))

```sql
SELECT body FROM triggers WHERE kind = 'cron' AND status = 'enabled' AND next_fire_at IS NOT NULL AND next_fire_at <= '2026-09-10T01:50:00.000000000Z' ORDER BY next_fire_at, id LIMIT 32
```

```
Projection_8 | 28.93 | 32 | root |  | time:1.04ms, loops:2, RU:4.57, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.body | 45.1 KB | N/A
└─TopN_9 | 28.93 | 32 | root |  | time:1.03ms, loops:2 | tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.next_fire_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.id, offset:0, count:32 | 35.9 KB | 0 Bytes
  └─TableReader_18 | 28.93 | 32 | root |  | time:964µs, loops:2, cop_task: {num: 1, max: 936.3µs, proc_keys: 200, tot_proc: 524.3µs, tot_wait: 40.4µs, copr_cache_hit_ratio: 0.00, build_task_duration: 8.71µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:925.1µs}} | data:TopN_17 | 35.4 KB | N/A
    └─TopN_17 | 28.93 | 32 | cop[tikv] |  | tikv_task:{time:0s, loops:1}, scan_detail: {total_process_keys: 200, total_process_keys_size: 257020, total_keys: 201, get_snapshot_time: 21.2µs, rocksdb: {key_skipped_count: 399, block: {cache_hit_count: 1}}}, time_detail: {total_process_time: 524.3µs, total_wait_time: 40.4µs, tikv_wall_time: 698.7µs} | tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.next_fire_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.id, offset:0, count:32 | N/A | N/A
      └─Selection_16 | 28.93 | 42 | cop[tikv] |  | tikv_task:{time:0s, loops:1} | eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.kind, "cron"), eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.status, "enabled"), le(tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.next_fire_at, "2026-09-10T01:50:00.000000000Z"), not(isnull(tsls_m_01m2qf2hz7hecqedsmcw953pjd.triggers.next_fire_at)) | N/A | N/A
        └─TableFullScan_15 | 200.00 | 200 | cop[tikv] | table:triggers | tikv_task:{time:0s, loops:1} | keep order:false | N/A | N/A
```

expected index: ["triggers_due"] -> NOT USED (sweep, recorded)

## reclaim: unreleased leases (SlotStore::reclaim_expired step 2)

```sql
SELECT body FROM leases WHERE released = 0 AND owner_id IS NOT NULL ORDER BY id
```

```
Projection_6 | 100.00 | 100 | root |  | time:2.49ms, loops:2, RU:3.51, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.leases.body | 138.8 KB | N/A
└─Sort_7 | 100.00 | 100 | root |  | time:2.48ms, loops:2 | tsls_m_01m2qf2hz7hecqedsmcw953pjd.leases.id | 140.3 KB | 0 Bytes
  └─IndexLookUp_19 | 100.00 | 100 | root |  | time:2.27ms, loops:2, index_task: {total_time: 446.8µs, fetch_handle: 442.3µs, build: 1.13µs, wait: 3.38µs}, table_task: {total_time: 1.7ms, num: 1, concurrency: 5}, next: {wait_index: 485.9µs, wait_table_lookup_build: 91.4µs, wait_table_lookup_resp: 1.62ms} |  | 140.1 KB | N/A
    ├─IndexRangeScan_16(Build) | 100.00 | 100 | cop[tikv] | table:leases, index:leases_released(released) | time:436.9µs, loops:3, cop_task: {num: 1, max: 417.4µs, proc_keys: 100, tot_proc: 120.5µs, tot_wait: 43.6µs, copr_cache_hit_ratio: 0.00, build_task_duration: 9.5µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:387.3µs}}, tikv_task:{time:0s, loops:3}, scan_detail: {total_process_keys: 100, total_process_keys_size: 4600, total_keys: 101, get_snapshot_time: 26.4µs, rocksdb: {key_skipped_count: 100, block: {}}}, time_detail: {total_process_time: 120.5µs, total_wait_time: 43.6µs, tikv_wall_time: 235.4µs} | range:[0,0], keep order:false | N/A | N/A
    └─Selection_18(Probe) | 100.00 | 100 | cop[tikv] |  | time:1.6ms, loops:2, cop_task: {num: 3, max: 1.53ms, min: 0s, avg: 923.9µs, p95: 1.53ms, max_proc_keys: 45, p95_proc_keys: 45, tot_proc: 2.6ms, tot_wait: 143µs, copr_cache_hit_ratio: 0.00, build_task_duration: 38.6µs, max_distsql_concurrency: 1, max_extra_concurrency: 1, store_batch_num: 1}, rpc_info:{Cop:{num_rpc:2, total_time:2.76ms}}, tikv_task:{proc max:1ms, min:0s, avg: 333.3µs, p80:1ms, p95:1ms, iters:4, tasks:3}, scan_detail: {total_process_keys: 100, total_process_keys_size: 128900, total_keys: 100, get_snapshot_time: 68.7µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 2.6ms, total_suspend_time: 9.42µs, total_wait_time: 143µs, total_kv_read_wall_time: 1ms, tikv_wall_time: 2.22ms} | not(isnull(tsls_m_01m2qf2hz7hecqedsmcw953pjd.leases.owner_id)) | N/A | N/A
      └─TableRowIDScan_17 | 100.00 | 100 | cop[tikv] | table:leases | tikv_task:{proc max:1ms, min:0s, avg: 333.3µs, p80:1ms, p95:1ms, iters:4, tasks:3} | keep order:false | N/A | N/A
```

expected index: ["leases_released_expires", "leases_released", "leases_owner_released"] -> used

## reclaim: invocations of a dead dispatcher (SlotStore::reclaim_expired step 3)

```sql
SELECT body FROM invocations WHERE owner_id = 'dsp_00000000000000000000000000' AND terminal = 0 ORDER BY id
```

```
Projection_6 | 100.00 | 100 | root |  | time:2.81ms, loops:2, RU:3.73, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.body | 138.8 KB | N/A
└─Sort_7 | 100.00 | 100 | root |  | time:2.8ms, loops:2 | tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.id | 140.3 KB | 0 Bytes
  └─IndexLookUp_14 | 100.00 | 100 | root |  | time:2.57ms, loops:2, index_task: {total_time: 424.8µs, fetch_handle: 418.3µs, build: 917ns, wait: 5.54µs}, table_task: {total_time: 2.02ms, num: 1, concurrency: 5}, next: {wait_index: 490.4µs, wait_table_lookup_build: 48.8µs, wait_table_lookup_resp: 1.93ms} |  | 141.2 KB | N/A
    ├─IndexRangeScan_12(Build) | 100.00 | 100 | cop[tikv] | table:invocations, index:invocations_owner_terminal(owner_id, terminal) | time:409.3µs, loops:3, cop_task: {num: 1, max: 387µs, proc_keys: 100, tot_proc: 108.5µs, tot_wait: 35.3µs, copr_cache_hit_ratio: 0.00, build_task_duration: 6.25µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:362.4µs}}, tikv_task:{time:0s, loops:3}, scan_detail: {total_process_keys: 100, total_process_keys_size: 13400, total_keys: 101, get_snapshot_time: 18.7µs, rocksdb: {key_skipped_count: 100, block: {}}}, time_detail: {total_process_time: 108.5µs, total_wait_time: 35.3µs, tikv_wall_time: 213.8µs} | range:["dsp_00000000000000000000000000" 0,"dsp_00000000000000000000000000" 0], keep order:false | N/A | N/A
    └─TableRowIDScan_13(Probe) | 100.00 | 100 | cop[tikv] | table:invocations | time:1.91ms, loops:2, cop_task: {num: 4, max: 1.84ms, min: 0s, avg: 911.3µs, p95: 1.84ms, max_proc_keys: 35, p95_proc_keys: 35, tot_proc: 4.54ms, tot_wait: 246.1µs, copr_cache_hit_ratio: 0.00, build_task_duration: 44.8µs, max_distsql_concurrency: 2, store_batch_num: 2}, rpc_info:{Cop:{num_rpc:2, total_time:3.63ms}}, tikv_task:{proc max:1ms, min:1ms, avg: 1ms, p80:1ms, p95:1ms, iters:5, tasks:4}, scan_detail: {total_process_keys: 100, total_process_keys_size: 127700, total_keys: 100, get_snapshot_time: 77.3µs, rocksdb: {block: {read_count: 9, read_byte: 284.2 KB, read_time: 251.1µs}}}, time_detail: {total_process_time: 4.54ms, total_suspend_time: 19.2µs, total_wait_time: 246.1µs, total_kv_read_wall_time: 4ms, tikv_wall_time: 3.06ms} | keep order:false | N/A | N/A
```

expected index: ["invocations_owner_terminal"] -> used

## heartbeat: leases of a dispatcher (SlotStore::heartbeat)

```sql
SELECT body FROM leases WHERE owner_id = 'dsp_00000000000000000000000000' AND released = 0 ORDER BY id
```

```
Projection_6 | 100.00 | 100 | root |  | time:3.4ms, loops:2, RU:3.80, Concurrency:OFF | tsls_m_01m2qf2hz7hecqedsmcw953pjd.leases.body | 138.8 KB | N/A
└─Sort_7 | 100.00 | 100 | root |  | time:3.39ms, loops:2 | tsls_m_01m2qf2hz7hecqedsmcw953pjd.leases.id | 140.3 KB | 0 Bytes
  └─IndexLookUp_14 | 100.00 | 100 | root |  | time:3.16ms, loops:2, index_task: {total_time: 563.4µs, fetch_handle: 560.1µs, build: 1.25µs, wait: 2µs}, table_task: {total_time: 2.47ms, num: 1, concurrency: 5}, next: {wait_index: 589.7µs, wait_table_lookup_build: 105.6µs, wait_table_lookup_resp: 2.36ms} |  | 139.9 KB | N/A
    ├─IndexRangeScan_12(Build) | 100.00 | 100 | cop[tikv] | table:leases, index:leases_owner_released(owner_id, released) | time:551.9µs, loops:3, cop_task: {num: 1, max: 530.2µs, proc_keys: 100, tot_proc: 117.3µs, tot_wait: 62.4µs, copr_cache_hit_ratio: 0.00, build_task_duration: 4.04µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:510.5µs}}, tikv_task:{time:1ms, loops:3}, scan_detail: {total_process_keys: 100, total_process_keys_size: 13400, total_keys: 101, get_snapshot_time: 31.1µs, rocksdb: {key_skipped_count: 100, block: {}}}, time_detail: {total_process_time: 117.3µs, total_wait_time: 62.4µs, total_kv_read_wall_time: 1ms, tikv_wall_time: 261µs} | range:["dsp_00000000000000000000000000" 0,"dsp_00000000000000000000000000" 0], keep order:false | N/A | N/A
    └─TableRowIDScan_13(Probe) | 100.00 | 100 | cop[tikv] | table:leases | time:2.33ms, loops:2, cop_task: {num: 3, max: 2.25ms, min: 0s, avg: 1.44ms, p95: 2.25ms, max_proc_keys: 45, p95_proc_keys: 45, tot_proc: 3.12ms, tot_wait: 175.6µs, copr_cache_hit_ratio: 0.00, build_task_duration: 34.5µs, max_distsql_concurrency: 1, max_extra_concurrency: 1, store_batch_num: 1}, rpc_info:{Cop:{num_rpc:2, total_time:4.3ms}}, tikv_task:{proc max:2ms, min:1ms, avg: 1.33ms, p80:2ms, p95:2ms, iters:4, tasks:3}, scan_detail: {total_process_keys: 100, total_process_keys_size: 128900, total_keys: 100, get_snapshot_time: 82.1µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 3.12ms, total_suspend_time: 11.8µs, total_wait_time: 175.6µs, total_kv_read_wall_time: 4ms, tikv_wall_time: 3.75ms} | keep order:false | N/A | N/A
```

expected index: ["leases_owner_released"] -> used

## acquire: unreleased lease of an environment (SlotStore::{acquire, release_to_pool})

```sql
SELECT 1 FROM leases WHERE environment_id = 'env_00000000000000000000000005' AND released = 0 LIMIT 1
```

```
Projection_7 | 1.00 | 0 | root |  | time:344.3µs, loops:1, RU:0.49, Concurrency:OFF | 1->Column#13 | 380 Bytes | N/A
└─Limit_8 | 1.00 | 0 | root |  | time:343.1µs, loops:1 | offset:0, count:1 | N/A | N/A
  └─IndexReader_12 | 1.00 | 0 | root |  | time:340.8µs, loops:1, cop_task: {num: 1, max: 325µs, proc_keys: 0, tot_proc: 44.7µs, tot_wait: 41.1µs, copr_cache_hit_ratio: 0.00, build_task_duration: 4.46µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:317.2µs}} | index:Limit_11 | 287 Bytes | N/A
    └─Limit_11 | 1.00 | 0 | cop[tikv] |  | tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 23µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 44.7µs, total_wait_time: 41.1µs, tikv_wall_time: 163.3µs} | offset:0, count:1 | N/A | N/A
      └─IndexRangeScan_10 | 1.00 | 0 | cop[tikv] | table:leases, index:leases_environment_released(environment_id, released) | tikv_task:{time:0s, loops:1} | range:["env_00000000000000000000000005" 0,"env_00000000000000000000000005" 0], keep order:false | N/A | N/A
```

expected index: ["leases_environment_released", "leases_environment"] -> used

## retention: expired inline outputs (StateStore::purge_expired_outputs)

```sql
SELECT body FROM invocations WHERE output_expires_at IS NOT NULL AND output_expires_at <= '2026-09-10T01:50:00.000000000Z'
```

```
TableReader_14 | 463.00 | 457 | root |  | time:6.35ms, loops:2, RU:204.24, cop_task: {num: 6, max: 4.3ms, min: 1.77ms, avg: 2.77ms, p95: 4.3ms, max_proc_keys: 3020, p95_proc_keys: 3020, tot_proc: 13.8ms, tot_wait: 590µs, copr_cache_hit_ratio: 0.00, build_task_duration: 13.4µs, max_distsql_concurrency: 4}, rpc_info:{Cop:{num_rpc:6, total_time:16.5ms}} | data:Projection_5 | 311.7 KB | N/A
└─Projection_5 | 463.00 | 457 | cop[tikv] |  | tikv_task:{proc max:4ms, min:1ms, avg: 2.17ms, p80:3ms, p95:4ms, iters:34, tasks:6}, scan_detail: {total_process_keys: 10000, total_process_keys_size: 12897411, total_keys: 10006, get_snapshot_time: 113.9µs, rocksdb: {key_skipped_count: 19994, block: {cache_hit_count: 14, read_count: 28, read_byte: 883.3 KB, read_time: 460.3µs}}}, time_detail: {total_process_time: 13.8ms, total_suspend_time: 31.7µs, total_wait_time: 590µs, total_kv_read_wall_time: 12ms, tikv_wall_time: 15ms} | tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.body | N/A | N/A
  └─Selection_13 | 463.00 | 457 | cop[tikv] |  | tikv_task:{proc max:4ms, min:1ms, avg: 2.17ms, p80:3ms, p95:4ms, iters:34, tasks:6} | le(tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.output_expires_at, "2026-09-10T01:50:00.000000000Z"), not(isnull(tsls_m_01m2qf2hz7hecqedsmcw953pjd.invocations.output_expires_at)) | N/A | N/A
    └─TableFullScan_12 | 10000.00 | 10000 | cop[tikv] | table:invocations | tikv_task:{proc max:4ms, min:1ms, avg: 2ms, p80:3ms, p95:4ms, iters:34, tasks:6} | keep order:false | N/A | N/A
```

expected index: ["invocations_output_expires"] -> NOT USED (sweep, recorded)

## retention: expired idempotency bindings (IdempotencyRepository::purge_expired_idempotency)

```sql
DELETE FROM idempotency WHERE expires_at IS NOT NULL AND expires_at <= '2026-09-10T01:50:00.000000000Z'
```

```
Delete_4 | N/A | root |  | N/A
└─SelectLock_7 | 1.00 | root |  | for update 0
  └─IndexLookUp_13 | 1.00 | root |  | 
    ├─IndexRangeScan_11(Build) | 1.00 | cop[tikv] | table:idempotency, index:idempotency_expires(expires_at) | range:[-inf,"2026-09-10T01:50:00.000000000Z"], keep order:false
    └─TableRowIDScan_12(Probe) | 1.00 | cop[tikv] | table:idempotency | keep order:false
```

expected index: ["idempotency_expires"] -> used

## retention: sent outbox rows (AsyncInvocationRepository::purge_sent (sqlite/outbox.rs))

```sql
DELETE FROM outbox WHERE sent = 1 AND sent_at < '2026-09-10T01:50:00.000000000Z'
```

```
Delete_4 | N/A | root |  | N/A
└─Projection_6 | 4511.55 | root |  | tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.event_id, tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.created_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.sent, tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.next_attempt_at, tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox._tidb_rowid
  └─SelectLock_7 | 4511.55 | root |  | for update 0
    └─TableReader_10 | 4511.55 | root |  | data:Selection_9
      └─Selection_9 | 4511.55 | cop[tikv] |  | eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.sent, 1), lt(tsls_m_01m2qf2hz7hecqedsmcw953pjd.outbox.sent_at, "2026-09-10T01:50:00.000000000Z")
        └─TableFullScan_8 | 10000.00 | cop[tikv] | table:outbox | keep order:false
```

expected index: ["outbox_sent_next", "outbox_sent_created"] -> NOT USED (sweep, recorded)

## retention: old trigger fires (TriggerRepository::purge_fires (sqlite/triggers.rs))

```sql
DELETE FROM trigger_fires WHERE kind = 'webhook' AND created_at < '2026-09-10T01:50:00.000000000Z'
```

```
Delete_4 | N/A | root |  | N/A
└─SelectLock_7 | 2520.00 | root |  | for update 0
  └─TableReader_10 | 2520.00 | root |  | data:Selection_9
    └─Selection_9 | 2520.00 | cop[tikv] |  | eq(tsls_m_01m2qf2hz7hecqedsmcw953pjd.trigger_fires.kind, "webhook"), lt(tsls_m_01m2qf2hz7hecqedsmcw953pjd.trigger_fires.created_at, "2026-09-10T01:50:00.000000000Z")
      └─TableFullScan_8 | 10000.00 | cop[tikv] | table:trigger_fires | keep order:false
```

expected index: ["trigger_fires_created"] -> NOT USED (sweep, recorded)


# SQLite 3.53.2 EXPLAIN QUERY PLAN (after ANALYZE)

## claim_for_reuse (reuse key) (SlotStore::claim_for_reuse)

```sql
SELECT body FROM environments WHERE state = 'idle' AND tenant_id = 'tnt_00000000000000000000000000' AND revision_id = 'rev_00000000000000000000000000' AND execution_role_version = 1 AND configuration_version = 0 AND resource_profile_digest = 'rp' AND runtime_profile = 'tachyon.runtime.v1' AND network_policy_version = 3 AND secret_binding_generation = 42 AND owner_id IS 'dsp_00000000000000000000000000' ORDER BY id LIMIT 1
```

```
SEARCH environments USING INDEX environments_reuse_key (state=? AND tenant_id=? AND revision_id=? AND execution_role_version=? AND configuration_version=? AND resource_profile_digest=? AND runtime_profile=? AND network_policy_version=? AND secret_binding_generation=?)
```

expected index: ["environments_reuse_key"] -> used

## list_idle (pool sweep) (SlotStore::list_idle)

```sql
SELECT body FROM environments WHERE state = 'idle' AND owner_id IS 'dsp_00000000000000000000000000' ORDER BY idle_since, id
```

```
SEARCH environments USING INDEX environments_state_idle_since (state=?)
```

expected index: ["environments_state_idle_since", "environments_reuse_key", "environments_owner_terminal"] -> used

## list invocations by function (InvocationRepository::list_by_function)

```sql
SELECT body FROM invocations WHERE function_id = 'fn_00000000000000000000000007' ORDER BY accepted_at DESC, id DESC LIMIT 50
```

```
SEARCH invocations USING INDEX invocations_function_accepted (function_id=?)
```

expected index: ["invocations_function_accepted"] -> used

## idempotency lookup (IdempotencyRepository::{lookup, insert_bound})

```sql
SELECT i.invocation_id, i.input_digest, i.expires_at FROM idempotency i JOIN invocations v ON v.id = i.invocation_id WHERE i.tenant_id = 'tnt_00000000000000000000000002' AND i.function_id = 'fn_00000000000000000000000042' AND i.idem_key = 'key-4242' AND (i.expires_at IS NULL OR i.expires_at > '2026-09-10T01:50:00.000000000Z')
```

```
SEARCH i USING INDEX sqlite_autoindex_idempotency_1 (tenant_id=? AND function_id=? AND idem_key=?)
SEARCH v USING COVERING INDEX sqlite_autoindex_invocations_1 (id=?)
```

expected index: ["sqlite_autoindex_idempotency_1"] -> used

## outbox claim (AsyncInvocationRepository::claim_outbox (sqlite/outbox.rs))

```sql
SELECT event_id FROM outbox WHERE sent = 0 AND next_attempt_at <= '2026-09-10T01:50:00.000000000Z' AND (claimed_by IS NULL OR claim_expires_at <= '2026-09-10T01:50:00.000000000Z') ORDER BY created_at, event_id LIMIT 32
```

```
SEARCH outbox USING INDEX outbox_sent_next (sent=? AND next_attempt_at<?)
USE TEMP B-TREE FOR ORDER BY
```

expected index: ["outbox_sent_next", "outbox_sent_created"] -> used

## trigger fire by key (unique) (TriggerRepository fire dedupe (sqlite/triggers.rs))

```sql
SELECT outcome FROM trigger_fires WHERE trigger_id = 'trg_00000000000000000000000043' AND fire_key = 'event:evt-4243'
```

```
SEARCH trigger_fires USING INDEX sqlite_autoindex_trigger_fires_1 (trigger_id=? AND fire_key=?)
```

expected index: ["sqlite_autoindex_trigger_fires_1"] -> used

## trigger fire by signature (unique) (TriggerRepository webhook replay (sqlite/triggers.rs))

```sql
SELECT outcome FROM trigger_fires WHERE trigger_id = 'trg_00000000000000000000000043' AND signature_digest = 'sha256:sig4243'
```

```
SEARCH trigger_fires USING INDEX trigger_fires_signature (trigger_id=? AND signature_digest=?)
```

expected index: ["trigger_fires_signature"] -> used

## due cron triggers (TriggerRepository::due_cron (sqlite/triggers.rs))

```sql
SELECT body FROM triggers WHERE kind = 'cron' AND status = 'enabled' AND next_fire_at IS NOT NULL AND next_fire_at <= '2026-09-10T01:50:00.000000000Z' ORDER BY next_fire_at, id LIMIT 32
```

```
SEARCH triggers USING INDEX triggers_due (kind=? AND status=? AND next_fire_at>? AND next_fire_at<?)
USE TEMP B-TREE FOR LAST TERM OF ORDER BY
```

expected index: ["triggers_due"] -> used

## reclaim: unreleased leases (SlotStore::reclaim_expired step 2)

```sql
SELECT body FROM leases WHERE released = 0 AND owner_id IS NOT NULL ORDER BY id
```

```
SEARCH leases USING INDEX leases_released (released=?)
USE TEMP B-TREE FOR ORDER BY
```

expected index: ["leases_released_expires", "leases_released"] -> used

## reclaim: invocations of a dead dispatcher (SlotStore::reclaim_expired step 3)

```sql
SELECT body FROM invocations WHERE owner_id = 'dsp_00000000000000000000000000' AND terminal = 0 ORDER BY id
```

```
SEARCH invocations USING INDEX invocations_owner_terminal (owner_id=? AND terminal=?)
USE TEMP B-TREE FOR ORDER BY
```

expected index: ["invocations_owner_terminal"] -> used

## heartbeat: leases of a dispatcher (SlotStore::heartbeat)

```sql
SELECT body FROM leases WHERE owner_id = 'dsp_00000000000000000000000000' AND released = 0 ORDER BY id
```

```
SEARCH leases USING INDEX leases_owner_released (owner_id=? AND released=?)
USE TEMP B-TREE FOR ORDER BY
```

expected index: ["leases_owner_released"] -> used

## acquire: unreleased lease of an environment (SlotStore::{acquire, release_to_pool})

```sql
SELECT 1 FROM leases WHERE environment_id = 'env_00000000000000000000000005' AND released = 0 LIMIT 1
```

```
SEARCH leases USING COVERING INDEX leases_environment_released (environment_id=? AND released=?)
```

expected index: ["leases_environment_released", "leases_environment"] -> used

## retention: expired inline outputs (StateStore::purge_expired_outputs)

```sql
SELECT body FROM invocations WHERE output_expires_at IS NOT NULL AND output_expires_at <= '2026-09-10T01:50:00.000000000Z'
```

```
SEARCH invocations USING INDEX invocations_output_expires (output_expires_at>? AND output_expires_at<?)
```

expected index: ["invocations_output_expires"] -> used

## retention: expired idempotency bindings (IdempotencyRepository::purge_expired_idempotency)

```sql
DELETE FROM idempotency WHERE expires_at IS NOT NULL AND expires_at <= '2026-09-10T01:50:00.000000000Z'
```

```
SEARCH idempotency USING INDEX idempotency_expires (expires_at>? AND expires_at<?)
```

expected index: ["idempotency_expires"] -> used

## retention: sent outbox rows (AsyncInvocationRepository::purge_sent (sqlite/outbox.rs))

```sql
DELETE FROM outbox WHERE sent = 1 AND sent_at < '2026-09-10T01:50:00.000000000Z'
```

```
SCAN outbox
```

expected index: ["outbox_sent_next", "outbox_sent_created", "SCAN outbox"] -> used

## retention: old trigger fires (TriggerRepository::purge_fires (sqlite/triggers.rs))

```sql
DELETE FROM trigger_fires WHERE kind = 'webhook' AND created_at < '2026-09-10T01:50:00.000000000Z'
```

```
SEARCH trigger_fires USING INDEX trigger_fires_created (kind=? AND created_at<?)
```

expected index: ["trigger_fires_created"] -> used


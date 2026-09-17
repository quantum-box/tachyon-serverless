//! [`TriggerRepository`] on `state.db` (migration 007, PLT-4641). Every write
//! is one `BEGIN IMMEDIATE` transaction; the `UPDATE`s carry their CAS
//! predicate (`generation = ?`, `status = 'enabled'`), so two gateways on one
//! file cannot both win.

use rusqlite::{Connection, OptionalExtension, Row, params};

use tachyon_serverless_domain::{
    FunctionId, Invocation, InvocationId, TenantId, Timestamp, TriggerId,
};

use super::super::outbox::{AsyncAcceptOutcome, AsyncInput, BacklogLimits, OutboxEvent};
use super::super::triggers::{
    FireClaim, FireOutcome, FireRecord, Trigger, TriggerAcceptOutcome, TriggerKind,
    TriggerRepository, TriggerStatus,
};
use super::outbox::{accept_in, check_accept};
use super::{RepoError, SqliteStore, big, from_json, to_json, ts};

fn parse_ts(raw: &str) -> Result<Timestamp, RepoError> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&chrono::Utc))
        .map_err(|e| RepoError::Serialization(format!("timestamp `{raw}`: {e}")))
}

fn ser<E: std::fmt::Display>(e: E) -> RepoError {
    RepoError::Serialization(e.to_string())
}

fn kind_of(raw: &str) -> Result<TriggerKind, RepoError> {
    match raw {
        "cron" => Ok(TriggerKind::Cron),
        "webhook" => Ok(TriggerKind::Webhook),
        other => Err(RepoError::Serialization(format!("trigger kind `{other}`"))),
    }
}

fn outcome_of(raw: &str) -> Result<FireOutcome, RepoError> {
    match raw {
        "accepted" => Ok(FireOutcome::Accepted),
        "refused" => Ok(FireOutcome::Refused),
        other => Err(RepoError::Serialization(format!("fire outcome `{other}`"))),
    }
}

fn get_trigger_in(c: &Connection, id: &TriggerId) -> Result<Option<Trigger>, RepoError> {
    let body: Option<String> = c
        .prepare_cached("SELECT body FROM triggers WHERE id = ?1")?
        .query_row([id.as_str()], |r| r.get(0))
        .optional()?;
    body.as_deref().map(from_json).transpose()
}

fn write_trigger_columns(c: &Connection, t: &Trigger) -> Result<usize, RepoError> {
    Ok(c.prepare_cached(
        "UPDATE triggers SET status = ?1, generation = ?2, next_fire_at = ?3, updated_at = ?4, \
         body = ?5 WHERE id = ?6",
    )?
    .execute(params![
        t.status.as_str(),
        big(t.generation, "trigger generation")?,
        next_fire_column(t),
        ts(&t.updated_at),
        to_json(t)?,
        t.id.as_str()
    ])?)
}

/// Only an enabled cron trigger has a due time.
fn next_fire_column(t: &Trigger) -> Option<String> {
    (t.kind() == TriggerKind::Cron && t.is_enabled())
        .then_some(t.next_fire_at.as_ref())
        .flatten()
        .map(ts)
}

const FIRE_COLUMNS: &str = "trigger_id, fire_key, tenant_id, kind, scheduled_at, event_id, \
     signature_digest, invocation_id, outcome, reason, created_at";

type RawFire = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    String,
);

fn raw_fire(r: &Row<'_>) -> rusqlite::Result<RawFire> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
        r.get(9)?,
        r.get(10)?,
    ))
}

fn fire_of(raw: RawFire) -> Result<FireRecord, RepoError> {
    let (trigger, key, tenant, kind, scheduled, event, sig, inv, outcome, reason, created) = raw;
    Ok(FireRecord {
        trigger_id: TriggerId::parse(&trigger).map_err(ser)?,
        tenant_id: TenantId::parse(&tenant).map_err(ser)?,
        fire_key: key,
        kind: kind_of(&kind)?,
        scheduled_at: scheduled.as_deref().map(parse_ts).transpose()?,
        event_id: event,
        signature_digest: sig,
        invocation_id: inv
            .as_deref()
            .map(InvocationId::parse)
            .transpose()
            .map_err(ser)?,
        outcome: outcome_of(&outcome)?,
        reason,
        created_at: parse_ts(&created)?,
    })
}

fn get_fire_in(
    c: &Connection,
    trigger: &TriggerId,
    key: &str,
) -> Result<Option<FireRecord>, RepoError> {
    c.prepare_cached(&format!(
        "SELECT {FIRE_COLUMNS} FROM trigger_fires WHERE trigger_id = ?1 AND fire_key = ?2"
    ))?
    .query_row(params![trigger.as_str(), key], raw_fire)
    .optional()?
    .map(fire_of)
    .transpose()
}

fn fire_by_signature_in(
    c: &Connection,
    trigger: &TriggerId,
    digest: &str,
) -> Result<Option<FireRecord>, RepoError> {
    c.prepare_cached(&format!(
        "SELECT {FIRE_COLUMNS} FROM trigger_fires WHERE trigger_id = ?1 AND signature_digest = ?2"
    ))?
    .query_row(params![trigger.as_str(), digest], raw_fire)
    .optional()?
    .map(fire_of)
    .transpose()
}

fn insert_fire_in(c: &Connection, f: &FireRecord) -> Result<usize, RepoError> {
    Ok(c.prepare_cached(&format!(
        "INSERT OR IGNORE INTO trigger_fires ({FIRE_COLUMNS}) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
    ))?
    .execute(params![
        f.trigger_id.as_str(),
        f.fire_key,
        f.tenant_id.as_str(),
        f.kind.as_str(),
        f.scheduled_at.as_ref().map(ts),
        f.event_id,
        f.signature_digest,
        f.invocation_id.as_ref().map(|i| i.as_str().to_string()),
        f.outcome.as_str(),
        f.reason.as_deref().map(|r| truncate(r, 512).to_string()),
        ts(&f.created_at)
    ])?)
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

impl TriggerRepository for SqliteStore {
    fn insert_trigger(
        &self,
        trigger: Trigger,
        sealed_secret: Option<Vec<u8>>,
        max_per_function: usize,
    ) -> Result<(), RepoError> {
        self.write(|tx| {
            let live: i64 = tx
                .prepare_cached(
                    "SELECT COUNT(*) FROM triggers WHERE function_id = ?1 AND status != 'deleted'",
                )?
                .query_row([trigger.function_id.as_str()], |r| r.get(0))?;
            if live as usize >= max_per_function {
                return Err(RepoError::Refused(format!(
                    "function {} already has {live} triggers (max {max_per_function})",
                    trigger.function_id
                )));
            }
            tx.prepare_cached(
                "INSERT INTO triggers (id, tenant_id, function_id, kind, status, generation, \
                 next_fire_at, secret_sealed, created_at, updated_at, body) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )?
            .execute(params![
                trigger.id.as_str(),
                trigger.tenant_id.as_str(),
                trigger.function_id.as_str(),
                trigger.kind().as_str(),
                trigger.status.as_str(),
                big(trigger.generation, "trigger generation")?,
                next_fire_column(&trigger),
                sealed_secret,
                ts(&trigger.created_at),
                ts(&trigger.updated_at),
                to_json(&trigger)?
            ])?;
            Ok(())
        })
    }

    fn get_trigger(&self, id: &TriggerId) -> Result<Option<Trigger>, RepoError> {
        self.read(|c| get_trigger_in(c, id))
    }

    fn list_triggers(&self, function: &FunctionId) -> Result<Vec<Trigger>, RepoError> {
        self.read(|c| {
            let bodies: Vec<String> = c
                .prepare_cached(
                    "SELECT body FROM triggers WHERE function_id = ?1 AND status != 'deleted' \
                     ORDER BY created_at, id",
                )?
                .query_map([function.as_str()], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            bodies.iter().map(|b| from_json(b)).collect()
        })
    }

    fn trigger_secret(&self, id: &TriggerId) -> Result<Option<Vec<u8>>, RepoError> {
        self.read(|c| {
            Ok(c.prepare_cached(
                "SELECT secret_sealed FROM triggers WHERE id = ?1 AND status != 'deleted'",
            )?
            .query_row([id.as_str()], |r| r.get::<_, Option<Vec<u8>>>(0))
            .optional()?
            .flatten())
        })
    }

    fn update_trigger(
        &self,
        trigger: Trigger,
        expected_generation: u64,
        sealed_secret: Option<Vec<u8>>,
    ) -> Result<bool, RepoError> {
        if trigger.generation != expected_generation + 1 {
            return Err(RepoError::Refused(
                "a trigger update carries expected_generation + 1".into(),
            ));
        }
        self.write(|tx| {
            let Some(old) = get_trigger_in(tx, &trigger.id)? else {
                return Err(RepoError::NotFound(format!("trigger {}", trigger.id)));
            };
            if old.tenant_id != trigger.tenant_id
                || old.function_id != trigger.function_id
                || old.kind() != trigger.kind()
            {
                return Err(RepoError::Refused(
                    "a trigger keeps its tenant, function and kind".into(),
                ));
            }
            if old.status == TriggerStatus::Deleted {
                return Ok(false);
            }
            let n = tx
                .prepare_cached(
                    "UPDATE triggers SET status = ?1, generation = ?2, next_fire_at = ?3, \
                     updated_at = ?4, body = ?5 WHERE id = ?6 AND generation = ?7 \
                     AND status != 'deleted'",
                )?
                .execute(params![
                    trigger.status.as_str(),
                    big(trigger.generation, "trigger generation")?,
                    next_fire_column(&trigger),
                    ts(&trigger.updated_at),
                    to_json(&trigger)?,
                    trigger.id.as_str(),
                    big(expected_generation, "trigger generation")?
                ])?;
            if n != 1 {
                return Ok(false);
            }
            if trigger.status == TriggerStatus::Deleted {
                tx.execute(
                    "UPDATE triggers SET secret_sealed = NULL WHERE id = ?1",
                    [trigger.id.as_str()],
                )?;
            } else if let Some(sealed) = sealed_secret {
                tx.execute(
                    "UPDATE triggers SET secret_sealed = ?1 WHERE id = ?2",
                    params![sealed, trigger.id.as_str()],
                )?;
            }
            Ok(true)
        })
    }

    fn due_cron_triggers(&self, now: Timestamp, limit: usize) -> Result<Vec<Trigger>, RepoError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.read(|c| {
            let bodies: Vec<String> = c
                .prepare_cached(
                    "SELECT body FROM triggers WHERE kind = 'cron' AND status = 'enabled' \
                     AND next_fire_at IS NOT NULL AND next_fire_at <= ?1 \
                     ORDER BY next_fire_at, id LIMIT ?2",
                )?
                .query_map(params![ts(&now), limit], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            bodies.iter().map(|b| from_json(b)).collect()
        })
    }

    fn next_cron_fire_at(&self) -> Result<Option<Timestamp>, RepoError> {
        let raw: Option<String> = self.read(|c| {
            Ok(c.prepare_cached(
                "SELECT MIN(next_fire_at) FROM triggers WHERE kind = 'cron' \
                 AND status = 'enabled' AND next_fire_at IS NOT NULL",
            )?
            .query_row([], |r| r.get(0))?)
        })?;
        raw.as_deref().map(parse_ts).transpose()
    }

    fn advance_cron_cursor(
        &self,
        id: &TriggerId,
        generation: u64,
        next_fire_at: Timestamp,
        last_scheduled_at: Option<Timestamp>,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let Some(mut t) = get_trigger_in(tx, id)? else {
                return Ok(false);
            };
            if t.generation != generation || !t.is_enabled() || t.kind() != TriggerKind::Cron {
                return Ok(false);
            }
            // The cursor never moves backwards.
            if t.next_fire_at.is_some_and(|cur| next_fire_at < cur) {
                return Ok(false);
            }
            t.next_fire_at = Some(next_fire_at);
            if let Some(last) = last_scheduled_at
                && t.last_scheduled_at.is_none_or(|prev| last > prev)
            {
                t.last_scheduled_at = Some(last);
            }
            Ok(write_trigger_columns(tx, &t)? == 1)
        })
    }

    fn accept_trigger_fire(
        &self,
        invocation: Invocation,
        input: AsyncInput,
        event: OutboxEvent,
        backlog: BacklogLimits,
        claim: FireClaim,
        before_commit: &dyn Fn() -> bool,
    ) -> Result<TriggerAcceptOutcome, RepoError> {
        check_accept(&invocation, &input, &event)?;
        let record = &claim.record;
        if record.invocation_id.as_ref() != Some(&invocation.id)
            || record.tenant_id != invocation.tenant_id
            || record.outcome != FireOutcome::Accepted
        {
            return Err(RepoError::Refused(
                "a fire claim names its accepted invocation and tenant".into(),
            ));
        }
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            // 1. The trigger as it is now: gone, not enabled or changed since
            //    the caller read it fires nothing.
            let current = get_trigger_in(tx, &record.trigger_id)?;
            let active = current.as_ref().is_some_and(|t| {
                t.is_enabled()
                    && t.tenant_id == invocation.tenant_id
                    && t.function_id == invocation.function_id
                    && claim.expected_generation.is_none_or(|g| g == t.generation)
            });
            if !active {
                return Ok(TriggerAcceptOutcome::Inactive(current.map(Box::new)));
            }
            // 2. The same scheduled time / event / signed delivery again.
            if let Some(existing) = get_fire_in(tx, &record.trigger_id, &record.fire_key)? {
                return Ok(TriggerAcceptOutcome::AlreadyFired(existing));
            }
            if let Some(digest) = &record.signature_digest
                && let Some(existing) = fire_by_signature_in(tx, &record.trigger_id, digest)?
            {
                return Ok(TriggerAcceptOutcome::AlreadyFired(existing));
            }
            // 3. The asynchronous acceptance rows (ADR-0010 §1.6).
            match accept_in(tx, &invocation, &input, &event, backlog, max, retention)? {
                AsyncAcceptOutcome::Accepted => {}
                AsyncAcceptOutcome::Existing(b) => return Ok(TriggerAcceptOutcome::Existing(b)),
                AsyncAcceptOutcome::Backlog(s) => return Ok(TriggerAcceptOutcome::Backlog(s)),
            }
            // 4. The fire row. The primary key makes a concurrent winner a
            //    constraint violation that rolls everything back.
            if insert_fire_in(tx, record)? != 1 {
                return Err(RepoError::Conflict(format!(
                    "fire {} of trigger {} was recorded concurrently",
                    record.fire_key, record.trigger_id
                )));
            }
            if before_commit() {
                return Err(RepoError::Store(
                    "failpoint accept.before_commit: the transaction is rolled back".into(),
                ));
            }
            Ok(TriggerAcceptOutcome::Accepted)
        })
    }

    fn record_fire(&self, record: FireRecord) -> Result<bool, RepoError> {
        self.write(|tx| Ok(insert_fire_in(tx, &record)? == 1))
    }

    fn get_fire(
        &self,
        trigger: &TriggerId,
        fire_key: &str,
    ) -> Result<Option<FireRecord>, RepoError> {
        self.read(|c| get_fire_in(c, trigger, fire_key))
    }

    fn fire_by_signature(
        &self,
        trigger: &TriggerId,
        signature_digest: &str,
    ) -> Result<Option<FireRecord>, RepoError> {
        self.read(|c| fire_by_signature_in(c, trigger, signature_digest))
    }

    fn list_fires(&self, trigger: &TriggerId, limit: usize) -> Result<Vec<FireRecord>, RepoError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.read(|c| {
            let raws: Vec<RawFire> = c
                .prepare_cached(&format!(
                    "SELECT {FIRE_COLUMNS} FROM trigger_fires WHERE trigger_id = ?1 \
                     ORDER BY created_at DESC, fire_key DESC LIMIT ?2"
                ))?
                .query_map(params![trigger.as_str(), limit], raw_fire)?
                .collect::<Result<_, _>>()?;
            raws.into_iter().map(fire_of).collect()
        })
    }

    fn purge_fires(
        &self,
        webhook_before: Timestamp,
        cron_before: Timestamp,
    ) -> Result<usize, RepoError> {
        self.write(|tx| {
            let a = tx.execute(
                "DELETE FROM trigger_fires WHERE kind = 'webhook' AND created_at < ?1",
                [ts(&webhook_before)],
            )?;
            let b = tx.execute(
                "DELETE FROM trigger_fires WHERE kind = 'cron' AND created_at < ?1",
                [ts(&cron_before)],
            )?;
            Ok(a + b)
        })
    }

    fn acquire_scheduler_lease(
        &self,
        owner: &str,
        now: Timestamp,
        ttl: chrono::Duration,
    ) -> Result<bool, RepoError> {
        let (now_s, expires) = (ts(&now), ts(&(now + ttl)));
        // Every gateway asks every scheduler tick; only the holder (or a
        // taker of an expired / abandoned lease) has anything to write. The
        // others learn "not yours" from a read and never take the write lock
        // (PLT-4646). The write below decides again.
        let held_by_another_live: bool = self.read(|c| {
            let current: Option<(String, String)> = c
                .prepare_cached(
                    "SELECT owner_id, expires_at FROM trigger_scheduler WHERE name = 'cron'",
                )?
                .query_row([], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            Ok(match current {
                Some((o, exp)) if o != owner && exp.as_str() > now_s.as_str() => c
                    .prepare_cached(
                        "SELECT 1 FROM dispatchers WHERE id = ?1 \
                         AND (stopped_at IS NOT NULL OR reclaimed_at IS NOT NULL)",
                    )?
                    .query_row([o.as_str()], |r| r.get::<_, i64>(0))
                    .optional()?
                    .is_none(),
                _ => false,
            })
        })?;
        if held_by_another_live {
            return Ok(false);
        }
        self.write(|tx| {
            let current: Option<(String, String)> = tx
                .prepare_cached(
                    "SELECT owner_id, expires_at FROM trigger_scheduler WHERE name = 'cron'",
                )?
                .query_row([], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            let take = match &current {
                None => true,
                Some((o, _)) if o == owner => true,
                Some((_, exp)) if exp.as_str() <= now_s.as_str() => true,
                Some((o, _)) => {
                    // A dispatcher that stopped or was reclaimed owns nothing.
                    let gone: Option<i64> = tx
                        .prepare_cached(
                            "SELECT 1 FROM dispatchers WHERE id = ?1 \
                             AND (stopped_at IS NOT NULL OR reclaimed_at IS NOT NULL)",
                        )?
                        .query_row([o.as_str()], |r| r.get(0))
                        .optional()?;
                    gone.is_some()
                }
            };
            if !take {
                return Ok(false);
            }
            let acquired_at = match &current {
                Some((o, _)) if o == owner => None,
                _ => Some(now_s.clone()),
            };
            tx.prepare_cached(
                "INSERT INTO trigger_scheduler (name, owner_id, acquired_at, expires_at) \
                 VALUES ('cron', ?1, ?2, ?3) ON CONFLICT (name) DO UPDATE SET \
                 owner_id = excluded.owner_id, expires_at = excluded.expires_at, \
                 acquired_at = COALESCE(?4, trigger_scheduler.acquired_at)",
            )?
            .execute(params![owner, now_s, expires, acquired_at])?;
            Ok(true)
        })
    }

    fn release_scheduler_lease(&self, owner: &str) -> Result<(), RepoError> {
        self.write(|tx| {
            tx.execute(
                "DELETE FROM trigger_scheduler WHERE name = 'cron' AND owner_id = ?1",
                [owner],
            )?;
            Ok(())
        })
    }
}

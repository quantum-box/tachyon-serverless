//! [`SlotStore`] on SQLite (PLT-4631).
//!
//! Every operation is one `BEGIN IMMEDIATE` transaction, so writers in other
//! processes on the same file are serialized by SQLite's lock, and every
//! write also carries its CAS predicate in the `UPDATE ... WHERE`
//! (`epoch = ?`, `state = ?`, `released = 0`, `reclaimed_at IS NULL`), so the
//! semantics do not depend on that lock alone: zero affected rows is a lost
//! race and nothing else of the transaction is written.

use std::collections::BTreeMap;

use rusqlite::{Connection, params};

use tachyon_serverless_domain::{
    AttemptId, DispatcherId, EnvironmentId, EnvironmentState, ExecutionEnvironment, ExecutionLease,
    Invocation, InvocationAttempt, LeaseId, ReuseKey, Timestamp,
};

use super::super::guard::{self, Write};
use super::super::restart::{self, Cause};
use super::super::slot::{
    AcquireOutcome, CompletionOutcome, DispatcherRecord, HeartbeatOutcome, ReclaimReport,
    ReclaimRequest, SlotAcquire, SlotCompletion, SlotStore, acquire_preconditions,
    lease_is_current,
};
use super::super::{PoolLimits, RepoError};
use super::{
    REUSE_KEY_MATCH, Retention, SqliteStore, bodies, body, cas_environment_row, duplicate, exists,
    get_attempt, get_environment, get_invocation, get_lease, insert_attempt_row, insert_lease_row,
    reuse_key_params, to_json, ts, update_attempt_row, update_invocation_row, write_lease_row,
};

fn get_dispatcher(
    c: &Connection,
    id: &DispatcherId,
) -> Result<Option<DispatcherRecord>, RepoError> {
    body(
        c,
        "SELECT body FROM dispatchers WHERE id = ?1",
        [id.as_str()],
    )
}

fn write_dispatcher(c: &Connection, d: &DispatcherRecord) -> Result<(), RepoError> {
    c.prepare_cached(
        "UPDATE dispatchers SET lease_expires_at = ?1, stopped_at = ?2, reclaimed_at = ?3, body = ?4 \
         WHERE id = ?5",
    )?
    .execute(params![
        ts(&d.lease_expires_at),
        d.stopped_at.as_ref().map(ts),
        d.reclaimed_at.as_ref().map(ts),
        to_json(d)?,
        d.id.as_str()
    ])?;
    Ok(())
}

fn owner_value(owner: Option<&DispatcherId>) -> rusqlite::types::Value {
    match owner {
        Some(o) => rusqlite::types::Value::Text(o.to_string()),
        None => rusqlite::types::Value::Null,
    }
}

fn has_unreleased_lease(c: &Connection, env: &EnvironmentId) -> Result<bool, RepoError> {
    let hit: Option<i64> = rusqlite::OptionalExtension::optional(
        c.prepare_cached(
            "SELECT 1 FROM leases WHERE environment_id = ?1 AND released = 0 LIMIT 1",
        )?
        .query_row([env.as_str()], |r| r.get(0)),
    )?;
    Ok(hit.is_some())
}

/// Fence `env` (if it is not terminal and not fenced yet) and record it.
fn fence(
    tx: &Connection,
    mut env: ExecutionEnvironment,
    now: Timestamp,
    report: &mut ReclaimReport,
) -> Result<(), RepoError> {
    if env.is_terminal() || env.is_fenced() {
        return Ok(());
    }
    let before = env.epoch;
    if env.fence(now).is_ok() && cas_environment_row(tx, &env, before, None)? {
        report.fenced.push(env);
    }
    Ok(())
}

fn settle_invocation(
    tx: &Connection,
    mut inv: Invocation,
    cause: Cause,
    retention: Retention,
    now: Timestamp,
    report: &mut ReclaimReport,
) -> Result<(), RepoError> {
    if restart::settle_invocation_with(&mut inv, cause, now) {
        update_invocation_row(tx, &inv, retention)?;
        report.invocations += 1;
    }
    let unknown = restart::is_outcome_unknown(&inv);
    let attempts: Vec<InvocationAttempt> = bodies(
        tx,
        "SELECT body FROM attempts WHERE invocation_id = ?1 AND terminal = 0 ORDER BY id",
        [inv.id.as_str()],
    )?;
    for mut att in attempts {
        if restart::settle_attempt_with(&mut att, unknown, cause, now) {
            update_attempt_row(tx, &att)?;
            report.attempts += 1;
        }
    }
    Ok(())
}

impl SlotStore for SqliteStore {
    fn register_dispatcher(&self, record: DispatcherRecord) -> Result<(), RepoError> {
        self.write(|tx| {
            if get_dispatcher(tx, &record.id)?.is_some() {
                return Err(duplicate("dispatcher", &record.id));
            }
            tx.execute(
                "INSERT INTO dispatchers (id, instance, hostname, pid, started_at, lease_expires_at, \
                 stopped_at, reclaimed_at, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    record.id.as_str(),
                    record.instance,
                    record.hostname,
                    i64::from(record.pid),
                    ts(&record.started_at),
                    ts(&record.lease_expires_at),
                    record.stopped_at.as_ref().map(ts),
                    record.reclaimed_at.as_ref().map(ts),
                    to_json(&record)?
                ],
            )?;
            Ok(())
        })
    }

    fn get_dispatcher(&self, id: &DispatcherId) -> Result<Option<DispatcherRecord>, RepoError> {
        self.read(|c| get_dispatcher(c, id))
    }

    fn list_dispatchers(&self) -> Result<Vec<DispatcherRecord>, RepoError> {
        self.read(|c| bodies(c, "SELECT body FROM dispatchers ORDER BY id", []))
    }

    fn heartbeat(
        &self,
        id: &DispatcherId,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<HeartbeatOutcome, RepoError> {
        self.write(|tx| {
            let Some(mut d) = get_dispatcher(tx, id)? else {
                return Ok(HeartbeatOutcome::Fenced);
            };
            // Only while unexpired: once the lease has passed, another
            // dispatcher may already be reclaiming it.
            if !d.is_live() || now >= d.lease_expires_at {
                return Ok(HeartbeatOutcome::Fenced);
            }
            d.heartbeat_at = now;
            d.lease_expires_at = d.lease_expires_at.max(now + ttl);
            let n = tx
                .prepare_cached(
                    "UPDATE dispatchers SET lease_expires_at = ?1, body = ?2 \
                     WHERE id = ?3 AND stopped_at IS NULL AND reclaimed_at IS NULL",
                )?
                .execute(params![ts(&d.lease_expires_at), to_json(&d)?, id.as_str()])?;
            if n != 1 {
                return Ok(HeartbeatOutcome::Fenced);
            }
            let leases: Vec<ExecutionLease> = bodies(
                tx,
                "SELECT body FROM leases WHERE owner_id = ?1 AND released = 0 ORDER BY id",
                [id.as_str()],
            )?;
            let mut renewed = 0;
            for mut lease in leases {
                // An expired slot lease is not revived, even for a live owner.
                if lease.renew(now, ttl).is_ok() {
                    write_lease_row(tx, &lease, false)?;
                    renewed += 1;
                }
            }
            Ok(HeartbeatOutcome::Renewed { leases: renewed })
        })
    }

    fn stop_dispatcher(&self, id: &DispatcherId, now: Timestamp) -> Result<(), RepoError> {
        self.write(|tx| {
            if let Some(mut d) = get_dispatcher(tx, id)?
                && d.stopped_at.is_none()
            {
                d.stopped_at = Some(now);
                write_dispatcher(tx, &d)?;
            }
            Ok(())
        })
    }

    fn list_idle(
        &self,
        owner: Option<&DispatcherId>,
    ) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM environments WHERE state = 'idle' AND owner_id IS ?1 \
                 ORDER BY idle_since, id",
                [owner_value(owner)],
            )
        })
    }

    fn claim_for_reuse(
        &self,
        key: &ReuseKey,
        owner: Option<&DispatcherId>,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError> {
        let mut values = reuse_key_params(key)?.to_vec();
        values.push(owner_value(owner));
        self.write(|tx| {
            // Oldest matching idle environment of this owner by id (a ULID),
            // like the volatile store. Served by `environments_reuse_key`.
            let candidate: Option<ExecutionEnvironment> = body(
                tx,
                &format!(
                    "SELECT body FROM environments WHERE {REUSE_KEY_MATCH} AND owner_id IS ?9 \
                     ORDER BY id LIMIT 1"
                ),
                rusqlite::params_from_iter(values.iter()),
            )?;
            let Some(env) = candidate else {
                return Ok(None);
            };
            if &env.reuse_key != key || env.owner.as_ref() != owner {
                return Ok(None);
            }
            let mut claimed = env.clone();
            if claimed.reserve(now).is_err() {
                return Ok(None);
            }
            if cas_environment_row(tx, &claimed, env.epoch, Some("idle"))? {
                Ok(Some(claimed))
            } else {
                Ok(None)
            }
        })
    }

    fn release_to_pool(
        &self,
        env: &ExecutionEnvironment,
        limits: PoolLimits,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError> {
        let key_params = reuse_key_params(&env.reuse_key)?;
        self.write(|tx| {
            let Some(current) = get_environment(tx, &env.id)? else {
                return Ok(None);
            };
            // A `min_ready` pre-start (PLT-4635) is published straight from
            // `Ready` at epoch 0: it never served an attempt.
            let prestarted = matches!(current.state, EnvironmentState::Ready) && current.epoch == 0;
            if current.epoch != env.epoch
                || !(matches!(current.state, EnvironmentState::Busy) || prestarted)
                || current.is_fenced()
                || has_unreleased_lease(tx, &env.id)?
            {
                return Ok(None);
            }
            let total: i64 = tx.query_row(
                "SELECT COUNT(*) FROM environments WHERE state = 'idle'",
                [],
                |r| r.get(0),
            )?;
            let per_key: i64 = tx
                .prepare_cached(&format!(
                    "SELECT COUNT(*) FROM environments WHERE {REUSE_KEY_MATCH}"
                ))?
                .query_row(rusqlite::params_from_iter(key_params.iter()), |r| r.get(0))?;
            let (total, per_key) = (total as usize, per_key as usize);
            if total >= limits.max_total_idle || per_key >= limits.max_idle_per_key {
                return Ok(None);
            }
            let mut pooled = env.clone();
            if matches!(pooled.state, EnvironmentState::Ready) && pooled.mark_busy(now).is_err() {
                return Ok(None);
            }
            if pooled.mark_idle(now).is_err() {
                return Ok(None);
            }
            guard::environment_update(&current, &pooled)?;
            let expected = if prestarted { "ready" } else { "busy" };
            if cas_environment_row(tx, &pooled, current.epoch, Some(expected))? {
                Ok(Some(pooled))
            } else {
                Ok(None)
            }
        })
    }

    fn take_idle_for_termination(
        &self,
        id: &EnvironmentId,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let Some(mut env) = get_environment(tx, id)? else {
                return Ok(false);
            };
            if !matches!(env.state, EnvironmentState::Idle) || env.mark_draining(now).is_err() {
                return Ok(false);
            }
            cas_environment_row(tx, &env, env.epoch, Some("idle"))
        })
    }

    fn acquire(&self, request: SlotAcquire) -> Result<AcquireOutcome, RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let stored = get_environment(tx, &request.env.id)?;
            if let Some(reason) = acquire_preconditions(&request, stored.as_ref())? {
                return Ok(AcquireOutcome::Lost(reason));
            }
            let Some(stored) = stored else {
                return Err(RepoError::NotFound(format!(
                    "environment {}",
                    request.env.id
                )));
            };
            let SlotAcquire {
                env,
                expected_epoch,
                lease,
                attempt,
                invocation,
            } = &request;
            let owner = lease.owner.as_ref().expect("checked by the preconditions");
            match get_dispatcher(tx, owner)? {
                Some(d) if d.is_live() => {}
                Some(_) => {
                    return Ok(AcquireOutcome::Lost(format!(
                        "dispatcher {owner} is stopped or was reclaimed"
                    )));
                }
                None => {
                    return Ok(AcquireOutcome::Lost(format!(
                        "dispatcher {owner} is not registered"
                    )));
                }
            }
            if has_unreleased_lease(tx, &env.id)? {
                return Ok(AcquireOutcome::Lost(format!(
                    "environment {} already has an unreleased lease",
                    env.id
                )));
            }
            if let Some(inv) = invocation {
                let Some(old) = get_invocation(tx, &inv.id)? else {
                    return Err(RepoError::NotFound(format!("invocation {}", inv.id)));
                };
                if old.status.is_terminal() {
                    return Ok(AcquireOutcome::Lost(format!(
                        "invocation {} is already {}",
                        inv.id,
                        old.status.name()
                    )));
                }
                guard::invocation_update(&old, inv, max)?;
            }
            if exists(tx, "attempts", attempt.id.as_str())? {
                return Err(duplicate("attempt", &attempt.id));
            }
            if exists(tx, "leases", lease.id.as_str())? {
                return Err(duplicate("lease", &lease.id));
            }
            guard::attempt_insert(
                get_invocation(tx, &attempt.invocation_id)?.as_ref(),
                attempt,
            )?;
            guard::lease_insert(Some(&stored), lease)?;
            // The CAS: nothing is written unless the row is still where the
            // caller read it.
            if !cas_environment_row(tx, env, *expected_epoch, Some(stored.state.name()))? {
                return Ok(AcquireOutcome::Lost(format!(
                    "environment {} changed concurrently",
                    env.id
                )));
            }
            insert_lease_row(tx, lease)?;
            insert_attempt_row(tx, attempt)?;
            if let Some(inv) = invocation {
                update_invocation_row(tx, inv, retention)?;
            }
            Ok(AcquireOutcome::Acquired)
        })
    }

    fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError> {
        self.read(|c| get_lease(c, id))
    }

    fn renew_lease(
        &self,
        id: &LeaseId,
        owner: &DispatcherId,
        epoch: u64,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let Some(mut lease) = get_lease(tx, id)? else {
                return Ok(false);
            };
            if lease.owner.as_ref() != Some(owner) || lease.epoch != epoch {
                return Ok(false);
            }
            if lease.renew(now, ttl).is_err() {
                return Ok(false);
            }
            write_lease_row(tx, &lease, false)?;
            Ok(true)
        })
    }

    fn complete(&self, completion: SlotCompletion) -> Result<CompletionOutcome, RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let SlotCompletion {
                lease_id,
                attempt,
                invocation,
                now,
            } = &completion;
            let lease = get_lease(tx, lease_id)?;
            let env = match &lease {
                Some(l) => get_environment(tx, &l.environment_id)?,
                None => None,
            };
            if let Some(reason) =
                lease_is_current(lease.as_ref(), &attempt.id, attempt.epoch, env.as_ref())
            {
                return Ok(CompletionOutcome::Stale(reason));
            }
            let Some(old_attempt) = get_attempt(tx, &attempt.id)? else {
                return Err(RepoError::NotFound(format!("attempt {}", attempt.id)));
            };
            if old_attempt.status.is_terminal() {
                return Ok(CompletionOutcome::Stale(format!(
                    "attempt {} is already settled",
                    attempt.id
                )));
            }
            let attempt_write = guard::attempt_update(&old_attempt, attempt)?;
            let mut invocation_write = None;
            if let Some(inv) = invocation {
                let Some(old) = get_invocation(tx, &inv.id)? else {
                    return Err(RepoError::NotFound(format!("invocation {}", inv.id)));
                };
                if old.status.is_terminal() {
                    return Ok(CompletionOutcome::Stale(format!(
                        "invocation {} is already {}",
                        inv.id,
                        old.status.name()
                    )));
                }
                invocation_write = Some(guard::invocation_update(&old, inv, max)?);
            }
            let mut lease = lease.expect("checked by lease_is_current");
            lease
                .release(*now)
                .map_err(|e| RepoError::Refused(e.to_string()))?;
            write_lease_row(tx, &lease, false)?;
            if attempt_write == Write::Apply {
                update_attempt_row(tx, attempt)?;
            }
            if let (Some(inv), Some(Write::Apply)) = (invocation, invocation_write) {
                update_invocation_row(tx, inv, retention)?;
            }
            Ok(CompletionOutcome::Accepted)
        })
    }

    fn release_lease(
        &self,
        id: &LeaseId,
        attempt: &AttemptId,
        epoch: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let lease = get_lease(tx, id)?;
            let env = match &lease {
                Some(l) => get_environment(tx, &l.environment_id)?,
                None => None,
            };
            if lease_is_current(lease.as_ref(), attempt, epoch, env.as_ref()).is_some() {
                return Ok(false);
            }
            let mut lease = lease.expect("checked by lease_is_current");
            if lease.release(now).is_err() {
                return Ok(false);
            }
            write_lease_row(tx, &lease, false)?;
            Ok(true)
        })
    }

    fn reclaim_expired(&self, request: ReclaimRequest) -> Result<ReclaimReport, RepoError> {
        let retention = self.retention();
        self.write(|tx| {
            let ReclaimRequest {
                reclaimer,
                now,
                skew,
                presumed_dead,
            } = &request;
            let now = *now;
            let mut report = ReclaimReport::default();

            // 1. Dispatchers that can no longer be trusted, marked once.
            let dispatchers: Vec<DispatcherRecord> =
                bodies(tx, "SELECT body FROM dispatchers ORDER BY id", [])?;
            let mut dead: BTreeMap<DispatcherId, Cause> = BTreeMap::new();
            for mut d in dispatchers {
                if &d.id == reclaimer {
                    continue;
                }
                if d.reclaimed_at.is_some() {
                    dead.insert(d.id.clone(), cause_of(&d, false));
                    continue;
                }
                let proven = presumed_dead.contains(&d.id);
                if d.stopped_at.is_none() && !proven && !d.is_expired(now, *skew) {
                    continue;
                }
                let cause = cause_of(&d, proven);
                d.reclaimed_at = Some(now);
                let n = tx
                    .prepare_cached(
                        "UPDATE dispatchers SET reclaimed_at = ?1, body = ?2 \
                         WHERE id = ?3 AND reclaimed_at IS NULL",
                    )?
                    .execute(params![ts(&now), to_json(&d)?, d.id.as_str()])?;
                if n == 1 {
                    report.dispatchers.push(d.id.clone());
                }
                dead.insert(d.id, cause);
            }

            // 2. Slot leases: of a dead owner, or past their own expiry.
            let leases: Vec<ExecutionLease> = bodies(
                tx,
                "SELECT body FROM leases WHERE released = 0 AND owner_id IS NOT NULL ORDER BY id",
                [],
            )?;
            for mut lease in leases {
                let Some(owner) = lease.owner.clone() else {
                    continue;
                };
                if &owner == reclaimer {
                    continue;
                }
                let cause = match dead.get(&owner) {
                    Some(cause) => *cause,
                    None if lease.is_expired(now, *skew) => Cause::LEASE_EXPIRED,
                    None => continue,
                };
                if lease.release(now).is_err() {
                    continue;
                }
                write_lease_row(tx, &lease, true)?;
                report.leases += 1;
                if let Some(mut att) = get_attempt(tx, &lease.attempt_id)? {
                    if restart::settle_attempt_with(&mut att, true, cause, now) {
                        update_attempt_row(tx, &att)?;
                        report.attempts += 1;
                    }
                    if let Some(inv) = get_invocation(tx, &att.invocation_id)?
                        && !inv.status.is_terminal()
                        && inv.attempt_ids.last() == Some(&lease.attempt_id)
                    {
                        settle_invocation(tx, inv, cause, retention, now, &mut report)?;
                    }
                }
                if let Some(env) = get_environment(tx, &lease.environment_id)?
                    && env.epoch == lease.epoch
                {
                    fence(tx, env, now, &mut report)?;
                }
            }

            // 3. Everything else a dead dispatcher owned.
            for (owner, cause) in &dead {
                let invocations: Vec<Invocation> = bodies(
                    tx,
                    "SELECT body FROM invocations WHERE owner_id = ?1 AND terminal = 0 ORDER BY id",
                    [owner.as_str()],
                )?;
                for inv in invocations {
                    settle_invocation(tx, inv, *cause, retention, now, &mut report)?;
                }
                let envs: Vec<ExecutionEnvironment> = bodies(
                    tx,
                    "SELECT body FROM environments WHERE owner_id = ?1 AND terminal = 0 \
                     AND fenced = 0 ORDER BY id",
                    [owner.as_str()],
                )?;
                for env in envs {
                    fence(tx, env, now, &mut report)?;
                }
            }
            Ok(report)
        })
    }

    fn list_fenced(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM environments WHERE fenced = 1 AND terminal = 0 ORDER BY id",
                [],
            )
        })
    }

    fn confirm_terminated(
        &self,
        id: &EnvironmentId,
        epoch: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let Some(mut env) = get_environment(tx, id)? else {
                return Ok(false);
            };
            if !env.is_fenced() || env.is_terminal() || env.epoch != epoch {
                return Ok(false);
            }
            if env.mark_lost(FENCED_TERMINATED, now).is_err() {
                return Ok(false);
            }
            cas_environment_row(tx, &env, epoch, Some("draining"))
        })
    }
}

/// Reason recorded on a fenced environment once its terminate is confirmed.
pub(crate) const FENCED_TERMINATED: &str =
    "owner lost its lease; environment fenced and terminate confirmed";

fn cause_of(d: &DispatcherRecord, proven_dead: bool) -> Cause {
    if proven_dead || d.stopped_at.is_some() {
        Cause::RESTARTED
    } else {
        Cause::LEASE_EXPIRED
    }
}

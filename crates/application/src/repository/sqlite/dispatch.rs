//! [`AsyncDispatchRepository`] on `state.db` (migration 008, PLT-4640). Every
//! method is one `BEGIN IMMEDIATE` transaction, and every write also carries
//! its CAS predicate in the `UPDATE ... WHERE`, so claims and settles are
//! serialized across every process that opens the file.

use rusqlite::{Connection, OptionalExtension, Row, params};

use tachyon_serverless_domain::{
    DeadLetterId, FunctionId, Invocation, InvocationError, InvocationId, InvocationMode, TenantId,
    Timestamp,
};

use super::super::dispatch::{
    AsyncDispatchRepository, ClaimOutcome, ClaimRequest, DeadLetter, DeadLetterStatus,
    DispatchCandidate, DispatchFence, DispatchRecord, DispatchSettle, DispatchState, Redrive,
    RedriveWrite, SettleOutcome,
};
use super::super::guard::{self, Write};
use super::outbox::{
    EVENT_COLUMNS, event_of, insert_input_row, insert_outbox_row, parse_ts, raw_event, ser,
};
use super::{
    RepoError, SqliteStore, big, bodies, body, from_json, get_invocation,
    insert_invocation_checked, to_json, ts, update_invocation_row,
};

const RECORD_COLUMNS: &str = "invocation_id, tenant_id, function_id, state, generation, attempts, \
     deferrals, claimed_by, claim_expires_at, next_attempt_at, first_attempt_at, last_attempt_at, \
     updated_at, last_error";

type RawRecord = (
    String,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
);

fn raw_record(r: &Row<'_>) -> rusqlite::Result<RawRecord> {
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
        r.get(11)?,
        r.get(12)?,
        r.get(13)?,
    ))
}

fn opt_ts(raw: Option<String>) -> Result<Option<Timestamp>, RepoError> {
    raw.as_deref().map(parse_ts).transpose()
}

fn record_of(raw: RawRecord) -> Result<DispatchRecord, RepoError> {
    let (
        id,
        tenant,
        function,
        state,
        generation,
        attempts,
        deferrals,
        claimed_by,
        claim_expires_at,
        next_attempt_at,
        first_attempt_at,
        last_attempt_at,
        updated_at,
        last_error,
    ) = raw;
    Ok(DispatchRecord {
        invocation_id: InvocationId::parse(&id).map_err(ser)?,
        tenant_id: TenantId::parse(&tenant).map_err(ser)?,
        function_id: FunctionId::parse(&function).map_err(ser)?,
        state: DispatchState::parse(&state)
            .ok_or_else(|| RepoError::Serialization(format!("dispatch state `{state}`")))?,
        generation: generation.max(0) as u64,
        attempts: u32::try_from(attempts.max(0)).unwrap_or(u32::MAX),
        deferrals: u32::try_from(deferrals.max(0)).unwrap_or(u32::MAX),
        claimed_by,
        claim_expires_at: opt_ts(claim_expires_at)?,
        next_attempt_at: opt_ts(next_attempt_at)?,
        first_attempt_at: opt_ts(first_attempt_at)?,
        last_attempt_at: opt_ts(last_attempt_at)?,
        updated_at: parse_ts(&updated_at)?,
        last_error: last_error
            .as_deref()
            .map(from_json::<InvocationError>)
            .transpose()?,
    })
}

fn get_record(c: &Connection, id: &InvocationId) -> Result<Option<DispatchRecord>, RepoError> {
    let raw = c
        .prepare_cached(&format!(
            "SELECT {RECORD_COLUMNS} FROM async_dispatch WHERE invocation_id = ?1"
        ))?
        .query_row([id.as_str()], raw_record)
        .optional()?;
    raw.map(record_of).transpose()
}

/// Insert or replace the row (the caller has checked its CAS predicate in the
/// same transaction).
fn write_record(c: &Connection, r: &DispatchRecord) -> Result<(), RepoError> {
    c.prepare_cached("DELETE FROM async_dispatch WHERE invocation_id = ?1")?
        .execute([r.invocation_id.as_str()])?;
    c.prepare_cached(&format!(
        "INSERT INTO async_dispatch ({RECORD_COLUMNS}) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"
    ))?
    .execute(params![
        r.invocation_id.as_str(),
        r.tenant_id.as_str(),
        r.function_id.as_str(),
        r.state.as_str(),
        big(r.generation, "dispatch generation")?,
        i64::from(r.attempts),
        i64::from(r.deferrals),
        r.claimed_by,
        r.claim_expires_at.as_ref().map(ts),
        r.next_attempt_at.as_ref().map(ts),
        r.first_attempt_at.as_ref().map(ts),
        r.last_attempt_at.as_ref().map(ts),
        ts(&r.updated_at),
        r.last_error.as_ref().map(to_json).transpose()?,
    ])?;
    Ok(())
}

fn insert_dead_letter(c: &Connection, d: &DeadLetter) -> Result<bool, RepoError> {
    let key = d.origin_key();
    let existing: Option<String> = c
        .prepare_cached("SELECT id FROM dead_letters WHERE origin_key = ?1")?
        .query_row([key.as_str()], |r| r.get(0))
        .optional()?;
    if existing.is_some() {
        return Ok(false);
    }
    c.prepare_cached(
        "INSERT INTO dead_letters (id, tenant_id, function_id, invocation_id, reason, status, \
         origin_key, created_at, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?
    .execute(params![
        d.id.as_str(),
        d.tenant_id.as_str(),
        d.function_id.as_ref().map(|f| f.as_str()),
        d.invocation_id.as_ref().map(|i| i.as_str()),
        d.reason.as_str(),
        d.status.as_str(),
        key,
        ts(&d.created_at),
        to_json(d)?,
    ])?;
    Ok(true)
}

fn is_async_with_input(c: &Connection, inv: &Invocation) -> Result<bool, RepoError> {
    if inv.mode != InvocationMode::Async {
        return Ok(false);
    }
    let hit: Option<i64> = c
        .prepare_cached("SELECT 1 FROM invocation_inputs WHERE invocation_id = ?1")?
        .query_row([inv.id.as_str()], |r| r.get(0))
        .optional()?;
    Ok(hit.is_some())
}

impl AsyncDispatchRepository for SqliteStore {
    fn dispatch_record(
        &self,
        invocation: &InvocationId,
    ) -> Result<Option<DispatchRecord>, RepoError> {
        self.read(|c| get_record(c, invocation))
    }

    fn claim_dispatch(&self, request: ClaimRequest) -> Result<ClaimOutcome, RepoError> {
        self.write(|tx| {
            let ClaimRequest {
                invocation_id,
                owner,
                generation,
                now,
                ttl,
            } = &request;
            let Some(invocation) = get_invocation(tx, invocation_id)? else {
                return Err(RepoError::NotFound(format!("invocation {invocation_id}")));
            };
            if invocation.status.is_terminal() {
                return Ok(ClaimOutcome::Terminal(Box::new(invocation)));
            }
            if !is_async_with_input(tx, &invocation)? {
                return Err(RepoError::Refused(format!(
                    "invocation {invocation_id} is not an asynchronous invocation"
                )));
            }
            let mut record = match get_record(tx, invocation_id)? {
                Some(r) => r,
                None => DispatchRecord::unclaimed(&invocation),
            };
            if *generation != record.generation {
                return Ok(ClaimOutcome::Stale {
                    current: record.generation,
                });
            }
            if record.claim_live(*now) {
                return Ok(ClaimOutcome::Held {
                    owner: record.claimed_by.clone().unwrap_or_default(),
                    expires_at: record.claim_expires_at.unwrap_or(*now),
                });
            }
            if record.state == DispatchState::Scheduled
                && let Some(at) = record.next_attempt_at
                && at > *now
            {
                return Ok(ClaimOutcome::NotDue { at });
            }
            // A `running` row whose claim expired was abandoned by a run that
            // already counted: this claim is the next attempt.
            record.state = DispatchState::Running;
            record.claimed_by = Some(owner.clone());
            record.claim_expires_at = Some(*now + *ttl);
            record.attempts = record.attempts.saturating_add(1);
            record.first_attempt_at.get_or_insert(*now);
            record.last_attempt_at = Some(*now);
            record.updated_at = *now;
            write_record(tx, &record)?;
            Ok(ClaimOutcome::Claimed {
                invocation: Box::new(invocation),
                record: Box::new(record),
            })
        })
    }

    fn renew_dispatch_claim(
        &self,
        invocation: &InvocationId,
        owner: &str,
        attempts: u32,
        now: Timestamp,
        ttl: chrono::Duration,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let n = tx
                .prepare_cached(
                    "UPDATE async_dispatch SET claim_expires_at = ?1, updated_at = ?2 \
                     WHERE invocation_id = ?3 AND state = 'running' AND claimed_by = ?4 \
                     AND attempts = ?5",
                )?
                .execute(params![
                    ts(&(now + ttl)),
                    ts(&now),
                    invocation.as_str(),
                    owner,
                    i64::from(attempts)
                ])?;
            Ok(n == 1)
        })
    }

    fn settle_dispatch(&self, settle: DispatchSettle) -> Result<SettleOutcome, RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let DispatchSettle {
                fence,
                invocation,
                state,
                next_attempt_at,
                last_error,
                counted,
                dead_letter,
                republish,
                now,
            } = &settle;
            let Some(old) = get_invocation(tx, &invocation.id)? else {
                return Err(RepoError::NotFound(format!("invocation {}", invocation.id)));
            };
            if old.status.is_terminal() {
                return Ok(SettleOutcome::Lost(format!(
                    "invocation {} is already {}",
                    old.id,
                    old.status.name()
                )));
            }
            let mut record = match get_record(tx, &invocation.id)? {
                Some(r) => r,
                None => DispatchRecord::unclaimed(&old),
            };
            let from_claim = match fence {
                DispatchFence::Claim { owner, attempts } => {
                    if record.state != DispatchState::Running
                        || record.claimed_by.as_deref() != Some(owner.as_str())
                        || record.attempts != *attempts
                    {
                        return Ok(SettleOutcome::Lost(format!(
                            "the claim of invocation {} is no longer {owner}'s attempt {attempts}",
                            old.id
                        )));
                    }
                    true
                }
                DispatchFence::Unclaimed { generation } => {
                    if record.generation != *generation || record.claim_live(*now) {
                        return Ok(SettleOutcome::Lost(format!(
                            "invocation {} moved on (generation {}, state {})",
                            old.id, record.generation, record.state
                        )));
                    }
                    false
                }
            };
            if !invocation.status.is_terminal() && *state != DispatchState::Scheduled {
                return Err(RepoError::Refused(
                    "only a scheduled retry leaves the invocation non-terminal".into(),
                ));
            }
            if guard::invocation_update(&old, invocation, max)? == Write::Apply {
                update_invocation_row(tx, invocation, retention)?;
            }
            if !*counted {
                if from_claim {
                    record.attempts = record.attempts.saturating_sub(1);
                }
                record.deferrals = record.deferrals.saturating_add(1);
            }
            record.state = *state;
            record.claimed_by = None;
            record.claim_expires_at = None;
            record.next_attempt_at = *next_attempt_at;
            if last_error.is_some() {
                record.last_error = last_error.clone();
            }
            record.updated_at = *now;
            // The event of this generation is done with, published or not.
            tx.prepare_cached("DELETE FROM outbox WHERE event_id = ?1")?
                .execute([invocation.id.as_str()])?;
            if let Some(event) = republish {
                if event.event_id != invocation.id
                    || event.tenant_id != invocation.tenant_id
                    || event.generation != record.generation + 1
                {
                    return Err(RepoError::Refused(format!(
                        "the next event of invocation {} must be its generation {}",
                        invocation.id,
                        record.generation + 1
                    )));
                }
                record.generation = event.generation;
                insert_outbox_row(tx, event)?;
            }
            write_record(tx, &record)?;
            if let Some(d) = dead_letter {
                if d.tenant_id != invocation.tenant_id
                    || d.invocation_id.as_ref() != Some(&invocation.id)
                {
                    return Err(RepoError::Refused(
                        "a dead letter must name its own invocation and tenant".into(),
                    ));
                }
                insert_dead_letter(tx, d)?;
            }
            Ok(SettleOutcome::Committed)
        })
    }

    fn dispatch_candidates(
        &self,
        now: Timestamp,
        quiet_before: Timestamp,
        accepted_before: Timestamp,
        limit: usize,
    ) -> Result<Vec<DispatchCandidate>, RepoError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.read(|c| {
            // Non-terminal asynchronous invocations (they have an input row)
            // where: a claim expired; or no unpublished event exists and the
            // last one went out (or the last try became due) long ago; or the
            // invocation is past its maximum age.
            let invocations: Vec<Invocation> = bodies(
                c,
                "SELECT v.body FROM invocations v \
                 JOIN invocation_inputs i ON i.invocation_id = v.id \
                 LEFT JOIN async_dispatch d ON d.invocation_id = v.id \
                 LEFT JOIN outbox o ON o.event_id = v.id \
                 WHERE v.terminal = 0 AND ( \
                   (d.state = 'running' AND d.claim_expires_at <= ?1) \
                   OR (v.accepted_at <= ?3 AND (d.state IS NULL OR d.state <> 'running' \
                       OR d.claim_expires_at <= ?1)) \
                   OR ((d.state IS NULL OR d.state = 'scheduled') \
                       AND (o.event_id IS NULL OR o.sent = 1) \
                       AND (o.sent_at IS NULL OR o.sent_at <= ?2) \
                       AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?2) \
                       AND v.accepted_at <= ?2) \
                 ) ORDER BY v.accepted_at, v.id LIMIT ?4",
                params![ts(&now), ts(&quiet_before), ts(&accepted_before), limit],
            )?;
            let mut out = Vec::with_capacity(invocations.len());
            for invocation in invocations {
                let record = match get_record(c, &invocation.id)? {
                    Some(r) => r,
                    None => DispatchRecord::unclaimed(&invocation),
                };
                let outbox = c
                    .prepare_cached(&format!(
                        "SELECT {EVENT_COLUMNS} FROM outbox WHERE event_id = ?1"
                    ))?
                    .query_row([invocation.id.as_str()], raw_event)
                    .optional()?
                    .map(event_of)
                    .transpose()?;
                out.push(DispatchCandidate {
                    invocation,
                    record,
                    outbox,
                });
            }
            Ok(out)
        })
    }

    fn record_poison(&self, dead_letter: DeadLetter) -> Result<bool, RepoError> {
        if dead_letter.invocation_id.is_some() {
            return Err(RepoError::Refused(
                "a poison dead letter names no invocation".into(),
            ));
        }
        self.write(|tx| insert_dead_letter(tx, &dead_letter))
    }

    fn dead_letter(&self, id: &DeadLetterId) -> Result<Option<DeadLetter>, RepoError> {
        self.read(|c| {
            body(
                c,
                "SELECT body FROM dead_letters WHERE id = ?1",
                [id.as_str()],
            )
        })
    }

    fn dead_letter_of(&self, invocation: &InvocationId) -> Result<Option<DeadLetter>, RepoError> {
        self.read(|c| {
            body(
                c,
                "SELECT body FROM dead_letters WHERE invocation_id = ?1 ORDER BY created_at DESC LIMIT 1",
                [invocation.as_str()],
            )
        })
    }

    fn list_dead_letters(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, RepoError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM dead_letters WHERE tenant_id = ?1 AND function_id = ?2 \
                 ORDER BY created_at DESC, id DESC LIMIT ?3",
                params![tenant.as_str(), function.as_str(), limit],
            )
        })
    }

    fn redrives_of(&self, dead_letter: &DeadLetterId) -> Result<Vec<Redrive>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM redrives WHERE dead_letter_id = ?1 ORDER BY created_at, id",
                [dead_letter.as_str()],
            )
        })
    }

    fn redrive_creating(&self, invocation: &InvocationId) -> Result<Option<Redrive>, RepoError> {
        self.read(|c| {
            body(
                c,
                "SELECT body FROM redrives WHERE invocation_id = ?1",
                [invocation.as_str()],
            )
        })
    }

    fn redrive(&self, write: RedriveWrite) -> Result<(), RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let RedriveWrite {
                dead_letter_id,
                invocation,
                input,
                event,
                redrive,
            } = &write;
            let Some(mut dead) = body::<DeadLetter>(
                tx,
                "SELECT body FROM dead_letters WHERE id = ?1",
                [dead_letter_id.as_str()],
            )?
            else {
                return Err(RepoError::NotFound(format!("dead letter {dead_letter_id}")));
            };
            if dead.status != DeadLetterStatus::Open {
                return Err(RepoError::Conflict(format!(
                    "dead letter {dead_letter_id} is {}",
                    dead.status.as_str()
                )));
            }
            // Nothing of the redrive may cross the dead letter's tenant.
            if invocation.tenant_id != dead.tenant_id
                || input.tenant_id != dead.tenant_id
                || event.tenant_id != dead.tenant_id
                || redrive.tenant_id != dead.tenant_id
                || input.invocation_id != invocation.id
                || event.event_id != invocation.id
                || redrive.invocation_id != invocation.id
                || dead.function_id.as_ref() != Some(&invocation.function_id)
                || dead.invocation_id.as_ref() != Some(&redrive.source_invocation_id)
            {
                return Err(RepoError::Refused(
                    "a redrive must stay within its dead letter's tenant and function".into(),
                ));
            }
            let now = invocation.accepted_at;
            insert_invocation_checked(tx, invocation, max, retention)?;
            insert_input_row(tx, input, now)?;
            insert_outbox_row(tx, event)?;
            tx.prepare_cached(
                "INSERT INTO redrives (id, dead_letter_id, tenant_id, source_invocation_id, \
                 invocation_id, created_at, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?
            .execute(params![
                redrive.id.as_str(),
                redrive.dead_letter_id.as_str(),
                redrive.tenant_id.as_str(),
                redrive.source_invocation_id.as_str(),
                redrive.invocation_id.as_str(),
                ts(&redrive.created_at),
                to_json(redrive)?,
            ])?;
            dead.status = DeadLetterStatus::Redriven;
            dead.redrive_count = dead.redrive_count.saturating_add(1);
            dead.redriven_at = Some(now);
            let n = tx
                .prepare_cached(
                    "UPDATE dead_letters SET status = ?1, body = ?2 WHERE id = ?3 AND status = 'open'",
                )?
                .execute(params![
                    dead.status.as_str(),
                    to_json(&dead)?,
                    dead.id.as_str()
                ])?;
            if n != 1 {
                return Err(RepoError::Conflict(format!(
                    "dead letter {dead_letter_id} was redriven concurrently"
                )));
            }
            Ok(())
        })
    }
}

//! [`AsyncInvocationRepository`] on `state.db` (migration 006). Every method
//! is one `BEGIN IMMEDIATE` transaction, so claims, marks and acceptances are
//! serialized across every process that opens the file.

use rusqlite::{Connection, OptionalExtension, Row, params};

use tachyon_serverless_domain::{Invocation, InvocationId, InvocationStatus, TenantId, Timestamp};
use tachyon_serverless_durable_port::{ObjectId, ObjectRef, ObjectScope, Region};

use super::super::objects::check_attachable;
use super::super::outbox::{
    AsyncAcceptOutcome, AsyncInput, AsyncInputBody, AsyncInvocationRepository, BacklogLimits,
    OutboxEvent, OutboxStats,
};
use super::objects::attach_in;
use super::{
    RepoError, SqliteStore, big, bind_idempotency, get_invocation, insert_invocation_checked,
    live_binding, ts, update_invocation_row,
};

fn parse_ts(raw: &str) -> Result<Timestamp, RepoError> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&chrono::Utc))
        .map_err(|e| RepoError::Serialization(format!("timestamp `{raw}`: {e}")))
}

fn ser<E: std::fmt::Display>(e: E) -> RepoError {
    RepoError::Serialization(e.to_string())
}

const EVENT_COLUMNS: &str = "event_id, tenant_id, topic, payload, created_at, sent_at, \
     publish_attempts, next_attempt_at, claimed_by, claim_expires_at, last_error, queue_sequence";

type RawEvent = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

fn raw_event(r: &Row<'_>) -> rusqlite::Result<RawEvent> {
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
    ))
}

fn event_of(raw: RawEvent) -> Result<OutboxEvent, RepoError> {
    let (id, tenant, topic, payload, created, sent, attempts, next, claimed, claim_exp, err, seq) =
        raw;
    Ok(OutboxEvent {
        event_id: InvocationId::parse(&id).map_err(ser)?,
        tenant_id: TenantId::parse(&tenant).map_err(ser)?,
        topic,
        payload,
        created_at: parse_ts(&created)?,
        sent_at: sent.as_deref().map(parse_ts).transpose()?,
        publish_attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
        next_attempt_at: parse_ts(&next)?,
        claimed_by: claimed,
        claim_expires_at: claim_exp.as_deref().map(parse_ts).transpose()?,
        last_error: err,
        queue_sequence: seq.map(|s| s as u64),
    })
}

fn stats_in(c: &Connection) -> Result<OutboxStats, RepoError> {
    let (pending, oldest): (i64, Option<String>) = c
        .prepare_cached("SELECT COUNT(*), MIN(created_at) FROM outbox WHERE sent = 0")?
        .query_row([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let sent: i64 = c
        .prepare_cached("SELECT COUNT(*) FROM outbox WHERE sent = 1")?
        .query_row([], |r| r.get(0))?;
    Ok(OutboxStats {
        pending: pending as u64,
        oldest_pending_at: oldest.as_deref().map(parse_ts).transpose()?,
        sent: sent as u64,
    })
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

/// The argument checks of an acceptance (outside the transaction).
pub(super) fn check_accept(
    invocation: &Invocation,
    input: &AsyncInput,
    event: &OutboxEvent,
) -> Result<(), RepoError> {
    if input.invocation_id != invocation.id
        || event.event_id != invocation.id
        || input.tenant_id != invocation.tenant_id
        || event.tenant_id != invocation.tenant_id
        || input.digest != invocation.input_digest
        || input.size_bytes != invocation.input_size_bytes
    {
        return Err(RepoError::Refused(
            "input, outbox event and invocation must name the same invocation, tenant and \
             digest"
                .into(),
        ));
    }
    if let AsyncInputBody::Object(object) = &input.body {
        check_attachable(&object.id, invocation.accepted_at)?;
    }
    Ok(())
}

/// Every row of an asynchronous acceptance, inside the caller's transaction:
/// idempotency, the backlog bound, invocation, key, input and outbox event.
/// Shared by [`AsyncInvocationRepository::accept_async`] and the trigger fire
/// transaction (PLT-4641), so a trigger fire is exactly an acceptance plus its
/// fire row.
pub(super) fn accept_in(
    tx: &Connection,
    invocation: &Invocation,
    input: &AsyncInput,
    event: &OutboxEvent,
    backlog: BacklogLimits,
    max: u64,
    retention: super::Retention,
) -> Result<AsyncAcceptOutcome, RepoError> {
    let now = invocation.accepted_at;
    // 1. Idempotency first: a replay answers even under backlog.
    if let Some(key) = &invocation.idempotency_key
        && let Some(existing) =
            live_binding(tx, &invocation.tenant_id, &invocation.function_id, key, now)?
    {
        return Ok(AsyncAcceptOutcome::Existing(existing));
    }
    // 2. The outbox bound, under the same write lock as the insert.
    let stats = stats_in(tx)?;
    if stats.exceeds(&backlog, now) {
        return Ok(AsyncAcceptOutcome::Backlog(stats));
    }
    // 3. The invocation, its key, its input, its event.
    insert_invocation_checked(tx, invocation, max, retention)?;
    bind_idempotency(tx, invocation, retention)?;
    let (storage, inline, object_id, region) = match &input.body {
        AsyncInputBody::Inline(bytes) => ("inline", Some(bytes.as_slice()), None, None),
        AsyncInputBody::Object(object) => {
            attach_in(tx, object, &invocation.id, now)?;
            (
                "object",
                None,
                Some(object.id.as_str()),
                Some(object.scope.region.as_str()),
            )
        }
    };
    tx.prepare_cached(
        "INSERT INTO invocation_inputs (invocation_id, tenant_id, storage, size_bytes, digest, \
         inline_body, object_id, region, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?
    .execute(params![
        input.invocation_id.as_str(),
        input.tenant_id.as_str(),
        storage,
        big(input.size_bytes, "input size")?,
        input.digest.as_str(),
        inline,
        object_id,
        region,
        ts(&now)
    ])?;
    tx.prepare_cached(
        "INSERT INTO outbox (event_id, tenant_id, topic, payload, created_at, sent, \
         publish_attempts, next_attempt_at) VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6)",
    )?
    .execute(params![
        event.event_id.as_str(),
        event.tenant_id.as_str(),
        event.topic,
        event.payload,
        ts(&event.created_at),
        ts(&event.next_attempt_at)
    ])?;
    Ok(AsyncAcceptOutcome::Accepted)
}

impl AsyncInvocationRepository for SqliteStore {
    fn accept_async(
        &self,
        invocation: Invocation,
        input: AsyncInput,
        event: OutboxEvent,
        backlog: BacklogLimits,
        before_commit: &dyn Fn() -> bool,
    ) -> Result<AsyncAcceptOutcome, RepoError> {
        check_accept(&invocation, &input, &event)?;
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let outcome = accept_in(tx, &invocation, &input, &event, backlog, max, retention)?;
            if outcome == AsyncAcceptOutcome::Accepted && before_commit() {
                return Err(RepoError::Store(
                    "failpoint accept.before_commit: the transaction is rolled back".into(),
                ));
            }
            Ok(outcome)
        })
    }

    fn async_input(&self, invocation: &InvocationId) -> Result<Option<AsyncInput>, RepoError> {
        type Raw = (
            String,
            String,
            i64,
            String,
            Option<Vec<u8>>,
            Option<String>,
            Option<String>,
        );
        let raw: Option<Raw> = self.read(|c| {
            Ok(c.prepare_cached(
                "SELECT tenant_id, storage, size_bytes, digest, inline_body, object_id, region \
                 FROM invocation_inputs WHERE invocation_id = ?1",
            )?
            .query_row([invocation.as_str()], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            })
            .optional()?)
        })?;
        let Some((tenant, storage, size, digest, inline, object_id, region)) = raw else {
            return Ok(None);
        };
        let tenant_id = TenantId::parse(&tenant).map_err(ser)?;
        let body = match (storage.as_str(), inline, object_id, region) {
            ("inline", Some(bytes), None, None) => AsyncInputBody::Inline(bytes),
            ("object", None, Some(id), Some(region)) => AsyncInputBody::Object(ObjectRef {
                id: ObjectId::parse(&id).map_err(ser)?,
                scope: ObjectScope {
                    tenant_id: tenant_id.clone(),
                    region: Region::parse(&region).map_err(ser)?,
                },
            }),
            _ => {
                return Err(RepoError::Serialization(format!(
                    "invocation_inputs row of {invocation} is inconsistent"
                )));
            }
        };
        Ok(Some(AsyncInput {
            invocation_id: invocation.clone(),
            tenant_id,
            size_bytes: size as u64,
            digest: tachyon_serverless_domain::Sha256Digest::parse(&digest).map_err(ser)?,
            body,
        }))
    }

    fn outbox_event(&self, event: &InvocationId) -> Result<Option<OutboxEvent>, RepoError> {
        let raw = self.read(|c| {
            Ok(c.prepare_cached(&format!(
                "SELECT {EVENT_COLUMNS} FROM outbox WHERE event_id = ?1"
            ))?
            .query_row([event.as_str()], raw_event)
            .optional()?)
        })?;
        raw.map(event_of).transpose()
    }

    fn outbox_stats(&self) -> Result<OutboxStats, RepoError> {
        self.read(stats_in)
    }

    fn claim_outbox(
        &self,
        owner: &str,
        now: Timestamp,
        ttl: chrono::Duration,
        max: usize,
    ) -> Result<Vec<OutboxEvent>, RepoError> {
        let limit = i64::try_from(max).unwrap_or(i64::MAX);
        let expires = ts(&(now + ttl));
        let now_s = ts(&now);
        self.write(|tx| {
            let ids: Vec<String> = tx
                .prepare_cached(
                    "SELECT event_id FROM outbox WHERE sent = 0 AND next_attempt_at <= ?1 \
                     AND (claimed_by IS NULL OR claim_expires_at <= ?1) \
                     ORDER BY created_at, event_id LIMIT ?2",
                )?
                .query_map(params![now_s, limit], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                // The same predicate again: the CAS a lock-free adapter needs.
                let n = tx
                    .prepare_cached(
                        "UPDATE outbox SET claimed_by = ?1, claim_expires_at = ?2, \
                         publish_attempts = publish_attempts + 1 \
                         WHERE event_id = ?3 AND sent = 0 \
                         AND (claimed_by IS NULL OR claim_expires_at <= ?4)",
                    )?
                    .execute(params![owner, expires, id, now_s])?;
                if n != 1 {
                    continue;
                }
                let raw = tx
                    .prepare_cached(&format!(
                        "SELECT {EVENT_COLUMNS} FROM outbox WHERE event_id = ?1"
                    ))?
                    .query_row([id.as_str()], raw_event)?;
                out.push(event_of(raw)?);
            }
            Ok(out)
        })
    }

    fn mark_outbox_sent(
        &self,
        event: &InvocationId,
        owner: &str,
        queue_sequence: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        let retention = self.retention();
        self.write(|tx| {
            let n = tx
                .prepare_cached(
                    "UPDATE outbox SET sent = 1, sent_at = ?1, queue_sequence = ?2, \
                     claimed_by = NULL, claim_expires_at = NULL, last_error = NULL \
                     WHERE event_id = ?3 AND sent = 0 AND claimed_by = ?4",
                )?
                .execute(params![
                    ts(&now),
                    big(queue_sequence, "queue sequence")?,
                    event.as_str(),
                    owner
                ])?;
            if n != 1 {
                return Ok(false);
            }
            // Accepted -> Queued. Anything else (cancelled meanwhile, or
            // already moved on by a dispatcher) is left as it is.
            if let Some(mut inv) = get_invocation(tx, event)?
                && inv.status == InvocationStatus::Accepted
                && inv.mark_queued().is_ok()
            {
                update_invocation_row(tx, &inv, retention)?;
            }
            Ok(true)
        })
    }

    fn retry_outbox(
        &self,
        event: &InvocationId,
        owner: &str,
        next_attempt_at: Timestamp,
        error: &str,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let n = tx
                .prepare_cached(
                    "UPDATE outbox SET claimed_by = NULL, claim_expires_at = NULL, \
                     next_attempt_at = ?1, last_error = ?2 \
                     WHERE event_id = ?3 AND sent = 0 AND claimed_by = ?4",
                )?
                .execute(params![
                    ts(&next_attempt_at),
                    truncate(error, 512),
                    event.as_str(),
                    owner
                ])?;
            Ok(n == 1)
        })
    }

    fn purge_sent_outbox(&self, before: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            Ok(tx.execute(
                "DELETE FROM outbox WHERE sent = 1 AND sent_at < ?1",
                [ts(&before)],
            )?)
        })
    }
}

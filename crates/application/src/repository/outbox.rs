//! Asynchronous acceptance and the transactional outbox (PLT-4639,
//! docs/adr/0010).
//!
//! - [`AsyncInvocationRepository::accept_async`] is the **one** transaction of
//!   an asynchronous acceptance: the invocation row, its idempotency binding,
//!   its input (inline, or a reference to a stored object plus the
//!   `object_refs` row that protects it from the GC) and the outbox event.
//!   Either all of it commits or none of it does. The bound on the outbox
//!   backlog is checked inside the same transaction, so two gateways on one
//!   `state.db` cannot overshoot it together.
//! - Publishing is claim → publish → mark: [`claim_outbox`] hands due rows to
//!   exactly one publisher (a lease per row, serialized by the store's write
//!   lock across processes), [`mark_outbox_sent`] is a CAS on that claim, and
//!   [`retry_outbox`] releases it with a backoff. A publisher that dies with a
//!   claim delays its rows by at most the claim TTL.
//!
//! Only the durable store implements this: an asynchronous invocation that
//! does not survive a restart is not accepted at all.
//!
//! [`claim_outbox`]: AsyncInvocationRepository::claim_outbox
//! [`mark_outbox_sent`]: AsyncInvocationRepository::mark_outbox_sent
//! [`retry_outbox`]: AsyncInvocationRepository::retry_outbox

use std::fmt;

use tachyon_serverless_domain::{Invocation, InvocationId, Sha256Digest, TenantId, Timestamp};
use tachyon_serverless_durable_port::ObjectRef;

use super::{IdempotencyBinding, RepoError};

/// Where an asynchronous input is kept.
#[derive(Clone, PartialEq, Eq)]
pub enum AsyncInputBody {
    Inline(Vec<u8>),
    Object(ObjectRef),
}

impl fmt::Debug for AsyncInputBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline(b) => write!(f, "Inline({} bytes)", b.len()),
            Self::Object(r) => write!(f, "Object({})", r.id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncInput {
    pub invocation_id: InvocationId,
    pub tenant_id: TenantId,
    pub size_bytes: u64,
    /// SHA-256 of the input bytes, equal to the invocation's `input_digest`.
    pub digest: Sha256Digest,
    pub body: AsyncInputBody,
}

impl AsyncInput {
    pub fn storage(&self) -> &'static str {
        match self.body {
            AsyncInputBody::Inline(_) => "inline",
            AsyncInputBody::Object(_) => "object",
        }
    }
}

/// One outbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    /// The invocation id: also the queue message id (broker dedup).
    pub event_id: InvocationId,
    pub tenant_id: TenantId,
    pub topic: String,
    /// The routing envelope (JSON). Never the input body.
    pub payload: String,
    pub created_at: Timestamp,
    pub sent_at: Option<Timestamp>,
    pub publish_attempts: u32,
    pub next_attempt_at: Timestamp,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<Timestamp>,
    pub last_error: Option<String>,
    pub queue_sequence: Option<u64>,
    /// Delivery generation (PLT-4640): 0 for the acceptance, n after the nth
    /// retry was scheduled. Part of the queue message id.
    pub generation: u64,
}

impl OutboxEvent {
    pub fn new(
        event_id: InvocationId,
        tenant_id: TenantId,
        topic: impl Into<String>,
        payload: String,
        now: Timestamp,
    ) -> Self {
        Self {
            event_id,
            tenant_id,
            topic: topic.into(),
            payload,
            created_at: now,
            sent_at: None,
            publish_attempts: 0,
            next_attempt_at: now,
            claimed_by: None,
            claim_expires_at: None,
            last_error: None,
            queue_sequence: None,
            generation: 0,
        }
    }

    /// The queue message id: the invocation id, suffixed with `.g<n>` from
    /// the first retry on (a new message, never deduplicated against the
    /// previous generation).
    pub fn message_id(&self) -> String {
        message_id_for(&self.event_id, self.generation)
    }
}

/// See [`OutboxEvent::message_id`].
pub fn message_id_for(event: &InvocationId, generation: u64) -> String {
    match generation {
        0 => event.to_string(),
        n => format!("{event}.g{n}"),
    }
}

/// Bounds checked inside the acceptance transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogLimits {
    pub max_pending: u64,
    pub max_pending_age: chrono::Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutboxStats {
    /// Events not published yet.
    pub pending: u64,
    pub oldest_pending_at: Option<Timestamp>,
    /// Published events still kept (until `sent_retention_seconds`).
    pub sent: u64,
}

impl OutboxStats {
    /// True when `limits` refuse another event at `now`.
    pub fn exceeds(&self, limits: &BacklogLimits, now: Timestamp) -> bool {
        self.pending >= limits.max_pending
            || self
                .oldest_pending_at
                .is_some_and(|t| now - t >= limits.max_pending_age)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncAcceptOutcome {
    /// Everything committed.
    Accepted,
    /// The idempotency key is already bound; nothing was written.
    Existing(IdempotencyBinding),
    /// The outbox is over its bound; nothing was written.
    Backlog(OutboxStats),
}

pub trait AsyncInvocationRepository: Send + Sync {
    /// The acceptance transaction (module docs). `before_commit` runs inside
    /// the transaction after every row is written; `true` rolls it back with
    /// `RepoError::Store` (a failpoint).
    fn accept_async(
        &self,
        invocation: Invocation,
        input: AsyncInput,
        event: OutboxEvent,
        backlog: BacklogLimits,
        before_commit: &dyn Fn() -> bool,
    ) -> Result<AsyncAcceptOutcome, RepoError>;

    fn async_input(&self, invocation: &InvocationId) -> Result<Option<AsyncInput>, RepoError>;

    fn outbox_event(&self, event: &InvocationId) -> Result<Option<OutboxEvent>, RepoError>;

    fn outbox_stats(&self) -> Result<OutboxStats, RepoError>;

    /// Claim up to `max` due, unsent rows that nobody holds (or whose claim
    /// expired) for `owner` until `now + ttl`. Counts one publish attempt each.
    fn claim_outbox(
        &self,
        owner: &str,
        now: Timestamp,
        ttl: chrono::Duration,
        max: usize,
    ) -> Result<Vec<OutboxEvent>, RepoError>;

    /// Mark a published row sent (CAS on `owner`'s claim) and move its
    /// invocation from `Accepted` to `Queued`. `Ok(false)` when the claim is
    /// no longer `owner`'s or the row was already sent.
    fn mark_outbox_sent(
        &self,
        event: &InvocationId,
        owner: &str,
        queue_sequence: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError>;

    /// Release `owner`'s claim after a failed publish; the row is due again at
    /// `next_attempt_at`. `Ok(false)` when the claim is no longer `owner`'s.
    fn retry_outbox(
        &self,
        event: &InvocationId,
        owner: &str,
        next_attempt_at: Timestamp,
        error: &str,
    ) -> Result<bool, RepoError>;

    /// Delete rows sent before `before`. Returns how many.
    fn purge_sent_outbox(&self, before: Timestamp) -> Result<usize, RepoError>;
}

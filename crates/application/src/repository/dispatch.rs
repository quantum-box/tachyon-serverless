//! Asynchronous dispatch state, dead letters and redrives (PLT-4640,
//! docs/adr/0013).
//!
//! The ledger, not the queue, decides what happens to an asynchronous
//! invocation. A delivery is only a hint that something may be due:
//!
//! - [`AsyncDispatchRepository::claim_dispatch`] takes ownership of the next
//!   run of one invocation (a CAS on its dispatch row: generation, state and
//!   the current claim). Exactly one dispatcher holds a claim at a time; a
//!   claim expires unless its holder renews it.
//! - [`AsyncDispatchRepository::settle_dispatch`] ends a run in **one**
//!   transaction: the invocation row (terminal, or back to `queued`), the
//!   dispatch row, the dead letter (if any) and the outbox event of the next
//!   try (if any). It is fenced by the claim ([`DispatchFence`]), so a run
//!   whose claim was taken over can never write a second outcome.
//! - The consumer acknowledges the message only after that commit.
//!
//! Only the durable store implements this.

use std::fmt;

use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    DeadLetterId, FunctionId, Invocation, InvocationError, InvocationId, RedriveId, RevisionId,
    Sha256Digest, TenantId, Timestamp,
};

use super::RepoError;
use super::outbox::{AsyncInput, OutboxEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchState {
    /// A dispatcher holds the claim and runs (or is about to run) it.
    Running,
    /// Waiting for the next try at `next_attempt_at`.
    Scheduled,
    /// Terminal (succeeded or cancelled).
    Done,
    /// Terminal and dead-lettered.
    Dead,
}

impl DispatchState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Scheduled => "scheduled",
            Self::Done => "done",
            Self::Dead => "dead",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "running" => Some(Self::Running),
            "scheduled" => Some(Self::Scheduled),
            "done" => Some(Self::Done),
            "dead" => Some(Self::Dead),
            _ => None,
        }
    }
}

/// One `async_dispatch` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchRecord {
    pub invocation_id: InvocationId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub state: DispatchState,
    pub generation: u64,
    /// Runs that counted against `max_attempts`.
    pub attempts: u32,
    /// Runs deferred without counting (capacity, retry budget).
    pub deferrals: u32,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<Timestamp>,
    pub next_attempt_at: Option<Timestamp>,
    pub first_attempt_at: Option<Timestamp>,
    pub last_attempt_at: Option<Timestamp>,
    pub updated_at: Timestamp,
    pub last_error: Option<InvocationError>,
}

impl DispatchRecord {
    /// The state of an invocation that was never claimed.
    pub fn unclaimed(invocation: &Invocation) -> Self {
        Self {
            invocation_id: invocation.id.clone(),
            tenant_id: invocation.tenant_id.clone(),
            function_id: invocation.function_id.clone(),
            state: DispatchState::Scheduled,
            generation: 0,
            attempts: 0,
            deferrals: 0,
            claimed_by: None,
            claim_expires_at: None,
            next_attempt_at: None,
            first_attempt_at: None,
            last_attempt_at: None,
            updated_at: invocation.accepted_at,
            last_error: None,
        }
    }

    /// A claim that is held and has not expired at `now`.
    pub fn claim_live(&self, now: Timestamp) -> bool {
        self.state == DispatchState::Running
            && self.claimed_by.is_some()
            && self.claim_expires_at.is_some_and(|t| t > now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRequest {
    pub invocation_id: InvocationId,
    /// The claiming dispatcher.
    pub owner: String,
    /// The generation of the delivery that asks. A delivery of another
    /// generation than the row's is stale.
    pub generation: u64,
    pub now: Timestamp,
    pub ttl: chrono::Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The claim is ours; `record.attempts` counts this run.
    Claimed {
        invocation: Box<Invocation>,
        record: Box<DispatchRecord>,
    },
    /// The invocation is already terminal (a duplicate, or an ACK that was
    /// lost after the terminal commit): nothing to run.
    Terminal(Box<Invocation>),
    /// The delivery's generation is not the current one.
    Stale { current: u64 },
    /// Another run holds a live claim.
    Held {
        owner: String,
        expires_at: Timestamp,
    },
    /// The next try is not due yet.
    NotDue { at: Timestamp },
}

/// What a settle is fenced by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchFence {
    /// The run that holds the claim: `owner` and the attempt count its claim
    /// produced must still be on the row.
    Claim { owner: String, attempts: u32 },
    /// Nobody may hold a live claim, and the row must still be at
    /// `generation` (pre-run dead-lettering, the reaper).
    Unclaimed { generation: u64 },
}

/// Why an invocation was dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadLetterReason {
    /// An input, validation or authorization error: retrying cannot help.
    NonRetryable,
    /// `max_attempts` runs ended with retryable errors.
    AttemptsExhausted,
    /// Older than `max_event_age_seconds` (or past its queue deadline).
    Expired,
    /// The function was deleted before the invocation could finish.
    FunctionDeleted,
    /// The pinned revision is gone or no longer runnable.
    RevisionUnavailable,
    /// The event could not be decoded or does not match the ledger.
    Poison,
}

impl DeadLetterReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NonRetryable => "non_retryable",
            Self::AttemptsExhausted => "attempts_exhausted",
            Self::Expired => "expired",
            Self::FunctionDeleted => "function_deleted",
            Self::RevisionUnavailable => "revision_unavailable",
            Self::Poison => "poison",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadLetterStatus {
    Open,
    Redriven,
}

impl DeadLetterStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Redriven => "redriven",
        }
    }
}

/// One dead letter. The input is not copied: it stays in `invocation_inputs`
/// (inline) or the object store (kept by the GC while the entry is open).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub id: DeadLetterId,
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_id: Option<FunctionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<InvocationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<RevisionId>,
    pub reason: DeadLetterReason,
    pub status: DeadLetterStatus,
    pub attempts: u32,
    pub deferrals: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<InvocationError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_attempt_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<Timestamp>,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_digest: Option<Sha256Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_size_bytes: Option<u64>,
    /// `inline` | `object`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_storage: Option<String>,
    /// Poison only: the queue message id and sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_sequence: Option<u64>,
    /// Poison only: why the event could not be read (no tenant data).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default)]
    pub redrive_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redriven_at: Option<Timestamp>,
}

impl DeadLetter {
    /// The uniqueness key of the entry (`dead_letters.origin_key`).
    pub fn origin_key(&self) -> String {
        match (&self.invocation_id, &self.message_id) {
            (Some(inv), _) => format!("inv:{inv}"),
            (None, Some(msg)) => format!("msg:{msg}:{}", self.message_sequence.unwrap_or(0)),
            (None, None) => format!("dlq:{}", self.id),
        }
    }
}

/// The audit record of one redrive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redrive {
    pub id: RedriveId,
    pub dead_letter_id: DeadLetterId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    /// The dead-lettered invocation.
    pub source_invocation_id: InvocationId,
    /// The new invocation the redrive created.
    pub invocation_id: InvocationId,
    /// The revision the new invocation is pinned to.
    pub revision_id: RevisionId,
    /// True when the caller chose another revision than the original one.
    pub revision_overridden: bool,
    /// Who asked (the principal's subject) and why.
    pub requested_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub created_at: Timestamp,
}

/// Everything a settle writes in one transaction.
#[derive(Debug, Clone)]
pub struct DispatchSettle {
    pub fence: DispatchFence,
    /// The invocation after the run: terminal, or `queued` again.
    pub invocation: Invocation,
    pub state: DispatchState,
    /// For [`DispatchState::Scheduled`]: when the next try is due.
    pub next_attempt_at: Option<Timestamp>,
    pub last_error: Option<InvocationError>,
    /// False when the run that ends did not count as an attempt (a deferral):
    /// `deferrals` counts it, and a claim's increment of `attempts` is undone.
    pub counted: bool,
    pub dead_letter: Option<DeadLetter>,
    /// The event of the next try (its `generation` must be the row's + 1).
    pub republish: Option<OutboxEvent>,
    pub now: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettleOutcome {
    Committed,
    /// The fence did not hold (claim taken over, generation moved on,
    /// invocation already terminal). Nothing was written.
    Lost(String),
}

/// A non-terminal asynchronous invocation the reaper looks at.
#[derive(Debug, Clone)]
pub struct DispatchCandidate {
    pub invocation: Invocation,
    pub record: DispatchRecord,
    /// The outbox row, if one is still kept.
    pub outbox: Option<OutboxEvent>,
}

/// Everything a redrive writes in one transaction.
#[derive(Debug, Clone)]
pub struct RedriveWrite {
    pub dead_letter_id: DeadLetterId,
    /// The new invocation (`accepted`, no idempotency key).
    pub invocation: Invocation,
    /// Its input: the same inline bytes or the same object reference.
    pub input: AsyncInput,
    pub event: OutboxEvent,
    pub redrive: Redrive,
}

pub trait AsyncDispatchRepository: Send + Sync {
    fn dispatch_record(
        &self,
        invocation: &InvocationId,
    ) -> Result<Option<DispatchRecord>, RepoError>;

    fn claim_dispatch(&self, request: ClaimRequest) -> Result<ClaimOutcome, RepoError>;

    /// Extend `owner`'s live claim to `now + ttl`. False when the claim is not
    /// `owner`'s any more (or not at `attempts`).
    fn renew_dispatch_claim(
        &self,
        invocation: &InvocationId,
        owner: &str,
        attempts: u32,
        now: Timestamp,
        ttl: chrono::Duration,
    ) -> Result<bool, RepoError>;

    fn settle_dispatch(&self, settle: DispatchSettle) -> Result<SettleOutcome, RepoError>;

    /// Non-terminal asynchronous invocations that may need the reaper: a run
    /// whose claim expired, an invocation with no unpublished event whose
    /// last event was published (or whose last try became due) before
    /// `quiet_before`, or anything accepted before `accepted_before` (past
    /// its maximum age).
    fn dispatch_candidates(
        &self,
        now: Timestamp,
        quiet_before: Timestamp,
        accepted_before: Timestamp,
        limit: usize,
    ) -> Result<Vec<DispatchCandidate>, RepoError>;

    /// Record a dead letter that belongs to no invocation (poison). `false`
    /// when the same message was already recorded.
    fn record_poison(&self, dead_letter: DeadLetter) -> Result<bool, RepoError>;

    fn dead_letter(&self, id: &DeadLetterId) -> Result<Option<DeadLetter>, RepoError>;

    /// The dead letter of `invocation`, if it has one.
    fn dead_letter_of(&self, invocation: &InvocationId) -> Result<Option<DeadLetter>, RepoError>;

    /// Newest first.
    fn list_dead_letters(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, RepoError>;

    /// Oldest first.
    fn redrives_of(&self, dead_letter: &DeadLetterId) -> Result<Vec<Redrive>, RepoError>;

    /// The redrive that created `invocation`, if any.
    fn redrive_creating(&self, invocation: &InvocationId) -> Result<Option<Redrive>, RepoError>;

    /// The redrive transaction. `Conflict` when the dead letter is not open
    /// any more (redriven concurrently).
    fn redrive(&self, write: RedriveWrite) -> Result<(), RepoError>;
}

impl fmt::Display for DispatchState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

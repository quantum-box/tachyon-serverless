//! Cron and webhook triggers (PLT-4641, docs/adr/0014).
//!
//! - A [`Trigger`] row carries its spec as JSON plus the columns the
//!   scheduler and the CAS need (`status`, `generation`, `next_fire_at`). A
//!   webhook secret is stored **sealed** (AES-256-GCM) in its own column and
//!   is never part of the JSON body.
//! - A [`FireRecord`] is unique per `(trigger_id, fire_key)`: `cron:<scheduled
//!   time>` or `event:<event id>`. The fire row of an accepted fire is written
//!   in the **same transaction** as the asynchronous invocation, its input
//!   and its outbox event ([`TriggerRepository::accept_trigger_fire`]), and
//!   that transaction re-reads the trigger: a trigger disabled, deleted or
//!   changed since the caller read it fires nothing. A restarted or second
//!   scheduler that computes the same scheduled time finds the row and
//!   creates nothing.
//! - One scheduler owns the cron loop at a time: a lease row owned by a
//!   dispatcher id (PLT-4631), taken over when it expired or its dispatcher
//!   stopped. The unique fire row, not the lease, is what makes a duplicate
//!   impossible; the lease only avoids wasted work.
//!
//! Only the durable store implements this: a trigger fire that does not
//! survive a restart is not accepted at all (same rule as ADR-0010 §7).

use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    AliasName, FunctionId, Invocation, InvocationId, RevisionId, TenantId, Timestamp, TriggerId,
};

use super::outbox::{AsyncInput, BacklogLimits, OutboxEvent, OutboxStats};
use super::{IdempotencyBinding, RepoError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    Cron,
    Webhook,
}

impl TriggerKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Webhook => "webhook",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerStatus {
    Enabled,
    Disabled,
    Deleted,
}

impl TriggerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Deleted => "deleted",
        }
    }
}

/// What a fire runs. Neither set means the `prod` alias.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<AliasName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<RevisionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MissedRunPolicy {
    Skip,
    RunOnce,
    RunAll { max_runs: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CronSpec {
    pub expression: String,
    pub timezone: String,
    pub payload: serde_json::Value,
    pub missed_run_policy: MissedRunPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSpec {
    pub source: String,
    pub tolerance_seconds: u64,
    pub max_body_bytes: u64,
    pub event_id_header: String,
    /// Fingerprint of the current secret, never the secret.
    pub secret_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TriggerSpec {
    Cron(CronSpec),
    Webhook(WebhookSpec),
}

impl TriggerSpec {
    pub fn kind(&self) -> TriggerKind {
        match self {
            Self::Cron(_) => TriggerKind::Cron,
            Self::Webhook(_) => TriggerKind::Webhook,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trigger {
    pub id: TriggerId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub name: String,
    pub target: TriggerTarget,
    pub spec: TriggerSpec,
    pub status: TriggerStatus,
    /// Why the platform (not a user) changed the status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    /// Starts at 1, bumped by every write.
    pub generation: u64,
    /// Cron: the next scheduled time not yet handled. `None` unless enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scheduled_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<Timestamp>,
}

impl Trigger {
    pub fn kind(&self) -> TriggerKind {
        self.spec.kind()
    }

    pub fn is_enabled(&self) -> bool {
        self.status == TriggerStatus::Enabled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FireOutcome {
    /// An asynchronous invocation was accepted for this fire.
    Accepted,
    /// The acceptance was refused for good (`reason`); nothing runs.
    Refused,
}

impl FireOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Refused => "refused",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FireRecord {
    pub trigger_id: TriggerId,
    pub tenant_id: TenantId,
    /// `cron:<scheduled_at>` | `event:<event_id>`.
    pub fire_key: String,
    pub kind: TriggerKind,
    pub scheduled_at: Option<Timestamp>,
    pub event_id: Option<String>,
    /// SHA-256 of the verified signature header: a signed delivery replayed
    /// under another event id is the same delivery.
    pub signature_digest: Option<String>,
    pub invocation_id: Option<InvocationId>,
    pub outcome: FireOutcome,
    pub reason: Option<String>,
    pub created_at: Timestamp,
}

impl FireRecord {
    pub fn cron_key(scheduled_at: &Timestamp) -> String {
        format!("cron:{}", scheduled_at.format("%Y-%m-%dT%H:%M:%SZ"))
    }

    pub fn event_key(event_id: &str) -> String {
        format!("event:{event_id}")
    }
}

/// The trigger-side half of a fire transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FireClaim {
    /// The generation the caller computed the fire from. `None` accepts any
    /// generation (webhooks: a changed tolerance does not invalidate a
    /// verified delivery), but the trigger must still be enabled.
    pub expected_generation: Option<u64>,
    /// Outcome `Accepted`, with the invocation id.
    pub record: FireRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerAcceptOutcome {
    /// Invocation, input, outbox event and fire row committed together.
    Accepted,
    /// The fire key (or, for a webhook, the signature) is already recorded;
    /// nothing was written.
    AlreadyFired(FireRecord),
    /// The trigger is gone, not enabled, or at another generation; nothing
    /// was written.
    Inactive(Option<Box<Trigger>>),
    /// The idempotency key is bound already; nothing was written.
    Existing(IdempotencyBinding),
    /// The outbox is over its bound; nothing was written.
    Backlog(OutboxStats),
}

pub trait TriggerRepository: Send + Sync {
    /// Insert a new trigger (and its sealed secret). Refused when the
    /// function already has `max_per_function` live triggers.
    fn insert_trigger(
        &self,
        trigger: Trigger,
        sealed_secret: Option<Vec<u8>>,
        max_per_function: usize,
    ) -> Result<(), RepoError>;

    fn get_trigger(&self, id: &TriggerId) -> Result<Option<Trigger>, RepoError>;

    /// Live (not deleted) triggers of a function.
    fn list_triggers(&self, function: &FunctionId) -> Result<Vec<Trigger>, RepoError>;

    /// The sealed secret of a webhook trigger that is not deleted.
    fn trigger_secret(&self, id: &TriggerId) -> Result<Option<Vec<u8>>, RepoError>;

    /// CAS write at `expected_generation` (the stored generation must equal
    /// it; the new row carries `expected_generation + 1`). `sealed_secret`
    /// replaces the secret when `Some`. A `Deleted` trigger's secret is
    /// erased. `Ok(false)` when the generation moved.
    fn update_trigger(
        &self,
        trigger: Trigger,
        expected_generation: u64,
        sealed_secret: Option<Vec<u8>>,
    ) -> Result<bool, RepoError>;

    /// Enabled cron triggers with `next_fire_at <= now`, oldest first.
    fn due_cron_triggers(&self, now: Timestamp, limit: usize) -> Result<Vec<Trigger>, RepoError>;

    /// The earliest `next_fire_at` of an enabled cron trigger.
    fn next_cron_fire_at(&self) -> Result<Option<Timestamp>, RepoError>;

    /// Move a cron trigger's cursor (CAS on generation and enabled status).
    /// Does not bump the generation: the cursor is scheduler state, not spec.
    fn advance_cron_cursor(
        &self,
        id: &TriggerId,
        generation: u64,
        next_fire_at: Timestamp,
        last_scheduled_at: Option<Timestamp>,
    ) -> Result<bool, RepoError>;

    /// The fire transaction (module docs), with the asynchronous acceptance
    /// rows of ADR-0010. `before_commit` as in
    /// [`super::AsyncInvocationRepository::accept_async`].
    #[allow(clippy::too_many_arguments)]
    fn accept_trigger_fire(
        &self,
        invocation: Invocation,
        input: AsyncInput,
        event: OutboxEvent,
        backlog: BacklogLimits,
        claim: FireClaim,
        before_commit: &dyn Fn() -> bool,
    ) -> Result<TriggerAcceptOutcome, RepoError>;

    /// Record a refused fire (insert or ignore). `Ok(false)` when the key was
    /// already recorded.
    fn record_fire(&self, record: FireRecord) -> Result<bool, RepoError>;

    fn get_fire(
        &self,
        trigger: &TriggerId,
        fire_key: &str,
    ) -> Result<Option<FireRecord>, RepoError>;

    fn fire_by_signature(
        &self,
        trigger: &TriggerId,
        signature_digest: &str,
    ) -> Result<Option<FireRecord>, RepoError>;

    /// Newest first.
    fn list_fires(&self, trigger: &TriggerId, limit: usize) -> Result<Vec<FireRecord>, RepoError>;

    /// Delete webhook fire rows created before `webhook_before` and cron fire
    /// rows created before `cron_before`.
    fn purge_fires(
        &self,
        webhook_before: Timestamp,
        cron_before: Timestamp,
    ) -> Result<usize, RepoError>;

    /// Take or renew the cron scheduler lease for `owner` (a dispatcher id)
    /// until `now + ttl`. Taken over from another owner only when its lease
    /// expired or its dispatcher stopped or was reclaimed.
    fn acquire_scheduler_lease(
        &self,
        owner: &str,
        now: Timestamp,
        ttl: chrono::Duration,
    ) -> Result<bool, RepoError>;

    /// Give the lease up (graceful stop).
    fn release_scheduler_lease(&self, owner: &str) -> Result<(), RepoError>;
}

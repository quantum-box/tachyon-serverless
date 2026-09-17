//! `SlotStore`: the cell-local port for environment slots, their leases,
//! the warm pool and the dispatchers that own them (docs/adr/0003 decision 1,
//! PLT-4631).
//!
//! The control-plane repositories (function, revision, alias, invocation,
//! idempotency, artifact ownership) stay in [`super`]. Everything that decides
//! *who may run what on which environment right now* lives here, so a future
//! control-plane adapter (TiDB) does not have to carry it and the invoke
//! pipeline only touches slots through this port.
//!
//! # Model
//!
//! - A **dispatcher** is one gateway process incarnation
//!   ([`DispatcherRecord`]). It registers on start, renews its own lease with
//!   a heartbeat ([`SlotStore::heartbeat`]) and marks itself stopped on a
//!   graceful shutdown. It owns the invocations it accepted and the
//!   environments it created (their bridge sessions exist only in its
//!   process).
//! - A **slot lease** ([`ExecutionLease`]) binds one attempt to one
//!   environment at one epoch, with an owner and an ownership expiry that the
//!   owner's heartbeat renews. [`SlotStore::acquire`] creates it atomically
//!   with the epoch bump, the attempt row and the invocation's `Running`
//!   state.
//! - **Fencing**: a completion is accepted only for the unreleased lease of
//!   the exact `(attempt_id, epoch)` on an environment still at that epoch
//!   ([`SlotStore::complete`]). A lease that expired is reclaimed exactly once
//!   ([`SlotStore::reclaim_expired`]); its environment is fenced (`Draining`,
//!   epoch + 1) and only a confirmed provider terminate
//!   ([`SlotStore::confirm_terminated`]) settles it. Expiry alone never frees
//!   or reuses an environment.

use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    AttemptId, DispatcherId, EnvironmentId, ExecutionEnvironment, ExecutionLease, Invocation,
    InvocationAttempt, LeaseId, ReuseKey, Timestamp,
};

use super::{PoolLimits, RepoError};

/// One gateway process incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatcherRecord {
    pub id: DispatcherId,
    /// Stable name of the gateway instance (`[dispatcher] instance`, by
    /// default derived from the listen address). A restart of the same
    /// instance on the same host is its "next incarnation".
    pub instance: String,
    pub hostname: String,
    pub pid: u32,
    pub started_at: Timestamp,
    pub heartbeat_at: Timestamp,
    /// The dispatcher's own lease. Renewed only while it has not passed.
    pub lease_expires_at: Timestamp,
    /// Set by a graceful shutdown: everything it still owns may be reclaimed
    /// at once.
    pub stopped_at: Option<Timestamp>,
    /// Set (once) by the dispatcher that reclaimed it. A reclaimed dispatcher
    /// can neither renew nor acquire again.
    pub reclaimed_at: Option<Timestamp>,
}

impl DispatcherRecord {
    /// Still allowed to hold and take slots: not stopped, not reclaimed.
    /// Expiry is decided by whoever reclaims, not here.
    pub fn is_live(&self) -> bool {
        self.stopped_at.is_none() && self.reclaimed_at.is_none()
    }

    /// True when another dispatcher whose clock reads `now` may reclaim it.
    pub fn is_expired(&self, now: Timestamp, skew: chrono::Duration) -> bool {
        now >= self.lease_expires_at + skew
    }
}

/// Everything [`SlotStore::acquire`] writes in one transaction.
#[derive(Debug, Clone)]
pub struct SlotAcquire {
    /// The caller's copy after [`ExecutionEnvironment::assign`]: `Busy`, at
    /// `expected_epoch + 1`.
    pub env: ExecutionEnvironment,
    /// The epoch the caller read. The CAS predicate, together with the state
    /// (`Ready` / `Idle`).
    pub expected_epoch: u64,
    /// Owned, with an ownership expiry, at the new epoch.
    pub lease: ExecutionLease,
    /// The attempt dispatched under the lease, at the new epoch.
    pub attempt: InvocationAttempt,
    /// The invocation moved to `Running` (or re-pointed at a retry).
    pub invocation: Option<Invocation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    /// Everything was written.
    Acquired,
    /// Someone else got there first, or the slot is not in a state that can
    /// be acquired (moved on, fenced, already leased, owner no longer live,
    /// invocation already settled). Nothing was written.
    Lost(String),
}

/// A completion (the fenced callback): the terminal attempt and invocation
/// of one lease.
#[derive(Debug, Clone)]
pub struct SlotCompletion {
    pub lease_id: LeaseId,
    /// Terminal, at the lease's `(attempt_id, epoch)`.
    pub attempt: InvocationAttempt,
    /// Terminal, when the attempt settles the invocation.
    pub invocation: Option<Invocation>,
    pub now: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionOutcome {
    /// Lease released, attempt and invocation written.
    Accepted,
    /// The lease is not the current one for this attempt any more (released,
    /// reclaimed, other epoch, fenced environment, settled rows). Nothing was
    /// written.
    Stale(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    /// The dispatcher lease and `leases` unexpired slot leases were renewed.
    Renewed { leases: usize },
    /// The dispatcher is stopped, reclaimed or already expired: it must not
    /// take new work (a renewal never revives an expired lease).
    Fenced,
}

#[derive(Debug, Clone)]
pub struct ReclaimRequest {
    pub reclaimer: DispatcherId,
    pub now: Timestamp,
    /// Clock difference tolerated between dispatchers: a lease is expired
    /// for the reclaimer only once its clock is past `expires_at + skew`.
    pub skew: chrono::Duration,
    /// Dispatchers the caller proved dead without waiting for their lease
    /// (the previous incarnation of the same instance on the same host whose
    /// process is gone).
    pub presumed_dead: Vec<DispatcherId>,
}

#[derive(Debug, Clone, Default)]
pub struct ReclaimReport {
    /// Dispatchers marked reclaimed by this pass.
    pub dispatchers: Vec<DispatcherId>,
    /// Slot leases released by this pass.
    pub leases: usize,
    pub invocations: usize,
    pub attempts: usize,
    /// Environments fenced by this pass. The caller terminates them and
    /// confirms with [`SlotStore::confirm_terminated`].
    pub fenced: Vec<ExecutionEnvironment>,
}

impl ReclaimReport {
    pub fn is_empty(&self) -> bool {
        self.dispatchers.is_empty()
            && self.leases == 0
            && self.invocations == 0
            && self.attempts == 0
            && self.fenced.is_empty()
    }
}

pub trait SlotStore: Send + Sync {
    // -- dispatchers ----------------------------------------------------------

    /// Register a new dispatcher incarnation. `Conflict` for a duplicate id.
    fn register_dispatcher(&self, record: DispatcherRecord) -> Result<(), RepoError>;
    fn get_dispatcher(&self, id: &DispatcherId) -> Result<Option<DispatcherRecord>, RepoError>;
    fn list_dispatchers(&self) -> Result<Vec<DispatcherRecord>, RepoError>;
    /// Renew the dispatcher lease and every slot lease it holds that has not
    /// expired yet, to `now + ttl`, in one transaction. Only a live dispatcher
    /// whose lease has not passed is renewed.
    fn heartbeat(
        &self,
        id: &DispatcherId,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<HeartbeatOutcome, RepoError>;
    /// [`Self::heartbeat`] for a dispatcher whose previous renewals failed
    /// because the **store** did not answer (PLT-4646, docs/adr/0003
    /// 「store が止まった間の lease」): its lease may have passed while it
    /// kept trying. It is renewed anyway — together with every unreleased
    /// slot lease it owns — as long as it is live, i.e. nobody stopped or
    /// reclaimed it. That check and the renewal are one transaction, and a
    /// reclaim marks the dispatcher and releases its leases in one
    /// transaction too, so the two are serialized by the store: either the
    /// reclaim committed first (this returns `Fenced`) or the renewal did (the
    /// reclaim then sees a valid lease). Expiry alone never let anyone act.
    ///
    /// The caller decides when a failure was the store's
    /// ([`crate::services::Dispatcher::heartbeat`]).
    fn renew_after_store_outage(
        &self,
        id: &DispatcherId,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<HeartbeatOutcome, RepoError>;

    /// Graceful shutdown: whatever the dispatcher still owns may be reclaimed
    /// immediately.
    fn stop_dispatcher(&self, id: &DispatcherId, now: Timestamp) -> Result<(), RepoError>;

    // -- pool -----------------------------------------------------------------

    /// The environments of `owner` currently in the pool, longest idle first.
    /// Only the owner holds their sessions, so only the owner sweeps them.
    fn list_idle(
        &self,
        owner: Option<&DispatcherId>,
    ) -> Result<Vec<ExecutionEnvironment>, RepoError>;

    /// Atomically take one **pooled** (`Idle`) environment of `owner` whose
    /// reuse key equals `key` in *every* field out of the pool: it becomes
    /// `Ready` at the same epoch ([`ExecutionEnvironment::reserve`]). The
    /// epoch advances when the caller [`Self::acquire`]s it.
    ///
    /// Of two concurrent claims exactly one can win. `None` means the caller
    /// must create an environment.
    fn claim_for_reuse(
        &self,
        key: &ReuseKey,
        owner: Option<&DispatcherId>,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError>;

    /// Atomically put a finished environment back into the pool. Refused
    /// (`None`) when the stored row moved on (other epoch, not `Busy`, still
    /// leased, fenced) or a cap in `limits` is reached.
    fn release_to_pool(
        &self,
        env: &ExecutionEnvironment,
        limits: PoolLimits,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError>;

    /// Atomically take one idle environment out of the pool for termination,
    /// moving it to `Draining`. False when it is no longer idle.
    fn take_idle_for_termination(
        &self,
        id: &EnvironmentId,
        now: Timestamp,
    ) -> Result<bool, RepoError>;

    // -- slots and leases -----------------------------------------------------

    /// Atomic slot acquisition: CAS on the environment's `(state, epoch)`
    /// (`Ready`/`Idle` at `expected_epoch`, not fenced, no unreleased lease,
    /// same owner as the lease, owner dispatcher live), then in the same
    /// transaction: the environment becomes `Busy` at `expected_epoch + 1`,
    /// the lease (owner, attempt, epoch, ownership expiry, execution
    /// deadline) and the attempt are inserted and the invocation is updated.
    fn acquire(&self, request: SlotAcquire) -> Result<AcquireOutcome, RepoError>;

    fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError>;

    /// Renew one lease to `now + ttl`, only while it is unreleased, not
    /// expired and still held by `owner` at `epoch`.
    fn renew_lease(
        &self,
        id: &LeaseId,
        owner: &DispatcherId,
        epoch: u64,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<bool, RepoError>;

    /// The fenced callback: release the lease and write the terminal attempt
    /// (and invocation) only if the lease is still the unreleased lease of
    /// that `(attempt_id, epoch)` and the environment is still at that epoch.
    fn complete(&self, completion: SlotCompletion) -> Result<CompletionOutcome, RepoError>;

    /// Release a lease without a result (cleanup paths). Fenced like
    /// [`Self::complete`]. False when it was not the current lease.
    fn release_lease(
        &self,
        id: &LeaseId,
        attempt: &AttemptId,
        epoch: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError>;

    /// Reclaim everything whose owner can no longer be trusted, exactly once
    /// across every connection and process:
    ///
    /// - dispatchers (other than the reclaimer) that stopped, whose lease is
    ///   past `expires + skew`, or that the caller proved dead, are marked
    ///   reclaimed;
    /// - unreleased slot leases of those dispatchers, and any other lease
    ///   past `expires_at + skew`, are released; their attempt and invocation
    ///   are settled (`OutcomeUnknown` once dispatched) and their environment
    ///   is fenced;
    /// - non-terminal invocations (and their attempts) and environments owned
    ///   by a reclaimed dispatcher are settled / fenced.
    ///
    /// Nothing of the reclaimer itself is touched, and a reclaimer that can
    /// no longer prove its own lease — stopped, reclaimed, not registered, or
    /// its lease passed by its own clock — reclaims nothing (an empty report):
    /// a gateway that was frozen past its lease must not wake up and reclaim
    /// the dispatchers that kept running while it was away (PLT-4646).
    fn reclaim_expired(&self, request: ReclaimRequest) -> Result<ReclaimReport, RepoError>;

    /// Fenced environments that are not settled yet (their terminate is not
    /// confirmed).
    fn list_fenced(&self) -> Result<Vec<ExecutionEnvironment>, RepoError>;

    /// A provider terminate of the fenced environment `id` at `epoch`
    /// returned successfully: settle it (`Lost`). False when it is not that
    /// fenced environment any more (already settled by someone else).
    fn confirm_terminated(
        &self,
        id: &EnvironmentId,
        epoch: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError>;
}

/// Checks shared by both stores for [`SlotStore::acquire`]. `Err` is a
/// malformed request (a programming error), `Ok(Some(reason))` a lost race.
pub(crate) fn acquire_preconditions(
    request: &SlotAcquire,
    stored: Option<&ExecutionEnvironment>,
) -> Result<Option<String>, RepoError> {
    let SlotAcquire {
        env,
        expected_epoch,
        lease,
        attempt,
        ..
    } = request;
    let new_epoch = expected_epoch + 1;
    if env.epoch != new_epoch || lease.epoch != new_epoch || attempt.epoch != new_epoch {
        return Err(RepoError::Refused(format!(
            "acquire of environment {}: environment, lease and attempt must all be at epoch {new_epoch}",
            env.id
        )));
    }
    if lease.environment_id != env.id || attempt.environment_id != env.id {
        return Err(RepoError::Refused(format!(
            "acquire of environment {}: lease or attempt names another environment",
            env.id
        )));
    }
    if lease.attempt_id != attempt.id {
        return Err(RepoError::Refused(format!(
            "acquire of environment {}: lease is for another attempt",
            env.id
        )));
    }
    if lease.owner.is_none() || lease.expires_at.is_none() || lease.released_at.is_some() {
        return Err(RepoError::Refused(format!(
            "acquire of environment {}: the lease needs an owner and an expiry",
            env.id
        )));
    }
    if !matches!(env.state, tachyon_serverless_domain::EnvironmentState::Busy) {
        return Err(RepoError::Refused(format!(
            "acquire of environment {}: the written row must be busy",
            env.id
        )));
    }
    let Some(stored) = stored else {
        return Err(RepoError::NotFound(format!("environment {}", env.id)));
    };
    super::guard::environment_identity(stored, env)?;
    if stored.owner != lease.owner {
        return Ok(Some(format!(
            "environment {} is owned by another dispatcher",
            env.id
        )));
    }
    if stored.is_terminal() || stored.is_fenced() {
        return Ok(Some(format!(
            "environment {} is {}",
            env.id,
            if stored.is_fenced() {
                "fenced"
            } else {
                stored.state.name()
            }
        )));
    }
    if stored.epoch != *expected_epoch {
        return Ok(Some(format!(
            "environment {} moved to epoch {} (expected {expected_epoch})",
            env.id, stored.epoch
        )));
    }
    if !stored.is_reusable() {
        return Ok(Some(format!(
            "environment {} is {}, not ready or idle",
            env.id,
            stored.state.name()
        )));
    }
    Ok(None)
}

/// Checks shared by both stores for [`SlotStore::complete`] /
/// [`SlotStore::release_lease`]: is `lease` still the current lease of
/// `(attempt, epoch)` on `env`? `Some(reason)` when it is stale.
pub(crate) fn lease_is_current(
    lease: Option<&ExecutionLease>,
    attempt: &AttemptId,
    epoch: u64,
    env: Option<&ExecutionEnvironment>,
) -> Option<String> {
    let Some(lease) = lease else {
        return Some("lease not found".into());
    };
    if lease.released_at.is_some() {
        return Some(format!("lease {} is already released", lease.id));
    }
    if !lease.accepts(attempt, epoch) {
        return Some(format!(
            "lease {} is for attempt {} at epoch {}, not {attempt} at epoch {epoch}",
            lease.id, lease.attempt_id, lease.epoch
        ));
    }
    match env {
        Some(env) if env.epoch != epoch => Some(format!(
            "environment {} moved to epoch {} (completion carries {epoch})",
            env.id, env.epoch
        )),
        Some(env) if env.is_fenced() => Some(format!("environment {} is fenced", env.id)),
        _ => None,
    }
}

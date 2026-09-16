//! ExecutionEnvironment: a microVM / process that can serve attempts, and the
//! lease that binds one attempt to one environment slot.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::DomainError;
use crate::ids::{AttemptId, EnvironmentId, LeaseId, RevisionId, TenantId};

/// Which provider realised the environment. The domain only records the name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    Firecracker,
    /// Local subprocess. No isolation. Development and tests only.
    Process,
    /// In-memory fake. Tests only.
    Fake,
    Other(String),
}

impl ProviderKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Firecracker => "firecracker",
            Self::Process => "process",
            Self::Fake => "fake",
            Self::Other(s) => s,
        }
    }
    /// True for providers that do not provide a real isolation boundary.
    pub fn is_unisolated(&self) -> bool {
        matches!(self, Self::Process | Self::Fake)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum EnvironmentState {
    Requested,
    Provisioning,
    Initializing,
    Ready,
    Busy,
    Idle,
    Draining,
    Stopped,
    Failed {
        reason: String,
    },
    /// Provider lost track of the environment (e.g. host died).
    Lost {
        reason: String,
    },
}

impl EnvironmentState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Stopped | Self::Failed { .. } | Self::Lost { .. }
        )
    }
    pub fn name(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Provisioning => "provisioning",
            Self::Initializing => "initializing",
            Self::Ready => "ready",
            Self::Busy => "busy",
            Self::Idle => "idle",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Failed { .. } => "failed",
            Self::Lost { .. } => "lost",
        }
    }
}

/// Evidence that a real environment was created, recorded for demos and audits
/// (RFC / PLT-4630: microVM boot ID, process id, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BootEvidence {
    /// Kernel boot id reported by the guest (`/proc/sys/kernel/random/boot_id`), if any.
    pub guest_boot_id: Option<String>,
    /// Host-side process id of the VMM / child process.
    pub host_pid: Option<u32>,
    /// Free-form provider details (VMM version, kernel digest, ...). Never secrets.
    pub details: serde_json::Map<String, serde_json::Value>,
}

/// Reuse key (RFC §5.3). Environments are only ever reused when *all* fields
/// match: one differing field makes two environments incompatible, so a
/// tenant, a revision, a changed configuration or a superseded secret
/// generation can never share a guest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReuseKey {
    pub tenant_id: TenantId,
    pub revision_id: RevisionId,
    pub execution_role_version: u64,
    pub configuration_version: u64,
    pub resource_profile_digest: String,
    pub runtime_profile: String,
    pub network_policy_version: u64,
    pub secret_binding_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEnvironment {
    pub id: EnvironmentId,
    pub tenant_id: TenantId,
    pub revision_id: RevisionId,
    pub provider: ProviderKind,
    pub state: EnvironmentState,
    pub reuse_key: ReuseKey,
    /// Incremented whenever the environment is (re)assigned. Results carrying
    /// an older epoch are rejected.
    pub epoch: u64,
    pub evidence: BootEvidence,
    pub created_at: Timestamp,
    pub ready_at: Option<Timestamp>,
    /// When the environment entered `Idle`, i.e. when it joined the pool.
    /// `None` in every other state; the idle TTL is measured from here.
    /// Defaulted so state written before pooling existed still loads.
    #[serde(default)]
    pub idle_since: Option<Timestamp>,
    pub stopped_at: Option<Timestamp>,
    pub updated_at: Timestamp,
}

impl ExecutionEnvironment {
    pub fn request(
        id: EnvironmentId,
        tenant_id: TenantId,
        revision_id: RevisionId,
        provider: ProviderKind,
        reuse_key: ReuseKey,
        now: Timestamp,
    ) -> Self {
        Self {
            id,
            tenant_id,
            revision_id,
            provider,
            state: EnvironmentState::Requested,
            reuse_key,
            epoch: 1,
            evidence: BootEvidence::default(),
            created_at: now,
            ready_at: None,
            idle_since: None,
            stopped_at: None,
            updated_at: now,
        }
    }

    fn transition(&mut self, to: EnvironmentState, now: Timestamp) -> Result<(), DomainError> {
        if self.state.is_terminal() {
            return Err(DomainError::Terminal {
                entity: "ExecutionEnvironment",
                state: self.state.name().into(),
            });
        }
        use EnvironmentState as S;
        let allowed = matches!(
            (&self.state, &to),
            (S::Requested, S::Provisioning)
                | (S::Provisioning, S::Initializing)
                | (S::Initializing, S::Ready)
                | (S::Ready, S::Busy)
                | (S::Busy, S::Idle)
                | (S::Idle, S::Ready)
                | (S::Ready | S::Busy | S::Idle, S::Draining)
                | (_, S::Stopped)
                | (_, S::Failed { .. })
                | (_, S::Lost { .. })
        );
        if !allowed {
            return Err(DomainError::IllegalTransition {
                entity: "ExecutionEnvironment",
                from: self.state.name().into(),
                to: to.name().into(),
            });
        }
        if matches!(to, S::Ready) && self.ready_at.is_none() {
            self.ready_at = Some(now);
        }
        // Pool membership starts and ends with `Idle`; leaving it (for a new
        // attempt, for draining or for termination) clears the TTL clock.
        self.idle_since = matches!(to, S::Idle).then_some(now);
        if to.is_terminal() {
            self.stopped_at = Some(now);
        }
        self.state = to;
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_provisioning(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Provisioning, now)
    }
    pub fn mark_initializing(
        &mut self,
        evidence: BootEvidence,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Initializing, now)?;
        self.evidence = evidence;
        Ok(())
    }
    pub fn mark_ready(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Ready, now)
    }
    pub fn mark_busy(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Busy, now)
    }
    pub fn mark_idle(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Idle, now)
    }
    pub fn mark_draining(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Draining, now)
    }
    pub fn mark_stopped(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(EnvironmentState::Stopped, now)
    }
    pub fn mark_failed(
        &mut self,
        reason: impl Into<String>,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.transition(
            EnvironmentState::Failed {
                reason: reason.into(),
            },
            now,
        )
    }
    pub fn mark_lost(
        &mut self,
        reason: impl Into<String>,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.transition(
            EnvironmentState::Lost {
                reason: reason.into(),
            },
            now,
        )
    }

    /// Merge guest-reported evidence (e.g. boot id learnt at handshake).
    pub fn record_guest_boot_id(&mut self, boot_id: impl Into<String>) {
        self.evidence.guest_boot_id = Some(boot_id.into());
    }

    /// True while the environment may still be handed to another attempt: the
    /// guest reported ready and nothing is in flight on it. Deliberately false
    /// for `Requested` / `Provisioning` / `Initializing`, so nothing is ever
    /// dispatched to an environment before it is ready, and for `Busy`, so a
    /// running attempt is never handed out a second time.
    pub fn is_reusable(&self) -> bool {
        matches!(self.state, EnvironmentState::Ready | EnvironmentState::Idle)
    }

    /// Take a pooled environment for a new attempt.
    ///
    /// An `Idle` environment is resumed through `Ready` first, so the state a
    /// dispatch happens from is always `Ready`. The epoch advances by one on
    /// every reassignment: that is what fences the previous attempt out, since
    /// [`ExecutionLease::accepts`] only takes `(attempt_id, epoch)` pairs of
    /// the current lease and the old attempt now carries a stale epoch.
    ///
    /// Fails (leaving the environment untouched) for anything that is not
    /// `Ready` or `Idle`.
    pub fn reassign(&mut self, now: Timestamp) -> Result<u64, DomainError> {
        if matches!(self.state, EnvironmentState::Idle) {
            self.mark_ready(now)?;
        }
        self.mark_busy(now)?;
        self.epoch += 1;
        Ok(self.epoch)
    }

    /// True when the environment has been idle for at least `ttl`. Always
    /// false for an environment that is not idle.
    pub fn idle_expired(&self, now: Timestamp, ttl: chrono::Duration) -> bool {
        match self.idle_since {
            Some(since) => now - since >= ttl,
            None => false,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionLease {
    pub id: LeaseId,
    pub environment_id: EnvironmentId,
    pub attempt_id: AttemptId,
    pub tenant_id: TenantId,
    pub epoch: u64,
    pub acquired_at: Timestamp,
    /// Hard deadline after which the host terminates the environment.
    pub deadline: Timestamp,
    pub released_at: Option<Timestamp>,
}

impl ExecutionLease {
    pub fn acquire(
        id: LeaseId,
        environment_id: EnvironmentId,
        attempt_id: AttemptId,
        tenant_id: TenantId,
        epoch: u64,
        deadline: Timestamp,
        now: Timestamp,
    ) -> Self {
        Self {
            id,
            environment_id,
            attempt_id,
            tenant_id,
            epoch,
            acquired_at: now,
            deadline,
            released_at: None,
        }
    }

    pub fn is_active(&self, now: Timestamp) -> bool {
        self.released_at.is_none() && now < self.deadline
    }

    pub fn release(&mut self, now: Timestamp) -> Result<(), DomainError> {
        if self.released_at.is_some() {
            return Err(DomainError::Terminal {
                entity: "ExecutionLease",
                state: "released".into(),
            });
        }
        self.released_at = Some(now);
        Ok(())
    }

    /// A completion carrying `(attempt_id, epoch)` is only accepted if it matches
    /// this lease exactly. Prevents stale results from overwriting state.
    pub fn accepts(&self, attempt_id: &AttemptId, epoch: u64) -> bool {
        self.released_at.is_none() && &self.attempt_id == attempt_id && self.epoch == epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    fn key(t: &TenantId, r: &RevisionId) -> ReuseKey {
        ReuseKey {
            tenant_id: t.clone(),
            revision_id: r.clone(),
            execution_role_version: 1,
            configuration_version: 1,
            resource_profile_digest: "d".into(),
            runtime_profile: "default".into(),
            network_policy_version: 1,
            secret_binding_generation: 1,
        }
    }

    #[test]
    fn lifecycle() {
        let t = TenantId::generate();
        let r = RevisionId::generate();
        let mut e = ExecutionEnvironment::request(
            EnvironmentId::generate(),
            t.clone(),
            r.clone(),
            ProviderKind::Firecracker,
            key(&t, &r),
            now(),
        );
        assert!(e.mark_ready(now()).is_err(), "cannot skip");
        e.mark_provisioning(now()).unwrap();
        e.mark_initializing(BootEvidence::default(), now()).unwrap();
        e.mark_ready(now()).unwrap();
        assert!(e.ready_at.is_some());
        e.mark_busy(now()).unwrap();
        e.mark_stopped(now()).unwrap();
        assert!(e.is_terminal());
        assert!(e.mark_ready(now()).is_err());
        assert!(
            e.mark_failed("late", now()).is_err(),
            "terminal updates rejected"
        );
    }

    #[test]
    fn failure_from_any_state_and_lost() {
        let t = TenantId::generate();
        let r = RevisionId::generate();
        let mut e = ExecutionEnvironment::request(
            EnvironmentId::generate(),
            t.clone(),
            r.clone(),
            ProviderKind::Process,
            key(&t, &r),
            now(),
        );
        e.mark_lost("host gone", now()).unwrap();
        assert!(matches!(e.state, EnvironmentState::Lost { .. }));
    }

    #[test]
    fn lease_fencing() {
        let att = AttemptId::generate();
        let mut l = ExecutionLease::acquire(
            LeaseId::generate(),
            EnvironmentId::generate(),
            att.clone(),
            TenantId::generate(),
            3,
            now() + Duration::seconds(10),
            now(),
        );
        assert!(l.accepts(&att, 3));
        assert!(!l.accepts(&att, 2), "stale epoch rejected");
        assert!(
            !l.accepts(&AttemptId::generate(), 3),
            "other attempt rejected"
        );
        assert!(l.is_active(now()));
        assert!(!l.is_active(now() + Duration::seconds(11)));
        l.release(now()).unwrap();
        assert!(!l.accepts(&att, 3));
        assert!(l.release(now()).is_err());
    }

    fn ready(t: &TenantId, r: &RevisionId) -> ExecutionEnvironment {
        let mut e = ExecutionEnvironment::request(
            EnvironmentId::generate(),
            t.clone(),
            r.clone(),
            ProviderKind::Fake,
            key(t, r),
            now(),
        );
        e.mark_provisioning(now()).unwrap();
        e.mark_initializing(BootEvidence::default(), now()).unwrap();
        e.mark_ready(now()).unwrap();
        e
    }

    /// Reuse is what makes the `(attempt_id, epoch)` fencing of
    /// `ExecutionLease::accepts` load-bearing: the epoch must advance on every
    /// reassignment so a late frame of the previous attempt cannot settle the
    /// new one.
    #[test]
    fn reassignment_advances_the_epoch_and_fences_the_previous_attempt() {
        let t = TenantId::generate();
        let r = RevisionId::generate();
        let mut e = ready(&t, &r);

        // The first (cold) assignment keeps epoch 1.
        e.mark_busy(now()).unwrap();
        assert_eq!(e.epoch, 1);
        let first = AttemptId::generate();
        let first_lease = ExecutionLease::acquire(
            LeaseId::generate(),
            e.id.clone(),
            first.clone(),
            t.clone(),
            e.epoch,
            now() + Duration::seconds(10),
            now(),
        );

        // Back into the pool, then handed out again.
        e.mark_idle(now()).unwrap();
        assert!(e.is_reusable());
        assert_eq!(e.reassign(now()).unwrap(), 2);
        assert_eq!(e.epoch, 2);
        assert_eq!(e.state, EnvironmentState::Busy);
        assert!(
            !e.is_reusable(),
            "a busy environment is not handed out again"
        );

        let second = AttemptId::generate();
        let second_lease = ExecutionLease::acquire(
            LeaseId::generate(),
            e.id.clone(),
            second.clone(),
            t.clone(),
            e.epoch,
            now() + Duration::seconds(10),
            now(),
        );
        assert!(second_lease.accepts(&second, 2));
        assert!(
            !second_lease.accepts(&second, 1),
            "a frame carrying the previous epoch is stale"
        );
        assert!(
            !second_lease.accepts(&first, 1) && !second_lease.accepts(&first, 2),
            "the previous attempt can never settle the new one"
        );
        assert!(
            first_lease.accepts(&first, 1) && !first_lease.accepts(&first, 2),
            "the old lease still only takes its own epoch"
        );
    }

    #[test]
    fn only_ready_or_idle_environments_are_handed_out() {
        let t = TenantId::generate();
        let r = RevisionId::generate();
        let fresh = || {
            ExecutionEnvironment::request(
                EnvironmentId::generate(),
                t.clone(),
                r.clone(),
                ProviderKind::Fake,
                key(&t, &r),
                now(),
            )
        };

        let mut requested = fresh();
        assert!(!requested.is_reusable());
        assert!(
            requested.reassign(now()).is_err(),
            "nothing is dispatched before Ready"
        );
        assert_eq!(
            requested.epoch, 1,
            "a refused reassignment does not move the epoch"
        );

        let mut initializing = fresh();
        initializing.mark_provisioning(now()).unwrap();
        initializing
            .mark_initializing(BootEvidence::default(), now())
            .unwrap();
        assert!(!initializing.is_reusable());
        assert!(initializing.reassign(now()).is_err());

        let mut busy = ready(&t, &r);
        busy.mark_busy(now()).unwrap();
        assert!(!busy.is_reusable());
        assert!(
            busy.reassign(now()).is_err(),
            "a busy environment is never handed out"
        );
        assert_eq!(busy.epoch, 1);

        busy.mark_stopped(now()).unwrap();
        assert!(!busy.is_reusable());
        assert!(busy.reassign(now()).is_err());
    }

    #[test]
    fn idle_since_tracks_pool_membership_and_the_ttl() {
        let t = TenantId::generate();
        let r = RevisionId::generate();
        let mut e = ready(&t, &r);
        e.mark_busy(now()).unwrap();
        assert_eq!(e.idle_since, None);
        assert!(
            !e.idle_expired(now() + Duration::days(1), Duration::seconds(1)),
            "only idle environments expire"
        );

        e.mark_idle(now()).unwrap();
        assert_eq!(e.idle_since, Some(now()));
        let ttl = Duration::seconds(60);
        assert!(!e.idle_expired(now() + Duration::seconds(59), ttl));
        assert!(e.idle_expired(now() + Duration::seconds(60), ttl));

        e.reassign(now()).unwrap();
        assert_eq!(e.idle_since, None, "leaving the pool clears the TTL clock");
    }
}

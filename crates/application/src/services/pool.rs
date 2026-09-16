//! Environment pool: warm reuse of execution environments (PLT-4632).
//!
//! An environment is only ever reused when **both** gates open:
//!
//! 1. the provider reports `idle_quiesce` *and* `idle_resume` as
//!    [`Support::Supported`](tachyon_serverless_provider_port::Support).
//!    `Unverified` is deliberately not enough: a runtime profile whose idle
//!    behaviour has not been measured keeps destroy-after-invoke; and
//! 2. `[pool] enabled = true` in the gateway configuration.
//!
//! Both shipped providers report `Unsupported` (docs/adr/0001 §5), so with
//! them this module only ever decides "no" and the invoke pipeline behaves
//! exactly as it did in P1.
//!
//! The pool has two halves.
//!
//! - The **ledger half** lives behind [`EnvironmentRepository`]. It owns
//!   membership, decides who gets an idle environment — one store mutation,
//!   so of two concurrent claims exactly one wins — and advances the epoch on
//!   every reassignment, which is what fences the previous attempt out
//!   (docs/threat-model.md T05).
//! - The **in-process half** is the live [`BridgeSession`] of each pooled
//!   environment. An open stream to a guest cannot be persisted, so this half
//!   dies with the process and the startup reconcile
//!   ([`crate::services::ReconcileService`]) reclaims what it left behind.
//!
//! [`EnvironmentRepository`]: crate::repository::EnvironmentRepository

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_domain::{
    Clock, EnvironmentId, EvidenceQuality, ExecutionEnvironment, FunctionRevision, ReuseKey,
    Sha256Digest, TenantId, Timestamp, UsageEvent, UsageEventType,
};
use tachyon_serverless_provider_port::{
    Capabilities, ExecutionProvider, TerminateReason, UsageSink,
};

use crate::bridge_session::BridgeSession;
use crate::config::PoolConfig;
use crate::repository::{PoolLimits, Repositories};

// ---------------------------------------------------------------------------
// policy
// ---------------------------------------------------------------------------

/// Why reuse is off. Recorded so an operator can tell a deliberate
/// configuration from an unverified capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReuseDisabled {
    /// `[pool] enabled = false`.
    Configuration,
    /// The provider does not report `idle_quiesce` as `Supported`.
    IdleQuiesceNotSupported,
    /// The provider does not report `idle_resume` as `Supported`.
    IdleResumeNotSupported,
}

impl ReuseDisabled {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Configuration => "pool.enabled is false",
            Self::IdleQuiesceNotSupported => "provider does not report idle_quiesce as supported",
            Self::IdleResumeNotSupported => "provider does not report idle_resume as supported",
        }
    }
}

/// The reuse decision for one (provider, configuration) pair. Computed once at
/// bootstrap; every reuse path consults it first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolPolicy {
    disabled: Option<ReuseDisabled>,
    limits: PoolLimits,
    idle_ttl: chrono::Duration,
}

impl PoolPolicy {
    pub fn decide(caps: &Capabilities, cfg: &PoolConfig) -> Self {
        let disabled = if !cfg.enabled {
            Some(ReuseDisabled::Configuration)
        } else if !caps.idle_quiesce.is_supported() {
            Some(ReuseDisabled::IdleQuiesceNotSupported)
        } else if !caps.idle_resume.is_supported() {
            Some(ReuseDisabled::IdleResumeNotSupported)
        } else {
            None
        };
        Self {
            disabled,
            limits: PoolLimits {
                max_idle_per_key: cfg.max_idle_per_revision,
                max_total_idle: cfg.max_total_idle,
            },
            idle_ttl: cfg.idle_ttl_chrono(),
        }
    }

    /// Reuse off, for a provider that cannot be asked (tests).
    pub fn off() -> Self {
        Self {
            disabled: Some(ReuseDisabled::Configuration),
            limits: PoolLimits {
                max_idle_per_key: 0,
                max_total_idle: 0,
            },
            idle_ttl: chrono::Duration::zero(),
        }
    }

    pub fn reuse_enabled(&self) -> bool {
        self.disabled.is_none()
    }

    pub fn disabled_reason(&self) -> Option<ReuseDisabled> {
        self.disabled
    }

    pub fn limits(&self) -> PoolLimits {
        self.limits
    }

    pub fn idle_ttl(&self) -> chrono::Duration {
        self.idle_ttl
    }
}

// ---------------------------------------------------------------------------
// reuse key
// ---------------------------------------------------------------------------

/// First 64 bits of the SHA-256 of `bytes`, as a version-like number.
fn digest_u64(bytes: &[u8]) -> u64 {
    let digest = Sha256Digest::of_bytes(bytes);
    digest
        .hex()
        .get(..16)
        .and_then(|prefix| u64::from_str_radix(prefix, 16).ok())
        .unwrap_or(0)
}

/// Random salt of this process, mixed into [`secret_binding_generation`].
///
/// `RandomState` seeds itself from the OS; two hashers built from the same one
/// give eight bytes each without adding a dependency.
static SECRET_GENERATION_SALT: LazyLock<[u8; 16]> = LazyLock::new(|| {
    let state = RandomState::new();
    let mut salt = [0u8; 16];
    for (i, chunk) in salt.chunks_mut(8).enumerate() {
        let mut hasher = state.build_hasher();
        hasher.write_usize(i);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    salt
});

/// Generation of a revision's *resolved* secret bindings: a salted digest over
/// the `(env name, binding ref, value)` triples, in binding order, each part
/// length-prefixed so that no two different binding lists can produce the same
/// buffer.
///
/// The value has to take part, otherwise a rotated secret would silently keep
/// reaching a guest that was started under the previous one.
///
/// **What this puts in the ledger.** The number lands in [`ReuseKey`] and is
/// written to `state.json`: it is a *digest derived from resolved secret
/// values*, not a value, and it is salted with a per-process random salt, so a
/// reader of the state file cannot test a guessed secret against it. The
/// values themselves stay in memory and only reach `HelloAck`
/// (docs/threat-model.md §6-4). The generation is therefore only comparable
/// inside this process — which is all the pool needs, because a pooled
/// environment never outlives the process that holds its session.
///
/// A revision with no bindings has generation 0.
pub fn secret_binding_generation<'a>(
    resolved: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
) -> u64 {
    let mut buf: Vec<u8> = SECRET_GENERATION_SALT.to_vec();
    let mut any = false;
    for (env_name, binding_ref, value) in resolved {
        any = true;
        for part in [env_name, binding_ref, value] {
            // Length-prefixed, not separated: a separator can appear inside a
            // value, and then two different binding lists share a buffer.
            buf.extend_from_slice(&(part.len() as u64).to_le_bytes());
            buf.extend_from_slice(part.as_bytes());
        }
    }
    if !any {
        return 0;
    }
    digest_u64(&buf)
}

/// Build the reuse key of a revision (RFC §5.3, [`ReuseKey`]).
///
/// Every dimension that would make two guests behave differently is a field,
/// and an environment is only reused when *all* of them match:
///
/// - `tenant_id` / `revision_id`: never cross either boundary;
/// - `execution_role_version`: fixed at 1 — the prototype has no versioned
///   execution role, so there is nothing to vary yet;
/// - `configuration_version`: digest of everything the guest is configured
///   with that no other field covers (artifact digest, non-secret env vars,
///   execution policy), so a changed configuration never shares a guest;
/// - `resource_profile_digest`: digest of the resource profile;
/// - `runtime_profile`: the runtime protocol the guest speaks;
/// - `network_policy_version`: derived from the egress profile;
/// - `secret_binding_generation`: see [`secret_binding_generation`].
pub fn reuse_key_for(
    tenant_id: &TenantId,
    revision: &FunctionRevision,
    secret_binding_generation: u64,
) -> ReuseKey {
    let spec = &revision.spec;
    let configuration = serde_json::json!({
        "artifact": spec.artifact.digest().as_str(),
        "env_vars": spec.env_vars,
        "execution": spec.execution,
        "secret_bindings": spec.secrets,
    });
    ReuseKey {
        tenant_id: tenant_id.clone(),
        revision_id: revision.id.clone(),
        execution_role_version: 1,
        configuration_version: digest_u64(&serde_json::to_vec(&configuration).unwrap_or_default()),
        resource_profile_digest: Sha256Digest::of_bytes(
            &serde_json::to_vec(&spec.resources).unwrap_or_default(),
        )
        .hex()
        .to_string(),
        runtime_profile: spec.runtime.protocol.clone(),
        network_policy_version: digest_u64(&serde_json::to_vec(&spec.egress).unwrap_or_default()),
        secret_binding_generation,
    }
}

// ---------------------------------------------------------------------------
// pool
// ---------------------------------------------------------------------------

/// A pooled environment handed to an attempt: the ledger row — already `Busy`
/// at its new epoch — and the live session of its guest.
pub struct WarmEnvironment {
    pub environment: ExecutionEnvironment,
    pub session: BridgeSession,
}

impl std::fmt::Debug for WarmEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WarmEnvironment")
            .field("environment_id", &self.environment.id)
            .field("epoch", &self.environment.epoch)
            .finish_non_exhaustive()
    }
}

/// Result of one TTL sweep (or drain).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PoolSweep {
    /// Idle environments the sweeper looked at.
    pub examined: usize,
    /// Environments it terminated (including retries of earlier failures).
    pub reaped: usize,
    /// Expired environments an attempt claimed before the sweeper could.
    pub raced: usize,
    /// Environments whose terminate failed. They stay `Draining` and the next
    /// sweep retries them.
    pub failed: usize,
}

pub struct EnvironmentPool {
    repos: Repositories,
    provider: Arc<dyn ExecutionProvider>,
    usage: Arc<dyn UsageSink>,
    clock: Arc<dyn Clock>,
    policy: PoolPolicy,
    /// Live guest connections of the pooled environments. Never persisted.
    sessions: Mutex<HashMap<EnvironmentId, BridgeSession>>,
    /// Environments this process took out of the pool but could not terminate.
    /// Their rows stay `Draining`, so no attempt can take them, and the next
    /// sweep tries again.
    pending_termination: Mutex<Vec<EnvironmentId>>,
}

impl std::fmt::Debug for EnvironmentPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvironmentPool")
            .field("policy", &self.policy)
            .field("sessions", &self.sessions.lock().len())
            .finish_non_exhaustive()
    }
}

impl EnvironmentPool {
    pub fn new(
        repos: Repositories,
        provider: Arc<dyn ExecutionProvider>,
        usage: Arc<dyn UsageSink>,
        clock: Arc<dyn Clock>,
        policy: PoolPolicy,
    ) -> Self {
        Self {
            repos,
            provider,
            usage,
            clock,
            policy,
            sessions: Mutex::new(HashMap::new()),
            pending_termination: Mutex::new(Vec::new()),
        }
    }

    pub fn policy(&self) -> &PoolPolicy {
        &self.policy
    }

    /// Environments this process currently holds open.
    pub fn held(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Take one pooled environment whose reuse key matches `key` exactly.
    ///
    /// `None` means the caller creates a cold environment: either reuse is
    /// gated off, or nothing in the pool matches.
    pub async fn claim(&self, key: &ReuseKey) -> Option<WarmEnvironment> {
        if !self.policy.reuse_enabled() {
            return None;
        }
        loop {
            let environment = match self
                .repos
                .environments
                .claim_for_reuse(key, self.clock.now())
            {
                Ok(Some(env)) => env,
                Ok(None) => return None,
                Err(e) => {
                    tracing::warn!(error = %e, "reuse lookup failed; starting a cold environment");
                    return None;
                }
            };
            // The ledger claim above already picked the single winner, so at
            // most one caller can ever take the session below.
            let session = self.sessions.lock().remove(&environment.id);
            match session {
                Some(mut session) => {
                    // Anything the guest queued while the environment was idle
                    // (or left over from the attempt before) belongs to the
                    // past, not to the attempt about to be dispatched. This
                    // also detects a guest that died while idle: it makes the
                    // session unusable instead of letting the next attempt run
                    // into it.
                    session.drain_stale().await;
                    if session.is_usable() {
                        tracing::debug!(
                            environment_id = %environment.id,
                            epoch = environment.epoch,
                            "reusing a pooled environment"
                        );
                        return Some(WarmEnvironment {
                            environment,
                            session,
                        });
                    }
                    drop(session);
                    self.retire(environment, "the pooled guest is gone").await;
                }
                // The row says pooled but there is no live guest behind it:
                // the row outlived the process that held its session. Retire
                // it and try the next candidate.
                None => {
                    self.retire(environment, "pooled environment has no session")
                        .await;
                }
            }
        }
    }

    /// Hand a finished environment back to the pool.
    ///
    /// `env` is the caller's `Busy` row for the attempt that just finished.
    /// `Ok` carries the pooled row (now `Idle`) and the pool has taken the
    /// session; `Err` hands the session back (boxed, because a live session is
    /// a large value), which means the caller shuts it down and terminates the
    /// environment exactly as it did in P1.
    pub fn release(
        &self,
        env: &ExecutionEnvironment,
        session: BridgeSession,
    ) -> Result<ExecutionEnvironment, Box<BridgeSession>> {
        if !self.policy.reuse_enabled() || !session.is_usable() {
            return Err(Box::new(session));
        }
        let now = self.clock.now();
        // The sessions lock is held *across* the ledger mutation: the moment
        // the row becomes `Idle` it is claimable, and a claimer that finds it
        // without its session would treat a healthy environment as dead and
        // terminate it. Holding the lock makes the row and its session appear
        // together. `claim` never holds this lock while taking the ledger's,
        // so the two orders cannot deadlock.
        let mut sessions = self.sessions.lock();
        match self
            .repos
            .environments
            .release_to_pool(env, self.policy.limits, now)
        {
            Ok(Some(pooled)) => {
                sessions.insert(pooled.id.clone(), session);
                tracing::debug!(
                    environment_id = %pooled.id,
                    epoch = pooled.epoch,
                    "environment returned to the pool"
                );
                Ok(pooled)
            }
            Ok(None) => Err(Box::new(session)),
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %env.id, "cannot pool environment");
                Err(Box::new(session))
            }
        }
    }

    /// Terminate every idle environment that is past its TTL.
    pub async fn sweep(&self) -> PoolSweep {
        self.reap(false).await
    }

    /// Terminate every idle environment regardless of its TTL (shutdown).
    pub async fn drain(&self) -> PoolSweep {
        self.reap(true).await
    }

    async fn reap(&self, everything: bool) -> PoolSweep {
        let now = self.clock.now();
        let ttl = self.policy.idle_ttl;
        let mut report = PoolSweep::default();
        // Environments an earlier sweep could not terminate are still on the
        // host. Their rows stayed `Draining`, so no attempt can take them and
        // nothing lists them any more: this process keeps their ids and tries
        // again here.
        let pending: Vec<EnvironmentId> = std::mem::take(&mut self.pending_termination.lock());
        for id in pending {
            if self
                .terminate_and_settle(&id, "retrying a failed termination")
                .await
            {
                report.reaped += 1;
            } else {
                report.failed += 1;
            }
        }
        let idle = match self.repos.environments.list_idle() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "cannot list idle environments");
                return report;
            }
        };
        report.examined = idle.len();
        for env in idle {
            if !everything && !env.idle_expired(now, ttl) {
                continue;
            }
            // Take it out of the pool before terminating anything: an
            // environment the sweeper owns must never reach an attempt.
            match self
                .repos
                .environments
                .take_idle_for_termination(&env.id, now)
            {
                Ok(true) => {}
                Ok(false) => {
                    report.raced += 1;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, environment_id = %env.id, "cannot take idle environment");
                    continue;
                }
            }
            if self.terminate_and_settle(&env.id, "idle timeout").await {
                report.reaped += 1;
                tracing::info!(environment_id = %env.id, "idle environment reaped");
            } else {
                report.failed += 1;
            }
        }
        if report.reaped > 0 || report.raced > 0 || report.failed > 0 {
            tracing::info!(
                examined = report.examined,
                reaped = report.reaped,
                raced = report.raced,
                failed = report.failed,
                "idle sweep finished"
            );
        }
        report
    }

    /// Terminate one environment the pool owns (its row is `Draining`) and
    /// settle it. `true` when it is really gone.
    ///
    /// Only a terminate that actually succeeded is recorded as a clean stop.
    /// When it fails the host may still be running the environment, so the row
    /// stays `Draining`: out of the pool, but still in `list_active`, so the
    /// startup reconcile of the next process still sees an owner instead of an
    /// environment nobody admits to. The id is kept for the next sweep.
    async fn terminate_and_settle(&self, id: &EnvironmentId, reason: &str) -> bool {
        let session = self.sessions.lock().remove(id);
        if let Some(mut session) = session {
            let _ = session.shutdown(reason).await;
        }
        if let Err(e) = self
            .provider
            .terminate_environment(id, TerminateReason::Shutdown)
            .await
        {
            tracing::warn!(
                error = %e,
                environment_id = %id,
                reason,
                "terminating a pooled environment failed; it stays draining for the next sweep"
            );
            self.pending_termination.lock().push(id.clone());
            return false;
        }
        let now = self.clock.now();
        match self.repos.environments.get(id) {
            Ok(Some(mut env)) => {
                if env.mark_stopped(now).is_ok()
                    && let Err(e) = self.repos.environments.update(env.clone())
                {
                    tracing::warn!(error = %e, environment_id = %id, "cannot record a reaped environment");
                }
                self.emit_stopped(&env, now).await;
            }
            Ok(None) => {
                tracing::warn!(environment_id = %id, "terminated an environment the ledger no longer has")
            }
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %id, "cannot load a reaped environment")
            }
        }
        true
    }

    /// Terminate a pooled environment we cannot use and settle its row.
    async fn retire(&self, mut env: ExecutionEnvironment, reason: &'static str) {
        tracing::warn!(environment_id = %env.id, reason, "retiring a pooled environment");
        let terminated = self
            .provider
            .terminate_environment(&env.id, TerminateReason::Reconcile)
            .await;
        let now = self.clock.now();
        let settled = match &terminated {
            Ok(_) => env.mark_failed(reason, now),
            // It may still be on the host: `Lost` says so, and the startup
            // reconcile of the next process reclaims it.
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %env.id, "terminating a retired environment failed");
                env.mark_lost(format!("{reason}; terminate failed: {e}"), now)
            }
        };
        if settled.is_ok()
            && let Err(e) = self.repos.environments.update(env.clone())
        {
            tracing::warn!(error = %e, environment_id = %env.id, "cannot record a retired environment");
        }
        self.emit_stopped(&env, now).await;
    }

    /// Report the end of a pooled environment's life to usage.
    ///
    /// The driver deliberately emits nothing when it hands an environment to
    /// the pool (it did not stop), so this is the only `EnvironmentStopped`
    /// such an environment ever gets: without it the whole warm part of its
    /// lifetime would never reach metering.
    ///
    /// `monotonic_duration_ms` is the host-observed lifetime, from the ledger's
    /// `created_at` to now — the environment outlived every single attempt on
    /// it, so no attempt's stopwatch can measure it. The id is
    /// `<environment>:<epoch>:pool-stopped`, which never collides with the
    /// driver's `<environment>:<epoch>:<sequence>` (a sequence is a number) and
    /// is stable, so a re-send of the same event still de-duplicates.
    async fn emit_stopped(&self, env: &ExecutionEnvironment, now: Timestamp) {
        let resources = self
            .repos
            .revisions
            .get(&env.revision_id)
            .ok()
            .flatten()
            .map(|r| r.spec.resources)
            .unwrap_or_default();
        self.usage
            .record(UsageEvent {
                event_id: format!("{}:{}:pool-stopped", env.id, env.epoch),
                tenant_id: env.tenant_id.clone(),
                environment_id: env.id.clone(),
                invocation_id: None,
                attempt_id: None,
                event_type: UsageEventType::EnvironmentStopped,
                sequence: env.epoch,
                observed_at: now,
                monotonic_duration_ms: Some((now - env.created_at).num_milliseconds().max(0) as u64),
                memory_mib: resources.memory_mib,
                cpu_millis: resources.cpu_millis,
                bytes_in: 0,
                bytes_out: 0,
                meter_version: 1,
                evidence_quality: EvidenceQuality::HostObserved,
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::{FramedRead, FramedWrite};

    use crate::bridge_session::{HelloAckParams, LogContext, LogForwarder};
    use crate::repository::{EnvironmentRepository, InMemoryStore, RepoError};
    use tachyon_serverless_domain::{
        Architecture, BootEvidence, EnvironmentState, ExecutionLease, LeaseId, Limits,
        ProviderKind, RevisionId, SystemClock,
    };
    use tachyon_serverless_protocol::{FrameCodec, GuestMessage, PROTOCOL_VERSION, encode_message};
    use tachyon_serverless_provider_port::{
        ArtifactLocation, EnvironmentHandle, EnvironmentObservation, EnvironmentSpec,
        IsolationLevel, PreflightReport, ProviderError, Support, TerminateReport,
    };

    fn caps(idle_quiesce: Support, idle_resume: Support) -> Capabilities {
        Capabilities {
            isolation: IsolationLevel::MicroVm,
            create_terminate: Support::Supported,
            observe: Support::Supported,
            enforce_deadline: Support::Supported,
            enforce_resource_limits: Support::unverified("not measured"),
            egress_none: Support::Supported,
            egress_restricted: Support::unsupported("P1"),
            egress_public_web: Support::unsupported("P1"),
            host_metering: Support::unverified("not measured"),
            idle_quiesce,
            idle_resume,
            snapshot_create: Support::unsupported("P1"),
            snapshot_clone: Support::unsupported("P1"),
            dev_only: false,
        }
    }

    fn on() -> PoolConfig {
        PoolConfig {
            enabled: true,
            ..PoolConfig::default()
        }
    }

    /// PLT-4632 acceptance 4: a runtime profile whose idle capability is not
    /// `Supported` has reuse disabled, whatever the configuration says.
    #[test]
    fn reuse_needs_both_idle_capabilities_supported_and_the_config_on() {
        let supported = || Support::Supported;
        let unverified = || Support::unverified("not measured on real hardware");
        let unsupported = || Support::unsupported("destroy-after-invoke");

        let both = PoolPolicy::decide(&caps(supported(), supported()), &on());
        assert!(both.reuse_enabled());
        assert_eq!(both.disabled_reason(), None);

        for (quiesce, resume, why) in [
            (
                unverified(),
                supported(),
                ReuseDisabled::IdleQuiesceNotSupported,
            ),
            (
                unsupported(),
                supported(),
                ReuseDisabled::IdleQuiesceNotSupported,
            ),
            (
                supported(),
                unverified(),
                ReuseDisabled::IdleResumeNotSupported,
            ),
            (
                supported(),
                unsupported(),
                ReuseDisabled::IdleResumeNotSupported,
            ),
        ] {
            let policy = PoolPolicy::decide(&caps(quiesce, resume), &on());
            assert!(!policy.reuse_enabled(), "{why:?}");
            assert_eq!(policy.disabled_reason(), Some(why));
        }

        // A fully capable provider still obeys the configuration switch.
        let off = PoolPolicy::decide(&caps(supported(), supported()), &PoolConfig::default());
        assert!(!off.reuse_enabled());
        assert_eq!(off.disabled_reason(), Some(ReuseDisabled::Configuration));
        assert!(!PoolPolicy::off().reuse_enabled());
    }

    #[test]
    fn pool_policy_carries_the_configured_caps_and_ttl() {
        let cfg = PoolConfig {
            enabled: true,
            max_idle_per_revision: 3,
            idle_ttl_seconds: 45,
            max_total_idle: 9,
        };
        let policy = PoolPolicy::decide(&caps(Support::Supported, Support::Supported), &cfg);
        assert_eq!(policy.limits().max_idle_per_key, 3);
        assert_eq!(policy.limits().max_total_idle, 9);
        assert_eq!(policy.idle_ttl(), chrono::Duration::seconds(45));
    }

    #[test]
    fn a_changed_secret_value_supersedes_the_binding_generation() {
        let base = secret_binding_generation([("DB", "db-binding", "old-value")]);
        assert_ne!(base, 0);
        assert_eq!(
            base,
            secret_binding_generation([("DB", "db-binding", "old-value")]),
            "the generation is stable for the same resolved bindings"
        );
        for changed in [
            ("DB", "db-binding", "new-value"),
            ("DB", "other-binding", "old-value"),
            ("OTHER", "db-binding", "old-value"),
        ] {
            assert_ne!(
                base,
                secret_binding_generation([changed]),
                "{changed:?} must supersede the generation"
            );
        }
        assert_eq!(
            secret_binding_generation(std::iter::empty()),
            0,
            "a revision without bindings has no generation"
        );
        // Order and grouping take part: two bindings are not one long one.
        assert_ne!(
            secret_binding_generation([("A", "a", "1"), ("B", "b", "2")]),
            secret_binding_generation([("B", "b", "2"), ("A", "a", "1")])
        );
    }

    /// Regression (review F8): what the ledger stores is a *salted* digest,
    /// and the parts cannot be re-cut into a different binding list.
    #[test]
    fn the_secret_generation_is_salted_and_unambiguous() {
        // A separator can appear inside a value; a length prefix cannot be
        // forged that way.
        assert_ne!(
            secret_binding_generation([("A", "B\0C", "D")]),
            secret_binding_generation([("A", "B", "C\0D")]),
            "two different binding lists must never share a generation"
        );

        // The stored number is not a digest of the resolved values alone, so
        // nobody holding state.json can test a guessed secret against it.
        let unsalted = {
            let mut buf = Vec::new();
            for part in ["DB", "db-binding", "s3cr3t"] {
                buf.extend_from_slice(part.as_bytes());
                buf.push(0);
            }
            digest_u64(&buf)
        };
        assert_ne!(
            secret_binding_generation([("DB", "db-binding", "s3cr3t")]),
            unsalted,
            "the digest of the values themselves must not be what is stored"
        );

        // It still does its job inside the process: stable, and superseded by
        // a rotation.
        assert_eq!(
            secret_binding_generation([("DB", "db-binding", "s3cr3t")]),
            secret_binding_generation([("DB", "db-binding", "s3cr3t")])
        );
        assert_ne!(
            secret_binding_generation([("DB", "db-binding", "s3cr3t")]),
            secret_binding_generation([("DB", "db-binding", "rotated")])
        );
    }

    // -----------------------------------------------------------------------
    // a pool with a real ledger, a stub provider and live sessions
    // -----------------------------------------------------------------------

    /// Records terminate calls and can be made to fail them. Nothing in these
    /// tests creates an environment through the provider.
    struct StubProvider {
        terminated: Mutex<Vec<EnvironmentId>>,
        fail_terminate: AtomicBool,
    }

    impl StubProvider {
        fn new() -> Self {
            Self {
                terminated: Mutex::new(Vec::new()),
                fail_terminate: AtomicBool::new(false),
            }
        }
        fn fail_terminate(&self, fail: bool) {
            self.fail_terminate.store(fail, Ordering::SeqCst);
        }
        fn terminated(&self) -> Vec<EnvironmentId> {
            self.terminated.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl ExecutionProvider for StubProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Fake
        }
        fn capabilities(&self) -> Capabilities {
            caps(Support::Supported, Support::Supported)
        }
        async fn preflight(&self) -> Result<PreflightReport, ProviderError> {
            unimplemented!("the pool never preflights")
        }
        async fn validate_artifact(
            &self,
            _: &ArtifactLocation,
            _: Architecture,
        ) -> Result<(), ProviderError> {
            unimplemented!("the pool never validates artifacts")
        }
        async fn create_environment(
            &self,
            _: EnvironmentSpec,
        ) -> Result<EnvironmentHandle, ProviderError> {
            unimplemented!("the pool never creates environments")
        }
        async fn terminate_environment(
            &self,
            id: &EnvironmentId,
            _: TerminateReason,
        ) -> Result<TerminateReport, ProviderError> {
            self.terminated.lock().push(id.clone());
            if self.fail_terminate.load(Ordering::SeqCst) {
                return Err(ProviderError::Internal("terminate failed".into()));
            }
            Ok(TerminateReport {
                was_running: true,
                cleaned: vec![id.to_string()],
            })
        }
        async fn observe_environment(
            &self,
            _: &EnvironmentId,
        ) -> Result<EnvironmentObservation, ProviderError> {
            unimplemented!("the pool never observes")
        }
        async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError> {
            unimplemented!("the pool never lists")
        }
    }

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<UsageEvent>>);

    impl RecordingSink {
        fn events(&self) -> Vec<UsageEvent> {
            self.0.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl UsageSink for RecordingSink {
        async fn record(&self, event: UsageEvent) {
            self.0.lock().push(event);
        }
    }

    /// An `EnvironmentRepository` that runs a hook right after the store has
    /// published the `Idle` row — exactly the window a claimer could use.
    struct ReleaseHook {
        inner: Arc<InMemoryStore>,
        after_release: Box<dyn Fn() + Send + Sync>,
    }

    impl EnvironmentRepository for ReleaseHook {
        fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
            EnvironmentRepository::insert(&*self.inner, env)
        }
        fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError> {
            EnvironmentRepository::get(&*self.inner, id)
        }
        fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
            EnvironmentRepository::update(&*self.inner, env)
        }
        fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
            self.inner.list_active()
        }
        fn list_idle(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
            self.inner.list_idle()
        }
        fn claim_for_reuse(
            &self,
            key: &ReuseKey,
            now: Timestamp,
        ) -> Result<Option<ExecutionEnvironment>, RepoError> {
            self.inner.claim_for_reuse(key, now)
        }
        fn release_to_pool(
            &self,
            env: &ExecutionEnvironment,
            limits: PoolLimits,
            now: Timestamp,
        ) -> Result<Option<ExecutionEnvironment>, RepoError> {
            let released = self.inner.release_to_pool(env, limits, now);
            (self.after_release)();
            released
        }
        fn take_idle_for_termination(
            &self,
            id: &EnvironmentId,
            now: Timestamp,
        ) -> Result<bool, RepoError> {
            self.inner.take_idle_for_termination(id, now)
        }
        fn insert_lease(&self, lease: ExecutionLease) -> Result<(), RepoError> {
            self.inner.insert_lease(lease)
        }
        fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError> {
            self.inner.get_lease(id)
        }
        fn update_lease(&self, lease: ExecutionLease) -> Result<(), RepoError> {
            self.inner.update_lease(lease)
        }
    }

    fn policy_on() -> PoolPolicy {
        PoolPolicy::decide(
            &caps(Support::Supported, Support::Supported),
            &PoolConfig {
                enabled: true,
                max_idle_per_revision: 2,
                // Everything idle is expired, so a sweep reaps immediately.
                idle_ttl_seconds: 0,
                max_total_idle: 4,
            },
        )
    }

    fn reuse_key() -> ReuseKey {
        ReuseKey {
            tenant_id: TenantId::generate(),
            revision_id: RevisionId::generate(),
            execution_role_version: 1,
            configuration_version: 1,
            resource_profile_digest: "rp".into(),
            runtime_profile: "tachyon.runtime.v1".into(),
            network_policy_version: 1,
            secret_binding_generation: 1,
        }
    }

    fn now() -> Timestamp {
        chrono::Utc::now()
    }

    /// A `Busy` environment in the store, as a finished attempt leaves it.
    fn busy_row(store: &Arc<InMemoryStore>, key: &ReuseKey) -> ExecutionEnvironment {
        let mut env = ExecutionEnvironment::request(
            EnvironmentId::generate(),
            key.tenant_id.clone(),
            key.revision_id.clone(),
            ProviderKind::Fake,
            key.clone(),
            now(),
        );
        env.mark_provisioning(now()).unwrap();
        env.mark_initializing(BootEvidence::default(), now())
            .unwrap();
        env.mark_ready(now()).unwrap();
        env.mark_busy(now()).unwrap();
        EnvironmentRepository::insert(&**store, env.clone()).unwrap();
        env
    }

    /// The same, already in the pool.
    fn idle_row(store: &Arc<InMemoryStore>, key: &ReuseKey) -> EnvironmentId {
        let mut env = busy_row(store, key);
        env.mark_idle(now()).unwrap();
        let id = env.id.clone();
        EnvironmentRepository::update(&**store, env).unwrap();
        id
    }

    fn state_of(store: &Arc<InMemoryStore>, id: &EnvironmentId) -> EnvironmentState {
        EnvironmentRepository::get(&**store, id)
            .unwrap()
            .unwrap()
            .state
    }

    /// A session whose guest answers the handshake and then stays connected
    /// and quiet: one the pool may hand out.
    async fn live_session(
        store: &Arc<InMemoryStore>,
        env_id: &EnvironmentId,
        epoch: u64,
    ) -> BridgeSession {
        let (host, guest) = tokio::io::duplex(64 * 1024);
        let id = env_id.to_string();
        tokio::spawn(async move {
            let (r, w) = tokio::io::split(guest);
            let mut reader = FramedRead::new(r, FrameCodec);
            let mut writer = FramedWrite::new(w, FrameCodec);
            let hello = GuestMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                bridge_version: "pool-test".into(),
                environment_id: id,
                guest_boot_id: Some("boot".into()),
                architecture: "aarch64".into(),
            };
            if writer.send(encode_message(&hello).unwrap()).await.is_err() {
                return;
            }
            while let Some(Ok(_)) = reader.next().await {}
        });
        let logs = LogForwarder::new(
            store.clone(),
            Arc::new(SystemClock),
            LogContext {
                tenant_id: TenantId::generate(),
                environment_id: env_id.clone(),
                invocation_id: None,
                max_line_bytes: 64,
            },
        );
        let (session, _) = BridgeSession::handshake(
            Box::new(host),
            env_id,
            epoch,
            HelloAckParams {
                entrypoint: "/function/app".into(),
                args: vec![],
                env: vec![],
                working_dir: "/tmp".into(),
                init_timeout: Duration::from_secs(1),
                max_response_bytes: 1024,
                max_log_line_bytes: 64,
            },
            logs,
            Duration::from_secs(5),
        )
        .await
        .expect("the guest completes the handshake");
        session
    }

    /// Regression (review F3): a claimer must never find the `Idle` row
    /// without the session behind it. The row and its session are published
    /// together, so a claim that races a release either waits for it or misses
    /// it — it never takes a healthy environment for dead and terminates it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_claim_racing_a_release_never_takes_a_row_without_its_session() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let session = live_session(&store, &env.id, env.epoch).await;

        // The claimer runs exactly in the window after the row is published.
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<Option<EnvironmentId>>();
        // The hook is shared, so the receiver needs a lock around it.
        let done_rx = Mutex::new(done_rx);
        let mut repos = Repositories::in_memory(store.clone());
        repos.environments = Arc::new(ReleaseHook {
            inner: store.clone(),
            after_release: Box::new(move || {
                let _ = go_tx.send(());
                // Before the fix the claimer answers here, having taken the
                // row without its session; after it, it is still blocked.
                let _ = done_rx.lock().recv_timeout(Duration::from_millis(500));
            }),
        });
        let provider = Arc::new(StubProvider::new());
        let pool = Arc::new(EnvironmentPool::new(
            repos,
            provider.clone(),
            Arc::new(RecordingSink::default()),
            Arc::new(SystemClock),
            policy_on(),
        ));

        let claimer = {
            let pool = pool.clone();
            let key = key.clone();
            tokio::spawn(async move {
                go_rx.recv().expect("the release signals the claimer");
                let claimed = pool.claim(&key).await;
                let id = claimed.as_ref().map(|w| w.environment.id.clone());
                let _ = done_tx.send(id.clone());
                (id, claimed.map(|w| w.environment.epoch))
            })
        };

        let pooled = pool.release(&env, session).expect("the environment pools");
        assert_eq!(pooled.state, EnvironmentState::Idle);
        let (claimed_id, claimed_epoch) = claimer.await.unwrap();
        assert_eq!(
            claimed_id,
            Some(env.id.clone()),
            "the claimer got the pooled environment together with its session"
        );
        assert_eq!(claimed_epoch, Some(env.epoch + 1));
        assert!(
            provider.terminated().is_empty(),
            "a healthy environment was terminated by a claim that saw the row without its session"
        );
        assert_eq!(pool.held(), 0, "the claimer took the session with the row");
    }

    /// Regression (review F9 and F5): a terminate that failed is not recorded
    /// as a clean stop. The row stays `Draining` — out of the pool, still in
    /// `list_active` so the startup reconcile sees an owner — and the next
    /// sweep retries it. Only a real termination is metered.
    #[tokio::test]
    async fn a_failed_terminate_keeps_the_environment_for_the_next_sweep() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let id = idle_row(&store, &key);
        let provider = Arc::new(StubProvider::new());
        let sink = Arc::new(RecordingSink::default());
        let pool = EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            sink.clone(),
            Arc::new(SystemClock),
            policy_on(),
        );

        provider.fail_terminate(true);
        let swept = pool.sweep().await;
        assert_eq!((swept.examined, swept.reaped, swept.failed), (1, 0, 1));
        assert_eq!(
            state_of(&store, &id),
            EnvironmentState::Draining,
            "a failed terminate must not be recorded as a clean stop"
        );
        assert!(
            EnvironmentRepository::list_active(&*store)
                .unwrap()
                .iter()
                .any(|e| e.id == id),
            "the row stays in the active set, so the startup reconcile still sees an owner"
        );
        assert!(
            sink.events().is_empty(),
            "nothing stopped, so nothing is metered"
        );
        assert!(
            store.list_idle().unwrap().is_empty(),
            "and it is out of the pool either way"
        );

        // The next sweep retries it, and now the host lets go.
        provider.fail_terminate(false);
        let swept = pool.sweep().await;
        assert_eq!((swept.examined, swept.reaped, swept.failed), (0, 1, 0));
        assert_eq!(state_of(&store, &id), EnvironmentState::Stopped);
        assert_eq!(provider.terminated().len(), 2, "it was retried once");

        let events = sink.events();
        assert_eq!(events.len(), 1, "one stop event for one environment");
        assert_eq!(events[0].event_type, UsageEventType::EnvironmentStopped);
        assert_eq!(events[0].environment_id, id);
        assert_eq!(events[0].tenant_id, key.tenant_id);
        assert!(events[0].monotonic_duration_ms.is_some());
        assert!(
            events[0].event_id.ends_with(":pool-stopped"),
            "{}",
            events[0].event_id
        );
    }
}

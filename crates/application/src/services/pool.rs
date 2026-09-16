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
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_domain::{
    Clock, EnvironmentId, ExecutionEnvironment, FunctionRevision, ReuseKey, Sha256Digest, TenantId,
};
use tachyon_serverless_provider_port::{Capabilities, ExecutionProvider, TerminateReason};

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

/// Generation of a revision's *resolved* secret bindings: a digest over the
/// `(env name, binding ref, value)` triples, in binding order.
///
/// The value has to take part, otherwise a rotated secret would silently keep
/// reaching a guest that was started under the previous one. The values are
/// already in memory at the call site (they go into `HelloAck`); the buffer
/// built here is local and only the digest survives, so no secret enters the
/// reuse key, the ledger or any log (docs/threat-model.md §6-4).
///
/// A revision with no bindings has generation 0.
pub fn secret_binding_generation<'a>(
    resolved: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
) -> u64 {
    let mut buf: Vec<u8> = Vec::new();
    let mut any = false;
    for (env_name, binding_ref, value) in resolved {
        any = true;
        for part in [env_name, binding_ref, value] {
            buf.extend_from_slice(part.as_bytes());
            buf.push(0);
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
    /// Of those, the ones it terminated.
    pub reaped: usize,
    /// Expired environments an attempt claimed before the sweeper could.
    pub raced: usize,
}

pub struct EnvironmentPool {
    repos: Repositories,
    provider: Arc<dyn ExecutionProvider>,
    clock: Arc<dyn Clock>,
    policy: PoolPolicy,
    /// Live guest connections of the pooled environments. Never persisted.
    sessions: Mutex<HashMap<EnvironmentId, BridgeSession>>,
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
        clock: Arc<dyn Clock>,
        policy: PoolPolicy,
    ) -> Self {
        Self {
            repos,
            provider,
            clock,
            policy,
            sessions: Mutex::new(HashMap::new()),
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
                Some(session) if session.is_usable() => {
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
                // The row says pooled but there is no live guest behind it:
                // its session died while idle, or the row outlived the
                // process that held it. Retire it and try the next candidate.
                gone => {
                    drop(gone);
                    self.retire(environment, "pooled environment has no usable session")
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
        match self
            .repos
            .environments
            .release_to_pool(env, self.policy.limits, now)
        {
            Ok(Some(pooled)) => {
                self.sessions.lock().insert(pooled.id.clone(), session);
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
        let idle = match self.repos.environments.list_idle() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "cannot list idle environments");
                return PoolSweep::default();
            }
        };
        let mut report = PoolSweep {
            examined: idle.len(),
            ..PoolSweep::default()
        };
        for mut env in idle {
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
            let session = self.sessions.lock().remove(&env.id);
            if let Some(mut session) = session {
                let _ = session.shutdown("idle timeout").await;
            }
            if let Err(e) = self
                .provider
                .terminate_environment(&env.id, TerminateReason::Shutdown)
                .await
            {
                tracing::warn!(error = %e, environment_id = %env.id, "terminating an idle environment failed");
            }
            if env.mark_stopped(self.clock.now()).is_ok()
                && let Err(e) = self.repos.environments.update(env.clone())
            {
                tracing::warn!(error = %e, environment_id = %env.id, "cannot record a reaped environment");
            }
            report.reaped += 1;
            tracing::info!(environment_id = %env.id, "idle environment reaped");
        }
        if report.reaped > 0 || report.raced > 0 {
            tracing::info!(
                examined = report.examined,
                reaped = report.reaped,
                raced = report.raced,
                "idle sweep finished"
            );
        }
        report
    }

    /// Terminate a pooled environment we cannot use and settle its row.
    async fn retire(&self, mut env: ExecutionEnvironment, reason: &'static str) {
        tracing::warn!(environment_id = %env.id, reason, "retiring a pooled environment");
        if let Err(e) = self
            .provider
            .terminate_environment(&env.id, TerminateReason::Reconcile)
            .await
        {
            tracing::warn!(error = %e, environment_id = %env.id, "terminating a retired environment failed");
        }
        if env.mark_failed(reason, self.clock.now()).is_ok()
            && let Err(e) = self.repos.environments.update(env.clone())
        {
            tracing::warn!(error = %e, environment_id = %env.id, "cannot record a retired environment");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_serverless_provider_port::{IsolationLevel, Support};

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
}

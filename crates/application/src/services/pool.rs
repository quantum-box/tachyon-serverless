//! Environment pool: warm reuse of execution environments (PLT-4632).
//!
//! An environment is only ever reused when **both** gates open:
//!
//! 1. the provider reports `idle_quiesce` *and* `idle_resume` as
//!    [`Support::Supported`](tachyon_serverless_provider_port::Support).
//!    `Unverified` is deliberately not enough: a runtime profile whose idle
//!    behaviour has not been measured keeps destroy-after-invoke — unless an
//!    operator sets `[pool] allow_unverified_idle` in order to *take* that
//!    measurement, and then [`PoolPolicy::idle_verified`] stays false so
//!    nothing can report the run as a verified warm setup; and
//! 2. `[pool] enabled = true` in the gateway configuration.
//!
//! With the shipped providers and the default configuration this module only
//! ever decides "no" — the process provider reports `Unsupported`, the
//! Firecracker provider `Unverified` (docs/adr/0001 §5) — and the invoke
//! pipeline behaves exactly as it did in P1.
//!
//! When reuse is on, this module also owns the provider side of an idle
//! environment: it is quiesced on the way into the pool and resumed on the way
//! out, the resume is followed by a readiness check that actually asks the
//! guest ([`BridgeSession::probe_ready`]), and an environment that does not
//! come back is retired instead of being dispatched into (PLT-4633).
//!
//! Three properties of that ownership are worth stating, because each of them
//! was a defect first:
//!
//! - **The quiesce is not on the caller's response path.** [`Self::release`]
//!   takes the environment over and returns; the pause, with the provider's
//!   API timeout behind it, happens in a task of the pool's own. The ledger
//!   row stays `Busy` — nothing can claim it — until the guest really is
//!   paused, and an environment that cannot be paused is terminated and
//!   metered by the pool rather than pooled or leaked.
//! - **A quiesced guest is never sent a `Shutdown` frame** and never waited
//!   for: its vCPUs are stopped, so it can neither read the frame nor power
//!   itself off. Those terminations use
//!   [`TerminateReason::Quiesced`](tachyon_serverless_provider_port::TerminateReason::Quiesced).
//! - **Readiness is evidence, not inference.** A resumed environment answers a
//!   probe before anything is dispatched into it.
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
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_api_types::ReuseInfo;
use tachyon_serverless_domain::{
    Clock, EnvironmentId, EvidenceQuality, ExecutionEnvironment, FunctionRevision, ReuseKey,
    Sha256Digest, TenantId, Timestamp, UsageEvent, UsageEventType,
};
use tachyon_serverless_provider_port::{
    Capabilities, ExecutionProvider, Support, TerminateReason, UsageSink,
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

/// How one idle capability was judged when the policy was decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdleGate {
    /// `Supported`: implemented *and* measured. The only state that makes a
    /// warm configuration a verified one.
    Verified,
    /// `Unverified`, accepted only because `[pool] allow_unverified_idle` is
    /// set: reuse runs so that the measurement can be taken.
    Measuring,
    /// Not usable: `Unsupported`, or `Unverified` without the switch.
    Blocked,
}

impl IdleGate {
    fn of(support: &Support, allow_unverified: bool) -> Self {
        if support.is_supported() {
            Self::Verified
        } else if support.is_unverified() && allow_unverified {
            Self::Measuring
        } else {
            Self::Blocked
        }
    }
}

/// The reuse decision for one (provider, configuration) pair. Computed once at
/// bootstrap; every reuse path consults it first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolPolicy {
    disabled: Option<ReuseDisabled>,
    /// The **provider** reports both idle capabilities as `Supported`, i.e.
    /// its pause/resume cycle was measured on real hardware. A fact about the
    /// provider alone: it does not say whether reuse is switched on.
    verified: bool,
    /// One sentence naming the gate that decided this, for the bootstrap log
    /// and `GET /v1/provider`.
    reason: &'static str,
    limits: PoolLimits,
    idle_ttl: chrono::Duration,
}

impl PoolPolicy {
    pub fn decide(caps: &Capabilities, cfg: &PoolConfig) -> Self {
        let quiesce = IdleGate::of(&caps.idle_quiesce, cfg.allow_unverified_idle);
        let resume = IdleGate::of(&caps.idle_resume, cfg.allow_unverified_idle);
        let disabled = if !cfg.enabled {
            Some(ReuseDisabled::Configuration)
        } else if quiesce == IdleGate::Blocked {
            Some(ReuseDisabled::IdleQuiesceNotSupported)
        } else if resume == IdleGate::Blocked {
            Some(ReuseDisabled::IdleResumeNotSupported)
        } else {
            None
        };
        // A capability fact, not a gate fact (PLT-4633 review F7): "was the
        // provider's idle support measured" is answered by the provider's
        // report and by nothing else. Folding `[pool] enabled` into it made a
        // measured provider with reuse switched off report `verified: false`,
        // which reads as "nobody measured this" — the opposite of the truth.
        // Whether reuse is *running* is `enabled`, and the pairing that must
        // never be mistaken for a warm success (enabled and not verified) is
        // still exactly that pairing.
        let verified = quiesce == IdleGate::Verified && resume == IdleGate::Verified;
        // Only point at the switch when the switch would actually help, i.e.
        // the capability that blocked this is `Unverified` (code exists,
        // nobody measured it) rather than `Unsupported` (no code at all).
        let blocked_on_unverified = match disabled {
            Some(ReuseDisabled::IdleQuiesceNotSupported) => caps.idle_quiesce.is_unverified(),
            Some(ReuseDisabled::IdleResumeNotSupported) => caps.idle_resume.is_unverified(),
            _ => false,
        };
        let reason = match (disabled, verified, blocked_on_unverified) {
            (None, true, _) => {
                "the provider reports both idle capabilities as supported and [pool] enabled is true"
            }
            (None, false, _) => {
                "[pool] allow_unverified_idle accepts an idle capability nobody has measured: \
                 this is a measurement run, not a verified warm configuration"
            }
            (Some(ReuseDisabled::Configuration), ..) => "[pool] enabled is false",
            (Some(_), _, true) => {
                "the provider reports an idle capability as unverified and \
                 [pool] allow_unverified_idle is not set"
            }
            (Some(_), _, false) => "the provider does not support idle quiesce and resume",
        };
        Self {
            disabled,
            verified,
            reason,
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
            verified: false,
            reason: "[pool] enabled is false",
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

    /// True when the provider reports both idle capabilities as `Supported`,
    /// i.e. its pause/resume cycle was measured on real hardware.
    ///
    /// This is a fact about the provider, independent of `[pool] enabled`: a
    /// gateway that has reuse switched off still reports the truth about the
    /// provider it runs (review F7).
    ///
    /// False while reuse runs on `[pool] allow_unverified_idle`, and that
    /// distinction is the point of the switch: such a run exists to *produce*
    /// the measurement, so no API response, log line or piece of evidence may
    /// present `reuse_enabled() && !idle_verified()` as a warm success
    /// (PLT-4633 acceptance 4).
    pub fn idle_verified(&self) -> bool {
        self.verified
    }

    pub fn disabled_reason(&self) -> Option<ReuseDisabled> {
        self.disabled
    }

    /// One sentence an operator can act on, on the enabled and the disabled
    /// path alike.
    pub fn reason(&self) -> &'static str {
        self.reason
    }

    /// What `GET /v1/provider` reports about reuse: what the gateway does,
    /// whether it was ever measured, why, and the two capabilities behind it.
    pub fn info(&self, caps: &Capabilities) -> ReuseInfo {
        ReuseInfo {
            enabled: self.reuse_enabled(),
            verified: self.idle_verified(),
            reason: self.reason.to_string(),
            idle_quiesce: caps.idle_quiesce.status_str().to_string(),
            idle_resume: caps.idle_resume.status_str().to_string(),
        }
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
        // The allowlist is part of the policy: a `restricted` guest never
        // shares an environment with a revision allowed somewhere else. An
        // empty list hashes as before, so `none` / `public-web` keys are stable.
        network_policy_version: if spec.egress_allow.is_empty() {
            digest_u64(&serde_json::to_vec(&spec.egress).unwrap_or_default())
        } else {
            digest_u64(&serde_json::to_vec(&(&spec.egress, &spec.egress_allow)).unwrap_or_default())
        },
        secret_binding_generation,
    }
}

// ---------------------------------------------------------------------------
// pool
// ---------------------------------------------------------------------------

/// The host-observed life of an environment, in milliseconds: from the moment
/// the ledger row was created to `now`, the moment the environment ended.
///
/// This is the single quantity `EnvironmentStopped` reports, on every path and
/// whoever ends the environment — the driver, the TTL sweeper, a drain, a
/// retire or the retry after a failed terminate. Once an environment can be
/// reused it outlives every single attempt on it, so no attempt's stopwatch
/// can measure it; taking it from the ledger instead makes the driver's number
/// and the pool's number the same quantity rather than two that happen to
/// share a field (docs/architecture.md §4).
pub fn environment_lifetime_ms(env: &ExecutionEnvironment, now: Timestamp) -> u64 {
    (now - env.created_at).num_milliseconds().max(0) as u64
}

/// What a warm start actually cost, measured on the host.
///
/// A warm start does not boot and does not initialize, so those two timings
/// are legitimately zero for it — but resuming the environment and checking
/// that it is fit to be dispatched into is real work, and an attempt that
/// reported nothing but zeros would make warm look free. These two numbers are
/// what let evidence compare warm against cold honestly (PLT-4633).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WarmStartTimings {
    /// `ExecutionProvider::idle_resume`, host-observed.
    pub resume_ms: u64,
    /// The readiness check that follows the resume: the stale drain and the
    /// usability check that must both pass before anything is dispatched.
    pub readiness_ms: u64,
}

/// How long the readiness probe waits for the guest's answer before the
/// environment is retired and the caller starts cold.
///
/// A resumed guest answers in microseconds — the bridge replies from its frame
/// loop without touching the user process — so this is not a budget to spend
/// but a bound on how long a *dead* environment may delay an invocation. It
/// has to be short enough that paying it is cheaper than the cold start it
/// avoids, and long enough to survive a loaded host. Getting it wrong in
/// either direction costs one cold start; getting no answer at all and
/// dispatching anyway would cost the whole execution deadline.
pub const READINESS_PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Milliseconds, rounded **up**.
///
/// Work that happened is never reported as zero: a resume that took 300 µs is
/// reported as 1 ms, not as 0. Cold timings keep truncating (`as_millis`)
/// because they are hundreds of milliseconds and a sub-millisecond boot does
/// not exist; a resume can genuinely be that fast.
fn ceil_ms(d: Duration) -> u64 {
    d.as_micros().div_ceil(1000) as u64
}

/// A pooled environment handed to an attempt: the ledger row — already `Busy`
/// at its new epoch — the live session of its guest, the usage sequence the
/// environment has reached, so the attempt continues the environment's own
/// event count instead of starting a second one
/// ([`UsageEvent::sequence`](tachyon_serverless_domain::UsageEvent)), and what
/// taking it out of the pool cost.
pub struct WarmEnvironment {
    pub environment: ExecutionEnvironment,
    pub session: BridgeSession,
    pub sequence: u64,
    pub timings: WarmStartTimings,
}

/// What the pool holds for one idle environment: the live guest connection,
/// which cannot be persisted, and the last usage sequence the environment
/// used. Both are handed back together when it is claimed.
struct PooledSession {
    session: BridgeSession,
    sequence: u64,
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

/// One environment the pool owns and is about to end. Everything the settling
/// needs travels together, so a terminate that failed can be retried later
/// exactly as it was meant the first time.
#[derive(Debug, Clone)]
struct Termination {
    id: EnvironmentId,
    /// Reason handed to the provider.
    terminate: TerminateReason,
    /// Operator-facing reason, for the log line and the guest's `Shutdown`.
    why: &'static str,
    /// `None` -> the row ends `Stopped`; `Some(reason)` -> `Failed{reason}`.
    failure: Option<&'static str>,
    /// Last usage sequence the environment used, so its stop event continues
    /// the environment's own count. Only meaningful once the session (which
    /// carries it) is gone, i.e. on a retry.
    sequence: u64,
    /// The environment is quiesced: its guest is not being scheduled, so it
    /// gets no `Shutdown` frame and the provider is told not to wait for one
    /// (PLT-4633 review F3). Every environment the pool holds is in this state
    /// — being quiesced is what being in the pool means.
    quiesced: bool,
}

pub struct EnvironmentPool {
    repos: Repositories,
    provider: Arc<dyn ExecutionProvider>,
    usage: Arc<dyn UsageSink>,
    clock: Arc<dyn Clock>,
    policy: PoolPolicy,
    /// Live guest connections of the pooled environments. Never persisted.
    sessions: Mutex<HashMap<EnvironmentId, PooledSession>>,
    /// Environments this process took out of the pool but could not terminate.
    /// Their rows stay `Draining`, so no attempt can take them, and the next
    /// sweep tries again.
    pending_termination: Mutex<Vec<Termination>>,
    /// How many environments [`EnvironmentPool::release`] has taken over whose
    /// quiesce has not finished yet.
    ///
    /// Such an environment is in neither half of the pool: its row is still
    /// `Busy` (nothing can claim it) and its session is held by the task doing
    /// the pausing. A drain therefore waits for these to land
    /// ([`EnvironmentPool::settle`]) before it sweeps, otherwise a shutdown
    /// could step over an environment that is about to become idle.
    quiescing: tokio::sync::watch::Sender<usize>,
}

impl std::fmt::Debug for EnvironmentPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvironmentPool")
            .field("policy", &self.policy)
            .field("sessions", &self.sessions.lock().len())
            .field("quiescing", &*self.quiescing.borrow())
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
            quiescing: tokio::sync::watch::Sender::new(0),
        }
    }

    pub fn policy(&self) -> &PoolPolicy {
        &self.policy
    }

    /// Environments this process currently holds open *in the pool*.
    pub fn held(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Environments handed over by [`Self::release`] that are still being
    /// quiesced, i.e. on their way into the pool or on their way to being
    /// terminated because they could not be paused.
    pub fn quiescing(&self) -> usize {
        *self.quiescing.borrow()
    }

    /// Wait until every environment [`Self::release`] took over has landed:
    /// pooled, or terminated and metered.
    ///
    /// A drain calls this first, so a shutdown cannot walk past an environment
    /// that is one instant away from being idle. Tests call it for the same
    /// reason: after it returns, what the pool holds is final.
    pub async fn settle(&self) {
        let mut rx = self.quiescing.subscribe();
        loop {
            if *rx.borrow_and_update() == 0 {
                return;
            }
            // The only sender is the pool itself, which lives as long as this.
            if rx.changed().await.is_err() {
                return;
            }
        }
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
            let pooled = self.sessions.lock().remove(&environment.id);
            match pooled {
                Some(PooledSession {
                    mut session,
                    sequence,
                }) => {
                    // The environment was quiesced on its way into the pool,
                    // so nothing can run in it until the provider says it is
                    // back. A resume that was not confirmed is never dispatched
                    // into: the environment is retired exactly like a dead
                    // guest and the caller starts a cold one instead
                    // (PLT-4633 acceptance 3).
                    let resume_started = Instant::now();
                    if let Err(e) = self.provider.idle_resume(&environment.id).await {
                        tracing::warn!(
                            error = %e,
                            environment_id = %environment.id,
                            epoch = environment.epoch,
                            "resuming a pooled environment failed; retiring it and starting cold"
                        );
                        drop(session);
                        // The resume was refused, so as far as anyone knows
                        // the environment is still paused: no shutdown frame
                        // and no waiting for a guest that cannot answer.
                        self.retire(environment, "idle resume failed", sequence, true)
                            .await;
                        continue;
                    }
                    let resume_ms = ceil_ms(resume_started.elapsed());
                    // Readiness, after the resume and before any dispatch, in
                    // two parts that answer two different questions.
                    //
                    // 1. `drain_stale` consumes what is *already* buffered:
                    //    anything the guest queued while the environment was
                    //    idle (or left over from the attempt before) belongs
                    //    to the past, not to the attempt about to be
                    //    dispatched, and an `Exited` frame among it makes the
                    //    session unusable.
                    // 2. `probe_ready` asks the guest whether it is there and
                    //    waits, briefly, for the answer. Without it the check
                    //    would only ever report what the host already knew, so
                    //    a guest that died or stopped being scheduled while it
                    //    was paused would be indistinguishable from a healthy
                    //    one — and the invocation dispatched into it would
                    //    fail on the execution deadline instead of falling
                    //    back to a cold start (PLT-4633 review F2).
                    //
                    // Both have to happen *after* the resume: a quiesced guest
                    // cannot answer for itself.
                    let readiness_started = Instant::now();
                    session.drain_stale().await;
                    let usable = session.is_usable()
                        && session
                            .probe_ready(Instant::now() + READINESS_PROBE_TIMEOUT)
                            .await;
                    let readiness_ms = ceil_ms(readiness_started.elapsed());
                    if usable {
                        tracing::debug!(
                            environment_id = %environment.id,
                            epoch = environment.epoch,
                            resume_ms,
                            readiness_ms,
                            "reusing a pooled environment"
                        );
                        return Some(WarmEnvironment {
                            environment,
                            session,
                            sequence,
                            timings: WarmStartTimings {
                                resume_ms,
                                readiness_ms,
                            },
                        });
                    }
                    drop(session);
                    // It answered the resume, so it is running again even
                    // though it is not fit to serve: this one is terminated
                    // the ordinary way.
                    self.retire(
                        environment,
                        "the pooled guest failed the readiness check",
                        sequence,
                        false,
                    )
                    .await;
                }
                // The row says pooled but there is no live guest behind it:
                // the row outlived the process that held its session. Retire
                // it and try the next candidate. No session also means no
                // sequence: this process never metered anything for it.
                // Whatever is left of it on the host is quiesced (that is what
                // being pooled means) and there is no session to say goodbye
                // through, so nothing waits for it.
                None => {
                    self.retire(environment, "pooled environment has no session", 0, true)
                        .await;
                }
            }
        }
    }

    /// Hand a finished environment over to the pool.
    ///
    /// `env` is the caller's `Busy` row for the attempt that just finished and
    /// `sequence` the last usage sequence it used for this environment, which
    /// the pool keeps so the environment's event count carries on into the
    /// next attempt and into its own stop event.
    ///
    /// **Returns immediately.** The pause itself — a hypervisor API call with
    /// its own timeout — runs in a task of the pool's, because it is work the
    /// *next* invocation benefits from and the caller of this one must not pay
    /// for it in its response time (PLT-4633 review F4). What that costs in
    /// exchange is that "pooled" is no longer true the moment this returns,
    /// which is why [`Self::settle`] exists.
    ///
    /// Correctness does not depend on the timing, because of two invariants:
    ///
    /// 1. **Nothing can claim the row until it is really paused.** The row
    ///    stays `Busy` — which [`EnvironmentRepository::claim_for_reuse`]
    ///    never hands out — until the quiesce has returned `Ok`, and only then
    ///    is it published as `Idle`, together with its session, under the
    ///    sessions lock.
    /// 2. **An environment that cannot be pooled is ended, not leaked.**
    ///    Whether the pause fails or the ledger refuses the row, this task
    ///    terminates the environment, settles it and emits its single
    ///    `EnvironmentStopped` — the work the caller used to do on the `Err`
    ///    path. `Ok(())` therefore means "the pool owns this now", not "it is
    ///    pooled".
    ///
    /// `Err` (reuse is gated off, or the session is unusable) hands the
    /// session straight back, boxed because a live session is a large value:
    /// nothing was taken over and the caller terminates the environment
    /// exactly as it did in P1.
    ///
    /// [`EnvironmentRepository::claim_for_reuse`]: crate::repository::EnvironmentRepository::claim_for_reuse
    pub fn release(
        self: Arc<Self>,
        env: &ExecutionEnvironment,
        session: BridgeSession,
        sequence: u64,
    ) -> Result<(), Box<BridgeSession>> {
        if !self.policy.reuse_enabled() || !session.is_usable() {
            return Err(Box::new(session));
        }
        self.quiescing.send_modify(|n| *n += 1);
        let env = env.clone();
        tokio::spawn(async move {
            self.quiesce_and_publish(env, session, sequence).await;
            self.quiescing.send_modify(|n| *n -= 1);
        });
        Ok(())
    }

    /// The work [`Self::release`] handed over: pause the environment, then
    /// publish it — or end it, when either step says it cannot be pooled.
    async fn quiesce_and_publish(
        &self,
        env: ExecutionEnvironment,
        session: BridgeSession,
        sequence: u64,
    ) {
        // Quiesce *before* the row is published. The moment the row is `Idle`
        // a claimer can take it, and every claimer resumes what it takes, so
        // the environment must already be quiesced by then. An environment
        // that cannot be quiesced is not pooled at all (PLT-4633 acceptance 3).
        if let Err(e) = self.provider.idle_quiesce(&env.id).await {
            tracing::warn!(
                error = %e,
                environment_id = %env.id,
                "quiescing the environment failed; terminating it instead of pooling it"
            );
            // The pause did not take, so the guest is still running and can be
            // said goodbye to: the ordinary destroy-after-invoke ending, done
            // here because the caller has already gone.
            self.end_unpooled(
                env,
                session,
                sequence,
                "idle quiesce failed",
                TerminateReason::Completed,
                false,
            )
            .await;
            return;
        }
        let now = self.clock.now();
        // `None` once the pool has taken the session; `Some` hands it back
        // because the ledger refused the row and this task has to end it.
        let refused: Option<BridgeSession> = {
            // The sessions lock is held *across* the ledger mutation: the
            // moment the row becomes `Idle` it is claimable, and a claimer
            // that finds it without its session would treat a healthy
            // environment as dead and terminate it. Holding the lock makes the
            // row and its session appear together. `claim` never holds this
            // lock while taking the ledger's, so the two orders cannot
            // deadlock. The lock is a `parking_lot` one, so the block also
            // keeps it from being held across an await.
            let mut sessions = self.sessions.lock();
            match self
                .repos
                .environments
                .release_to_pool(&env, self.policy.limits, now)
            {
                Ok(Some(pooled)) => {
                    tracing::debug!(
                        environment_id = %pooled.id,
                        epoch = pooled.epoch,
                        "environment returned to the pool"
                    );
                    sessions.insert(pooled.id.clone(), PooledSession { session, sequence });
                    None
                }
                Ok(None) => Some(session),
                Err(e) => {
                    tracing::warn!(error = %e, environment_id = %env.id, "cannot pool environment");
                    Some(session)
                }
            }
        };
        let Some(session) = refused else {
            return;
        };
        // Quiesced, and then refused by the ledger (a cap, or a row that moved
        // on). It is paused, so it is ended the way every paused environment
        // is: no `Shutdown` frame it could not read, and no grace period it
        // could not use.
        self.end_unpooled(
            env,
            session,
            sequence,
            "the pool refused the environment",
            TerminateReason::Quiesced,
            true,
        )
        .await;
    }

    /// End an environment the pool took over but did not pool, and settle it
    /// exactly like every other environment the pool ends: `Draining` first,
    /// then terminate, then one `EnvironmentStopped` when it is really gone.
    async fn end_unpooled(
        &self,
        mut env: ExecutionEnvironment,
        mut session: BridgeSession,
        sequence: u64,
        why: &'static str,
        terminate: TerminateReason,
        quiesced: bool,
    ) {
        if !quiesced {
            let _ = session.shutdown(why).await;
        }
        drop(session);
        let now = self.clock.now();
        if env.mark_draining(now).is_ok()
            && let Err(e) = self.repos.environments.update(env.clone())
        {
            tracing::warn!(error = %e, environment_id = %env.id, "cannot record a draining environment");
        }
        self.terminate_and_settle(&Termination {
            id: env.id.clone(),
            terminate,
            why,
            // It finished its attempt cleanly; it is simply not pooled.
            failure: None,
            sequence,
            quiesced,
        })
        .await;
    }

    /// Terminate every idle environment that is past its TTL.
    pub async fn sweep(&self) -> PoolSweep {
        self.reap(false).await
    }

    /// Terminate every idle environment regardless of its TTL (shutdown).
    ///
    /// Waits for the environments still being quiesced first: they are on
    /// their way into the pool, and a drain that swept before they landed
    /// would leave them running on the host with nothing left to reclaim them
    /// (PLT-4633 review F4).
    pub async fn drain(&self) -> PoolSweep {
        self.settle().await;
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
        let pending: Vec<Termination> = std::mem::take(&mut self.pending_termination.lock());
        for termination in pending {
            tracing::info!(
                environment_id = %termination.id,
                reason = termination.why,
                "retrying a failed termination"
            );
            if self.terminate_and_settle(&termination).await {
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
            // Everything in the pool is quiesced, so this is not a `Shutdown`:
            // the guest cannot read the frame and cannot power itself off, and
            // waiting for it would spend the provider's whole grace period on
            // every single sweep and drain (PLT-4633 review F3).
            let termination = Termination {
                id: env.id.clone(),
                terminate: TerminateReason::Quiesced,
                why: "idle timeout",
                failure: None,
                sequence: 0,
                quiesced: true,
            };
            if self.terminate_and_settle(&termination).await {
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
    /// Only a terminate that actually succeeded is recorded as a terminal
    /// state and metered. When it fails the host may still be running the
    /// environment, so the row stays `Draining`: out of the pool, but still in
    /// `list_active`, so the startup reconcile of the next process still sees
    /// an owner instead of an environment nobody admits to. The termination is
    /// kept, unchanged, for the next sweep — which is what keeps the
    /// environment's single `EnvironmentStopped` for the moment it is really
    /// gone.
    async fn terminate_and_settle(&self, t: &Termination) -> bool {
        debug_assert!(
            !(t.quiesced && t.terminate.waits_for_the_guest()),
            "a quiesced guest cannot use a grace period: {:?}",
            t.terminate
        );
        let pooled = self.sessions.lock().remove(&t.id);
        // The environment's event count lives with its session while it is
        // pooled; once that is gone (a retry, or a row this process never
        // held) the termination carries it.
        let sequence = pooled.as_ref().map_or(t.sequence, |p| p.sequence);
        if let Some(mut pooled) = pooled {
            // A quiesced guest is not being scheduled: nothing in it would
            // ever read the `Shutdown` frame, so it is not sent one and the
            // provider is told not to wait (`TerminateReason::Quiesced`).
            if t.quiesced {
                tracing::debug!(
                    environment_id = %t.id,
                    reason = t.why,
                    "terminating a quiesced environment without a shutdown frame"
                );
            } else {
                let _ = pooled.session.shutdown(t.why).await;
            }
        }
        if let Err(e) = self
            .provider
            .terminate_environment(&t.id, t.terminate)
            .await
        {
            tracing::warn!(
                error = %e,
                environment_id = %t.id,
                reason = t.why,
                "terminating a pooled environment failed; it stays draining for the next sweep"
            );
            self.pending_termination.lock().push(Termination {
                sequence,
                ..t.clone()
            });
            return false;
        }
        let now = self.clock.now();
        match self.repos.environments.get(&t.id) {
            Ok(Some(mut env)) => {
                let settled = match t.failure {
                    None => env.mark_stopped(now),
                    Some(reason) => env.mark_failed(reason, now),
                };
                if settled.is_ok()
                    && let Err(e) = self.repos.environments.update(env.clone())
                {
                    tracing::warn!(error = %e, environment_id = %t.id, "cannot record a reaped environment");
                }
                self.emit_stopped(&env, now, sequence).await;
            }
            Ok(None) => {
                tracing::warn!(environment_id = %t.id, "terminated an environment the ledger no longer has")
            }
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %t.id, "cannot load a reaped environment")
            }
        }
        true
    }

    /// Terminate a claimed environment the pool cannot use and settle its row.
    ///
    /// The claim already took the row out of the pool (it is `Busy` at a new
    /// epoch); this moves it to `Draining` first, so that a terminate which
    /// fails leaves exactly what the sweeper leaves: an environment nothing can
    /// claim, still in `list_active` for the startup reconcile of the next
    /// process, with the retry queued and *no* stop event — the environment is
    /// metered when it is really gone, once, whoever finally ends it.
    /// `quiesced` says whether the guest is still paused as far as anyone
    /// knows: a resume that was refused leaves it paused, a resume that
    /// succeeded does not.
    async fn retire(
        &self,
        mut env: ExecutionEnvironment,
        reason: &'static str,
        sequence: u64,
        quiesced: bool,
    ) {
        tracing::warn!(environment_id = %env.id, reason, quiesced, "retiring a pooled environment");
        let now = self.clock.now();
        if env.mark_draining(now).is_ok()
            && let Err(e) = self.repos.environments.update(env.clone())
        {
            tracing::warn!(error = %e, environment_id = %env.id, "cannot record a retiring environment");
        }
        self.terminate_and_settle(&Termination {
            id: env.id.clone(),
            terminate: match quiesced {
                true => TerminateReason::Quiesced,
                false => TerminateReason::Reconcile,
            },
            why: reason,
            failure: Some(reason),
            sequence,
            quiesced,
        })
        .await;
    }

    /// Report the end of a pooled environment's life to usage.
    ///
    /// The driver deliberately emits nothing when it hands an environment to
    /// the pool (it did not stop), so this is the only `EnvironmentStopped`
    /// such an environment ever gets: without it the whole warm part of its
    /// lifetime would never reach metering.
    ///
    /// `monotonic_duration_ms` is [`environment_lifetime_ms`], the same
    /// host-observed span the driver reports for an environment it ends
    /// itself. `sequence` continues the environment's own event count, so the
    /// stop event is the last number of its life. The id is
    /// `<environment>:<epoch>:pool-stopped`, which never collides with the
    /// driver's `<environment>:<epoch>:<sequence>` (a sequence is a number) and
    /// is stable, so a re-send of the same event still de-duplicates.
    async fn emit_stopped(&self, env: &ExecutionEnvironment, now: Timestamp, sequence: u64) {
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
                sequence: sequence + 1,
                observed_at: now,
                monotonic_duration_ms: Some(environment_lifetime_ms(env, now)),
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
    use tachyon_serverless_protocol::{
        FrameCodec, GuestMessage, HostMessage, PROTOCOL_VERSION, decode_message, encode_message,
    };
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

    /// Reuse on *and* the measurement switch: what an operator sets to take
    /// the idle measurement the capability is waiting for.
    fn measuring() -> PoolConfig {
        PoolConfig {
            enabled: true,
            allow_unverified_idle: true,
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

    /// PLT-4633 acceptance 4: an `Unverified` idle capability is gated off by
    /// default, `[pool] allow_unverified_idle` opens it for a measurement, and
    /// the result is still not a verified warm configuration.
    #[test]
    fn an_unverified_idle_capability_needs_the_switch_and_stays_unverified() {
        let unverified = || Support::unverified("not measured on real hardware");
        let both_unverified = caps(unverified(), unverified());

        let gated = PoolPolicy::decide(&both_unverified, &on());
        assert!(
            !gated.reuse_enabled(),
            "code without a measurement is not enough on its own"
        );
        assert_eq!(
            gated.disabled_reason(),
            Some(ReuseDisabled::IdleQuiesceNotSupported)
        );
        assert!(!gated.idle_verified());
        assert!(
            gated.reason().contains("allow_unverified_idle is not set"),
            "{}",
            gated.reason()
        );

        let measuring_policy = PoolPolicy::decide(&both_unverified, &measuring());
        assert!(
            measuring_policy.reuse_enabled(),
            "the switch opens the gate"
        );
        assert_eq!(measuring_policy.disabled_reason(), None);
        assert!(
            !measuring_policy.idle_verified(),
            "a measurement run must never count as a verified warm configuration"
        );
        assert!(
            measuring_policy.reason().contains("measurement run"),
            "{}",
            measuring_policy.reason()
        );

        // The switch accepts `Unverified` only. `Unsupported` stays closed,
        // and the reason does not send an operator to a switch that cannot
        // help them.
        let unsupported = PoolPolicy::decide(
            &caps(
                Support::unsupported("destroy-after-invoke"),
                Support::Supported,
            ),
            &measuring(),
        );
        assert!(!unsupported.reuse_enabled());
        assert!(
            !unsupported.reason().contains("allow_unverified_idle"),
            "{}",
            unsupported.reason()
        );

        // A measured provider is verified, switch or no switch.
        let verified =
            PoolPolicy::decide(&caps(Support::Supported, Support::Supported), &measuring());
        assert!(verified.reuse_enabled() && verified.idle_verified());
    }

    /// The reason and the two capability statuses are what `GET /v1/provider`
    /// shows, on the enabled and the disabled path alike.
    #[test]
    fn the_policy_reports_whether_reuse_is_on_and_whether_it_was_measured() {
        let supported = caps(Support::Supported, Support::Supported);
        let info = PoolPolicy::decide(&supported, &on()).info(&supported);
        assert!(info.enabled && info.verified);
        assert_eq!(info.idle_quiesce, "supported");
        assert_eq!(info.idle_resume, "supported");
        assert!(info.reason.contains("supported"), "{}", info.reason);

        let unverified = caps(
            Support::unverified("not measured"),
            Support::unverified("not measured"),
        );
        let info = PoolPolicy::decide(&unverified, &measuring()).info(&unverified);
        assert!(info.enabled, "reuse really is running");
        assert!(!info.verified, "and it is never shown as a warm success");
        assert_eq!(info.idle_quiesce, "unverified");
        assert_eq!(info.idle_resume, "unverified");

        // Reuse switched off on a provider whose idle support *was* measured.
        // The two answers are independent (review F7): the gateway does not
        // reuse anything, and that says nothing about whether the provider's
        // pause/resume was ever measured — which it was.
        let off = PoolPolicy::decide(&supported, &PoolConfig::default()).info(&supported);
        assert!(!off.enabled);
        assert!(
            off.verified,
            "`verified` reports the provider, not the [pool] switch"
        );
        assert_eq!(off.reason, "[pool] enabled is false");
        assert_eq!(PoolPolicy::off().reason(), "[pool] enabled is false");
        assert!(
            !PoolPolicy::off().idle_verified(),
            "a policy built without capabilities claims no measurement"
        );
    }

    /// Regression (review F7): `verified` is documented as "the provider's
    /// idle support was measured" and must be computed that way. Folding the
    /// configuration gate into it made a measured provider report
    /// `verified = false` whenever reuse happened to be off, which reads as
    /// "nobody measured this".
    #[test]
    fn verified_is_a_capability_fact_not_a_gate_fact() {
        let measured = caps(Support::Supported, Support::Supported);
        let unmeasured = caps(
            Support::unverified("not measured on real hardware"),
            Support::unverified("not measured on real hardware"),
        );

        for cfg in [PoolConfig::default(), on(), measuring()] {
            let policy = PoolPolicy::decide(&measured, &cfg);
            assert!(
                policy.idle_verified(),
                "the provider measured it, whatever [pool] says: {cfg:?}"
            );
        }
        for cfg in [PoolConfig::default(), on(), measuring()] {
            let policy = PoolPolicy::decide(&unmeasured, &cfg);
            assert!(
                !policy.idle_verified(),
                "nobody measured it, whatever [pool] says: {cfg:?}"
            );
        }

        // The pairing that must never be read as a warm success is still
        // exactly that pairing, and it is the only one that warns.
        let measuring_policy = PoolPolicy::decide(&unmeasured, &measuring());
        assert!(measuring_policy.reuse_enabled() && !measuring_policy.idle_verified());
    }

    #[test]
    fn pool_policy_carries_the_configured_caps_and_ttl() {
        let cfg = PoolConfig {
            enabled: true,
            max_idle_per_revision: 3,
            idle_ttl_seconds: 45,
            max_total_idle: 9,
            ..PoolConfig::default()
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
        terminated: Mutex<Vec<(EnvironmentId, TerminateReason)>>,
        fail_terminate: AtomicBool,
        quiesced: Mutex<Vec<EnvironmentId>>,
        resumed: Mutex<Vec<EnvironmentId>>,
        fail_quiesce: AtomicBool,
        fail_resume: AtomicBool,
        /// Milliseconds `idle_quiesce` takes, so a test can see whether
        /// anything waits for it.
        quiesce_delay_ms: std::sync::atomic::AtomicU64,
    }

    impl StubProvider {
        fn new() -> Self {
            Self {
                terminated: Mutex::new(Vec::new()),
                fail_terminate: AtomicBool::new(false),
                quiesced: Mutex::new(Vec::new()),
                resumed: Mutex::new(Vec::new()),
                fail_quiesce: AtomicBool::new(false),
                fail_resume: AtomicBool::new(false),
                quiesce_delay_ms: std::sync::atomic::AtomicU64::new(0),
            }
        }
        fn slow_quiesce(&self, d: Duration) {
            self.quiesce_delay_ms
                .store(d.as_millis() as u64, Ordering::SeqCst);
        }
        fn fail_terminate(&self, fail: bool) {
            self.fail_terminate.store(fail, Ordering::SeqCst);
        }
        fn fail_quiesce(&self, fail: bool) {
            self.fail_quiesce.store(fail, Ordering::SeqCst);
        }
        fn fail_resume(&self, fail: bool) {
            self.fail_resume.store(fail, Ordering::SeqCst);
        }
        fn terminated(&self) -> Vec<EnvironmentId> {
            self.terminated
                .lock()
                .iter()
                .map(|(id, _)| id.clone())
                .collect()
        }
        /// Every terminate with the reason it was given, so a test can assert
        /// that a quiesced environment is not terminated as if its guest could
        /// still act on a shutdown (review F3).
        fn terminations(&self) -> Vec<(EnvironmentId, TerminateReason)> {
            self.terminated.lock().clone()
        }
        fn quiesced(&self) -> Vec<EnvironmentId> {
            self.quiesced.lock().clone()
        }
        fn resumed(&self) -> Vec<EnvironmentId> {
            self.resumed.lock().clone()
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
            reason: TerminateReason,
        ) -> Result<TerminateReport, ProviderError> {
            self.terminated.lock().push((id.clone(), reason));
            if self.fail_terminate.load(Ordering::SeqCst) {
                return Err(ProviderError::Internal("terminate failed".into()));
            }
            Ok(TerminateReport {
                was_running: true,
                cleaned: vec![id.to_string()],
            })
        }
        async fn idle_quiesce(&self, id: &EnvironmentId) -> Result<(), ProviderError> {
            self.quiesced.lock().push(id.clone());
            let delay = self.quiesce_delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            match self.fail_quiesce.load(Ordering::SeqCst) {
                true => Err(ProviderError::Internal("quiesce failed".into())),
                false => Ok(()),
            }
        }
        async fn idle_resume(&self, id: &EnvironmentId) -> Result<(), ProviderError> {
            self.resumed.lock().push(id.clone());
            match self.fail_resume.load(Ordering::SeqCst) {
                true => Err(ProviderError::Internal("resume failed".into())),
                false => Ok(()),
            }
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
                // The capabilities above are `Supported`, so the measurement
                // switch is not what opens this gate.
                allow_unverified_idle: false,
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
        session_with(store, env_id, epoch, true).await.0
    }

    /// The same, with the two things a readiness test needs: whether the guest
    /// answers the probe at all, and a record of every host frame it saw (so a
    /// test can assert what was *not* sent to it).
    async fn session_with(
        store: &Arc<InMemoryStore>,
        env_id: &EnvironmentId,
        epoch: u64,
        answers_probe: bool,
    ) -> (BridgeSession, Arc<Mutex<Vec<HostMessage>>>) {
        let seen: Arc<Mutex<Vec<HostMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = seen.clone();
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
            // A live guest answers the readiness probe, like the real bridge:
            // without that the pool would never hand this session out.
            while let Some(Ok(frame)) = reader.next().await {
                let Ok(msg) = decode_message::<HostMessage>(&frame) else {
                    continue;
                };
                recorder.lock().push(msg.clone());
                if let HostMessage::Ping { nonce } = msg
                    && answers_probe
                {
                    let pong = encode_message(&GuestMessage::Pong { nonce }).unwrap();
                    if writer.send(pong).await.is_err() {
                        return;
                    }
                }
            }
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
        (session, seen)
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

        pool.clone()
            .release(&env, session, 3)
            .expect("the pool takes the environment over");
        pool.settle().await;
        assert_eq!(
            provider.quiesced(),
            vec![env.id.clone()],
            "it was quiesced before the row was published"
        );
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

    /// PLT-4633 acceptance 1 and 3: an environment is quiesced on its way into
    /// the pool and resumed on its way out, and a resume that fails is never
    /// dispatched into. The environment is retired exactly like a dead guest —
    /// terminated once, settled, metered once — and the caller gets `None`,
    /// which is the cold start.
    #[tokio::test]
    async fn a_failed_resume_retires_the_environment_and_hands_out_nothing() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let session = live_session(&store, &env.id, env.epoch).await;
        let provider = Arc::new(StubProvider::new());
        let sink = Arc::new(RecordingSink::default());
        let pool = Arc::new(EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            sink.clone(),
            Arc::new(SystemClock),
            policy_on(),
        ));

        pool.clone()
            .release(&env, session, 2)
            .expect("the pool takes the environment over");
        pool.settle().await;
        assert_eq!(
            state_of(&store, &env.id),
            EnvironmentState::Idle,
            "published once it really was paused"
        );
        assert_eq!(
            provider.quiesced(),
            vec![env.id.clone()],
            "quiesced on the way into the pool"
        );

        provider.fail_resume(true);
        assert!(
            pool.claim(&key).await.is_none(),
            "an environment whose resume was not confirmed is never handed out"
        );
        assert_eq!(provider.resumed(), vec![env.id.clone()]);
        assert_eq!(
            provider.terminated(),
            vec![env.id.clone()],
            "retired exactly once"
        );
        assert!(
            matches!(state_of(&store, &env.id), EnvironmentState::Failed { .. }),
            "{:?}",
            state_of(&store, &env.id)
        );
        assert_eq!(pool.held(), 0);
        let events = sink.events();
        assert_eq!(events.len(), 1, "metered once, when it was really gone");
        assert_eq!(events[0].event_type, UsageEventType::EnvironmentStopped);
        assert_eq!(
            events[0].sequence, 3,
            "the stop event continues the environment's own count"
        );
    }

    /// The other half of acceptance 3, and half of review F4: an environment
    /// that cannot be quiesced never enters the pool.
    ///
    /// The caller has already returned by then (the quiesce is off its
    /// response path), so the pool is what terminates and meters it. What must
    /// not happen is the environment being pooled, or being left running with
    /// nobody owning it.
    #[tokio::test]
    async fn a_failed_quiesce_terminates_the_environment_instead_of_pooling_it() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let session = live_session(&store, &env.id, env.epoch).await;
        let provider = Arc::new(StubProvider::new());
        provider.fail_quiesce(true);
        let sink = Arc::new(RecordingSink::default());
        let pool = Arc::new(EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            sink.clone(),
            Arc::new(SystemClock),
            policy_on(),
        ));

        pool.clone()
            .release(&env, session, 2)
            .expect("the pool takes the environment over before it knows it cannot pause it");
        pool.settle().await;

        assert_eq!(provider.quiesced(), vec![env.id.clone()]);
        assert!(
            provider.resumed().is_empty(),
            "nothing was paused, so nothing is resumed"
        );
        assert!(
            store.list_idle().unwrap().is_empty(),
            "the row never became claimable"
        );
        assert_eq!(pool.held(), 0);
        assert_eq!(pool.quiescing(), 0);
        // It really is gone, and the guest was still running, so it was ended
        // the ordinary way rather than as a quiesced environment.
        assert_eq!(
            provider.terminations(),
            vec![(env.id.clone(), TerminateReason::Completed)]
        );
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Stopped);
        let events = sink.events();
        assert_eq!(events.len(), 1, "metered exactly once, by the pool");
        assert_eq!(events[0].event_type, UsageEventType::EnvironmentStopped);
        assert_eq!(
            events[0].sequence, 3,
            "the stop event continues the environment's own count"
        );
        assert!(pool.claim(&key).await.is_none());
    }

    /// Regression (review F2): the readiness check asks the guest something.
    ///
    /// This guest is connected, has nothing queued and never reported
    /// anything, so everything the host already knew says it is healthy — and
    /// it no longer answers. Without a probe it would be handed out and the
    /// invocation would sit in it until the execution deadline; with one it is
    /// retired and the caller starts cold.
    #[tokio::test]
    async fn a_pooled_guest_that_stops_answering_is_retired_instead_of_handed_out() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let (session, seen) = session_with(&store, &env.id, env.epoch, false).await;
        let provider = Arc::new(StubProvider::new());
        let sink = Arc::new(RecordingSink::default());
        let pool = Arc::new(EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            sink.clone(),
            Arc::new(SystemClock),
            policy_on(),
        ));

        pool.clone()
            .release(&env, session, 4)
            .expect("the pool takes it over");
        pool.settle().await;
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Idle);

        let started = Instant::now();
        assert!(
            pool.claim(&key).await.is_none(),
            "a guest that does not answer is never dispatched into"
        );
        assert!(
            started.elapsed() < READINESS_PROBE_TIMEOUT * 4,
            "the probe is bounded; waiting here is what the bound is for"
        );
        assert_eq!(provider.resumed(), vec![env.id.clone()], "it was resumed");
        assert!(
            seen.lock()
                .iter()
                .any(|m| matches!(m, HostMessage::Ping { .. })),
            "the guest was actually asked"
        );
        assert_eq!(
            provider.terminations(),
            vec![(env.id.clone(), TerminateReason::Reconcile)],
            "it answered the resume, so it is ended the ordinary way"
        );
        assert!(
            matches!(state_of(&store, &env.id), EnvironmentState::Failed { .. }),
            "{:?}",
            state_of(&store, &env.id)
        );
        assert_eq!(pool.held(), 0);
        let events = sink.events();
        assert_eq!(events.len(), 1, "metered once, when it was really gone");
        assert_eq!(events[0].sequence, 5);
    }

    /// Regression (review F3): everything the pool terminates was quiesced, so
    /// its guest cannot read a `Shutdown` frame or power itself off. It is not
    /// sent one, and the provider is told not to wait for one — otherwise
    /// every sweep and every drain stalls for the whole grace period.
    #[tokio::test]
    async fn a_paused_environment_is_reaped_without_waiting_for_its_guest() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let (session, seen) = session_with(&store, &env.id, env.epoch, true).await;
        let provider = Arc::new(StubProvider::new());
        let pool = Arc::new(EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            Arc::new(RecordingSink::default()),
            Arc::new(SystemClock),
            policy_on(),
        ));
        pool.clone()
            .release(&env, session, 0)
            .expect("the pool takes it over");
        pool.settle().await;
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Idle);

        // The TTL of `policy_on` is zero, so this environment is expired.
        let swept = pool.sweep().await;
        assert_eq!((swept.examined, swept.reaped, swept.failed), (1, 1, 0));
        assert_eq!(
            provider.terminations(),
            vec![(env.id.clone(), TerminateReason::Quiesced)],
            "the reason has to tell the provider not to wait for the guest"
        );
        assert!(
            !TerminateReason::Quiesced.waits_for_the_guest(),
            "which is what that reason means"
        );
        assert!(
            !seen
                .lock()
                .iter()
                .any(|m| matches!(m, HostMessage::Shutdown { .. })),
            "a paused guest is not sent a frame it could never read: {:?}",
            seen.lock()
        );
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Stopped);

        // A drain of a second environment behaves the same way.
        let env2 = busy_row(&store, &key);
        let (session2, seen2) = session_with(&store, &env2.id, env2.epoch, true).await;
        pool.clone()
            .release(&env2, session2, 0)
            .expect("taken over");
        pool.settle().await;
        let drained = pool.drain().await;
        assert_eq!(drained.reaped, 1);
        assert!(
            provider
                .terminations()
                .iter()
                .all(|(_, r)| *r == TerminateReason::Quiesced)
        );
        assert!(
            !seen2
                .lock()
                .iter()
                .any(|m| matches!(m, HostMessage::Shutdown { .. }))
        );
    }

    /// Regression (review F4): the pause is not on the caller's response path,
    /// and taking it off that path does not make the row claimable early or
    /// leave the environment behind.
    #[tokio::test]
    async fn the_quiesce_runs_after_the_caller_is_gone_without_publishing_early() {
        const PAUSE: Duration = Duration::from_millis(300);
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let session = live_session(&store, &env.id, env.epoch).await;
        let provider = Arc::new(StubProvider::new());
        provider.slow_quiesce(PAUSE);
        let pool = Arc::new(EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            Arc::new(RecordingSink::default()),
            Arc::new(SystemClock),
            policy_on(),
        ));

        let started = Instant::now();
        pool.clone()
            .release(&env, session, 1)
            .expect("the pool takes it over");
        let handed_over_in = started.elapsed();
        assert!(
            handed_over_in < PAUSE / 3,
            "the caller waited {handed_over_in:?} for a {PAUSE:?} pause"
        );

        // While it is being paused it belongs to nobody else: the row is still
        // `Busy`, so no claim can take it and no sweep can see it.
        assert_eq!(pool.quiescing(), 1);
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Busy);
        assert!(
            pool.claim(&key).await.is_none(),
            "an environment that is not paused yet is not claimable"
        );
        assert_eq!(pool.sweep().await.examined, 0);

        // And it does land, as `Idle`, once the pause really finished.
        pool.settle().await;
        assert!(started.elapsed() >= PAUSE);
        assert_eq!(pool.quiescing(), 0);
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Idle);
        assert_eq!(pool.held(), 1);
        assert!(pool.claim(&key).await.is_some());
    }

    /// The other half of that invariant: a shutdown cannot walk past an
    /// environment that is still being paused, or it would be left running on
    /// the host with nothing left to reclaim it.
    #[tokio::test]
    async fn a_drain_waits_for_an_environment_that_is_still_being_quiesced() {
        let store = Arc::new(InMemoryStore::new(Limits::default()));
        let key = reuse_key();
        let env = busy_row(&store, &key);
        let session = live_session(&store, &env.id, env.epoch).await;
        let provider = Arc::new(StubProvider::new());
        provider.slow_quiesce(Duration::from_millis(200));
        let pool = Arc::new(EnvironmentPool::new(
            Repositories::in_memory(store.clone()),
            provider.clone(),
            Arc::new(RecordingSink::default()),
            Arc::new(SystemClock),
            policy_on(),
        ));

        pool.clone()
            .release(&env, session, 1)
            .expect("the pool takes it over");
        let drained = pool.drain().await;
        assert_eq!(
            drained.reaped, 1,
            "the drain waited for the pause and then reaped it"
        );
        assert_eq!(
            provider.terminations(),
            vec![(env.id.clone(), TerminateReason::Quiesced)]
        );
        assert_eq!(state_of(&store, &env.id), EnvironmentState::Stopped);
        assert_eq!(pool.held(), 0);
        assert_eq!(pool.quiescing(), 0);
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
        assert_eq!(
            events[0].sequence, 1,
            "the stop event continues the environment's own count"
        );
    }

    /// Regression (review R2): retiring a claimed environment the pool cannot
    /// use goes through the same settling as the sweeper. A terminate that
    /// failed is not a stop: the row stays `Draining` (out of the pool, still
    /// in `list_active`), the retry is queued, and the environment is metered
    /// once — when it is really gone.
    #[tokio::test]
    async fn a_failed_terminate_while_retiring_is_retried_and_metered_once() {
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

        // The row is pooled but this process holds no session behind it, so the
        // claim has to retire it — and the host refuses to let go.
        provider.fail_terminate(true);
        assert!(
            pool.claim(&key).await.is_none(),
            "a row without a session is never handed out"
        );
        assert_eq!(
            state_of(&store, &id),
            EnvironmentState::Draining,
            "a failed terminate must not put the row in a terminal state"
        );
        assert!(
            EnvironmentRepository::list_active(&*store)
                .unwrap()
                .iter()
                .any(|e| e.id == id),
            "the row stays in the active set, so the startup reconcile still sees an owner"
        );
        assert!(
            store.list_idle().unwrap().is_empty(),
            "and it is out of the pool either way"
        );
        assert!(
            sink.events().is_empty(),
            "nothing stopped, so nothing is metered"
        );

        // The next sweep retries it, and now the host lets go.
        provider.fail_terminate(false);
        let swept = pool.sweep().await;
        assert_eq!((swept.examined, swept.reaped, swept.failed), (0, 1, 0));
        assert!(
            matches!(state_of(&store, &id), EnvironmentState::Failed { .. }),
            "{:?}",
            state_of(&store, &id)
        );
        assert_eq!(provider.terminated().len(), 2, "it was retried once");

        let events = sink.events();
        assert_eq!(
            events.len(),
            1,
            "exactly one stop event over the environment's whole life"
        );
        assert_eq!(events[0].event_type, UsageEventType::EnvironmentStopped);
        assert_eq!(events[0].environment_id, id);
        assert!(events[0].monotonic_duration_ms.is_some());
    }
}

//! ExecutionProvider: create / observe / terminate execution environments.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use tachyon_serverless_domain::{
    Architecture, BootEvidence, EgressAllowRule, EgressProfile, EnvironmentId, ProviderKind,
    ResourceProfile, RevisionId, Sha256Digest, TenantId,
};

/// Whether a capability is available. `Unverified` means the provider has
/// code for it but it has not been measured on real hardware; it must not be
/// advertised as supported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Support {
    Supported,
    Unsupported { reason: String },
    Unverified { note: String },
}

impl Support {
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }
    pub fn unverified(note: impl Into<String>) -> Self {
        Self::Unverified { note: note.into() }
    }
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }
    /// True while a capability has code behind it that nobody has measured.
    /// Callers that gate on a capability must treat this as "no" unless an
    /// operator has explicitly opted into a measurement run
    /// (docs/architecture.md §4).
    pub fn is_unverified(&self) -> bool {
        matches!(self, Self::Unverified { .. })
    }
    /// Stable machine-readable status name, the same value `serde` writes for
    /// the tag: `supported` | `unsupported` | `unverified`.
    pub fn status_str(&self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Unsupported { .. } => "unsupported",
            Self::Unverified { .. } => "unverified",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Hardware-virtualized microVM (Firecracker, Cloud Hypervisor, Kata).
    MicroVm,
    /// OS container (namespaces / cgroups) without a VM boundary.
    Container,
    /// Plain host process. No isolation.
    Process,
}

/// Capability table (RFC §6.3). Every field is explicit so that a demo can
/// print exactly what was and was not verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub isolation: IsolationLevel,
    pub create_terminate: Support,
    pub observe: Support,
    pub enforce_deadline: Support,
    pub enforce_resource_limits: Support,
    pub egress_none: Support,
    pub egress_restricted: Support,
    pub egress_public_web: Support,
    pub host_metering: Support,
    pub idle_quiesce: Support,
    pub idle_resume: Support,
    pub snapshot_create: Support,
    pub snapshot_clone: Support,
    /// True for providers that must never be selected outside development.
    pub dev_only: bool,
}

/// Executable artifact as materialised on the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactLocation {
    /// Host path of the executable (static Linux binary for microVM providers;
    /// host-native binary for the process provider).
    pub path: PathBuf,
    pub digest: Sha256Digest,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct EnvironmentSpec {
    pub environment_id: EnvironmentId,
    pub tenant_id: TenantId,
    pub revision_id: RevisionId,
    pub artifact: ArtifactLocation,
    pub architecture: Architecture,
    pub resources: ResourceProfile,
    pub egress: EgressProfile,
    /// Allowlist of a `restricted` revision (empty for the other profiles).
    /// Providers that enforce `restricted` must deny everything not listed.
    pub egress_allow: Vec<EgressAllowRule>,
    /// Time allowed for the guest bridge to connect after creation starts.
    pub connect_timeout: Duration,
}

/// Duplex byte stream to the guest bridge.
pub trait BridgeStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> BridgeStream for T {}

/// A live environment. Dropping the handle does **not** terminate the
/// environment; callers must call [`ExecutionProvider::terminate_environment`].
pub struct EnvironmentHandle {
    pub environment_id: EnvironmentId,
    pub evidence: BootEvidence,
    /// Connected stream to the guest bridge (already accepted).
    pub stream: Box<dyn BridgeStream>,
    /// Host instant at which creation started, for boot timing.
    pub created_at: Instant,
    /// Host instant at which the bridge connection was accepted.
    pub connected_at: Instant,
}

impl std::fmt::Debug for EnvironmentHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvironmentHandle")
            .field("environment_id", &self.environment_id)
            .field("evidence", &self.evidence)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminateReason {
    Completed,
    Timeout,
    Cancelled,
    InitFailed,
    Crashed,
    Shutdown,
    /// The environment was quiesced ([`ExecutionProvider::idle_quiesce`]) when
    /// it was terminated: it sat in the idle pool and its guest is not being
    /// scheduled.
    ///
    /// Such a guest cannot read a `Shutdown` frame and cannot power itself
    /// off, so the host does not send it one and implementations must **not**
    /// wait for a self-initiated exit: waiting only spends the whole grace
    /// period on a guest that could never use it (PLT-4633 review F3).
    Quiesced,
    Reconcile,
}

impl TerminateReason {
    /// Whether the guest may still shut itself down, so a provider should wait
    /// for it before killing the process.
    ///
    /// Exactly two reasons qualify: the host sent a `Shutdown` frame to a
    /// *running* guest and gave it a chance to act on it. Everything else —
    /// a timeout, a crash, a reconcile, and a [`TerminateReason::Quiesced`]
    /// environment whose vCPUs are stopped — is killed straight away.
    pub fn waits_for_the_guest(&self) -> bool {
        matches!(self, Self::Completed | Self::Shutdown)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TerminateReport {
    /// False when the environment was already gone (idempotent no-op).
    pub was_running: bool,
    /// Host resources removed (paths, devices), for orphan audits.
    pub cleaned: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum EnvironmentObservation {
    Running {
        host_pid: Option<u32>,
    },
    Exited {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightCheck {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightReport {
    pub provider: String,
    pub ok: bool,
    pub checks: Vec<PreflightCheck>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// Provider cannot run on this host (e.g. no `/dev/kvm`).
    #[error("provider unavailable: {0}")]
    Unavailable(String),
    #[error("invalid environment spec: {0}")]
    InvalidSpec(String),
    #[error("artifact rejected: {0}")]
    ArtifactRejected(String),
    #[error("boot failed: {0}")]
    Boot(String),
    #[error("timeout during {stage}")]
    Timeout { stage: &'static str },
    #[error("environment not found: {0}")]
    NotFound(EnvironmentId),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("internal: {0}")]
    Internal(String),
}

/// The single abstraction over hypervisors / sandboxes. Domain and
/// application code never see Firecracker, Kata or Kubernetes types.
#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    fn kind(&self) -> ProviderKind;

    fn capabilities(&self) -> Capabilities;

    /// Check host prerequisites without creating anything.
    async fn preflight(&self) -> Result<PreflightReport, ProviderError>;

    /// Validate that an artifact can run under this provider (architecture,
    /// format). Called when a revision is published.
    async fn validate_artifact(
        &self,
        artifact: &ArtifactLocation,
        architecture: Architecture,
    ) -> Result<(), ProviderError>;

    /// Create an environment and wait until the guest bridge connects.
    /// Re-sending the same `environment_id` must not create a second
    /// environment: implementations return `InvalidSpec` if it already exists.
    async fn create_environment(
        &self,
        spec: EnvironmentSpec,
    ) -> Result<EnvironmentHandle, ProviderError>;

    /// Stop and clean up. Idempotent: a second call reports `was_running=false`.
    async fn terminate_environment(
        &self,
        environment_id: &EnvironmentId,
        reason: TerminateReason,
    ) -> Result<TerminateReport, ProviderError>;

    /// Quiesce an environment that is entering the idle pool: stop it
    /// consuming CPU while it waits for the next invocation, without losing
    /// the guest's state or the open bridge connection.
    ///
    /// Called exactly once when an environment enters the pool, and paired
    /// with exactly one [`ExecutionProvider::idle_resume`] when it is claimed
    /// again (docs/architecture.md §4). Implementations must be idempotent: an
    /// environment that is already quiesced is `Ok`, because that is the state
    /// the caller asked for.
    ///
    /// `Err` means the environment is **not** quiesced. The pool then does not
    /// pool it at all and falls back to terminating it, so a failure here can
    /// never leave a half-paused environment where an invocation could land.
    ///
    /// The default implementation reports [`ProviderError::Unavailable`], so a
    /// provider without idle support compiles unchanged — and is gated off
    /// anyway, because it cannot honestly report the capability.
    ///
    /// **Capability rule.** A provider may report
    /// [`Capabilities::idle_quiesce`] as [`Support::Supported`] only when this
    /// method is implemented *and* the pause/resume cycle has been measured on
    /// real hardware with the result recorded under `docs/evidence/`. Code
    /// without a measurement is [`Support::Unverified`], which the pool
    /// refuses unless an operator opts into a measurement run
    /// (`[pool] allow_unverified_idle`).
    async fn idle_quiesce(&self, environment_id: &EnvironmentId) -> Result<(), ProviderError> {
        Err(ProviderError::Unavailable(format!(
            "provider `{}` does not implement idle_quiesce (environment {environment_id})",
            self.kind().as_str()
        )))
    }

    /// Resume a quiesced environment so the next invocation can be dispatched
    /// into it. Idempotent, like [`ExecutionProvider::idle_quiesce`].
    ///
    /// `Err` means the environment is **not** running again. The caller must
    /// never dispatch into an environment whose resume was not confirmed: the
    /// pool retires it and starts a cold one instead.
    ///
    /// The same capability rule applies to [`Capabilities::idle_resume`]:
    /// `Supported` requires an implementation *and* a measurement.
    async fn idle_resume(&self, environment_id: &EnvironmentId) -> Result<(), ProviderError> {
        Err(ProviderError::Unavailable(format!(
            "provider `{}` does not implement idle_resume (environment {environment_id})",
            self.kind().as_str()
        )))
    }

    async fn observe_environment(
        &self,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentObservation, ProviderError>;

    /// Environments this provider still tracks; used for orphan reconciliation.
    async fn list_environments(&self) -> Result<Vec<EnvironmentId>, ProviderError>;
}

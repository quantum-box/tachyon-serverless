//! FunctionRevision: an immutable, published unit of code + configuration.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::DomainError;
use crate::ids::{FunctionId, RevisionId, Sha256Digest, TenantId};
use crate::limits::Limits;

/// Runtime protocol version spoken between host and guest bridge. Bumped on
/// incompatible protocol changes; the runtime bridge rejects mismatches.
pub const RUNTIME_PROTOCOL_V1: &str = "tachyon-invoke-v1";

/// CPU architecture of the artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Architecture {
    X86_64,
    Aarch64,
}

impl Architecture {
    /// Architecture of the host running this code.
    pub fn host() -> Option<Self> {
        if cfg!(target_arch = "x86_64") {
            Some(Self::X86_64)
        } else if cfg!(target_arch = "aarch64") {
            Some(Self::Aarch64)
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
            Self::Aarch64 => "aarch64",
        }
    }
}

/// Where the executable comes from. The prototype supports uploaded static
/// Linux binaries (stored by digest) and records OCI references for later
/// providers. OCI images are accepted syntactically but not executable by the
/// prototype providers; the revision fails validation with an explicit reason.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactRef {
    /// A single executable stored in the artifact store, addressed by digest.
    Binary {
        digest: Sha256Digest,
        /// Size in bytes as measured at upload.
        size_bytes: u64,
    },
    /// An OCI image reference pinned to a digest (`registry/repo@sha256:...`).
    OciImage {
        reference: String,
        digest: Sha256Digest,
    },
}

impl ArtifactRef {
    pub fn digest(&self) -> &Sha256Digest {
        match self {
            Self::Binary { digest, .. } | Self::OciImage { digest, .. } => digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeSpec {
    /// Must equal [`RUNTIME_PROTOCOL_V1`] for now.
    pub protocol: String,
    pub architecture: Architecture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceProfile {
    pub memory_mib: u32,
    pub cpu_millis: u32,
    pub ephemeral_storage_mib: u32,
}

impl Default for ResourceProfile {
    fn default() -> Self {
        Self {
            memory_mib: 256,
            cpu_millis: 500,
            ephemeral_storage_mib: 256,
        }
    }
}

impl ResourceProfile {
    /// Number of vCPUs to allocate for this profile (rounded up, at least 1).
    pub fn vcpus(&self) -> u32 {
        self.cpu_millis.div_ceil(1000).max(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    /// Handler execution timeout (from dispatch to result), seconds.
    pub timeout_seconds: u32,
    /// Time allowed from environment creation until the guest reports Ready.
    pub initialization_timeout_seconds: u32,
    /// Fixed at 1 for Functions in the prototype.
    pub concurrency_per_environment: u32,
    /// Upper bound of simultaneously running environments for this revision.
    pub max_concurrency: u32,
    /// Number of environments kept ready. Always 0 in the prototype (destroy-after-invoke).
    pub min_ready: u32,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            initialization_timeout_seconds: 30,
            concurrency_per_environment: 1,
            max_concurrency: 4,
            min_ready: 0,
        }
    }
}

/// Egress profile. The prototype providers only implement `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum EgressProfile {
    #[default]
    None,
    Restricted,
    PublicWeb,
}

/// Reference to a secret binding. Values never appear in the revision.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecretBinding {
    /// Environment variable name exposed to the guest.
    pub env_name: String,
    /// Opaque binding reference resolved by the secret provider at environment creation.
    pub binding_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RevisionStatus {
    Pending,
    Preparing,
    Validating,
    Ready,
    Failed { reason: String },
}

impl RevisionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Failed { .. })
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Preparing => "preparing",
            Self::Validating => "validating",
            Self::Ready => "ready",
            Self::Failed { .. } => "failed",
        }
    }
}

/// Revision specification as supplied at creation. Validated into a
/// [`FunctionRevision`]; immutable afterwards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionSpec {
    pub artifact: ArtifactRef,
    pub runtime: RuntimeSpec,
    pub resources: ResourceProfile,
    pub execution: ExecutionPolicy,
    pub egress: EgressProfile,
    /// Non-secret environment variables.
    pub env_vars: Vec<(String, String)>,
    pub secrets: Vec<SecretBinding>,
    /// Optional human description of the revision (e.g. git sha).
    pub description: String,
}

impl RevisionSpec {
    pub fn validate(&self, limits: &Limits) -> Result<(), DomainError> {
        if self.runtime.protocol != RUNTIME_PROTOCOL_V1 {
            return Err(DomainError::validation(
                "runtime.protocol",
                format!("unsupported protocol `{}`", self.runtime.protocol),
            ));
        }
        let r = &self.resources;
        if r.memory_mib < limits.min_memory_mib || r.memory_mib > limits.max_memory_mib {
            return Err(DomainError::validation(
                "resources.memory_mib",
                format!(
                    "must be within {}..={}",
                    limits.min_memory_mib, limits.max_memory_mib
                ),
            ));
        }
        if r.cpu_millis < limits.min_cpu_millis || r.cpu_millis > limits.max_cpu_millis {
            return Err(DomainError::validation(
                "resources.cpu_millis",
                format!(
                    "must be within {}..={}",
                    limits.min_cpu_millis, limits.max_cpu_millis
                ),
            ));
        }
        if r.ephemeral_storage_mib < limits.min_ephemeral_storage_mib
            || r.ephemeral_storage_mib > limits.max_ephemeral_storage_mib
        {
            return Err(DomainError::validation(
                "resources.ephemeral_storage_mib",
                format!(
                    "must be within {}..={}",
                    limits.min_ephemeral_storage_mib, limits.max_ephemeral_storage_mib
                ),
            ));
        }
        let e = &self.execution;
        if e.timeout_seconds == 0 || e.timeout_seconds > limits.max_execution_timeout_seconds {
            return Err(DomainError::validation(
                "execution.timeout_seconds",
                format!(
                    "must be within 1..={}",
                    limits.max_execution_timeout_seconds
                ),
            ));
        }
        if e.initialization_timeout_seconds == 0
            || e.initialization_timeout_seconds > limits.max_init_timeout_seconds
        {
            return Err(DomainError::validation(
                "execution.initialization_timeout_seconds",
                format!("must be within 1..={}", limits.max_init_timeout_seconds),
            ));
        }
        if e.concurrency_per_environment != 1 {
            return Err(DomainError::validation(
                "execution.concurrency_per_environment",
                "must be 1 (one invocation per environment)",
            ));
        }
        if e.max_concurrency == 0 || e.max_concurrency > 1000 {
            return Err(DomainError::validation(
                "execution.max_concurrency",
                "must be within 1..=1000",
            ));
        }
        if e.min_ready != 0 {
            return Err(DomainError::validation(
                "execution.min_ready",
                "must be 0 in the prototype (destroy-after-invoke)",
            ));
        }
        for (name, _) in &self.env_vars {
            validate_env_name(name)?;
        }
        for s in &self.secrets {
            validate_env_name(&s.env_name)?;
            if s.binding_ref.is_empty() || s.binding_ref.len() > 256 {
                return Err(DomainError::validation(
                    "secrets.binding_ref",
                    "must be 1..=256 chars",
                ));
            }
        }
        let mut names: Vec<&str> = self
            .env_vars
            .iter()
            .map(|(n, _)| n.as_str())
            .chain(self.secrets.iter().map(|s| s.env_name.as_str()))
            .collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(DomainError::validation(
                "env_vars",
                "duplicate environment variable name",
            ));
        }
        if self.description.len() > 1024 {
            return Err(DomainError::LimitExceeded {
                field: "description",
                actual: self.description.len() as u64,
                max: 1024,
            });
        }
        Ok(())
    }

    /// Canonical digest of the specification, used to prove immutability.
    pub fn digest(&self) -> Sha256Digest {
        let json = serde_json::to_vec(self).expect("RevisionSpec is serializable");
        Sha256Digest::of_bytes(&json)
    }
}

fn validate_env_name(name: &str) -> Result<(), DomainError> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && !name.as_bytes()[0].is_ascii_digit()
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.starts_with("TACHYON_");
    if ok {
        Ok(())
    } else {
        Err(DomainError::validation(
            "env_vars",
            format!("invalid or reserved environment variable name `{name}`"),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionRevision {
    pub id: RevisionId,
    pub function_id: FunctionId,
    pub tenant_id: TenantId,
    /// Monotonic per-function sequence number (1, 2, 3, ...).
    pub number: u64,
    pub spec: RevisionSpec,
    /// Digest of `spec` at creation; any later mismatch is corruption.
    pub spec_digest: Sha256Digest,
    pub status: RevisionStatus,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl FunctionRevision {
    pub fn new(
        id: RevisionId,
        function_id: FunctionId,
        tenant_id: TenantId,
        number: u64,
        spec: RevisionSpec,
        limits: &Limits,
        now: Timestamp,
    ) -> Result<Self, DomainError> {
        spec.validate(limits)?;
        let spec_digest = spec.digest();
        Ok(Self {
            id,
            function_id,
            tenant_id,
            number,
            spec,
            spec_digest,
            status: RevisionStatus::Pending,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.status, RevisionStatus::Ready)
    }

    /// Verify the stored spec still matches its digest.
    pub fn verify_integrity(&self) -> Result<(), DomainError> {
        if self.spec.digest() == self.spec_digest {
            Ok(())
        } else {
            Err(DomainError::validation(
                "spec_digest",
                "revision specification was modified after creation",
            ))
        }
    }

    fn transition(&mut self, to: RevisionStatus, now: Timestamp) -> Result<(), DomainError> {
        if self.status.is_terminal() {
            return Err(DomainError::Terminal {
                entity: "FunctionRevision",
                state: self.status.name().into(),
            });
        }
        let allowed = matches!(
            (&self.status, &to),
            (RevisionStatus::Pending, RevisionStatus::Preparing)
                | (RevisionStatus::Preparing, RevisionStatus::Validating)
                | (RevisionStatus::Validating, RevisionStatus::Ready)
                | (_, RevisionStatus::Failed { .. })
        );
        if !allowed {
            return Err(DomainError::IllegalTransition {
                entity: "FunctionRevision",
                from: self.status.name().into(),
                to: to.name().into(),
            });
        }
        self.status = to;
        self.updated_at = now;
        Ok(())
    }

    pub fn start_preparing(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(RevisionStatus::Preparing, now)
    }
    pub fn start_validating(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(RevisionStatus::Validating, now)
    }
    pub fn mark_ready(&mut self, now: Timestamp) -> Result<(), DomainError> {
        self.transition(RevisionStatus::Ready, now)
    }
    pub fn mark_failed(
        &mut self,
        reason: impl Into<String>,
        now: Timestamp,
    ) -> Result<(), DomainError> {
        self.transition(
            RevisionStatus::Failed {
                reason: reason.into(),
            },
            now,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn spec() -> RevisionSpec {
        RevisionSpec {
            artifact: ArtifactRef::Binary {
                digest: Sha256Digest::of_bytes(b"bin"),
                size_bytes: 3,
            },
            runtime: RuntimeSpec {
                protocol: RUNTIME_PROTOCOL_V1.into(),
                architecture: Architecture::Aarch64,
            },
            resources: ResourceProfile::default(),
            execution: ExecutionPolicy::default(),
            egress: EgressProfile::None,
            env_vars: vec![("GREETING".into(), "hi".into())],
            secrets: vec![SecretBinding {
                env_name: "DATABASE_URL".into(),
                binding_ref: "billing-db".into(),
            }],
            description: String::new(),
        }
    }

    fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    #[test]
    fn valid_spec_passes_and_digest_is_stable() {
        let s = spec();
        s.validate(&Limits::default()).unwrap();
        assert_eq!(s.digest(), spec().digest());
    }

    #[test]
    fn invalid_specs_are_rejected() {
        let limits = Limits::default();
        let mut s = spec();
        s.runtime.protocol = "other".into();
        assert!(s.validate(&limits).is_err());

        let mut s = spec();
        s.resources.memory_mib = 1;
        assert!(s.validate(&limits).is_err());

        // PLT-4622: the scratch drive is sized from this, so both ends are bounded.
        for mib in [
            0,
            limits.min_ephemeral_storage_mib - 1,
            limits.max_ephemeral_storage_mib + 1,
        ] {
            let mut s = spec();
            s.resources.ephemeral_storage_mib = mib;
            assert!(s.validate(&limits).is_err(), "ephemeral_storage_mib={mib}");
        }
        for mib in [
            limits.min_ephemeral_storage_mib,
            limits.max_ephemeral_storage_mib,
        ] {
            let mut s = spec();
            s.resources.ephemeral_storage_mib = mib;
            s.validate(&limits).unwrap();
        }

        let mut s = spec();
        s.execution.concurrency_per_environment = 2;
        assert!(s.validate(&limits).is_err());

        let mut s = spec();
        s.execution.timeout_seconds = 0;
        assert!(s.validate(&limits).is_err());

        let mut s = spec();
        s.env_vars.push(("TACHYON_SECRET".into(), "x".into()));
        assert!(s.validate(&limits).is_err());

        let mut s = spec();
        s.env_vars.push(("DATABASE_URL".into(), "dup".into()));
        assert!(
            s.validate(&limits).is_err(),
            "duplicate with secret env name"
        );
    }

    #[test]
    fn revision_lifecycle_and_terminal_rejection() {
        let mut r = FunctionRevision::new(
            RevisionId::generate(),
            FunctionId::generate(),
            TenantId::generate(),
            1,
            spec(),
            &Limits::default(),
            now(),
        )
        .unwrap();
        assert!(r.mark_ready(now()).is_err(), "cannot skip states");
        r.start_preparing(now()).unwrap();
        r.start_validating(now()).unwrap();
        r.mark_ready(now()).unwrap();
        assert!(r.is_ready());
        assert!(r.mark_failed("late", now()).is_err(), "terminal");
        r.verify_integrity().unwrap();
        r.spec.description = "tampered".into();
        assert!(r.verify_integrity().is_err());
    }

    #[test]
    fn failure_allowed_from_any_non_terminal_state() {
        let mut r = FunctionRevision::new(
            RevisionId::generate(),
            FunctionId::generate(),
            TenantId::generate(),
            1,
            spec(),
            &Limits::default(),
            now(),
        )
        .unwrap();
        r.mark_failed("artifact missing", now()).unwrap();
        assert!(matches!(r.status, RevisionStatus::Failed { .. }));
        assert!(r.start_preparing(now()).is_err());
    }
}

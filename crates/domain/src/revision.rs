//! FunctionRevision: an immutable, published unit of code + configuration.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::egress::{EgressAllowRule, MAX_EGRESS_ALLOW_RULES};
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
    /// Environments kept provisioned (starting, busy, parking or idle) while
    /// the revision is routed by an alias (PLT-4635). 0 (the default) lets the
    /// revision scale to zero. Above 0 the gateway pre-starts environments
    /// into the pool, which needs environment reuse to be on; the
    /// pre-started environments reserve node capacity and count against the
    /// quotas like any other.
    pub min_ready: u32,
    /// Seconds an idle pooled environment of this revision is kept before the
    /// sweeper may terminate it. `None`: `[pool] idle_ttl_seconds`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_ttl_seconds: Option<u32>,
    /// Seconds after a scale-up or an activation during which no environment
    /// of this revision is scaled down (hysteresis). `None`:
    /// `[scaling] scale_down_cooldown_seconds`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_down_cooldown_seconds: Option<u32>,
}

/// Upper bound of `execution.min_ready` (PLT-4635): pre-started environments
/// hold node capacity whether or not anything is invoked.
pub const MAX_MIN_READY: u32 = 16;
/// Upper bound of `execution.idle_ttl_seconds`.
pub const MAX_IDLE_TTL_SECONDS: u32 = 86_400;
/// Upper bound of `execution.scale_down_cooldown_seconds`.
pub const MAX_SCALE_DOWN_COOLDOWN_SECONDS: u32 = 3_600;

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            initialization_timeout_seconds: 30,
            concurrency_per_environment: 1,
            max_concurrency: 4,
            min_ready: 0,
            idle_ttl_seconds: None,
            scale_down_cooldown_seconds: None,
        }
    }
}

/// Egress profile (PLT-4622, docs/adr/0005-egress-profiles.md). Every profile
/// is default-deny towards the management network, the node, metadata,
/// link-local, private and other special-purpose ranges, and IPv6.
///
/// - `None`: no network device at all;
/// - `Restricted`: only the destinations in [`RevisionSpec::egress_allow`];
/// - `PublicWeb`: any globally reachable IPv4 unicast destination, with DNS
///   only through the provider's configured resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum EgressProfile {
    #[default]
    None,
    Restricted,
    PublicWeb,
}

impl EgressProfile {
    /// Wire name (`none`, `restricted`, `public-web`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Restricted => "restricted",
            Self::PublicWeb => "public-web",
        }
    }
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
    /// Destinations a `restricted` revision may open. Must be empty for the
    /// other profiles. Omitted from the serialized form when empty, so the
    /// digest of revisions created before the field existed is unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress_allow: Vec<EgressAllowRule>,
    /// Non-secret environment variables.
    pub env_vars: Vec<(String, String)>,
    pub secrets: Vec<SecretBinding>,
    /// Optional human description of the revision (e.g. git sha).
    pub description: String,
    /// Where this revision may run (PLT-4634). Omitted from the serialized
    /// form when unconstrained, so the digest of older revisions is unchanged.
    #[serde(default, skip_serializing_if = "Placement::is_unconstrained")]
    pub placement: Placement,
}

/// Placement constraint of a revision (docs/adr/0006-autoscaling-and-admission.md).
///
/// `region = "jp"` means *jp only*: admission rejects the invocation on a node
/// whose region label is anything else, or missing. It is never relaxed to
/// "anywhere" under load.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Placement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

impl Placement {
    pub fn is_unconstrained(&self) -> bool {
        self.region.is_none()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if let Some(region) = &self.region {
            let ok = !region.is_empty()
                && region.len() <= 64
                && region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
            if !ok {
                return Err(DomainError::validation(
                    "placement.region",
                    "must be 1..=64 bytes of [a-z0-9-]",
                ));
            }
        }
        Ok(())
    }
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
        if e.min_ready > MAX_MIN_READY || e.min_ready > e.max_concurrency {
            return Err(DomainError::validation(
                "execution.min_ready",
                format!("must be within 0..={MAX_MIN_READY} and at most execution.max_concurrency"),
            ));
        }
        if e.idle_ttl_seconds
            .is_some_and(|s| s == 0 || s > MAX_IDLE_TTL_SECONDS)
        {
            return Err(DomainError::validation(
                "execution.idle_ttl_seconds",
                format!("must be within 1..={MAX_IDLE_TTL_SECONDS}"),
            ));
        }
        if e.scale_down_cooldown_seconds
            .is_some_and(|s| s > MAX_SCALE_DOWN_COOLDOWN_SECONDS)
        {
            return Err(DomainError::validation(
                "execution.scale_down_cooldown_seconds",
                format!("must be within 0..={MAX_SCALE_DOWN_COOLDOWN_SECONDS}"),
            ));
        }
        match self.egress {
            EgressProfile::Restricted => {
                if self.egress_allow.is_empty() || self.egress_allow.len() > MAX_EGRESS_ALLOW_RULES
                {
                    return Err(DomainError::validation(
                        "egress_allow",
                        format!(
                            "egress `restricted` needs 1..={MAX_EGRESS_ALLOW_RULES} allow rules \
                             (everything else is denied)"
                        ),
                    ));
                }
                for rule in &self.egress_allow {
                    rule.validate()?;
                }
            }
            EgressProfile::None | EgressProfile::PublicWeb => {
                if !self.egress_allow.is_empty() {
                    return Err(DomainError::validation(
                        "egress_allow",
                        format!(
                            "allow rules only apply to egress `restricted`, not `{}`",
                            self.egress.as_str()
                        ),
                    ));
                }
            }
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
        self.placement.validate()?;
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
            egress_allow: Vec::new(),
            env_vars: vec![("GREETING".into(), "hi".into())],
            secrets: vec![SecretBinding {
                env_name: "DATABASE_URL".into(),
                binding_ref: "billing-db".into(),
            }],
            description: String::new(),
            placement: Placement::default(),
        }
    }

    #[test]
    fn placement_is_validated_and_absent_from_the_digest_when_unconstrained() {
        let plain = spec();
        let json = serde_json::to_string(&plain).unwrap();
        assert!(!json.contains("placement"), "{json}");
        let mut jp = spec();
        jp.placement.region = Some("jp".into());
        assert!(jp.validate(&Limits::default()).is_ok());
        assert_ne!(jp.digest(), plain.digest());
        jp.placement.region = Some("JP only".into());
        assert!(jp.validate(&Limits::default()).is_err());
    }

    fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    /// PLT-4635: the scale policy is bounded, and a revision that does not
    /// set it keeps the digest it had before the fields existed.
    #[test]
    fn scale_policy_is_validated_and_absent_from_the_digest_when_default() {
        let limits = Limits::default();
        let plain = spec();
        let json = serde_json::to_string(&plain).unwrap();
        assert!(!json.contains("idle_ttl_seconds") && !json.contains("scale_down_cooldown"));
        let ok = |f: &dyn Fn(&mut RevisionSpec)| {
            let mut s = spec();
            f(&mut s);
            s.validate(&limits)
        };
        assert!(ok(&|s| s.execution.min_ready = 2).is_ok());
        assert!(ok(&|s| s.execution.min_ready = s.execution.max_concurrency + 1).is_err());
        assert!(
            ok(&|s| {
                s.execution.max_concurrency = 100;
                s.execution.min_ready = MAX_MIN_READY + 1;
            })
            .is_err()
        );
        assert!(ok(&|s| s.execution.idle_ttl_seconds = Some(0)).is_err());
        assert!(ok(&|s| s.execution.idle_ttl_seconds = Some(5)).is_ok());
        assert!(ok(&|s| s.execution.scale_down_cooldown_seconds = Some(0)).is_ok());
        assert!(
            ok(&|s| s.execution.scale_down_cooldown_seconds =
                Some(MAX_SCALE_DOWN_COOLDOWN_SECONDS + 1))
            .is_err()
        );
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

    /// PLT-4622: `restricted` is an allowlist, the other profiles carry none,
    /// and an empty allowlist does not change the digest of older revisions.
    #[test]
    fn egress_allowlists_are_validated_per_profile() {
        use crate::egress::{EgressAllowRule, EgressProtocol};
        let limits = Limits::default();
        let allow = || EgressAllowRule {
            cidr: "1.1.1.1/32".into(),
            protocol: EgressProtocol::Tcp,
            ports: vec![443],
        };

        let mut s = spec();
        s.egress = EgressProfile::Restricted;
        assert!(s.validate(&limits).is_err(), "restricted without rules");
        s.egress_allow.push(allow());
        s.validate(&limits).unwrap();
        s.egress_allow[0].cidr = "169.254.169.254/32".into();
        assert!(s.validate(&limits).is_err(), "metadata is never allowed");

        for profile in [EgressProfile::None, EgressProfile::PublicWeb] {
            let mut s = spec();
            s.egress = profile;
            s.validate(&limits).unwrap();
            s.egress_allow.push(allow());
            assert!(s.validate(&limits).is_err(), "{profile:?} with rules");
        }

        let json = serde_json::to_value(spec()).unwrap();
        assert!(json.get("egress_allow").is_none());
        let back: RevisionSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back.digest(), spec().digest());
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

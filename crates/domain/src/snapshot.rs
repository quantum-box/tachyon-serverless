//! Snapshot manifest, restore policy and the compatibility check (X1,
//! PLT-4653, experimental; docs/adr/0017-snapshot-manifest-and-clone.md).
//!
//! A snapshot is the memory, device state and writable disk of one
//! environment, taken at the SDK checkpoint (ADR-0015 決定 2). Loading it
//! anywhere it was not taken for is unsafe in two directions: a different
//! host / VMM / kernel can corrupt the guest, and a different tenant /
//! revision / key generation would hand one principal's initialized state to
//! another. So every fact that decides "may this be loaded here" is fixed in
//! a [`SnapshotManifest`] when the snapshot is sealed, the manifest is signed
//! ([`SnapshotSigningKey`]) and [`check_compatibility`] compares it with what
//! the host would load it into, **field by field, refusing on any
//! difference**. There is no "close enough".
//!
//! Everything here is pure: no I/O, no clock reads (the caller passes `now`).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::DomainError;
use crate::ids::{EnvironmentId, FunctionId, RevisionId, Sha256Digest, SnapshotId, TenantId};
use crate::revision::{EgressProfile, RevisionSpec};

/// Version of the manifest format. A manifest of any other version is
/// refused, never interpreted.
pub const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

/// Per-revision restore policy (docs/api.md `restore.policy`).
///
/// - `disabled` (default): the revision is never restored; nothing changes.
/// - `prefer`: restore when a compatible, verified snapshot exists; otherwise
///   start cold and record why (the attempt is `start_kind = cold`).
/// - `require`: restore or fail with `Host.RestoreRequiredUnavailable`. Never
///   silently cold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestorePolicy {
    #[default]
    Disabled,
    Prefer,
    Require,
}

impl RestorePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Prefer => "prefer",
            Self::Require => "require",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        match raw {
            "disabled" => Ok(Self::Disabled),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            other => Err(DomainError::validation(
                "restore.policy",
                format!("must be disabled, prefer or require (got `{other}`)"),
            )),
        }
    }

    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// Restore settings of a revision. Omitted from the serialized spec while
/// default, so the digest of every revision created before PLT-4653 is
/// unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RestoreSettings {
    #[serde(default)]
    pub policy: RestorePolicy,
    /// The operator's statement that this revision's initialization builds
    /// only synthetic sample data (no customer data, no production secret).
    /// Required for any policy other than `disabled`, and for creating a
    /// snapshot at all.
    #[serde(default)]
    pub synthetic_init_sample: bool,
}

impl RestoreSettings {
    pub fn is_default(&self) -> bool {
        self.policy.is_disabled() && !self.synthetic_init_sample
    }

    /// Why a snapshot of `spec` must not be taken or restored, or `Ok`.
    ///
    /// X1 allows snapshots only of synthetic initialization samples without
    /// secret bindings (a snapshot's memory is the guest's memory, and there
    /// is no channel yet that delivers secrets after a restore), and only with
    /// egress `none` (a restored NIC would carry the source's IP, MAC and
    /// resolver; ADR-0015 決定 3 is not measured).
    pub fn snapshot_eligibility(spec: &RevisionSpec) -> Result<(), DomainError> {
        if !spec.restore.synthetic_init_sample {
            return Err(DomainError::validation(
                "restore.synthetic_init_sample",
                "snapshots are only allowed for revisions marked as a synthetic initialization \
                 sample (X1 experimental)",
            ));
        }
        if !spec.secrets.is_empty() {
            return Err(DomainError::validation(
                "restore",
                "snapshots are refused for revisions with secret bindings: a snapshot captures \
                 guest memory, and secrets cannot yet be delivered after a restore",
            ));
        }
        if spec.egress != EgressProfile::None {
            return Err(DomainError::validation(
                "restore",
                format!(
                    "snapshots support egress `none` only; `{}` is unsupported in X1",
                    spec.egress.as_str()
                ),
            ));
        }
        Ok(())
    }

    /// Validation at revision creation: a non-disabled policy needs a
    /// snapshot-eligible spec.
    pub fn validate(spec: &RevisionSpec) -> Result<(), DomainError> {
        if spec.restore.policy.is_disabled() {
            return Ok(());
        }
        Self::snapshot_eligibility(spec)
    }
}

/// Runtime the snapshot was taken under. Every field must match exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeProfile {
    /// `firecracker`.
    pub provider_kind: String,
    /// VMM version as reported by the binary (`Firecracker v1.17.0`).
    pub provider_version: String,
    pub vmm_sha256: Sha256Digest,
    pub kernel_sha256: Sha256Digest,
    pub rootfs_sha256: Sha256Digest,
    /// Bridge protocol version the restore frames were negotiated at.
    pub bridge_protocol_version: u32,
    /// `jailer uid=U gid=G new_pid_ns=B` or `off`.
    pub jailer_mode: String,
    /// `required` | `best-effort` | `off`.
    pub cgroup_mode: String,
    /// Host kernel release (`uname -r`). Firecracker supports loading only on
    /// the same host kernel (upstream "Where can I resume my snapshots?").
    pub host_kernel: String,
}

/// CPU and KVM identity of the host that took the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HostCpuIdentity {
    /// `aarch64` | `x86_64`.
    pub arch: String,
    /// SHA-256 over the CPU model and feature flags.
    pub cpu_model_hash: Sha256Digest,
    /// SHA-256 over the KVM API version and extension answers.
    pub kvm_capabilities_hash: Sha256Digest,
}

/// One virtio block device as configured on the source VMM.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DriveSlot {
    pub drive_id: String,
    /// Path the VMM recorded (relative to its chroot); a clone must provide
    /// a file at exactly this path.
    pub path_in_vmm: String,
    pub read_only: bool,
    pub root_device: bool,
}

/// Device model of the source VMM.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceModel {
    pub drives: Vec<DriveSlot>,
    pub vsock_guest_cid: u32,
    /// Port the guest bridge connects to on the host.
    pub vsock_port: u32,
    /// Guest listen port the host rings after a load.
    pub doorbell_port: u32,
    /// Network interfaces configured (0 for egress `none`).
    pub network_interfaces: u32,
}

/// Writable storage layout.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageLayout {
    pub scratch_mib: u32,
}

/// Network profile of the revision at snapshot time.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NetworkProfile {
    pub egress: EgressProfile,
    /// SHA-256 over the canonical `restricted` allowlist; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowlist_digest: Option<Sha256Digest>,
}

impl NetworkProfile {
    pub fn of(spec: &RevisionSpec) -> Self {
        let allowlist_digest = (spec.egress == EgressProfile::Restricted).then(|| {
            Sha256Digest::of_bytes(
                &serde_json::to_vec(&spec.egress_allow).expect("allowlist serializes"),
            )
        });
        Self {
            egress: spec.egress,
            allowlist_digest,
        }
    }
}

/// Digest and plaintext size of one artifact file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactDigest {
    pub sha256: Sha256Digest,
    pub size_bytes: u64,
}

/// The files a snapshot consists of. All four are captured at one pause
/// point (ADR-0015 決定 2) and verified before every load.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotArtifacts {
    /// Guest memory (MAP_PRIVATE by every clone; never written).
    pub memory: ArtifactDigest,
    /// VMM device state.
    pub vmstate: ArtifactDigest,
    /// Scratch drive copied while paused (the private writable base each
    /// clone copies).
    pub scratch: ArtifactDigest,
    /// Function drive the guest had mounted (its page cache refers to it).
    pub function_drive: ArtifactDigest,
}

/// Everything that decides whether a snapshot may be loaded somewhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub manifest_version: u32,
    pub snapshot_id: SnapshotId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub revision_id: RevisionId,
    pub revision_spec_digest: Sha256Digest,
    pub runtime: RuntimeProfile,
    pub host_cpu: HostCpuIdentity,
    pub devices: DeviceModel,
    pub memory_mib: u32,
    pub vcpus: u32,
    pub storage: StorageLayout,
    pub network: NetworkProfile,
    /// Id (fingerprint) of the key the artifacts are encrypted under.
    pub encryption_key_generation: String,
    /// Generation of the revision's secret bindings (`none` without any).
    pub secret_generation: String,
    pub created_at: Timestamp,
    pub expires_at: Timestamp,
    pub source_environment_id: EnvironmentId,
    /// `guest_boot_id` of the source guest: a restored copy reconnects with
    /// it, a cold boot has a different one.
    pub source_boot_id: String,
    /// SDK lifecycle contract version (`tachyon-lifecycle-version`).
    pub sdk_lifecycle_version: u32,
    /// Lifecycle phase the bridge reported at the snapshot (`checkpoint`).
    pub checkpoint_phase: String,
    pub artifacts: SnapshotArtifacts,
}

impl SnapshotManifest {
    /// Canonical serialization: compact JSON in declaration order (the
    /// manifest contains no maps, so the order is fixed by the type).
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a snapshot manifest always serializes")
    }

    pub fn digest(&self) -> Sha256Digest {
        Sha256Digest::of_bytes(&self.canonical_bytes())
    }
}

/// Lifecycle state of a snapshot in the catalog. Only `Active` may be loaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SnapshotState {
    Active,
    /// An operator (or a revision / key / secret change) withdrew it.
    Revoked {
        reason: String,
    },
    /// An integrity failure was observed; kept for inspection, never loaded.
    Quarantined {
        reason: String,
    },
    /// Past `expires_at`.
    Expired,
}

impl SnapshotState {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked { .. } => "revoked",
            Self::Quarantined { .. } => "quarantined",
            Self::Expired => "expired",
        }
    }
}

/// What the host would load a snapshot into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreTarget {
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub revision_id: RevisionId,
    pub revision_spec_digest: Sha256Digest,
    pub runtime: RuntimeProfile,
    pub host_cpu: HostCpuIdentity,
    pub devices: DeviceModel,
    pub memory_mib: u32,
    pub vcpus: u32,
    pub storage: StorageLayout,
    pub network: NetworkProfile,
    pub encryption_key_generation: String,
    pub secret_generation: String,
    pub sdk_lifecycle_version: u32,
}

/// Why a snapshot may not be loaded. `code()` is stable (metrics label,
/// evidence).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum IncompatibleReason {
    ManifestVersion {
        found: u32,
    },
    /// Another tenant's snapshot. Reported alone: nothing else about the
    /// snapshot is compared or revealed.
    TenantMismatch,
    FunctionMismatch,
    RevisionMismatch,
    SpecDigestMismatch,
    RuntimeMismatch {
        field: String,
    },
    HostCpuMismatch {
        field: String,
    },
    DeviceModelMismatch,
    MemoryMismatch,
    VcpuMismatch,
    StorageMismatch,
    NetworkMismatch,
    /// Egress other than `none` (X1 restores no NIC).
    EgressUnsupported,
    KeyGenerationChanged,
    SecretGenerationChanged,
    LifecycleVersionMismatch,
    CheckpointPhase {
        found: String,
    },
    Expired,
    Revoked {
        reason: String,
    },
    Quarantined {
        reason: String,
    },
}

impl IncompatibleReason {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ManifestVersion { .. } => "manifest_version",
            Self::TenantMismatch => "tenant_mismatch",
            Self::FunctionMismatch => "function_mismatch",
            Self::RevisionMismatch => "revision_mismatch",
            Self::SpecDigestMismatch => "spec_digest_mismatch",
            Self::RuntimeMismatch { .. } => "runtime_mismatch",
            Self::HostCpuMismatch { .. } => "host_cpu_mismatch",
            Self::DeviceModelMismatch => "device_model_mismatch",
            Self::MemoryMismatch => "memory_mismatch",
            Self::VcpuMismatch => "vcpu_mismatch",
            Self::StorageMismatch => "storage_mismatch",
            Self::NetworkMismatch => "network_mismatch",
            Self::EgressUnsupported => "egress_unsupported",
            Self::KeyGenerationChanged => "key_generation_changed",
            Self::SecretGenerationChanged => "secret_generation_changed",
            Self::LifecycleVersionMismatch => "lifecycle_version_mismatch",
            Self::CheckpointPhase { .. } => "checkpoint_phase",
            Self::Expired => "expired",
            Self::Revoked { .. } => "revoked",
            Self::Quarantined { .. } => "quarantined",
        }
    }
}

impl fmt::Display for IncompatibleReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManifestVersion { found } => write!(f, "manifest_version {found} unsupported"),
            Self::RuntimeMismatch { field } => write!(f, "runtime_mismatch ({field})"),
            Self::HostCpuMismatch { field } => write!(f, "host_cpu_mismatch ({field})"),
            Self::CheckpointPhase { found } => write!(f, "checkpoint_phase `{found}`"),
            Self::Revoked { reason } => write!(f, "revoked ({reason})"),
            Self::Quarantined { reason } => write!(f, "quarantined ({reason})"),
            other => f.write_str(other.code()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Compatibility {
    Compatible,
    Incompatible { reasons: Vec<IncompatibleReason> },
}

impl Compatibility {
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }

    pub fn reasons(&self) -> &[IncompatibleReason] {
        match self {
            Self::Compatible => &[],
            Self::Incompatible { reasons } => reasons,
        }
    }
}

/// The only lifecycle phase a snapshot may be taken in: after `checkpoint`,
/// before `continue` was answered, so `after_restore` (and anything it
/// creates, such as credentials) has never run.
pub const CHECKPOINT_PHASE: &str = "checkpoint";

/// Compare a manifest (already signature-verified) and its catalog state
/// with the target. Pure; `now` decides expiry.
pub fn check_compatibility(
    manifest: &SnapshotManifest,
    state: &SnapshotState,
    target: &RestoreTarget,
    now: Timestamp,
) -> Compatibility {
    // Another tenant's snapshot is refused before anything else is looked at.
    if manifest.tenant_id != target.tenant_id {
        return Compatibility::Incompatible {
            reasons: vec![IncompatibleReason::TenantMismatch],
        };
    }
    let mut reasons = Vec::new();
    if manifest.manifest_version != SNAPSHOT_MANIFEST_VERSION {
        reasons.push(IncompatibleReason::ManifestVersion {
            found: manifest.manifest_version,
        });
    }
    match state {
        SnapshotState::Active => {}
        SnapshotState::Revoked { reason } => reasons.push(IncompatibleReason::Revoked {
            reason: reason.clone(),
        }),
        SnapshotState::Quarantined { reason } => reasons.push(IncompatibleReason::Quarantined {
            reason: reason.clone(),
        }),
        SnapshotState::Expired => reasons.push(IncompatibleReason::Expired),
    }
    if now >= manifest.expires_at && !matches!(state, SnapshotState::Expired) {
        reasons.push(IncompatibleReason::Expired);
    }
    if manifest.function_id != target.function_id {
        reasons.push(IncompatibleReason::FunctionMismatch);
    }
    if manifest.revision_id != target.revision_id {
        reasons.push(IncompatibleReason::RevisionMismatch);
    }
    if manifest.revision_spec_digest != target.revision_spec_digest {
        reasons.push(IncompatibleReason::SpecDigestMismatch);
    }
    let (m, t) = (&manifest.runtime, &target.runtime);
    for (field, same) in [
        ("provider_kind", m.provider_kind == t.provider_kind),
        ("provider_version", m.provider_version == t.provider_version),
        ("vmm_sha256", m.vmm_sha256 == t.vmm_sha256),
        ("kernel_sha256", m.kernel_sha256 == t.kernel_sha256),
        ("rootfs_sha256", m.rootfs_sha256 == t.rootfs_sha256),
        (
            "bridge_protocol_version",
            m.bridge_protocol_version == t.bridge_protocol_version,
        ),
        ("jailer_mode", m.jailer_mode == t.jailer_mode),
        ("cgroup_mode", m.cgroup_mode == t.cgroup_mode),
        ("host_kernel", m.host_kernel == t.host_kernel),
    ] {
        if !same {
            reasons.push(IncompatibleReason::RuntimeMismatch {
                field: field.to_string(),
            });
        }
    }
    let (m, t) = (&manifest.host_cpu, &target.host_cpu);
    for (field, same) in [
        ("arch", m.arch == t.arch),
        ("cpu_model_hash", m.cpu_model_hash == t.cpu_model_hash),
        (
            "kvm_capabilities_hash",
            m.kvm_capabilities_hash == t.kvm_capabilities_hash,
        ),
    ] {
        if !same {
            reasons.push(IncompatibleReason::HostCpuMismatch {
                field: field.to_string(),
            });
        }
    }
    if manifest.devices != target.devices {
        reasons.push(IncompatibleReason::DeviceModelMismatch);
    }
    if manifest.memory_mib != target.memory_mib {
        reasons.push(IncompatibleReason::MemoryMismatch);
    }
    if manifest.vcpus != target.vcpus {
        reasons.push(IncompatibleReason::VcpuMismatch);
    }
    if manifest.storage != target.storage {
        reasons.push(IncompatibleReason::StorageMismatch);
    }
    if manifest.network != target.network {
        reasons.push(IncompatibleReason::NetworkMismatch);
    }
    if manifest.network.egress != EgressProfile::None
        || target.network.egress != EgressProfile::None
    {
        reasons.push(IncompatibleReason::EgressUnsupported);
    }
    if manifest.encryption_key_generation != target.encryption_key_generation {
        reasons.push(IncompatibleReason::KeyGenerationChanged);
    }
    if manifest.secret_generation != target.secret_generation {
        reasons.push(IncompatibleReason::SecretGenerationChanged);
    }
    if manifest.sdk_lifecycle_version != target.sdk_lifecycle_version {
        reasons.push(IncompatibleReason::LifecycleVersionMismatch);
    }
    if manifest.checkpoint_phase != CHECKPOINT_PHASE {
        reasons.push(IncompatibleReason::CheckpointPhase {
            found: manifest.checkpoint_phase.clone(),
        });
    }
    if reasons.is_empty() {
        Compatibility::Compatible
    } else {
        Compatibility::Incompatible { reasons }
    }
}

// ---------------------------------------------------------------------------
// signature
// ---------------------------------------------------------------------------

/// Why a sealed manifest was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest signature key {found} is not the configured key {expected}")]
    KeyMismatch { expected: String, found: String },
    #[error("manifest signature does not verify")]
    BadSignature,
    #[error("manifest digest does not match its content")]
    DigestMismatch,
    #[error("manifest is not valid: {0}")]
    Malformed(String),
    #[error("manifest is not in canonical form")]
    NotCanonical,
}

/// HMAC-SHA256 key that signs manifests. `Debug` never shows the key.
pub struct SnapshotSigningKey {
    key: [u8; 32],
    id: String,
}

impl fmt::Debug for SnapshotSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotSigningKey")
            .field("id", &self.id)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl Drop for SnapshotSigningKey {
    fn drop(&mut self) {
        self.key.fill(0);
    }
}

impl SnapshotSigningKey {
    pub fn from_bytes(key: [u8; 32]) -> Self {
        let mut labelled = b"tachyon-serverless/snapshot-signing-key-id/v1\0".to_vec();
        labelled.extend_from_slice(&key);
        let fingerprint = Sha256Digest::of_bytes(&labelled);
        labelled.fill(0);
        Self {
            key,
            id: format!("s1-{}", &fingerprint.hex()[..16]),
        }
    }

    pub fn from_hex(text: &str) -> Result<Self, DomainError> {
        let bytes = hex::decode(text.trim()).map_err(|_| {
            DomainError::validation("snapshot signing key", "must be 64 hex characters")
        })?;
        let key: [u8; 32] = bytes.try_into().map_err(|_| {
            DomainError::validation("snapshot signing key", "must be 32 bytes (64 hex)")
        })?;
        Ok(Self::from_bytes(key))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn mac(&self, message: &[u8]) -> [u8; 32] {
        hmac_sha256(&self.key, message)
    }

    /// Seal a manifest: canonical bytes, their digest and an HMAC over
    /// `label || key id || canonical bytes`.
    pub fn seal(&self, manifest: &SnapshotManifest) -> SealedManifest {
        let canonical = manifest.canonical_bytes();
        let digest = Sha256Digest::of_bytes(&canonical);
        let signature = hex::encode(self.mac(&signed_message(&self.id, &canonical)));
        SealedManifest {
            manifest: String::from_utf8(canonical).expect("JSON is UTF-8"),
            digest,
            key_id: self.id.clone(),
            signature,
        }
    }

    /// Verify a sealed manifest and return its content. Checks, in order:
    /// signing key id, HMAC (constant time), digest, parse, canonical form.
    pub fn verify(&self, sealed: &SealedManifest) -> Result<SnapshotManifest, ManifestError> {
        if sealed.key_id != self.id {
            return Err(ManifestError::KeyMismatch {
                expected: self.id.clone(),
                found: sealed.key_id.clone(),
            });
        }
        let expected = self.mac(&signed_message(&self.id, sealed.manifest.as_bytes()));
        let given = hex::decode(&sealed.signature).map_err(|_| ManifestError::BadSignature)?;
        if !constant_time_eq(&expected, &given) {
            return Err(ManifestError::BadSignature);
        }
        if Sha256Digest::of_bytes(sealed.manifest.as_bytes()) != sealed.digest {
            return Err(ManifestError::DigestMismatch);
        }
        let manifest: SnapshotManifest = serde_json::from_str(&sealed.manifest)
            .map_err(|e| ManifestError::Malformed(e.to_string()))?;
        if manifest.canonical_bytes() != sealed.manifest.as_bytes() {
            return Err(ManifestError::NotCanonical);
        }
        Ok(manifest)
    }
}

/// A manifest as stored: the exact signed bytes plus digest and signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedManifest {
    /// Canonical JSON of the [`SnapshotManifest`].
    pub manifest: String,
    pub digest: Sha256Digest,
    pub key_id: String,
    /// Hex HMAC-SHA256.
    pub signature: String,
}

fn signed_message(key_id: &str, canonical: &[u8]) -> Vec<u8> {
    let mut m = b"tachyon-serverless/snapshot-manifest/v1\0".to_vec();
    m.extend_from_slice(key_id.as_bytes());
    m.push(0);
    m.extend_from_slice(canonical);
    m
}

/// HMAC-SHA256 (RFC 2104) over `sha2`.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(message)
        .finalize();
    let outer = Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize();
    k.fill(0);
    outer.into()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;

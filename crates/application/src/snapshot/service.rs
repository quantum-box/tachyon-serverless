//! Snapshot creation, restore planning and revocation (X1, PLT-4653).
//!
//! Creation (an operator action, `POST /v1/functions/{id}/snapshots`):
//!
//! ```text
//! eligible revision (synthetic sample, no secret bindings, egress none)
//!   -> provider capability gate (Unverified needs [snapshots] allow_unverified)
//!   -> boot a source environment with HelloAck{snapshot_hold}, no secrets
//!   -> guest reports CheckpointWaiting{phase = checkpoint, after_restore_ran = false}
//!   -> provider pauses and captures memory + vmstate + scratch + function drive
//!   -> source terminated (never resumed)
//!   -> each file sealed (AES-256-GCM chunks, tenant-bound) with its plaintext sha256
//!   -> manifest built from the host profile and signed (HMAC-SHA256)
//! ```
//!
//! Restore planning (every invocation of a `prefer` / `require` revision):
//! candidates of the function, newest first -> state must be `active` ->
//! manifest signature -> [`check_compatibility`] against this host, this
//! revision and the current key -> the plaintext files the provider will load
//! are verified against the manifest digests (decrypted from the sealed store
//! first if missing). A corrupted file quarantines the snapshot; a key or
//! secret generation change revokes it; an expired one is marked expired.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tachyon_serverless_api_types::SnapshotResponse;
use tachyon_serverless_domain::{
    AliasName, ArtifactRef, Clock, Compatibility, EnvironmentId, Function, FunctionId,
    FunctionRevision, IdGenerator, IncompatibleReason, Limits, ManifestError, NetworkProfile,
    RestoreSettings, RestoreTarget, RevisionId, RevisionStatus, SNAPSHOT_MANIFEST_VERSION,
    SnapshotArtifacts, SnapshotId, SnapshotManifest, SnapshotSigningKey, SnapshotState,
    StorageLayout, check_compatibility,
};
use tachyon_serverless_protocol::runtime_api::lifecycle;
use tachyon_serverless_provider_port::restore::files;
use tachyon_serverless_provider_port::{
    ArtifactLocation, EnvironmentSpec, ExecutionProvider, Principal, ProviderError, Support,
    TerminateReason,
};

use super::store::{SnapshotRecord, SnapshotStore, StoreError, digest_file};
use crate::authz::{ensure_tenant, require_deploy, require_read};
use crate::bridge_session::{BridgeSession, HelloAckParams, LogContext, LogForwarder};
use crate::durable::ObjectKey;
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::repository::Repositories;

/// `[snapshots]` settings the service needs at run time.
#[derive(Debug, Clone)]
pub struct SnapshotSettings {
    /// Lifetime of a snapshot from its creation.
    pub ttl: Duration,
    /// Accept a provider whose snapshot capabilities are `Unverified`
    /// (a measurement run). Without it only `Supported` is accepted.
    pub allow_unverified: bool,
    /// Handshake budget of the source environment.
    pub handshake_timeout: Duration,
}

pub struct SnapshotServiceDeps {
    pub store: SnapshotStore,
    pub key: Arc<ObjectKey>,
    pub signing: Arc<SnapshotSigningKey>,
    pub provider: Arc<dyn ExecutionProvider>,
    pub repos: Repositories,
    pub artifacts: Arc<dyn tachyon_serverless_provider_port::ArtifactStore>,
    pub entrypoints: EntrypointPolicy,
    pub limits: Limits,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub settings: SnapshotSettings,
}

/// Why no restore is possible for an invocation. `code` is stable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreUnavailable {
    pub code: String,
    pub detail: String,
}

impl RestoreUnavailable {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for RestoreUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

/// A verified snapshot ready to be cloned.
#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub manifest: SnapshotManifest,
    pub manifest_digest: String,
    /// Plaintext files, verified against the manifest just now.
    pub dir: PathBuf,
    /// Generation of the clone about to be created (1 = first).
    pub generation: u64,
    /// Time spent verifying (and, if needed, decrypting) the files.
    pub verify_ms: u64,
}

pub struct SnapshotService {
    store: SnapshotStore,
    key: Arc<ObjectKey>,
    signing: Arc<SnapshotSigningKey>,
    provider: Arc<dyn ExecutionProvider>,
    repos: Repositories,
    artifacts: Arc<dyn tachyon_serverless_provider_port::ArtifactStore>,
    entrypoints: EntrypointPolicy,
    limits: Limits,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    settings: SnapshotSettings,
    /// Serializes catalog record updates.
    records: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for SnapshotService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotService")
            .field("root", &self.store.root())
            .field("key_id", &self.key.id())
            .field("signing_key_id", &self.signing.id())
            .finish_non_exhaustive()
    }
}

fn store_err(e: StoreError) -> AppError {
    AppError::platform(format!("snapshot store: {e}"))
}

/// Generation of a revision's secret bindings as a manifest records it.
pub fn secret_generation(revision: &FunctionRevision) -> String {
    if revision.spec.secrets.is_empty() {
        return "none".into();
    }
    let mut refs: Vec<String> = revision
        .spec
        .secrets
        .iter()
        .map(|s| format!("{}={}", s.env_name, s.binding_ref))
        .collect();
    refs.sort();
    tachyon_serverless_domain::Sha256Digest::of_bytes(refs.join("\n").as_bytes()).to_string()
}

impl SnapshotService {
    pub fn new(deps: SnapshotServiceDeps) -> Arc<Self> {
        Arc::new(Self {
            store: deps.store,
            key: deps.key,
            signing: deps.signing,
            provider: deps.provider,
            repos: deps.repos,
            artifacts: deps.artifacts,
            entrypoints: deps.entrypoints,
            limits: deps.limits,
            clock: deps.clock,
            ids: deps.ids,
            settings: deps.settings,
            records: tokio::sync::Mutex::new(()),
        })
    }

    pub fn store(&self) -> &SnapshotStore {
        &self.store
    }

    /// The provider may snapshot and clone: `Supported`, or `Unverified` with
    /// `allow_unverified`.
    pub fn capability_gate(&self) -> Result<(), RestoreUnavailable> {
        let caps = self.provider.capabilities();
        for (what, support) in [
            ("snapshot_create", &caps.snapshot_create),
            ("snapshot_clone", &caps.snapshot_clone),
        ] {
            match support {
                Support::Supported => {}
                Support::Unverified { note } if self.settings.allow_unverified => {
                    tracing::debug!(what, note, "unverified snapshot capability allowed");
                }
                Support::Unverified { note } => {
                    return Err(RestoreUnavailable::new(
                        "capability_unverified",
                        format!(
                            "provider {what} is unverified ({note}); set [snapshots] \
                             allow_unverified for a measurement run"
                        ),
                    ));
                }
                Support::Unsupported { reason } => {
                    return Err(RestoreUnavailable::new(
                        "capability_unsupported",
                        format!("provider {what} is unsupported: {reason}"),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn target(
        &self,
        function: &Function,
        revision: &FunctionRevision,
    ) -> Result<RestoreTarget, RestoreUnavailable> {
        let profile =
            self.provider.restore_profile().await.map_err(|e| {
                RestoreUnavailable::new("provider_profile_unavailable", e.to_string())
            })?;
        Ok(RestoreTarget {
            tenant_id: function.tenant_id.clone(),
            function_id: function.id.clone(),
            revision_id: revision.id.clone(),
            revision_spec_digest: revision.spec_digest.clone(),
            runtime: profile.runtime,
            host_cpu: profile.host_cpu,
            devices: profile.devices,
            memory_mib: revision.spec.resources.memory_mib,
            vcpus: revision.spec.resources.vcpus(),
            storage: StorageLayout {
                scratch_mib: revision.spec.resources.ephemeral_storage_mib,
            },
            network: NetworkProfile::of(&revision.spec),
            encryption_key_generation: self.key.id().to_string(),
            secret_generation: secret_generation(revision),
            sdk_lifecycle_version: lifecycle::VERSION,
        })
    }

    fn owned_function(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Function, AppError> {
        let function = self
            .repos
            .functions
            .get(function_id)?
            .ok_or_else(|| AppError::not_found("function not found"))?;
        ensure_tenant(principal, &function.tenant_id, "function")?;
        Ok(function)
    }

    fn view(&self, record: &SnapshotRecord) -> Option<SnapshotResponse> {
        let sealed = self.store.read_manifest(&record.snapshot_id).ok()?;
        // Listing does not verify the signature; restore planning does.
        let manifest: SnapshotManifest = serde_json::from_str(&sealed.manifest).ok()?;
        let reason = match &record.state {
            SnapshotState::Revoked { reason } | SnapshotState::Quarantined { reason } => {
                Some(reason.clone())
            }
            _ => None,
        };
        let state = match (&record.state, self.clock.now() >= manifest.expires_at) {
            (SnapshotState::Active, true) => "expired".to_string(),
            (s, _) => s.name().to_string(),
        };
        Some(SnapshotResponse {
            id: record.snapshot_id.to_string(),
            function_id: record.function_id.to_string(),
            revision_id: record.revision_id.to_string(),
            state,
            state_reason: reason,
            manifest_digest: sealed.digest.to_string(),
            manifest_version: manifest.manifest_version,
            provider: manifest.runtime.provider_kind.clone(),
            memory_mib: manifest.memory_mib,
            vcpus: manifest.vcpus,
            source_environment_id: manifest.source_environment_id.to_string(),
            restores: record.restores,
            created_at: manifest.created_at,
            expires_at: manifest.expires_at,
            timings: record.timings.clone(),
        })
    }

    pub fn list(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Vec<SnapshotResponse>, AppError> {
        require_read(principal)?;
        let function = self.owned_function(principal, function_id)?;
        Ok(self
            .store
            .list()
            .into_iter()
            .filter(|r| r.function_id == function.id && r.tenant_id == function.tenant_id)
            .filter_map(|r| self.view(&r))
            .collect())
    }

    pub async fn revoke(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        snapshot_id: &SnapshotId,
        reason: &str,
    ) -> Result<SnapshotResponse, AppError> {
        require_deploy(principal)?;
        let function = self.owned_function(principal, function_id)?;
        let record = self
            .store
            .read_record(snapshot_id)
            .ok()
            .filter(|r| r.function_id == function.id && r.tenant_id == function.tenant_id)
            .ok_or_else(|| AppError::not_found("snapshot not found"))?;
        let record = self
            .set_state(
                &record.snapshot_id,
                SnapshotState::Revoked {
                    reason: format!("revoked by {}: {reason}", principal.subject),
                },
            )
            .await
            .map_err(store_err)?;
        self.view(&record)
            .ok_or_else(|| AppError::platform("snapshot manifest unreadable"))
    }

    async fn set_state(
        &self,
        id: &SnapshotId,
        state: SnapshotState,
    ) -> Result<SnapshotRecord, StoreError> {
        let _guard = self.records.lock().await;
        let mut record = self.store.read_record(id)?;
        // A quarantine is never lifted by a later state change.
        if !matches!(record.state, SnapshotState::Quarantined { .. }) {
            tracing::warn!(snapshot_id = %id, from = record.state.name(), to = state.name(), "snapshot state changed");
            record.state = state;
            self.store.write_record(&record)?;
        }
        Ok(record)
    }

    /// Mark a snapshot quarantined (integrity failure). Best effort.
    pub async fn quarantine(&self, id: &SnapshotId, reason: impl Into<String>) {
        let reason = reason.into();
        tracing::error!(snapshot_id = %id, %reason, "snapshot quarantined");
        if let Err(e) = self
            .set_state(id, SnapshotState::Quarantined { reason })
            .await
        {
            tracing::error!(snapshot_id = %id, error = %e, "cannot record the quarantine");
        }
    }

    // -----------------------------------------------------------------------
    // create
    // -----------------------------------------------------------------------

    /// Create a snapshot of `revision_id` (default: the `prod` alias).
    pub async fn create(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        revision_id: Option<&RevisionId>,
    ) -> Result<SnapshotResponse, AppError> {
        require_deploy(principal)?;
        let function = self.owned_function(principal, function_id)?;
        if function.is_deleted() {
            return Err(AppError::FunctionDeleted(format!(
                "function {} is deleted",
                function.id
            )));
        }
        let revision_id = match revision_id {
            Some(id) => id.clone(),
            None => {
                let prod = AliasName::parse(AliasName::DEFAULT)?;
                self.repos
                    .aliases
                    .get(&function.id, &prod)?
                    .ok_or_else(|| AppError::not_found("alias prod not found"))?
                    .revision_id
            }
        };
        let revision = self
            .repos
            .revisions
            .get(&revision_id)?
            .filter(|r| r.function_id == function.id && r.tenant_id == function.tenant_id)
            .ok_or_else(|| AppError::not_found("revision not found"))?;
        if !matches!(revision.status, RevisionStatus::Ready) {
            return Err(AppError::RevisionNotReady(format!(
                "revision {} is {}",
                revision.id,
                revision.status.name()
            )));
        }
        RestoreSettings::snapshot_eligibility(&revision.spec)
            .map_err(|e| AppError::InvalidRequest(format!("snapshot refused: {e}")))?;
        self.capability_gate()
            .map_err(|u| AppError::ProviderUnavailable(format!("snapshot refused: {u}")))?;
        let target = self
            .target(&function, &revision)
            .await
            .map_err(|u| AppError::ProviderUnavailable(format!("snapshot refused: {u}")))?;
        let snapshot_id = SnapshotId::from_ulid(self.ids.next_ulid());
        let result = self
            .capture(&function, &revision, &snapshot_id, target)
            .await;
        if result.is_err() {
            let _ = self.store.remove(&snapshot_id);
            if let Some(dir) = self.provider.snapshot_dir(&snapshot_id) {
                let _ = tokio::fs::remove_dir_all(dir).await;
            }
        }
        result
    }

    async fn capture(
        &self,
        function: &Function,
        revision: &FunctionRevision,
        snapshot_id: &SnapshotId,
        target: RestoreTarget,
    ) -> Result<SnapshotResponse, AppError> {
        let ArtifactRef::Binary { digest, .. } = &revision.spec.artifact else {
            return Err(AppError::InvalidRequest(
                "only binary artifacts can be snapshotted".into(),
            ));
        };
        let stored = self
            .artifacts
            .get(digest)
            .await
            .map_err(|e| AppError::platform(format!("artifact unavailable: {e}")))?;
        let env_id = EnvironmentId::from_ulid(self.ids.next_ulid());
        let init_timeout = Duration::from_secs(u64::from(
            revision.spec.execution.initialization_timeout_seconds,
        ));
        let artifact = ArtifactLocation {
            path: stored.path.clone(),
            digest: stored.digest.clone(),
            size_bytes: stored.size_bytes,
        };
        let spec = EnvironmentSpec {
            environment_id: env_id.clone(),
            tenant_id: function.tenant_id.clone(),
            revision_id: revision.id.clone(),
            artifact: artifact.clone(),
            architecture: revision.spec.runtime.architecture,
            resources: revision.spec.resources,
            egress: revision.spec.egress,
            egress_allow: revision.spec.egress_allow.clone(),
            connect_timeout: init_timeout,
        };
        tracing::info!(%snapshot_id, environment_id = %env_id, revision_id = %revision.id, "snapshot: booting the source environment");
        let handle = self
            .provider
            .create_environment(spec)
            .await
            .map_err(|e| AppError::ProviderUnavailable(format!("snapshot source: {e}")))?;
        let outcome = self
            .hold_and_capture(
                function,
                revision,
                snapshot_id,
                &env_id,
                handle,
                &artifact,
                init_timeout,
            )
            .await;
        // The source is never resumed: its guest state now lives in the
        // snapshot, and a resumed copy would share its identity.
        let _ = self
            .provider
            .terminate_environment(&env_id, TerminateReason::Quiesced)
            .await;
        let (report_boot_id, lifecycle_version, timings) = outcome?;

        let seal_started = Instant::now();
        let dir = self
            .provider
            .snapshot_dir(snapshot_id)
            .ok_or_else(|| AppError::platform("provider has no snapshot directory"))?;
        let artifacts = {
            let store = self.store.clone();
            let key = self.key.clone();
            let tenant = function.tenant_id.clone();
            let id = snapshot_id.clone();
            tokio::task::spawn_blocking(move || -> Result<SnapshotArtifacts, StoreError> {
                let mut d = Vec::new();
                for name in files::ALL {
                    d.push(store.seal_artifact(&key, &tenant, &id, name, &dir.join(name))?);
                }
                Ok(SnapshotArtifacts {
                    memory: d[0].clone(),
                    vmstate: d[1].clone(),
                    scratch: d[2].clone(),
                    function_drive: d[3].clone(),
                })
            })
            .await
            .map_err(|e| AppError::platform(format!("seal task: {e}")))?
            .map_err(store_err)?
        };
        let seal_ms = seal_started.elapsed().as_millis() as u64;
        let now = self.clock.now();
        let manifest = SnapshotManifest {
            manifest_version: SNAPSHOT_MANIFEST_VERSION,
            snapshot_id: snapshot_id.clone(),
            tenant_id: target.tenant_id,
            function_id: target.function_id,
            revision_id: target.revision_id,
            revision_spec_digest: target.revision_spec_digest,
            runtime: target.runtime,
            host_cpu: target.host_cpu,
            devices: target.devices,
            memory_mib: target.memory_mib,
            vcpus: target.vcpus,
            storage: target.storage,
            network: target.network,
            encryption_key_generation: target.encryption_key_generation,
            secret_generation: target.secret_generation,
            created_at: now,
            expires_at: now
                + chrono::Duration::from_std(self.settings.ttl)
                    .unwrap_or(chrono::Duration::hours(1)),
            source_environment_id: env_id,
            source_boot_id: report_boot_id,
            sdk_lifecycle_version: lifecycle_version,
            checkpoint_phase: tachyon_serverless_domain::CHECKPOINT_PHASE.to_string(),
            artifacts,
        };
        let sealed = self.signing.seal(&manifest);
        self.store
            .write_manifest(snapshot_id, &sealed)
            .map_err(store_err)?;
        let record = SnapshotRecord {
            snapshot_id: snapshot_id.clone(),
            tenant_id: function.tenant_id.clone(),
            function_id: function.id.clone(),
            revision_id: revision.id.clone(),
            created_at: now,
            state: SnapshotState::Active,
            restores: 0,
            timings: serde_json::json!({
                "pause_ms": timings.pause_ms,
                "create_ms": timings.create_ms,
                "copy_ms": timings.copy_ms,
                "seal_ms": seal_ms,
            }),
        };
        self.store.write_record(&record).map_err(store_err)?;
        tracing::info!(%snapshot_id, digest = %sealed.digest, "snapshot sealed");
        self.view(&record)
            .ok_or_else(|| AppError::platform("snapshot manifest unreadable"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn hold_and_capture(
        &self,
        function: &Function,
        revision: &FunctionRevision,
        snapshot_id: &SnapshotId,
        env_id: &EnvironmentId,
        handle: tachyon_serverless_provider_port::EnvironmentHandle,
        artifact: &ArtifactLocation,
        init_timeout: Duration,
    ) -> Result<
        (
            String,
            u32,
            tachyon_serverless_provider_port::SnapshotTimings,
        ),
        AppError,
    > {
        let entry = self
            .entrypoints
            .resolve(&self.provider.kind(), &artifact.path, env_id);
        let mut env = revision.spec.env_vars.clone();
        if self.provider.capabilities().dev_only {
            env.push((
                tachyon_serverless_protocol::env::UNISOLATED.to_string(),
                "1".to_string(),
            ));
        }
        // No secret is resolved: the spec has no bindings (eligibility).
        let params = HelloAckParams {
            entrypoint: entry.entrypoint,
            args: entry.args,
            env,
            working_dir: entry.working_dir,
            init_timeout,
            max_response_bytes: self.limits.max_response_bytes,
            max_log_line_bytes: self.limits.max_log_line_bytes as u64,
        };
        let logs = LogForwarder::new(
            self.repos.logs.clone(),
            self.clock.clone(),
            LogContext {
                tenant_id: function.tenant_id.clone(),
                environment_id: env_id.clone(),
                invocation_id: None,
                max_line_bytes: self.limits.max_log_line_bytes,
            },
        );
        let (mut session, hello) = BridgeSession::handshake_for_snapshot(
            handle.stream,
            env_id,
            1,
            params,
            logs,
            self.settings.handshake_timeout,
        )
        .await
        .map_err(|e| AppError::InvalidRequest(format!("snapshot source handshake: {e}")))?;
        let boot_id = hello.guest_boot_id.clone().ok_or_else(|| {
            AppError::InvalidRequest(
                "the source guest reported no boot id; a restore could not be told from a cold boot"
                    .into(),
            )
        })?;
        let report = session
            .wait_checkpoint(handle.created_at + init_timeout)
            .await
            .map_err(|e| AppError::InvalidRequest(format!("snapshot source checkpoint: {e}")))?;
        tracing::info!(%snapshot_id, environment_id = %env_id, phase = %report.lifecycle_phase, "snapshot: source holds at the checkpoint");
        let capture = self
            .provider
            .snapshot_environment(env_id, snapshot_id)
            .await
            .map_err(|e: ProviderError| {
                AppError::ProviderUnavailable(format!("snapshot capture: {e}"))
            })?;
        drop(session);
        Ok((boot_id, report.lifecycle_version, capture.timings))
    }

    // -----------------------------------------------------------------------
    // restore planning
    // -----------------------------------------------------------------------

    /// Find, verify and reserve a snapshot this invocation may clone.
    pub async fn plan_restore(
        &self,
        function: &Function,
        revision: &FunctionRevision,
    ) -> Result<RestorePlan, RestoreUnavailable> {
        self.capability_gate()?;
        RestoreSettings::snapshot_eligibility(&revision.spec)
            .map_err(|e| RestoreUnavailable::new("revision_not_eligible", e.to_string()))?;
        let target = self.target(function, revision).await?;
        let now = self.clock.now();
        let candidates: Vec<SnapshotRecord> = self
            .store
            .list()
            .into_iter()
            .filter(|r| r.function_id == function.id)
            .collect();
        if candidates.is_empty() {
            return Err(RestoreUnavailable::new(
                "no_snapshot",
                format!("function {} has no snapshot", function.id),
            ));
        }
        let mut refusals: Vec<String> = Vec::new();
        let mut first_code: Option<String> = None;
        let mut note = |id: &SnapshotId, code: &str, detail: String| {
            if first_code.is_none() {
                first_code = Some(code.to_string());
            }
            refusals.push(format!("{id}: {detail}"));
        };
        for record in candidates {
            let id = record.snapshot_id.clone();
            let sealed = match self.store.read_manifest(&id) {
                Ok(s) => s,
                Err(e) => {
                    self.quarantine(&id, format!("manifest unreadable: {e}"))
                        .await;
                    note(&id, "manifest_invalid", format!("manifest unreadable: {e}"));
                    continue;
                }
            };
            let manifest = match self.signing.verify(&sealed) {
                Ok(m) => m,
                Err(e @ ManifestError::KeyMismatch { .. }) => {
                    let _ = self
                        .set_state(
                            &id,
                            SnapshotState::Revoked {
                                reason: format!("stale: {e}"),
                            },
                        )
                        .await;
                    note(&id, "key_generation_changed", e.to_string());
                    continue;
                }
                Err(e) => {
                    self.quarantine(&id, format!("manifest does not verify: {e}"))
                        .await;
                    note(&id, "manifest_invalid", e.to_string());
                    continue;
                }
            };
            match check_compatibility(&manifest, &record.state, &target, now) {
                Compatibility::Compatible => {}
                Compatibility::Incompatible { reasons } => {
                    let codes: Vec<String> = reasons.iter().map(ToString::to_string).collect();
                    if reasons.iter().any(|r| {
                        matches!(
                            r,
                            IncompatibleReason::KeyGenerationChanged
                                | IncompatibleReason::SecretGenerationChanged
                        )
                    }) && matches!(record.state, SnapshotState::Active)
                        && manifest.tenant_id == target.tenant_id
                    {
                        let _ = self
                            .set_state(
                                &id,
                                SnapshotState::Revoked {
                                    reason: format!("stale: {}", codes.join(", ")),
                                },
                            )
                            .await;
                    } else if reasons.contains(&IncompatibleReason::Expired)
                        && matches!(record.state, SnapshotState::Active)
                    {
                        let _ = self.set_state(&id, SnapshotState::Expired).await;
                    }
                    let code = reasons.first().map(|r| r.code()).unwrap_or("incompatible");
                    note(&id, code, format!("incompatible: {}", codes.join(", ")));
                    continue;
                }
            }
            let Some(dir) = self.provider.snapshot_dir(&id) else {
                note(
                    &id,
                    "provider_no_snapshot_dir",
                    "the provider keeps no snapshot files".into(),
                );
                continue;
            };
            let started = Instant::now();
            match self.verify_files(&manifest, &dir).await {
                Ok(()) => {}
                Err(e) => {
                    self.quarantine(&id, e.clone()).await;
                    note(&id, "artifact_corrupted", e);
                    continue;
                }
            }
            let verify_ms = started.elapsed().as_millis() as u64;
            let generation = {
                let _guard = self.records.lock().await;
                let mut r = match self.store.read_record(&id) {
                    Ok(r) => r,
                    Err(e) => {
                        note(&id, "catalog_unavailable", e.to_string());
                        continue;
                    }
                };
                if !matches!(r.state, SnapshotState::Active) {
                    note(&id, r.state.name(), "changed state while verifying".into());
                    continue;
                }
                r.restores += 1;
                if let Err(e) = self.store.write_record(&r) {
                    note(&id, "catalog_unavailable", e.to_string());
                    continue;
                }
                r.restores
            };
            return Ok(RestorePlan {
                manifest_digest: sealed.digest.to_string(),
                manifest,
                dir,
                generation,
                verify_ms,
            });
        }
        Err(RestoreUnavailable::new(
            first_code.unwrap_or_else(|| "no_snapshot".into()),
            format!("no usable snapshot: {}", refusals.join("; ")),
        ))
    }

    /// Verify the plaintext files against the manifest, decrypting a missing
    /// one from the sealed store first. `Err` names the failing artifact.
    async fn verify_files(
        &self,
        manifest: &SnapshotManifest,
        dir: &std::path::Path,
    ) -> Result<(), String> {
        let store = self.store.clone();
        let key = self.key.clone();
        let manifest = manifest.clone();
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            std::fs::create_dir_all(&dir).map_err(|e| format!("snapshot dir: {e}"))?;
            let a = &manifest.artifacts;
            for (name, expected) in [
                (files::MEMORY, &a.memory),
                (files::VMSTATE, &a.vmstate),
                (files::SCRATCH, &a.scratch),
                (files::FUNCTION_DRIVE, &a.function_drive),
            ] {
                let path = dir.join(name);
                if !path.exists() {
                    store
                        .open_artifact(
                            &key,
                            &manifest.tenant_id,
                            &manifest.snapshot_id,
                            name,
                            &path,
                            expected,
                        )
                        .map_err(|e| {
                            format!("artifact {name} cannot be restored from the sealed store: {e}")
                        })?;
                    continue;
                }
                let got = digest_file(&path).map_err(|e| format!("artifact {name}: {e}"))?;
                if &got != expected {
                    return Err(format!(
                        "artifact {name} digest mismatch: {} ({} bytes), manifest {} ({} bytes)",
                        got.sha256, got.size_bytes, expected.sha256, expected.size_bytes
                    ));
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| format!("verify task: {e}"))?
    }
}

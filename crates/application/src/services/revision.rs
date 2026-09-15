//! Revision creation and background validation.
//!
//! `create` stores the revision as `Pending` and spawns a task that walks
//! `Preparing -> Validating -> Ready` (artifact must exist in the store and
//! pass `provider.validate_artifact`) or ends in `Failed{reason}`. When the
//! request asked for `publish_to_prod`, the `prod` alias is pointed at the
//! revision once it is Ready.

use std::sync::Arc;
use std::time::Duration;

use tachyon_serverless_api_types::{ArtifactRequest, CreateRevisionRequest};
use tachyon_serverless_domain::{
    AliasName, Architecture, ArtifactRef, Clock, EgressProfile, ExecutionPolicy, Function,
    FunctionId, FunctionRevision, IdGenerator, Limits, RUNTIME_PROTOCOL_V1, ResourceProfile,
    RevisionId, RevisionSpec, RevisionStatus, RuntimeSpec, SecretBinding, Sha256Digest, TenantId,
};
use tachyon_serverless_provider_port::{
    ArtifactLocation, ArtifactStore, ExecutionProvider, Principal,
};

use crate::authz::{ensure_tenant, require_deploy, require_read};
use crate::error::AppError;
use crate::repository::Repositories;
use crate::services::alias::AliasService;
use crate::services::artifact::owned_artifact;

pub struct RevisionService {
    repos: Repositories,
    artifacts: Arc<dyn ArtifactStore>,
    provider: Arc<dyn ExecutionProvider>,
    aliases: Arc<AliasService>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    limits: Limits,
}

impl RevisionService {
    pub fn new(
        repos: Repositories,
        artifacts: Arc<dyn ArtifactStore>,
        provider: Arc<dyn ExecutionProvider>,
        aliases: Arc<AliasService>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
        limits: Limits,
    ) -> Self {
        Self {
            repos,
            artifacts,
            provider,
            aliases,
            clock,
            ids,
            limits,
        }
    }

    /// Build a domain spec from the API request for a function owned by
    /// `tenant_id`. Binary artifacts are looked up in the store to learn
    /// their size. A missing artifact, and an artifact `tenant_id` never
    /// uploaded, both yield size 0 and fail validation later with the same
    /// reason (docs/threat-model.md §14-1).
    pub async fn spec_from_request(
        &self,
        tenant_id: &TenantId,
        req: &CreateRevisionRequest,
    ) -> Result<RevisionSpec, AppError> {
        let artifact = match &req.artifact {
            ArtifactRequest::Binary { digest } => {
                let digest = Sha256Digest::parse(digest)?;
                let size_bytes =
                    match owned_artifact(&self.repos, self.artifacts.as_ref(), tenant_id, &digest)
                        .await
                    {
                        Ok(stored) => stored.size_bytes,
                        Err(tachyon_serverless_provider_port::ArtifactError::NotFound(_)) => 0,
                        Err(e) => return Err(e.into()),
                    };
                ArtifactRef::Binary { digest, size_bytes }
            }
            ArtifactRequest::OciImage { reference } => {
                let digest_part = reference.rsplit_once('@').map(|(_, d)| d).ok_or_else(|| {
                    AppError::InvalidRequest(
                        "oci reference must be pinned: registry/repo@sha256:<hex>".into(),
                    )
                })?;
                ArtifactRef::OciImage {
                    reference: reference.clone(),
                    digest: Sha256Digest::parse(digest_part)?,
                }
            }
        };
        let architecture = match req.architecture.as_str() {
            "x86_64" => Architecture::X86_64,
            "aarch64" => Architecture::Aarch64,
            other => {
                return Err(AppError::InvalidRequest(format!(
                    "unsupported architecture `{other}` (x86_64 | aarch64)"
                )));
            }
        };
        let egress = match req.egress.as_deref() {
            None | Some("none") => EgressProfile::None,
            Some("restricted") => EgressProfile::Restricted,
            Some("public-web") => EgressProfile::PublicWeb,
            Some(other) => {
                return Err(AppError::InvalidRequest(format!(
                    "unsupported egress `{other}` (none | restricted | public-web)"
                )));
            }
        };
        Ok(RevisionSpec {
            artifact,
            runtime: RuntimeSpec {
                protocol: RUNTIME_PROTOCOL_V1.to_string(),
                architecture,
            },
            resources: ResourceProfile {
                memory_mib: req.resources.memory_mib,
                cpu_millis: req.resources.cpu_millis,
                ephemeral_storage_mib: req.resources.ephemeral_storage_mib,
            },
            execution: ExecutionPolicy {
                timeout_seconds: req.execution.timeout_seconds,
                initialization_timeout_seconds: req.execution.initialization_timeout_seconds,
                concurrency_per_environment: 1,
                max_concurrency: req.execution.max_concurrency,
                min_ready: 0,
            },
            egress,
            env_vars: req.env_vars.clone(),
            secrets: req
                .secrets
                .iter()
                .map(|s| SecretBinding {
                    env_name: s.env_name.clone(),
                    binding_ref: s.binding_ref.clone(),
                })
                .collect(),
            description: req.description.clone(),
        })
    }

    /// Create a revision and start background validation.
    pub async fn create(
        self: &Arc<Self>,
        principal: &Principal,
        function_id: &FunctionId,
        req: &CreateRevisionRequest,
    ) -> Result<FunctionRevision, AppError> {
        require_deploy(principal)?;
        let function = self.owned_function(principal, function_id)?;
        if function.is_deleted() {
            return Err(AppError::FunctionDeleted(format!(
                "function {} is deleted",
                function.id
            )));
        }
        let spec = self.spec_from_request(&function.tenant_id, req).await?;
        let number = self.repos.revisions.allocate_number(&function.id)?;
        let revision = FunctionRevision::new(
            RevisionId::from_ulid(self.ids.next_ulid()),
            function.id.clone(),
            function.tenant_id.clone(),
            number,
            spec,
            &self.limits,
            self.clock.now(),
        )?;
        self.repos.revisions.insert(revision.clone())?;
        let this = Arc::clone(self);
        let id = revision.id.clone();
        let publish = req.publish_to_prod;
        tokio::spawn(async move {
            this.validate(&id, publish).await;
        });
        Ok(revision)
    }

    pub fn get(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        revision_id: &RevisionId,
    ) -> Result<FunctionRevision, AppError> {
        require_read(principal)?;
        self.owned_function(principal, function_id)?;
        self.repos
            .revisions
            .get(revision_id)?
            .filter(|r| &r.function_id == function_id && r.tenant_id == principal.tenant_id)
            .ok_or_else(|| AppError::not_found("revision not found"))
    }

    pub fn list(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Vec<FunctionRevision>, AppError> {
        require_read(principal)?;
        self.owned_function(principal, function_id)?;
        Ok(self.repos.revisions.list_by_function(function_id)?)
    }

    /// Poll until the revision reaches a terminal status or `timeout` elapses.
    pub async fn wait_terminal(
        &self,
        revision_id: &RevisionId,
        timeout: Duration,
    ) -> Result<FunctionRevision, AppError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let rev = self
                .repos
                .revisions
                .get(revision_id)?
                .ok_or_else(|| AppError::not_found("revision not found"))?;
            if rev.status.is_terminal() {
                return Ok(rev);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(rev);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn validate(&self, revision_id: &RevisionId, publish_to_prod: bool) {
        let outcome = self.validate_inner(revision_id).await;
        let now = self.clock.now();
        let Ok(Some(mut rev)) = self.repos.revisions.get(revision_id) else {
            return;
        };
        match outcome {
            Ok(()) => {
                if rev.mark_ready(now).is_ok() && self.repos.revisions.update(rev.clone()).is_ok() {
                    tracing::info!(revision_id = %rev.id, function_id = %rev.function_id, "revision ready");
                    if publish_to_prod {
                        match self.repos.functions.get(&rev.function_id) {
                            Ok(Some(function)) => {
                                if let Err(e) = self.aliases.publish(
                                    &function,
                                    &AliasName::default_alias(),
                                    &rev.id,
                                ) {
                                    tracing::warn!(error = %e, revision_id = %rev.id, "publish to prod failed");
                                }
                            }
                            _ => {
                                tracing::warn!(revision_id = %rev.id, "function vanished before publish")
                            }
                        }
                    }
                }
            }
            Err(reason) => {
                tracing::warn!(revision_id = %rev.id, %reason, "revision validation failed");
                if rev.mark_failed(reason, now).is_ok() {
                    let _ = self.repos.revisions.update(rev);
                }
            }
        }
    }

    async fn validate_inner(&self, revision_id: &RevisionId) -> Result<(), String> {
        let now = self.clock.now();
        let mut rev = self
            .repos
            .revisions
            .get(revision_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "revision vanished".to_string())?;
        rev.start_preparing(now).map_err(|e| e.to_string())?;
        self.repos
            .revisions
            .update(rev.clone())
            .map_err(|e| e.to_string())?;

        let location = match &rev.spec.artifact {
            ArtifactRef::Binary { digest, size_bytes } => {
                // A digest the revision's tenant never uploaded fails with the
                // exact reason of a digest that does not exist.
                let stored =
                    owned_artifact(&self.repos, self.artifacts.as_ref(), &rev.tenant_id, digest)
                        .await
                        .map_err(|e| format!("artifact unavailable: {e}"))?;
                if stored.size_bytes != *size_bytes {
                    return Err(format!(
                        "artifact size changed: spec says {} bytes, store has {}",
                        size_bytes, stored.size_bytes
                    ));
                }
                ArtifactLocation {
                    path: stored.path,
                    digest: stored.digest,
                    size_bytes: stored.size_bytes,
                }
            }
            ArtifactRef::OciImage { reference, .. } => {
                return Err(format!(
                    "oci image `{reference}` is not executable by prototype providers (upload a binary artifact)"
                ));
            }
        };

        rev.start_validating(self.clock.now())
            .map_err(|e| e.to_string())?;
        self.repos
            .revisions
            .update(rev.clone())
            .map_err(|e| e.to_string())?;
        self.provider
            .validate_artifact(&location, rev.spec.runtime.architecture)
            .await
            .map_err(|e| format!("provider rejected artifact: {e}"))?;
        Ok(())
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
}

/// Convenience for callers that only need to know whether a revision can be
/// invoked.
pub fn ensure_ready(revision: &FunctionRevision) -> Result<(), AppError> {
    match &revision.status {
        RevisionStatus::Ready => Ok(()),
        other => Err(AppError::RevisionNotReady(format!(
            "revision {} is {}",
            revision.id,
            other.name()
        ))),
    }
}

//! Artifact upload with tenant ownership (docs/threat-model.md §14-1).
//!
//! The artifact store is content-addressed and tenant-blind. Ownership is
//! recorded in the repository when a tenant uploads bytes, and every lookup
//! of a digest on behalf of a tenant goes through [`owned_artifact`], which
//! reports a digest the tenant never uploaded exactly like a digest that does
//! not exist (no existence oracle).

use std::sync::Arc;

use tachyon_serverless_domain::{Sha256Digest, TenantId};
use tachyon_serverless_provider_port::{ArtifactError, ArtifactStore, Principal, StoredArtifact};

use crate::authz::require_deploy;
use crate::error::AppError;
use crate::repository::Repositories;

pub struct ArtifactService {
    repos: Repositories,
    artifacts: Arc<dyn ArtifactStore>,
}

impl ArtifactService {
    pub fn new(repos: Repositories, artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self { repos, artifacts }
    }

    /// Store `bytes` and record that the caller's tenant owns the digest.
    /// Uploading identical bytes again (by the same or another tenant) is
    /// idempotent for the store and adds an ownership row for the caller.
    pub async fn upload(
        &self,
        principal: &Principal,
        bytes: &[u8],
    ) -> Result<StoredArtifact, AppError> {
        require_deploy(principal)?;
        if bytes.is_empty() {
            return Err(AppError::InvalidRequest("artifact body is empty".into()));
        }
        let stored = self.artifacts.put(bytes).await?;
        self.repos
            .artifact_owners
            .claim(&principal.tenant_id, &stored.digest)?;
        Ok(stored)
    }
}

/// Resolve `digest` on behalf of `tenant`. Ownership is checked before the
/// store is touched, so a foreign digest can never surface a store-specific
/// error (e.g. a corrupt file) that a missing digest would not.
pub async fn owned_artifact(
    repos: &Repositories,
    artifacts: &dyn ArtifactStore,
    tenant: &TenantId,
    digest: &Sha256Digest,
) -> Result<StoredArtifact, ArtifactError> {
    let owned = repos
        .artifact_owners
        .is_owned_by(tenant, digest)
        .map_err(|e| ArtifactError::Io(std::io::Error::other(e.to_string())))?;
    if !owned {
        return Err(ArtifactError::NotFound(digest.clone()));
    }
    artifacts.get(digest).await
}

//! Secret binding resolution. Values are only ever handed to an environment
//! that has been assigned; they never enter the ledger or logs.

use async_trait::async_trait;
use tachyon_serverless_domain::{EnvironmentId, RevisionId, TenantId};

/// A resolved secret value. Debug output is redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    /// Expose the value. Call sites must be the ones that hand it to the guest.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("binding not found: {0}")]
    NotFound(String),
    #[error("binding `{binding}` is not accessible to tenant {tenant}")]
    Forbidden { binding: String, tenant: TenantId },
    #[error("secret backend error: {0}")]
    Backend(String),
}

/// Context proving which environment the secret is being delivered to.
#[derive(Debug, Clone)]
pub struct SecretDeliveryContext {
    pub tenant_id: TenantId,
    pub revision_id: RevisionId,
    pub environment_id: EnvironmentId,
    pub epoch: u64,
}

#[async_trait]
pub trait SecretProvider: Send + Sync {
    async fn resolve(
        &self,
        ctx: &SecretDeliveryContext,
        binding_ref: &str,
    ) -> Result<SecretValue, SecretError>;
}

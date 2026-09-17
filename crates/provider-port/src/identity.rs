//! Identity: who is calling, which tenant, which roles.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tachyon_serverless_domain::TenantId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Create functions, publish revisions, update aliases.
    Deploy,
    /// Invoke functions and read invocation results / logs.
    Invoke,
    /// Platform operator: provider status plus own-tenant function, revision
    /// and alias metadata, read-only (docs/threat-model.md section 7).
    Operator,
    /// Re-submit dead-lettered asynchronous invocations of the tenant
    /// (PLT-4640). Only together with `Invoke`: a redrive is a new invocation.
    Redrive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub subject: String,
    pub tenant_id: TenantId,
    pub roles: Vec<Role>,
}

impl Principal {
    pub fn has(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }
}

/// Opaque bearer credential. Debug output is redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential(pub String);

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential(<redacted>)")
    }
}

#[async_trait]
pub trait IdentityProvider: Send + Sync {
    /// Resolve a credential to a principal. `None` means unauthenticated.
    async fn authenticate(&self, credential: &Credential) -> Option<Principal>;
}

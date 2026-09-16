//! Function: the tenant-owned logical unit.

use serde::{Deserialize, Serialize};

use crate::clock::Timestamp;
use crate::error::DomainError;
use crate::ids::{FunctionId, FunctionName, TenantId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Function {
    pub id: FunctionId,
    pub tenant_id: TenantId,
    pub name: FunctionName,
    pub description: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// Set when deletion has started. A deleted function accepts no new invocations.
    pub deleted_at: Option<Timestamp>,
}

impl Function {
    pub const MAX_DESCRIPTION_LEN: usize = 1024;

    pub fn new(
        id: FunctionId,
        tenant_id: TenantId,
        name: FunctionName,
        description: String,
        now: Timestamp,
    ) -> Result<Self, DomainError> {
        if description.len() > Self::MAX_DESCRIPTION_LEN {
            return Err(DomainError::LimitExceeded {
                field: "description",
                actual: description.len() as u64,
                max: Self::MAX_DESCRIPTION_LEN as u64,
            });
        }
        Ok(Self {
            id,
            tenant_id,
            name,
            description,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        })
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    pub fn mark_deleted(&mut self, now: Timestamp) -> Result<(), DomainError> {
        if self.deleted_at.is_some() {
            return Err(DomainError::Terminal {
                entity: "Function",
                state: "deleted".into(),
            });
        }
        self.deleted_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Guard used by every use case: the caller's tenant must own the function.
    pub fn ensure_owned_by(&self, tenant: &TenantId) -> Result<(), DomainError> {
        if &self.tenant_id == tenant {
            Ok(())
        } else {
            Err(DomainError::TenantMismatch(format!(
                "function {} is not owned by tenant {}",
                self.id, tenant
            )))
        }
    }
}

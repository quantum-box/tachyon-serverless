//! Role and tenant checks.
//!
//! - `Deploy` is required for mutations (functions, revisions, aliases).
//! - `Invoke` is required for invoking and for reading invocations / logs.
//! - Resources of another tenant answer `NotFound`, never `Forbidden`, so
//!   that existence is not leaked (docs/architecture.md §3 step 1).

use tachyon_serverless_domain::TenantId;
use tachyon_serverless_provider_port::{Principal, Role};

use crate::error::AppError;

/// Ensure the principal holds `role`.
pub fn require_role(principal: &Principal, role: Role) -> Result<(), AppError> {
    if principal.has(role) {
        Ok(())
    } else {
        Err(AppError::Forbidden(format!(
            "role `{}` is required",
            role_name(role)
        )))
    }
}

/// Mutation guard: `Deploy` role.
pub fn require_deploy(principal: &Principal) -> Result<(), AppError> {
    require_role(principal, Role::Deploy)
}

/// Invoke / read guard: `Invoke` role.
pub fn require_invoke(principal: &Principal) -> Result<(), AppError> {
    require_role(principal, Role::Invoke)
}

/// Read guard: either `Deploy` or `Invoke` may read management resources.
pub fn require_read(principal: &Principal) -> Result<(), AppError> {
    if principal.has(Role::Deploy) || principal.has(Role::Invoke) || principal.has(Role::Operator) {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "a deploy or invoke role is required".into(),
        ))
    }
}

/// Tenant scoping: a resource owned by a different tenant does not exist
/// from the caller's point of view.
pub fn ensure_tenant(principal: &Principal, owner: &TenantId, what: &str) -> Result<(), AppError> {
    if &principal.tenant_id == owner {
        Ok(())
    } else {
        Err(AppError::NotFound(format!("{what} not found")))
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Deploy => "deploy",
        Role::Invoke => "invoke",
        Role::Operator => "operator",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(roles: Vec<Role>) -> Principal {
        Principal {
            subject: "s".into(),
            tenant_id: TenantId::generate(),
            roles,
        }
    }

    #[test]
    fn roles() {
        assert!(require_deploy(&principal(vec![Role::Deploy])).is_ok());
        assert!(require_deploy(&principal(vec![Role::Invoke])).is_err());
        assert!(require_invoke(&principal(vec![Role::Invoke])).is_ok());
        assert!(require_read(&principal(vec![Role::Operator])).is_ok());
        assert!(require_read(&principal(vec![])).is_err());
    }

    #[test]
    fn foreign_tenant_is_not_found() {
        let p = principal(vec![Role::Invoke]);
        let other = TenantId::generate();
        let err = ensure_tenant(&p, &other, "function").unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
        assert!(ensure_tenant(&p, &p.tenant_id, "function").is_ok());
    }
}

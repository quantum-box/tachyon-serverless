//! Application error type and its mapping onto the API error codes.

use tachyon_serverless_api_types::{ApiError, ApiErrorBody, ErrorCode};
use tachyon_serverless_domain::{DomainError, ErrorClass, InvocationError, InvocationId};
use tachyon_serverless_provider_port::{ArtifactError, ProviderError, SecretError};

use crate::repository::RepoError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: u64, max: u64 },
    #[error("capacity exceeded: {0}")]
    CapacityExceeded(String),
    #[error("revision not ready: {0}")]
    RevisionNotReady(String),
    #[error("function deleted: {0}")]
    FunctionDeleted(String),
    /// An invocation ran (or was accepted) and ended in a non-success state.
    /// Carries the id so callers can fetch history and logs.
    #[error("invocation {invocation_id} failed: {}: {}", error.error_type, error.message)]
    Invocation {
        invocation_id: InvocationId,
        error: InvocationError,
    },
    #[error("provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("platform error: {0}")]
    Platform(String),
}

impl AppError {
    pub fn platform(msg: impl Into<String>) -> Self {
        Self::Platform(msg.into())
    }

    pub fn not_found(what: impl Into<String>) -> Self {
        Self::NotFound(what.into())
    }

    /// Stable machine-readable code for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Unauthorized(_) => ErrorCode::Unauthorized,
            Self::Forbidden(_) => ErrorCode::Forbidden,
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Conflict(_) => ErrorCode::Conflict,
            Self::InvalidRequest(_) => ErrorCode::InvalidRequest,
            Self::PayloadTooLarge { .. } => ErrorCode::PayloadTooLarge,
            Self::CapacityExceeded(_) => ErrorCode::CapacityExceeded,
            Self::RevisionNotReady(_) => ErrorCode::RevisionNotReady,
            Self::FunctionDeleted(_) => ErrorCode::FunctionDeleted,
            Self::Invocation { error, .. } => error_code_for_class(&error.class),
            Self::ProviderUnavailable(_) => ErrorCode::ProviderUnavailable,
            Self::Platform(_) => ErrorCode::PlatformError,
        }
    }

    pub fn http_status(&self) -> u16 {
        self.code().http_status()
    }

    /// Render as the API error body. `request_id` is echoed when present.
    pub fn to_api_body(&self, request_id: Option<String>) -> ApiErrorBody {
        let (invocation_id, error_type) = match self {
            Self::Invocation {
                invocation_id,
                error,
            } => (
                Some(invocation_id.to_string()),
                Some(error.error_type.clone()),
            ),
            _ => (None, None),
        };
        ApiErrorBody {
            error: ApiError {
                code: self.code(),
                message: self.to_string(),
                request_id,
                invocation_id,
                error_type,
            },
        }
    }
}

/// Map a domain error class onto the API code.
pub fn error_code_for_class(class: &ErrorClass) -> ErrorCode {
    match class {
        ErrorClass::UserError => ErrorCode::UserError,
        ErrorClass::Crash => ErrorCode::Crash,
        ErrorClass::InitError => ErrorCode::InitError,
        ErrorClass::Timeout => ErrorCode::Timeout,
        ErrorClass::QueueTimeout => ErrorCode::QueueTimeout,
        ErrorClass::PlatformError => ErrorCode::PlatformError,
        ErrorClass::Cancelled => ErrorCode::Cancelled,
        ErrorClass::OutcomeUnknown => ErrorCode::OutcomeUnknown,
    }
}

impl From<DomainError> for AppError {
    fn from(e: DomainError) -> Self {
        match e {
            DomainError::TenantMismatch(_) => Self::NotFound("resource not found".into()),
            DomainError::GenerationMismatch { .. } => Self::Conflict(e.to_string()),
            DomainError::Terminal { .. } | DomainError::IllegalTransition { .. } => {
                Self::Conflict(e.to_string())
            }
            DomainError::InvalidId { .. }
            | DomainError::InvalidName { .. }
            | DomainError::InvalidDigest(_)
            | DomainError::Validation { .. }
            | DomainError::LimitExceeded { .. } => Self::InvalidRequest(e.to_string()),
        }
    }
}

impl From<RepoError> for AppError {
    fn from(e: RepoError) -> Self {
        match e {
            RepoError::NotFound(what) => Self::NotFound(what),
            RepoError::Conflict(what) => Self::Conflict(what),
            RepoError::Refused(what) => Self::Conflict(what),
            RepoError::Store(msg) => Self::Platform(format!("storage: {msg}")),
            RepoError::Io(err) => Self::Platform(format!("storage: {err}")),
            RepoError::Serialization(msg) => Self::Platform(format!("storage: {msg}")),
        }
    }
}

impl From<ArtifactError> for AppError {
    fn from(e: ArtifactError) -> Self {
        match e {
            ArtifactError::TooLarge { size, max } => Self::PayloadTooLarge { size, max },
            ArtifactError::NotFound(d) => Self::NotFound(format!("artifact {d} not found")),
            ArtifactError::Io(err) => Self::Platform(format!("artifact store: {err}")),
        }
    }
}

impl From<SecretError> for AppError {
    fn from(e: SecretError) -> Self {
        // A binding that exists only for another tenant must be
        // indistinguishable from one that does not exist at all.
        match e {
            SecretError::NotFound(binding) | SecretError::Forbidden { binding, .. } => {
                Self::InvalidRequest(format!(
                    "secret binding `{binding}` is not available to this tenant"
                ))
            }
            SecretError::Backend(msg) => Self::Platform(format!("secret backend: {msg}")),
        }
    }
}

impl From<ProviderError> for AppError {
    fn from(e: ProviderError) -> Self {
        match e {
            ProviderError::Unavailable(msg) => Self::ProviderUnavailable(msg),
            other => Self::Platform(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_and_statuses() {
        assert_eq!(AppError::NotFound("x".into()).http_status(), 404);
        assert_eq!(AppError::CapacityExceeded("x".into()).http_status(), 429);
        let e = AppError::Invocation {
            invocation_id: InvocationId::generate(),
            error: InvocationError::new(ErrorClass::Timeout, "Host.Timeout", "late"),
        };
        assert_eq!(e.http_status(), 504);
        let body = e.to_api_body(Some("req".into()));
        assert_eq!(body.error.code, ErrorCode::Timeout);
        assert!(body.error.invocation_id.is_some());
        assert_eq!(body.error.error_type.as_deref(), Some("Host.Timeout"));
        assert_eq!(body.error.request_id.as_deref(), Some("req"));
    }

    #[test]
    fn foreign_and_missing_secret_bindings_are_indistinguishable() {
        let missing: AppError = SecretError::NotFound("db".into()).into();
        let foreign: AppError = SecretError::Forbidden {
            binding: "db".into(),
            tenant: tachyon_serverless_domain::TenantId::generate(),
        }
        .into();
        assert_eq!(missing.code(), foreign.code());
        assert_eq!(missing.to_string(), foreign.to_string());
        assert!(!foreign.to_string().contains("tn_"));
    }

    #[test]
    fn tenant_mismatch_is_not_found() {
        let e: AppError = DomainError::TenantMismatch("x".into()).into();
        assert_eq!(e.code(), ErrorCode::NotFound);
    }
}

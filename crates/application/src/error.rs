//! Application error type and its mapping onto the API error codes.

use tachyon_serverless_api_types::{ApiError, ApiErrorBody, ErrorCode};
use tachyon_serverless_domain::{DomainError, ErrorClass, InvocationError, InvocationId};
use tachyon_serverless_provider_port::{ArtifactError, ProviderError, SecretError};

use crate::control::ControlError;
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
    /// The `Idempotency-Key` is bound to an invocation whose input differs
    /// (409). Carries the bound invocation so the caller can look it up; the
    /// key is scoped to the caller's own tenant and function.
    #[error(
        "conflict: idempotency key `{key}` is bound to invocation {invocation_id} with a different input"
    )]
    IdempotencyConflict {
        key: String,
        invocation_id: InvocationId,
    },
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
    /// Refused because of the control plane, the configuration cache or the
    /// store (PLT-4636). `kind` gives the `error_type`.
    #[error("{}: {message}", kind.error_type())]
    Control { kind: ControlError, message: String },
}

impl AppError {
    pub fn platform(msg: impl Into<String>) -> Self {
        Self::Platform(msg.into())
    }

    pub fn not_found(what: impl Into<String>) -> Self {
        Self::NotFound(what.into())
    }

    pub fn control(kind: ControlError, message: impl Into<String>) -> Self {
        Self::Control {
            kind,
            message: message.into(),
        }
    }

    /// Stable machine-readable code for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Unauthorized(_) => ErrorCode::Unauthorized,
            Self::Forbidden(_) => ErrorCode::Forbidden,
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Conflict(_) | Self::IdempotencyConflict { .. } => ErrorCode::Conflict,
            Self::InvalidRequest(_) => ErrorCode::InvalidRequest,
            Self::PayloadTooLarge { .. } => ErrorCode::PayloadTooLarge,
            Self::CapacityExceeded(_) => ErrorCode::CapacityExceeded,
            Self::RevisionNotReady(_) => ErrorCode::RevisionNotReady,
            Self::FunctionDeleted(_) => ErrorCode::FunctionDeleted,
            // A cold start refused after acceptance is recorded as a platform
            // error, but answers with the code of its refusal (503).
            Self::Invocation { error, .. } => ControlError::from_error_type(&error.error_type)
                .map_or_else(|| error_code_for_class(&error.class), |k| k.code()),
            Self::ProviderUnavailable(_) => ErrorCode::ProviderUnavailable,
            Self::Platform(_) => ErrorCode::PlatformError,
            Self::Control { kind, .. } => kind.code(),
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
            Self::IdempotencyConflict { invocation_id, .. } => (
                Some(invocation_id.to_string()),
                Some(IDEMPOTENCY_KEY_REUSED.to_string()),
            ),
            Self::Control { kind, .. } => (None, Some(kind.error_type().to_string())),
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

/// `error.error_type` of the 409 for an `Idempotency-Key` reused with another
/// input.
pub const IDEMPOTENCY_KEY_REUSED: &str = "Host.IdempotencyKeyReused";

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
            // The store did not answer: retryable, 503 (PLT-4636).
            RepoError::Store(msg) => {
                Self::control(ControlError::StoreUnavailable, format!("storage: {msg}"))
            }
            RepoError::Io(err) => {
                Self::control(ControlError::StoreUnavailable, format!("storage: {err}"))
            }
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

    /// PLT-4636: every control refusal has its own error_type, and a cold
    /// start refused after acceptance answers with the refusal's code.
    #[test]
    fn control_refusals_are_distinct_and_retryable_ones_are_503() {
        let mut types = std::collections::BTreeSet::new();
        for kind in ControlError::ALL {
            assert!(types.insert(kind.error_type()));
            assert_eq!(ControlError::from_error_type(kind.error_type()), Some(kind));
            let e = AppError::control(kind, "x");
            let body = e.to_api_body(None);
            assert_eq!(body.error.error_type.as_deref(), Some(kind.error_type()));
        }
        for kind in [
            ControlError::ConfigNotDelivered,
            ControlError::ConfigExpired,
            ControlError::AuthLeaseExpired,
            ControlError::ColdStartRestricted,
            ControlError::ProviderControlUnavailable,
            ControlError::ControlPlaneUnavailable,
            ControlError::StoreUnavailable,
        ] {
            assert_eq!(AppError::control(kind, "x").http_status(), 503, "{kind:?}");
        }
        assert_eq!(
            AppError::control(ControlError::UnknownTenant, "x").http_status(),
            403
        );
        let refused_cold = AppError::Invocation {
            invocation_id: InvocationId::generate(),
            error: InvocationError::new(
                ErrorClass::PlatformError,
                ControlError::ColdStartRestricted.error_type(),
                "outage",
            ),
        };
        assert_eq!(refused_cold.http_status(), 503);
        let store: AppError = RepoError::Store("database is locked".into()).into();
        assert_eq!(store.code(), ErrorCode::ControlPlaneUnavailable);
    }

    #[test]
    fn tenant_mismatch_is_not_found() {
        let e: AppError = DomainError::TenantMismatch("x".into()).into();
        assert_eq!(e.code(), ErrorCode::NotFound);
    }
}

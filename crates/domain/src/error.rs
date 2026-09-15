//! Domain error taxonomy.

use thiserror::Error;

/// Errors produced by domain validation and state transitions.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("invalid identifier for prefix `{expected_prefix}`: `{value}`")]
    InvalidId {
        expected_prefix: String,
        value: String,
    },
    #[error("invalid {kind} name: `{value}`")]
    InvalidName { kind: &'static str, value: String },
    #[error("invalid digest: `{0}`")]
    InvalidDigest(String),
    #[error("invalid {field}: {reason}")]
    Validation { field: &'static str, reason: String },
    #[error("illegal state transition for {entity}: {from} -> {to}")]
    IllegalTransition {
        entity: &'static str,
        from: String,
        to: String,
    },
    #[error("{entity} is already in a terminal state ({state}); further updates are rejected")]
    Terminal { entity: &'static str, state: String },
    #[error("generation mismatch: expected {expected}, actual {actual}")]
    GenerationMismatch { expected: u64, actual: u64 },
    #[error("tenant mismatch: {0}")]
    TenantMismatch(String),
    #[error("limit exceeded: {field} = {actual} (max {max})")]
    LimitExceeded {
        field: &'static str,
        actual: u64,
        max: u64,
    },
}

impl DomainError {
    pub fn validation(field: &'static str, reason: impl Into<String>) -> Self {
        Self::Validation {
            field,
            reason: reason.into(),
        }
    }
}

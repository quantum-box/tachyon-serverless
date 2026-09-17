//! CLI error type and the exit-code contract documented in `docs/cli.md`.

use std::fmt;

use tachyon_serverless_api_types::{ApiError, ErrorCode};

/// Process exit codes. Stable: scripts (`scripts/e2e/demo.sh`) assert on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    /// Success.
    Ok = 0,
    /// Usage / configuration error (bad flags, missing token, unreadable file).
    Usage = 1,
    /// API, auth or validation error (4xx that is not an invocation outcome).
    Api = 2,
    /// Invocation failed in user code or during init (`user_error`, `crash`, `init_error`, `cancelled`).
    InvocationFailed = 3,
    /// `timeout` / `queue_timeout` (or a CLI-side wait timeout).
    Timeout = 4,
    /// `outcome_unknown`: the platform could not confirm the result.
    OutcomeUnknown = 5,
    /// Platform error, provider unavailable, unexpected 5xx or transport failure.
    Platform = 6,
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Map a stable API error code to an exit code.
    pub fn from_error_code(code: ErrorCode) -> Self {
        match code {
            ErrorCode::Unauthorized
            | ErrorCode::Forbidden
            | ErrorCode::NotFound
            | ErrorCode::Conflict
            | ErrorCode::InvalidRequest
            | ErrorCode::PayloadTooLarge
            | ErrorCode::CapacityExceeded
            | ErrorCode::RevisionNotReady
            | ErrorCode::FunctionDeleted => Self::Api,
            ErrorCode::UserError
            | ErrorCode::Crash
            | ErrorCode::InitError
            | ErrorCode::Cancelled => Self::InvocationFailed,
            ErrorCode::Timeout | ErrorCode::QueueTimeout => Self::Timeout,
            ErrorCode::OutcomeUnknown => Self::OutcomeUnknown,
            ErrorCode::PlatformError
            | ErrorCode::ProviderUnavailable
            | ErrorCode::ConfigUnavailable
            | ErrorCode::ControlPlaneUnavailable
            | ErrorCode::AsyncUnavailable => Self::Platform,
        }
    }

    /// Map a bare HTTP status (no parseable error body) to an exit code.
    pub fn from_http_status(status: u16) -> Self {
        if (400..500).contains(&status) {
            Self::Api
        } else {
            Self::Platform
        }
    }
}

#[derive(Debug)]
pub enum CliError {
    /// Bad arguments or local environment (exit 1).
    Usage(String),
    /// The gateway answered with a well-formed error body.
    Api {
        status: u16,
        error: Box<ApiError>,
        /// Raw body as received, for `--json` output.
        raw: String,
    },
    /// The gateway answered with an unexpected status and no parseable error body.
    Http { status: u16, body: String },
    /// Could not reach the gateway or the request failed on the wire.
    Transport(String),
    /// A CLI-side wait (deploy polling, gateway boot) elapsed.
    Timeout(String),
    /// An operation completed with a failure the CLI classified itself
    /// (e.g. a revision that became `failed`, a name that resolved to nothing).
    Failed {
        code: ExitCode,
        message: String,
        /// JSON printed under `--json`, when the failure has a natural JSON form.
        raw: Option<String>,
    },
}

impl CliError {
    pub fn usage(msg: impl Into<String>) -> Self {
        Self::Usage(msg.into())
    }

    pub fn failed(code: ExitCode, message: impl Into<String>, raw: Option<String>) -> Self {
        Self::Failed {
            code,
            message: message.into(),
            raw,
        }
    }

    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::Usage(_) => ExitCode::Usage,
            Self::Api { error, .. } => ExitCode::from_error_code(error.code),
            Self::Http { status, .. } => ExitCode::from_http_status(*status),
            Self::Transport(_) => ExitCode::Platform,
            Self::Timeout(_) => ExitCode::Timeout,
            Self::Failed { code, .. } => *code,
        }
    }

    /// The raw JSON body to print when `--json` is active, if any.
    pub fn raw_json(&self) -> Option<&str> {
        match self {
            Self::Api { raw, .. } => Some(raw.as_str()),
            Self::Failed { raw, .. } => raw.as_deref(),
            _ => None,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(m) => write!(f, "error: {m}"),
            Self::Api { status, error, .. } => {
                let code = serde_json::to_value(error.code)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_else(|| format!("{:?}", error.code));
                write!(f, "error: {code} (HTTP {status}): {}", error.message)?;
                if let Some(t) = &error.error_type {
                    write!(f, " [type={t}]")?;
                }
                if let Some(id) = &error.invocation_id {
                    write!(f, " [invocation={id}]")?;
                }
                if let Some(r) = &error.request_id {
                    write!(f, " [request={r}]")?;
                }
                Ok(())
            }
            Self::Http { status, body } => {
                let snippet: String = body.chars().take(200).collect();
                write!(f, "error: unexpected HTTP {status}: {snippet}")
            }
            Self::Transport(m) => write!(f, "error: cannot reach gateway: {m}"),
            Self::Timeout(m) => write!(f, "error: timed out: {m}"),
            Self::Failed { message, .. } => write!(f, "error: {message}"),
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        Self::Usage(e.to_string())
    }
}

impl From<serde_json::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        Self::Usage(format!("invalid JSON: {e}"))
    }
}

/// Stringify an [`ErrorCode`] the way the API does (`snake_case`).
pub fn error_code_str(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{code:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_follow_the_documented_table() {
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::NotFound),
            ExitCode::Api
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::Unauthorized),
            ExitCode::Api
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::UserError),
            ExitCode::InvocationFailed
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::Crash),
            ExitCode::InvocationFailed
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::InitError),
            ExitCode::InvocationFailed
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::Timeout),
            ExitCode::Timeout
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::QueueTimeout),
            ExitCode::Timeout
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::OutcomeUnknown),
            ExitCode::OutcomeUnknown
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::PlatformError),
            ExitCode::Platform
        );
        assert_eq!(
            ExitCode::from_error_code(ErrorCode::ProviderUnavailable),
            ExitCode::Platform
        );
        assert_eq!(ExitCode::from_http_status(418), ExitCode::Api);
        assert_eq!(ExitCode::from_http_status(503), ExitCode::Platform);
        assert_eq!(ExitCode::InvocationFailed.code(), 3);
        assert_eq!(ExitCode::Platform.code(), 6);
    }

    #[test]
    fn error_code_str_is_snake_case() {
        assert_eq!(error_code_str(ErrorCode::UserError), "user_error");
        assert_eq!(error_code_str(ErrorCode::QueueTimeout), "queue_timeout");
    }
}

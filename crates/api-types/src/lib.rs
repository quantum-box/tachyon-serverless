//! Management / invoke API contract (RFC §7).
//!
//! Paths (all under the gateway):
//!
//! ```text
//! GET    /healthz                                  liveness
//! GET    /readyz                                   readiness (provider preflight ok)
//! GET    /v1/provider                              provider kind + capabilities
//! POST   /v1/artifacts                             upload raw executable bytes -> digest
//! POST   /v1/functions                             create
//! GET    /v1/functions                             list (tenant scoped)
//! GET    /v1/functions/{function_id}               get
//! DELETE /v1/functions/{function_id}               delete (stops new invocations)
//! POST   /v1/functions/{function_id}/revisions     create revision (async validation)
//! GET    /v1/functions/{function_id}/revisions     list
//! GET    /v1/functions/{function_id}/revisions/{revision_id}
//! PUT    /v1/functions/{function_id}/aliases/{alias}   CAS update {revision_id, expected_generation}
//! GET    /v1/functions/{function_id}/aliases/{alias}
//! GET    /v1/functions/{function_id}/aliases
//! POST   /v1/functions/{function_id}:invoke        synchronous JSON invoke (also /invoke)
//! ANY    /v1/functions/{function_id}/http/{*path}  HTTP adapter: request wrapped as tachyon.http.v1
//! GET    /v1/functions/{function_id}/invocations   history
//! GET    /v1/invocations/{invocation_id}
//! POST   /v1/invocations/{invocation_id}:cancel    (also /cancel)
//! GET    /v1/invocations/{invocation_id}/logs
//! GET    /v1/functions/{function_id}/usage
//! GET    /openapi.json
//! ```
//!
//! Authentication: `Authorization: Bearer <token>`. The token resolves to a
//! tenant; an optional `X-Tachyon-Tenant-Id` header must match it. Resources of
//! other tenants answer 404, never 403, to avoid existence leaks.
//!
//! Every error response has the shape [`ApiErrorBody`].

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use tachyon_serverless_domain as domain;
use tachyon_serverless_provider_port as port;

pub mod headers {
    pub const TENANT_ID: &str = "x-tachyon-tenant-id";
    pub const IDEMPOTENCY_KEY: &str = "idempotency-key";
    /// Client-side overall deadline in milliseconds (relative). Capped by the revision timeout.
    pub const CLIENT_TIMEOUT_MS: &str = "x-tachyon-client-timeout-ms";
    pub const REQUEST_ID: &str = "x-request-id";
    pub const INVOCATION_ID: &str = "x-tachyon-invocation-id";
    pub const TRACE_ID: &str = "x-tachyon-trace-id";
}

pub type Timestamp = chrono::DateTime<chrono::Utc>;

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

/// Stable machine-readable error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    InvalidRequest,
    PayloadTooLarge,
    CapacityExceeded,
    RevisionNotReady,
    FunctionDeleted,
    /// Handler returned an error (user code).
    UserError,
    /// Process crashed / panicked.
    Crash,
    InitError,
    Timeout,
    QueueTimeout,
    Cancelled,
    OutcomeUnknown,
    PlatformError,
    ProviderUnavailable,
}

impl ErrorCode {
    pub fn http_status(&self) -> u16 {
        match self {
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Conflict => 409,
            Self::InvalidRequest => 400,
            Self::PayloadTooLarge => 413,
            Self::CapacityExceeded => 429,
            Self::RevisionNotReady | Self::FunctionDeleted => 409,
            Self::UserError | Self::Crash | Self::InitError => 502,
            Self::Timeout | Self::QueueTimeout => 504,
            Self::Cancelled => 499,
            Self::OutcomeUnknown => 502,
            Self::PlatformError => 500,
            Self::ProviderUnavailable => 503,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Present for invocation failures so callers can fetch history/logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorBody {
    pub error: ApiError,
}

// ---------------------------------------------------------------------------
// provider
// ---------------------------------------------------------------------------

/// Whether this gateway reuses (warms) execution environments, and why.
///
/// The two flags answer two different questions and neither implies the other
/// (PLT-4633 review F7):
///
/// - `enabled` is what this gateway *does*: both gates are open, so
///   environments really are pooled and reused.
/// - `verified` is a fact about the **provider**: it reports both
///   `idle_quiesce` and `idle_resume` as `supported`, which it may only do
///   once the pause/resume cycle has been measured on real hardware. It says
///   nothing about the `[pool]` configuration, so a gateway with reuse
///   switched off can still report `verified: true`.
///
/// They are separate on purpose: an operator may switch reuse on for an
/// `unverified` provider in order to take the measurement
/// (`[pool] allow_unverified_idle`), and such a run must never be presented as
/// a warm success. `enabled: true, verified: false` therefore means "reuse is
/// running so it can be measured", not "warm works here" (PLT-4633 acceptance
/// 4, docs/architecture.md §4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ReuseInfo {
    pub enabled: bool,
    /// The provider reports both idle capabilities as `supported`, i.e. they
    /// were measured. Independent of whether reuse is switched on.
    pub verified: bool,
    /// One sentence naming the gate that decided `enabled`. Always present, on
    /// the enabled and the disabled path alike.
    pub reason: String,
    /// `supported` | `unsupported` | `unverified`, as the provider reports it.
    pub idle_quiesce: String,
    pub idle_resume: String,
}

impl Default for ReuseInfo {
    fn default() -> Self {
        Self {
            enabled: false,
            verified: false,
            reason: "environment reuse is off".to_string(),
            idle_quiesce: "unknown".to_string(),
            idle_resume: "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProviderInfo {
    pub kind: String,
    pub dev_only: bool,
    pub isolation: String,
    #[schema(value_type = Object)]
    pub capabilities: serde_json::Value,
    #[schema(value_type = Object)]
    pub preflight: serde_json::Value,
    /// Warm reuse: on or off, measured or not, and why.
    #[serde(default)]
    pub reuse: ReuseInfo,
}

impl ProviderInfo {
    pub fn from_port(
        kind: &domain::ProviderKind,
        caps: &port::Capabilities,
        preflight: &port::PreflightReport,
        reuse: ReuseInfo,
    ) -> Self {
        Self {
            kind: kind.as_str().to_string(),
            dev_only: caps.dev_only,
            isolation: match caps.isolation {
                port::IsolationLevel::MicroVm => "micro_vm",
                port::IsolationLevel::Container => "container",
                port::IsolationLevel::Process => "process",
            }
            .to_string(),
            capabilities: serde_json::to_value(caps).unwrap_or_default(),
            preflight: serde_json::to_value(preflight).unwrap_or_default(),
            reuse,
        }
    }
}

// ---------------------------------------------------------------------------
// artifacts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ArtifactUploadResponse {
    pub digest: String,
    pub size_bytes: u64,
}

// ---------------------------------------------------------------------------
// functions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateFunctionRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FunctionResponse {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub description: String,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: Timestamp,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<Timestamp>,
}

impl From<&domain::Function> for FunctionResponse {
    fn from(f: &domain::Function) -> Self {
        Self {
            id: f.id.to_string(),
            tenant_id: f.tenant_id.to_string(),
            name: f.name.to_string(),
            description: f.description.clone(),
            created_at: f.created_at,
            updated_at: f.updated_at,
            deleted_at: f.deleted_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// ---------------------------------------------------------------------------
// revisions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactRequest {
    /// Digest returned by `POST /v1/artifacts`.
    Binary { digest: String },
    /// OCI reference pinned to a digest. Accepted but not executable by prototype providers.
    OciImage { reference: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResourcesRequest {
    #[serde(default = "default_memory")]
    pub memory_mib: u32,
    #[serde(default = "default_cpu")]
    pub cpu_millis: u32,
    #[serde(default = "default_ephemeral")]
    pub ephemeral_storage_mib: u32,
}
fn default_memory() -> u32 {
    256
}
fn default_cpu() -> u32 {
    500
}
fn default_ephemeral() -> u32 {
    256
}
impl Default for ResourcesRequest {
    fn default() -> Self {
        Self {
            memory_mib: default_memory(),
            cpu_millis: default_cpu(),
            ephemeral_storage_mib: default_ephemeral(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionRequest {
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u32,
    #[serde(default = "default_init_timeout")]
    pub initialization_timeout_seconds: u32,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
}
fn default_timeout() -> u32 {
    30
}
fn default_init_timeout() -> u32 {
    30
}
fn default_max_concurrency() -> u32 {
    4
}
impl Default for ExecutionRequest {
    fn default() -> Self {
        Self {
            timeout_seconds: default_timeout(),
            initialization_timeout_seconds: default_init_timeout(),
            max_concurrency: default_max_concurrency(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SecretBindingRequest {
    pub env_name: String,
    pub binding_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateRevisionRequest {
    pub artifact: ArtifactRequest,
    /// `x86_64` or `aarch64`.
    pub architecture: String,
    #[serde(default)]
    pub resources: ResourcesRequest,
    #[serde(default)]
    pub execution: ExecutionRequest,
    /// `none` (default), `restricted`, `public-web`.
    #[serde(default)]
    pub egress: Option<String>,
    #[serde(default)]
    pub env_vars: Vec<(String, String)>,
    #[serde(default)]
    pub secrets: Vec<SecretBindingRequest>,
    #[serde(default)]
    pub description: String,
    /// When true (default), also point alias `prod` at the revision once Ready.
    #[serde(default = "default_true")]
    pub publish_to_prod: bool,
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RevisionResponse {
    pub id: String,
    pub function_id: String,
    pub number: u64,
    /// `pending` | `preparing` | `validating` | `ready` | `failed`
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[schema(value_type = Object)]
    pub spec: serde_json::Value,
    pub spec_digest: String,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: Timestamp,
}

impl From<&domain::FunctionRevision> for RevisionResponse {
    fn from(r: &domain::FunctionRevision) -> Self {
        let (status, failure_reason) = match &r.status {
            domain::RevisionStatus::Failed { reason } => {
                ("failed".to_string(), Some(reason.clone()))
            }
            other => (other.name().to_string(), None),
        };
        Self {
            id: r.id.to_string(),
            function_id: r.function_id.to_string(),
            number: r.number,
            status,
            failure_reason,
            spec: serde_json::to_value(&r.spec).unwrap_or_default(),
            spec_digest: r.spec_digest.to_string(),
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

// ---------------------------------------------------------------------------
// aliases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpdateAliasRequest {
    pub revision_id: String,
    /// When set, the update only applies if the alias is at this generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AliasResponse {
    pub function_id: String,
    pub name: String,
    pub revision_id: String,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_revision_id: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: Timestamp,
}

impl From<&domain::FunctionAlias> for AliasResponse {
    fn from(a: &domain::FunctionAlias) -> Self {
        Self {
            function_id: a.function_id.to_string(),
            name: a.name.to_string(),
            revision_id: a.revision_id.to_string(),
            generation: a.generation,
            previous_revision_id: a.previous_revision_id.as_ref().map(ToString::to_string),
            updated_at: a.updated_at,
        }
    }
}

// ---------------------------------------------------------------------------
// invocations
// ---------------------------------------------------------------------------

/// Query parameters for `POST /v1/functions/{id}:invoke`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema)]
pub struct InvokeQuery {
    /// Alias to resolve (default `prod`).
    #[serde(default)]
    pub alias: Option<String>,
    /// Pin a specific revision instead of resolving an alias.
    #[serde(default)]
    pub revision_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InvocationErrorResponse {
    pub class: String,
    pub error_type: String,
    pub message: String,
}

impl From<&domain::InvocationError> for InvocationErrorResponse {
    fn from(e: &domain::InvocationError) -> Self {
        Self {
            class: e.class.as_str().to_string(),
            error_type: e.error_type.clone(),
            message: e.message.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TimingsResponse {
    pub queue_wait_ms: Option<u64>,
    pub environment_boot_ms: Option<u64>,
    pub runtime_init_ms: Option<u64>,
    /// Warm starts only: the environment resume and the readiness check that
    /// followed it. Absent on a cold start, which boots instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_ms: Option<u64>,
    pub handler_ms: Option<u64>,
    pub response_ms: Option<u64>,
    pub total_ms: Option<u64>,
}

impl From<&domain::AttemptTimings> for TimingsResponse {
    fn from(t: &domain::AttemptTimings) -> Self {
        Self {
            queue_wait_ms: t.queue_wait_ms,
            environment_boot_ms: t.environment_boot_ms,
            runtime_init_ms: t.runtime_init_ms,
            resume_ms: t.resume_ms,
            readiness_ms: t.readiness_ms,
            handler_ms: t.handler_ms,
            response_ms: t.response_ms,
            total_ms: t.total_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttemptResponse {
    pub id: String,
    pub number: u32,
    pub environment_id: String,
    pub epoch: u64,
    /// `dispatched` | `succeeded` | `failed` | `outcome_unknown`
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<InvocationErrorResponse>,
    /// `cold` | `warm` | `restored`
    pub start_kind: String,
    pub timings: TimingsResponse,
    #[schema(value_type = Object)]
    pub boot_evidence: serde_json::Value,
    #[schema(value_type = String, format = DateTime)]
    pub dispatched_at: Timestamp,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InvocationResponse {
    pub id: String,
    pub function_id: String,
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// `sync` | `async`
    pub mode: String,
    /// `accepted` | `queued` | `running` | `succeeded` | `failed` | `cancelled` | `outcome_unknown`
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<InvocationErrorResponse>,
    /// Inline result when small enough; `null` otherwise or when not succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    pub trace_id: String,
    pub input_digest: String,
    pub input_size_bytes: u64,
    #[schema(value_type = String, format = DateTime)]
    pub accepted_at: Timestamp,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    pub deadlines: DeadlinesResponse,
    #[serde(default)]
    pub attempts: Vec<AttemptResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeadlinesResponse {
    #[schema(value_type = String, format = DateTime)]
    pub queue_deadline: Timestamp,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_deadline: Option<Timestamp>,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_deadline: Option<Timestamp>,
    #[schema(value_type = String, format = DateTime)]
    pub client_deadline: Timestamp,
}

impl From<&domain::Deadlines> for DeadlinesResponse {
    fn from(d: &domain::Deadlines) -> Self {
        Self {
            queue_deadline: d.queue_deadline,
            init_deadline: d.init_deadline,
            execution_deadline: d.execution_deadline,
            client_deadline: d.client_deadline,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LogEntryResponse {
    #[schema(value_type = String, format = DateTime)]
    pub timestamp: Timestamp,
    /// `stdout` | `stderr` | `platform`
    pub stream: String,
    /// `boot` | `init` | `handler` | `shutdown`
    pub phase: String,
    pub environment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    pub line: String,
    #[serde(default)]
    pub truncated: bool,
}

impl From<&domain::LogRecord> for LogEntryResponse {
    fn from(r: &domain::LogRecord) -> Self {
        Self {
            timestamp: r.timestamp,
            stream: match r.stream {
                domain::LogStream::Stdout => "stdout",
                domain::LogStream::Stderr => "stderr",
                domain::LogStream::Platform => "platform",
            }
            .into(),
            phase: match r.phase {
                domain::LogPhase::Boot => "boot",
                domain::LogPhase::Init => "init",
                domain::LogPhase::Handler => "handler",
                domain::LogPhase::Shutdown => "shutdown",
            }
            .into(),
            environment_id: r.environment_id.to_string(),
            invocation_id: r.invocation_id.as_ref().map(ToString::to_string),
            attempt_id: r.attempt_id.as_ref().map(ToString::to_string),
            line: r.line.clone(),
            truncated: r.truncated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LogsResponse {
    pub items: Vec<LogEntryResponse>,
    /// True when lines were dropped because of retention limits.
    #[serde(default)]
    pub dropped: bool,
}

// ---------------------------------------------------------------------------
// usage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageSummaryResponse {
    pub function_id: String,
    pub invocations: u64,
    pub succeeded: u64,
    pub failed: u64,
    /// Host-observed handler milliseconds, summed.
    pub handler_ms_total: u64,
    /// Host-observed environment lifetime milliseconds, summed.
    pub environment_ms_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    /// Always true in the prototype: numbers are usage facts, not charges.
    pub not_billable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_revision_request_defaults() {
        let j = r#"{"artifact":{"kind":"binary","digest":"sha256:ab"},"architecture":"aarch64"}"#;
        let r: CreateRevisionRequest = serde_json::from_str(j).unwrap();
        assert_eq!(r.resources.memory_mib, 256);
        assert_eq!(r.execution.timeout_seconds, 30);
        assert!(r.publish_to_prod);
    }

    #[test]
    fn error_codes_map_to_status() {
        assert_eq!(ErrorCode::NotFound.http_status(), 404);
        assert_eq!(ErrorCode::Timeout.http_status(), 504);
        assert_eq!(ErrorCode::CapacityExceeded.http_status(), 429);
    }

    /// PLT-4633: `reuse` is new, so a provider response without it (an older
    /// gateway) still deserializes — and it defaults to "off", never to
    /// something a reader could take for a working warm configuration.
    #[test]
    fn provider_info_without_reuse_defaults_to_off() {
        let j = r#"{"kind":"process","dev_only":true,"isolation":"process",
                    "capabilities":{},"preflight":{}}"#;
        let info: ProviderInfo = serde_json::from_str(j).unwrap();
        assert!(!info.reuse.enabled);
        assert!(!info.reuse.verified);
        assert!(!info.reuse.reason.is_empty());
        assert_eq!(info.reuse.idle_quiesce, "unknown");
        assert_eq!(info.reuse.idle_resume, "unknown");

        // A response that carries it round-trips, including the combination
        // that must never be read as a warm success.
        let measuring = ProviderInfo {
            reuse: ReuseInfo {
                enabled: true,
                verified: false,
                reason: "a measurement run".into(),
                idle_quiesce: "unverified".into(),
                idle_resume: "unverified".into(),
            },
            ..info
        };
        let encoded = serde_json::to_string(&measuring).unwrap();
        let back: ProviderInfo = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back.reuse, measuring.reuse);
        assert!(back.reuse.enabled && !back.reuse.verified);
    }
}

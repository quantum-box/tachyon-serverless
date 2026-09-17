//! Management / invoke API contract (RFC §7).
//!
//! Paths (all under the gateway):
//!
//! ```text
//! GET    /healthz                                  liveness
//! GET    /readyz                                   readiness (provider preflight ok)
//! GET    /v1/provider                              provider kind + capabilities
//! GET    /v1/capacity                              node capacity, reservations, queue, autoscaler
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
//! GET    /v1/usage?from&to&group_by&function_id   provisional usage report (not an invoice)
//! POST   /v1/functions/{function_id}/triggers      cron / webhook triggers (see `triggers`)
//! POST   /v1/hooks/{trigger_id}                    signed webhook delivery
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

pub mod triggers;
pub use triggers::*;

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
    /// A data plane cannot accept new work from its configuration cache:
    /// the needed configuration was not delivered yet, expired, the
    /// authorization lease expired, or new cold starts are restricted while
    /// the control plane is unreachable (PLT-4636). `error_type` names which.
    ConfigUnavailable,
    /// The management API (the control plane or its store) is unavailable
    /// from this gateway (PLT-4636).
    ControlPlaneUnavailable,
    /// An asynchronous invocation cannot be durably accepted right now: the
    /// object store or the queue is unavailable, or asynchronous invoke is
    /// not configured (PLT-4639). `reason` says which. Nothing was recorded.
    AsyncUnavailable,
    /// New invocations are refused because the usage journal is full or
    /// unavailable: the gateway will not run what it cannot meter (PLT-4642).
    /// `reason` is `usage_journal_full` or `usage_journal_unavailable`.
    UsageJournalFull,
    /// The target exists for the caller but no longer takes new work: a
    /// disabled webhook trigger answering a correctly signed delivery
    /// (PLT-4641).
    Gone,
    /// The tenant's or function's hard budget limit does not admit the
    /// maximum charge of this invocation (PLT-4643). `reason` is `budget`,
    /// `error_type` `Host.BudgetExhausted`. Not retried automatically: the
    /// limit or the period has to change.
    BudgetExhausted,
    /// The budget cannot be enforced right now, so new invocations are
    /// refused (fail closed, PLT-4643): the budget was not delivered or its
    /// lease expired, usage collection is stalled (`Host.BudgetUnknown`), or
    /// the budget store is unavailable (`Host.BudgetStoreUnavailable`).
    BudgetUnavailable,
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
            Self::ConfigUnavailable | Self::ControlPlaneUnavailable => 503,
            Self::AsyncUnavailable => 503,
            Self::UsageJournalFull => 503,
            Self::Gone => 410,
            Self::BudgetExhausted => 429,
            Self::BudgetUnavailable => 503,
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
    /// Why admission refused or gave up on the request (PLT-4634):
    /// `capacity` | `quota` | `queue_full` | `queue_deadline` |
    /// `circuit_open` | `placement`; or why an asynchronous invocation was
    /// refused (PLT-4639): `backlog` | `queue_full` | `queue_unavailable` |
    /// `object_store_unavailable` | `object_quota` | `input_too_large` |
    /// `not_configured`. Absent for every other error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
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
// capacity (PLT-4634)
// ---------------------------------------------------------------------------

/// CPU / memory / ephemeral storage. In [`NodeInfo::capacity`] a missing
/// value means the dimension is not bounded by configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResourceAmounts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage_mib: Option<u64>,
}

/// The physical host. Distinct from the number of environments on it: adding
/// hosts is not something this single-node prototype does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Hosts behind this gateway. Always 1 in the prototype.
    pub hosts: u32,
    /// How hosts are added: `not_supported` (single node; scale-out is out of scope).
    pub host_scale_out: String,
    /// Configured capacity; a missing dimension is unbounded.
    pub capacity: ResourceAmounts,
    /// Added to every environment's reservation (VMM + bridge + host artefacts).
    pub per_environment_overhead: ResourceAmounts,
    /// Environments starting or busy at once, node-wide.
    pub max_concurrency: u64,
}

/// Environments by lifecycle, as the admission ledger counts them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentCounts {
    /// Booting (reserved from the moment the start was granted).
    pub starting: u64,
    pub busy: u64,
    /// Granted a pooled environment that has not been taken yet.
    pub promised: u64,
    /// Being quiesced on the way into the pool.
    pub parking: u64,
    pub idle: u64,
    pub draining: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct QueueInfo {
    pub length: u64,
    pub bytes: u64,
    pub max_length: u64,
    pub max_bytes: u64,
    pub timeout_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StartRateInfo {
    pub per_second: u32,
    pub burst: u32,
    pub tokens: u32,
}

/// The caller's own tenant. Other tenants are never listed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantCapacityInfo {
    pub tenant_id: String,
    pub in_flight: u64,
    pub queued: u64,
    pub queued_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queue: Option<u64>,
    pub weight: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_region: Option<String>,
}

/// Autoscaler view of one revision of the caller's tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RevisionCapacityInfo {
    pub revision_id: String,
    pub desired: u32,
    pub max_environments: u32,
    pub environments: EnvironmentCounts,
    pub queued: u64,
    pub arrival_rate_per_second: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg_duration_ms: Option<u64>,
    /// `closed` | `open` | `half_open`
    pub circuit_breaker: String,
    /// Environments kept provisioned while the revision is routed
    /// (`execution.min_ready`, PLT-4635). 0: scales to zero.
    #[serde(default)]
    pub min_ready: u32,
    /// Idle time before a pooled environment may be scaled down.
    #[serde(default)]
    pub idle_ttl_seconds: u64,
    /// No scale-down within this many seconds of a scale-up or activation.
    #[serde(default)]
    pub scale_down_cooldown_seconds: u64,
    /// `routed` (an alias points at it) | `unrouted` (pinned invocations
    /// only) | `superseded` (an alias moved away: draining) | `deleting`
    /// (its function is being deleted: draining, new work refused).
    #[serde(default)]
    pub route_state: String,
    /// The last scale decision taken for this revision, kept after it
    /// scaled to zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_event: Option<ScaleEventInfo>,
}

/// One scale decision (PLT-4635).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ScaleEventInfo {
    /// `activation` (a cold start from zero) | `scale_up` | `prestart`
    /// (min_ready) | `scale_down` | `scale_to_zero` | `drain` |
    /// `drain_timeout`
    pub kind: String,
    /// Why, e.g. `backlog`, `min_ready`, `idle_ttl`, `alias_switch`,
    /// `function_deleted`, `reuse_key_superseded`.
    pub reason: String,
    #[schema(value_type = String, format = DateTime)]
    pub at: Timestamp,
}

/// Node-wide scaling settings (PLT-4635).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
pub struct ScalingInfo {
    /// How often the scale reconciler runs (idle sweep, min_ready, drains).
    pub reconcile_interval_ms: u64,
    /// Idle TTL of revisions that do not set their own.
    pub default_idle_ttl_seconds: u64,
    /// Scale-down cooldown of revisions that do not set their own.
    pub default_scale_down_cooldown_seconds: u64,
    /// In-flight invocations of a drained revision still running this long
    /// after the drain started are stopped (`Host.DrainTimeout`).
    pub drain_timeout_seconds: u64,
    /// Whether this gateway can hold idle (warm) environments at all. When
    /// false every environment ends with its invocation and `min_ready` is
    /// not honoured.
    pub warm_pool: bool,
    /// What zero environments does *not* remove: the gateway, its store and
    /// the node keep running and cost what they cost.
    pub at_zero: String,
}

/// `GET /v1/capacity`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CapacityInfo {
    pub node: NodeInfo,
    /// Sum of every live reservation (starting, busy, parking, idle, draining).
    pub reserved: ResourceAmounts,
    pub environments: EnvironmentCounts,
    pub in_flight: u64,
    pub queue: QueueInfo,
    pub start_rate: StartRateInfo,
    /// Rejections since start, by reason.
    pub rejections: std::collections::BTreeMap<String, u64>,
    pub tenant: TenantCapacityInfo,
    pub revisions: Vec<RevisionCapacityInfo>,
    #[serde(default)]
    pub scaling: ScalingInfo,
    /// Whether environments are reused on this node, and the boot identity
    /// check behind it (PLT-4637). Node-wide; no tenant data.
    #[serde(default)]
    pub reuse: EnvironmentReuseReport,
}

/// Environment reuse on this node (PLT-4637, docs/metrics.md §boot identity).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
pub struct EnvironmentReuseReport {
    pub provider: String,
    /// `warm_reuse`: pooled environments serve later invocations.
    /// `every_invocation_boots`: the provider or configuration has no warm
    /// stage, so every invocation boots its own environment (the process
    /// provider always).
    pub mode: String,
    /// The gate that decided the mode.
    pub reason: String,
    /// Attempts that were the first dispatch into their environment and
    /// reported a guest boot id.
    pub first_boots: u64,
    /// Later attempts in the same environment that reported the boot id it
    /// booted with: reuse of the same guest.
    pub same_boot_reuses: u64,
    /// Later attempts that reported a different boot id for the same
    /// environment. Must stay 0.
    pub boot_id_changed: u64,
    /// Attempts without a guest boot id (no guest kernel, e.g. the process
    /// provider).
    pub boot_id_unreported: u64,
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
    /// `live` | `deleting` (new invocations refused, in-flight work and
    /// environments still draining) | `deleted` (drained).
    #[serde(default)]
    pub deletion_state: String,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drained_at: Option<Timestamp>,
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
            deletion_state: f.deletion_state().to_string(),
            drained_at: f.drained_at,
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
    /// Environments kept provisioned while an alias routes the revision
    /// (PLT-4635). 0 (default): scale to zero. Needs environment reuse.
    #[serde(default)]
    pub min_ready: u32,
    /// Idle seconds before a pooled environment may be scaled down.
    /// Default: the gateway's `[pool] idle_ttl_seconds`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_ttl_seconds: Option<u32>,
    /// No scale-down within this many seconds of a scale-up or activation.
    /// Default: the gateway's `[scaling] scale_down_cooldown_seconds`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_down_cooldown_seconds: Option<u32>,
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
            min_ready: 0,
            idle_ttl_seconds: None,
            scale_down_cooldown_seconds: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SecretBindingRequest {
    pub env_name: String,
    pub binding_ref: String,
}

/// One allowlist entry of a `restricted` revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EgressAllowRequest {
    /// IPv4 network, e.g. `203.0.113.7/32` (a bare address means `/32`).
    pub cidr: String,
    /// `tcp` (default) or `udp`.
    #[serde(default)]
    pub protocol: Option<String>,
    /// Destination ports (1..=16 entries).
    pub ports: Vec<u16>,
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
    /// Destinations a `restricted` revision may open (required for
    /// `restricted`, refused for the other profiles). IPv4 CIDRs only;
    /// special-purpose ranges (private, link-local, metadata, loopback, ...)
    /// are refused.
    #[serde(default)]
    pub egress_allow: Vec<EgressAllowRequest>,
    #[serde(default)]
    pub env_vars: Vec<(String, String)>,
    #[serde(default)]
    pub secrets: Vec<SecretBindingRequest>,
    #[serde(default)]
    pub description: String,
    /// When true (default), also point alias `prod` at the revision once Ready.
    #[serde(default = "default_true")]
    pub publish_to_prod: bool,
    /// Region the revision must run in (e.g. `jp`). Admission rejects the
    /// invocation on a node with another or no region label (PLT-4634).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_region: Option<String>,
    /// Experimental restore settings (X1, PLT-4653). Omitted means
    /// `{"policy": "disabled"}`: the revision never restores a snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore: Option<RestoreRequest>,
}
fn default_true() -> bool {
    true
}

/// Experimental restore settings of a revision (X1, PLT-4653;
/// docs/adr/0017-snapshot-manifest-and-clone.md).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RestoreRequest {
    /// `disabled` (default) | `prefer` (restore when a compatible, verified
    /// snapshot exists, otherwise start cold and record why) | `require`
    /// (restore or fail with `Host.RestoreRequiredUnavailable`).
    #[serde(default = "default_restore_policy")]
    pub policy: String,
    /// The operator states that initialization builds only synthetic sample
    /// data. Required for `prefer` / `require` and for creating a snapshot;
    /// such a revision may not have secret bindings and must use egress
    /// `none`.
    #[serde(default)]
    pub synthetic_init_sample: bool,
}
fn default_restore_policy() -> String {
    "disabled".into()
}

/// `POST /v1/functions/{function_id}/snapshots` (X1, experimental).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotRequest {
    /// Revision to snapshot. Defaults to the revision alias `prod` points at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
}

/// `POST /v1/functions/{function_id}/snapshots/{snapshot_id}/revoke`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RevokeSnapshotRequest {
    pub reason: String,
}

/// A snapshot as the API shows it: identity, state and the manifest digest.
/// The manifest's host facts are summarized, never the artifact bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SnapshotResponse {
    pub id: String,
    pub function_id: String,
    pub revision_id: String,
    /// `active` | `revoked` | `quarantined` | `expired`
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    /// `sha256:<hex>` of the canonical manifest.
    pub manifest_digest: String,
    pub manifest_version: u32,
    pub provider: String,
    pub memory_mib: u32,
    pub vcpus: u32,
    pub source_environment_id: String,
    /// How many clones were created from it.
    pub restores: u64,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: Timestamp,
    /// Snapshot timings of the source, milliseconds: `pause_ms`,
    /// `create_ms`, `copy_ms`, `seal_ms`.
    #[schema(value_type = Object)]
    pub timings: serde_json::Value,
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

/// `202 Accepted` of `POST /v1/functions/{id}:invokeAsync` (PLT-4639). Sent
/// only after the invocation, its input and its outbox event committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InvokeAsyncResponse {
    pub invocation_id: String,
    pub function_id: String,
    /// The revision fixed at acceptance. Every later delivery and retry of
    /// this invocation runs it, whatever the alias points at by then.
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// `accepted` (recorded, not yet in the queue) | `queued` (published) |
    /// later states when an idempotent replay finds it further along.
    pub status: String,
    /// `GET` this for the invocation (also in the `Location` header).
    pub status_url: String,
    pub input_digest: String,
    pub input_size_bytes: u64,
    /// `inline` (kept in the ledger) | `object` (kept in the object store).
    pub input_storage: String,
    /// True when an `Idempotency-Key` matched an earlier acceptance.
    pub replayed: bool,
    pub trace_id: String,
    #[schema(value_type = String, format = DateTime)]
    pub accepted_at: Timestamp,
}

/// The dispatch state of an asynchronous invocation (PLT-4640).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AsyncDispatchResponse {
    /// `pending` (never claimed) | `running` | `scheduled` | `done` | `dead`.
    pub state: String,
    /// Runs that counted against `max_attempts`.
    pub attempts: u32,
    /// Runs deferred without counting (capacity, retry budget, shutdown).
    pub deferrals: u32,
    /// Delivery generation: 0 at acceptance, n after the nth scheduled retry.
    pub generation: u64,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<InvocationErrorResponse>,
    /// The dead letter this invocation ended in, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_id: Option<String>,
    /// The redrive that created this invocation, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redriven_from: Option<RedriveResponse>,
}

/// A dead-lettered asynchronous invocation, or a poison event (PLT-4640).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeadLetterResponse {
    pub id: String,
    /// `non_retryable` | `attempts_exhausted` | `expired` | `function_deleted`
    /// | `revision_unavailable` | `poison`.
    pub reason: String,
    /// `open` | `redriven`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_id: Option<String>,
    /// `null` for a poison event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    pub attempts: u32,
    pub deferrals: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<InvocationErrorResponse>,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<Timestamp>,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_attempt_at: Option<Timestamp>,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<Timestamp>,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_size_bytes: Option<u64>,
    /// `inline` | `object`. The input is kept (never copied) while the entry
    /// is open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_storage: Option<String>,
    /// Poison only: the queue message id and why it could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub redrive_count: u32,
    #[schema(value_type = Option<String>, format = DateTime)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redriven_at: Option<Timestamp>,
    /// Oldest first. Present on `GET /v1/dead-letters/{id}`.
    #[serde(default)]
    pub redrives: Vec<RedriveResponse>,
}

/// `POST /v1/dead-letters/{id}:redrive` body (PLT-4640).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RedriveRequestBody {
    /// Run another revision **of the same function** instead of the original
    /// one. Must be explicit; the default keeps the revision the invocation
    /// was pinned to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    /// Why (kept in the audit record, at most 1024 bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// The audit record of one redrive (PLT-4640).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RedriveResponse {
    pub id: String,
    pub dead_letter_id: String,
    pub function_id: String,
    /// The dead-lettered invocation.
    pub source_invocation_id: String,
    /// The new invocation.
    pub invocation_id: String,
    pub revision_id: String,
    pub revision_overridden: bool,
    /// The subject of the principal that asked.
    pub requested_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
}

/// `202 Accepted` of a redrive: the audit record and the new invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RedriveAcceptedResponse {
    pub redrive: RedriveResponse,
    pub invocation: InvokeAsyncResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InvocationResponse {
    pub id: String,
    pub function_id: String,
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// Generation of the alias route that chose `revision_id`, resolved once
    /// at `accepted_at` (PLT-4635). An alias switch later never re-points an
    /// accepted invocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_generation: Option<u64>,
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
    /// Asynchronous invocations only: retries, dead letter and redrive links
    /// (PLT-4640).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<AsyncDispatchResponse>,
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

/// `GET /v1/usage` (PLT-4642): a **provisional** usage report of the caller's
/// tenant, rated with a versioned price table. Not an invoice: nothing is
/// charged, billing is hard-disabled in the prototype
/// (`billing_enabled = false`, `not_an_invoice = true`).
///
/// Three things are kept apart: `usage` (metered quantities),
/// `provisional_charges_micros` (usage × price table) and `cost` (what the
/// host spent on the tenant's environments; never charged).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageReportResponse {
    /// Always true.
    pub provisional: bool,
    /// Always true.
    pub not_an_invoice: bool,
    /// Always false in the prototype.
    pub billing_enabled: bool,
    pub notice: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_id: Option<String>,
    /// Inclusive, by the host wall clock of each event.
    #[schema(value_type = String, format = DateTime)]
    pub from: Timestamp,
    /// Exclusive.
    #[schema(value_type = String, format = DateTime)]
    pub to: Timestamp,
    /// `function`, `day`, both, or empty (one line).
    pub group_by: Vec<String>,
    pub price_table: PriceTableInfo,
    pub lines: Vec<UsageReportLine>,
    /// Sum of `lines` (charges are the sum of the line charges, not re-rounded).
    pub totals: UsageReportLine,
    /// Events of this tenant the usage journal refused (full or unavailable).
    /// They are not in any line: unmetered, never estimated.
    pub unjournaled_events: u64,
    /// Events still waiting in the journal are not in the report yet.
    #[schema(value_type = Option<String>, format = DateTime)]
    pub collected_through: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PriceTableInfo {
    pub version: String,
    #[schema(value_type = String, format = DateTime)]
    pub effective_from: Timestamp,
    pub currency: String,
    /// Segments whose host-measured milliseconds are charged as compute.
    pub billable_segments: Vec<String>,
    pub unit_prices_micros: UnitPricesMicros,
    /// The rounding rules, in the order they apply.
    pub rounding: Vec<String>,
}

/// Unit prices in micro-units (1e-6) of `currency`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct UnitPricesMicros {
    pub vcpu_second: u64,
    pub gib_second: u64,
    pub invocation: u64,
    pub gb_transferred: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct UsageReportLine {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_id: Option<String>,
    /// `YYYY-MM-DD` (UTC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    pub usage: UsageQuantities,
    pub unmetered: UnmeteredUsage,
    pub cost: HostCostFacts,
    pub provisional_charges_micros: ProvisionalCharges,
    pub guest_reported: GuestReportedTotals,
}

/// Metered quantities (host-measured or provider-reported only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct UsageQuantities {
    /// First attempts (one per invocation that was dispatched).
    pub invocations: u64,
    pub attempts: u64,
    pub retries: u64,
    pub outcomes: OutcomeCounts,
    pub segments_ms: SegmentTotals,
    /// Sum of the billable segments.
    pub billable_ms: u64,
    /// `billable_ms × requested cpu_millis`.
    pub vcpu_milli_ms: u64,
    /// `billable_ms × requested memory_mib`.
    pub mib_ms: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct OutcomeCounts {
    pub succeeded: u64,
    pub failed: u64,
    pub timeout: u64,
    pub cancelled: u64,
    pub outcome_unknown: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct SegmentTotals {
    pub queue_wait_ms: u64,
    pub vm_base_boot_ms: u64,
    pub user_init_ms: u64,
    pub handler_ms: u64,
    pub teardown_ms: u64,
    pub idle_pooled_ms: u64,
}

/// What was not measured: counted, contributes nothing to a charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct UnmeteredUsage {
    /// Attempts with at least one billable segment unknown or guest-reported.
    pub attempts: u64,
    /// Per segment: how many attempts had it unknown or guest-reported.
    pub segments: SegmentTotals,
    /// Attempts whose request or response bytes were not measured.
    pub bytes: u64,
}

/// What the host spent on the tenant's environments. Never charged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct HostCostFacts {
    pub environments_stopped: u64,
    /// Ledger `created_at` to the end of each environment (wall clock).
    pub environment_lifetime_ms: u64,
    pub idle_pooled_ms: u64,
    pub teardown_ms: u64,
    /// Boot and initialization of environments that never served an attempt.
    pub boot_without_attempt_ms: u64,
    /// cgroup v2 CPU of the VMMs (provider-reported).
    pub cgroup_cpu_usec: u64,
    /// Stopped environments without a cgroup reading.
    pub cgroup_cpu_unknown: u64,
    pub cgroup_memory_peak_bytes_max: u64,
}

/// Provisional charges in micro-units of the price table's currency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct ProvisionalCharges {
    pub vcpu: u64,
    pub memory: u64,
    pub invocations: u64,
    pub transfer: u64,
    pub total: u64,
}

/// Guest self-reports, shown for comparison only. Never rated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct GuestReportedTotals {
    pub guest_handler_ms: u64,
    pub guest_init_ms: u64,
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

// ---------------------------------------------------------------------------
// budget (PLT-4643)
// ---------------------------------------------------------------------------

/// `GET /v1/budget`: the caller's budget in one period. Amounts are integer
/// micro-units of the price table's currency, **provisional**, and cover only
/// what PLT-4642 rates (billable segments of `AttemptSettled`). Nothing is
/// charged; billing is disabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BudgetReportResponse {
    /// Whether this gateway enforces budgets.
    pub enabled: bool,
    /// Always true.
    pub provisional: bool,
    /// Always false in the prototype.
    pub billing_enabled: bool,
    pub notice: String,
    pub tenant_id: String,
    /// `YYYY-MM`.
    pub period: String,
    /// `calendar_month_utc`.
    pub period_kind: String,
    #[schema(value_type = String, format = DateTime)]
    pub period_start: Timestamp,
    #[schema(value_type = String, format = DateTime)]
    pub period_end: Timestamp,
    pub currency: String,
    pub price_table_version: String,
    /// `valid`, `expired` or `not_delivered`: the delivered budget as this
    /// gateway's configuration cache holds it.
    pub config_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_generation: Option<u64>,
    /// Whether new invocations of this tenant are admitted as far as the
    /// budget is concerned (before a per-function limit).
    pub admitting: bool,
    /// Why not, when `admitting` is false (`error_type`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    pub tenant: BudgetScopeReport,
    /// Functions with a budget or with reservations in the period.
    pub functions: Vec<BudgetScopeReport>,
    /// What the amounts do and do not guarantee.
    pub guarantee: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BudgetScopeReport {
    /// `None` for the tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_limit_micros: Option<u64>,
    pub alert_thresholds_percent: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_limit_micros: Option<u64>,
    /// Maximum charges held by runs not yet settled.
    pub reserved_micros: u64,
    /// Rated (provisional) charges of settled runs.
    pub settled_micros: u64,
    /// Reserved amounts kept because the run's usage could not be fully
    /// measured (expired runs, unjournaled or unknown segments). Counted
    /// against the limit, never billable.
    pub unmetered_hold_micros: u64,
    /// `reserved + settled + unmetered_hold`: what the hard limit compares.
    pub committed_micros: u64,
    /// `hard_limit - committed` (0 when over). `None` without a hard limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_micros: Option<u64>,
    /// Measured charges above their reservation (settled in full).
    pub overrun_micros: u64,
    pub active_reservations: u64,
    pub reservations: u64,
    pub settlements: u64,
    pub releases: u64,
    pub expiries: u64,
    pub refusals: u64,
    pub alerts_fired: Vec<BudgetAlert>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BudgetAlert {
    pub threshold_percent: u32,
    pub soft_limit_micros: u64,
    /// Settled + unmetered holds when it fired.
    pub consumed_micros: u64,
    #[schema(value_type = String, format = DateTime)]
    pub fired_at: Timestamp,
}

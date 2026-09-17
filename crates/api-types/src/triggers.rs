//! Cron and signed webhook triggers (PLT-4641, docs/adr/0014).
//!
//! ```text
//! POST   /v1/functions/{function_id}/triggers                         create (deploy role)
//! GET    /v1/functions/{function_id}/triggers                         list
//! GET    /v1/functions/{function_id}/triggers/{trigger_id}            get
//! PATCH  /v1/functions/{function_id}/triggers/{trigger_id}            update (CAS on expected_generation)
//! DELETE /v1/functions/{function_id}/triggers/{trigger_id}            delete (stops new fires at once)
//! GET    /v1/functions/{function_id}/triggers/{trigger_id}/fires      fire records (scheduled times, webhook events)
//! POST   /v1/hooks/{trigger_id}                                       webhook delivery (signature, no bearer token)
//! ```
//!
//! A trigger never runs anything itself: every fire is an ordinary
//! asynchronous invocation accepted through `invokeAsync` (authorization and
//! resolution from the configuration cache, revision pinned at acceptance,
//! outbox in the same transaction), so retries and the dead-letter path are
//! the common asynchronous ones.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::Timestamp;

/// Webhook headers of the `generic-hmac` source.
pub mod webhook_headers {
    /// Unix seconds at which the sender signed the delivery.
    pub const TIMESTAMP: &str = "x-tachyon-webhook-timestamp";
    /// `v1=<hex HMAC-SHA256(secret, "{timestamp}.{body}")>`; several
    /// comma-separated `v1=` entries are accepted (secret rotation on the
    /// sender side).
    pub const SIGNATURE: &str = "x-tachyon-webhook-signature";
    /// Default name of the event id header (per trigger: `event_id_header`).
    pub const EVENT_ID: &str = "x-tachyon-webhook-id";
}

/// The only webhook verification source of the prototype.
pub const WEBHOOK_SOURCE_GENERIC_HMAC: &str = "generic-hmac";

/// What a fire runs: an alias resolved at acceptance (default `prod`) or a
/// pinned revision. At most one of the two.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TriggerTargetRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
}

/// What happens to scheduled times that passed while no scheduler ran (a
/// stopped gateway, a disabled lease owner). Times older than
/// `[triggers] max_catchup_seconds` are always skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MissedRunPolicyRequest {
    /// Late times are not run (default).
    Skip,
    /// Only the most recent late time is run.
    RunOnce,
    /// The most recent `max_runs` late times are run, oldest first.
    RunAll { max_runs: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CronTriggerRequest {
    /// 5 fields (`minute hour day-of-month month day-of-week`) or 6 fields
    /// with a leading `second`. `*`, lists, ranges, steps, `JAN`-`DEC`,
    /// `SUN`-`SAT` (0 and 7 are Sunday) and `@hourly` / `@daily` /
    /// `@weekly` / `@monthly` / `@yearly`.
    pub expression: String,
    /// IANA time zone the expression is read in (default `UTC`).
    #[serde(default = "default_timezone")]
    pub timezone: String,
    /// Static JSON handed to the function as `payload` of every fire.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub payload: serde_json::Value,
    #[serde(default = "default_missed_run_policy")]
    pub missed_run_policy: MissedRunPolicyRequest,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_missed_run_policy() -> MissedRunPolicyRequest {
    MissedRunPolicyRequest::Skip
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WebhookTriggerRequest {
    /// Verification source. Only `generic-hmac`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Accepted difference between the signed timestamp and the gateway clock
    /// (default 300, at most 3600).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance_seconds: Option<u64>,
    /// Largest accepted body (default and maximum `[triggers]
    /// webhook_max_body_bytes`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_body_bytes: Option<u64>,
    /// Header that carries the sender's event id (default
    /// `x-tachyon-webhook-id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id_header: Option<String>,
}

/// `POST /v1/functions/{function_id}/triggers`. Exactly one of `cron` /
/// `webhook`, matching `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CreateTriggerRequest {
    /// 1..=64 characters, a label (not unique).
    pub name: String,
    /// `cron` | `webhook`.
    pub kind: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub target: TriggerTargetRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<CronTriggerRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookTriggerRequest>,
}

fn default_true() -> bool {
    true
}

/// `PATCH /v1/functions/{function_id}/triggers/{trigger_id}`. Absent fields
/// keep their value. With `expected_generation` the update applies only at
/// that generation (409 otherwise).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UpdateTriggerRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<TriggerTargetRequest>,
    /// Cron only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missed_run_policy: Option<MissedRunPolicyRequest>,
    /// Webhook only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id_header: Option<String>,
    /// Webhook only: generate a new secret, returned once in this response.
    /// The old secret stops verifying at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotate_secret: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CronTriggerInfo {
    pub expression: String,
    pub timezone: String,
    #[schema(value_type = Object)]
    pub payload: serde_json::Value,
    pub missed_run_policy: MissedRunPolicyRequest,
    /// The next scheduled time (UTC) the scheduler will fire; `null` while
    /// disabled or deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub next_fire_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_scheduled_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WebhookTriggerInfo {
    pub source: String,
    pub tolerance_seconds: u64,
    pub max_body_bytes: u64,
    pub event_id_header: String,
    pub timestamp_header: String,
    pub signature_header: String,
    /// `POST` deliveries here (relative to the gateway).
    pub url: String,
    /// Fingerprint of the current secret (`sha256:` + 12 hex of SHA-256 over
    /// the secret), for telling secrets apart. Never the secret.
    pub secret_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TriggerResponse {
    pub id: String,
    pub function_id: String,
    pub name: String,
    /// `cron` | `webhook`.
    pub kind: String,
    pub enabled: bool,
    /// `enabled` | `disabled` | `deleted`.
    pub status: String,
    /// Why the platform disabled it (`function_deleted`), if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    /// Bumped by every change; the CAS guard of `PATCH`.
    pub generation: u64,
    pub target: TriggerTargetRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<CronTriggerInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookTriggerInfo>,
    /// The webhook secret. Present **only** in the response that created or
    /// rotated it; never returned again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: Timestamp,
}

/// One fire of a trigger: a scheduled time or a webhook event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TriggerFireResponse {
    pub trigger_id: String,
    /// `cron:<scheduled_at>` | `event:<event_id>`.
    pub fire_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub scheduled_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// `accepted` (an asynchronous invocation exists) | `refused` (the
    /// acceptance was refused for good, `reason` says why).
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: Timestamp,
}

/// `202` of `POST /v1/hooks/{trigger_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WebhookAcceptedResponse {
    pub trigger_id: String,
    pub event_id: String,
    pub invocation_id: String,
    /// `accepted` | `queued` | later states on a replay.
    pub status: String,
    /// True when this event id (or this exact signed delivery) was accepted
    /// before: the same invocation, nothing new was created.
    pub replayed: bool,
}

//! Cron and signed webhook triggers (PLT-4641, docs/adr/0014).
//!
//! A trigger never runs anything itself. Every fire is an ordinary
//! asynchronous invocation accepted through
//! [`AsyncInvokeService::accept_for_trigger`]: the tenant is authorized and
//! function, alias and revision are resolved from the configuration cache,
//! the revision is pinned at acceptance, the input, the outbox event and the
//! **fire row** commit in one transaction, and retries / dead letters are the
//! common asynchronous path (PLT-4640).
//!
//! - **Cron** ([`TriggerService::run_scheduler_once`]): one gateway owns the
//!   scheduler (a lease row held by its dispatcher id). For each due trigger
//!   it computes the scheduled times in the trigger's zone since its cursor,
//!   applies the missed-run policy, fires each time with idempotency key
//!   `cron:{trigger}:{scheduled_at}` and the unique fire row
//!   `(trigger, cron:{scheduled_at})`, and moves the cursor. A restart, a
//!   second scheduler or a crash between the fire and the cursor move finds
//!   the fire row and creates nothing.
//! - **Webhook** ([`TriggerService::receive_webhook`]): timestamp tolerance,
//!   constant-time HMAC and the event id are checked before anything is
//!   written; the event id (and the signed delivery itself) is the dedup key.
//! - **Disable / delete** take effect for every fire whose transaction
//!   commits after them: the fire transaction re-reads the trigger. Fires
//!   accepted before are ordinary invocations and continue.

pub mod config;
pub mod cron;
pub mod metrics;
pub mod webhook;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;

use tachyon_serverless_api_types::{
    CreateTriggerRequest, MissedRunPolicyRequest, TriggerTargetRequest, UpdateTriggerRequest,
    WEBHOOK_SOURCE_GENERIC_HMAC, webhook_headers,
};
use tachyon_serverless_domain::{
    AliasName, Clock, Function, FunctionId, IdGenerator, Invocation, Limits, RevisionId, Timestamp,
    TriggerId,
};
use tachyon_serverless_provider_port::{Principal, Role};

pub use config::TriggersConfig;
pub use cron::{CronSchedule, parse_timezone};

use crate::authz::{ensure_tenant, require_deploy, require_read};
use crate::control::{ConfigCache, ControlError};
use crate::durable::ObjectKey;
use crate::error::AppError;
use crate::repository::{
    CronSpec, FireClaim, FireOutcome, FireRecord, MissedRunPolicy, Repositories, Trigger,
    TriggerKind, TriggerRepository, TriggerSpec, TriggerStatus, TriggerTarget, WebhookSpec,
};
use crate::services::Dispatcher;
use crate::services::invoke_async::{
    AsyncInvokeService, AsyncRefusal, InvokeAsyncRequest, TriggerAcceptance,
};

/// `status_reason` of a trigger the platform disabled because its function
/// was deleted.
pub const REASON_FUNCTION_DELETED: &str = "function_deleted";
const MAX_NAME_LEN: usize = 64;
/// Headers an event id header may not be.
const RESERVED_HEADERS: &[&str] = &[
    webhook_headers::TIMESTAMP,
    webhook_headers::SIGNATURE,
    "authorization",
    "content-type",
    "content-length",
    "host",
];

pub struct TriggerServiceDeps {
    pub repo: Arc<dyn TriggerRepository>,
    pub repos: Repositories,
    pub invoke_async: Arc<AsyncInvokeService>,
    pub cache: Arc<ConfigCache>,
    pub dispatcher: Arc<Dispatcher>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub limits: Limits,
    pub config: TriggersConfig,
    /// Seals webhook secrets. `None`: webhook triggers cannot be created.
    pub secret_key: Option<Arc<ObjectKey>>,
}

/// A created or rotated trigger, with the secret shown this once.
#[derive(Debug, Clone)]
pub struct TriggerWithSecret {
    pub trigger: Trigger,
    pub secret: Option<String>,
}

/// What one scheduler pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerReport {
    /// False when another gateway holds the scheduler lease (or this
    /// dispatcher is fenced): nothing was done.
    pub owner: bool,
    pub due_triggers: usize,
    pub accepted: usize,
    /// The scheduled time was fired before (restart, second scheduler).
    pub already_fired: usize,
    /// Refused for good and recorded (`reason` on the fire row).
    pub refused: usize,
    /// Late times dropped by the missed-run policy or the catch-up window.
    pub skipped_late: usize,
    /// Late times the missed-run policy fired (attempted).
    pub late_run: usize,
    /// A trigger changed, was disabled or deleted under the pass.
    pub inactive: usize,
    /// Retryable refusals (backlog, queue, configuration); retried next pass.
    pub deferred: usize,
    pub purged_fires: usize,
}

/// A delivery accepted (or recognized as a replay).
#[derive(Debug, Clone)]
pub struct WebhookAcceptance {
    pub trigger_id: TriggerId,
    pub event_id: String,
    pub invocation: Invocation,
    pub replayed: bool,
}

/// Headers of a webhook delivery, as the gateway read them.
#[derive(Debug, Clone, Default)]
pub struct WebhookDelivery {
    pub timestamp: Option<String>,
    pub signature: Option<String>,
    pub event_id: Option<String>,
    pub content_type: Option<String>,
}

enum Fire {
    Done,
    Refused,
    AlreadyFired,
    Inactive,
    Deferred,
}

pub struct TriggerService {
    repo: Arc<dyn TriggerRepository>,
    repos: Repositories,
    invoke_async: Arc<AsyncInvokeService>,
    cache: Arc<ConfigCache>,
    dispatcher: Arc<Dispatcher>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    limits: Limits,
    config: TriggersConfig,
    secret_key: Option<Arc<ObjectKey>>,
    last_purge: Mutex<Option<Timestamp>>,
    metrics: metrics::TriggerMetrics,
}

impl std::fmt::Debug for TriggerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriggerService")
            .field("config", &self.config)
            .field("secret_key", &self.secret_key.as_ref().map(|k| k.id()))
            .finish_non_exhaustive()
    }
}

fn secret_aad(trigger: &TriggerId, tenant: &tachyon_serverless_domain::TenantId) -> Vec<u8> {
    format!("tachyon-serverless/webhook-secret/v1\0{trigger}\0{tenant}").into_bytes()
}

fn invalid(msg: impl Into<String>) -> AppError {
    AppError::InvalidRequest(msg.into())
}

fn policy_from(req: MissedRunPolicyRequest, max: u32) -> Result<MissedRunPolicy, AppError> {
    Ok(match req {
        MissedRunPolicyRequest::Skip => MissedRunPolicy::Skip,
        MissedRunPolicyRequest::RunOnce => MissedRunPolicy::RunOnce,
        MissedRunPolicyRequest::RunAll { max_runs } => {
            if max_runs == 0 || max_runs > max {
                return Err(invalid(format!(
                    "missed_run_policy.run_all.max_runs must be 1..={max}"
                )));
            }
            MissedRunPolicy::RunAll { max_runs }
        }
    })
}

pub fn policy_to_api(p: MissedRunPolicy) -> MissedRunPolicyRequest {
    match p {
        MissedRunPolicy::Skip => MissedRunPolicyRequest::Skip,
        MissedRunPolicy::RunOnce => MissedRunPolicyRequest::RunOnce,
        MissedRunPolicy::RunAll { max_runs } => MissedRunPolicyRequest::RunAll { max_runs },
    }
}

pub fn target_to_api(t: &TriggerTarget) -> TriggerTargetRequest {
    TriggerTargetRequest {
        alias: t.alias.as_ref().map(ToString::to_string),
        revision_id: t.revision_id.as_ref().map(ToString::to_string),
    }
}

fn valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !RESERVED_HEADERS.contains(&name)
}

/// Whether a refused fire is worth retrying on the next pass.
fn is_transient(e: &AppError) -> bool {
    match e {
        AppError::AsyncRefused { reason, .. } => !matches!(reason, AsyncRefusal::InputTooLarge),
        AppError::Control { kind, .. } => !matches!(
            kind,
            ControlError::PolicyDenied | ControlError::UnknownTenant
        ),
        AppError::ProviderUnavailable(_) | AppError::Platform(_) => true,
        _ => false,
    }
}

impl TriggerService {
    pub fn new(deps: TriggerServiceDeps) -> Arc<Self> {
        Arc::new(Self {
            repo: deps.repo,
            repos: deps.repos,
            invoke_async: deps.invoke_async,
            cache: deps.cache,
            dispatcher: deps.dispatcher,
            clock: deps.clock,
            ids: deps.ids,
            limits: deps.limits,
            config: deps.config,
            secret_key: deps.secret_key,
            last_purge: Mutex::new(None),
            metrics: metrics::TriggerMetrics::default(),
        })
    }

    pub fn config(&self) -> &TriggersConfig {
        &self.config
    }

    /// Counters for `GET /metrics`.
    pub fn metrics(&self) -> &metrics::TriggerMetrics {
        &self.metrics
    }

    pub fn repository(&self) -> &Arc<dyn TriggerRepository> {
        &self.repo
    }

    // -- CRUD ----------------------------------------------------------------

    fn owned_function(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Function, AppError> {
        let function = self
            .repos
            .functions
            .get(function_id)?
            .ok_or_else(|| AppError::not_found("function not found"))?;
        ensure_tenant(principal, &function.tenant_id, "function")?;
        Ok(function)
    }

    /// A live trigger of the caller's function; anything else is 404.
    fn owned_trigger(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        trigger_id: &TriggerId,
    ) -> Result<Trigger, AppError> {
        self.owned_function(principal, function_id)?;
        self.repo
            .get_trigger(trigger_id)?
            .filter(|t| {
                t.tenant_id == principal.tenant_id
                    && &t.function_id == function_id
                    && t.status != TriggerStatus::Deleted
            })
            .ok_or_else(|| AppError::not_found("trigger not found"))
    }

    fn target_from(
        &self,
        function: &Function,
        req: &TriggerTargetRequest,
    ) -> Result<TriggerTarget, AppError> {
        match (&req.alias, &req.revision_id) {
            (Some(_), Some(_)) => Err(invalid("target takes an alias or a revision_id, not both")),
            (Some(a), None) => Ok(TriggerTarget {
                alias: Some(AliasName::parse(a).map_err(|e| invalid(e.to_string()))?),
                revision_id: None,
            }),
            (None, Some(r)) => {
                let id =
                    RevisionId::parse(r).map_err(|_| AppError::not_found("revision not found"))?;
                self.repos
                    .revisions
                    .get(&id)?
                    .filter(|rev| {
                        rev.function_id == function.id && rev.tenant_id == function.tenant_id
                    })
                    .ok_or_else(|| AppError::not_found("revision not found"))?;
                Ok(TriggerTarget {
                    alias: None,
                    revision_id: Some(id),
                })
            }
            (None, None) => Ok(TriggerTarget::default()),
        }
    }

    fn validate_name(name: &str) -> Result<(), AppError> {
        if name.trim().is_empty()
            || name.chars().count() > MAX_NAME_LEN
            || name.chars().any(char::is_control)
        {
            return Err(invalid(format!(
                "name must be 1..={MAX_NAME_LEN} characters without control characters"
            )));
        }
        Ok(())
    }

    fn validate_cron(&self, spec: &CronSpec) -> Result<(CronSchedule, chrono_tz::Tz), AppError> {
        let schedule = CronSchedule::parse(&spec.expression).map_err(invalid)?;
        let tz = parse_timezone(&spec.timezone).map_err(invalid)?;
        let size = serde_json::to_vec(&spec.payload)
            .map_err(|e| invalid(format!("payload: {e}")))?
            .len() as u64;
        // The fire event wraps the payload with a few hundred bytes.
        let max = self.limits.max_payload_bytes.saturating_sub(1024);
        if size > max {
            return Err(AppError::PayloadTooLarge { size, max });
        }
        Ok((schedule, tz))
    }

    fn validate_webhook(&self, spec: &WebhookSpec) -> Result<(), AppError> {
        if spec.source != WEBHOOK_SOURCE_GENERIC_HMAC {
            return Err(invalid(format!(
                "webhook.source must be `{WEBHOOK_SOURCE_GENERIC_HMAC}`"
            )));
        }
        if spec.tolerance_seconds == 0
            || spec.tolerance_seconds > self.config.webhook_max_tolerance_seconds
        {
            return Err(invalid(format!(
                "webhook.tolerance_seconds must be 1..={}",
                self.config.webhook_max_tolerance_seconds
            )));
        }
        if spec.max_body_bytes == 0
            || spec.max_body_bytes
                > self
                    .config
                    .effective_webhook_max_body_bytes(self.limits.max_payload_bytes)
        {
            return Err(invalid(format!(
                "webhook.max_body_bytes must be 1..={}",
                self.config
                    .effective_webhook_max_body_bytes(self.limits.max_payload_bytes)
            )));
        }
        if !valid_header_name(&spec.event_id_header) {
            return Err(invalid(
                "webhook.event_id_header must be a lowercase header name (a-z, 0-9, -) that is \
                 not the timestamp, signature or a standard header",
            ));
        }
        Ok(())
    }

    fn seal(&self, trigger: &Trigger, secret: &str) -> Result<Vec<u8>, AppError> {
        let key = self.secret_key.as_ref().ok_or_else(|| {
            invalid(
                "webhook triggers need a key to seal their secret: set [triggers] \
                 secret_key_file or secret_key_env",
            )
        })?;
        Ok(key.seal(
            secret.as_bytes(),
            &secret_aad(&trigger.id, &trigger.tenant_id),
        ))
    }

    fn unseal(&self, trigger: &Trigger) -> Result<String, AppError> {
        let sealed = self
            .repo
            .trigger_secret(&trigger.id)?
            .ok_or_else(|| AppError::not_found("trigger not found"))?;
        let key = self
            .secret_key
            .as_ref()
            .ok_or_else(|| AppError::platform("webhook secret key is not configured"))?;
        let plain = key
            .open(&sealed, &secret_aad(&trigger.id, &trigger.tenant_id))
            .ok_or_else(|| {
                AppError::platform("the webhook secret cannot be unsealed with the configured key")
            })?;
        String::from_utf8(plain).map_err(|_| AppError::platform("webhook secret is not UTF-8"))
    }

    pub fn create(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        req: &CreateTriggerRequest,
    ) -> Result<TriggerWithSecret, AppError> {
        require_deploy(principal)?;
        let function = self.owned_function(principal, function_id)?;
        if function.is_deleted() {
            return Err(AppError::FunctionDeleted(format!(
                "function {} is deleted",
                function.id
            )));
        }
        Self::validate_name(&req.name)?;
        let target = self.target_from(&function, &req.target)?;
        let now = self.clock.now();
        let id = TriggerId::from_ulid(self.ids.next_ulid());
        let status = if req.enabled {
            TriggerStatus::Enabled
        } else {
            TriggerStatus::Disabled
        };
        let mut secret = None;
        let (spec, next_fire_at) = match (req.kind.as_str(), &req.cron, &req.webhook) {
            ("cron", Some(c), None) => {
                let spec = CronSpec {
                    expression: c.expression.trim().to_string(),
                    timezone: c.timezone.clone(),
                    payload: c.payload.clone(),
                    missed_run_policy: policy_from(c.missed_run_policy, self.config.max_run_all)?,
                };
                let (schedule, tz) = self.validate_cron(&spec)?;
                let next = req.enabled.then(|| schedule.next_after(tz, now)).flatten();
                (TriggerSpec::Cron(spec), next)
            }
            ("webhook", None, w) => {
                let w = w.clone().unwrap_or_default();
                let s = webhook::generate_secret();
                let spec = WebhookSpec {
                    source: w
                        .source
                        .unwrap_or_else(|| WEBHOOK_SOURCE_GENERIC_HMAC.to_string()),
                    tolerance_seconds: w
                        .tolerance_seconds
                        .unwrap_or(self.config.webhook_default_tolerance_seconds),
                    max_body_bytes: w.max_body_bytes.unwrap_or(
                        self.config
                            .effective_webhook_max_body_bytes(self.limits.max_payload_bytes),
                    ),
                    event_id_header: w
                        .event_id_header
                        .map(|h| h.to_ascii_lowercase())
                        .unwrap_or_else(|| webhook_headers::EVENT_ID.to_string()),
                    secret_fingerprint: webhook::fingerprint(&s),
                };
                self.validate_webhook(&spec)?;
                secret = Some(s);
                (TriggerSpec::Webhook(spec), None)
            }
            ("cron", _, _) => return Err(invalid("a cron trigger takes `cron` and no `webhook`")),
            ("webhook", _, _) => return Err(invalid("a webhook trigger takes no `cron`")),
            (other, _, _) => {
                return Err(invalid(format!(
                    "kind `{other}` is not `cron` or `webhook`"
                )));
            }
        };
        let trigger = Trigger {
            id,
            tenant_id: function.tenant_id.clone(),
            function_id: function.id.clone(),
            name: req.name.clone(),
            target,
            spec,
            status,
            status_reason: None,
            generation: 1,
            next_fire_at,
            last_scheduled_at: None,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };
        let sealed = secret
            .as_deref()
            .map(|s| self.seal(&trigger, s))
            .transpose()?;
        self.repo.insert_trigger(
            trigger.clone(),
            sealed,
            self.config.max_triggers_per_function,
        )?;
        tracing::info!(
            trigger_id = %trigger.id,
            function_id = %trigger.function_id,
            kind = trigger.kind().as_str(),
            enabled = trigger.is_enabled(),
            next_fire_at = ?trigger.next_fire_at,
            "trigger created"
        );
        Ok(TriggerWithSecret { trigger, secret })
    }

    pub fn get(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        trigger_id: &TriggerId,
    ) -> Result<Trigger, AppError> {
        require_read(principal)?;
        self.owned_trigger(principal, function_id, trigger_id)
    }

    pub fn list(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
    ) -> Result<Vec<Trigger>, AppError> {
        require_read(principal)?;
        self.owned_function(principal, function_id)?;
        Ok(self
            .repo
            .list_triggers(function_id)?
            .into_iter()
            .filter(|t| t.tenant_id == principal.tenant_id)
            .collect())
    }

    pub fn update(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        trigger_id: &TriggerId,
        req: &UpdateTriggerRequest,
    ) -> Result<TriggerWithSecret, AppError> {
        require_deploy(principal)?;
        let current = self.owned_trigger(principal, function_id, trigger_id)?;
        if let Some(expected) = req.expected_generation
            && expected != current.generation
        {
            return Err(AppError::Conflict(format!(
                "trigger {} is at generation {}, not {expected}",
                current.id, current.generation
            )));
        }
        let function = self.owned_function(principal, function_id)?;
        let now = self.clock.now();
        let mut next = current.clone();
        if let Some(name) = &req.name {
            Self::validate_name(name)?;
            next.name = name.clone();
        }
        if let Some(target) = &req.target {
            next.target = self.target_from(&function, target)?;
        }
        let mut reschedule = false;
        if let Some(enabled) = req.enabled {
            let was = current.is_enabled();
            next.status = if enabled {
                TriggerStatus::Enabled
            } else {
                TriggerStatus::Disabled
            };
            next.status_reason = None;
            reschedule |= enabled && !was;
        }
        let cron_fields = req.expression.is_some()
            || req.timezone.is_some()
            || req.payload.is_some()
            || req.missed_run_policy.is_some();
        let webhook_fields = req.tolerance_seconds.is_some()
            || req.max_body_bytes.is_some()
            || req.event_id_header.is_some()
            || req.rotate_secret.is_some();
        let mut secret = None;
        match &mut next.spec {
            TriggerSpec::Cron(spec) => {
                if webhook_fields {
                    return Err(invalid("webhook fields do not apply to a cron trigger"));
                }
                if let Some(e) = &req.expression {
                    spec.expression = e.trim().to_string();
                    reschedule = true;
                }
                if let Some(tz) = &req.timezone {
                    spec.timezone = tz.clone();
                    reschedule = true;
                }
                if let Some(p) = &req.payload {
                    spec.payload = p.clone();
                }
                if let Some(p) = req.missed_run_policy {
                    spec.missed_run_policy = policy_from(p, self.config.max_run_all)?;
                }
                let spec = spec.clone();
                let (schedule, tz) = self.validate_cron(&spec)?;
                if !next.is_enabled() {
                    next.next_fire_at = None;
                } else if reschedule || next.next_fire_at.is_none() {
                    // A new schedule starts now: the past of a schedule that
                    // did not exist, or of a disabled one, is not "missed".
                    next.next_fire_at = schedule.next_after(tz, now);
                }
            }
            TriggerSpec::Webhook(spec) => {
                if cron_fields {
                    return Err(invalid("cron fields do not apply to a webhook trigger"));
                }
                if let Some(t) = req.tolerance_seconds {
                    spec.tolerance_seconds = t;
                }
                if let Some(m) = req.max_body_bytes {
                    spec.max_body_bytes = m;
                }
                if let Some(h) = &req.event_id_header {
                    spec.event_id_header = h.to_ascii_lowercase();
                }
                if req.rotate_secret == Some(true) {
                    let s = webhook::generate_secret();
                    spec.secret_fingerprint = webhook::fingerprint(&s);
                    secret = Some(s);
                }
                let spec = spec.clone();
                self.validate_webhook(&spec)?;
            }
        }
        next.generation = current.generation + 1;
        next.updated_at = now;
        let sealed = secret.as_deref().map(|s| self.seal(&next, s)).transpose()?;
        if !self
            .repo
            .update_trigger(next.clone(), current.generation, sealed)?
        {
            return Err(AppError::Conflict(format!(
                "trigger {} changed concurrently; read it again",
                current.id
            )));
        }
        tracing::info!(
            trigger_id = %next.id,
            generation = next.generation,
            status = next.status.as_str(),
            next_fire_at = ?next.next_fire_at,
            rotated_secret = secret.is_some(),
            "trigger updated"
        );
        Ok(TriggerWithSecret {
            trigger: next,
            secret,
        })
    }

    /// Stops every fire whose transaction commits after this one. Its secret
    /// is erased. Fires accepted before continue as ordinary invocations.
    pub fn delete(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        trigger_id: &TriggerId,
    ) -> Result<Trigger, AppError> {
        require_deploy(principal)?;
        let current = self.owned_trigger(principal, function_id, trigger_id)?;
        let now = self.clock.now();
        let mut next = current.clone();
        next.status = TriggerStatus::Deleted;
        next.next_fire_at = None;
        next.deleted_at = Some(now);
        next.updated_at = now;
        next.generation = current.generation + 1;
        if !self
            .repo
            .update_trigger(next.clone(), current.generation, None)?
        {
            return Err(AppError::Conflict(format!(
                "trigger {} changed concurrently; read it again",
                current.id
            )));
        }
        tracing::info!(trigger_id = %next.id, "trigger deleted");
        Ok(next)
    }

    pub fn list_fires(
        &self,
        principal: &Principal,
        function_id: &FunctionId,
        trigger_id: &TriggerId,
        limit: usize,
    ) -> Result<Vec<FireRecord>, AppError> {
        require_read(principal)?;
        let trigger = self.owned_trigger(principal, function_id, trigger_id)?;
        Ok(self.repo.list_fires(&trigger.id, limit.clamp(1, 1000))?)
    }

    // -- firing ----------------------------------------------------------------

    /// The principal a fire is accepted as: the trigger's tenant with the
    /// invoke role, authorized against the cache at every fire.
    fn principal_of(trigger: &Trigger) -> Principal {
        Principal {
            subject: format!("trigger:{}", trigger.id),
            tenant_id: trigger.tenant_id.clone(),
            roles: vec![Role::Invoke],
        }
    }

    fn request(
        trigger: &Trigger,
        payload: serde_json::Value,
        idempotency_key: String,
    ) -> InvokeAsyncRequest {
        InvokeAsyncRequest {
            principal: Self::principal_of(trigger),
            function_id: trigger.function_id.clone(),
            alias: trigger.target.alias.clone(),
            revision_id: trigger.target.revision_id.clone(),
            payload,
            idempotency_key: Some(idempotency_key),
            trace_id: None,
        }
    }

    // -- cron --------------------------------------------------------------

    /// One scheduler pass. The gateway runs it every `[triggers]
    /// scheduler_interval_ms`; tests call it directly.
    pub async fn run_scheduler_once(&self) -> SchedulerReport {
        let report = self.scheduler_pass().await;
        let m = &self.metrics;
        m.scheduler_owner(report.owner);
        m.cron("accepted", report.accepted);
        m.cron("already_fired", report.already_fired);
        m.cron("refused", report.refused);
        m.cron("deferred", report.deferred);
        m.cron("inactive", report.inactive);
        m.missed("run", report.late_run);
        m.missed("skipped", report.skipped_late);
        report
    }

    async fn scheduler_pass(&self) -> SchedulerReport {
        let mut report = SchedulerReport::default();
        if self.dispatcher.is_fenced() {
            return report;
        }
        let now = self.clock.now();
        let owner = self.dispatcher.id().to_string();
        match self
            .repo
            .acquire_scheduler_lease(&owner, now, self.dispatcher.config().lease_ttl())
        {
            Ok(true) => report.owner = true,
            Ok(false) => return report,
            Err(e) => {
                tracing::warn!(error = %e, "cannot take the trigger scheduler lease");
                return report;
            }
        }
        let due = match self
            .repo
            .due_cron_triggers(now, self.config.scheduler_batch)
        {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(error = %e, "cannot list due cron triggers");
                return report;
            }
        };
        report.due_triggers = due.len();
        for trigger in &due {
            self.process_cron_trigger(trigger, now, &mut report).await;
        }
        report.purged_fires = self.purge_if_due(now);
        report
    }

    /// Give the scheduler lease up (graceful shutdown).
    pub fn release_scheduler(&self) {
        if let Err(e) = self
            .repo
            .release_scheduler_lease(self.dispatcher.id().as_str())
        {
            tracing::warn!(error = %e, "cannot release the trigger scheduler lease");
        }
    }

    /// When the next pass is worth running: the earliest due time, at most
    /// one scheduler interval away.
    pub fn next_wake(&self) -> std::time::Duration {
        let interval = self.config.scheduler_interval();
        let Ok(Some(next)) = self.repo.next_cron_fire_at() else {
            return interval;
        };
        let wait = (next - self.clock.now())
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        wait.min(interval)
    }

    fn purge_if_due(&self, now: Timestamp) -> usize {
        let mut last = self.last_purge.lock();
        if last.is_some_and(|t| now - t < chrono::Duration::hours(1)) {
            return 0;
        }
        *last = Some(now);
        drop(last);
        match self.repo.purge_fires(
            now - self.config.webhook_dedup_retention(),
            now - self.config.fire_retention(),
        ) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "purging trigger fire records failed");
                0
            }
        }
    }

    /// Fire the scheduled times of one due trigger (module docs) and move its
    /// cursor.
    pub async fn process_cron_trigger(
        &self,
        trigger: &Trigger,
        now: Timestamp,
        report: &mut SchedulerReport,
    ) {
        let TriggerSpec::Cron(spec) = &trigger.spec else {
            return;
        };
        let (schedule, tz) = match (
            CronSchedule::parse(&spec.expression),
            parse_timezone(&spec.timezone),
        ) {
            (Ok(s), Ok(tz)) => (s, tz),
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!(trigger_id = %trigger.id, error = %e, "stored cron schedule is invalid");
                return;
            }
        };
        let Some(cursor) = trigger.next_fire_at else {
            return;
        };
        let grace_start = now - self.config.grace();
        let window_start = now - self.config.max_catchup();
        let keep_late = match spec.missed_run_policy {
            MissedRunPolicy::Skip => 0,
            MissedRunPolicy::RunOnce => 1,
            MissedRunPolicy::RunAll { max_runs } => max_runs.min(self.config.max_run_all) as usize,
        };
        // Scheduled times in [max(cursor, window_start), now].
        let mut late: VecDeque<Timestamp> = VecDeque::new();
        let mut on_time = Vec::new();
        let mut late_total = 0usize;
        let start = cursor.max(window_start);
        let mut at = start - chrono::Duration::seconds(1);
        if cursor < window_start {
            // Everything before the catch-up window is skipped uncounted.
            tracing::warn!(
                trigger_id = %trigger.id,
                cursor = %cursor,
                max_catchup_seconds = self.config.max_catchup_seconds,
                "scheduled times older than the catch-up window are skipped"
            );
        }
        while let Some(t) = schedule.next_after(tz, at) {
            if t > now {
                break;
            }
            if t < grace_start {
                late_total += 1;
                late.push_back(t);
                if late.len() > keep_late {
                    late.pop_front();
                }
            } else {
                on_time.push(t);
            }
            at = t;
        }
        let skipped = late_total - late.len();
        report.skipped_late += skipped;
        if skipped > 0 {
            tracing::info!(
                trigger_id = %trigger.id,
                skipped,
                policy = ?spec.missed_run_policy,
                "late scheduled times skipped by the missed-run policy"
            );
        }
        let mut last = None;
        report.late_run += late.len();
        for scheduled_at in late.into_iter().chain(on_time) {
            match self.fire_cron(trigger, spec, scheduled_at, now).await {
                Fire::Done => report.accepted += 1,
                Fire::AlreadyFired => report.already_fired += 1,
                Fire::Refused => report.refused += 1,
                Fire::Inactive => {
                    report.inactive += 1;
                    return;
                }
                Fire::Deferred => {
                    report.deferred += 1;
                    // Retry this time on the next pass (subject to the policy).
                    let _ = self.repo.advance_cron_cursor(
                        &trigger.id,
                        trigger.generation,
                        scheduled_at,
                        last,
                    );
                    return;
                }
            }
            last = Some(scheduled_at);
        }
        let Some(next) = schedule.next_after(tz, now) else {
            tracing::warn!(trigger_id = %trigger.id, "cron schedule has no further time");
            return;
        };
        match self
            .repo
            .advance_cron_cursor(&trigger.id, trigger.generation, next, last)
        {
            Ok(true) => {}
            Ok(false) => report.inactive += 1,
            Err(e) => {
                tracing::warn!(trigger_id = %trigger.id, error = %e, "cannot move the cron cursor")
            }
        }
    }

    async fn fire_cron(
        &self,
        trigger: &Trigger,
        spec: &CronSpec,
        scheduled_at: Timestamp,
        now: Timestamp,
    ) -> Fire {
        let fire_key = FireRecord::cron_key(&scheduled_at);
        match self.repo.get_fire(&trigger.id, &fire_key) {
            Ok(Some(_)) => return Fire::AlreadyFired,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(trigger_id = %trigger.id, error = %e, "cannot read fire records");
                return Fire::Deferred;
            }
        }
        let scheduled = scheduled_at.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let record = FireRecord {
            trigger_id: trigger.id.clone(),
            tenant_id: trigger.tenant_id.clone(),
            fire_key,
            kind: TriggerKind::Cron,
            scheduled_at: Some(scheduled_at),
            event_id: None,
            signature_digest: None,
            invocation_id: None,
            outcome: FireOutcome::Accepted,
            reason: None,
            created_at: now,
        };
        let result = match self.cache.authorize_tenant(&trigger.tenant_id).await {
            Err(e) => Err(e),
            Ok(()) => {
                let payload = serde_json::json!({
                    "source": "tachyon.cron",
                    "trigger_id": trigger.id.as_str(),
                    "trigger_name": trigger.name,
                    "scheduled_at": scheduled,
                    "timezone": spec.timezone,
                    "payload": spec.payload,
                });
                let req =
                    Self::request(trigger, payload, format!("cron:{}:{scheduled}", trigger.id));
                let claim = FireClaim {
                    expected_generation: Some(trigger.generation),
                    record: record.clone(),
                };
                self.invoke_async
                    .accept_for_trigger(req, self.repo.as_ref(), claim)
                    .await
            }
        };
        match result {
            Ok(TriggerAcceptance::Accepted(a)) => {
                tracing::info!(
                    trigger_id = %trigger.id,
                    scheduled_at = %scheduled,
                    invocation_id = %a.invocation.id,
                    revision_id = %a.invocation.revision_id,
                    replayed = a.replayed,
                    "cron trigger fired"
                );
                if a.replayed {
                    Fire::AlreadyFired
                } else {
                    Fire::Done
                }
            }
            Ok(TriggerAcceptance::AlreadyFired(_)) => Fire::AlreadyFired,
            Ok(TriggerAcceptance::Inactive(_)) => Fire::Inactive,
            Err(e) if is_transient(&e) => {
                tracing::warn!(
                    trigger_id = %trigger.id,
                    scheduled_at = %scheduled,
                    error = %e,
                    "cron fire deferred"
                );
                Fire::Deferred
            }
            Err(e) => {
                tracing::warn!(
                    trigger_id = %trigger.id,
                    scheduled_at = %scheduled,
                    error = %e,
                    "cron fire refused"
                );
                let refused = FireRecord {
                    outcome: FireOutcome::Refused,
                    reason: Some(format!("{}: {e}", e.code().http_status())),
                    ..record
                };
                if let Err(err) = self.repo.record_fire(refused) {
                    tracing::warn!(trigger_id = %trigger.id, error = %err, "cannot record a refused fire");
                    return Fire::Deferred;
                }
                if matches!(e, AppError::FunctionDeleted(_)) {
                    self.disable_for(trigger, REASON_FUNCTION_DELETED, now);
                    return Fire::Inactive;
                }
                Fire::Refused
            }
        }
    }

    /// The platform disables a trigger (its function was deleted).
    fn disable_for(&self, trigger: &Trigger, reason: &str, now: Timestamp) {
        let mut next = trigger.clone();
        next.status = TriggerStatus::Disabled;
        next.status_reason = Some(reason.to_string());
        next.next_fire_at = None;
        next.generation = trigger.generation + 1;
        next.updated_at = now;
        match self.repo.update_trigger(next, trigger.generation, None) {
            Ok(true) => tracing::warn!(trigger_id = %trigger.id, reason, "trigger disabled"),
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(trigger_id = %trigger.id, error = %e, "cannot disable trigger")
            }
        }
    }

    // -- webhook -------------------------------------------------------------

    /// The webhook trigger a delivery names. Unknown, deleted and cron
    /// triggers are all the same 404: the endpoint is unauthenticated until
    /// the signature is checked.
    pub fn webhook_trigger(&self, id: &TriggerId) -> Result<Trigger, AppError> {
        let found = self
            .repo
            .get_trigger(id)?
            .filter(|t| t.kind() == TriggerKind::Webhook && t.status != TriggerStatus::Deleted);
        found.ok_or_else(|| {
            self.metrics.webhook("not_found");
            AppError::not_found("trigger not found")
        })
    }

    /// A delivery refused by the gateway before the service saw its body
    /// (over `max_body_bytes` while reading).
    pub fn record_webhook_too_large(&self) {
        self.metrics.webhook("too_large");
    }

    /// Verify and accept one delivery (module docs). Every refusal before the
    /// acceptance transaction writes nothing.
    pub async fn receive_webhook(
        &self,
        trigger: &Trigger,
        delivery: &WebhookDelivery,
        body: &[u8],
    ) -> Result<WebhookAcceptance, AppError> {
        let result = self.verify_and_accept(trigger, delivery, body).await;
        self.metrics.webhook(match &result {
            Ok(a) if a.replayed => "replayed",
            Ok(_) => "accepted",
            Err(AppError::Unauthorized(m)) if m.contains("timestamp") => "timestamp_refused",
            Err(AppError::Unauthorized(_)) => "signature_refused",
            Err(AppError::PayloadTooLarge { .. }) => "too_large",
            Err(AppError::InvalidRequest(_)) => "invalid_event_id",
            Err(AppError::Gone(_)) => "disabled",
            Err(AppError::NotFound(_)) => "not_found",
            Err(_) => "refused",
        });
        result
    }

    async fn verify_and_accept(
        &self,
        trigger: &Trigger,
        delivery: &WebhookDelivery,
        body: &[u8],
    ) -> Result<WebhookAcceptance, AppError> {
        let TriggerSpec::Webhook(spec) = &trigger.spec else {
            return Err(AppError::not_found("trigger not found"));
        };
        let size = body.len() as u64;
        if size > spec.max_body_bytes {
            return Err(AppError::PayloadTooLarge {
                size,
                max: spec.max_body_bytes,
            });
        }
        let now = self.clock.now();
        let unauthorized = |e: webhook::VerifyError| {
            tracing::info!(trigger_id = %trigger.id, reason = e.as_str(), "webhook delivery refused");
            AppError::Unauthorized(format!("webhook verification failed: {}", e.as_str()))
        };
        webhook::check_timestamp(
            delivery.timestamp.as_deref(),
            now.timestamp(),
            spec.tolerance_seconds,
        )
        .map_err(unauthorized)?;
        let secret = self.unseal(trigger)?;
        let signature_digest = webhook::verify_signature(
            delivery.signature.as_deref(),
            &secret,
            delivery.timestamp.as_deref().unwrap_or_default(),
            body,
        )
        .map_err(unauthorized)?;
        drop(secret);
        // Only a correctly signed request learns that the trigger is disabled.
        if !trigger.is_enabled() {
            return Err(AppError::Gone(format!(
                "trigger {} is disabled",
                trigger.id
            )));
        }
        let event_id = delivery
            .event_id
            .as_deref()
            .filter(|e| webhook::valid_event_id(e))
            .ok_or_else(|| {
                invalid(format!(
                    "header `{}` must carry an event id of 1..={} visible ASCII characters",
                    spec.event_id_header,
                    webhook::MAX_EVENT_ID_BYTES
                ))
            })?
            .to_string();
        let fire_key = FireRecord::event_key(&event_id);
        // A resend of an accepted event, or of the same signed delivery under
        // another event id: the same invocation, nothing new.
        let seen = match self.repo.get_fire(&trigger.id, &fire_key)? {
            Some(f) => Some(f),
            None => self
                .repo
                .fire_by_signature(&trigger.id, &signature_digest)?,
        };
        if let Some(fire) = seen {
            return self.webhook_replay(trigger, &event_id, fire);
        }
        self.cache.authorize_tenant(&trigger.tenant_id).await?;
        let body_value = match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(v) => serde_json::json!({ "body": v }),
            Err(_) => match std::str::from_utf8(body) {
                Ok(text) => serde_json::json!({ "body_text": text }),
                Err(_) => {
                    use base64::Engine;
                    serde_json::json!({
                        "body_base64": base64::engine::general_purpose::STANDARD.encode(body)
                    })
                }
            },
        };
        let mut payload = serde_json::json!({
            "source": "tachyon.webhook",
            "trigger_id": trigger.id.as_str(),
            "trigger_name": trigger.name,
            "event_id": event_id,
            "content_type": delivery.content_type,
        });
        if let (Some(p), serde_json::Value::Object(b)) = (payload.as_object_mut(), body_value) {
            p.extend(b);
        }
        let record = FireRecord {
            trigger_id: trigger.id.clone(),
            tenant_id: trigger.tenant_id.clone(),
            fire_key,
            kind: TriggerKind::Webhook,
            scheduled_at: None,
            event_id: Some(event_id.clone()),
            signature_digest: Some(signature_digest),
            invocation_id: None,
            outcome: FireOutcome::Accepted,
            reason: None,
            created_at: now,
        };
        let req = Self::request(
            trigger,
            payload,
            format!("webhook:{}:{event_id}", trigger.id),
        );
        let claim = FireClaim {
            expected_generation: None,
            record,
        };
        match self
            .invoke_async
            .accept_for_trigger(req, self.repo.as_ref(), claim)
            .await?
        {
            TriggerAcceptance::Accepted(a) => {
                tracing::info!(
                    trigger_id = %trigger.id,
                    event_id = %event_id,
                    invocation_id = %a.invocation.id,
                    revision_id = %a.invocation.revision_id,
                    replayed = a.replayed,
                    "webhook trigger fired"
                );
                Ok(WebhookAcceptance {
                    trigger_id: trigger.id.clone(),
                    event_id,
                    invocation: a.invocation,
                    replayed: a.replayed,
                })
            }
            TriggerAcceptance::AlreadyFired(fire) => self.webhook_replay(trigger, &event_id, fire),
            TriggerAcceptance::Inactive(current) => match current {
                Some(t) if t.status == TriggerStatus::Disabled => Err(AppError::Gone(format!(
                    "trigger {} is disabled",
                    trigger.id
                ))),
                _ => Err(AppError::not_found("trigger not found")),
            },
        }
    }

    fn webhook_replay(
        &self,
        trigger: &Trigger,
        event_id: &str,
        fire: FireRecord,
    ) -> Result<WebhookAcceptance, AppError> {
        let invocation = fire
            .invocation_id
            .as_ref()
            .and_then(|id| self.repos.invocations.get(id).transpose())
            .transpose()?
            .ok_or_else(|| AppError::Conflict("the earlier delivery has no invocation".into()))?;
        Ok(WebhookAcceptance {
            trigger_id: trigger.id.clone(),
            event_id: fire.event_id.unwrap_or_else(|| event_id.to_string()),
            invocation,
            replayed: true,
        })
    }
}

//! Asynchronous invoke: durable acceptance and the transactional outbox
//! (PLT-4639, docs/adr/0010).
//!
//! Acceptance ([`AsyncInvokeService::accept`]), in this order:
//!
//! 1. authorize and resolve function, alias and revision from the
//!    configuration cache exactly like a synchronous invoke; the revision is
//!    **fixed here** and never re-resolved;
//! 2. validate the input (JSON, size, trace id) and compute its digest;
//! 3. an `Idempotency-Key` already bound answers with that invocation (same
//!    input) or 409 (another input, or a synchronous invocation), before
//!    anything is stored;
//! 4. refuse early when the outbox is over its bound (429 `backlog`), the
//!    queue reported itself full (429 `queue_full`), or the queue is down and
//!    the outbox has no headroom left (503 `queue_unavailable`);
//! 5. an input above `inline_input_max_bytes` is put into the object store
//!    (413 `input_too_large` without a store; 413 / 429 / 503 on the
//!    store's refusals);
//! 6. **one** ledger transaction: invocation (`Accepted`), idempotency
//!    binding, input row, `object_refs` row, outbox event — with the backlog
//!    bound re-checked under the write lock;
//! 7. only after `COMMIT`, the caller answers `202`.
//!
//! Nothing is published to the broker in the request path. The
//! [`OutboxPublisher`] does that later, from the committed row. A failure
//! after the object put leaves an unreferenced object that the object GC
//! collects after `[objects] orphan_grace_seconds` (it is never deleted
//! inline: whether the transaction committed is the ledger's answer, not the
//! error's).

pub mod config;
pub mod consumer;
pub mod publisher;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use tachyon_serverless_api_types::ErrorCode;
use tachyon_serverless_domain::{
    AliasName, Clock, Deadlines, EventKind, FunctionId, IdGenerator, Invocation, InvocationId,
    InvocationMode, Limits, RevisionId, Sha256Digest, TenantId, Timestamp,
};
use tachyon_serverless_durable_port::{
    ObjectError, ObjectRef, ObjectScope, ObjectStore, PutObject, QueueError, Region,
};
use tachyon_serverless_provider_port::Principal;

use crate::authz::require_invoke;
use crate::control::{InvokeGate, Resolved};
use crate::error::AppError;
use crate::failpoints::{self, Failpoints};
use crate::repository::{
    AsyncAcceptOutcome, AsyncInput, AsyncInputBody, AsyncInvocationRepository, BacklogLimits,
    FireClaim, FireRecord, IdempotencyBinding, IdempotencyRepository, InvocationRepository,
    OutboxEvent, OutboxStats, Trigger, TriggerAcceptOutcome, TriggerRepository,
};
use crate::services::invoke::MAX_TRACE_ID_BYTES;

pub use config::InvokeAsyncConfig;
pub use consumer::{AcceptedEvent, read_delivery};
pub use publisher::{OutboxPublisher, PublishReport};

/// Queue topic of asynchronous invoke events.
pub const INVOKE_TOPIC: &str = "invoke";
/// Version of [`InvokeEnvelope`].
pub const ENVELOPE_VERSION: u32 = 1;

/// Why an asynchronous invocation was refused. Nothing was committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncRefusal {
    /// The outbox holds too many, or too old, unpublished events (429).
    Backlog,
    /// The queue refused the last publish as full (429).
    QueueFull,
    /// The queue is unreachable and the outbox has no headroom left (503).
    QueueUnavailable,
    /// The object store could not store the input (503).
    ObjectStoreUnavailable,
    /// The tenant's object quota is exhausted (429).
    ObjectQuota,
    /// The input exceeds what can be stored (413).
    InputTooLarge,
    /// No queue, or no durable ledger (503).
    NotConfigured,
}

impl AsyncRefusal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Backlog => "backlog",
            Self::QueueFull => "queue_full",
            Self::QueueUnavailable => "queue_unavailable",
            Self::ObjectStoreUnavailable => "object_store_unavailable",
            Self::ObjectQuota => "object_quota",
            Self::InputTooLarge => "input_too_large",
            Self::NotConfigured => "not_configured",
        }
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Backlog | Self::QueueFull | Self::ObjectQuota => ErrorCode::CapacityExceeded,
            Self::InputTooLarge => ErrorCode::PayloadTooLarge,
            Self::QueueUnavailable | Self::ObjectStoreUnavailable | Self::NotConfigured => {
                ErrorCode::AsyncUnavailable
            }
        }
    }

    fn err(self, message: impl Into<String>) -> AppError {
        AppError::AsyncRefused {
            reason: self,
            message: message.into(),
        }
    }
}

/// What the last publish told us about the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueCondition {
    Healthy,
    Full,
    Unavailable,
}

/// The queue as the publisher last saw it (process-local).
#[derive(Debug)]
pub struct QueueHealth {
    last: Mutex<(QueueCondition, Option<Timestamp>)>,
}

impl Default for QueueHealth {
    fn default() -> Self {
        Self {
            last: Mutex::new((QueueCondition::Healthy, None)),
        }
    }
}

impl QueueHealth {
    pub fn condition(&self) -> QueueCondition {
        self.last.lock().0
    }

    pub fn since(&self) -> Option<Timestamp> {
        self.last.lock().1
    }

    pub fn record_ok(&self) {
        *self.last.lock() = (QueueCondition::Healthy, None);
    }

    pub fn record_error(&self, error: &QueueError, now: Timestamp) {
        let condition = match error {
            QueueError::QueueFull(_) => QueueCondition::Full,
            QueueError::MessageTooLarge { .. } | QueueError::InvalidMessage(_) => return,
            _ => QueueCondition::Unavailable,
        };
        let mut last = self.last.lock();
        if last.0 != condition {
            *last = (condition, Some(now));
        }
    }
}

/// The routing envelope published for an accepted asynchronous invocation.
/// It names the invocation; the input stays in the ledger or the object
/// store and is read through the ledger ([`read_delivery`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvokeEnvelope {
    pub version: u32,
    pub invocation_id: InvocationId,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    /// Fixed at acceptance.
    pub revision_id: RevisionId,
    pub event_kind: EventKind,
    pub input_digest: Sha256Digest,
    pub input_size_bytes: u64,
    /// `inline` | `object`.
    pub input_storage: String,
    pub accepted_at: Timestamp,
    pub queue_deadline: Timestamp,
    pub trace_id: String,
}

#[derive(Debug, Clone)]
pub struct InvokeAsyncRequest {
    pub principal: Principal,
    pub function_id: FunctionId,
    pub alias: Option<AliasName>,
    pub revision_id: Option<RevisionId>,
    pub payload: serde_json::Value,
    pub idempotency_key: Option<String>,
    pub trace_id: Option<String>,
}

/// What [`AsyncInvokeService::accept_for_trigger`] did.
#[derive(Debug, Clone)]
pub enum TriggerAcceptance {
    /// Accepted now, or an idempotent replay of an earlier acceptance.
    Accepted(Box<AsyncAcceptance>),
    /// The fire key (or signed delivery) was recorded before; nothing new.
    AlreadyFired(FireRecord),
    /// The trigger is gone, not enabled or at another generation; nothing
    /// was written.
    Inactive(Option<Box<Trigger>>),
}

#[derive(Debug, Clone)]
pub struct AsyncAcceptance {
    pub invocation: Invocation,
    /// `inline` | `object`.
    pub input_storage: &'static str,
    /// True when the `Idempotency-Key` matched an earlier acceptance.
    pub replayed: bool,
}

pub struct AsyncInvokeServiceDeps {
    pub ledger: Arc<dyn AsyncInvocationRepository>,
    pub invocations: Arc<dyn InvocationRepository>,
    pub idempotency: Arc<dyn IdempotencyRepository>,
    pub objects: Option<Arc<dyn ObjectStore>>,
    /// The region inputs are stored in (the first of `[objects] regions`).
    pub region: Option<Region>,
    pub gate: Arc<InvokeGate>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub limits: Limits,
    pub config: InvokeAsyncConfig,
    pub failpoints: Arc<Failpoints>,
    pub health: Arc<QueueHealth>,
    pub wake: Arc<Notify>,
}

pub struct AsyncInvokeService {
    ledger: Arc<dyn AsyncInvocationRepository>,
    invocations: Arc<dyn InvocationRepository>,
    idempotency: Arc<dyn IdempotencyRepository>,
    objects: Option<Arc<dyn ObjectStore>>,
    region: Option<Region>,
    gate: Arc<InvokeGate>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    limits: Limits,
    config: InvokeAsyncConfig,
    failpoints: Arc<Failpoints>,
    health: Arc<QueueHealth>,
    wake: Arc<Notify>,
    draining: AtomicBool,
}

impl std::fmt::Debug for AsyncInvokeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncInvokeService")
            .field("objects", &self.objects.as_ref().map(|o| o.backend()))
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The early refusal of step 4 (module docs), also used after the
/// transaction answered `Backlog`.
pub fn backlog_refusal(
    stats: &OutboxStats,
    limits: &BacklogLimits,
    queue: QueueCondition,
    now: Timestamp,
) -> Option<AppError> {
    let over = stats.exceeds(limits, now);
    let detail = || {
        format!(
            "{} unpublished events (max {}), oldest accepted at {}",
            stats.pending,
            limits.max_pending,
            stats
                .oldest_pending_at
                .map_or_else(|| "-".to_string(), |t| t.to_rfc3339())
        )
    };
    match (over, queue) {
        (true, QueueCondition::Unavailable) => Some(AsyncRefusal::QueueUnavailable.err(format!(
            "the queue is unreachable and the outbox is full: {}",
            detail()
        ))),
        (true, QueueCondition::Full) | (false, QueueCondition::Full) if stats.pending > 0 => {
            Some(AsyncRefusal::QueueFull.err(format!(
                "the queue refused the last publish as full: {}",
                detail()
            )))
        }
        (true, _) => Some(
            AsyncRefusal::Backlog.err(format!("the async outbox is over its bound: {}", detail())),
        ),
        (false, _) => None,
    }
}

impl AsyncInvokeService {
    pub fn new(deps: AsyncInvokeServiceDeps) -> Arc<Self> {
        Arc::new(Self {
            ledger: deps.ledger,
            invocations: deps.invocations,
            idempotency: deps.idempotency,
            objects: deps.objects,
            region: deps.region,
            gate: deps.gate,
            clock: deps.clock,
            ids: deps.ids,
            limits: deps.limits,
            config: deps.config,
            failpoints: deps.failpoints,
            health: deps.health,
            wake: deps.wake,
            draining: AtomicBool::new(false),
        })
    }

    pub fn config(&self) -> &InvokeAsyncConfig {
        &self.config
    }

    pub fn backlog_limits(&self) -> BacklogLimits {
        BacklogLimits {
            max_pending: self.config.max_pending_events,
            max_pending_age: self.config.max_pending_age(),
        }
    }

    /// Refuse new acceptances (graceful shutdown). Accepted work stays in the
    /// outbox for the next process.
    pub fn stop_accepting(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub async fn accept(&self, req: InvokeAsyncRequest) -> Result<AsyncAcceptance, AppError> {
        match self.accept_inner(req, None).await? {
            TriggerAcceptance::Accepted(a) => Ok(*a),
            TriggerAcceptance::AlreadyFired(_) | TriggerAcceptance::Inactive(_) => Err(
                AppError::platform("a plain acceptance has no trigger outcome"),
            ),
        }
    }

    /// The same acceptance for a trigger fire (PLT-4641, docs/adr/0014): the
    /// fire row of `claim` is written in the acceptance transaction, which
    /// also re-reads the trigger. `claim.record.invocation_id` is filled in
    /// here. Everything else (authorization and resolution from the cache,
    /// the revision pinned now, input, idempotency, backlog, outbox) is
    /// exactly [`Self::accept`].
    pub async fn accept_for_trigger(
        &self,
        req: InvokeAsyncRequest,
        triggers: &dyn TriggerRepository,
        claim: FireClaim,
    ) -> Result<TriggerAcceptance, AppError> {
        self.accept_inner(req, Some((triggers, claim))).await
    }

    async fn accept_inner(
        &self,
        req: InvokeAsyncRequest,
        trigger: Option<(&dyn TriggerRepository, FireClaim)>,
    ) -> Result<TriggerAcceptance, AppError> {
        require_invoke(&req.principal)?;
        if self.draining.load(Ordering::SeqCst) {
            return Err(AppError::ProviderUnavailable(
                "gateway is shutting down".into(),
            ));
        }
        // 1. Resolution from the configuration cache. No cold-start gate here:
        // nothing starts now; the dispatcher asks the gate when it does. A
        // deleted (deleting or drained) function is refused here exactly like
        // a synchronous invoke: 409 `function_deleted`, `Host.FunctionDeleted`
        // (PLT-4635).
        let Resolved {
            function,
            revision,
            alias,
            ..
        } = self
            .gate
            .cache()
            .resolve(
                &req.principal,
                &req.function_id,
                req.alias.as_ref(),
                req.revision_id.as_ref(),
            )
            .await?;

        // 2. Input.
        let bytes = serde_json::to_vec(&req.payload)
            .map_err(|e| AppError::InvalidRequest(format!("payload is not JSON: {e}")))?;
        let size = bytes.len() as u64;
        if size > self.limits.max_payload_bytes {
            return Err(AppError::PayloadTooLarge {
                size,
                max: self.limits.max_payload_bytes,
            });
        }
        let digest = Sha256Digest::of_bytes(&bytes);
        if let Some(trace) = &req.trace_id
            && trace.len() > MAX_TRACE_ID_BYTES
        {
            return Err(AppError::InvalidRequest(format!(
                "trace id must be at most {MAX_TRACE_ID_BYTES} bytes"
            )));
        }
        let id = InvocationId::from_ulid(self.ids.next_ulid());
        let now = self.clock.now();
        let exec = &revision.spec.execution;
        let queue_deadline = now + self.config.queue_deadline();
        let deadlines = Deadlines {
            queue_deadline,
            init_deadline: None,
            execution_deadline: None,
            client_deadline: queue_deadline
                + chrono::Duration::seconds(
                    i64::from(exec.timeout_seconds)
                        + i64::from(exec.initialization_timeout_seconds),
                ),
        };
        let trace_id = req
            .trace_id
            .clone()
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| id.to_string());
        let invocation = Invocation::accept(
            id.clone(),
            req.principal.tenant_id.clone(),
            function.id.clone(),
            alias,
            revision.id.clone(),
            InvocationMode::Async,
            EventKind::Json,
            deadlines,
            req.idempotency_key.clone(),
            digest.clone(),
            size,
            trace_id.clone(),
            now,
        )?;

        // 3. Idempotency before anything is stored.
        if let Some(key) = &req.idempotency_key
            && let Some(binding) =
                self.idempotency
                    .lookup(&function.tenant_id, &function.id, key, now)?
        {
            return self
                .replay(key, &digest, binding)
                .map(|a| TriggerAcceptance::Accepted(Box::new(a)));
        }

        // 4. Early backlog refusal (re-checked inside the transaction).
        let limits = self.backlog_limits();
        let stats = self.ledger.outbox_stats()?;
        if let Some(refusal) = backlog_refusal(&stats, &limits, self.health.condition(), now) {
            return Err(refusal);
        }

        // 5. Input storage.
        let inline = size <= self.config.inline_input_max_bytes;
        let body = if inline {
            AsyncInputBody::Inline(bytes)
        } else {
            AsyncInputBody::Object(self.put_input(&function.tenant_id, bytes, &digest).await?)
        };
        if self.failpoints.fire(failpoints::ACCEPT_AFTER_OBJECT_PUT) {
            return Err(AppError::platform(
                "failpoint accept.after_object_put: the input was stored, the transaction never ran",
            ));
        }
        let input = AsyncInput {
            invocation_id: id.clone(),
            tenant_id: function.tenant_id.clone(),
            size_bytes: size,
            digest: digest.clone(),
            body,
        };
        let envelope = InvokeEnvelope {
            version: ENVELOPE_VERSION,
            invocation_id: id.clone(),
            tenant_id: function.tenant_id.clone(),
            function_id: function.id.clone(),
            revision_id: revision.id.clone(),
            event_kind: EventKind::Json,
            input_digest: digest.clone(),
            input_size_bytes: size,
            input_storage: input.storage().to_string(),
            accepted_at: now,
            queue_deadline,
            trace_id,
        };
        let input_storage = input.storage();
        let payload = serde_json::to_string(&envelope)
            .map_err(|e| AppError::platform(format!("envelope: {e}")))?;
        let event = OutboxEvent::new(
            id.clone(),
            function.tenant_id.clone(),
            INVOKE_TOPIC,
            payload,
            now,
        );

        // 6. The one transaction.
        let fp = self.failpoints.clone();
        let before_commit = move || fp.fire(failpoints::ACCEPT_BEFORE_COMMIT);
        let outcome = match trigger {
            None => self.ledger.accept_async(
                invocation.clone(),
                input,
                event,
                limits,
                &before_commit,
            )?,
            Some((triggers, mut claim)) => {
                claim.record.invocation_id = Some(id.clone());
                match triggers.accept_trigger_fire(
                    invocation.clone(),
                    input,
                    event,
                    limits,
                    claim,
                    &before_commit,
                )? {
                    TriggerAcceptOutcome::Accepted => AsyncAcceptOutcome::Accepted,
                    TriggerAcceptOutcome::Existing(b) => AsyncAcceptOutcome::Existing(b),
                    TriggerAcceptOutcome::Backlog(s) => AsyncAcceptOutcome::Backlog(s),
                    TriggerAcceptOutcome::AlreadyFired(record) => {
                        return Ok(TriggerAcceptance::AlreadyFired(record));
                    }
                    TriggerAcceptOutcome::Inactive(current) => {
                        return Ok(TriggerAcceptance::Inactive(current));
                    }
                }
            }
        };
        match outcome {
            AsyncAcceptOutcome::Accepted => {}
            AsyncAcceptOutcome::Existing(binding) => {
                let key = req.idempotency_key.as_deref().unwrap_or_default();
                return self
                    .replay(key, &digest, binding)
                    .map(|a| TriggerAcceptance::Accepted(Box::new(a)));
            }
            AsyncAcceptOutcome::Backlog(stats) => {
                return Err(
                    backlog_refusal(&stats, &limits, self.health.condition(), now).unwrap_or_else(
                        || AsyncRefusal::Backlog.err("the async outbox is over its bound"),
                    ),
                );
            }
        }
        if self.failpoints.fire(failpoints::ACCEPT_AFTER_COMMIT) {
            return Err(AppError::platform(
                "failpoint accept.after_commit: committed, but the response was lost",
            ));
        }
        self.wake.notify_one();
        tracing::info!(
            invocation_id = %id,
            tenant_id = %invocation.tenant_id,
            revision_id = %invocation.revision_id,
            input_bytes = size,
            input_storage,
            "asynchronous invocation accepted"
        );
        Ok(TriggerAcceptance::Accepted(Box::new(AsyncAcceptance {
            invocation,
            input_storage,
            replayed: false,
        })))
    }

    async fn put_input(
        &self,
        tenant: &TenantId,
        bytes: Vec<u8>,
        digest: &Sha256Digest,
    ) -> Result<ObjectRef, AppError> {
        let size = bytes.len() as u64;
        let (Some(objects), Some(region)) = (&self.objects, &self.region) else {
            return Err(AsyncRefusal::InputTooLarge.err(format!(
                "the input is {size} bytes; without an object store ([objects]) asynchronous \
                 inputs are limited to inline_input_max_bytes = {}",
                self.config.inline_input_max_bytes
            )));
        };
        let result = if self.failpoints.fire(failpoints::OBJECT_PUT_UNAVAILABLE) {
            Err(ObjectError::Backend(
                "failpoint objects.put_unavailable".into(),
            ))
        } else {
            objects
                .put(PutObject {
                    scope: ObjectScope {
                        tenant_id: tenant.clone(),
                        region: region.clone(),
                    },
                    bytes,
                    ttl: None,
                })
                .await
        };
        let meta = result.map_err(|e| match e {
            ObjectError::TooLarge { size, max } => AsyncRefusal::InputTooLarge.err(format!(
                "the input is {size} bytes, the object store takes at most {max}"
            )),
            ObjectError::QuotaExceeded { .. } => AsyncRefusal::ObjectQuota.err(e.to_string()),
            other => {
                tracing::warn!(error = %other, "object store refused an asynchronous input");
                AsyncRefusal::ObjectStoreUnavailable.err(format!(
                    "the object store is unavailable ({})",
                    other.code()
                ))
            }
        })?;
        if &meta.digest != digest || meta.size_bytes != size {
            return Err(AsyncRefusal::ObjectStoreUnavailable
                .err("the object store recorded another digest than the input"));
        }
        Ok(meta.reference)
    }

    /// Same key and input: the bound asynchronous invocation. Another input,
    /// or a key bound to a synchronous invocation: 409.
    fn replay(
        &self,
        key: &str,
        digest: &Sha256Digest,
        binding: IdempotencyBinding,
    ) -> Result<AsyncAcceptance, AppError> {
        if &binding.input_digest != digest {
            return Err(AppError::IdempotencyConflict {
                key: key.to_string(),
                invocation_id: binding.invocation_id,
            });
        }
        let invocation = self
            .invocations
            .get(&binding.invocation_id)?
            .ok_or_else(|| AppError::not_found("invocation not found"))?;
        if invocation.mode != InvocationMode::Async {
            return Err(AppError::Conflict(format!(
                "idempotency key `{key}` is bound to synchronous invocation {}",
                invocation.id
            )));
        }
        let input_storage = self
            .ledger
            .async_input(&invocation.id)?
            .map_or("inline", |i| i.storage());
        Ok(AsyncAcceptance {
            invocation,
            input_storage,
            replayed: true,
        })
    }
}

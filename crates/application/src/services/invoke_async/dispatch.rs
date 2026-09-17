//! The asynchronous dispatcher: consumer, retries, dead letters and the
//! reaper (PLT-4640, docs/adr/0013).
//!
//! One delivery ([`AsyncDispatcher::handle`], batch = 1):
//!
//! 1. decode the envelope; an undecodable envelope, or one that does not match
//!    its message and the ledger, is **poison**: recorded as a dead letter
//!    (once, keyed by message id and sequence) and terminated, never
//!    redelivered;
//! 2. a terminal invocation is acknowledged without running anything: a
//!    duplicate delivery, or the redelivery of a message whose ACK was lost
//!    after the terminal commit;
//! 3. a delivery of an older generation, or while another run holds a live
//!    claim, is acknowledged (the ledger owns what happens next); one that is
//!    early is NAKed with the remaining delay;
//! 4. age, attempts, function deletion, revision and retry budget are checked;
//!    a dead end is dead-lettered here, a deferral is rescheduled;
//! 5. **claim** the run (a CAS on the dispatch row), renew the claim while it
//!    runs, read the stored input and run it through the synchronous pipeline
//!    ([`InvokeService::run_async`]) on the pinned revision;
//! 6. **settle** in one transaction, fenced by the claim: terminal, dead
//!    letter, or `queued` with the next generation's outbox event;
//! 7. only then **ACK**.
//!
//! Delivery and execution are at-least-once. A handler's external side effect
//! can happen and the process die before step 6: the claim expires and the
//! run is retried (docs/api.md「非同期 invoke の at-least-once 契約」). The
//! ledger never records two terminal outcomes for one invocation.
//!
//! The reaper ([`AsyncDispatcher::reap`]) makes the ledger, not the broker,
//! the source of truth: runs whose claim expired are rescheduled (or
//! dead-lettered when out of attempts), invocations past their age are
//! dead-lettered, and an invocation whose event the broker lost is published
//! again.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use tachyon_serverless_domain::{
    Clock, DeadLetterId, ErrorClass, IdGenerator, Invocation, InvocationError, InvocationStatus,
    PayloadRef, Sha256Digest, Timestamp,
};
use tachyon_serverless_durable_port::{
    ConsumerName, ConsumerSpec, Delivery, EventQueue, ObjectStore, QueueError, Topic,
};
use tachyon_serverless_provider_port::{Principal, Role};

use super::retry::{
    AsyncDispatchConfig, Disposition, EVENT_EXPIRED, INPUT_CORRUPT, INPUT_UNAVAILABLE,
    INVALID_INPUT, RETRY_BUDGET_EXHAUSTED, REVISION_UNAVAILABLE, RUN_ABANDONED, RetryBudget,
    RetryPolicy, classify,
};
use super::{ENVELOPE_VERSION, INVOKE_TOPIC, InvokeEnvelope};
use crate::control::{ControlError, InvokeGate, Resolved};
use crate::error::AppError;
use crate::failpoints::{self, Failpoints};
use crate::metrics::dispatch::AsyncDispatchMetrics;
use crate::repository::outbox::message_id_for;
use crate::repository::{
    AsyncDispatchRepository, AsyncInputBody, AsyncInvocationRepository, ClaimOutcome, ClaimRequest,
    DeadLetter, DeadLetterReason, DeadLetterStatus, DispatchFence, DispatchRecord, DispatchSettle,
    DispatchState, InvocationRepository, OutboxEvent, SettleOutcome,
};
use crate::services::InvokeService;
use crate::services::admission::FUNCTION_DELETED;
use crate::services::invoke::AsyncRun;

/// Subject of the internal principal a run resolves its pinned revision
/// under (the tenant's own; never a credential).
pub const DISPATCHER_SUBJECT: &str = "system:async-dispatcher";

/// What happened to one delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOutcome {
    /// Ran and committed a terminal outcome, then acked.
    Completed { status: &'static str },
    /// Ran (or deferred) and committed the next try, then acked.
    Rescheduled {
        next_attempt_at: Timestamp,
        counted: bool,
    },
    /// Committed a dead letter, then acked.
    DeadLettered(DeadLetterReason),
    /// Nothing to run: already terminal, stale generation, or another live
    /// claim. Acked.
    Skipped(&'static str),
    /// Not due yet: NAKed with the remaining delay.
    NotDue(Timestamp),
    /// Poison: recorded and terminated.
    Poison,
    /// The ledger or the queue failed: left for redelivery.
    Failed(String),
    /// The settle lost its fence (the claim was taken over): acked, the
    /// ledger's current owner decides.
    LostClaim,
}

impl HandleOutcome {
    /// The `outcome` label of `tsls_async_dispatch_deliveries_total`.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Rescheduled { .. } => "rescheduled",
            Self::DeadLettered(_) => "dead_lettered",
            Self::Skipped("terminal") => "skipped_terminal",
            Self::Skipped("stale_generation") => "skipped_stale",
            Self::Skipped(_) => "skipped_claimed",
            Self::NotDue(_) => "not_due",
            Self::Poison => "poison",
            Self::Failed(_) => "failed",
            Self::LostClaim => "lost_claim",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReapReport {
    pub examined: usize,
    /// Runs whose claim expired, rescheduled.
    pub abandoned: usize,
    /// Invocations whose event the broker lost, published again.
    pub republished: usize,
    pub dead_lettered: usize,
    pub lost: usize,
    pub failed: usize,
}

pub struct AsyncDispatcherDeps {
    pub ledger: Arc<dyn AsyncInvocationRepository>,
    pub dispatch: Arc<dyn AsyncDispatchRepository>,
    pub invocations: Arc<dyn InvocationRepository>,
    pub objects: Option<Arc<dyn ObjectStore>>,
    pub queue: Arc<dyn EventQueue>,
    pub invoke: Arc<InvokeService>,
    pub gate: Arc<InvokeGate>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    /// The claim owner: this process's dispatcher id.
    pub owner: String,
    pub config: AsyncDispatchConfig,
    pub failpoints: Arc<Failpoints>,
    /// Wakes the outbox publisher after a retry was scheduled.
    pub publisher_wake: Arc<Notify>,
    /// Full jitter source: a value in `[0, ceiling]`.
    pub jitter: Arc<dyn Fn(u64) -> u64 + Send + Sync>,
    /// `GET /metrics` counters (PLT-4640).
    pub metrics: Arc<AsyncDispatchMetrics>,
}

pub struct AsyncDispatcher {
    ledger: Arc<dyn AsyncInvocationRepository>,
    dispatch: Arc<dyn AsyncDispatchRepository>,
    invocations: Arc<dyn InvocationRepository>,
    objects: Option<Arc<dyn ObjectStore>>,
    queue: Arc<dyn EventQueue>,
    invoke: Arc<InvokeService>,
    gate: Arc<InvokeGate>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    owner: String,
    config: AsyncDispatchConfig,
    failpoints: Arc<Failpoints>,
    publisher_wake: Arc<Notify>,
    jitter: Arc<dyn Fn(u64) -> u64 + Send + Sync>,
    metrics: Arc<AsyncDispatchMetrics>,
    budget: RetryBudget,
    consumer: ConsumerName,
    stopping: AtomicBool,
    in_flight: AtomicUsize,
    idle: Notify,
}

impl std::fmt::Debug for AsyncDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncDispatcher")
            .field("owner", &self.owner)
            .field("consumer", &self.consumer)
            .field("queue", &self.queue.backend())
            .finish_non_exhaustive()
    }
}

/// Decrements the in-flight count when a delivery is done, however it ends.
struct InFlightGuard<'a>(&'a AsyncDispatcher);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.metrics.run_finished();
        if self.0.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// Where a run's decision leads.
struct Decision {
    invocation: Invocation,
    state: DispatchState,
    next_attempt_at: Option<Timestamp>,
    error: Option<InvocationError>,
    counted: bool,
    dead: Option<DeadLetterReason>,
}

impl AsyncDispatcher {
    pub fn new(deps: AsyncDispatcherDeps) -> Result<Arc<Self>, AppError> {
        let consumer = ConsumerName::parse(&deps.config.consumer)
            .map_err(|e| AppError::InvalidRequest(format!("[async_dispatch] consumer: {e}")))?;
        Ok(Arc::new(Self {
            ledger: deps.ledger,
            dispatch: deps.dispatch,
            invocations: deps.invocations,
            objects: deps.objects,
            queue: deps.queue,
            invoke: deps.invoke,
            gate: deps.gate,
            clock: deps.clock,
            ids: deps.ids,
            owner: deps.owner,
            config: deps.config,
            failpoints: deps.failpoints,
            publisher_wake: deps.publisher_wake,
            jitter: deps.jitter,
            metrics: deps.metrics,
            budget: RetryBudget::default(),
            consumer,
            stopping: AtomicBool::new(false),
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
        }))
    }

    pub fn config(&self) -> &AsyncDispatchConfig {
        &self.config
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Create (or confirm) the durable consumer on the `invoke` topic.
    pub async fn ensure_consumer(&self) -> Result<(), QueueError> {
        self.queue
            .ensure_consumer(&ConsumerSpec {
                name: self.consumer.clone(),
                topic: Topic::parse(INVOKE_TOPIC)?,
                ack_wait: self.config.ack_wait(),
                max_deliver: self.config.max_deliver,
            })
            .await
    }

    /// Stop taking deliveries (graceful shutdown).
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Wait until no delivery is being handled, at most `timeout`.
    pub async fn wait_idle(&self, timeout: Duration) {
        let wait = async {
            loop {
                let notified = self.idle.notified();
                if self.in_flight.load(Ordering::SeqCst) == 0 {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(timeout, wait).await;
    }

    /// Pull at most one delivery and handle it. `None` when nothing arrived
    /// within `fetch_wait_ms` (or the dispatcher is stopping).
    pub async fn run_once(&self) -> Option<HandleOutcome> {
        if self.is_stopping() {
            return None;
        }
        let deliveries = match self
            .queue
            .fetch(&self.consumer, 1, self.config.fetch_wait())
            .await
        {
            Ok(d) => d,
            Err(QueueError::ConsumerNotFound(_)) => {
                if let Err(e) = self.ensure_consumer().await {
                    tracing::warn!(error = %e, "async dispatch: creating the consumer failed");
                }
                tokio::time::sleep(self.config.fetch_wait()).await;
                return None;
            }
            Err(e) => {
                tracing::warn!(error = %e, "async dispatch: fetch failed");
                tokio::time::sleep(self.config.fetch_wait()).await;
                return None;
            }
        };
        let delivery = deliveries.into_iter().next()?;
        Some(self.handle(delivery).await)
    }

    async fn ack(&self, delivery: &Delivery) -> Result<(), String> {
        let result = self.queue.ack(&delivery.token).await;
        self.metrics.queue_operation("ack", result.is_ok());
        result.map_err(|e| {
            // The ACK is lost: the message is redelivered and step 2 finds the
            // committed state.
            tracing::warn!(message_id = %delivery.message_id, error = %e, "async dispatch: ack failed; the redelivery settles on the ledger");
            e.to_string()
        })
    }

    async fn nak(&self, delivery: &Delivery, delay: Duration) {
        let result = self.queue.nak(&delivery.token, Some(delay)).await;
        self.metrics.queue_operation("nak", result.is_ok());
        if let Err(e) = result {
            tracing::warn!(message_id = %delivery.message_id, error = %e, "async dispatch: nak failed");
        }
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(self.config.backoff_initial_ms)
    }

    pub async fn handle(&self, delivery: Delivery) -> HandleOutcome {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.metrics.run_started();
        let _guard = InFlightGuard(self);
        match self.handle_inner(&delivery).await {
            Ok(outcome) => {
                self.metrics.delivery(outcome.metric_label());
                tracing::info!(
                    message_id = %delivery.message_id,
                    sequence = delivery.sequence,
                    delivery_count = delivery.delivery_count,
                    outcome = ?outcome,
                    "async dispatch: delivery handled"
                );
                outcome
            }
            Err(e) => {
                self.metrics.delivery("failed");
                tracing::warn!(message_id = %delivery.message_id, error = %e, "async dispatch: the ledger failed; the message is redelivered");
                self.nak(&delivery, self.retry_delay()).await;
                HandleOutcome::Failed(e.to_string())
            }
        }
    }

    async fn handle_inner(&self, delivery: &Delivery) -> Result<HandleOutcome, AppError> {
        // 1. The envelope, and whether it matches its message and the ledger.
        let envelope: InvokeEnvelope = match serde_json::from_slice(&delivery.payload) {
            Ok(e) => e,
            Err(e) => {
                return self
                    .poison(delivery, format!("the envelope is not decodable: {e}"))
                    .await;
            }
        };
        if envelope.version != ENVELOPE_VERSION {
            return self
                .poison(
                    delivery,
                    format!("unsupported envelope version {}", envelope.version),
                )
                .await;
        }
        if message_id_for(&envelope.invocation_id, envelope.generation)
            != delivery.message_id.as_str()
            || envelope.tenant_id != delivery.tenant_id
        {
            return self
                .poison(
                    delivery,
                    "the envelope does not match its message id or routing tenant".into(),
                )
                .await;
        }
        let Some(invocation) = self.invocations.get(&envelope.invocation_id)? else {
            return self
                .poison(
                    delivery,
                    "the event names no invocation of this tenant".into(),
                )
                .await;
        };
        if invocation.tenant_id != delivery.tenant_id
            || invocation.function_id != envelope.function_id
            || invocation.revision_id != envelope.revision_id
            || invocation.input_digest != envelope.input_digest
            || invocation.mode != tachyon_serverless_domain::InvocationMode::Async
        {
            // Indistinguishable from an unknown invocation: a message routed
            // under another tenant reveals nothing about this one.
            return self
                .poison(
                    delivery,
                    "the event names no invocation of this tenant".into(),
                )
                .await;
        }

        // 2. Already terminal: a duplicate, or an ACK lost after the commit.
        if invocation.status.is_terminal() {
            self.ack(delivery).await.ok();
            return Ok(HandleOutcome::Skipped("terminal"));
        }

        // 3. Generation, claim, schedule.
        let now = self.clock.now();
        let record = self
            .dispatch
            .dispatch_record(&invocation.id)?
            .unwrap_or_else(|| DispatchRecord::unclaimed(&invocation));
        if envelope.generation != record.generation {
            self.ack(delivery).await.ok();
            return Ok(HandleOutcome::Skipped("stale_generation"));
        }
        if record.claim_live(now) {
            self.ack(delivery).await.ok();
            return Ok(HandleOutcome::Skipped("claimed"));
        }
        if record.state == DispatchState::Scheduled
            && let Some(at) = record.next_attempt_at
            && at > now
        {
            self.nak(delivery, (at - now).to_std().unwrap_or_default())
                .await;
            return Ok(HandleOutcome::NotDue(at));
        }

        // 4. Before anything runs.
        let policy = self.config.policy_for(&invocation.function_id);
        let fence = DispatchFence::Unclaimed {
            generation: record.generation,
        };
        if let Some(outcome) = self
            .pre_run_dead_end(&invocation, &record, &policy, now)
            .await
        {
            let (reason, error) = outcome;
            return self
                .dead_letter_and_ack(delivery, invocation, &record, fence, reason, error, true)
                .await;
        }
        let resolved = match self.resolve(&invocation).await {
            Ok(r) => r,
            Err(error) => {
                return match classify(&error) {
                    Disposition::DeadLetter(reason) => {
                        self.dead_letter_and_ack(
                            delivery, invocation, &record, fence, reason, error, true,
                        )
                        .await
                    }
                    _ => {
                        self.defer_and_ack(delivery, invocation, &record, fence, &policy, error)
                            .await
                    }
                };
            }
        };
        if record.attempts > 0
            && let Err(next_window) = self.budget.take(&self.config, &invocation.function_id, now)
        {
            let error = InvocationError::new(
                ErrorClass::PlatformError,
                RETRY_BUDGET_EXHAUSTED,
                format!(
                    "the retry budget of function {} ({} per {} s) is spent until {}",
                    invocation.function_id,
                    self.config.retry_budget,
                    self.config.retry_budget_window_seconds,
                    next_window.to_rfc3339()
                ),
            );
            return self
                .schedule_and_ack(
                    delivery,
                    invocation,
                    &record,
                    fence,
                    &policy,
                    error,
                    false,
                    Some(next_window),
                )
                .await;
        }

        // 5. Claim, input, run.
        let claimed = self.dispatch.claim_dispatch(ClaimRequest {
            invocation_id: invocation.id.clone(),
            owner: self.owner.clone(),
            generation: envelope.generation,
            now,
            ttl: self.config.claim_ttl(),
        })?;
        let (invocation, record) = match claimed {
            ClaimOutcome::Claimed { invocation, record } => (*invocation, *record),
            ClaimOutcome::Terminal(_) => {
                self.ack(delivery).await.ok();
                return Ok(HandleOutcome::Skipped("terminal"));
            }
            ClaimOutcome::Stale { .. } => {
                self.ack(delivery).await.ok();
                return Ok(HandleOutcome::Skipped("stale_generation"));
            }
            ClaimOutcome::Held { .. } => {
                self.ack(delivery).await.ok();
                return Ok(HandleOutcome::Skipped("claimed"));
            }
            ClaimOutcome::NotDue { at } => {
                self.nak(delivery, (at - now).to_std().unwrap_or_default())
                    .await;
                return Ok(HandleOutcome::NotDue(at));
            }
        };
        tracing::info!(
            invocation_id = %invocation.id,
            attempt = record.attempts,
            generation = record.generation,
            delivery_count = delivery.delivery_count,
            "asynchronous run claimed"
        );
        let fence = DispatchFence::Claim {
            owner: self.owner.clone(),
            attempts: record.attempts,
        };
        if self.failpoints.fire(failpoints::DISPATCH_AFTER_CLAIM) {
            // As if the process died mid-run: the claim expires and the
            // message is redelivered (or the reaper reschedules it).
            return Ok(HandleOutcome::Failed(
                "failpoint dispatch.after_claim".into(),
            ));
        }
        let payload = match self.read_input(&invocation).await {
            Ok(p) => p,
            Err(error) => {
                return match classify(&error) {
                    Disposition::DeadLetter(reason) => {
                        self.dead_letter_and_ack(
                            delivery, invocation, &record, fence, reason, error, true,
                        )
                        .await
                    }
                    _ => {
                        self.defer_and_ack(delivery, invocation, &record, fence, &policy, error)
                            .await
                    }
                };
            }
        };
        let attempt_base = self.invocations.attempts_of(&invocation.id)?.len() as u32;
        let renewal = self.spawn_renewal(&invocation, record.attempts);
        let report = self
            .invoke
            .run_async(AsyncRun {
                invocation: invocation.clone(),
                function: resolved.function,
                revision: resolved.revision,
                payload,
                attempt_base,
                admission_wait: self.config.admission_wait(),
            })
            .await;
        renewal.abort();
        if self.failpoints.fire(failpoints::DISPATCH_BEFORE_COMMIT) {
            return Ok(HandleOutcome::Failed(
                "failpoint dispatch.before_commit: the run's side effects happened, nothing is \
                 committed"
                    .into(),
            ));
        }

        // 6. Settle, 7. ACK.
        let current = self
            .invocations
            .get(&invocation.id)?
            .ok_or_else(|| AppError::not_found("invocation not found"))?;
        match report.outcome {
            Ok((output, http_status)) => {
                self.complete_and_ack(delivery, current, &record, fence, output, http_status)
                    .await
            }
            Err(error) => match classify(&error) {
                Disposition::Cancelled => self.cancel_and_ack(delivery, current, fence).await,
                Disposition::DeadLetter(reason) => {
                    self.dead_letter_and_ack(delivery, current, &record, fence, reason, error, true)
                        .await
                }
                Disposition::Defer => {
                    self.defer_and_ack(delivery, current, &record, fence, &policy, error)
                        .await
                }
                Disposition::Retry => {
                    self.schedule_and_ack(
                        delivery, current, &record, fence, &policy, error, true, None,
                    )
                    .await
                }
            },
        }
    }

    /// Age, attempts and deletion, before a claim.
    async fn pre_run_dead_end(
        &self,
        invocation: &Invocation,
        record: &DispatchRecord,
        policy: &RetryPolicy,
        now: Timestamp,
    ) -> Option<(DeadLetterReason, InvocationError)> {
        if now >= policy.expires_at(invocation) {
            return Some((
                DeadLetterReason::Expired,
                record.last_error.clone().unwrap_or_else(|| {
                    InvocationError::new(
                        ErrorClass::QueueTimeout,
                        EVENT_EXPIRED,
                        format!(
                            "the event is older than its maximum age ({} s) or queue deadline",
                            policy.max_event_age.num_seconds()
                        ),
                    )
                }),
            ));
        }
        if record.attempts >= policy.max_attempts {
            return Some((
                DeadLetterReason::AttemptsExhausted,
                record.last_error.clone().unwrap_or_else(|| {
                    InvocationError::new(
                        ErrorClass::OutcomeUnknown,
                        RUN_ABANDONED,
                        "every attempt ended without a recorded result",
                    )
                }),
            ));
        }
        if self
            .gate
            .cache()
            .function_deleted(&invocation.function_id)
            .await
        {
            return Some((
                DeadLetterReason::FunctionDeleted,
                InvocationError::new(
                    ErrorClass::PlatformError,
                    FUNCTION_DELETED,
                    "the function was deleted before the invocation could run",
                ),
            ));
        }
        None
    }

    /// The pinned revision, resolved from the configuration cache under the
    /// invocation's own tenant.
    async fn resolve(&self, invocation: &Invocation) -> Result<Resolved, InvocationError> {
        let principal = Principal {
            subject: DISPATCHER_SUBJECT.into(),
            tenant_id: invocation.tenant_id.clone(),
            roles: vec![Role::Invoke],
        };
        self.gate
            .cache()
            .resolve(
                &principal,
                &invocation.function_id,
                None,
                Some(&invocation.revision_id),
            )
            .await
            .map_err(|e| match e {
                AppError::FunctionDeleted(m) => {
                    InvocationError::new(ErrorClass::PlatformError, FUNCTION_DELETED, m)
                }
                AppError::NotFound(m) | AppError::RevisionNotReady(m) => InvocationError::new(
                    ErrorClass::PlatformError,
                    REVISION_UNAVAILABLE,
                    format!(
                        "the pinned revision {} cannot run: {m}",
                        invocation.revision_id
                    ),
                ),
                AppError::Control { kind, message } => {
                    InvocationError::new(ErrorClass::PlatformError, kind.error_type(), message)
                }
                other => InvocationError::new(
                    ErrorClass::PlatformError,
                    ControlError::StoreUnavailable.error_type(),
                    other.to_string(),
                ),
            })
            .and_then(|r| {
                if r.revision.id == invocation.revision_id {
                    Ok(r)
                } else {
                    Err(InvocationError::new(
                        ErrorClass::PlatformError,
                        REVISION_UNAVAILABLE,
                        "the pinned revision did not resolve",
                    ))
                }
            })
    }

    /// The stored input, from the ledger (inline) or the object store under
    /// the invocation's own tenant, checked against the accepted digest.
    async fn read_input(
        &self,
        invocation: &Invocation,
    ) -> Result<serde_json::Value, InvocationError> {
        let input = match self.ledger.async_input(&invocation.id) {
            Ok(Some(i)) if i.tenant_id == invocation.tenant_id => i,
            Ok(_) => {
                return Err(InvocationError::new(
                    ErrorClass::PlatformError,
                    INPUT_CORRUPT,
                    "the invocation has no stored input",
                ));
            }
            Err(e) => {
                return Err(InvocationError::new(
                    ErrorClass::PlatformError,
                    INPUT_UNAVAILABLE,
                    format!("reading the input: {e}"),
                ));
            }
        };
        let bytes = match input.body {
            AsyncInputBody::Inline(bytes) => bytes,
            AsyncInputBody::Object(reference) => {
                if reference.scope.tenant_id != invocation.tenant_id {
                    return Err(InvocationError::new(
                        ErrorClass::PlatformError,
                        INPUT_CORRUPT,
                        "the input object is not the invocation's",
                    ));
                }
                let Some(objects) = &self.objects else {
                    return Err(InvocationError::new(
                        ErrorClass::PlatformError,
                        INPUT_UNAVAILABLE,
                        "the input is in the object store, but none is configured",
                    ));
                };
                match objects.get(&reference.scope, &reference.id).await {
                    Ok(o) => o.bytes,
                    Err(e) => {
                        return Err(InvocationError::new(
                            ErrorClass::PlatformError,
                            INPUT_UNAVAILABLE,
                            format!("reading the input object ({})", e.code()),
                        ));
                    }
                }
            }
        };
        if Sha256Digest::of_bytes(&bytes) != invocation.input_digest {
            return Err(InvocationError::new(
                ErrorClass::PlatformError,
                INPUT_CORRUPT,
                "the stored input does not match the accepted digest",
            ));
        }
        serde_json::from_slice(&bytes).map_err(|e| {
            InvocationError::new(
                ErrorClass::PlatformError,
                INVALID_INPUT,
                format!("the input is not a JSON event: {e}"),
            )
        })
    }

    fn spawn_renewal(&self, invocation: &Invocation, attempts: u32) -> tokio::task::JoinHandle<()> {
        let dispatch = self.dispatch.clone();
        let clock = self.clock.clone();
        let owner = self.owner.clone();
        let id = invocation.id.clone();
        let ttl = self.config.claim_ttl();
        let every = (ttl / 3)
            .to_std()
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_millis(100));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                match dispatch.renew_dispatch_claim(&id, &owner, attempts, clock.now(), ttl) {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(invocation_id = %id, "async dispatch: the claim was taken over while running; this run's outcome will be refused");
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(invocation_id = %id, error = %e, "async dispatch: renewing the claim failed");
                    }
                }
            }
        })
    }

    // -- settles ---------------------------------------------------------

    async fn complete_and_ack(
        &self,
        delivery: &Delivery,
        mut invocation: Invocation,
        record: &DispatchRecord,
        fence: DispatchFence,
        output: Option<PayloadRef>,
        http_status: Option<u16>,
    ) -> Result<HandleOutcome, AppError> {
        let now = self.clock.now();
        if let Err(e) = invocation.mark_succeeded(output, http_status, now) {
            tracing::warn!(invocation_id = %invocation.id, error = %e, "async dispatch: cannot record success");
            let error = InvocationError::new(
                ErrorClass::OutcomeUnknown,
                "Host.OutcomeUnknown",
                format!("the run succeeded but could not be recorded: {e}"),
            );
            let policy = self.config.policy_for(&invocation.function_id);
            return self
                .schedule_and_ack(
                    delivery, invocation, record, fence, &policy, error, true, None,
                )
                .await;
        }
        let decision = Decision {
            invocation,
            state: DispatchState::Done,
            next_attempt_at: None,
            error: None,
            counted: true,
            dead: None,
        };
        self.commit_and_ack(delivery, record, fence, decision).await
    }

    async fn cancel_and_ack(
        &self,
        delivery: &Delivery,
        mut invocation: Invocation,
        fence: DispatchFence,
    ) -> Result<HandleOutcome, AppError> {
        let _ = invocation.mark_cancelled(self.clock.now());
        let record = self
            .dispatch
            .dispatch_record(&invocation.id)?
            .unwrap_or_else(|| DispatchRecord::unclaimed(&invocation));
        let decision = Decision {
            invocation,
            state: DispatchState::Done,
            next_attempt_at: None,
            error: None,
            counted: true,
            dead: None,
        };
        self.commit_and_ack(delivery, &record, fence, decision)
            .await
    }

    /// A deferral: rescheduled without counting.
    async fn defer_and_ack(
        &self,
        delivery: &Delivery,
        invocation: Invocation,
        record: &DispatchRecord,
        fence: DispatchFence,
        policy: &RetryPolicy,
        error: InvocationError,
    ) -> Result<HandleOutcome, AppError> {
        self.schedule_and_ack(
            delivery, invocation, record, fence, policy, error, false, None,
        )
        .await
    }

    /// Schedule the next try (or dead-letter when out of attempts or age).
    #[allow(clippy::too_many_arguments)]
    async fn schedule_and_ack(
        &self,
        delivery: &Delivery,
        invocation: Invocation,
        record: &DispatchRecord,
        fence: DispatchFence,
        policy: &RetryPolicy,
        error: InvocationError,
        counted: bool,
        not_before: Option<Timestamp>,
    ) -> Result<HandleOutcome, AppError> {
        let now = self.clock.now();
        let from_claim = matches!(fence, DispatchFence::Claim { .. });
        let attempts = if from_claim && !counted {
            record.attempts.saturating_sub(1)
        } else {
            record.attempts
        };
        if counted && attempts >= policy.max_attempts {
            return self
                .dead_letter_and_ack(
                    delivery,
                    invocation,
                    record,
                    fence,
                    DeadLetterReason::AttemptsExhausted,
                    error,
                    true,
                )
                .await;
        }
        let tries = if counted {
            attempts
        } else {
            record.deferrals.saturating_add(1)
        };
        let delay = self.config.backoff(tries.max(1), self.jitter.as_ref());
        let mut next = now + delay;
        if let Some(at) = not_before {
            next = next.max(at);
        }
        if next >= policy.expires_at(&invocation) {
            return self
                .dead_letter_and_ack(
                    delivery,
                    invocation,
                    record,
                    fence,
                    DeadLetterReason::Expired,
                    error,
                    counted,
                )
                .await;
        }
        let mut invocation = invocation;
        if let Err(e) = invocation.mark_requeued() {
            return Err(AppError::platform(format!(
                "cannot requeue invocation {}: {e}",
                invocation.id
            )));
        }
        if self
            .failpoints
            .fire(failpoints::DISPATCH_BEFORE_RETRY_COMMIT)
        {
            return Ok(HandleOutcome::Failed(
                "failpoint dispatch.before_retry_commit".into(),
            ));
        }
        let decision = Decision {
            invocation,
            state: DispatchState::Scheduled,
            next_attempt_at: Some(next),
            error: Some(error),
            counted,
            dead: None,
        };
        self.commit_and_ack(delivery, record, fence, decision).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dead_letter_and_ack(
        &self,
        delivery: &Delivery,
        mut invocation: Invocation,
        record: &DispatchRecord,
        fence: DispatchFence,
        reason: DeadLetterReason,
        error: InvocationError,
        counted: bool,
    ) -> Result<HandleOutcome, AppError> {
        settle_failed(&mut invocation, &error, self.clock.now());
        let decision = Decision {
            invocation,
            state: DispatchState::Dead,
            next_attempt_at: None,
            error: Some(error),
            counted,
            dead: Some(reason),
        };
        self.commit_and_ack(delivery, record, fence, decision).await
    }

    async fn commit_and_ack(
        &self,
        delivery: &Delivery,
        record: &DispatchRecord,
        fence: DispatchFence,
        decision: Decision,
    ) -> Result<HandleOutcome, AppError> {
        let outcome = match (&decision.dead, decision.state) {
            (Some(reason), _) => HandleOutcome::DeadLettered(*reason),
            (None, DispatchState::Scheduled) => HandleOutcome::Rescheduled {
                next_attempt_at: decision.next_attempt_at.unwrap_or_else(|| self.clock.now()),
                counted: decision.counted,
            },
            (None, _) => HandleOutcome::Completed {
                status: decision.invocation.status.name(),
            },
        };
        match self.commit(record, fence, decision)? {
            SettleOutcome::Committed => {}
            SettleOutcome::Lost(reason) => {
                tracing::warn!(message_id = %delivery.message_id, reason = %reason, "async dispatch: the settle lost its fence; nothing written");
                self.ack(delivery).await.ok();
                return Ok(HandleOutcome::LostClaim);
            }
        }
        if self.failpoints.fire(failpoints::DISPATCH_AFTER_COMMIT) {
            return Ok(HandleOutcome::Failed(
                "failpoint dispatch.after_commit: committed, the ACK was lost".into(),
            ));
        }
        self.ack(delivery).await.ok();
        Ok(outcome)
    }

    /// The settle transaction for `decision`.
    fn commit(
        &self,
        record: &DispatchRecord,
        fence: DispatchFence,
        decision: Decision,
    ) -> Result<SettleOutcome, AppError> {
        let now = self.clock.now();
        let Decision {
            invocation,
            state,
            next_attempt_at,
            error,
            counted,
            dead,
        } = decision;
        let republish = match (state, next_attempt_at) {
            (DispatchState::Scheduled, Some(at)) => {
                Some(self.next_event(&invocation, record, at)?)
            }
            _ => None,
        };
        let from_claim = matches!(fence, DispatchFence::Claim { .. });
        let dead_letter = match dead {
            Some(reason) => Some(self.dead_letter_for(
                &invocation,
                record,
                reason,
                &error,
                counted,
                from_claim,
            )?),
            None => None,
        };
        let status = invocation.status.name();
        let id = invocation.id.clone();
        let outcome = self.dispatch.settle_dispatch(DispatchSettle {
            fence,
            invocation,
            state,
            next_attempt_at,
            last_error: error.clone(),
            counted,
            dead_letter: dead_letter.clone(),
            republish,
            now,
        })?;
        if outcome == SettleOutcome::Committed {
            match (state, &dead_letter) {
                (DispatchState::Scheduled, _) => self.metrics.retry_scheduled(counted),
                (_, Some(d)) => self.metrics.dead_letter(d.reason.as_str()),
                _ => {}
            }
            match (state, &dead_letter) {
                (DispatchState::Scheduled, _) => {
                    self.publisher_wake.notify_one();
                    tracing::info!(
                        invocation_id = %id,
                        next_attempt_at = %next_attempt_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
                        counted,
                        error_type = error.as_ref().map(|e| e.error_type.as_str()).unwrap_or(""),
                        "asynchronous invocation rescheduled"
                    );
                }
                (_, Some(d)) => tracing::warn!(
                    invocation_id = %id,
                    dead_letter_id = %d.id,
                    reason = d.reason.as_str(),
                    attempts = d.attempts,
                    "asynchronous invocation dead-lettered"
                ),
                _ => tracing::info!(invocation_id = %id, status, "asynchronous invocation settled"),
            }
        }
        Ok(outcome)
    }

    fn next_event(
        &self,
        invocation: &Invocation,
        record: &DispatchRecord,
        due: Timestamp,
    ) -> Result<OutboxEvent, AppError> {
        let input = self
            .ledger
            .async_input(&invocation.id)?
            .ok_or_else(|| AppError::platform("the invocation has no stored input"))?;
        let generation = record.generation + 1;
        let envelope = InvokeEnvelope {
            version: ENVELOPE_VERSION,
            invocation_id: invocation.id.clone(),
            tenant_id: invocation.tenant_id.clone(),
            function_id: invocation.function_id.clone(),
            revision_id: invocation.revision_id.clone(),
            event_kind: invocation.event_kind,
            input_digest: invocation.input_digest.clone(),
            input_size_bytes: invocation.input_size_bytes,
            input_storage: input.storage().to_string(),
            accepted_at: invocation.accepted_at,
            queue_deadline: invocation.deadlines.queue_deadline,
            trace_id: invocation.trace_id.clone(),
            generation,
        };
        let payload = serde_json::to_string(&envelope)
            .map_err(|e| AppError::platform(format!("envelope: {e}")))?;
        // `created_at` is when the event becomes due: the outbox backlog age
        // counts from then, so a retry scheduled far ahead never makes new
        // acceptances look backlogged.
        let mut event = OutboxEvent::new(
            invocation.id.clone(),
            invocation.tenant_id.clone(),
            INVOKE_TOPIC,
            payload,
            due,
        );
        event.generation = generation;
        Ok(event)
    }

    fn dead_letter_for(
        &self,
        invocation: &Invocation,
        record: &DispatchRecord,
        reason: DeadLetterReason,
        error: &Option<InvocationError>,
        counted: bool,
        from_claim: bool,
    ) -> Result<DeadLetter, AppError> {
        let now = self.clock.now();
        let input = self.ledger.async_input(&invocation.id)?;
        // The same adjustment the settle transaction makes to the dispatch row.
        let (attempts, deferrals) = match (counted, from_claim) {
            (true, _) => (record.attempts, record.deferrals),
            (false, true) => (
                record.attempts.saturating_sub(1),
                record.deferrals.saturating_add(1),
            ),
            (false, false) => (record.attempts, record.deferrals.saturating_add(1)),
        };
        Ok(DeadLetter {
            id: DeadLetterId::from_ulid(self.ids.next_ulid()),
            tenant_id: invocation.tenant_id.clone(),
            function_id: Some(invocation.function_id.clone()),
            invocation_id: Some(invocation.id.clone()),
            revision_id: Some(invocation.revision_id.clone()),
            reason,
            status: DeadLetterStatus::Open,
            attempts,
            deferrals,
            last_error: error.clone(),
            accepted_at: Some(invocation.accepted_at),
            first_attempt_at: record.first_attempt_at,
            last_attempt_at: record.last_attempt_at,
            created_at: now,
            input_digest: Some(invocation.input_digest.clone()),
            input_size_bytes: Some(invocation.input_size_bytes),
            input_storage: input.map(|i| i.storage().to_string()),
            message_id: None,
            message_sequence: None,
            detail: None,
            redrive_count: 0,
            redriven_at: None,
        })
    }

    async fn poison(&self, delivery: &Delivery, detail: String) -> Result<HandleOutcome, AppError> {
        let dead = DeadLetter {
            id: DeadLetterId::from_ulid(self.ids.next_ulid()),
            tenant_id: delivery.tenant_id.clone(),
            function_id: None,
            invocation_id: None,
            revision_id: None,
            reason: DeadLetterReason::Poison,
            status: DeadLetterStatus::Open,
            attempts: 0,
            deferrals: 0,
            last_error: None,
            accepted_at: None,
            first_attempt_at: None,
            last_attempt_at: None,
            created_at: self.clock.now(),
            input_digest: Some(Sha256Digest::of_bytes(&delivery.payload)),
            input_size_bytes: Some(delivery.payload.len() as u64),
            input_storage: None,
            message_id: Some(delivery.message_id.to_string()),
            message_sequence: Some(delivery.sequence),
            detail: Some(detail.clone()),
            redrive_count: 0,
            redriven_at: None,
        };
        let recorded = self.dispatch.record_poison(dead)?;
        if recorded {
            self.metrics.dead_letter("poison");
        }
        tracing::warn!(
            message_id = %delivery.message_id,
            sequence = delivery.sequence,
            delivery_count = delivery.delivery_count,
            recorded,
            detail = %detail,
            "async dispatch: poison event dead-lettered and terminated"
        );
        let result = self.queue.term(&delivery.token).await;
        self.metrics.queue_operation("term", result.is_ok());
        if let Err(e) = result {
            tracing::warn!(message_id = %delivery.message_id, error = %e, "async dispatch: term failed; the redelivery is recognised by its dead letter");
        }
        Ok(HandleOutcome::Poison)
    }

    // -- reaper ------------------------------------------------------------

    /// One reaper pass (module docs). The gateway runs it every
    /// `reaper_interval_seconds`; tests call it directly.
    pub async fn reap(&self) -> ReapReport {
        let mut report = ReapReport::default();
        let now = self.clock.now();
        // A lost event only matters when the consumer has nothing left:
        // otherwise the event may simply still be waiting its turn.
        let queue_idle = match self.queue.stats(&self.consumer).await {
            Ok(s) => s.pending == 0 && s.ack_pending == 0,
            Err(_) => false,
        };
        let candidates = match self.dispatch.dispatch_candidates(
            now,
            now - self.config.stall_timeout(),
            now - self.config.min_event_age(),
            256,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "async reaper: listing candidates failed");
                report.failed += 1;
                return report;
            }
        };
        for candidate in candidates {
            report.examined += 1;
            let invocation = candidate.invocation;
            let record = candidate.record;
            let policy = self.config.policy_for(&invocation.function_id);
            let fence = DispatchFence::Unclaimed {
                generation: record.generation,
            };
            let abandoned = record.state == DispatchState::Running && !record.claim_live(now);
            let decision = if abandoned || now >= policy.expires_at(&invocation) {
                let (reason, error) = if now >= policy.expires_at(&invocation) {
                    (
                        DeadLetterReason::Expired,
                        record.last_error.clone().unwrap_or_else(|| {
                            InvocationError::new(
                                ErrorClass::QueueTimeout,
                                EVENT_EXPIRED,
                                "the event is older than its maximum age or queue deadline",
                            )
                        }),
                    )
                } else {
                    (
                        DeadLetterReason::AttemptsExhausted,
                        InvocationError::new(
                            ErrorClass::OutcomeUnknown,
                            RUN_ABANDONED,
                            "the dispatcher running this attempt stopped before recording a \
                             result; the handler may have run",
                        ),
                    )
                };
                if abandoned
                    && reason == DeadLetterReason::AttemptsExhausted
                    && record.attempts < policy.max_attempts
                {
                    // Retry the abandoned run (its attempt already counted).
                    let mut inv = invocation.clone();
                    if inv.mark_requeued().is_err() {
                        continue;
                    }
                    let next = now
                        + self
                            .config
                            .backoff(record.attempts.max(1), self.jitter.as_ref());
                    report.abandoned += 1;
                    Decision {
                        invocation: inv,
                        state: DispatchState::Scheduled,
                        next_attempt_at: Some(next),
                        error: Some(error),
                        counted: true,
                        dead: None,
                    }
                } else {
                    let mut inv = invocation.clone();
                    settle_failed(&mut inv, &error, now);
                    report.dead_lettered += 1;
                    Decision {
                        invocation: inv,
                        state: DispatchState::Dead,
                        next_attempt_at: None,
                        error: Some(error),
                        counted: true,
                        dead: Some(reason),
                    }
                }
            } else {
                // A lost event: no unpublished event, nothing claimed, quiet
                // for `stall_timeout_seconds`, and the consumer is idle.
                let unpublished = candidate
                    .outbox
                    .as_ref()
                    .is_some_and(|o| o.sent_at.is_none());
                if unpublished || !queue_idle {
                    continue;
                }
                let mut inv = invocation.clone();
                if inv.mark_requeued().is_err() {
                    continue;
                }
                report.republished += 1;
                Decision {
                    invocation: inv,
                    state: DispatchState::Scheduled,
                    next_attempt_at: Some(now),
                    error: None,
                    counted: true,
                    dead: None,
                }
            };
            match self.commit(&record, fence, decision) {
                Ok(SettleOutcome::Committed) => {}
                Ok(SettleOutcome::Lost(_)) => report.lost += 1,
                Err(e) => {
                    tracing::warn!(invocation_id = %invocation.id, error = %e, "async reaper: settle failed");
                    report.failed += 1;
                }
            }
        }
        self.metrics.reaper("abandoned", report.abandoned);
        self.metrics.reaper("republished", report.republished);
        self.metrics.reaper("dead_lettered", report.dead_lettered);
        self.metrics.reaper("lost", report.lost);
        self.metrics.reaper("failed", report.failed);
        if report != ReapReport::default() && report.examined > 0 {
            tracing::info!(?report, "async reaper pass");
        }
        report
    }
}

/// Make `invocation` terminal with `error` (an unknown outcome only when it
/// was dispatched).
fn settle_failed(invocation: &mut Invocation, error: &InvocationError, now: Timestamp) {
    match error.class {
        ErrorClass::OutcomeUnknown if invocation.status == InvocationStatus::Running => {
            if invocation
                .mark_outcome_unknown(error.message.clone(), now)
                .is_ok()
                && let InvocationStatus::OutcomeUnknown { error: e } = &mut invocation.status
            {
                e.error_type = error.error_type.clone();
            }
        }
        ErrorClass::OutcomeUnknown | ErrorClass::Cancelled => {
            let _ = invocation.mark_failed(
                InvocationError::new(
                    ErrorClass::PlatformError,
                    error.error_type.clone(),
                    error.message.clone(),
                ),
                now,
            );
        }
        _ => {
            let _ = invocation.mark_failed(error.clone(), now);
        }
    }
}

//! Synchronous invoke pipeline (docs/architecture.md §3).
//!
//! `invoke` performs the synchronous part (authz, resolution, limits,
//! validation, idempotency, acceptance) and then spawns a *driver* task that
//! owns the rest of the lifecycle: capacity, environment creation, handshake,
//! ready, attempt + lease, invoke, result classification, cleanup and usage
//! events. The caller only awaits a completion signal, so a client that
//! disconnects does not abort the invocation: the driver keeps tracking it
//! until its deadlines and records the outcome.
//!
//! Ownership and fencing (PLT-4631): every invocation is accepted under this
//! process's [`Dispatcher`]; the slot an attempt runs on is taken with one
//! atomic [`crate::repository::SlotStore::acquire`] (epoch bump, owned lease,
//! attempt, `Running`) and its result is written with one fenced
//! [`crate::repository::SlotStore::complete`], which the store refuses when
//! the lease was reclaimed in the meantime. A dispatched attempt is never
//! retried: a failure after dispatch is `Failed` / `OutcomeUnknown`, and
//! external side effects of the handler are at-least-once-or-unknown, never
//! exactly-once (docs/threat-model.md §9, §10).

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use futures::FutureExt;
use parking_lot::Mutex;
use tokio::sync::watch;

use tachyon_serverless_domain::{
    AliasName, AttemptId, AttemptKind, Clock, Deadlines, EnvironmentId, ErrorClass, EventKind,
    ExecutionEnvironment, ExecutionLease, Function, FunctionId, FunctionRevision, IdGenerator,
    Invocation, InvocationAttempt, InvocationError, InvocationId, InvocationMode, LeaseId, Limits,
    LogPhase, Metered, PayloadRef, ResourceProfile, ReuseKey, RevisionId, Sha256Digest, StartKind,
    Timestamp, UsageBytes, UsageEvent, UsageEventType, UsageOutcome, UsageResources, UsageSegments,
};
use tachyon_serverless_protocol::GuestErrorKind;
use tachyon_serverless_provider_port::{
    ArtifactLocation, ArtifactStore, EnvironmentSpec, EnvironmentStats, ExecutionProvider,
    Principal, ProviderError, SecretDeliveryContext, SecretError, SecretProvider, TerminateReason,
    UsageSink,
};

use crate::authz::{ensure_tenant, require_invoke};
use crate::bridge_session::{
    BridgeSession, HelloAckParams, InvokeParams, LogContext, LogForwarder, Outcome, SessionError,
};
use crate::config::{CapacityConfig, InvokeConfig};
use crate::control::{InvokeGate, Resolved};
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::repository::{
    AcquireOutcome, CompletionOutcome, IdempotencyBinding, IdempotencyOutcome, Repositories,
    SlotAcquire, SlotCompletion,
};
use crate::services::Dispatcher;
use crate::services::admission::{
    AdmissionController, FUNCTION_DELETED, Grant, GrantKind, Pending, RejectReason, WaitError,
    error_type_for,
};
use crate::services::history::{HistoryService, InvocationDetail};
use crate::services::pool::{
    EnvironmentPool, WarmEnvironment, WarmStartTimings, environment_lifetime_ms, reuse_key_for,
    secret_binding_generation,
};
use crate::usage::rating::ceil_ms;
use crate::usage::{MeteringAdmission, UsageMeter};

/// Upper bound of a caller-supplied trace id. Together with the fixed-size
/// ids it keeps the `Invoke` envelope within
/// [`crate::config::FRAME_ENVELOPE_RESERVE_BYTES`].
pub const MAX_TRACE_ID_BYTES: usize = 256;

/// How long to keep reading after a failed `Invoke` write, for frames the
/// guest queued before it closed the connection.
const UNDELIVERED_DRAIN: Duration = Duration::from_millis(200);

/// How often a replay of an invocation another process drives re-reads it.
const LEDGER_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct InvokeRequest {
    pub principal: Principal,
    pub function_id: FunctionId,
    /// Alias to resolve when `revision_id` is not pinned (default `prod`).
    pub alias: Option<AliasName>,
    pub revision_id: Option<RevisionId>,
    pub event_kind: EventKind,
    pub payload: serde_json::Value,
    pub idempotency_key: Option<String>,
    pub client_timeout_ms: Option<u64>,
    pub trace_id: Option<String>,
}

/// Result of a synchronous invoke: the ledger view plus, when the handler
/// produced one, the full response payload (which may exceed the inline
/// cap stored in the ledger).
#[derive(Debug, Clone)]
pub struct InvokeOutcome {
    pub detail: InvocationDetail,
    pub output: Option<serde_json::Value>,
    /// True when an idempotency key matched an earlier invocation.
    pub replayed: bool,
}

impl InvokeOutcome {
    pub fn invocation(&self) -> &Invocation {
        &self.detail.invocation
    }

    pub fn succeeded(&self) -> bool {
        matches!(
            self.invocation().status,
            tachyon_serverless_domain::InvocationStatus::Succeeded
        )
    }

    /// The API error to return when the invocation did not succeed.
    pub fn error(&self) -> Option<AppError> {
        use tachyon_serverless_domain::InvocationStatus as S;
        let inv = self.invocation();
        let error = match &inv.status {
            S::Succeeded => return None,
            S::Failed { error } | S::OutcomeUnknown { error } => error.clone(),
            S::Cancelled => InvocationError::new(
                ErrorClass::Cancelled,
                "Host.Cancelled",
                "invocation was cancelled",
            ),
            S::Accepted | S::Queued | S::Running => InvocationError::new(
                ErrorClass::PlatformError,
                "Host.Incomplete",
                format!("invocation is still {}", inv.status.name()),
            ),
        };
        Some(AppError::Invocation {
            invocation_id: inv.id.clone(),
            error,
        })
    }
}

/// Decode an inline output stored in the ledger back into JSON.
pub fn inline_output(invocation: &Invocation) -> Option<serde_json::Value> {
    match &invocation.output {
        Some(PayloadRef::Inline { bytes_base64, .. }) => base64::engine::general_purpose::STANDARD
            .decode(bytes_base64)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelKind {
    Client,
    Shutdown,
    /// The invocation's revision was drained (alias switch or function
    /// deletion) and it was still running when the drain timeout passed
    /// (PLT-4635). Ends like an execution timeout: `Host.DrainTimeout`.
    Drain,
}

/// `error_type` of an invocation stopped because its revision's drain timed
/// out (PLT-4635).
pub const DRAIN_TIMEOUT: &str = "Host.DrainTimeout";

/// The ledger error of an invocation that `kind` stopped while `what`.
fn cancel_error(kind: CancelKind, what: &str) -> InvocationError {
    match kind {
        CancelKind::Client => InvocationError::new(
            ErrorClass::Cancelled,
            "Host.Cancelled",
            format!("cancelled by request {what}"),
        ),
        CancelKind::Shutdown => InvocationError::new(
            ErrorClass::Cancelled,
            "Host.Cancelled",
            format!("cancelled by gateway shutdown {what}"),
        ),
        CancelKind::Drain => InvocationError::new(
            ErrorClass::Timeout,
            DRAIN_TIMEOUT,
            format!("stopped {what}: its revision was drained and the drain timeout passed"),
        ),
    }
}

struct DriverResult {
    output: Option<serde_json::Value>,
}

struct InFlight {
    cancel: watch::Sender<Option<CancelKind>>,
    done: watch::Receiver<Option<Arc<DriverResult>>>,
    revision: RevisionId,
    accepted_at: Timestamp,
    /// A `min_ready` pre-start, not an invocation (PLT-4635).
    prestart: bool,
}

pub struct InvokeService {
    repos: Repositories,
    artifacts: Arc<dyn ArtifactStore>,
    secrets: Arc<dyn SecretProvider>,
    usage: Arc<dyn UsageSink>,
    provider: Arc<dyn ExecutionProvider>,
    history: Arc<HistoryService>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
    limits: Limits,
    capacity: CapacityConfig,
    invoke_cfg: InvokeConfig,
    entrypoints: EntrypointPolicy,
    /// Warm environment pool. Hands out nothing unless both the provider's
    /// idle capabilities and `[pool] enabled` allow reuse, so with the shipped
    /// providers every invocation stays cold and destroy-after-invoke.
    pool: Arc<EnvironmentPool>,
    /// This process's dispatcher: owner of every invocation, environment and
    /// lease it creates.
    dispatcher: Arc<Dispatcher>,
    /// Configuration cache and start restrictions (PLT-4636).
    gate: Arc<InvokeGate>,
    /// Capacity ledger, fair queue, quotas, autoscaler gate, start-rate
    /// limiter and circuit breakers (PLT-4634).
    admission: Arc<AdmissionController>,
    /// Usage journal admission (PLT-4642): no new invocation starts that
    /// could not be metered, unless the dev-only policy says otherwise.
    meter: Arc<UsageMeter>,
    in_flight: Mutex<HashMap<InvocationId, InFlight>>,
    draining: AtomicBool,
}

pub struct InvokeServiceDeps {
    pub repos: Repositories,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub secrets: Arc<dyn SecretProvider>,
    pub usage: Arc<dyn UsageSink>,
    pub provider: Arc<dyn ExecutionProvider>,
    pub history: Arc<HistoryService>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub limits: Limits,
    pub capacity: CapacityConfig,
    pub invoke: InvokeConfig,
    pub entrypoints: EntrypointPolicy,
    pub pool: Arc<EnvironmentPool>,
    pub dispatcher: Arc<Dispatcher>,
    pub gate: Arc<InvokeGate>,
    pub admission: Arc<AdmissionController>,
    pub meter: Arc<UsageMeter>,
}

impl InvokeService {
    pub fn new(deps: InvokeServiceDeps) -> Arc<Self> {
        Arc::new(Self {
            repos: deps.repos,
            artifacts: deps.artifacts,
            secrets: deps.secrets,
            usage: deps.usage,
            provider: deps.provider,
            history: deps.history,
            clock: deps.clock,
            ids: deps.ids,
            limits: deps.limits,
            capacity: deps.capacity,
            invoke_cfg: deps.invoke,
            entrypoints: deps.entrypoints,
            pool: deps.pool,
            dispatcher: deps.dispatcher,
            gate: deps.gate,
            admission: deps.admission,
            meter: deps.meter,
            in_flight: Mutex::new(HashMap::new()),
            draining: AtomicBool::new(false),
        })
    }

    /// Invocations this process is driving (pre-starts are not counted).
    pub fn in_flight_count(&self) -> usize {
        self.in_flight
            .lock()
            .values()
            .filter(|e| !e.prestart)
            .count()
    }

    /// Stop what still runs on a drained revision (PLT-4635): every
    /// invocation of `revision` accepted before `accepted_before` is stopped
    /// like an execution timeout (`Host.DrainTimeout`), and every pre-start
    /// of it is abandoned. Returns how many invocations were told to stop.
    pub fn stop_for_drain(&self, revision: &RevisionId, accepted_before: Timestamp) -> usize {
        let map = self.in_flight.lock();
        let mut stopped = 0;
        for e in map.values().filter(|e| &e.revision == revision) {
            if e.prestart {
                let _ = e.cancel.send_replace(Some(CancelKind::Drain));
            } else if e.accepted_at < accepted_before && e.cancel.borrow().is_none() {
                let _ = e.cancel.send_replace(Some(CancelKind::Drain));
                stopped += 1;
            }
        }
        stopped
    }

    /// Abandon the pre-starts of `revision` (it stopped being routed).
    pub fn cancel_prestarts(&self, revision: &RevisionId) -> usize {
        let map = self.in_flight.lock();
        let mut n = 0;
        for e in map
            .values()
            .filter(|e| e.prestart && &e.revision == revision)
        {
            let _ = e.cancel.send_replace(Some(CancelKind::Drain));
            n += 1;
        }
        n
    }

    /// Invocations of `revision` this process is still driving.
    pub fn in_flight_of(&self, revision: &RevisionId) -> usize {
        self.in_flight
            .lock()
            .values()
            .filter(|e| !e.prestart && &e.revision == revision)
            .count()
    }

    /// Boot one environment of `revision` for `min_ready` and hand it to the
    /// pool (PLT-4635). `grant` is the pre-start reservation admission made
    /// ([`AdmissionController::try_prestart`]). Nothing is recorded as an
    /// invocation; the environment's ledger row, its logs and its usage
    /// events are real (a pre-started environment costs what it costs).
    /// `Err` says why it did not end up pooled; whatever was booted has been
    /// terminated then.
    pub async fn prestart(
        self: &Arc<Self>,
        function: Function,
        revision: FunctionRevision,
        grant: Grant,
    ) -> Result<(), String> {
        if self.draining.load(Ordering::SeqCst) || self.dispatcher.is_fenced() {
            return Err("the gateway is shutting down or lost its dispatcher lease".into());
        }
        // A pre-start costs what it costs: it is not started unmetered.
        if !matches!(self.meter.admit(), Ok(MeteringAdmission::Metered)) {
            return Err("the usage journal cannot meter a pre-start".into());
        }
        let id = InvocationId::from_ulid(self.ids.next_ulid());
        let (cancel_tx, cancel_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(None);
        let now = self.clock.now();
        self.in_flight.lock().insert(
            id.clone(),
            InFlight {
                cancel: cancel_tx,
                done: done_rx,
                revision: revision.id.clone(),
                accepted_at: now,
                prestart: true,
            },
        );
        let exec = &revision.spec.execution;
        let budget = Duration::from_secs(u64::from(exec.initialization_timeout_seconds))
            + self.invoke_cfg.handshake_timeout();
        let mut driver = Driver {
            svc: Arc::clone(self),
            invocation_id: id.clone(),
            function,
            revision,
            event_kind: EventKind::Json,
            payload: RetainedPayload::new(serde_json::Value::Null),
            input_size: 0,
            trace_id: id.to_string(),
            client_deadline: now
                + chrono::Duration::from_std(budget).unwrap_or(chrono::Duration::seconds(60)),
            cancel_rx,
            accepted_at: Instant::now(),
            pre: None,
            grant: Some(grant),
            env_id: None,
            attempt_id: None,
            lease_id: None,
            seq: 0,
            epoch: 1,
            warmup: true,
            meter: AttemptMeter::default(),
        };
        let result = match AssertUnwindSafe(driver.prewarm()).catch_unwind().await {
            Ok(r) => r,
            Err(_) => {
                tracing::error!("pre-start driver panicked");
                let _ = AssertUnwindSafe(driver.cleanup_after_panic())
                    .catch_unwind()
                    .await;
                Err("the pre-start panicked".into())
            }
        };
        driver.grant = None;
        self.in_flight.lock().remove(&id);
        done_tx.send_replace(Some(Arc::new(DriverResult { output: None })));
        result
    }

    /// Synchronous invoke. Returns once the invocation reached a terminal
    /// state (or immediately with a replay for a matching idempotency key).
    pub async fn invoke(self: &Arc<Self>, req: InvokeRequest) -> Result<InvokeOutcome, AppError> {
        require_invoke(&req.principal)?;
        if self.draining.load(Ordering::SeqCst) {
            return Err(AppError::ProviderUnavailable(
                "gateway is shutting down".into(),
            ));
        }
        // A dispatcher that lost its lease may already have had its work
        // reclaimed by another one: it takes nothing new (PLT-4631).
        if self.dispatcher.is_fenced() {
            return Err(AppError::ProviderUnavailable(
                "this gateway lost its dispatcher lease; retry against a live gateway".into(),
            ));
        }
        // 2. Function, route, revision and policy come from the configuration
        // cache only, never from the management store (PLT-4636).
        let Resolved {
            function,
            revision,
            alias,
            alias_generation,
        } = self
            .gate
            .resolve(
                &req.principal,
                &req.function_id,
                req.alias.as_ref(),
                req.revision_id.as_ref(),
                self.pool.policy().reuse_enabled(),
            )
            .await
            .inspect_err(|e| {
                if let AppError::Control { kind, .. } = e {
                    self.admission.metrics().gate_refusal(kind.error_type());
                }
            })?;

        let payload_bytes = serde_json::to_vec(&req.payload)
            .map_err(|e| AppError::InvalidRequest(format!("payload is not JSON: {e}")))?;
        let input_size = payload_bytes.len() as u64;
        if input_size > self.limits.max_payload_bytes {
            return Err(AppError::PayloadTooLarge {
                size: input_size,
                max: self.limits.max_payload_bytes,
            });
        }
        let input_digest = Sha256Digest::of_bytes(&payload_bytes);
        if let Some(trace) = &req.trace_id
            && trace.len() > MAX_TRACE_ID_BYTES
        {
            return Err(AppError::InvalidRequest(format!(
                "trace id must be at most {MAX_TRACE_ID_BYTES} bytes"
            )));
        }

        // 4. Build the invocation. `Invocation::accept` only validates (key
        // length, client deadline) and records nothing, so a 400 here leaves
        // no trace and does not consume the idempotency key.
        let invocation_id = InvocationId::from_ulid(self.ids.next_ulid());
        let now = self.clock.now();
        let exec = &revision.spec.execution;
        let budget_ms = (u64::from(exec.timeout_seconds)
            + u64::from(exec.initialization_timeout_seconds)
            + self.capacity.queue_timeout_seconds)
            * 1000;
        let client_ms = req
            .client_timeout_ms
            .filter(|ms| *ms > 0)
            .map_or(budget_ms, |ms| ms.min(budget_ms));
        let client_deadline = now + chrono::Duration::milliseconds(client_ms as i64);
        // No other deadline is ever set beyond the client deadline
        // (docs/threat-model.md §8).
        let deadlines = Deadlines {
            queue_deadline: (now
                + chrono::Duration::milliseconds(
                    (self.capacity.queue_timeout_seconds * 1000) as i64,
                ))
            .min(client_deadline),
            init_deadline: None,
            execution_deadline: None,
            client_deadline,
        };
        let trace_id = req
            .trace_id
            .clone()
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| invocation_id.to_string());
        let mut invocation = Invocation::accept(
            invocation_id.clone(),
            req.principal.tenant_id.clone(),
            function.id.clone(),
            alias,
            revision.id.clone(),
            InvocationMode::Sync,
            req.event_kind,
            deadlines,
            req.idempotency_key.clone(),
            input_digest.clone(),
            input_size,
            trace_id.clone(),
            now,
        )?;
        invocation.dispatcher_id = Some(self.dispatcher.id().clone());
        // The route was resolved once, now: an alias switch after this point
        // never re-points this invocation (PLT-4635).
        invocation.alias_generation = alias_generation;

        // 3. Idempotency: a key bound to an existing invocation replays it,
        // even when capacity is exhausted.
        if let Some(binding) = self.bound_invocation(&req)? {
            return self.replay_binding(&req, &input_digest, binding).await;
        }

        // Metering (PLT-4642): nothing starts that the usage journal could not
        // record. Refused before anything is recorded, like admission; a
        // replay above still answers.
        let unmetered = match self.meter.admit()? {
            MeteringAdmission::Metered => None,
            MeteringAdmission::Unmetered(refusal) => Some(refusal),
        };

        // 5 (first half). Admission is asked before anything is recorded so
        // that a refusal (full queue, quota, placement, open breaker) answers
        // without a ledger entry and without binding the idempotency key.
        let ticket = self.admission.ticket(
            &function.tenant_id,
            &revision,
            input_size,
            invocation.deadlines.queue_deadline,
        );
        let mut pre = match self.admission.admit(ticket) {
            Ok(pre) => pre,
            Err(rejection) => {
                // A concurrent request with the same key may have been
                // accepted in the meantime; its record is the better answer.
                if let Some(binding) = self.bound_invocation(&req)? {
                    return self.replay_binding(&req, &input_digest, binding).await;
                }
                tracing::info!(
                    tenant_id = %function.tenant_id,
                    revision_id = %revision.id,
                    reason = rejection.reason.as_str(),
                    "invocation refused by admission"
                );
                return Err(rejection.into());
            }
        };
        if pre.is_waiting() {
            let _ = invocation.mark_queued();
        }

        // Register the in-flight entry before the ledger row (and its key)
        // becomes visible: a replay that finds the row then always finds the
        // entry to wait on, or a terminal state once the driver is done.
        let (cancel_tx, cancel_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(None);
        self.in_flight.lock().insert(
            invocation_id.clone(),
            InFlight {
                cancel: cancel_tx,
                done: done_rx.clone(),
                revision: revision.id.clone(),
                accepted_at: now,
                prestart: false,
            },
        );
        // The key is bound in the same store mutation as the ledger insert.
        match self.repos.idempotency.insert_bound(invocation) {
            Ok(IdempotencyOutcome::Inserted) => {}
            Ok(IdempotencyOutcome::Existing(binding)) => {
                // Lost the race for the key: nothing of ours was recorded.
                self.in_flight.lock().remove(&invocation_id);
                drop(pre);
                return self.replay_binding(&req, &input_digest, binding).await;
            }
            Err(e) => {
                self.in_flight.lock().remove(&invocation_id);
                return Err(e.into());
            }
        }

        let driver = Driver {
            svc: Arc::clone(self),
            invocation_id: invocation_id.clone(),
            function,
            revision,
            event_kind: req.event_kind,
            payload: RetainedPayload::new(req.payload),
            input_size,
            trace_id,
            client_deadline,
            cancel_rx,
            accepted_at: Instant::now(),
            pre: Some(pre),
            grant: None,
            env_id: None,
            attempt_id: None,
            lease_id: None,
            seq: 0,
            epoch: 1,
            warmup: false,
            meter: AttemptMeter {
                unmetered: unmetered.is_some(),
                ..AttemptMeter::default()
            },
        };
        tokio::spawn(driver.run(done_tx));

        let result = Self::await_done(done_rx).await;
        let detail = self.history.detail(self.load(&invocation_id)?)?;
        Ok(InvokeOutcome {
            detail,
            output: result.and_then(|r| r.output.clone()),
            replayed: false,
        })
    }

    /// Cancel an in-flight invocation. Idempotent for already cancelled
    /// invocations; `Conflict` for ones that finished otherwise.
    pub async fn cancel(
        &self,
        principal: &Principal,
        invocation_id: &InvocationId,
    ) -> Result<InvocationDetail, AppError> {
        require_invoke(principal)?;
        let inv = self.load(invocation_id)?;
        ensure_tenant(principal, &inv.tenant_id, "invocation")?;
        use tachyon_serverless_domain::InvocationStatus as S;
        match inv.status {
            S::Cancelled => return self.history.detail(inv),
            s if s.is_terminal() => {
                return Err(AppError::Conflict(format!(
                    "invocation {} already finished ({})",
                    inv.id,
                    s.name()
                )));
            }
            _ => {}
        }
        let entry = {
            let map = self.in_flight.lock();
            map.get(invocation_id)
                .map(|e| (e.cancel.clone(), e.done.clone()))
        };
        match entry {
            Some((cancel, done)) => {
                let _ = cancel.send_replace(Some(CancelKind::Client));
                let wait = self.invoke_cfg.cancel_grace() + Duration::from_secs(5);
                let _ = tokio::time::timeout(wait, Self::await_done(done)).await;
            }
            None => {
                // Driven by another live gateway on the same store: only that
                // one can stop the handler, so settling the ledger here would
                // report a cancel that did not happen (PLT-4631).
                if let Some(owner) = &inv.dispatcher_id
                    && owner != self.dispatcher.id()
                    && self
                        .repos
                        .slots
                        .get_dispatcher(owner)?
                        .is_some_and(|d| d.is_live())
                {
                    return Err(AppError::Conflict(format!(
                        "invocation {} is driven by another gateway (dispatcher {owner}); \
                         cancel it there",
                        inv.id
                    )));
                }
                // No driver (e.g. recovered from disk): settle the ledger directly.
                let mut inv = self.load(invocation_id)?;
                if !inv.status.is_terminal() {
                    inv.mark_cancelled(self.clock.now())?;
                    self.repos.invocations.update(inv)?;
                }
            }
        }
        self.history.detail(self.load(invocation_id)?)
    }

    /// Refuse new invocations and cancel every in-flight one (used on
    /// graceful shutdown). Waits at most `timeout` for drivers to finish.
    pub async fn shutdown_all(&self, timeout: Duration) {
        self.draining.store(true, Ordering::SeqCst);
        let entries: Vec<(watch::Sender<Option<CancelKind>>, watch::Receiver<_>)> = self
            .in_flight
            .lock()
            .values()
            .map(|e| (e.cancel.clone(), e.done.clone()))
            .collect();
        if entries.is_empty() {
            return;
        }
        tracing::info!(
            count = entries.len(),
            "cancelling in-flight invocations for shutdown"
        );
        for (cancel, _) in &entries {
            let _ = cancel.send_replace(Some(CancelKind::Shutdown));
        }
        let all = futures::future::join_all(entries.into_iter().map(|(_, d)| Self::await_done(d)));
        let _ = tokio::time::timeout(timeout, all).await;
    }

    /// The live binding of the request's idempotency key, if any.
    fn bound_invocation(
        &self,
        req: &InvokeRequest,
    ) -> Result<Option<IdempotencyBinding>, AppError> {
        match &req.idempotency_key {
            Some(key) => Ok(self.repos.idempotency.lookup(
                &req.principal.tenant_id,
                &req.function_id,
                key,
                self.clock.now(),
            )?),
            None => Ok(None),
        }
    }

    /// Same key and same input: return the bound invocation. Same key and a
    /// different input: 409.
    async fn replay_binding(
        &self,
        req: &InvokeRequest,
        input_digest: &Sha256Digest,
        binding: IdempotencyBinding,
    ) -> Result<InvokeOutcome, AppError> {
        if &binding.input_digest != input_digest {
            return Err(AppError::IdempotencyConflict {
                key: req.idempotency_key.clone().unwrap_or_default(),
                invocation_id: binding.invocation_id,
            });
        }
        // A key bound to an asynchronous invocation (PLT-4639) is not replayed
        // as a synchronous result: that invocation may wait in the queue far
        // longer than any client deadline.
        if self
            .repos
            .invocations
            .get(&binding.invocation_id)?
            .is_some_and(|inv| inv.mode != InvocationMode::Sync)
        {
            return Err(AppError::Conflict(format!(
                "idempotency key `{}` is bound to asynchronous invocation {}",
                req.idempotency_key.as_deref().unwrap_or_default(),
                binding.invocation_id
            )));
        }
        self.replay(&binding.invocation_id).await
    }

    /// Same key, same input: the bound invocation. In flight in this process,
    /// the replay waits for its driver; in flight in *another* process (a
    /// second gateway on the same store), it follows the ledger until the
    /// invocation is terminal or its client deadline passes. Never a second
    /// execution.
    async fn replay(&self, existing: &InvocationId) -> Result<InvokeOutcome, AppError> {
        let done = self.in_flight.lock().get(existing).map(|e| e.done.clone());
        let result = match done {
            Some(rx) => Self::await_done(rx).await,
            None => {
                self.follow_ledger(existing).await?;
                None
            }
        };
        let inv = self.load(existing)?;
        let output = result
            .and_then(|r| r.output.clone())
            .or_else(|| inline_output(&inv));
        Ok(InvokeOutcome {
            detail: self.history.detail(inv)?,
            output,
            replayed: true,
        })
    }

    /// Poll the ledger until `id` is terminal or its client deadline passed.
    async fn follow_ledger(&self, id: &InvocationId) -> Result<(), AppError> {
        let inv = self.load(id)?;
        if inv.status.is_terminal() {
            return Ok(());
        }
        let wait = (inv.deadlines.client_deadline - self.clock.now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        let started = Instant::now();
        while started.elapsed() < wait {
            tokio::time::sleep(LEDGER_POLL.min(wait.saturating_sub(started.elapsed()))).await;
            if self.load(id)?.status.is_terminal() {
                break;
            }
        }
        Ok(())
    }

    async fn await_done(
        mut rx: watch::Receiver<Option<Arc<DriverResult>>>,
    ) -> Option<Arc<DriverResult>> {
        let waited = rx.wait_for(|v| v.is_some()).await.map(|v| v.clone());
        match waited {
            Ok(v) => v,
            Err(_) => rx.borrow().clone(),
        }
    }

    fn load(&self, id: &InvocationId) -> Result<Invocation, AppError> {
        self.repos
            .invocations
            .get(id)?
            .ok_or_else(|| AppError::not_found("invocation not found"))
    }
}

// ---------------------------------------------------------------------------
// driver
// ---------------------------------------------------------------------------

/// Outcome classification: the ledger result (output ref + http status on
/// success, the error otherwise) and how the environment ends.
type Classified = (
    Result<(Option<PayloadRef>, Option<u16>), InvocationError>,
    EnvEnd,
);

/// How the environment ended, for the ledger and the provider.
#[derive(Debug)]
struct EnvEnd {
    reason: TerminateReason,
    /// `None` -> Stopped, `Some(reason)` -> Failed{reason}.
    failure: Option<&'static str>,
}

/// What happened to the `Invoke` frame and the wait for its result.
enum Dispatch {
    /// The frame was written; the wait ended with this outcome.
    Finished(Outcome),
    /// The frame was written; the invocation was cancelled while waiting.
    Cancelled(CancelKind),
    /// The frame never reached the guest, so the handler did not start.
    /// `drained` holds what the guest had queued before closing, if the
    /// write failed because the connection was gone.
    NotDelivered {
        error: SessionError,
        drained: Option<Outcome>,
    },
}

/// Result of one dispatch onto one environment ([`Driver::attempt`]).
enum Attempted {
    /// Terminal: the ledger is settled and this is the handler output.
    Done(Option<serde_json::Value>),
    /// The pooled environment was gone before the `Invoke` frame reached its
    /// guest, so the handler cannot have started. The environment has been
    /// retired and the attempt settled; the caller dispatches once more, cold.
    /// Never produced when the caller disallowed a warm start, so a retry can
    /// never ask for another one.
    RetryCold,
}

/// A secret binding of the revision could not be resolved.
struct SecretResolutionFailure {
    binding_ref: String,
    error: SecretError,
}

impl SecretResolutionFailure {
    /// Operator-facing reason, for tracing only. Never tenant-facing.
    fn reason(&self) -> &'static str {
        match self.error {
            SecretError::NotFound(_) => "not_found",
            SecretError::Forbidden { .. } => "forbidden",
            SecretError::Backend(_) => "backend",
        }
    }

    /// Tenant-facing classification. A binding that exists only for another
    /// tenant and one that does not exist at all are reported identically,
    /// so the result is not an oracle for other tenants' binding names.
    fn invocation_error(&self) -> InvocationError {
        match &self.error {
            SecretError::NotFound(_) | SecretError::Forbidden { .. } => InvocationError::new(
                ErrorClass::InitError,
                "Host.SecretBindingUnavailable",
                format!(
                    "secret binding `{}` is not available to this tenant",
                    self.binding_ref
                ),
            ),
            SecretError::Backend(_) => InvocationError::new(
                ErrorClass::PlatformError,
                "Host.SecretBackend",
                format!(
                    "secret backend failed while resolving binding `{}`",
                    self.binding_ref
                ),
            ),
        }
    }
}

/// The environment half of the pipeline, however it was obtained: freshly
/// booted ([`Driver::prepare_cold`]) or taken out of the pool
/// ([`Driver::prepare_warm`]). From here on the two paths are identical.
struct Prepared {
    /// Ledger row, already `Busy`-able at the epoch this attempt will use.
    env: ExecutionEnvironment,
    session: BridgeSession,
    start_kind: StartKind,
    /// Milliseconds spent booting the environment. Zero for a warm start:
    /// nothing booted.
    environment_boot_ms: u64,
    /// Milliseconds spent waiting for the guest to report `Ready`. Zero for a
    /// warm start: the guest reported it during the invocation that booted it.
    runtime_init_ms: u64,
    /// What taking this environment out of the pool cost: the resume and the
    /// readiness check. `None` for a cold start, which booted instead.
    warm: Option<WarmStartTimings>,
    logs: LogForwarder,
}

/// The invocation payload while it is being dispatched.
///
/// A warm dispatch may still have to be repeated cold, so the `Invoke` frame
/// gets a copy and the original is retained — but only until the write has
/// resolved. Anything other than a lost connection means no retry can ask for
/// it again, and the copy is released there and then, so a payload (up to
/// `max_payload_bytes`, 1 MiB by default) is never held twice for the whole
/// handler execution. A cold dispatch hands over its only copy and retains
/// nothing.
struct RetainedPayload(Option<serde_json::Value>);

impl RetainedPayload {
    fn new(payload: serde_json::Value) -> Self {
        Self(Some(payload))
    }

    /// The copy that goes into the `Invoke` frame. `retryable` keeps the
    /// original for a possible cold retry; otherwise the only copy is handed
    /// over.
    fn checkout(&mut self, retryable: bool) -> serde_json::Value {
        match retryable {
            true => self.0.clone().unwrap_or(serde_json::Value::Null),
            false => self.0.take().unwrap_or(serde_json::Value::Null),
        }
    }

    /// The dispatch resolved. Only a lost connection can still need the
    /// retained copy (the caller then dispatches once more, cold); every other
    /// outcome releases it here. Returns whether a retry is still possible,
    /// which is exactly "the payload is still held".
    fn settle(&mut self, connection_lost: bool) -> bool {
        if connection_lost && self.0.is_some() {
            return true;
        }
        self.0 = None;
        false
    }

    #[cfg(test)]
    fn held(&self) -> bool {
        self.0.is_some()
    }
}

struct Driver {
    svc: Arc<InvokeService>,
    invocation_id: InvocationId,
    function: Function,
    revision: FunctionRevision,
    event_kind: EventKind,
    payload: RetainedPayload,
    input_size: u64,
    trace_id: String,
    client_deadline: Timestamp,
    cancel_rx: watch::Receiver<Option<CancelKind>>,
    accepted_at: Instant,
    /// The admission this invocation queued for in `invoke`.
    pre: Option<Pending>,
    /// The environment reservation (or pooled-environment promise) this
    /// driver holds. Released when dropped; handed to the pool with the
    /// environment when it is pooled.
    grant: Option<Grant>,
    // What the driver has recorded so far, so that a panic can still clean
    // up (see `cleanup_after_panic`). Cleared once the normal path finished.
    env_id: Option<EnvironmentId>,
    attempt_id: Option<AttemptId>,
    lease_id: Option<LeaseId>,
    /// Usage-event counter of the environment this driver currently works
    /// with: zero for one this driver booted, and the pool's count for one it
    /// reused. The events of a single environment are therefore monotonic over
    /// its whole life, across every invocation that ran on it
    /// (`UsageEvent::sequence`).
    seq: u64,
    /// Epoch of the environment this driver currently works with. It is part
    /// of every usage event id, so the events of an attempt on a reused
    /// environment never collide with those of the attempt before it.
    epoch: u64,
    /// A `min_ready` pre-start (PLT-4635): there is no invocation behind
    /// `invocation_id`, which is never stored and never put on a log line or
    /// a usage event.
    warmup: bool,
    /// Host-measured segments of the current attempt (PLT-4642).
    meter: AttemptMeter,
}

/// What the driver measured of the attempt it is on, for `AttemptSettled`
/// (PLT-4642). Durations come from the host's monotonic clock only; a
/// segment that was not measured stays `None` and is reported as unknown.
#[derive(Debug, Clone, Default)]
struct AttemptMeter {
    queue_wait: Option<Duration>,
    /// Cold: provider create to bridge connected. Warm: resume + readiness.
    vm_base_boot: Option<Duration>,
    /// Cold: bridge connected to `Ready`. Warm: zero.
    user_init: Option<Duration>,
    /// Idle time in the pool before a warm claim (zero for a cold start).
    idle_pooled_ms: Option<u64>,
    guest_init_ms: Option<u64>,
    boot_id: Option<String>,
    /// Admitted while the journal refused, under the dev-only
    /// `accept_unmetered` policy.
    unmetered: bool,
}

impl AttemptMeter {
    /// A new environment: forget everything but the queue wait.
    fn environment_changed(&mut self) {
        *self = Self {
            queue_wait: self.queue_wait,
            unmetered: self.unmetered,
            ..Self::default()
        };
    }
}

/// Requested resources of a revision plus what the host observed, as usage
/// resources (PLT-4642).
pub(crate) fn usage_resources(
    resources: &ResourceProfile,
    host: Option<&EnvironmentStats>,
) -> UsageResources {
    // Only a sample that covers the whole environment is a host resource
    // quantity: the VMM's cgroup (guest + VMM). The process provider's
    // procfs / rusage sample covers the bridge process only, not the user
    // process it runs, so it would undercount; it stays unknown.
    let whole = host.filter(|s| matches!(s.scope.as_str(), "cgroup_v2" | "fake"));
    let cpu_usec = whole
        .and_then(|s| s.cpu_seconds)
        .filter(|s| s.is_finite() && *s >= 0.0)
        .map(|s| (s * 1_000_000.0).round() as u64);
    UsageResources {
        requested_cpu_millis: resources.cpu_millis,
        requested_memory_mib: resources.memory_mib,
        requested_storage_mib: resources.ephemeral_storage_mib,
        cgroup_cpu_usec: cpu_usec.map_or_else(Metered::unknown, Metered::provider),
        cgroup_memory_peak_bytes: whole
            .and_then(|s| s.memory_peak_bytes)
            .map_or_else(Metered::unknown, Metered::provider),
    }
}

/// The provider's host sample of an environment, read just before it is
/// terminated (PLT-4637 `ExecutionProvider::environment_stats`, the same
/// source `GET /metrics` uses). `None` when the provider cannot measure it.
pub(crate) async fn sample_before_terminate(
    provider: &dyn ExecutionProvider,
    environment_id: &EnvironmentId,
) -> Option<EnvironmentStats> {
    match provider.environment_stats(environment_id).await {
        Ok(stats) => stats,
        Err(e) => {
            tracing::debug!(error = %e, environment_id = %environment_id, "no host usage sample before terminate");
            None
        }
    }
}

/// How an attempt's result reads as a usage outcome.
fn usage_outcome<T>(result: &Result<T, InvocationError>, completed: bool) -> UsageOutcome {
    if !completed {
        // Another dispatcher settled it: what this one saw is not the answer.
        return UsageOutcome::OutcomeUnknown;
    }
    match result {
        Ok(_) => UsageOutcome::Succeeded,
        Err(e) => match e.class {
            ErrorClass::Timeout => UsageOutcome::Timeout,
            ErrorClass::Cancelled => UsageOutcome::Cancelled,
            ErrorClass::OutcomeUnknown => UsageOutcome::OutcomeUnknown,
            _ => UsageOutcome::Failed,
        },
    }
}

async fn wait_cancel(rx: &mut watch::Receiver<Option<CancelKind>>) -> CancelKind {
    loop {
        if let Some(k) = *rx.borrow_and_update() {
            return k;
        }
        if rx.changed().await.is_err() {
            // Sender gone: nobody can cancel any more; park forever.
            std::future::pending::<()>().await;
        }
    }
}

impl Driver {
    async fn run(mut self, done: watch::Sender<Option<Arc<DriverResult>>>) {
        let id = self.invocation_id.clone();
        let output = match AssertUnwindSafe(self.execute()).catch_unwind().await {
            Ok(output) => output,
            Err(_) => {
                tracing::error!(invocation_id = %id, "invoke driver panicked");
                // The cleanup must never keep the in-flight entry or the
                // completion signal from being released below.
                if AssertUnwindSafe(self.cleanup_after_panic())
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    tracing::error!(invocation_id = %id, "cleanup after a driver panic panicked");
                }
                let recorded = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    self.fail_invocation(InvocationError::new(
                        ErrorClass::PlatformError,
                        "Host.DriverPanic",
                        "internal error while driving the invocation",
                    ))
                }));
                if recorded.is_err() {
                    tracing::error!(invocation_id = %id, "recording a driver panic panicked");
                }
                None
            }
        };
        self.svc.in_flight.lock().remove(&id);
        done.send_replace(Some(Arc::new(DriverResult { output })));
    }

    /// After a panic in `execute`: terminate the environment (idempotent),
    /// release the lease, fail the open attempt, mark the environment Failed
    /// and emit `EnvironmentStopped`, so destroy-after-invoke holds on every
    /// exit path (docs/threat-model.md T16).
    async fn cleanup_after_panic(&mut self) {
        let Some(env_id) = self.env_id.take() else {
            return;
        };
        let svc = self.svc.clone();
        tracing::warn!(environment_id = %env_id, "terminating environment after a driver panic");
        if let Err(e) = svc
            .provider
            .terminate_environment(&env_id, TerminateReason::Crashed)
            .await
        {
            tracing::warn!(error = %e, environment_id = %env_id, "terminate after driver panic failed");
        }
        let now = self.now();
        if let Some(lease_id) = self.lease_id.take()
            && let Some(attempt_id) = &self.attempt_id
        {
            // Fenced like every completion: a lease another dispatcher
            // reclaimed stays reclaimed.
            let _ = svc
                .repos
                .slots
                .release_lease(&lease_id, attempt_id, self.epoch, now);
        }
        if let Some(attempt_id) = self.attempt_id.take()
            && let Ok(Some(mut attempt)) = svc.repos.invocations.get_attempt(&attempt_id)
            && !attempt.status.is_terminal()
        {
            let _ = attempt.fail(
                InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.DriverPanic",
                    "internal error while driving the invocation",
                ),
                now,
            );
            let _ = svc.repos.invocations.update_attempt(attempt);
        }
        let row = svc.repos.environments.get(&env_id).ok().flatten();
        if let Some(env) = &row
            && !env.is_terminal()
        {
            let mut env = env.clone();
            let _ = env.mark_failed("driver panicked", now);
            self.save_env(&env);
        }
        self.seq += 1;
        // The environment's whole life, the same quantity every other path
        // reports for it.
        let duration = row.as_ref().map(|env| environment_lifetime_ms(env, now));
        self.emit_usage(
            &env_id,
            None,
            UsageEventType::EnvironmentStopped,
            self.seq,
            duration,
            0,
            0,
        )
        .await;
    }

    fn now(&self) -> Timestamp {
        self.svc.clock.now()
    }

    /// Time left until the client deadline (zero once it has passed).
    fn client_remaining(&self) -> Duration {
        (self.client_deadline - self.now())
            .to_std()
            .unwrap_or(Duration::ZERO)
    }

    fn client_deadline_elapsed(&self) -> bool {
        self.now() >= self.client_deadline
    }

    fn load_invocation(&self) -> Option<Invocation> {
        self.svc
            .repos
            .invocations
            .get(&self.invocation_id)
            .ok()
            .flatten()
    }

    fn save_invocation(&self, inv: Invocation) {
        if let Err(e) = self.svc.repos.invocations.update(inv) {
            tracing::warn!(error = %e, invocation_id = %self.invocation_id, "invocation update failed");
        }
    }

    fn fail_invocation(&self, error: InvocationError) {
        if let Some(mut inv) = self.load_invocation() {
            let now = self.now();
            let r = match error.class {
                ErrorClass::Cancelled => inv.mark_cancelled(now),
                ErrorClass::OutcomeUnknown => inv.mark_outcome_unknown(error.message.clone(), now),
                _ => inv.mark_failed(error.clone(), now),
            };
            if let Err(e) = r {
                tracing::warn!(error = %e, invocation_id = %inv.id, "cannot mark invocation failed");
            }
            tracing::info!(
                invocation_id = %inv.id,
                class = error.class.as_str(),
                error_type = %error.error_type,
                "invocation failed"
            );
            self.save_invocation(inv);
        }
    }

    fn save_env(&self, env: &ExecutionEnvironment) {
        if let Err(e) = self.svc.repos.environments.update(env.clone()) {
            tracing::warn!(error = %e, environment_id = %env.id, "environment update failed");
        }
    }

    /// The client deadline elapsed before the handler was dispatched: the
    /// handler is never started. The environment is stopped and terminated
    /// as cancelled and the invocation ends as `Timeout`
    /// (docs/threat-model.md §8).
    async fn stop_for_client_deadline(
        &self,
        session: Option<&mut BridgeSession>,
        env: &mut ExecutionEnvironment,
        logs: &LogForwarder,
    ) {
        logs.platform(
            LogPhase::Init,
            None,
            "client deadline elapsed before the handler was dispatched; not starting it",
        );
        if let Some(session) = session {
            let _ = session.shutdown("client deadline").await;
        }
        let _ = env.mark_stopped(self.now());
        self.save_env(env);
        let _ = self
            .svc
            .provider
            .terminate_environment(&env.id, TerminateReason::Cancelled)
            .await;
        self.fail_invocation(InvocationError::new(
            ErrorClass::Timeout,
            "Host.ClientDeadline",
            "client deadline elapsed before the handler could start",
        ));
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_usage(
        &self,
        env: &EnvironmentId,
        attempt: Option<&AttemptId>,
        event_type: UsageEventType,
        sequence: u64,
        duration_ms: Option<u64>,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        let mut event = self.usage_event(env, attempt, event_type, sequence);
        event.monotonic_duration_ms = duration_ms;
        event.bytes_in = bytes_in;
        event.bytes_out = bytes_out;
        match event_type {
            UsageEventType::EnvironmentStarted => {
                event.segments.vm_base_boot_ms =
                    Metered::host_opt(self.meter.vm_base_boot.map(ceil_ms));
            }
            UsageEventType::HandlerFinished => {
                event.segments.handler_ms = Metered::host_opt(duration_ms);
            }
            _ => {}
        }
        self.record_usage(event).await;
    }

    /// A v2 usage event of this driver's function, revision and invocation
    /// (PLT-4642), with the requested resources and nothing measured yet.
    fn usage_event(
        &self,
        env: &EnvironmentId,
        attempt: Option<&AttemptId>,
        event_type: UsageEventType,
        sequence: u64,
    ) -> UsageEvent {
        // Unique per (environment, assignment, event). The epoch advances
        // on every reassignment, so the events of a warm attempt never
        // collide with those of the invocation that ran on the same
        // environment before it — while re-sending the *same* event keeps
        // the same id, so the sink still de-duplicates it.
        let mut event = UsageEvent::new(
            format!("{env}:{}:{sequence}", self.epoch),
            self.function.tenant_id.clone(),
            env.clone(),
            event_type,
            sequence,
            self.now(),
        );
        let resources = &self.revision.spec.resources;
        event.invocation_id = (!self.warmup).then(|| self.invocation_id.clone());
        event.attempt_id = attempt.cloned();
        event.memory_mib = resources.memory_mib;
        event.cpu_millis = resources.cpu_millis;
        event.function_id = Some(self.function.id.clone());
        event.revision_id = Some(self.revision.id.clone());
        event.epoch = self.epoch;
        event.boot_id = self.meter.boot_id.clone();
        event.resources = usage_resources(resources, None);
        event
    }

    async fn record_usage(&self, mut event: UsageEvent) {
        if self.meter.unmetered {
            // Admitted under `accept_unmetered` while the journal refused:
            // every quantity is unknown, so nothing of it is ever rated.
            event.segments = UsageSegments::default();
            event.bytes = UsageBytes::default();
            event.resources.cgroup_cpu_usec = Metered::unknown();
            event.resources.cgroup_memory_peak_bytes = Metered::unknown();
            event.evidence_quality = tachyon_serverless_domain::EvidenceQuality::Unknown;
        }
        self.svc.usage.record(event).await;
    }

    /// `AttemptSettled`: the attempt's outcome and every segment the host
    /// measured of it (PLT-4642). `handler` is `None` when the `Invoke` frame
    /// never reached the guest: the handler provably did not run (zero).
    #[allow(clippy::too_many_arguments)]
    async fn emit_attempt_settled(
        &mut self,
        env: &EnvironmentId,
        attempt_id: &AttemptId,
        number: u32,
        outcome: UsageOutcome,
        handler: Option<Duration>,
        teardown: Option<Duration>,
        bytes_out: u64,
        guest_handler_ms: Option<u64>,
    ) {
        self.seq += 1;
        let mut event = self.usage_event(
            env,
            Some(attempt_id),
            UsageEventType::AttemptSettled,
            self.seq,
        );
        event.attempt_number = Some(number);
        event.attempt_kind = Some(AttemptKind::from_number(number));
        event.outcome = Some(outcome);
        event.segments = UsageSegments {
            queue_wait_ms: Metered::host_opt(self.meter.queue_wait.map(ceil_ms)),
            vm_base_boot_ms: Metered::host_opt(self.meter.vm_base_boot.map(ceil_ms)),
            user_init_ms: Metered::host_opt(self.meter.user_init.map(ceil_ms)),
            handler_ms: Metered::host(handler.map_or(0, ceil_ms)),
            teardown_ms: Metered::host_opt(teardown.map(ceil_ms)),
            idle_pooled_ms: Metered::host_opt(self.meter.idle_pooled_ms),
        };
        event.bytes = UsageBytes {
            request_bytes: Metered::host(self.input_size),
            response_bytes: Metered::host(bytes_out),
        };
        event.bytes_in = self.input_size;
        event.bytes_out = bytes_out;
        event.guest_reported.guest_handler_ms = guest_handler_ms;
        event.guest_reported.guest_init_ms = self.meter.guest_init_ms;
        self.record_usage(event).await;
    }

    /// An environment this driver booted ended before any attempt was
    /// dispatched onto it (a boot, handshake or init failure, a cancel or a
    /// client deadline during initialization). Its single
    /// `EnvironmentStopped` carries what was measured of the boot and the
    /// initialization, with the invocation's outcome and no attempt
    /// (PLT-4642). Nothing when no environment is left to account for.
    async fn emit_environment_abandoned(&mut self) {
        let Some(env_id) = self.env_id.take() else {
            return;
        };
        use tachyon_serverless_domain::InvocationStatus as S;
        let now = self.now();
        let row = self.svc.repos.environments.get(&env_id).ok().flatten();
        let outcome = match self.load_invocation().map(|inv| inv.status) {
            Some(S::Cancelled) => UsageOutcome::Cancelled,
            Some(S::Failed { error }) if error.class == ErrorClass::Timeout => {
                UsageOutcome::Timeout
            }
            Some(S::OutcomeUnknown { .. }) => UsageOutcome::OutcomeUnknown,
            _ => UsageOutcome::Failed,
        };
        self.seq += 1;
        let mut event =
            self.usage_event(&env_id, None, UsageEventType::EnvironmentStopped, self.seq);
        event.monotonic_duration_ms = row.as_ref().map(|env| environment_lifetime_ms(env, now));
        event.outcome = Some(outcome);
        event.segments = UsageSegments {
            queue_wait_ms: Metered::host_opt(self.meter.queue_wait.map(ceil_ms)),
            vm_base_boot_ms: Metered::host_opt(self.meter.vm_base_boot.map(ceil_ms)),
            user_init_ms: Metered::host_opt(self.meter.user_init.map(ceil_ms)),
            handler_ms: Metered::host(0),
            // These paths terminate without timing it.
            teardown_ms: Metered::unknown(),
            idle_pooled_ms: Metered::host(0),
        };
        event.guest_reported.guest_init_ms = self.meter.guest_init_ms;
        self.record_usage(event).await;
    }

    /// Record why admission ended this invocation while it was queued.
    fn fail_admission(&self, error: WaitError<CancelKind>, while_what: &str) {
        let error = match error {
            WaitError::Timeout(reason) => InvocationError::new(
                ErrorClass::QueueTimeout,
                error_type_for(reason),
                format!(
                    "no capacity became available before the queue deadline ({})",
                    reason.as_str()
                ),
            ),
            WaitError::Rejected(r)
                if matches!(
                    r.reason,
                    RejectReason::CircuitOpen | RejectReason::FunctionDeleted
                ) =>
            {
                InvocationError::new(
                    ErrorClass::PlatformError,
                    error_type_for(r.reason),
                    r.message,
                )
            }
            WaitError::Rejected(r) => InvocationError::new(
                ErrorClass::QueueTimeout,
                error_type_for(r.reason),
                r.message,
            ),
            WaitError::Cancelled(kind) => cancel_error(kind, &format!("while {while_what}")),
            WaitError::Closed => InvocationError::new(
                ErrorClass::PlatformError,
                "Host.CapacityClosed",
                "admission closed while the invocation waited",
            ),
        };
        self.fail_invocation(error);
    }

    /// Complete the admission started in `invoke`. The wait ends at the
    /// queue deadline, which never exceeds the client deadline.
    async fn acquire_capacity(&mut self) -> Result<Grant, WaitError<CancelKind>> {
        let deadline = (self.accepted_at + self.svc.capacity.queue_timeout())
            .min(Instant::now() + self.client_remaining());
        let pending = self.pre.take().expect("admission is taken once");
        pending
            .wait(deadline.into(), wait_cancel(&mut self.cancel_rx))
            .await
    }

    /// Make sure this driver holds a *cold* reservation before it boots an
    /// environment: a promise of a pooled environment that was not there is
    /// redeemed on the spot when the node allows it, and otherwise the
    /// invocation queues again (at the front of its tenant's queue, until
    /// its client deadline) for a cold start. `false` means the invocation
    /// has already been failed.
    async fn ensure_cold_grant(&mut self) -> bool {
        let svc = self.svc.clone();
        match self.grant.take() {
            Some(g) if g.kind() == GrantKind::Cold => {
                self.grant = Some(g);
                return true;
            }
            Some(promise) => {
                if let Some(cold) = svc.admission.redeem(promise) {
                    self.grant = Some(cold);
                    return true;
                }
            }
            None => {}
        }
        let ticket = svc.admission.ticket(
            &self.function.tenant_id,
            &self.revision,
            self.input_size,
            self.client_deadline,
        );
        let pending = match svc.admission.requeue_cold(ticket) {
            Ok(p) => p,
            Err(rejection) => {
                self.fail_admission(WaitError::Rejected(rejection), "waiting for a cold start");
                return false;
            }
        };
        let until = Instant::now() + self.client_remaining();
        match pending
            .wait(until.into(), wait_cancel(&mut self.cancel_rx))
            .await
        {
            Ok(g) => {
                self.grant = Some(g);
                true
            }
            Err(e) => {
                self.fail_admission(e, "waiting for a cold start");
                false
            }
        }
    }

    /// Whether the invoke gate still lets this invocation boot an environment
    /// (PLT-4636). A refusal fails the invocation with the gate's own error,
    /// which stays distinct from every admission refusal (PLT-4634).
    fn cold_start_permitted(&self) -> bool {
        match self
            .svc
            .gate
            .permit_cold_start(&self.revision, &self.function.tenant_id)
        {
            Ok(()) => true,
            Err((kind, message)) => {
                tracing::warn!(
                    invocation_id = %self.invocation_id,
                    error_type = kind.error_type(),
                    "cold start refused"
                );
                self.svc.admission.metrics().gate_refusal(kind.error_type());
                self.fail_invocation(InvokeGate::invocation_error(kind, message));
                false
            }
        }
    }

    /// Record a failed cold start for the revision's circuit breaker and
    /// release the reservation (the environment is already terminated).
    fn boot_failed(&mut self) {
        if let Some(g) = self.grant.take() {
            g.start_result(false);
        }
    }

    /// The whole lifecycle after acceptance. Returns the handler output on
    /// success. Every early return has already recorded the terminal state.
    async fn execute(&mut self) -> Option<serde_json::Value> {
        // 5. capacity ------------------------------------------------------
        match self.acquire_capacity().await {
            Ok(grant) => self.grant = Some(grant),
            Err(e) => {
                self.fail_admission(e, "queued");
                return None;
            }
        }
        let queue_wait = self.accepted_at.elapsed();
        self.meter.queue_wait = Some(queue_wait);
        let queue_wait_ms = queue_wait.as_millis() as u64;
        if self.client_deadline_elapsed() {
            self.fail_invocation(InvocationError::new(
                ErrorClass::Timeout,
                "Host.ClientDeadline",
                "client deadline elapsed before the invocation could start",
            ));
            return None;
        }

        // 6..10. one dispatch onto an environment, warm or cold ------------
        let output = match self.attempt(1, queue_wait_ms, true).await {
            Attempted::Done(output) => output,
            // The reused environment was already gone when the `Invoke` frame
            // was written, so the handler cannot have started: retry exactly
            // once, cold. `warm_allowed = false` makes `RetryCold` unreachable
            // in the retry, so this can never loop.
            Attempted::RetryCold => match self.attempt(2, queue_wait_ms, false).await {
                Attempted::Done(output) => output,
                Attempted::RetryCold => None,
            },
        };
        // Whatever this driver still holds (an environment that was
        // terminated, or a reservation it never used) is released here.
        self.grant = None;
        output
    }

    /// One dispatch of this invocation onto one environment: acquire the
    /// environment (warm when `warm_allowed` and the pool has a match, cold
    /// otherwise), record attempt and lease, invoke, classify, and settle
    /// everything. `number` is the attempt number in the ledger.
    async fn attempt(&mut self, number: u32, queue_wait_ms: u64, warm_allowed: bool) -> Attempted {
        let svc = self.svc.clone();

        // 6. environment ---------------------------------------------------
        // Reuse is decided before anything is created. The reuse key carries
        // the generation of the *resolved* secret bindings, so the bindings are
        // resolved first; a binding this tenant cannot use fails the invocation
        // here, without a ledger row and without booting anything.
        let tenant = self.function.tenant_id.clone();
        // A function deleted while this invocation queued is refused before
        // anything boots (PLT-4635).
        if svc.gate.cache().function_deleted(&self.function.id).await {
            self.fail_function_deleted();
            return Attempted::Done(None);
        }
        let prospective_env_id = EnvironmentId::from_ulid(svc.ids.next_ulid());
        let init_timeout = Duration::from_secs(u64::from(
            self.revision.spec.execution.initialization_timeout_seconds,
        ));
        let init_deadline_ts = (self.now()
            + chrono::Duration::milliseconds(init_timeout.as_millis() as i64))
        .min(self.client_deadline);
        let secret_env = match self.resolve_secret_env(&prospective_env_id).await {
            Ok(resolved) => resolved,
            Err(failure) => {
                let error = failure.invocation_error();
                tracing::warn!(
                    environment_id = %prospective_env_id,
                    binding_ref = %failure.binding_ref,
                    reason = failure.reason(),
                    error = %failure.error,
                    "secret binding could not be resolved; environment not created"
                );
                self.fail_invocation(error);
                return Attempted::Done(None);
            }
        };
        let reuse_key = self.reuse_key(&secret_env);
        // The newest key of the revision: pooled environments under an older
        // one (a rotated secret) are drained instead of waiting for their TTL
        // (PLT-4635).
        if svc.pool.note_current_key(&reuse_key) {
            tracing::info!(
                revision_id = %self.revision.id,
                "the revision's reuse key changed; its older pooled environments are drained"
            );
        }
        // A pooled environment is taken only when its reuse key matches in
        // every field. Everything else boots cold — which, with both shipped
        // providers, is every invocation: the pool never hands anything out
        // unless the provider reports both idle capabilities as `Supported`.
        let warm = match warm_allowed {
            true => svc.pool.claim(&reuse_key).await,
            // A retry after an undelivered warm dispatch: cold only.
            false => None,
        };
        let prepared = match warm {
            Some(mut warm) => {
                // The pooled environment's reservation becomes this driver's;
                // whatever it held for a cold start is released.
                if let Some(idle) = warm.reservation.take() {
                    let holder = self.grant.take();
                    self.grant = Some(svc.admission.adopt(holder, idle));
                }
                self.prepare_warm(warm)
            }
            None => {
                // A new environment needs the revision and the tenant's
                // authorization to still be valid, a provider whose control
                // API answers and, during a control-plane outage, the outage
                // policy's consent (PLT-4636). Nothing is booted otherwise.
                // Checked before admission reserves anything, so a refusal
                // never waits in the capacity queue, and again after it,
                // because that wait can be long (PLT-4634).
                if !self.cold_start_permitted() {
                    return Attempted::Done(None);
                }
                if !self.ensure_cold_grant().await || !self.cold_start_permitted() {
                    return Attempted::Done(None);
                }
                match self
                    .prepare_cold(prospective_env_id, reuse_key, secret_env, init_timeout)
                    .await
                {
                    Some(prepared) => prepared,
                    None => {
                        // Whatever booted and failed before a dispatch is
                        // still accounted for, once (PLT-4642).
                        self.emit_environment_abandoned().await;
                        return Attempted::Done(None);
                    }
                }
            }
        };
        let Prepared {
            mut env,
            mut session,
            start_kind,
            environment_boot_ms,
            runtime_init_ms,
            warm,
            logs,
        } = prepared;
        let env_id = env.id.clone();

        // Never start the handler after the client deadline. Checked before
        // any attempt, lease or Running state is recorded.
        if self.client_deadline_elapsed() {
            self.stop_for_client_deadline(Some(&mut session), &mut env, &logs)
                .await;
            self.emit_environment_abandoned().await;
            return Attempted::Done(None);
        }
        // Nor for a function whose deletion landed while the environment was
        // being prepared (PLT-4635). Nothing was dispatched, so this is a
        // refusal, not an outcome.
        if svc.gate.cache().function_deleted(&self.function.id).await {
            logs.platform(
                LogPhase::Init,
                None,
                "the function was deleted before the handler was dispatched; not starting it",
            );
            let _ = session.shutdown("function deleted").await;
            let _ = svc
                .provider
                .terminate_environment(&env_id, TerminateReason::Cancelled)
                .await;
            self.grant = None;
            let now = self.now();
            let _ = env.mark_stopped(now);
            self.save_env(&env);
            self.seq += 1;
            self.emit_usage(
                &env_id,
                None,
                UsageEventType::EnvironmentStopped,
                self.seq,
                Some(environment_lifetime_ms(&env, now)),
                0,
                0,
            )
            .await;
            self.env_id = None;
            self.fail_function_deleted();
            return Attempted::Done(None);
        }

        // 8. attempt + lease + invoke --------------------------------------
        let Some(mut inv) = self.load_invocation() else {
            tracing::warn!(invocation_id = %self.invocation_id, "invocation vanished before dispatch");
            let _ = session.shutdown("invocation missing").await;
            let _ = env.mark_failed("invocation record missing", self.now());
            self.save_env(&env);
            let _ = svc
                .provider
                .terminate_environment(&env_id, TerminateReason::Crashed)
                .await;
            self.emit_environment_abandoned().await;
            return Attempted::Done(None);
        };
        let timeout = Duration::from_secs(u64::from(self.revision.spec.execution.timeout_seconds));
        let now = self.now();
        let attempt_id = AttemptId::from_ulid(svc.ids.next_ulid());
        // The execution deadline never exceeds the client deadline. The guest
        // observes exactly the deadline the host enforces.
        let full_execution_deadline =
            now + chrono::Duration::milliseconds(timeout.as_millis() as i64);
        let execution_deadline_ts = full_execution_deadline.min(self.client_deadline);
        let execution_clamped = execution_deadline_ts < full_execution_deadline;
        let execution_wait = (execution_deadline_ts - now)
            .to_std()
            .unwrap_or(Duration::ZERO);
        // The slot: assigned at the next epoch, together with its lease, its
        // attempt and the invocation's `Running`, in one store transaction.
        let expected_epoch = env.epoch;
        let mut assigned = env.clone();
        if let Err(e) = assigned.assign(now) {
            tracing::warn!(error = %e, environment_id = %env_id, "environment cannot be assigned");
        }
        let mut attempt = InvocationAttempt::dispatch(
            attempt_id.clone(),
            self.invocation_id.clone(),
            tenant.clone(),
            number,
            env_id.clone(),
            assigned.epoch,
            start_kind,
            now,
        );
        let lease_id = LeaseId::from_ulid(svc.ids.next_ulid());
        let lease = ExecutionLease::acquire(
            lease_id.clone(),
            env_id.clone(),
            attempt_id.clone(),
            tenant.clone(),
            assigned.epoch,
            execution_deadline_ts,
            now,
        )
        .owned_by(
            svc.dispatcher.id().clone(),
            svc.dispatcher.lease_expiry(now),
        );
        let running = match number {
            1 => inv.mark_running(
                attempt_id.clone(),
                execution_deadline_ts,
                init_deadline_ts,
                now,
            ),
            // A retry after an undelivered dispatch: the invocation is already
            // Running and keeps the `started_at` of its first dispatch.
            _ => inv.mark_retry(attempt_id.clone(), execution_deadline_ts, init_deadline_ts),
        };
        if let Err(e) = running {
            tracing::warn!(error = %e, "cannot record the invocation as running");
        }
        let acquired = svc.repos.slots.acquire(SlotAcquire {
            env: assigned.clone(),
            expected_epoch,
            lease: lease.clone(),
            attempt: attempt.clone(),
            invocation: Some(inv),
        });
        match acquired {
            Ok(AcquireOutcome::Acquired) => {}
            outcome => {
                let reason = match outcome {
                    Ok(AcquireOutcome::Lost(reason)) => reason,
                    Err(e) => e.to_string(),
                    Ok(AcquireOutcome::Acquired) => unreachable!(),
                };
                // Nothing was dispatched: the handler provably did not start.
                tracing::warn!(
                    environment_id = %env_id,
                    invocation_id = %self.invocation_id,
                    reason = %reason,
                    "slot acquisition lost; the handler was not started"
                );
                logs.platform(
                    LogPhase::Init,
                    None,
                    &format!(
                        "execution slot could not be acquired ({reason}); not starting the handler"
                    ),
                );
                let _ = session.shutdown("slot lost").await;
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::Crashed)
                    .await;
                let _ = env.mark_failed("slot acquisition lost", self.now());
                self.save_env(&env);
                self.fail_invocation(InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.SlotLost",
                    format!("the execution slot could not be acquired: {reason}"),
                ));
                self.emit_environment_abandoned().await;
                return Attempted::Done(None);
            }
        }
        env = assigned;
        self.epoch = env.epoch;
        self.attempt_id = Some(attempt_id.clone());
        self.lease_id = Some(lease_id.clone());

        let deadline_ms = execution_deadline_ts.timestamp_millis().max(0) as u64;
        // What the guest actually computes its own deadline from. An absolute
        // host timestamp is meaningless to a guest that was quiesced — its
        // clock stopped with it, so after a resume of arbitrary length it
        // would read the deadline as further away than it is (PLT-4633 review
        // F5, docs/protocol.md §A). The host keeps enforcing
        // `execution_deadline` itself either way.
        let remaining_ms = (execution_deadline_ts - self.now())
            .num_milliseconds()
            .max(0) as u64;
        // A warm dispatch may still have to be repeated cold, and then the
        // payload is needed a second time. A cold one hands over its only copy.
        let retryable = warm_allowed && start_kind == StartKind::Warm;
        let payload = self.payload.checkout(retryable);
        let dispatched_at = Instant::now();
        let sent = session
            .send_invoke(InvokeParams {
                invocation_id: self.invocation_id.clone(),
                attempt_id: attempt_id.clone(),
                epoch: env.epoch,
                event_type: self.event_kind.event_type().to_string(),
                deadline_ms,
                remaining_ms,
                trace_id: self.trace_id.clone(),
                payload,
            })
            .await;
        // The frame is either on the wire or was refused before anything was
        // written: the retained copy can only still be needed when the
        // connection was lost, which is the one outcome that is retried.
        let can_retry = self
            .payload
            .settle(matches!(sent, Err(SessionError::Disconnected)));
        let dispatch = match sent {
            // The pooled guest was already gone: nothing reached it, so the
            // handler cannot have started. What the guest queued before it
            // closed still classifies the attempt, exactly as it does for a
            // cold dispatch (docs/threat-model.md §9); only the retry is
            // warm-specific.
            Err(SessionError::Disconnected) if can_retry => {
                self.retire_after_undelivered_warm(
                    &mut env,
                    &mut session,
                    attempt,
                    lease_id.clone(),
                    &logs,
                    queue_wait_ms,
                    warm,
                )
                .await;
                return Attempted::RetryCold;
            }
            Ok(()) => {
                self.seq += 1;
                self.emit_usage(
                    &env_id,
                    Some(&attempt_id),
                    UsageEventType::HandlerStarted,
                    self.seq,
                    None,
                    self.input_size,
                    0,
                )
                .await;
                tokio::select! {
                    o = session.wait_result(dispatched_at + execution_wait) => Dispatch::Finished(o),
                    k = wait_cancel(&mut self.cancel_rx) => Dispatch::Cancelled(k),
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, environment_id = %env_id, "invoke frame was not delivered");
                let drained = match error {
                    SessionError::Disconnected => Some(
                        session
                            .wait_result(Instant::now() + UNDELIVERED_DRAIN)
                            .await,
                    ),
                    _ => None,
                };
                Dispatch::NotDelivered { error, drained }
            }
        };
        let handler_started = !matches!(dispatch, Dispatch::NotDelivered { .. });
        let handler_elapsed = dispatched_at.elapsed();
        let handler_ms = handler_elapsed.as_millis() as u64;
        if handler_started {
            svc.admission
                .observe_duration(&self.revision.id, dispatched_at.elapsed());
        }
        let finish_started = Instant::now();

        // 9. classify ------------------------------------------------------
        let max_response = svc.limits.max_response_bytes;
        let inline_max = svc.invoke_cfg.inline_output_max_bytes;
        let mut output_value = None;
        let mut bytes_out = 0u64;
        // Guest-reported, kept apart from the host's `handler_ms` (PLT-4642).
        let mut guest_reported_handler_ms = None;
        let (result, env_end): Classified = match dispatch {
            Dispatch::Finished(Outcome::Response {
                payload,
                guest_handler_ms,
            }) => {
                guest_reported_handler_ms = guest_handler_ms;
                if let Some(ms) = guest_handler_ms {
                    env.evidence
                        .details
                        .insert("guest_handler_ms".into(), ms.into());
                }
                let bytes = serde_json::to_vec(&payload).unwrap_or_default();
                bytes_out = bytes.len() as u64;
                if bytes_out > max_response {
                    (
                        Err(InvocationError::new(
                            ErrorClass::PlatformError,
                            "Host.ResponseTooLarge",
                            format!("response is {bytes_out} bytes (max {max_response})"),
                        )),
                        EnvEnd {
                            reason: TerminateReason::Completed,
                            failure: None,
                        },
                    )
                } else {
                    let http_status = match self.event_kind {
                        EventKind::Http => payload
                            .get("status")
                            .and_then(serde_json::Value::as_u64)
                            .and_then(|s| u16::try_from(s).ok()),
                        EventKind::Json => None,
                    };
                    let output_ref = if bytes_out <= inline_max {
                        PayloadRef::Inline {
                            bytes_base64: base64::engine::general_purpose::STANDARD.encode(&bytes),
                            size_bytes: bytes_out,
                        }
                    } else {
                        PayloadRef::Digest {
                            digest: Sha256Digest::of_bytes(&bytes),
                            size_bytes: bytes_out,
                        }
                    };
                    output_value = Some(payload);
                    (
                        Ok((Some(output_ref), http_status)),
                        EnvEnd {
                            reason: TerminateReason::Completed,
                            failure: None,
                        },
                    )
                }
            }
            Dispatch::Finished(Outcome::GuestError {
                kind,
                error_type,
                message,
                stack_trace,
                guest_handler_ms,
            }) => {
                guest_reported_handler_ms = guest_handler_ms;
                if let Some(ms) = guest_handler_ms {
                    env.evidence
                        .details
                        .insert("guest_handler_ms".into(), ms.into());
                }
                if let Some(trace) = stack_trace {
                    logs.platform(LogPhase::Handler, Some(&attempt_id), &trace);
                }
                let (error, end) = classify_guest_error(kind, error_type, message);
                (Err(error), end)
            }
            Dispatch::Finished(Outcome::Timeout) => {
                let (error_type, message, line) = if execution_clamped {
                    (
                        "Host.ClientDeadline",
                        "client deadline elapsed while the handler was running".to_string(),
                        format!(
                            "client deadline elapsed after {} ms of execution; cancelling",
                            execution_wait.as_millis()
                        ),
                    )
                } else {
                    (
                        "Host.Timeout",
                        format!("handler did not finish within {} s", timeout.as_secs()),
                        format!(
                            "execution deadline ({} s) elapsed; cancelling",
                            timeout.as_secs()
                        ),
                    )
                };
                logs.platform(LogPhase::Handler, Some(&attempt_id), &line);
                let grace = svc.invoke_cfg.cancel_grace();
                let _ = session.cancel(&attempt_id, grace).await;
                session
                    .drain_until_closed(Instant::now() + grace + Duration::from_millis(200))
                    .await;
                (
                    Err(InvocationError::new(
                        ErrorClass::Timeout,
                        error_type,
                        message,
                    )),
                    EnvEnd {
                        reason: TerminateReason::Timeout,
                        failure: Some("execution timeout"),
                    },
                )
            }
            // Only reachable after the Invoke frame was written: the handler
            // may have run (docs/threat-model.md §9).
            Dispatch::Finished(Outcome::Disconnected) => (
                Err(InvocationError::new(
                    ErrorClass::OutcomeUnknown,
                    "Host.OutcomeUnknown",
                    "bridge connection was lost before a result arrived",
                )),
                EnvEnd {
                    reason: TerminateReason::Crashed,
                    failure: Some("bridge disconnected"),
                },
            ),
            Dispatch::Cancelled(kind) => {
                let grace = svc.invoke_cfg.cancel_grace();
                let _ = session.cancel(&attempt_id, grace).await;
                session
                    .drain_until_closed(Instant::now() + grace + Duration::from_millis(200))
                    .await;
                (
                    Err(match kind {
                        CancelKind::Drain => InvocationError::new(
                            ErrorClass::Timeout,
                            DRAIN_TIMEOUT,
                            "the revision was drained and the handler was still running when \
                             the drain timeout passed",
                        ),
                        _ => cancel_error(kind, "while the handler was running"),
                    }),
                    EnvEnd {
                        reason: cancel_reason(kind),
                        failure: (kind == CancelKind::Drain).then_some("drain timeout"),
                    },
                )
            }
            Dispatch::NotDelivered { error, drained } => {
                logs.platform(
                    LogPhase::Handler,
                    Some(&attempt_id),
                    &format!("invocation was not delivered to the guest: {error}"),
                );
                let (error, end) = undelivered_invoke(&error, drained);
                (Err(error), end)
            }
        };

        // 10. record, release, terminate ----------------------------------
        let now = self.now();
        let response_ms = finish_started.elapsed().as_millis() as u64;
        attempt.timings = tachyon_serverless_domain::AttemptTimings {
            queue_wait_ms: Some(queue_wait_ms),
            environment_boot_ms: Some(environment_boot_ms),
            runtime_init_ms: Some(runtime_init_ms),
            // A warm start reports what it really cost instead of nothing:
            // boot and init are zero because nothing booted, and the resume
            // and the readiness check say what happened instead (PLT-4633).
            resume_ms: warm.map(|w| w.resume_ms),
            readiness_ms: warm.map(|w| w.readiness_ms),
            handler_ms: Some(handler_ms),
            response_ms: Some(response_ms),
            total_ms: Some(self.accepted_at.elapsed().as_millis() as u64),
        };
        match &result {
            Ok(_) => {
                let _ = attempt.succeed(now);
            }
            Err(e) if e.class == ErrorClass::OutcomeUnknown => {
                let _ = attempt.outcome_unknown(e.clone(), now);
            }
            Err(e) => {
                let _ = attempt.fail(e.clone(), now);
            }
        }
        // PLT-4637: start kind, phase durations and the boot identity check
        // (a warm attempt must report the boot id its environment booted with).
        let boot_check = svc.admission.metrics().observe_attempt(
            &env_id,
            start_kind,
            attempt.status.name(),
            &attempt.timings,
            env.evidence.guest_boot_id.as_deref(),
        );
        if boot_check == crate::metrics::BootCheck::BootChanged {
            tracing::error!(
                environment_id = %env_id,
                attempt_id = %attempt_id,
                "the environment reported a different guest boot id than it booted with"
            );
        }
        let settled_invocation = self.load_invocation().map(|mut inv| {
            let r = match &result {
                Ok((output, http_status)) => inv.mark_succeeded(output.clone(), *http_status, now),
                Err(e) => match e.class {
                    ErrorClass::Cancelled => inv.mark_cancelled(now),
                    ErrorClass::OutcomeUnknown => inv.mark_outcome_unknown(e.message.clone(), now),
                    _ => inv.mark_failed(e.clone(), now),
                },
            };
            if let Err(err) = r {
                tracing::warn!(error = %err, invocation_id = %inv.id, "cannot record invocation outcome");
            }
            inv
        });
        let status = settled_invocation
            .as_ref()
            .map(|inv| inv.status.name())
            .unwrap_or("missing");
        // The fenced callback: accepted only while this lease is still the
        // slot's current lease at this epoch.
        let completion = svc.repos.slots.complete(SlotCompletion {
            lease_id: lease_id.clone(),
            attempt: attempt.clone(),
            invocation: settled_invocation,
            now,
        });
        let completed = match completion {
            Ok(CompletionOutcome::Accepted) => true,
            Ok(CompletionOutcome::Stale(reason)) => {
                tracing::warn!(
                    invocation_id = %self.invocation_id,
                    attempt_id = %attempt_id,
                    epoch = env.epoch,
                    reason = %reason,
                    "stale completion refused: another dispatcher reclaimed this slot"
                );
                false
            }
            Err(e) => {
                tracing::warn!(error = %e, invocation_id = %self.invocation_id, "cannot record the attempt outcome");
                false
            }
        };
        if completed {
            tracing::info!(
                invocation_id = %self.invocation_id,
                status,
                handler_ms,
                environment_boot_ms,
                runtime_init_ms,
                "invocation finished"
            );
        } else {
            // The ledger is the answer now (a reclaimer settled it); never
            // hand out a result the store refused.
            output_value = None;
        }
        if handler_started {
            self.seq += 1;
            self.emit_usage(
                &env_id,
                Some(&attempt_id),
                UsageEventType::HandlerFinished,
                self.seq,
                Some(handler_ms),
                self.input_size,
                bytes_out,
            )
            .await;
        }

        // The environment goes back into the pool only when this attempt left
        // it healthy: a clean end with nothing to clean up, an outcome that
        // says the guest is still there, reuse allowed by both gates, and no
        // shutdown in progress (a pooled environment must never outlive the
        // process holding its session). Everything else keeps
        // destroy-after-invoke (docs/architecture.md §4).
        //
        // The outcome matters on its own: `EnvEnd` is the *terminate reason*,
        // and a guest panic ends as `Completed` there while leaving a process
        // that just died behind. Only a success and a `UserError` — the
        // handler returned an error and the guest reported it — may be reused.
        let outcome_allows_reuse = match &result {
            Ok(_) => true,
            Err(e) => e.class == ErrorClass::UserError,
        };
        let may_reuse = outcome_allows_reuse
            && completed
            && env_end.failure.is_none()
            && env_end.reason == TerminateReason::Completed
            && !svc.draining.load(Ordering::SeqCst);
        // Whatever the guest queued after its result belongs to *this*
        // attempt: take it here, under this attempt's log context, before the
        // session can carry another one. An `Exited` frame or an EOF makes the
        // session unusable, and `release` then refuses to pool it.
        let teardown_started = Instant::now();
        let outcome = usage_outcome(&result, completed);
        let handler = handler_started.then_some(handler_elapsed);
        if may_reuse {
            session.drain_stale().await;
        }
        let release = if may_reuse {
            // The pool takes the environment's event count with it, so the
            // next attempt on it continues where this one stopped.
            //
            // `release` does not pause anything on this path any more: it
            // takes the environment over and returns, so the caller's response
            // never waits for the hypervisor's pause and its API timeout
            // (PLT-4633 review F4). `Ok` means the pool owns the environment
            // from here: it publishes the row only once the guest really is
            // quiesced, and if it cannot be, the pool terminates and meters it
            // instead. Either way this driver is done with it.
            //
            // The count handed over already includes this attempt's
            // `AttemptSettled`, emitted right after (PLT-4642).
            svc.pool.clone().release_for(
                &env,
                session,
                self.seq + 1,
                &mut self.grant,
                self.revision.spec.execution.min_ready,
            )
        } else {
            Err(Box::new(session))
        };
        let mut session = match release {
            Ok(()) => {
                logs.platform(
                    LogPhase::Shutdown,
                    None,
                    &format!(
                        "environment {env_id} handed to the pool at epoch {} (quiescing)",
                        env.epoch
                    ),
                );
                // The pool owns the environment and its session now: it is not
                // terminated, it did not stop (so no `EnvironmentStopped`), and
                // a later panic cleanup must not reclaim it.
                self.emit_attempt_settled(
                    &env_id,
                    &attempt_id,
                    number,
                    outcome,
                    handler,
                    Some(teardown_started.elapsed()),
                    bytes_out,
                    guest_reported_handler_ms,
                )
                .await;
                self.env_id = None;
                self.attempt_id = None;
                self.lease_id = None;
                return Attempted::Done(output_value);
            }
            Err(session) => *session,
        };
        let host_sample = sample_before_terminate(svc.provider.as_ref(), &env_id).await;
        let _ = session
            .shutdown(match env_end.reason {
                TerminateReason::Completed => "completed",
                TerminateReason::Timeout => "timeout",
                TerminateReason::Cancelled => "cancelled",
                TerminateReason::Crashed => "crashed",
                TerminateReason::Shutdown => "shutdown",
                TerminateReason::InitFailed => "init failed",
                // The driver never terminates a quiesced environment (only
                // the pool owns those), but the reason is part of the enum.
                TerminateReason::Quiesced => "quiesced",
                TerminateReason::Reconcile => "reconcile",
            })
            .await;
        let terminated = svc
            .provider
            .terminate_environment(&env_id, env_end.reason)
            .await;
        // The environment is gone (or its terminate failed and the startup
        // reconcile owns it): its reservation goes back to the node.
        self.grant = None;
        // Teardown is known only when the terminate succeeded (PLT-4642).
        let teardown = terminated.is_ok().then(|| teardown_started.elapsed());
        self.emit_attempt_settled(
            &env_id,
            &attempt_id,
            number,
            outcome,
            handler,
            teardown,
            bytes_out,
            guest_reported_handler_ms,
        )
        .await;
        match terminated {
            Ok(report) => logs.platform(
                LogPhase::Shutdown,
                None,
                &format!(
                    "environment terminated ({:?}); cleaned: {}",
                    env_end.reason,
                    report.cleaned.join(", ")
                ),
            ),
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %env_id, "terminate failed");
            }
        }
        let now = self.now();
        let _ = match env_end.failure {
            None => env.mark_stopped(now),
            Some(reason) => env.mark_failed(reason, now),
        };
        self.save_env(&env);
        self.seq += 1;
        let mut stopped = self.usage_event(
            &env_id,
            Some(&attempt_id),
            UsageEventType::EnvironmentStopped,
            self.seq,
        );
        // The environment's whole life, not this attempt's share of it:
        // the same quantity the pool reports for one it ends itself.
        stopped.monotonic_duration_ms = Some(environment_lifetime_ms(&env, now));
        stopped.segments.teardown_ms = Metered::host_opt(teardown.map(ceil_ms));
        stopped.segments.idle_pooled_ms = Metered::host(0);
        stopped.resources = usage_resources(&self.revision.spec.resources, host_sample.as_ref());
        self.record_usage(stopped).await;
        // Everything is recorded and terminated: nothing left for a panic
        // cleanup to do.
        self.env_id = None;
        self.attempt_id = None;
        self.lease_id = None;
        Attempted::Done(output_value)
    }

    /// The reuse key of this revision under the resolved secret values.
    fn reuse_key(&self, secret_env: &[(String, String)]) -> ReuseKey {
        reuse_key_for(
            &self.function.tenant_id,
            &self.revision,
            secret_binding_generation(self.revision.spec.secrets.iter().zip(secret_env).map(
                |(binding, (_, value))| {
                    (
                        binding.env_name.as_str(),
                        binding.binding_ref.as_str(),
                        value.as_str(),
                    )
                },
            )),
        )
    }

    /// The function was deleted before this invocation was dispatched.
    fn fail_function_deleted(&self) {
        self.fail_invocation(InvocationError::new(
            ErrorClass::PlatformError,
            FUNCTION_DELETED,
            format!(
                "function {} was deleted before the invocation started; it was not run",
                self.function.id
            ),
        ));
    }

    /// Boot one environment for `min_ready` and hand it to the pool
    /// ([`InvokeService::prestart`]).
    async fn prewarm(&mut self) -> Result<(), String> {
        let svc = self.svc.clone();
        let env_id = EnvironmentId::from_ulid(svc.ids.next_ulid());
        let init_timeout = Duration::from_secs(u64::from(
            self.revision.spec.execution.initialization_timeout_seconds,
        ));
        let secret_env = self
            .resolve_secret_env(&env_id)
            .await
            .map_err(|f| format!("secret binding unavailable ({})", f.reason()))?;
        let reuse_key = self.reuse_key(&secret_env);
        svc.pool.note_current_key(&reuse_key);
        svc.gate
            .permit_cold_start(&self.revision, &self.function.tenant_id)
            .map_err(|(kind, _)| kind.error_type().to_string())?;
        let Some(prepared) = self
            .prepare_cold(env_id, reuse_key, secret_env, init_timeout)
            .await
        else {
            self.grant = None;
            self.emit_environment_abandoned().await;
            return Err("the environment did not become ready".into());
        };
        let Prepared {
            mut env, session, ..
        } = prepared;
        let env_id = env.id.clone();
        // A finished attempt hands the pool a `Busy` row at its epoch; a
        // pre-started one hands it the `Ready` row at epoch 0, which the
        // ledger publishes as idle only while it never served an attempt
        // (its first attempt will be epoch 1).
        let pooled = svc.pool.clone().release_for(
            &env,
            session,
            self.seq,
            &mut self.grant,
            self.revision.spec.execution.min_ready,
        );
        match pooled {
            Ok(()) => {
                tracing::info!(
                    environment_id = %env_id,
                    revision_id = %self.revision.id,
                    "pre-started environment handed to the pool (min_ready)"
                );
                self.env_id = None;
                Ok(())
            }
            Err(session) => {
                let mut session = *session;
                let _ = session.shutdown("not pooled").await;
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::Completed)
                    .await;
                self.grant = None;
                let now = self.now();
                let _ = env.mark_stopped(now);
                self.save_env(&env);
                self.seq += 1;
                self.emit_usage(
                    &env_id,
                    None,
                    UsageEventType::EnvironmentStopped,
                    self.seq,
                    Some(environment_lifetime_ms(&env, now)),
                    0,
                    0,
                )
                .await;
                self.env_id = None;
                Err("the pool did not take the pre-started environment".into())
            }
        }
    }

    /// Resolve the revision's secret bindings for `env_id`, in binding order.
    ///
    /// Split out of [`Self::hello_ack_params`] because the reuse key needs the
    /// generation of the *resolved* bindings before the pipeline can decide
    /// whether a pooled environment may serve this attempt: a value that was
    /// rotated must never reach a guest started under the previous one.
    async fn resolve_secret_env(
        &self,
        env_id: &EnvironmentId,
    ) -> Result<Vec<(String, String)>, SecretResolutionFailure> {
        let ctx = SecretDeliveryContext {
            tenant_id: self.function.tenant_id.clone(),
            revision_id: self.revision.id.clone(),
            environment_id: env_id.clone(),
            epoch: 1,
        };
        let mut resolved = Vec::with_capacity(self.revision.spec.secrets.len());
        for binding in &self.revision.spec.secrets {
            let value = self
                .svc
                .secrets
                .resolve(&ctx, &binding.binding_ref)
                .await
                .map_err(|error| SecretResolutionFailure {
                    binding_ref: binding.binding_ref.clone(),
                    error,
                })?;
            resolved.push((binding.env_name.clone(), value.expose().to_string()));
        }
        Ok(resolved)
    }

    /// Take over a pooled environment.
    ///
    /// The ledger row is already `Busy` at its next epoch (the pool advanced it
    /// inside the claim, in one store mutation), and the guest is long past
    /// `Hello` and `Ready`, so nothing boots and nothing initializes here.
    fn prepare_warm(&mut self, warm: WarmEnvironment) -> Prepared {
        let WarmEnvironment {
            environment,
            mut session,
            sequence,
            timings,
            // Taken over by `attempt` before this is called.
            reservation: _,
            idle_ms,
        } = warm;
        // Nothing booted: the host work of a warm start is the resume and the
        // readiness check (PLT-4642 `vm_base_boot_ms`), and no initialization.
        self.meter.environment_changed();
        self.meter.vm_base_boot = Some(Duration::from_millis(
            timings.resume_ms.saturating_add(timings.readiness_ms),
        ));
        self.meter.user_init = Some(Duration::ZERO);
        self.meter.idle_pooled_ms = Some(idle_ms);
        self.meter.boot_id = environment.evidence.guest_boot_id.clone();
        let logs = LogForwarder::new(
            self.svc.repos.logs.clone(),
            self.svc.clock.clone(),
            LogContext {
                tenant_id: self.function.tenant_id.clone(),
                environment_id: environment.id.clone(),
                invocation_id: Some(self.invocation_id.clone()),
                max_line_bytes: self.svc.limits.max_log_line_bytes,
            },
        );
        // Re-point the session at this attempt. The claim only reserved the
        // environment; the acquire before dispatch moves it to the next epoch,
        // which is what fences the previous attempt out: a frame it left
        // behind no longer matches the lease and is counted as stale
        // (docs/threat-model.md T05).
        let next_epoch = environment.epoch + 1;
        session.rearm(next_epoch, logs.clone());
        // The pool has handed the environment over, so from here a panic must
        // terminate it exactly as it would a cold one.
        self.env_id = Some(environment.id.clone());
        self.epoch = next_epoch;
        // Continue the environment's own usage count instead of starting a
        // second one on the same environment.
        self.seq = sequence;
        logs.platform(
            LogPhase::Boot,
            None,
            &format!(
                "reusing pooled environment {} at epoch {} (resume {} ms, readiness check {} ms)",
                environment.id, next_epoch, timings.resume_ms, timings.readiness_ms
            ),
        );
        tracing::debug!(
            environment_id = %environment.id,
            epoch = next_epoch,
            resume_ms = timings.resume_ms,
            readiness_ms = timings.readiness_ms,
            "warm start"
        );
        Prepared {
            env: environment,
            session,
            start_kind: StartKind::Warm,
            environment_boot_ms: 0,
            runtime_init_ms: 0,
            warm: Some(timings),
            logs,
        }
    }

    /// The pooled environment's guest was gone before the `Invoke` frame could
    /// reach it. Settle this attempt, retire the environment exactly once and
    /// leave the driver as if nothing had been acquired, so the caller can
    /// dispatch again from a cold start.
    ///
    /// The attempt is classified from what the guest queued before it closed,
    /// by the rule every undelivered `Invoke` follows
    /// (docs/threat-model.md §9): `Exited` makes it `Crash` / `Runtime.Exited`,
    /// nothing makes it `Crash` / `Host.BridgeDisconnectedBeforeInvoke`. The
    /// same guest behaviour must not be classified differently just because
    /// the host was reusing the environment.
    ///
    /// The retry is the only warm-specific part, and it does not depend on the
    /// classification: the handler provably did not start, and *this* guest
    /// reported `Ready` for an earlier invocation and died afterwards, so a
    /// fresh environment is very likely to serve the request. A cold guest
    /// that never takes the frame died initializing for *this* invocation, so
    /// repeating it would only repeat the failure — which is why a cold
    /// undelivered dispatch is not retried.
    #[allow(clippy::too_many_arguments)]
    async fn retire_after_undelivered_warm(
        &mut self,
        env: &mut ExecutionEnvironment,
        session: &mut BridgeSession,
        mut attempt: InvocationAttempt,
        lease_id: LeaseId,
        logs: &LogForwarder,
        queue_wait_ms: u64,
        warm: Option<WarmStartTimings>,
    ) {
        let svc = self.svc.clone();
        let attempt_id = attempt.id.clone();
        let env_id = env.id.clone();
        // Keep reading briefly for the frames the guest queued before closing.
        let drained = session
            .wait_result(Instant::now() + UNDELIVERED_DRAIN)
            .await;
        let (error, env_end) = undelivered_invoke(&SessionError::Disconnected, Some(drained));
        let now = self.now();
        tracing::warn!(
            environment_id = %env_id,
            epoch = env.epoch,
            error_type = %error.error_type,
            "the reused environment was gone before dispatch; retrying with a cold start"
        );
        logs.platform(
            LogPhase::Handler,
            Some(&attempt_id),
            &format!(
                "the reused environment was gone before the invocation could be delivered \
                 ({}); retrying with a cold start",
                error.error_type
            ),
        );
        attempt.timings = tachyon_serverless_domain::AttemptTimings {
            queue_wait_ms: Some(queue_wait_ms),
            environment_boot_ms: Some(0),
            runtime_init_ms: Some(0),
            // The resume and the readiness check happened even though the
            // dispatch did not: this attempt cost that much before it failed.
            resume_ms: warm.map(|w| w.resume_ms),
            readiness_ms: warm.map(|w| w.readiness_ms),
            total_ms: Some(self.accepted_at.elapsed().as_millis() as u64),
            ..tachyon_serverless_domain::AttemptTimings::default()
        };
        let settled_number = attempt.number;
        let _ = attempt.fail(error, now);
        // The invocation stays `Running`: the cold retry dispatches it again.
        if let Ok(CompletionOutcome::Stale(reason)) = svc.repos.slots.complete(SlotCompletion {
            lease_id,
            attempt,
            invocation: None,
            now,
        }) {
            tracing::warn!(environment_id = %env_id, reason = %reason, "stale completion refused");
        }
        let host_sample = sample_before_terminate(svc.provider.as_ref(), &env_id).await;
        let teardown_started = Instant::now();
        let _ = session.shutdown("reused environment is gone").await;
        let terminated = svc
            .provider
            .terminate_environment(&env_id, env_end.reason)
            .await;
        if let Err(e) = &terminated {
            tracing::warn!(error = %e, environment_id = %env_id, "terminate of a gone environment failed");
        }
        let teardown = terminated.is_ok().then(|| teardown_started.elapsed());
        // The handler never started: zero, host-measured, and the attempt
        // failed (PLT-4642). The cold retry settles its own attempt.
        self.emit_attempt_settled(
            &env_id,
            &attempt_id,
            settled_number,
            UsageOutcome::Failed,
            None,
            teardown,
            0,
            None,
        )
        .await;
        // Released with the environment; the cold retry reserves anew.
        self.grant = None;
        let _ = match env_end.failure {
            None => env.mark_stopped(now),
            Some(reason) => env.mark_failed(reason, now),
        };
        self.save_env(env);
        let lifetime = environment_lifetime_ms(env, now);
        self.seq += 1;
        let mut stopped = self.usage_event(
            &env_id,
            Some(&attempt_id),
            UsageEventType::EnvironmentStopped,
            self.seq,
        );
        stopped.monotonic_duration_ms = Some(lifetime);
        stopped.segments.teardown_ms = Metered::host_opt(teardown.map(ceil_ms));
        stopped.segments.idle_pooled_ms = Metered::host(0);
        stopped.resources = usage_resources(&self.revision.spec.resources, host_sample.as_ref());
        self.record_usage(stopped).await;
        // Terminated exactly once, here: the retry starts from nothing and a
        // later panic cleanup has nothing of this environment left to reclaim.
        self.env_id = None;
        self.attempt_id = None;
        self.lease_id = None;
    }

    /// Create a fresh environment and drive it to `Ready` (the P1 path).
    ///
    /// `None` means the invocation already reached a terminal state and the
    /// environment, if one was created, is already terminated.
    async fn prepare_cold(
        &mut self,
        env_id: EnvironmentId,
        reuse_key: ReuseKey,
        secret_env: Vec<(String, String)>,
        init_timeout: Duration,
    ) -> Option<Prepared> {
        let svc = self.svc.clone();
        let tenant = self.function.tenant_id.clone();
        let mut env = ExecutionEnvironment::request(
            env_id.clone(),
            tenant.clone(),
            self.revision.id.clone(),
            svc.provider.kind(),
            reuse_key,
            self.now(),
        )
        .owned_by(svc.dispatcher.id().clone());
        if let Err(e) = svc.repos.environments.insert(env.clone()) {
            self.fail_invocation(InvocationError::new(
                ErrorClass::PlatformError,
                "Host.Storage",
                e.to_string(),
            ));
            return None;
        }
        // From here on a panic must still terminate the environment.
        self.env_id = Some(env_id.clone());
        self.epoch = env.epoch;
        // A newly booted environment starts its own usage count.
        self.seq = 0;
        self.meter.environment_changed();
        self.meter.idle_pooled_ms = Some(0);
        let _ = env.mark_provisioning(self.now());
        self.save_env(&env);

        let logs = LogForwarder::new(
            svc.repos.logs.clone(),
            svc.clock.clone(),
            LogContext {
                tenant_id: tenant.clone(),
                environment_id: env_id.clone(),
                invocation_id: (!self.warmup).then(|| self.invocation_id.clone()),
                max_line_bytes: svc.limits.max_log_line_bytes,
            },
        );

        let artifact = match &self.revision.spec.artifact {
            tachyon_serverless_domain::ArtifactRef::Binary { digest, .. } => {
                match svc.artifacts.get(digest).await {
                    Ok(stored) => ArtifactLocation {
                        path: stored.path,
                        digest: stored.digest,
                        size_bytes: stored.size_bytes,
                    },
                    Err(e) => {
                        let _ = env.mark_failed(format!("artifact unavailable: {e}"), self.now());
                        self.save_env(&env);
                        self.fail_invocation(InvocationError::new(
                            ErrorClass::InitError,
                            "Host.ArtifactUnavailable",
                            format!("artifact unavailable: {e}"),
                        ));
                        return None;
                    }
                }
            }
            tachyon_serverless_domain::ArtifactRef::OciImage { reference, .. } => {
                let _ = env.mark_failed("oci images are not executable", self.now());
                self.save_env(&env);
                self.fail_invocation(InvocationError::new(
                    ErrorClass::InitError,
                    "Host.UnsupportedArtifact",
                    format!("oci image `{reference}` is not executable by this provider"),
                ));
                return None;
            }
        };

        // Initialization waits end at the client deadline at the latest.
        let init_wait = init_timeout.min(self.client_remaining());
        let init_clamped = init_wait < init_timeout;

        // `HelloAck` carries the secrets the caller already resolved for
        // exactly this environment id.
        let hello_ack = self.hello_ack_params(&env_id, &artifact, init_wait, secret_env);

        let spec = EnvironmentSpec {
            environment_id: env_id.clone(),
            tenant_id: tenant.clone(),
            revision_id: self.revision.id.clone(),
            artifact: artifact.clone(),
            architecture: self.revision.spec.runtime.architecture,
            egress: self.revision.spec.egress,
            egress_allow: self.revision.spec.egress_allow.clone(),
            resources: self.revision.spec.resources,
            connect_timeout: init_wait,
        };
        let create_started = Instant::now();
        logs.platform(
            LogPhase::Boot,
            None,
            &format!(
                "creating environment {env_id} with provider {}",
                svc.provider.kind().as_str()
            ),
        );
        let created = tokio::select! {
            r = svc.provider.create_environment(spec) => r,
            k = wait_cancel(&mut self.cancel_rx) => {
                let _ = env.mark_stopped(self.now());
                self.save_env(&env);
                let _ = svc.provider.terminate_environment(&env_id, cancel_reason(k)).await;
                self.fail_invocation(cancel_error(k, "during environment creation"));
                return None;
            }
        };
        let handle = match created {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %env_id, "environment creation failed");
                if init_clamped && matches!(e, ProviderError::Timeout { .. }) {
                    self.stop_for_client_deadline(None, &mut env, &logs).await;
                    return None;
                }
                let _ = env.mark_failed(format!("create failed: {e}"), self.now());
                self.save_env(&env);
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::InitFailed)
                    .await;
                self.boot_failed();
                let (class, error_type) = match &e {
                    ProviderError::Boot(_)
                    | ProviderError::Timeout { .. }
                    | ProviderError::ArtifactRejected(_) => {
                        (ErrorClass::InitError, "Host.EnvironmentBootFailed")
                    }
                    _ => (ErrorClass::PlatformError, "Host.ProviderError"),
                };
                self.fail_invocation(InvocationError::new(class, error_type, e.to_string()));
                return None;
            }
        };
        let environment_boot_ms = handle
            .connected_at
            .saturating_duration_since(handle.created_at)
            .as_millis() as u64;
        let connected_at = handle.connected_at;
        self.meter.vm_base_boot = Some(
            handle
                .connected_at
                .saturating_duration_since(handle.created_at),
        );
        let _ = env.mark_initializing(handle.evidence.clone(), self.now());
        self.save_env(&env);
        self.seq += 1;
        self.emit_usage(
            &env_id,
            None,
            UsageEventType::EnvironmentStarted,
            self.seq,
            None,
            0,
            0,
        )
        .await;
        logs.platform(
            LogPhase::Boot,
            None,
            &format!(
                "bridge connected after {environment_boot_ms} ms (host_pid={:?})",
                handle.evidence.host_pid
            ),
        );

        // 7. handshake + ready ---------------------------------------------
        let handshake_timeout = svc.invoke_cfg.handshake_timeout();
        let handshake_wait = handshake_timeout.min(self.client_remaining());
        // The guest learns the epoch its first attempt runs at: the acquire
        // before dispatch moves the booted environment (epoch 0) to it.
        let handshake = BridgeSession::handshake(
            handle.stream,
            &env_id,
            env.epoch + 1,
            hello_ack,
            logs.clone(),
            handshake_wait,
        )
        .await;
        let (mut session, hello) = match handshake {
            Ok(x) => x,
            Err(e) => {
                if handshake_wait < handshake_timeout && matches!(e, SessionError::Timeout { .. }) {
                    self.stop_for_client_deadline(None, &mut env, &logs).await;
                    return None;
                }
                let _ = env.mark_failed(format!("handshake failed: {e}"), self.now());
                self.save_env(&env);
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::InitFailed)
                    .await;
                self.boot_failed();
                self.fail_invocation(InvocationError::new(
                    ErrorClass::InitError,
                    "Host.HandshakeFailed",
                    e.to_string(),
                ));
                return None;
            }
        };
        if let Some(boot_id) = &hello.guest_boot_id {
            env.record_guest_boot_id(boot_id.clone());
            self.meter.boot_id = Some(boot_id.clone());
        }
        env.evidence
            .details
            .insert("bridge_version".into(), hello.bridge_version.clone().into());
        env.evidence.details.insert(
            "guest_architecture".into(),
            hello.architecture.clone().into(),
        );
        self.save_env(&env);

        let init_deadline = create_started + init_wait;
        let ready = tokio::select! {
            r = session.wait_ready(init_deadline) => r,
            k = wait_cancel(&mut self.cancel_rx) => {
                let _ = session.shutdown("cancelled").await;
                let _ = env.mark_stopped(self.now());
                self.save_env(&env);
                let _ = svc.provider.terminate_environment(&env_id, cancel_reason(k)).await;
                self.fail_invocation(cancel_error(k, "during initialization"));
                return None;
            }
        };
        let ready = match ready {
            Ok(r) => r,
            Err(SessionError::Timeout { .. }) if init_clamped => {
                self.stop_for_client_deadline(Some(&mut session), &mut env, &logs)
                    .await;
                return None;
            }
            Err(e) => {
                let (error_type, message) = match &e {
                    SessionError::InitError {
                        error_type,
                        message,
                        ..
                    } => (error_type.clone(), message.clone()),
                    SessionError::Timeout { .. } => (
                        "Host.InitTimeout".to_string(),
                        format!(
                            "guest did not become ready within {} s",
                            init_timeout.as_secs()
                        ),
                    ),
                    SessionError::Disconnected => (
                        "Host.BridgeDisconnected".to_string(),
                        "bridge disconnected before Ready".to_string(),
                    ),
                    other => ("Host.InitProtocol".to_string(), other.to_string()),
                };
                logs.platform(LogPhase::Init, None, &format!("init failed: {message}"));
                let _ = session.shutdown("init failed").await;
                let _ = env.mark_failed(format!("init failed: {error_type}"), self.now());
                self.save_env(&env);
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::InitFailed)
                    .await;
                self.boot_failed();
                self.fail_invocation(InvocationError::new(
                    ErrorClass::InitError,
                    error_type,
                    message,
                ));
                return None;
            }
        };
        let user_init = connected_at.elapsed();
        self.meter.user_init = Some(user_init);
        self.meter.guest_init_ms = Some(ready.guest_init_ms);
        let runtime_init_ms = user_init.as_millis() as u64;
        let _ = env.mark_ready(self.now());
        // Starting -> Busy in the ledger, and a successful boot for the
        // revision's circuit breaker.
        if let Some(g) = &self.grant {
            g.ready();
            g.start_result(true);
        }
        env.evidence
            .details
            .insert("guest_init_ms".into(), ready.guest_init_ms.into());
        self.save_env(&env);

        Some(Prepared {
            env,
            session,
            start_kind: StartKind::Cold,
            environment_boot_ms,
            runtime_init_ms,
            warm: None,
            logs,
        })
    }

    /// Compose `HelloAck`: entrypoint policy + revision env vars + the secrets
    /// [`Self::resolve_secret_env`] resolved + `TACHYON_UNISOLATED=1` for
    /// dev-only providers.
    fn hello_ack_params(
        &self,
        env_id: &EnvironmentId,
        artifact: &ArtifactLocation,
        init_timeout: Duration,
        secret_env: Vec<(String, String)>,
    ) -> HelloAckParams {
        let svc = &self.svc;
        let entry = svc
            .entrypoints
            .resolve(&svc.provider.kind(), &artifact.path, env_id);
        let mut env_vars = self.revision.spec.env_vars.clone();
        env_vars.extend(secret_env);
        if svc.provider.capabilities().dev_only {
            env_vars.push((
                tachyon_serverless_protocol::env::UNISOLATED.to_string(),
                "1".to_string(),
            ));
        }
        HelloAckParams {
            entrypoint: entry.entrypoint,
            args: entry.args,
            env: env_vars,
            working_dir: entry.working_dir,
            init_timeout,
            max_response_bytes: svc.limits.max_response_bytes,
            max_log_line_bytes: svc.limits.max_log_line_bytes as u64,
        }
    }
}

fn cancel_reason(kind: CancelKind) -> TerminateReason {
    match kind {
        CancelKind::Client => TerminateReason::Cancelled,
        CancelKind::Shutdown => TerminateReason::Shutdown,
        CancelKind::Drain => TerminateReason::Timeout,
    }
}

/// Map a guest-reported error for the attempt onto the ledger class and the
/// environment end.
fn classify_guest_error(
    kind: GuestErrorKind,
    error_type: String,
    message: String,
) -> (InvocationError, EnvEnd) {
    let (class, end) = match kind {
        GuestErrorKind::Handler => (
            ErrorClass::UserError,
            EnvEnd {
                reason: TerminateReason::Completed,
                failure: None,
            },
        ),
        GuestErrorKind::Panic => (
            ErrorClass::Crash,
            EnvEnd {
                reason: TerminateReason::Completed,
                failure: None,
            },
        ),
        GuestErrorKind::Crash { .. } => (
            ErrorClass::Crash,
            EnvEnd {
                reason: TerminateReason::Crashed,
                failure: Some("user process crashed"),
            },
        ),
        GuestErrorKind::Protocol | GuestErrorKind::ResponseTooLarge { .. } => (
            ErrorClass::PlatformError,
            EnvEnd {
                reason: TerminateReason::Completed,
                failure: None,
            },
        ),
    };
    (InvocationError::new(class, error_type, message), end)
}

/// Classification when the `Invoke` frame never reached the guest. The
/// handler cannot have started, so this is never `OutcomeUnknown`
/// (docs/threat-model.md §9).
///
/// One rule for every dispatch, cold or warm: a write that failed is followed
/// by a short read of the frames the guest queued before closing, and the
/// classification comes from those. Whether the caller then retries is a
/// separate decision ([`Driver::retire_after_undelivered_warm`]) and never
/// changes what is recorded here, so the same guest behaviour cannot be
/// classified two ways.
fn undelivered_invoke(error: &SessionError, drained: Option<Outcome>) -> (InvocationError, EnvEnd) {
    match error {
        SessionError::Disconnected => match drained {
            // The guest said why it went away (typically `Exited` right after
            // Ready): classify it exactly as if the write had raced ahead.
            Some(Outcome::GuestError {
                kind,
                error_type,
                message,
                ..
            }) => classify_guest_error(kind, error_type, message),
            _ => (
                InvocationError::new(
                    ErrorClass::Crash,
                    "Host.BridgeDisconnectedBeforeInvoke",
                    "the bridge closed the connection before the invocation could be delivered",
                ),
                EnvEnd {
                    reason: TerminateReason::Crashed,
                    failure: Some("bridge disconnected before invoke"),
                },
            ),
        },
        // Nothing was written; the environment itself is healthy.
        SessionError::FrameTooLarge(size) => (
            InvocationError::new(
                ErrorClass::PlatformError,
                "Host.InvokeTooLarge",
                format!("invoke frame of {size} bytes exceeds the protocol frame limit"),
            ),
            EnvEnd {
                reason: TerminateReason::Completed,
                failure: None,
            },
        ),
        other => (
            InvocationError::new(
                ErrorClass::PlatformError,
                "Host.InvokeEncode",
                format!("invoke frame could not be encoded: {other}"),
            ),
            EnvEnd {
                reason: TerminateReason::Completed,
                failure: None,
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_serverless_domain::TenantId;

    fn error_of(c: &(InvocationError, EnvEnd)) -> &InvocationError {
        &c.0
    }

    #[test]
    fn undelivered_invoke_is_never_outcome_unknown() {
        let too_large = undelivered_invoke(&SessionError::FrameTooLarge(9 << 20), None);
        assert_eq!(error_of(&too_large).class, ErrorClass::PlatformError);
        assert_eq!(error_of(&too_large).error_type, "Host.InvokeTooLarge");
        assert_eq!(too_large.1.reason, TerminateReason::Completed);
        assert!(too_large.1.failure.is_none());

        let encode = undelivered_invoke(&SessionError::Protocol("bad".into()), None);
        assert_eq!(error_of(&encode).class, ErrorClass::PlatformError);
        assert_eq!(error_of(&encode).error_type, "Host.InvokeEncode");

        for drained in [None, Some(Outcome::Disconnected), Some(Outcome::Timeout)] {
            let gone = undelivered_invoke(&SessionError::Disconnected, drained);
            assert_eq!(error_of(&gone).class, ErrorClass::Crash);
            assert_eq!(
                error_of(&gone).error_type,
                "Host.BridgeDisconnectedBeforeInvoke"
            );
            assert_eq!(gone.1.reason, TerminateReason::Crashed);
        }

        let exited = undelivered_invoke(
            &SessionError::Disconnected,
            Some(Outcome::GuestError {
                kind: GuestErrorKind::Crash {
                    exit_code: Some(0),
                    signal: None,
                },
                error_type: "Runtime.Exited".into(),
                message: "user process exited".into(),
                stack_trace: None,
                guest_handler_ms: None,
            }),
        );
        assert_eq!(error_of(&exited).class, ErrorClass::Crash);
        assert_eq!(error_of(&exited).error_type, "Runtime.Exited");
        assert_eq!(exited.1.reason, TerminateReason::Crashed);
    }

    #[test]
    fn secret_binding_failures_do_not_reveal_other_tenants() {
        let missing = SecretResolutionFailure {
            binding_ref: "db".into(),
            error: SecretError::NotFound("db".into()),
        }
        .invocation_error();
        let foreign = SecretResolutionFailure {
            binding_ref: "db".into(),
            error: SecretError::Forbidden {
                binding: "db".into(),
                tenant: TenantId::generate(),
            },
        }
        .invocation_error();
        assert_eq!(missing, foreign);
        assert_eq!(missing.class, ErrorClass::InitError);
        assert_eq!(missing.error_type, "Host.SecretBindingUnavailable");
        assert!(!missing.message.contains("tn_"));
        assert!(!missing.message.to_lowercase().contains("forbidden"));
        assert!(!missing.message.to_lowercase().contains("not found"));

        let backend = SecretResolutionFailure {
            binding_ref: "db".into(),
            error: SecretError::Backend("vault down".into()),
        }
        .invocation_error();
        assert_eq!(backend.class, ErrorClass::PlatformError);
        assert_eq!(backend.error_type, "Host.SecretBackend");
    }

    /// Regression (review R5): the copy a warm dispatch keeps for a possible
    /// cold retry is released as soon as the write resolves as anything but a
    /// lost connection, so a payload is never held twice for the whole handler
    /// execution. `settle` is also what decides the retry, so the retained
    /// copy and the retry can never disagree.
    #[test]
    fn a_dispatched_payload_is_retained_only_while_a_cold_retry_can_need_it() {
        let payload = || serde_json::json!({ "n": 1 });

        // Cold: the frame gets the only copy and nothing is retained, whatever
        // the write did.
        for connection_lost in [false, true] {
            let mut cold = RetainedPayload::new(payload());
            assert_eq!(cold.checkout(false), payload());
            assert!(!cold.held(), "a cold dispatch keeps no second copy");
            assert!(
                !cold.settle(connection_lost),
                "a cold dispatch is never retried"
            );
            assert!(!cold.held());
        }

        // Warm, delivered: the retained copy goes the moment the write is
        // through, not when the handler finishes.
        let mut delivered = RetainedPayload::new(payload());
        assert_eq!(delivered.checkout(true), payload());
        assert!(
            delivered.held(),
            "it can still be needed while the write is in flight"
        );
        assert!(!delivered.settle(false));
        assert!(
            !delivered.held(),
            "a delivered payload is not held for the whole handler execution"
        );

        // Warm, connection lost: the retry needs it and gets the only copy.
        let mut lost = RetainedPayload::new(payload());
        let _ = lost.checkout(true);
        assert!(
            lost.settle(true),
            "a lost connection is exactly what is retried cold"
        );
        assert!(lost.held());
        assert_eq!(
            lost.checkout(false),
            payload(),
            "the retry dispatches the same payload"
        );
        assert!(!lost.held());
        assert!(
            !lost.settle(true),
            "and nothing is left to retry with afterwards"
        );
    }
}

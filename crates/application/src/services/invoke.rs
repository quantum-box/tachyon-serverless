//! Synchronous invoke pipeline (docs/architecture.md §3).
//!
//! `invoke` performs the synchronous part (authz, resolution, limits,
//! idempotency, acceptance) and then spawns a *driver* task that owns the
//! rest of the lifecycle: capacity, environment creation, handshake, ready,
//! attempt + lease, invoke, result classification, cleanup and usage
//! events. The caller only awaits a completion signal, so a client that
//! disconnects does not abort the invocation: the driver keeps tracking it
//! until its deadlines and records the outcome.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use futures::FutureExt;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use tachyon_serverless_domain::{
    AliasName, AttemptId, Clock, Deadlines, EnvironmentId, ErrorClass, EventKind, EvidenceQuality,
    ExecutionEnvironment, ExecutionLease, Function, FunctionId, FunctionRevision, IdGenerator,
    Invocation, InvocationAttempt, InvocationError, InvocationId, InvocationMode, LeaseId, Limits,
    LogPhase, PayloadRef, ReuseKey, RevisionId, Sha256Digest, StartKind, Timestamp, UsageEvent,
    UsageEventType,
};
use tachyon_serverless_protocol::GuestErrorKind;
use tachyon_serverless_provider_port::{
    ArtifactLocation, ArtifactStore, EnvironmentSpec, ExecutionProvider, Principal,
    SecretDeliveryContext, SecretProvider, TerminateReason, UsageSink,
};

use crate::authz::{ensure_tenant, require_invoke};
use crate::bridge_session::{
    BridgeSession, HelloAckParams, InvokeParams, LogContext, LogForwarder, Outcome, SessionError,
};
use crate::config::{CapacityConfig, InvokeConfig};
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::repository::{IdempotencyOutcome, Repositories};
use crate::services::history::{HistoryService, InvocationDetail};
use crate::services::revision::ensure_ready;

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
}

struct DriverResult {
    output: Option<serde_json::Value>,
}

struct InFlight {
    cancel: watch::Sender<Option<CancelKind>>,
    done: watch::Receiver<Option<Arc<DriverResult>>>,
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
    global_slots: Arc<Semaphore>,
    revision_slots: Mutex<HashMap<RevisionId, Arc<Semaphore>>>,
    queued: Arc<AtomicUsize>,
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
}

impl InvokeService {
    pub fn new(deps: InvokeServiceDeps) -> Arc<Self> {
        Arc::new(Self {
            global_slots: Arc::new(Semaphore::new(deps.capacity.max_concurrency)),
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
            revision_slots: Mutex::new(HashMap::new()),
            queued: Arc::new(AtomicUsize::new(0)),
            in_flight: Mutex::new(HashMap::new()),
            draining: AtomicBool::new(false),
        })
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.lock().len()
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
        let function = self.owned_function(&req.principal, &req.function_id)?;
        if function.is_deleted() {
            return Err(AppError::FunctionDeleted(format!(
                "function {} is deleted",
                function.id
            )));
        }
        let (revision, alias) = self.resolve_revision(&req, &function)?;

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

        let invocation_id = InvocationId::from_ulid(self.ids.next_ulid());
        if let Some(key) = &req.idempotency_key {
            match self.repos.idempotency.reserve(
                &req.principal.tenant_id,
                &function.id,
                key,
                &input_digest,
                &invocation_id,
            )? {
                IdempotencyOutcome::Reserved => {}
                IdempotencyOutcome::Existing {
                    invocation_id: existing,
                    input_digest: existing_digest,
                } => {
                    if existing_digest != input_digest {
                        return Err(AppError::Conflict(format!(
                            "idempotency key `{key}` was used with a different input"
                        )));
                    }
                    return self.replay(&existing).await;
                }
            }
        }

        // 5 (first half). Capacity is checked before anything is recorded so
        // that a full wait queue answers 429 without a ledger entry.
        let pre = self.preacquire(&revision)?;

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
        let deadlines = Deadlines {
            queue_deadline: now
                + chrono::Duration::milliseconds(
                    (self.capacity.queue_timeout_seconds * 1000) as i64,
                ),
            init_deadline: None,
            execution_deadline: None,
            client_deadline: now + chrono::Duration::milliseconds(client_ms as i64),
        };
        let trace_id = req
            .trace_id
            .clone()
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| invocation_id.to_string());
        let invocation = Invocation::accept(
            invocation_id.clone(),
            req.principal.tenant_id.clone(),
            function.id.clone(),
            alias,
            revision.id.clone(),
            InvocationMode::Sync,
            req.event_kind,
            deadlines,
            req.idempotency_key.clone(),
            input_digest,
            input_size,
            trace_id.clone(),
            now,
        )?;
        let mut invocation = invocation;
        if pre.queued() {
            let _ = invocation.mark_queued();
        }
        self.repos.invocations.insert(invocation)?;

        let (cancel_tx, cancel_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(None);
        self.in_flight.lock().insert(
            invocation_id.clone(),
            InFlight {
                cancel: cancel_tx,
                done: done_rx.clone(),
            },
        );
        let driver = Driver {
            svc: Arc::clone(self),
            invocation_id: invocation_id.clone(),
            function,
            revision,
            event_kind: req.event_kind,
            payload: req.payload,
            input_size,
            trace_id,
            client_deadline: deadlines.client_deadline,
            cancel_rx,
            accepted_at: Instant::now(),
            pre: Some(pre),
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

    async fn replay(&self, existing: &InvocationId) -> Result<InvokeOutcome, AppError> {
        let done = self.in_flight.lock().get(existing).map(|e| e.done.clone());
        let result = match done {
            Some(rx) => Self::await_done(rx).await,
            None => None,
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

    /// Resolve the revision at acceptance; it is pinned from here on.
    fn resolve_revision(
        &self,
        req: &InvokeRequest,
        function: &Function,
    ) -> Result<(FunctionRevision, Option<AliasName>), AppError> {
        let lookup = |id: &RevisionId| -> Result<FunctionRevision, AppError> {
            self.repos
                .revisions
                .get(id)?
                .filter(|r| r.function_id == function.id && r.tenant_id == function.tenant_id)
                .ok_or_else(|| AppError::not_found("revision not found"))
        };
        if let Some(pinned) = &req.revision_id {
            let rev = lookup(pinned)?;
            ensure_ready(&rev)?;
            return Ok((rev, None));
        }
        let name = req.alias.clone().unwrap_or_else(AliasName::default_alias);
        let alias = self
            .repos
            .aliases
            .get(&function.id, &name)?
            .ok_or_else(|| AppError::not_found(format!("alias `{name}` not found")))?;
        let rev = lookup(&alias.revision_id)?;
        ensure_ready(&rev)?;
        Ok((rev, Some(name)))
    }

    /// Try to take both capacity permits without waiting. When at least one
    /// is unavailable, reserve a slot in the bounded wait queue (or fail
    /// with `CapacityExceeded` when the queue is full).
    fn preacquire(&self, revision: &FunctionRevision) -> Result<Preacquired, AppError> {
        let rev_sem = self.revision_semaphore(revision);
        let global = self.global_slots.clone();
        let rev = rev_sem.clone().try_acquire_owned().ok();
        let global_permit = if rev.is_some() {
            global.clone().try_acquire_owned().ok()
        } else {
            None
        };
        let slot = if rev.is_none() || global_permit.is_none() {
            Some(
                QueueSlot::take(self.queued.clone(), self.capacity.max_queue).ok_or_else(|| {
                    AppError::CapacityExceeded(format!(
                        "no capacity available and the wait queue ({} slots) is full",
                        self.capacity.max_queue
                    ))
                })?,
            )
        } else {
            None
        };
        Ok(Preacquired {
            rev_sem,
            global_sem: global,
            rev,
            global: global_permit,
            slot,
        })
    }

    fn revision_semaphore(&self, revision: &FunctionRevision) -> Arc<Semaphore> {
        self.revision_slots
            .lock()
            .entry(revision.id.clone())
            .or_insert_with(|| {
                Arc::new(Semaphore::new(
                    revision.spec.execution.max_concurrency.max(1) as usize,
                ))
            })
            .clone()
    }
}

// ---------------------------------------------------------------------------
// driver
// ---------------------------------------------------------------------------

enum QueueError {
    QueueTimeout,
    Cancelled(CancelKind),
    Closed,
}

/// RAII slot in the bounded wait queue.
struct QueueSlot(Arc<AtomicUsize>);

impl QueueSlot {
    fn take(counter: Arc<AtomicUsize>, max: usize) -> Option<Self> {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |q| {
                (q < max).then_some(q + 1)
            })
            .ok()
            .map(|_| Self(counter))
    }
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Capacity state handed from the synchronous part of `invoke` to the driver.
struct Preacquired {
    rev_sem: Arc<Semaphore>,
    global_sem: Arc<Semaphore>,
    rev: Option<OwnedSemaphorePermit>,
    global: Option<OwnedSemaphorePermit>,
    /// Held while waiting for a permit; bounds the queue length.
    slot: Option<QueueSlot>,
}

impl Preacquired {
    fn queued(&self) -> bool {
        self.slot.is_some()
    }
}

/// Outcome classification: the ledger result (output ref + http status on
/// success, the error otherwise) and how the environment ends.
type Classified = (
    Result<(Option<PayloadRef>, Option<u16>), InvocationError>,
    EnvEnd,
);

/// How the environment ended, for the ledger and the provider.
struct EnvEnd {
    reason: TerminateReason,
    /// `None` -> Stopped, `Some(reason)` -> Failed{reason}.
    failure: Option<&'static str>,
}

struct Driver {
    svc: Arc<InvokeService>,
    invocation_id: InvocationId,
    function: Function,
    revision: FunctionRevision,
    event_kind: EventKind,
    payload: serde_json::Value,
    input_size: u64,
    trace_id: String,
    client_deadline: Timestamp,
    cancel_rx: watch::Receiver<Option<CancelKind>>,
    accepted_at: Instant,
    pre: Option<Preacquired>,
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
        let output = match std::panic::AssertUnwindSafe(self.execute())
            .catch_unwind()
            .await
        {
            Ok(output) => output,
            Err(_) => {
                tracing::error!(invocation_id = %id, "invoke driver panicked");
                self.fail_invocation(InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.DriverPanic",
                    "internal error while driving the invocation",
                ));
                None
            }
        };
        self.svc.in_flight.lock().remove(&id);
        done.send_replace(Some(Arc::new(DriverResult { output })));
    }

    fn now(&self) -> Timestamp {
        self.svc.clock.now()
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
        let event = UsageEvent {
            event_id: format!("{env}:{sequence}"),
            tenant_id: self.function.tenant_id.clone(),
            environment_id: env.clone(),
            invocation_id: Some(self.invocation_id.clone()),
            attempt_id: attempt.cloned(),
            event_type,
            sequence,
            observed_at: self.now(),
            monotonic_duration_ms: duration_ms,
            memory_mib: self.revision.spec.resources.memory_mib,
            cpu_millis: self.revision.spec.resources.cpu_millis,
            bytes_in,
            bytes_out,
            meter_version: 1,
            evidence_quality: EvidenceQuality::HostObserved,
        };
        self.svc.usage.record(event).await;
    }

    async fn acquire_one(
        &mut self,
        sem: Arc<Semaphore>,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, QueueError> {
        if let Ok(p) = sem.clone().try_acquire_owned() {
            return Ok(p);
        }
        tokio::select! {
            r = tokio::time::timeout_at(deadline.into(), sem.acquire_owned()) => match r {
                Ok(Ok(p)) => Ok(p),
                Ok(Err(_)) => Err(QueueError::Closed),
                Err(_) => Err(QueueError::QueueTimeout),
            },
            k = wait_cancel(&mut self.cancel_rx) => Err(QueueError::Cancelled(k)),
        }
    }

    /// Complete the capacity acquisition started in `invoke`. The queue slot
    /// is released as soon as both permits are held.
    async fn acquire_capacity(
        &mut self,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), QueueError> {
        let deadline = self.accepted_at + self.svc.capacity.queue_timeout();
        let pre = self.pre.take().expect("capacity state is taken once");
        let Preacquired {
            rev_sem,
            global_sem,
            rev,
            global,
            slot,
        } = pre;
        let rev_permit = match rev {
            Some(p) => p,
            None => self.acquire_one(rev_sem, deadline).await?,
        };
        let global_permit = match global {
            Some(p) => p,
            None => self.acquire_one(global_sem, deadline).await?,
        };
        drop(slot);
        Ok((rev_permit, global_permit))
    }

    /// The whole lifecycle after acceptance. Returns the handler output on
    /// success. Every early return has already recorded the terminal state.
    async fn execute(&mut self) -> Option<serde_json::Value> {
        let svc = self.svc.clone();

        // 5. capacity ------------------------------------------------------
        let _permits = match self.acquire_capacity().await {
            Ok(p) => p,
            Err(QueueError::QueueTimeout) => {
                self.fail_invocation(InvocationError::new(
                    ErrorClass::QueueTimeout,
                    "Host.QueueTimeout",
                    "no capacity became available before the queue deadline",
                ));
                return None;
            }
            Err(QueueError::Cancelled(kind)) => {
                self.fail_invocation(InvocationError::new(
                    ErrorClass::Cancelled,
                    "Host.Cancelled",
                    match kind {
                        CancelKind::Client => "cancelled by request while queued",
                        CancelKind::Shutdown => "cancelled by gateway shutdown while queued",
                    },
                ));
                return None;
            }
            Err(QueueError::Closed) => {
                self.fail_invocation(InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.CapacityClosed",
                    "capacity semaphore closed",
                ));
                return None;
            }
        };
        let queue_wait_ms = self.accepted_at.elapsed().as_millis() as u64;
        if self.now() >= self.client_deadline {
            self.fail_invocation(InvocationError::new(
                ErrorClass::Timeout,
                "Host.ClientDeadline",
                "client deadline elapsed before the invocation could start",
            ));
            return None;
        }

        // 6. environment ---------------------------------------------------
        let tenant = self.function.tenant_id.clone();
        let env_id = EnvironmentId::from_ulid(svc.ids.next_ulid());
        let reuse_key = ReuseKey {
            tenant_id: tenant.clone(),
            revision_id: self.revision.id.clone(),
            execution_role_version: 1,
            configuration_version: 1,
            resource_profile_digest: Sha256Digest::of_bytes(
                &serde_json::to_vec(&self.revision.spec.resources).unwrap_or_default(),
            )
            .hex()
            .to_string(),
            runtime_profile: self.revision.spec.runtime.protocol.clone(),
            network_policy_version: 1,
            secret_binding_generation: 1,
        };
        let mut env = ExecutionEnvironment::request(
            env_id.clone(),
            tenant.clone(),
            self.revision.id.clone(),
            svc.provider.kind(),
            reuse_key,
            self.now(),
        );
        if let Err(e) = svc.repos.environments.insert(env.clone()) {
            self.fail_invocation(InvocationError::new(
                ErrorClass::PlatformError,
                "Host.Storage",
                e.to_string(),
            ));
            return None;
        }
        let _ = env.mark_provisioning(self.now());
        self.save_env(&env);

        let logs = LogForwarder::new(
            svc.repos.logs.clone(),
            svc.clock.clone(),
            LogContext {
                tenant_id: tenant.clone(),
                environment_id: env_id.clone(),
                invocation_id: Some(self.invocation_id.clone()),
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

        let init_timeout = Duration::from_secs(u64::from(
            self.revision.spec.execution.initialization_timeout_seconds,
        ));
        let spec = EnvironmentSpec {
            environment_id: env_id.clone(),
            tenant_id: tenant.clone(),
            revision_id: self.revision.id.clone(),
            artifact: artifact.clone(),
            architecture: self.revision.spec.runtime.architecture,
            egress: self.revision.spec.egress,
            resources: self.revision.spec.resources,
            connect_timeout: init_timeout,
        };
        let create_started = Instant::now();
        let init_deadline_ts =
            self.now() + chrono::Duration::milliseconds(init_timeout.as_millis() as i64);
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
                self.fail_invocation(InvocationError::new(ErrorClass::Cancelled, "Host.Cancelled", "cancelled during environment creation"));
                return None;
            }
        };
        let handle = match created {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, environment_id = %env_id, "environment creation failed");
                let _ = env.mark_failed(format!("create failed: {e}"), self.now());
                self.save_env(&env);
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::InitFailed)
                    .await;
                let (class, error_type) = match &e {
                    tachyon_serverless_provider_port::ProviderError::Boot(_)
                    | tachyon_serverless_provider_port::ProviderError::Timeout { .. }
                    | tachyon_serverless_provider_port::ProviderError::ArtifactRejected(_) => {
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
        let _ = env.mark_initializing(handle.evidence.clone(), self.now());
        self.save_env(&env);
        let mut seq = 0u64;
        seq += 1;
        self.emit_usage(
            &env_id,
            None,
            UsageEventType::EnvironmentStarted,
            seq,
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
        let hello_ack = match self
            .hello_ack_params(&env_id, &artifact, init_timeout)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                let _ = env.mark_failed(format!("secret resolution failed: {e}"), self.now());
                self.save_env(&env);
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::InitFailed)
                    .await;
                self.fail_invocation(InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.SecretResolution",
                    e.to_string(),
                ));
                return None;
            }
        };
        let handshake = BridgeSession::handshake(
            handle.stream,
            &env_id,
            env.epoch,
            hello_ack,
            logs.clone(),
            svc.invoke_cfg.handshake_timeout(),
        )
        .await;
        let (mut session, hello) = match handshake {
            Ok(x) => x,
            Err(e) => {
                let _ = env.mark_failed(format!("handshake failed: {e}"), self.now());
                self.save_env(&env);
                let _ = svc
                    .provider
                    .terminate_environment(&env_id, TerminateReason::InitFailed)
                    .await;
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
        }
        env.evidence
            .details
            .insert("bridge_version".into(), hello.bridge_version.clone().into());
        env.evidence.details.insert(
            "guest_architecture".into(),
            hello.architecture.clone().into(),
        );
        self.save_env(&env);

        let init_deadline = create_started + init_timeout;
        let ready = tokio::select! {
            r = session.wait_ready(init_deadline) => r,
            k = wait_cancel(&mut self.cancel_rx) => {
                let _ = session.shutdown("cancelled").await;
                let _ = env.mark_stopped(self.now());
                self.save_env(&env);
                let _ = svc.provider.terminate_environment(&env_id, cancel_reason(k)).await;
                self.fail_invocation(InvocationError::new(ErrorClass::Cancelled, "Host.Cancelled", "cancelled during initialization"));
                return None;
            }
        };
        let ready = match ready {
            Ok(r) => r,
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
                self.fail_invocation(InvocationError::new(
                    ErrorClass::InitError,
                    error_type,
                    message,
                ));
                return None;
            }
        };
        let runtime_init_ms = connected_at.elapsed().as_millis() as u64;
        let _ = env.mark_ready(self.now());
        env.evidence
            .details
            .insert("guest_init_ms".into(), ready.guest_init_ms.into());
        self.save_env(&env);

        // 8. attempt + lease + invoke --------------------------------------
        let timeout = Duration::from_secs(u64::from(self.revision.spec.execution.timeout_seconds));
        let now = self.now();
        let attempt_id = AttemptId::from_ulid(svc.ids.next_ulid());
        let execution_deadline_ts =
            now + chrono::Duration::milliseconds(timeout.as_millis() as i64);
        let mut attempt = InvocationAttempt::dispatch(
            attempt_id.clone(),
            self.invocation_id.clone(),
            tenant.clone(),
            1,
            env_id.clone(),
            env.epoch,
            StartKind::Cold,
            now,
        );
        let mut lease = ExecutionLease::acquire(
            LeaseId::from_ulid(svc.ids.next_ulid()),
            env_id.clone(),
            attempt_id.clone(),
            tenant.clone(),
            env.epoch,
            execution_deadline_ts,
            now,
        );
        let mut inv = self.load_invocation()?;
        if let Err(e) = inv.mark_running(
            attempt_id.clone(),
            execution_deadline_ts,
            init_deadline_ts,
            now,
        ) {
            tracing::warn!(error = %e, "cannot mark invocation running");
        }
        let _ = env.mark_busy(now);
        let _ = svc.repos.invocations.insert_attempt(attempt.clone());
        let _ = svc.repos.environments.insert_lease(lease.clone());
        self.save_invocation(inv);
        self.save_env(&env);
        seq += 1;
        self.emit_usage(
            &env_id,
            Some(&attempt_id),
            UsageEventType::HandlerStarted,
            seq,
            None,
            self.input_size,
            0,
        )
        .await;

        let deadline_ms = execution_deadline_ts.timestamp_millis().max(0) as u64;
        let dispatched_at = Instant::now();
        let sent = session
            .send_invoke(InvokeParams {
                invocation_id: self.invocation_id.clone(),
                attempt_id: attempt_id.clone(),
                epoch: env.epoch,
                event_type: self.event_kind.event_type().to_string(),
                deadline_ms,
                trace_id: self.trace_id.clone(),
                payload: std::mem::take(&mut self.payload),
            })
            .await;
        let outcome = match sent {
            Ok(()) => tokio::select! {
                o = session.wait_result(dispatched_at + timeout) => Ok(o),
                k = wait_cancel(&mut self.cancel_rx) => Err(k),
            },
            Err(_) => Ok(Outcome::Disconnected),
        };
        let handler_ms = dispatched_at.elapsed().as_millis() as u64;
        let finish_started = Instant::now();

        // 9. classify ------------------------------------------------------
        let max_response = svc.limits.max_response_bytes;
        let inline_max = svc.invoke_cfg.inline_output_max_bytes;
        let mut output_value = None;
        let mut bytes_out = 0u64;
        let (result, env_end): Classified = match outcome {
            Ok(Outcome::Response {
                payload,
                guest_handler_ms,
            }) => {
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
            Ok(Outcome::GuestError {
                kind,
                error_type,
                message,
                stack_trace,
                guest_handler_ms,
            }) => {
                if let Some(ms) = guest_handler_ms {
                    env.evidence
                        .details
                        .insert("guest_handler_ms".into(), ms.into());
                }
                if let Some(trace) = stack_trace {
                    logs.platform(LogPhase::Handler, Some(&attempt_id), &trace);
                }
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
                (Err(InvocationError::new(class, error_type, message)), end)
            }
            Ok(Outcome::Timeout) => {
                logs.platform(
                    LogPhase::Handler,
                    Some(&attempt_id),
                    &format!(
                        "execution deadline ({} s) elapsed; cancelling",
                        timeout.as_secs()
                    ),
                );
                let grace = svc.invoke_cfg.cancel_grace();
                let _ = session.cancel(&attempt_id, grace).await;
                session
                    .drain_until_closed(Instant::now() + grace + Duration::from_millis(200))
                    .await;
                (
                    Err(InvocationError::new(
                        ErrorClass::Timeout,
                        "Host.Timeout",
                        format!("handler did not finish within {} s", timeout.as_secs()),
                    )),
                    EnvEnd {
                        reason: TerminateReason::Timeout,
                        failure: Some("execution timeout"),
                    },
                )
            }
            Ok(Outcome::Disconnected) => (
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
            Err(kind) => {
                let grace = svc.invoke_cfg.cancel_grace();
                let _ = session.cancel(&attempt_id, grace).await;
                session
                    .drain_until_closed(Instant::now() + grace + Duration::from_millis(200))
                    .await;
                (
                    Err(InvocationError::new(
                        ErrorClass::Cancelled,
                        "Host.Cancelled",
                        match kind {
                            CancelKind::Client => "cancelled by request",
                            CancelKind::Shutdown => "cancelled by gateway shutdown",
                        },
                    )),
                    EnvEnd {
                        reason: cancel_reason(kind),
                        failure: None,
                    },
                )
            }
        };

        // 10. record, release, terminate ----------------------------------
        let now = self.now();
        let _ = lease.release(now);
        let _ = svc.repos.environments.update_lease(lease);
        let response_ms = finish_started.elapsed().as_millis() as u64;
        attempt.timings = tachyon_serverless_domain::AttemptTimings {
            queue_wait_ms: Some(queue_wait_ms),
            environment_boot_ms: Some(environment_boot_ms),
            runtime_init_ms: Some(runtime_init_ms),
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
        let _ = svc.repos.invocations.update_attempt(attempt.clone());
        if let Some(mut inv) = self.load_invocation() {
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
            tracing::info!(
                invocation_id = %inv.id,
                status = inv.status.name(),
                handler_ms,
                environment_boot_ms,
                runtime_init_ms,
                "invocation finished"
            );
            self.save_invocation(inv);
        }
        seq += 1;
        self.emit_usage(
            &env_id,
            Some(&attempt_id),
            UsageEventType::HandlerFinished,
            seq,
            Some(handler_ms),
            self.input_size,
            bytes_out,
        )
        .await;

        let _ = session
            .shutdown(match env_end.reason {
                TerminateReason::Completed => "completed",
                TerminateReason::Timeout => "timeout",
                TerminateReason::Cancelled => "cancelled",
                TerminateReason::Crashed => "crashed",
                TerminateReason::Shutdown => "shutdown",
                TerminateReason::InitFailed => "init failed",
                TerminateReason::Reconcile => "reconcile",
            })
            .await;
        match svc
            .provider
            .terminate_environment(&env_id, env_end.reason)
            .await
        {
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
        seq += 1;
        self.emit_usage(
            &env_id,
            Some(&attempt_id),
            UsageEventType::EnvironmentStopped,
            seq,
            Some(create_started.elapsed().as_millis() as u64),
            0,
            0,
        )
        .await;
        drop(_permits);
        output_value
    }

    /// Compose `HelloAck`: entrypoint policy + revision env vars + resolved
    /// secrets + `TACHYON_UNISOLATED=1` for dev-only providers.
    async fn hello_ack_params(
        &self,
        env_id: &EnvironmentId,
        artifact: &ArtifactLocation,
        init_timeout: Duration,
    ) -> Result<HelloAckParams, AppError> {
        let svc = &self.svc;
        let entry = svc
            .entrypoints
            .resolve(&svc.provider.kind(), &artifact.path, env_id);
        let mut env_vars = self.revision.spec.env_vars.clone();
        let ctx = SecretDeliveryContext {
            tenant_id: self.function.tenant_id.clone(),
            revision_id: self.revision.id.clone(),
            environment_id: env_id.clone(),
            epoch: 1,
        };
        for binding in &self.revision.spec.secrets {
            let value = svc.secrets.resolve(&ctx, &binding.binding_ref).await?;
            env_vars.push((binding.env_name.clone(), value.expose().to_string()));
        }
        if svc.provider.capabilities().dev_only {
            env_vars.push((
                tachyon_serverless_protocol::env::UNISOLATED.to_string(),
                "1".to_string(),
            ));
        }
        Ok(HelloAckParams {
            entrypoint: entry.entrypoint,
            args: entry.args,
            env: env_vars,
            working_dir: entry.working_dir,
            init_timeout,
            max_response_bytes: svc.limits.max_response_bytes,
            max_log_line_bytes: svc.limits.max_log_line_bytes as u64,
        })
    }
}

fn cancel_reason(kind: CancelKind) -> TerminateReason {
    match kind {
        CancelKind::Client => TerminateReason::Cancelled,
        CancelKind::Shutdown => TerminateReason::Shutdown,
    }
}

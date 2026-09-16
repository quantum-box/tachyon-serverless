//! Synchronous invoke pipeline (docs/architecture.md §3).
//!
//! `invoke` performs the synchronous part (authz, resolution, limits,
//! validation, idempotency, acceptance) and then spawns a *driver* task that
//! owns the rest of the lifecycle: capacity, environment creation, handshake,
//! ready, attempt + lease, invoke, result classification, cleanup and usage
//! events. The caller only awaits a completion signal, so a client that
//! disconnects does not abort the invocation: the driver keeps tracking it
//! until its deadlines and records the outcome.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
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
    ArtifactLocation, ArtifactStore, EnvironmentSpec, ExecutionProvider, Principal, ProviderError,
    SecretDeliveryContext, SecretError, SecretProvider, TerminateReason, UsageSink,
};

use crate::authz::{ensure_tenant, require_invoke};
use crate::bridge_session::{
    BridgeSession, HelloAckParams, InvokeParams, LogContext, LogForwarder, Outcome, SessionError,
};
use crate::config::{CapacityConfig, InvokeConfig};
use crate::entrypoint::EntrypointPolicy;
use crate::error::AppError;
use crate::repository::{IdempotencyBinding, IdempotencyOutcome, Repositories};
use crate::services::history::{HistoryService, InvocationDetail};
use crate::services::pool::{
    EnvironmentPool, WarmEnvironment, reuse_key_for, secret_binding_generation,
};
use crate::services::revision::ensure_ready;

/// Upper bound of a caller-supplied trace id. Together with the fixed-size
/// ids it keeps the `Invoke` envelope within
/// [`crate::config::FRAME_ENVELOPE_RESERVE_BYTES`].
pub const MAX_TRACE_ID_BYTES: usize = 256;

/// How long to keep reading after a failed `Invoke` write, for frames the
/// guest queued before it closed the connection.
const UNDELIVERED_DRAIN: Duration = Duration::from_millis(200);

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
    /// Warm environment pool. Hands out nothing unless both the provider's
    /// idle capabilities and `[pool] enabled` allow reuse, so with the shipped
    /// providers every invocation stays cold and destroy-after-invoke.
    pool: Arc<EnvironmentPool>,
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
    pub pool: Arc<EnvironmentPool>,
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
            pool: deps.pool,
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

        // 3. Idempotency: a key bound to an existing invocation replays it,
        // even when capacity is exhausted.
        if let Some(binding) = self.bound_invocation(&req)? {
            return self.replay_binding(&req, &input_digest, binding).await;
        }

        // 5 (first half). Capacity is checked before anything is recorded so
        // that a full wait queue answers 429 without a ledger entry and
        // without binding the idempotency key.
        let pre = match self.preacquire(&revision) {
            Ok(pre) => pre,
            Err(e) => {
                // A concurrent request with the same key may have been
                // accepted in the meantime; its record is the better answer.
                if let Some(binding) = self.bound_invocation(&req)? {
                    return self.replay_binding(&req, &input_digest, binding).await;
                }
                return Err(e);
            }
        };
        if pre.queued() {
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
            payload: req.payload,
            input_size,
            trace_id,
            client_deadline,
            cancel_rx,
            accepted_at: Instant::now(),
            pre: Some(pre),
            env_id: None,
            env_started: None,
            attempt_id: None,
            lease_id: None,
            seq: 0,
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
            return Err(AppError::Conflict(format!(
                "idempotency key `{}` was used with a different input",
                req.idempotency_key.as_deref().unwrap_or_default()
            )));
        }
        self.replay(&binding.invocation_id).await
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
    logs: LogForwarder,
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
    // What the driver has recorded so far, so that a panic can still clean
    // up (see `cleanup_after_panic`). Cleared once the normal path finished.
    env_id: Option<EnvironmentId>,
    env_started: Option<Instant>,
    attempt_id: Option<AttemptId>,
    lease_id: Option<LeaseId>,
    /// Last usage-event sequence number used for the environment.
    seq: u64,
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
            && let Ok(Some(mut lease)) = svc.repos.environments.get_lease(&lease_id)
        {
            let _ = lease.release(now);
            let _ = svc.repos.environments.update_lease(lease);
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
        if let Ok(Some(mut env)) = svc.repos.environments.get(&env_id)
            && !env.is_terminal()
        {
            let _ = env.mark_failed("driver panicked", now);
            self.save_env(&env);
        }
        self.seq += 1;
        let duration = self.env_started.map(|t| t.elapsed().as_millis() as u64);
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
    /// is released as soon as both permits are held. The wait ends at the
    /// queue deadline, which never exceeds the client deadline.
    async fn acquire_capacity(
        &mut self,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), QueueError> {
        let deadline = (self.accepted_at + self.svc.capacity.queue_timeout())
            .min(Instant::now() + self.client_remaining());
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
        if self.client_deadline_elapsed() {
            self.fail_invocation(InvocationError::new(
                ErrorClass::Timeout,
                "Host.ClientDeadline",
                "client deadline elapsed before the invocation could start",
            ));
            return None;
        }

        // 6. environment ---------------------------------------------------
        // Reuse is decided before anything is created. The reuse key carries
        // the generation of the *resolved* secret bindings, so the bindings are
        // resolved first; a binding this tenant cannot use fails the invocation
        // here, without a ledger row and without booting anything.
        let tenant = self.function.tenant_id.clone();
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
                return None;
            }
        };
        let reuse_key = reuse_key_for(
            &tenant,
            &self.revision,
            secret_binding_generation(self.revision.spec.secrets.iter().zip(&secret_env).map(
                |(binding, (_, value))| {
                    (
                        binding.env_name.as_str(),
                        binding.binding_ref.as_str(),
                        value.as_str(),
                    )
                },
            )),
        );
        // A pooled environment is taken only when its reuse key matches in
        // every field. Everything else boots cold — which, with both shipped
        // providers, is every invocation: the pool never hands anything out
        // unless the provider reports both idle capabilities as `Supported`.
        let warm = svc.pool.claim(&reuse_key).await;
        let prepared = match warm {
            Some(warm) => self.prepare_warm(warm),
            None => {
                self.prepare_cold(prospective_env_id, reuse_key, secret_env, init_timeout)
                    .await?
            }
        };
        let Prepared {
            mut env,
            mut session,
            start_kind,
            environment_boot_ms,
            runtime_init_ms,
            logs,
        } = prepared;
        let env_id = env.id.clone();

        // Never start the handler after the client deadline. Checked before
        // any attempt, lease or Running state is recorded.
        if self.client_deadline_elapsed() {
            self.stop_for_client_deadline(Some(&mut session), &mut env, &logs)
                .await;
            return None;
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
            self.env_id = None;
            return None;
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
        let mut attempt = InvocationAttempt::dispatch(
            attempt_id.clone(),
            self.invocation_id.clone(),
            tenant.clone(),
            1,
            env_id.clone(),
            env.epoch,
            start_kind,
            now,
        );
        let lease_id = LeaseId::from_ulid(svc.ids.next_ulid());
        let mut lease = ExecutionLease::acquire(
            lease_id.clone(),
            env_id.clone(),
            attempt_id.clone(),
            tenant.clone(),
            env.epoch,
            execution_deadline_ts,
            now,
        );
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
        self.attempt_id = Some(attempt_id.clone());
        self.lease_id = Some(lease_id);
        self.save_invocation(inv);
        self.save_env(&env);

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
        let dispatch = match sent {
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
        let handler_ms = dispatched_at.elapsed().as_millis() as u64;
        let finish_started = Instant::now();

        // 9. classify ------------------------------------------------------
        let max_response = svc.limits.max_response_bytes;
        let inline_max = svc.invoke_cfg.inline_output_max_bytes;
        let mut output_value = None;
        let mut bytes_out = 0u64;
        let (result, env_end): Classified = match dispatch {
            Dispatch::Finished(Outcome::Response {
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
            Dispatch::Finished(Outcome::GuestError {
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
                classify_guest_error(kind, error_type, message)
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
            Dispatch::NotDelivered { error, drained } => {
                logs.platform(
                    LogPhase::Handler,
                    Some(&attempt_id),
                    &format!("invocation was not delivered to the guest: {error}"),
                );
                undelivered_invoke(&error, drained)
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
        // it healthy: a clean end with nothing to clean up, reuse allowed by
        // both gates, and no shutdown in progress (a pooled environment must
        // never outlive the process holding its session). Everything else
        // keeps destroy-after-invoke (docs/architecture.md §4).
        let may_reuse = env_end.failure.is_none()
            && env_end.reason == TerminateReason::Completed
            && !svc.draining.load(Ordering::SeqCst);
        let release = if may_reuse {
            svc.pool.release(&env, session)
        } else {
            Err(Box::new(session))
        };
        let mut session = match release {
            Ok(pooled) => {
                logs.platform(
                    LogPhase::Shutdown,
                    None,
                    &format!(
                        "environment {env_id} returned to the pool (idle at epoch {})",
                        pooled.epoch
                    ),
                );
                // The pool owns the environment and its session now: it is not
                // terminated, it did not stop (so no `EnvironmentStopped`), and
                // a later panic cleanup must not reclaim it.
                self.env_id = None;
                self.attempt_id = None;
                self.lease_id = None;
                drop(_permits);
                return output_value;
            }
            Err(session) => *session,
        };
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
        self.seq += 1;
        self.emit_usage(
            &env_id,
            Some(&attempt_id),
            UsageEventType::EnvironmentStopped,
            self.seq,
            self.env_started.map(|t| t.elapsed().as_millis() as u64),
            0,
            0,
        )
        .await;
        // Everything is recorded and terminated: nothing left for a panic
        // cleanup to do.
        self.env_id = None;
        self.attempt_id = None;
        self.lease_id = None;
        drop(_permits);
        output_value
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
        } = warm;
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
        // Re-point the session at this attempt. The new epoch is what fences
        // the previous attempt out: a frame it left behind no longer matches
        // the lease and is counted as stale (docs/threat-model.md T05).
        session.rearm(environment.epoch, logs.clone());
        // The pool has handed the environment over, so from here a panic must
        // terminate it exactly as it would a cold one.
        self.env_id = Some(environment.id.clone());
        self.env_started = Some(Instant::now());
        logs.platform(
            LogPhase::Boot,
            None,
            &format!(
                "reusing pooled environment {} at epoch {}",
                environment.id, environment.epoch
            ),
        );
        tracing::debug!(
            environment_id = %environment.id,
            epoch = environment.epoch,
            "warm start"
        );
        Prepared {
            env: environment,
            session,
            start_kind: StartKind::Warm,
            environment_boot_ms: 0,
            runtime_init_ms: 0,
            logs,
        }
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
        );
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
            resources: self.revision.spec.resources,
            connect_timeout: init_wait,
        };
        let create_started = Instant::now();
        self.env_started = Some(create_started);
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
        let handshake = BridgeSession::handshake(
            handle.stream,
            &env_id,
            env.epoch,
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

        let init_deadline = create_started + init_wait;
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

        Some(Prepared {
            env,
            session,
            start_kind: StartKind::Cold,
            environment_boot_ms,
            runtime_init_ms,
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
    }
}

/// Map a guest-reported error for the attempt onto the ledger class and the
/// environment end.
fn classify_guest_error(kind: GuestErrorKind, error_type: String, message: String) -> Classified {
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

/// Classification when the `Invoke` frame never reached the guest. The
/// handler cannot have started, so this is never `OutcomeUnknown`
/// (docs/threat-model.md §9).
fn undelivered_invoke(error: &SessionError, drained: Option<Outcome>) -> Classified {
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
                Err(InvocationError::new(
                    ErrorClass::Crash,
                    "Host.BridgeDisconnectedBeforeInvoke",
                    "the bridge closed the connection before the invocation could be delivered",
                )),
                EnvEnd {
                    reason: TerminateReason::Crashed,
                    failure: Some("bridge disconnected before invoke"),
                },
            ),
        },
        // Nothing was written; the environment itself is healthy.
        SessionError::FrameTooLarge(size) => (
            Err(InvocationError::new(
                ErrorClass::PlatformError,
                "Host.InvokeTooLarge",
                format!("invoke frame of {size} bytes exceeds the protocol frame limit"),
            )),
            EnvEnd {
                reason: TerminateReason::Completed,
                failure: None,
            },
        ),
        other => (
            Err(InvocationError::new(
                ErrorClass::PlatformError,
                "Host.InvokeEncode",
                format!("invoke frame could not be encoded: {other}"),
            )),
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

    fn error_of(c: &Classified) -> &InvocationError {
        c.0.as_ref().expect_err("classified as a failure")
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
}

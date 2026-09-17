//! Admission control and autoscaling (PLT-4634,
//! docs/adr/0006-autoscaling-and-admission.md).
//!
//! Replaces the gateway-wide semaphore of P1. Every invocation asks the
//! [`AdmissionController`] for a *grant* before it may boot or take an
//! environment:
//!
//! - a **capacity ledger** reserves CPU, memory and ephemeral storage per
//!   environment (revision resources plus the node's per-environment VMM and
//!   bridge overhead) from the moment a start is granted (`Starting`) through
//!   `Busy`, the pool (`Parking`, `Idle`, `Draining`) until the environment is
//!   gone. A reservation is a value ([`Grant`]) with exactly one owner — the
//!   driver, then the pool, then a driver again — and it is released when that
//!   owner drops it, so nothing is counted twice or leaked;
//! - a **fair bounded queue** (per-tenant sub-queues, least attained service
//!   with round-robin tie-break, weighted) with limits on count, payload bytes
//!   and each item's queue deadline;
//! - an **autoscaler gate** per revision (`desired` from arrival rate, handler
//!   duration, in-flight and backlog) that coalesces activations, a
//!   **start-rate token bucket**, and a **start-failure circuit breaker**;
//! - explicit **reject reasons**: `capacity`, `quota`, `queue_full`,
//!   `queue_deadline`, `circuit_open`, `placement` (jp-only is never relaxed).
//!
//! The state machine is [`state::AdmissionState`] (synchronous, clock passed
//! in); this module only adds the lock, the delivery of grants to waiting
//! drivers and a ticker for the start-rate refill.

pub mod config;
pub mod resources;
pub mod scaler;
pub mod state;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use tachyon_serverless_api_types::CapacityInfo;
use tachyon_serverless_domain::{Clock, FunctionRevision, RevisionId, TenantId, Timestamp};

pub use config::{
    AutoscalerConfig, CircuitBreakerConfig, NodeConfig, StartRateConfig, TenantQuotaConfig,
    TenantQuotaEntry,
};
pub use resources::{NodeCapacity, Resources};
pub use state::{
    AdmissionCounters, AdmissionMetrics, AdmissionSettings, AdmissionState, BlockReason,
    DrainReason, GrantKind, KeepReason, PrestartSkip, RejectReason, Rejection, ReservationId,
    RevisionMetrics, ScaleDown, TenantMetrics, Ticket, WaiterId,
};

use crate::config::CapacityConfig;

/// How often queued waiters are re-evaluated while the queue is not empty
/// (start-rate refill, breaker cooldown, queue deadlines).
const TICK: Duration = Duration::from_millis(25);

/// Error type prefixes recorded on invocations that admission gave up on.
pub const QUEUE_TIMEOUT: &str = "Host.QueueTimeout";
pub const CAPACITY_WAIT_TIMEOUT: &str = "Host.CapacityWaitTimeout";
pub const QUOTA_WAIT_TIMEOUT: &str = "Host.QuotaWaitTimeout";
pub const START_CIRCUIT_OPEN: &str = "Host.StartCircuitOpen";
/// A queued invocation whose function was deleted before it started
/// (PLT-4635). Also the `error_type` of the 409 for a new one.
pub const FUNCTION_DELETED: &str = "Host.FunctionDeleted";

/// The `error.error_type` of an invocation that admission ended with `reason`.
pub fn error_type_for(reason: RejectReason) -> &'static str {
    match reason {
        RejectReason::Capacity => CAPACITY_WAIT_TIMEOUT,
        RejectReason::Quota => QUOTA_WAIT_TIMEOUT,
        RejectReason::CircuitOpen => START_CIRCUIT_OPEN,
        RejectReason::FunctionDeleted => FUNCTION_DELETED,
        RejectReason::QueueFull | RejectReason::QueueDeadline | RejectReason::Placement => {
            QUEUE_TIMEOUT
        }
    }
}

/// The reject reason behind an invocation `error_type`, for error bodies.
pub fn reason_for_error_type(error_type: &str) -> Option<RejectReason> {
    match error_type {
        CAPACITY_WAIT_TIMEOUT => Some(RejectReason::Capacity),
        QUOTA_WAIT_TIMEOUT => Some(RejectReason::Quota),
        QUEUE_TIMEOUT => Some(RejectReason::QueueDeadline),
        START_CIRCUIT_OPEN => Some(RejectReason::CircuitOpen),
        FUNCTION_DELETED => Some(RejectReason::FunctionDeleted),
        _ => None,
    }
}

impl AdmissionSettings {
    pub fn from_config(c: &CapacityConfig) -> Self {
        Self {
            node: c.node.clone(),
            max_concurrency: c.max_concurrency,
            max_queue: c.max_queue,
            max_queue_bytes: c.max_queue_bytes,
            queue_timeout_seconds: c.queue_timeout_seconds,
            tenant_defaults: c.tenant_defaults.clone(),
            tenants: c.tenants.clone(),
            start_rate_per_second: c.start_rate.per_second,
            start_burst: c.start_rate.burst,
            breaker_threshold: c.circuit_breaker.failure_threshold,
            breaker_cooldown_seconds: c.circuit_breaker.cooldown_seconds,
            rate_window_seconds: c.autoscaler.rate_window_seconds,
        }
    }
}

/// Delivered to a waiting driver.
enum Delivered {
    Granted(Grant),
    Rejected(Rejection),
}

struct Inner {
    state: AdmissionState,
    senders: HashMap<WaiterId, oneshot::Sender<Delivered>>,
}

type Evictor = Arc<dyn Fn(usize) + Send + Sync>;

/// Scale defaults a ticket resolves a revision's own settings against
/// (PLT-4635), and what `GET /v1/capacity` reports about scaling.
#[derive(Debug, Clone)]
pub struct ScaleDefaults {
    pub idle_ttl_seconds: u64,
    pub scale_down_cooldown_seconds: u64,
    pub info: tachyon_serverless_api_types::ScalingInfo,
}

impl Default for ScaleDefaults {
    fn default() -> Self {
        Self {
            idle_ttl_seconds: 60,
            scale_down_cooldown_seconds: 30,
            info: tachyon_serverless_api_types::ScalingInfo::default(),
        }
    }
}

pub struct AdmissionController {
    inner: Mutex<Inner>,
    clock: Arc<dyn Clock>,
    ticking: AtomicBool,
    evictor: Mutex<Option<Evictor>>,
    scale: Mutex<ScaleDefaults>,
    /// Event metrics of everything that holds this controller: the invoke
    /// driver, the pool and the gateway (PLT-4637).
    metrics: Arc<crate::metrics::Metrics>,
}

impl std::fmt::Debug for AdmissionController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionController")
            .field("state", &self.inner.lock().state)
            .finish_non_exhaustive()
    }
}

impl AdmissionController {
    pub fn new(settings: AdmissionSettings, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                state: AdmissionState::new(settings),
                senders: HashMap::new(),
            }),
            clock,
            ticking: AtomicBool::new(false),
            evictor: Mutex::new(None),
            scale: Mutex::new(ScaleDefaults::default()),
            metrics: Arc::new(crate::metrics::Metrics::default()),
        })
    }

    /// The event metrics registry (PLT-4637).
    pub fn metrics(&self) -> &Arc<crate::metrics::Metrics> {
        &self.metrics
    }

    /// Admission gauges and counters of every tenant and revision, for
    /// `GET /metrics` only (PLT-4637).
    pub fn metrics_view(self: &Arc<Self>) -> AdmissionMetrics {
        self.with_state(|inner, now| inner.state.metrics(now))
    }

    /// Set the scale defaults and the scaling report (PLT-4635).
    pub fn set_scale_defaults(&self, defaults: ScaleDefaults) {
        *self.scale.lock() = defaults;
    }

    /// Called with the number of idle pooled environments to evict when a
    /// start is blocked on node resources while the pool holds some.
    pub fn set_evictor(&self, f: impl Fn(usize) + Send + Sync + 'static) {
        *self.evictor.lock() = Some(Arc::new(f));
    }

    /// Build the admission ticket of one invocation of `revision`.
    pub fn ticket(
        &self,
        tenant: &TenantId,
        revision: &FunctionRevision,
        payload_bytes: u64,
        deadline: Timestamp,
    ) -> Ticket {
        let overhead = self.inner.lock().state.overhead();
        let exec = &revision.spec.execution;
        let defaults = self.scale.lock().clone();
        Ticket {
            tenant: tenant.clone(),
            function: revision.function_id.clone(),
            revision: revision.id.clone(),
            idle_ttl_seconds: exec
                .idle_ttl_seconds
                .map_or(defaults.idle_ttl_seconds, u64::from),
            scale_down_cooldown_seconds: exec
                .scale_down_cooldown_seconds
                .map_or(defaults.scale_down_cooldown_seconds, u64::from),
            resources: Resources::for_environment(&revision.spec.resources, overhead),
            max_environments: exec.max_concurrency.max(1),
            concurrency_per_environment: exec.concurrency_per_environment.max(1),
            min_ready: exec.min_ready,
            payload_bytes,
            deadline,
            required_region: revision.spec.placement.region.clone(),
            cold_only: false,
        }
    }

    /// Run `f` under the lock, then deliver what it decided with the lock
    /// released (a delivery that finds its receiver gone drops the grant,
    /// which takes the lock again).
    fn with_state<R>(self: &Arc<Self>, f: impl FnOnce(&mut Inner, Timestamp) -> R) -> R {
        let now = self.clock.now();
        let (result, deliveries, evictions, queued) = {
            let mut inner = self.inner.lock();
            let result = f(&mut inner, now);
            let mut deliveries = Vec::new();
            loop {
                let outbox = inner.state.take_outbox();
                if outbox.is_empty() {
                    break;
                }
                for (id, outcome) in outbox {
                    match (inner.senders.remove(&id), outcome) {
                        (Some(tx), outcome) => deliveries.push((tx, outcome)),
                        // Nobody is waiting for it any more: undo the grant.
                        (None, state::Outcome::Granted { reservation, .. }) => {
                            inner.state.release(reservation, now)
                        }
                        (None, state::Outcome::Rejected(_)) => {}
                    }
                }
            }
            (
                result,
                deliveries,
                inner.state.take_evictions(),
                inner.state.queue_len(),
            )
        };
        for (tx, outcome) in deliveries {
            let message = match outcome {
                state::Outcome::Granted { reservation, kind } => Delivered::Granted(Grant {
                    ctrl: self.clone(),
                    id: reservation,
                    kind,
                }),
                state::Outcome::Rejected(r) => Delivered::Rejected(r),
            };
            // A receiver that is gone drops the grant here, which releases it.
            let _ = tx.send(message);
        }
        if evictions > 0 {
            let evictor = self.evictor.lock().clone();
            if let Some(evict) = evictor {
                evict(evictions);
            }
        }
        if queued > 0 {
            self.ensure_ticker();
        }
        result
    }

    fn ensure_ticker(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self.ticking.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            loop {
                tokio::time::sleep(TICK).await;
                let Some(ctrl) = weak.upgrade() else {
                    return;
                };
                let queued = ctrl.with_state(|inner, now| {
                    inner.state.pump(now);
                    inner.state.queue_len()
                });
                if queued == 0 {
                    ctrl.ticking.store(false, Ordering::SeqCst);
                    // A waiter that arrived between the check and the store
                    // restarts the ticker itself.
                    if ctrl.inner.lock().state.queue_len() > 0 {
                        ctrl.ensure_ticker();
                    }
                    return;
                }
            }
        });
    }

    fn enqueue(self: &Arc<Self>, ticket: Ticket, retry: bool) -> Result<Pending, Rejection> {
        let (tx, rx) = oneshot::channel();
        let id = self.with_state(|inner, now| {
            // The sender is registered before the scheduler runs, so a grant
            // made inside `enqueue` finds its receiver.
            let Inner { state, senders } = inner;
            let reserved_id = state.peek_next_waiter_id();
            senders.insert(reserved_id, tx);
            match state.enqueue(ticket, retry, now) {
                Ok(id) => {
                    debug_assert_eq!(id, reserved_id);
                    Ok(id)
                }
                Err(r) => {
                    senders.remove(&reserved_id);
                    Err(r)
                }
            }
        })?;
        Ok(Pending {
            ctrl: self.clone(),
            id,
            rx: Some(rx),
            ready: None,
        })
    }

    /// The immediate refusals of [`Self::admit`] (placement, a deleted
    /// function, an open breaker, a reservation that can never fit, a zero
    /// quota) without queueing anything or granting anything. Asked before
    /// the budget reservation (PLT-4643), so a request admission would refuse
    /// anyway never holds budget, and a budget refusal never holds capacity.
    pub fn precheck(self: &Arc<Self>, ticket: &Ticket) -> Result<(), Rejection> {
        self.with_state(|inner, now| inner.state.precheck(ticket, now))
    }

    /// Admit a new invocation. `Err` is an immediate refusal (nothing was
    /// queued); `Ok` may already be granted ([`Pending::is_waiting`]).
    pub fn admit(self: &Arc<Self>, ticket: Ticket) -> Result<Pending, Rejection> {
        self.enqueue(ticket, false)
    }

    /// Queue again, at the front of the tenant's queue, for a cold start only:
    /// the invocation had won its turn but the environment it was given is
    /// gone.
    pub fn requeue_cold(self: &Arc<Self>, mut ticket: Ticket) -> Result<Pending, Rejection> {
        ticket.cold_only = true;
        self.enqueue(ticket, true)
    }

    /// The driver took a pooled environment: its own grant (if any) is
    /// released and the pooled environment's reservation becomes its grant.
    pub fn adopt(self: &Arc<Self>, holder: Option<Grant>, idle: Grant) -> Grant {
        let holder_id = holder.as_ref().map(|g| g.id);
        let id = self.with_state(|inner, now| inner.state.adopt(holder_id, idle.id, now));
        // `holder` is gone from the ledger now; its drop is a no-op release.
        drop(holder);
        let mut idle = idle;
        idle.kind = GrantKind::Warm;
        debug_assert_eq!(id, idle.id);
        idle
    }

    /// Turn a promise whose pooled environment vanished into a cold start
    /// right away, if possible. `None`: the promise was released; requeue.
    pub fn redeem(self: &Arc<Self>, promise: Grant) -> Option<Grant> {
        let id = promise.id;
        let converted = self.with_state(|inner, now| inner.state.redeem(id, now));
        if converted {
            let mut g = promise;
            g.kind = GrantKind::Cold;
            Some(g)
        } else {
            None
        }
    }

    pub fn observe_duration(self: &Arc<Self>, revision: &RevisionId, duration: Duration) {
        self.with_state(|inner, _| {
            inner
                .state
                .observe_duration(revision, duration.as_secs_f64())
        });
    }

    /// `GET /v1/capacity` for one tenant.
    pub fn snapshot(self: &Arc<Self>, tenant: &TenantId) -> CapacityInfo {
        let mut info = self.with_state(|inner, now| inner.state.snapshot(tenant, now));
        info.scaling = self.scale.lock().info.clone();
        info
    }

    // -- scale to zero, min_ready, drains (PLT-4635) -------------------------

    /// The revisions an alias routes, from a valid configuration only.
    pub fn set_routed(self: &Arc<Self>, routed: std::collections::HashSet<RevisionId>) {
        self.with_state(|inner, _| inner.state.set_routed(routed));
    }

    pub fn is_routed(&self, revision: &RevisionId) -> bool {
        self.inner.lock().state.is_routed(revision)
    }

    /// Start draining a revision; a deletion refuses its waiters at once.
    pub fn begin_drain(self: &Arc<Self>, revision: &RevisionId, reason: DrainReason) -> bool {
        self.with_state(|inner, now| inner.state.begin_drain(revision, reason, now))
    }

    /// A superseded revision is routed again.
    pub fn end_drain(self: &Arc<Self>, revision: &RevisionId) -> bool {
        self.with_state(|inner, _| inner.state.end_drain(revision))
    }

    pub fn drain_reason(&self, revision: &RevisionId) -> Option<DrainReason> {
        self.inner.lock().state.drain_reason(revision)
    }

    pub fn forget_drain(self: &Arc<Self>, revision: &RevisionId) {
        self.with_state(|inner, _| inner.state.forget_drain(revision));
    }

    /// Nothing of `revision` is reserved or waiting.
    pub fn revision_is_empty(&self, revision: &RevisionId) -> bool {
        self.inner.lock().state.revision_is_empty(revision)
    }

    /// Environments of `revision` starting, busy, parking or idle.
    pub fn provisioned(&self, revision: &RevisionId) -> u32 {
        self.inner.lock().state.provisioned(revision)
    }

    /// Reserve one `min_ready` pre-start (never queued, never ahead of a
    /// waiter). The grant is a cold one, owned by the caller.
    pub fn try_prestart(self: &Arc<Self>, ticket: Ticket) -> Result<Grant, PrestartSkip> {
        let id = self.with_state(|inner, now| inner.state.try_prestart(ticket, now))?;
        Ok(Grant {
            ctrl: self.clone(),
            id,
            kind: GrantKind::Cold,
        })
    }

    fn withdraw(self: &Arc<Self>, id: WaiterId) -> Option<BlockReason> {
        self.with_state(|inner, _| {
            let blocked = inner.state.withdraw(id);
            if blocked.is_some() {
                inner.senders.remove(&id);
            }
            blocked
        })
    }
}

/// Why a queued invocation did not get a grant.
#[derive(Debug)]
pub enum WaitError<K> {
    /// The wait ended at its deadline; the reason says what blocked it.
    Timeout(RejectReason),
    /// Admission refused it while it waited (breaker opened, deadline).
    Rejected(Rejection),
    Cancelled(K),
    Closed,
}

/// A queued (or already granted) admission.
pub struct Pending {
    ctrl: Arc<AdmissionController>,
    id: WaiterId,
    rx: Option<oneshot::Receiver<Delivered>>,
    ready: Option<Delivered>,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending").field("id", &self.id).finish()
    }
}

impl Pending {
    /// True while no grant or refusal has arrived.
    pub fn is_waiting(&mut self) -> bool {
        if self.ready.is_none()
            && let Some(rx) = self.rx.as_mut()
            && let Ok(d) = rx.try_recv()
        {
            self.ready = Some(d);
        }
        self.ready.is_none()
    }

    /// Wait for the grant until `deadline` or until `cancel` resolves.
    pub async fn wait<K>(
        mut self,
        deadline: tokio::time::Instant,
        cancel: impl Future<Output = K>,
    ) -> Result<Grant, WaitError<K>> {
        let into = |d: Delivered| match d {
            Delivered::Granted(g) => Ok(g),
            Delivered::Rejected(r) => Err(WaitError::Rejected(r)),
        };
        if let Some(d) = self.ready.take() {
            return into(d);
        }
        let Some(mut rx) = self.rx.take() else {
            return Err(WaitError::Closed);
        };
        let ctrl = self.ctrl.clone();
        let id = self.id;
        tokio::select! {
            r = tokio::time::timeout_at(deadline, &mut rx) => match r {
                Ok(Ok(d)) => into(d),
                Ok(Err(_)) => Err(WaitError::Closed),
                Err(_) => match ctrl.withdraw(id) {
                    Some(blocked) => Err(WaitError::Timeout(blocked.on_deadline())),
                    // Decided in the same instant: take what was decided.
                    None => match rx.await {
                        Ok(d) => into(d),
                        Err(_) => Err(WaitError::Closed),
                    },
                },
            },
            k = cancel => {
                // Withdraw first; a grant that raced in is released when `rx`
                // drops at the end of this function.
                let _ = ctrl.withdraw(id);
                Err(WaitError::Cancelled(k))
            }
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        // Idempotent: a waiter that was already granted or refused is not
        // queued any more, and a grant still sitting in the channel is
        // released when the channel drops right after this.
        let _ = self.ctrl.withdraw(self.id);
    }
}

/// One environment's reservation (or a promise of a pooled one). Released
/// when dropped.
pub struct Grant {
    ctrl: Arc<AdmissionController>,
    id: ReservationId,
    kind: GrantKind,
}

impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .finish()
    }
}

impl Grant {
    pub fn kind(&self) -> GrantKind {
        self.kind
    }

    pub fn controller(&self) -> &Arc<AdmissionController> {
        &self.ctrl
    }

    /// The booted environment reported `Ready`.
    pub fn ready(&self) {
        let id = self.id;
        self.ctrl.with_state(|inner, _| inner.state.ready(id));
    }

    /// Record the boot result for the revision's circuit breaker.
    pub fn start_result(&self, ok: bool) {
        let id = self.id;
        self.ctrl
            .with_state(|inner, now| inner.state.start_result(id, ok, now));
    }

    /// Handed to the pool, being quiesced.
    pub fn park(&self) {
        let id = self.id;
        self.ctrl.with_state(|inner, now| inner.state.park(id, now));
    }

    /// Published as idle in the pool.
    pub fn parked(&self) {
        let id = self.id;
        self.ctrl
            .with_state(|inner, now| inner.state.parked(id, now));
    }

    /// May the idle environment behind this reservation be terminated now
    /// (PLT-4635)? On `Ok` the reservation is already `Draining`.
    pub fn try_scale_down(&self, how: ScaleDown) -> Result<(), KeepReason> {
        let id = self.id;
        self.ctrl
            .with_state(|inner, now| inner.state.try_scale_down(id, how, now))
    }

    /// Being terminated by the pool.
    pub fn drain(&self) {
        let id = self.id;
        self.ctrl
            .with_state(|inner, now| inner.state.drain(id, now));
    }
}

impl Drop for Grant {
    fn drop(&mut self) {
        let id = self.id;
        self.ctrl
            .with_state(|inner, now| inner.state.release(id, now));
    }
}

#[cfg(test)]
mod tests;

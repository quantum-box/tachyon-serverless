//! The admission state machine: capacity ledger, fair bounded queue,
//! autoscaler gate, start-rate limiter and circuit breakers, behind one lock.
//!
//! Nothing here is async and nothing reads a clock: every operation takes
//! `now`, and outcomes for waiters are collected in an outbox that the async
//! wrapper ([`super::AdmissionController`]) delivers after the lock is
//! released. The fake-clock scale tests at the bottom drive this type
//! directly.

use std::collections::{BTreeMap, HashMap, VecDeque};

use tachyon_serverless_api_types as api;
use tachyon_serverless_domain::{FunctionId, RevisionId, TenantId, Timestamp};

use super::config::{NodeConfig, TenantQuotaConfig, TenantQuotaEntry};
use super::resources::{NodeCapacity, Resources};
use super::scaler::{
    BreakerGate, BreakerState, CircuitBreaker, DemandInput, DemandStats, TokenBucket,
    desired_environments,
};

pub type WaiterId = u64;
pub type ReservationId = u64;

/// Why admission refused or gave up on an invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RejectReason {
    /// The node cannot fit it (it never could, or it stayed full until the
    /// queue deadline).
    Capacity,
    /// A tenant or revision (pool) quota refused it or kept it waiting until
    /// the queue deadline.
    Quota,
    /// The bounded queue (count or bytes) is full.
    QueueFull,
    /// Its queue deadline passed while it waited for its turn or a start token.
    QueueDeadline,
    /// The revision's start-failure circuit breaker is open.
    CircuitOpen,
    /// The tenant or revision requires a region this node is not in.
    Placement,
    /// The revision's function is being deleted (PLT-4635): a waiter that was
    /// accepted before the deletion is refused instead of started.
    FunctionDeleted,
}

impl RejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::Quota => "quota",
            Self::QueueFull => "queue_full",
            Self::QueueDeadline => "queue_deadline",
            Self::CircuitOpen => "circuit_open",
            Self::Placement => "placement",
            Self::FunctionDeleted => "function_deleted",
        }
    }
}

/// Why a revision is being drained (PLT-4635).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DrainReason {
    /// Every alias that routed the revision moved to another one.
    AliasSwitch,
    /// The revision's function is being deleted: new work is refused.
    FunctionDeleted,
}

impl DrainReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AliasSwitch => "alias_switch",
            Self::FunctionDeleted => "function_deleted",
        }
    }
}

/// How the pool wants to end one idle environment (PLT-4635).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDown {
    /// The sweeper: idle since `idle_since`. Subject to the revision's idle
    /// TTL, its cooldown and `min_ready`.
    Idle { idle_since: Timestamp },
    /// A drain (the revision is superseded or deleted, or the environment's
    /// reuse key was superseded): no TTL, cooldown or `min_ready`.
    Drain { reason: &'static str },
}

/// Why an idle environment was kept (PLT-4635).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepReason {
    /// The reservation is not idle any more (claimed, or already draining).
    NotIdle,
    /// Invocations of the revision are waiting in the queue.
    Queued,
    /// Every idle environment of the revision is promised to a waiter.
    Promised,
    /// Idle for less than the revision's idle TTL.
    IdleTtl,
    /// Within the cooldown after a scale-up or an activation.
    Cooldown,
    /// Needed for `min_ready`.
    MinReady,
}

impl KeepReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotIdle => "not_idle",
            Self::Queued => "queued",
            Self::Promised => "promised",
            Self::IdleTtl => "idle_ttl",
            Self::Cooldown => "cooldown",
            Self::MinReady => "min_ready",
        }
    }
}

/// Why a `min_ready` pre-start did not happen (PLT-4635).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrestartSkip {
    /// Enough environments are provisioned already.
    Satisfied,
    /// The revision is not routed, or it is being drained.
    NotRouted,
    /// Invocations are waiting: they get the capacity first.
    WaitersFirst,
    /// A cap (node, tenant, revision, start rate, breaker) says no now.
    Blocked(&'static str),
    /// Refused outright (placement, never fits, open breaker).
    Refused(Rejection),
}

/// One scale decision, for `GET /v1/capacity`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaleEvent {
    pub kind: &'static str,
    pub reason: &'static str,
    pub at: Timestamp,
}

/// What is remembered about a revision's scaling after its ledger entry is
/// gone (it scaled to zero).
#[derive(Debug, Clone)]
struct ScaleRecord {
    tenant: TenantId,
    event: ScaleEvent,
    min_ready: u32,
    max_environments: u32,
    idle_ttl_seconds: u64,
    cooldown_seconds: u64,
}

/// Scale records kept for revisions without reservations.
const MAX_SCALE_RECORDS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub reason: RejectReason,
    pub message: String,
}

impl Rejection {
    fn new(reason: RejectReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }
}

/// What kept a waiter in the queue the last time it was considered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    /// Not considered yet.
    Pending,
    /// Node `max_concurrency` or node resources.
    Capacity,
    /// Tenant `max_concurrency` or the revision's `max_concurrency`.
    Quota,
    /// Start-rate token bucket empty.
    StartRate,
    /// The revision already has as many environments ready or starting as
    /// the autoscaler wants (activation coalescing).
    Scaling,
    /// Half-open breaker with its probe still running.
    Probe,
}

impl BlockReason {
    /// The reason reported when the waiter's deadline passes.
    pub fn on_deadline(&self) -> RejectReason {
        match self {
            Self::Capacity => RejectReason::Capacity,
            Self::Quota => RejectReason::Quota,
            _ => RejectReason::QueueDeadline,
        }
    }
}

/// What an invocation asks admission for.
#[derive(Debug, Clone)]
pub struct Ticket {
    pub tenant: TenantId,
    pub function: FunctionId,
    pub revision: RevisionId,
    /// The revision's idle TTL, resolved against the gateway default.
    pub idle_ttl_seconds: u64,
    /// The revision's scale-down cooldown, resolved against the default.
    pub scale_down_cooldown_seconds: u64,
    /// Revision resources plus the node's per-environment overhead.
    pub resources: Resources,
    /// The revision's `max_concurrency`: its pool quota.
    pub max_environments: u32,
    pub concurrency_per_environment: u32,
    pub min_ready: u32,
    pub payload_bytes: u64,
    /// The invocation's queue deadline.
    pub deadline: Timestamp,
    /// The revision's own placement requirement.
    pub required_region: Option<String>,
    /// Only a cold start will do (a retry after the environment it was given
    /// turned out to be gone).
    pub cold_only: bool,
}

/// A granted admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantKind {
    /// A new environment may boot: resources are reserved (`Starting`).
    Cold,
    /// A pooled environment of the revision is idle: take it. Counts as in
    /// flight, reserves nothing new (the idle environment already does).
    Warm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Granted {
        reservation: ReservationId,
        kind: GrantKind,
    },
    Rejected(Rejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResState {
    Promised,
    Starting,
    Busy,
    Parking,
    Idle,
    Draining,
}

impl ResState {
    fn index(self) -> usize {
        self as usize
    }

    fn in_flight(self) -> bool {
        matches!(self, Self::Promised | Self::Starting | Self::Busy)
    }

    /// Whether the reservation holds node resources. A promise does not: the
    /// idle environment it points at already does.
    fn holds_resources(self) -> bool {
        !matches!(self, Self::Promised)
    }
}

#[derive(Debug, Clone)]
struct Reservation {
    tenant: TenantId,
    revision: RevisionId,
    resources: Resources,
    state: ResState,
    /// Half-open probe start whose result is still outstanding.
    probe: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct Counts([u32; 6]);

impl Counts {
    fn get(&self, s: ResState) -> u32 {
        self.0[s.index()]
    }
    fn inc(&mut self, s: ResState) {
        self.0[s.index()] += 1;
    }
    fn dec(&mut self, s: ResState) {
        debug_assert!(self.0[s.index()] > 0, "{s:?} underflow");
        self.0[s.index()] = self.0[s.index()].saturating_sub(1);
    }
    fn in_flight(&self) -> u32 {
        self.get(ResState::Promised) + self.get(ResState::Starting) + self.get(ResState::Busy)
    }
    fn is_empty(&self) -> bool {
        self.0.iter().all(|n| *n == 0)
    }
    /// Environments that exist or are on their way: what `min_ready` and
    /// the autoscaler's `desired` are compared with.
    fn provisioned(&self) -> u32 {
        self.get(ResState::Starting)
            + self.get(ResState::Busy)
            + self.get(ResState::Idle)
            + self.get(ResState::Parking)
    }
    fn api(&self) -> api::EnvironmentCounts {
        api::EnvironmentCounts {
            starting: self.get(ResState::Starting).into(),
            busy: self.get(ResState::Busy).into(),
            promised: self.get(ResState::Promised).into(),
            parking: self.get(ResState::Parking).into(),
            idle: self.get(ResState::Idle).into(),
            draining: self.get(ResState::Draining).into(),
        }
    }
}

#[derive(Debug)]
struct RevisionEntry {
    tenant: TenantId,
    max_environments: u32,
    concurrency_per_environment: u32,
    min_ready: u32,
    idle_ttl_seconds: u64,
    cooldown_seconds: u64,
    /// Last cold start granted (activation, scale-up or pre-start): the
    /// cooldown runs from here.
    last_scale_up: Option<Timestamp>,
    counts: Counts,
    queued: u32,
    stats: DemandStats,
    breaker: CircuitBreaker,
}

#[derive(Debug, Clone)]
struct TenantQuota {
    max_concurrency: Option<usize>,
    max_queue: Option<usize>,
    weight: u32,
    required_region: Option<String>,
}

#[derive(Debug)]
struct TenantEntry {
    quota: TenantQuota,
    in_flight: usize,
    queue: VecDeque<WaiterId>,
    queued_bytes: u64,
    /// Serve sequence of the last grant: the round-robin tie-break.
    last_served: u64,
    /// Grants since start (PLT-4637: starvation is "waiting while others
    /// are granted").
    grants: u64,
}

#[derive(Debug)]
struct Waiter {
    ticket: Ticket,
    enqueued_at: Timestamp,
    blocked: BlockReason,
    /// Held back by the autoscaler gate at least once: it waited for an
    /// environment that was ready or starting instead of booting its own
    /// (activation coalescing, PLT-4637 metrics).
    coalesced: bool,
}

/// Monotonic admission counters since the process started (PLT-4637).
/// Kept outside the per-revision entries, which are forgotten at zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdmissionCounters {
    /// New arrivals (retries are not counted).
    pub arrivals: u64,
    /// Grants that reserved a new environment (`Starting`).
    pub grants_cold: u64,
    /// Grants of a pooled environment (a promise).
    pub grants_warm: u64,
    /// Grants to waiters the autoscaler gate held back at least once.
    pub coalesced_waits: u64,
    /// Of those, the ones served by an existing environment: a boot the
    /// coalescing avoided.
    pub starts_avoided: u64,
    /// Cold start results reported to the breakers.
    pub start_successes: u64,
    pub start_failures: u64,
    /// Transitions of a revision's breaker into `open`.
    pub breaker_opens: u64,
    /// Scale decisions by (kind, reason).
    pub scale_events: BTreeMap<(&'static str, &'static str), u64>,
}

/// One revision in [`AdmissionMetrics`].
#[derive(Debug, Clone, PartialEq)]
pub struct RevisionMetrics {
    pub tenant: TenantId,
    pub revision: RevisionId,
    pub environments: api::EnvironmentCounts,
    pub desired: u32,
    pub max_environments: u32,
    pub min_ready: u32,
    pub queued: u32,
    /// `closed` | `half_open` | `open`.
    pub breaker: &'static str,
}

/// One tenant in [`AdmissionMetrics`].
#[derive(Debug, Clone, PartialEq)]
pub struct TenantMetrics {
    pub tenant: TenantId,
    pub in_flight: u64,
    pub queued: u64,
    pub queued_bytes: u64,
    /// Age of the tenant's oldest waiter.
    pub oldest_wait_seconds: Option<f64>,
    /// Grants since start.
    pub grants: u64,
    pub max_concurrency: Option<u64>,
}

/// Everything admission knows, for `GET /metrics` (PLT-4637). Unlike
/// [`AdmissionState::snapshot`] it covers every tenant, so it is served only
/// to an operator credential.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionMetrics {
    pub node_name: String,
    pub capacity: NodeCapacity,
    pub overhead: Resources,
    pub reserved: Resources,
    pub max_concurrency: u64,
    pub in_flight: u64,
    pub environments: api::EnvironmentCounts,
    pub queue_length: u64,
    pub queue_bytes: u64,
    pub max_queue: u64,
    pub max_queue_bytes: u64,
    pub oldest_wait_seconds: Option<f64>,
    pub start_tokens: u32,
    pub rejections: BTreeMap<RejectReason, u64>,
    pub counters: AdmissionCounters,
    pub revisions: Vec<RevisionMetrics>,
    pub tenants: Vec<TenantMetrics>,
}

impl RejectReason {
    /// Every reason, so a metric can start each series at zero.
    pub const ALL: [RejectReason; 7] = [
        RejectReason::Capacity,
        RejectReason::Quota,
        RejectReason::QueueFull,
        RejectReason::QueueDeadline,
        RejectReason::CircuitOpen,
        RejectReason::Placement,
        RejectReason::FunctionDeleted,
    ];
}

/// Static settings of [`AdmissionState`].
#[derive(Debug, Clone)]
pub struct AdmissionSettings {
    pub node: NodeConfig,
    pub max_concurrency: usize,
    pub max_queue: usize,
    pub max_queue_bytes: u64,
    pub queue_timeout_seconds: u64,
    pub tenant_defaults: TenantQuotaConfig,
    pub tenants: Vec<TenantQuotaEntry>,
    pub start_rate_per_second: u32,
    pub start_burst: u32,
    pub breaker_threshold: u32,
    pub breaker_cooldown_seconds: u64,
    pub rate_window_seconds: u64,
}

pub struct AdmissionState {
    settings: AdmissionSettings,
    capacity: NodeCapacity,
    overhead: Resources,
    tenant_quotas: HashMap<TenantId, TenantQuota>,
    reserved: Resources,
    counts: Counts,
    reservations: HashMap<ReservationId, Reservation>,
    revisions: HashMap<RevisionId, RevisionEntry>,
    tenants: BTreeMap<TenantId, TenantEntry>,
    waiters: HashMap<WaiterId, Waiter>,
    queued: usize,
    queued_bytes: u64,
    bucket: TokenBucket,
    next_id: u64,
    serve_seq: u64,
    outbox: Vec<(WaiterId, Outcome)>,
    /// Idle environments the pool is asked to evict because a start is
    /// blocked on node resources. Cleared when one is released.
    eviction_requested: bool,
    evictions: usize,
    rejections: BTreeMap<RejectReason, u64>,
    /// Revisions an alias currently routes (PLT-4635), as the scale
    /// reconciler last saw them in a valid configuration.
    routed: std::collections::HashSet<RevisionId>,
    /// Revisions being drained, and why.
    drains: HashMap<RevisionId, DrainReason>,
    /// The last scale decision per revision, kept after it scaled to zero.
    scale_records: HashMap<RevisionId, ScaleRecord>,
    /// Monotonic counters for metrics (PLT-4637).
    counters: AdmissionCounters,
}

impl std::fmt::Debug for AdmissionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionState")
            .field("reserved", &self.reserved)
            .field("in_flight", &self.counts.in_flight())
            .field("queued", &self.queued)
            .finish_non_exhaustive()
    }
}

impl AdmissionState {
    pub fn new(settings: AdmissionSettings) -> Self {
        let quota = |c: &TenantQuotaConfig| TenantQuota {
            max_concurrency: c.max_concurrency,
            max_queue: c.max_queue,
            weight: c.weight.unwrap_or(1).max(1),
            required_region: c.required_region.clone(),
        };
        let defaults = quota(&settings.tenant_defaults);
        let tenant_quotas = settings
            .tenants
            .iter()
            .map(|t| {
                (
                    t.tenant_id.clone(),
                    TenantQuota {
                        max_concurrency: t.max_concurrency.or(defaults.max_concurrency),
                        max_queue: t.max_queue.or(defaults.max_queue),
                        weight: t.weight.unwrap_or(defaults.weight).max(1),
                        required_region: t
                            .required_region
                            .clone()
                            .or_else(|| defaults.required_region.clone()),
                    },
                )
            })
            .collect();
        Self {
            capacity: NodeCapacity {
                cpu_millis: settings.node.cpu_millis,
                memory_mib: settings.node.memory_mib,
                ephemeral_storage_mib: settings.node.ephemeral_storage_mib,
            },
            overhead: settings.node.overhead(),
            tenant_quotas,
            bucket: TokenBucket::new(settings.start_rate_per_second, settings.start_burst),
            settings,
            reserved: Resources::ZERO,
            counts: Counts::default(),
            reservations: HashMap::new(),
            revisions: HashMap::new(),
            tenants: BTreeMap::new(),
            waiters: HashMap::new(),
            queued: 0,
            queued_bytes: 0,
            next_id: 0,
            serve_seq: 0,
            outbox: Vec::new(),
            eviction_requested: false,
            evictions: 0,
            rejections: BTreeMap::new(),
            routed: std::collections::HashSet::new(),
            drains: HashMap::new(),
            scale_records: HashMap::new(),
            counters: AdmissionCounters::default(),
        }
    }

    pub fn overhead(&self) -> Resources {
        self.overhead
    }

    pub fn take_outbox(&mut self) -> Vec<(WaiterId, Outcome)> {
        std::mem::take(&mut self.outbox)
    }

    /// Idle environments the pool should evict to make room (then reset).
    pub fn take_evictions(&mut self) -> usize {
        std::mem::take(&mut self.evictions)
    }

    pub fn queue_len(&self) -> usize {
        self.queued
    }

    pub fn is_queued(&self, id: WaiterId) -> bool {
        self.waiters.contains_key(&id)
    }

    /// The id the next successful [`Self::enqueue`] will return.
    pub fn peek_next_waiter_id(&self) -> WaiterId {
        self.next_id + 1
    }

    fn next(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn quota_for(&self, tenant: &TenantId) -> TenantQuota {
        self.tenant_quotas.get(tenant).cloned().unwrap_or_else(|| {
            let d = &self.settings.tenant_defaults;
            TenantQuota {
                max_concurrency: d.max_concurrency,
                max_queue: d.max_queue,
                weight: d.weight.unwrap_or(1).max(1),
                required_region: d.required_region.clone(),
            }
        })
    }

    fn tenant_entry(&mut self, tenant: &TenantId) -> &mut TenantEntry {
        if !self.tenants.contains_key(tenant) {
            let quota = self.quota_for(tenant);
            self.tenants.insert(
                tenant.clone(),
                TenantEntry {
                    quota,
                    in_flight: 0,
                    queue: VecDeque::new(),
                    queued_bytes: 0,
                    last_served: 0,
                    grants: 0,
                },
            );
        }
        self.tenants.get_mut(tenant).expect("inserted above")
    }

    fn revision_entry(&mut self, t: &Ticket) -> &mut RevisionEntry {
        let threshold = self.settings.breaker_threshold;
        let cooldown = self.settings.breaker_cooldown_seconds;
        let e = self
            .revisions
            .entry(t.revision.clone())
            .or_insert_with(|| RevisionEntry {
                tenant: t.tenant.clone(),
                max_environments: t.max_environments,
                concurrency_per_environment: t.concurrency_per_environment,
                min_ready: t.min_ready,
                idle_ttl_seconds: t.idle_ttl_seconds,
                cooldown_seconds: t.scale_down_cooldown_seconds,
                last_scale_up: None,
                counts: Counts::default(),
                queued: 0,
                stats: DemandStats::default(),
                breaker: CircuitBreaker::new(threshold, cooldown),
            });
        e.max_environments = t.max_environments.max(1);
        e.concurrency_per_environment = t.concurrency_per_environment.max(1);
        e.min_ready = t.min_ready;
        e.idle_ttl_seconds = t.idle_ttl_seconds;
        e.cooldown_seconds = t.scale_down_cooldown_seconds;
        e
    }

    /// Remember a scale decision for `revision` (bounded; the oldest record
    /// of a revision without reservations goes first).
    fn record_scale(
        &mut self,
        revision: &RevisionId,
        kind: &'static str,
        reason: &'static str,
        now: Timestamp,
    ) {
        *self
            .counters
            .scale_events
            .entry((kind, reason))
            .or_default() += 1;
        let event = ScaleEvent {
            kind,
            reason,
            at: now,
        };
        let Some(rev) = self.revisions.get(revision) else {
            if let Some(record) = self.scale_records.get_mut(revision) {
                record.event = event;
            }
            return;
        };
        let record = ScaleRecord {
            tenant: rev.tenant.clone(),
            event,
            min_ready: rev.min_ready,
            max_environments: rev.max_environments,
            idle_ttl_seconds: rev.idle_ttl_seconds,
            cooldown_seconds: rev.cooldown_seconds,
        };
        if self.scale_records.len() >= MAX_SCALE_RECORDS
            && !self.scale_records.contains_key(revision)
            && let Some(oldest) = self
                .scale_records
                .iter()
                .filter(|(id, _)| !self.revisions.contains_key(*id))
                .min_by_key(|(_, r)| r.event.at)
                .map(|(id, _)| id.clone())
        {
            self.scale_records.remove(&oldest);
        }
        self.scale_records.insert(revision.clone(), record);
    }

    fn reject_count(&mut self, reason: RejectReason) {
        *self.rejections.entry(reason).or_default() += 1;
    }

    fn window(&self) -> f64 {
        self.settings.rate_window_seconds.max(1) as f64
    }

    // -----------------------------------------------------------------------
    // arrivals
    // -----------------------------------------------------------------------

    /// Admit an invocation: check placement and the breaker, queue it, run
    /// the scheduler, and refuse it if it is still waiting while its tenant's
    /// queue share or the node queue (count or bytes) is over its limit.
    ///
    /// `retry` re-queues an invocation that already won its turn once (a
    /// promised pooled environment vanished, or a reused one died before the
    /// dispatch): it goes to the front of its tenant's queue, is not counted
    /// as a new arrival and is not refused for queue length.
    pub fn enqueue(
        &mut self,
        ticket: Ticket,
        retry: bool,
        now: Timestamp,
    ) -> Result<WaiterId, Rejection> {
        match self.check_static(&ticket, now) {
            Ok(()) => {}
            Err(r) => {
                self.reject_count(r.reason);
                return Err(r);
            }
        }
        let window = self.window();
        let rev = self.revision_entry(&ticket);
        if !retry {
            rev.stats.record_arrival(now, window);
        }
        rev.queued += 1;
        if !retry {
            self.counters.arrivals += 1;
        }
        let id = self.next();
        let tenant = ticket.tenant.clone();
        let bytes = ticket.payload_bytes;
        let t = self.tenant_entry(&tenant);
        t.queued_bytes += bytes;
        if retry {
            t.queue.push_front(id);
        } else {
            t.queue.push_back(id);
        }
        self.queued += 1;
        self.queued_bytes += bytes;
        self.waiters.insert(
            id,
            Waiter {
                ticket,
                enqueued_at: now,
                blocked: BlockReason::Pending,
                coalesced: false,
            },
        );
        self.pump(now);
        if retry || !self.waiters.contains_key(&id) {
            return Ok(id);
        }
        let t = &self.tenants[&tenant];
        let limit = if t.quota.max_queue.is_some_and(|max| t.queue.len() > max) {
            Some(Rejection::new(
                RejectReason::Quota,
                format!(
                    "the tenant's wait queue ({} slots) is full",
                    t.quota.max_queue.unwrap_or_default()
                ),
            ))
        } else if self.queued > self.settings.max_queue {
            Some(Rejection::new(
                RejectReason::QueueFull,
                format!(
                    "no capacity available and the wait queue ({} slots) is full",
                    self.settings.max_queue
                ),
            ))
        } else if self.queued_bytes > self.settings.max_queue_bytes {
            Some(Rejection::new(
                RejectReason::QueueFull,
                format!(
                    "no capacity available and the wait queue holds {} of {} payload bytes",
                    self.queued_bytes - bytes,
                    self.settings.max_queue_bytes
                ),
            ))
        } else {
            None
        };
        match limit {
            Some(rejection) => {
                self.remove_waiter(id);
                self.reject_count(rejection.reason);
                Err(rejection)
            }
            None => Ok(id),
        }
    }

    /// [`Self::check_static`] on its own, counted like a refusal of
    /// [`Self::enqueue`] (PLT-4643 asks it before the budget).
    pub fn precheck(&mut self, ticket: &Ticket, now: Timestamp) -> Result<(), Rejection> {
        self.check_static(ticket, now).inspect_err(|r| {
            self.reject_count(r.reason);
        })
    }

    /// Checks that do not depend on the moment: placement, an open breaker,
    /// and a reservation that can never fit.
    fn check_static(&mut self, t: &Ticket, now: Timestamp) -> Result<(), Rejection> {
        let quota = self.quota_for(&t.tenant);
        let node_region = self.settings.node.region.as_deref();
        for required in [
            quota.required_region.as_deref(),
            t.required_region.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if node_region != Some(required) {
                return Err(Rejection::new(
                    RejectReason::Placement,
                    format!(
                        "placement requires region `{required}` but node `{}` is in {}; \
                         the constraint is never relaxed",
                        self.settings.node.name,
                        node_region.map_or("no region".to_string(), |r| format!("region `{r}`"))
                    ),
                ));
            }
        }
        if self.drains.get(&t.revision) == Some(&DrainReason::FunctionDeleted) {
            return Err(function_deleted(&t.revision));
        }
        if let Some(rev) = self.revisions.get_mut(&t.revision)
            && rev.breaker.is_open(now)
        {
            return Err(circuit_open(&t.revision));
        }
        if !self.capacity.admits(t.resources) {
            return Err(Rejection::new(
                RejectReason::Capacity,
                format!(
                    "one environment needs {:?} including overhead, more than node `{}` has",
                    t.resources, self.settings.node.name
                ),
            ));
        }
        if quota.max_concurrency == Some(0) {
            return Err(Rejection::new(RejectReason::Quota, "tenant quota is zero"));
        }
        Ok(())
    }

    /// Remove a waiter that is still queued. Returns what blocked it last.
    pub fn withdraw(&mut self, id: WaiterId) -> Option<BlockReason> {
        self.remove_waiter(id).map(|w| w.blocked)
    }

    fn remove_waiter(&mut self, id: WaiterId) -> Option<Waiter> {
        let w = self.waiters.remove(&id)?;
        if let Some(t) = self.tenants.get_mut(&w.ticket.tenant) {
            t.queue.retain(|x| *x != id);
            t.queued_bytes -= w.ticket.payload_bytes;
        }
        if let Some(r) = self.revisions.get_mut(&w.ticket.revision) {
            r.queued = r.queued.saturating_sub(1);
        }
        self.queued -= 1;
        self.queued_bytes -= w.ticket.payload_bytes;
        Some(w)
    }

    /// Refuse every waiter whose queue deadline passed, with the reason that
    /// kept it waiting.
    pub fn expire(&mut self, now: Timestamp) {
        let expired: Vec<WaiterId> = self
            .waiters
            .iter()
            .filter(|(_, w)| w.ticket.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(w) = self.remove_waiter(id) {
                let reason = w.blocked.on_deadline();
                self.reject_count(reason);
                self.outbox.push((
                    id,
                    Outcome::Rejected(Rejection::new(
                        reason,
                        format!(
                            "queue deadline passed after {} ms waiting ({})",
                            (now - w.enqueued_at).num_milliseconds().max(0),
                            reason.as_str()
                        ),
                    )),
                ));
            }
        }
    }

    // -----------------------------------------------------------------------
    // scheduler
    // -----------------------------------------------------------------------

    /// Grant everything that can be granted now, fairest tenant first.
    ///
    /// Tenants with waiters are ordered by in-flight environments divided by
    /// weight (least attained service), ties broken by who was served least
    /// recently. The first waiter that is blocked on a *node-wide* limit
    /// (resources, `max_concurrency`, start rate) stops cold starts for every
    /// less fair waiter behind it, so small requests of a busy tenant cannot
    /// keep jumping ahead of it (no backfill). Quota, scaling and probe
    /// blocks are per tenant or per revision and do not stop anyone else.
    pub fn pump(&mut self, now: Timestamp) {
        loop {
            self.expire(now);
            let mut order: Vec<(u64, u64, TenantId)> = self
                .tenants
                .iter()
                .filter(|(_, t)| !t.queue.is_empty())
                .map(|(id, t)| {
                    // Scaled integer share: in_flight / weight.
                    let share =
                        (t.in_flight as u64).saturating_mul(1_000_000) / u64::from(t.quota.weight);
                    (share, t.last_served, id.clone())
                })
                .collect();
            if order.is_empty() {
                return;
            }
            order.sort();
            let mut node_block: Option<BlockReason> = None;
            let mut decision: Option<(WaiterId, Result<GrantKind, Rejection>)> = None;
            'tenants: for (_, _, tenant) in &order {
                let ids: Vec<WaiterId> = self.tenants[tenant].queue.iter().copied().collect();
                for id in ids {
                    match self.evaluate(id, now, node_block) {
                        Eval::Grant(kind) => {
                            decision = Some((id, Ok(kind)));
                            break 'tenants;
                        }
                        Eval::Reject(r) => {
                            decision = Some((id, Err(r)));
                            break 'tenants;
                        }
                        Eval::Blocked(reason, node_wide) => {
                            if let Some(w) = self.waiters.get_mut(&id) {
                                w.blocked = reason;
                                w.coalesced |= reason == BlockReason::Scaling;
                            }
                            if node_wide && node_block.is_none() {
                                node_block = Some(reason);
                            }
                        }
                    }
                }
            }
            match decision {
                Some((id, Ok(kind))) => self.grant(id, kind, now),
                Some((id, Err(rejection))) => {
                    self.remove_waiter(id);
                    self.reject_count(rejection.reason);
                    self.outbox.push((id, Outcome::Rejected(rejection)));
                }
                None => return,
            }
        }
    }

    fn evaluate(&mut self, id: WaiterId, now: Timestamp, node_block: Option<BlockReason>) -> Eval {
        let Some(w) = self.waiters.get(&id) else {
            return Eval::Blocked(BlockReason::Pending, false);
        };
        let t = w.ticket.clone();
        if self.drains.get(&t.revision) == Some(&DrainReason::FunctionDeleted) {
            return Eval::Reject(function_deleted(&t.revision));
        }
        let window = self.window();
        let tenant = &self.tenants[&t.tenant];
        let tenant_in_flight = tenant.in_flight;
        let tenant_max = tenant.quota.max_concurrency;
        let node_in_flight = self.counts.in_flight() as usize;
        let Some(rev) = self.revisions.get_mut(&t.revision) else {
            return Eval::Blocked(BlockReason::Pending, false);
        };
        let gate = rev.breaker.gate(now);
        if gate == BreakerGate::Reject {
            return Eval::Reject(circuit_open(&t.revision));
        }
        if tenant_max.is_some_and(|m| tenant_in_flight >= m)
            || rev.counts.in_flight() >= rev.max_environments
        {
            return Eval::Blocked(BlockReason::Quota, false);
        }
        if node_in_flight >= self.settings.max_concurrency {
            return Eval::Blocked(BlockReason::Capacity, true);
        }
        if !t.cold_only && rev.counts.get(ResState::Idle) > rev.counts.get(ResState::Promised) {
            return Eval::Grant(GrantKind::Warm);
        }
        if let Some(block) = node_block {
            return Eval::Blocked(block, false);
        }
        if gate == BreakerGate::Wait {
            return Eval::Blocked(BlockReason::Probe, false);
        }
        // Activation coalescing: environments that are ready (busy, idle or
        // on their way into the pool) or already starting count against the
        // autoscaler's target before another one boots.
        let desired = desired_environments(DemandInput {
            arrival_rate: rev.stats.arrival_rate(now, window),
            avg_duration_seconds: rev.stats.avg_duration().unwrap_or(0.0),
            in_flight: rev.counts.in_flight(),
            backlog: rev.queued,
            concurrency_per_environment: rev.concurrency_per_environment,
            min_ready: rev.min_ready,
            max_environments: tenant_max
                .map_or(rev.max_environments, |m| rev.max_environments.min(m as u32)),
        });
        let provisioned = rev.counts.get(ResState::Starting)
            + rev.counts.get(ResState::Busy)
            + rev.counts.get(ResState::Idle)
            + rev.counts.get(ResState::Parking);
        if provisioned >= desired {
            return Eval::Blocked(BlockReason::Scaling, false);
        }
        if !self.capacity.admits(self.reserved.plus(t.resources)) {
            let idle = self.counts.get(ResState::Idle);
            if idle > 0 && !self.eviction_requested {
                self.eviction_requested = true;
                self.evictions += 1;
            }
            return Eval::Blocked(BlockReason::Capacity, true);
        }
        if !self.bucket.available(now) {
            return Eval::Blocked(BlockReason::StartRate, true);
        }
        Eval::Grant(GrantKind::Cold)
    }

    fn grant(&mut self, id: WaiterId, kind: GrantKind, now: Timestamp) {
        let Some(w) = self.remove_waiter(id) else {
            return;
        };
        match kind {
            GrantKind::Cold => self.counters.grants_cold += 1,
            GrantKind::Warm => self.counters.grants_warm += 1,
        }
        if w.coalesced {
            self.counters.coalesced_waits += 1;
            if kind == GrantKind::Warm {
                self.counters.starts_avoided += 1;
            }
        }
        let t = w.ticket;
        let (state, resources, probe) = match kind {
            GrantKind::Warm => (ResState::Promised, t.resources, false),
            GrantKind::Cold => {
                let took = self.bucket.take(now);
                debug_assert!(took, "evaluate checked the bucket");
                let rev = self
                    .revisions
                    .get_mut(&t.revision)
                    .expect("queued revision");
                let probe = rev.breaker.gate(now) == BreakerGate::Probe;
                if probe {
                    rev.breaker.probe_started();
                }
                // The cooldown runs from every cold start an invocation
                // asked for: from zero it is an activation.
                let kind = if rev.counts.provisioned() == 0 {
                    "activation"
                } else {
                    "scale_up"
                };
                rev.last_scale_up = Some(now);
                self.record_scale(&t.revision, kind, "backlog", now);
                (ResState::Starting, t.resources, probe)
            }
        };
        let rid = self.next();
        self.insert_reservation(
            rid,
            Reservation {
                tenant: t.tenant.clone(),
                revision: t.revision.clone(),
                resources,
                state,
                probe,
            },
        );
        self.serve_seq += 1;
        let seq = self.serve_seq;
        let entry = self.tenant_entry(&t.tenant);
        entry.last_served = seq;
        entry.grants += 1;
        self.outbox.push((
            id,
            Outcome::Granted {
                reservation: rid,
                kind,
            },
        ));
    }

    // -----------------------------------------------------------------------
    // reservations
    // -----------------------------------------------------------------------

    fn account(&mut self, r: &Reservation, add: bool) {
        let s = r.state;
        if add {
            self.counts.inc(s);
        } else {
            self.counts.dec(s);
        }
        if s.holds_resources() {
            self.reserved = if add {
                self.reserved.plus(r.resources)
            } else {
                self.reserved.minus(r.resources)
            };
        }
        if s.in_flight() {
            let t = self.tenant_entry(&r.tenant);
            if add {
                t.in_flight += 1;
            } else {
                t.in_flight -= 1;
            }
        }
        if let Some(rev) = self.revisions.get_mut(&r.revision) {
            if add {
                rev.counts.inc(s);
            } else {
                rev.counts.dec(s);
            }
        }
    }

    fn insert_reservation(&mut self, id: ReservationId, r: Reservation) {
        self.account(&r, true);
        self.reservations.insert(id, r);
    }

    fn set_state(&mut self, id: ReservationId, to: ResState) -> bool {
        let Some(r) = self.reservations.get(&id).cloned() else {
            return false;
        };
        if r.state == to {
            return true;
        }
        self.account(&r, false);
        let r = Reservation { state: to, ..r };
        self.account(&r, true);
        self.reservations.insert(id, r);
        true
    }

    /// The environment of a cold grant reported `Ready`: `Starting` → `Busy`.
    pub fn ready(&mut self, id: ReservationId) {
        if self.reservations.get(&id).map(|r| r.state) == Some(ResState::Starting) {
            self.set_state(id, ResState::Busy);
        }
    }

    /// The environment went to the pool and is being quiesced.
    pub fn park(&mut self, id: ReservationId, now: Timestamp) {
        self.set_state(id, ResState::Parking);
        self.pump(now);
    }

    /// The pool published the environment as idle.
    pub fn parked(&mut self, id: ReservationId, now: Timestamp) {
        self.set_state(id, ResState::Idle);
        self.pump(now);
    }

    /// The pool is terminating the environment.
    pub fn drain(&mut self, id: ReservationId, now: Timestamp) {
        self.set_state(id, ResState::Draining);
        self.pump(now);
    }

    /// The environment behind the reservation is gone.
    pub fn release(&mut self, id: ReservationId, now: Timestamp) {
        if let Some(r) = self.reservations.remove(&id) {
            self.account(&r, false);
            if r.probe
                && let Some(rev) = self.revisions.get_mut(&r.revision)
            {
                rev.breaker.probe_abandoned();
            }
            if matches!(
                r.state,
                ResState::Idle | ResState::Draining | ResState::Parking
            ) {
                self.eviction_requested = false;
            }
            self.forget_revision_if_idle(&r.revision, now);
        }
        self.pump(now);
    }

    /// A driver holding `holder` (a promise or a cold reservation) took the
    /// pooled environment behind `idle`: the holder is released and the idle
    /// reservation becomes the driver's, `Busy`. Returns the reservation the
    /// driver now holds.
    pub fn adopt(
        &mut self,
        holder: Option<ReservationId>,
        idle: ReservationId,
        now: Timestamp,
    ) -> ReservationId {
        if let Some(h) = holder
            && h != idle
            && let Some(r) = self.reservations.remove(&h)
        {
            self.account(&r, false);
            if r.probe
                && let Some(rev) = self.revisions.get_mut(&r.revision)
            {
                rev.breaker.probe_abandoned();
            }
        }
        self.set_state(idle, ResState::Busy);
        self.pump(now);
        idle
    }

    /// A promised pooled environment was not there after all. Turn the
    /// promise into a cold reservation right away if the node, the breaker
    /// and the start rate allow it (the invocation already won its turn and
    /// its quota), else release the promise and return `false`: the caller
    /// re-queues with `retry = true`.
    pub fn redeem(&mut self, id: ReservationId, now: Timestamp) -> bool {
        let Some(r) = self.reservations.get(&id).cloned() else {
            return false;
        };
        if r.state != ResState::Promised {
            return r.state == ResState::Starting;
        }
        let fits = self.capacity.admits(self.reserved.plus(r.resources));
        let Some(rev) = self.revisions.get_mut(&r.revision) else {
            return false;
        };
        let gate = rev.breaker.gate(now);
        if fits && matches!(gate, BreakerGate::Allow | BreakerGate::Probe) && self.bucket.take(now)
        {
            let rev = self.revisions.get_mut(&r.revision).expect("checked");
            let probe = gate == BreakerGate::Probe;
            if probe {
                rev.breaker.probe_started();
            }
            rev.last_scale_up = Some(now);
            let revision = r.revision.clone();
            self.record_scale(&revision, "scale_up", "promised_environment_gone", now);
            self.account(&r, false);
            let converted = Reservation {
                state: ResState::Starting,
                probe,
                ..r
            };
            self.account(&converted, true);
            self.reservations.insert(id, converted);
            return true;
        }
        self.release(id, now);
        false
    }

    /// Record the boot result of a cold start. A failure that opens the
    /// breaker refuses every waiter of the revision at once.
    pub fn start_result(&mut self, id: ReservationId, ok: bool, now: Timestamp) {
        let Some(r) = self.reservations.get_mut(&id) else {
            return;
        };
        r.probe = false;
        let revision = r.revision.clone();
        if ok {
            self.counters.start_successes += 1;
        } else {
            self.counters.start_failures += 1;
        }
        let Some(rev) = self.revisions.get_mut(&revision) else {
            return;
        };
        if ok {
            rev.breaker.record_success();
        } else if rev.breaker.record_failure(now) {
            self.counters.breaker_opens += 1;
            let doomed: Vec<WaiterId> = self
                .waiters
                .iter()
                .filter(|(_, w)| w.ticket.revision == revision)
                .map(|(id, _)| *id)
                .collect();
            for w in doomed {
                self.remove_waiter(w);
                self.reject_count(RejectReason::CircuitOpen);
                self.outbox
                    .push((w, Outcome::Rejected(circuit_open(&revision))));
            }
        }
        self.pump(now);
    }

    /// A handler of the revision ran for `seconds`.
    pub fn observe_duration(&mut self, revision: &RevisionId, seconds: f64) {
        if let Some(rev) = self.revisions.get_mut(revision) {
            rev.stats.record_duration(seconds);
        }
    }

    // -----------------------------------------------------------------------
    // scale to zero, min_ready and drains (PLT-4635)
    // -----------------------------------------------------------------------

    /// The revisions an alias routes now, from a *valid* configuration. The
    /// caller does not call this while its configuration is expired, so an
    /// outage never looks like "every route went away".
    pub fn set_routed(&mut self, routed: std::collections::HashSet<RevisionId>) {
        self.routed = routed;
    }

    pub fn is_routed(&self, revision: &RevisionId) -> bool {
        self.routed.contains(revision)
    }

    /// Start draining `revision`. A function deletion also refuses every
    /// waiter of the revision right away (they never started). Returns true
    /// when this started the drain.
    pub fn begin_drain(
        &mut self,
        revision: &RevisionId,
        reason: DrainReason,
        now: Timestamp,
    ) -> bool {
        let previous = self.drains.get(revision).copied();
        // A deletion outranks an alias switch and is never undone.
        let reason = match previous {
            Some(DrainReason::FunctionDeleted) => DrainReason::FunctionDeleted,
            _ => reason,
        };
        self.drains.insert(revision.clone(), reason);
        let started = previous != Some(reason);
        if started {
            self.record_scale(revision, "drain", reason.as_str(), now);
        }
        if reason == DrainReason::FunctionDeleted {
            let doomed: Vec<WaiterId> = self
                .waiters
                .iter()
                .filter(|(_, w)| &w.ticket.revision == revision)
                .map(|(id, _)| *id)
                .collect();
            for w in doomed {
                self.remove_waiter(w);
                self.reject_count(RejectReason::FunctionDeleted);
                self.outbox
                    .push((w, Outcome::Rejected(function_deleted(revision))));
            }
            self.pump(now);
        }
        started
    }

    /// Stop draining a superseded revision an alias routes again (a
    /// rollback). A deletion is never ended.
    pub fn end_drain(&mut self, revision: &RevisionId) -> bool {
        if self.drains.get(revision) == Some(&DrainReason::AliasSwitch) {
            self.drains.remove(revision);
            return true;
        }
        false
    }

    pub fn drain_reason(&self, revision: &RevisionId) -> Option<DrainReason> {
        self.drains.get(revision).copied()
    }

    /// Forget a finished drain (nothing of the revision is left).
    pub fn forget_drain(&mut self, revision: &RevisionId) {
        self.drains.remove(revision);
    }

    /// Whether anything of `revision` is still reserved or waiting.
    pub fn revision_is_empty(&self, revision: &RevisionId) -> bool {
        self.revisions
            .get(revision)
            .is_none_or(|r| r.counts.is_empty() && r.queued == 0)
    }

    /// May the idle environment behind `id` be terminated now? When it may,
    /// the reservation moves to `Draining` in the same step, so no waiter can
    /// be promised it afterwards; the caller then takes the ledger row with
    /// its own compare-and-set, and a claimer that got there first simply
    /// adopts the reservation back to `Busy`.
    ///
    /// Always kept: a revision with waiters, or with every idle environment
    /// promised. For [`ScaleDown::Idle`], unless the revision is drained,
    /// also kept: idle for less than the idle TTL, within the cooldown after
    /// the last cold start, or needed for `min_ready` of a routed revision.
    pub fn try_scale_down(
        &mut self,
        id: ReservationId,
        how: ScaleDown,
        now: Timestamp,
    ) -> Result<(), KeepReason> {
        let Some(r) = self.reservations.get(&id) else {
            return Err(KeepReason::NotIdle);
        };
        if r.state != ResState::Idle {
            return Err(KeepReason::NotIdle);
        }
        let revision = r.revision.clone();
        let drained = self.drains.contains_key(&revision);
        let routed = self.routed.contains(&revision);
        let Some(rev) = self.revisions.get(&revision) else {
            return Err(KeepReason::NotIdle);
        };
        if rev.queued > 0 {
            return Err(KeepReason::Queued);
        }
        if rev.counts.get(ResState::Idle) <= rev.counts.get(ResState::Promised) {
            return Err(KeepReason::Promised);
        }
        let reason = match how {
            ScaleDown::Idle { idle_since } => {
                if !drained {
                    let ttl = chrono::Duration::seconds(
                        rev.idle_ttl_seconds.min(i64::MAX as u64 / 1000) as i64,
                    );
                    if now - idle_since < ttl {
                        return Err(KeepReason::IdleTtl);
                    }
                    let cooldown = chrono::Duration::seconds(
                        rev.cooldown_seconds.min(i64::MAX as u64 / 1000) as i64,
                    );
                    if rev.last_scale_up.is_some_and(|t| now - t < cooldown) {
                        return Err(KeepReason::Cooldown);
                    }
                    if routed && rev.counts.provisioned() <= rev.min_ready {
                        return Err(KeepReason::MinReady);
                    }
                    "idle_ttl"
                } else {
                    self.drains.get(&revision).map_or("drain", |d| d.as_str())
                }
            }
            ScaleDown::Drain { reason } => reason,
        };
        self.set_state(id, ResState::Draining);
        let left = self
            .revisions
            .get(&revision)
            .map_or(0, |r| r.counts.provisioned());
        let kind = match how {
            ScaleDown::Drain { .. } => "drain",
            _ if drained => "drain",
            _ if left == 0 => "scale_to_zero",
            _ => "scale_down",
        };
        self.record_scale(&revision, kind, reason, now);
        Ok(())
    }

    /// Reserve one speculative cold start for `min_ready` (PLT-4635). Never
    /// queues and never goes ahead of a waiter: it happens only when the
    /// revision is routed and not drained, fewer environments than
    /// `min_ready` are provisioned, nobody is waiting anywhere, and every cap
    /// that binds an invocation (node resources and `max_concurrency`,
    /// tenant and revision quotas, start rate, a closed breaker) allows it.
    /// The reservation counts against all of them like any other.
    pub fn try_prestart(
        &mut self,
        ticket: Ticket,
        now: Timestamp,
    ) -> Result<ReservationId, PrestartSkip> {
        self.check_static(&ticket, now)
            .map_err(PrestartSkip::Refused)?;
        if !self.routed.contains(&ticket.revision) || self.drains.contains_key(&ticket.revision) {
            return Err(PrestartSkip::NotRouted);
        }
        if ticket.min_ready == 0 {
            return Err(PrestartSkip::Satisfied);
        }
        if self.queued > 0 {
            return Err(PrestartSkip::WaitersFirst);
        }
        let tenant_quota = self.quota_for(&ticket.tenant);
        let tenant_in_flight = self.tenants.get(&ticket.tenant).map_or(0, |t| t.in_flight);
        let node_full = self.counts.in_flight() as usize >= self.settings.max_concurrency;
        let fits = self.capacity.admits(self.reserved.plus(ticket.resources));
        let bucket_ok = self.bucket.available(now);
        let rev = self.revision_entry(&ticket);
        if rev.counts.provisioned() >= rev.min_ready {
            return Err(PrestartSkip::Satisfied);
        }
        if tenant_quota
            .max_concurrency
            .is_some_and(|m| tenant_in_flight >= m)
            || rev.counts.in_flight() >= rev.max_environments
        {
            return Err(PrestartSkip::Blocked("quota"));
        }
        if rev.breaker.gate(now) != BreakerGate::Allow {
            return Err(PrestartSkip::Blocked("circuit_breaker"));
        }
        if node_full || !fits {
            return Err(PrestartSkip::Blocked("capacity"));
        }
        if !bucket_ok {
            return Err(PrestartSkip::Blocked("start_rate"));
        }
        rev.last_scale_up = Some(now);
        let took = self.bucket.take(now);
        debug_assert!(took);
        let rid = self.next();
        self.insert_reservation(
            rid,
            Reservation {
                tenant: ticket.tenant.clone(),
                revision: ticket.revision.clone(),
                resources: ticket.resources,
                state: ResState::Starting,
                probe: false,
            },
        );
        self.record_scale(&ticket.revision, "prestart", "min_ready", now);
        Ok(rid)
    }

    /// Environments of `revision` starting, busy, parking or idle.
    pub fn provisioned(&self, revision: &RevisionId) -> u32 {
        self.revisions
            .get(revision)
            .map_or(0, |r| r.counts.provisioned())
    }

    fn forget_revision_if_idle(&mut self, revision: &RevisionId, now: Timestamp) {
        let window = self.window();
        let forget = self.revisions.get_mut(revision).is_some_and(|rev| {
            rev.counts.is_empty()
                && rev.queued == 0
                && matches!(
                    rev.breaker.state(now),
                    BreakerState::Closed {
                        consecutive_failures: 0
                    }
                )
                && rev.stats.arrival_rate(now, window) < 0.001
        });
        if forget {
            self.revisions.remove(revision);
        }
    }

    // -----------------------------------------------------------------------
    // reporting
    // -----------------------------------------------------------------------

    /// `GET /v1/capacity` for `tenant`: node-wide totals, and only this
    /// tenant's own queue and revisions.
    pub fn snapshot(&mut self, tenant: &TenantId, now: Timestamp) -> api::CapacityInfo {
        let some = |v: u64| Some(v);
        let oldest = |ids: &mut dyn Iterator<Item = &Waiter>| {
            ids.map(|w| (now - w.enqueued_at).num_milliseconds().max(0) as u64)
                .max()
        };
        let window = self.window();
        let quota = self
            .tenants
            .get(tenant)
            .map(|t| t.quota.clone())
            .unwrap_or_else(|| self.quota_for(tenant));
        let tenant_entry = self.tenants.get(tenant);
        let tenant_info = api::TenantCapacityInfo {
            tenant_id: tenant.to_string(),
            in_flight: tenant_entry.map_or(0, |t| t.in_flight as u64),
            queued: tenant_entry.map_or(0, |t| t.queue.len() as u64),
            queued_bytes: tenant_entry.map_or(0, |t| t.queued_bytes),
            oldest_age_ms: oldest(
                &mut self.waiters.values().filter(|w| &w.ticket.tenant == tenant),
            ),
            max_concurrency: quota.max_concurrency.map(|v| v as u64),
            max_queue: quota.max_queue.map(|v| v as u64),
            weight: quota.weight,
            required_region: quota.required_region.clone(),
        };
        let routed = &self.routed;
        let drains = &self.drains;
        let records = &self.scale_records;
        let route_state = |id: &RevisionId| match drains.get(id) {
            Some(DrainReason::FunctionDeleted) => "deleting",
            Some(DrainReason::AliasSwitch) => "superseded",
            None if routed.contains(id) => "routed",
            None => "unrouted",
        };
        let event_info = |id: &RevisionId| {
            records.get(id).map(|r| api::ScaleEventInfo {
                kind: r.event.kind.to_string(),
                reason: r.event.reason.to_string(),
                at: r.event.at,
            })
        };
        let mut revisions: Vec<api::RevisionCapacityInfo> = self
            .revisions
            .iter_mut()
            .filter(|(_, r)| &r.tenant == tenant)
            .map(|(id, rev)| api::RevisionCapacityInfo {
                revision_id: id.to_string(),
                desired: desired_environments(DemandInput {
                    arrival_rate: rev.stats.arrival_rate(now, window),
                    avg_duration_seconds: rev.stats.avg_duration().unwrap_or(0.0),
                    in_flight: rev.counts.in_flight(),
                    backlog: rev.queued,
                    concurrency_per_environment: rev.concurrency_per_environment,
                    min_ready: rev.min_ready,
                    max_environments: rev.max_environments,
                }),
                max_environments: rev.max_environments,
                environments: rev.counts.api(),
                queued: rev.queued.into(),
                arrival_rate_per_second: (rev.stats.arrival_rate(now, window) * 1000.0).round()
                    / 1000.0,
                avg_duration_ms: rev
                    .stats
                    .avg_duration()
                    .map(|s| (s * 1000.0).round() as u64),
                circuit_breaker: rev.breaker.name(now).to_string(),
                min_ready: rev.min_ready,
                idle_ttl_seconds: rev.idle_ttl_seconds,
                scale_down_cooldown_seconds: rev.cooldown_seconds,
                route_state: route_state(id).to_string(),
                last_scale_event: event_info(id),
            })
            .collect();
        // Revisions that scaled to zero keep their last decision visible.
        for (id, record) in records {
            if &record.tenant != tenant || self.revisions.contains_key(id) {
                continue;
            }
            revisions.push(api::RevisionCapacityInfo {
                revision_id: id.to_string(),
                desired: 0,
                max_environments: record.max_environments,
                environments: Counts::default().api(),
                queued: 0,
                arrival_rate_per_second: 0.0,
                avg_duration_ms: None,
                circuit_breaker: "closed".to_string(),
                min_ready: record.min_ready,
                idle_ttl_seconds: record.idle_ttl_seconds,
                scale_down_cooldown_seconds: record.cooldown_seconds,
                route_state: route_state(id).to_string(),
                last_scale_event: event_info(id),
            });
        }
        revisions.sort_by(|a, b| a.revision_id.cmp(&b.revision_id));
        let node = &self.settings.node;
        api::CapacityInfo {
            node: api::NodeInfo {
                name: node.name.clone(),
                region: node.region.clone(),
                hosts: 1,
                host_scale_out: "not_supported".into(),
                capacity: api::ResourceAmounts {
                    cpu_millis: self.capacity.cpu_millis,
                    memory_mib: self.capacity.memory_mib,
                    ephemeral_storage_mib: self.capacity.ephemeral_storage_mib,
                },
                per_environment_overhead: api::ResourceAmounts {
                    cpu_millis: some(self.overhead.cpu_millis),
                    memory_mib: some(self.overhead.memory_mib),
                    ephemeral_storage_mib: some(self.overhead.ephemeral_storage_mib),
                },
                max_concurrency: self.settings.max_concurrency as u64,
            },
            reserved: api::ResourceAmounts {
                cpu_millis: some(self.reserved.cpu_millis),
                memory_mib: some(self.reserved.memory_mib),
                ephemeral_storage_mib: some(self.reserved.ephemeral_storage_mib),
            },
            environments: self.counts.api(),
            in_flight: self.counts.in_flight().into(),
            queue: api::QueueInfo {
                length: self.queued as u64,
                bytes: self.queued_bytes,
                max_length: self.settings.max_queue as u64,
                max_bytes: self.settings.max_queue_bytes,
                timeout_seconds: self.settings.queue_timeout_seconds,
                oldest_age_ms: oldest(&mut self.waiters.values()),
            },
            start_rate: api::StartRateInfo {
                per_second: self.bucket.per_second(),
                burst: self.bucket.burst(),
                tokens: self.bucket.tokens(now),
            },
            rejections: self
                .rejections
                .iter()
                .map(|(r, n)| (r.as_str().to_string(), *n))
                .collect(),
            tenant: tenant_info,
            revisions,
            scaling: api::ScalingInfo::default(),
            reuse: api::EnvironmentReuseReport::default(),
        }
    }

    /// Every tenant and revision, for `GET /metrics` (PLT-4637). Revisions
    /// that scaled to zero and were forgotten are not listed (their counts
    /// are zero); the monotonic counters survive that.
    pub fn metrics(&mut self, now: Timestamp) -> AdmissionMetrics {
        let age = |w: &Waiter| (now - w.enqueued_at).num_milliseconds().max(0) as f64 / 1000.0;
        let window = self.window();
        let mut oldest_by_tenant: HashMap<&TenantId, f64> = HashMap::new();
        let mut oldest: Option<f64> = None;
        for w in self.waiters.values() {
            let a = age(w);
            oldest = Some(oldest.map_or(a, |o: f64| o.max(a)));
            let e = oldest_by_tenant.entry(&w.ticket.tenant).or_insert(a);
            *e = e.max(a);
        }
        let tenants = self
            .tenants
            .iter()
            .map(|(id, t)| TenantMetrics {
                tenant: id.clone(),
                in_flight: t.in_flight as u64,
                queued: t.queue.len() as u64,
                queued_bytes: t.queued_bytes,
                oldest_wait_seconds: oldest_by_tenant.get(id).copied(),
                grants: t.grants,
                max_concurrency: t.quota.max_concurrency.map(|m| m as u64),
            })
            .collect();
        let mut revisions: Vec<RevisionMetrics> = self
            .revisions
            .iter_mut()
            .map(|(id, rev)| RevisionMetrics {
                tenant: rev.tenant.clone(),
                revision: id.clone(),
                environments: rev.counts.api(),
                desired: desired_environments(DemandInput {
                    arrival_rate: rev.stats.arrival_rate(now, window),
                    avg_duration_seconds: rev.stats.avg_duration().unwrap_or(0.0),
                    in_flight: rev.counts.in_flight(),
                    backlog: rev.queued,
                    concurrency_per_environment: rev.concurrency_per_environment,
                    min_ready: rev.min_ready,
                    max_environments: rev.max_environments,
                }),
                max_environments: rev.max_environments,
                min_ready: rev.min_ready,
                queued: rev.queued,
                breaker: rev.breaker.name(now),
            })
            .collect();
        revisions.sort_by(|a, b| a.revision.cmp(&b.revision));
        AdmissionMetrics {
            node_name: self.settings.node.name.clone(),
            capacity: self.capacity,
            overhead: self.overhead,
            reserved: self.reserved,
            max_concurrency: self.settings.max_concurrency as u64,
            in_flight: self.counts.in_flight().into(),
            environments: self.counts.api(),
            queue_length: self.queued as u64,
            queue_bytes: self.queued_bytes,
            max_queue: self.settings.max_queue as u64,
            max_queue_bytes: self.settings.max_queue_bytes,
            oldest_wait_seconds: oldest,
            start_tokens: self.bucket.tokens(now),
            rejections: self.rejections.clone(),
            counters: self.counters.clone(),
            revisions,
            tenants,
        }
    }

    /// Recompute every counter from the reservations and compare
    /// (tests; the property test calls it after every operation).
    #[cfg(test)]
    pub fn check_invariants(&self) {
        let mut reserved = Resources::ZERO;
        let mut counts = Counts::default();
        let mut tenant_in_flight: HashMap<TenantId, usize> = HashMap::new();
        let mut rev_counts: HashMap<RevisionId, Counts> = HashMap::new();
        for r in self.reservations.values() {
            counts.inc(r.state);
            rev_counts
                .entry(r.revision.clone())
                .or_default()
                .inc(r.state);
            if r.state.holds_resources() {
                reserved = reserved.plus(r.resources);
            }
            if r.state.in_flight() {
                *tenant_in_flight.entry(r.tenant.clone()).or_default() += 1;
            }
        }
        assert_eq!(reserved, self.reserved, "reserved resources drifted");
        assert_eq!(counts.0, self.counts.0, "state counts drifted");
        assert!(self.capacity.admits(self.reserved), "over node capacity");
        assert!(
            self.counts.in_flight() as usize <= self.settings.max_concurrency,
            "over max_concurrency"
        );
        for (id, t) in &self.tenants {
            let n = tenant_in_flight.get(id).copied().unwrap_or(0);
            assert_eq!(t.in_flight, n, "tenant in-flight drifted");
            if let Some(max) = t.quota.max_concurrency {
                assert!(n <= max, "tenant over quota");
            }
        }
        for (id, rev) in &self.revisions {
            let c = rev_counts.get(id).copied().unwrap_or_default();
            assert_eq!(c.0, rev.counts.0, "revision counts drifted");
            assert!(
                c.in_flight() <= rev.max_environments,
                "revision over pool quota"
            );
        }
        let queued: usize = self.tenants.values().map(|t| t.queue.len()).sum();
        assert_eq!(queued, self.queued);
        assert_eq!(queued, self.waiters.len());
        let bytes: u64 = self.waiters.values().map(|w| w.ticket.payload_bytes).sum();
        assert_eq!(bytes, self.queued_bytes);
    }

    #[cfg(test)]
    pub fn state_of(&self, id: ReservationId) -> Option<ResState> {
        self.reservations.get(&id).map(|r| r.state)
    }

    #[cfg(test)]
    pub fn revision_of(&self, id: ReservationId) -> Option<RevisionId> {
        self.reservations.get(&id).map(|r| r.revision.clone())
    }

    #[cfg(test)]
    pub fn reserved(&self) -> Resources {
        self.reserved
    }
}

enum Eval {
    Grant(GrantKind),
    Reject(Rejection),
    /// Blocked, and whether the block is node-wide.
    Blocked(BlockReason, bool),
}

fn function_deleted(revision: &RevisionId) -> Rejection {
    Rejection::new(
        RejectReason::FunctionDeleted,
        format!(
            "the function of revision {revision} is being deleted; new work is refused \
             (work already running completes)"
        ),
    )
}

fn circuit_open(revision: &RevisionId) -> Rejection {
    Rejection::new(
        RejectReason::CircuitOpen,
        format!(
            "revision {revision} failed to start repeatedly; starts are paused \
             (circuit breaker open)"
        ),
    )
}

//! Scale to zero, `min_ready`, cooldown and drains (PLT-4635,
//! docs/adr/0009-scale-to-zero-and-drain.md).
//!
//! The [`ScaleController`] is a reconciler: [`ScaleController::reconcile`]
//! runs every `[scaling] reconcile_interval_ms` (the gateway's loop; tests
//! call it directly with a fake clock) and converges, one step at a time:
//!
//! 1. **Routes.** From a *valid* configuration cache only
//!    ([`ConfigCache::scale_view`]): which revisions an alias routes, which
//!    functions are deleted. A revision that was routed and no longer is
//!    starts an `alias_switch` drain; every revision of a deleted function
//!    starts a `function_deleted` drain, which also refuses its waiters. While
//!    the cache is expired nothing is re-classified (the view is *held*), so
//!    an outage or a reconnect storm never drains and re-creates anything.
//! 2. **Drain timeout.** Invocations of a drained revision accepted before the
//!    drain started and still running `drain_timeout_seconds` later are
//!    stopped like an execution timeout (`Host.DrainTimeout`).
//! 3. **Idle sweep.** [`EnvironmentPool::sweep`], where admission decides per
//!    environment (waiters, promises, idle TTL, cooldown, `min_ready`, drains)
//!    and the ledger compare-and-set decides a race with a claim.
//! 4. **`min_ready`.** For every routed revision below `min_ready`, reserve
//!    pre-starts through admission (never queued, never ahead of a waiter,
//!    within every cap) and boot them into the pool.
//! 5. **Deletion finalization** (on the gateway that owns the management
//!    store): once nothing of a deleted function runs or remains on the host,
//!    record `drained_at`.
//!
//! Scaling to zero environments does not scale the host to zero: the gateway
//! process, its store and the node keep running (and costing) whatever the
//! environment count is.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    Clock, EnvironmentState, Function, FunctionId, FunctionRevision, RevisionId, Timestamp,
};

use crate::control::InvokeGate;
use crate::repository::Repositories;
use crate::services::admission::{AdmissionController, DrainReason, PrestartSkip};
use crate::services::invoke::InvokeService;
use crate::services::pool::{EnvironmentPool, PoolSweep};

/// `[scaling]` (PLT-4635).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ScalingConfig {
    /// How often the scale reconciler runs.
    pub reconcile_interval_ms: u64,
    /// Scale-down cooldown of a revision that does not set
    /// `execution.scale_down_cooldown_seconds`.
    pub scale_down_cooldown_seconds: u64,
    /// How long invocations of a drained revision (alias switch, deletion)
    /// may keep running after the drain started before they are stopped as
    /// `Host.DrainTimeout`.
    pub drain_timeout_seconds: u64,
    /// Wait before a revision's next `min_ready` pre-start after one failed
    /// (boot failure, refused by the pool, cold starts restricted).
    pub prestart_backoff_seconds: u64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            reconcile_interval_ms: 1_000,
            scale_down_cooldown_seconds: 30,
            drain_timeout_seconds: 300,
            prestart_backoff_seconds: 5,
        }
    }
}

impl ScalingConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(50..=60_000).contains(&self.reconcile_interval_ms) {
            return Err("scaling.reconcile_interval_ms must be within 50..=60000".into());
        }
        if self.scale_down_cooldown_seconds > 3_600 {
            return Err("scaling.scale_down_cooldown_seconds must be <= 3600".into());
        }
        if self.drain_timeout_seconds == 0 || self.drain_timeout_seconds > 86_400 {
            return Err("scaling.drain_timeout_seconds must be within 1..=86400".into());
        }
        if self.prestart_backoff_seconds == 0 || self.prestart_backoff_seconds > 3_600 {
            return Err("scaling.prestart_backoff_seconds must be within 1..=3600".into());
        }
        Ok(())
    }

    pub fn reconcile_interval(&self) -> Duration {
        Duration::from_millis(self.reconcile_interval_ms)
    }

    fn drain_timeout(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.drain_timeout_seconds as i64)
    }

    fn prestart_backoff(&self) -> chrono::Duration {
        chrono::Duration::seconds(self.prestart_backoff_seconds as i64)
    }
}

/// What one [`ScaleController::reconcile`] did.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ScaleReport {
    /// `valid`: routes were re-read. `held`: the configuration cache is not
    /// valid, so routes and drains were left exactly as they were.
    pub view: &'static str,
    pub routed: usize,
    pub drains_started: Vec<(RevisionId, &'static str)>,
    pub drains_ended: Vec<RevisionId>,
    /// Invocations stopped because their revision's drain timed out.
    pub drain_timeouts: usize,
    pub sweep: Option<PoolSweep>,
    pub prestarts: usize,
    pub prestart_skips: Vec<(RevisionId, String)>,
    /// Deleted functions whose deletion drained in this round.
    pub finalized: Vec<FunctionId>,
}

#[derive(Debug, Clone)]
struct DrainTrack {
    reason: DrainReason,
    since: Timestamp,
    timed_out: bool,
}

#[derive(Default)]
struct ControllerState {
    /// The routed revisions of the last valid view.
    routed: HashSet<RevisionId>,
    drains: HashMap<RevisionId, DrainTrack>,
    prestart_not_before: HashMap<RevisionId, Timestamp>,
    prestarts: Vec<tokio::task::JoinHandle<()>>,
}

pub struct ScaleController {
    admission: Arc<AdmissionController>,
    pool: Arc<EnvironmentPool>,
    invoke: Arc<InvokeService>,
    gate: Arc<InvokeGate>,
    repos: Repositories,
    clock: Arc<dyn Clock>,
    config: ScalingConfig,
    /// This gateway owns the management store and records `drained_at`.
    finalizes: bool,
    state: Mutex<ControllerState>,
    /// One reconcile at a time.
    running: tokio::sync::Mutex<()>,
    me: Mutex<Weak<ScaleController>>,
}

impl std::fmt::Debug for ScaleController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScaleController")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ScaleController {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        admission: Arc<AdmissionController>,
        pool: Arc<EnvironmentPool>,
        invoke: Arc<InvokeService>,
        gate: Arc<InvokeGate>,
        repos: Repositories,
        clock: Arc<dyn Clock>,
        config: ScalingConfig,
        finalizes: bool,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            admission,
            pool,
            invoke,
            gate,
            repos,
            clock,
            config,
            finalizes,
            state: Mutex::new(ControllerState::default()),
            running: tokio::sync::Mutex::new(()),
            me: Mutex::new(Weak::new()),
        });
        *this.me.lock() = Arc::downgrade(&this);
        this
    }

    pub fn config(&self) -> &ScalingConfig {
        &self.config
    }

    /// Wait for every pre-start this controller spawned, then for the pool
    /// to settle (tests: after this, what the pool holds is final).
    pub async fn settle(&self) {
        loop {
            let handles: Vec<_> = std::mem::take(&mut self.state.lock().prestarts);
            if handles.is_empty() {
                break;
            }
            for h in handles {
                let _ = h.await;
            }
        }
        self.pool.settle().await;
    }

    /// One reconcile round (see the module documentation).
    pub async fn reconcile(&self) -> ScaleReport {
        let _one = self.running.lock().await;
        let mut report = ScaleReport {
            view: "held",
            ..ScaleReport::default()
        };
        let cache = self.gate.cache().clone();
        cache.sync_if_authoritative().await;
        let now = self.clock.now();
        let view = cache.scale_view();

        // 1. routes and drains -------------------------------------------------
        if let Some(view) = &view {
            report.view = "valid";
            let routed: HashSet<RevisionId> = view.routed.keys().cloned().collect();
            report.routed = routed.len();
            let mut deleted_revisions: HashMap<RevisionId, FunctionId> = HashMap::new();

            for (function, revisions) in &view.deleted {
                for r in revisions {
                    deleted_revisions.insert(r.clone(), function.id.clone());
                }
            }
            let mut state = self.state.lock();
            // Superseded: routed in the previous valid view, not any more.
            let superseded: Vec<RevisionId> = state
                .routed
                .difference(&routed)
                .filter(|r| !deleted_revisions.contains_key(*r))
                .cloned()
                .collect();
            let mut starts: Vec<(RevisionId, DrainReason)> = Vec::new();
            for r in superseded {
                if !state.drains.contains_key(&r) {
                    starts.push((r, DrainReason::AliasSwitch));
                }
            }
            for r in deleted_revisions.keys() {
                if state
                    .drains
                    .get(r)
                    .is_none_or(|d| d.reason != DrainReason::FunctionDeleted)
                {
                    starts.push((r.clone(), DrainReason::FunctionDeleted));
                }
            }
            // Routed again (a rollback): an alias-switch drain ends.
            let ended: Vec<RevisionId> = state
                .drains
                .iter()
                .filter(|(r, d)| d.reason == DrainReason::AliasSwitch && routed.contains(*r))
                .map(|(r, _)| r.clone())
                .collect();
            for r in &ended {
                state.drains.remove(r);
            }
            for (r, reason) in &starts {
                let since = match state.drains.get(r) {
                    // An alias switch escalated to a deletion keeps its clock.
                    Some(d) => d.since,
                    None => now,
                };
                state.drains.insert(
                    r.clone(),
                    DrainTrack {
                        reason: *reason,
                        since,
                        timed_out: false,
                    },
                );
            }
            state.routed = routed.clone();
            drop(state);
            self.admission.set_routed(routed);
            for r in &ended {
                self.admission.end_drain(r);
                self.pool.set_draining(r, None);
                tracing::info!(revision_id = %r, "revision routed again; drain ended");
            }
            for (r, reason) in starts {
                self.admission.begin_drain(&r, reason);
                self.pool.set_draining(&r, Some(reason.as_str()));
                self.invoke.cancel_prestarts(&r);
                tracing::info!(
                    revision_id = %r,
                    reason = reason.as_str(),
                    "draining revision: new work goes elsewhere (or is refused), running work completes"
                );
                report.drains_started.push((r, reason.as_str()));
            }
            report.drains_ended = ended;
        }

        // 2. drain timeout -------------------------------------------------------
        let expired: Vec<(RevisionId, Timestamp)> = {
            let mut state = self.state.lock();
            let limit = self.config.drain_timeout();
            state
                .drains
                .iter_mut()
                .filter(|(_, d)| !d.timed_out && now - d.since >= limit)
                .map(|(r, d)| {
                    d.timed_out = true;
                    (r.clone(), d.since)
                })
                .collect()
        };
        for (r, since) in expired {
            let stopped = self.invoke.stop_for_drain(&r, since);
            if stopped > 0 {
                tracing::warn!(
                    revision_id = %r,
                    stopped,
                    drain_timeout_seconds = self.config.drain_timeout_seconds,
                    "drain timeout: stopping invocations still running on a drained revision"
                );
            }
            report.drain_timeouts += stopped;
        }

        // 3. idle sweep -----------------------------------------------------------
        if self.pool.policy().reuse_enabled() {
            report.sweep = Some(self.pool.sweep().await);
        }

        // Finished alias-switch drains are forgotten: nothing of the revision
        // is left, and a later pinned invocation may pool it again.
        {
            let finished: Vec<RevisionId> = self
                .state
                .lock()
                .drains
                .iter()
                .filter(|(r, d)| {
                    d.reason == DrainReason::AliasSwitch
                        && self.admission.revision_is_empty(r)
                        && self.invoke.in_flight_of(r) == 0
                })
                .map(|(r, _)| r.clone())
                .collect();
            for r in finished {
                self.state.lock().drains.remove(&r);
                self.admission.forget_drain(&r);
                self.pool.set_draining(&r, None);
            }
        }

        // 4. min_ready -------------------------------------------------------------
        if let Some(view) = &view
            && self.pool.policy().reuse_enabled()
        {
            self.prestart(&view.routed, now, &mut report);
        }

        // 5. deletion finalization ---------------------------------------------------
        if self.finalizes
            && let Some(view) = &view
        {
            for (function, revisions) in &view.deleted {
                if function.drained_at.is_some() {
                    continue;
                }
                if self.deletion_drained(function, revisions) {
                    match self.finalize(function) {
                        Ok(true) => report.finalized.push(function.id.clone()),
                        Ok(false) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, function_id = %function.id, "cannot record the drained deletion")
                        }
                    }
                }
            }
        }
        report
    }

    fn prestart(
        &self,
        routed: &HashMap<RevisionId, (Function, FunctionRevision)>,
        now: Timestamp,
        report: &mut ScaleReport,
    ) {
        let limits = self.pool.policy().limits();
        for (id, (function, revision)) in routed {
            let min_ready = revision.spec.execution.min_ready;
            if min_ready == 0 {
                continue;
            }
            if self
                .state
                .lock()
                .prestart_not_before
                .get(id)
                .is_some_and(|t| now < *t)
            {
                report.prestart_skips.push((id.clone(), "backoff".into()));
                continue;
            }
            let provisioned = self.admission.provisioned(id);
            for _ in provisioned..min_ready {
                // The pool's global cap: a pre-start it would refuse is not
                // booted at all (boot, refuse, terminate, repeat would flap).
                if self.pool.held() + self.pool.quiescing() >= limits.max_total_idle {
                    report.prestart_skips.push((id.clone(), "pool_full".into()));
                    break;
                }
                if let Err((kind, _)) = self.gate.permit_cold_start(revision, &function.tenant_id) {
                    report
                        .prestart_skips
                        .push((id.clone(), kind.error_type().to_string()));
                    self.backoff(id, now);
                    break;
                }
                let ticket = self.admission.ticket(
                    &function.tenant_id,
                    revision,
                    0,
                    now + chrono::Duration::seconds(3_600),
                );
                match self.admission.try_prestart(ticket) {
                    Ok(grant) => {
                        report.prestarts += 1;
                        self.spawn_prestart(function.clone(), revision.clone(), grant);
                    }
                    Err(skip) => {
                        let why = match &skip {
                            PrestartSkip::Satisfied => "satisfied".to_string(),
                            PrestartSkip::NotRouted => "not_routed".to_string(),
                            PrestartSkip::WaitersFirst => "waiters_first".to_string(),
                            PrestartSkip::Blocked(r) => (*r).to_string(),
                            PrestartSkip::Refused(r) => r.reason.as_str().to_string(),
                        };
                        if matches!(skip, PrestartSkip::Refused(_)) {
                            self.backoff(id, now);
                        }
                        if !matches!(skip, PrestartSkip::Satisfied) {
                            report.prestart_skips.push((id.clone(), why));
                        }
                        break;
                    }
                }
            }
        }
    }

    fn backoff(&self, revision: &RevisionId, now: Timestamp) {
        self.state
            .lock()
            .prestart_not_before
            .insert(revision.clone(), now + self.config.prestart_backoff());
    }

    fn spawn_prestart(
        &self,
        function: Function,
        revision: FunctionRevision,
        grant: crate::services::admission::Grant,
    ) {
        let me = self.me.lock().clone();
        let invoke = self.invoke.clone();
        let handle = tokio::spawn(async move {
            let id = revision.id.clone();
            if let Err(why) = invoke.prestart(function, revision, grant).await {
                tracing::warn!(revision_id = %id, reason = %why, "min_ready pre-start failed; backing off");
                if let Some(me) = me.upgrade() {
                    let now = me.clock.now();
                    me.backoff(&id, now);
                }
            }
        });
        let mut state = self.state.lock();
        state.prestarts.retain(|h| !h.is_finished());
        state.prestarts.push(handle);
    }

    /// Nothing of `function` runs or remains: no invocation in flight here,
    /// nothing reserved or queued in admission, no non-terminal invocation
    /// among its recent ones in the ledger, no active environment of its
    /// revisions.
    fn deletion_drained(&self, function: &Function, revisions: &[RevisionId]) -> bool {
        let revs: HashSet<&RevisionId> = revisions.iter().collect();
        if revisions
            .iter()
            .any(|r| self.invoke.in_flight_of(r) > 0 || !self.admission.revision_is_empty(r))
        {
            return false;
        }
        match self.repos.invocations.list_by_function(&function.id, 1_000) {
            Ok(invs) if invs.iter().all(|i| i.status.is_terminal()) => {}
            _ => return false,
        }
        match self.repos.environments.list_active() {
            Ok(envs) => !envs.iter().any(|e| {
                revs.contains(&e.revision_id) && !matches!(e.state, EnvironmentState::Stopped)
            }),
            Err(_) => false,
        }
    }

    fn finalize(&self, function: &Function) -> Result<bool, crate::repository::RepoError> {
        let Some(mut current) = self.repos.functions.get(&function.id)? else {
            return Ok(false);
        };
        if current.drained_at.is_some() {
            return Ok(false);
        }
        if current.mark_drained(self.clock.now()).is_err() {
            return Ok(false);
        }
        self.repos.functions.update(current)?;
        tracing::info!(function_id = %function.id, "function deletion drained: no work and no environment left");
        Ok(true)
    }
}

//! Startup reconciliation: reclaim environments a previous process left behind.
//!
//! Loading `state.json` settles the *ledger* (see
//! [`crate::repository`]), but the host processes, sockets, drives and
//! workdirs of a gateway that crashed or was killed survive it. This service
//! asks the provider what it still tracks and terminates everything this
//! gateway does not own, with
//! [`TerminateReason::Reconcile`](tachyon_serverless_provider_port::TerminateReason::Reconcile)
//! (docs/architecture.md §4, docs/threat-model.md §12 T10).
//!
//! **Ownership, not just the id.** An environment is adopted only when the
//! ledger knows it as active *and* the host is still running the incarnation
//! the ledger recorded. The identity compared here is the provider's
//! environment id plus the boot evidence stored with it at its current epoch —
//! this platform's equivalent of a Pod UID. A host process that answers to a
//! known id but is a different incarnation (a previous process's environment,
//! or one the provider re-created) is reclaimed and its row marked `Lost`,
//! because nothing in this process holds a session to it.
//!
//! It is meant to run once, before the listener accepts. An environment of a
//! live invocation of *this* process is recorded in the ledger before
//! `create_environment` is called, so it is always part of the known set and
//! is never terminated here.
//!
//! **Other dispatchers (PLT-4631).** Another gateway may share the
//! `data_dir` (and the provider's workdir). An environment owned by a
//! dispatcher that is still live is left entirely alone: not adopted, not
//! terminated, not marked `Lost`. Before any of that, [`ReconcileService::reclaim`]
//! reclaims the work of dispatchers that lost their lease (the ledger half,
//! [`crate::services::Dispatcher::reclaim_ledger`]) and terminates the
//! environments that reclaim fenced, settling each one only once the
//! provider confirmed the terminate. The gateway also runs `reclaim` on its
//! heartbeat timer.
//!
//! A pass never fails: a provider that cannot be listed is logged and startup
//! continues.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_domain::{
    Clock, DispatcherId, EnvironmentId, ExecutionEnvironment, StoppedBy,
};
use tachyon_serverless_provider_port::{
    EnvironmentObservation, ExecutionProvider, TerminateReason, UsageSink,
};

use crate::repository::Repositories;
use crate::services::Dispatcher;
use crate::services::invoke::sample_before_terminate;
use crate::services::stopped::{ReclaimedStop, record_reclaimed_stop};

/// Result of one reconcile pass. Also rendered by `GET /readyz`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReconcileReport {
    /// Environments the provider still tracks.
    pub found: usize,
    /// Of those, the ones this gateway knows as active and still owns.
    pub adopted: usize,
    /// Orphans terminated with `TerminateReason::Reconcile`.
    pub terminated: usize,
    /// Of the terminated ones, those the ledger knew by id but whose host
    /// incarnation did not match the recorded boot evidence.
    pub disowned: usize,
    /// Orphans whose terminate failed; they are still on the host.
    pub failed: usize,
    /// Known environments the provider no longer tracks: marked `Lost`.
    pub lost: usize,
    /// Environments owned by another live dispatcher: left alone.
    pub foreign: usize,
    /// What the reclaim before the pass did.
    pub reclaim: ReclaimSummary,
    /// Why the pass could not complete. Startup continues either way.
    pub error: Option<String>,
}

/// One reclaim of the work of dispatchers that lost their lease (PLT-4631).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReclaimSummary {
    /// Dispatchers marked reclaimed by this pass.
    pub dispatchers: usize,
    /// Slot leases released because their owner lost its lease.
    pub leases: usize,
    /// Invocations settled (`OutcomeUnknown` once dispatched).
    pub invocations: usize,
    /// Environments fenced by this pass.
    pub fenced: usize,
    /// Fenced environments (from this or an earlier pass) whose terminate the
    /// provider confirmed: settled as `Lost`.
    pub terminated: usize,
    /// Fenced environments whose terminate failed: still `Draining`, retried
    /// by the next pass, never reused or counted free.
    pub pending: usize,
    pub error: Option<String>,
}

/// Reclaims orphaned environments. Lives in the application layer so every
/// provider benefits from it.
pub struct ReconcileService {
    repos: Repositories,
    provider: Arc<dyn ExecutionProvider>,
    clock: Arc<dyn Clock>,
    dispatcher: Arc<Dispatcher>,
    /// Every environment a reclaim or this reconcile ends is metered here,
    /// once (docs/adr/0012 「回収された環境の計量」).
    usage: Arc<dyn UsageSink>,
    last: Mutex<Option<ReconcileReport>>,
}

impl ReconcileService {
    pub fn new(
        repos: Repositories,
        provider: Arc<dyn ExecutionProvider>,
        clock: Arc<dyn Clock>,
        dispatcher: Arc<Dispatcher>,
        usage: Arc<dyn UsageSink>,
    ) -> Self {
        Self {
            repos,
            provider,
            clock,
            dispatcher,
            usage,
            last: Mutex::new(None),
        }
    }

    /// Reclaim the work of dispatchers that lost their lease, then terminate
    /// every fenced environment and settle the ones whose terminate the
    /// provider confirmed. Lease expiry alone never frees an environment: one
    /// whose terminate fails stays fenced (`Draining`) for the next pass.
    pub async fn reclaim(&self) -> ReclaimSummary {
        let mut summary = ReclaimSummary::default();
        match self.dispatcher.reclaim_ledger() {
            Ok(r) => {
                summary.dispatchers = r.dispatchers.len();
                summary.leases = r.leases;
                summary.invocations = r.invocations;
                summary.fenced = r.fenced.len();
            }
            Err(e) => summary.error = Some(format!("ledger: {e}")),
        }
        let fenced = match self.repos.slots.list_fenced() {
            Ok(v) => v,
            Err(e) => {
                summary.error = Some(format!("ledger: {e}"));
                return summary;
            }
        };
        for env in fenced {
            // Sampled while the provider can still see the VMM (on Firecracker
            // its cgroup, whichever process started it).
            let host_sample = sample_before_terminate(self.provider.as_ref(), &env.id).await;
            let teardown_started = std::time::Instant::now();
            match self
                .provider
                .terminate_environment(&env.id, TerminateReason::Reconcile)
                .await
            {
                Ok(done) => {
                    // Metered before the row is settled: a crash in between
                    // leaves it fenced, the next pass terminates again and
                    // emits the same event id, which the ledger drops.
                    record_reclaimed_stop(
                        &self.repos,
                        self.usage.as_ref(),
                        ReclaimedStop {
                            env: &env,
                            stopped_by: StoppedBy::Reclaim,
                            host_sample,
                            teardown: Some(teardown_started.elapsed()),
                            ended_at: Some(self.clock.now()),
                            now: self.clock.now(),
                        },
                    )
                    .await;
                    match self
                        .repos
                        .slots
                        .confirm_terminated(&env.id, env.epoch, self.clock.now())
                    {
                        Ok(true) => {
                            summary.terminated += 1;
                            tracing::info!(
                                environment_id = %env.id,
                                owner = ?env.owner,
                                was_running = done.was_running,
                                "fenced environment terminated and settled"
                            );
                        }
                        Ok(false) => {}
                        Err(e) => {
                            summary.pending += 1;
                            tracing::warn!(error = %e, environment_id = %env.id, "cannot settle a fenced environment");
                        }
                    }
                }
                Err(e) => {
                    summary.pending += 1;
                    tracing::warn!(
                        error = %e,
                        environment_id = %env.id,
                        "terminating a fenced environment failed; it stays fenced"
                    );
                }
            }
        }
        summary
    }

    /// Dispatchers other than this one that are still live.
    fn live_foreign_owners(&self) -> BTreeSet<DispatcherId> {
        match self.repos.slots.list_dispatchers() {
            Ok(records) => records
                .into_iter()
                .filter(|d| d.is_live() && &d.id != self.dispatcher.id())
                .map(|d| d.id)
                .collect(),
            // Without the list nothing may be treated as ours to reclaim.
            Err(e) => {
                tracing::warn!(error = %e, "cannot list dispatchers");
                BTreeSet::new()
            }
        }
    }

    fn is_foreign(&self, env: &ExecutionEnvironment, live: &BTreeSet<DispatcherId>) -> bool {
        env.owner.as_ref().is_some_and(|o| live.contains(o))
    }

    /// The most recent pass, if one ran.
    pub fn last_report(&self) -> Option<ReconcileReport> {
        self.last.lock().clone()
    }

    /// Run one pass: adopt what this gateway owns, terminate everything else
    /// the provider still runs, mark environments the provider no longer
    /// knows as `Lost`, and record the summary.
    pub async fn reconcile(&self) -> ReconcileReport {
        let mut report = ReconcileReport {
            reclaim: self.reclaim().await,
            ..ReconcileReport::default()
        };
        // The ledger is read first: an environment this gateway knows as
        // active belongs to a live invocation or to the pool and must never
        // be terminated here. Without that snapshot nothing may be
        // terminated at all.
        let known = match self.repos.environments.list_active() {
            Ok(envs) => envs,
            Err(e) => {
                report.error = Some(format!("ledger: {e}"));
                return self.finish(report);
            }
        };
        let live = self.live_foreign_owners();
        // Fenced environments belong to the reclaim above; environments of a
        // live dispatcher belong to that dispatcher. Neither is judged here.
        let (skipped, known): (Vec<_>, Vec<_>) = known
            .into_iter()
            .partition(|e| e.is_fenced() || self.is_foreign(e, &live));
        let skipped: BTreeSet<EnvironmentId> = skipped.into_iter().map(|e| e.id).collect();
        let known_by_id: BTreeMap<EnvironmentId, ExecutionEnvironment> =
            known.iter().map(|e| (e.id.clone(), e.clone())).collect();
        let listed = match self.provider.list_environments().await {
            Ok(ids) => ids,
            Err(e) => {
                report.error = Some(e.to_string());
                return self.finish(report);
            }
        };
        report.found = listed.len();
        let mut seen: BTreeSet<EnvironmentId> = BTreeSet::new();
        for id in listed {
            seen.insert(id.clone());
            if skipped.contains(&id) {
                report.foreign += 1;
                continue;
            }
            // Not in the snapshot: another dispatcher may have recorded it
            // since. Read it again before treating it as an orphan.
            if !known_by_id.contains_key(&id)
                && let Ok(Some(env)) = self.repos.environments.get(&id)
                && (env.is_fenced() || self.is_foreign(&env, &self.live_foreign_owners()))
            {
                report.foreign += 1;
                continue;
            }
            let disowned = match known_by_id.get(&id) {
                Some(env) if self.owns(env).await => {
                    report.adopted += 1;
                    tracing::debug!(
                        environment_id = %id,
                        epoch = env.epoch,
                        state = env.state.name(),
                        "environment adopted"
                    );
                    continue;
                }
                // Known by id, but the host is running something else under
                // it: not ours, so reclaim it like any other orphan and take
                // the row out of the active set.
                Some(env) => {
                    report.disowned += 1;
                    tracing::warn!(
                        environment_id = %id,
                        epoch = env.epoch,
                        recorded_host_pid = ?env.evidence.host_pid,
                        "the ledger knows this environment but the host runs another incarnation"
                    );
                    Some(env.clone())
                }
                None => None,
            };
            let host_sample = sample_before_terminate(self.provider.as_ref(), &id).await;
            let teardown_started = std::time::Instant::now();
            match self
                .provider
                .terminate_environment(&id, TerminateReason::Reconcile)
                .await
            {
                Ok(done) => {
                    // The ledger row, when there is one (a disowned row, or one
                    // already terminal whose host process outlived it), is
                    // metered once: a stop its driver already reported has
                    // the same event id. Without a row there is no tenant to
                    // meter against.
                    let row = match &disowned {
                        Some(env) => Some(env.clone()),
                        None => self.repos.environments.get(&id).ok().flatten(),
                    };
                    match &row {
                        Some(env) => {
                            record_reclaimed_stop(
                                &self.repos,
                                self.usage.as_ref(),
                                ReclaimedStop {
                                    env,
                                    stopped_by: StoppedBy::Reconcile,
                                    host_sample,
                                    teardown: Some(teardown_started.elapsed()),
                                    ended_at: Some(self.clock.now()),
                                    now: self.clock.now(),
                                },
                            )
                            .await
                        }
                        None => tracing::warn!(
                            environment_id = %id,
                            "orphan without a ledger row: its host usage cannot be attributed"
                        ),
                    }
                    report.terminated += 1;
                    tracing::info!(
                        environment_id = %id,
                        was_running = done.was_running,
                        cleaned = done.cleaned.len(),
                        "orphaned environment reclaimed"
                    );
                }
                Err(e) => {
                    report.failed += 1;
                    tracing::warn!(
                        error = %e,
                        environment_id = %id,
                        "cannot reclaim orphaned environment"
                    );
                }
            }
            if let Some(env) = disowned {
                self.mark_lost(env, "host runs a different incarnation of this environment");
            }
        }
        // Known to the ledger but gone from the host: nothing left to stop.
        for env in known {
            if seen.contains(&env.id) {
                continue;
            }
            if self.mark_lost(env.clone(), "provider no longer tracks this environment") {
                report.lost += 1;
                // Nobody saw it end and nothing is left to sample: the stop is
                // recorded with every quantity unknown, never estimated.
                record_reclaimed_stop(
                    &self.repos,
                    self.usage.as_ref(),
                    ReclaimedStop {
                        env: &env,
                        stopped_by: StoppedBy::Reconcile,
                        host_sample: None,
                        teardown: None,
                        ended_at: None,
                        now: self.clock.now(),
                    },
                )
                .await;
            }
        }
        self.finish(report)
    }

    /// Does this gateway still own the environment the provider reports?
    ///
    /// The id alone proves nothing: the ledger row and the host process must
    /// be the same incarnation. The boot evidence recorded when the
    /// environment was created carries the host pid, so a host process that
    /// differs from it is a different incarnation. Evidence missing on either
    /// side cannot disprove ownership, and terminating a live environment is
    /// far worse than leaving an orphan for the next pass, so that case
    /// adopts.
    async fn owns(&self, env: &ExecutionEnvironment) -> bool {
        match self.provider.observe_environment(&env.id).await {
            Ok(EnvironmentObservation::Running { host_pid }) => {
                match (env.evidence.host_pid, host_pid) {
                    (Some(recorded), Some(running)) => recorded == running,
                    _ => true,
                }
            }
            // Exited / NotFound: nothing of ours is running under this id.
            Ok(_) => false,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    environment_id = %env.id,
                    "cannot observe an environment; not adopting it"
                );
                false
            }
        }
    }

    fn mark_lost(&self, mut env: ExecutionEnvironment, reason: &str) -> bool {
        if env.mark_lost(reason, self.clock.now()).is_err() {
            return false;
        }
        match self.repos.environments.update(env.clone()) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    environment_id = %env.id,
                    "cannot mark an environment lost"
                );
                false
            }
        }
    }

    fn finish(&self, report: ReconcileReport) -> ReconcileReport {
        match &report.error {
            Some(error) => tracing::warn!(
                provider = self.provider.kind().as_str(),
                error = %error,
                found = report.found,
                adopted = report.adopted,
                terminated = report.terminated,
                disowned = report.disowned,
                failed = report.failed,
                lost = report.lost,
                foreign = report.foreign,
                reclaimed_dispatchers = report.reclaim.dispatchers,
                fenced_terminated = report.reclaim.terminated,
                fenced_pending = report.reclaim.pending,
                "startup reconcile incomplete"
            ),
            None => tracing::info!(
                provider = self.provider.kind().as_str(),
                found = report.found,
                adopted = report.adopted,
                terminated = report.terminated,
                disowned = report.disowned,
                failed = report.failed,
                lost = report.lost,
                foreign = report.foreign,
                reclaimed_dispatchers = report.reclaim.dispatchers,
                fenced_terminated = report.reclaim.terminated,
                fenced_pending = report.reclaim.pending,
                "startup reconcile finished"
            ),
        }
        *self.last.lock() = Some(report.clone());
        report
    }
}

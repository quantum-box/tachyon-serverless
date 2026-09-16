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
//! It is meant to run once, before the listener accepts, so everything the
//! ledger still reports as active belongs to the previous process. An
//! environment of a live invocation of *this* process is recorded in the
//! ledger before `create_environment` is called, so it is always part of the
//! known set and is never terminated here.
//!
//! A pass never fails: a provider that cannot be listed is logged and startup
//! continues.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_domain::{Clock, EnvironmentId, ExecutionEnvironment};
use tachyon_serverless_provider_port::{
    EnvironmentObservation, ExecutionProvider, TerminateReason,
};

use crate::repository::Repositories;

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
    /// Why the pass could not complete. Startup continues either way.
    pub error: Option<String>,
}

/// Reclaims orphaned environments. Lives in the application layer so every
/// provider benefits from it.
pub struct ReconcileService {
    repos: Repositories,
    provider: Arc<dyn ExecutionProvider>,
    clock: Arc<dyn Clock>,
    last: Mutex<Option<ReconcileReport>>,
}

impl ReconcileService {
    pub fn new(
        repos: Repositories,
        provider: Arc<dyn ExecutionProvider>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repos,
            provider,
            clock,
            last: Mutex::new(None),
        }
    }

    /// The most recent pass, if one ran.
    pub fn last_report(&self) -> Option<ReconcileReport> {
        self.last.lock().clone()
    }

    /// Run one pass: adopt what this gateway owns, terminate everything else
    /// the provider still runs, mark environments the provider no longer
    /// knows as `Lost`, and record the summary.
    pub async fn reconcile(&self) -> ReconcileReport {
        let mut report = ReconcileReport::default();
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
            match self
                .provider
                .terminate_environment(&id, TerminateReason::Reconcile)
                .await
            {
                Ok(done) => {
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
            if self.mark_lost(env, "provider no longer tracks this environment") {
                report.lost += 1;
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
                "startup reconcile finished"
            ),
        }
        *self.last.lock() = Some(report.clone());
        report
    }
}

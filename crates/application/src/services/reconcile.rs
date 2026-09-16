//! Startup reconciliation: reclaim environments a previous process left behind.
//!
//! Loading `state.json` settles the *ledger* (see
//! [`crate::repository`]), but the host processes, sockets, drives and
//! workdirs of a gateway that crashed or was killed survive it. This service
//! asks the provider what it still tracks and terminates everything this
//! gateway does not know as active, with
//! [`TerminateReason::Reconcile`](tachyon_serverless_provider_port::TerminateReason::Reconcile)
//! (docs/architecture.md §4, docs/threat-model.md §12 T10).
//!
//! It is meant to run once, before the listener accepts, so everything the
//! ledger still reports as active belongs to the previous process. An
//! environment of a live invocation of *this* process is recorded in the
//! ledger before `create_environment` is called, so it is always part of the
//! known set and is never terminated here.
//!
//! A pass never fails: a provider that cannot be listed is logged and startup
//! continues.

use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_domain::{Clock, EnvironmentId};
use tachyon_serverless_provider_port::{ExecutionProvider, TerminateReason};

use crate::repository::Repositories;

/// Result of one reconcile pass. Also rendered by `GET /readyz`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReconcileReport {
    /// Environments the provider still tracks.
    pub found: usize,
    /// Of those, the ones this gateway knows as active: left running.
    pub adopted: usize,
    /// Orphans terminated with `TerminateReason::Reconcile`.
    pub terminated: usize,
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

    /// Run one pass: terminate the provider's orphans, mark environments the
    /// provider no longer knows as `Lost`, and record the summary.
    pub async fn reconcile(&self) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        // The ledger is read first: an environment this gateway knows as
        // active belongs to a live invocation and must never be terminated
        // here. Without that snapshot nothing may be terminated at all.
        let known = match self.repos.environments.list_active() {
            Ok(envs) => envs,
            Err(e) => {
                report.error = Some(format!("ledger: {e}"));
                return self.finish(report);
            }
        };
        let known_ids: BTreeSet<EnvironmentId> = known.iter().map(|e| e.id.clone()).collect();
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
            if known_ids.contains(&id) {
                report.adopted += 1;
                continue;
            }
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
        }
        // Known to the ledger but gone from the host: nothing left to stop.
        let now = self.clock.now();
        for mut env in known {
            if seen.contains(&env.id) {
                continue;
            }
            if env
                .mark_lost("provider no longer tracks this environment", now)
                .is_err()
            {
                continue;
            }
            match self.repos.environments.update(env.clone()) {
                Ok(()) => report.lost += 1,
                Err(e) => tracing::warn!(
                    error = %e,
                    environment_id = %env.id,
                    "cannot mark a vanished environment lost"
                ),
            }
        }
        self.finish(report)
    }

    fn finish(&self, report: ReconcileReport) -> ReconcileReport {
        match &report.error {
            Some(error) => tracing::warn!(
                provider = self.provider.kind().as_str(),
                error = %error,
                found = report.found,
                adopted = report.adopted,
                terminated = report.terminated,
                failed = report.failed,
                lost = report.lost,
                "startup reconcile incomplete"
            ),
            None => tracing::info!(
                provider = self.provider.kind().as_str(),
                found = report.found,
                adopted = report.adopted,
                terminated = report.terminated,
                failed = report.failed,
                lost = report.lost,
                "startup reconcile finished"
            ),
        }
        *self.last.lock() = Some(report.clone());
        report
    }
}

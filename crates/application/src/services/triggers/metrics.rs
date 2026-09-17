//! Trigger counters for `GET /metrics` (PLT-4641, docs/metrics.md §3.8).
//!
//! Labels are closed sets (no tenant, trigger or event id), so the families
//! stay bounded whatever the number of triggers.

use std::collections::BTreeMap;

use parking_lot::Mutex;

/// `tsls_trigger_cron_fires_total{result}`.
pub const CRON_RESULTS: [&str; 5] = [
    "accepted",
    "already_fired",
    "refused",
    "deferred",
    "inactive",
];
/// `tsls_trigger_cron_missed_runs_total{action}`: late scheduled times the
/// missed-run policy ran or skipped.
pub const MISSED_ACTIONS: [&str; 2] = ["run", "skipped"];
/// `tsls_trigger_webhook_deliveries_total{result}`.
pub const WEBHOOK_RESULTS: [&str; 9] = [
    "accepted",
    "replayed",
    "signature_refused",
    "timestamp_refused",
    "too_large",
    "invalid_event_id",
    "disabled",
    "not_found",
    "refused",
];

#[derive(Debug, Default)]
struct Inner {
    cron: BTreeMap<&'static str, u64>,
    missed: BTreeMap<&'static str, u64>,
    webhook: BTreeMap<&'static str, u64>,
    scheduler_owner: bool,
}

#[derive(Debug, Default)]
pub struct TriggerMetrics {
    inner: Mutex<Inner>,
}

/// A copy for rendering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TriggerMetricsSnapshot {
    pub cron: BTreeMap<&'static str, u64>,
    pub missed: BTreeMap<&'static str, u64>,
    pub webhook: BTreeMap<&'static str, u64>,
    /// Whether this gateway held the scheduler lease on its last pass.
    pub scheduler_owner: bool,
}

impl TriggerMetrics {
    pub fn cron(&self, result: &'static str, n: usize) {
        if n > 0 {
            *self.inner.lock().cron.entry(result).or_default() += n as u64;
        }
    }

    pub fn missed(&self, action: &'static str, n: usize) {
        if n > 0 {
            *self.inner.lock().missed.entry(action).or_default() += n as u64;
        }
    }

    pub fn webhook(&self, result: &'static str) {
        *self.inner.lock().webhook.entry(result).or_default() += 1;
    }

    pub fn scheduler_owner(&self, owner: bool) {
        self.inner.lock().scheduler_owner = owner;
    }

    pub fn snapshot(&self) -> TriggerMetricsSnapshot {
        let inner = self.inner.lock();
        TriggerMetricsSnapshot {
            cron: inner.cron.clone(),
            missed: inner.missed.clone(),
            webhook: inner.webhook.clone(),
            scheduler_owner: inner.scheduler_owner,
        }
    }
}

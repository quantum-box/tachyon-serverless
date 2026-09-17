//! Budget reservation, hard limits, alerts and fail-closed admission
//! (PLT-4643, docs/adr/0016).
//!
//! ```text
//!  admission: static quota ─▶ budget reserve (max charge, CAS on totals) ─▶ queue / capacity
//!                                     │                                         │ granted after waiting:
//!                                     │                                         └─ budget recheck
//!  run ends ─▶ finish(attempt ids, journal mark)
//!  collector ─▶ ledger ─▶ settle: rated actual of those attempts replaces the reservation
//!  crash    ─▶ expire at run deadline + grace: the maximum stays as an unmetered hold
//! ```
//!
//! What the amounts guarantee: a hard limit bounds `reserved + settled +
//! unmetered holds` of the **provisional rating of PLT-4642** (billable
//! segments of `AttemptSettled`, requested vCPU / memory, transfer bytes,
//! invocation count) in one calendar month (UTC), on one region, with the
//! price table of this deployment. It does not cover host cost (idle pool,
//! teardown, boots without an attempt, cgroup CPU), anything the metering does
//! not rate, a price table changed mid-period, or overruns of the computed
//! maximum (settled in full and counted, so the committed amount can exceed
//! the limit by at most the overruns). Nothing is charged to anyone.

pub mod charge;
pub mod config;
pub mod publish;
pub mod store;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_api_types::{BudgetAlert, BudgetReportResponse, BudgetScopeReport};
use tachyon_serverless_domain::{
    Clock, Function, FunctionId, FunctionRevision, InvocationId, TenantId, Timestamp, UsageEvent,
};
use tachyon_serverless_provider_port::Principal;

pub use charge::{MaxCharge, RunBounds, check_table, max_charge};
pub use config::{
    BudgetBook, BudgetConfig, BudgetLimits, FunctionBudget, FunctionBudgetConfig,
    PERIOD_CALENDAR_MONTH_UTC, TenantBudget, TenantBudgetConfig,
};
pub use publish::{BudgetPublisher, PublicationStatus};
pub use store::{
    AlertFired, AlertLimits, BudgetScope, BudgetStore, LimitRefusal, NewReservation,
    ReservationRow, ReservationState, ReserveOutcome, ScopeTotals, Settlement, StoreStats,
    TENANT_SCOPE, Transition,
};

use crate::authz::require_invoke;
use crate::control::{BudgetEntry, ConfigCache};
use crate::error::AppError;
use crate::usage::UsageMeter;
use crate::usage::rating::{GroupBy, rate};

/// `error_type` of a refusal by a hard budget limit.
pub const BUDGET_EXHAUSTED: &str = "Host.BudgetExhausted";
/// `error_type` of a refusal because the budget cannot be known (not
/// delivered, lease expired, price table mismatch, usage collection stalled).
pub const BUDGET_UNKNOWN: &str = "Host.BudgetUnknown";
/// `error_type` of a refusal because the budget store is unavailable.
pub const BUDGET_STORE_UNAVAILABLE: &str = "Host.BudgetStoreUnavailable";

/// The `reason` every budget refusal carries in the error body (next to
/// `quota` and `capacity` of PLT-4634).
pub const REASON: &str = "budget";

pub const NOTICE: &str = "provisional budget: amounts are the PLT-4642 provisional rating, not \
     an invoice; nothing is charged, billing is disabled in this prototype";

/// What the money guarantee covers, as `GET /v1/budget` states it.
pub const GUARANTEE: [&str; 6] = [
    "covers only the provisional rating of PLT-4642: billable segments of AttemptSettled \
     (host-measured), requested vCPU and memory, transfer bytes and the invocation count",
    "a hard limit bounds reserved + settled + unmetered holds; each run reserves the maximum \
     charge its timeouts, resources and size limits allow before it may start",
    "a run measured above its reservation is settled at the measured value (overrun_micros), \
     so committed can exceed the hard limit by at most the overruns",
    "runs whose usage cannot be fully measured keep the unmeasured rest of their reservation as \
     an unmetered hold (never billable, never released below what was measured)",
    "host cost (idle pool, teardown, boots without an attempt, cgroup CPU) is not covered",
    "one calendar month (UTC), one region, one provisional price table; no payment, no invoice",
];

/// Why a budget refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetRefusal {
    Exhausted,
    Unknown,
    StoreUnavailable,
}

impl BudgetRefusal {
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::Exhausted => BUDGET_EXHAUSTED,
            Self::Unknown => BUDGET_UNKNOWN,
            Self::StoreUnavailable => BUDGET_STORE_UNAVAILABLE,
        }
    }

    pub fn from_error_type(raw: &str) -> Option<Self> {
        [Self::Exhausted, Self::Unknown, Self::StoreUnavailable]
            .into_iter()
            .find(|r| r.error_type() == raw)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Exhausted => "budget_exhausted",
            Self::Unknown => "budget_unknown",
            Self::StoreUnavailable => "budget_store_unavailable",
        }
    }
}

/// The finer cause of a refusal (a bounded metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalCause {
    TenantLimit,
    FunctionLimit,
    NotDelivered,
    LeaseExpired,
    PriceTableMismatch,
    CollectorStalled,
    StoreError,
}

impl RefusalCause {
    pub const ALL: [RefusalCause; 7] = [
        Self::TenantLimit,
        Self::FunctionLimit,
        Self::NotDelivered,
        Self::LeaseExpired,
        Self::PriceTableMismatch,
        Self::CollectorStalled,
        Self::StoreError,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TenantLimit => "tenant_hard_limit",
            Self::FunctionLimit => "function_hard_limit",
            Self::NotDelivered => "not_delivered",
            Self::LeaseExpired => "lease_expired",
            Self::PriceTableMismatch => "price_table_mismatch",
            Self::CollectorStalled => "collector_stalled",
            Self::StoreError => "store_unavailable",
        }
    }

    pub fn refusal(&self) -> BudgetRefusal {
        match self {
            Self::TenantLimit | Self::FunctionLimit => BudgetRefusal::Exhausted,
            Self::StoreError => BudgetRefusal::StoreUnavailable,
            _ => BudgetRefusal::Unknown,
        }
    }
}

/// `YYYY-MM` of a UTC timestamp.
pub fn period_of(t: &Timestamp) -> String {
    t.format("%Y-%m").to_string()
}

/// `[start, end)` of a `YYYY-MM` period.
pub fn period_bounds(period: &str) -> Option<(Timestamp, Timestamp)> {
    use chrono::{Datelike, NaiveDate};
    let start = NaiveDate::parse_from_str(&format!("{period}-01"), "%Y-%m-%d").ok()?;
    let next = if start.month() == 12 {
        NaiveDate::from_ymd_opt(start.year() + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(start.year(), start.month() + 1, 1)?
    };
    Some((
        start.and_hms_opt(0, 0, 0)?.and_utc(),
        next.and_hms_opt(0, 0, 0)?.and_utc(),
    ))
}

/// A reservation a run holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetHold {
    pub reservation_id: String,
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub period: String,
    pub amount_micros: u64,
    pub max: MaxCharge,
}

/// Bounds that come from the gateway configuration, not from the revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayBounds {
    pub handshake_timeout_ms: u64,
    pub cancel_grace_ms: u64,
    pub max_response_bytes: u64,
}

/// One run asking for a reservation.
#[derive(Debug, Clone)]
pub struct RunRequest<'a> {
    /// The reservation key: the invocation id for a synchronous invoke, the
    /// invocation id and run number for an asynchronous run.
    pub run_id: String,
    pub function: &'a Function,
    pub revision: &'a FunctionRevision,
    pub invocation_id: &'a InvocationId,
    pub request_bytes: u64,
    /// Admission to the run deadline.
    pub window_ms: u64,
    pub queue_timeout_ms: u64,
    pub run_deadline: Timestamp,
}

/// Result of one settlement pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct SettleReport {
    pub settled: u64,
    pub settled_incomplete: u64,
    pub waiting: u64,
    pub expired: u64,
    pub alerts: u64,
    pub errors: u64,
}

/// Event counters of this process.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct BudgetCounters {
    pub reservations: u64,
    pub refusals: BTreeMap<RefusalCause, u64>,
    pub recheck_refusals: u64,
    pub releases: u64,
    pub settlements: u64,
    pub settlements_incomplete: u64,
    pub expiries: u64,
    pub alerts: BTreeMap<&'static str, u64>,
    pub overrun_micros: u64,
    pub settle_passes: u64,
    pub settle_errors: u64,
}

/// `/readyz` view (operator facts only, no tenant data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BudgetStatus {
    pub enabled: bool,
    /// Whether new invocations can be admitted as far as the budget machinery
    /// (not a particular tenant's limit) is concerned.
    pub accepting: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<&'static str>,
    pub period: String,
    pub price_table_version: String,
    pub store: BudgetStoreStatus,
    pub collector_stalled: bool,
    pub max_unsettled_age_seconds: u64,
    pub oldest_unsettled_age_seconds: Option<u64>,
    pub expiry_grace_seconds: u64,
    pub counters: BudgetCounters,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publication: Option<PublicationStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BudgetStoreStatus {
    pub healthy: bool,
    pub durable: bool,
    pub path: Option<String>,
    pub stats: Option<StoreStats>,
    pub last_error: Option<String>,
}

/// Per-tenant figures for `GET /metrics` (current period).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetMetrics {
    pub enabled: bool,
    pub store_healthy: bool,
    pub collector_stalled: bool,
    pub active_reservations: u64,
    pub finished_unsettled: u64,
    pub oldest_unsettled_age_seconds: Option<u64>,
    /// `(tenant, totals, hard limit)`.
    pub tenants: Vec<(String, ScopeTotals, Option<u64>)>,
    pub counters: BudgetCounters,
}

pub struct BudgetService {
    config: BudgetConfig,
    store: Arc<BudgetStore>,
    meter: Arc<UsageMeter>,
    cache: Arc<ConfigCache>,
    clock: Arc<dyn Clock>,
    bounds: GatewayBounds,
    publisher: Mutex<Option<Arc<BudgetPublisher>>>,
    counters: Mutex<BudgetCounters>,
}

impl std::fmt::Debug for BudgetService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetService")
            .field("enabled", &self.config.enabled)
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

fn limit_cause(scope: BudgetScope) -> RefusalCause {
    match scope {
        BudgetScope::Tenant => RefusalCause::TenantLimit,
        BudgetScope::Function => RefusalCause::FunctionLimit,
    }
}

impl BudgetService {
    /// The store lives at `<data_dir>/usage/budget.db` (`None`: in memory).
    pub fn open(
        config: &BudgetConfig,
        data_dir: Option<&Path>,
        meter: Arc<UsageMeter>,
        cache: Arc<ConfigCache>,
        clock: Arc<dyn Clock>,
        bounds: GatewayBounds,
    ) -> Result<Arc<Self>, String> {
        config.validate()?;
        if config.enabled {
            check_table(meter.price_table())?;
        }
        let store = Arc::new(BudgetStore::open(
            data_dir.map(|d| d.join("usage").join("budget.db")),
        ));
        Ok(Arc::new(Self {
            config: config.clone(),
            store,
            meter,
            cache,
            clock,
            bounds,
            publisher: Mutex::new(None),
            counters: Mutex::new(BudgetCounters::default()),
        }))
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn config(&self) -> &BudgetConfig {
        &self.config
    }

    pub fn store(&self) -> &Arc<BudgetStore> {
        &self.store
    }

    /// The control plane's publication, for `/readyz`.
    pub fn set_publisher(&self, publisher: Arc<BudgetPublisher>) {
        *self.publisher.lock() = Some(publisher);
    }

    fn refuse(&self, cause: RefusalCause, message: String) -> AppError {
        *self.counters.lock().refusals.entry(cause).or_default() += 1;
        tracing::info!(
            cause = cause.as_str(),
            error_type = cause.refusal().error_type(),
            "invocation refused by budget"
        );
        AppError::Budget {
            refusal: cause.refusal(),
            message,
        }
    }

    /// The delivered budget of `tenant`, or the refusal.
    fn delivered(&self, tenant: &TenantId) -> Result<(TenantBudget, u64), AppError> {
        match self.cache.budget(tenant) {
            BudgetEntry::Valid { budget, generation } => {
                let table = self.meter.price_table();
                if budget.price_table_version != table.version || budget.currency != table.currency
                {
                    return Err(self.refuse(
                        RefusalCause::PriceTableMismatch,
                        format!(
                            "the delivered budget is in price table {} ({}), this gateway rates \
                             with {} ({}): new invocations are refused until they agree",
                            budget.price_table_version,
                            budget.currency,
                            table.version,
                            table.currency
                        ),
                    ));
                }
                Ok((budget, generation))
            }
            BudgetEntry::Expired => Err(self.refuse(
                RefusalCause::LeaseExpired,
                "the tenant's budget was not confirmed by the control plane within the auth \
                 lease: new invocations are refused until it is"
                    .into(),
            )),
            BudgetEntry::NotDelivered => Err(self.refuse(
                RefusalCause::NotDelivered,
                "no budget is delivered for this tenant: new invocations are refused (fail \
                 closed)"
                    .into(),
            )),
        }
    }

    /// Whether usage collection is stalled beyond the threshold: a finished
    /// run has waited longer than `max_unsettled_age_seconds` for settlement.
    fn stalled(&self, stats: &StoreStats, now: Timestamp) -> Option<u64> {
        let oldest = stats.oldest_finished_unsettled_at?;
        let age = (now - oldest).num_seconds().max(0) as u64;
        (age >= self.config.max_unsettled_age_seconds).then_some(age)
    }

    /// The maximum charge of a run of `revision` (what [`Self::reserve`]
    /// reserves for it).
    pub fn max_charge_for(
        &self,
        revision: &FunctionRevision,
        request_bytes: u64,
        window_ms: u64,
        queue_timeout_ms: u64,
    ) -> MaxCharge {
        let spec = &revision.spec;
        let exec = &spec.execution;
        max_charge(
            self.meter.price_table(),
            &RunBounds {
                cpu_millis: spec.resources.cpu_millis,
                memory_mib: spec.resources.memory_mib,
                window_ms,
                queue_timeout_ms,
                init_timeout_ms: u64::from(exec.initialization_timeout_seconds) * 1000,
                handshake_timeout_ms: self.bounds.handshake_timeout_ms,
                execution_timeout_ms: u64::from(exec.timeout_seconds) * 1000,
                cancel_grace_ms: self.bounds.cancel_grace_ms,
                request_bytes,
                max_response_bytes: self.bounds.max_response_bytes,
                slack_ms: self.config.reservation_slack_ms,
            },
        )
    }

    /// Reserve the maximum charge of a run before it is admitted. `Ok(None)`
    /// when budgets are not enforced on this gateway.
    pub fn reserve(&self, run: &RunRequest<'_>) -> Result<Option<BudgetHold>, AppError> {
        if !self.config.enabled {
            return Ok(None);
        }
        let tenant = &run.function.tenant_id;
        let (budget, generation) = self.delivered(tenant)?;
        let now = self.clock.now();
        let stats = match self.store.stats() {
            Ok(s) => s,
            Err(e) => {
                return Err(self.refuse(
                    RefusalCause::StoreError,
                    format!("the budget store is unavailable: {e}"),
                ));
            }
        };
        if let Some(age) = self.stalled(&stats, now) {
            return Err(self.refuse(
                RefusalCause::CollectorStalled,
                format!(
                    "usage of a finished run has waited {age} s for collection (limit {} s): the \
                     budget cannot be known, new invocations are refused",
                    self.config.max_unsettled_age_seconds
                ),
            ));
        }
        let max = self.max_charge_for(
            run.revision,
            run.request_bytes,
            run.window_ms,
            run.queue_timeout_ms,
        );
        let period = period_of(&now);
        let request = NewReservation {
            reservation_id: run.run_id.clone(),
            tenant_id: tenant.to_string(),
            function_id: run.function.id.to_string(),
            invocation_id: run.invocation_id.to_string(),
            period: period.clone(),
            amount_micros: max.total_micros,
            expires_at: run.run_deadline
                + chrono::Duration::seconds(self.config.expiry_grace_seconds as i64),
            price_table_version: self.meter.price_table().version.clone(),
            config_generation: generation,
            tenant_limit: budget.limits.hard_limit_micros,
            function_limit: budget
                .function(&run.function.id)
                .and_then(|l| l.hard_limit_micros),
        };
        match self.store.reserve(&request, now) {
            Ok(ReserveOutcome::Reserved)
            | Ok(ReserveOutcome::Exists(ReservationState::Reserved)) => {
                self.counters.lock().reservations += 1;
                Ok(Some(BudgetHold {
                    reservation_id: run.run_id.clone(),
                    tenant_id: tenant.clone(),
                    function_id: run.function.id.clone(),
                    period,
                    amount_micros: max.total_micros,
                    max,
                }))
            }
            Ok(ReserveOutcome::Exists(state)) => Err(AppError::Conflict(format!(
                "run {} already has a {} budget reservation",
                run.run_id,
                state.as_str()
            ))),
            Ok(ReserveOutcome::Refused(r)) => Err(self.refuse(
                limit_cause(r.scope),
                format!(
                    "the {} budget's hard limit ({} micro-units) does not admit this invocation's \
                     maximum charge of {} ({} already committed in {period})",
                    r.scope.as_str(),
                    r.hard_limit_micros,
                    r.requested_micros,
                    r.committed_micros
                ),
            )),
            Err(e) => Err(self.refuse(
                RefusalCause::StoreError,
                format!("the budget store is unavailable: {e}"),
            )),
        }
    }

    /// A queued run was granted capacity: re-check its budget (it may have
    /// expired or been lowered while it waited). `Err` refuses the run.
    pub fn recheck(&self, hold: &BudgetHold) -> Result<(), AppError> {
        let (budget, _) = self.delivered(&hold.tenant_id)?;
        let result = self.store.recheck(
            &hold.reservation_id,
            budget.limits.hard_limit_micros,
            budget
                .function(&hold.function_id)
                .and_then(|l| l.hard_limit_micros),
        );
        match result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(r)) => {
                self.counters.lock().recheck_refusals += 1;
                Err(self.refuse(
                    limit_cause(r.scope),
                    format!(
                        "the {} budget's hard limit ({} micro-units) was lowered while this \
                         invocation waited and no longer admits it ({} committed including it)",
                        r.scope.as_str(),
                        r.hard_limit_micros,
                        r.committed_micros.saturating_add(r.requested_micros)
                    ),
                ))
            }
            Err(e) => Err(self.refuse(
                RefusalCause::StoreError,
                format!("the budget store is unavailable: {e}"),
            )),
        }
    }

    /// Give a reservation back: the run was refused before it started.
    pub fn release(&self, hold: &BudgetHold) {
        match self.store.release(&hold.reservation_id, self.clock.now()) {
            Ok(t) if t.changed => self.counters.lock().releases += 1,
            Ok(_) => {}
            Err(e) => tracing::warn!(
                error = %e,
                reservation_id = %hold.reservation_id,
                "budget release failed; the reservation expires at its deadline instead"
            ),
        }
    }

    fn alert_limits(&self, tenant: &str, function: &str) -> AlertLimits {
        let Ok(tenant) = TenantId::parse(tenant) else {
            return AlertLimits::default();
        };
        match self.cache.budget(&tenant) {
            BudgetEntry::Valid { budget, .. } => AlertLimits {
                function: FunctionId::parse(function)
                    .ok()
                    .and_then(|f| budget.function(&f).cloned()),
                tenant: Some(budget.limits),
            },
            _ => AlertLimits::default(),
        }
    }

    fn note_transition(&self, t: &Transition, incomplete: bool, overrun: u64) {
        let mut c = self.counters.lock();
        if t.changed {
            match t.state {
                ReservationState::Settled if incomplete => c.settlements_incomplete += 1,
                ReservationState::Settled => c.settlements += 1,
                ReservationState::Expired => c.expiries += 1,
                ReservationState::Released => c.releases += 1,
                ReservationState::Reserved => {}
            }
            c.overrun_micros = c.overrun_micros.saturating_add(overrun);
        }
        for a in &t.alerts {
            let scope = if a.scope.is_empty() {
                "tenant"
            } else {
                "function"
            };
            *c.alerts.entry(scope).or_default() += 1;
            tracing::warn!(
                tenant_id = %a.tenant_id,
                function_id = %a.scope,
                period = %a.period,
                threshold_percent = a.threshold_percent,
                soft_limit_micros = a.soft_limit_micros,
                consumed_micros = a.consumed_micros,
                "budget alert: soft limit threshold crossed (alert only, nothing is stopped)"
            );
        }
    }

    /// The run is over (whatever its outcome). `attempts` are the attempts it
    /// emitted `AttemptSettled` for; `complete` is false when the driver could
    /// not vouch for that list (a panic).
    pub fn finish(&self, hold: &BudgetHold, attempts: &[String], complete: bool) {
        let mark = self.meter.journal().head_seq().ok();
        let now = self.clock.now();
        match self
            .store
            .finish(&hold.reservation_id, attempts, complete, mark, now)
        {
            Ok(true) if attempts.is_empty() && complete => {
                // Nothing ran that rating could charge: settle at zero now.
                let alerts = self.alert_limits(hold.tenant_id.as_str(), hold.function_id.as_str());
                match self.store.settle(
                    &hold.reservation_id,
                    Settlement {
                        measured_micros: 0,
                        complete: true,
                    },
                    now,
                    &alerts,
                ) {
                    Ok(t) => self.note_transition(&t, false, 0),
                    Err(e) => {
                        tracing::warn!(error = %e, "budget settlement failed; retried by the collector")
                    }
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(
                error = %e,
                reservation_id = %hold.reservation_id,
                "recording the end of a run in the budget store failed; its reservation expires \
                 at its deadline and is held (fail closed)"
            ),
        }
    }

    /// Settle every finished run whose usage reached the ledger, and expire
    /// runs that never reported back. Runs after each collector pass.
    pub fn settle_ready(&self) -> SettleReport {
        let mut report = SettleReport::default();
        if !self.config.enabled {
            return report;
        }
        let now = self.clock.now();
        let journal = self.meter.journal().status();
        let cursor = journal.healthy.then_some(journal.cursor_seq);
        let head = self.meter.journal().head_seq().ok();
        let table = self.meter.price_table().clone();
        match self.store.finished_unsettled(self.config.settle_batch) {
            Ok(rows) => {
                for row in rows {
                    let events = match self
                        .meter
                        .ledger()
                        .attempt_settled_events(&row.tenant_id, &row.attempts)
                    {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!(error = %e, "usage ledger unavailable for budget settlement");
                            report.errors += 1;
                            continue;
                        }
                    };
                    let found: BTreeSet<String> = events
                        .iter()
                        .filter_map(|e| e.attempt_id.as_ref().map(|a| a.to_string()))
                        .collect();
                    let all_found = row.attempts.iter().all(|a| found.contains(a));
                    // Events not in the ledger are either still in the journal
                    // (wait) or were never journaled (settle what is there).
                    let mark = row.journal_mark.or(head);
                    let journal_passed = match (cursor, mark) {
                        (Some(c), Some(m)) => c >= m,
                        _ => false,
                    };
                    if !all_found && !journal_passed {
                        report.waiting += 1;
                        continue;
                    }
                    let (measured, metered_fully) = rated(&events, &table);
                    let complete =
                        all_found && metered_fully && row.finished_complete.unwrap_or(false);
                    let alerts = self.alert_limits(&row.tenant_id, &row.function_id);
                    match self.store.settle(
                        &row.reservation_id,
                        Settlement {
                            measured_micros: measured,
                            complete,
                        },
                        now,
                        &alerts,
                    ) {
                        Ok(t) => {
                            if t.changed {
                                if complete {
                                    report.settled += 1;
                                } else {
                                    report.settled_incomplete += 1;
                                }
                            }
                            report.alerts += t.alerts.len() as u64;
                            self.note_transition(
                                &t,
                                !complete,
                                measured.saturating_sub(row.reserved_micros),
                            );
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "budget settlement failed");
                            report.errors += 1;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "budget store unavailable for settlement");
                report.errors += 1;
            }
        }
        match self.store.due_for_expiry(now, self.config.settle_batch) {
            Ok(rows) => {
                for row in rows {
                    let alerts = self.alert_limits(&row.tenant_id, &row.function_id);
                    match self.store.expire(&row.reservation_id, now, &alerts) {
                        Ok(t) => {
                            if t.changed {
                                report.expired += 1;
                                tracing::warn!(
                                    reservation_id = %row.reservation_id,
                                    invocation_id = %row.invocation_id,
                                    held_micros = row.reserved_micros,
                                    "budget reservation expired without a report from its run; \
                                     its maximum is kept as an unmetered hold"
                                );
                            }
                            report.alerts += t.alerts.len() as u64;
                            self.note_transition(&t, false, 0);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "budget expiry failed");
                            report.errors += 1;
                        }
                    }
                }
            }
            Err(_) => report.errors += 1,
        }
        let mut c = self.counters.lock();
        c.settle_passes += 1;
        c.settle_errors += report.errors;
        report
    }

    pub fn counters(&self) -> BudgetCounters {
        self.counters.lock().clone()
    }

    /// `/readyz`.
    pub fn status(&self) -> BudgetStatus {
        let now = self.clock.now();
        let stats = self.store.stats();
        let healthy = stats.is_ok();
        let stats = stats.ok();
        let stalled_age = stats.as_ref().and_then(|s| self.stalled(s, now));
        let refusal = if !self.config.enabled {
            None
        } else if !healthy {
            Some(BUDGET_STORE_UNAVAILABLE)
        } else if stalled_age.is_some() {
            Some(BUDGET_UNKNOWN)
        } else {
            None
        };
        let oldest_unsettled_age_seconds = stats
            .as_ref()
            .and_then(|s| s.oldest_finished_unsettled_at)
            .map(|t| (now - t).num_seconds().max(0) as u64);
        BudgetStatus {
            enabled: self.config.enabled,
            accepting: refusal.is_none(),
            refusal,
            period: period_of(&now),
            price_table_version: self.meter.price_table().version.clone(),
            store: BudgetStoreStatus {
                healthy,
                durable: self.store.path().is_some(),
                path: self.store.path().map(|p| p.display().to_string()),
                last_error: self.store.last_error(),
                stats,
            },
            collector_stalled: stalled_age.is_some(),
            max_unsettled_age_seconds: self.config.max_unsettled_age_seconds,
            oldest_unsettled_age_seconds,
            expiry_grace_seconds: self.config.expiry_grace_seconds,
            counters: self.counters(),
            publication: self.publisher.lock().as_ref().map(|p| p.status()),
        }
    }

    /// `GET /metrics` figures of the current period.
    pub fn metrics(&self, max_tenants: usize) -> BudgetMetrics {
        let status = self.status();
        let period = status.period.clone();
        let mut tenants: Vec<(String, ScopeTotals, Option<u64>)> = self
            .store
            .tenant_totals(&period)
            .unwrap_or_default()
            .into_iter()
            .map(|t| {
                let limit = TenantId::parse(&t.tenant_id).ok().and_then(|id| {
                    match self.cache.budget(&id) {
                        BudgetEntry::Valid { budget, .. } => budget.limits.hard_limit_micros,
                        _ => None,
                    }
                });
                (t.tenant_id.clone(), t, limit)
            })
            .collect();
        // Bounded labels: the tenants with the most committed are kept.
        tenants.sort_by(|a, b| {
            b.1.committed()
                .cmp(&a.1.committed())
                .then_with(|| a.0.cmp(&b.0))
        });
        tenants.truncate(max_tenants);
        tenants.sort_by(|a, b| a.0.cmp(&b.0));
        let stats = status.store.stats.clone().unwrap_or_default();
        BudgetMetrics {
            enabled: status.enabled,
            store_healthy: status.store.healthy,
            collector_stalled: status.collector_stalled,
            active_reservations: stats.active_reservations,
            finished_unsettled: stats.finished_unsettled,
            oldest_unsettled_age_seconds: status.oldest_unsettled_age_seconds,
            tenants,
            counters: status.counters,
        }
    }

    /// `GET /v1/budget`: the caller's tenant only.
    pub async fn report(
        &self,
        principal: &Principal,
        period: Option<&str>,
    ) -> Result<BudgetReportResponse, AppError> {
        require_invoke(principal)?;
        // An in-process control plane answers with its latest budgets.
        self.cache.sync_if_authoritative().await;
        let now = self.clock.now();
        let period = match period.filter(|p| !p.is_empty()) {
            Some(p) => p.to_string(),
            None => period_of(&now),
        };
        let Some((start, end)) = period_bounds(&period) else {
            return Err(AppError::InvalidRequest(format!(
                "budget: period must be YYYY-MM, got `{period}`"
            )));
        };
        let tenant = &principal.tenant_id;
        let (budget, generation, config_state) = match self.cache.budget(tenant) {
            BudgetEntry::Valid { budget, generation } => (Some(budget), Some(generation), "valid"),
            BudgetEntry::Expired => (None, None, "expired"),
            BudgetEntry::NotDelivered => (None, None, "not_delivered"),
        };
        let totals = self
            .store
            .totals(tenant.as_str(), &period)
            .map_err(|e| AppError::Budget {
                refusal: BudgetRefusal::StoreUnavailable,
                message: format!("the budget store is unavailable: {e}"),
            })?;
        let alerts = self
            .store
            .alerts(tenant.as_str(), &period)
            .unwrap_or_default();
        let stats = self.store.stats().ok();
        let refusal = if !self.config.enabled {
            None
        } else {
            match (&budget, config_state) {
                (_, "expired") | (_, "not_delivered") => Some(BUDGET_UNKNOWN),
                (Some(b), _) if b.price_table_version != self.meter.price_table().version => {
                    Some(BUDGET_UNKNOWN)
                }
                _ => match &stats {
                    None => Some(BUDGET_STORE_UNAVAILABLE),
                    Some(s) if self.stalled(s, now).is_some() => Some(BUDGET_UNKNOWN),
                    _ => None,
                },
            }
        };
        let scope_report = |scope: &str, limits: Option<&BudgetLimits>| -> BudgetScopeReport {
            let t = totals
                .iter()
                .find(|t| t.scope == scope)
                .cloned()
                .unwrap_or_default();
            let committed = t.committed();
            let hard = limits.and_then(|l| l.hard_limit_micros);
            BudgetScopeReport {
                function_id: (!scope.is_empty()).then(|| scope.to_string()),
                soft_limit_micros: limits.and_then(|l| l.soft_limit_micros),
                alert_thresholds_percent: limits
                    .map(|l| l.alert_thresholds_percent.clone())
                    .unwrap_or_default(),
                hard_limit_micros: hard,
                reserved_micros: t.reserved_micros,
                settled_micros: t.settled_micros,
                unmetered_hold_micros: t.held_micros,
                committed_micros: committed,
                remaining_micros: hard.map(|h| h.saturating_sub(committed)),
                overrun_micros: t.overrun_micros,
                active_reservations: t.active,
                reservations: t.reservations,
                settlements: t.settlements,
                releases: t.releases,
                expiries: t.expiries,
                refusals: t.refusals,
                alerts_fired: alerts
                    .iter()
                    .filter(|a| a.scope == scope)
                    .map(|a| BudgetAlert {
                        threshold_percent: a.threshold_percent,
                        soft_limit_micros: a.soft_limit_micros,
                        consumed_micros: a.consumed_micros,
                        fired_at: a.fired_at,
                    })
                    .collect(),
            }
        };
        let tenant_report = scope_report(TENANT_SCOPE, budget.as_ref().map(|b| &b.limits));
        let mut function_ids: BTreeSet<String> = totals
            .iter()
            .filter(|t| !t.scope.is_empty())
            .map(|t| t.scope.clone())
            .collect();
        if let Some(b) = &budget {
            function_ids.extend(b.functions.iter().map(|f| f.function_id.to_string()));
        }
        let functions = function_ids
            .iter()
            .map(|f| {
                let limits = budget.as_ref().and_then(|b| {
                    b.functions
                        .iter()
                        .find(|x| x.function_id.as_str() == f)
                        .map(|x| &x.limits)
                });
                scope_report(f, limits)
            })
            .collect();
        let table = self.meter.price_table();
        Ok(BudgetReportResponse {
            enabled: self.config.enabled,
            provisional: true,
            billing_enabled: crate::usage::BILLING_ENABLED,
            notice: NOTICE.into(),
            tenant_id: tenant.to_string(),
            period,
            period_kind: self.config.period.clone(),
            period_start: start,
            period_end: end,
            currency: table.currency.clone(),
            price_table_version: table.version.clone(),
            config_state: config_state.into(),
            config_generation: generation,
            admitting: refusal.is_none() && tenant_report.remaining_micros.is_none_or(|r| r > 0),
            refusal: refusal.map(str::to_string),
            tenant: tenant_report,
            functions,
            guarantee: GUARANTEE.iter().map(|g| g.to_string()).collect(),
        })
    }
}

/// Rated charge of a run's `AttemptSettled` events, and whether every rated
/// quantity was measured (nothing unknown or guest-reported).
pub fn rated(events: &[UsageEvent], table: &crate::usage::PriceTable) -> (u64, bool) {
    if events.is_empty() {
        return (0, true);
    }
    let (_, totals) = rate(
        events,
        table,
        GroupBy {
            function: false,
            day: false,
        },
    );
    let fully = totals.unmetered.attempts == 0 && totals.unmetered.bytes == 0;
    (totals.provisional_charges_micros.total, fully)
}

//! Function usage metering: journal → collector → ledger → provisional rating
//! (PLT-4642, docs/adr/0012).
//!
//! ```text
//! driver / pool ──(UsageEvent v2, synchronous)──▶ journal (bounded, durable, chained)
//!                                                     │ collector: read after cursor,
//!                                                     │ deliver, then commit cursor
//!                                                     ▼
//!                                  ledger (dedup by event_id) ──▶ rating (price table vN)
//! ```
//!
//! - Delivery is at-least-once: the cursor moves only after the ledger
//!   committed, so a crash between the two re-delivers the batch and the
//!   ledger's primary key drops the duplicates.
//! - Quantities are host-measured monotonic durations and gateway-counted
//!   bytes. Wall-clock time only places an event on a day; the difference
//!   between the collector's clock and the event's is recorded as skew.
//! - Nothing here bills anyone. `[usage.billing] enabled` cannot be turned on,
//!   and this is not the build billing of tachyon-apps.

pub mod config;
pub mod journal;
pub mod ledger;
pub mod rating;

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_api_types::UsageReportResponse;
use tachyon_serverless_domain::{Clock, FunctionId, TenantId, Timestamp, UsageEvent};
use tachyon_serverless_provider_port::{Principal, UsageSink};

pub use config::{BILLING_ENABLED, BillingConfig, JournalFullPolicy, UsageConfig};
pub use journal::{JournalEntry, JournalLimits, JournalRefusal, JournalStatus, UsageJournal};
pub use ledger::{AcceptReport, LedgerStats, UsageLedger};
pub use rating::{GroupBy, PriceTable};

use crate::authz::require_invoke;
use crate::config::Profile;
use crate::error::AppError;
use crate::local_ports::InMemoryUsageSink;

/// Environment variable naming a crash point of the collector
/// (`collector.after_ledger_commit`). Honoured only under `profile = "dev"`:
/// the process kills itself with `SIGKILL` right after the ledger committed a
/// batch and before the cursor moved, which is the window at-least-once
/// delivery has to survive (`scripts/usage/usage-e2e.sh`).
pub const CRASH_POINT_ENV: &str = "TSLS_USAGE_CRASH_POINT";
pub const CRASH_AFTER_LEDGER_COMMIT: &str = "collector.after_ledger_commit";

/// A file whose presence pauses the collector (`<data_dir>/usage/` + this),
/// honoured only under `profile = "dev"`: the E2E of PLT-4643 stops usage
/// collection with it to prove budget admission fails closed.
pub const COLLECTOR_PAUSE_FILE: &str = "collector.pause";

/// The notice every usage report carries.
pub const PROVISIONAL_NOTICE: &str = "provisional usage estimate: not an invoice, nothing is \
     charged, billing is disabled in this prototype";

/// Longest report range.
pub const MAX_REPORT_DAYS: i64 = 92;

/// Result of one collection (every batch until the journal is drained).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CollectReport {
    pub batches: u64,
    pub read: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub cursor_seq: u64,
}

/// Operator view of the collector.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CollectorStatus {
    pub runs: u64,
    pub last_run_at: Option<Timestamp>,
    pub last_success_at: Option<Timestamp>,
    pub last_error: Option<String>,
    pub delivered: u64,
    pub inserted: u64,
    pub duplicates: u64,
}

/// `/readyz` view of metering (operator facts only, no tenant data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UsageStatus {
    /// Whether new invocations are admitted as far as metering is concerned.
    pub accepting: bool,
    /// Whether they are admitted *metered*.
    pub metered: bool,
    pub policy: &'static str,
    pub billing_enabled: bool,
    pub price_table_version: String,
    pub journal: JournalStatus,
    pub collector: CollectorStatus,
    pub ledger: Option<LedgerStats>,
}

/// How an invocation was admitted by metering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeteringAdmission {
    Metered,
    /// The journal refused, and the dev-only policy accepts anyway: the
    /// events it cannot take are counted as unjournaled.
    Unmetered(JournalRefusal),
}

pub struct UsageMeter {
    config: UsageConfig,
    journal: Arc<UsageJournal>,
    ledger: Arc<UsageLedger>,
    price_table: PriceTable,
    clock: Arc<dyn Clock>,
    collector: Mutex<CollectorStatus>,
    crash_after_ledger_commit: bool,
    /// Dev-only pause flag file (PLT-4643 E2E).
    pause_flag: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for UsageMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageMeter")
            .field("journal", &self.journal)
            .field("ledger", &self.ledger)
            .field("price_table", &self.price_table.version)
            .finish_non_exhaustive()
    }
}

impl UsageMeter {
    /// Journal and ledger under `<data_dir>/usage/` (`None`: in memory).
    pub fn open(
        config: &UsageConfig,
        data_dir: Option<&Path>,
        clock: Arc<dyn Clock>,
        profile: Profile,
    ) -> Result<Arc<Self>, String> {
        config.validate(profile)?;
        let price_table = match &config.price_table {
            Some(path) => PriceTable::load(path)?,
            None => PriceTable::builtin(),
        };
        let dir = data_dir.map(|d| d.join("usage"));
        let journal = Arc::new(UsageJournal::open(
            dir.as_ref().map(|d| d.join("journal.db")),
            config.limits(),
        ));
        let ledger = Arc::new(UsageLedger::open(
            dir.as_ref().map(|d| d.join("ledger.db")),
        )?);
        let crash_after_ledger_commit = profile == Profile::Dev
            && std::env::var(CRASH_POINT_ENV).is_ok_and(|v| v == CRASH_AFTER_LEDGER_COMMIT);
        if crash_after_ledger_commit {
            tracing::warn!(
                crash_point = CRASH_AFTER_LEDGER_COMMIT,
                "usage collector crash point armed (dev profile, test only)"
            );
        }
        let pause_flag = match (&dir, profile) {
            (Some(d), Profile::Dev) => Some(d.join(COLLECTOR_PAUSE_FILE)),
            _ => None,
        };
        Ok(Arc::new(Self {
            config: config.clone(),
            journal,
            ledger,
            price_table,
            clock,
            collector: Mutex::new(CollectorStatus::default()),
            crash_after_ledger_commit,
            pause_flag,
        }))
    }

    pub fn journal(&self) -> &Arc<UsageJournal> {
        &self.journal
    }

    pub fn ledger(&self) -> &Arc<UsageLedger> {
        &self.ledger
    }

    pub fn price_table(&self) -> &PriceTable {
        &self.price_table
    }

    pub fn config(&self) -> &UsageConfig {
        &self.config
    }

    /// Whether a new invocation may start. `Err` is the refusal the caller
    /// answers with (fail closed); `Ok(Unmetered)` only under the dev-only
    /// `accept_unmetered` policy.
    pub fn admit(&self) -> Result<MeteringAdmission, AppError> {
        let refusal = match self.journal.admission() {
            Ok(()) => return Ok(MeteringAdmission::Metered),
            Err(JournalRefusal::Unavailable) if self.journal.probe() => {
                match self.journal.admission() {
                    Ok(()) => return Ok(MeteringAdmission::Metered),
                    Err(r) => r,
                }
            }
            Err(r) => r,
        };
        match self.config.on_journal_full {
            JournalFullPolicy::AcceptUnmetered => {
                tracing::warn!(
                    reason = refusal.as_str(),
                    "usage journal refused; accepting unmetered (dev policy)"
                );
                Ok(MeteringAdmission::Unmetered(refusal))
            }
            JournalFullPolicy::Refuse => Err(AppError::UsageJournal {
                refusal,
                message: match refusal {
                    JournalRefusal::Full => {
                        "the usage journal is full: new invocations are refused until the \
                         collector catches up"
                    }
                    JournalRefusal::Unavailable => {
                        "the usage journal is unavailable: new invocations are refused until it \
                         can be written"
                    }
                }
                .into(),
            }),
        }
    }

    /// Deliver the journal to the ledger until it is drained or a step fails.
    pub fn collect(&self) -> Result<CollectReport, String> {
        let mut status = self.collector.lock();
        status.runs += 1;
        status.last_run_at = Some(self.clock.now());
        if let Some(flag) = self.pause_flag.as_ref().filter(|f| f.exists()) {
            let e = format!(
                "collector paused by {} (dev profile, test only)",
                flag.display()
            );
            status.last_error = Some(e.clone());
            return Err(e);
        }
        let result = self.collect_locked();
        match &result {
            Ok(r) => {
                status.last_success_at = Some(self.clock.now());
                status.last_error = None;
                status.delivered += r.read;
                status.inserted += r.inserted;
                status.duplicates += r.duplicates;
            }
            Err(e) => {
                tracing::warn!(error = %e, "usage collection failed; retried on the next tick");
                status.last_error = Some(e.clone());
            }
        }
        result
    }

    fn collect_locked(&self) -> Result<CollectReport, String> {
        self.journal.probe();
        let mut report = CollectReport::default();
        loop {
            let (cursor, _chain, entries) = self.journal.read_batch(self.config.collect_batch)?;
            report.cursor_seq = cursor;
            let Some(last) = entries.last() else {
                return Ok(report);
            };
            let (last_seq, last_chain) = (last.seq, last.chain.clone());
            let events: Vec<UsageEvent> = entries.into_iter().map(|e| e.event).collect();
            let accepted = self.ledger.accept(&events, self.clock.now())?;
            report.batches += 1;
            report.read += events.len() as u64;
            report.inserted += accepted.inserted;
            report.duplicates += accepted.duplicates;
            if self.crash_after_ledger_commit {
                tracing::error!(
                    last_seq,
                    "usage crash point: SIGKILL after the ledger commit, before the cursor"
                );
                #[cfg(unix)]
                // SAFETY: kill(2) on our own pid; the process ends here.
                unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                }
                // The signal is delivered asynchronously: wait for it rather
                // than racing it with an abort (which would be SIGABRT).
                std::thread::sleep(std::time::Duration::from_secs(10));
                std::process::abort();
            }
            // Only now: a crash before this line re-delivers the batch, and
            // the ledger drops what it already has.
            if !self.journal.commit_cursor(cursor, last_seq, &last_chain)? {
                // Another collector moved it; start again from where it is.
                continue;
            }
            report.cursor_seq = last_seq;
        }
    }

    pub fn status(&self) -> UsageStatus {
        let journal = self.journal.status();
        let metered = journal.healthy && journal.admitting;
        UsageStatus {
            accepting: metered || self.config.on_journal_full == JournalFullPolicy::AcceptUnmetered,
            metered,
            policy: self.config.on_journal_full.as_str(),
            billing_enabled: BILLING_ENABLED,
            price_table_version: self.price_table.version.clone(),
            journal,
            collector: self.collector.lock().clone(),
            ledger: self.ledger.stats().ok(),
        }
    }

    /// The caller's provisional usage report.
    pub fn report(
        &self,
        principal: &Principal,
        query: &UsageQuery,
    ) -> Result<UsageReportResponse, AppError> {
        require_invoke(principal)?;
        let now = self.clock.now();
        let to = query.to.unwrap_or(now);
        // By default the last 31 days, never before the price table applies.
        let from = query.from.unwrap_or_else(|| {
            (to - chrono::Duration::days(31)).max(self.price_table.effective_from)
        });
        if from >= to {
            return Err(AppError::InvalidRequest(
                "usage: `from` must be before `to`".into(),
            ));
        }
        if to - from > chrono::Duration::days(MAX_REPORT_DAYS) {
            return Err(AppError::InvalidRequest(format!(
                "usage: the range must not exceed {MAX_REPORT_DAYS} days"
            )));
        }
        if from < self.price_table.effective_from {
            return Err(AppError::InvalidRequest(format!(
                "usage: `from` is before the price table's effective_from ({}); one report is \
                 rated with one table",
                self.price_table.effective_from.to_rfc3339()
            )));
        }
        let group_by =
            GroupBy::parse(query.group_by.as_deref()).map_err(AppError::InvalidRequest)?;
        let tenant: &TenantId = &principal.tenant_id;
        let events = self
            .ledger
            .events_for(tenant, query.function_id.as_ref(), from, to)
            .map_err(|e| AppError::platform(format!("usage ledger: {e}")))?;
        let (lines, totals) = rating::rate(&events, &self.price_table, group_by);
        Ok(UsageReportResponse {
            provisional: true,
            not_an_invoice: true,
            billing_enabled: BILLING_ENABLED,
            notice: PROVISIONAL_NOTICE.into(),
            tenant_id: tenant.to_string(),
            function_id: query.function_id.as_ref().map(|f| f.to_string()),
            from,
            to,
            group_by: group_by.keys(),
            price_table: self.price_table.info(),
            lines,
            totals,
            unjournaled_events: self.journal.unjournaled_for(tenant.as_str()),
            collected_through: self.collector.lock().last_success_at,
        })
    }
}

/// A report bound: RFC 3339, or a `YYYY-MM-DD` day (its 00:00 UTC).
pub fn parse_report_time(raw: Option<&str>, what: &str) -> Result<Option<Timestamp>, AppError> {
    let Some(raw) = raw.filter(|r| !r.is_empty()) else {
        return Ok(None);
    };
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(Some(t.with_timezone(&chrono::Utc)));
    }
    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| Some(t.and_utc()))
        .ok_or_else(|| {
            AppError::InvalidRequest(format!(
                "usage: `{what}` must be RFC 3339 or YYYY-MM-DD, got `{raw}`"
            ))
        })
}

/// `GET /v1/usage` parameters.
#[derive(Debug, Clone, Default)]
pub struct UsageQuery {
    pub from: Option<Timestamp>,
    pub to: Option<Timestamp>,
    pub group_by: Option<String>,
    pub function_id: Option<FunctionId>,
}

/// The sink the driver and the pool emit into: journal first (synchronously,
/// on disk), then the in-memory view the history API and tests read.
pub struct JournalingUsageSink {
    meter: Arc<UsageMeter>,
    memory: Arc<InMemoryUsageSink>,
}

impl JournalingUsageSink {
    pub fn new(meter: Arc<UsageMeter>, memory: Arc<InMemoryUsageSink>) -> Self {
        Self { meter, memory }
    }
}

#[async_trait]
impl UsageSink for JournalingUsageSink {
    async fn record(&self, event: UsageEvent) {
        // A refusal is logged and counted per tenant by the journal: the
        // event is unmetered, never estimated later.
        let _ = self.meter.journal.append(&event, self.meter.clock.now());
        self.memory.record(event).await;
    }
}

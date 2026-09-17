//! Budget reservations and period totals (PLT-4643, docs/adr/0016 §2).
//!
//! `<data_dir>/usage/budget.db`, a SQLite database of its own next to the
//! usage journal and ledger (in memory when state is not persisted). Not a
//! `state.db` migration: budget state belongs to the usage path, and it keeps
//! the migration numbering of the ledger free.
//!
//! Every operation is one `BEGIN IMMEDIATE` transaction, so two gateways on
//! the same `data_dir` (and many threads of one) serialize on the database:
//!
//! - **reserve** inserts the reservation *and* checks the limits against the
//!   period totals in the same transaction (a compare-and-set on the totals),
//!   so concurrent reservations can never sum above a hard limit;
//! - a reservation is keyed by its run id and moves only forward:
//!   `reserved → settled | released | expired`. Every transition is an
//!   `UPDATE … WHERE state = 'reserved'`; a duplicate or late call changes
//!   nothing and reports what the reservation already is;
//! - the totals move in the same transaction as the row, and their columns
//!   carry `CHECK (… >= 0)`, so a bug that would drive a balance negative
//!   aborts the transaction instead of committing it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;

use tachyon_serverless_domain::Timestamp;

use super::config::BudgetLimits;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS function_budget_reservations (
    reservation_id       TEXT    PRIMARY KEY,
    tenant_id            TEXT    NOT NULL,
    function_id          TEXT    NOT NULL,
    invocation_id        TEXT    NOT NULL,
    period               TEXT    NOT NULL,
    state                TEXT    NOT NULL CHECK (state IN ('reserved', 'settled', 'released', 'expired')),
    reserved_micros      INTEGER NOT NULL CHECK (reserved_micros >= 0),
    settled_micros       INTEGER NOT NULL DEFAULT 0 CHECK (settled_micros >= 0),
    held_micros          INTEGER NOT NULL DEFAULT 0 CHECK (held_micros >= 0),
    overrun_micros       INTEGER NOT NULL DEFAULT 0 CHECK (overrun_micros >= 0),
    price_table_version  TEXT    NOT NULL,
    config_generation    INTEGER NOT NULL,
    created_at           TEXT    NOT NULL,
    expires_at           TEXT    NOT NULL,
    finished_at          TEXT,
    finished_complete    INTEGER,
    attempts             TEXT,
    journal_mark         INTEGER,
    closed_at            TEXT,
    close_reason         TEXT
);
CREATE INDEX IF NOT EXISTS function_budget_reservations_open
    ON function_budget_reservations (state, finished_at);
CREATE INDEX IF NOT EXISTS function_budget_reservations_tenant
    ON function_budget_reservations (tenant_id, period);
CREATE TABLE IF NOT EXISTS function_budget_totals (
    tenant_id        TEXT    NOT NULL,
    scope            TEXT    NOT NULL,
    period           TEXT    NOT NULL,
    reserved_micros  INTEGER NOT NULL DEFAULT 0 CHECK (reserved_micros >= 0),
    settled_micros   INTEGER NOT NULL DEFAULT 0 CHECK (settled_micros >= 0),
    held_micros      INTEGER NOT NULL DEFAULT 0 CHECK (held_micros >= 0),
    overrun_micros   INTEGER NOT NULL DEFAULT 0 CHECK (overrun_micros >= 0),
    active           INTEGER NOT NULL DEFAULT 0 CHECK (active >= 0),
    reservations     INTEGER NOT NULL DEFAULT 0,
    settlements      INTEGER NOT NULL DEFAULT 0,
    releases         INTEGER NOT NULL DEFAULT 0,
    expiries         INTEGER NOT NULL DEFAULT 0,
    refusals         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, scope, period)
);
CREATE TABLE IF NOT EXISTS function_budget_alerts (
    tenant_id          TEXT    NOT NULL,
    scope              TEXT    NOT NULL,
    period             TEXT    NOT NULL,
    threshold_percent  INTEGER NOT NULL,
    soft_limit_micros  INTEGER NOT NULL,
    consumed_micros    INTEGER NOT NULL,
    fired_at           TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, scope, period, threshold_percent)
);
"#;

/// The tenant-wide scope key in the totals table.
pub const TENANT_SCOPE: &str = "";

fn ts(t: &Timestamp) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

fn parse_ts(raw: &str) -> Option<Timestamp> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc))
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Where a reservation is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationState {
    Reserved,
    Settled,
    Released,
    Expired,
}

impl ReservationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Settled => "settled",
            Self::Released => "released",
            Self::Expired => "expired",
        }
    }

    fn parse(raw: &str) -> Self {
        match raw {
            "settled" => Self::Settled,
            "released" => Self::Released,
            "expired" => Self::Expired,
            _ => Self::Reserved,
        }
    }
}

/// Which limit refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetScope {
    Tenant,
    Function,
}

impl BudgetScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Function => "function",
        }
    }
}

/// A hard limit that does not admit the requested amount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LimitRefusal {
    pub scope: BudgetScope,
    pub hard_limit_micros: u64,
    /// Reserved + settled + unmetered holds before this request.
    pub committed_micros: u64,
    pub requested_micros: u64,
}

/// A new reservation.
#[derive(Debug, Clone)]
pub struct NewReservation {
    pub reservation_id: String,
    pub tenant_id: String,
    pub function_id: String,
    pub invocation_id: String,
    pub period: String,
    pub amount_micros: u64,
    pub expires_at: Timestamp,
    pub price_table_version: String,
    pub config_generation: u64,
    pub tenant_limit: Option<u64>,
    pub function_limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveOutcome {
    Reserved,
    /// The run id already has a reservation: nothing changed.
    Exists(ReservationState),
    Refused(LimitRefusal),
}

/// What a settlement found in the usage ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settlement {
    /// Rated (provisional) charge of the host-measured part.
    pub measured_micros: u64,
    /// Every attempt of the run is in the ledger and fully metered. Otherwise
    /// the rest of the reservation stays as an unmetered hold.
    pub complete: bool,
}

/// The outcome of a transition request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    /// The state after the call.
    pub state: ReservationState,
    /// Whether this call made the transition (false: a duplicate / no-op).
    pub changed: bool,
    pub alerts: Vec<AlertFired>,
}

/// An alert threshold crossed for the first time in a period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AlertFired {
    pub tenant_id: String,
    /// `""` for the tenant, the function id otherwise.
    pub scope: String,
    pub period: String,
    pub threshold_percent: u32,
    pub soft_limit_micros: u64,
    pub consumed_micros: u64,
    pub fired_at: Timestamp,
}

/// Limits to evaluate alerts against when a transition moves the totals.
#[derive(Debug, Clone, Default)]
pub struct AlertLimits {
    pub tenant: Option<BudgetLimits>,
    pub function: Option<BudgetLimits>,
}

/// One reservation row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReservationRow {
    pub reservation_id: String,
    pub tenant_id: String,
    pub function_id: String,
    pub invocation_id: String,
    pub period: String,
    pub state: ReservationState,
    pub reserved_micros: u64,
    pub settled_micros: u64,
    pub held_micros: u64,
    pub overrun_micros: u64,
    pub created_at: Option<Timestamp>,
    pub expires_at: Option<Timestamp>,
    pub finished_at: Option<Timestamp>,
    pub finished_complete: Option<bool>,
    pub attempts: Vec<String>,
    pub journal_mark: Option<u64>,
}

/// Totals of one scope in one period.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScopeTotals {
    pub tenant_id: String,
    pub scope: String,
    pub period: String,
    pub reserved_micros: u64,
    pub settled_micros: u64,
    pub held_micros: u64,
    pub overrun_micros: u64,
    pub active: u64,
    pub reservations: u64,
    pub settlements: u64,
    pub releases: u64,
    pub expiries: u64,
    pub refusals: u64,
}

impl ScopeTotals {
    /// What counts against a hard limit.
    pub fn committed(&self) -> u64 {
        self.reserved_micros
            .saturating_add(self.settled_micros)
            .saturating_add(self.held_micros)
    }

    /// What has been consumed (alerts are evaluated on this, not on
    /// reservations that will mostly be released).
    pub fn consumed(&self) -> u64 {
        self.settled_micros.saturating_add(self.held_micros)
    }
}

/// Operator facts of the store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct StoreStats {
    pub active_reservations: u64,
    /// Runs that finished and wait for their usage to be collected.
    pub finished_unsettled: u64,
    pub oldest_finished_unsettled_at: Option<Timestamp>,
}

/// SQLite `busy_timeout` of `budget.db`. A call that finds the database
/// locked by another process is refused (`Host.BudgetStoreUnavailable`)
/// after at most this plus [`crate::sqlite_wait::STORE_WAIT`], whatever the
/// number of concurrent callers (PLT-4646).
pub(crate) const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

pub struct BudgetStore {
    path: Option<PathBuf>,
    /// Taken with [`crate::sqlite_wait::lock_connection`] (bounded). Lock
    /// order: `conn`, then `last_error`.
    conn: Mutex<Option<Connection>>,
    /// Kept apart from the connection so `/readyz` never waits behind a
    /// caller that sits in the busy timeout.
    last_error: Mutex<Option<String>>,
    forced_unavailable: AtomicBool,
}

impl std::fmt::Debug for BudgetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

fn open_connection(path: Option<&Path>) -> Result<Connection, String> {
    let conn = match path {
        Some(p) => {
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir)
                    .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
            }
            Connection::open(p).map_err(|e| format!("cannot open {}: {e}", p.display()))?
        }
        None => Connection::open_in_memory().map_err(|e| e.to_string())?,
    };
    conn.busy_timeout(BUSY_TIMEOUT).map_err(|e| e.to_string())?;
    if path.is_some() {
        let _: String = conn
            .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
            .map_err(|e| e.to_string())?;
    }
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|e| e.to_string())?;
    conn.execute_batch(SCHEMA).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    if let Some(p) = path {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
    }
    Ok(conn)
}

fn totals_row(
    tx: &Transaction<'_>,
    tenant: &str,
    scope: &str,
    period: &str,
) -> rusqlite::Result<ScopeTotals> {
    tx.query_row(
        "SELECT reserved_micros, settled_micros, held_micros, overrun_micros, active, \
         reservations, settlements, releases, expiries, refusals \
         FROM function_budget_totals WHERE tenant_id = ?1 AND scope = ?2 AND period = ?3",
        params![tenant, scope, period],
        |r| {
            Ok(ScopeTotals {
                tenant_id: tenant.to_string(),
                scope: scope.to_string(),
                period: period.to_string(),
                reserved_micros: r.get::<_, i64>(0)? as u64,
                settled_micros: r.get::<_, i64>(1)? as u64,
                held_micros: r.get::<_, i64>(2)? as u64,
                overrun_micros: r.get::<_, i64>(3)? as u64,
                active: r.get::<_, i64>(4)? as u64,
                reservations: r.get::<_, i64>(5)? as u64,
                settlements: r.get::<_, i64>(6)? as u64,
                releases: r.get::<_, i64>(7)? as u64,
                expiries: r.get::<_, i64>(8)? as u64,
                refusals: r.get::<_, i64>(9)? as u64,
            })
        },
    )
    .optional()
    .map(|o| {
        o.unwrap_or_else(|| ScopeTotals {
            tenant_id: tenant.to_string(),
            scope: scope.to_string(),
            period: period.to_string(),
            ..ScopeTotals::default()
        })
    })
}

/// Signed movement of one scope's totals.
#[derive(Debug, Clone, Copy, Default)]
struct Delta {
    reserved: i64,
    settled: i64,
    held: i64,
    overrun: i64,
    active: i64,
    reservations: i64,
    settlements: i64,
    releases: i64,
    expiries: i64,
    refusals: i64,
}

fn apply_delta(
    tx: &Transaction<'_>,
    tenant: &str,
    scope: &str,
    period: &str,
    d: Delta,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO function_budget_totals (tenant_id, scope, period) VALUES (?1, ?2, ?3)",
        params![tenant, scope, period],
    )?;
    // The CHECK constraints abort the transaction if any column would go
    // negative (a double release, a settlement of something not reserved).
    tx.execute(
        "UPDATE function_budget_totals SET \
         reserved_micros = reserved_micros + ?4, settled_micros = settled_micros + ?5, \
         held_micros = held_micros + ?6, overrun_micros = overrun_micros + ?7, \
         active = active + ?8, reservations = reservations + ?9, \
         settlements = settlements + ?10, releases = releases + ?11, \
         expiries = expiries + ?12, refusals = refusals + ?13 \
         WHERE tenant_id = ?1 AND scope = ?2 AND period = ?3",
        params![
            tenant,
            scope,
            period,
            d.reserved,
            d.settled,
            d.held,
            d.overrun,
            d.active,
            d.reservations,
            d.settlements,
            d.releases,
            d.expiries,
            d.refusals
        ],
    )?;
    Ok(())
}

fn fire_alerts(
    tx: &Transaction<'_>,
    tenant: &str,
    scope: &str,
    period: &str,
    limits: Option<&BudgetLimits>,
    now: Timestamp,
) -> rusqlite::Result<Vec<AlertFired>> {
    let Some(limits) = limits else {
        return Ok(Vec::new());
    };
    let Some(soft) = limits.soft_limit_micros else {
        return Ok(Vec::new());
    };
    if limits.alert_thresholds_percent.is_empty() {
        return Ok(Vec::new());
    }
    let consumed = totals_row(tx, tenant, scope, period)?.consumed();
    let mut fired = Vec::new();
    for &p in &limits.alert_thresholds_percent {
        // consumed >= soft × p / 100, in integers.
        if u128::from(consumed) * 100 < u128::from(soft) * u128::from(p) {
            continue;
        }
        let n = tx.execute(
            "INSERT OR IGNORE INTO function_budget_alerts \
             (tenant_id, scope, period, threshold_percent, soft_limit_micros, consumed_micros, fired_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                tenant,
                scope,
                period,
                i64::from(p),
                to_i64(soft),
                to_i64(consumed),
                ts(&now)
            ],
        )?;
        if n == 1 {
            fired.push(AlertFired {
                tenant_id: tenant.to_string(),
                scope: scope.to_string(),
                period: period.to_string(),
                threshold_percent: p,
                soft_limit_micros: soft,
                consumed_micros: consumed,
                fired_at: now,
            });
        }
    }
    Ok(fired)
}

fn read_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ReservationRow> {
    let attempts: Option<String> = r.get(14)?;
    Ok(ReservationRow {
        reservation_id: r.get(0)?,
        tenant_id: r.get(1)?,
        function_id: r.get(2)?,
        invocation_id: r.get(3)?,
        period: r.get(4)?,
        state: ReservationState::parse(&r.get::<_, String>(5)?),
        reserved_micros: r.get::<_, i64>(6)? as u64,
        settled_micros: r.get::<_, i64>(7)? as u64,
        held_micros: r.get::<_, i64>(8)? as u64,
        overrun_micros: r.get::<_, i64>(9)? as u64,
        created_at: parse_ts(&r.get::<_, String>(10)?),
        expires_at: parse_ts(&r.get::<_, String>(11)?),
        finished_at: r
            .get::<_, Option<String>>(12)?
            .as_deref()
            .and_then(parse_ts),
        finished_complete: r.get::<_, Option<i64>>(13)?.map(|v| v != 0),
        attempts: attempts
            .and_then(|a| serde_json::from_str(&a).ok())
            .unwrap_or_default(),
        journal_mark: r.get::<_, Option<i64>>(15)?.map(|v| v as u64),
    })
}

const ROW_COLUMNS: &str = "reservation_id, tenant_id, function_id, invocation_id, period, state, \
     reserved_micros, settled_micros, held_micros, overrun_micros, created_at, expires_at, \
     finished_at, finished_complete, attempts, journal_mark";

fn get_row(tx: &Transaction<'_>, id: &str) -> rusqlite::Result<Option<ReservationRow>> {
    tx.query_row(
        &format!(
            "SELECT {ROW_COLUMNS} FROM function_budget_reservations WHERE reservation_id = ?1"
        ),
        params![id],
        read_row,
    )
    .optional()
}

impl BudgetStore {
    /// Open (or create) the store. Never fails: a store that cannot be
    /// opened is unavailable, and admission refuses until it opens.
    pub fn open(path: Option<PathBuf>) -> Self {
        let (conn, last_error) = match open_connection(path.as_deref()) {
            Ok(c) => (Some(c), None),
            Err(e) => {
                tracing::error!(error = %e, "budget store unavailable; new invocations are refused");
                (None, Some(e))
            }
        };
        Self {
            path,
            conn: Mutex::new(conn),
            last_error: Mutex::new(last_error),
            forced_unavailable: AtomicBool::new(false),
        }
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Test hook: behave as if the database were gone (`true`) or back.
    #[doc(hidden)]
    pub fn force_unavailable(&self, unavailable: bool) {
        self.forced_unavailable.store(unavailable, Ordering::SeqCst);
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().clone()
    }

    fn with_tx<T>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, String> {
        if self.forced_unavailable.load(Ordering::SeqCst) {
            return Err("budget store is unavailable (forced)".into());
        }
        let Some(mut guard) = crate::sqlite_wait::lock_connection(&self.conn) else {
            let msg = crate::sqlite_wait::busy_message("budget store");
            *self.last_error.lock() = Some(msg.clone());
            return Err(msg);
        };
        if guard.is_none() {
            match open_connection(self.path.as_deref()) {
                Ok(c) => {
                    tracing::info!("budget store available again");
                    *guard = Some(c);
                    *self.last_error.lock() = None;
                }
                Err(e) => {
                    *self.last_error.lock() = Some(e.clone());
                    return Err(e);
                }
            }
        }
        let conn = guard.as_mut().expect("opened above");
        let result = (|| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let v = f(&tx)?;
            tx.commit()?;
            Ok(v)
        })();
        match result {
            Ok(v) => {
                *self.last_error.lock() = None;
                Ok(v)
            }
            Err(e) => {
                let e: rusqlite::Error = e;
                let msg = e.to_string();
                if matches!(
                    e.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DiskFull)
                        | Some(rusqlite::ErrorCode::CannotOpen)
                        | Some(rusqlite::ErrorCode::SystemIoFailure)
                        | Some(rusqlite::ErrorCode::ReadOnly)
                ) {
                    *guard = None;
                }
                *self.last_error.lock() = Some(msg.clone());
                Err(msg)
            }
        }
    }

    /// Reserve `amount` for a run, atomically against the hard limits.
    /// Idempotent by `reservation_id`.
    pub fn reserve(&self, r: &NewReservation, now: Timestamp) -> Result<ReserveOutcome, String> {
        self.with_tx(|tx| {
            if let Some(existing) = get_row(tx, &r.reservation_id)? {
                return Ok(ReserveOutcome::Exists(existing.state));
            }
            let tenant = totals_row(tx, &r.tenant_id, TENANT_SCOPE, &r.period)?;
            let function = totals_row(tx, &r.tenant_id, &r.function_id, &r.period)?;
            for (scope, totals, limit) in [
                (BudgetScope::Tenant, &tenant, r.tenant_limit),
                (BudgetScope::Function, &function, r.function_limit),
            ] {
                let Some(limit) = limit else { continue };
                let committed = totals.committed();
                if committed.saturating_add(r.amount_micros) > limit {
                    let refusal = Delta {
                        refusals: 1,
                        ..Delta::default()
                    };
                    apply_delta(tx, &r.tenant_id, TENANT_SCOPE, &r.period, refusal)?;
                    apply_delta(tx, &r.tenant_id, &r.function_id, &r.period, refusal)?;
                    return Ok(ReserveOutcome::Refused(LimitRefusal {
                        scope,
                        hard_limit_micros: limit,
                        committed_micros: committed,
                        requested_micros: r.amount_micros,
                    }));
                }
            }
            tx.execute(
                "INSERT INTO function_budget_reservations \
                 (reservation_id, tenant_id, function_id, invocation_id, period, state, \
                  reserved_micros, price_table_version, config_generation, created_at, expires_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 'reserved', ?6, ?7, ?8, ?9, ?10)",
                params![
                    r.reservation_id,
                    r.tenant_id,
                    r.function_id,
                    r.invocation_id,
                    r.period,
                    to_i64(r.amount_micros),
                    r.price_table_version,
                    to_i64(r.config_generation),
                    ts(&now),
                    ts(&r.expires_at)
                ],
            )?;
            let d = Delta {
                reserved: to_i64(r.amount_micros),
                active: 1,
                reservations: 1,
                ..Delta::default()
            };
            apply_delta(tx, &r.tenant_id, TENANT_SCOPE, &r.period, d)?;
            apply_delta(tx, &r.tenant_id, &r.function_id, &r.period, d)?;
            Ok(ReserveOutcome::Reserved)
        })
    }

    /// Whether the reservation still fits the (possibly lowered) limits.
    /// Used when a queued run is granted capacity.
    pub fn recheck(
        &self,
        reservation_id: &str,
        tenant_limit: Option<u64>,
        function_limit: Option<u64>,
    ) -> Result<Result<ReservationState, LimitRefusal>, String> {
        self.with_tx(|tx| {
            let Some(row) = get_row(tx, reservation_id)? else {
                return Ok(Ok(ReservationState::Released));
            };
            if row.state != ReservationState::Reserved {
                return Ok(Ok(row.state));
            }
            let tenant = totals_row(tx, &row.tenant_id, TENANT_SCOPE, &row.period)?;
            let function = totals_row(tx, &row.tenant_id, &row.function_id, &row.period)?;
            for (scope, totals, limit) in [
                (BudgetScope::Tenant, &tenant, tenant_limit),
                (BudgetScope::Function, &function, function_limit),
            ] {
                // `committed` already includes this reservation.
                if let Some(limit) = limit
                    && totals.committed() > limit
                {
                    return Ok(Err(LimitRefusal {
                        scope,
                        hard_limit_micros: limit,
                        committed_micros: totals.committed().saturating_sub(row.reserved_micros),
                        requested_micros: row.reserved_micros,
                    }));
                }
            }
            Ok(Ok(ReservationState::Reserved))
        })
    }

    /// The run is over: record which attempts it settled and the journal
    /// position their events are at or before. Idempotent.
    pub fn finish(
        &self,
        reservation_id: &str,
        attempts: &[String],
        complete: bool,
        journal_mark: Option<u64>,
        now: Timestamp,
    ) -> Result<bool, String> {
        let attempts = serde_json::to_string(attempts).unwrap_or_else(|_| "[]".into());
        self.with_tx(|tx| {
            let n = tx.execute(
                "UPDATE function_budget_reservations SET finished_at = ?1, finished_complete = ?2, \
                 attempts = ?3, journal_mark = ?4 \
                 WHERE reservation_id = ?5 AND state = 'reserved' AND finished_at IS NULL",
                params![
                    ts(&now),
                    i64::from(complete),
                    attempts,
                    journal_mark.map(to_i64),
                    reservation_id
                ],
            )?;
            Ok(n == 1)
        })
    }

    fn transition(
        &self,
        reservation_id: &str,
        now: Timestamp,
        alerts: &AlertLimits,
        plan: impl FnOnce(&ReservationRow) -> Option<(ReservationState, u64, u64, &'static str)>,
    ) -> Result<Transition, String> {
        self.with_tx(|tx| {
            let Some(row) = get_row(tx, reservation_id)? else {
                return Ok(Transition {
                    state: ReservationState::Released,
                    changed: false,
                    alerts: Vec::new(),
                });
            };
            if row.state != ReservationState::Reserved {
                return Ok(Transition {
                    state: row.state,
                    changed: false,
                    alerts: Vec::new(),
                });
            }
            let Some((to, settled, held, reason)) = plan(&row) else {
                return Ok(Transition {
                    state: row.state,
                    changed: false,
                    alerts: Vec::new(),
                });
            };
            let overrun = settled.saturating_sub(row.reserved_micros);
            let n = tx.execute(
                "UPDATE function_budget_reservations SET state = ?1, settled_micros = ?2, \
                 held_micros = ?3, overrun_micros = ?4, closed_at = ?5, close_reason = ?6 \
                 WHERE reservation_id = ?7 AND state = 'reserved'",
                params![
                    to.as_str(),
                    to_i64(settled),
                    to_i64(held),
                    to_i64(overrun),
                    ts(&now),
                    reason,
                    reservation_id
                ],
            )?;
            if n != 1 {
                return Ok(Transition {
                    state: row.state,
                    changed: false,
                    alerts: Vec::new(),
                });
            }
            let d = Delta {
                reserved: -to_i64(row.reserved_micros),
                settled: to_i64(settled),
                held: to_i64(held),
                overrun: to_i64(overrun),
                active: -1,
                settlements: i64::from(to == ReservationState::Settled),
                releases: i64::from(to == ReservationState::Released),
                expiries: i64::from(to == ReservationState::Expired),
                ..Delta::default()
            };
            apply_delta(tx, &row.tenant_id, TENANT_SCOPE, &row.period, d)?;
            apply_delta(tx, &row.tenant_id, &row.function_id, &row.period, d)?;
            let mut fired = fire_alerts(
                tx,
                &row.tenant_id,
                TENANT_SCOPE,
                &row.period,
                alerts.tenant.as_ref(),
                now,
            )?;
            fired.extend(fire_alerts(
                tx,
                &row.tenant_id,
                &row.function_id,
                &row.period,
                alerts.function.as_ref(),
                now,
            )?);
            Ok(Transition {
                state: to,
                changed: true,
                alerts: fired,
            })
        })
    }

    /// Replace the reservation by what was measured. Never below the measured
    /// charge (an overrun is settled in full); an incomplete measurement keeps
    /// the unmeasured rest of the reservation as an unmetered hold.
    pub fn settle(
        &self,
        reservation_id: &str,
        s: Settlement,
        now: Timestamp,
        alerts: &AlertLimits,
    ) -> Result<Transition, String> {
        self.transition(reservation_id, now, alerts, |row| {
            let held = if s.complete {
                0
            } else {
                row.reserved_micros.saturating_sub(s.measured_micros)
            };
            Some((
                ReservationState::Settled,
                s.measured_micros,
                held,
                if s.complete {
                    "settled"
                } else {
                    "settled_incomplete"
                },
            ))
        })
    }

    /// Give the whole reservation back: the run never started anything.
    /// Refused (no-op) once the run reported that it finished.
    pub fn release(&self, reservation_id: &str, now: Timestamp) -> Result<Transition, String> {
        self.transition(reservation_id, now, &AlertLimits::default(), |row| {
            row.finished_at
                .is_none()
                .then_some((ReservationState::Released, 0, 0, "released"))
        })
    }

    /// Expire one reservation whose run never reported back by its expiry:
    /// the whole maximum becomes an unmetered hold (fail closed).
    pub fn expire(
        &self,
        reservation_id: &str,
        now: Timestamp,
        alerts: &AlertLimits,
    ) -> Result<Transition, String> {
        self.transition(reservation_id, now, alerts, |row| {
            (row.finished_at.is_none() && row.expires_at.is_some_and(|e| e <= now)).then_some((
                ReservationState::Expired,
                0,
                row.reserved_micros,
                "expired",
            ))
        })
    }

    pub fn get(&self, reservation_id: &str) -> Result<Option<ReservationRow>, String> {
        self.with_tx(|tx| get_row(tx, reservation_id))
    }

    fn rows(
        &self,
        sql_where: &str,
        args: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<ReservationRow>, String> {
        self.with_tx(|tx| {
            let mut stmt = tx.prepare(&format!(
                "SELECT {ROW_COLUMNS} FROM function_budget_reservations WHERE {sql_where}"
            ))?;
            let rows = stmt
                .query_map(args, read_row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    /// Finished runs waiting for settlement, oldest first.
    pub fn finished_unsettled(&self, limit: usize) -> Result<Vec<ReservationRow>, String> {
        self.rows(
            "state = 'reserved' AND finished_at IS NOT NULL ORDER BY finished_at LIMIT ?1",
            &[&(limit as i64)],
        )
    }

    /// Unfinished reservations past their expiry.
    pub fn due_for_expiry(
        &self,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<ReservationRow>, String> {
        self.rows(
            "state = 'reserved' AND finished_at IS NULL AND expires_at <= ?1 ORDER BY expires_at LIMIT ?2",
            &[&ts(&now), &(limit as i64)],
        )
    }

    /// Every reservation of a tenant in a period (tests, diagnostics).
    pub fn reservations_of(
        &self,
        tenant: &str,
        period: &str,
    ) -> Result<Vec<ReservationRow>, String> {
        self.rows(
            "tenant_id = ?1 AND period = ?2 ORDER BY created_at",
            &[&tenant, &period],
        )
    }

    /// Totals of the tenant and of each of its functions in `period`.
    pub fn totals(&self, tenant: &str, period: &str) -> Result<Vec<ScopeTotals>, String> {
        self.with_tx(|tx| {
            let mut stmt = tx.prepare(
                "SELECT scope FROM function_budget_totals WHERE tenant_id = ?1 AND period = ?2 \
                 ORDER BY scope",
            )?;
            let scopes = stmt
                .query_map(params![tenant, period], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(stmt);
            let mut out = Vec::with_capacity(scopes.len().max(1));
            if !scopes.iter().any(|s| s == TENANT_SCOPE) {
                out.push(totals_row(tx, tenant, TENANT_SCOPE, period)?);
            }
            for s in scopes {
                out.push(totals_row(tx, tenant, &s, period)?);
            }
            Ok(out)
        })
    }

    /// Tenant-scope totals of every tenant in `period` (metrics).
    pub fn tenant_totals(&self, period: &str) -> Result<Vec<ScopeTotals>, String> {
        self.with_tx(|tx| {
            let mut stmt = tx.prepare(
                "SELECT tenant_id FROM function_budget_totals WHERE scope = '' AND period = ?1 \
                 ORDER BY tenant_id",
            )?;
            let tenants = stmt
                .query_map(params![period], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(stmt);
            tenants
                .iter()
                .map(|t| totals_row(tx, t, TENANT_SCOPE, period))
                .collect()
        })
    }

    pub fn alerts(&self, tenant: &str, period: &str) -> Result<Vec<AlertFired>, String> {
        self.with_tx(|tx| {
            let mut stmt = tx.prepare(
                "SELECT scope, threshold_percent, soft_limit_micros, consumed_micros, fired_at \
                 FROM function_budget_alerts WHERE tenant_id = ?1 AND period = ?2 \
                 ORDER BY scope, threshold_percent",
            )?;
            let rows = stmt
                .query_map(params![tenant, period], |r| {
                    Ok(AlertFired {
                        tenant_id: tenant.to_string(),
                        scope: r.get(0)?,
                        period: period.to_string(),
                        threshold_percent: r.get::<_, i64>(1)? as u32,
                        soft_limit_micros: r.get::<_, i64>(2)? as u64,
                        consumed_micros: r.get::<_, i64>(3)? as u64,
                        fired_at: parse_ts(&r.get::<_, String>(4)?).unwrap_or_default(),
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    pub fn stats(&self) -> Result<StoreStats, String> {
        self.with_tx(|tx| {
            let active: i64 = tx.query_row(
                "SELECT COUNT(*) FROM function_budget_reservations WHERE state = 'reserved'",
                [],
                |r| r.get(0),
            )?;
            let (finished, oldest): (i64, Option<String>) = tx.query_row(
                "SELECT COUNT(*), MIN(finished_at) FROM function_budget_reservations \
                 WHERE state = 'reserved' AND finished_at IS NOT NULL",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok(StoreStats {
                active_reservations: active as u64,
                finished_unsettled: finished as u64,
                oldest_finished_unsettled_at: oldest.as_deref().and_then(parse_ts),
            })
        })
    }

    /// Recompute every total from the reservation rows and compare (tests and
    /// the property test's invariant). `Err` names the first mismatch.
    pub fn verify_totals(&self) -> Result<(), String> {
        let mismatch = self.with_tx(|tx| {
            let mut stmt = tx.prepare(
                "SELECT t.tenant_id, t.scope, t.period, t.reserved_micros, t.settled_micros, \
                 t.held_micros, t.active, \
                 (SELECT COALESCE(SUM(CASE WHEN state = 'reserved' THEN reserved_micros END), 0) \
                    FROM function_budget_reservations r WHERE r.tenant_id = t.tenant_id \
                    AND r.period = t.period AND (t.scope = '' OR r.function_id = t.scope)), \
                 (SELECT COALESCE(SUM(settled_micros), 0) FROM function_budget_reservations r \
                    WHERE r.tenant_id = t.tenant_id AND r.period = t.period \
                    AND (t.scope = '' OR r.function_id = t.scope)), \
                 (SELECT COALESCE(SUM(held_micros), 0) FROM function_budget_reservations r \
                    WHERE r.tenant_id = t.tenant_id AND r.period = t.period \
                    AND (t.scope = '' OR r.function_id = t.scope)), \
                 (SELECT COUNT(*) FROM function_budget_reservations r WHERE state = 'reserved' \
                    AND r.tenant_id = t.tenant_id AND r.period = t.period \
                    AND (t.scope = '' OR r.function_id = t.scope)) \
                 FROM function_budget_totals t",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    let got: [i64; 4] = [r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?];
                    let want: [i64; 4] = [r.get(7)?, r.get(8)?, r.get(9)?, r.get(10)?];
                    Ok((
                        format!(
                            "{}/{}/{}",
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?
                        ),
                        got,
                        want,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows
                .into_iter()
                .find(|(_, got, want)| got != want)
                .map(|(k, got, want)| {
                    format!("totals {k}: stored {got:?} (reserved, settled, held, active) != rows {want:?}")
                }))
        })?;
        match mismatch {
            Some(m) => Err(m),
            None => Ok(()),
        }
    }
}

//! Function usage ledger (PLT-4642, docs/adr/0012 §3).
//!
//! `<data_dir>/usage/ledger.db` in the prototype; in a regional deployment
//! this is the service the collectors of every node deliver to. It accepts
//! events **by `event_id`**: the primary key makes a re-sent event, a replay
//! after a collector restart and a late or out-of-order event all land at
//! most once. It stores facts only — no price, no charge. Rating reads it
//! (`rating.rs`).
//!
//! The tables are `function_usage_*`: this is not, and never feeds, the
//! build billing of tachyon-apps (a different pipeline, docs/adr/0012 §5).

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rusqlite::{Connection, TransactionBehavior, params};
use serde::Serialize;

use tachyon_serverless_domain::{FunctionId, TenantId, Timestamp, UsageEvent};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS function_usage_events (
    event_id            TEXT    PRIMARY KEY,
    schema_version      INTEGER NOT NULL,
    tenant_id           TEXT    NOT NULL,
    function_id         TEXT,
    revision_id         TEXT,
    invocation_id       TEXT,
    attempt_id          TEXT,
    environment_id      TEXT    NOT NULL,
    epoch               INTEGER NOT NULL,
    sequence            INTEGER NOT NULL,
    event_type          TEXT    NOT NULL,
    observed_at         TEXT    NOT NULL,
    received_at         TEXT    NOT NULL,
    wall_clock_skew_ms  INTEGER NOT NULL,
    body                TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS function_usage_events_tenant_time
    ON function_usage_events (tenant_id, observed_at);
CREATE INDEX IF NOT EXISTS function_usage_events_attempt
    ON function_usage_events (attempt_id);
CREATE TABLE IF NOT EXISTS function_usage_ledger_stats (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    accepted    INTEGER NOT NULL,
    duplicates  INTEGER NOT NULL
);
INSERT OR IGNORE INTO function_usage_ledger_stats (id, accepted, duplicates) VALUES (1, 0, 0);
"#;

fn ts(t: &Timestamp) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

/// Result of delivering one batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct AcceptReport {
    /// Events the ledger did not have.
    pub inserted: u64,
    /// Events it already had (same `event_id`): re-sends and replays.
    pub duplicates: u64,
}

/// Totals of the ledger, for the operator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct LedgerStats {
    pub events: u64,
    pub duplicates_ignored: u64,
}

pub struct UsageLedger {
    path: Option<PathBuf>,
    conn: Mutex<Connection>,
    /// Test hook: refuse deliveries as if the ledger service were down.
    unavailable: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for UsageLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageLedger")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl UsageLedger {
    pub fn open(path: Option<PathBuf>) -> Result<Self, String> {
        let conn = match &path {
            Some(p) => {
                if let Some(dir) = p.parent() {
                    std::fs::create_dir_all(dir)
                        .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
                }
                Connection::open(p).map_err(|e| format!("cannot open {}: {e}", p.display()))?
            }
            None => Connection::open_in_memory().map_err(|e| e.to_string())?,
        };
        conn.busy_timeout(crate::sqlite_wait::STORE_WAIT)
            .map_err(|e| e.to_string())?;
        if path.is_some() {
            let _: String = conn
                .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
                .map_err(|e| e.to_string())?;
        }
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(|e| e.to_string())?;
        conn.execute_batch(SCHEMA).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        if let Some(p) = &path {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
        }
        Ok(Self {
            path,
            conn: Mutex::new(conn),
            unavailable: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The connection, waiting at most [`crate::sqlite_wait::STORE_WAIT`]:
    /// while a delivery sits in SQLite's busy timeout (`ledger.db` locked by
    /// another process), the other callers are refused instead of queueing
    /// one busy timeout after the other (PLT-4646). The collector keeps its
    /// cursor and retries on its next tick; `/readyz` shows the error.
    fn connection(&self) -> Result<parking_lot::MutexGuard<'_, Connection>, String> {
        crate::sqlite_wait::lock_connection(&self.conn).ok_or_else(|| {
            format!(
                "usage ledger unavailable: {}",
                crate::sqlite_wait::busy_message("usage ledger")
            )
        })
    }

    #[doc(hidden)]
    pub fn force_unavailable(&self, unavailable: bool) {
        self.unavailable
            .store(unavailable, std::sync::atomic::Ordering::SeqCst);
    }

    /// Accept a batch in one transaction. Idempotent per `event_id`.
    pub fn accept(
        &self,
        events: &[UsageEvent],
        received_at: Timestamp,
    ) -> Result<AcceptReport, String> {
        if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("usage ledger unavailable (forced)".into());
        }
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let mut report = AcceptReport::default();
        {
            let mut stmt = tx
                .prepare(
                    "INSERT OR IGNORE INTO function_usage_events \
                     (event_id, schema_version, tenant_id, function_id, revision_id, invocation_id, \
                      attempt_id, environment_id, epoch, sequence, event_type, observed_at, \
                      received_at, wall_clock_skew_ms, body) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                )
                .map_err(|e| e.to_string())?;
            for e in events {
                let body = serde_json::to_string(e).map_err(|e| e.to_string())?;
                // Informational only: quantities come from monotonic
                // durations, never from the difference of two wall clocks.
                let skew = (received_at - e.observed_at).num_milliseconds();
                let n = stmt
                    .execute(params![
                        e.event_id,
                        e.meter_version,
                        e.tenant_id.as_str(),
                        e.function_id.as_ref().map(|f| f.as_str().to_string()),
                        e.revision_id.as_ref().map(|r| r.as_str().to_string()),
                        e.invocation_id.as_ref().map(|i| i.as_str().to_string()),
                        e.attempt_id.as_ref().map(|a| a.as_str().to_string()),
                        e.environment_id.as_str(),
                        e.epoch as i64,
                        e.sequence as i64,
                        e.event_type.as_str(),
                        ts(&e.observed_at),
                        ts(&received_at),
                        skew,
                        body,
                    ])
                    .map_err(|e| e.to_string())?;
                match n {
                    1 => report.inserted += 1,
                    _ => report.duplicates += 1,
                }
            }
        }
        tx.execute(
            "UPDATE function_usage_ledger_stats SET accepted = accepted + ?1, \
             duplicates = duplicates + ?2 WHERE id = 1",
            params![report.inserted as i64, report.duplicates as i64],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(report)
    }

    /// Events of `tenant` observed in `[from, to)`, optionally of one function.
    pub fn events_for(
        &self,
        tenant: &TenantId,
        function: Option<&FunctionId>,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<UsageEvent>, String> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare(
                "SELECT body FROM function_usage_events \
                 WHERE tenant_id = ?1 AND observed_at >= ?2 AND observed_at < ?3 \
                   AND (?4 IS NULL OR function_id = ?4) \
                 ORDER BY observed_at, event_id",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(
                params![
                    tenant.as_str(),
                    ts(&from),
                    ts(&to),
                    function.map(|f| f.as_str().to_string())
                ],
                |r| r.get::<_, String>(0),
            )
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for body in rows {
            let body = body.map_err(|e| e.to_string())?;
            let event: UsageEvent = serde_json::from_str(&body).map_err(|e| e.to_string())?;
            // Defence in depth: the row's tenant column and the body agree.
            if &event.tenant_id == tenant {
                out.push(event);
            }
        }
        Ok(out)
    }

    /// The `AttemptSettled` events of `attempt_ids` of `tenant` (PLT-4643
    /// budget settlement).
    pub fn attempt_settled_events(
        &self,
        tenant: &str,
        attempt_ids: &[String],
    ) -> Result<Vec<UsageEvent>, String> {
        if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("usage ledger unavailable (forced)".into());
        }
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare(
                "SELECT body FROM function_usage_events \
                 WHERE tenant_id = ?1 AND attempt_id = ?2 AND event_type = 'attempt_settled' \
                 ORDER BY event_id",
            )
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for attempt in attempt_ids {
            let rows = stmt
                .query_map(params![tenant, attempt], |r| r.get::<_, String>(0))
                .map_err(|e| e.to_string())?;
            for body in rows {
                let body = body.map_err(|e| e.to_string())?;
                let event: UsageEvent = serde_json::from_str(&body).map_err(|e| e.to_string())?;
                if event.tenant_id.as_str() == tenant {
                    out.push(event);
                }
            }
        }
        Ok(out)
    }

    /// Wall-clock skew recorded for `event_id` (received − observed), ms.
    pub fn recorded_skew_ms(&self, event_id: &str) -> Option<i64> {
        let conn = self.connection().ok()?;
        conn.query_row(
            "SELECT wall_clock_skew_ms FROM function_usage_events WHERE event_id = ?1",
            params![event_id],
            |r| r.get(0),
        )
        .ok()
    }

    pub fn stats(&self) -> Result<LedgerStats, String> {
        let conn = self.connection()?;
        let events: i64 = conn
            .query_row("SELECT COUNT(*) FROM function_usage_events", [], |r| {
                r.get(0)
            })
            .map_err(|e| e.to_string())?;
        let duplicates: i64 = conn
            .query_row(
                "SELECT duplicates FROM function_usage_ledger_stats WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        Ok(LedgerStats {
            events: events as u64,
            duplicates_ignored: duplicates as u64,
        })
    }
}

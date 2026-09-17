//! Bounded, durable, append-only usage journal (PLT-4642, docs/adr/0012 §2).
//!
//! `<data_dir>/usage/journal.db` (an in-memory SQLite database when the
//! gateway does not persist state). The emitter appends every usage event
//! synchronously, in one `BEGIN IMMEDIATE` transaction with `synchronous =
//! FULL`, at the metering point: an event the gateway reported to anyone has
//! been written to disk first.
//!
//! - **Bound.** Pending (not yet collected) events are bounded by count and by
//!   bytes. The counters live in the same database and are updated in the same
//!   transaction as the row, so a second gateway on the same `data_dir` sees
//!   the same numbers. An append that would cross either bound is refused and
//!   counted as *unjournaled* per tenant, never silently dropped.
//! - **Admission.** New invocations are refused (`usage_journal_full`) once
//!   the pending events are within `admission_headroom_*` of the bound, so
//!   the invocations already admitted still have room for their events.
//! - **Unavailable.** A journal that cannot be opened or written refuses
//!   appends and admissions alike (`usage_journal_unavailable`) until a probe
//!   succeeds again.
//! - **Integrity.** Every row carries `chain = sha256(previous chain, body)`.
//!   The collector recomputes it from the committed cursor and stops at the
//!   first row that does not match (a modified, inserted or deleted row).
//!   Someone who can rewrite the whole file can also rewrite the chain; that
//!   is a residual risk (docs/threat-model.md T36).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

use tachyon_serverless_domain::{Timestamp, UsageEvent};

/// Chain value before the first row.
pub const GENESIS_CHAIN: &str = "genesis";

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS function_usage_journal (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT    NOT NULL,
    tenant_id    TEXT    NOT NULL,
    body         TEXT    NOT NULL,
    body_bytes   INTEGER NOT NULL,
    chain        TEXT    NOT NULL,
    appended_at  TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS function_usage_journal_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version  INTEGER NOT NULL,
    cursor_seq      INTEGER NOT NULL,
    cursor_chain    TEXT    NOT NULL,
    pending_events  INTEGER NOT NULL,
    pending_bytes   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS function_usage_journal_losses (
    tenant_id  TEXT    PRIMARY KEY,
    events     INTEGER NOT NULL
);
INSERT OR IGNORE INTO function_usage_journal_state
    (id, schema_version, cursor_seq, cursor_chain, pending_events, pending_bytes)
    VALUES (1, 1, 0, 'genesis', 0, 0);
"#;

/// Why the journal refused an append or an admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalRefusal {
    /// A bound (events or bytes) is reached.
    Full,
    /// The journal cannot be opened or written.
    Unavailable,
}

impl JournalRefusal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Full => "usage_journal_full",
            Self::Unavailable => "usage_journal_unavailable",
        }
    }
}

/// Bounds of the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct JournalLimits {
    pub max_events: u64,
    pub max_bytes: u64,
    /// New invocations are refused once fewer than this many events are left.
    pub admission_headroom_events: u64,
    /// New invocations are refused once fewer than this many bytes are left.
    pub admission_headroom_bytes: u64,
}

/// One journal row, in append order.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub seq: u64,
    pub event: UsageEvent,
    pub chain: String,
}

/// Operator view of the journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JournalStatus {
    pub healthy: bool,
    pub durable: bool,
    pub path: Option<String>,
    pub pending_events: u64,
    pub pending_bytes: u64,
    pub cursor_seq: u64,
    pub limits: JournalLimits,
    /// Whether new invocations are admitted right now.
    pub admitting: bool,
    /// Events the journal refused (full or unavailable), all tenants.
    pub unjournaled_events: u64,
    pub last_error: Option<String>,
}

struct Inner {
    conn: Option<Connection>,
    last_error: Option<String>,
    /// Refusals while the database could not record them itself.
    lost_unavailable: BTreeMap<String, u64>,
    /// Test hook: behave as if the database were unavailable.
    forced_unavailable: bool,
}

pub struct UsageJournal {
    path: Option<PathBuf>,
    limits: JournalLimits,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for UsageJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageJournal")
            .field("path", &self.path)
            .field("limits", &self.limits)
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
    conn.busy_timeout(Duration::from_secs(5))
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
    if let Some(p) = path {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
    }
    Ok(conn)
}

/// `sha256(previous || "\n" || body)`, hex.
pub fn chain_next(previous: &str, body: &str) -> String {
    let mut h = Sha256::new();
    h.update(previous.as_bytes());
    h.update(b"\n");
    h.update(body.as_bytes());
    hex::encode(h.finalize())
}

fn ts(t: &Timestamp) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

impl UsageJournal {
    /// Open (or create) the journal. `None` keeps it in memory. Never fails:
    /// a journal that cannot be opened is *unavailable* and refuses admission
    /// until [`Self::probe`] opens it.
    pub fn open(path: Option<PathBuf>, limits: JournalLimits) -> Self {
        let (conn, last_error) = match open_connection(path.as_deref()) {
            Ok(c) => (Some(c), None),
            Err(e) => {
                tracing::error!(error = %e, "usage journal unavailable; new invocations are refused");
                (None, Some(e))
            }
        };
        Self {
            path,
            limits,
            inner: Mutex::new(Inner {
                conn,
                last_error,
                lost_unavailable: BTreeMap::new(),
                forced_unavailable: false,
            }),
        }
    }

    pub fn limits(&self) -> JournalLimits {
        self.limits
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Test hook: make every operation behave as if the database were gone
    /// (`true`) or restore it (`false`).
    #[doc(hidden)]
    pub fn force_unavailable(&self, unavailable: bool) {
        self.inner.lock().forced_unavailable = unavailable;
    }

    /// Re-open an unavailable journal. Returns whether it is usable now.
    pub fn probe(&self) -> bool {
        let mut inner = self.inner.lock();
        if inner.forced_unavailable {
            return false;
        }
        if inner.conn.is_some() {
            return true;
        }
        match open_connection(self.path.as_deref()) {
            Ok(c) => {
                tracing::info!("usage journal available again");
                inner.conn = Some(c);
                inner.last_error = None;
                true
            }
            Err(e) => {
                inner.last_error = Some(e);
                false
            }
        }
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, String> {
        let mut inner = self.inner.lock();
        if inner.forced_unavailable {
            return Err("usage journal is unavailable (forced)".into());
        }
        let Some(conn) = inner.conn.as_mut() else {
            return Err(inner
                .last_error
                .clone()
                .unwrap_or_else(|| "usage journal is not open".into()));
        };
        match f(conn) {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                // A disk that is full or a file that went away: drop the
                // connection so the next probe re-opens it.
                if matches!(
                    e.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DiskFull)
                        | Some(rusqlite::ErrorCode::CannotOpen)
                        | Some(rusqlite::ErrorCode::SystemIoFailure)
                        | Some(rusqlite::ErrorCode::ReadOnly)
                ) {
                    inner.conn = None;
                }
                inner.last_error = Some(msg.clone());
                Err(msg)
            }
        }
    }

    fn note_unavailable_loss(&self, tenant: &str) {
        let mut inner = self.inner.lock();
        *inner
            .lost_unavailable
            .entry(tenant.to_string())
            .or_default() += 1;
    }

    /// Append one event. `Ok(seq)` once it is on disk.
    pub fn append(&self, event: &UsageEvent, now: Timestamp) -> Result<u64, JournalRefusal> {
        let tenant = event.tenant_id.to_string();
        let body = match serde_json::to_string(event) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, event_id = %event.event_id, "usage event not serializable");
                self.note_unavailable_loss(&tenant);
                return Err(JournalRefusal::Unavailable);
            }
        };
        let bytes = body.len() as u64;
        let limits = self.limits;
        let result = self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (pending_events, pending_bytes, cursor_chain): (i64, i64, String) = tx.query_row(
                "SELECT pending_events, pending_bytes, cursor_chain \
                 FROM function_usage_journal_state WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let (pending_events, pending_bytes) = (pending_events as u64, pending_bytes as u64);
            if pending_events + 1 > limits.max_events || pending_bytes + bytes > limits.max_bytes {
                tx.execute(
                    "INSERT INTO function_usage_journal_losses (tenant_id, events) VALUES (?1, 1) \
                     ON CONFLICT(tenant_id) DO UPDATE SET events = events + 1",
                    params![tenant],
                )?;
                tx.commit()?;
                return Ok(None);
            }
            let previous: String = tx
                .query_row(
                    "SELECT chain FROM function_usage_journal ORDER BY seq DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(cursor_chain);
            let chain = chain_next(&previous, &body);
            tx.execute(
                "INSERT INTO function_usage_journal \
                 (event_id, tenant_id, body, body_bytes, chain, appended_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![event.event_id, tenant, body, bytes as i64, chain, ts(&now)],
            )?;
            let seq = tx.last_insert_rowid() as u64;
            tx.execute(
                "UPDATE function_usage_journal_state \
                 SET pending_events = pending_events + 1, pending_bytes = pending_bytes + ?1 \
                 WHERE id = 1",
                params![bytes as i64],
            )?;
            tx.commit()?;
            Ok(Some(seq))
        });
        match result {
            Ok(Some(seq)) => Ok(seq),
            Ok(None) => {
                tracing::error!(
                    event_id = %event.event_id,
                    tenant_id = %tenant,
                    max_events = limits.max_events,
                    max_bytes = limits.max_bytes,
                    "usage journal full: event not journaled (counted as unjournaled)"
                );
                Err(JournalRefusal::Full)
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    event_id = %event.event_id,
                    tenant_id = %tenant,
                    "usage journal unavailable: event not journaled (counted as unjournaled)"
                );
                self.note_unavailable_loss(&tenant);
                Err(JournalRefusal::Unavailable)
            }
        }
    }

    /// Whether a new invocation may be admitted: the journal is available and
    /// more than the headroom is left under both bounds.
    pub fn admission(&self) -> Result<(), JournalRefusal> {
        let limits = self.limits;
        let state = self.with_conn(|conn| {
            conn.query_row(
                "SELECT pending_events, pending_bytes FROM function_usage_journal_state WHERE id = 1",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
        });
        match state {
            Err(_) => Err(JournalRefusal::Unavailable),
            Ok((events, bytes)) => {
                let events_left = limits.max_events.saturating_sub(events);
                let bytes_left = limits.max_bytes.saturating_sub(bytes);
                if events_left <= limits.admission_headroom_events
                    || bytes_left <= limits.admission_headroom_bytes
                {
                    Err(JournalRefusal::Full)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Up to `limit` entries after the committed cursor, in order, with the
    /// chain recomputed from the cursor. `Err` names the first row whose chain
    /// does not match (integrity) or the database error.
    pub fn read_batch(&self, limit: usize) -> Result<(u64, String, Vec<JournalEntry>), String> {
        let rows = self
            .with_conn(|conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
                let (cursor_seq, cursor_chain): (i64, String) = tx.query_row(
                    "SELECT cursor_seq, cursor_chain FROM function_usage_journal_state WHERE id = 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                let mut stmt = tx.prepare(
                    "SELECT seq, body, chain FROM function_usage_journal \
                     WHERE seq > ?1 ORDER BY seq LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(params![cursor_seq, limit as i64], |r| {
                        Ok((
                            r.get::<_, i64>(0)? as u64,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                drop(stmt);
                tx.commit()?;
                Ok((cursor_seq as u64, cursor_chain, rows))
            })
            .map_err(|e| format!("usage journal unavailable: {e}"))?;
        let (cursor_seq, cursor_chain, rows) = rows;
        let mut previous = cursor_chain.clone();
        let mut entries = Vec::with_capacity(rows.len());
        for (seq, body, chain) in rows {
            let expected = chain_next(&previous, &body);
            if expected != chain {
                return Err(format!(
                    "usage journal integrity: row {seq} does not continue the chain \
                     (modified, inserted or deleted rows before it)"
                ));
            }
            let event: UsageEvent = serde_json::from_str(&body)
                .map_err(|e| format!("usage journal integrity: row {seq} is not an event: {e}"))?;
            previous = chain.clone();
            entries.push(JournalEntry { seq, event, chain });
        }
        Ok((cursor_seq, cursor_chain, entries))
    }

    /// Move the cursor from `from_seq` to `to_seq` (a compare-and-set: another
    /// collector that moved it first wins) and delete the collected rows.
    /// Returns whether this call moved it.
    pub fn commit_cursor(
        &self,
        from_seq: u64,
        to_seq: u64,
        to_chain: &str,
    ) -> Result<bool, String> {
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (count, bytes): (i64, i64) = tx.query_row(
                "SELECT COUNT(*), COALESCE(SUM(body_bytes), 0) FROM function_usage_journal \
                 WHERE seq > ?1 AND seq <= ?2",
                params![from_seq as i64, to_seq as i64],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let moved = tx.execute(
                "UPDATE function_usage_journal_state \
                 SET cursor_seq = ?1, cursor_chain = ?2, \
                     pending_events = MAX(pending_events - ?3, 0), \
                     pending_bytes = MAX(pending_bytes - ?4, 0) \
                 WHERE id = 1 AND cursor_seq = ?5",
                params![to_seq as i64, to_chain, count, bytes, from_seq as i64],
            )?;
            if moved == 1 {
                tx.execute(
                    "DELETE FROM function_usage_journal WHERE seq <= ?1",
                    params![to_seq as i64],
                )?;
            }
            tx.commit()?;
            Ok(moved == 1)
        })
    }

    /// Events of `tenant` the journal refused (full or unavailable).
    pub fn unjournaled_for(&self, tenant: &str) -> u64 {
        let in_memory = self
            .inner
            .lock()
            .lost_unavailable
            .get(tenant)
            .copied()
            .unwrap_or(0);
        let on_disk = self
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT events FROM function_usage_journal_losses WHERE tenant_id = ?1",
                    params![tenant],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
            })
            .ok()
            .flatten()
            .unwrap_or(0) as u64;
        in_memory + on_disk
    }

    pub fn status(&self) -> JournalStatus {
        let state = self.with_conn(|conn| {
            let (cursor, events, bytes): (i64, i64, i64) = conn.query_row(
                "SELECT cursor_seq, pending_events, pending_bytes \
                 FROM function_usage_journal_state WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let lost: i64 = conn.query_row(
                "SELECT COALESCE(SUM(events), 0) FROM function_usage_journal_losses",
                [],
                |r| r.get(0),
            )?;
            Ok((cursor as u64, events as u64, bytes as u64, lost as u64))
        });
        let admitting = self.admission().is_ok();
        let inner = self.inner.lock();
        let lost_memory: u64 = inner.lost_unavailable.values().sum();
        let (healthy, (cursor_seq, pending_events, pending_bytes, lost_disk)) = match state {
            Ok(s) => (true, s),
            Err(_) => (false, (0, 0, 0, 0)),
        };
        JournalStatus {
            healthy,
            durable: self.path.is_some(),
            path: self.path.as_ref().map(|p| p.display().to_string()),
            pending_events,
            pending_bytes,
            cursor_seq,
            limits: self.limits,
            admitting,
            unjournaled_events: lost_disk + lost_memory,
            last_error: inner.last_error.clone(),
        }
    }
}

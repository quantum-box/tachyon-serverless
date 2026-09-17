//! `<data_dir>/logs/logs.db`: the durable log store (docs/adr/0018).
//!
//! - **Append never waits for IO.** [`LogRepository::append`] takes the queue
//!   mutex for O(1) work and returns. A full queue (`[logs] queue_max_lines`
//!   / `queue_max_bytes`) drops the line, counts it and remembers the loss
//!   per invocation so the writer can store a marker line for it later.
//! - **One writer thread** commits the queue in one `BEGIN IMMEDIATE`
//!   transaction every `flush_interval_ms`, or earlier once `flush_max_lines`
//!   are queued. `synchronous = FULL` in WAL mode: a committed batch
//!   survives a crash or a power loss, the file is never left corrupt, and a
//!   crash loses at most the lines still queued (younger than one flush
//!   interval, or the batch being committed).
//! - **Caps** are enforced by the writer against the counters stored with the
//!   lines, so they hold across restarts: `[limits]
//!   max_log_lines_per_invocation` / `max_log_bytes_per_invocation` per
//!   invocation and `[logs] max_lines_per_attempt` / `max_bytes_per_attempt`
//!   per attempt. The first line over a cap stores one marker line (stream
//!   `platform`, text starting with [`MARKER_PREFIX`]); the rest are only
//!   counted. Markers do not count against the caps, at most
//!   [`MAX_MARKERS_PER_INVOCATION`] per invocation.
//! - **Unavailable.** A batch the database refuses (locked past the busy
//!   timeout, disk full, cannot be opened) is dropped and counted, and its
//!   invocations get a marker once the database answers again. The gateway
//!   keeps running invocations; `/readyz` shows `logs.healthy = false`
//!   without failing readiness.
//! - **Tenant scope.** Every row carries its tenant; every read filters by it.
//! - **Retention.** The writer deletes, per invocation, logs whose last line
//!   is older than `retention_seconds`, then the oldest *terminal*
//!   invocations' logs while the stored line bytes exceed `max_total_bytes`.
//!   Logs of an invocation the ledger still reports as running are never
//!   removed by the size cap.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;

use tachyon_serverless_domain::{
    AttemptId, Clock, EnvironmentId, InvocationId, Limits, LogPhase, LogRecord, LogStream,
    TenantId, Timestamp,
};

use super::LogsConfig;
use crate::repository::{AppendOutcome, InvocationRepository, LogQuery, LogRepository, RepoError};
use crate::sqlite_wait::{busy_message, lock_connection};

/// Schema of `logs.db` (its own numbering, independent of `state.db`).
pub const SCHEMA_VERSION: i64 = 1;

/// Every line the platform writes about a log loss starts with this.
pub const MARKER_PREFIX: &str = "[tachyon] ";

/// Marker lines one invocation may carry (limit and loss markers together).
pub const MAX_MARKERS_PER_INVOCATION: i64 = 16;

/// Why a line was not stored.
pub const DROP_REASONS: &[&str] = &[
    "queue_full",
    "store_unavailable",
    "invocation_limit",
    "attempt_limit",
    "unattributed",
];

/// SQLite busy timeout of the log store: short, a locked `logs.db` only
/// delays the writer thread, never an invocation.
const BUSY_TIMEOUT: Duration = Duration::from_secs(1);
/// How often the writer retries opening a database it could not open.
const REOPEN_EVERY: Duration = Duration::from_secs(5);
/// Invocations whose losses are remembered for a marker; beyond that losses
/// are only counted.
const MAX_LOSS_KEYS: usize = 4096;
const RETENTION_PAGE: usize = 256;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS log_meta (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version  INTEGER NOT NULL,
    stored_lines    INTEGER NOT NULL,
    stored_bytes    INTEGER NOT NULL
);
INSERT OR IGNORE INTO log_meta (id, schema_version, stored_lines, stored_bytes)
    VALUES (1, 1, 0, 0);
CREATE TABLE IF NOT EXISTS log_lines (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT    NOT NULL,
    invocation_id   TEXT    NOT NULL,
    attempt_id      TEXT    NOT NULL,
    environment_id  TEXT    NOT NULL,
    stream          TEXT    NOT NULL,
    phase           TEXT    NOT NULL,
    ts_ns           INTEGER NOT NULL,
    line            TEXT    NOT NULL,
    bytes           INTEGER NOT NULL,
    truncated       INTEGER NOT NULL,
    marker          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS log_lines_by_attempt
    ON log_lines (tenant_id, invocation_id, attempt_id, seq);
CREATE INDEX IF NOT EXISTS log_lines_by_time ON log_lines (ts_ns);
CREATE TABLE IF NOT EXISTS log_invocations (
    tenant_id       TEXT    NOT NULL,
    invocation_id   TEXT    NOT NULL,
    first_seq       INTEGER NOT NULL,
    last_ts_ns      INTEGER NOT NULL,
    lines           INTEGER NOT NULL,
    bytes           INTEGER NOT NULL,
    stored_lines    INTEGER NOT NULL,
    stored_bytes    INTEGER NOT NULL,
    dropped_lines   INTEGER NOT NULL,
    dropped_bytes   INTEGER NOT NULL,
    markers         INTEGER NOT NULL,
    limit_marked    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, invocation_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS log_invocations_by_age ON log_invocations (last_ts_ns);
CREATE INDEX IF NOT EXISTS log_invocations_by_first_seq ON log_invocations (first_seq);
CREATE TABLE IF NOT EXISTS log_attempts (
    tenant_id       TEXT    NOT NULL,
    invocation_id   TEXT    NOT NULL,
    attempt_id      TEXT    NOT NULL,
    lines           INTEGER NOT NULL,
    bytes           INTEGER NOT NULL,
    dropped_lines   INTEGER NOT NULL,
    limit_marked    INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, invocation_id, attempt_id)
) WITHOUT ROWID;
"#;

// ---------------------------------------------------------------------------
// settings, queue, counters
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Settings {
    max_line_bytes: usize,
    invocation_lines: i64,
    invocation_bytes: i64,
    attempt_lines: i64,
    attempt_bytes: i64,
    flush_interval: Duration,
    flush_max_lines: usize,
    queue_max_lines: usize,
    queue_max_bytes: u64,
    retention_seconds: u64,
    max_total_bytes: u64,
    retention_interval: Duration,
    read_flush_wait: Duration,
}

impl Settings {
    fn new(config: &LogsConfig, limits: &Limits) -> Self {
        let clamp = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        Self {
            max_line_bytes: limits.max_log_line_bytes,
            invocation_lines: i64::from(limits.max_log_lines_per_invocation),
            invocation_bytes: clamp(limits.max_log_bytes_per_invocation),
            attempt_lines: i64::from(
                config
                    .max_lines_per_attempt
                    .unwrap_or(limits.max_log_lines_per_invocation),
            ),
            attempt_bytes: clamp(
                config
                    .max_bytes_per_attempt
                    .unwrap_or(limits.max_log_bytes_per_invocation),
            ),
            flush_interval: config.flush_interval(),
            flush_max_lines: config.flush_max_lines,
            queue_max_lines: config.queue_max_lines,
            queue_max_bytes: config.queue_max_bytes,
            retention_seconds: config.retention_seconds,
            max_total_bytes: config.max_total_bytes,
            retention_interval: config.retention_interval(),
            read_flush_wait: config.read_flush_wait(),
        }
    }
}

struct Pending {
    seq: u64,
    record: LogRecord,
    at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LossKey {
    tenant: TenantId,
    invocation: InvocationId,
    reason: &'static str,
}

#[derive(Debug, Clone)]
struct Loss {
    environment_id: EnvironmentId,
    attempt_id: Option<AttemptId>,
    phase: LogPhase,
    lines: u64,
    bytes: u64,
}

#[derive(Default)]
struct Queue {
    items: Vec<Pending>,
    bytes: u64,
    /// Sequence number of the last line accepted.
    enqueued: u64,
    flush_requested: bool,
    shutdown: bool,
    losses: BTreeMap<LossKey, Loss>,
}

fn remember_loss(losses: &mut BTreeMap<LossKey, Loss>, reason: &'static str, record: &LogRecord) {
    let Some(invocation) = record.invocation_id.clone() else {
        return;
    };
    let key = LossKey {
        tenant: record.tenant_id.clone(),
        invocation,
        reason,
    };
    let len = losses.len();
    match losses.get_mut(&key) {
        Some(loss) => {
            loss.lines += 1;
            loss.bytes += record.line.len() as u64;
        }
        None if len < MAX_LOSS_KEYS => {
            losses.insert(
                key,
                Loss {
                    environment_id: record.environment_id.clone(),
                    attempt_id: record.attempt_id.clone(),
                    phase: record.phase,
                    lines: 1,
                    bytes: record.line.len() as u64,
                },
            );
        }
        None => {}
    }
}

#[derive(Default)]
struct Counters {
    written_lines: AtomicU64,
    written_bytes: AtomicU64,
    truncated_lines: AtomicU64,
    markers: AtomicU64,
    dropped: [AtomicU64; 5],
    flushes: AtomicU64,
    flush_failures: AtomicU64,
    last_flush_lag_us: AtomicU64,
    stored_lines: AtomicU64,
    stored_bytes: AtomicU64,
    retention_age_lines: AtomicU64,
    retention_size_lines: AtomicU64,
    retention_invocations: AtomicU64,
    retention_skipped_non_terminal: AtomicU64,
}

fn drop_index(reason: &str) -> usize {
    DROP_REASONS
        .iter()
        .position(|r| *r == reason)
        .expect("a known drop reason")
}

#[derive(Default)]
struct Health {
    writer_open: bool,
    last_flush_failed: bool,
    last_error: Option<String>,
    last_flush_at: Option<Timestamp>,
    last_retention_at: Option<Timestamp>,
}

struct Connections {
    conn: Option<Connection>,
    next_open: Option<Instant>,
}

struct Shared {
    path: PathBuf,
    settings: Settings,
    clock: Arc<dyn Clock>,
    queue: Mutex<Queue>,
    queue_cv: Condvar,
    committed: Mutex<u64>,
    committed_cv: Condvar,
    /// Taken with [`lock_connection`]. The writer thread and retention.
    writer: Mutex<Connections>,
    /// Taken with [`lock_connection`]. Reads only (WAL: never waits for the
    /// writer).
    reader: Mutex<Connections>,
    counters: Counters,
    health: Mutex<Health>,
    ledger: Mutex<Option<Arc<dyn InvocationRepository>>>,
    /// Test hook: every flush first sleeps this long (a slow disk).
    flush_delay_ms: AtomicU64,
}

// ---------------------------------------------------------------------------
// the store
// ---------------------------------------------------------------------------

/// The durable log store. Cheap to share (`Arc`); dropping the last handle
/// flushes the queue and stops the writer thread.
pub struct DurableLogStore {
    shared: Arc<Shared>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for DurableLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableLogStore")
            .field("path", &self.shared.path)
            .finish_non_exhaustive()
    }
}

/// What one retention pass removed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RetentionReport {
    pub age_invocations: u64,
    pub age_lines: u64,
    pub size_invocations: u64,
    pub size_lines: u64,
    /// Invocations the size cap would have removed but the ledger reports as
    /// not terminal (or could not be asked).
    pub skipped_non_terminal: u64,
}

/// Operator facts of the log store, for `/readyz` (`logs`) and
/// `GET /metrics`. No tenant data.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LogStoreStatus {
    pub backend: &'static str,
    pub durable: bool,
    /// `logs.db` is open and the last flush committed. `false` is a degraded
    /// log store: invocations still run, their lines are dropped and counted.
    pub healthy: bool,
    pub path: Option<String>,
    pub last_error: Option<String>,
    pub queued_lines: u64,
    pub queued_bytes: u64,
    pub queue_max_lines: u64,
    pub queue_max_bytes: u64,
    pub stored_lines: u64,
    pub stored_bytes: u64,
    pub max_total_bytes: u64,
    pub retention_seconds: u64,
    pub flush_interval_ms: u64,
    pub last_flush_lag_ms: Option<f64>,
    pub last_flush_at: Option<Timestamp>,
    pub last_retention_at: Option<Timestamp>,
    pub lines_written: u64,
    pub bytes_written: u64,
    pub lines_truncated: u64,
    pub marker_lines: u64,
    pub lines_dropped: BTreeMap<&'static str, u64>,
    pub flushes: u64,
    pub flush_failures: u64,
    pub retention_deleted_lines: BTreeMap<&'static str, u64>,
    pub retention_deleted_invocations: u64,
    pub retention_skipped_non_terminal: u64,
}

/// The metrics view is the status itself.
pub type LogStoreMetrics = LogStoreStatus;

impl LogStoreStatus {
    /// The status of the memory buffer (`[store] backend = "memory"`).
    pub fn memory() -> Self {
        Self {
            backend: "memory",
            durable: false,
            healthy: true,
            path: None,
            last_error: None,
            queued_lines: 0,
            queued_bytes: 0,
            queue_max_lines: 0,
            queue_max_bytes: 0,
            stored_lines: 0,
            stored_bytes: 0,
            max_total_bytes: 0,
            retention_seconds: 0,
            flush_interval_ms: 0,
            last_flush_lag_ms: None,
            last_flush_at: None,
            last_retention_at: None,
            lines_written: 0,
            bytes_written: 0,
            lines_truncated: 0,
            marker_lines: 0,
            lines_dropped: BTreeMap::new(),
            flushes: 0,
            flush_failures: 0,
            retention_deleted_lines: BTreeMap::new(),
            retention_deleted_invocations: 0,
            retention_skipped_non_terminal: 0,
        }
    }
}

fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    if dir.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Create the database file readable by its owner only; SQLite gives
/// `-wal` / `-shm` the same mode. An existing file keeps its mode.
fn create_private_file(path: &Path) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

fn open_writer(path: &Path) -> Result<Connection, String> {
    let io = |e: std::io::Error| format!("cannot create {}: {e}", path.display());
    if let Some(dir) = path.parent() {
        create_private_dir(dir).map_err(io)?;
    }
    create_private_file(path).map_err(io)?;
    let sql = |e: rusqlite::Error| format!("{}: {e}", path.display());
    let conn = Connection::open(path).map_err(sql)?;
    conn.busy_timeout(BUSY_TIMEOUT).map_err(sql)?;
    // Only takes effect before the first table exists (a new file).
    conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")
        .map_err(sql)?;
    let _mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
        .map_err(sql)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(sql)?;
    // User output must not linger in free pages after retention.
    conn.pragma_update(None, "secure_delete", "ON")
        .map_err(sql)?;
    let existing: Option<i64> = conn
        .query_row(
            "SELECT schema_version FROM log_meta WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .ok();
    if let Some(v) = existing
        && v > SCHEMA_VERSION
    {
        return Err(format!(
            "{} has schema version {v}, newer than this binary ({SCHEMA_VERSION}); \
             logs are not written",
            path.display()
        ));
    }
    conn.execute_batch(SCHEMA).map_err(sql)?;
    Ok(conn)
}

fn open_reader(path: &Path) -> Result<Connection, String> {
    let sql = |e: rusqlite::Error| format!("{}: {e}", path.display());
    if !path.exists() {
        return Err(format!("{} does not exist yet", path.display()));
    }
    let conn = Connection::open(path).map_err(sql)?;
    conn.busy_timeout(BUSY_TIMEOUT).map_err(sql)?;
    conn.pragma_update(None, "query_only", "ON").map_err(sql)?;
    Ok(conn)
}

fn stream_str(s: LogStream) -> &'static str {
    match s {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
        LogStream::Platform => "platform",
    }
}

fn phase_str(p: LogPhase) -> &'static str {
    match p {
        LogPhase::Boot => "boot",
        LogPhase::Init => "init",
        LogPhase::Handler => "handler",
        LogPhase::Shutdown => "shutdown",
    }
}

fn ns(t: &Timestamp) -> i64 {
    t.timestamp_nanos_opt().unwrap_or(i64::MAX)
}

impl DurableLogStore {
    pub const DIR_NAME: &'static str = "logs";
    pub const FILE_NAME: &'static str = "logs.db";

    /// Open (or create) `<data_dir>/logs/logs.db` and start the writer
    /// thread. Never fails: a database that cannot be opened is a degraded
    /// store that drops (and counts) lines and retries opening.
    pub fn open(
        data_dir: &Path,
        config: &LogsConfig,
        limits: &Limits,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let path = data_dir.join(Self::DIR_NAME).join(Self::FILE_NAME);
        let settings = Settings::new(config, limits);
        let mut health = Health::default();
        let writer = match open_writer(&path) {
            Ok(c) => {
                health.writer_open = true;
                Some(c)
            }
            Err(e) => {
                tracing::warn!(error = %e, "log store unavailable; invocation logs are dropped until it opens");
                health.last_error = Some(e);
                None
            }
        };
        let reader = writer.as_ref().and_then(|_| open_reader(&path).ok());
        let shared = Arc::new(Shared {
            path,
            settings,
            clock,
            queue: Mutex::new(Queue::default()),
            queue_cv: Condvar::new(),
            committed: Mutex::new(0),
            committed_cv: Condvar::new(),
            writer: Mutex::new(Connections {
                conn: writer,
                next_open: Some(Instant::now() + REOPEN_EVERY),
            }),
            reader: Mutex::new(Connections {
                conn: reader,
                next_open: None,
            }),
            counters: Counters::default(),
            health: Mutex::new(health),
            ledger: Mutex::new(None),
            flush_delay_ms: AtomicU64::new(0),
        });
        shared.load_totals();
        let thread_shared = shared.clone();
        let handle = std::thread::Builder::new()
            .name("tsls-log-writer".into())
            .spawn(move || thread_shared.run_writer())
            .expect("spawn the log writer thread");
        Self {
            shared,
            writer: Mutex::new(Some(handle)),
        }
    }

    /// The database file.
    pub fn path(&self) -> &Path {
        &self.shared.path
    }

    /// The ledger the size cap asks whether an invocation is terminal.
    pub fn set_ledger(&self, ledger: Arc<dyn InvocationRepository>) {
        *self.shared.ledger.lock() = Some(ledger);
    }

    /// Wait (at most `timeout`) until every line queued before this call was
    /// committed or dropped. `true` when it was.
    pub fn flush(&self, timeout: Duration) -> bool {
        self.shared.wait_for_queued(timeout)
    }

    /// Flush what is queued and stop the writer thread. Later appends are
    /// dropped (`store_unavailable`). Idempotent.
    pub fn shutdown(&self) {
        {
            let mut q = self.shared.queue.lock();
            q.shutdown = true;
        }
        self.shared.queue_cv.notify_all();
        if let Some(handle) = self.writer.lock().take() {
            let _ = handle.join();
        }
    }

    /// One retention pass now (the writer also runs one every
    /// `retention_interval_seconds`).
    pub fn run_retention(&self) -> Result<RetentionReport, RepoError> {
        self.shared.run_retention()
    }

    pub fn status(&self) -> LogStoreStatus {
        self.shared.status()
    }

    /// Test hook: make every flush sleep first (a slow disk).
    #[doc(hidden)]
    pub fn set_flush_delay(&self, delay: Duration) {
        self.shared
            .flush_delay_ms
            .store(delay.as_millis() as u64, Ordering::Relaxed);
    }
}

impl Drop for DurableLogStore {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl LogRepository for DurableLogStore {
    fn append(&self, record: LogRecord) -> AppendOutcome {
        self.shared.append(record)
    }

    fn query(&self, tenant: &TenantId, invocation: &InvocationId) -> Result<LogQuery, RepoError> {
        self.shared.query(tenant, invocation)
    }
}

// ---------------------------------------------------------------------------
// shared: append, writer, query, retention
// ---------------------------------------------------------------------------

#[derive(Default)]
struct BatchOutcome {
    written_lines: u64,
    written_bytes: u64,
    truncated_lines: u64,
    markers: u64,
    invocation_limit: u64,
    attempt_limit: u64,
    stored_lines: i64,
    stored_bytes: i64,
}

#[derive(Debug, Clone, Default)]
struct InvocationRow {
    first_seq: i64,
    last_ts_ns: i64,
    lines: i64,
    bytes: i64,
    stored_lines: i64,
    stored_bytes: i64,
    dropped_lines: i64,
    dropped_bytes: i64,
    markers: i64,
    limit_marked: bool,
}

#[derive(Debug, Clone, Default)]
struct AttemptRow {
    lines: i64,
    bytes: i64,
    dropped_lines: i64,
    limit_marked: bool,
}

struct Line<'a> {
    tenant: &'a str,
    invocation: &'a str,
    attempt: &'a str,
    environment: &'a str,
    stream: LogStream,
    phase: LogPhase,
    ts_ns: i64,
    text: &'a str,
    truncated: bool,
    marker: bool,
}

fn insert_line(tx: &rusqlite::Transaction<'_>, line: &Line<'_>) -> rusqlite::Result<i64> {
    tx.execute(
        "INSERT INTO log_lines (tenant_id, invocation_id, attempt_id, environment_id, stream, \
         phase, ts_ns, line, bytes, truncated, marker) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            line.tenant,
            line.invocation,
            line.attempt,
            line.environment,
            stream_str(line.stream),
            phase_str(line.phase),
            line.ts_ns,
            line.text,
            line.text.len() as i64,
            line.truncated,
            line.marker,
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

type InvKey = (String, String);
type AttKey = (String, String, String);

fn load_invocation<'m>(
    tx: &rusqlite::Transaction<'_>,
    cache: &'m mut HashMap<InvKey, InvocationRow>,
    key: &InvKey,
) -> rusqlite::Result<&'m mut InvocationRow> {
    if !cache.contains_key(key) {
        let row = tx
            .query_row(
                "SELECT first_seq, last_ts_ns, lines, bytes, stored_lines, stored_bytes, \
                 dropped_lines, dropped_bytes, markers, limit_marked \
                 FROM log_invocations WHERE tenant_id = ?1 AND invocation_id = ?2",
                params![key.0, key.1],
                |r| {
                    Ok(InvocationRow {
                        first_seq: r.get(0)?,
                        last_ts_ns: r.get(1)?,
                        lines: r.get(2)?,
                        bytes: r.get(3)?,
                        stored_lines: r.get(4)?,
                        stored_bytes: r.get(5)?,
                        dropped_lines: r.get(6)?,
                        dropped_bytes: r.get(7)?,
                        markers: r.get(8)?,
                        limit_marked: r.get(9)?,
                    })
                },
            )
            .optional()?
            .unwrap_or_default();
        cache.insert(key.clone(), row);
    }
    Ok(cache.get_mut(key).expect("just inserted"))
}

fn load_attempt<'m>(
    tx: &rusqlite::Transaction<'_>,
    cache: &'m mut HashMap<AttKey, AttemptRow>,
    key: &AttKey,
) -> rusqlite::Result<&'m mut AttemptRow> {
    if !cache.contains_key(key) {
        let row = tx
            .query_row(
                "SELECT lines, bytes, dropped_lines, limit_marked FROM log_attempts \
                 WHERE tenant_id = ?1 AND invocation_id = ?2 AND attempt_id = ?3",
                params![key.0, key.1, key.2],
                |r| {
                    Ok(AttemptRow {
                        lines: r.get(0)?,
                        bytes: r.get(1)?,
                        dropped_lines: r.get(2)?,
                        limit_marked: r.get(3)?,
                    })
                },
            )
            .optional()?
            .unwrap_or_default();
        cache.insert(key.clone(), row);
    }
    Ok(cache.get_mut(key).expect("just inserted"))
}

/// Record a stored row (a line or a marker) on its invocation row.
fn account_stored(inv: &mut InvocationRow, seq: i64, ts_ns: i64, bytes: i64) {
    if inv.first_seq == 0 {
        inv.first_seq = seq;
    }
    inv.last_ts_ns = inv.last_ts_ns.max(ts_ns);
    inv.stored_lines += 1;
    inv.stored_bytes += bytes;
}

fn loss_text(reason: &str, lines: u64, bytes: u64) -> String {
    let why = match reason {
        "queue_full" => "the log writer queue was full",
        _ => "the log store was unavailable",
    };
    format!(
        "{MARKER_PREFIX}{lines} log line(s) ({bytes} bytes) of this invocation were dropped: {why}"
    )
}

fn write_batch(
    conn: &mut Connection,
    settings: &Settings,
    now: Timestamp,
    batch: &[Pending],
    losses: &BTreeMap<LossKey, Loss>,
) -> rusqlite::Result<BatchOutcome> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut out = BatchOutcome::default();
    let mut invs: HashMap<InvKey, InvocationRow> = HashMap::new();
    let mut atts: HashMap<AttKey, AttemptRow> = HashMap::new();

    for (key, loss) in losses {
        let ikey = (key.tenant.to_string(), key.invocation.to_string());
        let inv = load_invocation(&tx, &mut invs, &ikey)?;
        inv.dropped_lines += loss.lines as i64;
        inv.dropped_bytes += loss.bytes as i64;
        if inv.markers < MAX_MARKERS_PER_INVOCATION {
            let text = loss_text(key.reason, loss.lines, loss.bytes);
            let attempt = loss
                .attempt_id
                .as_ref()
                .map(|a| a.to_string())
                .unwrap_or_default();
            let ts_ns = ns(&now);
            let seq = insert_line(
                &tx,
                &Line {
                    tenant: &ikey.0,
                    invocation: &ikey.1,
                    attempt: &attempt,
                    environment: loss.environment_id.as_str(),
                    stream: LogStream::Platform,
                    phase: loss.phase,
                    ts_ns,
                    text: &text,
                    truncated: false,
                    marker: true,
                },
            )?;
            inv.markers += 1;
            account_stored(inv, seq, ts_ns, text.len() as i64);
            out.markers += 1;
            out.stored_lines += 1;
            out.stored_bytes += text.len() as i64;
        }
    }

    for pending in batch {
        let rec = &pending.record;
        let Some(invocation) = &rec.invocation_id else {
            continue;
        };
        let ikey = (rec.tenant_id.to_string(), invocation.to_string());
        let attempt = rec
            .attempt_id
            .as_ref()
            .map(|a| a.to_string())
            .unwrap_or_default();
        let akey = (ikey.0.clone(), ikey.1.clone(), attempt.clone());
        let bytes = rec.line.len() as i64;
        let ts_ns = ns(&rec.timestamp);
        let base = Line {
            tenant: &ikey.0,
            invocation: &ikey.1,
            attempt: &attempt,
            environment: rec.environment_id.as_str(),
            stream: rec.stream,
            phase: rec.phase,
            ts_ns,
            text: &rec.line,
            truncated: rec.truncated,
            marker: false,
        };

        let inv = load_invocation(&tx, &mut invs, &ikey)?;
        if inv.lines + 1 > settings.invocation_lines
            || inv.bytes + bytes > settings.invocation_bytes
        {
            inv.dropped_lines += 1;
            inv.dropped_bytes += bytes;
            out.invocation_limit += 1;
            if !inv.limit_marked && inv.markers < MAX_MARKERS_PER_INVOCATION {
                let text = format!(
                    "{MARKER_PREFIX}log limit of this invocation reached ({} lines / {} bytes): \
                     further lines are dropped",
                    settings.invocation_lines, settings.invocation_bytes
                );
                let seq = insert_line(
                    &tx,
                    &Line {
                        stream: LogStream::Platform,
                        text: &text,
                        truncated: false,
                        marker: true,
                        ..base
                    },
                )?;
                inv.limit_marked = true;
                inv.markers += 1;
                account_stored(inv, seq, ts_ns, text.len() as i64);
                out.markers += 1;
                out.stored_lines += 1;
                out.stored_bytes += text.len() as i64;
            }
            continue;
        }
        let attempt_over = {
            let att = load_attempt(&tx, &mut atts, &akey)?;
            att.lines + 1 > settings.attempt_lines || att.bytes + bytes > settings.attempt_bytes
        };
        if attempt_over {
            let att = atts.get_mut(&akey).expect("loaded");
            att.dropped_lines += 1;
            let mark = !att.limit_marked;
            let inv = invs.get_mut(&ikey).expect("loaded");
            inv.dropped_lines += 1;
            inv.dropped_bytes += bytes;
            out.attempt_limit += 1;
            if mark && inv.markers < MAX_MARKERS_PER_INVOCATION {
                let text = format!(
                    "{MARKER_PREFIX}log limit of this attempt reached ({} lines / {} bytes): \
                     further lines of this attempt are dropped",
                    settings.attempt_lines, settings.attempt_bytes
                );
                let seq = insert_line(
                    &tx,
                    &Line {
                        stream: LogStream::Platform,
                        text: &text,
                        truncated: false,
                        marker: true,
                        ..base
                    },
                )?;
                inv.markers += 1;
                account_stored(inv, seq, ts_ns, text.len() as i64);
                atts.get_mut(&akey).expect("loaded").limit_marked = true;
                out.markers += 1;
                out.stored_lines += 1;
                out.stored_bytes += text.len() as i64;
            }
            continue;
        }
        let seq = insert_line(&tx, &base)?;
        let inv = invs.get_mut(&ikey).expect("loaded");
        inv.lines += 1;
        inv.bytes += bytes;
        account_stored(inv, seq, ts_ns, bytes);
        let att = atts.get_mut(&akey).expect("loaded");
        att.lines += 1;
        att.bytes += bytes;
        out.written_lines += 1;
        out.written_bytes += bytes as u64;
        out.truncated_lines += u64::from(rec.truncated);
        out.stored_lines += 1;
        out.stored_bytes += bytes;
    }

    for ((tenant, invocation), r) in &invs {
        tx.execute(
            "INSERT INTO log_invocations (tenant_id, invocation_id, first_seq, last_ts_ns, lines, \
             bytes, stored_lines, stored_bytes, dropped_lines, dropped_bytes, markers, limit_marked) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) \
             ON CONFLICT (tenant_id, invocation_id) DO UPDATE SET \
             first_seq = excluded.first_seq, last_ts_ns = excluded.last_ts_ns, \
             lines = excluded.lines, bytes = excluded.bytes, \
             stored_lines = excluded.stored_lines, stored_bytes = excluded.stored_bytes, \
             dropped_lines = excluded.dropped_lines, dropped_bytes = excluded.dropped_bytes, \
             markers = excluded.markers, limit_marked = excluded.limit_marked",
            params![
                tenant,
                invocation,
                r.first_seq,
                r.last_ts_ns,
                r.lines,
                r.bytes,
                r.stored_lines,
                r.stored_bytes,
                r.dropped_lines,
                r.dropped_bytes,
                r.markers,
                r.limit_marked,
            ],
        )?;
    }
    for ((tenant, invocation, attempt), r) in &atts {
        tx.execute(
            "INSERT INTO log_attempts (tenant_id, invocation_id, attempt_id, lines, bytes, \
             dropped_lines, limit_marked) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT (tenant_id, invocation_id, attempt_id) DO UPDATE SET \
             lines = excluded.lines, bytes = excluded.bytes, \
             dropped_lines = excluded.dropped_lines, limit_marked = excluded.limit_marked",
            params![
                tenant,
                invocation,
                attempt,
                r.lines,
                r.bytes,
                r.dropped_lines,
                r.limit_marked
            ],
        )?;
    }
    tx.execute(
        "UPDATE log_meta SET stored_lines = stored_lines + ?1, stored_bytes = stored_bytes + ?2 \
         WHERE id = 1",
        params![out.stored_lines, out.stored_bytes],
    )?;
    tx.commit()?;
    Ok(out)
}

/// Delete everything of one invocation. Returns the rows deleted.
fn delete_invocation(
    tx: &rusqlite::Transaction<'_>,
    tenant: &str,
    invocation: &str,
    stored_bytes: i64,
) -> rusqlite::Result<u64> {
    let lines = tx.execute(
        "DELETE FROM log_lines WHERE tenant_id = ?1 AND invocation_id = ?2",
        params![tenant, invocation],
    )?;
    tx.execute(
        "DELETE FROM log_attempts WHERE tenant_id = ?1 AND invocation_id = ?2",
        params![tenant, invocation],
    )?;
    tx.execute(
        "DELETE FROM log_invocations WHERE tenant_id = ?1 AND invocation_id = ?2",
        params![tenant, invocation],
    )?;
    tx.execute(
        "UPDATE log_meta SET stored_lines = MAX(0, stored_lines - ?1), \
         stored_bytes = MAX(0, stored_bytes - ?2) WHERE id = 1",
        params![lines as i64, stored_bytes],
    )?;
    Ok(lines as u64)
}

struct Candidate {
    tenant: String,
    invocation: String,
    first_seq: i64,
}

impl Shared {
    fn append(&self, mut record: LogRecord) -> AppendOutcome {
        let c = &self.counters;
        if record.invocation_id.is_none() {
            // Never served (logs are read per invocation): not stored.
            c.dropped[drop_index("unattributed")].fetch_add(1, Ordering::Relaxed);
            return AppendOutcome::Dropped;
        }
        if record.line.len() > self.settings.max_line_bytes {
            let (line, _) = LogRecord::bounded_line(&record.line, self.settings.max_line_bytes);
            record.line = line;
            record.truncated = true;
        }
        let bytes = record.line.len() as u64;
        let notify = {
            let mut q = self.queue.lock();
            if q.shutdown {
                c.dropped[drop_index("store_unavailable")].fetch_add(1, Ordering::Relaxed);
                return AppendOutcome::Dropped;
            }
            if q.items.len() >= self.settings.queue_max_lines
                || q.bytes + bytes > self.settings.queue_max_bytes
            {
                remember_loss(&mut q.losses, "queue_full", &record);
                c.dropped[drop_index("queue_full")].fetch_add(1, Ordering::Relaxed);
                return AppendOutcome::Dropped;
            }
            q.enqueued += 1;
            let seq = q.enqueued;
            q.bytes += bytes;
            q.items.push(Pending {
                seq,
                record,
                at: Instant::now(),
            });
            q.items.len() >= self.settings.flush_max_lines
        };
        if notify {
            self.queue_cv.notify_all();
        }
        AppendOutcome::Stored
    }

    fn wait_for_queued(&self, timeout: Duration) -> bool {
        let target = {
            let mut q = self.queue.lock();
            if q.items.is_empty() && q.losses.is_empty() && *self.committed.lock() >= q.enqueued {
                return true;
            }
            q.flush_requested = true;
            q.enqueued
        };
        self.queue_cv.notify_all();
        let deadline = Instant::now() + timeout;
        let mut committed = self.committed.lock();
        while *committed < target {
            if self
                .committed_cv
                .wait_until(&mut committed, deadline)
                .timed_out()
            {
                return *committed >= target;
            }
        }
        true
    }

    fn run_writer(self: Arc<Self>) {
        let mut next_retention = Instant::now() + self.settings.retention_interval;
        loop {
            let (batch, losses, shutdown) = {
                let mut q = self.queue.lock();
                if !q.shutdown
                    && !q.flush_requested
                    && q.items.len() < self.settings.flush_max_lines
                {
                    let wait = self
                        .settings
                        .flush_interval
                        .min(next_retention.saturating_duration_since(Instant::now()))
                        .max(Duration::from_millis(1));
                    let _ = self.queue_cv.wait_for(&mut q, wait);
                }
                q.flush_requested = false;
                q.bytes = 0;
                (
                    std::mem::take(&mut q.items),
                    std::mem::take(&mut q.losses),
                    q.shutdown,
                )
            };
            let last_seq = batch.last().map(|p| p.seq);
            if !batch.is_empty() || !losses.is_empty() {
                self.flush_batch(batch, losses);
            }
            if let Some(seq) = last_seq {
                let mut committed = self.committed.lock();
                *committed = (*committed).max(seq);
            }
            self.committed_cv.notify_all();
            if shutdown {
                return;
            }
            if Instant::now() >= next_retention {
                if let Err(e) = self.run_retention() {
                    tracing::warn!(error = %e, "log retention pass failed");
                }
                next_retention = Instant::now() + self.settings.retention_interval;
            }
        }
    }

    /// Run `f` on the writer connection, opening it first if needed.
    fn with_writer<T>(
        &self,
        f: impl FnOnce(&mut Connection) -> rusqlite::Result<T>,
    ) -> Result<T, String> {
        let Some(mut guard) = lock_connection(&self.writer) else {
            return Err(busy_message("logs"));
        };
        if guard.conn.is_none() {
            let due = guard.next_open.is_none_or(|t| Instant::now() >= t);
            if !due {
                return Err(self
                    .health
                    .lock()
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "logs.db is not open".into()));
            }
            match open_writer(&self.path) {
                Ok(c) => {
                    guard.conn = Some(c);
                    self.health.lock().writer_open = true;
                    drop(guard);
                    self.load_totals();
                    tracing::info!(path = %self.path.display(), "log store opened");
                    return self.with_writer(f);
                }
                Err(e) => {
                    guard.next_open = Some(Instant::now() + REOPEN_EVERY);
                    return Err(e);
                }
            }
        }
        let conn = guard.conn.as_mut().expect("open");
        f(conn).map_err(|e| e.to_string())
    }

    fn load_totals(&self) {
        let Some(mut guard) = lock_connection(&self.writer) else {
            return;
        };
        let Some(conn) = guard.conn.as_mut() else {
            return;
        };
        if let Ok((lines, bytes)) = conn.query_row(
            "SELECT stored_lines, stored_bytes FROM log_meta WHERE id = 1",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        ) {
            self.counters
                .stored_lines
                .store(lines.max(0) as u64, Ordering::Relaxed);
            self.counters
                .stored_bytes
                .store(bytes.max(0) as u64, Ordering::Relaxed);
        }
    }

    fn flush_batch(&self, batch: Vec<Pending>, losses: BTreeMap<LossKey, Loss>) {
        let delay = self.flush_delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        let now = self.clock.now();
        let c = &self.counters;
        c.flushes.fetch_add(1, Ordering::Relaxed);
        match self.with_writer(|conn| write_batch(conn, &self.settings, now, &batch, &losses)) {
            Ok(out) => {
                c.written_lines
                    .fetch_add(out.written_lines, Ordering::Relaxed);
                c.written_bytes
                    .fetch_add(out.written_bytes, Ordering::Relaxed);
                c.truncated_lines
                    .fetch_add(out.truncated_lines, Ordering::Relaxed);
                c.markers.fetch_add(out.markers, Ordering::Relaxed);
                c.dropped[drop_index("invocation_limit")]
                    .fetch_add(out.invocation_limit, Ordering::Relaxed);
                c.dropped[drop_index("attempt_limit")]
                    .fetch_add(out.attempt_limit, Ordering::Relaxed);
                let add = |a: &AtomicU64, d: i64| {
                    let _ = a.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some((v as i64 + d).max(0) as u64)
                    });
                };
                add(&c.stored_lines, out.stored_lines);
                add(&c.stored_bytes, out.stored_bytes);
                if let Some(oldest) = batch.first() {
                    c.last_flush_lag_us
                        .store(oldest.at.elapsed().as_micros() as u64, Ordering::Relaxed);
                }
                let mut h = self.health.lock();
                if h.last_flush_failed {
                    tracing::info!("log store writable again");
                }
                h.last_flush_failed = false;
                h.last_error = None;
                h.last_flush_at = Some(now);
            }
            Err(e) => {
                c.flush_failures.fetch_add(1, Ordering::Relaxed);
                c.dropped[drop_index("store_unavailable")]
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                {
                    let mut h = self.health.lock();
                    if !h.last_flush_failed {
                        tracing::warn!(error = %e, lines = batch.len(), "log store unavailable; invocation logs are dropped");
                    }
                    h.last_flush_failed = true;
                    h.last_error = Some(e);
                }
                // The losses stay remembered, so their invocations get a
                // marker once the store answers again.
                let mut q = self.queue.lock();
                for (key, loss) in losses {
                    let len = q.losses.len();
                    match q.losses.get_mut(&key) {
                        Some(l) => {
                            l.lines += loss.lines;
                            l.bytes += loss.bytes;
                        }
                        None if len < MAX_LOSS_KEYS => {
                            q.losses.insert(key, loss);
                        }
                        None => {}
                    }
                }
                for p in &batch {
                    remember_loss(&mut q.losses, "store_unavailable", &p.record);
                }
            }
        }
    }

    fn query(&self, tenant: &TenantId, invocation: &InvocationId) -> Result<LogQuery, RepoError> {
        // Read-your-writes, bounded: lines queued before this read are
        // committed first unless the store is slow or locked.
        self.wait_for_queued(self.settings.read_flush_wait);
        let pending_loss = {
            let q = self.queue.lock();
            q.losses
                .keys()
                .any(|k| &k.tenant == tenant && &k.invocation == invocation)
        };
        let Some(mut guard) = lock_connection(&self.reader) else {
            return Err(RepoError::Store(busy_message("logs")));
        };
        if guard.conn.is_none() {
            guard.conn = Some(open_reader(&self.path).map_err(RepoError::Store)?);
        }
        let conn = guard.conn.as_ref().expect("open");
        let store = |e: rusqlite::Error| RepoError::Store(format!("logs.db: {e}"));
        let limit = self.settings.invocation_lines + MAX_MARKERS_PER_INVOCATION;
        let mut stmt = conn
            .prepare_cached(
                "SELECT attempt_id, environment_id, stream, phase, ts_ns, line, truncated \
                 FROM log_lines WHERE tenant_id = ?1 AND invocation_id = ?2 \
                 ORDER BY seq LIMIT ?3",
            )
            .map_err(store)?;
        let rows = stmt
            .query_map(params![tenant.as_str(), invocation.as_str(), limit], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, bool>(6)?,
                ))
            })
            .map_err(store)?;
        let bad = |what: &str, v: &str| RepoError::Serialization(format!("logs.db {what}: {v}"));
        let mut records = Vec::new();
        for row in rows {
            let (attempt, environment, stream, phase, ts_ns, line, truncated) =
                row.map_err(store)?;
            records.push(LogRecord {
                tenant_id: tenant.clone(),
                environment_id: EnvironmentId::parse(&environment)
                    .map_err(|_| bad("environment", &environment))?,
                invocation_id: Some(invocation.clone()),
                attempt_id: if attempt.is_empty() {
                    None
                } else {
                    Some(AttemptId::parse(&attempt).map_err(|_| bad("attempt", &attempt))?)
                },
                stream: match stream.as_str() {
                    "stdout" => LogStream::Stdout,
                    "stderr" => LogStream::Stderr,
                    "platform" => LogStream::Platform,
                    other => return Err(bad("stream", other)),
                },
                phase: match phase.as_str() {
                    "boot" => LogPhase::Boot,
                    "init" => LogPhase::Init,
                    "handler" => LogPhase::Handler,
                    "shutdown" => LogPhase::Shutdown,
                    other => return Err(bad("phase", other)),
                },
                timestamp: chrono::DateTime::from_timestamp_nanos(ts_ns),
                line,
                truncated,
            });
        }
        let dropped_lines: i64 = conn
            .query_row(
                "SELECT dropped_lines FROM log_invocations WHERE tenant_id = ?1 AND invocation_id = ?2",
                params![tenant.as_str(), invocation.as_str()],
                |r| r.get(0),
            )
            .optional()
            .map_err(store)?
            .unwrap_or(0);
        Ok(LogQuery {
            records,
            dropped: dropped_lines > 0 || pending_loss,
        })
    }

    /// `true` when the size cap may delete the invocation's logs.
    fn deletable(&self, tenant: &str, invocation: &str) -> bool {
        let Some(ledger) = self.ledger.lock().clone() else {
            return true;
        };
        let Ok(id) = InvocationId::parse(invocation) else {
            return true;
        };
        match ledger.get(&id) {
            Ok(Some(inv)) => inv.tenant_id.as_str() != tenant || inv.status.is_terminal(),
            Ok(None) => true,
            Err(_) => false,
        }
    }

    fn run_retention(&self) -> Result<RetentionReport, RepoError> {
        let now = self.clock.now();
        let mut report = RetentionReport::default();
        let err = RepoError::Store;
        if self.settings.retention_seconds > 0 {
            let horizon = chrono::Duration::seconds(
                i64::try_from(
                    self.settings
                        .retention_seconds
                        .min(i64::MAX as u64 / 1_000_000),
                )
                .unwrap_or(i64::MAX),
            );
            let cutoff = ns(&(now - horizon));
            loop {
                let (invocations, lines) = self
                    .with_writer(|conn| {
                        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                        let victims: Vec<(String, String, i64)> = {
                            let mut stmt = tx.prepare(
                                "SELECT tenant_id, invocation_id, stored_bytes FROM log_invocations \
                                 WHERE last_ts_ns < ?1 LIMIT ?2",
                            )?;
                            stmt.query_map(params![cutoff, RETENTION_PAGE as i64], |r| {
                                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                            })?
                            .collect::<rusqlite::Result<_>>()?
                        };
                        let mut lines = 0;
                        for (t, i, b) in &victims {
                            lines += delete_invocation(&tx, t, i, *b)?;
                        }
                        tx.commit()?;
                        Ok((victims.len() as u64, lines))
                    })
                    .map_err(err)?;
                report.age_invocations += invocations;
                report.age_lines += lines;
                if invocations < RETENTION_PAGE as u64 {
                    break;
                }
            }
        }
        if self.settings.max_total_bytes > 0 {
            let max = i64::try_from(self.settings.max_total_bytes).unwrap_or(i64::MAX);
            let mut after_seq = 0_i64;
            loop {
                let (stored, candidates) = self
                    .with_writer(|conn| {
                        let stored: i64 = conn.query_row(
                            "SELECT stored_bytes FROM log_meta WHERE id = 1",
                            [],
                            |r| r.get(0),
                        )?;
                        if stored <= max {
                            return Ok((stored, Vec::new()));
                        }
                        let mut stmt = conn.prepare(
                            "SELECT tenant_id, invocation_id, first_seq \
                             FROM log_invocations WHERE first_seq > ?1 \
                             ORDER BY first_seq LIMIT ?2",
                        )?;
                        let rows = stmt
                            .query_map(params![after_seq, RETENTION_PAGE as i64], |r| {
                                Ok(Candidate {
                                    tenant: r.get(0)?,
                                    invocation: r.get(1)?,
                                    first_seq: r.get(2)?,
                                })
                            })?
                            .collect::<rusqlite::Result<Vec<_>>>()?;
                        Ok((stored, rows))
                    })
                    .map_err(err)?;
                if stored <= max || candidates.is_empty() {
                    break;
                }
                after_seq = candidates.last().map(|c| c.first_seq).unwrap_or(after_seq);
                // The ledger is asked outside the log store's lock.
                let deletable: Vec<Candidate> = candidates
                    .into_iter()
                    .filter(|c| {
                        let ok = self.deletable(&c.tenant, &c.invocation);
                        report.skipped_non_terminal += u64::from(!ok);
                        ok
                    })
                    .collect();
                let (invocations, lines) = self
                    .with_writer(|conn| {
                        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                        let mut stored: i64 = tx.query_row(
                            "SELECT stored_bytes FROM log_meta WHERE id = 1",
                            [],
                            |r| r.get(0),
                        )?;
                        let (mut invocations, mut lines) = (0_u64, 0_u64);
                        for c in &deletable {
                            if stored <= max {
                                break;
                            }
                            // Re-read: the row may have grown since the page.
                            let Some(bytes) = tx
                                .query_row(
                                    "SELECT stored_bytes FROM log_invocations \
                                     WHERE tenant_id = ?1 AND invocation_id = ?2",
                                    params![c.tenant, c.invocation],
                                    |r| r.get::<_, i64>(0),
                                )
                                .optional()?
                            else {
                                continue;
                            };
                            lines += delete_invocation(&tx, &c.tenant, &c.invocation, bytes)?;
                            invocations += 1;
                            stored -= bytes;
                        }
                        tx.commit()?;
                        Ok((invocations, lines))
                    })
                    .map_err(err)?;
                report.size_invocations += invocations;
                report.size_lines += lines;
            }
        }
        let c = &self.counters;
        if report.age_lines + report.size_lines > 0 {
            let _ = self.with_writer(|conn| conn.execute_batch("PRAGMA incremental_vacuum"));
            self.load_totals();
        }
        c.retention_age_lines
            .fetch_add(report.age_lines, Ordering::Relaxed);
        c.retention_size_lines
            .fetch_add(report.size_lines, Ordering::Relaxed);
        c.retention_invocations.fetch_add(
            report.age_invocations + report.size_invocations,
            Ordering::Relaxed,
        );
        c.retention_skipped_non_terminal
            .store(report.skipped_non_terminal, Ordering::Relaxed);
        self.health.lock().last_retention_at = Some(now);
        if report != RetentionReport::default() {
            tracing::info!(?report, "log retention pass");
        }
        Ok(report)
    }

    fn status(&self) -> LogStoreStatus {
        let c = &self.counters;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let (queued_lines, queued_bytes) = {
            let q = self.queue.lock();
            (q.items.len() as u64, q.bytes)
        };
        let h = self.health.lock();
        let flushes = get(&c.flushes);
        LogStoreStatus {
            backend: "sqlite",
            durable: true,
            healthy: h.writer_open && !h.last_flush_failed,
            path: Some(self.path.display().to_string()),
            last_error: h.last_error.clone(),
            queued_lines,
            queued_bytes,
            queue_max_lines: self.settings.queue_max_lines as u64,
            queue_max_bytes: self.settings.queue_max_bytes,
            stored_lines: get(&c.stored_lines),
            stored_bytes: get(&c.stored_bytes),
            max_total_bytes: self.settings.max_total_bytes,
            retention_seconds: self.settings.retention_seconds,
            flush_interval_ms: self.settings.flush_interval.as_millis() as u64,
            last_flush_lag_ms: (flushes > 0).then(|| get(&c.last_flush_lag_us) as f64 / 1000.0),
            last_flush_at: h.last_flush_at,
            last_retention_at: h.last_retention_at,
            lines_written: get(&c.written_lines),
            bytes_written: get(&c.written_bytes),
            lines_truncated: get(&c.truncated_lines),
            marker_lines: get(&c.markers),
            lines_dropped: DROP_REASONS
                .iter()
                .enumerate()
                .map(|(i, r)| (*r, get(&c.dropped[i])))
                .collect(),
            flushes,
            flush_failures: get(&c.flush_failures),
            retention_deleted_lines: BTreeMap::from([
                ("age", get(&c.retention_age_lines)),
                ("size", get(&c.retention_size_lines)),
            ]),
            retention_deleted_invocations: get(&c.retention_invocations),
            retention_skipped_non_terminal: get(&c.retention_skipped_non_terminal),
        }
    }
}

//! Embedded SQLite store: `<data_dir>/state.db` (docs/adr/0003).
//!
//! - One connection per store, serialized by a mutex. Every write is one
//!   `BEGIN IMMEDIATE` transaction, so writers in other processes that open
//!   the same file are serialized by SQLite's own lock (WAL mode,
//!   `busy_timeout`). The read-modify-write of every update happens inside
//!   that transaction, and the `UPDATE` itself carries the CAS predicate
//!   (`generation = ?`, `epoch = ?`, `terminal = 0`, `state = 'idle'`), so a
//!   future adapter without a database-wide write lock keeps the same
//!   semantics: zero affected rows means someone else won.
//! - Every row stores the domain object as JSON in `body`; the other columns
//!   exist for lookups, uniqueness, CAS and retention
//!   (`migrations/001_initial.sql`).
//! - Opening applies pending migrations, imports a P1 `state.json` once,
//!   settles the in-flight rows that have **no owner** exactly like P1 did
//!   ([`super::restart`]) and applies output and idempotency retention. Rows
//!   owned by a dispatcher are left to [`super::SlotStore::reclaim_expired`]
//!   (`slot.rs`), which only touches owners whose lease expired, that stopped
//!   or whose previous incarnation is gone (PLT-4631): a second gateway that
//!   opens the same file never settles the first one's live work.
//! - Logs stay in memory, bounded per invocation (decision 5). Secrets are
//!   never written: nothing in the domain rows carries a secret value.

use std::path::{Path, PathBuf};
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::{Connection, ErrorCode, OptionalExtension, Params, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use tachyon_serverless_domain::{
    AliasName, AttemptId, EnvironmentId, ExecutionEnvironment, ExecutionLease, Function,
    FunctionAlias, FunctionId, FunctionName, FunctionRevision, Invocation, InvocationAttempt,
    InvocationId, LeaseId, Limits, LogRecord, PayloadRef, ReuseKey, RevisionId, Sha256Digest,
    TenantId, Timestamp,
};

use super::guard::{self, Write};
use super::logs::LogBuffer;
use super::restart;
use super::{
    AliasRepository, AppendOutcome, ArtifactOwnerRepository, EnvironmentRepository,
    FunctionRepository, IdempotencyBinding, IdempotencyOutcome, IdempotencyRepository,
    InvocationRepository, LogQuery, LogRepository, RepoError, RevisionRepository, StateStore,
    legacy,
};

mod config;
mod dispatch;
pub mod migrations;
mod objects;
mod outbox;
mod slot;
mod triggers;

/// Fixed-width RFC 3339 UTC, so timestamps order correctly as text.
pub(crate) fn ts(t: &Timestamp) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

impl From<rusqlite::Error> for RepoError {
    fn from(e: rusqlite::Error) -> Self {
        match &e {
            rusqlite::Error::SqliteFailure(f, _) if f.code == ErrorCode::ConstraintViolation => {
                RepoError::Conflict(e.to_string())
            }
            _ => RepoError::Store(e.to_string()),
        }
    }
}

fn to_json<T: Serialize>(v: &T) -> Result<String, RepoError> {
    serde_json::to_string(v).map_err(|e| RepoError::Serialization(e.to_string()))
}

fn from_json<T: DeserializeOwned>(s: &str) -> Result<T, RepoError> {
    serde_json::from_str(s).map_err(|e| RepoError::Serialization(e.to_string()))
}

fn big(v: u64, what: &str) -> Result<i64, RepoError> {
    i64::try_from(v).map_err(|_| RepoError::Refused(format!("{what} {v} does not fit a BIGINT")))
}

/// Reuse-key versions are opaque 64-bit values (some are truncated digests),
/// so they are stored bit for bit: equality, which is all a lookup needs,
/// survives the signed column. A counter goes through [`big`] instead.
fn bits(v: u64) -> i64 {
    v as i64
}

fn flag(b: bool) -> i64 {
    i64::from(b)
}

/// Store options that come from `[store]`.
#[derive(Debug, Clone, Default)]
pub struct SqliteOptions {
    /// How long an inline invocation output is kept after the invocation
    /// finished. After that the body is replaced by its digest. `None` keeps
    /// it for as long as the row exists.
    pub output_retention: Option<chrono::Duration>,
    /// How long an idempotency key keeps answering after its invocation
    /// finished (PLT-4631). `None` keeps it for as long as the row exists.
    pub idempotency_retention: Option<chrono::Duration>,
}

/// Both retention periods, passed to every row writer that can make an
/// invocation terminal.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Retention {
    pub output: Option<chrono::Duration>,
    pub idempotency: Option<chrono::Duration>,
}

/// What [`SqliteStore::open`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenReport {
    pub schema_version: i64,
    pub migrations_applied: Vec<i64>,
    /// Where a P1 `state.json` was moved after its import.
    pub imported_state_json: Option<PathBuf>,
    pub settled: Settled,
    pub outputs_purged: usize,
}

/// Rows the restart reconcile changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Settled {
    pub invocations: usize,
    pub attempts: usize,
    pub environments: usize,
    pub leases: usize,
    pub idempotency_dropped: usize,
    /// Expired idempotency bindings purged on open.
    pub idempotency_purged: usize,
}

pub struct SqliteStore {
    conn: Mutex<Connection>,
    logs: LogBuffer,
    limits: Limits,
    options: SqliteOptions,
    path: Option<PathBuf>,
    report: OpenReport,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore")
            .field("path", &self.path)
            .field("schema_version", &self.report.schema_version)
            .finish_non_exhaustive()
    }
}

fn configure(conn: &Connection) -> Result<(), RepoError> {
    conn.busy_timeout(Duration::from_secs(5))?;
    let _mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    // Overwrite freed content, so a body replaced by its digest does not
    // linger in free pages (docs/threat-model.md §14-4).
    conn.pragma_update(None, "secure_delete", "ON")?;
    Ok(())
}

/// Create the database file readable by its owner only
/// (docs/threat-model.md §14-4). SQLite gives `-wal` / `-shm` the same mode.
/// An existing file keeps whatever mode the operator gave it.
fn create_private(path: &Path) -> Result<(), RepoError> {
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
        Err(e) => Err(e.into()),
    }
}

impl SqliteStore {
    pub const FILE_NAME: &'static str = "state.db";

    /// Open (or create) `<data_dir>/state.db`, migrate it, import a P1
    /// `state.json` once and settle what the previous process left in flight.
    pub fn open(
        data_dir: &Path,
        limits: Limits,
        options: SqliteOptions,
        now: Timestamp,
    ) -> Result<Self, RepoError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(Self::FILE_NAME);
        create_private(&path)?;
        let mut conn = Connection::open(&path)?;
        configure(&conn)?;
        let applied = migrations::migrate_to(&mut conn, migrations::LATEST, now)?;
        let mut store = Self {
            conn: Mutex::new(conn),
            logs: LogBuffer::default(),
            limits,
            options,
            path: Some(path),
            report: OpenReport::default(),
        };
        let imported = store.import_legacy(data_dir, now)?;
        let mut settled = store.reconcile_after_restart(now)?;
        store.backfill_output_expiry()?;
        store.backfill_idempotency_expiry()?;
        let purged = store.purge_expired_outputs(now)?;
        settled.idempotency_purged = store.purge_expired_idempotency(now)?;
        store.report = OpenReport {
            schema_version: store.read(migrations::current_version)?,
            migrations_applied: applied.iter().map(|m| m.version).collect(),
            imported_state_json: imported,
            settled,
            outputs_purged: purged,
        };
        Ok(store)
    }

    /// A migrated database that lives in memory only (tests). No import, no
    /// reconcile.
    pub fn open_volatile(limits: Limits, options: SqliteOptions) -> Result<Self, RepoError> {
        let mut conn = Connection::open_in_memory()?;
        configure(&conn)?;
        migrations::migrate_to(&mut conn, migrations::LATEST, chrono::Utc::now())?;
        Ok(Self {
            conn: Mutex::new(conn),
            logs: LogBuffer::default(),
            limits,
            options,
            path: None,
            report: OpenReport {
                schema_version: migrations::LATEST,
                ..OpenReport::default()
            },
        })
    }

    pub fn open_report(&self) -> &OpenReport {
        &self.report
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    fn read<R>(&self, f: impl FnOnce(&Connection) -> Result<R, RepoError>) -> Result<R, RepoError> {
        let conn = self.conn.lock();
        f(&conn)
    }

    /// One `BEGIN IMMEDIATE` transaction. An error rolls everything back.
    fn write<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R, RepoError>,
    ) -> Result<R, RepoError> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }

    fn retention(&self) -> Retention {
        Retention {
            output: self.options.output_retention,
            idempotency: self.options.idempotency_retention,
        }
    }

    fn max_inline(&self) -> u64 {
        self.limits.max_response_bytes
    }

    // -- open-time steps ----------------------------------------------------

    /// docs/adr/0003 「`state.json` からの移行」 2-3.
    fn import_legacy(&self, data_dir: &Path, now: Timestamp) -> Result<Option<PathBuf>, RepoError> {
        let json = data_dir.join(legacy::FILE_NAME);
        if !json.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&json)?;
        let digest = Sha256Digest::of_bytes(&bytes).to_string();
        let imported = self.read(|c| meta(c, META_LEGACY_DIGEST))?;
        // The same file was imported and only the rename was lost: finish it.
        if imported.as_deref() != Some(digest.as_str()) {
            if self.read(ledger_has_rows)? {
                let db = self.path.clone().unwrap_or_default();
                return Err(RepoError::Refused(format!(
                    "both {} and {} hold a ledger. state.json is only imported into an empty \
                     database: move {} aside if it is already imported, or move {} aside to \
                     import the JSON ledger again",
                    json.display(),
                    db.display(),
                    json.display(),
                    db.display()
                )));
            }
            let state = legacy::parse(&bytes, &json)?;
            let retention = self.retention();
            self.write(|tx| {
                insert_legacy(tx, &state, retention)?;
                tx.execute(
                    "DELETE FROM store_meta WHERE meta_key = ?1",
                    [META_LEGACY_DIGEST],
                )?;
                tx.execute(
                    "INSERT INTO store_meta (meta_key, meta_value) VALUES (?1, ?2)",
                    params![META_LEGACY_DIGEST, digest],
                )?;
                Ok(())
            })?;
        }
        let stamp = now.format("%Y%m%dT%H%M%SZ");
        let mut target = data_dir.join(format!("{}.imported-{stamp}", legacy::FILE_NAME));
        let mut n = 1;
        while target.exists() {
            target = data_dir.join(format!("{}.imported-{stamp}.{n}", legacy::FILE_NAME));
            n += 1;
        }
        std::fs::rename(&json, &target)?;
        tracing::info!(from = %json.display(), to = %target.display(), "imported state.json into state.db");
        Ok(Some(target))
    }

    /// The P1 restart semantics (docs/threat-model.md §9) for rows **without
    /// an owner**: nothing that was in flight survives the process that drove
    /// it. Rows owned by a dispatcher are settled only by
    /// [`super::SlotStore::reclaim_expired`] (PLT-4631).
    fn reconcile_after_restart(&self, now: Timestamp) -> Result<Settled, RepoError> {
        let retention = self.retention();
        self.write(|tx| {
            let mut s = Settled {
                idempotency_dropped: tx.execute(
                    "DELETE FROM idempotency WHERE invocation_id NOT IN (SELECT id FROM invocations)",
                    [],
                )?,
                ..Settled::default()
            };
            let invocations: Vec<Invocation> = bodies(
                tx,
                // An asynchronous invocation survives a restart in any
                // non-terminal state: its input, its outbox event and its
                // dispatch state are durable, and a run the restart cut short
                // is retried once its claim expires (PLT-4639, PLT-4640).
                "SELECT body FROM invocations WHERE terminal = 0 AND owner_id IS NULL \
                 AND id NOT IN (SELECT invocation_id FROM invocation_inputs) \
                 ORDER BY id",
                [],
            )?;
            for mut inv in invocations {
                if restart::settle_invocation(&mut inv, now) {
                    update_invocation_row(tx, &inv, retention)?;
                    s.invocations += 1;
                }
            }
            let attempts: Vec<InvocationAttempt> = bodies(
                tx,
                "SELECT a.body FROM attempts a JOIN invocations v ON v.id = a.invocation_id \
                 WHERE a.terminal = 0 AND v.owner_id IS NULL ORDER BY a.id",
                [],
            )?;
            for mut att in attempts {
                let unknown = get_invocation(tx, &att.invocation_id)?
                    .is_some_and(|inv| restart::attempts_unknown(&inv));
                if restart::settle_attempt(&mut att, unknown, now) {
                    update_attempt_row(tx, &att)?;
                    s.attempts += 1;
                }
            }
            let envs: Vec<ExecutionEnvironment> = bodies(
                tx,
                "SELECT body FROM environments WHERE terminal = 0 AND owner_id IS NULL ORDER BY id",
                [],
            )?;
            for mut env in envs {
                let epoch = env.epoch;
                if restart::settle_environment(&mut env, now) {
                    update_environment_row(tx, &env, epoch, None)?;
                    s.environments += 1;
                }
            }
            let leases: Vec<ExecutionLease> = bodies(
                tx,
                "SELECT body FROM leases WHERE released = 0 AND owner_id IS NULL ORDER BY id",
                [],
            )?;
            for mut lease in leases {
                if restart::settle_lease(&mut lease, now) {
                    update_lease_row(tx, &lease)?;
                    s.leases += 1;
                }
            }
            Ok(s)
        })
    }

    /// Give inline outputs written before retention existed (schema 1, or a
    /// `state.json` import) their expiry.
    fn backfill_output_expiry(&self) -> Result<(), RepoError> {
        let Some(retention) = self.retention().output else {
            return Ok(());
        };
        self.write(|tx| {
            let rows: Vec<Invocation> = bodies(
                tx,
                "SELECT body FROM invocations WHERE terminal = 1 AND output_kind = 'inline' \
                 AND output_expires_at IS NULL",
                [],
            )?;
            for inv in rows {
                tx.execute(
                    "UPDATE invocations SET output_expires_at = ?1 WHERE id = ?2",
                    params![output_expires_at(&inv, Some(retention)), inv.id.as_str()],
                )?;
            }
            Ok(())
        })
    }

    /// Give bindings of invocations that finished before idempotency expiry
    /// existed (schema 2, a `state.json` import) their expiry.
    fn backfill_idempotency_expiry(&self) -> Result<(), RepoError> {
        let retention = self.retention();
        if retention.idempotency.is_none() {
            return Ok(());
        }
        self.write(|tx| {
            let rows: Vec<Invocation> = bodies(
                tx,
                "SELECT v.body FROM invocations v JOIN idempotency i ON i.invocation_id = v.id \
                 WHERE v.terminal = 1 AND i.expires_at IS NULL",
                [],
            )?;
            for inv in rows {
                set_idempotency_expiry(tx, &inv, retention)?;
            }
            Ok(())
        })
    }
}

const META_LEGACY_DIGEST: &str = "legacy_state_json_sha256";

fn meta(c: &Connection, key: &str) -> Result<Option<String>, RepoError> {
    Ok(c.query_row(
        "SELECT meta_value FROM store_meta WHERE meta_key = ?1",
        [key],
        |r| r.get(0),
    )
    .optional()?)
}

fn ledger_has_rows(c: &Connection) -> Result<bool, RepoError> {
    for table in [
        "functions",
        "revisions",
        "aliases",
        "invocations",
        "attempts",
        "environments",
        "idempotency",
        "artifact_owners",
    ] {
        let hit: Option<i64> = c
            .query_row(&format!("SELECT 1 FROM {table} LIMIT 1"), [], |r| r.get(0))
            .optional()?;
        if hit.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn insert_legacy(
    tx: &Connection,
    state: &legacy::PersistedState,
    retention: Retention,
) -> Result<(), RepoError> {
    for f in state.functions.values() {
        insert_function_row(tx, f)?;
    }
    for r in state.revisions.values() {
        insert_revision_row(tx, r)?;
    }
    for (function, n) in &state.revision_counters {
        tx.execute(
            "INSERT INTO revision_counters (function_id, last_number) VALUES (?1, ?2)",
            params![function.as_str(), big(*n, "revision counter")?],
        )?;
    }
    for a in state.aliases.values() {
        insert_alias_row(tx, a)?;
    }
    for inv in state.invocations.values() {
        insert_invocation_row(tx, inv, retention)?;
    }
    for att in state.attempts.values() {
        insert_attempt_row(tx, att)?;
    }
    for env in state.environments.values() {
        insert_environment_row(tx, env)?;
    }
    for (k, e) in &state.idempotency {
        tx.execute(
            "INSERT INTO idempotency (tenant_id, function_id, idem_key, invocation_id, input_digest) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                k.tenant_id.as_str(),
                k.function_id.as_str(),
                k.key,
                e.invocation_id.as_str(),
                e.input_digest.as_str()
            ],
        )?;
    }
    for (digest, tenants) in &state.artifact_owners {
        for t in tenants {
            tx.execute(
                "INSERT INTO artifact_owners (digest, tenant_id) VALUES (?1, ?2)",
                params![digest.as_str(), t.as_str()],
            )?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// row helpers
// ---------------------------------------------------------------------------

fn body<T: DeserializeOwned>(
    c: &Connection,
    sql: &str,
    p: impl Params,
) -> Result<Option<T>, RepoError> {
    let raw: Option<String> = c
        .prepare_cached(sql)?
        .query_row(p, |r| r.get(0))
        .optional()?;
    raw.map(|s| from_json(&s)).transpose()
}

fn bodies<T: DeserializeOwned>(
    c: &Connection,
    sql: &str,
    p: impl Params,
) -> Result<Vec<T>, RepoError> {
    let mut stmt = c.prepare_cached(sql)?;
    let rows = stmt.query_map(p, |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(from_json(&row?)?);
    }
    Ok(out)
}

fn get_function(c: &Connection, id: &FunctionId) -> Result<Option<Function>, RepoError> {
    body(c, "SELECT body FROM functions WHERE id = ?1", [id.as_str()])
}

fn get_revision(c: &Connection, id: &RevisionId) -> Result<Option<FunctionRevision>, RepoError> {
    body(c, "SELECT body FROM revisions WHERE id = ?1", [id.as_str()])
}

fn get_alias(
    c: &Connection,
    function: &FunctionId,
    name: &AliasName,
) -> Result<Option<FunctionAlias>, RepoError> {
    body(
        c,
        "SELECT body FROM aliases WHERE function_id = ?1 AND name = ?2",
        [function.as_str(), name.as_str()],
    )
}

fn get_invocation(c: &Connection, id: &InvocationId) -> Result<Option<Invocation>, RepoError> {
    body(
        c,
        "SELECT body FROM invocations WHERE id = ?1",
        [id.as_str()],
    )
}

fn get_attempt(c: &Connection, id: &AttemptId) -> Result<Option<InvocationAttempt>, RepoError> {
    body(c, "SELECT body FROM attempts WHERE id = ?1", [id.as_str()])
}

fn get_environment(
    c: &Connection,
    id: &EnvironmentId,
) -> Result<Option<ExecutionEnvironment>, RepoError> {
    body(
        c,
        "SELECT body FROM environments WHERE id = ?1",
        [id.as_str()],
    )
}

fn get_lease(c: &Connection, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError> {
    body(c, "SELECT body FROM leases WHERE id = ?1", [id.as_str()])
}

fn exists(c: &Connection, table: &str, id: &str) -> Result<bool, RepoError> {
    let hit: Option<i64> = c
        .prepare_cached(&format!("SELECT 1 FROM {table} WHERE id = ?1"))?
        .query_row([id], |r| r.get(0))
        .optional()?;
    Ok(hit.is_some())
}

fn duplicate(kind: &str, id: impl std::fmt::Display) -> RepoError {
    RepoError::Conflict(format!("{kind} {id} already exists"))
}

fn lost_race(kind: &str, id: impl std::fmt::Display) -> RepoError {
    RepoError::Refused(format!("{kind} {id} changed concurrently"))
}

fn insert_function_row(c: &Connection, f: &Function) -> Result<(), RepoError> {
    c.prepare_cached(
        "INSERT INTO functions (id, tenant_id, name, live_name, created_at, body) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?
    .execute(params![
        f.id.as_str(),
        f.tenant_id.as_str(),
        f.name.as_str(),
        (!f.is_deleted()).then(|| f.name.as_str()),
        ts(&f.created_at),
        to_json(f)?
    ])?;
    Ok(())
}

fn insert_revision_row(c: &Connection, r: &FunctionRevision) -> Result<(), RepoError> {
    c.prepare_cached(
        "INSERT INTO revisions (id, function_id, tenant_id, number, status, spec_digest, created_at, body) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?
    .execute(params![
        r.id.as_str(),
        r.function_id.as_str(),
        r.tenant_id.as_str(),
        big(r.number, "revision number")?,
        r.status.name(),
        r.spec_digest.as_str(),
        ts(&r.created_at),
        to_json(r)?
    ])?;
    Ok(())
}

fn insert_alias_row(c: &Connection, a: &FunctionAlias) -> Result<(), RepoError> {
    c.prepare_cached(
        "INSERT INTO aliases (function_id, name, tenant_id, revision_id, generation, updated_at, body) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?
    .execute(params![
        a.function_id.as_str(),
        a.name.as_str(),
        a.tenant_id.as_str(),
        a.revision_id.as_str(),
        big(a.generation, "alias generation")?,
        ts(&a.updated_at),
        to_json(a)?
    ])?;
    Ok(())
}

fn output_kind(inv: &Invocation) -> Option<&'static str> {
    inv.output.as_ref().map(|o| match o {
        PayloadRef::Inline { .. } => "inline",
        PayloadRef::Digest { .. } => "digest",
    })
}

/// When a binding of `inv` stops answering: `finished_at + retention` once
/// the invocation is terminal, never while it is in flight.
fn idempotency_expires_at(inv: &Invocation, retention: Retention) -> Option<String> {
    let retention = retention.idempotency?;
    if !inv.status.is_terminal() {
        return None;
    }
    let finished = inv.finished_at.unwrap_or(inv.accepted_at);
    Some(ts(&(finished + retention)))
}

fn set_idempotency_expiry(
    c: &Connection,
    inv: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    if inv.idempotency_key.is_some() {
        c.prepare_cached("UPDATE idempotency SET expires_at = ?1 WHERE invocation_id = ?2")?
            .execute(params![
                idempotency_expires_at(inv, retention),
                inv.id.as_str()
            ])?;
    }
    Ok(())
}

fn output_expires_at(inv: &Invocation, retention: Option<chrono::Duration>) -> Option<String> {
    let retention = retention?;
    if !inv.status.is_terminal() || !matches!(inv.output, Some(PayloadRef::Inline { .. })) {
        return None;
    }
    let finished = inv.finished_at.unwrap_or(inv.accepted_at);
    Some(ts(&(finished + retention)))
}

fn insert_invocation_row(
    c: &Connection,
    inv: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    c.prepare_cached(
        "INSERT INTO invocations (id, tenant_id, function_id, revision_id, status, terminal, \
         accepted_at, finished_at, input_digest, output_kind, output_expires_at, body, owner_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )?
    .execute(params![
        inv.id.as_str(),
        inv.tenant_id.as_str(),
        inv.function_id.as_str(),
        inv.revision_id.as_str(),
        inv.status.name(),
        flag(inv.status.is_terminal()),
        ts(&inv.accepted_at),
        inv.finished_at.as_ref().map(ts),
        inv.input_digest.as_str(),
        output_kind(inv),
        output_expires_at(inv, retention.output),
        to_json(inv)?,
        inv.dispatcher_id.as_ref().map(|d| d.as_str())
    ])?;
    Ok(())
}

/// CAS: only a row that is not terminal yet is rewritten.
fn update_invocation_row(
    c: &Connection,
    inv: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    let n = c
        .prepare_cached(
            "UPDATE invocations SET revision_id = ?1, status = ?2, terminal = ?3, finished_at = ?4, \
             output_kind = ?5, output_expires_at = ?6, body = ?7 WHERE id = ?8 AND terminal = 0",
        )?
        .execute(params![
            inv.revision_id.as_str(),
            inv.status.name(),
            flag(inv.status.is_terminal()),
            inv.finished_at.as_ref().map(ts),
            output_kind(inv),
            output_expires_at(inv, retention.output),
            to_json(inv)?,
            inv.id.as_str()
        ])?;
    if n != 1 {
        return Err(lost_race("invocation", &inv.id));
    }
    if inv.status.is_terminal() {
        set_idempotency_expiry(c, inv, retention)?;
    }
    Ok(())
}

fn insert_attempt_row(c: &Connection, a: &InvocationAttempt) -> Result<(), RepoError> {
    c.prepare_cached(
        "INSERT INTO attempts (id, invocation_id, tenant_id, number, environment_id, epoch, status, \
         terminal, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?
    .execute(params![
        a.id.as_str(),
        a.invocation_id.as_str(),
        a.tenant_id.as_str(),
        i64::from(a.number),
        a.environment_id.as_str(),
        big(a.epoch, "attempt epoch")?,
        a.status.name(),
        flag(a.status.is_terminal()),
        to_json(a)?
    ])?;
    Ok(())
}

fn update_attempt_row(c: &Connection, a: &InvocationAttempt) -> Result<(), RepoError> {
    let n = c
        .prepare_cached(
            "UPDATE attempts SET status = ?1, terminal = ?2, body = ?3 WHERE id = ?4 AND terminal = 0",
        )?
        .execute(params![
            a.status.name(),
            flag(a.status.is_terminal()),
            to_json(a)?,
            a.id.as_str()
        ])?;
    if n == 1 {
        Ok(())
    } else {
        Err(lost_race("attempt", &a.id))
    }
}

fn insert_environment_row(c: &Connection, e: &ExecutionEnvironment) -> Result<(), RepoError> {
    let k = &e.reuse_key;
    c.prepare_cached(
        "INSERT INTO environments (id, tenant_id, revision_id, provider, state, terminal, epoch, \
         execution_role_version, configuration_version, resource_profile_digest, runtime_profile, \
         network_policy_version, secret_binding_generation, idle_since, created_at, body, owner_id, \
         fenced) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
    )?
    .execute(params![
        e.id.as_str(),
        e.tenant_id.as_str(),
        e.revision_id.as_str(),
        e.provider.as_str(),
        e.state.name(),
        flag(e.is_terminal()),
        big(e.epoch, "environment epoch")?,
        bits(k.execution_role_version),
        bits(k.configuration_version),
        k.resource_profile_digest,
        k.runtime_profile,
        bits(k.network_policy_version),
        bits(k.secret_binding_generation),
        e.idle_since.as_ref().map(ts),
        ts(&e.created_at),
        to_json(e)?,
        e.owner.as_ref().map(|d| d.as_str()),
        flag(e.is_fenced())
    ])?;
    Ok(())
}

/// CAS on the epoch the caller read, the terminal flag and (optionally) the
/// state. Returns whether the row was written.
fn cas_environment_row(
    c: &Connection,
    e: &ExecutionEnvironment,
    expected_epoch: u64,
    expected_state: Option<&str>,
) -> Result<bool, RepoError> {
    let n = c
        .prepare_cached(
            "UPDATE environments SET state = ?1, terminal = ?2, epoch = ?3, idle_since = ?4, body = ?5, \
             fenced = ?9 WHERE id = ?6 AND epoch = ?7 AND terminal = 0 AND (?8 IS NULL OR state = ?8)",
        )?
        .execute(params![
            e.state.name(),
            flag(e.is_terminal()),
            big(e.epoch, "environment epoch")?,
            e.idle_since.as_ref().map(ts),
            to_json(e)?,
            e.id.as_str(),
            big(expected_epoch, "environment epoch")?,
            expected_state,
            flag(e.is_fenced())
        ])?;
    Ok(n == 1)
}

fn update_environment_row(
    c: &Connection,
    e: &ExecutionEnvironment,
    expected_epoch: u64,
    expected_state: Option<&str>,
) -> Result<(), RepoError> {
    if cas_environment_row(c, e, expected_epoch, expected_state)? {
        Ok(())
    } else {
        Err(lost_race("environment", &e.id))
    }
}

fn insert_lease_row(c: &Connection, l: &ExecutionLease) -> Result<(), RepoError> {
    c.prepare_cached(
        "INSERT INTO leases (id, environment_id, attempt_id, tenant_id, epoch, deadline, released, body, \
         owner_id, expires_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?
    .execute(params![
        l.id.as_str(),
        l.environment_id.as_str(),
        l.attempt_id.as_str(),
        l.tenant_id.as_str(),
        big(l.epoch, "lease epoch")?,
        ts(&l.deadline),
        flag(l.released_at.is_some()),
        to_json(l)?,
        l.owner.as_ref().map(|d| d.as_str()),
        l.expires_at.as_ref().map(ts)
    ])?;
    Ok(())
}

/// CAS on the released flag.
fn update_lease_row(c: &Connection, l: &ExecutionLease) -> Result<(), RepoError> {
    write_lease_row(c, l, false)
}

/// CAS on the released flag. `reclaimed` marks a release by another
/// dispatcher after the lease expired.
fn write_lease_row(c: &Connection, l: &ExecutionLease, reclaimed: bool) -> Result<(), RepoError> {
    let n = c
        .prepare_cached(
            "UPDATE leases SET deadline = ?1, released = ?2, body = ?3, expires_at = ?5, \
             reclaimed = ?6 WHERE id = ?4 AND released = 0",
        )?
        .execute(params![
            ts(&l.deadline),
            flag(l.released_at.is_some()),
            to_json(l)?,
            l.id.as_str(),
            l.expires_at.as_ref().map(ts),
            flag(reclaimed)
        ])?;
    if n == 1 {
        Ok(())
    } else {
        Err(lost_race("lease", &l.id))
    }
}

const REUSE_KEY_MATCH: &str = "state = 'idle' AND tenant_id = ?1 AND revision_id = ?2 \
     AND execution_role_version = ?3 AND configuration_version = ?4 \
     AND resource_profile_digest = ?5 AND runtime_profile = ?6 \
     AND network_policy_version = ?7 AND secret_binding_generation = ?8";

fn reuse_key_params(k: &ReuseKey) -> Result<[rusqlite::types::Value; 8], RepoError> {
    use rusqlite::types::Value;
    Ok([
        Value::Text(k.tenant_id.to_string()),
        Value::Text(k.revision_id.to_string()),
        Value::Integer(bits(k.execution_role_version)),
        Value::Integer(bits(k.configuration_version)),
        Value::Text(k.resource_profile_digest.clone()),
        Value::Text(k.runtime_profile.clone()),
        Value::Integer(bits(k.network_policy_version)),
        Value::Integer(bits(k.secret_binding_generation)),
    ])
}

// ---------------------------------------------------------------------------
// repositories
// ---------------------------------------------------------------------------

impl StateStore for SqliteStore {
    fn backend(&self) -> &'static str {
        "sqlite"
    }

    fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn flush(&self) -> Result<(), RepoError> {
        if self.path.is_some() {
            self.read(|c| {
                c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn purge_expired_outputs(&self, now: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            let expired: Vec<Invocation> = bodies(
                tx,
                "SELECT body FROM invocations WHERE output_expires_at IS NOT NULL \
                 AND output_expires_at <= ?1",
                [ts(&now)],
            )?;
            let mut n = 0;
            for mut inv in expired {
                if let Some(PayloadRef::Inline {
                    bytes_base64,
                    size_bytes,
                }) = &inv.output
                {
                    use base64::Engine as _;
                    let digest =
                        match base64::engine::general_purpose::STANDARD.decode(bytes_base64) {
                            Ok(bytes) => Sha256Digest::of_bytes(&bytes),
                            Err(_) => Sha256Digest::of_bytes(bytes_base64.as_bytes()),
                        };
                    inv.output = Some(PayloadRef::Digest {
                        digest,
                        size_bytes: *size_bytes,
                    });
                }
                tx.execute(
                    "UPDATE invocations SET output_kind = ?1, output_expires_at = NULL, body = ?2 \
                     WHERE id = ?3",
                    params![output_kind(&inv), to_json(&inv)?, inv.id.as_str()],
                )?;
                n += 1;
            }
            Ok(n)
        })
    }
}

impl FunctionRepository for SqliteStore {
    fn insert(&self, function: Function) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "functions", function.id.as_str())? {
                return Err(duplicate("function", &function.id));
            }
            if !function.is_deleted() {
                let live: Option<i64> = tx
                    .prepare_cached(
                        "SELECT 1 FROM functions WHERE tenant_id = ?1 AND live_name = ?2",
                    )?
                    .query_row([function.tenant_id.as_str(), function.name.as_str()], |r| {
                        r.get(0)
                    })
                    .optional()?;
                if live.is_some() {
                    return Err(RepoError::Conflict(format!(
                        "function name `{}` already exists",
                        function.name
                    )));
                }
            }
            insert_function_row(tx, &function)
        })
    }

    fn get(&self, id: &FunctionId) -> Result<Option<Function>, RepoError> {
        self.read(|c| get_function(c, id))
    }

    fn find_by_name(
        &self,
        tenant: &TenantId,
        name: &FunctionName,
    ) -> Result<Option<Function>, RepoError> {
        self.read(|c| {
            body(
                c,
                "SELECT body FROM functions WHERE tenant_id = ?1 AND live_name = ?2",
                [tenant.as_str(), name.as_str()],
            )
        })
    }

    fn list(&self, tenant: &TenantId) -> Result<Vec<Function>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM functions WHERE tenant_id = ?1 ORDER BY created_at, id",
                [tenant.as_str()],
            )
        })
    }

    fn update(&self, function: Function) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_function(tx, &function.id)? else {
                return Err(RepoError::NotFound(format!("function {}", function.id)));
            };
            if guard::function_update(&old, &function)? == Write::Apply {
                tx.execute(
                    "UPDATE functions SET live_name = ?1, body = ?2 WHERE id = ?3",
                    params![
                        (!function.is_deleted()).then(|| function.name.as_str()),
                        to_json(&function)?,
                        function.id.as_str()
                    ],
                )?;
            }
            Ok(())
        })
    }
}

impl RevisionRepository for SqliteStore {
    fn allocate_number(&self, function: &FunctionId) -> Result<u64, RepoError> {
        self.write(|tx| {
            let current: Option<i64> = tx
                .query_row(
                    "SELECT last_number FROM revision_counters WHERE function_id = ?1",
                    [function.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            let next = match current {
                None => {
                    tx.execute(
                        "INSERT INTO revision_counters (function_id, last_number) VALUES (?1, 1)",
                        [function.as_str()],
                    )?;
                    1
                }
                Some(n) => {
                    let updated = tx.execute(
                        "UPDATE revision_counters SET last_number = ?1 \
                         WHERE function_id = ?2 AND last_number = ?3",
                        params![n + 1, function.as_str(), n],
                    )?;
                    if updated != 1 {
                        return Err(lost_race("revision counter of", function));
                    }
                    n + 1
                }
            };
            u64::try_from(next).map_err(|_| RepoError::Store("negative revision counter".into()))
        })
    }

    fn insert(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "revisions", revision.id.as_str())? {
                return Err(duplicate("revision", &revision.id));
            }
            guard::revision_insert(get_function(tx, &revision.function_id)?.as_ref(), &revision)?;
            insert_revision_row(tx, &revision).map_err(|e| match e {
                RepoError::Conflict(_) => RepoError::Conflict(format!(
                    "revision number {} already exists for function {}",
                    revision.number, revision.function_id
                )),
                other => other,
            })
        })
    }

    fn get(&self, id: &RevisionId) -> Result<Option<FunctionRevision>, RepoError> {
        self.read(|c| get_revision(c, id))
    }

    fn list_by_function(&self, function: &FunctionId) -> Result<Vec<FunctionRevision>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM revisions WHERE function_id = ?1 ORDER BY number",
                [function.as_str()],
            )
        })
    }

    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_revision(tx, &revision.id)? else {
                return Err(RepoError::NotFound(format!("revision {}", revision.id)));
            };
            if guard::revision_update(&old, &revision)? == Write::Apply {
                let n = tx.execute(
                    "UPDATE revisions SET status = ?1, body = ?2 WHERE id = ?3 AND status = ?4",
                    params![
                        revision.status.name(),
                        to_json(&revision)?,
                        revision.id.as_str(),
                        old.status.name()
                    ],
                )?;
                if n != 1 {
                    return Err(lost_race("revision", &revision.id));
                }
            }
            Ok(())
        })
    }
}

impl AliasRepository for SqliteStore {
    fn get(
        &self,
        function: &FunctionId,
        name: &AliasName,
    ) -> Result<Option<FunctionAlias>, RepoError> {
        self.read(|c| get_alias(c, function, name))
    }

    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM aliases WHERE function_id = ?1 ORDER BY name",
                [function.as_str()],
            )
        })
    }

    fn insert(&self, alias: FunctionAlias) -> Result<(), RepoError> {
        self.write(|tx| {
            if get_alias(tx, &alias.function_id, &alias.name)?.is_some() {
                return Err(RepoError::Conflict(format!(
                    "alias `{}` already exists",
                    alias.name
                )));
            }
            guard::alias_target(
                get_function(tx, &alias.function_id)?.as_ref(),
                get_revision(tx, &alias.revision_id)?.as_ref(),
                &alias,
            )?;
            insert_alias_row(tx, &alias)
        })
    }

    fn compare_and_set(
        &self,
        alias: FunctionAlias,
        expected_generation: u64,
    ) -> Result<bool, RepoError> {
        self.write(|tx| {
            let Some(current) = get_alias(tx, &alias.function_id, &alias.name)? else {
                return Err(RepoError::NotFound(format!("alias `{}`", alias.name)));
            };
            if current.generation != expected_generation {
                return Ok(false);
            }
            guard::alias_update(&current, &alias)?;
            guard::alias_target(
                get_function(tx, &alias.function_id)?.as_ref(),
                get_revision(tx, &alias.revision_id)?.as_ref(),
                &alias,
            )?;
            let n = tx
                .prepare_cached(
                    "UPDATE aliases SET revision_id = ?1, generation = ?2, updated_at = ?3, body = ?4 \
                     WHERE function_id = ?5 AND name = ?6 AND generation = ?7",
                )?
                .execute(params![
                    alias.revision_id.as_str(),
                    big(alias.generation, "alias generation")?,
                    ts(&alias.updated_at),
                    to_json(&alias)?,
                    alias.function_id.as_str(),
                    alias.name.as_str(),
                    big(expected_generation, "alias generation")?
                ])?;
            Ok(n == 1)
        })
    }
}

fn insert_invocation_checked(
    tx: &Connection,
    invocation: &Invocation,
    max_inline: u64,
    retention: Retention,
) -> Result<(), RepoError> {
    if exists(tx, "invocations", invocation.id.as_str())? {
        return Err(duplicate("invocation", &invocation.id));
    }
    guard::invocation_insert(
        get_function(tx, &invocation.function_id)?.as_ref(),
        invocation,
        max_inline,
    )?;
    insert_invocation_row(tx, invocation, retention)
}

impl InvocationRepository for SqliteStore {
    fn insert(&self, invocation: Invocation) -> Result<(), RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| insert_invocation_checked(tx, &invocation, max, retention))
    }

    fn get(&self, id: &InvocationId) -> Result<Option<Invocation>, RepoError> {
        self.read(|c| get_invocation(c, id))
    }

    fn update(&self, invocation: Invocation) -> Result<(), RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let Some(old) = get_invocation(tx, &invocation.id)? else {
                return Err(RepoError::NotFound(format!("invocation {}", invocation.id)));
            };
            if guard::invocation_update(&old, &invocation, max)? == Write::Apply {
                update_invocation_row(tx, &invocation, retention)?;
            }
            Ok(())
        })
    }

    fn list_by_function(
        &self,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<Invocation>, RepoError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM invocations WHERE function_id = ?1 \
                 ORDER BY accepted_at DESC, id DESC LIMIT ?2",
                params![function.as_str(), limit],
            )
        })
    }

    fn insert_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "attempts", attempt.id.as_str())? {
                return Err(duplicate("attempt", &attempt.id));
            }
            guard::attempt_insert(
                get_invocation(tx, &attempt.invocation_id)?.as_ref(),
                &attempt,
            )?;
            insert_attempt_row(tx, &attempt)
        })
    }

    fn get_attempt(&self, id: &AttemptId) -> Result<Option<InvocationAttempt>, RepoError> {
        self.read(|c| get_attempt(c, id))
    }

    fn update_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_attempt(tx, &attempt.id)? else {
                return Err(RepoError::NotFound(format!("attempt {}", attempt.id)));
            };
            if guard::attempt_update(&old, &attempt)? == Write::Apply {
                update_attempt_row(tx, &attempt)?;
            }
            Ok(())
        })
    }

    fn attempts_of(&self, invocation: &InvocationId) -> Result<Vec<InvocationAttempt>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM attempts WHERE invocation_id = ?1 ORDER BY number, id",
                [invocation.as_str()],
            )
        })
    }
}

impl EnvironmentRepository for SqliteStore {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "environments", env.id.as_str())? {
                return Err(duplicate("environment", &env.id));
            }
            guard::environment_insert(get_revision(tx, &env.revision_id)?.as_ref(), &env)?;
            insert_environment_row(tx, &env)
        })
    }

    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError> {
        self.read(|c| get_environment(c, id))
    }

    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_environment(tx, &env.id)? else {
                return Err(RepoError::NotFound(format!("environment {}", env.id)));
            };
            if guard::environment_update(&old, &env)? == Write::Apply {
                update_environment_row(tx, &env, old.epoch, None)?;
            }
            Ok(())
        })
    }

    fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM environments WHERE terminal = 0 ORDER BY id",
                [],
            )
        })
    }
}

impl LogRepository for SqliteStore {
    fn append(&self, record: LogRecord) -> AppendOutcome {
        self.logs.append(&self.limits, record)
    }

    fn query(&self, invocation: &InvocationId) -> LogQuery {
        self.logs.query(invocation)
    }
}

/// The binding of `key` whose invocation exists and that has not expired at
/// `now`.
fn live_binding(
    c: &Connection,
    tenant: &TenantId,
    function: &FunctionId,
    key: &str,
    now: Timestamp,
) -> Result<Option<IdempotencyBinding>, RepoError> {
    let row: Option<(String, String, Option<String>)> = c
        .prepare_cached(
            "SELECT i.invocation_id, i.input_digest, i.expires_at FROM idempotency i \
             JOIN invocations v ON v.id = i.invocation_id \
             WHERE i.tenant_id = ?1 AND i.function_id = ?2 AND i.idem_key = ?3 \
             AND (i.expires_at IS NULL OR i.expires_at > ?4)",
        )?
        .query_row(
            params![tenant.as_str(), function.as_str(), key, ts(&now)],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    row.map(|(inv, digest, expires)| {
        Ok(IdempotencyBinding {
            invocation_id: InvocationId::parse(&inv)
                .map_err(|e| RepoError::Serialization(e.to_string()))?,
            input_digest: Sha256Digest::parse(&digest)
                .map_err(|e| RepoError::Serialization(e.to_string()))?,
            expires_at: expires
                .map(|t| {
                    chrono::DateTime::parse_from_rfc3339(&t)
                        .map(|t| t.with_timezone(&chrono::Utc))
                        .map_err(|e| RepoError::Serialization(e.to_string()))
                })
                .transpose()?,
        })
    })
    .transpose()
}

/// Bind the invocation's key (if any) to it. A binding left without its
/// invocation, or expired, is stale and replaced; the caller checked
/// [`live_binding`] in the same transaction. The primary key keeps the binding
/// unique across every process on this file.
fn bind_idempotency(
    tx: &Connection,
    invocation: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    if let Some(key) = &invocation.idempotency_key {
        tx.execute(
            "DELETE FROM idempotency WHERE tenant_id = ?1 AND function_id = ?2 AND idem_key = ?3",
            params![
                invocation.tenant_id.as_str(),
                invocation.function_id.as_str(),
                key
            ],
        )?;
        tx.execute(
            "INSERT INTO idempotency (tenant_id, function_id, idem_key, invocation_id, input_digest, \
             expires_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                invocation.tenant_id.as_str(),
                invocation.function_id.as_str(),
                key,
                invocation.id.as_str(),
                invocation.input_digest.as_str(),
                idempotency_expires_at(invocation, retention)
            ],
        )?;
    }
    Ok(())
}

impl IdempotencyRepository for SqliteStore {
    fn lookup(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
        now: Timestamp,
    ) -> Result<Option<IdempotencyBinding>, RepoError> {
        self.read(|c| live_binding(c, tenant, function, key, now))
    }

    fn insert_bound(&self, invocation: Invocation) -> Result<IdempotencyOutcome, RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            if let Some(key) = &invocation.idempotency_key
                && let Some(existing) = live_binding(
                    tx,
                    &invocation.tenant_id,
                    &invocation.function_id,
                    key,
                    invocation.accepted_at,
                )?
            {
                return Ok(IdempotencyOutcome::Existing(existing));
            }
            insert_invocation_checked(tx, &invocation, max, retention)?;
            bind_idempotency(tx, &invocation, retention)?;
            Ok(IdempotencyOutcome::Inserted)
        })
    }

    fn purge_expired_idempotency(&self, now: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            Ok(tx.execute(
                "DELETE FROM idempotency WHERE expires_at IS NOT NULL AND expires_at <= ?1",
                [ts(&now)],
            )?)
        })
    }
}

impl ArtifactOwnerRepository for SqliteStore {
    fn claim(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<(), RepoError> {
        self.write(|tx| {
            let owned: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM artifact_owners WHERE digest = ?1 AND tenant_id = ?2",
                    [digest.as_str(), tenant.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            if owned.is_none() {
                tx.execute(
                    "INSERT INTO artifact_owners (digest, tenant_id) VALUES (?1, ?2)",
                    [digest.as_str(), tenant.as_str()],
                )?;
            }
            Ok(())
        })
    }

    fn is_owned_by(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<bool, RepoError> {
        self.read(|c| {
            let owned: Option<i64> = c
                .query_row(
                    "SELECT 1 FROM artifact_owners WHERE digest = ?1 AND tenant_id = ?2",
                    [digest.as_str(), tenant.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(owned.is_some())
        })
    }
}

#[cfg(test)]
mod tests;

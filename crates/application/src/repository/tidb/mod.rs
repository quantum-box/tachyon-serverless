//! TiDB (MySQL protocol) adapter of the repository ports, **test-only**
//! (docs/adr/0003 「TiDB 検証」, PLT-4618).
//!
//! The production store stays [`super::SqliteStore`]. This adapter exists to
//! prove on a real TiDB that the schema (`migrations/`, mirroring the SQLite
//! migrations 001–008) and the repository contract (`contract_tests.rs`,
//! instantiated as `repository::contract_tests::tidb::*` when
//! `TSLS_TIDB_URL` is set) hold without SQLite's database-wide write lock.
//! It is compiled only for tests and is not reachable from `[store]`: it
//! does not implement the asynchronous-invocation outbox or the trigger
//! repository the gateway also needs.
//!
//! # Transactions
//!
//! - Every connection runs `tidb_txn_mode = 'pessimistic'` at
//!   `READ-COMMITTED` (TiDB allows RC only in pessimistic mode), so each
//!   statement reads the latest committed data and `SELECT ... FOR UPDATE`
//!   takes row locks that are held to commit. Rows a write reads to decide
//!   are read `FOR UPDATE`.
//! - **At READ-COMMITTED a `FOR UPDATE` on a key that has no row takes no
//!   lock** (measured on v8.5.8: an insert of that key from another session
//!   does not wait; at REPEATABLE-READ it waits). "Check, then insert" is
//!   therefore never trusted to be serialized by the check: the unique key
//!   decides, and where the loser must see the winner's row (revision
//!   counters, idempotency keys) its duplicate-key error re-runs the whole
//!   transaction, which then reads the committed row. Decisions that need a
//!   mutex without a natural row lock a row that always exists
//!   ([`LOCK_ROWS`] in `store_meta`, created on open) or insert the key they
//!   race on first (collection tombstones, `objects.rs`).
//! - Every CAS is still in the `UPDATE ... WHERE` (epoch, state, terminal,
//!   released, generation, `reclaimed_at IS NULL`), and the connection sets
//!   `CLIENT_FOUND_ROWS`, so "affected rows" means "matched rows" and zero is
//!   a lost race, never "matched but unchanged".
//! - Unique keys are the last line: a duplicate key (MySQL error 1062) is
//!   [`RepoError::Conflict`].
//! - Deadlocks (1213), write conflicts (9007), lock-wait timeouts (1205) and
//!   schema-changed errors (8028) roll the transaction back and re-run the
//!   whole closure, up to [`MAX_ATTEMPTS`] times with a short backoff. Every
//!   closure re-reads what it needs, so a retry never reuses stale reads.

use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{Opts, OptsBuilder, Params, Pool, PoolConstraints, PoolOpts};
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
use super::sqlite::{Retention, SqliteOptions, ts};
use super::{
    AliasRepository, AppendOutcome, ArtifactOwnerRepository, EnvironmentRepository,
    FunctionRepository, IdempotencyBinding, IdempotencyOutcome, IdempotencyRepository,
    InvocationRepository, LogQuery, LogRepository, RepoError, RevisionRepository, StateStore,
};

mod config;
pub mod migrations;
mod objects;
mod slot;

mod tests;

/// Positional parameters: `p![a, b, c]`.
macro_rules! p {
    () => {
        mysql::Params::Empty
    };
    ($($v:expr),+ $(,)?) => {
        mysql::Params::Positional(vec![$(mysql::Value::from($v)),+])
    };
}
pub(crate) use p;

/// How often a transaction that hit a deadlock or write conflict is re-run.
pub const MAX_ATTEMPTS: usize = 12;

const RETRYABLE: &str = "tidb retryable: ";

/// `store_meta` rows that exist only to be locked `FOR UPDATE`: the pool caps
/// (`slot.rs`) and the configuration stamping (`config.rs`).
pub const LOCK_ROWS: &[&str] = &[POOL_LOCK, CONFIG_LOCK];
pub(crate) const POOL_LOCK: &str = "lock:pool_release";
pub(crate) const CONFIG_LOCK: &str = "lock:config_stamp";

/// A duplicate key the transaction must re-run on (the loser of a
/// check-then-insert race re-reads the winner's committed row).
fn retry_on_duplicate(e: RepoError) -> RepoError {
    match e {
        RepoError::Conflict(m) => RepoError::Store(format!("{RETRYABLE}{m}")),
        other => other,
    }
}

/// MySQL / TiDB error codes that mean "roll back and run the transaction
/// again": deadlock, lock wait timeout, TiDB write conflict, schema changed.
const RETRYABLE_CODES: &[u16] = &[1213, 1205, 9007, 8028, 8022];

impl From<mysql::Error> for RepoError {
    fn from(e: mysql::Error) -> Self {
        match &e {
            mysql::Error::MySqlError(m) if m.code == 1062 => RepoError::Conflict(e.to_string()),
            mysql::Error::MySqlError(m) if RETRYABLE_CODES.contains(&m.code) => {
                RepoError::Store(format!("{RETRYABLE}{e}"))
            }
            _ => RepoError::Store(e.to_string()),
        }
    }
}

fn is_retryable(e: &RepoError) -> bool {
    matches!(e, RepoError::Store(m) if m.starts_with(RETRYABLE))
}

fn to_json<T: Serialize>(v: &T) -> Result<String, RepoError> {
    serde_json::to_string(v).map_err(|e| RepoError::Serialization(e.to_string()))
}

fn from_json<T: DeserializeOwned>(s: &str) -> Result<T, RepoError> {
    serde_json::from_str(s).map_err(|e| RepoError::Serialization(e.to_string()))
}

fn flag(b: bool) -> i64 {
    i64::from(b)
}

fn lock(sql: &str, lock: bool) -> String {
    if lock {
        format!("{sql} FOR UPDATE")
    } else {
        sql.to_string()
    }
}

/// Run a statement and return the rows it matched (`CLIENT_FOUND_ROWS`).
fn exec_count<Q: Queryable>(c: &mut Q, sql: &str, params: Params) -> Result<u64, RepoError> {
    let result = c.exec_iter(sql, params)?;
    Ok(result.affected_rows())
}

pub struct TidbStore {
    pool: Pool,
    database: String,
    /// Root URL (no database) used to drop an ephemeral database on drop.
    admin: Option<Opts>,
    logs: LogBuffer,
    limits: Limits,
    options: SqliteOptions,
    migrations_applied: Vec<i64>,
}

impl std::fmt::Debug for TidbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TidbStore")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

fn session_opts(base: &Opts, database: Option<&str>, max_conns: usize) -> Result<Opts, RepoError> {
    let constraints = PoolConstraints::new(1, max_conns.max(1))
        .ok_or_else(|| RepoError::Store("invalid pool constraints".into()))?;
    let builder = OptsBuilder::from_opts(base.clone())
        .db_name(database)
        .additional_capabilities(mysql::consts::CapabilityFlags::CLIENT_FOUND_ROWS)
        .stmt_cache_size(Some(256))
        .tcp_connect_timeout(Some(Duration::from_secs(10)))
        .pool_opts(PoolOpts::default().with_constraints(constraints))
        .init(vec![
            "SET SESSION tidb_txn_mode = 'pessimistic'",
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
            "SET SESSION time_zone = '+00:00'",
        ]);
    Ok(builder.into())
}

/// The URL of the test cluster, e.g. `mysql://root@127.0.0.1:4000`.
pub fn test_url() -> Option<String> {
    std::env::var("TSLS_TIDB_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
}

impl TidbStore {
    /// Connect to `url`, create `database` if needed, migrate it and apply
    /// the open-time backfills and purges.
    pub fn open(
        url: &str,
        database: &str,
        limits: Limits,
        options: SqliteOptions,
        now: Timestamp,
    ) -> Result<Self, RepoError> {
        Self::open_with(url, database, limits, options, now, migrations::MIGRATIONS)
    }

    pub(crate) fn open_with(
        url: &str,
        database: &str,
        limits: Limits,
        options: SqliteOptions,
        now: Timestamp,
        set: &[migrations::Migration],
    ) -> Result<Self, RepoError> {
        if !database
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(RepoError::Refused(format!(
                "database name `{database}` must be [A-Za-z0-9_]"
            )));
        }
        let base = Opts::from_url(url).map_err(|e| RepoError::Store(e.to_string()))?;
        {
            let mut admin = mysql::Conn::new(session_opts(&base, None, 1)?)?;
            admin.query_drop(format!(
                "CREATE DATABASE IF NOT EXISTS `{database}` \
                 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
            ))?;
        }
        let pool = Pool::new(session_opts(&base, Some(database), 32)?)?;
        let applied = {
            let mut conn = pool.get_conn()?;
            let target = set.last().map(|m| m.version).unwrap_or(0);
            migrations::migrate_to(&mut conn, set, target, now)?
        };
        let store = Self {
            pool,
            database: database.to_string(),
            admin: None,
            logs: LogBuffer::default(),
            limits,
            options,
            migrations_applied: applied,
        };
        {
            let mut conn = store.pool.get_conn()?;
            for key in LOCK_ROWS {
                conn.exec_drop(
                    "INSERT IGNORE INTO store_meta (meta_key, meta_value) VALUES (?, '')",
                    p![*key],
                )?;
            }
        }
        if set.len() >= migrations::MIGRATIONS.len() {
            store.backfill_output_expiry()?;
            store.backfill_idempotency_expiry()?;
            store.purge_expired_outputs(now)?;
            store.purge_expired_idempotency(now)?;
        }
        Ok(store)
    }

    /// A migrated database of its own, dropped when the store is dropped
    /// (tests).
    pub fn open_ephemeral(url: &str, limits: Limits) -> Result<Self, RepoError> {
        let database = format!(
            "tsls_t_{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        let mut store = Self::open(
            url,
            &database,
            limits,
            SqliteOptions::default(),
            chrono::Utc::now(),
        )?;
        store.admin = Some(Opts::from_url(url).map_err(|e| RepoError::Store(e.to_string()))?);
        Ok(store)
    }

    pub fn migrations_applied(&self) -> &[i64] {
        &self.migrations_applied
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    fn read<R>(
        &self,
        f: impl FnOnce(&mut mysql::PooledConn) -> Result<R, RepoError>,
    ) -> Result<R, RepoError> {
        let mut conn = self.pool.get_conn()?;
        f(&mut conn)
    }

    /// One pessimistic READ-COMMITTED transaction, re-run on deadlock or
    /// write conflict. An error rolls everything back.
    fn write<R>(
        &self,
        mut f: impl FnMut(&mut mysql::Transaction<'_>) -> Result<R, RepoError>,
    ) -> Result<R, RepoError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = (|| {
                let mut conn = self.pool.get_conn()?;
                let mut tx = conn.start_transaction(mysql::TxOpts::default())?;
                let out = f(&mut tx)?;
                tx.commit()?;
                Ok(out)
            })();
            match result {
                Err(e) if is_retryable(&e) && attempt < MAX_ATTEMPTS => {
                    std::thread::sleep(Duration::from_millis(5 * attempt as u64));
                }
                other => return other,
            }
        }
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

    fn backfill_output_expiry(&self) -> Result<(), RepoError> {
        let Some(retention) = self.retention().output else {
            return Ok(());
        };
        self.write(|tx| {
            let rows: Vec<Invocation> = bodies(
                tx,
                "SELECT body FROM invocations WHERE terminal = 1 AND output_kind = 'inline' \
                 AND output_expires_at IS NULL FOR UPDATE",
                p![],
            )?;
            for inv in rows {
                tx.exec_drop(
                    "UPDATE invocations SET output_expires_at = ? WHERE id = ?",
                    p![output_expires_at(&inv, Some(retention)), inv.id.as_str()],
                )?;
            }
            Ok(())
        })
    }

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
                p![],
            )?;
            for inv in rows {
                set_idempotency_expiry(tx, &inv, retention)?;
            }
            Ok(())
        })
    }
}

impl Drop for TidbStore {
    fn drop(&mut self) {
        if let Some(admin) = self.admin.take()
            && let Ok(opts) = session_opts(&admin, None, 1)
            && let Ok(mut conn) = mysql::Conn::new(opts)
        {
            let _ = conn.query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.database));
        }
    }
}

// ---------------------------------------------------------------------------
// row helpers
// ---------------------------------------------------------------------------

fn body<Q: Queryable, T: DeserializeOwned>(
    c: &mut Q,
    sql: &str,
    params: Params,
) -> Result<Option<T>, RepoError> {
    let raw: Option<String> = c.exec_first(sql, params)?;
    raw.map(|s| from_json(&s)).transpose()
}

fn bodies<Q: Queryable, T: DeserializeOwned>(
    c: &mut Q,
    sql: &str,
    params: Params,
) -> Result<Vec<T>, RepoError> {
    let raw: Vec<String> = c.exec(sql, params)?;
    raw.iter().map(|s| from_json(s)).collect()
}

fn get_function<Q: Queryable>(
    c: &mut Q,
    id: &FunctionId,
    locked: bool,
) -> Result<Option<Function>, RepoError> {
    body(
        c,
        &lock("SELECT body FROM functions WHERE id = ?", locked),
        p![id.as_str()],
    )
}

fn get_revision<Q: Queryable>(
    c: &mut Q,
    id: &RevisionId,
    locked: bool,
) -> Result<Option<FunctionRevision>, RepoError> {
    body(
        c,
        &lock("SELECT body FROM revisions WHERE id = ?", locked),
        p![id.as_str()],
    )
}

fn get_alias<Q: Queryable>(
    c: &mut Q,
    function: &FunctionId,
    name: &AliasName,
    locked: bool,
) -> Result<Option<FunctionAlias>, RepoError> {
    body(
        c,
        &lock(
            "SELECT body FROM aliases WHERE function_id = ? AND name = ?",
            locked,
        ),
        p![function.as_str(), name.as_str()],
    )
}

pub(super) fn get_invocation<Q: Queryable>(
    c: &mut Q,
    id: &InvocationId,
    locked: bool,
) -> Result<Option<Invocation>, RepoError> {
    body(
        c,
        &lock("SELECT body FROM invocations WHERE id = ?", locked),
        p![id.as_str()],
    )
}

fn get_attempt<Q: Queryable>(
    c: &mut Q,
    id: &AttemptId,
    locked: bool,
) -> Result<Option<InvocationAttempt>, RepoError> {
    body(
        c,
        &lock("SELECT body FROM attempts WHERE id = ?", locked),
        p![id.as_str()],
    )
}

fn get_environment<Q: Queryable>(
    c: &mut Q,
    id: &EnvironmentId,
    locked: bool,
) -> Result<Option<ExecutionEnvironment>, RepoError> {
    body(
        c,
        &lock("SELECT body FROM environments WHERE id = ?", locked),
        p![id.as_str()],
    )
}

fn get_lease<Q: Queryable>(
    c: &mut Q,
    id: &LeaseId,
    locked: bool,
) -> Result<Option<ExecutionLease>, RepoError> {
    body(
        c,
        &lock("SELECT body FROM leases WHERE id = ?", locked),
        p![id.as_str()],
    )
}

fn exists<Q: Queryable>(c: &mut Q, table: &str, id: &str) -> Result<bool, RepoError> {
    let hit: Option<i64> = c.exec_first(format!("SELECT 1 FROM {table} WHERE id = ?"), p![id])?;
    Ok(hit.is_some())
}

fn duplicate(kind: &str, id: impl std::fmt::Display) -> RepoError {
    RepoError::Conflict(format!("{kind} {id} already exists"))
}

fn lost_race(kind: &str, id: impl std::fmt::Display) -> RepoError {
    RepoError::Refused(format!("{kind} {id} changed concurrently"))
}

fn insert_function_row<Q: Queryable>(c: &mut Q, f: &Function) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO functions (id, tenant_id, name, live_name, created_at, body) \
         VALUES (?, ?, ?, ?, ?, ?)",
        p![
            f.id.as_str(),
            f.tenant_id.as_str(),
            f.name.as_str(),
            (!f.is_deleted()).then(|| f.name.as_str()),
            ts(&f.created_at),
            to_json(f)?
        ],
    )?;
    Ok(())
}

fn insert_revision_row<Q: Queryable>(c: &mut Q, r: &FunctionRevision) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO revisions (id, function_id, tenant_id, number, status, spec_digest, \
         created_at, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        p![
            r.id.as_str(),
            r.function_id.as_str(),
            r.tenant_id.as_str(),
            r.number,
            r.status.name(),
            r.spec_digest.as_str(),
            ts(&r.created_at),
            to_json(r)?
        ],
    )?;
    Ok(())
}

fn insert_alias_row<Q: Queryable>(c: &mut Q, a: &FunctionAlias) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO aliases (function_id, name, tenant_id, revision_id, generation, updated_at, \
         body) VALUES (?, ?, ?, ?, ?, ?, ?)",
        p![
            a.function_id.as_str(),
            a.name.as_str(),
            a.tenant_id.as_str(),
            a.revision_id.as_str(),
            a.generation,
            ts(&a.updated_at),
            to_json(a)?
        ],
    )?;
    Ok(())
}

fn output_kind(inv: &Invocation) -> Option<&'static str> {
    inv.output.as_ref().map(|o| match o {
        PayloadRef::Inline { .. } => "inline",
        PayloadRef::Digest { .. } => "digest",
    })
}

fn idempotency_expires_at(inv: &Invocation, retention: Retention) -> Option<String> {
    let retention = retention.idempotency?;
    if !inv.status.is_terminal() {
        return None;
    }
    let finished = inv.finished_at.unwrap_or(inv.accepted_at);
    Some(ts(&(finished + retention)))
}

fn set_idempotency_expiry<Q: Queryable>(
    c: &mut Q,
    inv: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    if inv.idempotency_key.is_some() {
        c.exec_drop(
            "UPDATE idempotency SET expires_at = ? WHERE invocation_id = ?",
            p![idempotency_expires_at(inv, retention), inv.id.as_str()],
        )?;
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

fn insert_invocation_row<Q: Queryable>(
    c: &mut Q,
    inv: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO invocations (id, tenant_id, function_id, revision_id, status, terminal, \
         accepted_at, finished_at, input_digest, output_kind, output_expires_at, body, owner_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        p![
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
        ],
    )?;
    Ok(())
}

/// CAS: only a row that is not terminal yet is rewritten.
fn update_invocation_row<Q: Queryable>(
    c: &mut Q,
    inv: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    let n = exec_count(
        c,
        "UPDATE invocations SET revision_id = ?, status = ?, terminal = ?, finished_at = ?, \
         output_kind = ?, output_expires_at = ?, body = ? WHERE id = ? AND terminal = 0",
        p![
            inv.revision_id.as_str(),
            inv.status.name(),
            flag(inv.status.is_terminal()),
            inv.finished_at.as_ref().map(ts),
            output_kind(inv),
            output_expires_at(inv, retention.output),
            to_json(inv)?,
            inv.id.as_str()
        ],
    )?;
    if n != 1 {
        return Err(lost_race("invocation", &inv.id));
    }
    if inv.status.is_terminal() {
        set_idempotency_expiry(c, inv, retention)?;
    }
    Ok(())
}

fn insert_attempt_row<Q: Queryable>(c: &mut Q, a: &InvocationAttempt) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO attempts (id, invocation_id, tenant_id, number, environment_id, epoch, \
         status, terminal, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        p![
            a.id.as_str(),
            a.invocation_id.as_str(),
            a.tenant_id.as_str(),
            u64::from(a.number),
            a.environment_id.as_str(),
            a.epoch,
            a.status.name(),
            flag(a.status.is_terminal()),
            to_json(a)?
        ],
    )?;
    Ok(())
}

fn update_attempt_row<Q: Queryable>(c: &mut Q, a: &InvocationAttempt) -> Result<(), RepoError> {
    let n = exec_count(
        c,
        "UPDATE attempts SET status = ?, terminal = ?, body = ? WHERE id = ? AND terminal = 0",
        p![
            a.status.name(),
            flag(a.status.is_terminal()),
            to_json(a)?,
            a.id.as_str()
        ],
    )?;
    if n == 1 {
        Ok(())
    } else {
        Err(lost_race("attempt", &a.id))
    }
}

fn insert_environment_row<Q: Queryable>(
    c: &mut Q,
    e: &ExecutionEnvironment,
) -> Result<(), RepoError> {
    let k = &e.reuse_key;
    c.exec_drop(
        "INSERT INTO environments (id, tenant_id, revision_id, provider, state, terminal, epoch, \
         execution_role_version, configuration_version, resource_profile_digest, \
         runtime_profile, network_policy_version, secret_binding_generation, idle_since, \
         created_at, body, owner_id, fenced) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        p![
            e.id.as_str(),
            e.tenant_id.as_str(),
            e.revision_id.as_str(),
            e.provider.as_str(),
            e.state.name(),
            flag(e.is_terminal()),
            e.epoch,
            k.execution_role_version,
            k.configuration_version,
            k.resource_profile_digest.as_str(),
            k.runtime_profile.as_str(),
            k.network_policy_version,
            k.secret_binding_generation,
            e.idle_since.as_ref().map(ts),
            ts(&e.created_at),
            to_json(e)?,
            e.owner.as_ref().map(|d| d.as_str()),
            flag(e.is_fenced())
        ],
    )?;
    Ok(())
}

/// CAS on the epoch the caller read, the terminal flag and (optionally) the
/// state. Returns whether the row was written.
fn cas_environment_row<Q: Queryable>(
    c: &mut Q,
    e: &ExecutionEnvironment,
    expected_epoch: u64,
    expected_state: Option<&str>,
) -> Result<bool, RepoError> {
    let base = "UPDATE environments SET state = ?, terminal = ?, epoch = ?, idle_since = ?, \
                body = ?, fenced = ? WHERE id = ? AND epoch = ? AND terminal = 0";
    let mut values = vec![
        mysql::Value::from(e.state.name()),
        mysql::Value::from(flag(e.is_terminal())),
        mysql::Value::from(e.epoch),
        mysql::Value::from(e.idle_since.as_ref().map(ts)),
        mysql::Value::from(to_json(e)?),
        mysql::Value::from(flag(e.is_fenced())),
        mysql::Value::from(e.id.as_str()),
        mysql::Value::from(expected_epoch),
    ];
    let sql = match expected_state {
        Some(state) => {
            values.push(mysql::Value::from(state));
            format!("{base} AND state = ?")
        }
        None => base.to_string(),
    };
    Ok(exec_count(c, &sql, Params::Positional(values))? == 1)
}

fn update_environment_row<Q: Queryable>(
    c: &mut Q,
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

fn insert_lease_row<Q: Queryable>(c: &mut Q, l: &ExecutionLease) -> Result<(), RepoError> {
    c.exec_drop(
        "INSERT INTO leases (id, environment_id, attempt_id, tenant_id, epoch, deadline, \
         released, body, owner_id, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        p![
            l.id.as_str(),
            l.environment_id.as_str(),
            l.attempt_id.as_str(),
            l.tenant_id.as_str(),
            l.epoch,
            ts(&l.deadline),
            flag(l.released_at.is_some()),
            to_json(l)?,
            l.owner.as_ref().map(|d| d.as_str()),
            l.expires_at.as_ref().map(ts)
        ],
    )?;
    Ok(())
}

/// CAS on the released flag. `reclaimed` marks a release by another
/// dispatcher after the lease expired.
fn write_lease_row<Q: Queryable>(
    c: &mut Q,
    l: &ExecutionLease,
    reclaimed: bool,
) -> Result<(), RepoError> {
    let n = exec_count(
        c,
        "UPDATE leases SET deadline = ?, released = ?, body = ?, expires_at = ?, reclaimed = ? \
         WHERE id = ? AND released = 0",
        p![
            ts(&l.deadline),
            flag(l.released_at.is_some()),
            to_json(l)?,
            l.expires_at.as_ref().map(ts),
            flag(reclaimed),
            l.id.as_str()
        ],
    )?;
    if n == 1 {
        Ok(())
    } else {
        Err(lost_race("lease", &l.id))
    }
}

const REUSE_KEY_MATCH: &str = "state = 'idle' AND tenant_id = ? AND revision_id = ? \
     AND execution_role_version = ? AND configuration_version = ? \
     AND resource_profile_digest = ? AND runtime_profile = ? \
     AND network_policy_version = ? AND secret_binding_generation = ?";

fn reuse_key_values(k: &ReuseKey) -> Vec<mysql::Value> {
    vec![
        mysql::Value::from(k.tenant_id.as_str()),
        mysql::Value::from(k.revision_id.as_str()),
        mysql::Value::from(k.execution_role_version),
        mysql::Value::from(k.configuration_version),
        mysql::Value::from(k.resource_profile_digest.as_str()),
        mysql::Value::from(k.runtime_profile.as_str()),
        mysql::Value::from(k.network_policy_version),
        mysql::Value::from(k.secret_binding_generation),
    ]
}

// ---------------------------------------------------------------------------
// repositories
// ---------------------------------------------------------------------------

impl StateStore for TidbStore {
    fn backend(&self) -> &'static str {
        "tidb"
    }

    fn purge_expired_outputs(&self, now: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            let expired: Vec<Invocation> = bodies(
                tx,
                "SELECT body FROM invocations WHERE output_expires_at IS NOT NULL \
                 AND output_expires_at <= ? FOR UPDATE",
                p![ts(&now)],
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
                tx.exec_drop(
                    "UPDATE invocations SET output_kind = ?, output_expires_at = NULL, body = ? \
                     WHERE id = ?",
                    p![output_kind(&inv), to_json(&inv)?, inv.id.as_str()],
                )?;
                n += 1;
            }
            Ok(n)
        })
    }
}

impl FunctionRepository for TidbStore {
    fn insert(&self, function: Function) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "functions", function.id.as_str())? {
                return Err(duplicate("function", &function.id));
            }
            if !function.is_deleted() {
                // Serialized by the unique key (tenant_id, live_name); the
                // read only gives the usual message.
                let live: Option<i64> = tx.exec_first(
                    "SELECT 1 FROM functions WHERE tenant_id = ? AND live_name = ? FOR UPDATE",
                    p![function.tenant_id.as_str(), function.name.as_str()],
                )?;
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
        self.read(|c| get_function(c, id, false))
    }

    fn find_by_name(
        &self,
        tenant: &TenantId,
        name: &FunctionName,
    ) -> Result<Option<Function>, RepoError> {
        self.read(|c| {
            body(
                c,
                "SELECT body FROM functions WHERE tenant_id = ? AND live_name = ?",
                p![tenant.as_str(), name.as_str()],
            )
        })
    }

    fn list(&self, tenant: &TenantId) -> Result<Vec<Function>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM functions WHERE tenant_id = ? ORDER BY created_at, id",
                p![tenant.as_str()],
            )
        })
    }

    fn update(&self, function: Function) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_function(tx, &function.id, true)? else {
                return Err(RepoError::NotFound(format!("function {}", function.id)));
            };
            if guard::function_update(&old, &function)? == Write::Apply {
                tx.exec_drop(
                    "UPDATE functions SET live_name = ?, body = ? WHERE id = ?",
                    p![
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

impl RevisionRepository for TidbStore {
    fn allocate_number(&self, function: &FunctionId) -> Result<u64, RepoError> {
        self.write(|tx| {
            // Locks an existing counter; a missing one is created by one
            // writer, the other gets a duplicate key and re-runs.
            let current: Option<u64> = tx.exec_first(
                "SELECT last_number FROM revision_counters WHERE function_id = ? FOR UPDATE",
                p![function.as_str()],
            )?;
            match current {
                None => {
                    tx.exec_drop(
                        "INSERT INTO revision_counters (function_id, last_number) VALUES (?, 1)",
                        p![function.as_str()],
                    )
                    .map_err(|e| retry_on_duplicate(e.into()))?;
                    Ok(1)
                }
                Some(n) => {
                    let updated = exec_count(
                        tx,
                        "UPDATE revision_counters SET last_number = ? \
                         WHERE function_id = ? AND last_number = ?",
                        p![n + 1, function.as_str(), n],
                    )?;
                    if updated != 1 {
                        return Err(lost_race("revision counter of", function));
                    }
                    Ok(n + 1)
                }
            }
        })
    }

    fn insert(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "revisions", revision.id.as_str())? {
                return Err(duplicate("revision", &revision.id));
            }
            guard::revision_insert(
                get_function(tx, &revision.function_id, false)?.as_ref(),
                &revision,
            )?;
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
        self.read(|c| get_revision(c, id, false))
    }

    fn list_by_function(&self, function: &FunctionId) -> Result<Vec<FunctionRevision>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM revisions WHERE function_id = ? ORDER BY number",
                p![function.as_str()],
            )
        })
    }

    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_revision(tx, &revision.id, true)? else {
                return Err(RepoError::NotFound(format!("revision {}", revision.id)));
            };
            if guard::revision_update(&old, &revision)? == Write::Apply {
                let n = exec_count(
                    tx,
                    "UPDATE revisions SET status = ?, body = ? WHERE id = ? AND status = ?",
                    p![
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

impl AliasRepository for TidbStore {
    fn get(
        &self,
        function: &FunctionId,
        name: &AliasName,
    ) -> Result<Option<FunctionAlias>, RepoError> {
        self.read(|c| get_alias(c, function, name, false))
    }

    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError> {
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM aliases WHERE function_id = ? ORDER BY name",
                p![function.as_str()],
            )
        })
    }

    fn insert(&self, alias: FunctionAlias) -> Result<(), RepoError> {
        self.write(|tx| {
            if get_alias(tx, &alias.function_id, &alias.name, true)?.is_some() {
                return Err(RepoError::Conflict(format!(
                    "alias `{}` already exists",
                    alias.name
                )));
            }
            guard::alias_target(
                get_function(tx, &alias.function_id, false)?.as_ref(),
                get_revision(tx, &alias.revision_id, false)?.as_ref(),
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
            let Some(current) = get_alias(tx, &alias.function_id, &alias.name, true)? else {
                return Err(RepoError::NotFound(format!("alias `{}`", alias.name)));
            };
            if current.generation != expected_generation {
                return Ok(false);
            }
            guard::alias_update(&current, &alias)?;
            guard::alias_target(
                get_function(tx, &alias.function_id, false)?.as_ref(),
                get_revision(tx, &alias.revision_id, false)?.as_ref(),
                &alias,
            )?;
            let n = exec_count(
                tx,
                "UPDATE aliases SET revision_id = ?, generation = ?, updated_at = ?, body = ? \
                 WHERE function_id = ? AND name = ? AND generation = ?",
                p![
                    alias.revision_id.as_str(),
                    alias.generation,
                    ts(&alias.updated_at),
                    to_json(&alias)?,
                    alias.function_id.as_str(),
                    alias.name.as_str(),
                    expected_generation
                ],
            )?;
            Ok(n == 1)
        })
    }
}

fn insert_invocation_checked<Q: Queryable>(
    tx: &mut Q,
    invocation: &Invocation,
    max_inline: u64,
    retention: Retention,
) -> Result<(), RepoError> {
    if exists(tx, "invocations", invocation.id.as_str())? {
        return Err(duplicate("invocation", &invocation.id));
    }
    guard::invocation_insert(
        get_function(tx, &invocation.function_id, false)?.as_ref(),
        invocation,
        max_inline,
    )?;
    insert_invocation_row(tx, invocation, retention).map_err(|e| match e {
        RepoError::Conflict(_) => duplicate("invocation", &invocation.id),
        other => other,
    })
}

impl InvocationRepository for TidbStore {
    fn insert(&self, invocation: Invocation) -> Result<(), RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| insert_invocation_checked(tx, &invocation, max, retention))
    }

    fn get(&self, id: &InvocationId) -> Result<Option<Invocation>, RepoError> {
        self.read(|c| get_invocation(c, id, false))
    }

    fn update(&self, invocation: Invocation) -> Result<(), RepoError> {
        let (max, retention) = (self.max_inline(), self.retention());
        self.write(|tx| {
            let Some(old) = get_invocation(tx, &invocation.id, true)? else {
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
        let limit = u64::try_from(limit).unwrap_or(u64::MAX);
        self.read(|c| {
            bodies(
                c,
                "SELECT body FROM invocations WHERE function_id = ? \
                 ORDER BY accepted_at DESC, id DESC LIMIT ?",
                p![function.as_str(), limit],
            )
        })
    }

    fn insert_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "attempts", attempt.id.as_str())? {
                return Err(duplicate("attempt", &attempt.id));
            }
            guard::attempt_insert(
                get_invocation(tx, &attempt.invocation_id, false)?.as_ref(),
                &attempt,
            )?;
            insert_attempt_row(tx, &attempt)
        })
    }

    fn get_attempt(&self, id: &AttemptId) -> Result<Option<InvocationAttempt>, RepoError> {
        self.read(|c| get_attempt(c, id, false))
    }

    fn update_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_attempt(tx, &attempt.id, true)? else {
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
                "SELECT body FROM attempts WHERE invocation_id = ? ORDER BY number, id",
                p![invocation.as_str()],
            )
        })
    }
}

impl EnvironmentRepository for TidbStore {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        self.write(|tx| {
            if exists(tx, "environments", env.id.as_str())? {
                return Err(duplicate("environment", &env.id));
            }
            guard::environment_insert(get_revision(tx, &env.revision_id, false)?.as_ref(), &env)?;
            insert_environment_row(tx, &env)
        })
    }

    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError> {
        self.read(|c| get_environment(c, id, false))
    }

    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        self.write(|tx| {
            let Some(old) = get_environment(tx, &env.id, true)? else {
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
                p![],
            )
        })
    }
}

impl LogRepository for TidbStore {
    fn append(&self, record: LogRecord) -> AppendOutcome {
        self.logs.append(&self.limits, record)
    }

    fn query(&self, tenant: &TenantId, invocation: &InvocationId) -> Result<LogQuery, RepoError> {
        Ok(self.logs.query(tenant, invocation))
    }
}

fn parse_ts(s: &str) -> Result<Timestamp, RepoError> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&chrono::Utc))
        .map_err(|e| RepoError::Serialization(e.to_string()))
}

fn live_binding<Q: Queryable>(
    c: &mut Q,
    tenant: &TenantId,
    function: &FunctionId,
    key: &str,
    now: Timestamp,
) -> Result<Option<IdempotencyBinding>, RepoError> {
    let row: Option<(String, String, Option<String>)> = c.exec_first(
        "SELECT i.invocation_id, i.input_digest, i.expires_at FROM idempotency i \
         JOIN invocations v ON v.id = i.invocation_id \
         WHERE i.tenant_id = ? AND i.function_id = ? AND i.idem_key = ? \
         AND (i.expires_at IS NULL OR i.expires_at > ?)",
        p![tenant.as_str(), function.as_str(), key, ts(&now)],
    )?;
    row.map(|(inv, digest, expires)| {
        Ok(IdempotencyBinding {
            invocation_id: InvocationId::parse(&inv)
                .map_err(|e| RepoError::Serialization(e.to_string()))?,
            input_digest: Sha256Digest::parse(&digest)
                .map_err(|e| RepoError::Serialization(e.to_string()))?,
            expires_at: expires.as_deref().map(parse_ts).transpose()?,
        })
    })
    .transpose()
}

fn bind_idempotency<Q: Queryable>(
    tx: &mut Q,
    invocation: &Invocation,
    retention: Retention,
) -> Result<(), RepoError> {
    if let Some(key) = &invocation.idempotency_key {
        // Only a stale binding is removed (its invocation is gone, or it
        // expired). A plain delete by key would, at READ-COMMITTED, also
        // delete a live binding another transaction committed after this
        // one checked, and both would "win".
        tx.exec_drop(
            "DELETE i FROM idempotency i LEFT JOIN invocations v ON v.id = i.invocation_id \
             WHERE i.tenant_id = ? AND i.function_id = ? AND i.idem_key = ? \
             AND (v.id IS NULL OR (i.expires_at IS NOT NULL AND i.expires_at <= ?))",
            p![
                invocation.tenant_id.as_str(),
                invocation.function_id.as_str(),
                key.as_str(),
                ts(&invocation.accepted_at)
            ],
        )?;
        tx.exec_drop(
            "INSERT INTO idempotency (tenant_id, function_id, idem_key, invocation_id, \
             input_digest, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
            p![
                invocation.tenant_id.as_str(),
                invocation.function_id.as_str(),
                key.as_str(),
                invocation.id.as_str(),
                invocation.input_digest.as_str(),
                idempotency_expires_at(invocation, retention)
            ],
        )?;
    }
    Ok(())
}

impl IdempotencyRepository for TidbStore {
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
            if let Some(key) = &invocation.idempotency_key {
                // Locks an existing binding. For a key nobody bound yet this
                // takes no lock at READ-COMMITTED; two binders then race to
                // the primary key and the loser re-runs, finding the winner.
                let _: Option<String> = tx.exec_first(
                    "SELECT invocation_id FROM idempotency \
                     WHERE tenant_id = ? AND function_id = ? AND idem_key = ? FOR UPDATE",
                    p![
                        invocation.tenant_id.as_str(),
                        invocation.function_id.as_str(),
                        key.as_str()
                    ],
                )?;
                if let Some(existing) = live_binding(
                    tx,
                    &invocation.tenant_id,
                    &invocation.function_id,
                    key,
                    invocation.accepted_at,
                )? {
                    return Ok(IdempotencyOutcome::Existing(existing));
                }
            }
            insert_invocation_checked(tx, &invocation, max, retention)?;
            bind_idempotency(tx, &invocation, retention).map_err(retry_on_duplicate)?;
            Ok(IdempotencyOutcome::Inserted)
        })
    }

    fn purge_expired_idempotency(&self, now: Timestamp) -> Result<usize, RepoError> {
        self.write(|tx| {
            Ok(exec_count(
                tx,
                "DELETE FROM idempotency WHERE expires_at IS NOT NULL AND expires_at <= ?",
                p![ts(&now)],
            )? as usize)
        })
    }
}

impl ArtifactOwnerRepository for TidbStore {
    fn claim(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<(), RepoError> {
        self.write(|tx| {
            // Idempotent under concurrency: the primary key decides.
            tx.exec_drop(
                "INSERT IGNORE INTO artifact_owners (digest, tenant_id) VALUES (?, ?)",
                p![digest.as_str(), tenant.as_str()],
            )?;
            Ok(())
        })
    }

    fn is_owned_by(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<bool, RepoError> {
        self.read(|c| {
            let owned: Option<i64> = c.exec_first(
                "SELECT 1 FROM artifact_owners WHERE digest = ? AND tenant_id = ?",
                p![digest.as_str(), tenant.as_str()],
            )?;
            Ok(owned.is_some())
        })
    }
}

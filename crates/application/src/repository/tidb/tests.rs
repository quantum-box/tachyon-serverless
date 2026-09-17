//! TiDB-only behaviour, against the cluster named by `TSLS_TIDB_URL`
//! (skipped with a message when it is unset): migrations on an empty and on
//! an older database, a failing migration, a newer schema, concurrent
//! migrators, races across separate connection pools, and the index review
//! (`EXPLAIN ANALYZE` on a seeded dataset; docs/db-index-review.md).
//!
//! `index_review_sqlite` runs without TiDB: it is the SQLite half of the same
//! review on the same dataset.

use std::collections::BTreeSet;
use std::sync::Arc;

use mysql::prelude::Queryable;
use rusqlite::Connection;

use tachyon_serverless_domain::{
    AliasName, AttemptId, DispatcherId, ExecutionEnvironment, ExecutionLease, FunctionAlias,
    FunctionId, InvocationAttempt, LeaseId, Limits, RevisionId, StartKind, TenantId,
};

use super::super::contract_tests::fx::{self, now};
use super::*;
use crate::repository::{
    AcquireOutcome, CompletionOutcome, DispatcherRecord, PoolLimits, ReclaimRequest, SlotAcquire,
    SlotCompletion, SlotStore,
};

fn url_or_skip(test: &str) -> Option<String> {
    let url = test_url();
    if url.is_none() {
        eprintln!("skipped {test}: TSLS_TIDB_URL is not set (scripts/db/tidb-verify.sh sets it)");
    }
    url
}

/// A database name of its own, dropped when the guard goes.
struct Db {
    url: String,
    name: String,
}

impl Db {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            name: format!(
                "tsls_m_{}",
                ulid::Ulid::new().to_string().to_ascii_lowercase()
            ),
        }
    }

    fn conn(&self) -> mysql::Conn {
        let base = Opts::from_url(&self.url).unwrap();
        let mut admin = mysql::Conn::new(session_opts(&base, None, 1).unwrap()).unwrap();
        admin
            .query_drop(format!("CREATE DATABASE IF NOT EXISTS `{}`", self.name))
            .unwrap();
        drop(admin);
        mysql::Conn::new(session_opts(&base, Some(&self.name), 1).unwrap()).unwrap()
    }

    fn open(&self, options: SqliteOptions) -> Result<TidbStore, RepoError> {
        TidbStore::open(&self.url, &self.name, Limits::default(), options, now())
    }

    fn open_with(&self, set: &[migrations::Migration]) -> Result<TidbStore, RepoError> {
        TidbStore::open_with(
            &self.url,
            &self.name,
            Limits::default(),
            SqliteOptions::default(),
            now(),
            set,
        )
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if let Ok(base) = Opts::from_url(&self.url)
            && let Ok(opts) = session_opts(&base, None, 1)
            && let Ok(mut c) = mysql::Conn::new(opts)
        {
            let _ = c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.name));
        }
    }
}

fn versions(c: &mut impl Queryable) -> Vec<i64> {
    c.query("SELECT version FROM schema_version ORDER BY version")
        .unwrap()
}

/// Every index name SQLite creates (autoindexes excluded).
fn sqlite_index_names() -> BTreeSet<String> {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::repository::sqlite::migrations::migrate_to(
        &mut conn,
        crate::repository::sqlite::migrations::LATEST,
        now(),
    )
    .unwrap();
    let mut st = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")
        .unwrap();
    st.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn sqlite_tables() -> BTreeSet<String> {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::repository::sqlite::migrations::migrate_to(
        &mut conn,
        crate::repository::sqlite::migrations::LATEST,
        now(),
    )
    .unwrap();
    let mut st = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap();
    st.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// `(table, column) -> (column type, collation)` of the current database.
fn tidb_columns(c: &mut impl Queryable) -> Vec<(String, String, String, Option<String>)> {
    c.query(
        "SELECT table_name, column_name, column_type, collation_name \
         FROM information_schema.columns WHERE table_schema = DATABASE() \
         ORDER BY table_name, ordinal_position",
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// migrations
// ---------------------------------------------------------------------------

#[test]
fn migrations_apply_to_an_empty_tidb_database() {
    let Some(url) = url_or_skip("migrations_apply_to_an_empty_tidb_database") else {
        return;
    };
    let db = Db::new(&url);
    let started = std::time::Instant::now();
    let store = db.open(SqliteOptions::default()).unwrap();
    eprintln!(
        "migrations 001-{:03} on an empty TiDB database: {:?}",
        migrations::LATEST,
        started.elapsed()
    );
    assert_eq!(
        store.migrations_applied(),
        (1..=migrations::LATEST).collect::<Vec<_>>()
    );
    drop(store);

    let mut c = db.conn();
    assert_eq!(
        migrations::current_version(&mut c).unwrap(),
        migrations::LATEST
    );
    assert_eq!(
        versions(&mut c),
        (1..=migrations::LATEST).collect::<Vec<_>>()
    );

    // Same tables as SQLite.
    let tables: BTreeSet<String> = c
        .query("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(tables, sqlite_tables(), "table set differs from SQLite");

    // Every SQLite index exists under the same name.
    let indexes: BTreeSet<String> = c
        .query(
            "SELECT DISTINCT index_name FROM information_schema.statistics \
             WHERE table_schema = DATABASE()",
        )
        .unwrap()
        .into_iter()
        .collect();
    for name in sqlite_index_names() {
        assert!(
            indexes.contains(&name),
            "index {name} missing in {indexes:?}"
        );
    }

    // Explicit types: ids are utf8mb4_bin, counters BIGINT UNSIGNED.
    let cols = tidb_columns(&mut c);
    let col = |t: &str, n: &str| {
        cols.iter()
            .find(|(tt, cc, _, _)| tt == t && cc == n)
            .unwrap_or_else(|| panic!("{t}.{n}"))
            .clone()
    };
    for (t, n) in [
        ("environments", "epoch"),
        ("environments", "secret_binding_generation"),
        ("aliases", "generation"),
        ("revisions", "number"),
        ("config_publication", "generation"),
    ] {
        assert_eq!(col(t, n).2, "bigint unsigned", "{t}.{n}");
    }
    for (t, n) in [
        ("invocations", "id"),
        ("environments", "tenant_id"),
        ("idempotency", "idem_key"),
        ("trigger_fires", "fire_key"),
    ] {
        assert_eq!(col(t, n).3.as_deref(), Some("utf8mb4_bin"), "{t}.{n}");
    }
    assert_eq!(col("invocations", "body").2, "longtext");

    // Reopening applies nothing.
    let again = db.open(SqliteOptions::default()).unwrap();
    assert!(again.migrations_applied().is_empty());
}

/// A database left at schema 3 by an older binary, with rows in it, is
/// upgraded in place to the latest schema: rows survive and the
/// retention backfill runs.
#[test]
fn migrations_upgrade_a_tidb_database_at_an_older_version() {
    let Some(url) = url_or_skip("migrations_upgrade_a_tidb_database_at_an_older_version") else {
        return;
    };
    let db = Db::new(&url);
    let t = TenantId::generate();
    let f = fx::function(&t, "old");
    let r = fx::ready_revision(&f, 1);
    let key = fx::key(&t, &r.id);
    let env = fx::ready_environment(&key);
    let mut inv = fx::invocation(&t, &f.id, Some("k-old"));
    inv.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    inv.mark_succeeded(Some(fx::inline(b"{\"kept\":1}")), Some(200), now())
        .unwrap();
    {
        let mut c = db.conn();
        let applied = migrations::migrate_to(&mut c, migrations::MIGRATIONS, 3, now()).unwrap();
        assert_eq!(applied, vec![1, 2, 3]);
        assert_eq!(migrations::current_version(&mut c).unwrap(), 3);
        let tables: Vec<String> = c
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_name = 'config_publication'",
            )
            .unwrap();
        assert!(tables.is_empty(), "004 not applied yet");
        insert_function_row(&mut c, &f).unwrap();
        insert_revision_row(&mut c, &r).unwrap();
        insert_environment_row(&mut c, &env).unwrap();
        // The schema-3 shape of an invocation whose inline output has no
        // expiry yet, and its idempotency binding.
        c.exec_drop(
            "INSERT INTO invocations (id, tenant_id, function_id, revision_id, status, terminal, \
             accepted_at, finished_at, input_digest, output_kind, body) \
             VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, 'inline', ?)",
            p![
                inv.id.as_str(),
                t.as_str(),
                f.id.as_str(),
                inv.revision_id.as_str(),
                inv.status.name(),
                ts(&inv.accepted_at),
                inv.finished_at.as_ref().map(ts),
                inv.input_digest.as_str(),
                to_json(&inv).unwrap()
            ],
        )
        .unwrap();
        c.exec_drop(
            "INSERT INTO idempotency (tenant_id, function_id, idem_key, invocation_id, \
             input_digest) VALUES (?, ?, 'k-old', ?, ?)",
            p![
                t.as_str(),
                f.id.as_str(),
                inv.id.as_str(),
                inv.input_digest.as_str()
            ],
        )
        .unwrap();
    }

    let retention = chrono::Duration::hours(1);
    let store = db
        .open(SqliteOptions {
            output_retention: Some(retention),
            idempotency_retention: Some(retention),
        })
        .unwrap();
    assert_eq!(
        store.migrations_applied(),
        (4..=migrations::LATEST).collect::<Vec<_>>()
    );
    assert_eq!(
        FunctionRepository::get(&store, &f.id).unwrap(),
        Some(f.clone())
    );
    assert_eq!(RevisionRepository::get(&store, &r.id).unwrap(), Some(r));
    assert_eq!(
        EnvironmentRepository::get(&store, &env.id).unwrap(),
        Some(env)
    );
    assert_eq!(
        InvocationRepository::get(&store, &inv.id).unwrap(),
        Some(inv.clone())
    );
    let binding = store.lookup(&t, &f.id, "k-old", now()).unwrap().unwrap();
    assert_eq!(binding.invocation_id, inv.id);
    let finished = inv.finished_at.unwrap();
    assert_eq!(binding.expires_at, Some(finished + retention));
    let expires: Option<Option<String>> = store
        .pool()
        .get_conn()
        .unwrap()
        .exec_first(
            "SELECT output_expires_at FROM invocations WHERE id = ?",
            p![inv.id.as_str()],
        )
        .unwrap();
    assert_eq!(
        expires.flatten(),
        Some(ts(&(finished + retention))),
        "the pre-existing inline output got its expiry"
    );
    let mut c = db.conn();
    assert_eq!(
        versions(&mut c),
        (1..=migrations::LATEST).collect::<Vec<_>>()
    );
}

/// TiDB DDL is not transactional. A migration that fails at its second
/// statement keeps its first (additive) statement, does not advance
/// `schema_version`, leaves the atomic multi-schema `ALTER` it failed in
/// unapplied, and the store still works at the previous version. Fixing the
/// migration and opening again finishes it.
#[test]
fn a_failing_tidb_migration_keeps_the_previous_schema_usable_and_can_be_rerun() {
    let Some(url) = url_or_skip("a_failing_tidb_migration_keeps_the_previous_schema_usable") else {
        return;
    };
    let db = Db::new(&url);
    let t = TenantId::generate();
    let f = fx::function(&t, "keep");
    {
        let store = db.open(SqliteOptions::default()).unwrap();
        FunctionRepository::insert(&store, f.clone()).unwrap();
    }
    let broken = migrations::Migration {
        version: migrations::LATEST + 1,
        name: "probe",
        sql: "CREATE TABLE IF NOT EXISTS migration_probe (id VARCHAR(64) NOT NULL PRIMARY KEY);\n\
              ALTER TABLE invocations ADD COLUMN IF NOT EXISTS probe_col BIGINT NULL, \
              ADD INDEX IF NOT EXISTS probe_idx (no_such_column);\n",
    };
    let fixed = migrations::Migration {
        sql: "CREATE TABLE IF NOT EXISTS migration_probe (id VARCHAR(64) NOT NULL PRIMARY KEY);\n\
              ALTER TABLE invocations ADD COLUMN IF NOT EXISTS probe_col BIGINT NULL;\n\
              ALTER TABLE invocations ADD INDEX IF NOT EXISTS probe_idx (probe_col);\n",
        ..broken
    };
    let mut with_broken = migrations::MIGRATIONS.to_vec();
    with_broken.push(broken);
    let err = db.open_with(&with_broken).unwrap_err().to_string();
    eprintln!("failing migration: {err}");
    assert!(
        err.contains("probe") && err.contains("statement 2"),
        "{err}"
    );

    let mut c = db.conn();
    assert_eq!(
        migrations::current_version(&mut c).unwrap(),
        migrations::LATEST
    );
    let probe_table: Vec<String> = c
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_name = 'migration_probe'",
        )
        .unwrap();
    assert_eq!(probe_table.len(), 1, "the first DDL committed on its own");
    let probe_col: Vec<String> = c
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = 'invocations' \
             AND column_name = 'probe_col'",
        )
        .unwrap();
    assert!(
        probe_col.is_empty(),
        "the failed multi-schema ALTER applied none of its clauses"
    );

    // The current binary (latest = previous version) still opens and writes.
    let store = db.open(SqliteOptions::default()).unwrap();
    assert!(store.migrations_applied().is_empty());
    assert_eq!(FunctionRepository::get(&store, &f.id).unwrap(), Some(f));
    FunctionRepository::insert(&store, fx::function(&t, "after-failure")).unwrap();
    drop(store);

    let mut with_fixed = migrations::MIGRATIONS.to_vec();
    with_fixed.push(fixed);
    let store = db.open_with(&with_fixed).unwrap();
    assert_eq!(store.migrations_applied(), &[migrations::LATEST + 1]);
    assert_eq!(
        migrations::current_version(&mut c).unwrap(),
        migrations::LATEST + 1
    );
}

#[test]
fn a_tidb_schema_newer_than_the_binary_is_refused() {
    let Some(url) = url_or_skip("a_tidb_schema_newer_than_the_binary_is_refused") else {
        return;
    };
    let db = Db::new(&url);
    drop(db.open(SqliteOptions::default()).unwrap());
    db.conn()
        .query_drop(
            "INSERT INTO schema_version (version, name, applied_at) VALUES (99, 'future', 'x')",
        )
        .unwrap();
    let err = db.open(SqliteOptions::default()).unwrap_err().to_string();
    assert!(
        err.contains("version 99") && err.contains("forward-only"),
        "{err}"
    );
}

/// Two openers racing on an empty database: the migration lock lets one
/// migrate, the other applies nothing, and every version is recorded once.
#[test]
fn concurrent_tidb_migrators_apply_each_migration_once() {
    let Some(url) = url_or_skip("concurrent_tidb_migrators_apply_each_migration_once") else {
        return;
    };
    let db = Arc::new(Db::new(&url));
    drop(db.conn());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let handles: Vec<_> = (0..3)
        .map(|_| {
            let (db, barrier) = (db.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                db.open(SqliteOptions::default())
                    .unwrap()
                    .migrations_applied()
                    .to_vec()
            })
        })
        .collect();
    let mut applied: Vec<Vec<i64>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    applied.sort();
    assert_eq!(
        applied,
        vec![vec![], vec![], (1..=migrations::LATEST).collect()]
    );
    assert_eq!(
        versions(&mut db.conn()),
        (1..=migrations::LATEST).collect::<Vec<_>>()
    );
}

/// Pins the TiDB behaviour the adapter is designed around (mod.rs): at
/// READ-COMMITTED a `SELECT ... FOR UPDATE` of a key that has no row takes no
/// lock, so another session's insert of that key does not wait; at
/// REPEATABLE-READ it does. If a TiDB upgrade changes this, the design notes
/// must be revisited.
#[test]
fn tidb_read_committed_does_not_lock_a_missing_key() {
    let Some(url) = url_or_skip("tidb_read_committed_does_not_lock_a_missing_key") else {
        return;
    };
    let db = Db::new(&url);
    let mut setup = db.conn();
    setup
        .query_drop(
            "CREATE TABLE probe (k VARCHAR(64) NOT NULL, PRIMARY KEY (k) CLUSTERED) \
             DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin",
        )
        .unwrap();
    for (isolation, waits) in [("READ-COMMITTED", false), ("REPEATABLE-READ", true)] {
        let key = format!("k-{isolation}");
        let mut holder = db.conn();
        holder
            .query_drop(format!("SET SESSION transaction_isolation = '{isolation}'"))
            .unwrap();
        holder.query_drop("BEGIN PESSIMISTIC").unwrap();
        let _: Option<String> = holder
            .exec_first(
                "SELECT k FROM probe WHERE k = ? FOR UPDATE",
                p![key.as_str()],
            )
            .unwrap();
        let mut other = db.conn();
        other
            .query_drop("SET SESSION innodb_lock_wait_timeout = 1")
            .unwrap();
        other
            .query_drop(format!("SET SESSION transaction_isolation = '{isolation}'"))
            .unwrap();
        let started = std::time::Instant::now();
        let inserted = other.exec_drop("INSERT INTO probe (k) VALUES (?)", p![key.as_str()]);
        let waited = started.elapsed();
        holder.query_drop("ROLLBACK").unwrap();
        eprintln!("{isolation}: insert {inserted:?} after {waited:?}");
        if waits {
            assert!(
                inserted.is_err() || waited >= std::time::Duration::from_millis(900),
                "{isolation}: the insert did not wait ({waited:?})"
            );
        } else {
            assert!(inserted.is_ok(), "{isolation}: {inserted:?}");
            assert!(
                waited < std::time::Duration::from_millis(900),
                "{isolation}: the insert waited {waited:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// races across separate connection pools (ADR-0003 A1 / A6 on TiDB)
// ---------------------------------------------------------------------------

fn register(store: &dyn SlotStore, instance: &str, ttl_s: i64) -> DispatcherId {
    let id = DispatcherId::generate();
    store
        .register_dispatcher(DispatcherRecord {
            id: id.clone(),
            instance: instance.into(),
            hostname: "tidb-test".into(),
            pid: std::process::id(),
            started_at: now(),
            heartbeat_at: now(),
            lease_expires_at: now() + chrono::Duration::seconds(ttl_s),
            stopped_at: None,
            reclaimed_at: None,
        })
        .unwrap();
    id
}

fn request(
    env: &ExecutionEnvironment,
    inv: &Invocation,
    owner: &DispatcherId,
    ttl_s: i64,
) -> SlotAcquire {
    let mut assigned = env.clone();
    assigned.assign(now()).unwrap();
    let attempt = InvocationAttempt::dispatch(
        AttemptId::generate(),
        inv.id.clone(),
        inv.tenant_id.clone(),
        1,
        env.id.clone(),
        assigned.epoch,
        StartKind::Cold,
        now(),
    );
    let lease = ExecutionLease::acquire(
        LeaseId::generate(),
        env.id.clone(),
        attempt.id.clone(),
        env.tenant_id.clone(),
        assigned.epoch,
        now() + chrono::Duration::seconds(600),
        now(),
    )
    .owned_by(owner.clone(), now() + chrono::Duration::seconds(ttl_s));
    let mut running = inv.clone();
    running
        .mark_running(
            attempt.id.clone(),
            now() + chrono::Duration::seconds(600),
            now(),
            now(),
        )
        .unwrap();
    SlotAcquire {
        env: assigned,
        expected_epoch: env.epoch,
        lease,
        attempt,
        invocation: Some(running),
    }
}

fn owned_invocation(store: &TidbStore, tenant: &TenantId, owner: &DispatcherId) -> Invocation {
    let mut inv = fx::invocation(tenant, &FunctionId::generate(), None);
    inv.dispatcher_id = Some(owner.clone());
    InvocationRepository::insert(store, inv.clone()).unwrap();
    inv
}

fn ready_owned_env(store: &TidbStore, owner: &DispatcherId) -> ExecutionEnvironment {
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let env = fx::ready_environment(&key).owned_by(owner.clone());
    EnvironmentRepository::insert(store, env.clone()).unwrap();
    env
}

fn pools(db: &Db, n: usize) -> Vec<Arc<TidbStore>> {
    (0..n)
        .map(|_| Arc::new(db.open(SqliteOptions::default()).unwrap()))
        .collect()
}

/// N stores, each with its own connection pool, race to acquire the same
/// slot at the same epoch: exactly one wins per round, the epoch moves by
/// one and one lease is live.
#[test]
fn tidb_acquires_on_separate_pools_have_one_winner_per_epoch() {
    let Some(url) = url_or_skip("tidb_acquires_on_separate_pools_have_one_winner_per_epoch") else {
        return;
    };
    let db = Db::new(&url);
    let setup = db.open(SqliteOptions::default()).unwrap();
    let owner = register(&setup, "race", 3600);
    let env = ready_owned_env(&setup, &owner);
    let big = PoolLimits {
        max_idle_per_key: 64,
        max_total_idle: 64,
    };
    for racers in [2usize, 4, 8, 12] {
        let stores = pools(&db, racers);
        let stored = EnvironmentRepository::get(&setup, &env.id)
            .unwrap()
            .unwrap();
        let requests: Vec<SlotAcquire> = (0..racers)
            .map(|_| {
                let inv = owned_invocation(&setup, &env.tenant_id, &owner);
                request(&stored, &inv, &owner, 30)
            })
            .collect();
        let barrier = Arc::new(std::sync::Barrier::new(racers));
        let handles: Vec<_> = stores
            .into_iter()
            .zip(requests.clone())
            .map(|(store, req)| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.acquire(req).unwrap()
                })
            })
            .collect();
        let outcomes: Vec<AcquireOutcome> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let winners: Vec<usize> = outcomes
            .iter()
            .enumerate()
            .filter(|(_, o)| **o == AcquireOutcome::Acquired)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(winners.len(), 1, "{racers} racers: {outcomes:?}");
        let after = EnvironmentRepository::get(&setup, &env.id)
            .unwrap()
            .unwrap();
        assert_eq!(after.epoch, stored.epoch + 1);
        let leases: Option<u64> = setup
            .pool()
            .get_conn()
            .unwrap()
            .exec_first(
                "SELECT COUNT(*) FROM leases WHERE environment_id = ? AND released = 0",
                p![env.id.as_str()],
            )
            .unwrap();
        assert_eq!(leases, Some(1), "one live lease per slot");
        let win = &requests[winners[0]];
        let mut done = win.attempt.clone();
        done.succeed(now()).unwrap();
        assert_eq!(
            setup
                .complete(SlotCompletion {
                    lease_id: win.lease.id.clone(),
                    attempt: done,
                    invocation: None,
                    now: now(),
                })
                .unwrap(),
            CompletionOutcome::Accepted
        );
        setup.release_to_pool(&after, big, now()).unwrap().unwrap();
    }
}

/// Alias CAS and pool claims raced from separate pools: one alias winner,
/// one claimer of the single idle environment.
#[test]
fn tidb_alias_cas_and_pool_claims_hold_across_pools() {
    let Some(url) = url_or_skip("tidb_alias_cas_and_pool_claims_hold_across_pools") else {
        return;
    };
    let db = Db::new(&url);
    let setup = db.open(SqliteOptions::default()).unwrap();
    let t = TenantId::generate();
    let f = fx::function(&t, "shared");
    let base = fx::ready_revision(&f, 1);
    let key = fx::key(&t, &base.id);
    let racers = 8u64;
    let targets: Vec<_> = (0..racers).map(|i| fx::ready_revision(&f, 2 + i)).collect();
    let alias = FunctionAlias::new(
        f.id.clone(),
        t.clone(),
        AliasName::default_alias(),
        base.id.clone(),
        now(),
    );
    FunctionRepository::insert(&setup, f.clone()).unwrap();
    RevisionRepository::insert(&setup, base.clone()).unwrap();
    for r in &targets {
        RevisionRepository::insert(&setup, r.clone()).unwrap();
    }
    AliasRepository::insert(&setup, alias.clone()).unwrap();
    let pooled_env = {
        let mut env = fx::ready_environment(&key);
        env.assign(now()).unwrap();
        env.mark_idle(now()).unwrap();
        EnvironmentRepository::insert(&setup, env.clone()).unwrap();
        env.id
    };
    let stores = pools(&db, racers as usize);
    let barrier = Arc::new(std::sync::Barrier::new(racers as usize));
    let handles: Vec<_> = stores
        .into_iter()
        .zip(targets)
        .map(|(store, r)| {
            let barrier = barrier.clone();
            let key = key.clone();
            let mut next = alias.clone();
            next.update(r.id.clone(), Some(1), now()).unwrap();
            std::thread::spawn(move || {
                barrier.wait();
                let alias_won = store.compare_and_set(next, 1).unwrap();
                let claimed = SlotStore::claim_for_reuse(&*store, &key, None, now()).unwrap();
                (alias_won, claimed)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|(won, _)| *won).count(), 1);
    let claims: Vec<_> = results.iter().filter_map(|(_, c)| c.clone()).collect();
    assert_eq!(claims.len(), 1, "one pool claims the idle environment");
    assert_eq!(claims[0].id, pooled_env);
    assert_eq!(
        AliasRepository::get(&setup, &f.id, &AliasName::default_alias())
            .unwrap()
            .unwrap()
            .generation,
        2
    );
}

/// The reclaim of an expired lease and the binding of one idempotency key
/// each happen exactly once when separate pools race for them; revision
/// numbers allocated concurrently are unique.
#[test]
fn tidb_reclaim_key_binding_and_counters_are_exactly_once_across_pools() {
    let Some(url) =
        url_or_skip("tidb_reclaim_key_binding_and_counters_are_exactly_once_across_pools")
    else {
        return;
    };
    let db = Db::new(&url);
    let setup = db.open(SqliteOptions::default()).unwrap();
    let dead = register(&setup, "dead", 10);
    let env = ready_owned_env(&setup, &dead);
    let inv = owned_invocation(&setup, &env.tenant_id, &dead);
    assert_eq!(
        setup.acquire(request(&env, &inv, &dead, 10)).unwrap(),
        AcquireOutcome::Acquired
    );
    let racers = 8;
    let reclaimers: Vec<DispatcherId> = (0..racers)
        .map(|i| register(&setup, &format!("r{i}"), 3600))
        .collect();
    let stores = pools(&db, racers);
    let barrier = Arc::new(std::sync::Barrier::new(racers));
    let handles: Vec<_> = stores
        .iter()
        .cloned()
        .zip(reclaimers)
        .map(|(store, me)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .reclaim_expired(ReclaimRequest {
                        reclaimer: me,
                        now: now() + chrono::Duration::seconds(20),
                        skew: chrono::Duration::seconds(2),
                        presumed_dead: Vec::new(),
                    })
                    .unwrap()
            })
        })
        .collect();
    let reports: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(reports.iter().map(|r| r.leases).sum::<usize>(), 1);
    assert_eq!(reports.iter().map(|r| r.fenced.len()).sum::<usize>(), 1);
    assert_eq!(
        reports.iter().map(|r| r.dispatchers.len()).sum::<usize>(),
        1
    );

    let t = TenantId::generate();
    let f = FunctionId::generate();
    let barrier = Arc::new(std::sync::Barrier::new(racers));
    let handles: Vec<_> = stores
        .iter()
        .cloned()
        .map(|store| {
            let barrier = barrier.clone();
            let inv = fx::invocation(&t, &f, Some("same-key"));
            std::thread::spawn(move || {
                barrier.wait();
                (inv.id.clone(), store.insert_bound(inv).unwrap())
            })
        })
        .collect();
    let bound: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let inserted: Vec<_> = bound
        .iter()
        .filter(|(_, o)| *o == IdempotencyOutcome::Inserted)
        .map(|(id, _)| id.clone())
        .collect();
    assert_eq!(inserted.len(), 1, "{bound:?}");
    for (_, o) in &bound {
        if let IdempotencyOutcome::Existing(b) = o {
            assert_eq!(b.invocation_id, inserted[0]);
        }
    }

    let function = FunctionId::generate();
    let barrier = Arc::new(std::sync::Barrier::new(racers));
    let handles: Vec<_> = stores
        .into_iter()
        .map(|store| {
            let barrier = barrier.clone();
            let function = function.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (0..5)
                    .map(|_| store.allocate_number(&function).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut numbers: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    numbers.sort_unstable();
    assert_eq!(numbers, (1..=(racers as u64 * 5)).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// index review (docs/db-index-review.md)
// ---------------------------------------------------------------------------

mod review {
    //! The hot queries, a seeded dataset and the plan checks shared by the
    //! SQLite and TiDB halves of the index review.

    use std::fmt::Write as _;

    pub const INVOCATIONS: usize = 10_000;
    pub const FUNCTIONS: usize = 100;
    pub const ENVIRONMENTS: usize = 2_000;
    pub const LEASES: usize = 10_000;
    pub const OUTBOX: usize = 10_000;
    pub const FIRES: usize = 10_000;
    pub const TRIGGERS: usize = 200;

    pub fn id(prefix: &str, i: usize) -> String {
        // Time-ordered like a ULID: a fixed-width counter.
        format!("{prefix}_{i:026}")
    }

    pub fn t(seconds: usize) -> String {
        let at = chrono::DateTime::from_timestamp(1_789_000_000 + seconds as i64, 0).unwrap();
        crate::repository::sqlite::ts(&at)
    }

    /// One seeded row per table, as column name / SQL literal pairs, so both
    /// backends get identical data. Bodies are small placeholders: the
    /// review is about access paths, not payload size.
    pub type Row = Vec<(&'static str, String)>;

    /// A body of realistic size (about 1 KiB of JSON): with a two-byte
    /// placeholder a full scan looks cheaper to TiDB's cost model than it is.
    pub fn body() -> String {
        format!("{{\"pad\":\"{}\"}}", "x".repeat(1_000))
    }

    fn q(s: &str) -> String {
        format!("'{}'", s.replace('\'', "''"))
    }

    pub fn dataset() -> Vec<(&'static str, Vec<Row>)> {
        let mut out = Vec::new();
        let dispatchers: Vec<Row> = (0..20)
            .map(|i| {
                vec![
                    ("id", q(&id("dsp", i))),
                    ("instance", q(&format!("gw{i}"))),
                    ("hostname", q("h")),
                    ("pid", i.to_string()),
                    ("started_at", q(&t(0))),
                    ("lease_expires_at", q(&t(10_000 + i))),
                    ("body", q(&body())),
                ]
            })
            .collect();
        out.push(("dispatchers", dispatchers));
        let invocations: Vec<Row> = (0..INVOCATIONS)
            .map(|i| {
                let terminal = i % 50 != 0;
                vec![
                    ("id", q(&id("inv", i))),
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("function_id", q(&id("fn", i % FUNCTIONS))),
                    ("revision_id", q(&id("rev", i % FUNCTIONS))),
                    ("status", q(if terminal { "succeeded" } else { "running" })),
                    ("terminal", (terminal as i64).to_string()),
                    ("accepted_at", q(&t(i))),
                    ("input_digest", q("sha256:x")),
                    ("output_kind", q("inline")),
                    (
                        "output_expires_at",
                        if terminal && i % 3 == 0 {
                            q(&t(i + 3600))
                        } else {
                            "NULL".into()
                        },
                    ),
                    ("owner_id", q(&id("dsp", i % 20))),
                    ("body", q(&body())),
                ]
            })
            .collect();
        out.push(("invocations", invocations));
        let states = ["idle", "busy", "ready", "lost", "terminated"];
        let environments: Vec<Row> = (0..ENVIRONMENTS)
            .map(|i| {
                let state = states[i % states.len()];
                let terminal = matches!(state, "lost" | "terminated");
                vec![
                    ("id", q(&id("env", i))),
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("revision_id", q(&id("rev", i % FUNCTIONS))),
                    ("provider", q("fake")),
                    ("state", q(state)),
                    ("terminal", (terminal as i64).to_string()),
                    ("epoch", (i % 7).to_string()),
                    ("execution_role_version", "1".into()),
                    ("configuration_version", (i % 3).to_string()),
                    ("resource_profile_digest", q("rp")),
                    ("runtime_profile", q("tachyon.runtime.v1")),
                    ("network_policy_version", "3".into()),
                    ("secret_binding_generation", "42".into()),
                    (
                        "idle_since",
                        if state == "idle" {
                            q(&t(i))
                        } else {
                            "NULL".into()
                        },
                    ),
                    ("created_at", q(&t(i))),
                    ("owner_id", q(&id("dsp", i % 20))),
                    ("fenced", ((i % 97 == 0) as i64).to_string()),
                    ("body", q(&body())),
                ]
            })
            .collect();
        out.push(("environments", environments));
        let leases: Vec<Row> = (0..LEASES)
            .map(|i| {
                let released = i % 100 != 0;
                vec![
                    ("id", q(&id("lse", i))),
                    ("environment_id", q(&id("env", i % ENVIRONMENTS))),
                    ("attempt_id", q(&id("att", i))),
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("epoch", (i % 7).to_string()),
                    ("deadline", q(&t(i + 600))),
                    ("released", (released as i64).to_string()),
                    ("owner_id", q(&id("dsp", i % 20))),
                    ("expires_at", q(&t(i + 30))),
                    ("body", q(&body())),
                ]
            })
            .collect();
        out.push(("leases", leases));
        let idempotency: Vec<Row> = (0..INVOCATIONS)
            .map(|i| {
                vec![
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("function_id", q(&id("fn", i % FUNCTIONS))),
                    ("idem_key", q(&format!("key-{i}"))),
                    ("invocation_id", q(&id("inv", i))),
                    ("input_digest", q("sha256:x")),
                    (
                        "expires_at",
                        if i % 50 != 0 {
                            q(&t(i + 86_400))
                        } else {
                            "NULL".into()
                        },
                    ),
                ]
            })
            .collect();
        out.push(("idempotency", idempotency));
        let outbox: Vec<Row> = (0..OUTBOX)
            .map(|i| {
                let sent = i % 20 != 0;
                vec![
                    ("event_id", q(&id("inv", i))),
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("topic", q("invoke")),
                    ("payload", q(&body())),
                    ("created_at", q(&t(i))),
                    ("sent", (sent as i64).to_string()),
                    ("publish_attempts", "1".into()),
                    ("next_attempt_at", q(&t(i))),
                    ("sent_at", if sent { q(&t(i + 1)) } else { "NULL".into() }),
                ]
            })
            .collect();
        out.push(("outbox", outbox));
        let triggers: Vec<Row> = (0..TRIGGERS)
            .map(|i| {
                let cron = i % 2 == 0;
                vec![
                    ("id", q(&id("trg", i))),
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("function_id", q(&id("fn", i % FUNCTIONS))),
                    ("kind", q(if cron { "cron" } else { "webhook" })),
                    (
                        "status",
                        q(if i % 10 == 9 { "disabled" } else { "enabled" }),
                    ),
                    ("generation", "1".into()),
                    (
                        "next_fire_at",
                        if cron { q(&t(i * 60)) } else { "NULL".into() },
                    ),
                    ("created_at", q(&t(i))),
                    ("updated_at", q(&t(i))),
                    ("body", q(&body())),
                ]
            })
            .collect();
        out.push(("triggers", triggers));
        let fires: Vec<Row> = (0..FIRES)
            .map(|i| {
                let cron = i % 2 == 0;
                vec![
                    ("trigger_id", q(&id("trg", i % TRIGGERS))),
                    (
                        "fire_key",
                        q(&if cron {
                            format!("cron:{}", t(i * 60))
                        } else {
                            format!("event:evt-{i}")
                        }),
                    ),
                    ("tenant_id", q(&id("tnt", i % 10))),
                    ("kind", q(if cron { "cron" } else { "webhook" })),
                    (
                        "signature_digest",
                        if cron {
                            "NULL".into()
                        } else {
                            q(&format!("sha256:sig{i}"))
                        },
                    ),
                    ("invocation_id", q(&id("inv", i))),
                    ("outcome", q("accepted")),
                    ("created_at", q(&t(i))),
                ]
            })
            .collect();
        out.push(("trigger_fires", fires));
        out
    }

    /// Multi-row INSERT statements, `chunk` rows each.
    pub fn inserts(table: &str, rows: &[Row], chunk: usize) -> Vec<String> {
        rows.chunks(chunk)
            .map(|part| {
                let cols: Vec<&str> = part[0].iter().map(|(c, _)| *c).collect();
                let mut sql = format!("INSERT INTO {table} ({}) VALUES ", cols.join(", "));
                for (i, row) in part.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    let vals: Vec<&str> = row.iter().map(|(_, v)| v.as_str()).collect();
                    let _ = write!(sql, "({})", vals.join(", "));
                }
                sql
            })
            .collect()
    }

    pub struct HotQuery {
        pub name: &'static str,
        /// Where it runs (code path).
        pub path: &'static str,
        pub sqlite: String,
        pub tidb: String,
        /// Index names the plan must mention on SQLite / TiDB (any of).
        pub sqlite_index: &'static [&'static str],
        pub tidb_index: &'static [&'static str],
        /// A DML statement: EXPLAIN only (no ANALYZE, which would execute it).
        pub dml: bool,
        /// A periodic sweep (retention, pool sweep, reclaim scan): the plan is
        /// recorded, but a full scan is not a failure (docs/db-index-review.md
        /// discusses each). Request-path queries must use their index.
        pub sweep: bool,
    }

    pub fn queries() -> Vec<HotQuery> {
        let reuse = format!(
            "state = 'idle' AND tenant_id = '{}' AND revision_id = '{}' \
             AND execution_role_version = 1 AND configuration_version = 0 \
             AND resource_profile_digest = 'rp' AND runtime_profile = 'tachyon.runtime.v1' \
             AND network_policy_version = 3 AND secret_binding_generation = 42",
            id("tnt", 0),
            id("rev", 0)
        );
        let owner = id("dsp", 0);
        let now = t(5_000);
        let both = |sql: String| (sql.clone(), sql);
        let mut out = Vec::new();
        let (s, d) = (
            format!(
                "SELECT body FROM environments WHERE {reuse} AND owner_id IS '{owner}' ORDER BY id LIMIT 1"
            ),
            format!(
                "SELECT body FROM environments WHERE {reuse} AND owner_id <=> '{owner}' ORDER BY id LIMIT 1 FOR UPDATE"
            ),
        );
        out.push(HotQuery {
            name: "claim_for_reuse (reuse key)",
            path: "SlotStore::claim_for_reuse",
            sqlite: s,
            tidb: d,
            sqlite_index: &["environments_reuse_key"],
            tidb_index: &["environments_reuse_key"],
            dml: false,
            sweep: false,
        });
        let (s, d) = (
            format!(
                "SELECT body FROM environments WHERE state = 'idle' AND owner_id IS '{owner}' ORDER BY idle_since, id"
            ),
            format!(
                "SELECT body FROM environments WHERE state = 'idle' AND owner_id <=> '{owner}' ORDER BY idle_since, id"
            ),
        );
        out.push(HotQuery {
            name: "list_idle (pool sweep)",
            path: "SlotStore::list_idle",
            sqlite: s,
            tidb: d,
            sqlite_index: &[
                "environments_state_idle_since",
                "environments_reuse_key",
                "environments_owner_terminal",
            ],
            tidb_index: &[
                "environments_state_idle_since",
                "environments_reuse_key",
                "environments_owner_terminal",
            ],
            dml: false,
            sweep: true,
        });
        let (s, d) = both(format!(
            "SELECT body FROM invocations WHERE function_id = '{}' ORDER BY accepted_at DESC, id DESC LIMIT 50",
            id("fn", 7)
        ));
        out.push(HotQuery {
            name: "list invocations by function",
            path: "InvocationRepository::list_by_function",
            sqlite: s,
            tidb: d,
            sqlite_index: &["invocations_function_accepted"],
            tidb_index: &["invocations_function_accepted"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT i.invocation_id, i.input_digest, i.expires_at FROM idempotency i \
             JOIN invocations v ON v.id = i.invocation_id \
             WHERE i.tenant_id = '{}' AND i.function_id = '{}' AND i.idem_key = 'key-4242' \
             AND (i.expires_at IS NULL OR i.expires_at > '{now}')",
            id("tnt", 2),
            id("fn", 42)
        ));
        out.push(HotQuery {
            name: "idempotency lookup",
            path: "IdempotencyRepository::{lookup, insert_bound}",
            sqlite: s,
            tidb: d,
            sqlite_index: &["sqlite_autoindex_idempotency_1"],
            tidb_index: &["PRIMARY"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT event_id FROM outbox WHERE sent = 0 AND next_attempt_at <= '{now}' \
             AND (claimed_by IS NULL OR claim_expires_at <= '{now}') \
             ORDER BY created_at, event_id LIMIT 32"
        ));
        out.push(HotQuery {
            name: "outbox claim",
            path: "AsyncInvocationRepository::claim_outbox (sqlite/outbox.rs)",
            sqlite: s,
            tidb: d,
            sqlite_index: &["outbox_sent_next", "outbox_sent_created"],
            tidb_index: &["outbox_sent_next", "outbox_sent_created"],
            dml: false,
            // A publisher poll loop: recorded, discussed in docs/db-index-review.md.
            sweep: true,
        });
        let (s, d) = both(format!(
            "SELECT outcome FROM trigger_fires WHERE trigger_id = '{}' AND fire_key = 'event:evt-4243'",
            id("trg", 43)
        ));
        out.push(HotQuery {
            name: "trigger fire by key (unique)",
            path: "TriggerRepository fire dedupe (sqlite/triggers.rs)",
            sqlite: s,
            tidb: d,
            sqlite_index: &["sqlite_autoindex_trigger_fires_1"],
            tidb_index: &["PRIMARY"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT outcome FROM trigger_fires WHERE trigger_id = '{}' AND signature_digest = 'sha256:sig4243'",
            id("trg", 43)
        ));
        out.push(HotQuery {
            name: "trigger fire by signature (unique)",
            path: "TriggerRepository webhook replay (sqlite/triggers.rs)",
            sqlite: s,
            tidb: d,
            sqlite_index: &["trigger_fires_signature"],
            tidb_index: &["trigger_fires_signature"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT body FROM triggers WHERE kind = 'cron' AND status = 'enabled' \
             AND next_fire_at IS NOT NULL AND next_fire_at <= '{now}' ORDER BY next_fire_at, id LIMIT 32"
        ));
        out.push(HotQuery {
            name: "due cron triggers",
            path: "TriggerRepository::due_cron (sqlite/triggers.rs)",
            sqlite: s,
            tidb: d,
            sqlite_index: &["triggers_due"],
            tidb_index: &["triggers_due"],
            dml: false,
            sweep: true,
        });
        let (s, d) = both(
            "SELECT body FROM leases WHERE released = 0 AND owner_id IS NOT NULL ORDER BY id"
                .to_string(),
        );
        out.push(HotQuery {
            name: "reclaim: unreleased leases",
            path: "SlotStore::reclaim_expired step 2",
            sqlite: s,
            tidb: d,
            sqlite_index: &["leases_released_expires", "leases_released"],
            tidb_index: &[
                "leases_released_expires",
                "leases_released",
                "leases_owner_released",
            ],
            dml: false,
            sweep: true,
        });
        let (s, d) = both(format!(
            "SELECT body FROM invocations WHERE owner_id = '{owner}' AND terminal = 0 ORDER BY id"
        ));
        out.push(HotQuery {
            name: "reclaim: invocations of a dead dispatcher",
            path: "SlotStore::reclaim_expired step 3",
            sqlite: s,
            tidb: d,
            sqlite_index: &["invocations_owner_terminal"],
            tidb_index: &["invocations_owner_terminal"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT body FROM leases WHERE owner_id = '{owner}' AND released = 0 ORDER BY id"
        ));
        out.push(HotQuery {
            name: "heartbeat: leases of a dispatcher",
            path: "SlotStore::heartbeat",
            sqlite: s,
            tidb: d,
            sqlite_index: &["leases_owner_released"],
            tidb_index: &["leases_owner_released"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT 1 FROM leases WHERE environment_id = '{}' AND released = 0 LIMIT 1",
            id("env", 5)
        ));
        out.push(HotQuery {
            name: "acquire: unreleased lease of an environment",
            path: "SlotStore::{acquire, release_to_pool}",
            sqlite: s,
            tidb: d,
            sqlite_index: &["leases_environment_released", "leases_environment"],
            tidb_index: &["leases_environment_released", "leases_environment"],
            dml: false,
            sweep: false,
        });
        let (s, d) = both(format!(
            "SELECT body FROM invocations WHERE output_expires_at IS NOT NULL AND output_expires_at <= '{now}'"
        ));
        out.push(HotQuery {
            name: "retention: expired inline outputs",
            path: "StateStore::purge_expired_outputs",
            sqlite: s,
            tidb: d,
            sqlite_index: &["invocations_output_expires"],
            tidb_index: &["invocations_output_expires"],
            dml: false,
            sweep: true,
        });
        let (s, d) = both(format!(
            "DELETE FROM idempotency WHERE expires_at IS NOT NULL AND expires_at <= '{now}'"
        ));
        out.push(HotQuery {
            name: "retention: expired idempotency bindings",
            path: "IdempotencyRepository::purge_expired_idempotency",
            sqlite: s,
            tidb: d,
            sqlite_index: &["idempotency_expires"],
            tidb_index: &["idempotency_expires"],
            dml: true,
            sweep: true,
        });
        let (s, d) = both(format!(
            "DELETE FROM outbox WHERE sent = 1 AND sent_at < '{now}'"
        ));
        out.push(HotQuery {
            name: "retention: sent outbox rows",
            path: "AsyncInvocationRepository::purge_sent (sqlite/outbox.rs)",
            sqlite: s,
            tidb: d,
            // No index leads with sent_at. With 95% of the rows sent, SQLite's
            // planner prefers the full scan; accepted and documented in
            // docs/db-index-review.md.
            sqlite_index: &["outbox_sent_next", "outbox_sent_created", "SCAN outbox"],
            tidb_index: &["outbox_sent_next", "outbox_sent_created"],
            dml: true,
            sweep: true,
        });
        let (s, d) = both(format!(
            "DELETE FROM trigger_fires WHERE kind = 'webhook' AND created_at < '{now}'"
        ));
        out.push(HotQuery {
            name: "retention: old trigger fires",
            path: "TriggerRepository::purge_fires (sqlite/triggers.rs)",
            sqlite: s,
            tidb: d,
            sqlite_index: &["trigger_fires_created"],
            tidb_index: &["trigger_fires_created"],
            dml: true,
            sweep: true,
        });
        out
    }

    pub fn uses_any(plan: &str, names: &[&str]) -> bool {
        names.iter().any(|n| plan.contains(n))
    }

    pub fn evidence_dir() -> Option<std::path::PathBuf> {
        std::env::var_os("TSLS_TIDB_EVIDENCE_DIR").map(std::path::PathBuf::from)
    }
}

/// The SQLite half of the index review: the same dataset and hot queries,
/// `EXPLAIN QUERY PLAN` after `ANALYZE`. Runs without TiDB.
#[test]
fn index_review_sqlite() {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::repository::sqlite::migrations::migrate_to(
        &mut conn,
        crate::repository::sqlite::migrations::LATEST,
        now(),
    )
    .unwrap();
    for (table, rows) in review::dataset() {
        for sql in review::inserts(table, &rows, 500) {
            conn.execute_batch(&sql).unwrap();
        }
    }
    conn.execute_batch("ANALYZE").unwrap();
    let mut report = format!(
        "# SQLite {} EXPLAIN QUERY PLAN (after ANALYZE)\n\n",
        rusqlite::version()
    );
    let mut misses = Vec::new();
    for hq in review::queries() {
        let mut st = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {}", hq.sqlite))
            .unwrap();
        let plan = st
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join("\n");
        let ok = review::uses_any(&plan, hq.sqlite_index);
        let ok_or_sweep = ok || hq.sweep;
        report.push_str(&format!(
            "## {} ({})\n\n```sql\n{}\n```\n\n```\n{}\n```\n\nexpected index: {:?} -> {}\n\n",
            hq.name,
            hq.path,
            hq.sqlite,
            plan,
            hq.sqlite_index,
            if ok {
                "used"
            } else if hq.sweep {
                "NOT USED (sweep, recorded)"
            } else {
                "NOT USED"
            }
        ));
        if !ok_or_sweep {
            misses.push(format!("{}: {plan}", hq.name));
        }
    }
    if let Some(dir) = review::evidence_dir() {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("explain-sqlite.md"), &report).unwrap();
    }
    assert!(misses.is_empty(), "{misses:#?}\n{report}");
}

/// The TiDB half: the same dataset in a database of its own, `ANALYZE
/// TABLE`, then `EXPLAIN ANALYZE` (plain `EXPLAIN` for the retention
/// deletes).
#[test]
fn index_review_tidb() {
    let Some(url) = url_or_skip("index_review_tidb") else {
        return;
    };
    let db = Db::new(&url);
    drop(db.open(SqliteOptions::default()).unwrap());
    let mut c = db.conn();
    let started = std::time::Instant::now();
    let mut tables = Vec::new();
    for (table, rows) in review::dataset() {
        for sql in review::inserts(table, &rows, 500) {
            c.query_drop(&sql).unwrap();
        }
        tables.push(table);
    }
    let seeded = started.elapsed();
    for table in &tables {
        c.query_drop(format!("ANALYZE TABLE {table}")).unwrap();
    }
    let version: Option<String> = c.query_first("SELECT tidb_version()").unwrap();
    let mut report = format!(
        "# TiDB EXPLAIN ANALYZE (after ANALYZE TABLE)\n\n```\n{}\n```\n\nseeded {} invocations, \
         {} environments, {} leases, {} idempotency bindings, {} outbox rows, {} trigger fires \
         in {:?}\n\n",
        version.unwrap_or_default(),
        review::INVOCATIONS,
        review::ENVIRONMENTS,
        review::LEASES,
        review::INVOCATIONS,
        review::OUTBOX,
        review::FIRES,
        seeded
    );
    let mut misses = Vec::new();
    for hq in review::queries() {
        let explain = if hq.dml { "EXPLAIN" } else { "EXPLAIN ANALYZE" };
        let sql = format!("{explain} {}", hq.tidb);
        // A locking read needs a transaction to be meaningful; analyze it
        // inside one and roll back.
        let mut tx = c.start_transaction(mysql::TxOpts::default()).unwrap();
        let rows: Vec<mysql::Row> = tx.query(&sql).unwrap();
        tx.rollback().unwrap();
        let mut plan = String::new();
        for row in rows {
            let cols: Vec<String> = (0..row.len())
                .map(|i| match row.as_ref(i) {
                    Some(mysql::Value::Bytes(b)) => String::from_utf8_lossy(b).to_string(),
                    Some(mysql::Value::NULL) | None => String::new(),
                    Some(v) => v.as_sql(true),
                })
                .collect();
            plan.push_str(&cols.join(" | "));
            plan.push('\n');
        }
        let ok = review::uses_any(&plan, hq.tidb_index);
        let ok_or_sweep = ok || hq.sweep;
        report.push_str(&format!(
            "## {} ({})\n\n```sql\n{}\n```\n\n```\n{}```\n\nexpected index: {:?} -> {}\n\n",
            hq.name,
            hq.path,
            hq.tidb,
            plan,
            hq.tidb_index,
            if ok {
                "used"
            } else if hq.sweep {
                "NOT USED (sweep, recorded)"
            } else {
                "NOT USED"
            }
        ));
        if !ok_or_sweep {
            misses.push(format!("{}:\n{plan}", hq.name));
        }
    }
    if let Some(dir) = review::evidence_dir() {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("explain-tidb.md"), &report).unwrap();
    }
    assert!(misses.is_empty(), "{}\n{report}", misses.join("\n"));
}

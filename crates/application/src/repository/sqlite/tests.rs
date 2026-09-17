//! SQLite-only behaviour: migrations, persistence across opens, the restart
//! reconcile, the one-time `state.json` import, output retention, index use
//! and CAS across separate connections to the same file.

use std::path::Path;
use std::sync::Arc;

use tachyon_serverless_domain::{
    AliasName, AttemptId, AttemptStatus, DispatcherId, EnvironmentState, ErrorClass,
    ExecutionEnvironment, ExecutionLease, FunctionAlias, FunctionId, InvocationAttempt,
    InvocationStatus, LeaseId, Limits, PayloadRef, RevisionId, Sha256Digest, StartKind, TenantId,
};

use super::super::contract_tests::fx::{self, now};
use super::*;
use crate::repository::{
    AcquireOutcome, CompletionOutcome, DispatcherRecord, HOST_RESTARTED, PoolLimits,
    ReclaimRequest, SlotAcquire, SlotCompletion, SlotStore,
};

fn open(dir: &Path) -> SqliteStore {
    SqliteStore::open(dir, Limits::default(), SqliteOptions::default(), now()).unwrap()
}

fn open_err(dir: &Path) -> String {
    match SqliteStore::open(dir, Limits::default(), SqliteOptions::default(), now()) {
        Ok(_) => panic!("opening {} must fail", dir.display()),
        Err(e) => e.to_string(),
    }
}

fn tables(c: &Connection) -> Vec<String> {
    let mut st = c
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    st.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn columns(c: &Connection, table: &str) -> Vec<String> {
    let mut st = c
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .unwrap();
    st.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

// ---------------------------------------------------------------------------
// migrations
// ---------------------------------------------------------------------------

#[test]
fn migrations_apply_to_an_empty_database() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let report = store.open_report().clone();
    assert_eq!(report.schema_version, migrations::LATEST);
    assert_eq!(
        report.migrations_applied,
        migrations::MIGRATIONS
            .iter()
            .map(|m| m.version)
            .collect::<Vec<_>>()
    );
    let conn = store.conn.lock();
    let t = tables(&conn);
    for table in [
        "aliases",
        "artifact_owners",
        "attempts",
        "environments",
        "functions",
        "idempotency",
        "invocations",
        "leases",
        "revision_counters",
        "revisions",
        "schema_version",
        "store_meta",
        "dispatchers",
    ] {
        assert!(t.contains(&table.to_string()), "{table} in {t:?}");
    }
    assert!(columns(&conn, "invocations").contains(&"output_expires_at".to_string()));
    assert!(columns(&conn, "invocations").contains(&"owner_id".to_string()));
    assert!(columns(&conn, "environments").contains(&"fenced".to_string()));
    assert!(columns(&conn, "leases").contains(&"expires_at".to_string()));
    assert!(columns(&conn, "idempotency").contains(&"expires_at".to_string()));
    drop(conn);
    drop(store);

    // Reopening applies nothing.
    let again = open(dir.path());
    assert!(again.open_report().migrations_applied.is_empty());
    assert_eq!(again.open_report().schema_version, migrations::LATEST);
}

/// A database left at schema 1 by an older binary, with rows in it, is
/// upgraded in place: the rows survive, the new column is added and
/// back-filled.
#[test]
fn migrations_upgrade_a_database_at_an_older_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(SqliteStore::FILE_NAME);
    let t = TenantId::generate();
    let f = fx::function(&t, "old");
    let mut inv = fx::invocation(&t, &f.id, None);
    inv.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    inv.mark_succeeded(Some(fx::inline(b"{\"kept\":1}")), Some(200), now())
        .unwrap();
    {
        let mut conn = Connection::open(&path).unwrap();
        configure(&conn).unwrap();
        let applied = migrations::migrate_to(&mut conn, 1, now()).unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(migrations::current_version(&conn).unwrap(), 1);
        assert!(!columns(&conn, "invocations").contains(&"output_expires_at".to_string()));
        insert_function_row(&conn, &f).unwrap();
        // The schema-1 shape of an invocation row.
        conn.execute(
            "INSERT INTO invocations (id, tenant_id, function_id, revision_id, status, terminal, \
             accepted_at, finished_at, input_digest, output_kind, body) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?8, 'inline', ?9)",
            params![
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
    }

    let retention = chrono::Duration::hours(1);
    let store = SqliteStore::open(
        dir.path(),
        Limits::default(),
        SqliteOptions {
            output_retention: Some(retention),
            ..SqliteOptions::default()
        },
        now(),
    )
    .unwrap();
    assert_eq!(store.open_report().migrations_applied, vec![2, 3]);
    assert_eq!(store.open_report().schema_version, 3);
    assert_eq!(FunctionRepository::get(&store, &f.id).unwrap(), Some(f));
    assert_eq!(
        InvocationRepository::get(&store, &inv.id).unwrap(),
        Some(inv.clone())
    );
    let expires: Option<String> = store
        .conn
        .lock()
        .query_row(
            "SELECT output_expires_at FROM invocations WHERE id = ?1",
            [inv.id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        expires,
        Some(ts(&(inv.finished_at.unwrap() + retention))),
        "the pre-existing inline output got its expiry"
    );
}

#[test]
fn a_database_newer_than_the_binary_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    drop(open(dir.path()));
    let conn = Connection::open(dir.path().join(SqliteStore::FILE_NAME)).unwrap();
    conn.execute(
        "INSERT INTO schema_version (version, name, applied_at) VALUES (99, 'future', 'x')",
        [],
    )
    .unwrap();
    drop(conn);
    let msg = open_err(dir.path());
    assert!(msg.contains("schema version 99"), "{msg}");
    assert!(msg.contains("forward-only"), "{msg}");
}

#[test]
fn a_failing_migration_leaves_the_previous_schema_intact() {
    let mut conn = Connection::open_in_memory().unwrap();
    migrations::migrate_to(&mut conn, 1, now()).unwrap();
    // Column already there: migration 2 fails half way.
    conn.execute(
        "ALTER TABLE invocations ADD COLUMN output_expires_at VARCHAR(40)",
        [],
    )
    .unwrap();
    let err = migrations::migrate_to(&mut conn, migrations::LATEST, now()).unwrap_err();
    assert!(err.to_string().contains("002_output_retention"), "{err}");
    assert_eq!(migrations::current_version(&conn).unwrap(), 1);
}

/// docs/adr/0003 A5: the reuse-key lookup and the pool scans are served by
/// an index, not by a table scan.
#[test]
fn pool_lookups_use_their_indexes() {
    let store = SqliteStore::open_volatile(Limits::default(), SqliteOptions::default()).unwrap();
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    for _ in 0..50 {
        EnvironmentRepository::insert(&store, fx::ready_environment(&key)).unwrap();
    }
    let conn = store.conn.lock();
    conn.execute_batch("ANALYZE").unwrap();
    let plan = |sql: &str| -> String {
        let mut st = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let params = reuse_key_params(&key).unwrap();
        let n = st.parameter_count();
        st.query_map(rusqlite::params_from_iter(params.iter().take(n)), |r| {
            r.get::<_, String>(3)
        })
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .join(" | ")
    };
    let claim = plan(&format!(
        "SELECT body FROM environments WHERE {REUSE_KEY_MATCH} ORDER BY id LIMIT 1"
    ));
    assert!(claim.contains("environments_reuse_key"), "{claim}");
    let idle = plan("SELECT body FROM environments WHERE state = 'idle' ORDER BY idle_since, id");
    assert!(idle.contains("environments_state_idle_since"), "{idle}");
    let active = plan("SELECT body FROM environments WHERE terminal = 0 ORDER BY id");
    assert!(active.contains("environments_terminal"), "{active}");
    let history = plan(
        "SELECT body FROM invocations WHERE function_id = 'fn_x' ORDER BY accepted_at DESC, id DESC LIMIT 10",
    );
    assert!(
        history.contains("invocations_function_accepted"),
        "{history}"
    );
}

// ---------------------------------------------------------------------------
// persistence and restart
// ---------------------------------------------------------------------------

#[test]
fn persistence_roundtrip_and_restart_reconcile() {
    let dir = tempfile::tempdir().unwrap();
    let t = TenantId::generate();
    let f = fx::function(&t, "persisted");
    let r = fx::ready_revision(&f, 1);
    let key = fx::key(&t, &r.id);
    let alias = FunctionAlias::new(
        f.id.clone(),
        t.clone(),
        AliasName::default_alias(),
        r.id.clone(),
        now(),
    );
    let digest = Sha256Digest::of_bytes(b"binary");
    let inv = fx::invocation(&t, &f.id, Some("k"));
    let mut pooled = fx::ready_environment(&key);
    pooled.assign(now()).unwrap();
    pooled.mark_idle(now()).unwrap();
    let mut done = fx::ready_environment(&key);
    done.mark_stopped(now()).unwrap();
    {
        let store = open(dir.path());
        FunctionRepository::insert(&store, f.clone()).unwrap();
        assert_eq!(store.allocate_number(&f.id).unwrap(), 1);
        RevisionRepository::insert(&store, r.clone()).unwrap();
        AliasRepository::insert(&store, alias.clone()).unwrap();
        store.claim(&t, &digest).unwrap();
        store.insert_bound(inv.clone()).unwrap();
        EnvironmentRepository::insert(&store, pooled.clone()).unwrap();
        EnvironmentRepository::insert(&store, done.clone()).unwrap();
        let lease = ExecutionLease::acquire(
            LeaseId::generate(),
            pooled.id.clone(),
            AttemptId::generate(),
            t.clone(),
            1,
            now(),
            now(),
        );
        // A lease without an owner, as a schema-2 gateway left it.
        insert_lease_row(&store.conn.lock(), &lease).unwrap();
        store.flush().unwrap();
    }
    assert!(dir.path().join(SqliteStore::FILE_NAME).exists());
    assert!(
        !dir.path().join("state.json").exists(),
        "nothing writes JSON"
    );

    let store = open(dir.path());
    let settled = store.open_report().settled;
    assert_eq!(
        (settled.invocations, settled.environments, settled.leases),
        (1, 1, 1)
    );
    assert_eq!(
        FunctionRepository::get(&store, &f.id).unwrap(),
        Some(f.clone())
    );
    assert_eq!(
        RevisionRepository::get(&store, &r.id).unwrap(),
        Some(r.clone())
    );
    assert_eq!(
        AliasRepository::get(&store, &f.id, &alias.name).unwrap(),
        Some(alias)
    );
    assert_eq!(
        store.allocate_number(&f.id).unwrap(),
        2,
        "the counter survives"
    );
    assert!(store.is_owned_by(&t, &digest).unwrap());
    assert_eq!(
        store
            .lookup(&t, &f.id, "k", now())
            .unwrap()
            .map(|b| b.invocation_id),
        Some(inv.id.clone())
    );
    let invs = InvocationRepository::list_by_function(&store, &f.id, 10).unwrap();
    assert_eq!(invs.len(), 1);
    assert!(
        invs[0].status.is_terminal(),
        "work that never started is failed on restart"
    );
    assert!(
        EnvironmentRepository::list_active(&store)
            .unwrap()
            .is_empty(),
        "a pooled environment does not survive its process"
    );
    assert!(matches!(
        EnvironmentRepository::get(&store, &pooled.id)
            .unwrap()
            .unwrap()
            .state,
        EnvironmentState::Lost { .. }
    ));
    assert_eq!(
        EnvironmentRepository::get(&store, &done.id).unwrap(),
        Some(done),
        "terminal environments are untouched"
    );

    // A third open finds nothing left to settle.
    drop(store);
    assert_eq!(open(dir.path()).open_report().settled, Settled::default());
}

/// docs/threat-model.md §9: a restart may only report a failure for work
/// that provably never started.
#[test]
fn restart_separates_dispatched_work_from_work_that_never_started() {
    let dir = tempfile::tempdir().unwrap();
    let t = TenantId::generate();
    let f = FunctionId::generate();

    let mut queued = fx::invocation(&t, &f, None);
    queued.mark_queued().unwrap();
    let mut running = fx::invocation(&t, &f, None);
    let attempt_id = AttemptId::generate();
    running
        .mark_running(attempt_id.clone(), now(), now(), now())
        .unwrap();
    let mut finished = fx::invocation(&t, &f, None);
    finished
        .mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    finished.mark_succeeded(None, None, now()).unwrap();
    let mut attempt = fx::attempt(&running, &EnvironmentId::generate());
    attempt.id = attempt_id.clone();
    let queued_attempt = fx::attempt(&queued, &EnvironmentId::generate());
    let (queued_id, running_id, finished_id) =
        (queued.id.clone(), running.id.clone(), finished.id.clone());
    {
        let store = open(dir.path());
        for inv in [queued, running, finished] {
            InvocationRepository::insert(&store, inv).unwrap();
        }
        store.insert_attempt(attempt).unwrap();
        store.insert_attempt(queued_attempt.clone()).unwrap();
        // A dangling key left by an older version.
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO idempotency (tenant_id, function_id, idem_key, invocation_id, input_digest) \
                 VALUES (?1, ?2, 'dangling', ?3, ?4)",
                params![
                    t.as_str(),
                    f.as_str(),
                    InvocationId::generate().as_str(),
                    Sha256Digest::of_bytes(b"{}").as_str()
                ],
            )
            .unwrap();
    }

    let store = open(dir.path());
    assert_eq!(store.open_report().settled.idempotency_dropped, 1);
    assert_eq!(store.lookup(&t, &f, "dangling", now()).unwrap(), None);
    let load = |id: &InvocationId| InvocationRepository::get(&store, id).unwrap().unwrap();
    match load(&queued_id).status {
        InvocationStatus::Failed { error } => {
            assert_eq!(error.class, ErrorClass::PlatformError);
            assert_eq!(error.error_type, HOST_RESTARTED);
        }
        other => panic!("a queued invocation was never dispatched: {other:?}"),
    }
    match load(&running_id).status {
        InvocationStatus::OutcomeUnknown { error } => {
            assert_eq!(error.class, ErrorClass::OutcomeUnknown);
            assert_eq!(error.error_type, HOST_RESTARTED);
        }
        other => panic!("a dispatched invocation may have run: {other:?}"),
    }
    assert_eq!(
        load(&finished_id).status,
        InvocationStatus::Succeeded,
        "terminal invocations are untouched"
    );
    match store.get_attempt(&attempt_id).unwrap().unwrap().status {
        AttemptStatus::OutcomeUnknown { error } => {
            assert_eq!(error.error_type, HOST_RESTARTED);
        }
        other => panic!("the attempt follows its invocation: {other:?}"),
    }
    assert!(matches!(
        store
            .get_attempt(&queued_attempt.id)
            .unwrap()
            .unwrap()
            .status,
        AttemptStatus::Failed { .. }
    ));
}

#[cfg(unix)]
#[test]
fn the_database_file_is_private_to_its_owner() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    FunctionRepository::insert(&store, fx::function(&TenantId::generate(), "private")).unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let p = dir
            .path()
            .join(format!("{}{suffix}", SqliteStore::FILE_NAME));
        if let Ok(meta) = std::fs::metadata(&p) {
            assert_eq!(
                meta.permissions().mode() & 0o077,
                0,
                "{} is readable by others",
                p.display()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// state.json import
// ---------------------------------------------------------------------------

/// A ledger in the exact shape the P1 `InMemoryStore` wrote.
fn write_p1_state(dir: &Path) -> serde_json::Value {
    let t = TenantId::generate();
    let f = fx::function(&t, "legacy");
    let r = fx::ready_revision(&f, 1);
    let alias = FunctionAlias::new(
        f.id.clone(),
        t.clone(),
        AliasName::default_alias(),
        r.id.clone(),
        now(),
    );
    let mut done = fx::invocation(&t, &f.id, Some("replay"));
    done.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    done.mark_succeeded(Some(fx::inline(b"{\"legacy\":true}")), Some(200), now())
        .unwrap();
    let mut in_flight = fx::invocation(&t, &f.id, None);
    in_flight
        .mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    let attempt = fx::attempt(&in_flight, &EnvironmentId::generate());
    let key = fx::key(&t, &r.id);
    let mut env = fx::ready_environment(&key);
    env.mark_busy(now()).unwrap();
    let digest = Sha256Digest::of_bytes(b"artifact");

    let map = |pairs: Vec<(String, serde_json::Value)>| {
        serde_json::Value::Object(pairs.into_iter().collect())
    };
    let state = serde_json::json!({
        "functions": map(vec![(f.id.to_string(), j(&f))]),
        "revisions": map(vec![(r.id.to_string(), j(&r))]),
        "revision_counters": {f.id.to_string(): 1},
        "aliases": map(vec![(format!("{}/{}", f.id, alias.name), j(&alias))]),
        "invocations": map(vec![
            (done.id.to_string(), j(&done)),
            (in_flight.id.to_string(), j(&in_flight)),
        ]),
        "attempts": map(vec![(attempt.id.to_string(), j(&attempt))]),
        "environments": map(vec![(env.id.to_string(), j(&env))]),
        "idempotency": [
            [
                {"tenant_id": t.to_string(), "function_id": f.id.to_string(), "key": "replay"},
                {"invocation_id": done.id.to_string(), "input_digest": done.input_digest.to_string()}
            ],
            [
                {"tenant_id": t.to_string(), "function_id": f.id.to_string(), "key": "dangling"},
                {"invocation_id": InvocationId::generate().to_string(),
                 "input_digest": Sha256Digest::of_bytes(b"{}").to_string()}
            ]
        ],
        "artifact_owners": {digest.to_string(): [t.to_string()]},
    });
    std::fs::write(
        dir.join("state.json"),
        serde_json::to_vec_pretty(&state).unwrap(),
    )
    .unwrap();
    state
}

fn j<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).unwrap()
}

fn ids(v: &serde_json::Value, field: &str) -> Vec<String> {
    let mut out: Vec<String> = v[field].as_object().unwrap().keys().cloned().collect();
    out.sort();
    out
}

/// docs/adr/0003 A8.
#[test]
fn a_p1_state_json_is_imported_once_and_moved_aside() {
    let dir = tempfile::tempdir().unwrap();
    let state = write_p1_state(dir.path());
    let t = TenantId::parse(
        state["functions"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()["tenant_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let f = FunctionId::parse(&ids(&state, "functions")[0]).unwrap();

    let store = open(dir.path());
    let report = store.open_report().clone();
    let moved = report.imported_state_json.clone().expect("imported");
    assert!(!dir.path().join("state.json").exists(), "renamed, not left");
    assert!(moved.exists(), "the JSON ledger is kept, never deleted");
    assert!(
        moved
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("state.json.imported-")
    );

    // The ledger matches, row for row.
    assert_eq!(FunctionRepository::list(&store, &t).unwrap().len(), 1);
    assert_eq!(
        RevisionRepository::list_by_function(&store, &f)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(AliasRepository::list(&store, &f).unwrap().len(), 1);
    let mut invocations: Vec<String> = InvocationRepository::list_by_function(&store, &f, 10)
        .unwrap()
        .iter()
        .map(|i| i.id.to_string())
        .collect();
    invocations.sort();
    assert_eq!(invocations, ids(&state, "invocations"));
    assert_eq!(
        store.allocate_number(&f).unwrap(),
        2,
        "the counter came along"
    );
    let digest = Sha256Digest::parse(&ids(&state, "artifact_owners")[0]).unwrap();
    assert!(store.is_owned_by(&t, &digest).unwrap());
    // The import went through the P1 restart semantics.
    assert_eq!(report.settled.invocations, 1);
    assert_eq!(report.settled.attempts, 1);
    assert_eq!(report.settled.environments, 1);
    assert_eq!(report.settled.idempotency_dropped, 1);
    assert!(store.lookup(&t, &f, "replay", now()).unwrap().is_some());
    assert!(store.lookup(&t, &f, "dangling", now()).unwrap().is_none());
    drop(store);

    // The second start does not import again.
    let again = open(dir.path());
    assert_eq!(again.open_report().imported_state_json, None);
    assert_eq!(
        InvocationRepository::list_by_function(&again, &f, 10)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn an_interrupted_import_only_finishes_the_rename() {
    let dir = tempfile::tempdir().unwrap();
    write_p1_state(dir.path());
    let original = std::fs::read(dir.path().join("state.json")).unwrap();
    drop(open(dir.path()));
    // The rename was lost: the same bytes are back next to a populated db.
    std::fs::write(dir.path().join("state.json"), &original).unwrap();
    let store = open(dir.path());
    assert!(store.open_report().imported_state_json.is_some());
    assert!(!dir.path().join("state.json").exists());
    let n: i64 = store
        .conn
        .lock()
        .query_row("SELECT COUNT(*) FROM invocations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "nothing was imported twice");
}

#[test]
fn a_state_json_next_to_a_populated_database_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = open(dir.path());
        FunctionRepository::insert(&store, fx::function(&TenantId::generate(), "live")).unwrap();
    }
    write_p1_state(dir.path());
    let msg = open_err(dir.path());
    assert!(msg.contains("state.json"), "{msg}");
    assert!(msg.contains("state.db"), "{msg}");
    assert!(
        dir.path().join("state.json").exists(),
        "a refused import leaves the file where it was"
    );
}

#[test]
fn corrupt_state_file_is_refused_with_a_hint() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("state.json"), vec![0u8; 64]).unwrap();
    let msg = open_err(dir.path());
    assert!(msg.contains("not a valid state file"), "{msg}");
    assert!(msg.contains("Move it aside"), "{msg}");
    assert!(dir.path().join("state.json").exists());
    // Nothing half-imported: once the file is moved aside the store opens empty.
    std::fs::remove_file(dir.path().join("state.json")).unwrap();
    let store = open(dir.path());
    assert!(!ledger_has_rows(&store.conn.lock()).unwrap());
}

#[test]
fn state_without_artifact_owners_still_loads() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("state.json"), br#"{"functions": {}}"#).unwrap();
    let store = open(dir.path());
    assert!(
        !store
            .is_owned_by(&TenantId::generate(), &Sha256Digest::of_bytes(b"x"))
            .unwrap()
    );
    assert!(store.open_report().imported_state_json.is_some());
}

// ---------------------------------------------------------------------------
// bodies and retention
// ---------------------------------------------------------------------------

#[test]
fn inline_output_is_replaced_by_its_digest_after_retention() {
    let dir = tempfile::tempdir().unwrap();
    let retention = chrono::Duration::hours(1);
    let options = SqliteOptions {
        output_retention: Some(retention),
        ..SqliteOptions::default()
    };
    let store = SqliteStore::open(dir.path(), Limits::default(), options.clone(), now()).unwrap();
    let t = TenantId::generate();
    let f = FunctionId::generate();
    let body = b"{\"secretless\":\"but private\"}";
    let mut inv = fx::invocation(&t, &f, None);
    InvocationRepository::insert(&store, inv.clone()).unwrap();
    inv.mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    InvocationRepository::update(&store, inv.clone()).unwrap();
    inv.mark_succeeded(Some(fx::inline(body)), Some(200), now())
        .unwrap();
    InvocationRepository::update(&store, inv.clone()).unwrap();

    assert_eq!(store.purge_expired_outputs(now()).unwrap(), 0, "not yet");
    assert_eq!(
        store
            .purge_expired_outputs(now() + retention - chrono::Duration::seconds(1))
            .unwrap(),
        0
    );
    assert_eq!(store.purge_expired_outputs(now() + retention).unwrap(), 1);
    let purged = InvocationRepository::get(&store, &inv.id).unwrap().unwrap();
    assert_eq!(
        purged.output,
        Some(PayloadRef::Digest {
            digest: Sha256Digest::of_bytes(body),
            size_bytes: body.len() as u64,
        })
    );
    assert_eq!(purged.status, InvocationStatus::Succeeded);
    assert_eq!(
        store.purge_expired_outputs(now() + retention * 2).unwrap(),
        0,
        "purged once"
    );
    store.flush().unwrap();
    drop(store);
    let mut on_disk = std::fs::read(dir.path().join(SqliteStore::FILE_NAME)).unwrap();
    on_disk.extend(std::fs::read(dir.path().join("state.db-wal")).unwrap_or_default());
    let text = String::from_utf8_lossy(&on_disk);
    assert!(
        !text.contains(&fx_b64(body)),
        "the body is gone from the file after a checkpoint"
    );

    // Opening past the retention purges as well; without retention nothing is.
    let kept = SqliteStore::open(
        tempfile::tempdir().unwrap().path(),
        Limits::default(),
        SqliteOptions::default(),
        now(),
    )
    .unwrap();
    let mut other = fx::invocation(&t, &f, None);
    other
        .mark_running(AttemptId::generate(), now(), now(), now())
        .unwrap();
    other
        .mark_succeeded(Some(fx::inline(body)), None, now())
        .unwrap();
    InvocationRepository::insert(&kept, other.clone()).unwrap();
    assert_eq!(
        kept.purge_expired_outputs(now() + chrono::Duration::days(3650))
            .unwrap(),
        0
    );
}

fn fx_b64(bytes: &[u8]) -> String {
    match fx::inline(bytes) {
        PayloadRef::Inline { bytes_base64, .. } => bytes_base64,
        PayloadRef::Digest { .. } => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// separate connections to one file
// ---------------------------------------------------------------------------

/// The same CAS races as the contract suite, but every racer holds its own
/// connection to the same `state.db`, as separate gateway processes would.
/// Serialization comes from SQLite's file lock, not from the store's mutex.
/// (Threads, not OS processes: docs/adr/0003 A1/A6 with real processes are
/// PLT-4631's.)
#[test]
fn cas_holds_across_separate_connections_to_the_same_file() {
    let dir = tempfile::tempdir().unwrap();
    let t = TenantId::generate();
    let f = fx::function(&t, "shared");
    let base = fx::ready_revision(&f, 1);
    let key = fx::key(&t, &base.id);
    let racers = 6;
    let targets: Vec<_> = (0..racers).map(|i| fx::ready_revision(&f, 2 + i)).collect();
    let alias = FunctionAlias::new(
        f.id.clone(),
        t.clone(),
        AliasName::default_alias(),
        base.id.clone(),
        now(),
    );
    {
        let store = open(dir.path());
        FunctionRepository::insert(&store, f.clone()).unwrap();
        RevisionRepository::insert(&store, base.clone()).unwrap();
        for r in &targets {
            RevisionRepository::insert(&store, r.clone()).unwrap();
        }
        AliasRepository::insert(&store, alias.clone()).unwrap();
        let mut env = fx::ready_environment(&key);
        env.assign(now()).unwrap();
        env.mark_idle(now()).unwrap();
        EnvironmentRepository::insert(&store, env).unwrap();
    }
    // Opened before the race, so no racer's open-time reconcile interferes.
    let stores: Vec<Arc<SqliteStore>> = (0..racers).map(|_| Arc::new(open(dir.path()))).collect();
    // The reconcile of those opens settled the idle environment as Lost;
    // pool it again through one connection.
    let pooled_env = {
        let mut env = fx::ready_environment(&key);
        env.assign(now()).unwrap();
        env.mark_idle(now()).unwrap();
        EnvironmentRepository::insert(&*stores[0], env.clone()).unwrap();
        env.id
    };

    let barrier = Arc::new(std::sync::Barrier::new(racers as usize));
    let handles: Vec<_> = stores
        .iter()
        .cloned()
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
    assert_eq!(
        claims.len(),
        1,
        "one connection claims the idle environment"
    );
    assert_eq!(claims[0].id, pooled_env);
    assert_eq!(
        claims[0].epoch, 1,
        "a claim reserves; the acquire moves the epoch"
    );

    let check = open(dir.path());
    assert_eq!(
        AliasRepository::get(&check, &f.id, &AliasName::default_alias())
            .unwrap()
            .unwrap()
            .generation,
        2
    );
}

// ---------------------------------------------------------------------------
// slots across connections and OS processes (PLT-4631, ADR-0003 A1 / A2 / A6)
// ---------------------------------------------------------------------------

fn at(seconds: i64) -> Timestamp {
    now() + chrono::Duration::seconds(seconds)
}

fn register(store: &dyn SlotStore, instance: &str, pid: u32, ttl_s: i64) -> DispatcherId {
    let id = DispatcherId::generate();
    store
        .register_dispatcher(DispatcherRecord {
            id: id.clone(),
            instance: instance.into(),
            hostname: crate::services::dispatcher::hostname(),
            pid,
            started_at: now(),
            heartbeat_at: now(),
            lease_expires_at: at(ttl_s),
            stopped_at: None,
            reclaimed_at: None,
        })
        .unwrap();
    id
}

/// The acquire request of `env` (as stored) for `inv` by `owner`.
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
        at(600),
        now(),
    )
    .owned_by(owner.clone(), at(ttl_s));
    let mut running = inv.clone();
    running
        .mark_running(attempt.id.clone(), at(600), now(), now())
        .unwrap();
    SlotAcquire {
        env: assigned,
        expected_epoch: env.epoch,
        lease,
        attempt,
        invocation: Some(running),
    }
}

fn owned_invocation(store: &SqliteStore, tenant: &TenantId, owner: &DispatcherId) -> Invocation {
    let mut inv = fx::invocation(tenant, &FunctionId::generate(), None);
    inv.dispatcher_id = Some(owner.clone());
    InvocationRepository::insert(store, inv.clone()).unwrap();
    inv
}

fn ready_owned_env(store: &SqliteStore, owner: &DispatcherId) -> ExecutionEnvironment {
    let key = fx::key(&TenantId::generate(), &RevisionId::generate());
    let env = fx::ready_environment(&key).owned_by(owner.clone());
    EnvironmentRepository::insert(store, env.clone()).unwrap();
    env
}

/// Property (ADR-0003 A1 with threads): N connections, each its own SQLite
/// connection to the same file, race to acquire the same slot at the same
/// epoch. In every round exactly one wins and the epoch moves by one.
#[test]
fn concurrent_acquires_on_separate_connections_have_one_winner_per_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let setup = open(dir.path());
    let owner = register(&setup, "race", std::process::id(), 3600);
    let env = ready_owned_env(&setup, &owner);
    let big = PoolLimits {
        max_idle_per_key: 64,
        max_total_idle: 64,
    };
    for racers in [2usize, 4, 8, 12] {
        let stores: Vec<Arc<SqliteStore>> =
            (0..racers).map(|_| Arc::new(open(dir.path()))).collect();
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
        let leases: i64 = setup
            .conn
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM leases WHERE environment_id = ?1 AND released = 0",
                [env.id.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leases, 1, "one live lease per slot");
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

/// The reclaim of an expired lease and the binding of one idempotency key
/// each succeed exactly once when several connections race for them.
#[test]
fn reclaim_and_key_binding_are_exactly_once_across_connections() {
    let dir = tempfile::tempdir().unwrap();
    let setup = open(dir.path());
    let dead = register(&setup, "dead", std::process::id(), 10);
    let env = ready_owned_env(&setup, &dead);
    let inv = owned_invocation(&setup, &env.tenant_id, &dead);
    assert_eq!(
        setup.acquire(request(&env, &inv, &dead, 10)).unwrap(),
        AcquireOutcome::Acquired
    );
    let racers = 8;
    let reclaimers: Vec<DispatcherId> = (0..racers)
        .map(|i| register(&setup, &format!("r{i}"), std::process::id(), 3600))
        .collect();
    let stores: Vec<Arc<SqliteStore>> = (0..racers).map(|_| Arc::new(open(dir.path()))).collect();
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
                        now: at(20),
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
        .into_iter()
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
    assert_eq!(inserted.len(), 1);
    for (_, o) in &bound {
        if let IdempotencyOutcome::Existing(b) = o {
            assert_eq!(b.invocation_id, inserted[0]);
        }
    }
}

// -- OS processes ------------------------------------------------------------

const CHILD_TEST: &str = "repository::sqlite::tests::slot_race_child";

/// Not a test on its own: the body of a child process spawned by the tests
/// below (it returns at once unless `TSLS_SLOT_RACE_DB` is set). Actions:
///
/// - `acquire`: race to acquire `TSLS_ENV` for `TSLS_INVOCATION` as
///   `TSLS_OWNER`;
/// - `bind`: race to bind the idempotency key `TSLS_KEY`;
/// - `hold`: register as instance `child`, acquire a slot with a 10 s lease
///   and exit without releasing it (a crashed dispatcher).
///
/// Every child waits for `<db dir>/go` before acting, so they start together,
/// and writes its outcome to `TSLS_OUT`.
#[test]
fn slot_race_child() {
    let Ok(dir) = std::env::var("TSLS_SLOT_RACE_DB") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let var = |k: &str| std::env::var(k).unwrap();
    let store =
        SqliteStore::open(&dir, Limits::default(), SqliteOptions::default(), now()).unwrap();
    // Everything a racer reads, it reads before the start signal: every
    // process then holds the same view of the slot, as racing dispatchers do.
    let prepared = (var("TSLS_ACTION") == "acquire").then(|| {
        let owner = DispatcherId::parse(&var("TSLS_OWNER")).unwrap();
        let env_id = tachyon_serverless_domain::EnvironmentId::parse(&var("TSLS_ENV")).unwrap();
        let inv_id = InvocationId::parse(&var("TSLS_INVOCATION")).unwrap();
        let env = EnvironmentRepository::get(&store, &env_id)
            .unwrap()
            .unwrap();
        let inv = InvocationRepository::get(&store, &inv_id).unwrap().unwrap();
        request(&env, &inv, &owner, 30)
    });
    std::fs::write(format!("{}.ready", var("TSLS_OUT")), b"ready").unwrap();
    let go = dir.join("go");
    while !go.exists() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let out = match var("TSLS_ACTION").as_str() {
        "acquire" => match store.acquire(prepared.unwrap()).unwrap() {
            AcquireOutcome::Acquired => "acquired".to_string(),
            AcquireOutcome::Lost(_) => "lost".to_string(),
        },
        "bind" => {
            let t = TenantId::parse(&var("TSLS_TENANT")).unwrap();
            let f = FunctionId::parse(&var("TSLS_FUNCTION")).unwrap();
            let inv = fx::invocation(&t, &f, Some(&var("TSLS_KEY")));
            match store.insert_bound(inv.clone()).unwrap() {
                IdempotencyOutcome::Inserted => format!("inserted {}", inv.id),
                IdempotencyOutcome::Existing(b) => format!("existing {}", b.invocation_id),
            }
        }
        "hold" => {
            let owner = register(&store, "child", std::process::id(), 10);
            let env = ready_owned_env(&store, &owner);
            let inv = owned_invocation(&store, &env.tenant_id, &owner);
            let req = request(&env, &inv, &owner, 10);
            assert_eq!(
                store.acquire(req.clone()).unwrap(),
                AcquireOutcome::Acquired
            );
            format!(
                "{} {} {} {} {}",
                owner,
                env.id,
                req.lease.id,
                req.attempt.id,
                std::process::id()
            )
        }
        other => panic!("unknown action {other}"),
    };
    std::fs::write(var("TSLS_OUT"), out).unwrap();
    // Exit without dropping anything: nothing is released on the way out.
    std::process::exit(0);
}

fn spawn_child(
    dir: &Path,
    n: usize,
    action: &str,
    vars: &[(&str, String)],
) -> (std::process::Child, std::path::PathBuf) {
    let out = dir.join(format!("out-{action}-{n}"));
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
        .env("TSLS_SLOT_RACE_DB", dir)
        .env("TSLS_ACTION", action)
        .env("TSLS_OUT", &out)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (k, v) in vars {
        cmd.env(k, v);
    }
    (cmd.spawn().unwrap(), out)
}

fn run_children(
    dir: &Path,
    children: Vec<(std::process::Child, std::path::PathBuf)>,
) -> Vec<String> {
    // Start them together: only once every child has prepared and is waiting.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    for (_, out) in &children {
        let ready = std::path::PathBuf::from(format!("{}.ready", out.display()));
        while !ready.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "a child never got ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        std::fs::remove_file(ready).unwrap();
    }
    std::fs::write(dir.join("go"), b"go").unwrap();
    children
        .into_iter()
        .map(|(child, out)| {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "child failed: {}\n{}{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let text = std::fs::read_to_string(&out).unwrap();
            let _ = std::fs::remove_file(&out);
            text
        })
        .collect()
}

/// ADR-0003 A1 and A6 with **OS processes**: several gateway-like processes
/// open the same `state.db`; for one slot exactly one acquire wins, and for
/// one idempotency key exactly one binds while every other process gets the
/// same invocation back.
#[test]
fn separate_processes_racing_for_one_slot_or_one_key_have_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let setup = open(dir.path());
    let owner = register(&setup, "race", std::process::id(), 3600);
    let env = ready_owned_env(&setup, &owner);
    let processes = 6;
    let children: Vec<_> = (0..processes)
        .map(|n| {
            let inv = owned_invocation(&setup, &env.tenant_id, &owner);
            spawn_child(
                dir.path(),
                n,
                "acquire",
                &[
                    ("TSLS_OWNER", owner.to_string()),
                    ("TSLS_ENV", env.id.to_string()),
                    ("TSLS_INVOCATION", inv.id.to_string()),
                ],
            )
        })
        .collect();
    let results = run_children(dir.path(), children);
    assert_eq!(
        results.iter().filter(|r| *r == "acquired").count(),
        1,
        "{results:?}"
    );
    assert_eq!(
        results.iter().filter(|r| *r == "lost").count(),
        processes - 1
    );
    assert_eq!(
        EnvironmentRepository::get(&setup, &env.id)
            .unwrap()
            .unwrap()
            .epoch,
        env.epoch + 1
    );
    std::fs::remove_file(dir.path().join("go")).unwrap();

    let t = TenantId::generate();
    let f = FunctionId::generate();
    let children: Vec<_> = (0..processes)
        .map(|n| {
            spawn_child(
                dir.path(),
                n,
                "bind",
                &[
                    ("TSLS_TENANT", t.to_string()),
                    ("TSLS_FUNCTION", f.to_string()),
                    ("TSLS_KEY", "one-key".to_string()),
                ],
            )
        })
        .collect();
    let results = run_children(dir.path(), children);
    let inserted: Vec<&str> = results
        .iter()
        .filter_map(|r| r.strip_prefix("inserted "))
        .collect();
    assert_eq!(inserted.len(), 1, "{results:?}");
    for r in &results {
        let id = r.split_once(' ').unwrap().1;
        assert_eq!(id, inserted[0], "every process sees the same invocation");
    }
}

/// ADR-0003 A2 with an **OS process**: a dispatcher process takes a slot and
/// exits without releasing it. Another dispatcher (another instance) cannot
/// reclaim it before its expiry (plus the skew), then reclaims it exactly
/// once; the late completion of the dead process's attempt is refused. The
/// same instance restarting on this host proves the process is gone and may
/// reclaim at once.
#[test]
fn a_lease_left_by_an_exited_process_is_reclaimed_once_and_only_after_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<SqliteStore> = Arc::new(open(dir.path()));
    let child = spawn_child(dir.path(), 0, "hold", &[]);
    let out = run_children(dir.path(), vec![child]);
    let fields: Vec<&str> = out[0].split(' ').collect();
    let dead_owner = DispatcherId::parse(fields[0]).unwrap();
    let env_id = tachyon_serverless_domain::EnvironmentId::parse(fields[1]).unwrap();
    let lease_id = LeaseId::parse(fields[2]).unwrap();
    let attempt_id = AttemptId::parse(fields[3]).unwrap();
    let child_pid: u32 = fields[4].parse().unwrap();
    assert!(
        !crate::services::dispatcher::pid_alive(child_pid),
        "the child exited"
    );

    let other = register(&*store, "parent", std::process::id(), 3600);
    let reclaim = |when| {
        store
            .reclaim_expired(ReclaimRequest {
                reclaimer: other.clone(),
                now: when,
                skew: chrono::Duration::seconds(2),
                presumed_dead: Vec::new(),
            })
            .unwrap()
    };
    // Dead, but another instance cannot know that: nothing before expiry.
    assert!(reclaim(at(5)).is_empty());
    assert!(reclaim(at(11)).is_empty(), "within the clock skew");
    let report = reclaim(at(12));
    assert_eq!(report.leases, 1);
    assert_eq!(report.dispatchers, vec![dead_owner.clone()]);
    assert_eq!(report.fenced.len(), 1);
    assert!(reclaim(at(13)).is_empty(), "exactly once");

    // The dead attempt's late result is stale.
    let mut late = InvocationRepository::get_attempt(&*store, &attempt_id)
        .unwrap()
        .unwrap();
    late.status = AttemptStatus::Dispatched;
    let mut done = late.clone();
    let _ = done.succeed(at(14));
    let completion = store
        .complete(SlotCompletion {
            lease_id,
            attempt: done,
            invocation: None,
            now: at(14),
        })
        .unwrap();
    assert!(matches!(completion, CompletionOutcome::Stale(_)));
    let env = EnvironmentRepository::get(&*store, &env_id)
        .unwrap()
        .unwrap();
    assert!(env.is_fenced());
    assert!(!env.is_terminal(), "fenced until a terminate is confirmed");

    // The same instance on the same host proves the process gone at once.
    let dir2 = tempfile::tempdir().unwrap();
    let store2: Arc<SqliteStore> = Arc::new(open(dir2.path()));
    let child = spawn_child(dir2.path(), 0, "hold", &[]);
    let out = run_children(dir2.path(), vec![child]);
    let dead2 = DispatcherId::parse(out[0].split(' ').next().unwrap()).unwrap();
    let restarted = crate::services::Dispatcher::register(
        store2.clone(),
        Arc::new(tachyon_serverless_domain::FixedClock::new(at(1))),
        &tachyon_serverless_domain::UlidGenerator,
        crate::config::DispatcherConfig::default(),
        "child".into(),
    )
    .unwrap();
    assert_eq!(restarted.presumed_dead(), vec![dead2.clone()]);
    let report = restarted.reclaim_ledger().unwrap();
    assert_eq!(report.dispatchers, vec![dead2]);
    assert_eq!(report.leases, 1);
    let unrelated = crate::services::Dispatcher::register(
        store2,
        Arc::new(tachyon_serverless_domain::FixedClock::new(at(1))),
        &tachyon_serverless_domain::UlidGenerator,
        crate::config::DispatcherConfig::default(),
        "another-instance".into(),
    )
    .unwrap();
    assert!(
        unrelated.presumed_dead().is_empty(),
        "another instance never presumes a process dead"
    );
}

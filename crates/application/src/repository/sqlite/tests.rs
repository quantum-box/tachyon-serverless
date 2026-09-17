//! SQLite-only behaviour: migrations, persistence across opens, the restart
//! reconcile, the one-time `state.json` import, output retention, index use
//! and CAS across separate connections to the same file.

use std::path::Path;
use std::sync::Arc;

use tachyon_serverless_domain::{
    AliasName, AttemptId, AttemptStatus, EnvironmentState, ErrorClass, ExecutionLease,
    FunctionAlias, FunctionId, InvocationStatus, LeaseId, Limits, PayloadRef, RevisionId,
    Sha256Digest, TenantId,
};

use super::super::contract_tests::fx::{self, now};
use super::*;
use crate::repository::HOST_RESTARTED;

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
    ] {
        assert!(t.contains(&table.to_string()), "{table} in {t:?}");
    }
    assert!(columns(&conn, "invocations").contains(&"output_expires_at".to_string()));
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
        },
        now(),
    )
    .unwrap();
    assert_eq!(store.open_report().migrations_applied, vec![2]);
    assert_eq!(store.open_report().schema_version, 2);
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
    pooled.mark_busy(now()).unwrap();
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
        store.insert_lease(lease).unwrap();
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
            .lookup(&t, &f.id, "k")
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
    assert_eq!(store.lookup(&t, &f, "dangling").unwrap(), None);
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
    assert!(store.lookup(&t, &f, "replay").unwrap().is_some());
    assert!(store.lookup(&t, &f, "dangling").unwrap().is_none());
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
        env.mark_busy(now()).unwrap();
        env.mark_idle(now()).unwrap();
        EnvironmentRepository::insert(&store, env).unwrap();
    }
    // Opened before the race, so no racer's open-time reconcile interferes.
    let stores: Vec<Arc<SqliteStore>> = (0..racers).map(|_| Arc::new(open(dir.path()))).collect();
    // The reconcile of those opens settled the idle environment as Lost;
    // pool it again through one connection.
    let pooled_env = {
        let mut env = fx::ready_environment(&key);
        env.mark_busy(now()).unwrap();
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
                let claimed = store.claim_for_reuse(&key, now()).unwrap();
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
    assert_eq!(claims[0].epoch, 2);

    let check = open(dir.path());
    assert_eq!(
        AliasRepository::get(&check, &f.id, &AliasName::default_alias())
            .unwrap()
            .unwrap()
            .generation,
        2
    );
}

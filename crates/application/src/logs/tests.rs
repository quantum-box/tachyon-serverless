//! The durable log store (docs/adr/0018).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tachyon_serverless_domain::{
    AttemptId, Clock, EnvironmentId, FixedClock, FunctionId, InvocationId, Limits, LogPhase,
    LogRecord, LogStream, SystemClock, TenantId,
};

use super::store::{MARKER_PREFIX, MAX_MARKERS_PER_INVOCATION};
use super::{DurableLogStore, LogsConfig};
use crate::repository::contract_tests::fx;
use crate::repository::{
    AppendOutcome, InMemoryStore, InvocationRepository, LogRepository, RepoError,
};

const WAIT: Duration = Duration::from_secs(10);

fn config() -> LogsConfig {
    LogsConfig {
        flush_interval_ms: 10,
        retention_interval_seconds: 3600,
        ..LogsConfig::default()
    }
}

fn open(dir: &Path, config: &LogsConfig, limits: &Limits) -> DurableLogStore {
    DurableLogStore::open(dir, config, limits, Arc::new(SystemClock))
}

fn record(
    tenant: &TenantId,
    invocation: &InvocationId,
    attempt: Option<&AttemptId>,
    line: &str,
) -> LogRecord {
    LogRecord {
        tenant_id: tenant.clone(),
        environment_id: EnvironmentId::parse("env_01hzzzzzzzzzzzzzzzzzzzzzza").unwrap(),
        invocation_id: Some(invocation.clone()),
        attempt_id: attempt.cloned(),
        stream: LogStream::Stdout,
        phase: LogPhase::Handler,
        timestamp: SystemClock.now(),
        line: line.into(),
        truncated: false,
    }
}

fn lines(store: &DurableLogStore, tenant: &TenantId, inv: &InvocationId) -> Vec<String> {
    store
        .query(tenant, inv)
        .unwrap()
        .records
        .into_iter()
        .map(|r| r.line)
        .collect()
}

fn user_lines(store: &DurableLogStore, tenant: &TenantId, inv: &InvocationId) -> Vec<String> {
    lines(store, tenant, inv)
        .into_iter()
        .filter(|l| !l.starts_with(MARKER_PREFIX))
        .collect()
}

#[test]
fn logs_survive_a_restart_on_the_same_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    let att = AttemptId::generate();
    {
        let store = open(dir.path(), &config(), &Limits::default());
        for i in 0..5 {
            assert_eq!(
                store.append(record(&t, &inv, Some(&att), &format!("line {i}"))),
                AppendOutcome::Stored
            );
        }
        // Dropping the last handle commits what is queued.
    }
    let store = open(dir.path(), &config(), &Limits::default());
    let q = store.query(&t, &inv).unwrap();
    assert_eq!(
        q.records
            .iter()
            .map(|r| r.line.as_str())
            .collect::<Vec<_>>(),
        ["line 0", "line 1", "line 2", "line 3", "line 4"]
    );
    assert!(q.records.iter().all(|r| r.attempt_id.as_ref() == Some(&att)
        && r.stream == LogStream::Stdout
        && r.phase == LogPhase::Handler));
    assert!(!q.dropped);
    assert_eq!(store.status().stored_lines, 5);
}

#[test]
fn lines_keep_their_order_across_many_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        flush_max_lines: 7,
        ..config()
    };
    let store = open(dir.path(), &cfg, &Limits::default());
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    let expected: Vec<String> = (0..500).map(|i| format!("{i:04}")).collect();
    for (i, l) in expected.iter().enumerate() {
        store.append(record(&t, &inv, None, l));
        if i % 50 == 0 {
            std::thread::sleep(Duration::from_millis(15));
        }
    }
    assert!(store.flush(WAIT));
    assert!(store.status().flushes > 1, "more than one batch");
    assert_eq!(lines(&store, &t, &inv), expected);
}

#[test]
fn a_long_line_is_cut_at_the_line_limit_and_marked_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_log_line_bytes: 8,
        ..Limits::default()
    };
    let store = open(dir.path(), &config(), &limits);
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    store.append(record(&t, &inv, None, "a very long line"));
    store.append(record(&t, &inv, None, "short"));
    let q = store.query(&t, &inv).unwrap();
    assert_eq!(q.records[0].line, "a very l");
    assert!(q.records[0].truncated);
    assert_eq!(q.records[1].line, "short");
    assert!(!q.records[1].truncated);
    assert_eq!(store.status().lines_truncated, 1);
}

#[test]
fn the_invocation_cap_drops_counts_and_marks_once() {
    let dir = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_log_lines_per_invocation: 3,
        ..Limits::default()
    };
    let store = open(dir.path(), &config(), &limits);
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    for i in 0..10 {
        store.append(record(&t, &inv, None, &format!("l{i}")));
    }
    let q = store.query(&t, &inv).unwrap();
    assert!(q.dropped);
    let got: Vec<&str> = q.records.iter().map(|r| r.line.as_str()).collect();
    assert_eq!(&got[..3], ["l0", "l1", "l2"]);
    assert_eq!(got.len(), 4, "three lines and one marker: {got:?}");
    assert!(got[3].starts_with(MARKER_PREFIX) && got[3].contains("limit of this invocation"));
    assert_eq!(q.records[3].stream, LogStream::Platform);
    let s = store.status();
    assert_eq!(s.lines_dropped["invocation_limit"], 7);
    assert_eq!(s.marker_lines, 1);
    assert_eq!(s.lines_written, 3);

    // The byte cap too.
    let limits = Limits {
        max_log_bytes_per_invocation: 10,
        ..Limits::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path(), &config(), &limits);
    for l in ["12345", "12345", "1"] {
        store.append(record(&t, &inv, None, l));
    }
    assert_eq!(user_lines(&store, &t, &inv), ["12345", "12345"]);
    assert!(store.query(&t, &inv).unwrap().dropped);
}

#[test]
fn the_attempt_cap_is_applied_per_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        max_lines_per_attempt: Some(2),
        ..config()
    };
    let store = open(dir.path(), &cfg, &Limits::default());
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    let (a1, a2) = (AttemptId::generate(), AttemptId::generate());
    for i in 0..4 {
        store.append(record(&t, &inv, Some(&a1), &format!("a1-{i}")));
    }
    for i in 0..4 {
        store.append(record(&t, &inv, Some(&a2), &format!("a2-{i}")));
    }
    let q = store.query(&t, &inv).unwrap();
    assert!(q.dropped);
    let got: Vec<&str> = q.records.iter().map(|r| r.line.as_str()).collect();
    assert_eq!(got.len(), 6, "{got:?}");
    assert_eq!(&got[..2], ["a1-0", "a1-1"]);
    assert!(got[2].contains("limit of this attempt"));
    assert_eq!(&got[3..5], ["a2-0", "a2-1"]);
    assert!(got[5].contains("limit of this attempt"));
    assert_eq!(q.records[2].attempt_id.as_ref(), Some(&a1));
    assert_eq!(store.status().lines_dropped["attempt_limit"], 4);
}

#[test]
fn caps_hold_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_log_lines_per_invocation: 3,
        ..Limits::default()
    };
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    {
        let store = open(dir.path(), &config(), &limits);
        store.append(record(&t, &inv, None, "before-1"));
        store.append(record(&t, &inv, None, "before-2"));
    }
    let store = open(dir.path(), &config(), &limits);
    for i in 0..5 {
        store.append(record(&t, &inv, None, &format!("after-{i}")));
    }
    assert_eq!(
        user_lines(&store, &t, &inv),
        ["before-1", "before-2", "after-0"]
    );
    assert!(store.query(&t, &inv).unwrap().dropped);
}

#[test]
fn markers_per_invocation_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        max_lines_per_attempt: Some(1),
        ..config()
    };
    let store = open(dir.path(), &cfg, &Limits::default());
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    for _ in 0..40 {
        let a = AttemptId::generate();
        store.append(record(&t, &inv, Some(&a), "x"));
        store.append(record(&t, &inv, Some(&a), "over"));
    }
    let all = lines(&store, &t, &inv);
    let markers = all.iter().filter(|l| l.starts_with(MARKER_PREFIX)).count();
    assert_eq!(markers as i64, MAX_MARKERS_PER_INVOCATION);
    assert_eq!(all.len() - markers, 40);
}

#[test]
fn another_tenant_never_reads_the_lines() {
    let dir = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_log_lines_per_invocation: 1,
        ..Limits::default()
    };
    let store = open(dir.path(), &config(), &limits);
    let (a, b, inv) = (
        TenantId::generate(),
        TenantId::generate(),
        InvocationId::generate(),
    );
    store.append(record(&a, &inv, None, "tenant a secret output"));
    store.append(record(&a, &inv, None, "dropped"));
    assert_eq!(lines(&store, &a, &inv).len(), 2);
    let q = store.query(&b, &inv).unwrap();
    assert!(q.records.is_empty(), "cross-tenant read refused");
    assert!(
        !q.dropped,
        "nothing about the other tenant's invocation leaks"
    );
    // A line attributed to tenant B under the same id stays B's own.
    store.append(record(&b, &inv, None, "tenant b line"));
    assert_eq!(lines(&store, &b, &inv), ["tenant b line"]);
    assert!(
        !lines(&store, &a, &inv)
            .iter()
            .any(|l| l.contains("tenant b"))
    );
}

#[test]
fn lines_without_an_invocation_are_not_stored() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path(), &config(), &Limits::default());
    let mut r = record(
        &TenantId::generate(),
        &InvocationId::generate(),
        None,
        "boot",
    );
    r.invocation_id = None;
    assert_eq!(store.append(r), AppendOutcome::Dropped);
    assert!(store.flush(WAIT));
    let s = store.status();
    assert_eq!(s.lines_dropped["unattributed"], 1);
    assert_eq!(s.stored_lines, 0);
}

#[test]
fn a_full_queue_drops_and_counts_without_blocking_the_caller() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        queue_max_lines: 10,
        flush_max_lines: 1000,
        read_flush_wait_ms: 10_000,
        ..config()
    };
    let store = open(dir.path(), &cfg, &Limits::default());
    // A disk that takes 300 ms per batch.
    store.set_flush_delay(Duration::from_millis(300));
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    let started = Instant::now();
    let mut stored = 0;
    for i in 0..2000 {
        if store.append(record(&t, &inv, None, &format!("{i}"))) == AppendOutcome::Stored {
            stored += 1;
        }
    }
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "appends never wait for the writer: {:?}",
        started.elapsed()
    );
    let dropped = store.status().lines_dropped["queue_full"];
    assert_eq!(stored + dropped, 2000);
    assert!(dropped >= 1990 - 10, "{dropped}");
    store.set_flush_delay(Duration::ZERO);
    let q = store.query(&t, &inv).unwrap();
    assert!(q.dropped);
    let markers: Vec<&str> = q
        .records
        .iter()
        .map(|r| r.line.as_str())
        .filter(|l| l.starts_with(MARKER_PREFIX))
        .collect();
    assert_eq!(markers.len(), 1, "{markers:?}");
    assert!(markers[0].contains("writer queue was full"), "{markers:?}");
    assert_eq!(q.records.len() as u64, stored + 1);
}

#[test]
fn a_locked_database_degrades_the_store_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        read_flush_wait_ms: 5000,
        ..config()
    };
    let store = open(dir.path(), &cfg, &Limits::default());
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    store.append(record(&t, &inv, None, "before"));
    assert!(store.flush(WAIT));
    assert!(store.status().healthy);

    // Another process holds the write lock.
    let other = rusqlite::Connection::open(store.path()).unwrap();
    other.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    for i in 0..50 {
        assert_eq!(
            store.append(record(&t, &inv, None, &format!("lost {i}"))),
            AppendOutcome::Stored
        );
    }
    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(store.flush(WAIT));
    let s = store.status();
    assert!(!s.healthy, "degraded while locked");
    assert!(s.last_error.is_some());
    assert_eq!(s.lines_dropped["store_unavailable"], 50);
    assert!(s.flush_failures >= 1);
    // Reads still answer (WAL readers do not wait for the writer).
    assert_eq!(user_lines(&store, &t, &inv), ["before"]);

    other.execute_batch("ROLLBACK").unwrap();
    store.append(record(&t, &inv, None, "after"));
    assert!(store.flush(WAIT));
    assert!(store.status().healthy, "recovered");
    let q = store.query(&t, &inv).unwrap();
    assert!(q.dropped);
    let got: Vec<&str> = q.records.iter().map(|r| r.line.as_str()).collect();
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(got[0], "before");
    assert!(
        got.iter()
            .any(|l| l.contains("50 log line(s)") && l.contains("unavailable"))
    );
    assert!(got.contains(&"after"));
}

#[test]
fn a_database_that_cannot_be_opened_drops_lines_and_never_fails_the_caller() {
    let dir = tempfile::tempdir().unwrap();
    // `logs` is a file, not a directory: logs.db cannot be created.
    std::fs::write(dir.path().join(DurableLogStore::DIR_NAME), b"not a dir").unwrap();
    let store = open(dir.path(), &config(), &Limits::default());
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    assert_eq!(
        store.append(record(&t, &inv, None, "x")),
        AppendOutcome::Stored
    );
    assert!(store.flush(WAIT));
    let s = store.status();
    assert!(!s.healthy);
    assert_eq!(s.lines_dropped["store_unavailable"], 1);
    assert!(matches!(store.query(&t, &inv), Err(RepoError::Store(_))));
}

#[test]
fn concurrent_appends_keep_every_line_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        flush_max_lines: 64,
        ..config()
    };
    let store = Arc::new(open(dir.path(), &cfg, &Limits::default()));
    let t = TenantId::generate();
    let invocations: Vec<InvocationId> = (0..8).map(|_| InvocationId::generate()).collect();
    let threads: Vec<_> = invocations
        .iter()
        .cloned()
        .map(|inv| {
            let store = store.clone();
            let t = t.clone();
            std::thread::spawn(move || {
                for i in 0..300 {
                    assert_eq!(
                        store.append(record(&t, &inv, None, &format!("{i:03}"))),
                        AppendOutcome::Stored
                    );
                }
            })
        })
        .collect();
    // Readers race the writers.
    for inv in &invocations {
        let _ = store.query(&t, inv).unwrap();
    }
    for th in threads {
        th.join().unwrap();
    }
    let expected: Vec<String> = (0..300).map(|i| format!("{i:03}")).collect();
    for inv in &invocations {
        assert_eq!(lines(&store, &t, inv), expected);
    }
    assert_eq!(store.status().stored_lines, 2400);
}

#[test]
fn retention_by_age_removes_whole_invocations_past_the_horizon() {
    let dir = tempfile::tempdir().unwrap();
    let t0 = fx::now();
    let clock = Arc::new(FixedClock::new(t0));
    let cfg = LogsConfig {
        retention_seconds: 7 * 24 * 3600,
        ..config()
    };
    let store = DurableLogStore::open(dir.path(), &cfg, &Limits::default(), clock.clone());
    let t = TenantId::generate();
    let (old, young) = (InvocationId::generate(), InvocationId::generate());
    let mut r = record(&t, &old, None, "old");
    r.timestamp = t0;
    store.append(r);
    let mut r = record(&t, &young, None, "young");
    r.timestamp = t0 + chrono::Duration::days(6);
    store.append(r);
    assert!(store.flush(WAIT));
    clock.set(t0 + chrono::Duration::days(8));
    let report = store.run_retention().unwrap();
    assert_eq!((report.age_invocations, report.age_lines), (1, 1));
    assert!(lines(&store, &t, &old).is_empty());
    assert_eq!(lines(&store, &t, &young), ["young"]);
    let s = store.status();
    assert_eq!(s.retention_deleted_lines["age"], 1);
    assert_eq!(s.stored_lines, 1);
    assert_eq!(s.stored_bytes, 5);
}

#[test]
fn the_size_cap_removes_the_oldest_terminal_invocations_and_keeps_running_ones() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogsConfig {
        retention_seconds: 0,
        max_total_bytes: 250,
        ..config()
    };
    let store = open(dir.path(), &cfg, &Limits::default());
    let ledger = Arc::new(InMemoryStore::new(Limits::default()));
    let t = TenantId::generate();
    let f = FunctionId::generate();
    // Oldest first: running, done, done, done (100 bytes each).
    let mut ids = Vec::new();
    for i in 0..4 {
        let mut inv = fx::invocation(&t, &f, None);
        if i > 0 {
            inv.mark_cancelled(fx::now()).unwrap();
        }
        ledger.insert(inv.clone()).unwrap();
        store.append(record(&t, &inv.id, None, &"x".repeat(100)));
        assert!(store.flush(WAIT));
        ids.push(inv.id);
    }
    store.set_ledger(ledger.clone());
    assert_eq!(store.status().stored_bytes, 400);
    let report = store.run_retention().unwrap();
    assert_eq!(report.size_invocations, 2, "{report:?}");
    assert!(report.skipped_non_terminal >= 1);
    assert_eq!(lines(&store, &t, &ids[0]).len(), 1, "running: kept");
    assert!(
        lines(&store, &t, &ids[1]).is_empty(),
        "oldest terminal: deleted"
    );
    assert!(lines(&store, &t, &ids[2]).is_empty());
    assert_eq!(lines(&store, &t, &ids[3]).len(), 1, "newest: kept");
    let s = store.status();
    assert_eq!(s.stored_bytes, 200);
    assert_eq!(s.retention_deleted_lines["size"], 2);
    assert!(s.retention_skipped_non_terminal >= 1);

    // Over the cap with only a running invocation left: it is kept (the
    // documented exception) until the ledger reports it terminal.
    let cfg = LogsConfig {
        max_total_bytes: 50,
        ..cfg
    };
    drop(store);
    let store = open(dir.path(), &cfg, &Limits::default());
    store.set_ledger(ledger.clone());
    store.run_retention().unwrap();
    assert_eq!(lines(&store, &t, &ids[0]).len(), 1);
    assert!(lines(&store, &t, &ids[3]).is_empty());
    let mut running = ledger.get(&ids[0]).unwrap().unwrap();
    running.mark_cancelled(fx::now()).unwrap();
    ledger.update(running).unwrap();
    store.run_retention().unwrap();
    assert!(lines(&store, &t, &ids[0]).is_empty());
    assert_eq!(store.status().stored_bytes, 0);
}

#[cfg(unix)]
#[test]
fn the_directory_and_the_database_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path(), &config(), &Limits::default());
    let (t, inv) = (TenantId::generate(), InvocationId::generate());
    store.append(record(&t, &inv, None, "x"));
    assert!(store.flush(WAIT));
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&dir.path().join(DurableLogStore::DIR_NAME)), 0o700);
    assert_eq!(mode(store.path()), 0o600);
    let wal = format!("{}-wal", store.path().display());
    assert_eq!(mode(Path::new(&wal)), 0o600);
}

#[test]
fn a_newer_schema_is_refused_and_the_store_stays_degraded() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = open(dir.path(), &config(), &Limits::default());
        assert!(store.status().healthy);
        let path = store.path().to_path_buf();
        drop(store);
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute("UPDATE log_meta SET schema_version = 99", [])
            .unwrap();
    }
    let store = open(dir.path(), &config(), &Limits::default());
    let s = store.status();
    assert!(!s.healthy);
    assert!(s.last_error.unwrap().contains("schema version 99"));
}

#[test]
fn config_is_validated() {
    assert!(LogsConfig::default().validate().is_ok());
    for bad in [
        LogsConfig {
            flush_interval_ms: 0,
            ..LogsConfig::default()
        },
        LogsConfig {
            queue_max_lines: 0,
            ..LogsConfig::default()
        },
        LogsConfig {
            queue_max_bytes: 1,
            ..LogsConfig::default()
        },
        LogsConfig {
            max_lines_per_attempt: Some(0),
            ..LogsConfig::default()
        },
        LogsConfig {
            retention_interval_seconds: 0,
            ..LogsConfig::default()
        },
    ] {
        assert!(bad.validate().is_err(), "{bad:?}");
    }
}

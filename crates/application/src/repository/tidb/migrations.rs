//! Forward-only schema migrations on TiDB (docs/adr/0003 「TiDB 検証」).
//!
//! Same versions and names as [`crate::repository::sqlite::migrations`], but
//! TiDB DDL is **not transactional**: every DDL statement commits on its own,
//! so a migration with several statements cannot be rolled back as a whole.
//! The discipline that replaces the SQLite transaction:
//!
//! 1. **Additive only (expand).** A migration only creates tables, adds
//!    nullable / constant-default columns and adds indexes. The previous
//!    binary keeps working on a partly or fully expanded schema, so "a
//!    failing migration leaves the previous schema" holds in the sense that
//!    matters: every table, column and index the previous version used is
//!    still there, unchanged, and `schema_version` still names the previous
//!    version.
//! 2. **Idempotent statements.** Every statement is `IF NOT EXISTS`
//!    (`CREATE TABLE`, `ADD COLUMN`, `ADD INDEX`; TiDB extensions that MySQL
//!    8.0 does not have). A re-run after a failure skips what exists and
//!    finishes the rest. One `ALTER TABLE` with several clauses is one atomic
//!    multi-schema change in TiDB; an index on a column added by the same
//!    migration goes in a later `ALTER TABLE` (TiDB v8.5 answers ERROR 1072
//!    when both are in one).
//! 3. **Version last.** The `schema_version` row of a migration is inserted
//!    only after all of its statements succeeded.
//! 4. **Contract later.** Backfill (in code, like `backfill_output_expiry`)
//!    and switching readers / writers ship in the release that expands.
//!    Dropping or retyping a column is a separate migration in a later
//!    release, once no supported binary reads it. Never destructive DDL in
//!    the release that stops using something.
//! 5. **One migrator at a time.** `GET_LOCK('tsls_schema_migration')`
//!    serializes concurrent openers; a waiter re-reads the version after it
//!    gets the lock.

use mysql::prelude::Queryable;

use tachyon_serverless_domain::Timestamp;

use super::{RepoError, p, ts};

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial",
        sql: include_str!("migrations/001_initial.sql"),
    },
    Migration {
        version: 2,
        name: "output_retention",
        sql: include_str!("migrations/002_output_retention.sql"),
    },
    Migration {
        version: 3,
        name: "slot_leases",
        sql: include_str!("migrations/003_slot_leases.sql"),
    },
    Migration {
        version: 4,
        name: "config_publication",
        sql: include_str!("migrations/004_config_publication.sql"),
    },
    Migration {
        version: 5,
        name: "object_refs",
        sql: include_str!("migrations/005_object_refs.sql"),
    },
    Migration {
        version: 6,
        name: "invoke_async_outbox",
        sql: include_str!("migrations/006_invoke_async_outbox.sql"),
    },
    Migration {
        version: 7,
        name: "triggers",
        sql: include_str!("migrations/007_triggers.sql"),
    },
    Migration {
        version: 8,
        name: "async_dispatch",
        sql: include_str!("migrations/008_async_dispatch.sql"),
    },
];

pub const LATEST: i64 = MIGRATIONS[MIGRATIONS.len() - 1].version;

const CREATE_SCHEMA_VERSION: &str = "CREATE TABLE IF NOT EXISTS schema_version (
    version    BIGINT       NOT NULL,
    name       VARCHAR(128) NOT NULL,
    applied_at VARCHAR(40)  NOT NULL,
    PRIMARY KEY (version) CLUSTERED
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin";

const LOCK_NAME: &str = "tsls_schema_migration";
const LOCK_WAIT_SECONDS: i64 = 120;

/// The statements of one migration file: comment lines dropped, split at a
/// semicolon that ends a line.
pub fn statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("--") || trimmed.is_empty() {
            continue;
        }
        current.push_str(line);
        current.push('\n');
        if trimmed.ends_with(';') {
            let stmt = current.trim().trim_end_matches(';').trim().to_string();
            if !stmt.is_empty() {
                out.push(stmt);
            }
            current.clear();
        }
    }
    let rest = current.trim();
    if !rest.is_empty() {
        out.push(rest.to_string());
    }
    out
}

/// The schema version recorded in the current database (0 when empty).
pub fn current_version<Q: Queryable>(conn: &mut Q) -> Result<i64, RepoError> {
    let exists: Option<String> = conn.exec_first(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name = 'schema_version'",
        (),
    )?;
    if exists.is_none() {
        return Ok(0);
    }
    let v: Option<Option<i64>> = conn.exec_first("SELECT MAX(version) FROM schema_version", ())?;
    Ok(v.flatten().unwrap_or(0))
}

/// Apply every migration of `set` up to and including `target`. Returns the
/// versions applied. `set` is [`MIGRATIONS`] outside of tests.
pub fn migrate_to<Q: Queryable>(
    conn: &mut Q,
    set: &[Migration],
    target: i64,
    now: Timestamp,
) -> Result<Vec<i64>, RepoError> {
    let got: Option<Option<i64>> =
        conn.exec_first("SELECT GET_LOCK(?, ?)", p![LOCK_NAME, LOCK_WAIT_SECONDS])?;
    if got.flatten() != Some(1) {
        return Err(RepoError::Store(format!(
            "could not take the schema migration lock `{LOCK_NAME}` within {LOCK_WAIT_SECONDS}s"
        )));
    }
    let result = migrate_locked(conn, set, target, now);
    let _: Option<Option<i64>> = conn.exec_first("SELECT RELEASE_LOCK(?)", p![LOCK_NAME])?;
    result
}

fn migrate_locked<Q: Queryable>(
    conn: &mut Q,
    set: &[Migration],
    target: i64,
    now: Timestamp,
) -> Result<Vec<i64>, RepoError> {
    conn.query_drop(CREATE_SCHEMA_VERSION)?;
    let current = current_version(conn)?;
    let latest = set.last().map(|m| m.version).unwrap_or(0);
    if current > latest {
        return Err(RepoError::Refused(format!(
            "the TiDB schema is at version {current}, newer than this binary supports \
             ({latest}). Migrations are forward-only: run a newer gateway"
        )));
    }
    let mut applied = Vec::new();
    for m in set
        .iter()
        .filter(|m| m.version > current && m.version <= target)
    {
        for (step, stmt) in statements(m.sql).iter().enumerate() {
            conn.query_drop(stmt).map_err(|e| {
                RepoError::Store(format!(
                    "migration {:03}_{} failed at statement {}: {e}. Nothing is rolled back \
                     (TiDB DDL is not transactional); every statement is additive and \
                     idempotent, so the previous binary still runs and a re-run finishes it",
                    m.version,
                    m.name,
                    step + 1
                ))
            })?;
        }
        conn.exec_drop(
            "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
            p![m.version, m.name, ts(&now)],
        )?;
        applied.push(m.version);
    }
    Ok(applied)
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn versions_mirror_the_sqlite_migrations() {
        let sqlite = crate::repository::sqlite::migrations::MIGRATIONS;
        assert_eq!(
            MIGRATIONS.len(),
            sqlite.len(),
            "every SQLite migration needs its TiDB mirror (or a documented exception)"
        );
        for (t, s) in MIGRATIONS.iter().zip(sqlite) {
            assert_eq!((t.version, t.name), (s.version, s.name));
        }
    }

    #[test]
    fn every_statement_is_idempotent_and_additive() {
        for m in MIGRATIONS {
            let stmts = statements(m.sql);
            assert!(!stmts.is_empty(), "{}", m.name);
            for s in stmts {
                let upper = s.to_uppercase();
                assert!(
                    upper.starts_with("CREATE TABLE IF NOT EXISTS")
                        || upper.starts_with("CREATE INDEX IF NOT EXISTS")
                        || upper.starts_with("CREATE UNIQUE INDEX IF NOT EXISTS")
                        || upper.starts_with("ALTER TABLE"),
                    "{}: {s}",
                    m.name
                );
                for forbidden in ["DROP ", "MODIFY ", "CHANGE ", "RENAME ", "TRUNCATE "] {
                    assert!(!upper.contains(forbidden), "{}: {forbidden} in {s}", m.name);
                }
                if upper.starts_with("ALTER TABLE") {
                    for clause in upper.split(',') {
                        if clause.contains("ADD COLUMN") {
                            assert!(clause.contains("ADD COLUMN IF NOT EXISTS"), "{s}");
                        }
                        if clause.contains("ADD INDEX") {
                            assert!(clause.contains("ADD INDEX IF NOT EXISTS"), "{s}");
                        }
                    }
                }
            }
        }
    }
}

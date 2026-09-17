//! Forward-only schema migrations (docs/adr/0003 「`state.json` からの移行」 5).
//!
//! `schema_version` holds one row per applied migration. Opening a store
//! applies every pending migration in one `BEGIN IMMEDIATE` transaction
//! (SQLite DDL is transactional, so a failed migration leaves the previous
//! schema intact) and refuses a database whose version is newer than this
//! binary: there is no down migration.
//!
//! Rules for a new migration: append, never edit an applied one; expand
//! first (add a nullable column / table / index), switch readers and writers
//! in code, and contract (drop the old column) only in a later migration once
//! no supported binary reads it.

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use tachyon_serverless_domain::Timestamp;

use super::{RepoError, ts};

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
];

pub const LATEST: i64 = MIGRATIONS[MIGRATIONS.len() - 1].version;

const CREATE_SCHEMA_VERSION: &str = "CREATE TABLE IF NOT EXISTS schema_version (
    version    BIGINT       NOT NULL PRIMARY KEY,
    name       VARCHAR(128) NOT NULL,
    applied_at VARCHAR(40)  NOT NULL
)";

/// The schema version recorded in `conn` (0 for an empty database).
pub fn current_version(conn: &Connection) -> Result<i64, RepoError> {
    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if exists.is_none() {
        return Ok(0);
    }
    let v: Option<i64> =
        conn.query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))?;
    Ok(v.unwrap_or(0))
}

/// Apply every migration up to and including `target`. Returns the
/// migrations that were applied.
pub fn migrate_to(
    conn: &mut Connection,
    target: i64,
    now: Timestamp,
) -> Result<Vec<Migration>, RepoError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(CREATE_SCHEMA_VERSION, [])?;
    let current: i64 = tx
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| {
            r.get::<_, Option<i64>>(0)
        })?
        .unwrap_or(0);
    if current > LATEST {
        return Err(RepoError::Refused(format!(
            "state.db is at schema version {current}, newer than this binary supports \
             ({LATEST}). Migrations are forward-only: run a newer gateway, or restore the \
             database from before the upgrade"
        )));
    }
    let mut applied = Vec::new();
    for m in MIGRATIONS
        .iter()
        .filter(|m| m.version > current && m.version <= target)
    {
        tx.execute_batch(m.sql).map_err(|e| {
            RepoError::Store(format!("migration {:03}_{} failed: {e}", m.version, m.name))
        })?;
        tx.execute(
            "INSERT INTO schema_version (version, name, applied_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![m.version, m.name, ts(&now)],
        )?;
        applied.push(*m);
    }
    tx.commit()?;
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_contiguous_and_increasing() {
        for (i, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(m.version, i as i64 + 1, "{}", m.name);
        }
    }
}

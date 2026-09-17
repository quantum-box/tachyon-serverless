//! [`ConfigPublicationRepository`] on SQLite (migration 004).

use std::collections::BTreeMap;

use rusqlite::params;

use super::super::config::{
    ConfigPublicationRepository, ConfigRows, Observe, PublishedRow, StampedConfig, above, stamp,
};
use super::{RepoError, SqliteStore, big, bodies, meta};

const META_CONFIG_GENERATION: &str = "config_generation";

/// How many times [`SqliteStore::stamp_config`] retries its optimistic write
/// before it stamps under the write lock from start to end.
const STAMP_ATTEMPTS: usize = 3;

type Published = BTreeMap<String, PublishedRow>;

/// The publication as one read transaction saw it.
struct Snapshot {
    counter: u64,
    rows: Published,
    source: ConfigRows,
}

fn read_counter(tx: &rusqlite::Connection) -> Result<u64, RepoError> {
    match meta(tx, META_CONFIG_GENERATION)? {
        Some(v) => v.parse::<u64>().map_err(|_| {
            RepoError::Serialization(format!("store_meta {META_CONFIG_GENERATION} = {v}"))
        }),
        None => Ok(0),
    }
}

fn read_source(tx: &rusqlite::Connection) -> Result<ConfigRows, RepoError> {
    Ok(ConfigRows {
        functions: bodies(tx, "SELECT body FROM functions ORDER BY created_at, id", [])?,
        aliases: bodies(
            tx,
            "SELECT body FROM aliases ORDER BY function_id, name",
            [],
        )?,
        revisions: bodies(
            tx,
            "SELECT body FROM revisions ORDER BY function_id, number",
            [],
        )?,
    })
}

fn read_published(tx: &rusqlite::Connection) -> Result<Published, RepoError> {
    let mut rows = BTreeMap::new();
    let mut stmt = tx.prepare_cached(
        "SELECT entry_key, generation, natural_version, digest, body \
         FROM config_publication",
    )?;
    let mapped = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<String>>(4)?,
        ))
    })?;
    for row in mapped {
        let (key, generation, natural, digest, body) = row?;
        rows.insert(
            key,
            PublishedRow {
                generation: generation as u64,
                natural_version: natural as u64,
                digest,
                body,
            },
        );
    }
    Ok(rows)
}

fn write_changes(
    tx: &rusqlite::Connection,
    rows: &Published,
    changed: &[String],
    before: u64,
    counter: u64,
) -> Result<(), RepoError> {
    for key in changed {
        let row = &rows[key];
        tx.execute(
            "INSERT INTO config_publication \
             (entry_key, generation, natural_version, digest, body) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (entry_key) DO UPDATE SET generation = excluded.generation, \
             natural_version = excluded.natural_version, digest = excluded.digest, \
             body = excluded.body",
            params![
                key,
                big(row.generation, "config generation")?,
                big(row.natural_version, "natural version")?,
                row.digest,
                row.body
            ],
        )?;
    }
    if counter != before {
        tx.execute(
            "INSERT INTO store_meta (meta_key, meta_value) VALUES (?1, ?2) \
             ON CONFLICT (meta_key) DO UPDATE SET meta_value = excluded.meta_value",
            params![META_CONFIG_GENERATION, counter.to_string()],
        )?;
    }
    Ok(())
}

impl SqliteStore {
    /// The publication and its source rows, from one read transaction. In WAL
    /// mode a reader takes no write lock: other processes keep writing.
    fn config_snapshot(&self) -> Result<Snapshot, RepoError> {
        self.read(|c| {
            let tx = c.unchecked_transaction()?;
            let snapshot = Snapshot {
                counter: read_counter(&tx)?,
                rows: read_published(&tx)?,
                source: read_source(&tx)?,
            };
            tx.commit()?;
            Ok(snapshot)
        })
    }

    /// The pre-PLT-4646 stamp: everything — the source rows, `observe`, the
    /// publication rows and the writes — inside one write transaction. Only
    /// the fallback after [`STAMP_ATTEMPTS`] lost optimistic rounds.
    fn stamp_config_locked(
        &self,
        observe: Observe<'_>,
        since: u64,
    ) -> Result<StampedConfig, RepoError> {
        self.write(|tx| {
            let observed = observe(read_source(tx)?)?;
            let mut counter = read_counter(tx)?;
            let mut rows = read_published(tx)?;
            let before = counter;
            let changed = stamp(&mut rows, &mut counter, observed);
            write_changes(tx, &rows, &changed, before, counter)?;
            Ok(StampedConfig {
                generation: counter,
                entries: above(&rows, since),
            })
        })
    }
}

impl ConfigPublicationRepository for SqliteStore {
    /// Stamp without holding the database write lock for the expensive part
    /// (PLT-4646, docs/adr/0003 「書込み transaction の規律」).
    ///
    /// This runs on every configuration sync, and a sync follows every commit
    /// another gateway makes on a shared `state.db`. It used to read and
    /// parse every function, alias and revision, serialize and hash each one
    /// and read the budget file inside `BEGIN IMMEDIATE`: the longest write
    /// transaction a gateway held on a timer, so the likeliest place for a
    /// frozen process to be holding every other writer out. Now:
    ///
    /// 1. one **read** transaction takes the source rows, the publication and
    ///    its generation counter;
    /// 2. `observe` and the stamping run with no transaction and no lock;
    /// 3. nothing changed (the steady state): done, no write transaction at
    ///    all;
    /// 4. something changed: a short write transaction re-reads the counter
    ///    and writes the changed rows only if nobody stamped in between (the
    ///    publication rows only ever change together with the counter). If
    ///    someone did, start again; after [`STAMP_ATTEMPTS`] rounds, stamp
    ///    under the lock as before.
    ///
    /// A source row changed by another writer after step 1 but not yet
    /// stamped is picked up by the next sync (that commit moves this
    /// connection's `data_version`); what step 4 writes is what was true at
    /// step 1, never older than what is already published.
    fn stamp_config(&self, observe: Observe<'_>, since: u64) -> Result<StampedConfig, RepoError> {
        for _ in 0..STAMP_ATTEMPTS {
            let Snapshot {
                counter: before,
                mut rows,
                source,
            } = self.config_snapshot()?;
            let observed = observe(source)?;
            let mut counter = before;
            let changed = stamp(&mut rows, &mut counter, observed);
            if changed.is_empty() {
                return Ok(StampedConfig {
                    generation: counter,
                    entries: above(&rows, since),
                });
            }
            let written = self.write(|tx| {
                if read_counter(tx)? != before {
                    return Ok(false);
                }
                write_changes(tx, &rows, &changed, before, counter)?;
                Ok(true)
            })?;
            if written {
                return Ok(StampedConfig {
                    generation: counter,
                    entries: above(&rows, since),
                });
            }
        }
        self.stamp_config_locked(observe, since)
    }

    fn external_change_marker(&self) -> Option<u64> {
        self.read(|c| Ok(c.query_row("PRAGMA data_version", [], |r| r.get::<_, i64>(0))?))
            .ok()
            .map(|v| v as u64)
    }
}

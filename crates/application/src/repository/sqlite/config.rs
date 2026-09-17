//! [`ConfigPublicationRepository`] on SQLite (migration 004).

use std::collections::BTreeMap;

use rusqlite::params;

use tachyon_serverless_domain::{Function, FunctionAlias, FunctionRevision};

use super::super::config::{
    ConfigPublicationRepository, ConfigRows, Observe, PublishedRow, StampedConfig, above, stamp,
};
use super::{RepoError, SqliteStore, big, bodies, meta};

const META_CONFIG_GENERATION: &str = "config_generation";

impl ConfigPublicationRepository for SqliteStore {
    fn stamp_config(&self, observe: Observe<'_>, since: u64) -> Result<StampedConfig, RepoError> {
        self.write(|tx| {
            let functions: Vec<Function> =
                bodies(tx, "SELECT body FROM functions ORDER BY created_at, id", [])?;
            let aliases: Vec<FunctionAlias> = bodies(
                tx,
                "SELECT body FROM aliases ORDER BY function_id, name",
                [],
            )?;
            let revisions: Vec<FunctionRevision> = bodies(
                tx,
                "SELECT body FROM revisions ORDER BY function_id, number",
                [],
            )?;
            let observed = observe(ConfigRows {
                functions,
                aliases,
                revisions,
            })?;

            let mut counter: u64 = match meta(tx, META_CONFIG_GENERATION)? {
                Some(v) => v.parse::<u64>().map_err(|_| {
                    RepoError::Serialization(format!("store_meta {META_CONFIG_GENERATION} = {v}"))
                })?,
                None => 0,
            };
            let mut rows = BTreeMap::new();
            {
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
            }
            let before = counter;
            let changed = stamp(&mut rows, &mut counter, observed);
            for key in &changed {
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
            Ok(StampedConfig {
                generation: counter,
                entries: above(&rows, since),
            })
        })
    }

    fn external_change_marker(&self) -> Option<u64> {
        self.read(|c| Ok(c.query_row("PRAGMA data_version", [], |r| r.get::<_, i64>(0))?))
            .ok()
            .map(|v| v as u64)
    }
}

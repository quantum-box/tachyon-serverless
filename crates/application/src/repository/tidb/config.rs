//! [`ConfigPublicationRepository`] on TiDB (migration 004).
//!
//! The stamping transaction first locks the config lock row in `store_meta`
//! (created on open), so two publishers stamp one after the other, as
//! `BEGIN IMMEDIATE` does on SQLite.

use std::collections::BTreeMap;

use mysql::prelude::Queryable;

use tachyon_serverless_domain::{Function, FunctionAlias, FunctionRevision};

use super::super::config::{
    ConfigPublicationRepository, ConfigRows, Observe, PublishedRow, StampedConfig, above, stamp,
};
use super::{CONFIG_LOCK, RepoError, TidbStore, bodies, p};

const META_CONFIG_GENERATION: &str = "config_generation";

impl ConfigPublicationRepository for TidbStore {
    fn stamp_config(&self, observe: Observe<'_>, since: u64) -> Result<StampedConfig, RepoError> {
        self.write(|tx| {
            let _: Option<String> = tx.exec_first(
                "SELECT meta_value FROM store_meta WHERE meta_key = ? FOR UPDATE",
                p![CONFIG_LOCK],
            )?;
            let stored: Option<String> = tx.exec_first(
                "SELECT meta_value FROM store_meta WHERE meta_key = ?",
                p![META_CONFIG_GENERATION],
            )?;
            let functions: Vec<Function> = bodies(
                tx,
                "SELECT body FROM functions ORDER BY created_at, id",
                p![],
            )?;
            let aliases: Vec<FunctionAlias> = bodies(
                tx,
                "SELECT body FROM aliases ORDER BY function_id, name",
                p![],
            )?;
            let revisions: Vec<FunctionRevision> = bodies(
                tx,
                "SELECT body FROM revisions ORDER BY function_id, number",
                p![],
            )?;
            let observed = observe(ConfigRows {
                functions,
                aliases,
                revisions,
            })?;

            let mut counter: u64 = match stored {
                Some(v) => v.parse::<u64>().map_err(|_| {
                    RepoError::Serialization(format!("store_meta {META_CONFIG_GENERATION} = {v}"))
                })?,
                None => 0,
            };
            let mut rows = BTreeMap::new();
            let raw: Vec<(String, u64, u64, String, Option<String>)> = tx.exec(
                "SELECT entry_key, generation, natural_version, digest, body \
                 FROM config_publication",
                p![],
            )?;
            for (key, generation, natural, digest, body) in raw {
                rows.insert(
                    key,
                    PublishedRow {
                        generation,
                        natural_version: natural,
                        digest,
                        body,
                    },
                );
            }
            let before = counter;
            let changed = stamp(&mut rows, &mut counter, observed);
            for key in &changed {
                let row = &rows[key];
                tx.exec_drop(
                    "INSERT INTO config_publication \
                     (entry_key, generation, natural_version, digest, body) \
                     VALUES (?, ?, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE generation = VALUES(generation), \
                     natural_version = VALUES(natural_version), digest = VALUES(digest), \
                     body = VALUES(body)",
                    p![
                        key.as_str(),
                        row.generation,
                        row.natural_version,
                        row.digest.as_str(),
                        row.body.clone()
                    ],
                )?;
            }
            if counter != before {
                tx.exec_drop(
                    "INSERT INTO store_meta (meta_key, meta_value) VALUES (?, ?) \
                     ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)",
                    p![META_CONFIG_GENERATION, counter.to_string()],
                )?;
            }
            Ok(StampedConfig {
                generation: counter,
                entries: above(&rows, since),
            })
        })
    }
}

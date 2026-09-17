//! Generation stamps of the configuration a control plane publishes to data
//! planes (PLT-4636, docs/adr/0007-config-distribution-and-auth-leases.md).
//!
//! The publication is a table next to the ledger: one row per published key
//! with the generation at which its content last changed, the content digest,
//! a natural version and the body (NULL for a tombstone), plus one counter.
//! [`ConfigPublicationRepository::stamp_config`] reads the source rows and
//! stamps them **in one transaction**, so two concurrent publications cannot
//! interleave a stale read with a newer stamp. The stamping rules
//! ([`stamp`]) are shared by both stores.

use std::collections::BTreeMap;

use tachyon_serverless_domain::{Function, FunctionAlias, FunctionRevision};

use super::RepoError;

/// The ledger rows a publication is built from, read in the stamping
/// transaction.
#[derive(Debug, Clone, Default)]
pub struct ConfigRows {
    pub functions: Vec<Function>,
    pub aliases: Vec<FunctionAlias>,
    pub revisions: Vec<FunctionRevision>,
}

/// One entry as the publisher currently sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigObservation {
    pub key: String,
    pub natural_version: u64,
    pub digest: String,
    pub body: String,
}

/// One row of the publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedRow {
    pub generation: u64,
    pub natural_version: u64,
    pub digest: String,
    /// `None`: tombstone.
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StampedEntry {
    pub key: String,
    pub generation: u64,
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StampedConfig {
    /// The publication counter after stamping.
    pub generation: u64,
    /// Rows with `generation > since`, tombstones included, by generation.
    pub entries: Vec<StampedEntry>,
}

pub type Observe<'a> = &'a mut dyn FnMut(ConfigRows) -> Result<Vec<ConfigObservation>, RepoError>;

pub trait ConfigPublicationRepository: Send + Sync {
    /// In one transaction: read the config rows, let `observe` turn them
    /// (plus whatever it closes over) into keyed observations, stamp every
    /// changed key with a new generation, tombstone every previously
    /// published key that is no longer observed, and return the rows above
    /// `since`.
    fn stamp_config(&self, observe: Observe<'_>, since: u64) -> Result<StampedConfig, RepoError>;

    /// A value that changes when **another connection or process** committed
    /// to the store (SQLite `PRAGMA data_version`). Writes through this store
    /// handle do not move it. `None` when the store cannot tell.
    fn external_change_marker(&self) -> Option<u64> {
        None
    }
}

/// Apply `observed` to `rows`, advancing `counter`. Returns the keys whose
/// row changed.
///
/// - a new key, or a key whose digest changed with a natural version at
///   least the stored one, gets `++counter`;
/// - a value whose natural version is *below* the stored one is ignored (a
///   stale read never rolls a route back);
/// - a previously published key that is not observed any more becomes a
///   tombstone with `++counter` (once);
/// - a tombstoned key that is observed again is republished with `++counter`.
pub fn stamp(
    rows: &mut BTreeMap<String, PublishedRow>,
    counter: &mut u64,
    observed: Vec<ConfigObservation>,
) -> Vec<String> {
    let mut changed = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for o in observed {
        seen.insert(o.key.clone());
        let replace = match rows.get(&o.key) {
            None => true,
            Some(row) if row.body.is_none() => true,
            Some(row) => row.digest != o.digest && o.natural_version >= row.natural_version,
        };
        if replace {
            *counter += 1;
            rows.insert(
                o.key.clone(),
                PublishedRow {
                    generation: *counter,
                    natural_version: o.natural_version,
                    digest: o.digest,
                    body: Some(o.body),
                },
            );
            changed.push(o.key);
        }
    }
    for (key, row) in rows.iter_mut() {
        if row.body.is_some() && !seen.contains(key) {
            *counter += 1;
            row.generation = *counter;
            row.body = None;
            changed.push(key.clone());
        }
    }
    changed
}

/// The rows above `since`, ordered by generation.
pub fn above(rows: &BTreeMap<String, PublishedRow>, since: u64) -> Vec<StampedEntry> {
    let mut out: Vec<StampedEntry> = rows
        .iter()
        .filter(|(_, r)| r.generation > since)
        .map(|(k, r)| StampedEntry {
            key: k.clone(),
            generation: r.generation,
            body: r.body.clone(),
        })
        .collect();
    out.sort_by_key(|e| e.generation);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(key: &str, natural: u64, digest: &str) -> ConfigObservation {
        ConfigObservation {
            key: key.into(),
            natural_version: natural,
            digest: digest.into(),
            body: format!("{key}:{digest}"),
        }
    }

    #[test]
    fn stamps_changes_ignores_stale_values_and_tombstones_removals() {
        let mut rows = BTreeMap::new();
        let mut counter = 0;
        stamp(
            &mut rows,
            &mut counter,
            vec![obs("a", 1, "x"), obs("b", 0, "y")],
        );
        assert_eq!(counter, 2);
        // unchanged: no new generation
        assert!(
            stamp(
                &mut rows,
                &mut counter,
                vec![obs("a", 1, "x"), obs("b", 0, "y")]
            )
            .is_empty()
        );
        // a changed forward, b removed
        let changed = stamp(&mut rows, &mut counter, vec![obs("a", 2, "z")]);
        assert_eq!(changed, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(rows["a"].generation, 3);
        assert_eq!(rows["b"].generation, 4);
        assert!(rows["b"].body.is_none());
        // a stale read of a (natural 1) never replaces natural 2
        assert!(stamp(&mut rows, &mut counter, vec![obs("a", 1, "x")]).is_empty());
        assert_eq!(rows["a"].digest, "z");
        // the tombstone is not re-stamped, a resurrection is
        assert!(stamp(&mut rows, &mut counter, vec![obs("a", 2, "z")]).is_empty());
        stamp(
            &mut rows,
            &mut counter,
            vec![obs("a", 2, "z"), obs("b", 0, "y")],
        );
        assert_eq!(rows["b"].generation, 5);
        assert_eq!(above(&rows, 3).len(), 1);
        assert_eq!(above(&rows, 0).len(), 2);
    }
}

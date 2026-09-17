//! The P1 ledger file, `<data_dir>/state.json`, read once for import into
//! `state.db` (docs/adr/0003 「`state.json` からの移行」). Nothing writes it
//! any more.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Deserialize;

use tachyon_serverless_domain::{
    AttemptId, EnvironmentId, ExecutionEnvironment, Function, FunctionAlias, FunctionId,
    FunctionRevision, Invocation, InvocationAttempt, InvocationId, RevisionId, Sha256Digest,
    TenantId,
};

use super::RepoError;

pub(crate) const FILE_NAME: &str = "state.json";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LegacyIdempotencyKey {
    pub tenant_id: TenantId,
    pub function_id: FunctionId,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LegacyIdempotencyEntry {
    pub invocation_id: InvocationId,
    pub input_digest: Sha256Digest,
}

/// The durable part of the P1 `InMemoryStore`, field for field. Every field
/// defaults, so a file written by any P1 version loads.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct PersistedState {
    #[serde(default)]
    pub functions: BTreeMap<FunctionId, Function>,
    #[serde(default)]
    pub revisions: BTreeMap<RevisionId, FunctionRevision>,
    #[serde(default)]
    pub revision_counters: BTreeMap<FunctionId, u64>,
    /// Keyed by `"<function_id>/<alias>"`.
    #[serde(default)]
    pub aliases: BTreeMap<String, FunctionAlias>,
    #[serde(default)]
    pub invocations: BTreeMap<InvocationId, Invocation>,
    #[serde(default)]
    pub attempts: BTreeMap<AttemptId, InvocationAttempt>,
    #[serde(default)]
    pub environments: BTreeMap<EnvironmentId, ExecutionEnvironment>,
    #[serde(default)]
    pub idempotency: Vec<(LegacyIdempotencyKey, LegacyIdempotencyEntry)>,
    #[serde(default)]
    pub artifact_owners: BTreeMap<Sha256Digest, BTreeSet<TenantId>>,
}

/// Parse a `state.json`. A file that does not parse is refused with the same
/// hint P1 gave (docs/kvm.md §7.1: a zero-filled file after the disk filled
/// up); it is never silently discarded.
pub(crate) fn parse(bytes: &[u8], path: &Path) -> Result<PersistedState, RepoError> {
    let text = String::from_utf8_lossy(bytes);
    if text.trim().is_empty() {
        return Ok(PersistedState::default());
    }
    serde_json::from_str(&text).map_err(|e| {
        RepoError::Serialization(format!(
            "{} is not a valid state file ({e}); it may have been truncated by an \
             unclean shutdown. Move it aside to start with an empty ledger.",
            path.display()
        ))
    })
}

//! Repositories: traits plus the in-memory implementation.
//!
//! [`InMemoryStore`] keeps everything behind a single `parking_lot::RwLock`
//! and (optionally) writes the durable part through to
//! `<data_dir>/state.json` after every mutation, best effort. Logs are kept
//! in memory only and bounded per invocation by [`Limits`]. Leases are
//! transient and never persisted. No secret ever enters the store.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    AliasName, AttemptId, EnvironmentId, ErrorClass, ExecutionEnvironment, ExecutionLease,
    Function, FunctionAlias, FunctionId, FunctionName, FunctionRevision, Invocation,
    InvocationAttempt, InvocationError, InvocationId, LeaseId, Limits, LogRecord, RevisionId,
    Sha256Digest, TenantId, Timestamp,
};

#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("{0} not found")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization: {0}")]
    Serialization(String),
}

// ---------------------------------------------------------------------------
// traits
// ---------------------------------------------------------------------------

pub trait FunctionRepository: Send + Sync {
    fn insert(&self, function: Function) -> Result<(), RepoError>;
    fn get(&self, id: &FunctionId) -> Result<Option<Function>, RepoError>;
    fn find_by_name(
        &self,
        tenant: &TenantId,
        name: &FunctionName,
    ) -> Result<Option<Function>, RepoError>;
    fn list(&self, tenant: &TenantId) -> Result<Vec<Function>, RepoError>;
    fn update(&self, function: Function) -> Result<(), RepoError>;
}

pub trait RevisionRepository: Send + Sync {
    /// Allocate the next per-function revision number (1, 2, 3, ...).
    fn allocate_number(&self, function: &FunctionId) -> Result<u64, RepoError>;
    fn insert(&self, revision: FunctionRevision) -> Result<(), RepoError>;
    fn get(&self, id: &RevisionId) -> Result<Option<FunctionRevision>, RepoError>;
    fn list_by_function(&self, function: &FunctionId) -> Result<Vec<FunctionRevision>, RepoError>;
    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError>;
}

pub trait AliasRepository: Send + Sync {
    fn get(
        &self,
        function: &FunctionId,
        name: &AliasName,
    ) -> Result<Option<FunctionAlias>, RepoError>;
    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError>;
    /// Atomically read-modify-write one alias slot. The closure sees the
    /// current alias (or `None`) and may replace it; the store is only
    /// mutated when the closure returns `Ok`.
    fn modify(
        &self,
        function: &FunctionId,
        name: &AliasName,
        f: &mut dyn FnMut(&mut Option<FunctionAlias>) -> Result<(), crate::error::AppError>,
    ) -> Result<Option<FunctionAlias>, crate::error::AppError>;
}

pub trait InvocationRepository: Send + Sync {
    fn insert(&self, invocation: Invocation) -> Result<(), RepoError>;
    fn get(&self, id: &InvocationId) -> Result<Option<Invocation>, RepoError>;
    fn update(&self, invocation: Invocation) -> Result<(), RepoError>;
    /// Newest first.
    fn list_by_function(
        &self,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<Invocation>, RepoError>;
    fn insert_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError>;
    fn get_attempt(&self, id: &AttemptId) -> Result<Option<InvocationAttempt>, RepoError>;
    fn update_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError>;
    fn attempts_of(&self, invocation: &InvocationId) -> Result<Vec<InvocationAttempt>, RepoError>;
}

pub trait EnvironmentRepository: Send + Sync {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError>;
    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError>;
    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError>;
    fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError>;
    fn insert_lease(&self, lease: ExecutionLease) -> Result<(), RepoError>;
    fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError>;
    fn update_lease(&self, lease: ExecutionLease) -> Result<(), RepoError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Stored,
    /// Dropped because the per-invocation line or byte limit was reached.
    Dropped,
}

#[derive(Debug, Clone, Default)]
pub struct LogQuery {
    pub records: Vec<LogRecord>,
    /// True when at least one line was dropped by retention limits.
    pub dropped: bool,
}

pub trait LogRepository: Send + Sync {
    fn append(&self, record: LogRecord) -> AppendOutcome;
    fn query(&self, invocation: &InvocationId) -> LogQuery;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    /// The key was free and is now bound to the given invocation.
    Reserved,
    /// The key is already bound.
    Existing {
        invocation_id: InvocationId,
        input_digest: Sha256Digest,
    },
}

pub trait IdempotencyRepository: Send + Sync {
    fn reserve(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
        input_digest: &Sha256Digest,
        invocation_id: &InvocationId,
    ) -> Result<IdempotencyOutcome, RepoError>;
}

/// All repositories bundled; services take what they need.
#[derive(Clone)]
pub struct Repositories {
    pub functions: Arc<dyn FunctionRepository>,
    pub revisions: Arc<dyn RevisionRepository>,
    pub aliases: Arc<dyn AliasRepository>,
    pub invocations: Arc<dyn InvocationRepository>,
    pub environments: Arc<dyn EnvironmentRepository>,
    pub logs: Arc<dyn LogRepository>,
    pub idempotency: Arc<dyn IdempotencyRepository>,
}

impl Repositories {
    pub fn in_memory(store: Arc<InMemoryStore>) -> Self {
        Self {
            functions: store.clone(),
            revisions: store.clone(),
            aliases: store.clone(),
            invocations: store.clone(),
            environments: store.clone(),
            logs: store.clone(),
            idempotency: store,
        }
    }
}

// ---------------------------------------------------------------------------
// in-memory store
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct IdempotencyKey {
    tenant_id: TenantId,
    function_id: FunctionId,
    key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdempotencyEntry {
    invocation_id: InvocationId,
    input_digest: Sha256Digest,
}

/// Durable part of the state (JSON on disk).
#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    #[serde(default)]
    functions: BTreeMap<FunctionId, Function>,
    #[serde(default)]
    revisions: BTreeMap<RevisionId, FunctionRevision>,
    #[serde(default)]
    revision_counters: BTreeMap<FunctionId, u64>,
    /// Keyed by `"<function_id>/<alias>"`.
    #[serde(default)]
    aliases: BTreeMap<String, FunctionAlias>,
    #[serde(default)]
    invocations: BTreeMap<InvocationId, Invocation>,
    #[serde(default)]
    attempts: BTreeMap<AttemptId, InvocationAttempt>,
    #[serde(default)]
    environments: BTreeMap<EnvironmentId, ExecutionEnvironment>,
    #[serde(default)]
    idempotency: Vec<(IdempotencyKey, IdempotencyEntry)>,
}

#[derive(Debug, Default)]
struct LogBucket {
    records: Vec<LogRecord>,
    bytes: u64,
    dropped: bool,
}

#[derive(Default)]
struct State {
    durable: PersistedState,
    idempotency: HashMap<IdempotencyKey, IdempotencyEntry>,
    leases: HashMap<LeaseId, ExecutionLease>,
    logs: HashMap<String, LogBucket>,
}

/// Single-process store. Cheap to clone via `Arc`.
pub struct InMemoryStore {
    state: RwLock<State>,
    limits: Limits,
    persist_path: Option<PathBuf>,
}

impl std::fmt::Debug for InMemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStore")
            .field("persist_path", &self.persist_path)
            .finish_non_exhaustive()
    }
}

fn alias_key(function: &FunctionId, name: &AliasName) -> String {
    format!("{function}/{name}")
}

impl InMemoryStore {
    /// Volatile store (tests).
    pub fn new(limits: Limits) -> Self {
        Self {
            state: RwLock::new(State::default()),
            limits,
            persist_path: None,
        }
    }

    /// Store that loads `<data_dir>/state.json` if present and writes it
    /// through after every mutation. Non-terminal invocations, attempts and
    /// environments found on disk are reconciled to a failed/lost state
    /// because their driver tasks no longer exist.
    pub fn with_persistence(
        data_dir: &Path,
        limits: Limits,
        now: Timestamp,
    ) -> Result<Self, RepoError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("state.json");
        let mut durable = if path.exists() {
            let text = std::fs::read_to_string(&path)?;
            if text.trim().is_empty() {
                PersistedState::default()
            } else {
                serde_json::from_str(&text).map_err(|e| RepoError::Serialization(e.to_string()))?
            }
        } else {
            PersistedState::default()
        };
        reconcile_after_restart(&mut durable, now);
        let idempotency = durable.idempotency.iter().cloned().collect();
        let store = Self {
            state: RwLock::new(State {
                durable,
                idempotency,
                leases: HashMap::new(),
                logs: HashMap::new(),
            }),
            limits,
            persist_path: Some(path),
        };
        store.persist_now()?;
        Ok(store)
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn persist_path(&self) -> Option<&Path> {
        self.persist_path.as_deref()
    }

    /// Run a mutation under the write lock, then write through (best effort).
    fn mutate<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut guard = self.state.write();
        let out = f(&mut guard);
        if let Some(path) = &self.persist_path {
            guard.durable.idempotency = guard
                .idempotency
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if let Err(e) = write_state(path, &guard.durable) {
                tracing::warn!(error = %e, path = %path.display(), "state.json write failed");
            }
        }
        out
    }

    /// Force a synchronous write of the current state.
    pub fn persist_now(&self) -> Result<(), RepoError> {
        if let Some(path) = &self.persist_path {
            let mut guard = self.state.write();
            guard.durable.idempotency = guard
                .idempotency
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            write_state(path, &guard.durable)?;
        }
        Ok(())
    }
}

fn write_state(path: &Path, state: &PersistedState) -> Result<(), RepoError> {
    let json =
        serde_json::to_vec_pretty(state).map_err(|e| RepoError::Serialization(e.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Anything that was in flight when the previous process died cannot be
/// resumed: mark it as a platform failure so the ledger stays consistent.
fn reconcile_after_restart(state: &mut PersistedState, now: Timestamp) {
    for inv in state.invocations.values_mut() {
        if !inv.status.is_terminal() {
            let _ = inv.mark_failed(
                InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.Restarted",
                    "gateway restarted while the invocation was in flight",
                ),
                now,
            );
        }
    }
    for att in state.attempts.values_mut() {
        if !att.status.is_terminal() {
            let _ = att.fail(
                InvocationError::new(
                    ErrorClass::PlatformError,
                    "Host.Restarted",
                    "gateway restarted while the attempt was in flight",
                ),
                now,
            );
        }
    }
    for env in state.environments.values_mut() {
        if !env.is_terminal() {
            let _ = env.mark_lost("gateway restarted", now);
        }
    }
}

impl FunctionRepository for InMemoryStore {
    fn insert(&self, function: Function) -> Result<(), RepoError> {
        self.mutate(|s| {
            if s.durable.functions.contains_key(&function.id) {
                return Err(RepoError::Conflict(format!(
                    "function {} already exists",
                    function.id
                )));
            }
            let dup = s.durable.functions.values().any(|f| {
                f.tenant_id == function.tenant_id && f.name == function.name && !f.is_deleted()
            });
            if dup {
                return Err(RepoError::Conflict(format!(
                    "function name `{}` already exists",
                    function.name
                )));
            }
            s.durable.functions.insert(function.id.clone(), function);
            Ok(())
        })
    }

    fn get(&self, id: &FunctionId) -> Result<Option<Function>, RepoError> {
        Ok(self.state.read().durable.functions.get(id).cloned())
    }

    fn find_by_name(
        &self,
        tenant: &TenantId,
        name: &FunctionName,
    ) -> Result<Option<Function>, RepoError> {
        Ok(self
            .state
            .read()
            .durable
            .functions
            .values()
            .find(|f| &f.tenant_id == tenant && &f.name == name && !f.is_deleted())
            .cloned())
    }

    fn list(&self, tenant: &TenantId) -> Result<Vec<Function>, RepoError> {
        let mut v: Vec<Function> = self
            .state
            .read()
            .durable
            .functions
            .values()
            .filter(|f| &f.tenant_id == tenant)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        Ok(v)
    }

    fn update(&self, function: Function) -> Result<(), RepoError> {
        self.mutate(|s| match s.durable.functions.get_mut(&function.id) {
            Some(slot) => {
                *slot = function;
                Ok(())
            }
            None => Err(RepoError::NotFound(format!("function {}", function.id))),
        })
    }
}

impl RevisionRepository for InMemoryStore {
    fn allocate_number(&self, function: &FunctionId) -> Result<u64, RepoError> {
        Ok(self.mutate(|s| {
            let c = s
                .durable
                .revision_counters
                .entry(function.clone())
                .or_insert(0);
            *c += 1;
            *c
        }))
    }

    fn insert(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        self.mutate(|s| {
            if s.durable.revisions.contains_key(&revision.id) {
                return Err(RepoError::Conflict(format!(
                    "revision {} already exists",
                    revision.id
                )));
            }
            s.durable.revisions.insert(revision.id.clone(), revision);
            Ok(())
        })
    }

    fn get(&self, id: &RevisionId) -> Result<Option<FunctionRevision>, RepoError> {
        Ok(self.state.read().durable.revisions.get(id).cloned())
    }

    fn list_by_function(&self, function: &FunctionId) -> Result<Vec<FunctionRevision>, RepoError> {
        let mut v: Vec<FunctionRevision> = self
            .state
            .read()
            .durable
            .revisions
            .values()
            .filter(|r| &r.function_id == function)
            .cloned()
            .collect();
        v.sort_by_key(|r| r.number);
        Ok(v)
    }

    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        self.mutate(|s| match s.durable.revisions.get_mut(&revision.id) {
            Some(slot) => {
                *slot = revision;
                Ok(())
            }
            None => Err(RepoError::NotFound(format!("revision {}", revision.id))),
        })
    }
}

impl AliasRepository for InMemoryStore {
    fn get(
        &self,
        function: &FunctionId,
        name: &AliasName,
    ) -> Result<Option<FunctionAlias>, RepoError> {
        Ok(self
            .state
            .read()
            .durable
            .aliases
            .get(&alias_key(function, name))
            .cloned())
    }

    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError> {
        let mut v: Vec<FunctionAlias> = self
            .state
            .read()
            .durable
            .aliases
            .values()
            .filter(|a| &a.function_id == function)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    fn modify(
        &self,
        function: &FunctionId,
        name: &AliasName,
        f: &mut dyn FnMut(&mut Option<FunctionAlias>) -> Result<(), crate::error::AppError>,
    ) -> Result<Option<FunctionAlias>, crate::error::AppError> {
        let key = alias_key(function, name);
        self.mutate(|s| {
            let mut slot = s.durable.aliases.get(&key).cloned();
            f(&mut slot)?;
            match &slot {
                Some(a) => {
                    s.durable.aliases.insert(key, a.clone());
                }
                None => {
                    s.durable.aliases.remove(&key);
                }
            }
            Ok(slot)
        })
    }
}

impl InvocationRepository for InMemoryStore {
    fn insert(&self, invocation: Invocation) -> Result<(), RepoError> {
        self.mutate(|s| {
            if s.durable.invocations.contains_key(&invocation.id) {
                return Err(RepoError::Conflict(format!(
                    "invocation {} already exists",
                    invocation.id
                )));
            }
            s.durable
                .invocations
                .insert(invocation.id.clone(), invocation);
            Ok(())
        })
    }

    fn get(&self, id: &InvocationId) -> Result<Option<Invocation>, RepoError> {
        Ok(self.state.read().durable.invocations.get(id).cloned())
    }

    fn update(&self, invocation: Invocation) -> Result<(), RepoError> {
        self.mutate(|s| match s.durable.invocations.get_mut(&invocation.id) {
            Some(slot) => {
                *slot = invocation;
                Ok(())
            }
            None => Err(RepoError::NotFound(format!("invocation {}", invocation.id))),
        })
    }

    fn list_by_function(
        &self,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<Invocation>, RepoError> {
        let mut v: Vec<Invocation> = self
            .state
            .read()
            .durable
            .invocations
            .values()
            .filter(|i| &i.function_id == function)
            .cloned()
            .collect();
        v.sort_by(|a, b| b.accepted_at.cmp(&a.accepted_at).then(b.id.cmp(&a.id)));
        v.truncate(limit);
        Ok(v)
    }

    fn insert_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        self.mutate(|s| {
            if s.durable.attempts.contains_key(&attempt.id) {
                return Err(RepoError::Conflict(format!(
                    "attempt {} already exists",
                    attempt.id
                )));
            }
            s.durable.attempts.insert(attempt.id.clone(), attempt);
            Ok(())
        })
    }

    fn get_attempt(&self, id: &AttemptId) -> Result<Option<InvocationAttempt>, RepoError> {
        Ok(self.state.read().durable.attempts.get(id).cloned())
    }

    fn update_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        self.mutate(|s| match s.durable.attempts.get_mut(&attempt.id) {
            Some(slot) => {
                *slot = attempt;
                Ok(())
            }
            None => Err(RepoError::NotFound(format!("attempt {}", attempt.id))),
        })
    }

    fn attempts_of(&self, invocation: &InvocationId) -> Result<Vec<InvocationAttempt>, RepoError> {
        let mut v: Vec<InvocationAttempt> = self
            .state
            .read()
            .durable
            .attempts
            .values()
            .filter(|a| &a.invocation_id == invocation)
            .cloned()
            .collect();
        v.sort_by_key(|a| a.number);
        Ok(v)
    }
}

impl EnvironmentRepository for InMemoryStore {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        self.mutate(|s| {
            if s.durable.environments.contains_key(&env.id) {
                return Err(RepoError::Conflict(format!(
                    "environment {} already exists",
                    env.id
                )));
            }
            s.durable.environments.insert(env.id.clone(), env);
            Ok(())
        })
    }

    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError> {
        Ok(self.state.read().durable.environments.get(id).cloned())
    }

    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        self.mutate(|s| match s.durable.environments.get_mut(&env.id) {
            Some(slot) => {
                *slot = env;
                Ok(())
            }
            None => Err(RepoError::NotFound(format!("environment {}", env.id))),
        })
    }

    fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        Ok(self
            .state
            .read()
            .durable
            .environments
            .values()
            .filter(|e| !e.is_terminal())
            .cloned()
            .collect())
    }

    fn insert_lease(&self, lease: ExecutionLease) -> Result<(), RepoError> {
        let mut s = self.state.write();
        if s.leases.contains_key(&lease.id) {
            return Err(RepoError::Conflict(format!(
                "lease {} already exists",
                lease.id
            )));
        }
        s.leases.insert(lease.id.clone(), lease);
        Ok(())
    }

    fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError> {
        Ok(self.state.read().leases.get(id).cloned())
    }

    fn update_lease(&self, lease: ExecutionLease) -> Result<(), RepoError> {
        let mut s = self.state.write();
        match s.leases.get_mut(&lease.id) {
            Some(slot) => {
                *slot = lease;
                Ok(())
            }
            None => Err(RepoError::NotFound(format!("lease {}", lease.id))),
        }
    }
}

fn log_key(record: &LogRecord) -> String {
    match &record.invocation_id {
        Some(inv) => format!("inv:{inv}"),
        None => format!("env:{}", record.environment_id),
    }
}

impl LogRepository for InMemoryStore {
    fn append(&self, record: LogRecord) -> AppendOutcome {
        let key = log_key(&record);
        let max_lines = self.limits.max_log_lines_per_invocation as usize;
        let max_bytes = self.limits.max_log_bytes_per_invocation;
        let mut s = self.state.write();
        let bucket = s.logs.entry(key).or_default();
        let line_bytes = record.line.len() as u64;
        if bucket.records.len() >= max_lines || bucket.bytes + line_bytes > max_bytes {
            bucket.dropped = true;
            return AppendOutcome::Dropped;
        }
        bucket.bytes += line_bytes;
        bucket.records.push(record);
        AppendOutcome::Stored
    }

    fn query(&self, invocation: &InvocationId) -> LogQuery {
        let s = self.state.read();
        match s.logs.get(&format!("inv:{invocation}")) {
            Some(b) => LogQuery {
                records: b.records.clone(),
                dropped: b.dropped,
            },
            None => LogQuery::default(),
        }
    }
}

impl IdempotencyRepository for InMemoryStore {
    fn reserve(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
        input_digest: &Sha256Digest,
        invocation_id: &InvocationId,
    ) -> Result<IdempotencyOutcome, RepoError> {
        let k = IdempotencyKey {
            tenant_id: tenant.clone(),
            function_id: function.clone(),
            key: key.to_string(),
        };
        Ok(self.mutate(|s| match s.idempotency.get(&k) {
            Some(existing) => IdempotencyOutcome::Existing {
                invocation_id: existing.invocation_id.clone(),
                input_digest: existing.input_digest.clone(),
            },
            None => {
                s.idempotency.insert(
                    k,
                    IdempotencyEntry {
                        invocation_id: invocation_id.clone(),
                        input_digest: input_digest.clone(),
                    },
                );
                IdempotencyOutcome::Reserved
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tachyon_serverless_domain::{
        Deadlines, EventKind, InvocationMode, LogPhase, LogStream, ProviderKind, ReuseKey,
    };

    fn now() -> Timestamp {
        chrono::Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap()
    }

    fn function(tenant: &TenantId, name: &str) -> Function {
        Function::new(
            FunctionId::generate(),
            tenant.clone(),
            FunctionName::parse(name).unwrap(),
            String::new(),
            now(),
        )
        .unwrap()
    }

    #[test]
    fn function_name_unique_per_tenant() {
        let store = InMemoryStore::new(Limits::default());
        let t = TenantId::generate();
        FunctionRepository::insert(&store, function(&t, "hello")).unwrap();
        assert!(matches!(
            FunctionRepository::insert(&store, function(&t, "hello")),
            Err(RepoError::Conflict(_))
        ));
        let other = TenantId::generate();
        FunctionRepository::insert(&store, function(&other, "hello")).unwrap();
        assert_eq!(FunctionRepository::list(&store, &t).unwrap().len(), 1);
    }

    #[test]
    fn logs_are_bounded_per_invocation() {
        let limits = Limits {
            max_log_lines_per_invocation: 2,
            ..Limits::default()
        };
        let store = InMemoryStore::new(limits);
        let inv = InvocationId::generate();
        let rec = |line: &str| LogRecord {
            tenant_id: TenantId::generate(),
            environment_id: EnvironmentId::generate(),
            invocation_id: Some(inv.clone()),
            attempt_id: None,
            stream: LogStream::Stdout,
            phase: LogPhase::Handler,
            timestamp: now(),
            line: line.into(),
            truncated: false,
        };
        assert_eq!(store.append(rec("a")), AppendOutcome::Stored);
        assert_eq!(store.append(rec("b")), AppendOutcome::Stored);
        assert_eq!(store.append(rec("c")), AppendOutcome::Dropped);
        let q = store.query(&inv);
        assert_eq!(q.records.len(), 2);
        assert!(q.dropped);
    }

    #[test]
    fn idempotency_reserve_then_existing() {
        let store = InMemoryStore::new(Limits::default());
        let t = TenantId::generate();
        let f = FunctionId::generate();
        let d = Sha256Digest::of_bytes(b"{}");
        let inv = InvocationId::generate();
        assert_eq!(
            store.reserve(&t, &f, "k", &d, &inv).unwrap(),
            IdempotencyOutcome::Reserved
        );
        let again = store
            .reserve(&t, &f, "k", &d, &InvocationId::generate())
            .unwrap();
        assert_eq!(
            again,
            IdempotencyOutcome::Existing {
                invocation_id: inv,
                input_digest: d
            }
        );
    }

    #[test]
    fn persistence_roundtrip_and_restart_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let t = TenantId::generate();
        let f = function(&t, "persisted");
        let fid = f.id.clone();
        let rev = RevisionId::generate();
        {
            let store =
                InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
            FunctionRepository::insert(&store, f).unwrap();
            let inv = Invocation::accept(
                InvocationId::generate(),
                t.clone(),
                fid.clone(),
                None,
                rev.clone(),
                InvocationMode::Sync,
                EventKind::Json,
                Deadlines {
                    queue_deadline: now(),
                    init_deadline: None,
                    execution_deadline: None,
                    client_deadline: now(),
                },
                None,
                Sha256Digest::of_bytes(b"{}"),
                2,
                "trace".into(),
                now(),
            )
            .unwrap();
            InvocationRepository::insert(&store, inv).unwrap();
            let env = ExecutionEnvironment::request(
                EnvironmentId::generate(),
                t.clone(),
                rev.clone(),
                ProviderKind::Fake,
                ReuseKey {
                    tenant_id: t.clone(),
                    revision_id: rev.clone(),
                    execution_role_version: 1,
                    configuration_version: 1,
                    resource_profile_digest: "d".into(),
                    runtime_profile: "p".into(),
                    network_policy_version: 1,
                    secret_binding_generation: 1,
                },
                now(),
            );
            EnvironmentRepository::insert(&store, env).unwrap();
            assert!(dir.path().join("state.json").exists());
        }
        let store = InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
        assert!(FunctionRepository::get(&store, &fid).unwrap().is_some());
        let invs = InvocationRepository::list_by_function(&store, &fid, 10).unwrap();
        assert_eq!(invs.len(), 1);
        assert!(
            invs[0].status.is_terminal(),
            "in-flight work is failed on restart"
        );
        assert!(
            EnvironmentRepository::list_active(&store)
                .unwrap()
                .is_empty()
        );
        let text = std::fs::read_to_string(dir.path().join("state.json")).unwrap();
        assert!(text.contains("persisted"));
    }
}

//! Repositories: traits plus the in-memory implementation.
//!
//! [`InMemoryStore`] keeps everything behind a single `parking_lot::RwLock`
//! and (optionally) writes the durable part through to
//! `<data_dir>/state.json` after every mutation, best effort. Logs are kept
//! in memory only and bounded per invocation by [`Limits`]. Leases are
//! transient and never persisted. No secret ever enters the store.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{
    AliasName, AttemptId, EnvironmentId, EnvironmentState, ErrorClass, ExecutionEnvironment,
    ExecutionLease, Function, FunctionAlias, FunctionId, FunctionName, FunctionRevision,
    Invocation, InvocationAttempt, InvocationError, InvocationId, InvocationStatus, LeaseId,
    Limits, LogRecord, ReuseKey, RevisionId, Sha256Digest, TenantId, Timestamp,
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

/// Caps the pool enforces when an environment is released back into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolLimits {
    /// Idle environments kept per reuse key.
    pub max_idle_per_key: usize,
    /// Idle environments kept across every reuse key.
    pub max_total_idle: usize,
}

pub trait EnvironmentRepository: Send + Sync {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError>;
    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError>;
    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError>;
    fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError>;

    /// The environments currently in the pool, longest idle first.
    fn list_idle(&self) -> Result<Vec<ExecutionEnvironment>, RepoError>;

    /// Atomically hand out one pooled environment whose reuse key equals
    /// `key` in *every* field, moving it to `Busy` and advancing its epoch.
    ///
    /// Searching, the state change and the epoch bump all happen inside one
    /// store mutation, so of two concurrent claims exactly one can win: the
    /// loser sees the environment as `Busy` and skips it, or finds no
    /// candidate at all. `None` means the caller must create an environment.
    fn claim_for_reuse(
        &self,
        key: &ReuseKey,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError>;

    /// Atomically put a finished environment back into the pool.
    ///
    /// `env` is the caller's copy of the `Busy` row, including whatever
    /// evidence the attempt added. It is refused (`None`) when the stored row
    /// moved on since — a different epoch, or no longer `Busy` — and when a
    /// cap in `limits` is reached. The caller then terminates it instead.
    fn release_to_pool(
        &self,
        env: &ExecutionEnvironment,
        limits: PoolLimits,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError>;

    /// Atomically take one idle environment out of the pool for termination
    /// (TTL sweep or drain), moving it to `Draining`. False when it is no
    /// longer idle, i.e. an attempt claimed it first.
    fn take_idle_for_termination(
        &self,
        id: &EnvironmentId,
        now: Timestamp,
    ) -> Result<bool, RepoError>;

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

/// An idempotency key bound to an invocation that exists in the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyBinding {
    pub invocation_id: InvocationId,
    pub input_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    /// The invocation was inserted and its key (if any) is now bound to it.
    Inserted,
    /// The key is already bound to another invocation; nothing was inserted.
    Existing(IdempotencyBinding),
}

/// Idempotency keys (docs/threat-model.md §10).
///
/// A key is only ever bound together with the ledger row of its invocation,
/// in one store mutation, so a request that is rejected before acceptance
/// (400 / 413 / 429) never consumes its key and a replay can never observe a
/// key without its invocation.
pub trait IdempotencyRepository: Send + Sync {
    /// The invocation `key` is bound to, if that invocation exists.
    fn lookup(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
    ) -> Result<Option<IdempotencyBinding>, RepoError>;

    /// Atomically insert `invocation` and bind its `idempotency_key`. When
    /// the key is already bound to an existing invocation nothing is written
    /// and that binding is returned instead. A binding whose invocation does
    /// not exist is stale and is replaced.
    fn insert_bound(&self, invocation: Invocation) -> Result<IdempotencyOutcome, RepoError>;
}

/// Tenant ownership of content-addressed artifacts (docs/threat-model.md
/// §14-1). A digest a tenant never uploaded must be indistinguishable from a
/// digest that does not exist.
pub trait ArtifactOwnerRepository: Send + Sync {
    /// Record that `tenant` uploaded `digest`. Idempotent; identical bytes
    /// uploaded by two tenants give each tenant its own ownership row.
    fn claim(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<(), RepoError>;
    fn is_owned_by(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<bool, RepoError>;
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
    pub artifact_owners: Arc<dyn ArtifactOwnerRepository>,
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
            idempotency: store.clone(),
            artifact_owners: store,
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
    /// Tenants that uploaded each artifact digest.
    #[serde(default)]
    artifact_owners: BTreeMap<Sha256Digest, BTreeSet<TenantId>>,
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
    /// environments found on disk are settled by [`reconcile_after_restart`]
    /// because their driver tasks no longer exist: a dispatched invocation
    /// ends as `OutcomeUnknown`, one that never started as
    /// `Failed{PlatformError}`, and every environment as `Lost`.
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
                serde_json::from_str(&text).map_err(|e| {
                    RepoError::Serialization(format!(
                        "{} is not a valid state file ({e}); it may have been truncated by an \
                         unclean shutdown. Move it aside to start with an empty ledger.",
                        path.display()
                    ))
                })?
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

/// Error type carried by everything the restart reconcile settles.
pub const HOST_RESTARTED: &str = "Host.Restarted";

/// Classify what was in flight when the previous process died
/// (docs/threat-model.md §9).
///
/// An invocation that was already `Running` had its `Invoke` frame written,
/// so the handler may have run: its outcome is unknown and must never be
/// reported as a plain failure. One that never left `Accepted` / `Queued`
/// was never dispatched, so it provably did not start and fails with
/// `PlatformError`. Attempts follow their invocation and environments become
/// `Lost`; the host processes behind them are reclaimed separately by
/// [`crate::services::ReconcileService`].
///
/// Idempotency keys bound to an invocation that is not in the ledger (left
/// behind by older versions that reserved keys before acceptance) are
/// dropped, so a retry with such a key is accepted as a new invocation.
fn reconcile_after_restart(state: &mut PersistedState, now: Timestamp) {
    const INVOCATION_MSG: &str = "gateway restarted while the invocation was in flight";
    const UNKNOWN_MSG: &str =
        "gateway restarted after the invocation was dispatched; the handler may have run";
    const ATTEMPT_MSG: &str = "gateway restarted while the attempt was in flight";

    let PersistedState {
        invocations,
        idempotency,
        ..
    } = &mut *state;
    idempotency.retain(|(_, entry)| invocations.contains_key(&entry.invocation_id));
    for inv in state.invocations.values_mut() {
        if inv.status.is_terminal() {
            continue;
        }
        if matches!(inv.status, InvocationStatus::Running) {
            if inv.mark_outcome_unknown(UNKNOWN_MSG, now).is_ok()
                && let InvocationStatus::OutcomeUnknown { error } = &mut inv.status
            {
                // The domain stamps the generic `Host.OutcomeUnknown`; a
                // restart names itself so the cause stays visible.
                error.error_type = HOST_RESTARTED.to_string();
            }
        } else {
            let _ = inv.mark_failed(
                InvocationError::new(ErrorClass::PlatformError, HOST_RESTARTED, INVOCATION_MSG),
                now,
            );
        }
    }
    // An attempt is settled like the invocation it belongs to, so a caller
    // never sees a failed attempt under an unknown outcome.
    let unknown: BTreeSet<InvocationId> = state
        .invocations
        .values()
        .filter(|inv| matches!(inv.status, InvocationStatus::OutcomeUnknown { .. }))
        .map(|inv| inv.id.clone())
        .collect();
    for att in state.attempts.values_mut() {
        if att.status.is_terminal() {
            continue;
        }
        if unknown.contains(&att.invocation_id) {
            let _ = att.outcome_unknown(
                InvocationError::new(ErrorClass::OutcomeUnknown, HOST_RESTARTED, UNKNOWN_MSG),
                now,
            );
        } else {
            let _ = att.fail(
                InvocationError::new(ErrorClass::PlatformError, HOST_RESTARTED, ATTEMPT_MSG),
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

    fn list_idle(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        let mut v: Vec<ExecutionEnvironment> = self
            .state
            .read()
            .durable
            .environments
            .values()
            .filter(|e| matches!(e.state, EnvironmentState::Idle))
            .cloned()
            .collect();
        v.sort_by(|a, b| a.idle_since.cmp(&b.idle_since).then(a.id.cmp(&b.id)));
        Ok(v)
    }

    fn claim_for_reuse(
        &self,
        key: &ReuseKey,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError> {
        Ok(self.mutate(|s| {
            // `values()` is ordered by id (a ULID), so the oldest match wins.
            let id = s
                .durable
                .environments
                .values()
                .find(|e| e.is_reusable() && &e.reuse_key == key)
                .map(|e| e.id.clone())?;
            let env = s.durable.environments.get_mut(&id)?;
            // `reassign` refuses anything that is not Ready or Idle (so a Busy
            // environment is never handed out) and is what advances the epoch.
            env.reassign(now).ok()?;
            Some(env.clone())
        }))
    }

    fn release_to_pool(
        &self,
        env: &ExecutionEnvironment,
        limits: PoolLimits,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError> {
        Ok(self.mutate(|s| {
            let current = s.durable.environments.get(&env.id)?;
            if current.epoch != env.epoch || !matches!(current.state, EnvironmentState::Busy) {
                return None;
            }
            let is_idle = |e: &&ExecutionEnvironment| matches!(e.state, EnvironmentState::Idle);
            let total = s.durable.environments.values().filter(is_idle).count();
            let per_key = s
                .durable
                .environments
                .values()
                .filter(is_idle)
                .filter(|e| e.reuse_key == env.reuse_key)
                .count();
            if total >= limits.max_total_idle || per_key >= limits.max_idle_per_key {
                return None;
            }
            let mut pooled = env.clone();
            pooled.mark_idle(now).ok()?;
            s.durable
                .environments
                .insert(pooled.id.clone(), pooled.clone());
            Some(pooled)
        }))
    }

    fn take_idle_for_termination(
        &self,
        id: &EnvironmentId,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        Ok(self.mutate(|s| match s.durable.environments.get_mut(id) {
            Some(env) if matches!(env.state, EnvironmentState::Idle) => {
                env.mark_draining(now).is_ok()
            }
            _ => false,
        }))
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

fn live_binding(s: &State, key: &IdempotencyKey) -> Option<IdempotencyBinding> {
    s.idempotency
        .get(key)
        .filter(|e| s.durable.invocations.contains_key(&e.invocation_id))
        .map(|e| IdempotencyBinding {
            invocation_id: e.invocation_id.clone(),
            input_digest: e.input_digest.clone(),
        })
}

impl IdempotencyRepository for InMemoryStore {
    fn lookup(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
    ) -> Result<Option<IdempotencyBinding>, RepoError> {
        let k = IdempotencyKey {
            tenant_id: tenant.clone(),
            function_id: function.clone(),
            key: key.to_string(),
        };
        Ok(live_binding(&self.state.read(), &k))
    }

    fn insert_bound(&self, invocation: Invocation) -> Result<IdempotencyOutcome, RepoError> {
        let key = invocation
            .idempotency_key
            .as_ref()
            .map(|key| IdempotencyKey {
                tenant_id: invocation.tenant_id.clone(),
                function_id: invocation.function_id.clone(),
                key: key.clone(),
            });
        self.mutate(|s| {
            if let Some(k) = &key
                && let Some(existing) = live_binding(s, k)
            {
                return Ok(IdempotencyOutcome::Existing(existing));
            }
            if s.durable.invocations.contains_key(&invocation.id) {
                return Err(RepoError::Conflict(format!(
                    "invocation {} already exists",
                    invocation.id
                )));
            }
            if let Some(k) = key {
                s.idempotency.insert(
                    k,
                    IdempotencyEntry {
                        invocation_id: invocation.id.clone(),
                        input_digest: invocation.input_digest.clone(),
                    },
                );
            }
            s.durable
                .invocations
                .insert(invocation.id.clone(), invocation);
            Ok(IdempotencyOutcome::Inserted)
        })
    }
}

impl ArtifactOwnerRepository for InMemoryStore {
    fn claim(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<(), RepoError> {
        if self.is_owned_by(tenant, digest)? {
            return Ok(());
        }
        self.mutate(|s| {
            s.durable
                .artifact_owners
                .entry(digest.clone())
                .or_default()
                .insert(tenant.clone());
        });
        Ok(())
    }

    fn is_owned_by(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<bool, RepoError> {
        Ok(self
            .state
            .read()
            .durable
            .artifact_owners
            .get(digest)
            .is_some_and(|owners| owners.contains(tenant)))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn corrupt_state_file_is_refused_with_a_hint() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("state.json"), vec![0u8; 64]).unwrap();
        let Err(err) =
            InMemoryStore::with_persistence(dir.path(), Limits::default(), chrono::Utc::now())
        else {
            panic!("corrupt state must be refused");
        };
        let msg = err.to_string();
        assert!(msg.contains("not a valid state file"), "{msg}");
        assert!(msg.contains("Move it aside"), "{msg}");
    }

    use super::*;
    use chrono::TimeZone;
    use tachyon_serverless_domain::{
        AttemptStatus, BootEvidence, Deadlines, EventKind, InvocationMode, LogPhase, LogStream,
        ProviderKind, StartKind,
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

    fn keyed_invocation(t: &TenantId, f: &FunctionId, key: Option<&str>) -> Invocation {
        Invocation::accept(
            InvocationId::generate(),
            t.clone(),
            f.clone(),
            None,
            RevisionId::generate(),
            InvocationMode::Sync,
            EventKind::Json,
            Deadlines {
                queue_deadline: now(),
                init_deadline: None,
                execution_deadline: None,
                client_deadline: now(),
            },
            key.map(str::to_string),
            Sha256Digest::of_bytes(b"{}"),
            2,
            "trace".into(),
            now(),
        )
        .unwrap()
    }

    #[test]
    fn idempotency_key_is_bound_with_its_invocation() {
        let store = InMemoryStore::new(Limits::default());
        let t = TenantId::generate();
        let f = FunctionId::generate();
        assert_eq!(store.lookup(&t, &f, "k").unwrap(), None);
        let first = keyed_invocation(&t, &f, Some("k"));
        let first_id = first.id.clone();
        assert_eq!(
            store.insert_bound(first).unwrap(),
            IdempotencyOutcome::Inserted
        );
        let binding = IdempotencyBinding {
            invocation_id: first_id.clone(),
            input_digest: Sha256Digest::of_bytes(b"{}"),
        };
        assert_eq!(store.lookup(&t, &f, "k").unwrap(), Some(binding.clone()));
        assert!(
            InvocationRepository::get(&store, &first_id)
                .unwrap()
                .is_some()
        );

        // A second invocation with the same key is not inserted.
        let second = keyed_invocation(&t, &f, Some("k"));
        let second_id = second.id.clone();
        assert_eq!(
            store.insert_bound(second).unwrap(),
            IdempotencyOutcome::Existing(binding)
        );
        assert!(
            InvocationRepository::get(&store, &second_id)
                .unwrap()
                .is_none()
        );
        // Same key, other tenant: its own scope.
        let other = TenantId::generate();
        assert_eq!(
            store
                .insert_bound(keyed_invocation(&other, &f, Some("k")))
                .unwrap(),
            IdempotencyOutcome::Inserted
        );
        // No key: plain insert; a duplicate id conflicts.
        let plain = keyed_invocation(&t, &f, None);
        assert_eq!(
            store.insert_bound(plain.clone()).unwrap(),
            IdempotencyOutcome::Inserted
        );
        assert!(matches!(
            store.insert_bound(plain),
            Err(RepoError::Conflict(_))
        ));
    }

    #[test]
    fn dangling_idempotency_entries_are_dropped_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let t = TenantId::generate();
        let f = FunctionId::generate();
        let live = keyed_invocation(&t, &f, Some("live"));
        let live_id = live.id.clone();
        {
            let store =
                InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
            store.insert_bound(live).unwrap();
        }
        // Simulate a state file written by a version that reserved keys
        // before acceptance: a key bound to an invocation that never existed.
        let path = dir.path().join("state.json");
        let mut state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        state["idempotency"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!([
                {"tenant_id": t.to_string(), "function_id": f.to_string(), "key": "dangling"},
                {"invocation_id": InvocationId::generate().to_string(),
                 "input_digest": Sha256Digest::of_bytes(b"{}").to_string()}
            ]));
        std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();

        let store = InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
        assert_eq!(store.lookup(&t, &f, "dangling").unwrap(), None);
        assert_eq!(
            store
                .lookup(&t, &f, "live")
                .unwrap()
                .map(|b| b.invocation_id),
            Some(live_id)
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("dangling"), "healed state is written back");
        assert_eq!(
            store
                .insert_bound(keyed_invocation(&t, &f, Some("dangling")))
                .unwrap(),
            IdempotencyOutcome::Inserted
        );
    }

    #[test]
    fn artifact_ownership_is_per_tenant_and_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let a = TenantId::generate();
        let b = TenantId::generate();
        let d = Sha256Digest::of_bytes(b"binary");
        {
            let store =
                InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
            assert!(!store.is_owned_by(&a, &d).unwrap());
            store.claim(&a, &d).unwrap();
            store.claim(&a, &d).unwrap();
            assert!(store.is_owned_by(&a, &d).unwrap());
            assert!(!store.is_owned_by(&b, &d).unwrap());
        }
        let store = InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
        assert!(store.is_owned_by(&a, &d).unwrap());
        assert!(!store.is_owned_by(&b, &d).unwrap());
        store.claim(&b, &d).unwrap();
        assert!(store.is_owned_by(&b, &d).unwrap());
    }

    #[test]
    fn state_without_artifact_owners_still_loads() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("state.json"), br#"{"functions": {}}"#).unwrap();
        let store = InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
        assert!(
            !store
                .is_owned_by(&TenantId::generate(), &Sha256Digest::of_bytes(b"x"))
                .unwrap()
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
            "work that never started is failed on restart"
        );
        assert!(
            EnvironmentRepository::list_active(&store)
                .unwrap()
                .is_empty()
        );
        let text = std::fs::read_to_string(dir.path().join("state.json")).unwrap();
        assert!(text.contains("persisted"));
    }

    /// docs/threat-model.md §9: a restart may only report a failure for work
    /// that provably never started.
    #[test]
    fn restart_separates_dispatched_work_from_work_that_never_started() {
        let dir = tempfile::tempdir().unwrap();
        let t = TenantId::generate();
        let f = FunctionId::generate();

        let mut queued = keyed_invocation(&t, &f, None);
        queued.mark_queued().unwrap();
        let mut running = keyed_invocation(&t, &f, None);
        let attempt_id = AttemptId::generate();
        running
            .mark_running(attempt_id.clone(), now(), now(), now())
            .unwrap();
        let mut finished = keyed_invocation(&t, &f, None);
        finished
            .mark_running(AttemptId::generate(), now(), now(), now())
            .unwrap();
        finished.mark_succeeded(None, None, now()).unwrap();
        let attempt = InvocationAttempt::dispatch(
            attempt_id.clone(),
            running.id.clone(),
            t.clone(),
            1,
            EnvironmentId::generate(),
            1,
            StartKind::Cold,
            now(),
        );
        let (queued_id, running_id, finished_id) =
            (queued.id.clone(), running.id.clone(), finished.id.clone());
        {
            let store =
                InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
            for inv in [queued, running, finished] {
                InvocationRepository::insert(&store, inv).unwrap();
            }
            store.insert_attempt(attempt).unwrap();
        }

        let store = InMemoryStore::with_persistence(dir.path(), Limits::default(), now()).unwrap();
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
    }

    // -----------------------------------------------------------------------
    // environment pool (PLT-4632)
    // -----------------------------------------------------------------------

    const POOL: PoolLimits = PoolLimits {
        max_idle_per_key: 2,
        max_total_idle: 4,
    };

    fn pool_key(t: &TenantId, r: &RevisionId) -> ReuseKey {
        ReuseKey {
            tenant_id: t.clone(),
            revision_id: r.clone(),
            execution_role_version: 1,
            configuration_version: 7,
            resource_profile_digest: "rp".into(),
            runtime_profile: "tachyon.runtime.v1".into(),
            network_policy_version: 3,
            secret_binding_generation: 11,
        }
    }

    fn ready_environment(key: &ReuseKey) -> ExecutionEnvironment {
        let mut env = ExecutionEnvironment::request(
            EnvironmentId::generate(),
            key.tenant_id.clone(),
            key.revision_id.clone(),
            ProviderKind::Fake,
            key.clone(),
            now(),
        );
        env.mark_provisioning(now()).unwrap();
        env.mark_initializing(BootEvidence::default(), now())
            .unwrap();
        env.mark_ready(now()).unwrap();
        env
    }

    /// An environment that reached Ready, served an attempt and went back
    /// into the pool.
    fn pooled(store: &InMemoryStore, key: &ReuseKey) -> EnvironmentId {
        let mut env = ready_environment(key);
        env.mark_busy(now()).unwrap();
        env.mark_idle(now()).unwrap();
        let id = env.id.clone();
        EnvironmentRepository::insert(store, env).unwrap();
        id
    }

    /// One dimension of the reuse key: its name, and how to make it differ.
    type Dimension = (&'static str, fn(&mut ReuseKey));

    /// PLT-4632 acceptance 1: one differing field of the reuse key is enough
    /// to make two environments incompatible.
    #[test]
    fn only_an_exactly_matching_reuse_key_is_reused() {
        let store = InMemoryStore::new(Limits::default());
        let key = pool_key(&TenantId::generate(), &RevisionId::generate());
        let id = pooled(&store, &key);

        let dimensions: [Dimension; 8] = [
            ("tenant", |k| k.tenant_id = TenantId::generate()),
            ("revision", |k| k.revision_id = RevisionId::generate()),
            ("execution role version", |k| k.execution_role_version += 1),
            ("configuration version", |k| k.configuration_version += 1),
            ("resource profile", |k| {
                k.resource_profile_digest = "other".into()
            }),
            ("runtime profile", |k| {
                k.runtime_profile = "tachyon.runtime.v2".into()
            }),
            ("network policy version", |k| k.network_policy_version += 1),
            ("secret binding generation", |k| {
                k.secret_binding_generation += 1
            }),
        ];
        for (name, change) in dimensions {
            let mut other = key.clone();
            change(&mut other);
            assert_ne!(other, key, "{name} must actually differ");
            assert!(
                store.claim_for_reuse(&other, now()).unwrap().is_none(),
                "a differing {name} must never reuse the environment"
            );
        }
        // Nothing above touched the pooled environment.
        assert_eq!(store.list_idle().unwrap().len(), 1);
        let claimed = store
            .claim_for_reuse(&key, now())
            .unwrap()
            .expect("the exact key hits");
        assert_eq!(claimed.id, id);
        assert_eq!(claimed.state, EnvironmentState::Busy);
        assert_eq!(claimed.epoch, 2, "a reassignment advances the epoch");
        assert!(
            store.claim_for_reuse(&key, now()).unwrap().is_none(),
            "it was handed out once"
        );
    }

    /// PLT-4632 acceptance 2.
    #[test]
    fn nothing_is_dispatched_before_ready_and_busy_is_never_handed_out() {
        let store = InMemoryStore::new(Limits::default());
        let key = pool_key(&TenantId::generate(), &RevisionId::generate());
        let fresh = || {
            ExecutionEnvironment::request(
                EnvironmentId::generate(),
                key.tenant_id.clone(),
                key.revision_id.clone(),
                ProviderKind::Fake,
                key.clone(),
                now(),
            )
        };

        let mut provisioning = fresh();
        provisioning.mark_provisioning(now()).unwrap();
        let mut initializing = fresh();
        initializing.mark_provisioning(now()).unwrap();
        initializing
            .mark_initializing(BootEvidence::default(), now())
            .unwrap();
        let mut busy = ready_environment(&key);
        busy.mark_busy(now()).unwrap();
        let busy_id = busy.id.clone();
        let mut draining = ready_environment(&key);
        draining.mark_busy(now()).unwrap();
        draining.mark_idle(now()).unwrap();
        draining.mark_draining(now()).unwrap();
        let mut stopped = ready_environment(&key);
        stopped.mark_stopped(now()).unwrap();

        for env in [fresh(), provisioning, initializing, busy, draining, stopped] {
            EnvironmentRepository::insert(&store, env).unwrap();
        }
        assert!(
            store.claim_for_reuse(&key, now()).unwrap().is_none(),
            "only an environment that reported ready and is free may be handed out"
        );
        assert_eq!(
            EnvironmentRepository::get(&store, &busy_id)
                .unwrap()
                .unwrap()
                .epoch,
            1,
            "a refused claim never advances an epoch"
        );
        assert!(store.list_idle().unwrap().is_empty());

        // The same key hits as soon as one is actually pooled.
        let id = pooled(&store, &key);
        assert_eq!(
            store.claim_for_reuse(&key, now()).unwrap().map(|e| e.id),
            Some(id)
        );
    }

    /// Property: with `pooled` idle environments and `claimers` threads racing
    /// for them, exactly `min(pooled, claimers)` claims win, no environment is
    /// handed to two callers, and every winner comes back one epoch further
    /// on. This is the single-store-mutation guarantee of `claim_for_reuse`.
    #[test]
    fn concurrent_claims_never_hand_the_same_environment_to_two_callers() {
        for (idle, claimers) in [(1usize, 2usize), (1, 16), (3, 8), (8, 3), (4, 4)] {
            let store = Arc::new(InMemoryStore::new(Limits::default()));
            let key = pool_key(&TenantId::generate(), &RevisionId::generate());
            for _ in 0..idle {
                pooled(&store, &key);
            }
            let barrier = Arc::new(std::sync::Barrier::new(claimers));
            let racers: Vec<_> = (0..claimers)
                .map(|_| {
                    let store = store.clone();
                    let key = key.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        store.claim_for_reuse(&key, now()).unwrap()
                    })
                })
                .collect();
            let winners: Vec<ExecutionEnvironment> = racers
                .into_iter()
                .filter_map(|h| h.join().expect("claim panicked"))
                .collect();

            let case = format!("idle={idle} claimers={claimers}");
            assert_eq!(winners.len(), idle.min(claimers), "{case}");
            let mut ids: Vec<String> = winners.iter().map(|e| e.id.to_string()).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(
                ids.len(),
                winners.len(),
                "{case}: an environment was handed out twice"
            );
            for w in &winners {
                assert_eq!(w.state, EnvironmentState::Busy, "{case}");
                assert_eq!(w.epoch, 2, "{case}: every reassignment advances the epoch");
            }
            assert_eq!(
                store.list_idle().unwrap().len(),
                idle.saturating_sub(claimers),
                "{case}: the losers' environments stay pooled"
            );
        }
    }

    #[test]
    fn releasing_respects_the_pool_caps_and_refuses_a_stale_copy() {
        let store = InMemoryStore::new(Limits::default());
        let key = pool_key(&TenantId::generate(), &RevisionId::generate());
        let other_key = pool_key(&key.tenant_id, &RevisionId::generate());

        // Two per key is the cap: the third stays Busy for the caller to kill.
        let mut busy = Vec::new();
        for _ in 0..3 {
            let mut env = ready_environment(&key);
            env.mark_busy(now()).unwrap();
            EnvironmentRepository::insert(&store, env.clone()).unwrap();
            busy.push(env);
        }
        assert!(
            store
                .release_to_pool(&busy[0], POOL, now())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .release_to_pool(&busy[1], POOL, now())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .release_to_pool(&busy[2], POOL, now())
                .unwrap()
                .is_none(),
            "max_idle_per_key is enforced"
        );
        assert_eq!(
            EnvironmentRepository::get(&store, &busy[2].id)
                .unwrap()
                .unwrap()
                .state,
            EnvironmentState::Busy,
            "a refused release leaves the environment for the caller to terminate"
        );

        // A stale copy (the row moved on since) is refused.
        let pooled_row = EnvironmentRepository::get(&store, &busy[0].id)
            .unwrap()
            .unwrap();
        assert!(
            store
                .release_to_pool(&busy[0], POOL, now())
                .unwrap()
                .is_none(),
            "the row is Idle now, not Busy"
        );
        let mut wrong_epoch = busy[2].clone();
        wrong_epoch.epoch = 99;
        assert!(
            store
                .release_to_pool(&wrong_epoch, POOL, now())
                .unwrap()
                .is_none(),
            "a copy from another epoch never goes back into the pool"
        );
        assert_eq!(pooled_row.state, EnvironmentState::Idle);
        assert_eq!(pooled_row.idle_since, Some(now()));

        // Two more keys still fit under max_total_idle (4).
        for _ in 0..2 {
            let mut env = ready_environment(&other_key);
            env.mark_busy(now()).unwrap();
            EnvironmentRepository::insert(&store, env.clone()).unwrap();
            assert!(store.release_to_pool(&env, POOL, now()).unwrap().is_some());
        }
        let mut env = ready_environment(&other_key);
        env.mark_busy(now()).unwrap();
        EnvironmentRepository::insert(&store, env.clone()).unwrap();
        assert!(
            store.release_to_pool(&env, POOL, now()).unwrap().is_none(),
            "max_total_idle is enforced across keys"
        );
        assert_eq!(store.list_idle().unwrap().len(), 4);
    }

    #[test]
    fn taking_an_idle_environment_for_termination_excludes_a_claim() {
        let store = InMemoryStore::new(Limits::default());
        let key = pool_key(&TenantId::generate(), &RevisionId::generate());
        let id = pooled(&store, &key);

        assert!(store.take_idle_for_termination(&id, now()).unwrap());
        assert!(
            !store.take_idle_for_termination(&id, now()).unwrap(),
            "taking it is idempotent-safe: the second caller loses"
        );
        assert!(
            store.claim_for_reuse(&key, now()).unwrap().is_none(),
            "an environment the sweeper owns is never handed to an attempt"
        );
        assert_eq!(
            EnvironmentRepository::get(&store, &id)
                .unwrap()
                .unwrap()
                .state,
            EnvironmentState::Draining
        );

        // The other way round: a claimed environment cannot be swept.
        let claimed = pooled(&store, &key);
        assert!(store.claim_for_reuse(&key, now()).unwrap().is_some());
        assert!(!store.take_idle_for_termination(&claimed, now()).unwrap());
    }
}

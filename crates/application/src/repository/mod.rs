//! Repositories: the control-plane ports, the cell-local [`SlotStore`] port
//! (slots, leases, pool, dispatchers; PLT-4631) and their two implementations
//! (docs/adr/0003-execution-state-persistence.md).
//!
//! - [`SqliteStore`] is the durable store the gateway runs on:
//!   `<data_dir>/state.db`, WAL, every write inside `BEGIN IMMEDIATE`,
//!   forward-only schema migrations and CAS updates (`WHERE` on the current
//!   `generation` / `epoch` / terminal flag, zero affected rows = lost race).
//!   A `state.json` left by a P1 gateway is imported once.
//! - [`InMemoryStore`] is the volatile implementation for tests. It never
//!   touches the file system.
//!
//! Both enforce the same row invariants ([`guard`]): a child row cannot cross
//! the tenant of the parent it names, identity fields never change, a terminal
//! row is never rewritten, an environment copy from another epoch is refused
//! and inline output is bounded. Logs are bounded per invocation and kept in
//! memory by both. No secret value ever enters either store.

use std::path::Path;
use std::sync::Arc;

use tachyon_serverless_domain::{
    AliasName, AttemptId, EnvironmentId, ExecutionEnvironment, Function, FunctionAlias, FunctionId,
    FunctionName, FunctionRevision, Invocation, InvocationAttempt, InvocationId, LogRecord,
    RevisionId, Sha256Digest, TenantId, Timestamp,
};

pub mod config;
pub mod guard;
mod legacy;
mod logs;
mod memory;
pub mod objects;
pub mod outbox;
pub mod restart;
pub mod slot;
pub mod sqlite;

#[cfg(test)]
pub(crate) mod contract_tests;
#[cfg(test)]
mod object_contract_tests;

pub use config::{
    ConfigObservation, ConfigPublicationRepository, ConfigRows, StampedConfig, StampedEntry,
};
pub use memory::InMemoryStore;
pub use objects::{CollectDecision, CollectReason, ObjectReferenceRepository};
pub use outbox::{
    AsyncAcceptOutcome, AsyncInput, AsyncInputBody, AsyncInvocationRepository, BacklogLimits,
    OutboxEvent, OutboxStats,
};
pub use restart::{HOST_LEASE_EXPIRED, HOST_RESTARTED};
pub use slot::{
    AcquireOutcome, CompletionOutcome, DispatcherRecord, HeartbeatOutcome, ReclaimReport,
    ReclaimRequest, SlotAcquire, SlotCompletion, SlotStore,
};
pub use sqlite::{SqliteOptions, SqliteStore};

#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("{0} not found")]
    NotFound(String),
    /// The row already exists (duplicate id or unique key).
    #[error("conflict: {0}")]
    Conflict(String),
    /// The write would break a row invariant: crossing a tenant, changing an
    /// identity field, rewriting a terminal row or writing a stale copy.
    #[error("refused: {0}")]
    Refused(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization: {0}")]
    Serialization(String),
    /// The database failed (not a constraint the caller can act on).
    #[error("store: {0}")]
    Store(String),
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
    /// Status changes only. The spec, number, function and tenant of a
    /// revision are immutable, and a Ready / Failed revision is final.
    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError>;
}

pub trait AliasRepository: Send + Sync {
    fn get(
        &self,
        function: &FunctionId,
        name: &AliasName,
    ) -> Result<Option<FunctionAlias>, RepoError>;
    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError>;
    /// Create an alias. `Conflict` when one with that name already exists.
    /// The revision must exist and belong to the same function and tenant.
    fn insert(&self, alias: FunctionAlias) -> Result<(), RepoError>;
    /// Replace the stored alias with `alias` only if its generation is still
    /// `expected_generation`. `Ok(false)` means another writer got there
    /// first and nothing was written; the caller re-reads and decides.
    fn compare_and_set(
        &self,
        alias: FunctionAlias,
        expected_generation: u64,
    ) -> Result<bool, RepoError>;
}

pub trait InvocationRepository: Send + Sync {
    fn insert(&self, invocation: Invocation) -> Result<(), RepoError>;
    fn get(&self, id: &InvocationId) -> Result<Option<Invocation>, RepoError>;
    /// Refused once the stored invocation is terminal (unless `invocation`
    /// is identical to it, which is a no-op).
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

/// Environment rows (lifecycle before and after an attempt). Assigning an
/// environment to an attempt, leases, the pool and fencing go through
/// [`SlotStore`].
pub trait EnvironmentRepository: Send + Sync {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError>;
    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError>;
    /// Refused when the stored row is terminal or at another epoch than
    /// `env` (a stale copy must never overwrite a reassigned environment),
    /// when it would make the environment `Busy` (only an acquire does) and
    /// when it would change the owner or the fencing.
    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError>;
    fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError>;
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
    /// When the binding stops answering: the invocation's `finished_at` plus
    /// the idempotency retention. `None` while the invocation is in flight
    /// (a binding never expires under a running invocation) or when
    /// retention is unlimited.
    pub expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    /// The invocation was inserted and its key (if any) is now bound to it.
    Inserted,
    /// The key is already bound to another invocation; nothing was inserted.
    Existing(IdempotencyBinding),
}

/// Idempotency keys (docs/threat-model.md §10, PLT-4631).
///
/// A key is only ever bound together with the ledger row of its invocation,
/// in one store mutation, so a request that is rejected before acceptance
/// (400 / 413 / 429) never consumes its key and a replay can never observe a
/// key without its invocation. `(tenant, function, key)` is unique in the
/// store (a primary key in SQLite), so two processes can never bind the same
/// key twice. A binding carries the input digest and, once its invocation is
/// terminal, an expiry after which it no longer answers and can be purged.
pub trait IdempotencyRepository: Send + Sync {
    /// The invocation `key` is bound to, if that invocation exists and the
    /// binding has not expired at `now`.
    fn lookup(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
        now: Timestamp,
    ) -> Result<Option<IdempotencyBinding>, RepoError>;

    /// Atomically insert `invocation` and bind its `idempotency_key`. When
    /// the key is already bound to an existing invocation (and the binding
    /// has not expired at the invocation's `accepted_at`) nothing is written
    /// and that binding is returned instead. A binding whose invocation does
    /// not exist, or that expired, is replaced.
    fn insert_bound(&self, invocation: Invocation) -> Result<IdempotencyOutcome, RepoError>;

    /// Delete bindings that expired at `now`. Returns how many.
    fn purge_expired_idempotency(&self, now: Timestamp) -> Result<usize, RepoError>;
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

/// One store implementing every repository, as the composition root holds it.
pub trait StateStore:
    FunctionRepository
    + RevisionRepository
    + AliasRepository
    + InvocationRepository
    + EnvironmentRepository
    + LogRepository
    + IdempotencyRepository
    + ArtifactOwnerRepository
    + SlotStore
    + ConfigPublicationRepository
    + ObjectReferenceRepository
    + std::fmt::Debug
{
    /// `"sqlite"` or `"memory"`.
    fn backend(&self) -> &'static str;

    /// The database file, for a durable store.
    fn path(&self) -> Option<&Path> {
        None
    }

    /// Make everything written so far durable (a WAL checkpoint for SQLite).
    /// Every committed write is already durable; this only shortens recovery.
    fn flush(&self) -> Result<(), RepoError> {
        Ok(())
    }

    /// Replace inline outputs whose retention has passed by their digest.
    /// Returns how many invocations were rewritten.
    fn purge_expired_outputs(&self, _now: Timestamp) -> Result<usize, RepoError> {
        Ok(0)
    }
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
    /// Cell-local slots, leases, pool and dispatchers (PLT-4631).
    pub slots: Arc<dyn SlotStore>,
}

impl Repositories {
    pub fn from_store(store: Arc<dyn StateStore>) -> Self {
        Self {
            functions: store.clone(),
            revisions: store.clone(),
            aliases: store.clone(),
            invocations: store.clone(),
            environments: store.clone(),
            logs: store.clone(),
            idempotency: store.clone(),
            artifact_owners: store.clone(),
            slots: store,
        }
    }

    pub fn in_memory(store: Arc<InMemoryStore>) -> Self {
        Self::from_store(store)
    }
}

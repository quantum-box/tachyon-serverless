//! Volatile store for tests: everything behind one `parking_lot::RwLock`,
//! nothing on disk. It enforces the same row invariants as [`super::SqliteStore`]
//! ([`super::guard`]) and passes the same contract suite.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use parking_lot::RwLock;

use tachyon_serverless_domain::{
    AliasName, AttemptId, DispatcherId, EnvironmentId, EnvironmentState, ExecutionEnvironment,
    ExecutionLease, Function, FunctionAlias, FunctionId, FunctionName, FunctionRevision,
    Invocation, InvocationAttempt, InvocationId, LeaseId, Limits, LogRecord, ReuseKey, RevisionId,
    Sha256Digest, TenantId, Timestamp,
};

use super::guard::{self, Write};
use super::logs::LogBuffer;
use super::restart::{self, Cause};
use super::slot::{
    AcquireOutcome, CompletionOutcome, DispatcherRecord, HeartbeatOutcome, ReclaimReport,
    ReclaimRequest, SlotAcquire, SlotCompletion, SlotStore, acquire_preconditions,
    lease_is_current,
};
use super::{
    AliasRepository, AppendOutcome, ArtifactOwnerRepository, EnvironmentRepository,
    FunctionRepository, IdempotencyBinding, IdempotencyOutcome, IdempotencyRepository,
    InvocationRepository, LogQuery, LogRepository, PoolLimits, RepoError, RevisionRepository,
    StateStore,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct IdempotencyKey {
    tenant_id: TenantId,
    function_id: FunctionId,
    key: String,
}

#[derive(Debug, Default)]
struct State {
    functions: BTreeMap<FunctionId, Function>,
    revisions: BTreeMap<RevisionId, FunctionRevision>,
    revision_counters: BTreeMap<FunctionId, u64>,
    aliases: BTreeMap<(FunctionId, AliasName), FunctionAlias>,
    invocations: BTreeMap<InvocationId, Invocation>,
    attempts: BTreeMap<AttemptId, InvocationAttempt>,
    environments: BTreeMap<EnvironmentId, ExecutionEnvironment>,
    idempotency: HashMap<IdempotencyKey, IdempotencyBinding>,
    artifact_owners: BTreeMap<Sha256Digest, BTreeSet<TenantId>>,
    leases: BTreeMap<LeaseId, ExecutionLease>,
    dispatchers: BTreeMap<DispatcherId, DispatcherRecord>,
    publication: BTreeMap<String, super::config::PublishedRow>,
    publication_generation: u64,
    /// object id -> referencing invocations (PLT-4638).
    object_refs: BTreeMap<String, BTreeSet<InvocationId>>,
    /// object id -> collection reason.
    object_tombstones: BTreeMap<String, (super::objects::CollectReason, Timestamp)>,
}

/// Single-process volatile store. Cheap to clone via `Arc`.
#[derive(Debug)]
pub struct InMemoryStore {
    state: RwLock<State>,
    logs: LogBuffer,
    limits: Limits,
    idempotency_retention: Option<chrono::Duration>,
}

impl InMemoryStore {
    pub fn new(limits: Limits) -> Self {
        Self {
            state: RwLock::new(State::default()),
            logs: LogBuffer::default(),
            limits,
            idempotency_retention: None,
        }
    }

    /// How long an idempotency key keeps answering after its invocation
    /// finished (`None`: as long as the store lives).
    pub fn with_idempotency_retention(mut self, retention: Option<chrono::Duration>) -> Self {
        self.idempotency_retention = retention;
        self
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

impl StateStore for InMemoryStore {
    fn backend(&self) -> &'static str {
        "memory"
    }
}

impl super::config::ConfigPublicationRepository for InMemoryStore {
    fn stamp_config(
        &self,
        observe: super::config::Observe<'_>,
        since: u64,
    ) -> Result<super::config::StampedConfig, RepoError> {
        // The write lock plays the role of the SQLite transaction: the read
        // of the rows and the stamp cannot interleave with another stamp.
        let mut s = self.state.write();
        let rows = super::config::ConfigRows {
            functions: s.functions.values().cloned().collect(),
            aliases: s.aliases.values().cloned().collect(),
            revisions: s.revisions.values().cloned().collect(),
        };
        let observed = observe(rows)?;
        let State {
            publication,
            publication_generation,
            ..
        } = &mut *s;
        super::config::stamp(publication, publication_generation, observed);
        Ok(super::config::StampedConfig {
            generation: *publication_generation,
            entries: super::config::above(publication, since),
        })
    }
}

fn duplicate(kind: &str, id: impl std::fmt::Display) -> RepoError {
    RepoError::Conflict(format!("{kind} {id} already exists"))
}

impl FunctionRepository for InMemoryStore {
    fn insert(&self, function: Function) -> Result<(), RepoError> {
        let mut s = self.state.write();
        if s.functions.contains_key(&function.id) {
            return Err(duplicate("function", &function.id));
        }
        let dup = s.functions.values().any(|f| {
            f.tenant_id == function.tenant_id && f.name == function.name && !f.is_deleted()
        });
        if dup && !function.is_deleted() {
            return Err(RepoError::Conflict(format!(
                "function name `{}` already exists",
                function.name
            )));
        }
        s.functions.insert(function.id.clone(), function);
        Ok(())
    }

    fn get(&self, id: &FunctionId) -> Result<Option<Function>, RepoError> {
        Ok(self.state.read().functions.get(id).cloned())
    }

    fn find_by_name(
        &self,
        tenant: &TenantId,
        name: &FunctionName,
    ) -> Result<Option<Function>, RepoError> {
        Ok(self
            .state
            .read()
            .functions
            .values()
            .find(|f| &f.tenant_id == tenant && &f.name == name && !f.is_deleted())
            .cloned())
    }

    fn list(&self, tenant: &TenantId) -> Result<Vec<Function>, RepoError> {
        let mut v: Vec<Function> = self
            .state
            .read()
            .functions
            .values()
            .filter(|f| &f.tenant_id == tenant)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        Ok(v)
    }

    fn update(&self, function: Function) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let Some(slot) = s.functions.get_mut(&function.id) else {
            return Err(RepoError::NotFound(format!("function {}", function.id)));
        };
        if guard::function_update(slot, &function)? == Write::Apply {
            *slot = function;
        }
        Ok(())
    }
}

impl RevisionRepository for InMemoryStore {
    fn allocate_number(&self, function: &FunctionId) -> Result<u64, RepoError> {
        let mut s = self.state.write();
        let c = s.revision_counters.entry(function.clone()).or_insert(0);
        *c += 1;
        Ok(*c)
    }

    fn insert(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        let mut s = self.state.write();
        if s.revisions.contains_key(&revision.id) {
            return Err(duplicate("revision", &revision.id));
        }
        guard::revision_insert(s.functions.get(&revision.function_id), &revision)?;
        if s.revisions
            .values()
            .any(|r| r.function_id == revision.function_id && r.number == revision.number)
        {
            return Err(RepoError::Conflict(format!(
                "revision number {} already exists for function {}",
                revision.number, revision.function_id
            )));
        }
        s.revisions.insert(revision.id.clone(), revision);
        Ok(())
    }

    fn get(&self, id: &RevisionId) -> Result<Option<FunctionRevision>, RepoError> {
        Ok(self.state.read().revisions.get(id).cloned())
    }

    fn list_by_function(&self, function: &FunctionId) -> Result<Vec<FunctionRevision>, RepoError> {
        let mut v: Vec<FunctionRevision> = self
            .state
            .read()
            .revisions
            .values()
            .filter(|r| &r.function_id == function)
            .cloned()
            .collect();
        v.sort_by_key(|r| r.number);
        Ok(v)
    }

    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let Some(slot) = s.revisions.get_mut(&revision.id) else {
            return Err(RepoError::NotFound(format!("revision {}", revision.id)));
        };
        if guard::revision_update(slot, &revision)? == Write::Apply {
            *slot = revision;
        }
        Ok(())
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
            .aliases
            .get(&(function.clone(), name.clone()))
            .cloned())
    }

    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError> {
        let mut v: Vec<FunctionAlias> = self
            .state
            .read()
            .aliases
            .values()
            .filter(|a| &a.function_id == function)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    fn insert(&self, alias: FunctionAlias) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let key = (alias.function_id.clone(), alias.name.clone());
        if s.aliases.contains_key(&key) {
            return Err(RepoError::Conflict(format!(
                "alias `{}` already exists",
                alias.name
            )));
        }
        guard::alias_target(
            s.functions.get(&alias.function_id),
            s.revisions.get(&alias.revision_id),
            &alias,
        )?;
        s.aliases.insert(key, alias);
        Ok(())
    }

    fn compare_and_set(
        &self,
        alias: FunctionAlias,
        expected_generation: u64,
    ) -> Result<bool, RepoError> {
        let mut s = self.state.write();
        let key = (alias.function_id.clone(), alias.name.clone());
        let Some(current) = s.aliases.get(&key) else {
            return Err(RepoError::NotFound(format!("alias `{}`", alias.name)));
        };
        if current.generation != expected_generation {
            return Ok(false);
        }
        guard::alias_update(current, &alias)?;
        guard::alias_target(
            s.functions.get(&alias.function_id),
            s.revisions.get(&alias.revision_id),
            &alias,
        )?;
        s.aliases.insert(key, alias);
        Ok(true)
    }
}

impl InvocationRepository for InMemoryStore {
    fn insert(&self, invocation: Invocation) -> Result<(), RepoError> {
        let mut s = self.state.write();
        insert_invocation(&mut s, invocation, self.limits.max_response_bytes)
    }

    fn get(&self, id: &InvocationId) -> Result<Option<Invocation>, RepoError> {
        Ok(self.state.read().invocations.get(id).cloned())
    }

    fn update(&self, invocation: Invocation) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let Some(slot) = s.invocations.get_mut(&invocation.id) else {
            return Err(RepoError::NotFound(format!("invocation {}", invocation.id)));
        };
        if guard::invocation_update(slot, &invocation, self.limits.max_response_bytes)?
            == Write::Apply
        {
            *slot = invocation.clone();
            set_binding_expiry(&mut s, &invocation, self.idempotency_retention);
        }
        Ok(())
    }

    fn list_by_function(
        &self,
        function: &FunctionId,
        limit: usize,
    ) -> Result<Vec<Invocation>, RepoError> {
        let mut v: Vec<Invocation> = self
            .state
            .read()
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
        let mut s = self.state.write();
        if s.attempts.contains_key(&attempt.id) {
            return Err(duplicate("attempt", &attempt.id));
        }
        guard::attempt_insert(s.invocations.get(&attempt.invocation_id), &attempt)?;
        s.attempts.insert(attempt.id.clone(), attempt);
        Ok(())
    }

    fn get_attempt(&self, id: &AttemptId) -> Result<Option<InvocationAttempt>, RepoError> {
        Ok(self.state.read().attempts.get(id).cloned())
    }

    fn update_attempt(&self, attempt: InvocationAttempt) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let Some(slot) = s.attempts.get_mut(&attempt.id) else {
            return Err(RepoError::NotFound(format!("attempt {}", attempt.id)));
        };
        if guard::attempt_update(slot, &attempt)? == Write::Apply {
            *slot = attempt;
        }
        Ok(())
    }

    fn attempts_of(&self, invocation: &InvocationId) -> Result<Vec<InvocationAttempt>, RepoError> {
        let mut v: Vec<InvocationAttempt> = self
            .state
            .read()
            .attempts
            .values()
            .filter(|a| &a.invocation_id == invocation)
            .cloned()
            .collect();
        v.sort_by_key(|a| a.number);
        Ok(v)
    }
}

/// Once `inv` is terminal, its bindings expire `retention` after it finished.
fn set_binding_expiry(s: &mut State, inv: &Invocation, retention: Option<chrono::Duration>) {
    if inv.idempotency_key.is_none() || !inv.status.is_terminal() {
        return;
    }
    let expires_at = retention.map(|r| inv.finished_at.unwrap_or(inv.accepted_at) + r);
    for binding in s.idempotency.values_mut() {
        if binding.invocation_id == inv.id {
            binding.expires_at = expires_at;
        }
    }
}

fn insert_invocation(
    s: &mut State,
    invocation: Invocation,
    max_inline_bytes: u64,
) -> Result<(), RepoError> {
    if s.invocations.contains_key(&invocation.id) {
        return Err(duplicate("invocation", &invocation.id));
    }
    guard::invocation_insert(
        s.functions.get(&invocation.function_id),
        &invocation,
        max_inline_bytes,
    )?;
    s.invocations.insert(invocation.id.clone(), invocation);
    Ok(())
}

impl EnvironmentRepository for InMemoryStore {
    fn insert(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        let mut s = self.state.write();
        if s.environments.contains_key(&env.id) {
            return Err(duplicate("environment", &env.id));
        }
        guard::environment_insert(s.revisions.get(&env.revision_id), &env)?;
        s.environments.insert(env.id.clone(), env);
        Ok(())
    }

    fn get(&self, id: &EnvironmentId) -> Result<Option<ExecutionEnvironment>, RepoError> {
        Ok(self.state.read().environments.get(id).cloned())
    }

    fn update(&self, env: ExecutionEnvironment) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let Some(slot) = s.environments.get_mut(&env.id) else {
            return Err(RepoError::NotFound(format!("environment {}", env.id)));
        };
        if guard::environment_update(slot, &env)? == Write::Apply {
            *slot = env;
        }
        Ok(())
    }

    fn list_active(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        Ok(self
            .state
            .read()
            .environments
            .values()
            .filter(|e| !e.is_terminal())
            .cloned()
            .collect())
    }
}

impl LogRepository for InMemoryStore {
    fn append(&self, record: LogRecord) -> AppendOutcome {
        self.logs.append(&self.limits, record)
    }

    fn query(&self, invocation: &InvocationId) -> LogQuery {
        self.logs.query(invocation)
    }
}

fn live_binding(s: &State, key: &IdempotencyKey, now: Timestamp) -> Option<IdempotencyBinding> {
    s.idempotency
        .get(key)
        .filter(|b| s.invocations.contains_key(&b.invocation_id))
        .filter(|b| b.expires_at.is_none_or(|e| e > now))
        .cloned()
}

impl IdempotencyRepository for InMemoryStore {
    fn lookup(
        &self,
        tenant: &TenantId,
        function: &FunctionId,
        key: &str,
        now: Timestamp,
    ) -> Result<Option<IdempotencyBinding>, RepoError> {
        let k = IdempotencyKey {
            tenant_id: tenant.clone(),
            function_id: function.clone(),
            key: key.to_string(),
        };
        Ok(live_binding(&self.state.read(), &k, now))
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
        let mut s = self.state.write();
        if let Some(k) = &key
            && let Some(existing) = live_binding(&s, k, invocation.accepted_at)
        {
            return Ok(IdempotencyOutcome::Existing(existing));
        }
        let binding = IdempotencyBinding {
            invocation_id: invocation.id.clone(),
            input_digest: invocation.input_digest.clone(),
            expires_at: None,
        };
        let inserted = invocation.clone();
        insert_invocation(&mut s, invocation, self.limits.max_response_bytes)?;
        if let Some(k) = key {
            s.idempotency.insert(k, binding);
            set_binding_expiry(&mut s, &inserted, self.idempotency_retention);
        }
        Ok(IdempotencyOutcome::Inserted)
    }

    fn purge_expired_idempotency(&self, now: Timestamp) -> Result<usize, RepoError> {
        let mut s = self.state.write();
        let before = s.idempotency.len();
        s.idempotency
            .retain(|_, b| b.expires_at.is_none_or(|e| e > now));
        Ok(before - s.idempotency.len())
    }
}

impl ArtifactOwnerRepository for InMemoryStore {
    fn claim(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<(), RepoError> {
        self.state
            .write()
            .artifact_owners
            .entry(digest.clone())
            .or_default()
            .insert(tenant.clone());
        Ok(())
    }

    fn is_owned_by(&self, tenant: &TenantId, digest: &Sha256Digest) -> Result<bool, RepoError> {
        Ok(self
            .state
            .read()
            .artifact_owners
            .get(digest)
            .is_some_and(|owners| owners.contains(tenant)))
    }
}

// ---------------------------------------------------------------------------
// slots (PLT-4631): the same semantics as `sqlite/slot.rs`, under one lock
// ---------------------------------------------------------------------------

fn has_unreleased_lease(s: &State, env: &EnvironmentId) -> bool {
    s.leases
        .values()
        .any(|l| &l.environment_id == env && l.released_at.is_none())
}

fn fence_env(s: &mut State, id: &EnvironmentId, now: Timestamp, report: &mut ReclaimReport) {
    if let Some(env) = s.environments.get_mut(id)
        && !env.is_terminal()
        && !env.is_fenced()
        && env.fence(now).is_ok()
    {
        report.fenced.push(env.clone());
    }
}

fn settle_invocation_rows(
    s: &mut State,
    id: &InvocationId,
    cause: Cause,
    retention: Option<chrono::Duration>,
    now: Timestamp,
    report: &mut ReclaimReport,
) {
    let Some(mut inv) = s.invocations.get(id).cloned() else {
        return;
    };
    if !restart::survives_dispatcher(&inv) && restart::settle_invocation_with(&mut inv, cause, now)
    {
        s.invocations.insert(inv.id.clone(), inv.clone());
        set_binding_expiry(s, &inv, retention);
        report.invocations += 1;
    }
    let unknown = restart::attempts_unknown(&inv);
    for att in s.attempts.values_mut() {
        if att.invocation_id == inv.id && restart::settle_attempt_with(att, unknown, cause, now) {
            report.attempts += 1;
        }
    }
}

fn cause_of(d: &DispatcherRecord, proven_dead: bool) -> Cause {
    if proven_dead || d.stopped_at.is_some() {
        Cause::RESTARTED
    } else {
        Cause::LEASE_EXPIRED
    }
}

impl SlotStore for InMemoryStore {
    fn register_dispatcher(&self, record: DispatcherRecord) -> Result<(), RepoError> {
        let mut s = self.state.write();
        if s.dispatchers.contains_key(&record.id) {
            return Err(duplicate("dispatcher", &record.id));
        }
        s.dispatchers.insert(record.id.clone(), record);
        Ok(())
    }

    fn get_dispatcher(&self, id: &DispatcherId) -> Result<Option<DispatcherRecord>, RepoError> {
        Ok(self.state.read().dispatchers.get(id).cloned())
    }

    fn list_dispatchers(&self) -> Result<Vec<DispatcherRecord>, RepoError> {
        Ok(self.state.read().dispatchers.values().cloned().collect())
    }

    fn heartbeat(
        &self,
        id: &DispatcherId,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<HeartbeatOutcome, RepoError> {
        let mut s = self.state.write();
        let Some(d) = s.dispatchers.get_mut(id) else {
            return Ok(HeartbeatOutcome::Fenced);
        };
        if !d.is_live() || now >= d.lease_expires_at {
            return Ok(HeartbeatOutcome::Fenced);
        }
        d.heartbeat_at = now;
        d.lease_expires_at = d.lease_expires_at.max(now + ttl);
        let mut renewed = 0;
        for lease in s.leases.values_mut() {
            if lease.owner.as_ref() == Some(id)
                && lease.released_at.is_none()
                && lease.renew(now, ttl).is_ok()
            {
                renewed += 1;
            }
        }
        Ok(HeartbeatOutcome::Renewed { leases: renewed })
    }

    fn stop_dispatcher(&self, id: &DispatcherId, now: Timestamp) -> Result<(), RepoError> {
        if let Some(d) = self.state.write().dispatchers.get_mut(id)
            && d.stopped_at.is_none()
        {
            d.stopped_at = Some(now);
        }
        Ok(())
    }

    fn list_idle(
        &self,
        owner: Option<&DispatcherId>,
    ) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        let mut v: Vec<ExecutionEnvironment> = self
            .state
            .read()
            .environments
            .values()
            .filter(|e| matches!(e.state, EnvironmentState::Idle) && e.owner.as_ref() == owner)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.idle_since.cmp(&b.idle_since).then(a.id.cmp(&b.id)));
        Ok(v)
    }

    fn claim_for_reuse(
        &self,
        key: &ReuseKey,
        owner: Option<&DispatcherId>,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError> {
        let mut s = self.state.write();
        // Pool membership is exactly `Idle` (see the SQLite store). `values()`
        // is ordered by id (a ULID), so the oldest match wins.
        let Some(env) = s.environments.values_mut().find(|e| {
            matches!(e.state, EnvironmentState::Idle)
                && &e.reuse_key == key
                && e.owner.as_ref() == owner
        }) else {
            return Ok(None);
        };
        let mut claimed = env.clone();
        if claimed.reserve(now).is_err() {
            return Ok(None);
        }
        *env = claimed.clone();
        Ok(Some(claimed))
    }

    fn release_to_pool(
        &self,
        env: &ExecutionEnvironment,
        limits: PoolLimits,
        now: Timestamp,
    ) -> Result<Option<ExecutionEnvironment>, RepoError> {
        let mut s = self.state.write();
        let Some(current) = s.environments.get(&env.id) else {
            return Ok(None);
        };
        // A `min_ready` pre-start (PLT-4635) is published straight from
        // `Ready` at epoch 0: it never served an attempt.
        let prestarted = matches!(current.state, EnvironmentState::Ready) && current.epoch == 0;
        if current.epoch != env.epoch
            || !(matches!(current.state, EnvironmentState::Busy) || prestarted)
            || current.is_fenced()
            || has_unreleased_lease(&s, &env.id)
        {
            return Ok(None);
        }
        let is_idle = |e: &&ExecutionEnvironment| matches!(e.state, EnvironmentState::Idle);
        let total = s.environments.values().filter(is_idle).count();
        let per_key = s
            .environments
            .values()
            .filter(is_idle)
            .filter(|e| e.reuse_key == env.reuse_key)
            .count();
        if total >= limits.max_total_idle || per_key >= limits.max_idle_per_key {
            return Ok(None);
        }
        let mut pooled = env.clone();
        if matches!(pooled.state, EnvironmentState::Ready) && pooled.mark_busy(now).is_err() {
            return Ok(None);
        }
        if pooled.mark_idle(now).is_err() {
            return Ok(None);
        }
        guard::environment_update(current, &pooled)?;
        s.environments.insert(pooled.id.clone(), pooled.clone());
        Ok(Some(pooled))
    }

    fn take_idle_for_termination(
        &self,
        id: &EnvironmentId,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        let mut s = self.state.write();
        Ok(match s.environments.get_mut(id) {
            Some(env) if matches!(env.state, EnvironmentState::Idle) => {
                env.mark_draining(now).is_ok()
            }
            _ => false,
        })
    }

    fn acquire(&self, request: SlotAcquire) -> Result<AcquireOutcome, RepoError> {
        let mut s = self.state.write();
        let stored = s.environments.get(&request.env.id).cloned();
        if let Some(reason) = acquire_preconditions(&request, stored.as_ref())? {
            return Ok(AcquireOutcome::Lost(reason));
        }
        let Some(stored) = stored else {
            return Err(RepoError::NotFound(format!(
                "environment {}",
                request.env.id
            )));
        };
        let SlotAcquire {
            env,
            lease,
            attempt,
            invocation,
            ..
        } = request;
        let owner = lease.owner.clone().expect("checked by the preconditions");
        match s.dispatchers.get(&owner) {
            Some(d) if d.is_live() => {}
            Some(_) => {
                return Ok(AcquireOutcome::Lost(format!(
                    "dispatcher {owner} is stopped or was reclaimed"
                )));
            }
            None => {
                return Ok(AcquireOutcome::Lost(format!(
                    "dispatcher {owner} is not registered"
                )));
            }
        }
        if has_unreleased_lease(&s, &env.id) {
            return Ok(AcquireOutcome::Lost(format!(
                "environment {} already has an unreleased lease",
                env.id
            )));
        }
        if let Some(inv) = &invocation {
            let Some(old) = s.invocations.get(&inv.id) else {
                return Err(RepoError::NotFound(format!("invocation {}", inv.id)));
            };
            if old.status.is_terminal() {
                return Ok(AcquireOutcome::Lost(format!(
                    "invocation {} is already {}",
                    inv.id,
                    old.status.name()
                )));
            }
            guard::invocation_update(old, inv, self.limits.max_response_bytes)?;
        }
        if s.attempts.contains_key(&attempt.id) {
            return Err(duplicate("attempt", &attempt.id));
        }
        if s.leases.contains_key(&lease.id) {
            return Err(duplicate("lease", &lease.id));
        }
        guard::attempt_insert(s.invocations.get(&attempt.invocation_id), &attempt)?;
        guard::lease_insert(Some(&stored), &lease)?;
        s.environments.insert(env.id.clone(), env);
        s.leases.insert(lease.id.clone(), lease);
        s.attempts.insert(attempt.id.clone(), attempt);
        if let Some(inv) = invocation {
            s.invocations.insert(inv.id.clone(), inv);
        }
        Ok(AcquireOutcome::Acquired)
    }

    fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError> {
        Ok(self.state.read().leases.get(id).cloned())
    }

    fn renew_lease(
        &self,
        id: &LeaseId,
        owner: &DispatcherId,
        epoch: u64,
        ttl: chrono::Duration,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        let mut s = self.state.write();
        let Some(lease) = s.leases.get_mut(id) else {
            return Ok(false);
        };
        if lease.owner.as_ref() != Some(owner) || lease.epoch != epoch {
            return Ok(false);
        }
        Ok(lease.renew(now, ttl).is_ok())
    }

    fn complete(&self, completion: SlotCompletion) -> Result<CompletionOutcome, RepoError> {
        let mut s = self.state.write();
        let SlotCompletion {
            lease_id,
            attempt,
            invocation,
            now,
        } = completion;
        let lease = s.leases.get(&lease_id).cloned();
        let env = lease
            .as_ref()
            .and_then(|l| s.environments.get(&l.environment_id))
            .cloned();
        if let Some(reason) =
            lease_is_current(lease.as_ref(), &attempt.id, attempt.epoch, env.as_ref())
        {
            return Ok(CompletionOutcome::Stale(reason));
        }
        let Some(old_attempt) = s.attempts.get(&attempt.id) else {
            return Err(RepoError::NotFound(format!("attempt {}", attempt.id)));
        };
        if old_attempt.status.is_terminal() {
            return Ok(CompletionOutcome::Stale(format!(
                "attempt {} is already settled",
                attempt.id
            )));
        }
        let attempt_write = guard::attempt_update(old_attempt, &attempt)?;
        let mut invocation_write = None;
        if let Some(inv) = &invocation {
            let Some(old) = s.invocations.get(&inv.id) else {
                return Err(RepoError::NotFound(format!("invocation {}", inv.id)));
            };
            if old.status.is_terminal() {
                return Ok(CompletionOutcome::Stale(format!(
                    "invocation {} is already {}",
                    inv.id,
                    old.status.name()
                )));
            }
            invocation_write = Some(guard::invocation_update(
                old,
                inv,
                self.limits.max_response_bytes,
            )?);
        }
        let mut lease = lease.expect("checked by lease_is_current");
        lease
            .release(now)
            .map_err(|e| RepoError::Refused(e.to_string()))?;
        s.leases.insert(lease.id.clone(), lease);
        if attempt_write == Write::Apply {
            s.attempts.insert(attempt.id.clone(), attempt);
        }
        if let (Some(inv), Some(Write::Apply)) = (invocation, invocation_write) {
            s.invocations.insert(inv.id.clone(), inv.clone());
            set_binding_expiry(&mut s, &inv, self.idempotency_retention);
        }
        Ok(CompletionOutcome::Accepted)
    }

    fn release_lease(
        &self,
        id: &LeaseId,
        attempt: &AttemptId,
        epoch: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        let mut s = self.state.write();
        let lease = s.leases.get(id).cloned();
        let env = lease
            .as_ref()
            .and_then(|l| s.environments.get(&l.environment_id))
            .cloned();
        if lease_is_current(lease.as_ref(), attempt, epoch, env.as_ref()).is_some() {
            return Ok(false);
        }
        Ok(s.leases.get_mut(id).is_some_and(|l| l.release(now).is_ok()))
    }

    fn reclaim_expired(&self, request: ReclaimRequest) -> Result<ReclaimReport, RepoError> {
        let ReclaimRequest {
            reclaimer,
            now,
            skew,
            presumed_dead,
        } = request;
        let retention = self.idempotency_retention;
        let mut s = self.state.write();
        let mut report = ReclaimReport::default();

        let mut dead: BTreeMap<DispatcherId, Cause> = BTreeMap::new();
        for d in s.dispatchers.values_mut() {
            if d.id == reclaimer {
                continue;
            }
            if d.reclaimed_at.is_some() {
                dead.insert(d.id.clone(), cause_of(d, false));
                continue;
            }
            let proven = presumed_dead.contains(&d.id);
            if d.stopped_at.is_none() && !proven && !d.is_expired(now, skew) {
                continue;
            }
            d.reclaimed_at = Some(now);
            report.dispatchers.push(d.id.clone());
            dead.insert(d.id.clone(), cause_of(d, proven));
        }

        let expired: Vec<(ExecutionLease, Cause)> = s
            .leases
            .values()
            .filter(|l| l.released_at.is_none())
            .filter_map(|l| {
                let owner = l.owner.as_ref()?;
                if owner == &reclaimer {
                    return None;
                }
                match dead.get(owner) {
                    Some(cause) => Some((l.clone(), *cause)),
                    None if l.is_expired(now, skew) => Some((l.clone(), Cause::LEASE_EXPIRED)),
                    None => None,
                }
            })
            .collect();
        for (mut lease, cause) in expired {
            if lease.release(now).is_err() {
                continue;
            }
            s.leases.insert(lease.id.clone(), lease.clone());
            report.leases += 1;
            if let Some(att) = s.attempts.get_mut(&lease.attempt_id) {
                if restart::settle_attempt_with(att, true, cause, now) {
                    report.attempts += 1;
                }
                let invocation_id = att.invocation_id.clone();
                if s.invocations.get(&invocation_id).is_some_and(|inv| {
                    !inv.status.is_terminal() && inv.attempt_ids.last() == Some(&lease.attempt_id)
                }) {
                    settle_invocation_rows(
                        &mut s,
                        &invocation_id,
                        cause,
                        retention,
                        now,
                        &mut report,
                    );
                }
            }
            if s.environments
                .get(&lease.environment_id)
                .is_some_and(|e| e.epoch == lease.epoch)
            {
                fence_env(&mut s, &lease.environment_id, now, &mut report);
            }
        }

        for (owner, cause) in &dead {
            let invocations: Vec<InvocationId> = s
                .invocations
                .values()
                .filter(|i| i.dispatcher_id.as_ref() == Some(owner) && !i.status.is_terminal())
                .map(|i| i.id.clone())
                .collect();
            for id in invocations {
                settle_invocation_rows(&mut s, &id, *cause, retention, now, &mut report);
            }
            let envs: Vec<EnvironmentId> = s
                .environments
                .values()
                .filter(|e| e.owner.as_ref() == Some(owner) && !e.is_terminal() && !e.is_fenced())
                .map(|e| e.id.clone())
                .collect();
            for id in envs {
                fence_env(&mut s, &id, now, &mut report);
            }
        }
        Ok(report)
    }

    fn list_fenced(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        Ok(self
            .state
            .read()
            .environments
            .values()
            .filter(|e| e.is_fenced() && !e.is_terminal())
            .cloned()
            .collect())
    }

    fn confirm_terminated(
        &self,
        id: &EnvironmentId,
        epoch: u64,
        now: Timestamp,
    ) -> Result<bool, RepoError> {
        let mut s = self.state.write();
        let Some(env) = s.environments.get_mut(id) else {
            return Ok(false);
        };
        if !env.is_fenced() || env.is_terminal() || env.epoch != epoch {
            return Ok(false);
        }
        Ok(env.mark_lost(FENCED_TERMINATED, now).is_ok())
    }
}

/// Reason recorded on a fenced environment once its terminate is confirmed.
const FENCED_TERMINATED: &str = "owner lost its lease; environment fenced and terminate confirmed";

impl super::objects::ObjectReferenceRepository for InMemoryStore {
    fn attach_object(
        &self,
        object: &tachyon_serverless_durable_port::ObjectRef,
        invocation: &InvocationId,
        now: Timestamp,
    ) -> Result<(), RepoError> {
        super::objects::check_attachable(&object.id, now)?;
        let mut s = self.state.write();
        let inv = s
            .invocations
            .get(invocation)
            .ok_or_else(|| RepoError::NotFound(format!("invocation {invocation}")))?;
        if inv.tenant_id != object.scope.tenant_id {
            return Err(RepoError::Refused(format!(
                "object {} belongs to another tenant than invocation {invocation}",
                object.id
            )));
        }
        if let Some((reason, _)) = s.object_tombstones.get(object.id.as_str()) {
            return Err(RepoError::Refused(format!(
                "object {} is being collected ({}); store it again",
                object.id,
                reason.as_str()
            )));
        }
        s.object_refs
            .entry(object.id.as_str().to_string())
            .or_default()
            .insert(invocation.clone());
        Ok(())
    }

    fn object_references(
        &self,
        object: &tachyon_serverless_durable_port::ObjectId,
    ) -> Result<Vec<InvocationId>, RepoError> {
        Ok(self
            .state
            .read()
            .object_refs
            .get(object.as_str())
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default())
    }

    fn claim_for_collection(
        &self,
        object: &tachyon_serverless_durable_port::ObjectRef,
        reason: super::objects::CollectReason,
        now: Timestamp,
    ) -> Result<super::objects::CollectDecision, RepoError> {
        use super::objects::{CollectDecision, CollectReason};
        let mut s = self.state.write();
        if s.object_tombstones.contains_key(object.id.as_str()) {
            return Ok(CollectDecision::Collect);
        }
        let refs = s
            .object_refs
            .get(object.id.as_str())
            .cloned()
            .unwrap_or_default();
        let live = refs
            .iter()
            .filter(|id| {
                s.invocations
                    .get(*id)
                    .is_none_or(|inv| !inv.status.is_terminal())
            })
            .count();
        if live > 0 {
            return Ok(CollectDecision::InUse { invocations: live });
        }
        if reason == CollectReason::Orphan && !refs.is_empty() {
            return Ok(CollectDecision::Referenced);
        }
        s.object_tombstones
            .insert(object.id.as_str().to_string(), (reason, now));
        Ok(CollectDecision::Collect)
    }

    fn forget_object(
        &self,
        object: &tachyon_serverless_durable_port::ObjectId,
    ) -> Result<(), RepoError> {
        let mut s = self.state.write();
        s.object_refs.remove(object.as_str());
        Ok(())
    }

    fn purge_tombstones(&self, before: Timestamp) -> Result<usize, RepoError> {
        let mut s = self.state.write();
        let n = s.object_tombstones.len();
        s.object_tombstones.retain(|_, (_, at)| *at >= before);
        Ok(n - s.object_tombstones.len())
    }
}

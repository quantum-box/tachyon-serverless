//! Volatile store for tests: everything behind one `parking_lot::RwLock`,
//! nothing on disk. It enforces the same row invariants as [`super::SqliteStore`]
//! ([`super::guard`]) and passes the same contract suite.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use parking_lot::RwLock;

use tachyon_serverless_domain::{
    AliasName, AttemptId, EnvironmentId, EnvironmentState, ExecutionEnvironment, ExecutionLease,
    Function, FunctionAlias, FunctionId, FunctionName, FunctionRevision, Invocation,
    InvocationAttempt, InvocationId, LeaseId, Limits, LogRecord, ReuseKey, RevisionId,
    Sha256Digest, TenantId, Timestamp,
};

use super::guard::{self, Write};
use super::logs::LogBuffer;
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
    leases: HashMap<LeaseId, ExecutionLease>,
}

/// Single-process volatile store. Cheap to clone via `Arc`.
#[derive(Debug)]
pub struct InMemoryStore {
    state: RwLock<State>,
    logs: LogBuffer,
    limits: Limits,
}

impl InMemoryStore {
    pub fn new(limits: Limits) -> Self {
        Self {
            state: RwLock::new(State::default()),
            logs: LogBuffer::default(),
            limits,
        }
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
            *slot = invocation;
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

    fn list_idle(&self) -> Result<Vec<ExecutionEnvironment>, RepoError> {
        let mut v: Vec<ExecutionEnvironment> = self
            .state
            .read()
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
        let mut s = self.state.write();
        // Pool membership is exactly `Idle`: `release_to_pool` is the only
        // way in and `list_idle` the only way to enumerate it. A `Ready` row
        // belongs to the cold start that created it and is about to dispatch
        // into it, so it is never a candidate here even though the domain
        // would allow the transition. `values()` is ordered by id (a ULID),
        // so the oldest match wins.
        let Some(id) = s
            .environments
            .values()
            .find(|e| matches!(e.state, EnvironmentState::Idle) && &e.reuse_key == key)
            .map(|e| e.id.clone())
        else {
            return Ok(None);
        };
        let Some(env) = s.environments.get_mut(&id) else {
            return Ok(None);
        };
        // `reassign` is the domain guard (it refuses anything that is not
        // Ready or Idle) and is what advances the epoch.
        let mut claimed = env.clone();
        if claimed.reassign(now).is_err() {
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
        if current.epoch != env.epoch || !matches!(current.state, EnvironmentState::Busy) {
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

    fn insert_lease(&self, lease: ExecutionLease) -> Result<(), RepoError> {
        let mut s = self.state.write();
        if s.leases.contains_key(&lease.id) {
            return Err(duplicate("lease", &lease.id));
        }
        guard::lease_insert(s.environments.get(&lease.environment_id), &lease)?;
        s.leases.insert(lease.id.clone(), lease);
        Ok(())
    }

    fn get_lease(&self, id: &LeaseId) -> Result<Option<ExecutionLease>, RepoError> {
        Ok(self.state.read().leases.get(id).cloned())
    }

    fn update_lease(&self, lease: ExecutionLease) -> Result<(), RepoError> {
        let mut s = self.state.write();
        let Some(slot) = s.leases.get_mut(&lease.id) else {
            return Err(RepoError::NotFound(format!("lease {}", lease.id)));
        };
        if guard::lease_update(slot, &lease)? == Write::Apply {
            *slot = lease;
        }
        Ok(())
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

fn live_binding(s: &State, key: &IdempotencyKey) -> Option<IdempotencyBinding> {
    s.idempotency
        .get(key)
        .filter(|b| s.invocations.contains_key(&b.invocation_id))
        .cloned()
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
        let mut s = self.state.write();
        if let Some(k) = &key
            && let Some(existing) = live_binding(&s, k)
        {
            return Ok(IdempotencyOutcome::Existing(existing));
        }
        let binding = IdempotencyBinding {
            invocation_id: invocation.id.clone(),
            input_digest: invocation.input_digest.clone(),
        };
        insert_invocation(&mut s, invocation, self.limits.max_response_bytes)?;
        if let Some(k) = key {
            s.idempotency.insert(k, binding);
        }
        Ok(IdempotencyOutcome::Inserted)
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

//! The control-plane side of configuration distribution (PLT-4636).
//!
//! [`ConfigSource`] is the port a data plane's [`super::ConfigCache`] pulls
//! from. Two implementations exist:
//!
//! - [`LedgerConfigSource`] (here): in-process, from the ledger and the
//!   gateway configuration. A `combined` gateway feeds its own cache with it
//!   and serves it on `GET /v1/internal/config` to data planes.
//! - an HTTP client of that endpoint, in `apps/gateway` (the application
//!   crate does not depend on an HTTP stack).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use parking_lot::Mutex;

use tachyon_serverless_domain::{
    AliasName, Function, FunctionAlias, FunctionId, FunctionName, FunctionRevision, RevisionId,
    TenantId,
};

use super::wire::{
    AuthGrant, ConfigDelivery, ConfigEntry, ConfigKey, ConfigPolicy, ConfigValue, TenantGrant,
    grant_key,
};
use crate::config::{ControlPlaneConfig, TokenConfig};
use crate::repository::{
    AliasRepository, ConfigObservation, FunctionRepository, RepoError, RevisionRepository,
    StateStore,
};

/// Why a fetch did not produce a delivery.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    /// The control plane (or its store) did not answer: connection refused,
    /// timeout, 5xx, a store error.
    #[error("control plane unavailable: {0}")]
    Unavailable(String),
    /// It answered, but not with something this data plane may use: bad
    /// credential, malformed body.
    #[error("control plane refused the request: {0}")]
    Rejected(String),
}

#[async_trait]
pub trait ConfigSource: Send + Sync {
    /// For logs and `/readyz`.
    fn describe(&self) -> String;

    /// True when the source reads the authoritative store of this very
    /// process. "Not delivered" then means "does not exist", and the cache
    /// may refresh on the request path because a refresh does not leave the
    /// process.
    fn authoritative(&self) -> bool {
        false
    }

    /// A marker that moves whenever the source's content may have changed
    /// (an authoritative source only). The cache refreshes before answering
    /// when it moved since its last refresh.
    fn change_marker(&self) -> Option<(u64, u64)> {
        None
    }

    /// Every entry with a generation above `since`.
    async fn fetch(&self, since: u64) -> Result<ConfigDelivery, SourceError>;
}

/// Bumped by every function / revision / alias write this process makes.
#[derive(Debug, Default)]
pub struct ConfigChangeSignal(AtomicU64);

impl ConfigChangeSignal {
    pub fn bump(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    pub fn version(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Publishes the ledger's functions, aliases and revisions plus the
/// gateway's `[[identity.tokens]]` and `[control_plane]` policy.
pub struct LedgerConfigSource {
    store: Arc<dyn StateStore>,
    signal: Arc<ConfigChangeSignal>,
    grants: Vec<(ConfigKey, AuthGrant)>,
    policy: ConfigPolicy,
    config_ttl_seconds: u64,
    auth_lease_seconds: u64,
    name: String,
    /// Serializes publications within this process (the store transaction
    /// serializes them across processes).
    publishing: Mutex<()>,
    /// Tenant budgets (PLT-4643). `None`: no budget is published.
    budgets: Option<Arc<crate::budget::BudgetPublisher>>,
}

impl std::fmt::Debug for LedgerConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerConfigSource")
            .field("name", &self.name)
            .field("grants", &self.grants.len())
            .finish_non_exhaustive()
    }
}

impl LedgerConfigSource {
    pub fn new(
        store: Arc<dyn StateStore>,
        signal: Arc<ConfigChangeSignal>,
        tokens: &[TokenConfig],
        control: &ControlPlaneConfig,
        name: String,
    ) -> Self {
        let secret = grant_secret(control);
        let grants = tokens
            .iter()
            .map(|t| {
                (
                    ConfigKey::Grant {
                        token_digest: grant_key(&secret, t.token.expose()),
                    },
                    AuthGrant {
                        subject: t.subject.clone(),
                        tenant_id: t.tenant_id.clone(),
                        roles: t.roles.clone(),
                    },
                )
            })
            .collect();
        Self {
            store,
            signal,
            grants,
            policy: ConfigPolicy {
                allowed_egress: control.allowed_egress.clone(),
            },
            config_ttl_seconds: control.config_ttl_seconds,
            auth_lease_seconds: control.auth_lease_seconds,
            name,
            publishing: Mutex::new(()),
            budgets: None,
        }
    }

    /// Publish tenant budgets with the rest (PLT-4643).
    pub fn with_budgets(mut self, budgets: Arc<crate::budget::BudgetPublisher>) -> Self {
        self.budgets = Some(budgets);
        self
    }

    pub fn budgets(&self) -> Option<&Arc<crate::budget::BudgetPublisher>> {
        self.budgets.as_ref()
    }

    fn observe(&self, rows: crate::repository::ConfigRows) -> Vec<(ConfigKey, ConfigValue)> {
        let mut out = Vec::new();
        for f in rows.functions {
            out.push((
                ConfigKey::Function {
                    function_id: f.id.clone(),
                },
                ConfigValue::Function(f),
            ));
        }
        for a in rows.aliases {
            out.push((
                ConfigKey::Route {
                    function_id: a.function_id.clone(),
                    alias: a.name.clone(),
                },
                ConfigValue::Route(a),
            ));
        }
        for r in rows.revisions {
            out.push((
                ConfigKey::Revision {
                    revision_id: r.id.clone(),
                },
                ConfigValue::Revision(Box::new(r)),
            ));
        }
        let mut tenants = BTreeSet::new();
        for (key, grant) in &self.grants {
            tenants.insert(grant.tenant_id.clone());
            out.push((key.clone(), ConfigValue::Grant(grant.clone())));
        }
        if let Some(budgets) = &self.budgets {
            let known: Vec<TenantId> = tenants.iter().cloned().collect();
            for budget in budgets.budgets(&known) {
                out.push((
                    ConfigKey::Budget {
                        tenant_id: budget.tenant_id.clone(),
                    },
                    ConfigValue::Budget(budget),
                ));
            }
        }
        for tenant_id in tenants {
            out.push((
                ConfigKey::Tenant {
                    tenant_id: tenant_id.clone(),
                },
                ConfigValue::Tenant(TenantGrant { tenant_id }),
            ));
        }
        out.push((ConfigKey::Policy, ConfigValue::Policy(self.policy.clone())));
        out
    }

    /// Stamp and return the entries above `since`.
    pub fn publish(&self, since: u64) -> Result<ConfigDelivery, RepoError> {
        let _serial = self.publishing.lock();
        let mut observe = |rows| -> Result<Vec<ConfigObservation>, RepoError> {
            Ok(self
                .observe(rows)
                .into_iter()
                .map(|(key, value)| ConfigObservation {
                    key: key.storage_key(),
                    natural_version: value.natural_version(),
                    digest: value.digest().to_string(),
                    body: serde_json::to_string(&value).unwrap_or_default(),
                })
                .collect())
        };
        let stamped = self.store.stamp_config(&mut observe, since)?;
        let mut entries = Vec::with_capacity(stamped.entries.len());
        for e in stamped.entries {
            let Some(key) = ConfigKey::from_storage_key(&e.key) else {
                return Err(RepoError::Serialization(format!(
                    "unreadable publication key {}",
                    e.key
                )));
            };
            let value = match e.body {
                Some(body) => Some(
                    serde_json::from_str::<ConfigValue>(&body)
                        .map_err(|err| RepoError::Serialization(err.to_string()))?,
                ),
                None => None,
            };
            entries.push(ConfigEntry {
                key,
                generation: e.generation,
                value,
            });
        }
        Ok(ConfigDelivery {
            source: self.name.clone(),
            generation: stamped.generation,
            since,
            config_ttl_seconds: self.config_ttl_seconds,
            auth_lease_seconds: self.auth_lease_seconds,
            entries,
        })
    }
}

#[async_trait]
impl ConfigSource for LedgerConfigSource {
    fn describe(&self) -> String {
        format!("ledger:{}", self.name)
    }

    fn authoritative(&self) -> bool {
        true
    }

    fn change_marker(&self) -> Option<(u64, u64)> {
        // A changed budget file moves the marker too (PLT-4643).
        let budgets = self.budgets.as_ref().map_or(0, |b| b.marker());
        Some((
            self.signal.version() ^ budgets.rotate_left(17),
            self.store.external_change_marker().unwrap_or(0),
        ))
    }

    async fn fetch(&self, since: u64) -> Result<ConfigDelivery, SourceError> {
        self.publish(since)
            .map_err(|e| SourceError::Unavailable(format!("ledger: {e}")))
    }
}

/// The key bearer tokens are digested under ([`grant_key`]): the internal
/// credential when one is configured, otherwise a fixed label (a `combined`
/// gateway without data planes never lets the digests leave the process).
pub fn grant_secret(control: &ControlPlaneConfig) -> Vec<u8> {
    match &control.internal_token {
        Some(t) => t.expose().as_bytes().to_vec(),
        None => b"tachyon-serverless/grant-key/in-process".to_vec(),
    }
}

// ---------------------------------------------------------------------------
// change signal on the config repositories
// ---------------------------------------------------------------------------

/// The store with every successful function / revision / alias write bumping
/// `signal`, so an in-process cache knows to refresh before it answers.
pub struct SignalingConfigRepos {
    inner: Arc<dyn StateStore>,
    signal: Arc<ConfigChangeSignal>,
}

impl SignalingConfigRepos {
    pub fn new(inner: Arc<dyn StateStore>, signal: Arc<ConfigChangeSignal>) -> Self {
        Self { inner, signal }
    }

    fn signal<T>(&self, r: Result<T, RepoError>) -> Result<T, RepoError> {
        // Also on an error: a failed write may still have committed on the
        // other side of a broken connection, and a spurious refresh is cheap.
        self.signal.bump();
        r
    }
}

impl FunctionRepository for SignalingConfigRepos {
    fn insert(&self, function: Function) -> Result<(), RepoError> {
        let r = FunctionRepository::insert(&*self.inner, function);
        self.signal(r)
    }
    fn get(&self, id: &FunctionId) -> Result<Option<Function>, RepoError> {
        FunctionRepository::get(&*self.inner, id)
    }
    fn find_by_name(
        &self,
        tenant: &TenantId,
        name: &FunctionName,
    ) -> Result<Option<Function>, RepoError> {
        self.inner.find_by_name(tenant, name)
    }
    fn list(&self, tenant: &TenantId) -> Result<Vec<Function>, RepoError> {
        FunctionRepository::list(&*self.inner, tenant)
    }
    fn update(&self, function: Function) -> Result<(), RepoError> {
        let r = FunctionRepository::update(&*self.inner, function);
        self.signal(r)
    }
}

impl RevisionRepository for SignalingConfigRepos {
    fn allocate_number(&self, function: &FunctionId) -> Result<u64, RepoError> {
        self.inner.allocate_number(function)
    }
    fn insert(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        let r = RevisionRepository::insert(&*self.inner, revision);
        self.signal(r)
    }
    fn get(&self, id: &RevisionId) -> Result<Option<FunctionRevision>, RepoError> {
        RevisionRepository::get(&*self.inner, id)
    }
    fn list_by_function(&self, function: &FunctionId) -> Result<Vec<FunctionRevision>, RepoError> {
        RevisionRepository::list_by_function(&*self.inner, function)
    }
    fn update(&self, revision: FunctionRevision) -> Result<(), RepoError> {
        let r = RevisionRepository::update(&*self.inner, revision);
        self.signal(r)
    }
}

impl AliasRepository for SignalingConfigRepos {
    fn get(
        &self,
        function: &FunctionId,
        name: &AliasName,
    ) -> Result<Option<FunctionAlias>, RepoError> {
        AliasRepository::get(&*self.inner, function, name)
    }
    fn list(&self, function: &FunctionId) -> Result<Vec<FunctionAlias>, RepoError> {
        AliasRepository::list(&*self.inner, function)
    }
    fn insert(&self, alias: FunctionAlias) -> Result<(), RepoError> {
        let r = AliasRepository::insert(&*self.inner, alias);
        self.signal(r)
    }
    fn compare_and_set(
        &self,
        alias: FunctionAlias,
        expected_generation: u64,
    ) -> Result<bool, RepoError> {
        let r = self.inner.compare_and_set(alias, expected_generation);
        self.signal(r)
    }
}
